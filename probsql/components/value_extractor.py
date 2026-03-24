"""
Value Extractor — Extracts SQL literal values from English phrases.

Handles numbers, strings, enums, booleans, NULL, and computed values.
"""

import re


WORD_NUMBERS = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
    "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60, "seventy": 70,
    "eighty": 80, "ninety": 90, "hundred": 100, "thousand": 1000, "million": 1000000,
}


class ValueExtractor:
    def extract(self, english_phrase, column_info, operator):
        """Extract a SQL value from an English phrase.

        Args:
            english_phrase: The English text
            column_info: dict or object with column_type, enum_values, column_name
            operator: The SQL operator being used

        Returns:
            tuple: (value, value_type, confidence)
        """
        if isinstance(column_info, dict):
            col_type = column_info.get("column_type", column_info.get("type", "VARCHAR"))
            col_name = column_info.get("column_name", column_info.get("name", ""))
            enum_values = column_info.get("enum_values", [])
        else:
            col_type = getattr(column_info, "column_type", "VARCHAR")
            col_name = getattr(column_info, "column_name", "")
            enum_values = getattr(column_info, "enum_values", [])

        base_type = col_type.upper().split("(")[0]
        phrase = english_phrase.strip()

        # IS NULL / IS NOT NULL don't need values
        if operator in ("IS NULL", "IS NOT NULL"):
            return None, "null", 0.95

        # Boolean columns
        if base_type == "BOOLEAN":
            return self._extract_boolean(phrase, col_name)

        # Numeric types
        if base_type in ("INT", "BIGINT", "FLOAT", "DECIMAL"):
            return self._extract_number(phrase)

        # Enum columns
        if enum_values:
            return self._fuzzy_match_enum(phrase, enum_values)

        # Timestamp/Date handled by temporal parser
        if base_type in ("TIMESTAMP", "DATE"):
            return None, "temporal", 0.5

        # For REAL type, try number extraction
        if base_type == "REAL":
            val, vtype, conf = self._extract_number(phrase)
            if val is not None:
                return val, vtype, conf
            # Fall through to string extraction for REAL columns with string values

        # String types (VARCHAR, TEXT, REAL-as-string)
        if base_type in ("VARCHAR", "TEXT", "REAL"):
            return self._extract_string_value(phrase, col_name)

        return self._extract_generic(phrase)

    def _extract_boolean(self, phrase, col_name):
        """Extract boolean value from English phrase."""
        phrase_lower = phrase.lower()

        # Negative indicators
        neg_words = ["not", "isn't", "aren't", "no", "non-", "un", "in", "dis",
                     "false", "inactive", "disabled", "unavailable"]
        for w in neg_words:
            if w in phrase_lower:
                return False, "boolean", 0.85

        # Positive is default for boolean columns
        return True, "boolean", 0.8

    def _extract_number(self, phrase):
        """Extract numeric value from English phrase."""
        phrase_lower = phrase.lower()

        # Remove currency symbols
        cleaned = re.sub(r'[$€£¥]', '', phrase_lower)

        # Try direct number match (including decimals)
        numbers = re.findall(r'(\d+(?:,\d{3})*(?:\.\d+)?)', cleaned)
        if numbers:
            # Take the most prominent number (usually last one near comparison words)
            num_str = numbers[-1].replace(",", "")
            try:
                if "." in num_str:
                    return float(num_str), "number", 0.9
                else:
                    return int(num_str), "number", 0.9
            except ValueError:
                pass

        # Try word numbers
        for word, val in sorted(WORD_NUMBERS.items(), key=lambda x: -len(x[0])):
            if word in phrase_lower:
                return val, "number", 0.8

        # Percentage
        m = re.search(r'(\d+(?:\.\d+)?)\s*%', phrase_lower)
        if m:
            return float(m.group(1)), "number", 0.85

        return None, "unknown", 0.3

    def _fuzzy_match_enum(self, phrase, enum_values):
        """Match phrase against known enum values using fuzzy matching."""
        phrase_lower = phrase.lower()

        # Exact match first
        for ev in enum_values:
            if ev.lower() in phrase_lower:
                return ev, "enum", 0.95

        # Fuzzy match using Levenshtein distance
        phrase_words = set(re.findall(r'\b\w+\b', phrase_lower))
        best_match = None
        best_distance = float('inf')

        for ev in enum_values:
            ev_lower = ev.lower()
            ev_words = set(ev_lower.replace("_", " ").split())

            # Word overlap
            overlap = phrase_words & ev_words
            if overlap:
                return ev, "enum", 0.85

            # Levenshtein on each phrase word vs enum value
            for word in phrase_words:
                dist = self._levenshtein(word, ev_lower)
                if dist < best_distance and dist <= 2:
                    best_distance = dist
                    best_match = ev

        if best_match:
            confidence = max(0.5, 0.9 - best_distance * 0.15)
            return best_match, "enum", confidence

        # If multiple enum values match, return as list for IN operator
        matches = [ev for ev in enum_values if any(
            self._levenshtein(w, ev.lower().replace("_", "")) <= 2
            for w in phrase_words
        )]
        if len(matches) > 1:
            return matches, "enum_list", 0.7

        return enum_values[0] if enum_values else None, "enum", 0.3

    def _extract_string_value(self, phrase, col_name):
        """Extract string value from English phrase."""
        phrase_lower = phrase.lower()

        # Quoted strings (highest priority)
        m = re.search(r'["\']([^"\']+)["\']', phrase)
        if m:
            return m.group(1), "string_literal", 0.95

        # Value after "is", "equals", "named", "called", "titled", "for", "of", "played for"
        for keyword in ["is", "equals", "named", "called", "titled", "labeled",
                        "played for", "plays for", "played at", "from"]:
            m = re.search(rf'\b{keyword}\s+["\']?(.+?)["\']?(?:\s*\??\s*)$', phrase_lower)
            if m:
                val = m.group(1).strip().rstrip("?. ")
                if len(val) > 1:
                    return val, "string_literal", 0.8

        # Proper noun sequences (capitalized multi-word: "Butler CC (KS)", "Amir Johnson")
        # Match sequences of capitalized words possibly with special chars between them
        proper_nouns = re.findall(
            r'(?:^|\s)([A-Z][a-zA-Z]*(?:[\s\-/()]+[A-Za-z0-9.()]+)*)',
            phrase
        )
        # Filter out sentence-initial words by checking if they're at position > 0
        candidates = []
        for pn in proper_nouns:
            pn = pn.strip()
            start_pos = phrase.find(pn)
            # Skip if it's the very first word and only one word
            if start_pos == 0 and " " not in pn and len(pn.split()) == 1:
                # Could be sentence start — check if it looks like a question word
                first_word = pn.split()[0].lower()
                if first_word in ("what", "who", "where", "when", "how", "which", "the"):
                    continue
            if len(pn) > 1:
                candidates.append(pn)

        if candidates:
            # Prefer longer proper nouns (more specific)
            best = max(candidates, key=len)
            return best, "string_literal", 0.7

        # Number-like strings that should stay as strings (e.g., "number 42", "player 3")
        m = re.search(r'\b(?:number|no\.?|#)\s*(\d+)', phrase_lower)
        if m:
            return m.group(1), "string_literal", 0.8

        # Bare numbers at end of phrase for text columns
        m = re.search(r'\b(\d+)\s*\??$', phrase_lower)
        if m:
            return m.group(1), "string_literal", 0.5

        return None, "unknown", 0.3

    def _extract_generic(self, phrase):
        """Generic value extraction as fallback."""
        # Try number first
        val, vtype, conf = self._extract_number(phrase)
        if val is not None:
            return val, vtype, conf

        # Try string
        val, vtype, conf = self._extract_string_value(phrase, "")
        if val is not None:
            return val, vtype, conf

        return None, "unknown", 0.2

    @staticmethod
    def _levenshtein(s1, s2):
        """Compute Levenshtein edit distance between two strings."""
        if len(s1) < len(s2):
            return ValueExtractor._levenshtein(s2, s1)
        if len(s2) == 0:
            return len(s1)

        prev_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            curr_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (c1 != c2)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row

        return prev_row[-1]


if __name__ == "__main__":
    ve = ValueExtractor()

    tests = [
        ("more than $500", {"type": "DECIMAL(10,2)"}, ">="),
        ("active users", {"type": "BOOLEAN", "column_name": "is_active"}, "="),
        ("status is cancelled", {"type": "VARCHAR(20)", "enum_values": ["active", "cancelled", "pending"]}, "="),
        ("named John", {"type": "VARCHAR(100)", "column_name": "name"}, "="),
        ("at least 3 orders", {"type": "INT"}, ">="),
        ("missing email", {"type": "VARCHAR(255)", "column_name": "email"}, "IS NULL"),
    ]

    for phrase, col_info, op in tests:
        val, vtype, conf = ve.extract(phrase, col_info, op)
        print(f"  {phrase:30s} -> value={val}, type={vtype}, conf={conf:.2f}")
