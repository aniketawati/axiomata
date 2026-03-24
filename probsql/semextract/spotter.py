"""
ValueSpotter — Extracts candidate values with types from question text.

Given "What position does the player who played for Butler CC (KS) play?",
outputs:
  [
    {"value": "Butler CC (KS)", "type": "institution", "confidence": 0.9},
  ]

Value types: person_name, institution, location, number, year_string,
             season_string, category, date_string, string
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"

# Words that are NOT values (question structure words)
STOP_WORDS = {
    "what", "who", "where", "when", "how", "which", "the", "a", "an",
    "is", "are", "was", "were", "did", "does", "do", "has", "have", "had",
    "that", "this", "of", "in", "on", "for", "with", "by", "at", "to",
    "and", "or", "not", "all", "many", "much", "some", "any", "no",
    "be", "been", "being", "can", "could", "would", "should", "will", "shall",
    "may", "might", "must", "its", "it", "he", "she", "they", "them",
    "their", "there", "here", "than", "then", "also", "very", "most",
    "total", "number", "amount",
}

# Verb/function words that appear in questions but aren't values
VERB_WORDS = {
    "play", "played", "plays", "playing",
    "score", "scored", "scoring",
    "win", "won", "winning",
    "come", "came", "comes",
    "go", "went", "goes",
    "take", "took", "takes",
    "make", "made", "makes",
    "get", "got", "gets",
    "give", "gave", "gives",
    "wear", "wears", "wore",
    "represent", "represented", "represents",
    "attend", "attended", "attends",
    "list", "listed",
    "locate", "located",
    "elect", "elected",
    "born", "live", "lived",
    "direct", "directed",
    "write", "wrote", "written",
}


class ValueSpotter:
    def __init__(self):
        self.value_type_patterns = {}  # pattern → value_type
        self.column_value_types = {}   # column_name_pattern → typical value_type

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "spotter_patterns.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.value_type_patterns = data.get("value_type_patterns", {})
                self.column_value_types = data.get("column_value_types", {})

    def spot(self, question, headers=None):
        """Extract candidate values from a question.

        Args:
            question: The English question text
            headers: Optional list of column headers for context

        Returns:
            List of dicts: [{value, type, confidence, start, end}, ...]
        """
        candidates = []
        q = question.strip()

        # 1. Extract proper noun sequences (capitalized multi-word)
        candidates.extend(self._extract_proper_nouns(q))

        # 2. Extract numbers
        candidates.extend(self._extract_numbers(q))

        # 3. Extract year/season strings
        candidates.extend(self._extract_temporal_strings(q))

        # 4. Extract quoted strings
        candidates.extend(self._extract_quoted(q))

        # Deduplicate — prefer longer matches, higher confidence
        candidates = self._deduplicate(candidates)

        # Filter out values that are just question structure
        candidates = [c for c in candidates if not self._is_stop_value(c["value"])]

        return candidates

    def _extract_proper_nouns(self, text):
        """Extract capitalized word sequences as potential entity values."""
        candidates = []

        # Multi-word proper nouns: "Butler CC (KS)", "Amir Johnson", "New York"
        # Match: Capital word followed by more capital words, possibly with connectors
        for m in re.finditer(
            r'(?<!\w)([A-Z][a-z]+(?:\s+(?:[A-Z][a-z]+|[A-Z]{2,}|of|the|and|de|von|van|le|la))*'
            r'(?:\s*\([^)]+\))?)',
            text
        ):
            val = m.group(1).strip()
            # Skip if it's a question word at start
            if m.start() == 0:
                first = val.split()[0].lower()
                if first in STOP_WORDS:
                    continue
            if len(val) > 1:
                vtype = self._classify_proper_noun(val)
                candidates.append({
                    "value": val, "type": vtype,
                    "confidence": 0.8, "start": m.start(), "end": m.end(),
                })

        # Also try all-caps words/abbreviations that might be values
        for m in re.finditer(r'\b([A-Z]{2,}(?:\s+[A-Z]{2,})*)\b', text):
            val = m.group(1)
            if val not in {"AND", "OR", "NOT", "THE", "FOR"}:
                candidates.append({
                    "value": val, "type": "abbreviation",
                    "confidence": 0.6, "start": m.start(), "end": m.end(),
                })

        return candidates

    def _extract_numbers(self, text):
        """Extract numeric values."""
        candidates = []
        # Match standalone numbers (not embedded in alphanumeric codes)
        for m in re.finditer(r'(?<![a-zA-Z])(\d[\d,]*\.?\d*)(?![a-zA-Z])', text):
            val = m.group(1).replace(",", "")
            # Don't extract if it's part of a year/season pattern
            full_context = text[max(0, m.start()-5):m.end()+5]
            if re.search(r'\d{4}[-–]\d{2,4}', full_context):
                continue  # handled by temporal
            # Don't extract if it's part of a score like "7-2" or "3–2"
            if re.search(r'\d+\s*[-–]\s*\d+', full_context):
                continue
            candidates.append({
                "value": val, "type": "number",
                "confidence": 0.85, "start": m.start(), "end": m.end(),
            })
        return candidates

    def _extract_temporal_strings(self, text):
        """Extract year and season strings (treated as string values, not dates)."""
        candidates = []
        # Season patterns: "2005-06", "1996-97"
        for m in re.finditer(r'\b(\d{4}[-–]\d{2,4})\b', text):
            candidates.append({
                "value": m.group(1), "type": "season_string",
                "confidence": 0.9, "start": m.start(), "end": m.end(),
            })
        # Bare years: "2005", "1996" (only if 4 digits and plausible year)
        for m in re.finditer(r'\b(\d{4})\b', text):
            year = int(m.group(1))
            if 1800 <= year <= 2100:
                # Check it's not already part of a season pattern
                full = text[max(0, m.start()-1):m.end()+4]
                if not re.search(r'\d{4}[-–]\d', full):
                    candidates.append({
                        "value": m.group(1), "type": "year_string",
                        "confidence": 0.7, "start": m.start(), "end": m.end(),
                    })
        return candidates

    def _extract_quoted(self, text):
        """Extract quoted strings."""
        candidates = []
        for m in re.finditer(r'["\']([^"\']+)["\']', text):
            candidates.append({
                "value": m.group(1), "type": "string",
                "confidence": 0.95, "start": m.start(), "end": m.end(),
            })
        return candidates

    def _classify_proper_noun(self, value):
        """Classify a proper noun's likely type."""
        val_lower = value.lower()

        # Check learned patterns
        for pattern, vtype in self.value_type_patterns.items():
            if re.search(pattern, val_lower):
                return vtype

        # Heuristic classification
        words = value.split()
        if len(words) == 1:
            return "person_name"  # single capitalized word, likely a name

        # Multi-word: check for institution indicators
        inst_words = {"university", "college", "school", "institute", "academy",
                      "cc", "fc", "united", "city", "team", "club"}
        if any(w.lower() in inst_words for w in words):
            return "institution"

        # Location indicators
        loc_words = {"island", "islands", "county", "state", "republic", "kingdom"}
        if any(w.lower() in loc_words for w in words):
            return "location"

        # Default: person name (most common in WikiSQL)
        return "person_name"

    def _deduplicate(self, candidates):
        """Remove overlapping candidates, keeping higher confidence."""
        if not candidates:
            return []
        # Sort by confidence desc, then length desc
        candidates.sort(key=lambda c: (-c["confidence"], -len(c["value"])))
        result = []
        used_spans = []
        for c in candidates:
            # Check overlap with already-selected spans
            overlaps = False
            for start, end in used_spans:
                if not (c["end"] <= start or c["start"] >= end):
                    overlaps = True
                    break
            if not overlaps:
                result.append(c)
                used_spans.append((c["start"], c["end"]))
        return result

    def _is_stop_value(self, value):
        """Check if a value is just a question structure word."""
        words = set(re.findall(r'\b\w+\b', value.lower()))
        return words.issubset(STOP_WORDS | VERB_WORDS)


def build_spotter_knowledge(oracle_path=None, output_dir=None):
    """Build spotter knowledge from oracle data."""
    output_dir = Path(output_dir) if output_dir else KNOWLEDGE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    if oracle_path is None:
        oracle_path = Path(__file__).parent / "oracle" / "dataset" / "spotter_dev.json"
    with open(oracle_path) as f:
        data = json.load(f)

    # Build column_name → value_type mapping
    col_value_types = defaultdict(Counter)
    for ex in data:
        col = ex.get("column_name", "")
        vtype = ex.get("value_type", "")
        if col and vtype:
            col_value_types[col.lower()][vtype] += 1

    col_types = {}
    for col, type_counts in col_value_types.items():
        top = type_counts.most_common(1)[0][0]
        col_types[col] = top

    knowledge = {
        "column_value_types": col_types,
        "value_type_patterns": {},  # could add regex patterns learned from data
    }

    with open(output_dir / "spotter_patterns.json", "w") as f:
        json.dump(knowledge, f, indent=2)

    print(f"Spotter knowledge: {len(col_types)} column→type mappings")
    return knowledge


if __name__ == "__main__":
    build_spotter_knowledge()
