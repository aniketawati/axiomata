"""
ColumnResolver — Maps (value, value_type, question, schema) to the best WHERE column.

Uses Bayesian chaining of simple conditional probability factors:
  P(col | question) = P(col|value_type) × P(col|trigger) × P(col≠SELECT) × P(col|schema)

Each factor is a simple lookup table. The multiplication IS the reasoning —
no attention mechanism needed.

Steps:
  1. Direct mention: Does a column name appear in the question?
  2. Value extraction: What candidate values exist in the question?
  3. Type classification: What type is each value? (person, institution, number, ...)
  4. Type compatibility: Which columns accept this value type?
  5. SELECT exclusion: Which column is being asked about? Exclude it.
  6. Trigger boost: Do verb/preposition phrases hint at a specific column?
"""

import json
import re
from collections import defaultdict
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"

# Step 4: P(column accepts value_type)
# Maps value_type → set of column name keywords that accept it
TYPE_COLUMN_KEYWORDS = {
    "person_name": {"player", "name", "winner", "candidate", "person", "incumbent",
                    "commander", "director", "artist", "author", "rider", "driver",
                    "coach", "manager", "captain", "member", "representative",
                    "minister", "leader", "actor", "actress", "singer", "composer"},
    "institution": {"school", "club", "team", "university", "college", "company",
                    "network", "party", "organization", "affiliate", "employer",
                    "sponsor", "carrier", "airline", "publisher"},
    "location": {"country", "city", "location", "venue", "capital", "state",
                 "headquarters", "base", "district", "county", "province",
                 "region", "nation", "hometown", "birthplace", "ground",
                 "stadium", "arena"},
    "category": {"position", "type", "genre", "status", "result", "class",
                 "division", "league", "category", "branch", "rating",
                 "conference", "office", "title", "rank", "role", "format"},
    "number": {"no", "number", "#", "rank", "score", "points", "goals",
               "attendance", "crowd", "population", "votes", "episode",
               "season", "week", "game", "round", "cap", "total", "pick",
               "weight", "height", "age", "wins", "losses"},
    "year_string": {"year", "season", "founded", "established", "elected",
                    "launched", "opened", "date", "first elected", "years"},
    "season_string": {"year", "season", "years", "term"},
    "date_string": {"date", "air date", "premiered", "launched", "opened",
                    "original air date", "release date"},
}

# Step 6: Trigger phrase → column keyword patterns
# (verb/preposition phrases that hint at a WHERE column)
TRIGGER_RULES = [
    # Verb relations
    (r'\bplayed?\s+for\b', {"school", "team", "club", "university", "college"}),
    (r'\bplays?\s+for\b', {"school", "team", "club"}),
    (r'\bplayed?\s+at\b', {"school", "venue", "stadium", "ground"}),
    (r'\bplayed?\s+(?:in|on)\b', {"team", "position", "year", "season"}),
    (r'\bwears?\s+(?:number|no\.?|#)', {"no", "number", "#"}),
    (r'\brepresent', {"country", "nation", "team"}),
    (r'\bdirected\s+by\b', {"director", "directed"}),
    (r'\bwritten\s+by\b', {"writer", "written", "author"}),
    (r'\bcoached?\s+by\b', {"coach", "manager"}),
    (r'\bmanaged?\s+by\b', {"manager", "coach"}),
    (r'\bborn\s+in\b', {"birthplace", "country", "city", "birth"}),
    (r'\belected\s+in\b', {"year", "elected", "first elected"}),
    (r'\baired\s+(?:on|in)\b', {"date", "air date", "original air date"}),
    (r'\bwon\s+(?:in|at)\b', {"year", "tournament", "event"}),
    (r'\bscored?\s+(?:in|at|against)\b', {"opponent", "game", "round", "match"}),
    # Preposition signals
    (r'\bfrom\s+(?=[A-Z])', {"country", "city", "location", "school", "team", "state"}),
    (r'\bat\s+(?=[A-Z])', {"venue", "location", "stadium", "ground", "school"}),
    (r'\bagainst\s+', {"opponent", "team", "away"}),
]

# Step 5: Question word → SELECT column type hints
SELECT_HINTS = {
    "who": {"player", "name", "person", "winner", "candidate", "incumbent", "driver", "rider"},
    "where": {"location", "venue", "city", "country", "place", "ground", "stadium"},
    "when": {"date", "year", "time", "season", "day"},
    "how many": {"count", "total", "number"},
    "how much": {"amount", "price", "cost", "salary"},
}


class ColumnResolver:
    def __init__(self):
        self.trigger_rules = TRIGGER_RULES
        self.type_column_keywords = TYPE_COLUMN_KEYWORDS
        self.select_hints = SELECT_HINTS
        self.learned_triggers = []

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        sem_path = kdir / "semantic_rules.json"
        if sem_path.exists():
            with open(sem_path) as f:
                data = json.load(f)
                self.learned_triggers = data.get("trigger_rules", [])

    def resolve(self, value, value_type, columns, question=None, exclude_columns=None):
        """Resolve a value to the best matching column using Bayesian chaining.

        Empirical base rates from 1500 LLM-labeled examples:
          65% column_name_mentioned (proximity to value in question)
          14% trigger_phrase_indicates (verb/prep pattern)
          12% value_is_entity_name / value_type_match
           9% other

        P(col) = 0.65 * P(proximity) + 0.14 * P(trigger) + 0.12 * P(type) + 0.09 * base

        Args:
            value: The extracted value string
            value_type: The classified type
            columns: List of {"name": str, "type": str} dicts
            question: The original question text
            exclude_columns: Columns to exclude (e.g., SELECT column)

        Returns:
            List of (column_name, confidence) tuples, sorted desc
        """
        exclude = set(c.lower() for c in (exclude_columns or []))
        q_lower = (question or "").lower()
        val_lower = str(value).lower()

        scores = {}
        for col in columns:
            col_name = col["name"]
            if col_name.lower() in exclude:
                continue

            col_lower = col_name.lower()
            col_words = set(re.findall(r'\b\w+\b', col_lower))

            # Factor 1 (base rate 0.76): Column name proximity to value
            f_prox = self._score_proximity(col_name, val_lower, q_lower)

            # Factor 2 (base rate 0.13): Trigger phrase
            f_trigger = self._score_triggers(q_lower, col_words)

            # Factor 3 (base rate 0.07): Value type compatibility
            f_type = self._score_type_compat(value_type, col_words)

            # Weighted combination using empirical base rates (1500 labeled examples)
            score = 0.65 * f_prox + 0.14 * f_trigger + 0.12 * f_type + 0.09 * 0.3

            scores[col_name] = score

        result = sorted(scores.items(), key=lambda x: -x[1])
        return result

    def _score_proximity(self, col_name, val_lower, q_lower):
        """P(column | proximity to value in question).

        The dominant signal (76% of cases). Checks if column name
        or its significant words appear near the value in the question.
        """
        col_lower = col_name.lower()

        # Find value position in question
        val_pos = q_lower.find(val_lower)
        if val_pos < 0:
            # Try numeric match
            for num in re.findall(r'\d+', val_lower):
                pos = q_lower.find(num)
                if pos >= 0:
                    val_pos = pos
                    break

        # Get significant column words
        col_words = re.findall(r'\b\w+\b', col_lower)
        sig_words = [w for w in col_words if len(w) > 2 and w not in
                     {"the", "and", "for", "from", "with", "of", "in", "at", "by", "to"}]

        if not sig_words:
            return 0.0

        # Exact full column name in question
        if col_lower in q_lower:
            col_pos = q_lower.find(col_lower)
            if val_pos >= 0:
                distance = abs(col_pos - val_pos)
                return 0.95 if distance < 50 else 0.7
            return 0.6

        # Significant words near value
        best = 0.0
        for w in sig_words:
            w_pos = q_lower.find(w)
            if w_pos >= 0:
                if val_pos >= 0:
                    distance = abs(w_pos - val_pos)
                    if distance < 30:
                        best = max(best, 0.8)
                    elif distance < 60:
                        best = max(best, 0.5)
                    else:
                        best = max(best, 0.3)
                else:
                    best = max(best, 0.3)

        return best

    def identify_select_column(self, question, headers):
        """Identify which column the question is asking about (SELECT target).

        This column should be EXCLUDED from WHERE column candidates.
        Empirical: 86% identified by column name after question word.
        """
        q_lower = question.lower().rstrip("?").strip()
        candidates = []

        # Pattern 1 (86%): Column name appears right after question word
        # "What POSITION does...", "What is the SCORE...", "Which TEAM..."
        qword_patterns = [
            r'^what\s+(?:is\s+(?:the\s+)?|are\s+(?:the\s+)?|was\s+(?:the\s+)?|were\s+(?:the\s+)?)?(\w[\w\s/()#.,-]*?)(?:\s+(?:of|for|did|does|do|is|are|was|were|when|where|that|who|which|with|in|on|at|from|has|have|had)\b)',
            r'^what\s+(\w[\w\s/()#.,-]*?)\s*$',
            r'^which\s+(\w[\w\s/()#.,-]*?)(?:\s+(?:did|does|do|is|are|was|were|has|have|had)\b)',
            r'^how\s+many\s+(\w[\w\s/()#.,-]*?)(?:\s+(?:did|does|do|is|are|was|were|has|have|had)\b)',
        ]

        for pattern in qword_patterns:
            m = re.match(pattern, q_lower)
            if m:
                hint = m.group(1).strip()
                # Match hint against headers
                for h in headers:
                    h_lower = h.lower()
                    if h_lower == hint or h_lower in hint or hint in h_lower:
                        candidates.append((h, 0.95, "direct_after_qword"))
                    else:
                        # Word overlap
                        hint_words = set(re.findall(r'\b\w{3,}\b', hint))
                        h_words = set(re.findall(r'\b\w{3,}\b', h_lower))
                        overlap = hint_words & h_words
                        if overlap:
                            score = 0.85 * len(overlap) / max(len(h_words), 1)
                            candidates.append((h, score, "partial_after_qword"))
                break  # use first matching pattern

        # Pattern 2 (10%): Question word implies SELECT type
        # "Who..." → person/name column, "Where..." → location column
        if not candidates:
            for q_word, col_keywords in self.select_hints.items():
                if q_lower.startswith(q_word):
                    for h in headers:
                        h_words = set(re.findall(r'\b\w+\b', h.lower()))
                        if h_words & col_keywords:
                            candidates.append((h, 0.7, "question_word"))

        if candidates:
            candidates.sort(key=lambda x: -x[1])
            return candidates[0][0]

        return None

    def _score_type_compat(self, value_type, col_words):
        """P(column | value_type): Does this column accept values of this type?"""
        keywords = self.type_column_keywords.get(value_type, set())
        if col_words & keywords:
            return 0.85
        return 0.1

    def _score_direct_mention(self, col_name, q_lower):
        """P(column | direct_mention): Is the column name in the question?"""
        col_lower = col_name.lower()
        if col_lower in q_lower:
            return 0.9
        # Partial: check significant words
        words = re.findall(r'\b\w+\b', col_lower)
        sig_words = [w for w in words if len(w) > 3 and w not in {"the", "and", "for", "from", "with"}]
        if sig_words:
            matches = sum(1 for w in sig_words if w in q_lower)
            if matches == len(sig_words):
                return 0.8
            elif matches > 0:
                return 0.5
        return 0.0

    def _score_triggers(self, q_lower, col_words):
        """P(column | trigger_phrase): Do verb/prep phrases hint at this column?"""
        best = 0.0
        for pattern, keywords in self.trigger_rules:
            if re.search(pattern, q_lower):
                if col_words & keywords:
                    best = max(best, 0.9)

        # Also check learned trigger rules
        for rule in self.learned_triggers:
            trigger = rule.get("trigger", "")
            if len(trigger) <= 4:
                continue
            if trigger in q_lower:
                col_pattern = rule.get("column_pattern", "")
                patterns = {p.strip() for p in col_pattern.split("|")}
                if col_words & patterns:
                    conf = rule.get("confidence", 0.7)
                    best = max(best, conf)

        return best

    def _score_sql_type(self, value_type, sql_type):
        """P(column | sql_type_compat): Does SQL type match value type?"""
        sql_lower = sql_type.lower()
        if value_type == "number" and sql_lower == "real":
            return 0.6
        if value_type in ("person_name", "institution", "location", "category") and sql_lower == "text":
            return 0.4
        return 0.2


def build_resolver_knowledge(oracle_path=None, output_dir=None):
    """Build resolver knowledge from oracle data."""
    output_dir = Path(output_dir) if output_dir else KNOWLEDGE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    if oracle_path is None:
        oracle_path = Path(__file__).parent / "oracle" / "dataset" / "resolver_dev.json"
    with open(oracle_path) as f:
        data = json.load(f)

    from collections import Counter
    type_col_counts = defaultdict(Counter)
    for ex in data:
        vtype = ex.get("value_type", "string")
        col = ex.get("correct_column", "")
        if col:
            type_col_counts[vtype][col] += 1

    type_to_patterns = {}
    for vtype, col_counts in type_col_counts.items():
        total = sum(col_counts.values())
        patterns = {col: count / total for col, count in col_counts.items() if count >= 2}
        top = dict(sorted(patterns.items(), key=lambda x: -x[1])[:20])
        type_to_patterns[vtype] = top

    knowledge = {"type_to_column_patterns": type_to_patterns}
    with open(output_dir / "resolver_tables.json", "w") as f:
        json.dump(knowledge, f, indent=2)

    print(f"Resolver knowledge saved ({len(type_to_patterns)} value types)")
    return knowledge


if __name__ == "__main__":
    build_resolver_knowledge()
