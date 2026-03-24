"""
ColumnResolver — Maps (value, value_type) to the best column in a schema.

Given:
  value="Butler CC (KS)", type="institution", columns=["Player","No.","Position","School/Club Team"]
Returns:
  "School/Club Team" (confidence=0.92)

Uses probability tables P(column_name_pattern | value_type) extracted from WikiSQL.
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


class ColumnResolver:
    def __init__(self):
        # P(column_pattern | value_type) — the core probability table
        self.type_to_column_patterns = {}
        # Column name keywords that strongly indicate a column's purpose
        self.column_keywords = {}
        # Semantic trigger rules from LLM extraction
        self.trigger_rules = []

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "resolver_tables.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.type_to_column_patterns = data.get("type_to_column_patterns", {})
                self.column_keywords = data.get("column_keywords", {})
        # Load semantic trigger rules
        sem_path = kdir / "semantic_rules.json"
        if sem_path.exists():
            with open(sem_path) as f:
                data = json.load(f)
                self.trigger_rules = data.get("trigger_rules", [])

    def resolve(self, value, value_type, columns, exclude_columns=None, question=None):
        """Resolve a value to the best matching column.

        Args:
            value: The extracted value string
            value_type: The classified type (person_name, institution, etc.)
            columns: List of {"name": str, "type": str} dicts
            exclude_columns: Set of column names to exclude (e.g., SELECT column)
            question: The original question text (for trigger phrase matching)

        Returns:
            List of (column_name, confidence) tuples, sorted by confidence desc
        """
        exclude = set(c.lower() for c in (exclude_columns or []))
        candidates = []

        # First: check semantic trigger rules from LLM extraction
        trigger_scores = {}
        if question and self.trigger_rules:
            trigger_scores = self._score_by_triggers(question, columns, exclude)

        for col in columns:
            col_name = col["name"]
            if col_name.lower() in exclude:
                continue

            # Combine trigger score with type-based score
            trigger_score = trigger_scores.get(col_name, 0.0)
            type_score = self._score_column(value, value_type, col_name, col.get("type", "text"))

            # Trigger rules take priority when they fire
            if trigger_score > 0.5:
                score = trigger_score
            elif trigger_score > 0:
                score = max(trigger_score, type_score)
            else:
                score = type_score

            candidates.append((col_name, score))

        candidates.sort(key=lambda x: -x[1])
        return candidates

    def _score_by_triggers(self, question, columns, exclude):
        """Score columns based on semantic trigger rules from LLM extraction."""
        q_lower = question.lower()
        scores = {}

        for rule in self.trigger_rules:
            trigger = rule["trigger"]
            col_pattern = rule["column_pattern"]
            confidence = rule.get("confidence", 0.7)

            # Skip overly generic single-word triggers
            if len(trigger.split()) <= 1 and len(trigger) <= 4:
                continue

            # Check if trigger phrase appears in question
            if trigger in q_lower:
                # Find columns matching the column pattern
                patterns = [p.strip() for p in col_pattern.split("|")]
                for col in columns:
                    col_name = col["name"]
                    if col_name.lower() in exclude:
                        continue
                    col_lower = col_name.lower()
                    col_words = set(re.findall(r'\b\w+\b', col_lower))
                    for p in patterns:
                        if p in col_lower or p in col_words:
                            current = scores.get(col_name, 0)
                            scores[col_name] = max(current, confidence)

        return scores

    def _score_column(self, value, value_type, col_name, col_type):
        """Score how likely a value belongs to a column."""
        col_lower = col_name.lower()
        col_words = set(re.findall(r'\b\w+\b', col_lower))
        score = 0.0

        # 1. Check value_type → column pattern probability table (primary signal)
        if value_type in self.type_to_column_patterns:
            patterns = self.type_to_column_patterns[value_type]
            for pattern, prob in patterns.items():
                if pattern.lower() in col_lower or col_lower in pattern.lower():
                    score = max(score, prob)
                # Check individual words
                pattern_words = set(re.findall(r'\b\w+\b', pattern.lower()))
                overlap = col_words & pattern_words
                if overlap and len(overlap) >= len(pattern_words) * 0.5:
                    score = max(score, prob * 0.8)

        # 2. Hardcoded type→column rules (high confidence fallback)
        type_rules = {
            "person_name": {"player", "name", "winner", "candidate", "person",
                           "incumbent", "commander", "director", "artist", "author",
                           "rider", "driver", "coach", "manager", "captain"},
            "institution": {"school", "club", "team", "university", "college",
                           "company", "network", "party", "organization"},
            "location": {"country", "city", "location", "venue", "capital",
                        "state", "headquarters", "base", "district", "county",
                        "province", "region", "nation"},
            "category": {"position", "type", "genre", "status", "result",
                        "class", "division", "league", "category", "branch",
                        "rating", "conference", "office", "title"},
            "number": {"no", "number", "rank", "score", "points", "goals",
                      "attendance", "crowd", "population", "votes", "episode",
                      "season", "week", "game", "round", "cap"},
            "year_string": {"year", "season", "founded", "established", "elected",
                           "launched", "opened", "date"},
            "season_string": {"year", "season", "years"},
            "date_string": {"date", "air", "premiered", "launched", "opened"},
        }

        if value_type in type_rules:
            keywords = type_rules[value_type]
            if col_words & keywords:
                score = max(score, 0.85)

        # 3. Column type compatibility
        if value_type == "number" and col_type.lower() == "real":
            score = max(score, 0.5)
        elif value_type in ("person_name", "institution", "location", "category",
                           "year_string", "season_string") and col_type.lower() == "text":
            score = max(score, 0.3)

        # 4. Value itself appears in column name (rare but strong signal)
        val_lower = str(value).lower()
        if val_lower in col_lower:
            score = max(score, 0.4)

        # 5. Penalize unlikely matches
        # Person names rarely go in "date", "score", "number" columns
        if value_type == "person_name" and col_words & {"date", "score", "number", "no", "year", "round", "week"}:
            score *= 0.2
        # Numbers rarely go in "name", "player", "country" columns
        if value_type == "number" and col_words & {"name", "player", "country", "city", "person"}:
            score *= 0.3

        return score


def build_resolver_knowledge(oracle_path=None, output_dir=None):
    """Build resolver knowledge from oracle data."""
    output_dir = Path(output_dir) if output_dir else KNOWLEDGE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    if oracle_path is None:
        oracle_path = Path(__file__).parent / "oracle" / "dataset" / "resolver_dev.json"
    with open(oracle_path) as f:
        data = json.load(f)

    # Build P(column_name | value_type)
    type_col_counts = defaultdict(Counter)
    for ex in data:
        vtype = ex.get("value_type", "string")
        col = ex.get("correct_column", "")
        if col:
            type_col_counts[vtype][col] += 1

    type_to_patterns = {}
    for vtype, col_counts in type_col_counts.items():
        total = sum(col_counts.values())
        # Keep columns that appear at least 2 times
        patterns = {col: count / total for col, count in col_counts.items() if count >= 2}
        # Keep top 20 per type
        top = dict(sorted(patterns.items(), key=lambda x: -x[1])[:20])
        type_to_patterns[vtype] = top

    # Build column keyword index
    column_keywords = defaultdict(Counter)
    for ex in data:
        col = ex.get("correct_column", "")
        vtype = ex.get("value_type", "")
        if col:
            for word in re.findall(r'\b\w+\b', col.lower()):
                if len(word) > 1:
                    column_keywords[word][vtype] += 1

    col_kw = {}
    for word, type_counts in column_keywords.items():
        top_type = type_counts.most_common(1)[0]
        if top_type[1] >= 5:
            col_kw[word] = {"primary_type": top_type[0], "count": top_type[1]}

    knowledge = {
        "type_to_column_patterns": type_to_patterns,
        "column_keywords": col_kw,
    }

    with open(output_dir / "resolver_tables.json", "w") as f:
        json.dump(knowledge, f, indent=2)

    print(f"Resolver knowledge:")
    for vtype, patterns in type_to_patterns.items():
        print(f"  {vtype}: {len(patterns)} column patterns (top: {list(patterns.keys())[:3]})")
    print(f"  Column keywords: {len(col_kw)}")

    return knowledge


if __name__ == "__main__":
    build_resolver_knowledge()
