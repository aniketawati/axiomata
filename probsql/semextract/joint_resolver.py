"""
Joint Value-Column Resolver — Scores (value_span, column) pairs together
instead of detecting values and resolving columns sequentially.

P(span, column | question, schema) =
    P(span | question) ×           # span boundary score
    P(column | span, question) ×   # column compatibility given value
    P(match_reason | span, column) # WHY this value belongs to this column

The match_reason probabilities come from 1200 Opus-labeled joint examples:
  name_to_name_column:      person name → Player/Name column
  team_to_team_column:      team name → Team/Opponent column
  place_to_location_column: place → Location/Venue column
  number_to_numeric_column: number → Score/Points column
  number_to_id_column:      number → No./Episode column
  category_to_category_column: category → Position/Result column
  text_to_matching_column:  text matches column name keywords
  context_implies:          verb/preposition links value to column
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from probsql.semextract.span_detector import ValueSpanDetector, SpanCandidate

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"
ORACLE_DIR = Path(__file__).parent / "oracle" / "dataset"

# Column semantic categories (which match_reasons map to which column keywords)
MATCH_REASON_COLUMN_KEYWORDS = {
    "name_to_name_column": {"player", "name", "winner", "candidate", "person",
                            "incumbent", "driver", "rider", "artist", "director",
                            "coach", "manager", "captain", "author", "singer"},
    "team_to_team_column": {"team", "opponent", "club", "away", "home",
                            "school", "university", "college", "affiliate"},
    "place_to_location_column": {"location", "venue", "country", "city", "capital",
                                  "state", "district", "county", "ground", "stadium",
                                  "nation", "headquarters", "base", "region"},
    "number_to_numeric_column": {"score", "points", "goals", "attendance", "crowd",
                                  "population", "total", "rank", "votes", "viewers"},
    "number_to_id_column": {"no", "number", "#", "episode", "week", "round",
                            "game", "pick", "season"},
    "year_to_year_column": {"year", "season", "founded", "elected", "date",
                            "established", "launched"},
    "category_to_category_column": {"position", "result", "type", "status", "class",
                                     "division", "genre", "league", "conference",
                                     "branch", "rating", "format"},
}


def classify_value_for_matching(value):
    """Classify a value span for match_reason determination."""
    if not value:
        return "unknown"

    words = value.split()
    val_lower = value.lower()

    # Number
    try:
        float(value.replace(",", "").replace("$", ""))
        return "number"
    except ValueError:
        pass

    if re.match(r'^\d', value):
        return "number"

    # Year
    if re.match(r'^\d{4}$', value):
        return "year"
    if re.match(r'^\d{4}[-–]\d{2,4}$', value):
        return "year"

    # All capitalized multi-word → likely person or team or place name
    if len(words) >= 2 and all(w[0].isupper() for w in words if w.isalpha()):
        return "proper_noun"

    # Single capitalized word → could be name, place, or category
    if len(words) == 1 and value[0].isupper():
        return "single_proper"

    # Lowercase → likely category or text match
    return "text"


class JointResolver:
    """Scores (value_span, column) pairs jointly.

    For each candidate span × each column, compute:
      score = P(span) × P(match_reason | value_type, col_keywords) × P(disambig)
    """

    def __init__(self):
        self.match_reason_probs = {}     # P(match_reason | value_classification)
        self.disambig_probs = {}         # P(disambig_feature | correct_column)
        self.span_detector = ValueSpanDetector()

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "joint_resolver_tables.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.match_reason_probs = data.get("match_reason_probs", {})
                self.disambig_probs = data.get("disambig_probs", {})

    def resolve(self, question, headers, n_conditions=1, select_col=None):
        """Find the best (value_span, column) pairs jointly.

        Returns list of {column, value, score, match_reason}
        """
        # Generate candidate spans
        if n_conditions == 1:
            spans = []
            single = self.span_detector.detect(question, headers)
            if single:
                spans = [single]
        else:
            spans = self.span_detector.detect_multiple(question, headers, max_spans=n_conditions)

        if not spans:
            return []

        q_lower = question.lower()
        exclude = {select_col.lower()} if select_col else set()

        results = []
        used_columns = set(exclude)

        for span in spans:
            # Score this span against all columns
            value_class = classify_value_for_matching(span.text)
            best_col = None
            best_score = 0.0
            best_reason = "unknown"

            for h in headers:
                if h.lower() in used_columns:
                    continue

                h_lower = h.lower()
                h_words = set(re.findall(r'\b\w+\b', h_lower))

                # Score each possible match_reason
                score, reason = self._score_pair(
                    span, value_class, h, h_words, q_lower
                )

                if score > best_score:
                    best_score = score
                    best_col = h
                    best_reason = reason

            if best_col:
                used_columns.add(best_col.lower())
                results.append({
                    "column": best_col,
                    "value": span.text,
                    "score": best_score,
                    "match_reason": best_reason,
                    "span_score": span.score,
                })

        return results

    def _score_pair(self, span, value_class, header, h_words, q_lower):
        """Score a (value_span, column) pair.

        Returns (score, best_match_reason)
        """
        best_score = 0.0
        best_reason = "unknown"

        # Check each match_reason
        for reason, keywords in MATCH_REASON_COLUMN_KEYWORDS.items():
            if not (h_words & keywords):
                continue

            # Does the value class match this reason?
            reason_score = self._value_reason_compatibility(value_class, reason)

            if reason_score > best_score:
                best_score = reason_score
                best_reason = reason

        # Also check: column name proximity to value in question
        prox_score = self._proximity_score(span, header, q_lower)
        if prox_score > best_score:
            best_score = prox_score
            best_reason = "text_to_matching_column"

        # Check: learned match_reason probabilities
        if self.match_reason_probs:
            for reason, prob in self.match_reason_probs.items():
                keywords = MATCH_REASON_COLUMN_KEYWORDS.get(reason, set())
                if h_words & keywords:
                    learned_score = prob * self._value_reason_compatibility(value_class, reason)
                    if learned_score > best_score:
                        best_score = learned_score
                        best_reason = reason

        # Span score as a multiplier (good spans boost the pair score)
        final_score = best_score * (0.5 + 0.5 * span.score)

        return final_score, best_reason

    def _value_reason_compatibility(self, value_class, reason):
        """P(match_reason | value_classification)"""
        compat = {
            ("proper_noun", "name_to_name_column"): 0.7,
            ("proper_noun", "team_to_team_column"): 0.6,
            ("proper_noun", "place_to_location_column"): 0.6,
            ("single_proper", "name_to_name_column"): 0.5,
            ("single_proper", "category_to_category_column"): 0.6,
            ("single_proper", "place_to_location_column"): 0.4,
            ("number", "number_to_numeric_column"): 0.7,
            ("number", "number_to_id_column"): 0.6,
            ("year", "year_to_year_column"): 0.9,
            ("text", "category_to_category_column"): 0.6,
            ("text", "text_to_matching_column"): 0.5,
        }
        return compat.get((value_class, reason), 0.2)

    def _proximity_score(self, span, header, q_lower):
        """Score based on column name words appearing near the value."""
        h_lower = header.lower()
        h_words = [w for w in re.findall(r'\b\w{3,}\b', h_lower)
                   if w not in {"the", "and", "for", "from", "with", "of"}]
        if not h_words:
            return 0.0

        val_lower = span.text.lower()
        val_pos = q_lower.find(val_lower)
        if val_pos < 0:
            return 0.0

        best = 0.0
        for w in h_words:
            w_pos = q_lower.find(w)
            if w_pos >= 0:
                dist = abs(w_pos - val_pos)
                if dist < 20:
                    best = max(best, 0.8)
                elif dist < 40:
                    best = max(best, 0.5)
                else:
                    best = max(best, 0.3)
        return best


def build_joint_tables():
    """Build probability tables from Opus-labeled joint data."""
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    labels = []
    for f in sorted(ORACLE_DIR.glob("joint_labeled_*.json")):
        with open(f) as fh:
            data = json.load(fh)
            if isinstance(data, list):
                labels.extend(data)

    print(f"Loaded {len(labels)} joint labels")
    if not labels:
        print("No labels found.")
        return

    # Compute P(match_reason)
    reason_counts = Counter()
    disambig_counts = Counter()

    for ex in labels:
        for pair in ex.get("value_column_pairs", []):
            reason = pair.get("match_reason", "unknown")
            reason_counts[reason] += 1

        for feat in ex.get("column_disambiguation_features", []):
            disambig_counts[feat] += 1

    total_reasons = sum(reason_counts.values())
    match_reason_probs = {r: c / total_reasons for r, c in reason_counts.items()}

    total_disambig = sum(disambig_counts.values())
    disambig_probs = {d: c / total_disambig for d, c in disambig_counts.items()}

    tables = {
        "match_reason_probs": match_reason_probs,
        "disambig_probs": disambig_probs,
    }

    with open(KNOWLEDGE_DIR / "joint_resolver_tables.json", "w") as f:
        json.dump(tables, f, indent=2)

    print(f"\nMatch reason distribution:")
    for r, p in sorted(match_reason_probs.items(), key=lambda x: -x[1]):
        print(f"  {r}: {p:.0%}")

    print(f"\nDisambiguation features:")
    for d, p in sorted(disambig_probs.items(), key=lambda x: -x[1]):
        print(f"  {d}: {p:.0%}")

    return tables


if __name__ == "__main__":
    build_joint_tables()
