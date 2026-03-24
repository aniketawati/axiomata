"""
Condition Count Estimator — Predicts how many WHERE conditions a question needs.

Uses P(n_conditions | question_features) learned from LLM labels.

Features:
  - has_and: explicit "and" connecting clauses
  - has_comma_clause: comma separating conditions
  - has_with: "with a/an/the" introducing conditions
  - has_when: "when/where" introducing conditions
  - word_count: question length
  - column_mentions: how many column names appear in the question

This is a simple Bayesian classifier:
  P(n | features) ∝ P(features | n) × P(n)
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"
ORACLE_DIR = Path(__file__).parent / "oracle" / "dataset"


def extract_question_features(question, headers=None):
    """Extract features from a question for condition count estimation."""
    q_lower = question.lower()
    words = q_lower.split()

    # Has "and" connecting clauses (not within a value like "bread and butter")
    # Heuristic: "and a", "and an", "and the", ", and" suggest clause conjunction
    has_and = bool(re.search(r'\band\s+(?:a|an|the)\b', q_lower) or
                   re.search(r',\s*and\b', q_lower))

    # Has comma separating conditions
    has_comma = ", " in q_lower and not q_lower.endswith(",")

    # Has "with" introducing a condition
    has_with = bool(re.search(r'\bwith\s+(?:a|an|the)\b', q_lower))

    # Has "when/where" introducing a condition
    has_when = bool(re.search(r'\bwhen\b', q_lower) and not q_lower.startswith("when"))
    has_where = bool(re.search(r'\bwhere\b', q_lower) and not q_lower.startswith("where"))

    # Word count
    word_count = len(words)

    # Column mentions
    col_mentions = 0
    if headers:
        for h in headers:
            h_lower = h.lower()
            # Check if significant column words appear
            h_words = [w for w in re.findall(r'\b\w{3,}\b', h_lower)
                       if w not in {"the", "and", "for", "from", "with"}]
            if any(w in q_lower for w in h_words):
                col_mentions += 1

    # Count comparison words (suggests additional numeric conditions)
    has_comparison = bool(re.search(
        r'\b(?:more|less|greater|fewer|larger|smaller|higher|lower|at least|at most|over|under|above|below)\s+than\b',
        q_lower
    ))

    return {
        "has_and": has_and,
        "has_comma": has_comma,
        "has_with": has_with,
        "has_when": has_when or has_where,
        "has_comparison": has_comparison,
        "word_count": word_count,
        "col_mentions": col_mentions,
    }


class ConditionEstimator:
    """Estimates the number of WHERE conditions from question features."""

    def __init__(self):
        # P(n | feature_set) learned from LLM labels
        self.feature_weights = {}
        # P(n) prior
        self.prior = {1: 0.69, 2: 0.25, 3: 0.05, 4: 0.01}

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "condition_estimator.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.feature_weights = data.get("feature_weights", {})
                self.prior = {int(k): v for k, v in data.get("prior", {}).items()}

    def estimate(self, question, headers=None):
        """Estimate number of conditions.

        Returns (n_conditions, confidence)
        """
        features = extract_question_features(question, headers)

        if self.feature_weights:
            return self._bayesian_estimate(features)
        return self._rule_estimate(features)

    def _bayesian_estimate(self, features):
        """Estimate using learned feature weights."""
        scores = {}
        for n in [1, 2, 3, 4]:
            n_str = str(n)
            log_score = 0.0
            prior_p = self.prior.get(n, 0.01)
            log_score += _safe_log(prior_p)

            if n_str in self.feature_weights:
                weights = self.feature_weights[n_str]
                for fname, fval in features.items():
                    if fname == "word_count":
                        # Discretize: short (<10), medium (10-15), long (>15)
                        bucket = "short" if fval < 10 else ("medium" if fval < 16 else "long")
                        key = f"word_count_{bucket}"
                        log_score += _safe_log(weights.get(key, 0.33))
                    elif fname == "col_mentions":
                        bucket = "few" if fval < 3 else ("some" if fval < 5 else "many")
                        key = f"col_mentions_{bucket}"
                        log_score += _safe_log(weights.get(key, 0.33))
                    else:
                        p = weights.get(fname, 0.5)
                        if fval:
                            log_score += _safe_log(p)
                        else:
                            log_score += _safe_log(1 - p)

            scores[n] = log_score

        # Softmax
        import math
        max_score = max(scores.values())
        exp_scores = {n: math.exp(s - max_score) for n, s in scores.items()}
        total = sum(exp_scores.values())
        probs = {n: s / total for n, s in exp_scores.items()}

        best_n = max(probs, key=probs.get)
        return best_n, probs[best_n]

    def _rule_estimate(self, features):
        """Fallback rule-based estimation."""
        n = 1

        if features["has_and"]:
            n += 1
        if features["has_comma"]:
            n += 1
        if features["has_with"]:
            n += 1
        if features["has_when"]:
            n += 1

        # Long questions with many column mentions → likely multi-condition
        if features["word_count"] > 15 and features["col_mentions"] >= 4:
            n = max(n, 2)

        n = min(n, 4)
        confidence = 0.7 if n == 1 else 0.5

        return n, confidence


def _safe_log(x):
    import math
    return math.log(max(x, 1e-10))


def build_from_labels():
    """Build condition estimator from LLM-labeled data."""
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    # Load labels
    labels = []
    for f in sorted(ORACLE_DIR.glob("ncond_labeled_*.json")):
        with open(f) as fh:
            data = json.load(fh)
            if isinstance(data, list):
                labels.extend(data)

    print(f"Loaded {len(labels)} condition count labels")
    if not labels:
        print("No labels found.")
        return

    # Compute P(n) prior
    n_counts = Counter(ex.get("n_conditions", 1) for ex in labels)
    total = sum(n_counts.values())
    prior = {n: c / total for n, c in n_counts.items()}
    print(f"Prior P(n): {prior}")

    # Compute P(feature | n) for each feature
    feature_given_n = defaultdict(lambda: defaultdict(list))

    for ex in labels:
        n = ex.get("n_conditions", 1)
        feats = ex.get("question_features", {})
        for fname, fval in feats.items():
            feature_given_n[n][fname].append(fval)

    feature_weights = {}
    for n in [1, 2, 3, 4]:
        weights = {}
        feats = feature_given_n.get(n, {})
        for fname, values in feats.items():
            if fname in ("word_count", "column_mentions"):
                # Discretize
                if fname == "word_count":
                    for bucket, test in [("short", lambda v: v < 10),
                                         ("medium", lambda v: 10 <= v < 16),
                                         ("long", lambda v: v >= 16)]:
                        key = f"{fname}_{bucket}"
                        weights[key] = sum(1 for v in values if test(v)) / max(len(values), 1)
                elif fname == "column_mentions":
                    for bucket, test in [("few", lambda v: v < 3),
                                         ("some", lambda v: 3 <= v < 5),
                                         ("many", lambda v: v >= 5)]:
                        key = f"{fname}_{bucket}"
                        weights[key] = sum(1 for v in values if test(v)) / max(len(values), 1)
            else:
                # Boolean feature: P(True | n)
                weights[fname] = sum(1 for v in values if v) / max(len(values), 1)
        feature_weights[str(n)] = weights

    # Print key findings
    print(f"\nKey feature probabilities:")
    for feat in ["has_and", "has_comma", "has_with", "has_when"]:
        for n in [1, 2, 3]:
            p = feature_weights.get(str(n), {}).get(feat, 0)
            print(f"  P({feat}=True | n={n}) = {p:.2f}")
        print()

    knowledge = {
        "prior": prior,
        "feature_weights": feature_weights,
    }

    with open(KNOWLEDGE_DIR / "condition_estimator.json", "w") as f:
        json.dump(knowledge, f, indent=2)

    print(f"Saved to {KNOWLEDGE_DIR / 'condition_estimator.json'}")
    return knowledge


if __name__ == "__main__":
    build_from_labels()
