"""
Bayesian ProbProg — Wraps ProbabilisticResolver with fully Bayesian components.

Replaces rule-based steps with probability tables built from LLM labels:
1. Question type: P(q_type | features) from 1316 labels
2. SELECT identifier: P(select | question_word, header_overlap) from 1500 labels
3. Value type: P(v_type | value_features) from 1000 labels

Loads tables from probprog_bayesian_tables.json.
"""

import json
import math
import re
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


class BayesianClassifier:
    """Generic Naive Bayes classifier using feature probability tables."""

    def __init__(self, prior, feature_probs):
        self.prior = prior  # {class: P(class)}
        self.feature_probs = feature_probs  # {class: {feature: P(True|class)}}

    def classify(self, features):
        """Returns (best_class, probability)."""
        scores = {}
        for cls, prior_p in self.prior.items():
            log_score = math.log(prior_p + 1e-10)
            cls_probs = self.feature_probs.get(cls, {})
            for fname, fval in features.items():
                p_true = cls_probs.get(fname, 0.5)
                if isinstance(fval, bool):
                    if fval:
                        log_score += math.log(p_true + 1e-10)
                    else:
                        log_score += math.log(1 - p_true + 1e-10)
            scores[cls] = log_score

        # Softmax
        max_score = max(scores.values())
        exp_scores = {c: math.exp(s - max_score) for c, s in scores.items()}
        total = sum(exp_scores.values())
        probs = {c: s / total for c, s in exp_scores.items()}

        best = max(probs, key=probs.get)
        return best, probs[best]


class BayesianQuestionType:
    """P(q_type | question_features)"""

    def __init__(self):
        self.classifier = None

    def load(self, tables):
        qt = tables.get("question_type", {})
        if qt:
            self.classifier = BayesianClassifier(qt["prior"], qt["feature_probs"])

    def classify(self, question):
        if not self.classifier:
            return "lookup", 0.5

        q_lower = question.lower()
        words = q_lower.split()
        qword = words[0] if words else "what"

        features = {
            "qword_what": qword == "what",
            "qword_who": qword == "who",
            "qword_how": qword == "how",
            "qword_which": qword == "which",
            "qword_where": qword == "where",
            "qword_when": qword == "when",
            "qword_name": qword == "name",
            "has_comparison": bool(re.search(r'\b(more|less|greater|fewer|larger|smaller|higher|lower)\s+than\b', q_lower)),
            "has_superlative": bool(re.search(r'\b(most|least|highest|lowest|maximum|minimum|largest|smallest)\b', q_lower)),
            "has_aggregation": bool(re.search(r'\b(total|average|sum|count|how many|how much)\b', q_lower)),
            "word_count_short": len(words) < 10,
            "word_count_long": len(words) >= 15,
        }

        return self.classifier.classify(features)


class BayesianSelectIdentifier:
    """P(header_is_SELECT | features)"""

    def __init__(self):
        self.qword_keywords = {}
        self.p_select_if_prefix = 0.5

    def load(self, tables):
        si = tables.get("select_identifier", {})
        self.qword_keywords = si.get("qword_select_keywords", {})
        self.p_select_if_prefix = si.get("P_select_if_in_prefix", 0.5)

    def identify(self, question, headers):
        q_lower = question.lower()
        words = q_lower.split()
        qword = words[0] if words else "what"

        candidates = []

        # Score each header
        for h in headers:
            h_lower = h.lower()
            h_words = set(re.findall(r'\b\w{3,}\b', h_lower))
            score = 0.0

            # Feature 1: Header word appears in question prefix (first 5 words)
            prefix_words = set(words[:5])
            prefix_overlap = h_words & prefix_words
            if prefix_overlap:
                score += self.p_select_if_prefix * len(prefix_overlap)

            # Feature 2: Question word → expected SELECT column keywords
            kw_probs = self.qword_keywords.get(qword, {})
            for w in h_words:
                if w in kw_probs:
                    score += kw_probs[w]

            # Feature 3: Full header name in question prefix
            if h_lower in " ".join(words[:6]):
                score += 0.9

            candidates.append((h, score))

        candidates.sort(key=lambda x: -x[1])
        if candidates and candidates[0][1] > 0.1:
            return candidates[0][0]
        return None


class BayesianValueType:
    """P(v_type | value_features)"""

    def __init__(self):
        self.classifier = None

    def load(self, tables):
        vt = tables.get("value_type", {})
        if vt:
            self.classifier = BayesianClassifier(vt["prior"], vt["feature_probs"])

    def classify(self, value):
        if not self.classifier or not value:
            return "string", 0.5

        words = value.split()
        features = {
            "first_char_upper": value[0].isupper() if value else False,
            "all_words_upper": all(w[0].isupper() for w in words if w.isalpha()) if words else False,
            "starts_with_digit": bool(re.match(r'\d', value)),
            "has_digits": bool(re.search(r'\d', value)),
            "has_special": bool(re.search(r'[-–/().,$#=]', value)),
            "has_comma": "," in value,
            "single_word": len(words) == 1,
            "two_words": len(words) == 2,
            "multi_word": len(words) >= 3,
            "short_value": len(value) < 5,
            "long_value": len(value) > 20,
            "all_lowercase": value == value.lower(),
            "has_parentheses": "(" in value,
        }

        return self.classifier.classify(features)


class FullBayesianProbProg:
    """Fully Bayesian ProbProg path — all components use probability tables."""

    def __init__(self):
        self.q_type_classifier = BayesianQuestionType()
        self.select_identifier = BayesianSelectIdentifier()
        self.value_type_classifier = BayesianValueType()
        self.tables_loaded = False

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "probprog_bayesian_tables.json"
        if path.exists():
            with open(path) as f:
                tables = json.load(f)
            self.q_type_classifier.load(tables)
            self.select_identifier.load(tables)
            self.value_type_classifier.load(tables)
            self.tables_loaded = True

    def classify_question_type(self, question):
        return self.q_type_classifier.classify(question)

    def identify_select(self, question, headers):
        return self.select_identifier.identify(question, headers)

    def classify_value_type(self, value):
        return self.value_type_classifier.classify(value)
