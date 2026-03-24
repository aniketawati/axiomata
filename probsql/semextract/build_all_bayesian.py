"""
Build ALL Bayesian probability tables for the ProbProg path from existing labeled data.

Converts 4 rule-based components to probabilistic:
1. Question type classifier — P(q_type | features)
2. SELECT identifier — P(header_is_SELECT | features)
3. Value type classifier — P(v_type | value_features)
4. Improved span detector — P(start|word,structure) × P(end|word,structure)

All built from EXISTING labeled data — no new LLM calls needed.
"""

import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

ORACLE_DIR = Path(__file__).parent / "oracle" / "dataset"
KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


def build_question_type_classifier():
    """Build P(q_type | features) from condition count labels.

    Question types: lookup, comparison, count, superlative
    """
    labels = []
    for f in sorted(ORACLE_DIR.glob("ncond_labeled_*.json")):
        with open(f) as fh:
            labels.extend(json.load(fh))

    # Classify each question's type from its signals
    type_features = defaultdict(lambda: defaultdict(list))

    for ex in labels:
        q = ex.get("question", "").lower()
        signals = ex.get("condition_signals", [])

        # Determine question type
        if any(w in q for w in ["how many", "how much", "total number", "count"]):
            q_type = "count"
        elif any(w in q for w in ["most", "least", "highest", "lowest", "maximum", "minimum", "largest", "smallest"]):
            q_type = "superlative"
        elif "comparative" in signals or any(w in q for w in ["more than", "less than", "greater", "fewer", "larger", "smaller"]):
            q_type = "comparison"
        else:
            q_type = "lookup"

        # Extract features
        words = q.split()
        features = {
            "qword_what": words[0] if words else "" == "what",
            "qword_who": words[0] if words else "" == "who",
            "qword_how": words[0] if words else "" == "how",
            "qword_which": words[0] if words else "" == "which",
            "qword_where": words[0] if words else "" == "where",
            "qword_when": words[0] if words else "" == "when",
            "qword_name": words[0] if words else "" == "name",
            "has_comparison": bool(re.search(r'\b(more|less|greater|fewer|larger|smaller|higher|lower)\s+than\b', q)),
            "has_superlative": bool(re.search(r'\b(most|least|highest|lowest|maximum|minimum|largest|smallest)\b', q)),
            "has_aggregation": bool(re.search(r'\b(total|average|sum|count|how many|how much)\b', q)),
            "word_count_short": len(words) < 10,
            "word_count_long": len(words) >= 15,
        }

        for fname, fval in features.items():
            type_features[q_type][fname].append(fval)

    # Compute P(feature | q_type)
    q_type_probs = {}
    type_counts = Counter()
    for q_type, feats in type_features.items():
        n = len(list(feats.values())[0]) if feats else 0
        type_counts[q_type] = n
        probs = {}
        for fname, values in feats.items():
            probs[fname] = (sum(1 for v in values if v) + 1) / (len(values) + 2)
        q_type_probs[q_type] = probs

    total = sum(type_counts.values())
    prior = {qt: c / total for qt, c in type_counts.items()}

    return {"prior": prior, "feature_probs": q_type_probs}


def build_select_identifier():
    """Build P(header_is_SELECT | features) from reasoning labels.

    Uses 1500 labels with select_column and select_reasoning.
    """
    labels = []
    for pattern in ["reasoning_labeled_*.json", "r6_labeled_*.json"]:
        for f in sorted(ORACLE_DIR.glob(pattern)):
            with open(f) as fh:
                labels.extend(json.load(fh))

    # Build P(select_reasoning_type | question_word)
    qword_to_reasoning = defaultdict(Counter)
    # Build P(column_keyword ∈ SELECT | question_word)
    qword_to_select_kw = defaultdict(Counter)

    for ex in labels:
        q = ex.get("question", "").lower()
        sel_col = ex.get("select_column", "")
        sel_reason = ex.get("select_reasoning", "")
        qword = q.split()[0] if q.split() else "what"

        qword_to_reasoning[qword][sel_reason] += 1

        # Extract keywords from select column
        for w in re.findall(r'\b\w{3,}\b', sel_col.lower()):
            if w not in {"the", "and", "for", "from"}:
                qword_to_select_kw[qword][w] += 1

    # Normalize
    select_probs = {}
    for qword, kw_counts in qword_to_select_kw.items():
        total = sum(kw_counts.values())
        select_probs[qword] = {kw: c / total for kw, c in kw_counts.most_common(15)}

    reasoning_probs = {}
    for qword, reason_counts in qword_to_reasoning.items():
        total = sum(reason_counts.values())
        reasoning_probs[qword] = {r: c / total for r, c in reason_counts.items()}

    # Also build: P(header_word_is_in_SELECT | header_word_appears_in_question_prefix)
    # When a header word appears in the first 5 words of the question → P(SELECT)
    prefix_select_prob = {"appears_in_prefix": 0, "total_prefix_matches": 0}
    for ex in labels:
        q_words = set(ex.get("question", "").lower().split()[:5])
        sel_col = ex.get("select_column", "").lower()
        headers = ex.get("headers", [])
        for h in headers:
            h_words = set(re.findall(r'\b\w{3,}\b', h.lower()))
            if h_words & q_words:
                prefix_select_prob["total_prefix_matches"] += 1
                if h.lower() == sel_col.lower():
                    prefix_select_prob["appears_in_prefix"] += 1

    p_select_if_prefix = (prefix_select_prob["appears_in_prefix"] + 1) / (prefix_select_prob["total_prefix_matches"] + 2)

    return {
        "qword_select_keywords": select_probs,
        "qword_reasoning": reasoning_probs,
        "P_select_if_in_prefix": p_select_if_prefix,
    }


def build_value_type_classifier():
    """Build P(v_type | value_features) from value span labels.

    Uses 1000 Opus-labeled value spans with value_structure.
    """
    labels = []
    for f in sorted(ORACLE_DIR.glob("vspan_labeled_*.json")):
        with open(f) as fh:
            labels.extend(json.load(fh))

    # Extract features for each labeled value
    type_features = defaultdict(lambda: defaultdict(list))

    for ex in labels:
        v_type = ex.get("value_structure", "string")
        value = ex.get("value_span_in_question", "")
        if not value:
            continue

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

        for fname, fval in features.items():
            type_features[v_type][fname].append(fval)

    # Compute P(feature | v_type)
    vtype_probs = {}
    type_counts = Counter()
    for v_type, feats in type_features.items():
        n = len(list(feats.values())[0]) if feats else 0
        type_counts[v_type] = n
        probs = {}
        for fname, values in feats.items():
            probs[fname] = (sum(1 for v in values if v) + 1) / (len(values) + 2)
        vtype_probs[v_type] = probs

    total = sum(type_counts.values())
    prior = {vt: c / total for vt, c in type_counts.items()}

    return {"prior": prior, "feature_probs": vtype_probs}


def build_improved_span_detector():
    """Build conditional span boundary probabilities from value span labels.

    Improves on current P(start|word) by conditioning on value_structure:
    P(start | left_word, value_structure)
    P(end | right_word, value_structure)
    P(span_length | value_structure)
    """
    labels = []
    for f in sorted(ORACLE_DIR.glob("vspan_labeled_*.json")):
        with open(f) as fh:
            labels.extend(json.load(fh))

    # P(start_signal | value_structure)
    start_by_structure = defaultdict(Counter)
    end_by_structure = defaultdict(Counter)
    length_by_structure = defaultdict(list)

    # P(left_word | start_signal)
    left_word_by_signal = defaultdict(Counter)
    right_word_by_signal = defaultdict(Counter)

    for ex in labels:
        structure = ex.get("value_structure", "string")
        start_sig = ex.get("start_signal", "")
        end_sig = ex.get("end_signal", "")
        left_ctx = ex.get("left_context", "")
        right_ctx = ex.get("right_context", "")
        value = ex.get("value_span_in_question", "")

        start_by_structure[structure][start_sig] += 1
        end_by_structure[structure][end_sig] += 1
        length_by_structure[structure].append(len(value.split()))

        if left_ctx:
            last_word = left_ctx.strip().split()[-1].lower().rstrip(",.")
            left_word_by_signal[start_sig][last_word] += 1
        if right_ctx:
            first_word = right_ctx.strip().split()[0].lower().rstrip(",?.")
            right_word_by_signal[end_sig][first_word] += 1

    # Normalize
    def normalize_counter(counter):
        total = sum(counter.values())
        return {k: v / total for k, v in counter.most_common()}

    span_probs = {
        "P_start_given_structure": {s: normalize_counter(c) for s, c in start_by_structure.items()},
        "P_end_given_structure": {s: normalize_counter(c) for s, c in end_by_structure.items()},
        "P_left_word_given_start": {s: normalize_counter(c) for s, c in left_word_by_signal.items()},
        "P_right_word_given_end": {s: normalize_counter(c) for s, c in right_word_by_signal.items()},
        "avg_length_by_structure": {s: sum(v) / len(v) for s, v in length_by_structure.items()},
    }

    return span_probs


def build_all():
    """Build all Bayesian tables and save."""
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    print("Building all Bayesian components from existing data...")
    print("=" * 60)

    # 1. Question type
    print("\n1. Question Type Classifier")
    qt = build_question_type_classifier()
    print(f"   Types: {qt['prior']}")

    # 2. SELECT identifier
    print("\n2. SELECT Identifier")
    si = build_select_identifier()
    print(f"   P(SELECT if word in prefix): {si['P_select_if_in_prefix']:.2f}")
    print(f"   Question words covered: {list(si['qword_select_keywords'].keys())}")

    # 3. Value type classifier
    print("\n3. Value Type Classifier")
    vt = build_value_type_classifier()
    print(f"   Types: {vt['prior']}")

    # 4. Improved span detector
    print("\n4. Span Detector (improved)")
    sd = build_improved_span_detector()
    print(f"   Avg span length by structure:")
    for s, l in sorted(sd['avg_length_by_structure'].items()):
        print(f"     {s}: {l:.1f} words")

    # Save all
    all_tables = {
        "question_type": qt,
        "select_identifier": si,
        "value_type": vt,
        "span_detector": sd,
    }

    with open(KNOWLEDGE_DIR / "probprog_bayesian_tables.json", "w") as f:
        json.dump(all_tables, f, indent=2)

    print(f"\nAll tables saved to {KNOWLEDGE_DIR / 'probprog_bayesian_tables.json'}")
    return all_tables


if __name__ == "__main__":
    build_all()
