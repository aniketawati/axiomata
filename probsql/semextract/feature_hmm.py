"""
Feature-Based HMM for Value Span Detection.

Instead of P(word | role) emissions, uses P(feature_vector | role):
  - is_capitalized: token starts with uppercase
  - is_number: token is numeric
  - has_special: token contains -/./()/$
  - is_stop_word: common function word
  - follows_trigger: previous token is a trigger verb/prep
  - follows_value: previous token was classified as VALUE (continuity)
  - in_column_header: token word appears in a column name
  - position: early/mid/late in question

These features are dense and structural — they capture WHY a token
is a value, not just which words happen to be values.

The feature probabilities P(feature=T | role) are computed from
Opus-labeled value span data.
"""

import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"
ORACLE_DIR = Path(__file__).parent / "oracle" / "dataset"

ROLES = ["QWORD", "SELECT_HINT", "FILLER", "TRIGGER", "VALUE", "CONTEXT"]

STOP_WORDS = {
    "what", "who", "where", "when", "how", "which", "the", "a", "an",
    "is", "are", "was", "were", "did", "does", "do", "has", "have", "had",
    "of", "in", "on", "for", "with", "by", "at", "to", "and", "or",
    "that", "this", "it", "be", "been", "than", "then", "also", "not",
    "many", "much", "some", "any", "all", "no", "there", "their", "they",
    "he", "she", "its", "if", "but",
}

TRIGGER_WORDS = {
    "played", "plays", "play", "directed", "written", "coached", "managed",
    "born", "elected", "aired", "scored", "won", "lost", "beat", "from",
    "against", "located", "based", "starring", "featuring", "produced",
    "representing", "attended", "wore", "wears",
}

QWORDS = {"what", "who", "where", "when", "how", "which", "name", "list",
          "tell", "give", "find"}


def extract_features(token, prev_token, prev_role, position_ratio, header_words):
    """Extract feature vector for a single token.

    Returns dict of feature_name → bool/float
    """
    t_lower = token.lower().rstrip("?,.")
    prev_lower = prev_token.lower().rstrip("?,") if prev_token else ""

    features = {
        "is_capitalized": token[0].isupper() if token else False,
        "is_all_caps": token.isupper() and len(token) > 1,
        "is_number": bool(re.match(r'^\d', token)),
        "has_special": bool(re.search(r'[-–/().,$#=]', token)),
        "is_stop_word": t_lower in STOP_WORDS,
        "is_qword": t_lower in QWORDS,
        "is_trigger_word": t_lower in TRIGGER_WORDS,
        "follows_trigger": prev_lower in TRIGGER_WORDS or prev_lower in {"for", "of", "by", "on", "at", "from", "in"},
        "follows_value": prev_role == "VALUE",
        "follows_filler": prev_role == "FILLER",
        "in_column_header": t_lower in header_words and len(t_lower) > 2,
        "position_early": position_ratio < 0.3,
        "position_late": position_ratio > 0.7,
        "is_short": len(token) <= 2,
        "is_long": len(token) >= 6,
    }
    return features


class FeatureHMM:
    """HMM with feature-based emission probabilities.

    Emission: P(features | role) = product of P(feature_i | role) for each feature
    Transition: P(role_t | role_{t-1})
    """

    def __init__(self):
        # P(feature=True | role) for each feature and role
        self.feature_probs = {}  # {role: {feature: P(True)}}
        # P(role_t | role_{t-1})
        self.trans_probs = {}
        # P(role_0)
        self.start_probs = {}
        self.trained = False

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "feature_hmm_params.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.feature_probs = data.get("feature_probs", {})
                self.trans_probs = data.get("trans_probs", {})
                self.start_probs = data.get("start_probs", {})
                self.trained = bool(self.feature_probs)

    def parse(self, question, headers=None):
        """Parse question into token roles using feature-based Viterbi."""
        tokens = re.findall(r'\S+', question)
        if not tokens:
            return [], None

        header_words = set()
        if headers:
            for h in headers:
                header_words.update(w.lower() for w in re.findall(r'\b\w+\b', h))

        n = len(tokens)
        S = len(ROLES)

        # Viterbi
        V = [[float('-inf')] * S for _ in range(n)]
        backptr = [[0] * S for _ in range(n)]

        # Initialize
        features_0 = extract_features(tokens[0], None, None, 0.0, header_words)
        for s in range(S):
            role = ROLES[s]
            start_p = self._get_start_prob(role)
            emit_p = self._get_feature_emit_prob(features_0, role)
            V[0][s] = math.log(start_p + 1e-10) + math.log(emit_p + 1e-10)

        # Forward
        prev_roles = [ROLES[max(range(S), key=lambda s: V[0][s])]] * n  # rough estimate for feature extraction
        for t in range(1, n):
            best_prev_role = ROLES[max(range(S), key=lambda s: V[t-1][s])]
            features = extract_features(
                tokens[t], tokens[t-1], best_prev_role,
                t / n, header_words
            )
            for s in range(S):
                role = ROLES[s]
                emit_p = self._get_feature_emit_prob(features, role)
                best_score = float('-inf')
                best_prev = 0
                for prev_s in range(S):
                    trans_p = self._get_trans_prob(ROLES[prev_s], role)
                    score = V[t-1][prev_s] + math.log(trans_p + 1e-10) + math.log(emit_p + 1e-10)
                    if score > best_score:
                        best_score = score
                        best_prev = prev_s
                V[t][s] = best_score
                backptr[t][s] = best_prev

        # Backtrack
        roles = [0] * n
        roles[n-1] = max(range(S), key=lambda s: V[n-1][s])
        for t in range(n-2, -1, -1):
            roles[t] = backptr[t+1][roles[t+1]]

        role_names = [ROLES[r] for r in roles]

        # Extract value span
        value_tokens = [tokens[i] for i in range(n) if role_names[i] == "VALUE"]
        value_span = " ".join(value_tokens) if value_tokens else None

        return list(zip(tokens, role_names)), value_span

    def _get_start_prob(self, role):
        if self.trained and role in self.start_probs:
            return self.start_probs[role]
        # Default: questions start with QWORD
        defaults = {"QWORD": 0.85, "FILLER": 0.05, "SELECT_HINT": 0.05,
                    "TRIGGER": 0.02, "VALUE": 0.01, "CONTEXT": 0.02}
        return defaults.get(role, 0.02)

    def _get_trans_prob(self, from_role, to_role):
        if self.trained and from_role in self.trans_probs:
            return self.trans_probs[from_role].get(to_role, 0.02)
        defaults = {
            ("QWORD", "SELECT_HINT"): 0.5, ("QWORD", "FILLER"): 0.35,
            ("SELECT_HINT", "FILLER"): 0.55, ("SELECT_HINT", "SELECT_HINT"): 0.35,
            ("FILLER", "FILLER"): 0.30, ("FILLER", "TRIGGER"): 0.18,
            ("FILLER", "VALUE"): 0.18, ("FILLER", "CONTEXT"): 0.10,
            ("FILLER", "SELECT_HINT"): 0.10,
            ("TRIGGER", "VALUE"): 0.45, ("TRIGGER", "TRIGGER"): 0.30,
            ("TRIGGER", "FILLER"): 0.15,
            ("VALUE", "VALUE"): 0.50, ("VALUE", "FILLER"): 0.25,
            ("VALUE", "CONTEXT"): 0.15,
            ("CONTEXT", "FILLER"): 0.30, ("CONTEXT", "VALUE"): 0.20,
            ("CONTEXT", "CONTEXT"): 0.30,
        }
        return defaults.get((from_role, to_role), 0.03)

    def _get_feature_emit_prob(self, features, role):
        """P(features | role) = product of P(feature_i | role).

        Uses learned probabilities when available, rule-based otherwise.
        """
        if self.trained and role in self.feature_probs:
            prob = 1.0
            role_probs = self.feature_probs[role]
            for fname, fval in features.items():
                if fname in role_probs:
                    p_true = role_probs[fname]
                    if fval:
                        prob *= p_true
                    else:
                        prob *= (1 - p_true)
            return max(prob, 1e-10)

        # Rule-based emission using features
        return self._rule_feature_emit(features, role)

    def _rule_feature_emit(self, features, role):
        """Rule-based feature emission probability."""
        if role == "QWORD":
            if features["is_qword"]:
                return 0.9
            return 0.01
        elif role == "SELECT_HINT":
            if features["is_qword"] or features["is_stop_word"]:
                return 0.03
            if features["in_column_header"]:
                return 0.5
            if features["position_early"]:
                return 0.2
            return 0.1
        elif role == "FILLER":
            if features["is_stop_word"]:
                return 0.7
            if features["is_capitalized"]:
                return 0.05
            return 0.15
        elif role == "TRIGGER":
            if features["is_trigger_word"]:
                return 0.8
            if features["in_column_header"] and not features["position_early"]:
                return 0.3
            return 0.05
        elif role == "VALUE":
            score = 0.1
            if features["is_capitalized"] and not features["is_qword"]:
                score = 0.6
            if features["is_number"]:
                score = 0.7
            if features["has_special"]:
                score = max(score, 0.5)
            if features["follows_value"]:
                score = max(score, 0.6)  # continuity
            if features["follows_trigger"]:
                score = max(score, 0.4)
            if features["is_stop_word"] and not features["follows_value"]:
                score = 0.05
            return score
        elif role == "CONTEXT":
            return 0.15
        return 0.1


def train_from_value_spans(labels):
    """Train feature-based HMM from Opus-labeled value span data.

    Instead of token-by-token role labels (noisy), we use value span
    boundaries to construct reliable role assignments:
    - Tokens inside the value span → VALUE
    - Tokens before the value → derive role from position/features
    - Tokens after the value → FILLER/CONTEXT
    """
    # Reconstruct token roles from value spans
    all_sequences = []

    for ex in labels:
        question = ex.get("question", "")
        value_span = ex.get("value_span_in_question", "")
        start_char = ex.get("span_start_char", -1)
        end_char = ex.get("span_end_char", -1)
        start_signal = ex.get("start_signal", "")
        value_structure = ex.get("value_structure", "")

        if not value_span or start_char < 0:
            continue

        tokens = re.findall(r'\S+', question)
        # Map character positions to token indices
        token_roles = []
        char_pos = 0
        for token in tokens:
            tok_start = question.find(token, char_pos)
            tok_end = tok_start + len(token)
            char_pos = tok_end

            # Is this token inside the value span?
            if tok_start >= start_char and tok_end <= end_char + 1:
                token_roles.append("VALUE")
            elif tok_start < start_char:
                # Before value
                t_lower = token.lower().rstrip("?,.")
                if t_lower in QWORDS:
                    token_roles.append("QWORD")
                elif t_lower in TRIGGER_WORDS:
                    token_roles.append("TRIGGER")
                elif t_lower in STOP_WORDS:
                    token_roles.append("FILLER")
                else:
                    token_roles.append("SELECT_HINT" if len(token_roles) < 4 else "CONTEXT")
            else:
                # After value
                token_roles.append("FILLER")

        if len(tokens) == len(token_roles):
            all_sequences.append((tokens, token_roles))

    print(f"Constructed {len(all_sequences)} role sequences from value spans")

    if not all_sequences:
        return None

    # Compute feature-based emission probabilities
    feature_counts = {role: defaultdict(lambda: [0, 0]) for role in ROLES}  # {role: {feature: [true_count, total_count]}}
    trans_counts = defaultdict(Counter)
    start_counts = Counter()

    for tokens, roles in all_sequences:
        header_words = set()  # not available in this context
        prev_role = None
        for i, (token, role) in enumerate(zip(tokens, roles)):
            features = extract_features(
                token, tokens[i-1] if i > 0 else None,
                prev_role, i / len(tokens), header_words
            )

            for fname, fval in features.items():
                feature_counts[role][fname][1] += 1  # total
                if fval:
                    feature_counts[role][fname][0] += 1  # true

            if i == 0:
                start_counts[role] += 1
            if prev_role is not None:
                trans_counts[prev_role][role] += 1
            prev_role = role

    # Compute probabilities with smoothing
    feature_probs = {}
    for role in ROLES:
        role_probs = {}
        for fname, (true_count, total_count) in feature_counts[role].items():
            if total_count > 0:
                role_probs[fname] = (true_count + 1) / (total_count + 2)  # Laplace
        feature_probs[role] = role_probs

    # Transitions
    trans_probs = {}
    for from_role in ROLES:
        total = sum(trans_counts[from_role].values()) + len(ROLES)
        trans_probs[from_role] = {
            to_role: (trans_counts[from_role][to_role] + 1) / total
            for to_role in ROLES
        }

    # Start probs
    total_starts = sum(start_counts.values()) + len(ROLES)
    start_probs = {r: (start_counts[r] + 1) / total_starts for r in ROLES}

    return {
        "feature_probs": feature_probs,
        "trans_probs": trans_probs,
        "start_probs": start_probs,
        "stats": {
            "n_sequences": len(all_sequences),
            "n_tokens": sum(len(t) for t, _ in all_sequences),
        },
    }


def load_vspan_labels():
    """Load all value span labels."""
    labels = []
    for f in sorted(ORACLE_DIR.glob("vspan_labeled_*.json")):
        with open(f) as fh:
            data = json.load(fh)
            if isinstance(data, list):
                labels.extend(data)
    return labels


def build_and_save():
    """Build feature HMM from value span labels and save."""
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    labels = load_vspan_labels()
    print(f"Loaded {len(labels)} value span labels")

    if not labels:
        print("No labels found. Run vspan labeling agents first.")
        return

    # Analyze start/end signals
    start_signals = Counter(ex.get("start_signal", "?") for ex in labels)
    end_signals = Counter(ex.get("end_signal", "?") for ex in labels)
    value_structures = Counter(ex.get("value_structure", "?") for ex in labels)

    print(f"\nStart signals: {dict(start_signals.most_common())}")
    print(f"End signals: {dict(end_signals.most_common())}")
    print(f"Value structures: {dict(value_structures.most_common())}")

    # Train feature HMM
    params = train_from_value_spans(labels)
    if not params:
        print("Training failed.")
        return

    with open(KNOWLEDGE_DIR / "feature_hmm_params.json", "w") as f:
        json.dump(params, f, indent=2)

    stats = params["stats"]
    print(f"\nFeature HMM trained:")
    print(f"  Sequences: {stats['n_sequences']}")
    print(f"  Tokens: {stats['n_tokens']}")

    # Show learned feature probabilities
    print(f"\nP(feature=True | role) for key features:")
    key_features = ["is_capitalized", "is_number", "has_special", "is_stop_word",
                    "follows_trigger", "follows_value", "in_column_header"]
    header = f"{'Feature':<20}" + "".join(f"{r:<14}" for r in ROLES)
    print(f"  {header}")
    for feat in key_features:
        row = f"  {feat:<20}"
        for role in ROLES:
            p = params["feature_probs"].get(role, {}).get(feat, 0)
            row += f"{p:<14.2f}"
        print(row)

    return params


if __name__ == "__main__":
    build_and_save()
