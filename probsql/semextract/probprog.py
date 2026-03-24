"""
Compositional Probabilistic Program for Text-to-SQL Column Resolution.

Models the resolution as a generative program where each step is a
conditional probability table, and the reasoning follows a Markov chain:

    Step 1: Classify question type
            P(q_type | question_words)

    Step 2: Parse question tokens into roles (HMM)
            P(role_t | role_{t-1}) × P(word_t | role_t)
            Roles: QWORD, SELECT_HINT, FILLER, TRIGGER, VALUE, CONTEXT

    Step 3: Identify SELECT column from parsed roles
            P(select_col | SELECT_HINT tokens, headers)

    Step 4: Extract value from VALUE tokens
            (deterministic from HMM parse)

    Step 5: Classify value type
            P(v_type | value_format, context)

    Step 6: Resolve WHERE column via Markov reasoning chain
            State 0: Prior → P(col | v_type)
            State 1: Update → P(col | proximity to value)
            State 2: Update → P(col | trigger phrase)
            State 3: Exclude → P(col | col ≠ select_col)
            Each transition has weight from empirical base rates.

    Step 7: Determine operator
            P(op | q_type, v_type, comparison_words)

Each P() table is extracted from LLM labels. The program structure
(which steps, in what order, with what dependencies) is itself
derived from LLM reasoning chain analysis.
"""

import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


# ============================================================
# Data structures
# ============================================================

@dataclass
class TokenRole:
    """A token with its assigned role from HMM parsing."""
    token: str
    role: str       # QWORD, SELECT_HINT, FILLER, TRIGGER, VALUE, CONTEXT
    probability: float = 1.0


@dataclass
class ParsedQuestion:
    """Result of parsing a question into structured slots."""
    question: str
    q_type: str                          # lookup, comparison, count, temporal
    token_roles: list = field(default_factory=list)   # List[TokenRole]
    select_hint: str = ""                # text that hints at SELECT column
    select_column: Optional[str] = None  # resolved SELECT column
    trigger_phrase: str = ""             # verb/prep phrase indicating WHERE column
    value_span: str = ""                 # extracted WHERE value
    value_type: str = "string"           # person, institution, location, number, ...


@dataclass
class ResolvedColumn:
    """Result of column resolution with reasoning trace."""
    column: str
    confidence: float
    reasoning_chain: list = field(default_factory=list)  # [(state, score, explanation)]


# ============================================================
# Step 1: Question Type Classification
# ============================================================

# P(q_type | question_pattern)
QTYPE_PATTERNS = [
    (r'^(?:how many|how much|what is the (?:total|number|count|sum|average))', "count"),
    (r'(?:more than|less than|greater|fewer|at least|at most|over|under|above|below)', "comparison"),
    (r'(?:when|what (?:date|year|time|day))', "temporal"),
    (r'^(?:what|which|who|where|name)', "lookup"),
]


def classify_question_type(question):
    """Step 1: P(q_type | question_words)"""
    q_lower = question.lower().strip()
    for pattern, qtype in QTYPE_PATTERNS:
        if re.search(pattern, q_lower):
            return qtype
    return "lookup"  # default


# ============================================================
# Step 2: HMM Token Role Parsing
# ============================================================

# Hidden states
ROLES = ["QWORD", "SELECT_HINT", "FILLER", "TRIGGER", "VALUE", "CONTEXT"]
ROLE_IDX = {r: i for i, r in enumerate(ROLES)}

# Question words
QWORDS = {"what", "which", "who", "where", "when", "how", "name", "list",
           "give", "tell", "find"}

# Common filler words
FILLERS = {"is", "are", "was", "were", "the", "a", "an", "of", "does", "did",
           "do", "has", "have", "had", "for", "in", "on", "at", "by", "to",
           "with", "and", "or", "that", "this", "its", "it", "be", "been",
           "being", "than", "then", "also", "all", "many", "much", "some",
           "any", "no", "not", "total", "number", "amount", "if", "there",
           "their", "they", "them", "he", "she", "his", "her"}

# Known trigger verbs/prepositions
TRIGGER_STARTERS = {"played", "plays", "play", "wore", "wears", "wear",
                     "directed", "written", "coached", "managed", "born",
                     "elected", "aired", "scored", "won", "lost", "beat",
                     "defeated", "represented", "attended", "from", "against",
                     "located", "based", "starring", "featuring", "produced"}


class HMMParser:
    """Step 2: Parse question tokens into roles using HMM.

    Uses learned transition and emission probabilities.
    Falls back to rule-based assignment when HMM parameters aren't available.
    """

    def __init__(self):
        # Transition probabilities P(role_t | role_{t-1})
        # Learned from LLM token-level annotations
        self.trans_prob = None
        # Emission probabilities P(word | role)
        self.emit_prob = None
        # Whether HMM is trained
        self.trained = False

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "hmm_params.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.trans_prob = data.get("transition")
                self.emit_prob = data.get("emission")
                self.trained = True

    def parse(self, question, headers=None):
        """Parse question tokens into roles."""
        tokens = self._tokenize(question)

        if self.trained:
            roles = self._viterbi(tokens)
        else:
            roles = self._rule_based_parse(tokens, headers or [])

        return [TokenRole(t, r) for t, r in zip(tokens, roles)]

    def _tokenize(self, question):
        """Split question into tokens preserving capitalization."""
        return re.findall(r'\b[\w.#/()-]+\b', question)

    def _viterbi(self, tokens):
        """Viterbi algorithm for most likely role sequence."""
        n = len(tokens)
        if n == 0:
            return []

        S = len(ROLES)
        # Viterbi tables
        V = [[0.0] * S for _ in range(n)]
        backptr = [[0] * S for _ in range(n)]

        # Initialize
        for s in range(S):
            emit_p = self._get_emit_prob(tokens[0], ROLES[s])
            # Start probabilities: first token is usually QWORD
            start_p = 0.8 if ROLES[s] == "QWORD" else 0.04
            V[0][s] = math.log(start_p + 1e-10) + math.log(emit_p + 1e-10)

        # Forward pass
        for t in range(1, n):
            for s in range(S):
                emit_p = self._get_emit_prob(tokens[t], ROLES[s])
                best_score = float('-inf')
                best_prev = 0
                for prev_s in range(S):
                    trans_p = self._get_trans_prob(ROLES[prev_s], ROLES[s])
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

        return [ROLES[r] for r in roles]

    def _get_trans_prob(self, from_role, to_role):
        """Get transition probability with fallback."""
        if self.trans_prob and from_role in self.trans_prob:
            return self.trans_prob[from_role].get(to_role, 0.01)
        # Default transitions
        defaults = {
            ("QWORD", "SELECT_HINT"): 0.5, ("QWORD", "FILLER"): 0.3,
            ("SELECT_HINT", "FILLER"): 0.6, ("SELECT_HINT", "SELECT_HINT"): 0.3,
            ("FILLER", "FILLER"): 0.3, ("FILLER", "TRIGGER"): 0.15,
            ("FILLER", "VALUE"): 0.15, ("FILLER", "CONTEXT"): 0.1,
            ("TRIGGER", "VALUE"): 0.5, ("TRIGGER", "TRIGGER"): 0.3,
            ("TRIGGER", "FILLER"): 0.15,
            ("VALUE", "VALUE"): 0.4, ("VALUE", "FILLER"): 0.3,
            ("VALUE", "CONTEXT"): 0.2,
            ("CONTEXT", "FILLER"): 0.3, ("CONTEXT", "VALUE"): 0.2,
            ("CONTEXT", "CONTEXT"): 0.3,
        }
        return defaults.get((from_role, to_role), 0.05)

    def _get_emit_prob(self, token, role):
        """Get emission probability with fallback."""
        t_lower = token.lower()

        if self.emit_prob and role in self.emit_prob:
            return self.emit_prob[role].get(t_lower, 0.01)

        # Rule-based emission probabilities
        if role == "QWORD":
            return 0.9 if t_lower in QWORDS else 0.01
        elif role == "SELECT_HINT":
            # Likely a column-name word (not a filler, not a question word)
            if t_lower in QWORDS or t_lower in FILLERS:
                return 0.05
            return 0.3
        elif role == "FILLER":
            return 0.7 if t_lower in FILLERS else 0.1
        elif role == "TRIGGER":
            return 0.8 if t_lower in TRIGGER_STARTERS else 0.05
        elif role == "VALUE":
            # Capitalized (proper noun) or number → likely value
            if token[0].isupper() and t_lower not in QWORDS:
                return 0.7
            if re.match(r'\d', token):
                return 0.8
            return 0.1
        elif role == "CONTEXT":
            return 0.2  # generic
        return 0.1

    def _rule_based_parse(self, tokens, headers):
        """Fallback rule-based parsing when HMM isn't trained."""
        roles = []
        header_words = set()
        for h in headers:
            header_words.update(w.lower() for w in re.findall(r'\b\w+\b', h))

        in_value = False
        seen_trigger = False

        for i, token in enumerate(tokens):
            t_lower = token.lower()

            if i == 0 and t_lower in QWORDS:
                roles.append("QWORD")
            elif t_lower in TRIGGER_STARTERS:
                roles.append("TRIGGER")
                seen_trigger = True
                in_value = False
            elif t_lower in FILLERS:
                roles.append("FILLER")
                in_value = False
            elif token[0].isupper() and t_lower not in QWORDS and t_lower not in FILLERS:
                roles.append("VALUE")
                in_value = True
            elif in_value and (token[0].isupper() or re.match(r'\d', token) or t_lower in ("of", "the", "and")):
                roles.append("VALUE")  # continuation
            elif re.match(r'\d', token):
                roles.append("VALUE")
                in_value = True
            elif i <= 3 and t_lower in header_words:
                roles.append("SELECT_HINT")
            else:
                roles.append("CONTEXT" if seen_trigger else "FILLER")
                in_value = False

        return roles


# ============================================================
# Step 3: SELECT Column Identification
# ============================================================

SELECT_WORD_HINTS = {
    "who": {"player", "name", "person", "winner", "candidate", "incumbent",
            "driver", "rider", "coach", "manager", "artist", "director"},
    "where": {"location", "venue", "city", "country", "place", "ground", "stadium"},
    "when": {"date", "year", "time", "season", "day"},
}


def identify_select(parsed_tokens, headers, question):
    """Step 3: Identify SELECT column from parsed tokens.

    P(select_col | SELECT_HINT tokens, question_word, headers)
    """
    # Gather SELECT_HINT tokens
    hint_tokens = [t.token.lower() for t in parsed_tokens if t.role == "SELECT_HINT"]
    hint_text = " ".join(hint_tokens)

    # Match against headers
    candidates = []
    for h in headers:
        h_lower = h.lower()
        h_words = set(re.findall(r'\b\w{3,}\b', h_lower))

        # Full hint match
        if hint_text and (hint_text in h_lower or h_lower in hint_text):
            candidates.append((h, 0.95))
            continue

        # Word overlap
        hint_set = set(w for w in hint_tokens if len(w) > 2)
        overlap = hint_set & h_words
        if overlap:
            score = 0.8 * len(overlap) / max(len(h_words), 1)
            candidates.append((h, score))

    # Question word hints
    q_lower = question.lower()
    qword = parsed_tokens[0].token.lower() if parsed_tokens else ""
    if qword in SELECT_WORD_HINTS:
        for h in headers:
            h_words = set(re.findall(r'\b\w+\b', h.lower()))
            if h_words & SELECT_WORD_HINTS[qword]:
                candidates.append((h, 0.7))

    if candidates:
        candidates.sort(key=lambda x: -x[1])
        return candidates[0][0]
    return None


# ============================================================
# Step 4 & 5: Value Extraction and Type Classification
# ============================================================

def extract_value_from_parse(parsed_tokens):
    """Step 4: Extract value from VALUE tokens (deterministic from HMM)."""
    value_tokens = []
    for t in parsed_tokens:
        if t.role == "VALUE":
            value_tokens.append(t.token)

    if not value_tokens:
        return None
    return " ".join(value_tokens)


# P(v_type | value_format)
def classify_value_type(value):
    """Step 5: Classify value type from its format."""
    if value is None:
        return "unknown"

    # Number
    try:
        float(value.replace(",", ""))
        return "number"
    except ValueError:
        pass

    # Year
    if re.match(r'^\d{4}$', value):
        return "year_string"
    if re.match(r'^\d{4}[-–]\d{2,4}$', value):
        return "season_string"

    # Capitalization patterns
    words = value.split()
    if len(words) >= 1 and all(w[0].isupper() for w in words if w.isalpha()):
        # All capitalized → likely proper noun
        if len(words) == 1:
            return "category_or_name"  # could be "Guard" (category) or "Smith" (name)
        return "proper_noun"  # likely person name or institution

    return "string"


# ============================================================
# Step 6: Markov Chain Column Resolution
# ============================================================

class MarkovResolver:
    """Step 6: Resolve WHERE column via sequential Bayesian updates.

    Each state in the chain applies one factor and updates the
    probability distribution over columns.

    State transitions:
      Prior(v_type) → Proximity(question) → Trigger(phrase) → Exclusion(select)

    The transition weights determine how much each update matters.
    """

    def __init__(self):
        # Transition weights (from empirical base rates)
        # How much each state's update affects the final distribution
        self.state_weights = {
            "prior_vtype": 0.12,      # P(col | value_type)
            "proximity": 0.65,        # P(col | column_name near value)
            "trigger": 0.14,          # P(col | trigger phrase)
            "exclusion": 0.09,        # P(col ≠ select_col)
        }
        # Type → column keyword mappings
        self.type_keywords = {}
        # Trigger → column keyword mappings
        self.trigger_keywords = {}

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        # Load compiled tables
        path = kdir / "bayesian_compiled.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                # Update state weights from empirical data
                reasoning = data.get("P_reasoning_type", {})
                if reasoning:
                    self.state_weights["proximity"] = reasoning.get("column_name_mentioned", 0.65)
                    self.state_weights["trigger"] = reasoning.get("trigger_phrase_indicates", 0.14)
                    vtype_w = reasoning.get("value_is_entity_name", 0) + reasoning.get("value_matches_column_type", 0)
                    self.state_weights["prior_vtype"] = max(vtype_w, 0.12)
                    # Normalize
                    total = sum(self.state_weights.values())
                    self.state_weights = {k: v/total for k, v in self.state_weights.items()}

        # Load trigger rules
        sem_path = kdir / "semantic_rules.json"
        if sem_path.exists():
            with open(sem_path) as f:
                data = json.load(f)
                for rule in data.get("trigger_rules", []):
                    trigger = rule.get("trigger", "")
                    if len(trigger) > 4:
                        col_pattern = rule.get("column_pattern", "")
                        self.trigger_keywords[trigger] = set(
                            p.strip() for p in col_pattern.split("|") if p.strip()
                        )

    def resolve(self, value, value_type, headers, question,
                select_col=None, trigger_phrase=None):
        """Resolve WHERE column via Markov chain of Bayesian updates.

        Returns:
            ResolvedColumn with reasoning chain trace
        """
        q_lower = question.lower()
        val_lower = (value or "").lower()
        n_cols = len(headers)
        exclude = select_col.lower() if select_col else None

        # Initialize uniform prior
        probs = {h: 1.0 / n_cols for h in headers}
        chain = []

        # State 1: Prior from value type
        probs, explanation = self._update_vtype(probs, value_type, headers)
        chain.append(("prior_vtype", dict(probs), explanation))

        # State 2: Proximity update
        probs, explanation = self._update_proximity(probs, val_lower, q_lower, headers)
        chain.append(("proximity", dict(probs), explanation))

        # State 3: Trigger update
        probs, explanation = self._update_trigger(probs, q_lower, trigger_phrase, headers)
        chain.append(("trigger", dict(probs), explanation))

        # State 4: SELECT exclusion
        probs, explanation = self._update_exclusion(probs, exclude, headers)
        chain.append(("exclusion", dict(probs), explanation))

        # Pick best
        best_col = max(probs, key=probs.get)
        best_score = probs[best_col]

        return ResolvedColumn(
            column=best_col,
            confidence=best_score,
            reasoning_chain=chain,
        )

    def _update_vtype(self, probs, value_type, headers):
        """Bayesian update from value type."""
        w = self.state_weights["prior_vtype"]
        type_kw = {
            "proper_noun": {"player", "name", "winner", "candidate", "person",
                           "incumbent", "director", "artist", "driver", "rider"},
            "category_or_name": {"position", "status", "type", "result", "class",
                                "player", "name", "winner"},
            "number": {"no", "number", "#", "rank", "score", "points", "goals",
                       "attendance", "total", "pick", "round", "week", "episode"},
            "year_string": {"year", "season", "founded", "elected", "date"},
            "season_string": {"year", "season", "years"},
            "string": set(),  # no strong prior
        }

        keywords = type_kw.get(value_type, set())
        explanation = f"v_type={value_type}"

        if keywords:
            updated = {}
            for h in headers:
                h_words = set(re.findall(r'\b\w+\b', h.lower()))
                if h_words & keywords:
                    updated[h] = probs[h] * (1 + w * 3)  # boost matching columns
                else:
                    updated[h] = probs[h] * (1 - w * 0.3)  # slight penalty
            # Normalize
            total = sum(updated.values()) or 1
            probs = {h: v/total for h, v in updated.items()}
            matched = [h for h in headers if set(re.findall(r'\b\w+\b', h.lower())) & keywords]
            explanation += f" → boosted {matched}"

        return probs, explanation

    def _update_proximity(self, probs, val_lower, q_lower, headers):
        """Bayesian update from column name proximity to value."""
        w = self.state_weights["proximity"]

        val_pos = q_lower.find(val_lower) if val_lower else -1
        explanation = f"val_pos={val_pos}"

        updated = dict(probs)
        best_match = None
        best_dist = float('inf')

        for h in headers:
            h_lower = h.lower()
            # Check if column name or significant words appear in question
            sig_words = [word for word in re.findall(r'\b\w{3,}\b', h_lower)
                        if word not in {"the", "and", "for", "from", "with", "of"}]

            if not sig_words:
                continue

            # Full column name match
            if h_lower in q_lower:
                col_pos = q_lower.find(h_lower)
                if val_pos >= 0:
                    dist = abs(col_pos - val_pos)
                    if dist < best_dist:
                        best_dist = dist
                        best_match = h
                    if dist < 50:
                        updated[h] = probs[h] * (1 + w * 5)
                    else:
                        updated[h] = probs[h] * (1 + w * 2)
                else:
                    updated[h] = probs[h] * (1 + w * 2)
                continue

            # Significant word match with proximity
            for word in sig_words:
                w_pos = q_lower.find(word)
                if w_pos >= 0:
                    if val_pos >= 0:
                        dist = abs(w_pos - val_pos)
                        if dist < 30:
                            updated[h] = probs[h] * (1 + w * 3)
                        elif dist < 60:
                            updated[h] = probs[h] * (1 + w * 1.5)
                    else:
                        updated[h] = probs[h] * (1 + w * 1)
                    break

        # Normalize
        total = sum(updated.values()) or 1
        probs = {h: v/total for h, v in updated.items()}
        if best_match:
            explanation += f" → nearest col: {best_match} (dist={best_dist})"

        return probs, explanation

    def _update_trigger(self, probs, q_lower, trigger_phrase, headers):
        """Bayesian update from trigger phrase."""
        w = self.state_weights["trigger"]
        explanation = "no trigger"

        # Check hardcoded triggers
        triggers_found = []
        trigger_col_kw = {
            r'\bplayed?\s+for\b': {"school", "team", "club", "university", "college"},
            r'\bplays?\s+for\b': {"school", "team", "club"},
            r'\bplayed?\s+at\b': {"school", "venue", "stadium", "ground"},
            r'\bwears?\s+(?:number|no|#)': {"no", "number", "#"},
            r'\bdirected\s+by\b': {"director", "directed"},
            r'\bwritten\s+by\b': {"writer", "written", "author"},
            r'\bborn\s+in\b': {"birthplace", "country", "city", "birth"},
            r'\belected\s+in\b': {"year", "elected", "first"},
            r'\baired\b': {"date", "air"},
            r'\bfrom\s+(?=[A-Z])': {"country", "city", "location", "school", "team", "state"},
            r'\bagainst\s+': {"opponent", "team", "away"},
            r'\bwon\s+(?:in|at)\b': {"year", "tournament", "event"},
        }

        for pattern, keywords in trigger_col_kw.items():
            if re.search(pattern, q_lower):
                triggers_found.append((pattern, keywords))

        # Also check learned triggers
        for trigger, keywords in self.trigger_keywords.items():
            if trigger in q_lower and keywords:
                triggers_found.append((trigger, keywords))

        if triggers_found:
            updated = dict(probs)
            for pattern, keywords in triggers_found:
                for h in headers:
                    h_words = set(re.findall(r'\b\w+\b', h.lower()))
                    if h_words & keywords:
                        updated[h] = probs[h] * (1 + w * 4)

            total = sum(updated.values()) or 1
            probs = {h: v/total for h, v in updated.items()}
            explanation = f"triggers: {[t[0][:20] for t in triggers_found]}"

        return probs, explanation

    def _update_exclusion(self, probs, exclude_col, headers):
        """Bayesian update: exclude SELECT column from WHERE candidates."""
        explanation = f"exclude={exclude_col}"

        if exclude_col:
            updated = {}
            for h in headers:
                if h.lower() == exclude_col:
                    updated[h] = probs[h] * 0.05  # heavy penalty, not zero
                else:
                    updated[h] = probs[h]
            total = sum(updated.values()) or 1
            probs = {h: v/total for h, v in updated.items()}

        return probs, explanation


# ============================================================
# Step 7: Operator Selection
# ============================================================

# P(operator | q_type, v_type, comparison_words)
def determine_operator(question, q_type, v_type):
    """Step 7: Determine SQL operator."""
    q_lower = question.lower()

    # Empirical P(op | v_type) from 76K WikiSQL examples
    if v_type in ("proper_noun", "category_or_name", "string"):
        return "=", 0.95

    if q_type == "comparison" or v_type == "number":
        if any(w in q_lower for w in ["more than", "greater than", "over", "above", "at least", "larger", "higher"]):
            return ">", 0.9
        if any(w in q_lower for w in ["less than", "under", "below", "fewer", "at most", "smaller", "lower"]):
            return "<", 0.9
        return "=", 0.5  # numbers default to = but uncertain

    if v_type in ("year_string", "season_string"):
        return "=", 0.8  # years are usually string equality

    return "=", 0.7


# ============================================================
# Main Compositional Program
# ============================================================

class ProbabilisticResolver:
    """The compositional probabilistic program.

    Chains all steps together with proper dependency structure.
    Each step's output feeds into the next step's input.
    """

    def __init__(self):
        self.hmm_parser = HMMParser()
        self.markov_resolver = MarkovResolver()

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        self.hmm_parser.load_knowledge(kdir)
        self.markov_resolver.load_knowledge(kdir)

    def resolve(self, question, headers, col_types=None):
        """Run the full compositional program.

        Args:
            question: English question text
            headers: List of column name strings
            col_types: Optional list of column SQL types

        Returns:
            dict with: where_column, where_value, operator, confidence,
                       select_column, parsed_question, reasoning_chain
        """
        # Step 1: Classify question type
        q_type = classify_question_type(question)

        # Step 2: Parse tokens into roles (HMM)
        token_roles = self.hmm_parser.parse(question, headers)

        # Step 3: Identify SELECT column
        select_col = identify_select(token_roles, headers, question)

        # Step 4: Extract value from VALUE tokens
        value = extract_value_from_parse(token_roles)

        # Step 5: Classify value type
        v_type = classify_value_type(value) if value else "unknown"

        # Step 6: Extract trigger phrase from TRIGGER tokens
        trigger_tokens = [t.token.lower() for t in token_roles if t.role == "TRIGGER"]
        trigger_phrase = " ".join(trigger_tokens) if trigger_tokens else None

        # Step 7: Resolve WHERE column via Markov chain
        resolved = self.markov_resolver.resolve(
            value=value,
            value_type=v_type,
            headers=headers,
            question=question,
            select_col=select_col,
            trigger_phrase=trigger_phrase,
        )

        # Step 8: Determine operator
        operator, op_confidence = determine_operator(question, q_type, v_type)

        # Compute overall confidence
        confidence = resolved.confidence * op_confidence
        if value is None:
            confidence *= 0.3  # heavy penalty for no value

        return {
            "where_column": resolved.column,
            "where_value": value,
            "operator": operator,
            "confidence": confidence,
            "select_column": select_col,
            "value_type": v_type,
            "q_type": q_type,
            "reasoning_chain": resolved.reasoning_chain,
            "token_roles": [(t.token, t.role) for t in token_roles],
        }


# ============================================================
# Testing
# ============================================================

if __name__ == "__main__":
    resolver = ProbabilisticResolver()

    # Try to load knowledge
    if KNOWLEDGE_DIR.exists():
        resolver.load_knowledge()

    headers = ["Player", "No.", "Nationality", "Position", "Years in Toronto", "School/Club Team"]

    tests = [
        "What position does the player who played for Butler CC (KS) play?",
        "Who is the player that wears number 42?",
        "What school/club team is Amir Johnson on?",
        "How many players are from the United States?",
        "What player played guard for toronto in 1996-97?",
    ]

    for q in tests:
        result = resolver.resolve(q, headers)
        print(f"\nQ: {q}")
        print(f"  Tokens: {result['token_roles']}")
        print(f"  SELECT: {result['select_column']}")
        print(f"  VALUE:  {result['where_value']} (type={result['value_type']})")
        print(f"  WHERE:  {result['where_column']} (conf={result['confidence']:.2f})")
        print(f"  OP:     {result['operator']}")
        print(f"  Chain:  {[(s, {k: f'{v:.2f}' for k,v in p.items() if v > 0.1}) for s, p, _ in result['reasoning_chain']]}")
