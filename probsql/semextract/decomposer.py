"""
QuestionDecomposer — Parses question structure to identify SELECT vs WHERE parts.

Given a question like "What position does the player who played for Butler CC play?",
outputs:
  - select_hint: "position" (what column the answer comes from)
  - filter_phrase: "played for Butler CC" (the part containing the WHERE condition)
  - question_type: "what_attribute" (structural pattern)

Built from patterns extracted from WikiSQL ground truth.
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


# Question structure templates extracted from WikiSQL analysis
# Format: (regex_pattern, question_type, select_group, filter_group)
QUESTION_TEMPLATES = [
    # "What X is/are/was Y?" → SELECT=X, filter contains Y
    (r'^what\s+(?:is|are|was|were)\s+(?:the\s+)?(.+?)(?:\s+(?:of|for|in|on|when|where|with|by)\s+(.+))?\s*\??$',
     "what_is", 1, 2),

    # "What X does/did Y verb?" → SELECT=X, Y is the entity
    (r'^what\s+(.+?)\s+(?:does|did|do)\s+(.+?)(?:\s+(?:play|have|get|hold|wear|score|earn|win|make|serve|use))?\s*\??$',
     "what_does", 1, 2),

    # "Who is/are/was the X that/who Y?" → SELECT=person, filter from Y
    (r'^who\s+(?:is|are|was|were)\s+(?:the\s+)?(.+?)(?:\s+(?:that|who|which)\s+(.+))?\s*\??$',
     "who_is", 1, 2),

    # "Who verb Y?" → SELECT=person/name, Y contains filter
    (r'^who\s+(.+)\s*\??$',
     "who_verb", None, 1),

    # "How many X verb/are Y?" → SELECT=count, filter from Y
    (r'^how\s+many\s+(.+?)\s+(?:are|were|is|was|did|does|do|have|has)\s+(.+)\s*\??$',
     "how_many", 1, 2),

    # "Where is/was X?" → SELECT=location, X is the entity
    (r'^where\s+(?:is|are|was|were|did)\s+(.+)\s*\??$',
     "where_is", None, 1),

    # "When did X verb?" → SELECT=date/time, X+verb contain filter
    (r'^when\s+(?:did|does|was|were|is)\s+(.+)\s*\??$',
     "when_did", None, 1),

    # "Which X verb Y?" → SELECT=X, Y contains filter
    (r'^which\s+(.+?)\s+(?:is|are|was|were|has|have|had|did|does|do)\s+(.+)\s*\??$',
     "which", 1, 2),

    # Generic "What X Y?" fallback
    (r'^what\s+(.+)\s*\??$',
     "what_generic", 1, None),
]


class QuestionDecomposer:
    def __init__(self):
        self.templates = QUESTION_TEMPLATES
        self.select_word_map = {}  # word → likely column name patterns
        self.verb_column_map = {}  # verb phrase → likely WHERE column patterns

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "decomposer_patterns.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.select_word_map = data.get("select_word_map", {})
                self.verb_column_map = data.get("verb_column_map", {})

    def decompose(self, question, headers=None):
        """Decompose a question into structured slots.

        Returns:
            dict with keys: question_type, select_hint, filter_phrase,
                           select_column_candidates, where_column_candidates
        """
        q = question.strip().rstrip("?").strip()
        q_lower = q.lower()

        result = {
            "question_type": "unknown",
            "select_hint": None,
            "filter_phrase": None,
            "raw_question": question,
        }

        # Try each template
        for pattern, qtype, sel_group, filter_group in self.templates:
            m = re.match(pattern, q_lower, re.IGNORECASE)
            if m:
                result["question_type"] = qtype
                if sel_group and m.group(sel_group):
                    result["select_hint"] = m.group(sel_group).strip()
                if filter_group:
                    try:
                        if m.group(filter_group):
                            result["filter_phrase"] = m.group(filter_group).strip()
                    except IndexError:
                        pass
                break

        # If no filter phrase found, the whole question minus the select hint is the filter
        if not result["filter_phrase"]:
            result["filter_phrase"] = q_lower

        # Resolve select hint to column candidates if headers provided
        if headers and result["select_hint"]:
            result["select_column_candidates"] = self._match_select_to_headers(
                result["select_hint"], result["question_type"], headers
            )

        # Identify WHERE column hints from verb phrases in filter
        if result["filter_phrase"]:
            result["where_column_hints"] = self._extract_where_hints(result["filter_phrase"])

        return result

    def _match_select_to_headers(self, select_hint, question_type, headers):
        """Match the select hint to actual column headers."""
        candidates = []
        hint_lower = select_hint.lower()
        hint_words = set(re.findall(r'\b\w+\b', hint_lower))

        for i, header in enumerate(headers):
            h_lower = header.lower()
            h_words = set(re.findall(r'\b\w+\b', h_lower))

            # Exact match
            if hint_lower == h_lower:
                candidates.append((header, 1.0, i))
                continue

            # Substring match
            if hint_lower in h_lower or h_lower in hint_lower:
                candidates.append((header, 0.9, i))
                continue

            # Word overlap
            overlap = hint_words & h_words - {"the", "a", "an", "of", "in", "for"}
            if overlap:
                score = len(overlap) / max(len(h_words), 1)
                candidates.append((header, score * 0.8, i))

        # Question type hints
        if question_type == "who_verb" or question_type == "who_is":
            for i, header in enumerate(headers):
                h_lower = header.lower()
                if any(w in h_lower for w in ["player", "name", "person", "winner", "candidate"]):
                    candidates.append((header, 0.7, i))
        elif question_type == "where_is":
            for i, header in enumerate(headers):
                h_lower = header.lower()
                if any(w in h_lower for w in ["location", "venue", "city", "country", "place"]):
                    candidates.append((header, 0.7, i))
        elif question_type == "when_did":
            for i, header in enumerate(headers):
                h_lower = header.lower()
                if any(w in h_lower for w in ["date", "year", "time", "when"]):
                    candidates.append((header, 0.7, i))

        # Also use learned select_word_map
        for word in hint_words:
            if word in self.select_word_map:
                for col_pattern in self.select_word_map[word]:
                    for i, header in enumerate(headers):
                        if col_pattern.lower() in header.lower():
                            candidates.append((header, 0.6, i))

        # Deduplicate and sort
        seen = set()
        unique = []
        for header, score, idx in sorted(candidates, key=lambda x: -x[1]):
            if idx not in seen:
                seen.add(idx)
                unique.append((header, score, idx))

        return unique[:3]

    def _extract_where_hints(self, filter_phrase):
        """Extract hints about what WHERE column might be from verb phrases."""
        hints = []

        # Known verb→column patterns
        verb_patterns = {
            r'\bplayed?\s+for\b': ["school", "team", "club"],
            r'\bwears?\s+(?:number|no\.?|#)\b': ["no.", "number", "#"],
            r'\bfrom\b': ["country", "city", "location", "school", "state"],
            r'\bborn\s+(?:in|on)\b': ["birth", "date", "born"],
            r'\bdirected\s+by\b': ["director", "directed"],
            r'\bwritten\s+by\b': ["writer", "written", "author"],
            r'\baired\s+(?:on|in)\b': ["air date", "date", "aired"],
            r'\bscored?\b': ["score", "points", "goals"],
            r'\bwon\b': ["winner", "result"],
            r'\belected\b': ["elected", "year", "election"],
            r'\brepresent': ["country", "team", "nation"],
        }

        for pattern, col_hints in verb_patterns.items():
            if re.search(pattern, filter_phrase, re.IGNORECASE):
                hints.extend(col_hints)

        # Also use learned verb_column_map
        for verb, cols in self.verb_column_map.items():
            if verb in filter_phrase:
                hints.extend(cols)

        return hints


def build_decomposer_knowledge(oracle_path=None, output_dir=None):
    """Build decomposer knowledge from oracle data."""
    output_dir = Path(output_dir) if output_dir else KNOWLEDGE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load oracle data
    if oracle_path is None:
        oracle_path = Path(__file__).parent / "oracle" / "dataset" / "decomposer_dev.json"
    with open(oracle_path) as f:
        data = json.load(f)

    # Build select_word_map: question words → column name patterns
    select_word_map = defaultdict(Counter)
    for ex in data:
        hint = ex.get("select_hint", "")
        col = ex.get("select_column", "")
        if hint and col:
            for word in re.findall(r'\b\w+\b', hint.lower()):
                if len(word) > 2 and word not in ("the", "and", "for", "are", "was", "did"):
                    select_word_map[word][col] += 1

    # Keep top 3 columns per word
    select_map = {}
    for word, col_counts in select_word_map.items():
        top = [col for col, _ in col_counts.most_common(3)]
        if col_counts.most_common(1)[0][1] >= 3:  # at least 3 occurrences
            select_map[word] = top

    # Build verb_column_map from filter phrases
    verb_column_map = defaultdict(Counter)
    verbs_to_check = ["played for", "plays for", "wears", "from", "in",
                      "born", "directed by", "written by", "scored",
                      "won", "elected", "represents", "located"]
    for ex in data:
        q_lower = ex.get("question", "").lower()
        where_col = ex.get("where_column", "")
        for verb in verbs_to_check:
            if verb in q_lower and where_col:
                verb_column_map[verb][where_col] += 1

    verb_map = {}
    for verb, col_counts in verb_column_map.items():
        top = [col for col, _ in col_counts.most_common(3)]
        if col_counts.most_common(1)[0][1] >= 2:
            verb_map[verb] = top

    knowledge = {
        "select_word_map": select_map,
        "verb_column_map": verb_map,
    }

    with open(output_dir / "decomposer_patterns.json", "w") as f:
        json.dump(knowledge, f, indent=2)

    print(f"Decomposer knowledge: {len(select_map)} select word mappings, {len(verb_map)} verb mappings")
    return knowledge


if __name__ == "__main__":
    build_decomposer_knowledge()
