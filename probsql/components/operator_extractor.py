"""
Operator Extractor — Maps English expressions to SQL operators.

Extracts rules from oracle data and provides a callable interface
for determining the SQL operator from an English phrase + column info.
"""

import json
import re
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent.parent / "knowledge" / "base"

# Priority-ordered operator rules (checked top to bottom)
DEFAULT_OPERATOR_RULES = [
    {"pattern": r"\bbetween\b.+\band\b", "operator": "BETWEEN", "priority": 10},
    {"pattern": r"\bfrom\b.+\bto\b", "operator": "BETWEEN", "priority": 10},
    {"pattern": r"\bstarts?\s+with\b|\bbeginning\s+with\b", "operator": "LIKE", "value_transform": "prefix_wildcard", "priority": 9},
    {"pattern": r"\bends?\s+with\b|\bending\s+with\b", "operator": "LIKE", "value_transform": "suffix_wildcard", "priority": 9},
    {"pattern": r"\bcontains?\b|\bincludes?\b|\bmatching\b|\blike\b", "operator": "LIKE", "value_transform": "contains_wildcard", "priority": 8},
    {"pattern": r"\bin\s*\[|\bone\s+of\b|\bany\s+of\b|\beither\b", "operator": "IN", "priority": 8},
    {"pattern": r"\bmissing\b|\bempty\b|\bblank\b|\bnull\b|\bno\s+\w+\s+set\b|\bnot\s+set\b|\bnot\s+provided\b|\bwithout\b", "operator": "IS NULL", "priority": 7},
    {"pattern": r"\bhas\s+a\b|\bhas\s+an\b|\bexists?\b|\bpresent\b|\bprovided\b|\bnot\s+null\b|\bnot\s+empty\b|\bhas\s+\w+\b", "operator": "IS NOT NULL", "priority": 6},
    {"pattern": r"\bexactly\b|\bequals?\b|\bis\s+exactly\b|\bequal\s+to\b", "operator": "=", "priority": 5},
    {"pattern": r"\bmore\s+than\b|\bgreater\s+than\b|\babove\b|\bover\b|\bexceeds?\b|\bhigher\s+than\b", "operator": ">", "priority": 4},
    {"pattern": r"\bat\s+least\b|\bminimum\b|\bno\s+less\s+than\b|\bor\s+more\b", "operator": ">=", "priority": 4},
    {"pattern": r"\bless\s+than\b|\bbelow\b|\bunder\b|\bfewer\s+than\b|\blower\s+than\b", "operator": "<", "priority": 4},
    {"pattern": r"\bat\s+most\b|\bmaximum\b|\bno\s+more\s+than\b|\bor\s+less\b|\bor\s+fewer\b", "operator": "<=", "priority": 4},
    {"pattern": r"\bnot\b|\bisn'?t\b|\baren'?t\b|\bhasn'?t\b|\bhaven'?t\b|\bdoesn'?t\b|\bdon'?t\b|\bwithout\b|\bexcluding\b|\bexcept\b|\bother\s+than\b|\bnon-", "operator": "!=", "priority": 3},
]

# Default operator by column type
TYPE_DEFAULTS = {
    "BOOLEAN": {"default_operator": "=", "true_words": ["is", "are", "has", "have", "active", "verified", "enabled", "available", "premium", "featured"],
                "false_words": ["not", "isn't", "hasn't", "no", "inactive", "disabled", "unavailable"]},
    "ENUM": {"default_operator": "=", "fallback": "IN"},
    "TIMESTAMP": {"default_operator": ">=", "fallback": "BETWEEN"},
    "DATE": {"default_operator": ">=", "fallback": "BETWEEN"},
    "VARCHAR": {"default_operator": "=", "fallback": "LIKE"},
    "TEXT": {"default_operator": "LIKE", "fallback": "="},
    "INT": {"default_operator": "=", "comparison_operators": [">=", "<=", ">", "<"]},
    "BIGINT": {"default_operator": "=", "comparison_operators": [">=", "<=", ">", "<"]},
    "FLOAT": {"default_operator": ">=", "comparison_operators": [">=", "<=", ">", "<"]},
    "DECIMAL": {"default_operator": ">=", "comparison_operators": [">=", "<=", ">", "<"]},
}


class OperatorExtractor:
    def __init__(self):
        self.rules = DEFAULT_OPERATOR_RULES
        self.type_defaults = TYPE_DEFAULTS

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "operator_rules.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                if "operator_rules" in data:
                    self.rules = data["operator_rules"]
                if "operator_by_column_type" in data:
                    self.type_defaults = data["operator_by_column_type"]

    def extract(self, english_phrase, column_info):
        """Determine the SQL operator from English phrase + column info.

        Args:
            english_phrase: The English text describing the condition
            column_info: ColumnCandidate or dict with column_type, enum_values, column_name

        Returns:
            tuple: (operator, confidence, value_transform)
        """
        phrase_lower = english_phrase.lower()

        if isinstance(column_info, dict):
            col_type = column_info.get("column_type", column_info.get("type", "VARCHAR"))
            col_name = column_info.get("column_name", column_info.get("name", ""))
            enum_values = column_info.get("enum_values", [])
        else:
            col_type = getattr(column_info, "column_type", "VARCHAR")
            col_name = getattr(column_info, "column_name", "")
            enum_values = getattr(column_info, "enum_values", [])

        base_type = col_type.upper().split("(")[0]

        # Phase 1: Check explicit operator patterns (highest priority)
        matched_rules = []
        for rule in self.rules:
            if re.search(rule["pattern"], phrase_lower):
                matched_rules.append(rule)

        if matched_rules:
            # Take highest priority match
            best = max(matched_rules, key=lambda r: r.get("priority", 0))
            op = best["operator"]
            transform = best.get("value_transform")

            # Handle negation modifier
            if op == "!=" and len(matched_rules) > 1:
                # Negation modifies another operator
                others = [r for r in matched_rules if r["operator"] != "!="]
                if others:
                    base_op = max(others, key=lambda r: r.get("priority", 0))["operator"]
                    op = self._negate_operator(base_op)

            # Special handling for IS NULL with negation
            if op == "IS NULL" and self._has_negation(phrase_lower):
                op = "IS NOT NULL"
            elif op == "IS NOT NULL" and self._has_negation(phrase_lower):
                op = "IS NULL"

            return op, 0.85, transform

        # Phase 2: Type-based defaults
        type_info = self.type_defaults.get(base_type, {})

        # Boolean column
        if base_type == "BOOLEAN":
            if any(w in phrase_lower for w in type_info.get("false_words", [])):
                return "=", 0.8, "boolean_false"
            return "=", 0.8, "boolean_true"

        # Enum column
        if enum_values:
            matched_enums = [ev for ev in enum_values if ev.lower() in phrase_lower]
            if len(matched_enums) > 1:
                return "IN", 0.75, None
            elif matched_enums:
                return "=", 0.8, None

        # Numeric types with comparison context
        if base_type in ("INT", "BIGINT", "FLOAT", "DECIMAL"):
            if re.search(r'\d', phrase_lower):
                return type_info.get("default_operator", "="), 0.6, None

        # Timestamp/Date columns
        if base_type in ("TIMESTAMP", "DATE"):
            return ">=", 0.6, None

        # Default
        default_op = type_info.get("default_operator", "=")
        return default_op, 0.5, None

    def _has_negation(self, phrase):
        """Check if phrase contains negation."""
        neg_patterns = [r"\bnot\b", r"\bno\b", r"\bnon-", r"\bn't\b",
                       r"\bnever\b", r"\bwithout\b", r"\bexclud"]
        return any(re.search(p, phrase) for p in neg_patterns)

    def _negate_operator(self, op):
        """Return the negated form of an operator."""
        negation_map = {
            "=": "!=",
            ">": "<=",
            "<": ">=",
            ">=": "<",
            "<=": ">",
            "LIKE": "NOT LIKE",
            "IN": "NOT IN",
            "IS NULL": "IS NOT NULL",
            "IS NOT NULL": "IS NULL",
            "BETWEEN": "NOT BETWEEN",
        }
        return negation_map.get(op, f"NOT {op}")


def build_knowledge_from_oracle(oracle_path, output_dir=None):
    """Extract operator rules from oracle dataset."""
    output_dir = Path(output_dir) if output_dir else KNOWLEDGE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(oracle_path) as f:
        data = json.load(f)
    examples = data if isinstance(data, list) else data.get("examples", [])

    from collections import Counter
    operator_counts = Counter()
    for ex in examples:
        ops = ex.get("latent_variables", {}).get("operators_used", [])
        operator_counts.update(ops)

    rules_data = {
        "operator_rules": DEFAULT_OPERATOR_RULES,
        "operator_by_column_type": TYPE_DEFAULTS,
        "operator_frequency": dict(operator_counts),
    }

    path = output_dir / "operator_rules.json"
    with open(path, "w") as f:
        json.dump(rules_data, f, indent=2)
    print(f"Operator rules saved to {path}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        build_knowledge_from_oracle(sys.argv[1])
    else:
        print("Usage: python operator_extractor.py <oracle_dataset_path>")
