"""
Predicate Tree — Core data structure for the predicate AST.

Supports atomic predicates, compound (AND/OR), and negated predicates.
Provides SQL rendering, serialization, and confidence computation.
"""

from dataclasses import dataclass, field
from typing import Union, Optional


@dataclass
class AtomicPredicate:
    """A single SQL condition."""
    english_phrase: str
    table: str
    column: str
    operator: str
    value: object  # str, int, float, list (IN), tuple (BETWEEN), None (IS NULL)
    confidence: float = 0.0
    column_match_score: float = 0.0
    alternatives: list = field(default_factory=list)


@dataclass
class CompoundPredicate:
    """A tree of predicates joined by conjunction."""
    conjunction: str  # "AND" or "OR"
    left: 'PredicateNode'
    right: 'PredicateNode'


@dataclass
class NegatedPredicate:
    """A negated predicate."""
    inner: 'PredicateNode'
    negation_type: str  # "operator", "existence", "null_check"


PredicateNode = Union[AtomicPredicate, CompoundPredicate, NegatedPredicate]


def to_sql(node):
    """Render the predicate tree as a SQL WHERE clause string."""
    if isinstance(node, AtomicPredicate):
        return _atomic_to_sql(node)
    elif isinstance(node, CompoundPredicate):
        left_sql = to_sql(node.left)
        right_sql = to_sql(node.right)
        # Add parens for compound children of OR when parent is AND
        if isinstance(node.left, CompoundPredicate) and node.left.conjunction != node.conjunction:
            left_sql = f"({left_sql})"
        if isinstance(node.right, CompoundPredicate) and node.right.conjunction != node.conjunction:
            right_sql = f"({right_sql})"
        return f"{left_sql} {node.conjunction} {right_sql}"
    elif isinstance(node, NegatedPredicate):
        inner_sql = to_sql(node.inner)
        return f"NOT ({inner_sql})"
    return ""


def _quote_col(name):
    """Quote a column name if it contains spaces or special characters."""
    if " " in name or "/" in name or "(" in name or "#" in name or "." in name:
        return f'"{name}"'
    return name


def _atomic_to_sql(node):
    """Render a single atomic predicate as SQL."""
    col_name = _quote_col(node.column)
    col = f"{node.table}.{col_name}" if node.table else col_name
    op = node.operator
    val = node.value

    if op in ("IS NULL", "IS NOT NULL"):
        return f"{col} {op}"

    if op == "BETWEEN" and isinstance(val, (list, tuple)) and len(val) == 2:
        v1 = _format_value(val[0])
        v2 = _format_value(val[1])
        return f"{col} BETWEEN {v1} AND {v2}"

    if op == "NOT BETWEEN" and isinstance(val, (list, tuple)) and len(val) == 2:
        v1 = _format_value(val[0])
        v2 = _format_value(val[1])
        return f"{col} NOT BETWEEN {v1} AND {v2}"

    if op in ("IN", "NOT IN") and isinstance(val, (list, tuple)):
        vals = ", ".join(_format_value(v) for v in val)
        return f"{col} {op} ({vals})"

    if op in ("LIKE", "NOT LIKE"):
        formatted = _format_value(val)
        return f"{col} {op} {formatted}"

    formatted = _format_value(val)
    return f"{col} {op} {formatted}"


def _format_value(val):
    """Format a value for SQL."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, str):
        # Check if it's already a SQL expression (date functions, etc.)
        if val.startswith("date(") or val.startswith("datetime(") or val.startswith("DATE("):
            return val
        # Escape single quotes
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    return str(val)


def to_dict(node):
    """Serialize the tree to a dict for debugging/logging."""
    if isinstance(node, AtomicPredicate):
        return {
            "type": "atomic",
            "english": node.english_phrase,
            "table": node.table,
            "column": node.column,
            "operator": node.operator,
            "value": _serialize_value(node.value),
            "confidence": node.confidence,
            "column_match_score": node.column_match_score,
        }
    elif isinstance(node, CompoundPredicate):
        return {
            "type": "compound",
            "conjunction": node.conjunction,
            "left": to_dict(node.left),
            "right": to_dict(node.right),
        }
    elif isinstance(node, NegatedPredicate):
        return {
            "type": "negated",
            "negation_type": node.negation_type,
            "inner": to_dict(node.inner),
        }
    return {}


def _serialize_value(val):
    if isinstance(val, (list, tuple)):
        return list(val)
    return val


def compute_confidence(node):
    """Compute aggregate confidence (product of leaf confidences)."""
    if isinstance(node, AtomicPredicate):
        return node.confidence
    elif isinstance(node, CompoundPredicate):
        left_conf = compute_confidence(node.left)
        right_conf = compute_confidence(node.right)
        return left_conf * right_conf
    elif isinstance(node, NegatedPredicate):
        return compute_confidence(node.inner) * 0.9  # slight penalty for negation
    return 0.0


def count_leaves(node):
    """Count the number of atomic predicates in the tree."""
    if isinstance(node, AtomicPredicate):
        return 1
    elif isinstance(node, CompoundPredicate):
        return count_leaves(node.left) + count_leaves(node.right)
    elif isinstance(node, NegatedPredicate):
        return count_leaves(node.inner)
    return 0


def get_tables(node):
    """Get all table names referenced in the tree."""
    if isinstance(node, AtomicPredicate):
        return {node.table} if node.table else set()
    elif isinstance(node, CompoundPredicate):
        return get_tables(node.left) | get_tables(node.right)
    elif isinstance(node, NegatedPredicate):
        return get_tables(node.inner)
    return set()
