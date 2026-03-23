"""
SQL Formatter — Produces clean, formatted SQL from predicate trees.

Handles parenthesization, quoting, NULL, LIKE wildcards, BETWEEN ordering, IN lists.
"""

from probsql.engine.predicate_tree import (
    PredicateNode, AtomicPredicate, CompoundPredicate, NegatedPredicate, to_sql
)


def format_sql(tree):
    """Render predicate tree as a clean SQL WHERE clause string.

    This is the main formatting entry point. It produces properly
    parenthesized, consistently quoted SQL.
    """
    if tree is None:
        return "1=1"
    raw = to_sql(tree)
    return _clean_sql(raw)


def _clean_sql(sql):
    """Clean up SQL formatting."""
    # Normalize whitespace
    sql = " ".join(sql.split())

    # Remove redundant parentheses around simple conditions
    # (but keep them for compound conditions)

    # Ensure proper spacing around operators
    for op in [">=", "<=", "!=", "<>", "IS NOT NULL", "IS NULL",
               "NOT IN", "NOT LIKE", "NOT BETWEEN"]:
        sql = sql.replace(f"  {op}  ", f" {op} ")

    return sql.strip()


def format_value_for_like(value, transform=None):
    """Format a value for LIKE operator with wildcards."""
    if value is None:
        return "'%'"
    val_str = str(value).replace("'", "''")

    if transform == "prefix_wildcard":
        return f"'{val_str}%'"
    elif transform == "suffix_wildcard":
        return f"'%{val_str}'"
    elif transform == "contains_wildcard":
        return f"'%{val_str}%'"
    else:
        # Default: contains
        if "%" not in val_str:
            return f"'%{val_str}%'"
        return f"'{val_str}'"


def format_in_list(values):
    """Format a list of values for IN operator."""
    if not values:
        return "(NULL)"
    formatted = []
    for v in values:
        if isinstance(v, str):
            formatted.append(f"'{v.replace(chr(39), chr(39)+chr(39))}'")
        elif isinstance(v, (int, float)):
            formatted.append(str(v))
        elif v is None:
            formatted.append("NULL")
        else:
            formatted.append(f"'{v}'")
    return f"({', '.join(formatted)})"


def format_between(low, high):
    """Format BETWEEN values, ensuring correct ordering."""
    if isinstance(low, str) and isinstance(high, str):
        # Ensure lower value first for strings
        if low > high:
            low, high = high, low
        return f"'{low}' AND '{high}'"
    elif isinstance(low, (int, float)) and isinstance(high, (int, float)):
        if low > high:
            low, high = high, low
        return f"{low} AND {high}"
    return f"{_fmt(low)} AND {_fmt(high)}"


def _fmt(val):
    if val is None:
        return "NULL"
    if isinstance(val, str):
        return f"'{val}'"
    return str(val)
