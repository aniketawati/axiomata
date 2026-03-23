"""Tests for the operator extractor."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.components.operator_extractor import OperatorExtractor


def test_greater_than():
    oe = OperatorExtractor()
    op, conf, _ = oe.extract("more than 100", {"type": "INT"})
    assert op == ">"
    assert conf > 0.5


def test_at_least():
    oe = OperatorExtractor()
    op, conf, _ = oe.extract("at least 5 orders", {"type": "INT"})
    assert op == ">="


def test_less_than():
    oe = OperatorExtractor()
    op, conf, _ = oe.extract("less than $50", {"type": "DECIMAL(10,2)"})
    assert op == "<"


def test_between():
    oe = OperatorExtractor()
    op, conf, _ = oe.extract("between 10 and 20", {"type": "INT"})
    assert op == "BETWEEN"


def test_like_contains():
    oe = OperatorExtractor()
    op, conf, transform = oe.extract("contains premium", {"type": "VARCHAR(100)"})
    assert op == "LIKE"
    assert transform == "contains_wildcard"


def test_starts_with():
    oe = OperatorExtractor()
    op, conf, transform = oe.extract("starts with John", {"type": "VARCHAR(100)"})
    assert op == "LIKE"
    assert transform == "prefix_wildcard"


def test_is_null():
    oe = OperatorExtractor()
    op, conf, _ = oe.extract("missing email", {"type": "VARCHAR(255)"})
    assert op == "IS NULL"


def test_in_list():
    oe = OperatorExtractor()
    op, conf, _ = oe.extract("one of active, pending, or suspended", {"type": "VARCHAR(20)"})
    assert op == "IN"


def test_boolean_true():
    oe = OperatorExtractor()
    op, conf, transform = oe.extract("active users", {"type": "BOOLEAN", "column_name": "is_active"})
    assert op == "="
    assert transform == "boolean_true"


def test_boolean_false():
    oe = OperatorExtractor()
    op, conf, transform = oe.extract("not active", {"type": "BOOLEAN", "column_name": "is_active"})
    assert op in ("=", "!=")


def test_enum_default():
    oe = OperatorExtractor()
    op, conf, _ = oe.extract("pending status", {"type": "VARCHAR(20)", "enum_values": ["active", "pending"]})
    assert op == "="


def test_numeric_default():
    oe = OperatorExtractor()
    op, conf, _ = oe.extract("rating of 5", {"type": "INT"})
    assert op == "="


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS: {t.__name__}")
        except Exception as e:
            failed += 1
            print(f"  FAIL: {t.__name__}: {e}")
    print(f"\n{passed} passed, {failed} failed out of {passed + failed}")
