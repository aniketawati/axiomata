"""Tests for the negation handler."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.components.negation_handler import NegationHandler


def test_no_negation():
    h = NegationHandler()
    info = h.detect("active users")
    assert not info.has_negation
    assert info.negation_type is None


def test_not_negation():
    h = NegationHandler()
    info = h.detect("users who are not premium")
    assert info.has_negation
    assert info.negation_type == "operator"


def test_contraction_negation():
    h = NegationHandler()
    info = h.detect("users who haven't logged in")
    assert info.has_negation
    assert info.negation_type == "operator"


def test_prefix_negation():
    h = NegationHandler()
    info = h.detect("non-premium users")
    assert info.has_negation
    assert info.negation_type == "prefix"


def test_without_null_check():
    h = NegationHandler()
    info = h.detect("orders without a shipping address")
    assert info.has_negation
    assert info.negation_type == "null_check"


def test_missing_null_check():
    h = NegationHandler()
    info = h.detect("missing email address")
    assert info.has_negation
    assert info.negation_type == "null_check"


def test_excluding_filter():
    h = NegationHandler()
    info = h.detect("excluding cancelled orders")
    assert info.has_negation
    assert info.negation_type == "filter"


def test_no_existence():
    h = NegationHandler()
    info = h.detect("no orders placed")
    assert info.has_negation
    assert info.negation_type == "existence"


def test_apply_negation_equals():
    h = NegationHandler()
    info = h.detect("not active")
    op, val = h.apply_negation("=", "'active'", info)
    assert op == "!="


def test_apply_negation_null():
    h = NegationHandler()
    info = h.detect("without shipping address")
    op, val = h.apply_negation("=", "'addr'", info)
    assert op == "IS NULL"


def test_cleaned_phrase():
    h = NegationHandler()
    info = h.detect("excluding cancelled orders")
    assert "excluding" not in info.cleaned_phrase
    assert "cancelled" in info.cleaned_phrase


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
