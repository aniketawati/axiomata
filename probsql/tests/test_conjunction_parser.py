"""Tests for the conjunction parser."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.components.conjunction_parser import ConjunctionParser, LeafPredicate, ConjunctionNode


def test_simple_predicate():
    p = ConjunctionParser()
    tree = p.parse("active users")
    assert isinstance(tree, LeafPredicate)
    assert tree.text == "active users"


def test_and_conjunction():
    p = ConjunctionParser()
    tree = p.parse("active users and verified accounts")
    assert isinstance(tree, ConjunctionNode)
    assert tree.conjunction == "AND"
    assert isinstance(tree.left, LeafPredicate)
    assert isinstance(tree.right, LeafPredicate)


def test_or_conjunction():
    p = ConjunctionParser()
    tree = p.parse("premium users or enterprise users")
    assert isinstance(tree, ConjunctionNode)
    assert tree.conjunction == "OR"


def test_who_relative_clause():
    p = ConjunctionParser()
    tree = p.parse("users who signed up last month")
    assert isinstance(tree, ConjunctionNode)
    assert tree.conjunction == "AND"
    leaves = p.get_leaves(tree)
    assert len(leaves) == 2


def test_between_not_split():
    p = ConjunctionParser()
    tree = p.parse("products between $10 and $50")
    # "between X and Y" should NOT be split at "and"
    assert isinstance(tree, LeafPredicate)


def test_either_or():
    p = ConjunctionParser()
    tree = p.parse("either premium or enterprise users")
    assert isinstance(tree, ConjunctionNode)
    assert tree.conjunction == "OR"


def test_compound_with_who():
    p = ConjunctionParser()
    tree = p.parse("active users who signed up last month and have premium status")
    leaves = p.get_leaves(tree)
    assert len(leaves) >= 2


def test_get_conjunction_type_none():
    p = ConjunctionParser()
    tree = p.parse("active users")
    assert p.get_conjunction_type(tree) == "none"


def test_get_conjunction_type_and():
    p = ConjunctionParser()
    tree = p.parse("active users and verified accounts")
    assert p.get_conjunction_type(tree) == "and"


def test_empty_string():
    p = ConjunctionParser()
    tree = p.parse("")
    assert isinstance(tree, LeafPredicate)


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
