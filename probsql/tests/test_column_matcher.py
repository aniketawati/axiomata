"""Tests for the column matcher."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.components.column_matcher import ColumnMatcher

DEMO_SCHEMA = {
    "tables": [
        {
            "name": "users",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "email", "type": "VARCHAR(255)"},
                {"name": "name", "type": "VARCHAR(100)"},
                {"name": "created_at", "type": "TIMESTAMP"},
                {"name": "is_active", "type": "BOOLEAN"},
                {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "inactive", "suspended"]},
                {"name": "lifetime_value", "type": "DECIMAL(10,2)"},
            ]
        },
        {
            "name": "orders",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "user_id", "type": "INT"},
                {"name": "total_amount", "type": "DECIMAL(10,2)"},
                {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "shipped", "delivered"]},
                {"name": "created_at", "type": "TIMESTAMP"},
            ]
        }
    ]
}


def test_active_matches_is_active():
    m = ColumnMatcher()
    results = m.match("active users", DEMO_SCHEMA)
    assert len(results) > 0
    top = results[0]
    assert top.column_name == "is_active" or (top.column_name == "status" and top.table_name == "users")


def test_email_matches_email():
    m = ColumnMatcher()
    results = m.match("email address", DEMO_SCHEMA)
    assert len(results) > 0
    assert results[0].column_name == "email"


def test_temporal_matches_timestamp():
    m = ColumnMatcher()
    results = m.match("signed up last month", DEMO_SCHEMA)
    assert len(results) > 0
    top = results[0]
    assert top.column_name == "created_at"


def test_amount_matches_monetary():
    m = ColumnMatcher()
    results = m.match("spent more than $500", DEMO_SCHEMA)
    assert len(results) > 0
    top = results[0]
    assert top.column_name in ("total_amount", "lifetime_value")


def test_enum_value_matches():
    m = ColumnMatcher()
    results = m.match("pending orders", DEMO_SCHEMA)
    assert len(results) > 0
    top = results[0]
    assert top.column_name == "status"
    assert top.table_name == "orders"


def test_returns_multiple_candidates():
    m = ColumnMatcher()
    results = m.match("user name", DEMO_SCHEMA)
    assert len(results) >= 2


def test_score_ordering():
    m = ColumnMatcher()
    results = m.match("active status", DEMO_SCHEMA)
    assert len(results) >= 2
    assert results[0].score >= results[1].score


def test_empty_phrase():
    m = ColumnMatcher()
    results = m.match("", DEMO_SCHEMA)
    assert len(results) >= 0  # should not crash


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
