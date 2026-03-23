"""Integration tests for the ProbSQL engine."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.engine.engine import ProbSQLEngine

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
                {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "shipped", "delivered", "cancelled"]},
                {"name": "created_at", "type": "TIMESTAMP"},
            ]
        }
    ]
}


def get_engine():
    engine = ProbSQLEngine()
    knowledge_dir = Path(__file__).parent.parent / "knowledge" / "base"
    if knowledge_dir.exists():
        engine.load_knowledge(str(knowledge_dir))
    return engine


def test_simple_boolean():
    engine = get_engine()
    result = engine.generate("active users", DEMO_SCHEMA)
    assert result.sql_where
    assert result.confidence > 0


def test_simple_comparison():
    engine = get_engine()
    result = engine.generate("orders over $100", DEMO_SCHEMA)
    assert "100" in result.sql_where
    assert result.confidence > 0


def test_temporal():
    engine = get_engine()
    result = engine.generate("users who signed up this year", DEMO_SCHEMA)
    assert result.sql_where
    assert "created_at" in result.sql_where or "start of year" in result.sql_where


def test_enum_match():
    engine = get_engine()
    result = engine.generate("cancelled orders", DEMO_SCHEMA)
    assert "cancelled" in result.sql_where.lower()


def test_returns_generation_result():
    engine = get_engine()
    result = engine.generate("active users", DEMO_SCHEMA)
    assert hasattr(result, "sql_where")
    assert hasattr(result, "confidence")
    assert hasattr(result, "alternatives")
    assert hasattr(result, "predicate_tree")
    assert hasattr(result, "debug_info")


def test_compound_predicate():
    engine = get_engine()
    result = engine.generate("active users and pending orders", DEMO_SCHEMA)
    assert "AND" in result.sql_where


def test_empty_input():
    engine = get_engine()
    result = engine.generate("", DEMO_SCHEMA)
    assert result.sql_where is not None


def test_debug_info_present():
    engine = get_engine()
    result = engine.generate("active users", DEMO_SCHEMA)
    assert "steps" in result.debug_info
    assert len(result.debug_info["steps"]) > 0


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
