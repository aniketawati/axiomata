"""Tests for the temporal expression parser."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.components.temporal_parser import TemporalParser


def test_today():
    p = TemporalParser()
    r = p.parse("today", "created_at")
    assert "date('now')" in r.sql_condition
    assert r.temporal_type == "relative_to_now"
    assert r.confidence >= 0.9


def test_yesterday():
    p = TemporalParser()
    r = p.parse("yesterday", "created_at")
    assert "-1 day" in r.sql_condition
    assert r.temporal_type == "relative_to_now"


def test_this_month():
    p = TemporalParser()
    r = p.parse("this month", "created_at")
    assert "start of month" in r.sql_condition
    assert r.temporal_type == "relative_to_now"


def test_last_week():
    p = TemporalParser()
    r = p.parse("last week", "created_at")
    assert "AND" in r.sql_condition
    assert r.temporal_type == "relative_to_now"


def test_past_30_days():
    p = TemporalParser()
    r = p.parse("past 30 days", "created_at")
    assert "-30 days" in r.sql_condition
    assert r.temporal_type == "relative_to_now"


def test_last_7_days():
    p = TemporalParser()
    r = p.parse("last 7 days", "created_at")
    assert "-7 days" in r.sql_condition


def test_recently():
    p = TemporalParser()
    r = p.parse("recently", "created_at")
    assert "-30 days" in r.sql_condition
    assert r.confidence < 0.9  # lower confidence for vague term


def test_absolute_month_year():
    p = TemporalParser()
    r = p.parse("in January 2024", "created_at")
    assert "'2024-01-01'" in r.sql_condition
    assert "'2024-02-01'" in r.sql_condition
    assert r.temporal_type == "absolute_date"


def test_absolute_quarter():
    p = TemporalParser()
    r = p.parse("in Q3 2024", "created_at")
    assert "'2024-07-01'" in r.sql_condition
    assert "'2024-10-01'" in r.sql_condition


def test_absolute_year():
    p = TemporalParser()
    r = p.parse("in 2024", "created_at")
    assert "'2024-01-01'" in r.sql_condition
    assert "'2025-01-01'" in r.sql_condition


def test_since():
    p = TemporalParser()
    r = p.parse("since 2023", "created_at")
    assert ">=" in r.sql_condition
    assert "'2023-01-01'" in r.sql_condition


def test_before():
    p = TemporalParser()
    r = p.parse("before March 2024", "created_at")
    assert "<" in r.sql_condition
    assert "'2024-03-01'" in r.sql_condition


def test_on_specific_date():
    p = TemporalParser()
    r = p.parse("on January 15, 2024", "created_at")
    assert "'2024-01-15'" in r.sql_condition


def test_between_months():
    p = TemporalParser()
    r = p.parse("between January and March 2024", "created_at")
    assert "'2024-01-01'" in r.sql_condition
    assert "'2024-04-01'" in r.sql_condition
    assert r.temporal_type == "date_range"


def test_is_temporal():
    p = TemporalParser()
    assert p.is_temporal("users who signed up last month")
    assert p.is_temporal("orders from this year")
    assert p.is_temporal("recently active users")
    assert not p.is_temporal("active users")
    assert not p.is_temporal("orders over $100")


def test_non_temporal_returns_fallback():
    p = TemporalParser()
    r = p.parse("some random text", "col")
    assert r.confidence <= 0.3


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
