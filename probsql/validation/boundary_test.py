"""
Boundary Test — Tests the engine with deliberately difficult inputs.

Feeds ambiguous, complex, and edge-case predicates to identify:
- Where confidence is correctly low (engine knows it's uncertain)
- Where confidence is incorrectly high (engine is wrong but confident)
- What categories of failure emerge

Usage: python phase4_validation/boundary_test.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.engine.engine import ProbSQLEngine

REPORTS_DIR = Path(__file__).parent / "reports"

# Manually crafted boundary cases with expected difficulty
BOUNDARY_CASES = [
    # Ambiguous column matching
    {"english": "show me the date", "difficulty": "ambiguous_column",
     "why": "Multiple date columns exist: created_at, updated_at, shipped_at"},
    {"english": "users with a name", "difficulty": "ambiguous_column",
     "why": "name column exists but 'with a name' implies IS NOT NULL, could mean 'named X'"},

    # Ambiguous operators
    {"english": "products around $100", "difficulty": "ambiguous_operator",
     "why": "'around' is vague — could be BETWEEN 90 AND 110, or approximate"},
    {"english": "roughly 5 star rating", "difficulty": "ambiguous_operator",
     "why": "'roughly' is imprecise"},
    {"english": "a few orders", "difficulty": "ambiguous_value",
     "why": "'a few' is not a specific number"},

    # Complex temporal
    {"english": "orders from the second week of last month", "difficulty": "complex_temporal",
     "why": "Requires computing specific date range within a past month"},
    {"english": "users who signed up on a Monday", "difficulty": "complex_temporal",
     "why": "Requires day-of-week extraction"},

    # Negation scope ambiguity
    {"english": "not orders shipped last month", "difficulty": "ambiguous_negation",
     "why": "Unclear: NOT shipped, or NOT last month?"},
    {"english": "users who didn't order anything expensive", "difficulty": "ambiguous_negation",
     "why": "Negation scope spans a subquery with implicit threshold"},

    # Domain jargon
    {"english": "churned users", "difficulty": "domain_jargon",
     "why": "'churned' has no direct column mapping"},
    {"english": "power users", "difficulty": "domain_jargon",
     "why": "'power user' is a concept, not a column value"},
    {"english": "MQLs from last quarter", "difficulty": "domain_jargon",
     "why": "Marketing qualified leads — acronym with no schema mapping"},

    # Requires aggregation (beyond WHERE clause)
    {"english": "users with more than 5 orders", "difficulty": "requires_aggregation",
     "why": "Needs GROUP BY/HAVING, not just WHERE"},
    {"english": "the most expensive product", "difficulty": "requires_aggregation",
     "why": "Needs ORDER BY/LIMIT, not WHERE"},
    {"english": "average order value above $50", "difficulty": "requires_aggregation",
     "why": "Needs AVG() and HAVING"},

    # Contradictory conditions
    {"english": "active and deleted users", "difficulty": "contradictory",
     "why": "Logically contradictory status values"},

    # Implicit joins
    {"english": "customers whose orders were shipped", "difficulty": "implicit_join",
     "why": "Requires JOIN between customers and orders"},
    {"english": "products reviewed by verified purchasers", "difficulty": "implicit_join",
     "why": "Requires JOIN and filtering on review properties"},

    # Very long/complex
    {"english": "active verified users who signed up in the past 6 months, placed at least 3 orders, and have not cancelled any order, but excluding users from the free plan",
     "difficulty": "very_complex",
     "why": "Multiple conditions, negation, temporal, aggregation, join"},

    # Empty/minimal
    {"english": "everything", "difficulty": "too_vague",
     "why": "No meaningful filter"},
    {"english": "good stuff", "difficulty": "too_vague",
     "why": "Completely vague, no mapping to SQL"},
]

DEMO_SCHEMA = {
    "tables": [
        {
            "name": "users",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "email", "type": "VARCHAR(255)"},
                {"name": "name", "type": "VARCHAR(100)"},
                {"name": "created_at", "type": "TIMESTAMP"},
                {"name": "updated_at", "type": "TIMESTAMP"},
                {"name": "is_active", "type": "BOOLEAN"},
                {"name": "is_verified", "type": "BOOLEAN"},
                {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "inactive", "suspended", "deleted"]},
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
                {"name": "shipped_at", "type": "TIMESTAMP"},
            ]
        },
        {
            "name": "products",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "name", "type": "VARCHAR(200)"},
                {"name": "price", "type": "DECIMAL(10,2)"},
                {"name": "rating", "type": "FLOAT"},
                {"name": "is_active", "type": "BOOLEAN"},
                {"name": "stock_quantity", "type": "INT"},
            ]
        },
        {
            "name": "reviews",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "product_id", "type": "INT"},
                {"name": "user_id", "type": "INT"},
                {"name": "rating", "type": "INT"},
                {"name": "is_verified_purchase", "type": "BOOLEAN"},
                {"name": "created_at", "type": "TIMESTAMP"},
            ]
        },
    ]
}


def run_boundary_test():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    engine = ProbSQLEngine()
    knowledge_dir = Path(__file__).parent.parent / "knowledge" / "base"
    if knowledge_dir.exists():
        engine.load_knowledge(str(knowledge_dir))

    results = {
        "total": len(BOUNDARY_CASES),
        "correctly_low_confidence": 0,  # low confidence on hard cases (GOOD)
        "incorrectly_high_confidence": 0,  # high confidence but likely wrong (BAD)
        "by_difficulty": {},
        "cases": [],
    }

    for case in BOUNDARY_CASES:
        english = case["english"]
        difficulty = case["difficulty"]
        why = case["why"]

        try:
            result = engine.generate(english, DEMO_SCHEMA)
            sql = result.sql_where
            confidence = result.confidence
        except Exception as e:
            sql = f"ERROR: {e}"
            confidence = 0.0

        # For boundary cases, low confidence is GOOD (engine knows it's uncertain)
        is_correctly_uncertain = confidence < 0.6
        is_incorrectly_confident = confidence > 0.8

        if is_correctly_uncertain:
            results["correctly_low_confidence"] += 1
        if is_incorrectly_confident:
            results["incorrectly_high_confidence"] += 1

        if difficulty not in results["by_difficulty"]:
            results["by_difficulty"][difficulty] = {"total": 0, "low_conf": 0, "high_conf": 0}
        results["by_difficulty"][difficulty]["total"] += 1
        if is_correctly_uncertain:
            results["by_difficulty"][difficulty]["low_conf"] += 1
        if is_incorrectly_confident:
            results["by_difficulty"][difficulty]["high_conf"] += 1

        results["cases"].append({
            "english": english,
            "difficulty": difficulty,
            "why": why,
            "sql": sql,
            "confidence": confidence,
            "correctly_uncertain": is_correctly_uncertain,
        })

    # Save report
    report_path = REPORTS_DIR / "boundary_report.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)

    # Print summary
    print(f"{'=' * 60}")
    print(f"BOUNDARY TEST RESULTS")
    print(f"{'=' * 60}")
    print(f"Total boundary cases: {results['total']}")
    print(f"Correctly low confidence: {results['correctly_low_confidence']} (GOOD - engine knows it's uncertain)")
    print(f"Incorrectly high confidence: {results['incorrectly_high_confidence']} (BAD - overconfident)")

    print(f"\nBy difficulty category:")
    for diff, data in sorted(results["by_difficulty"].items()):
        print(f"  {diff}: {data['total']} cases, low_conf={data['low_conf']}, high_conf={data['high_conf']}")

    print(f"\nDetailed results:")
    for case in results["cases"]:
        status = "OK" if case["correctly_uncertain"] else "WARN" if case["confidence"] > 0.8 else "MED"
        print(f"  [{status}] conf={case['confidence']:.2f} | {case['english']}")
        print(f"       SQL: {case['sql'][:80]}")


if __name__ == "__main__":
    run_boundary_test()
