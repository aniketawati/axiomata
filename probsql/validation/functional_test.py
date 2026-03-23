"""
Functional Test — Core validation of the ProbSQL engine.

For held-out examples from the oracle dataset:
1. Generate SQL with ProbSQL engine
2. Compare to oracle SQL
3. Execute both against test SQLite databases
4. Measure accuracy metrics

Usage: python phase4_validation/functional_test.py
"""

import json
import random
import re
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.engine.engine import ProbSQLEngine

SCRIPT_DIR = Path(__file__).parent
DB_DIR = SCRIPT_DIR / "test_databases"
REPORTS_DIR = SCRIPT_DIR / "reports"
ORACLE_DIR = Path(__file__).parent.parent / "oracle" / "dataset"
SCHEMAS_DIR = Path(__file__).parent.parent / "oracle" / "schemas"

DOMAIN_DB_MAP = {
    "ecommerce": "ecommerce.db",
    "saas": "saas.db",
    "healthcare": "healthcare.db",
    "hr": "hr.db",
    "education": "education.db",
}


def load_holdout_examples(n=500, seed=42):
    """Load n examples for testing, stratified by predicate type."""
    all_path = ORACLE_DIR / "all_examples.json"
    if not all_path.exists():
        print("ERROR: all_examples.json not found. Run predicate_generator.py first.")
        return []

    with open(all_path) as f:
        examples = json.load(f)

    # Filter to domains we have test DBs for
    examples = [ex for ex in examples if ex.get("domain") in DOMAIN_DB_MAP]

    rng = random.Random(seed)
    rng.shuffle(examples)
    return examples[:n]


def load_schema(schema_id):
    schema_path = SCHEMAS_DIR / f"{schema_id}.json"
    if schema_path.exists():
        with open(schema_path) as f:
            return json.load(f)
    return None


def normalize_sql(sql):
    """Normalize SQL for comparison."""
    if not sql:
        return ""
    sql = sql.strip().lower()
    sql = re.sub(r'\s+', ' ', sql)
    sql = sql.replace("( ", "(").replace(" )", ")")
    sql = sql.replace(" ,", ",")
    return sql


def execute_sql(db_path, table, where_clause):
    """Execute a SELECT with WHERE clause and return result set."""
    try:
        conn = sqlite3.connect(str(db_path))
        c = conn.cursor()
        query = f"SELECT * FROM {table} WHERE {where_clause} LIMIT 1000"
        c.execute(query)
        rows = c.fetchall()
        conn.close()
        return rows, None
    except Exception as e:
        return None, str(e)


def run_functional_test(num_examples=500):
    """Run the full functional test."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    engine = ProbSQLEngine()
    knowledge_dir = Path(__file__).parent.parent / "knowledge" / "base"
    if knowledge_dir.exists():
        engine.load_knowledge(str(knowledge_dir))

    examples = load_holdout_examples(num_examples)
    if not examples:
        print("No examples to test.")
        return

    print(f"Testing {len(examples)} examples...")

    results = {
        "total": len(examples),
        "exact_sql_match": 0,
        "result_set_equivalent": 0,
        "engine_error": 0,
        "oracle_error": 0,
        "both_error": 0,
        "by_predicate_type": {},
        "by_domain": {},
        "confidence_buckets": {},
        "details": [],
    }

    for i, ex in enumerate(examples):
        schema_id = ex.get("schema_id", "")
        schema = load_schema(schema_id)
        if not schema:
            continue

        domain = ex.get("domain", "unknown")
        db_name = DOMAIN_DB_MAP.get(domain)
        db_path = DB_DIR / db_name if db_name else None
        target_table = ex.get("target_table", "")
        oracle_sql = ex.get("sql_where", "")
        english = ex.get("english", "")
        ptype = ex.get("latent_variables", {}).get("predicate_type", "unknown")

        # Generate SQL
        try:
            result = engine.generate(english, schema)
            engine_sql = result.sql_where
            confidence = result.confidence
        except Exception as e:
            results["engine_error"] += 1
            continue

        # Compare SQL strings
        exact_match = normalize_sql(engine_sql) == normalize_sql(oracle_sql)
        if exact_match:
            results["exact_sql_match"] += 1

        # Execute both against DB (if available)
        result_equiv = False
        if db_path and db_path.exists() and target_table:
            engine_rows, engine_err = execute_sql(db_path, target_table, engine_sql)
            oracle_rows, oracle_err = execute_sql(db_path, target_table, oracle_sql)

            if engine_err:
                results["engine_error"] += 1
            if oracle_err:
                results["oracle_error"] += 1
            if engine_err and oracle_err:
                results["both_error"] += 1

            if engine_rows is not None and oracle_rows is not None:
                result_equiv = set(map(tuple, engine_rows)) == set(map(tuple, oracle_rows))
                if result_equiv:
                    results["result_set_equivalent"] += 1

        # Track by predicate type
        if ptype not in results["by_predicate_type"]:
            results["by_predicate_type"][ptype] = {"total": 0, "exact": 0, "equiv": 0}
        results["by_predicate_type"][ptype]["total"] += 1
        if exact_match:
            results["by_predicate_type"][ptype]["exact"] += 1
        if result_equiv:
            results["by_predicate_type"][ptype]["equiv"] += 1

        # Track by domain
        if domain not in results["by_domain"]:
            results["by_domain"][domain] = {"total": 0, "exact": 0, "equiv": 0}
        results["by_domain"][domain]["total"] += 1
        if exact_match:
            results["by_domain"][domain]["exact"] += 1
        if result_equiv:
            results["by_domain"][domain]["equiv"] += 1

        # Confidence bucket
        bucket = f"{int(confidence * 10) / 10:.1f}"
        if bucket not in results["confidence_buckets"]:
            results["confidence_buckets"][bucket] = {"total": 0, "correct": 0}
        results["confidence_buckets"][bucket]["total"] += 1
        if exact_match or result_equiv:
            results["confidence_buckets"][bucket]["correct"] += 1

        if i % 100 == 0 and i > 0:
            print(f"  Processed {i}/{len(examples)}...")

    # Compute rates
    total = max(results["total"], 1)
    results["exact_sql_match_rate"] = results["exact_sql_match"] / total
    results["result_set_equiv_rate"] = results["result_set_equivalent"] / total
    results["error_rate"] = results["engine_error"] / total

    # Save report
    report_path = REPORTS_DIR / "functional_report.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"FUNCTIONAL TEST RESULTS")
    print(f"{'=' * 60}")
    print(f"Total examples tested: {results['total']}")
    print(f"Exact SQL match: {results['exact_sql_match']} ({results['exact_sql_match_rate']:.1%})")
    print(f"Result set equivalent: {results['result_set_equivalent']} ({results['result_set_equiv_rate']:.1%})")
    print(f"Engine errors: {results['engine_error']} ({results['error_rate']:.1%})")
    print(f"\nBy predicate type:")
    for ptype, data in sorted(results["by_predicate_type"].items()):
        t = data["total"]
        print(f"  {ptype}: {t} examples, exact={data['exact']}/{t}, equiv={data['equiv']}/{t}")
    print(f"\nBy domain:")
    for domain, data in sorted(results["by_domain"].items()):
        t = data["total"]
        print(f"  {domain}: {t} examples, exact={data['exact']}/{t}, equiv={data['equiv']}/{t}")


if __name__ == "__main__":
    run_functional_test()
