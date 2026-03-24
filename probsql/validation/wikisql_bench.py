"""
WikiSQL Benchmark — Tests ProbSQL engine against the WikiSQL dataset.

WikiSQL contains 80K+ simple text-to-SQL examples (SELECT + WHERE only)
with SQLite databases for execution-based evaluation.

Usage:
    python -m probsql.validation.wikisql_bench [--split dev|test] [--limit N]

Metrics:
    - WHERE clause condition accuracy (column match, operator match, value match)
    - Execution accuracy (same result set)
    - Component-level breakdown
"""

import json
import re
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.engine.engine import ProbSQLEngine

WIKISQL_DIR = Path(__file__).parent.parent.parent / "wikisql_data" / "data"
REPORTS_DIR = Path(__file__).parent / "reports"

AGG_OPS = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
COND_OPS = ['=', '>', '<', '>=', '<=']


def load_wikisql(split="dev"):
    """Load WikiSQL examples and tables for a given split."""
    examples_path = WIKISQL_DIR / f"{split}.jsonl"
    tables_path = WIKISQL_DIR / f"{split}.tables.jsonl"
    db_path = WIKISQL_DIR / f"{split}.db"

    tables = {}
    with open(tables_path) as f:
        for line in f:
            t = json.loads(line)
            tables[t["id"]] = t

    examples = []
    with open(examples_path) as f:
        for line in f:
            examples.append(json.loads(line))

    return examples, tables, db_path


def wikisql_table_to_schema(table):
    """Convert a WikiSQL table definition to our schema format."""
    columns = []
    for i, (header, dtype) in enumerate(zip(table["header"], table["types"])):
        col_type = "TEXT" if dtype == "text" else "REAL"
        columns.append({
            "name": header,
            "type": col_type,
            "wikisql_index": i,
        })
    return {
        "tables": [{
            "name": table.get("name", "table_" + table.get("id", "unknown").replace("-", "_")),
            "columns": columns,
        }]
    }


def reconstruct_where(sql_obj, table):
    """Reconstruct the ground-truth WHERE clause from WikiSQL format."""
    conds = sql_obj.get("conds", [])
    if not conds:
        return "", []

    parts = []
    parsed_conds = []
    for col_idx, op_idx, val in conds:
        col_name = table["header"][col_idx]
        op = COND_OPS[op_idx]
        # Quote the column name for SQLite (may contain spaces/special chars)
        safe_col = f'"{col_name}"'
        # Format value
        if isinstance(val, str):
            safe_val = val.replace("'", "''")
            parts.append(f"{safe_col} {op} '{safe_val}'")
        else:
            parts.append(f"{safe_col} {op} {val}")
        parsed_conds.append({"column": col_name, "column_index": col_idx, "operator": op, "value": val})

    return " AND ".join(parts), parsed_conds


def to_sqlite_where(where_clause, table):
    """Convert a WHERE clause with human-readable column names to col0/col1/... format."""
    if not where_clause:
        return where_clause
    result = where_clause
    # Strip table name prefixes (e.g., "table_1008653_1." → "")
    table_name = table.get("name", "")
    if table_name:
        result = result.replace(f"{table_name}.", "")
    # Replace human-readable column names with col0, col1, etc.
    # Sort by length descending to avoid partial replacements
    for i, header in sorted(enumerate(table["header"]), key=lambda x: -len(x[1])):
        result = result.replace(f'"{header}"', f'col{i}')
        result = result.replace(header, f'col{i}')
    return result


def get_sqlite_table_name(table):
    """Get the actual SQLite table name (table_1_XXXXX format)."""
    tid = table["id"]
    # WikiSQL uses "1-XXXXX" in id, SQLite uses "table_1_XXXXX"
    return "table_" + tid.replace("-", "_")


def normalize_value(val):
    """Normalize a value for comparison."""
    if val is None:
        return None
    s = str(val).strip().lower()
    # Remove quotes
    s = s.strip("'\"")
    # Try numeric
    try:
        return float(s)
    except ValueError:
        return s


def execute_query(db_path, table_name, where_clause):
    """Execute a SELECT * WHERE query and return results."""
    try:
        conn = sqlite3.connect(str(db_path))
        conn.create_function("lower", 1, lambda x: x.lower() if isinstance(x, str) else x)
        c = conn.cursor()
        if where_clause and where_clause.strip():
            # Make string comparisons case-insensitive
            where_ci = where_clause.replace(" = '", " = '").replace(" COLLATE NOCASE", "")
            query = f'SELECT * FROM "{table_name}" WHERE {where_ci}'
        else:
            query = f'SELECT * FROM "{table_name}"'
        c.execute(query)
        rows = c.fetchall()
        conn.close()
        return rows, None
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        return None, str(e)


def extract_conditions_from_engine(result, table):
    """Extract structured conditions from engine output for comparison."""
    sql = result.sql_where
    if not sql or sql == "1=1":
        return []

    conditions = []
    # Try to parse the engine's SQL output into conditions
    # Split on AND (simple approach)
    parts = re.split(r'\s+AND\s+', sql, flags=re.IGNORECASE)
    for part in parts:
        part = part.strip()
        # Try to match: column op value
        m = re.match(r'(?:(\w+)\.)?["\']?(.+?)["\']?\s*(=|!=|>|<|>=|<=|LIKE|IN|IS)\s*(.*)', part, re.IGNORECASE)
        if m:
            col = m.group(2).strip().strip('"\'')
            op = m.group(3).strip()
            val = m.group(4).strip().strip("'\"")
            conditions.append({"column": col, "operator": op, "value": val})

    return conditions


def compare_conditions(engine_conds, oracle_conds, table):
    """Compare engine conditions against oracle conditions."""
    if not oracle_conds:
        return {"column_match": not engine_conds, "operator_match": not engine_conds,
                "value_match": not engine_conds, "full_match": not engine_conds}

    if not engine_conds:
        return {"column_match": False, "operator_match": False,
                "value_match": False, "full_match": False}

    # For each oracle condition, find best matching engine condition
    col_matches = 0
    op_matches = 0
    val_matches = 0
    full_matches = 0

    header_lower = [h.lower() for h in table["header"]]

    for oracle_cond in oracle_conds:
        oracle_col = oracle_cond["column"].lower()
        oracle_op = oracle_cond["operator"]
        oracle_val = normalize_value(oracle_cond["value"])

        best_col = False
        best_op = False
        best_val = False

        for eng_cond in engine_conds:
            eng_col = eng_cond["column"].lower()
            eng_op = eng_cond["operator"]
            eng_val = normalize_value(eng_cond["value"])

            # Column match — check exact or fuzzy
            col_match = (eng_col == oracle_col or
                        eng_col in oracle_col or
                        oracle_col in eng_col or
                        any(eng_col == h for h in header_lower if h == oracle_col))

            if col_match:
                best_col = True
                if eng_op == oracle_op:
                    best_op = True
                if eng_val == oracle_val:
                    best_val = True

        if best_col:
            col_matches += 1
        if best_col and best_op:
            op_matches += 1
        if best_col and best_op and best_val:
            full_matches += 1

    n = len(oracle_conds)
    return {
        "column_match": col_matches / n if n else True,
        "operator_match": op_matches / n if n else True,
        "value_match": full_matches / n if n else True,
        "full_match": full_matches == n,
    }


def run_benchmark(split="dev", limit=None, verbose=False):
    """Run the WikiSQL benchmark."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Loading WikiSQL {split} set...")
    examples, tables, db_path = load_wikisql(split)
    if limit:
        examples = examples[:limit]
    print(f"Loaded {len(examples)} examples, {len(tables)} tables")

    print("Initializing ProbSQL engine...")
    engine = ProbSQLEngine()
    knowledge_dir = Path(__file__).parent.parent / "knowledge" / "base"
    if knowledge_dir.exists():
        engine.load_knowledge(str(knowledge_dir))

    # Filter to examples with WHERE conditions (skip those with 0 conditions)
    examples_with_where = [ex for ex in examples if ex["sql"]["conds"]]
    print(f"Examples with WHERE conditions: {len(examples_with_where)} / {len(examples)}")

    results = {
        "split": split,
        "total": len(examples_with_where),
        "column_accuracy": 0,
        "operator_accuracy": 0,
        "value_accuracy": 0,
        "full_condition_match": 0,
        "execution_accuracy": 0,
        "engine_errors": 0,
        "oracle_exec_errors": 0,
        "engine_exec_errors": 0,
        "by_num_conditions": {},
        "by_operator": {},
        "avg_confidence": 0,
        "avg_latency_ms": 0,
    }

    total_confidence = 0
    total_latency = 0
    col_acc_sum = 0
    op_acc_sum = 0
    val_acc_sum = 0
    full_match_count = 0
    exec_match_count = 0

    for i, ex in enumerate(examples_with_where):
        table = tables[ex["table_id"]]
        schema = wikisql_table_to_schema(table)
        question = ex["question"]

        # Ground truth
        oracle_where, oracle_conds = reconstruct_where(ex["sql"], table)
        num_conds = len(oracle_conds)

        # Engine prediction
        start = time.perf_counter()
        try:
            result = engine.generate(question, schema)
            engine_sql = result.sql_where
            confidence = result.confidence
        except Exception as e:
            results["engine_errors"] += 1
            if verbose:
                print(f"  ERROR [{i}]: {e}")
            continue
        latency_ms = (time.perf_counter() - start) * 1000
        total_latency += latency_ms
        total_confidence += confidence

        # Compare conditions structurally
        engine_conds = extract_conditions_from_engine(result, table)
        comparison = compare_conditions(engine_conds, oracle_conds, table)

        col_acc_sum += comparison["column_match"]
        op_acc_sum += comparison["operator_match"]
        val_acc_sum += comparison["value_match"]
        if comparison["full_match"]:
            full_match_count += 1

        # Execution accuracy — translate to SQLite col0/col1 format
        sqlite_table = get_sqlite_table_name(table)
        oracle_sqlite_where = to_sqlite_where(oracle_where, table)
        engine_sqlite_where = to_sqlite_where(engine_sql, table)
        oracle_rows, oracle_err = execute_query(db_path, sqlite_table, oracle_sqlite_where)
        engine_rows, engine_err = execute_query(db_path, sqlite_table, engine_sqlite_where)

        if oracle_err:
            results["oracle_exec_errors"] += 1
        if engine_err:
            results["engine_exec_errors"] += 1

        if oracle_rows is not None and engine_rows is not None:
            if set(map(tuple, oracle_rows)) == set(map(tuple, engine_rows)):
                exec_match_count += 1

        # Track by num conditions
        key = str(num_conds)
        if key not in results["by_num_conditions"]:
            results["by_num_conditions"][key] = {"total": 0, "full_match": 0, "exec_match": 0, "col_match": 0}
        results["by_num_conditions"][key]["total"] += 1
        if comparison["full_match"]:
            results["by_num_conditions"][key]["full_match"] += 1
        if oracle_rows is not None and engine_rows is not None and set(map(tuple, oracle_rows)) == set(map(tuple, engine_rows)):
            results["by_num_conditions"][key]["exec_match"] += 1
        results["by_num_conditions"][key]["col_match"] += comparison["column_match"]

        # Track by operator
        for cond in oracle_conds:
            op = cond["operator"]
            if op not in results["by_operator"]:
                results["by_operator"][op] = {"total": 0, "matched": 0}
            results["by_operator"][op]["total"] += 1

        if verbose and i < 20:
            print(f"\n  [{i}] Q: {question}")
            print(f"       Oracle: {oracle_where}")
            print(f"       Engine: {engine_sql}")
            print(f"       Col: {comparison['column_match']:.0%} Op: {comparison['operator_match']:.0%} Full: {comparison['full_match']}")
            if oracle_rows is not None and engine_rows is not None:
                print(f"       Exec match: {set(map(tuple, oracle_rows)) == set(map(tuple, engine_rows))} (oracle={len(oracle_rows)} rows, engine={len(engine_rows)} rows)")

        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(examples_with_where)}...")

    n = len(examples_with_where) - results["engine_errors"]
    n = max(n, 1)

    results["column_accuracy"] = col_acc_sum / n
    results["operator_accuracy"] = op_acc_sum / n
    results["value_accuracy"] = val_acc_sum / n
    results["full_condition_match"] = full_match_count / n
    results["execution_accuracy"] = exec_match_count / n
    results["avg_confidence"] = total_confidence / n
    results["avg_latency_ms"] = total_latency / n

    # Save report
    report_path = REPORTS_DIR / f"wikisql_{split}_report.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)

    # Print summary
    print(f"\n{'=' * 65}")
    print(f"WIKISQL BENCHMARK RESULTS ({split} set)")
    print(f"{'=' * 65}")
    print(f"Total examples (with WHERE):  {results['total']}")
    print(f"Engine errors:                {results['engine_errors']}")
    print(f"Engine exec errors:           {results['engine_exec_errors']}")
    print(f"Oracle exec errors:           {results['oracle_exec_errors']}")
    print(f"")
    print(f"Column match accuracy:        {results['column_accuracy']:.1%}")
    print(f"Operator match accuracy:      {results['operator_accuracy']:.1%}")
    print(f"Full condition match:         {results['full_condition_match']:.1%}")
    print(f"Execution accuracy:           {results['execution_accuracy']:.1%}")
    print(f"")
    print(f"Avg confidence:               {results['avg_confidence']:.3f}")
    print(f"Avg latency:                  {results['avg_latency_ms']:.2f}ms")
    print(f"")
    print(f"By number of conditions:")
    for k in sorted(results["by_num_conditions"].keys()):
        d = results["by_num_conditions"][k]
        t = d["total"]
        print(f"  {k} cond: {t} examples, col={d['col_match']/t:.1%}, full={d['full_match']}/{t} ({d['full_match']/t:.1%}), exec={d['exec_match']}/{t} ({d['exec_match']/t:.1%})")

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="WikiSQL benchmark for ProbSQL")
    parser.add_argument("--split", default="dev", choices=["dev", "test", "train"])
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    run_benchmark(split=args.split, limit=args.limit, verbose=args.verbose)
