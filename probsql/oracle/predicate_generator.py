"""
Orchestrates subagent-based predicate generation.

Usage: python phase1_oracle/predicate_generator.py

This script is NOT run directly — it documents the generation logic.
The actual generation is done via Claude Code subagents that are spawned
from the main conversation. See the generate_batch() function for the
subagent prompt construction.

Reads schemas from phase1_oracle/schemas/
Outputs oracle data to phase1_oracle/oracle_dataset/

For each schema:
  1. Load schema JSON
  2. Spawn subagent with the prompt template + schema
  3. Parse subagent's JSON response
  4. Validate each example (SQL syntax check, column name verification)
  5. Save to oracle_dataset/

After all schemas processed:
  6. Combine into a single oracle_dataset/all_examples.json
  7. Run statistics
  8. Print summary report
"""

import json
import re
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
SCHEMAS_DIR = SCRIPT_DIR / "schemas"
DATASET_DIR = SCRIPT_DIR / "dataset"
PROMPT_PATH = SCRIPT_DIR / "prompts" / "generate_predicates.md"


def load_prompt_template():
    return PROMPT_PATH.read_text()


def build_prompt(schema_json_str):
    template = load_prompt_template()
    return template.replace("{schema_json}", schema_json_str)


def get_all_columns(schema):
    """Get set of all table.column references in a schema."""
    cols = set()
    for table in schema["tables"]:
        for col in table["columns"]:
            cols.add(f"{table['name']}.{col['name']}")
    return cols


def validate_example(example, schema):
    """Validate a single generated example against its schema."""
    errors = []
    all_cols = get_all_columns(schema)
    table_names = {t["name"] for t in schema["tables"]}

    # Check target_table exists
    if example.get("target_table") not in table_names:
        errors.append(f"target_table '{example.get('target_table')}' not in schema")

    # Check columns_referenced exist
    latent = example.get("latent_variables", {})
    for col_ref in latent.get("columns_referenced", []):
        if col_ref not in all_cols:
            errors.append(f"column '{col_ref}' not in schema")

    # Check latent variable consistency
    if latent.get("has_temporal") and not latent.get("temporal_type"):
        errors.append("has_temporal=true but temporal_type is null")

    if latent.get("has_negation") and not latent.get("negation_scope"):
        errors.append("has_negation=true but negation_scope is null")

    # Check sql_where is non-empty
    if not example.get("sql_where", "").strip():
        errors.append("sql_where is empty")

    # Check english is non-empty
    if not example.get("english", "").strip():
        errors.append("english is empty")

    # Basic SQL syntax check: balanced parentheses
    sql = example.get("sql_where", "")
    if sql.count("(") != sql.count(")"):
        errors.append("unbalanced parentheses in sql_where")

    return errors


def validate_batch(examples, schema):
    """Validate a batch of examples, return valid ones and error log."""
    valid = []
    error_log = []
    seen = set()

    for i, ex in enumerate(examples):
        errors = validate_example(ex, schema)

        # Check for duplicates
        key = (ex.get("english", ""), ex.get("sql_where", ""))
        if key in seen:
            errors.append("duplicate example")
        seen.add(key)

        if errors:
            error_log.append({"index": i, "errors": errors, "english": ex.get("english", "?")})
        else:
            valid.append(ex)

    return valid, error_log


def combine_all_datasets():
    """Combine all per-schema dataset files into one."""
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    all_examples = []

    for f in sorted(DATASET_DIR.glob("*.json")):
        if f.name in ("all_examples.json", "stats.json"):
            continue
        with open(f) as fh:
            data = json.load(fh)
            if isinstance(data, list):
                all_examples.extend(data)
            elif isinstance(data, dict) and "examples" in data:
                all_examples.extend(data["examples"])

    out_path = DATASET_DIR / "all_examples.json"
    with open(out_path, "w") as f:
        json.dump(all_examples, f, indent=2)

    print(f"Combined {len(all_examples)} examples into {out_path}")
    return all_examples


def compute_stats(examples):
    """Compute and save statistics on the oracle dataset."""
    from collections import Counter

    stats = {
        "total_examples": len(examples),
        "by_predicate_type": dict(Counter(
            ex.get("latent_variables", {}).get("predicate_type", "unknown") for ex in examples
        )),
        "by_domain": dict(Counter(
            ex.get("domain", "unknown") for ex in examples
        )),
        "operator_frequency": dict(Counter(
            op for ex in examples
            for op in ex.get("latent_variables", {}).get("operators_used", [])
        )),
        "temporal_type_counts": dict(Counter(
            ex.get("latent_variables", {}).get("temporal_type")
            for ex in examples
            if ex.get("latent_variables", {}).get("has_temporal")
        )),
        "conjunction_type_counts": dict(Counter(
            ex.get("latent_variables", {}).get("conjunction_type", "unknown") for ex in examples
        )),
        "avg_english_length_words": sum(
            len(ex.get("english", "").split()) for ex in examples
        ) / max(len(examples), 1),
        "avg_sql_length_chars": sum(
            len(ex.get("sql_where", "")) for ex in examples
        ) / max(len(examples), 1),
        "has_negation_count": sum(
            1 for ex in examples if ex.get("latent_variables", {}).get("has_negation")
        ),
        "requires_join_count": sum(
            1 for ex in examples if ex.get("requires_join")
        ),
    }

    stats_path = DATASET_DIR / "stats.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"\n=== Oracle Dataset Statistics ===")
    print(f"Total examples: {stats['total_examples']}")
    print(f"\nBy predicate type:")
    for k, v in sorted(stats['by_predicate_type'].items()):
        print(f"  {k}: {v}")
    print(f"\nBy domain:")
    for k, v in sorted(stats['by_domain'].items()):
        print(f"  {k}: {v}")
    print(f"\nOperator frequency (top 10):")
    for k, v in sorted(stats['operator_frequency'].items(), key=lambda x: -x[1])[:10]:
        print(f"  {k}: {v}")
    print(f"\nTemporal types:")
    for k, v in sorted(stats['temporal_type_counts'].items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print(f"\nAvg english length: {stats['avg_english_length_words']:.1f} words")
    print(f"Avg SQL length: {stats['avg_sql_length_chars']:.0f} chars")
    print(f"Examples with negation: {stats['has_negation_count']}")
    print(f"Examples requiring join: {stats['requires_join_count']}")

    return stats


if __name__ == "__main__":
    # When called directly, combine existing datasets and compute stats
    examples = combine_all_datasets()
    if examples:
        compute_stats(examples)
    else:
        print("No examples found. Run subagent generation first.")
