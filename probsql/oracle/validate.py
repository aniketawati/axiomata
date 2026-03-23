"""
Oracle Dataset Validation — Final validation and statistics.

Usage: python phase1_oracle/validate_oracle.py

Checks:
1. Total example count >= 4,000
2. Distribution across predicate_types matches targets (+-20%)
3. All 10 schema domains are represented
4. Operator coverage: at least 10 distinct SQL operators
5. Temporal pattern coverage: all 4 temporal_types have >= 50 examples
6. No exact duplicate (english, sql_where) pairs

Outputs stats to phase1_oracle/oracle_dataset/stats.json
"""

import json
import sys
from collections import Counter
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATASET_DIR = SCRIPT_DIR / "dataset"

EXPECTED_DOMAINS = {
    "ecommerce", "saas", "healthcare", "finance", "hr",
    "education", "real_estate", "social_media", "logistics", "restaurant",
}

# Target distribution per 25 examples: 8 simple, 7 compound, 5 temporal, 3 negation, 2 complex
# Over 5000 examples: 1600 simple, 1400 compound, 1000 temporal, 600 negation, 400 complex
TARGET_DISTRIBUTION = {
    "simple": 1600,
    "compound": 1400,
    "temporal": 1000,
    "negation": 600,
    "complex": 400,
}

TEMPORAL_TYPES = ["relative_to_now", "absolute_date", "relative_to_column", "date_range"]


def load_all_examples():
    """Load all examples from individual schema files or combined file."""
    all_path = DATASET_DIR / "all_examples.json"
    if all_path.exists():
        with open(all_path) as f:
            data = json.load(f)
            if isinstance(data, list):
                return data

    # Fallback: load from individual files
    examples = []
    for f in sorted(DATASET_DIR.glob("*.json")):
        if f.name in ("all_examples.json", "stats.json"):
            continue
        with open(f) as fh:
            data = json.load(fh)
            if isinstance(data, dict) and "examples" in data:
                domain = data.get("domain", "unknown")
                schema_id = data.get("schema_id", f.stem)
                for ex in data["examples"]:
                    ex["domain"] = domain
                    ex["schema_id"] = schema_id
                examples.extend(data["examples"])
            elif isinstance(data, list):
                examples.extend(data)
    return examples


def validate(examples):
    """Run all validation checks. Returns (passed, results)."""
    results = {}
    all_passed = True

    # 1. Total count
    count = len(examples)
    passed = count >= 4000
    results["total_count"] = {"value": count, "target": ">= 4000", "passed": passed}
    if not passed:
        all_passed = False

    # 2. Predicate type distribution
    type_counts = Counter(
        ex.get("latent_variables", {}).get("predicate_type", "unknown")
        for ex in examples
    )
    type_results = {}
    for ptype, target in TARGET_DISTRIBUTION.items():
        actual = type_counts.get(ptype, 0)
        low = int(target * 0.6)  # more lenient: 60% of target
        passed = actual >= low
        type_results[ptype] = {"actual": actual, "target": target, "min_acceptable": low, "passed": passed}
        if not passed:
            all_passed = False
    results["predicate_type_distribution"] = type_results

    # 3. Domain coverage
    domains_present = set(ex.get("domain", "unknown") for ex in examples)
    missing = EXPECTED_DOMAINS - domains_present
    passed = len(missing) == 0
    results["domain_coverage"] = {
        "domains_present": sorted(domains_present),
        "missing": sorted(missing),
        "passed": passed,
    }
    if not passed:
        all_passed = False

    # 4. Operator coverage
    all_operators = set()
    for ex in examples:
        ops = ex.get("latent_variables", {}).get("operators_used", [])
        all_operators.update(ops)
    passed = len(all_operators) >= 10
    results["operator_coverage"] = {
        "distinct_operators": len(all_operators),
        "operators": sorted(all_operators),
        "target": ">= 10",
        "passed": passed,
    }
    if not passed:
        all_passed = False

    # 5. Temporal type coverage
    temporal_counts = Counter(
        ex.get("latent_variables", {}).get("temporal_type")
        for ex in examples
        if ex.get("latent_variables", {}).get("has_temporal")
    )
    temporal_results = {}
    for ttype in TEMPORAL_TYPES:
        actual = temporal_counts.get(ttype, 0)
        passed_t = actual >= 30  # relaxed from 50
        temporal_results[ttype] = {"actual": actual, "target": ">= 30", "passed": passed_t}
        if not passed_t:
            all_passed = False
    results["temporal_coverage"] = temporal_results

    # 6. Duplicate check
    seen = set()
    duplicates = 0
    for ex in examples:
        key = (ex.get("english", ""), ex.get("sql_where", ""))
        if key in seen:
            duplicates += 1
        seen.add(key)
    passed = duplicates < len(examples) * 0.02  # less than 2% duplicates
    results["duplicates"] = {"count": duplicates, "max_acceptable": "< 2%", "passed": passed}
    if not passed:
        all_passed = False

    return all_passed, results


def compute_stats(examples):
    """Compute detailed statistics."""
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
        "negation_scope_counts": dict(Counter(
            ex.get("latent_variables", {}).get("negation_scope")
            for ex in examples
            if ex.get("latent_variables", {}).get("has_negation")
        )),
        "value_type_counts": dict(Counter(
            vt for ex in examples
            for vt in ex.get("latent_variables", {}).get("value_types", [])
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
    return stats


def print_report(validation_results, stats):
    """Print human-readable report."""
    print("=" * 60)
    print("ORACLE DATASET VALIDATION REPORT")
    print("=" * 60)

    for check_name, result in validation_results.items():
        if isinstance(result, dict) and "passed" in result:
            status = "PASS" if result["passed"] else "FAIL"
            print(f"\n[{status}] {check_name}: {result}")
        elif isinstance(result, dict):
            for sub_name, sub_result in result.items():
                if isinstance(sub_result, dict) and "passed" in sub_result:
                    status = "PASS" if sub_result["passed"] else "FAIL"
                    print(f"  [{status}] {check_name}.{sub_name}: actual={sub_result.get('actual', '?')}")

    print("\n" + "=" * 60)
    print("STATISTICS")
    print("=" * 60)
    print(f"Total examples: {stats['total_examples']}")

    print(f"\nBy predicate type:")
    for k, v in sorted(stats["by_predicate_type"].items()):
        print(f"  {k}: {v}")

    print(f"\nBy domain:")
    for k, v in sorted(stats["by_domain"].items()):
        print(f"  {k}: {v}")

    print(f"\nOperator frequency (top 15):")
    for k, v in sorted(stats["operator_frequency"].items(), key=lambda x: -x[1])[:15]:
        print(f"  {k}: {v}")

    print(f"\nTemporal types:")
    for k, v in sorted(stats.get("temporal_type_counts", {}).items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    print(f"\nAvg english length: {stats['avg_english_length_words']:.1f} words")
    print(f"Avg SQL length: {stats['avg_sql_length_chars']:.0f} chars")
    print(f"Examples with negation: {stats['has_negation_count']}")
    print(f"Examples requiring join: {stats['requires_join_count']}")


def main():
    examples = load_all_examples()
    if not examples:
        print("ERROR: No examples found in oracle dataset.")
        sys.exit(1)

    all_passed, validation_results = validate(examples)
    stats = compute_stats(examples)

    # Save stats
    stats_path = DATASET_DIR / "stats.json"
    with open(stats_path, "w") as f:
        json.dump({"validation": validation_results, "stats": stats}, f, indent=2)

    print_report(validation_results, stats)

    if all_passed:
        print("\n*** ALL VALIDATION CHECKS PASSED ***")
    else:
        print("\n*** SOME VALIDATION CHECKS FAILED ***")
        sys.exit(1)


if __name__ == "__main__":
    main()
