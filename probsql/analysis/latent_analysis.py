"""
Latent Variable Analysis — Discovers the dependency structure of the
probabilistic program from oracle dataset labels.

Computes conditional frequency tables, identifies deterministic rules
and uncertain mappings, and computes mutual information between variables.
"""

import json
import math
from collections import Counter, defaultdict
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent.parent / "knowledge" / "base"


def load_examples(oracle_path):
    """Load all examples from oracle dataset."""
    with open(oracle_path) as f:
        data = json.load(f)
    return data if isinstance(data, list) else data.get("examples", [])


def compute_conditional_tables(examples):
    """Compute conditional frequency tables P(Y|X)."""
    tables = {}

    # P(operator | predicate_type)
    tables["operator_given_type"] = _conditional_freq(
        examples,
        x_fn=lambda ex: ex.get("latent_variables", {}).get("predicate_type", "unknown"),
        y_fn=lambda ex: ex.get("latent_variables", {}).get("operators_used", []),
        y_is_list=True,
    )

    # P(conjunction_type | predicate_type)
    tables["conjunction_given_type"] = _conditional_freq(
        examples,
        x_fn=lambda ex: ex.get("latent_variables", {}).get("predicate_type", "unknown"),
        y_fn=lambda ex: [ex.get("latent_variables", {}).get("conjunction_type", "none")],
        y_is_list=True,
    )

    # P(has_temporal | domain)
    tables["temporal_given_domain"] = _conditional_freq(
        examples,
        x_fn=lambda ex: ex.get("domain", "unknown"),
        y_fn=lambda ex: [str(ex.get("latent_variables", {}).get("has_temporal", False))],
        y_is_list=True,
    )

    # P(has_negation | predicate_type)
    tables["negation_given_type"] = _conditional_freq(
        examples,
        x_fn=lambda ex: ex.get("latent_variables", {}).get("predicate_type", "unknown"),
        y_fn=lambda ex: [str(ex.get("latent_variables", {}).get("has_negation", False))],
        y_is_list=True,
    )

    # P(requires_join | predicate_type)
    tables["join_given_type"] = _conditional_freq(
        examples,
        x_fn=lambda ex: ex.get("latent_variables", {}).get("predicate_type", "unknown"),
        y_fn=lambda ex: [str(ex.get("requires_join", False))],
        y_is_list=True,
    )

    # P(temporal_type | has_temporal=True)
    temporal_examples = [ex for ex in examples if ex.get("latent_variables", {}).get("has_temporal")]
    if temporal_examples:
        tables["temporal_type_dist"] = _marginal_freq(
            temporal_examples,
            fn=lambda ex: ex.get("latent_variables", {}).get("temporal_type", "unknown"),
        )

    return tables


def _conditional_freq(examples, x_fn, y_fn, y_is_list=False):
    """Compute P(Y|X) as a dict of {x: {y: probability, ..., "_count": n}}."""
    joint = defaultdict(Counter)
    x_counts = Counter()

    for ex in examples:
        x = x_fn(ex)
        ys = y_fn(ex)
        if not y_is_list:
            ys = [ys]
        x_counts[x] += 1
        for y in ys:
            joint[x][y] += 1

    table = {}
    for x, y_counts in joint.items():
        total = sum(y_counts.values())
        table[x] = {y: count / total for y, count in y_counts.items()}
        table[x]["_count"] = x_counts[x]
        table[x]["_total_observations"] = total

    return table


def _marginal_freq(examples, fn):
    """Compute marginal frequency distribution."""
    counts = Counter(fn(ex) for ex in examples)
    total = sum(counts.values())
    return {k: {"probability": v / total, "count": v} for k, v in counts.items()}


def identify_rules(conditional_tables):
    """Identify deterministic rules where P(Y|X) > 0.97."""
    rules = []
    for table_name, table in conditional_tables.items():
        for x, y_dist in table.items():
            for y, prob in y_dist.items():
                if y is None or (isinstance(y, str) and y.startswith("_")):
                    continue
                if isinstance(prob, (int, float)) and prob > 0.97:
                    count = y_dist.get("_count", 0)
                    if count >= 10:
                        rules.append({
                            "table": table_name,
                            "condition": x,
                            "result": y,
                            "probability": prob,
                            "sample_count": count,
                        })
    return rules


def identify_uncertain(conditional_tables):
    """Identify uncertain mappings where no Y has P(Y|X) > 0.7."""
    uncertain = []
    for table_name, table in conditional_tables.items():
        for x, y_dist in table.items():
            probs = {y: p for y, p in y_dist.items() if y is not None and not (isinstance(y, str) and y.startswith("_")) and isinstance(p, (int, float))}
            if probs:
                max_prob = max(probs.values())
                if max_prob < 0.7:
                    uncertain.append({
                        "table": table_name,
                        "condition": x,
                        "max_probability": max_prob,
                        "distribution": probs,
                        "sample_count": y_dist.get("_count", 0),
                    })
    return uncertain


def compute_mutual_information(examples):
    """Compute mutual information between pairs of latent variables."""
    variables = {
        "predicate_type": lambda ex: ex.get("latent_variables", {}).get("predicate_type", "unknown"),
        "conjunction_type": lambda ex: ex.get("latent_variables", {}).get("conjunction_type", "unknown"),
        "has_temporal": lambda ex: str(ex.get("latent_variables", {}).get("has_temporal", False)),
        "has_negation": lambda ex: str(ex.get("latent_variables", {}).get("has_negation", False)),
        "requires_join": lambda ex: str(ex.get("requires_join", False)),
    }

    mi_scores = {}
    var_names = list(variables.keys())

    for i in range(len(var_names)):
        for j in range(i + 1, len(var_names)):
            v1_name = var_names[i]
            v2_name = var_names[j]
            v1_fn = variables[v1_name]
            v2_fn = variables[v2_name]

            mi = _mutual_information(examples, v1_fn, v2_fn)
            mi_scores[f"{v1_name} <-> {v2_name}"] = mi

    return dict(sorted(mi_scores.items(), key=lambda x: -x[1]))


def _mutual_information(examples, fn1, fn2):
    """Compute MI between two discrete variables."""
    n = len(examples)
    if n == 0:
        return 0.0

    joint = Counter()
    marginal1 = Counter()
    marginal2 = Counter()

    for ex in examples:
        v1 = fn1(ex)
        v2 = fn2(ex)
        joint[(v1, v2)] += 1
        marginal1[v1] += 1
        marginal2[v2] += 1

    mi = 0.0
    for (v1, v2), count in joint.items():
        p_joint = count / n
        p1 = marginal1[v1] / n
        p2 = marginal2[v2] / n
        if p_joint > 0 and p1 > 0 and p2 > 0:
            mi += p_joint * math.log2(p_joint / (p1 * p2))

    return mi


def run_analysis(oracle_path, output_dir=None):
    """Run full latent variable analysis."""
    output_dir = Path(output_dir) if output_dir else KNOWLEDGE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    examples = load_examples(oracle_path)
    print(f"Loaded {len(examples)} examples")

    # Compute conditional tables
    tables = compute_conditional_tables(examples)
    with open(output_dir / "conditional_tables.json", "w") as f:
        json.dump(tables, f, indent=2)
    print(f"Conditional tables computed: {len(tables)} tables")

    # Identify deterministic rules
    rules = identify_rules(tables)
    with open(output_dir / "deterministic_rules.json", "w") as f:
        json.dump(rules, f, indent=2)
    print(f"Deterministic rules found: {len(rules)}")

    # Identify uncertain mappings
    uncertain = identify_uncertain(tables)
    with open(output_dir / "uncertain_mappings.json", "w") as f:
        json.dump(uncertain, f, indent=2)
    print(f"Uncertain mappings found: {len(uncertain)}")

    # Compute mutual information
    mi_scores = compute_mutual_information(examples)
    with open(output_dir / "dependency_graph.json", "w") as f:
        json.dump(mi_scores, f, indent=2)
    print(f"\nMutual Information (variable pairs):")
    for pair, mi in list(mi_scores.items())[:10]:
        print(f"  {pair}: {mi:.4f}")

    # Print summary
    print(f"\n=== Key Findings ===")
    if rules:
        print(f"Top deterministic rules:")
        for r in rules[:5]:
            print(f"  P({r['result']} | {r['condition']}) = {r['probability']:.3f} (n={r['sample_count']})")
    if uncertain:
        print(f"Most uncertain mappings:")
        for u in uncertain[:5]:
            print(f"  {u['table']}: {u['condition']} → max_p={u['max_probability']:.3f}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        run_analysis(sys.argv[1])
    else:
        # Try default path
        default = Path(__file__).parent.parent / "oracle" / "dataset" / "all_examples.json"
        if default.exists():
            run_analysis(str(default))
        else:
            print("Usage: python latent_analysis.py <oracle_dataset_path>")
