"""
Distribution Prober — Probes ambiguity in English->SQL mapping.

Usage: This script's logic is executed via subagents from the main conversation.

For a subset of 500 examples, asks a subagent to generate MULTIPLE possible
SQL interpretations, revealing the ambiguity structure.

Outputs to phase1_oracle/distribution_probes/all_probes.json
"""

import json
import random
from collections import Counter
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATASET_DIR = SCRIPT_DIR / "dataset"
PROBES_DIR = SCRIPT_DIR / "probes"
SCHEMAS_DIR = SCRIPT_DIR / "schemas"

PROBE_PROMPT_TEMPLATE = """You are analyzing ambiguity in English-to-SQL translation.

## Schema
{schema_json}

## English Predicate
"{english_predicate}"

## Task
Provide your top 3 possible SQL WHERE clause interpretations of this English predicate,
ranked by likelihood. For each interpretation, explain what assumption makes it different.

Return ONLY a JSON array of 3 objects, each with:
- "sql_where": the SQL WHERE clause
- "probability": your estimated probability this is the intended meaning (all 3 should sum to ~1.0)
- "assumption": one sentence explaining the interpretation choice
- "differs_in": what specifically differs from the top interpretation ("column_choice", "operator", "value", "temporal_resolution", "scope", "join_strategy")

No markdown, no preamble, just the JSON array.
"""


def sample_examples(n=500, seed=42):
    """Sample n examples stratified by predicate_type."""
    all_path = DATASET_DIR / "all_examples.json"
    if not all_path.exists():
        # Try loading from individual files
        examples = []
        for f in sorted(DATASET_DIR.glob("*.json")):
            if f.name in ("all_examples.json", "stats.json"):
                continue
            with open(f) as fh:
                data = json.load(fh)
                if isinstance(data, dict) and "examples" in data:
                    for ex in data["examples"]:
                        ex["domain"] = data.get("domain", "unknown")
                        ex["schema_id"] = data.get("schema_id", f.stem)
                    examples.extend(data["examples"])
        return _stratified_sample(examples, n, seed)

    with open(all_path) as f:
        examples = json.load(f)
    return _stratified_sample(examples, n, seed)


def _stratified_sample(examples, n, seed):
    """Stratified sample by predicate_type."""
    rng = random.Random(seed)
    by_type = {}
    for ex in examples:
        ptype = ex.get("latent_variables", {}).get("predicate_type", "unknown")
        by_type.setdefault(ptype, []).append(ex)

    sampled = []
    per_type = n // len(by_type) if by_type else 0
    remainder = n - per_type * len(by_type)

    for ptype, exs in sorted(by_type.items()):
        count = min(per_type, len(exs))
        sampled.extend(rng.sample(exs, count))

    # Fill remainder from largest groups
    remaining_pool = [ex for ex in examples if ex not in sampled]
    if remaining_pool and remainder > 0:
        sampled.extend(rng.sample(remaining_pool, min(remainder, len(remaining_pool))))

    return sampled[:n]


def build_probe_prompt(example):
    """Build the probe prompt for a single example."""
    schema_id = example.get("schema_id", "")
    schema_path = SCHEMAS_DIR / f"{schema_id}.json"
    if schema_path.exists():
        with open(schema_path) as f:
            schema_json = f.read()
    else:
        schema_json = "{}"

    return PROBE_PROMPT_TEMPLATE.format(
        schema_json=schema_json,
        english_predicate=example.get("english", ""),
    )


def save_probes(probes):
    """Save all probe results."""
    PROBES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROBES_DIR / "all_probes.json"
    with open(out_path, "w") as f:
        json.dump(probes, f, indent=2)
    print(f"Saved {len(probes)} probes to {out_path}")


def analyze_probes(probes):
    """Analyze probe results for ambiguity patterns."""
    high_confidence = 0  # top interpretation > 0.95
    medium_confidence = 0  # top interpretation 0.7-0.95
    ambiguous = 0  # top interpretation < 0.7

    differs_in_counts = Counter()

    for probe in probes:
        interpretations = probe.get("interpretations", [])
        if not interpretations:
            continue
        top_prob = interpretations[0].get("probability", 0)
        if top_prob > 0.95:
            high_confidence += 1
        elif top_prob > 0.7:
            medium_confidence += 1
        else:
            ambiguous += 1

        for interp in interpretations[1:]:
            differs_in_counts[interp.get("differs_in", "unknown")] += 1

    total = len(probes)
    print(f"\n=== Ambiguity Analysis ===")
    print(f"Total probes: {total}")
    print(f"High confidence (>0.95): {high_confidence} ({high_confidence/max(total,1)*100:.0f}%)")
    print(f"Medium confidence (0.7-0.95): {medium_confidence} ({medium_confidence/max(total,1)*100:.0f}%)")
    print(f"Ambiguous (<0.7): {ambiguous} ({ambiguous/max(total,1)*100:.0f}%)")
    print(f"\nDifference categories:")
    for k, v in differs_in_counts.most_common():
        print(f"  {k}: {v}")

    return {
        "total": total,
        "high_confidence": high_confidence,
        "medium_confidence": medium_confidence,
        "ambiguous": ambiguous,
        "differs_in_distribution": dict(differs_in_counts),
    }


if __name__ == "__main__":
    sampled = sample_examples(500)
    print(f"Sampled {len(sampled)} examples for probing")
    print("Run distribution probing via subagents in the main conversation.")
