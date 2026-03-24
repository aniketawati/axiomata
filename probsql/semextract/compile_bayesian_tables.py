"""
Compile LLM-labeled reasoning data + empirical WikiSQL frequencies
into calibrated Bayesian probability tables for the resolver chain.

Combines:
- Mechanical frequencies from compute_probabilities.py (76K examples)
- LLM reasoning labels from subagents (500 examples)
→ Proper conditional probability tables for each Bayesian factor
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"
ORACLE_DIR = Path(__file__).parent / "oracle" / "dataset"


def load_reasoning_labels():
    """Load all LLM-labeled reasoning data."""
    labels = []
    for pattern in ["reasoning_labeled_*.json", "r6_labeled_*.json"]:
        for f in sorted(ORACLE_DIR.glob(pattern)):
            with open(f) as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    labels.extend(data)
    return labels


def compile_tables():
    """Compile all probability tables from labeled data + mechanical frequencies."""
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    # Load mechanical probabilities (from compute_probabilities.py)
    mech_path = KNOWLEDGE_DIR / "bayesian_tables.json"
    if mech_path.exists():
        with open(mech_path) as f:
            mechanical = json.load(f)
    else:
        mechanical = {}

    # Load LLM reasoning labels
    labels = load_reasoning_labels()
    print(f"Loaded {len(labels)} LLM-labeled examples")

    if not labels:
        print("No labeled data found. Run labeling subagents first.")
        return

    # === Table 1: P(why_this_column) — distribution of reasoning types ===
    why_counts = Counter(ex.get("why_this_column", "unknown") for ex in labels)
    total = sum(why_counts.values())
    p_why = {k: round(v / total, 4) for k, v in why_counts.most_common()}

    # === Table 2: P(column_keyword | trigger_phrase) from LLM labels ===
    # More precise than mechanical because LLM identified the actual trigger
    trigger_to_colkw = defaultdict(Counter)
    trigger_counts = Counter()
    for ex in labels:
        trigger = ex.get("trigger_phrase")
        col_kw = ex.get("column_keyword", "")
        if trigger and col_kw and len(trigger) > 3:
            # Normalize trigger
            trigger_norm = trigger.lower().strip()
            trigger_to_colkw[trigger_norm][col_kw.lower()] += 1
            trigger_counts[trigger_norm] += 1

    p_colkw_trigger = {}
    for trigger, kw_counts in trigger_to_colkw.items():
        total_t = sum(kw_counts.values())
        if total_t >= 2:  # need at least 2 examples
            p_colkw_trigger[trigger] = {
                "distribution": {kw: round(c / total_t, 4) for kw, c in kw_counts.most_common(5)},
                "count": trigger_counts[trigger],
            }

    # === Table 3: P(select_reasoning) — how SELECT column is identified ===
    select_reason_counts = Counter(ex.get("select_reasoning", "unknown") for ex in labels)
    total_s = sum(select_reason_counts.values())
    p_select_reasoning = {k: round(v / total_s, 4) for k, v in select_reason_counts.most_common()}

    # === Table 4: P(column_keyword | why_this_column) ===
    # When reasoning is "value_is_entity_name", which column keywords appear?
    why_to_colkw = defaultdict(Counter)
    for ex in labels:
        why = ex.get("why_this_column", "")
        col_kw = ex.get("column_keyword", "")
        if why and col_kw:
            why_to_colkw[why][col_kw.lower()] += 1

    p_colkw_why = {}
    for why, kw_counts in why_to_colkw.items():
        total_w = sum(kw_counts.values())
        p_colkw_why[why] = {
            "distribution": {kw: round(c / total_w, 4) for kw, c in kw_counts.most_common(10)},
            "count": total_w,
        }

    # === Table 5: Merged trigger rules (LLM + mechanical) ===
    # Combine LLM-identified triggers with mechanical frequency triggers
    all_triggers = {}

    # From LLM labels (higher quality, lower quantity)
    for trigger, data in p_colkw_trigger.items():
        top_kw = list(data["distribution"].keys())[0] if data["distribution"] else ""
        top_prob = list(data["distribution"].values())[0] if data["distribution"] else 0
        all_triggers[trigger] = {
            "top_column_keyword": top_kw,
            "probability": top_prob,
            "source": "llm",
            "count": data["count"],
            "all_keywords": data["distribution"],
        }

    # From mechanical (lower quality, higher quantity)
    mech_triggers = mechanical.get("P_colkw_given_trigger", {})
    for trigger, data in mech_triggers.items():
        if trigger not in all_triggers:
            dist = data.get("distribution", {})
            top_kw = list(dist.keys())[0] if dist else ""
            top_prob = list(dist.values())[0] if dist else 0
            all_triggers[trigger] = {
                "top_column_keyword": top_kw,
                "probability": top_prob,
                "source": "mechanical",
                "count": data.get("total_examples", 0),
                "all_keywords": dist,
            }

    # === Assemble final tables ===
    bayesian_compiled = {
        "P_reasoning_type": p_why,
        "P_colkw_given_trigger": all_triggers,
        "P_select_reasoning": p_select_reasoning,
        "P_colkw_given_reasoning_type": p_colkw_why,
        # Include mechanical tables as-is
        "P_colkw_given_valuetype": mechanical.get("P_colkw_given_valuetype", {}),
        "P_role_given_mentioned": mechanical.get("P_role_given_mentioned", {}),
        "P_select_colkw_given_qword": mechanical.get("P_select_colkw_given_qword", {}),
        "P_operator_given_valuetype": mechanical.get("P_operator_given_valuetype", {}),
    }

    with open(KNOWLEDGE_DIR / "bayesian_compiled.json", "w") as f:
        json.dump(bayesian_compiled, f, indent=2)

    # Print summary
    print(f"\n{'='*60}")
    print("COMPILED BAYESIAN TABLES")
    print(f"{'='*60}")

    print(f"\nReasoning type distribution (n={len(labels)}):")
    for k, v in p_why.items():
        print(f"  {k}: {v:.0%}")

    print(f"\nTrigger → column keyword ({len(all_triggers)} triggers):")
    for trigger, data in sorted(all_triggers.items(), key=lambda x: -x[1]["count"])[:10]:
        print(f"  '{trigger}' → {data['top_column_keyword']} ({data['probability']:.0%}, n={data['count']}, src={data['source']})")

    print(f"\nSELECT reasoning distribution:")
    for k, v in p_select_reasoning.items():
        print(f"  {k}: {v:.0%}")

    print(f"\nColumn keywords by reasoning type:")
    for why, data in p_colkw_why.items():
        top3 = list(data["distribution"].items())[:3]
        print(f"  {why} (n={data['count']}): {', '.join(f'{k}={v:.0%}' for k,v in top3)}")

    return bayesian_compiled


if __name__ == "__main__":
    compile_tables()
