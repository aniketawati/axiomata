"""
SemExtract Knowledge Builder — Compiles LLM-extracted semantic mapping rules
into probability tables for the micro-engines.

Takes the raw semantic mappings from subagents and produces:
1. Trigger phrase → column pattern rules (for ColumnResolver)
2. Question structure → SELECT/WHERE column patterns (for QuestionDecomposer)
3. Value boundary patterns (for ValueSpotter)
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

ORACLE_DIR = Path(__file__).parent / "oracle" / "dataset"
KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


def load_semantic_mappings():
    """Load all semantic mapping files produced by subagents."""
    mappings = []
    for f in sorted(ORACLE_DIR.glob("semantic_mappings_*.json")):
        with open(f) as fh:
            data = json.load(fh)
            if isinstance(data, list):
                mappings.extend(data)
    return mappings


def compile_trigger_rules(mappings):
    """Extract trigger_phrase → column_pattern rules with confidence.

    This is the core output: generalizable rules like:
    "played for" (verb_relation) → column matching "school|team|club" (conf=0.95)
    """
    rules = []
    seen = set()

    for ex in mappings:
        for mapping in ex.get("semantic_mappings", []):
            trigger = mapping.get("trigger_phrase", "").lower().strip()
            trigger_type = mapping.get("trigger_type", "")
            col_pattern = mapping.get("column_pattern", "").lower().strip()
            confidence = mapping.get("confidence", 0.5)
            reasoning = mapping.get("reasoning", "")

            if not trigger or not col_pattern:
                continue

            key = (trigger, col_pattern)
            if key in seen:
                continue
            seen.add(key)

            rules.append({
                "trigger": trigger,
                "trigger_type": trigger_type,
                "column_pattern": col_pattern,
                "confidence": confidence,
                "reasoning": reasoning,
            })

    return rules


def compile_select_signals(mappings):
    """Extract question patterns that identify the SELECT column."""
    signals = []
    seen = set()

    for ex in mappings:
        sig = ex.get("select_signal", {})
        phrase = sig.get("signal_phrase", "").lower().strip()
        sig_type = sig.get("signal_type", "")
        select_col = ex.get("select_column", "")

        if not phrase:
            continue

        key = (phrase, select_col)
        if key in seen:
            continue
        seen.add(key)

        signals.append({
            "signal_phrase": phrase,
            "signal_type": sig_type,
            "select_column": select_col,
        })

    return signals


def compile_value_boundaries(mappings):
    """Extract value boundary patterns for the ValueSpotter."""
    boundaries = []
    seen = set()

    for ex in mappings:
        vb = ex.get("value_boundaries", {})
        left = vb.get("left_boundary", "").lower().strip()
        right = vb.get("right_boundary", "").lower().strip()
        pattern = vb.get("boundary_pattern", "").lower().strip()

        if not left and not pattern:
            continue

        key = (left, pattern)
        if key in seen:
            continue
        seen.add(key)

        boundaries.append({
            "left_boundary": left,
            "right_boundary": right,
            "boundary_pattern": pattern,
        })

    return boundaries


def build_all():
    """Build all semextract knowledge from LLM-extracted mappings."""
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    mappings = load_semantic_mappings()
    print(f"Loaded {len(mappings)} semantic mapping examples")

    if not mappings:
        print("No semantic mappings found. Run subagents first.")
        return

    # Compile trigger rules
    trigger_rules = compile_trigger_rules(mappings)
    print(f"Compiled {len(trigger_rules)} trigger rules")

    # Compile select signals
    select_signals = compile_select_signals(mappings)
    print(f"Compiled {len(select_signals)} select signals")

    # Compile value boundaries
    value_boundaries = compile_value_boundaries(mappings)
    print(f"Compiled {len(value_boundaries)} value boundary patterns")

    # Save
    knowledge = {
        "trigger_rules": trigger_rules,
        "select_signals": select_signals,
        "value_boundaries": value_boundaries,
    }

    with open(KNOWLEDGE_DIR / "semantic_rules.json", "w") as f:
        json.dump(knowledge, f, indent=2)

    # Print top rules by trigger type
    by_type = defaultdict(list)
    for r in trigger_rules:
        by_type[r["trigger_type"]].append(r)

    print(f"\nRules by trigger type:")
    for ttype, rules in sorted(by_type.items()):
        print(f"  {ttype}: {len(rules)} rules")
        for r in rules[:3]:
            print(f"    \"{r['trigger']}\" → {r['column_pattern']} ({r['confidence']:.1%})")

    return knowledge


if __name__ == "__main__":
    build_all()
