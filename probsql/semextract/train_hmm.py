"""
Train HMM parameters from LLM-annotated token role data.

Computes:
  - Transition probabilities: P(role_t | role_{t-1})
  - Emission probabilities: P(word | role)
  - Start probabilities: P(role_0)

Saves to probsql/semextract/knowledge/hmm_params.json
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

ORACLE_DIR = Path(__file__).parent / "oracle" / "dataset"
KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"

ROLES = ["QWORD", "SELECT_HINT", "FILLER", "TRIGGER", "VALUE", "CONTEXT"]


def load_annotations():
    """Load all HMM token annotations from labeled files."""
    annotations = []
    for f in sorted(ORACLE_DIR.glob("hmm_labeled_*.json")):
        with open(f) as fh:
            data = json.load(fh)
            if isinstance(data, list):
                annotations.extend(data)
    return annotations


def train(annotations, min_emit_count=2):
    """Train HMM parameters from annotated data.

    Args:
        annotations: List of {"question": str, "tokens": [{"token": str, "role": str}]}
        min_emit_count: Minimum count to include a word in emission table

    Returns:
        dict with transition, emission, start probabilities
    """
    # Count transitions
    trans_counts = defaultdict(Counter)  # from_role → {to_role: count}
    start_counts = Counter()  # role → count for first token
    emit_counts = defaultdict(Counter)  # role → {word: count}
    role_counts = Counter()  # total tokens per role

    n_sequences = 0
    n_tokens = 0

    for ann in annotations:
        tokens = ann.get("tokens", [])
        if not tokens:
            continue

        n_sequences += 1
        prev_role = None

        for i, tok in enumerate(tokens):
            word = tok.get("token", "").lower()
            role = tok.get("role", "FILLER")

            # Normalize role
            if role not in ROLES:
                role = "FILLER"

            if i == 0:
                start_counts[role] += 1
            if prev_role is not None:
                trans_counts[prev_role][role] += 1

            emit_counts[role][word] += 1
            role_counts[role] += 1
            prev_role = role
            n_tokens += 1

    # Compute probabilities with Laplace smoothing
    n_roles = len(ROLES)

    # Start probabilities
    total_starts = sum(start_counts.values()) + n_roles
    start_prob = {r: (start_counts[r] + 1) / total_starts for r in ROLES}

    # Transition probabilities
    transition = {}
    for from_role in ROLES:
        total = sum(trans_counts[from_role].values()) + n_roles
        transition[from_role] = {
            to_role: (trans_counts[from_role][to_role] + 1) / total
            for to_role in ROLES
        }

    # Emission probabilities (keep top words per role, use floor for unseen)
    emission = {}
    for role in ROLES:
        total = sum(emit_counts[role].values())
        if total == 0:
            continue
        # Keep words with count >= min_emit_count
        role_emit = {}
        for word, count in emit_counts[role].items():
            if count >= min_emit_count:
                role_emit[word] = count / total
        emission[role] = role_emit

    params = {
        "transition": transition,
        "emission": emission,
        "start": start_prob,
        "stats": {
            "n_sequences": n_sequences,
            "n_tokens": n_tokens,
            "role_distribution": {r: role_counts[r] for r in ROLES},
            "vocab_per_role": {r: len(emission.get(r, {})) for r in ROLES},
        },
    }

    return params


def build_and_save():
    """Load annotations, train HMM, save parameters."""
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    annotations = load_annotations()
    print(f"Loaded {len(annotations)} annotated sequences")

    if not annotations:
        print("No annotations found. Run HMM labeling agents first.")
        return

    params = train(annotations)

    with open(KNOWLEDGE_DIR / "hmm_params.json", "w") as f:
        json.dump(params, f, indent=2)

    stats = params["stats"]
    print(f"\nHMM Training Results:")
    print(f"  Sequences: {stats['n_sequences']}")
    print(f"  Tokens: {stats['n_tokens']}")
    print(f"\n  Role distribution:")
    for role in ROLES:
        count = stats["role_distribution"].get(role, 0)
        vocab = stats["vocab_per_role"].get(role, 0)
        print(f"    {role}: {count} tokens, {vocab} unique words")

    print(f"\n  Start probabilities:")
    for role in ROLES:
        print(f"    {role}: {params['start'][role]:.3f}")

    print(f"\n  Top transitions:")
    for from_role in ROLES:
        top = sorted(params["transition"][from_role].items(), key=lambda x: -x[1])[:3]
        top_str = ", ".join(f"{r}={p:.2f}" for r, p in top)
        print(f"    {from_role} → {top_str}")

    print(f"\n  Top emissions per role:")
    for role in ROLES:
        if role in params["emission"]:
            top = sorted(params["emission"][role].items(), key=lambda x: -x[1])[:5]
            top_str = ", ".join(f"{w}={p:.3f}" for w, p in top)
            print(f"    {role}: {top_str}")

    return params


if __name__ == "__main__":
    build_and_save()
