"""
Knowledge Builder — Orchestrates extraction of all knowledge artifacts
from the oracle dataset into the knowledge/ directory.

Usage:
    python -m probsql.analysis.knowledge_builder [--oracle-path PATH] [--output-dir DIR]

Builds:
    knowledge/base/column_semantic_map.json
    knowledge/base/tfidf_vectors.json
    knowledge/base/operator_rules.json
    knowledge/base/conditional_tables.json
    knowledge/base/deterministic_rules.json
    knowledge/base/uncertain_mappings.json
    knowledge/base/dependency_graph.json
"""

import argparse
from pathlib import Path

from probsql.components.column_matcher import build_knowledge_from_oracle as build_column_knowledge
from probsql.components.operator_extractor import build_knowledge_from_oracle as build_operator_knowledge
from probsql.analysis.latent_analysis import run_analysis


def build_all(oracle_path, output_dir=None):
    """Build all knowledge files from the oracle dataset."""
    oracle_path = Path(oracle_path)
    output_dir = Path(output_dir) if output_dir else oracle_path.parent.parent / "knowledge" / "base"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Building knowledge from: {oracle_path}")
    print(f"Output to: {output_dir}")
    print("=" * 60)

    print("\n1. Column matching knowledge...")
    build_column_knowledge(str(oracle_path), str(output_dir))

    print("\n2. Operator rules...")
    build_operator_knowledge(str(oracle_path), str(output_dir))

    print("\n3. Latent variable analysis...")
    run_analysis(str(oracle_path), str(output_dir))

    print("\n" + "=" * 60)
    print("Knowledge build complete.")
    print(f"Files in {output_dir}:")
    for f in sorted(output_dir.glob("*.json")):
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name}: {size_kb:.1f} KB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build knowledge from oracle dataset")
    parser.add_argument("--oracle-path", default=None, help="Path to all_examples.json")
    parser.add_argument("--output-dir", default=None, help="Output directory for knowledge files")
    args = parser.parse_args()

    oracle_path = args.oracle_path
    if not oracle_path:
        default = Path(__file__).parent.parent / "oracle" / "dataset" / "all_examples.json"
        if default.exists():
            oracle_path = str(default)
        else:
            print("ERROR: No oracle dataset found. Provide --oracle-path.")
            exit(1)

    build_all(oracle_path, args.output_dir)
