"""
ProbSQL Engine — Probabilistic SQL WHERE Clause Generator

Usage:
    from probsql import ProbSQLEngine

    engine = ProbSQLEngine()
    engine.load_knowledge("probsql/knowledge/base/")

    result = engine.generate(
        english="active users who signed up last month and spent more than $100",
        schema=schema_dict
    )

    print(result.sql_where)     # "users.status = 'active' AND ..."
    print(result.confidence)    # 0.87
    print(result.alternatives)  # [("...", 0.09), ...]
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from probsql.components.column_matcher import ColumnMatcher
from probsql.components.operator_extractor import OperatorExtractor
from probsql.components.temporal_parser import TemporalParser
from probsql.components.value_extractor import ValueExtractor
from probsql.components.conjunction_parser import ConjunctionParser, LeafPredicate, ConjunctionNode
from probsql.components.negation_handler import NegationHandler
from probsql.engine.predicate_tree import (
    AtomicPredicate, CompoundPredicate, NegatedPredicate,
    to_sql, to_dict, compute_confidence
)
from probsql.engine.confidence import ConfidenceCalibrator
from probsql.engine.formatter import format_sql


@dataclass
class GenerationResult:
    sql_where: str
    confidence: float
    alternatives: list = field(default_factory=list)
    predicate_tree: object = None
    debug_info: dict = field(default_factory=dict)


class ProbSQLEngine:
    def __init__(self):
        self.column_matcher = ColumnMatcher()
        self.operator_extractor = OperatorExtractor()
        self.temporal_parser = TemporalParser()
        self.value_extractor = ValueExtractor()
        self.conjunction_parser = ConjunctionParser()
        self.negation_handler = NegationHandler()
        self.confidence_calibrator = ConfidenceCalibrator()
        self._loaded = False

    def load_knowledge(self, knowledge_dir):
        """Load all extracted knowledge from JSON files."""
        kdir = Path(knowledge_dir)
        self.column_matcher.load_knowledge(kdir)
        self.operator_extractor.load_knowledge(kdir)
        self.confidence_calibrator.load(kdir)
        self._loaded = True

    def generate(self, english, schema):
        """Main entry point. English predicate + schema → SQL WHERE clause.

        Args:
            english: English predicate sentence
            schema: Schema dict with "tables" list

        Returns:
            GenerationResult
        """
        debug = {"steps": []}

        # Step 1: Parse compound structure
        predicate_tree = self.conjunction_parser.parse(english)
        debug["steps"].append({
            "step": "conjunction_parse",
            "result": str(predicate_tree),
        })

        # Step 2: Resolve each leaf into an atomic SQL predicate
        resolved_tree = self._resolve_tree(predicate_tree, schema, debug)

        # Step 3: Compute confidence
        raw_confidence = compute_confidence(resolved_tree)
        calibrated_confidence = self.confidence_calibrator.calibrate(raw_confidence)

        # Step 4: Generate alternatives for low-confidence cases
        alternatives = []
        if calibrated_confidence < 0.8:
            alternatives = self._generate_alternatives(resolved_tree, schema)

        # Step 5: Render to SQL
        sql_where = format_sql(resolved_tree)

        return GenerationResult(
            sql_where=sql_where,
            confidence=calibrated_confidence,
            alternatives=alternatives,
            predicate_tree=resolved_tree,
            debug_info=debug,
        )

    def _resolve_tree(self, tree, schema, debug):
        """Recursively resolve the parsed tree into SQL predicates."""
        if isinstance(tree, LeafPredicate):
            return self._resolve_leaf(tree.text, schema, debug)
        elif isinstance(tree, ConjunctionNode):
            left = self._resolve_tree(tree.left, schema, debug)
            right = self._resolve_tree(tree.right, schema, debug)
            return CompoundPredicate(
                conjunction=tree.conjunction,
                left=left,
                right=right,
            )
        # Fallback
        return AtomicPredicate(
            english_phrase=str(tree),
            table="",
            column="",
            operator="=",
            value=None,
            confidence=0.1,
        )

    def _resolve_leaf(self, english_phrase, schema, debug):
        """Resolve a single English phrase into an AtomicPredicate."""
        phrase = english_phrase.strip()
        if not phrase:
            return AtomicPredicate("", "", "", "=", None, 0.0)

        # Detect negation
        neg_info = self.negation_handler.detect(phrase)
        working_phrase = neg_info.cleaned_phrase if neg_info.has_negation else phrase

        # Match column
        column_candidates = self.column_matcher.match(working_phrase, schema)
        if not column_candidates:
            return AtomicPredicate(phrase, "", "", "=", None, 0.1)

        best_col = column_candidates[0]
        col_info = {
            "column_type": best_col.column_type,
            "column_name": best_col.column_name,
            "enum_values": best_col.enum_values,
        }

        # Check if temporal — only for TIMESTAMP/DATE columns, not TEXT/REAL
        col_type_upper = best_col.column_type.upper().split("(")[0]
        is_temporal_column = col_type_upper in ("TIMESTAMP", "DATE")
        if is_temporal_column and self.temporal_parser.is_temporal(working_phrase):
            temporal_result = self.temporal_parser.parse(working_phrase, best_col.column_name)
            # Temporal parser returns a complete SQL condition
            pred = AtomicPredicate(
                english_phrase=phrase,
                table=best_col.table_name,
                column=best_col.column_name,
                operator="TEMPORAL",
                value=temporal_result.sql_condition,
                confidence=best_col.score * temporal_result.confidence,
                column_match_score=best_col.score,
                alternatives=[(c.full_name, c.score) for c in column_candidates[1:3]],
            )
            debug["steps"].append({
                "step": "resolve_leaf",
                "phrase": phrase,
                "column": best_col.full_name,
                "temporal": True,
                "sql": temporal_result.sql_condition,
            })
            return pred

        # Determine operator
        operator, op_confidence, value_transform = self.operator_extractor.extract(working_phrase, col_info)

        # Extract value
        value, value_type, val_confidence = self.value_extractor.extract(
            working_phrase, col_info, operator
        )

        # Apply negation
        if neg_info.has_negation:
            operator, value = self.negation_handler.apply_negation(operator, value, neg_info)

        # Apply value transforms (LIKE wildcards)
        if value_transform and operator in ("LIKE", "NOT LIKE") and isinstance(value, str):
            if value_transform == "prefix_wildcard":
                value = f"{value}%"
            elif value_transform == "suffix_wildcard":
                value = f"%{value}"
            elif value_transform == "contains_wildcard":
                value = f"%{value}%"

        # Compute combined confidence
        confidence = best_col.score * op_confidence * val_confidence

        pred = AtomicPredicate(
            english_phrase=phrase,
            table=best_col.table_name,
            column=best_col.column_name,
            operator=operator,
            value=value,
            confidence=confidence,
            column_match_score=best_col.score,
            alternatives=[(c.full_name, c.score) for c in column_candidates[1:3]],
        )

        debug["steps"].append({
            "step": "resolve_leaf",
            "phrase": phrase,
            "column": best_col.full_name,
            "operator": operator,
            "value": str(value),
            "confidence": confidence,
            "negation": neg_info.has_negation,
        })

        return pred

    def _generate_alternatives(self, tree, schema):
        """Generate alternative SQL interpretations for low-confidence results."""
        alternatives = []

        if isinstance(tree, AtomicPredicate) and tree.alternatives:
            for alt_name, alt_score in tree.alternatives[:2]:
                parts = alt_name.split(".")
                if len(parts) == 2:
                    alt_pred = AtomicPredicate(
                        english_phrase=tree.english_phrase,
                        table=parts[0],
                        column=parts[1],
                        operator=tree.operator,
                        value=tree.value,
                        confidence=alt_score,
                    )
                    alt_sql = format_sql(alt_pred)
                    alternatives.append((alt_sql, alt_score))

        return alternatives


# Override to_sql for TEMPORAL operator
_original_to_sql = to_sql

def _patched_to_sql(node):
    """Extended to_sql that handles TEMPORAL operator."""
    if isinstance(node, AtomicPredicate) and node.operator == "TEMPORAL":
        return str(node.value) if node.value else "1=1"
    return _original_to_sql(node)

# Monkey-patch
import probsql.engine.predicate_tree as pt
pt.to_sql = _patched_to_sql


if __name__ == "__main__":
    # Quick demo
    engine = ProbSQLEngine()

    # Try to load knowledge
    knowledge_dir = Path(__file__).parent.parent / "knowledge" / "base"
    if knowledge_dir.exists():
        engine.load_knowledge(str(knowledge_dir))

    # Demo schema
    demo_schema = {
        "tables": [
            {
                "name": "users",
                "columns": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "email", "type": "VARCHAR(255)"},
                    {"name": "name", "type": "VARCHAR(100)"},
                    {"name": "created_at", "type": "TIMESTAMP"},
                    {"name": "is_active", "type": "BOOLEAN"},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "inactive", "suspended"]},
                    {"name": "lifetime_value", "type": "DECIMAL(10,2)"},
                ]
            },
            {
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "user_id", "type": "INT"},
                    {"name": "total_amount", "type": "DECIMAL(10,2)"},
                    {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "shipped", "delivered", "cancelled"]},
                    {"name": "created_at", "type": "TIMESTAMP"},
                ]
            }
        ]
    }

    test_phrases = [
        "active users",
        "orders over $100",
        "users who signed up last month",
        "cancelled orders",
        "users who haven't verified their email",
        "expensive orders from this year",
    ]

    for phrase in test_phrases:
        result = engine.generate(phrase, demo_schema)
        print(f"\n  English: {phrase}")
        print(f"  SQL:     {result.sql_where}")
        print(f"  Conf:    {result.confidence:.2f}")
        if result.alternatives:
            print(f"  Alts:    {result.alternatives}")
