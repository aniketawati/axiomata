# ProbSQL — Probabilistic SQL WHERE Clause Generator

A pure Python program that takes an English predicate sentence + a database schema and produces a SQL WHERE clause. No ML frameworks, no neural networks, no GPU required.

## Quick Start

```python
from probsql import ProbSQLEngine

engine = ProbSQLEngine()
engine.load_knowledge("probsql/knowledge/base/")

schema = {
    "tables": [
        {
            "name": "users",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "email", "type": "VARCHAR(255)"},
                {"name": "created_at", "type": "TIMESTAMP"},
                {"name": "is_active", "type": "BOOLEAN"},
                {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "inactive", "suspended"]},
                {"name": "lifetime_value", "type": "DECIMAL(10,2)"},
            ]
        }
    ]
}

result = engine.generate("active users who signed up last month", schema)
print(result.sql_where)      # users.is_active = 1 AND ...
print(result.confidence)      # 0.72
print(result.alternatives)    # alternative interpretations for low-confidence results
```

## Performance

| Metric | Result | Target |
|--------|--------|--------|
| p99 Latency | ~1.5ms | <10ms |
| Throughput | ~1,400 qps | >500 qps |
| Memory (RSS) | ~13MB | <50MB |
| Startup time | <2ms | <2s |
| Artifact size | <0.1MB | <30MB |
| Dependencies | Python stdlib only | - |

**~960x faster than LLM API calls, at zero cost per query.**

## Architecture

```
English predicate
    ↓
[Conjunction Parser]  — splits "X and Y" into tree
    ↓
[Negation Handler]    — detects "not", "without", "excluding"
    ↓
For each leaf:
  [Column Matcher]    — TF-IDF + semantic expansion → best column
  [Operator Extractor] — pattern rules → SQL operator
  [Temporal Parser]   — recursive descent → date SQL
  [Value Extractor]   — numbers, enums, strings, booleans
    ↓
[Confidence Calibrator] — isotonic regression calibration
    ↓
SQL WHERE clause + confidence score
```

## Project Structure

```
probsql/
├── engine/              # Core engine
│   ├── engine.py        # ProbSQLEngine entry point
│   ├── predicate_tree.py
│   ├── confidence.py
│   └── formatter.py
├── components/          # Reusable NLP components
│   ├── column_matcher.py
│   ├── operator_extractor.py
│   ├── temporal_parser.py
│   ├── value_extractor.py
│   ├── conjunction_parser.py
│   └── negation_handler.py
├── knowledge/           # Extracted knowledge (JSON)
│   ├── base/            # Domain-agnostic defaults
│   └── domains/         # Domain-specific overrides
├── analysis/            # Structure learning tools
│   ├── latent_analysis.py
│   └── knowledge_builder.py
├── oracle/              # Data generation pipeline
│   ├── schema_generator.py
│   ├── predicate_generator.py
│   ├── validate.py
│   ├── schemas/
│   └── dataset/
├── validation/          # Testing & benchmarking
│   ├── performance_bench.py
│   ├── boundary_test.py
│   ├── functional_test.py
│   └── comparison_test.py
└── tests/               # Unit tests (65 tests)
```

## Building Knowledge

To rebuild knowledge from the oracle dataset:

```bash
python -m probsql.analysis.knowledge_builder
```

Or with custom paths:

```bash
python -m probsql.analysis.knowledge_builder --oracle-path path/to/all_examples.json --output-dir probsql/knowledge/base/
```

## Running Tests

```bash
python probsql/tests/test_temporal_parser.py
python probsql/tests/test_operator_extractor.py
python probsql/tests/test_conjunction_parser.py
python probsql/tests/test_negation_handler.py
python probsql/tests/test_column_matcher.py
python probsql/tests/test_engine_integration.py
```

## Running Benchmarks

```bash
python probsql/validation/performance_bench.py    # Performance metrics
python probsql/validation/boundary_test.py        # Edge case testing
python probsql/validation/comparison_test.py      # vs LLM comparison
```

## Requirements

- Python 3.11+
- No external dependencies (stdlib only: json, re, math, sqlite3, dataclasses)

## Limitations

- Single-table WHERE clauses only (JOINs are flagged but not generated)
- No aggregation support (GROUP BY/HAVING)
- SQLite-compatible date functions (configurable for other dialects)
- Confidence degrades on domain jargon not seen in training data
- Temporal expressions must follow common English patterns
