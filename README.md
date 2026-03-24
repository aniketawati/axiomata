# Axiomata — Extracting LLM Intelligence into Domain-Specific Probabilistic Programs

A research project exploring systematic methods for distilling Large Language Model knowledge into small, fast, interpretable probabilistic engines that run with zero ML dependencies.

## Core Thesis

LLMs encode structured reasoning knowledge that can be extracted into composable probabilistic programs through:

1. **LLM as Reasoning Oracle** — structured prompts that extract not just answers but the reasoning process (features, categories, conditional dependencies)
2. **Empirical Probability Computation** — from labeled data to conditional probability tables P(Y|X)
3. **Probabilistic Program Assembly** — chains of Bayesian classifiers and Markov models that mirror the LLM's reasoning decomposition
4. **Benchmark-Driven Iteration** — error categorization → targeted re-labeling → recompilation

The resulting programs are 960x faster than LLM API calls, fully interpretable, and run on Python stdlib alone.

## Case Study: ProbSQL (Text-to-SQL)

The first domain-specific engine built with this methodology. Takes an English question + database schema and produces a SQL WHERE clause.

**Results on WikiSQL benchmark (8,357 examples):**
- 37.6% execution accuracy
- 1.5ms p99 latency (vs ~1,440ms for LLM API)
- Zero external dependencies
- 13MB memory, 0.5MB artifact size

Built through 18 rounds of iterative improvement with ~265,000 LLM-labeled examples across 11 prompt types.

See [probsql/README.md](probsql/README.md) for usage, architecture, and implementation details.

## Research Documentation

- **[Whitepaper](probsql/WHITEPAPER.md)** — Full methodology, results, and 6 key empirical findings
- **[System Review](probsql/SYSTEM_REVIEW.md)** — Pipeline step accuracy breakdown and improvement priorities
- **[ProbProg Plan](probsql/PROBPROG_PLAN.md)** — Plan for fully Bayesian probabilistic program

## Architecture Overview

```
LLM (Claude)
    ↓ Structured prompts extracting reasoning process
Labeled Data (~265K examples, 11 prompt types)
    ↓ Empirical probability computation
Conditional Probability Tables (JSON)
    ↓ Compositional assembly
Probabilistic Program
    ├── Bayesian classifiers (question type, value type, operator)
    ├── Markov chains (5-state column resolver)
    ├── Boundary models (value span detection)
    ├── Compatibility tables ("probabilistic attention")
    └── Entity knowledge base (5,990 entities + 16,601 column types)
    ↓
Fast, interpretable inference (1.5ms, zero dependencies)
```

## Project Structure

```
axiomata/
├── probsql/                    # Text-to-SQL probabilistic engine
│   ├── engine/                 # Core engine (predicate tree, SQL generation)
│   ├── components/             # NLP components (column matcher, temporal parser, etc.)
│   ├── semextract/             # Semantic extraction micro-engines
│   │   ├── probprog.py         # Compositional probabilistic program (7/8 Bayesian)
│   │   ├── entity_resolver.py  # World knowledge + compatibility table
│   │   ├── span_detector.py    # Value boundary detection
│   │   ├── condition_estimator.py
│   │   ├── oracle/             # LLM labeling prompts and datasets
│   │   └── knowledge/          # Learned probability tables
│   ├── knowledge/              # Base knowledge (TF-IDF, operator rules, entity types)
│   ├── oracle/                 # Oracle data generation pipeline
│   ├── validation/             # WikiSQL benchmark harness
│   ├── tests/                  # Unit tests (65 tests)
│   ├── WHITEPAPER.md           # Research whitepaper
│   └── README.md               # ProbSQL-specific documentation
└── README.md                   # This file
```

## Requirements

- Python 3.11+
- No external dependencies (stdlib only: json, re, math, sqlite3, dataclasses)

## Key Findings

1. **Structural priors beat learned word frequencies** — rule-based features (capitalization, number patterns) outperform HMM emissions trained on 3,000 LLM annotations
2. **Empirical Bayesian weights from LLM labels outperform hand-tuning** — the LLM's self-reported reasoning distribution is the best weighting scheme
3. **Extract the reasoning process, not just answers** — prompt for WHY the LLM chose an answer, and those categories become the probabilistic program's structure
4. **World knowledge must be Bayesian updates, not replacements** — entity knowledge should modulate probabilities, not override them
5. **Joint models only help when both sides are accurate** — sequential pipelines are more robust than joint optimization when upstream components have errors
6. **Naive max-confidence beats isotonic calibration for ensembles** — per-example confidence contains more signal than per-model calibration
