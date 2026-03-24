# Plan: Make Entire ProbProg Path Bayesian

## Current State: 4/8 components probabilistic

| Step | Component | Current | Target | Data Available |
|------|-----------|---------|--------|----------------|
| 1 | Question type classifier | RULE-BASED | P(q_type \| features) | 1366 condition labels (implicit) |
| 2 | Condition count estimator | BAYESIAN ✓ | Keep | 1366 Opus labels |
| 3 | Span boundary detector | PROBABILISTIC ✓ | Improve with features | 1000 Opus value spans |
| 4 | SELECT identifier | RULE-BASED | P(select \| question, headers) | 1500 reasoning labels |
| 5 | Value type classifier | RULE-BASED | P(v_type \| features) | 1000 value span labels |
| 6 | Markov column resolver | BAYESIAN ✓ | Richer features | 1500 reasoning + 88K WikiSQL |
| 7 | Operator selector | BAYESIAN ✓ | Keep | 76K WikiSQL |
| 8 | SQL generation | RULE-BASED | Keep (deterministic) | N/A |

## Steps to Convert

### Step 1: Question Type → Bayesian
**Can build from EXISTING condition count labels.**

Features to extract from the 1366 ncond labels:
- question_word (what/who/where/when/how)
- has_comparison_words
- has_aggregation_words (total, average, sum, count)
- has_superlative (most, least, highest, lowest)

P(q_type | features) where q_type ∈ {lookup, comparison, count, superlative}

**No new LLM calls needed** — derive from existing labels.

### Step 4: SELECT Identifier → Bayesian
**Can build from EXISTING 1500 reasoning labels.**

Each reasoning label has: select_column, select_reasoning (column_name_after_question_word 86%, question_word_implies 11%, context_implies 3%)

Build: P(header_is_SELECT | header_similarity_to_question_hint, question_word_type, header_position)

Features per header:
- word_overlap_with_question_prefix: how many words match between header and first 4 words of question
- question_word_type: what/who/where/when → hints at column semantic type
- header_semantic_type: person/location/date/number/category (from column name keywords)
- header_position_in_schema: first, middle, last

**No new LLM calls needed** — compute from existing labels.

### Step 5: Value Type → Bayesian
**Can build from EXISTING 1000 value span labels.**

Each label has: value_structure (proper_noun_sequence, number, lowercase_phrase, etc.)

Build: P(v_type | value_features) where features are:
- is_capitalized: first word starts uppercase
- all_capitalized: all words start uppercase
- is_numeric: starts with digit
- has_special: contains -/./()
- word_count: 1, 2, 3+
- char_length: short (<5), medium (5-15), long (>15)
- has_digits: contains any digits

**No new LLM calls needed** — compute from existing labels.

### Step 3: Span Detector → Improved Bayesian
**Can improve from EXISTING 1000 value span labels.**

Current: P(start | left_word) × P(end | right_word) as independent factors.

Improvement: Add CONDITIONAL probabilities:
- P(start | left_word, value_structure) — "of" + proper_noun → different from "of" + number
- P(end | right_word, value_structure) — proper nouns end differently than numbers
- P(span_length | value_structure) — numbers are typically 1 token, names are 2-3

**No new LLM calls needed** — compute from existing labels.

### Step 6: Column Resolver → Richer Features
**Needs new LLM labels for the ambiguous cases.**

Current Markov chain: Prior(v_type) → Proximity → Trigger → Exclusion

Improvement: Add a 5th state: **Schema Context**
P(col | schema_category, value_type, n_similar_columns)

When multiple columns could match, the schema context disambiguates:
- If table has "Home team" and "Away team", and value is a team name → need context to pick
- If table has "Date" and "Year", and value is "2005" → Year is better than Date

**Need 500-1000 new Opus labels** specifically for ambiguous column cases.

## Execution Plan

### Phase A: Build from existing data (no new LLM calls)
1. Build P(q_type | features) from condition count labels
2. Build P(select | features) from reasoning labels
3. Build P(v_type | value_features) from value span labels
4. Improve span detector with conditional probabilities

### Phase B: New LLM labels for column resolver
5. Sample 1000 ambiguous column cases
6. Label with Opus: "Which column and why?"
7. Build P(col | schema_context, value_type, ambiguity_features)

### Phase C: Integration and benchmark
8. Wire all Bayesian components into probprog
9. Full benchmark
10. Iterate on failures
