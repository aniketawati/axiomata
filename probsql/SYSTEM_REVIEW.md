# System Review & Improvement Plan

## Current Architecture

```
Question + Schema
    │
    ├── [Old Path] Conjunction parser → per-leaf resolution → TF-IDF column matching
    │
    ├── [SemExtract Path] Decomposer → ValueSpotter → Bayesian ColumnResolver
    │
    └── [ProbProg Path] ← CURRENTLY WINS MOST (0.93 avg confidence)
            │
            ├── Step 1: Question type classification    P(q_type | question_words)
            ├── Step 2: Span boundary detection         P(start|left_word) × P(end|right_word)
            ├── Step 3: SELECT column identification    P(select|question_word, headers)
            ├── Step 4: Value type classification        P(v_type | value_format)
            ├── Step 5: Markov chain column resolution  Prior → Proximity → Trigger → Exclusion
            ├── Step 6: Operator selection               P(op | v_type, question_words)
            └── Step 7: SQL generation with LOWER()
    │
    Ensemble: max(confidence) across 3 paths
```

## Pipeline Step Accuracy (n=999)

| Step | Accuracy | Loss | What's Lost |
|------|----------|------|-------------|
| Span detector finds value | 68.9% | 31.1% | Can't find value in question |
| Value in final SQL | 82.9% | — | Old path recovers some |
| Column correct | 65.8% | 17.1% | Wrong column selected |
| Operator correct | 94.8% | ~0% | Nearly solved |
| Condition count correct | 72.1% | 22.7% | Too many/few conditions |
| **Execution correct** | **37.7%** | — | **Final result** |

## What Each Step Needs

### Step 2: Value Span Detection (68.9% → target 85%)

**Current model:** Rule-based boundary signals from 1000 Opus labels.
P(start | left_word) and P(end | right_word) as fixed lookup tables.

**What's missing:**
- The span detector uses word-level signals but values can START at lowercase words
  ("los angeles rams" starts at "los" — no capitalization signal)
- No schema awareness — doesn't know which column headers hint at value boundaries
- Can't handle values that span across comma boundaries ("4–6, 6–4, 6–3")

**Proposed improvement: Markov chain for span boundaries**

Instead of independent P(start) and P(end), model the value boundary detection
as a 3-state Markov chain:

```
BEFORE_VALUE → VALUE → AFTER_VALUE
```

Transition probabilities conditioned on token features:
```
P(BEFORE→VALUE | is_capitalized, follows_preposition, follows_copula)
P(VALUE→VALUE | has_special, follows_value, not_is_stop_word)
P(VALUE→AFTER | is_verb, is_question_mark, is_conjunction)
```

This captures continuity (VALUE→VALUE is high when features match) and
boundaries (VALUE→AFTER when a stop signal appears).

Extract from LLM: For 2000 examples, ask the LLM to label which features
caused the value to start and stop. Compute transition probabilities
conditioned on feature combinations (not individual features).

### Step 3: Column Resolution (65.8% → target 80%)

**Current model:** Markov chain with 4 states.
```
Prior(v_type) → Proximity(col_name near value) → Trigger(verb phrase) → Exclusion(select)
```
Weights: 0.65 proximity + 0.14 trigger + 0.12 type + 0.09 base

**What's missing:**
- Proximity dominates (0.65) but only works when column name appears in question
- For 35% of cases, column name is NOT in question — trigger and type must carry it
- Trigger rules are sparse (98 rules from LLM labels)
- No schema-level features — doesn't consider which columns are "compatible" with the value

**Proposed improvement: Richer feature conditioning for each Markov state**

State 1 (Prior): Instead of just P(col | v_type), use:
```
P(col | v_type, col_type, col_position_in_schema)
```
Extract from LLM: For each (value_type, column), ask "How likely is a [v_type]
value to belong to this column?" Get probabilities, not just binary match.

State 2 (Proximity): Instead of just "is col name in question?", use:
```
P(col | col_words_in_question, distance_to_value, position_in_question)
```
Extract from LLM: For ambiguous cases where multiple columns have words in
the question, ask "Which column name is most semantically connected to the value?"

State 3 (Trigger): Instead of sparse trigger rules, use:
```
P(col | trigger_type, col_semantic_category)
```
Where trigger_type is generalized ("played for" → "activity_at_institution")
and col_semantic_category is generalized ("School/Club Team" → "institution").
Extract from LLM: Ask for generalized trigger categories, not specific phrases.

State 4 (Exclusion): Keep current approach but also consider:
```
P(col | col ≠ select, col ≠ question_word_target)
```

### Step 5: Condition Count (72.1% → target 85%)

**Current model:** Rule-based conjunction detection (", and", "with a", etc.)

**What's missing:**
- 56% of multi-condition questions have no explicit conjunction
- The model either generates 1 condition or 3+, rarely exactly 2

**Proposed improvement: P(n_conditions | question_features)**

Features:
- question_length (longer questions tend to have more conditions)
- number_of_values_detected (from span detector)
- has_conjunction_words
- number_of_column_mentions

Extract from LLM: For 1000 questions, ask "How many WHERE conditions does
this question require?" Get the count + reasoning.

## Process for Knowledge Extraction

For each step, the process is:

1. **Define the conditional probability table** P(output | input_features)
2. **Design LLM prompt** that elicits the reasoning for this specific table
3. **Generate labeled data** at scale (1000-3000 examples via agents)
4. **Compute empirical probabilities** from labeled data
5. **Build Markov chain** that chains the tables together
6. **Benchmark** and identify remaining failures
7. **Iterate** with error-targeted labeling

## Priority Order

1. **Condition count estimation** — easiest, 28% of errors, just need P(n_conds | features)
2. **Value span improvement** — 31% of errors, needs feature-conditioned Markov chain
3. **Column resolution enrichment** — 34% of errors but partially masked by #1 and #2
