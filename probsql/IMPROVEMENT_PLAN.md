# Improvement Plan: From 34.9% to Target 50%+ Execution Accuracy

## Current State (Full WikiSQL Dev, n=8,357)
- Execution accuracy: 34.9%
- Column accuracy: 48.0%
- Value found in SQL: ~58%
- Latency: 1.24ms, zero dependencies

## Root Cause Analysis (random sample n=1000)
1. **Value not extracted (40% miss)**: The engine can't find the WHERE value in the question
2. **Wrong column (45% miss)**: Even when value is found, wrong column is picked
3. **"name" questions worst (34%)**: "Name the X where Y is Z" pattern not handled

## Three Improvement Tracks

### Track 1: Opus-Quality Question Template Library
**What**: Have Opus generate a comprehensive library of question structural templates with slot extraction rules.
**Why**: Templates directly address both value extraction AND column matching — the template tells you WHERE the value is and WHICH column it maps to.
**Scale**: 200 Opus-generated templates (vs our current ~10 regex patterns)
**How**:
- Give Opus 50 diverse WikiSQL questions
- Ask: "Group these by structural pattern. For each pattern, write a regex template with named groups for SELECT_COL, WHERE_COL, WHERE_VALUE"
- Ask for 200 total templates covering all question structures
- Compile templates into a priority-ordered matching engine

### Track 2: Opus-Quality Value Boundary Rules
**What**: Have Opus extract precise rules for where values start and end in questions.
**Why**: Current value spotter misses 40% of values — fragments dates, misses lowercase proper nouns, can't handle "Name the X" pattern.
**Scale**: 500 Opus-labeled examples focused specifically on value boundary annotation
**How**:
- Give Opus examples where our engine fails
- Ask: "For each question, mark the exact character span of the WHERE value. Explain the rule for identifying these boundaries."
- Extract generalizable boundary rules (not word-level HMM, but span-level patterns)
- Example rules: "After 'named/called/titled' → next proper noun sequence is the value"
  "After trigger verb + preposition → value starts at next content word"
  "Numbers at end of question after comparison words → value"

### Track 3: Schema-Aware Column Resolution at Scale
**What**: Have Opus label 2000 examples with detailed column resolution reasoning, specifically for the 45% of cases where column matching fails.
**Why**: Current resolver uses generic type→column mappings. Opus can provide schema-specific reasoning.
**Scale**: 2000 Opus-labeled examples (double current), focused on failures
**How**:
- Sample 1000 current failures + 1000 successes
- Ask Opus: "Given this question and these columns, explain step by step how you determine which column the value belongs to. Which column words match which question words?"
- Extract: P(column_word | question_context_word) — not just P(column | value_type)
- This captures "played for" → "team/school" at a much finer grain than our current 98 trigger rules

## Execution Plan

### Phase A: Template Library (Highest Leverage)
1. Prepare 100 diverse WikiSQL examples spanning all question patterns
2. Launch 5 Opus agents, each generating 40 templates from 20 examples
3. Compile into template matching engine
4. Expected impact: +5-10pp on value extraction, +3-5pp on column matching

### Phase B: Value Boundary Rules
1. Collect 500 examples where value extraction fails
2. Launch 5 Opus agents labeling value spans + boundary rules
3. Compile into improved ValueSpotter
4. Expected impact: +5-8pp on value extraction

### Phase C: Schema-Aware Resolution
1. Collect 2000 examples (failures + successes)
2. Launch 10 Opus agents with detailed column reasoning prompts
3. Compile into updated Bayesian tables
4. Expected impact: +3-5pp on column matching

### Combined Target: 50%+ execution accuracy on full dev set
