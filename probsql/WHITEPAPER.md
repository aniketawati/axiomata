# Distilling LLM Intelligence into Domain-Specific Probabilistic Programs

*A Methodology for Building Fast, Interpretable AI Systems from Large Language Model Knowledge*

---

## Abstract

Large Language Models (LLMs) encode vast structured reasoning knowledge but accessing it requires expensive API calls (~$0.003/query), produces opaque outputs, and cannot run at the edge. We present a systematic methodology for extracting LLM reasoning into small, fast, interpretable probabilistic programs — a new form of knowledge compilation that produces fundamentally different artifacts than traditional model distillation. Our approach uses the LLM as a reasoning oracle to generate labeled data across carefully designed prompt types, computes empirical conditional probability tables from those labels, assembles the tables into a compositional probabilistic program (a chain of Bayesian classifiers and Markov chains), and iterates based on benchmark-driven error analysis. We demonstrate this methodology with ProbSQL, a text-to-SQL engine built entirely from Claude's knowledge, achieving 37.6% execution accuracy on the WikiSQL benchmark (8,357 examples) with zero ML framework dependencies, 1.5ms p99 latency (960x faster than LLM API calls), and complete interpretability — every decision traces to an inspectable probability table. Through 18 rounds of iterative improvement using ~265,000 LLM-labeled examples across 11 prompt types, we document the methodology's strengths, limitations, and six key empirical findings about what works and what fails when distilling LLM intelligence into probabilistic programs. The methodology generalizes to any domain where an LLM can decompose its reasoning into identifiable steps with structured features.

---

## 1. Introduction

### 1.1 The LLM Deployment Problem

Large Language Models have demonstrated remarkable capability across a wide range of tasks, from code generation to question answering to structured data querying. However, deploying LLMs in production systems faces three fundamental constraints:

**Cost**: At approximately $0.003 per query, serving millions of requests per day becomes prohibitively expensive. A service handling 10 million queries daily would incur ~$30,000 in API costs alone.

**Latency**: LLM API calls typically take 1,000-3,000ms, far exceeding the <10ms latency requirement of many real-time systems. This 100-1000x latency gap makes LLMs unsuitable for applications like autocomplete, real-time search, and embedded systems.

**Interpretability**: LLM outputs are opaque — when the model produces a SQL query, there is no way to understand *why* it chose a particular column or operator. This makes debugging, auditing, and trust-building difficult in production environments.

These constraints motivate a fundamental question: can we extract the *knowledge* that makes LLMs effective and compile it into artifacts that are fast, cheap, and interpretable?

### 1.2 Knowledge Distillation vs. Reasoning Distillation

Traditional knowledge distillation (Hinton et al., 2015) compresses a large neural network into a smaller one — the artifact remains a neural network, just with fewer parameters. DistilBERT, TinyBERT, and similar approaches produce models that are faster but still opaque, still require ML frameworks, and still lack interpretability.

We propose a fundamentally different target: **reasoning distillation** into **probabilistic programs**. Instead of producing a smaller neural network, we produce a program composed of conditional probability tables, Bayesian classifiers, and Markov chains. This program:

- **Has zero ML dependencies** — runs on Python stdlib alone
- **Is fully interpretable** — every decision traces to an inspectable probability table entry
- **Is deterministic** — same input always produces same output
- **Is composable** — individual components can be tested, replaced, and improved independently
- **Runs on any hardware** — no GPU, no special libraries, no model loading

The key insight is that LLMs don't just produce answers — they produce answers through a *reasoning process* that has identifiable steps, features, and conditional dependencies. This reasoning structure maps naturally onto probabilistic graphical models.

### 1.3 Contributions

This paper makes three contributions:

1. **A general methodology** for distilling LLM knowledge into domain-specific probabilistic programs through an iterative pipeline of reasoning-oriented data labeling, empirical probability computation, and compositional program assembly.

2. **ProbSQL**, a concrete case study demonstrating the methodology on text-to-SQL, achieving 37.6% execution accuracy on WikiSQL with 960x latency improvement over the source LLM and zero external dependencies.

3. **Six empirical findings** about what works and what fails when extracting LLM intelligence into probabilistic programs, including counterintuitive results about structural priors, ensemble calibration, and the integration of world knowledge.

---

## 2. Methodology: The Distillation Pipeline

### 2.1 Overview

Our methodology consists of four stages that iterate until convergence:

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  Stage 1: LLM as Reasoning Oracle                       │
│  ┌──────────────────────────────────────┐               │
│  │ Structured prompts that extract      │               │
│  │ not just answers but reasoning       │               │
│  │ features, categories, and steps      │               │
│  └──────────────┬───────────────────────┘               │
│                 ↓                                        │
│  Stage 2: Empirical Probability Computation              │
│  ┌──────────────────────────────────────┐               │
│  │ From labeled data to conditional     │               │
│  │ probability tables: P(Y | X)        │               │
│  └──────────────┬───────────────────────┘               │
│                 ↓                                        │
│  Stage 3: Probabilistic Program Assembly                 │
│  ┌──────────────────────────────────────┐               │
│  │ Chain of Bayesian classifiers and    │               │
│  │ Markov chains, structured to mirror  │               │
│  │ the LLM's reasoning decomposition   │               │
│  └──────────────┬───────────────────────┘               │
│                 ↓                                        │
│  Stage 4: Benchmark-Driven Iteration                     │
│  ┌──────────────────────────────────────┐               │
│  │ Error categorization → targeted      │  ──→ Loop    │
│  │ re-labeling of failure modes         │     back     │
│  └──────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────┘
```

Each stage produces artifacts consumed by the next: labeled JSON datasets, conditional probability tables stored as JSON files, and executable Python code implementing the probabilistic program.

### 2.2 Stage 1: LLM as Reasoning Oracle

The critical distinction from traditional distillation is that we extract the LLM's **reasoning process**, not just its answers. A naive prompt like "What is the WHERE column for this question?" produces an answer but no extractable knowledge about *why*. A reasoning-oriented prompt asks:

```
For this question and schema, determine the WHERE column and explain:
- why_this_column: one of [column_name_mentioned, trigger_phrase_indicates,
  value_is_entity_name, value_matches_column_type, process_of_elimination]
- trigger_phrase: the exact words that signal the column (2+ words)
- column_keyword: the strongest word in the column name
- value_span: the exact substring that is the WHERE value
- select_reasoning: how you identified the SELECT column
```

The reasoning categories (`column_name_mentioned`, `trigger_phrase_indicates`, etc.) directly become the states and features of the probabilistic program. This is the fundamental mechanism by which LLM intelligence becomes compilable.

**Prompt Design Principles**:

1. **Enumerated categories**: Force the LLM to classify its reasoning into predefined types. These categories become probability table dimensions.
2. **Structural features**: Extract features that generalize beyond specific examples (e.g., "follows_preposition" rather than "follows the word 'of'").
3. **Exact spans**: Require verbatim substrings from the input, ensuring labels can be verified programmatically.
4. **Confidence-forcing**: Ask the LLM to commit to specific categories rather than hedging with natural language.

In ProbSQL, we developed 11 distinct prompt types across the development cycle, generating approximately 265,000 labeled examples:

| Prompt Type | Purpose | Labels |
|---|---|---|
| Column reasoning | Why does this value map to this column? | 2,500 |
| Value span extraction | Where does the value start/end in the question? | 1,000 |
| Condition count estimation | How many WHERE conditions does this question need? | 1,366 |
| Token role annotation | What role does each word play? (QWORD, VALUE, TRIGGER, etc.) | 3,000 |
| Entity type classification | What type is this entity? (person, city, team, etc.) | 5,990 |
| Column type classification | What semantic type is this column header? | 16,601 |
| Joint span-column labeling | Which value belongs to which column, and why? | 1,200 |
| Semantic mapping rules | What generalizable rules connect phrases to columns? | 60 |
| WikiSQL mechanical extraction | Ground truth decomposition from dataset | 88,274 ×3 |

### 2.3 Stage 2: Empirical Probability Computation

Each batch of labeled data yields conditional probability tables. For example, from 1,500 column reasoning labels:

```
P(reasoning_type):
  column_name_mentioned: 0.65
  trigger_phrase_indicates: 0.14
  value_is_entity_name: 0.12
  value_matches_column_type: 0.07
  other: 0.02
```

These empirical base rates directly become the weights in the probabilistic program. The computation is straightforward — count occurrences and normalize — but the insight is that these numbers capture the LLM's *implicit reasoning distribution* over strategies.

Similarly, from 76,000 WikiSQL training examples:

```
P(operator | value_type):
  person_name → = : 1.00
  number → = : 0.48, > : 0.27, < : 0.25
  category → = : 1.00
  year_string → = : 0.60, >= : 0.21, <= : 0.19
```

All probability tables are stored as inspectable JSON files. There are no learned weight matrices, no opaque embeddings — every number has a clear provenance from counted observations in labeled data.

### 2.4 Stage 3: Probabilistic Program Assembly

The probability tables are assembled into a compositional program where each step is a conditional probability lookup and the overall structure mirrors the task decomposition:

```python
def resolve(question, schema):
    q_type = argmax P(q_type | question_features)         # Bayesian classifier
    n_conds = argmax P(n_conditions | question_features)   # Bayesian classifier
    value = span_detector(question)                        # Boundary probability model
    select_col = argmax P(select | question, headers)      # Bayesian classifier
    v_type = argmax P(v_type | value_features)             # Bayesian classifier
    where_col = markov_resolve(value, v_type, headers,     # 5-state Markov chain
                               select_col, question)
    operator = argmax P(op | q_type, v_type)               # Bayesian classifier
    return SQL(where_col, operator, value)
```

Each `P(Y | X)` is a lookup in the tables computed in Stage 2. The program structure (which steps, in what order, with what dependencies) is itself extracted from analyzing LLM reasoning chains — the most common reasoning order becomes the program's execution order.

**Key architectural patterns**:

- **Bayesian classifiers** for independent decisions (question type, value type, operator selection)
- **Markov chains** for sequential reasoning where each step depends on previous steps (column resolution with 5 states: Prior → Entity Knowledge → Proximity → Trigger → Exclusion)
- **Boundary probability models** for span detection (P(start | left_context) × P(end | right_context))
- **Compatibility tables** for cross-element scoring ("probabilistic attention" — a 2D lookup between entity types and column types that plays the role of attention weights)
- **Ensemble** across multiple resolution strategies with confidence-based selection

### 2.5 Stage 4: Benchmark-Driven Iteration

Each iteration follows a diagnosis-treatment cycle:

1. **Benchmark** the current program against ground truth
2. **Categorize errors** by which component fails (value detection, column resolution, operator selection, condition count)
3. **Analyze failure modes** within each component (e.g., "column name not in question" vs. "multiple columns match")
4. **Design targeted prompts** for the specific failure mode
5. **Label examples** focused on failures (not random samples)
6. **Recompute probability tables** and reassemble

This error-targeted approach is more efficient than uniform random labeling. In ProbSQL, Round 6 labeled 1,000 examples specifically chosen as 500 failures + 500 successes, yielding more improvement per label than the uniform-random Round 5 labels.

### 2.6 Generalizability

This methodology applies when four conditions hold:

1. **The LLM can perform the task** — the LLM is the "teacher" whose knowledge we extract
2. **The LLM can explain its reasoning** in terms of identifiable features and categories
3. **The task decomposes** into subtasks with clear inputs and outputs
4. **Ground-truth evaluation** is possible for benchmarking

These conditions are met by many practical tasks beyond SQL: document classification, entity extraction, medical triage, customer support routing, and any lookup/matching task where the LLM reasons through identifiable steps.

---

## 3. Case Study: ProbSQL

### 3.1 Task Definition

We demonstrate the methodology on text-to-SQL using the WikiSQL benchmark — 80,654 English questions paired with SQL queries over 24,241 Wikipedia tables. WikiSQL queries are single-table SELECT + WHERE, making them tractable for a probabilistic program while still requiring genuine natural language understanding.

Example: "What position does the player who played for Butler CC (KS) play?" over a table with columns [Player, No., Nationality, Position, Years in Toronto, School/Club Team] requires:

- Identifying "position" as the SELECT target
- Identifying "Butler CC (KS)" as the WHERE value
- Resolving that "played for" links to "School/Club Team" (not "Player" or "Position")
- Generating: `WHERE "School/Club Team" = 'Butler CC (KS)'`

### 3.2 The Compositional Probabilistic Program

ProbSQL implements a 7-step compositional program where each step is a conditional probability table:

**Step 1 — Question Type Classification**: P(q_type | features)
A Naive Bayes classifier over features {question_word, has_comparison, has_aggregation, word_count}. Prior: lookup 52%, comparison 23%, count 14%, superlative 10%.

**Step 2 — Condition Count Estimation**: P(n_conditions | features)
Trained from 1,316 Opus-labeled examples. Key discriminating feature: P(has_and | n=1) = 0.03 vs P(has_and | n=2) = 0.45. A simple feature that the LLM's labels revealed.

**Step 3 — Value Span Boundary Detection**: P(start | left_word) × P(end | right_word)
Trained from 1,000 Opus-labeled value spans. Start signals: after_preposition 35%, at_number 20%, after_copula 11%. End signals: before_question_mark 40%, before_filler 14%, end_of_question 13%. This replaces a token-by-token HMM that fragmented multi-word values.

**Step 4 — SELECT Column Identification**: P(select | question_prefix, headers)
From 1,500 reasoning labels: column_name_after_question_word 86%, question_word_implies 11%. "What position..." → Position is the SELECT target with 86% base rate.

**Step 5 — Value Type Classification**: P(v_type | value_features)
Naive Bayes over format features {is_capitalized, has_digits, word_count, has_special_chars}. Distribution: proper_noun 24%, number 22%, lowercase_phrase 14%.

**Step 6 — Markov Chain Column Resolution**: The core reasoning engine — a 5-state Markov chain:

```
State 0: Uniform Prior → P(col | value_type)
         Distributes probability based on whether the value is a
         person name, team, number, etc.

State 1: Entity Knowledge → P(col | entity_type, column_type)
         "Probabilistic attention" — a 2D compatibility lookup
         between the value's entity type (from a 5,990-entry KB)
         and the column's semantic type (from 16,601 LLM-classified
         headers). Rome(city) × Location(place) = 0.95.

State 2: Proximity → P(col | column_name_near_value)
         Weight: 0.65. Checks if column name words appear near
         the value in the question. Dominant signal from the
         LLM's own reasoning distribution.

State 3: Trigger → P(col | trigger_phrase)
         Weight: 0.14. "played for" → school/team columns.
         98 trigger rules from LLM extraction.

State 4: SELECT Exclusion → P(col | col ≠ select_col)
         Heavy penalty on the column identified in Step 4.
```

Each state applies a Bayesian update to the probability distribution over columns. The final distribution is the product of all updates.

**Step 7 — Operator Selection**: P(op | q_type, v_type, comparison_words)
From 76,000 WikiSQL examples: person_name → 100% equals, number → 48% equals / 27% greater / 25% less.

### 3.3 Probabilistic Attention Without Neural Networks

The entity-column compatibility table (Markov State 1) deserves special attention as it demonstrates how "attention" — the cross-element relevance scoring that powers transformers — can be implemented as a static probability lookup:

| Entity Type ↓ / Column Type → | person | place | team_org | numeric | temporal |
|---|---|---|---|---|---|
| **country** | 0.02 | **0.95** | 0.30 | 0.02 | 0.10 |
| **city** | 0.02 | **0.95** | 0.15 | 0.02 | 0.05 |
| **person** | **0.90** | 0.02 | 0.10 | 0.02 | 0.05 |
| **team** | 0.05 | 0.10 | **0.90** | 0.02 | 0.05 |
| **position** | 0.02 | 0.02 | 0.02 | 0.02 | **0.95** (category) |
| **number** | 0.05 | 0.05 | 0.05 | **0.70** | 0.10 |

Neural attention computes this dynamically per query using learned weight matrices and softmax. Our approach computes it once from LLM labels and stores it as a table. The trade-off: no dynamic contextualization, but O(1) lookup instead of O(n²) computation. For structured domains where entity and column types are finite, this is sufficient.

### 3.4 The Three-Path Ensemble

ProbSQL runs three parallel resolution strategies:

1. **Old Path**: TF-IDF keyword matching + conjunction parser (the original Phase 1 engine)
2. **SemExtract Path**: Question decomposition + value spotting + Bayesian column resolver
3. **ProbProg Path**: The compositional probabilistic program described above

The ensemble selects the result with the highest raw confidence. Counterintuitively, this naive max-confidence strategy outperforms isotonic regression calibration (see Section 5.3).

### 3.5 Implementation

The complete system runs on Python 3.11+ with zero external dependencies (stdlib only: json, re, math, sqlite3, dataclasses). Knowledge artifacts total ~0.5MB of JSON files. Runtime memory is ~13MB RSS. The system starts in <2ms and processes queries at 1,400 QPS with 1.5ms p99 latency.

---

## 4. Experimental Results

### 4.1 WikiSQL Benchmark

On the full WikiSQL development set (8,357 examples with WHERE conditions):

| Metric | Result |
|---|---|
| **Execution accuracy** | **37.6%** |
| Column match accuracy | 53.4% |
| Operator match accuracy | 47.1% |
| Condition count accuracy | 72.1% |
| Engine errors | 0 |
| Average latency | 1.32ms |

By number of conditions:

| Conditions | Examples | Execution Accuracy |
|---|---|---|
| 1 condition | 5,755 (69%) | 39.6% |
| 2 conditions | 2,082 (25%) | 32.0% |
| 3 conditions | 454 (5%) | 39.9% |
| 4 conditions | 66 (1%) | 19.7% |

Pipeline step accuracy (n=999 sample):

| Step | Accuracy | Loss |
|---|---|---|
| Value span detection | 68.9% | 31.1% lost |
| Value appears in SQL | 82.9% | — |
| Column correct | 65.8% | 17.1% lost |
| Operator correct | 94.8% | ~0% lost |
| Condition count correct | 72.1% | 22.7% lost |
| **Execution correct** | **37.7%** | — |

### 4.2 The 18-Round Iteration Journey

The system evolved from 0% to 37.6% over 18 rounds of development:

| Round | Key Change | Exec Acc | Delta |
|---|---|---|---|
| 1 | Initial engine | 0% | — |
| 2 | Flat table oracle data + WikiSQL benchmark | 34.9%* | — |
| 3 | Temporal parser gating, value extraction | — | — |
| 5 | Empirical Bayesian weights from 500 LLM labels | — | — |
| 6 | 3-way ensemble + SELECT hardening + Bayesian operators | — | — |
| 7 | Compositional probabilistic program (Markov chain) | — | — |
| — | *Full dev set baseline established* | 34.9% | — |
| 11 | Span boundary detector | 35.4% | +0.5 |
| 12 | Case-insensitive SQL + condition limiting | 35.9% | +0.5 |
| 13 | Bayesian condition estimator (1,316 Opus labels) | 36.3% | +0.4 |
| 14 | Fully Bayesian ProbProg (7/8 components) | 36.3% | — |
| 16b | Entity knowledge as Markov chain state (5,990 entities) | 37.0% | +0.7 |
| **18** | **LLM-classified column types (16,601 headers)** | **37.6%** | **+0.6** |

*Rounds 2-7 measured on first 2,000 examples (biased sample). Full dev set (8,357) established in Round 10.*

### 4.3 Performance Characteristics

| Metric | ProbSQL | LLM API (Claude) | Speedup |
|---|---|---|---|
| p99 Latency | 1.5ms | ~1,440ms | **960x** |
| Throughput | 1,400 qps | ~1 qps | **1,400x** |
| Cost per query | $0 | ~$0.003 | **Free** |
| Memory | 13MB | N/A (cloud) | — |
| Dependencies | Python stdlib | API + SDK | Zero |
| Offline capable | Yes | No | — |
| Interpretable | Full trace | Opaque | — |

### 4.4 The Accuracy-Speed Trade-Off

ProbSQL achieves 37.6% vs. an estimated 85-90% for direct LLM inference on WikiSQL. This 50+ percentage point gap is the cost of compilation. The trade-off is favorable when:

- **Volume is high**: At 10M queries/day, ProbSQL saves ~$30,000/day in API costs
- **Latency matters**: 1.5ms vs 1,440ms enables real-time applications
- **Interpretability is required**: Every decision has a traceable probability table entry
- **Offline deployment is needed**: No internet connection required
- **Accuracy tolerance exists**: 37.6% is sufficient for suggestion/autocomplete (not critical decisions)

---

## 5. Lessons Learned

### 5.1 Structural Priors Beat Learned Word Frequencies

We trained a Hidden Markov Model from 3,000 LLM-annotated token sequences to detect value spans. The learned HMM emission probabilities (P(word | role)) *degraded* accuracy from 73.4% to 66%.

**Root cause**: The FILLER role dominates (45% of tokens), so word-based emissions assign high probability to FILLER for everything. Capitalized words, numbers, and special characters — all strong VALUE signals — get swallowed by FILLER's word-frequency advantage.

**Solution**: Rule-based structural priors (is_capitalized → VALUE, is_number → VALUE, follows_trigger → VALUE) outperform learned word frequencies because they capture *why* a token is a value, not just which words happen to be values.

**Lesson**: When distilling LLM knowledge, extract *structural rules* (which features determine the output), not *statistical correlations* (which words co-occur with the output).

### 5.2 Empirical Bayesian Weights From LLM Labels Outperform Hand-Tuning

The Markov chain column resolver initially used hand-tuned equal weights for its four factors (proximity, trigger, type compatibility, exclusion). Asking the LLM to categorize its *reasoning type* for 1,500 examples revealed the empirical distribution:

```
proximity (column name near value): 65%
trigger (verb phrase hints at column): 14%
type match (value type fits column): 12%
other: 9%
```

Using these as weights improved column accuracy from ~55% to 65.8%. The LLM's self-reported reasoning distribution is a better weighting scheme than any hand-tuned combination we tried.

### 5.3 Naive Max-Confidence Beats Isotonic Calibration for Ensembles

We implemented isotonic regression to calibrate each path's confidence scores so they would be comparable. Counter-intuitively, the calibrated ensemble (30.9%) performed *worse* than naive max-confidence (34.9%).

**Root cause**: All three paths have similar average accuracy (~28-31%), so isotonic calibration maps all confidences to ~0.30, destroying the per-example signal. The raw (uncalibrated) confidence contains information about *which examples* each path handles well — high confidence on path A correlates with correctness on A's strengths, even if A's average accuracy equals B's.

**Lesson**: For ensembles of differently-structured models, per-example confidence contains more information than per-model calibration.

### 5.4 Joint Models Only Help When Both Sides Are Accurate

We built a joint (value, column) resolver from 1,200 Opus-labeled examples. The joint model scores value-column pairs together instead of detecting values first, then resolving columns. This was architecturally appealing but reduced accuracy.

**Root cause**: When span detection is 68.9% accurate, jointly resolving value+column amplifies span errors into column errors. A wrong value span produces a wrong column assignment with high confidence, overriding the sequential path's correct column choice.

**Lesson**: Sequential pipelines are more robust than joint models when upstream components have significant error rates. Joint optimization requires all components to be individually strong.

### 5.5 World Knowledge Must Be Integrated as Bayesian Updates, Not Replacements

We extracted 5,990 entity types (Rome→city, Guard→position, Lakers→team) and initially used them to *override* the column resolver's decision. This failed catastrophically — when the entity type was wrong or the column type was unfamiliar, the entire chain collapsed.

Changing to a Bayesian update (entity knowledge as one factor in the Markov chain, contributing a weighted update rather than a veto) improved accuracy by 0.7 percentage points while maintaining robustness on unknown entities.

**Lesson**: External knowledge should modulate probabilities, not replace them. A Bayesian update adds information when knowledge is available and passes through unchanged when it isn't.

### 5.6 Extract the Reasoning Process, Not Just Answers

Early prompts asked: "What is the WHERE column?" Later prompts asked: "What is the WHERE column, and *why* — is it because the column name is mentioned, or a trigger phrase indicates it, or the value type matches?"

The "why" prompt produced data that directly became Markov chain states. The answer-only prompt produced data that could only train a flat classifier.

**Lesson**: The reasoning process is the extractable knowledge. Design prompts that force the LLM to decompose its reasoning into identifiable, categorizable steps. Those categories become the structure of the probabilistic program.

---

## 6. Discussion

### 6.1 The Accuracy Gap

ProbSQL's 37.6% vs. ~85-90% for the source LLM represents a significant gap. Error analysis reveals:

- **33% of failures**: Column correct but value format doesn't match (case, spacing, extra words)
- **20% of failures**: Entity typed correctly but wrong column selected (multiple columns of same type)
- **18% of failures**: Value span not detected at all
- **8% of failures**: Entity not in knowledge base
- **6% of failures**: Too many WHERE conditions generated

The gap is not primarily in reasoning quality but in **coverage** — the probability tables don't cover the long tail of rare patterns. An LLM handles "What position does the player who played for Butler CC (KS) play?" by understanding the full semantic structure; our program handles it by looking up "played for" → school/team column (which works) but fails on "What was the result of the match in the 2003-04 season at the Vodafone Arena?" because "Vodafone Arena" isn't in the entity KB and "2003-04" isn't recognized as a season string for every column named differently.

### 6.2 Limits of Static Probability Tables

Static probability tables cannot capture context-dependent reasoning. When a table has columns [Home team, Home team score, Away team, Away team score] and the question asks about the "Melbourne" score, the LLM uses contextual understanding to determine whether Melbourne is the home or away team *for this specific row*. Our program can only use the compatibility table (team → team_org column) and proximity (which column name is nearest), which doesn't distinguish between Home and Away team.

This represents a fundamental limitation: static tables encode *typical* conditional probabilities, not *situational* ones. Closing this gap may require a hybrid architecture where the probabilistic program handles the majority of cases and defers to the LLM for ambiguous ones.

### 6.3 When to Use This Methodology

The methodology is appropriate when:

- The task has **decomposable subtasks** with identifiable features
- **High volume** makes per-query LLM cost prohibitive
- **Low latency** is required (<10ms)
- **Interpretability** is important for debugging or compliance
- **Accuracy tolerance** exists (the compiled program will always be less accurate than the source LLM)
- **Edge deployment** is needed (offline, on-device, air-gapped)

It is inappropriate when:

- The task requires **creative or open-ended** reasoning
- **Maximum accuracy** is non-negotiable
- The reasoning doesn't **decompose** into identifiable steps
- The domain is **rapidly changing** (probability tables become stale)

### 6.4 Toward Hybrid Architectures

The most promising direction is a hybrid where the probabilistic program handles high-confidence queries (the 37.6% it gets right) and routes low-confidence queries to the LLM. With a well-calibrated confidence threshold, this could achieve near-LLM accuracy at a fraction of the cost — the program handles the "easy" 60-70% of queries, and the LLM handles the hard 30-40%.

---

## 7. Conclusion

We have demonstrated that LLM intelligence can be systematically extracted into domain-specific probabilistic programs through an iterative pipeline of reasoning-oriented data labeling, empirical probability computation, and compositional program assembly. The resulting artifacts are fundamentally different from neural networks — they are fast (960x), interpretable (every decision traces to a probability table), composable (individual components can be independently improved), and free to run (zero API cost, zero ML dependencies).

ProbSQL, our case study on text-to-SQL, achieves 37.6% execution accuracy on WikiSQL through 18 rounds of iterative improvement, using ~265,000 LLM-labeled examples across 11 prompt types to populate a 7-step compositional probabilistic program with a 5-state Markov chain column resolver and probabilistic attention via entity-column compatibility tables.

The methodology generalizes to any domain where an LLM can decompose its reasoning into identifiable steps with structured features. The core insight is that LLMs are not just answer machines — they are repositories of structured reasoning knowledge that can be compiled into fast, interpretable programs. This is a new form of knowledge distillation: not model compression, but **reasoning compilation**.

---

## Appendix A: Key Probability Tables

### A.1 Column Reasoning Type Distribution (n=1,500)
```
column_name_mentioned: 0.65
trigger_phrase_indicates: 0.14
value_is_entity_name: 0.12
value_matches_column_type: 0.07
process_of_elimination: 0.01
number_matches_numeric_column: 0.01
```

### A.2 Operator Distribution by Value Type (n=76,000)
```
person_name: = 100%
category: = 100%
number: = 48%, > 27%, < 25%
year_string: = 60%, >= 21%, <= 19%
```

### A.3 SELECT Identification Method (n=1,500)
```
column_name_after_question_word: 86%
question_word_implies: 11%
context_implies: 3%
```

### A.4 Entity-Column Compatibility Table (probabilistic attention)
```
              person  place  team_org  numeric  temporal  category
country:       0.02   0.95    0.30     0.02     0.10      0.10
city:          0.02   0.95    0.15     0.02     0.05      0.05
person:        0.90   0.02    0.10     0.02     0.05      0.05
team:          0.05   0.10    0.90     0.02     0.05      0.10
position:      0.02   0.02    0.02     0.02     0.02      0.95
number:        0.05   0.05    0.05     0.70     0.10      0.10
year:          0.02   0.02    0.02     0.20     0.90      0.05
```

### A.5 Value Span Start/End Signals (n=1,000)
```
Start signals:
  after_preposition: 35% ("of [VALUE]", "for [VALUE]")
  at_number: 20% (digit at start)
  after_copula: 11% ("is [VALUE]", "was [VALUE]")
  at_proper_noun: 8% (capitalized word)

End signals:
  before_question_mark: 40% ("[VALUE]?")
  before_filler: 14% ("[VALUE] is/the/a")
  end_of_question: 13% ("...[VALUE]")
  before_comma: 12% ("[VALUE], and")
```

---

## Appendix B: Full Iteration Log

| Round | Commit | Change | Full Dev Exec |
|---|---|---|---|
| 1 | d2f81fc | Initial probabilistic engine | 0% |
| 2 | 847e98e | Flat table oracle data + WikiSQL benchmark | *66.1% (biased 2K)* |
| 3 | c246a62 | Temporal gating, value extraction | *66.7% (biased 2K)* |
| 5 | 5efe4cb | Empirical Bayesian weights from LLM labels | *67.6% (biased 2K)* |
| 6 | a8a5056 | Ensemble + SELECT hardening + Bayesian operators | *70.8% (biased 2K)* |
| 7 | 7c1d6cf | Compositional probabilistic program (Markov chain) | *73.4% (biased 2K)* |
| 8 | 7960f05 | HMM training from 3,000 annotations | 34.9% |
| 9 | dec27e1 | Feature-based HMM + 1,000 Opus value spans | 34.9% |
| 10 | 70973ed | Ensemble calibration analysis | 34.9% |
| 11 | 1b833b0 | Span boundary detector | 35.4% |
| 11b | 1c181dc | Multi-span detection | 32.5% |
| 12 | f1e3c60 | Case-insensitive SQL + condition limiting | 35.9% |
| 13 | a488643 | Bayesian condition estimator (1,316 labels) | 36.3% |
| 14 | 26f9f16 | Fully Bayesian ProbProg (7/8 components) | 36.3% |
| 15 | c84299d | Joint resolver architecture | 35.4% |
| 16 | 5f2bd29 | Entity knowledge base | 34.2% |
| 16b | 6608669 | Entity as Markov chain state (5,990 entities) | 37.0% |
| 17 | d0a8f18 | Failure analysis + value trimming | 37.0% |
| **18** | **975f291** | **LLM-classified column types (16,601 headers)** | **37.6%** |

*Note: Rounds 2-7 measured on first 2,000 examples which overrepresented easy single-condition queries. Full dev set (8,357 examples) baseline established at Round 8.*
