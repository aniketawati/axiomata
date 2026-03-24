# WikiSQL Gap Analysis

## What WikiSQL reveals about our oracle data blind spots

### 1. Column naming convention mismatch
- **WikiSQL**: 32% of columns have spaces ("School/Club Team", "Years in Toronto", "# of Titles")
- **Our oracle**: 100% snake_case ("created_at", "total_amount", "is_active")
- **Impact**: Column matcher was never trained on human-readable column names. The TF-IDF and semantic map are useless for columns like "Official or native language(s) (alphabet/script)"

### 2. Column is often NOT mentioned in the question
- In WikiSQL, the WHERE column is often **implied**, not stated:
  - Q: "Who is the player that wears number 42?" → col="No." (word "number" maps to "No.")
  - Q: "What position does the player who played for butler cc (ks) play?" → col="School/Club Team" (phrase "played for" maps to school)
- **Our oracle**: Generates predicates where the column mapping is more direct ("active users" → is_active)
- **Impact**: Our column matcher relies on keyword overlap. WikiSQL requires deeper semantic understanding of what the question is *asking about* vs what it's *filtering on*.

### 3. Value appears verbatim in the question
- WikiSQL: The WHERE value almost always appears word-for-word in the question ("Butler CC (KS)", "Guard", "1996-97")
- **Our oracle**: Values are often transformed ("expensive" → price > 100, "last month" → date range)
- **Impact**: Our value extractor looks for transformed values. WikiSQL needs verbatim extraction.

### 4. Operator distribution is different
- **WikiSQL**: 83% are `=`, 9% are `>`, 8% are `<`. Almost no LIKE, IN, IS NULL, BETWEEN.
- **Our oracle**: Much more diverse operators (LIKE, IN, IS NULL, BETWEEN, etc.)
- **Impact**: Our operator extractor was trained on the richer operator set. WikiSQL is simpler but needs high accuracy on `=`.

### 5. Temporal false positives
- WikiSQL has strings like "1996-97", "2005-06" that look like dates but are just string values
- Our temporal parser aggressively claims these as temporal expressions
- **Impact**: Need to be smarter about when temporal parsing applies

### 6. Schema structure differences
- **WikiSQL**: Single flat table, human-readable headers, text/real types only
- **Our oracle**: Multi-table with FKs, developer-style naming, rich type system (BOOLEAN, ENUM, TIMESTAMP)
- **Impact**: Our engine was designed for the developer schema paradigm

## What to fix in the PIPELINE (not the code)

### Oracle Generation (Phase 1) improvements needed:

1. **Add flat-table schemas with human-readable column names**
   - Generate Wikipedia-style tables: "Player", "Year", "Score", "Country"
   - Mix of snake_case AND human-readable columns
   - Single-table schemas (no JOINs needed)

2. **Add "lookup-style" predicates**
   - "What is X where Y is Z?" pattern — column is implied, value is verbatim
   - "Who/What/Where/When" questions that map to different SELECT columns
   - The WHERE condition references one column, the question asks about another

3. **Add verbatim value extraction examples**
   - Questions containing the exact value: "played for Butler CC (KS)"
   - Proper nouns, mixed case, special characters in values
   - Numeric values as strings ("number 42" → "42" not 42)

4. **Expand column-reference patterns**
   - "player who played for X" → School/Club Team column (indirect reference)
   - "player that wears number X" → No. column (synonym mapping)
   - Column names with spaces, parens, slashes, abbreviations

5. **Reduce temporal parser aggressiveness**
   - Need examples where year-like strings ("1996-97") are NOT temporal
   - Train on the distinction: "in 2024" (temporal) vs "season 2005-06" (string value)

### Structure Learning (Phase 2) improvements:

6. **Column matcher needs two modes**
   - Developer schemas: use current TF-IDF + semantic expansion
   - Flat/human-readable schemas: use direct substring matching + synonym expansion

7. **Value extractor needs "verbatim scan"**
   - Scan the question for substrings that match actual table data
   - Proper noun detection needs improvement

8. **Operator extractor default should be `=` for text, not LIKE**
   - Current default for VARCHAR is `=` with LIKE fallback
   - But in practice it's hitting LIKE too often — need to tighten the LIKE triggers

### What NOT to change:
- The engine architecture is sound
- The predicate tree / conjunction parser / negation handler work well
- The confidence calibration approach is correct
- Performance is excellent and doesn't need changes
