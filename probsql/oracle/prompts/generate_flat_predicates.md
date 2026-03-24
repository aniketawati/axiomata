You are a data generation oracle. Given a single flat table with human-readable column headers, generate exactly 25 English questions that a person would ask about this table, along with the SQL WHERE clause that answers each question.

## Input Schema
{schema_json}

## CRITICAL: This is a flat lookup table (like a Wikipedia table)

These tables have human-readable column names like "Player", "School/Club Team", "Population", "Year". The questions should be natural lookup questions that a human would ask when looking at this table.

## Requirements

Generate 25 examples with this distribution:
- 12 SIMPLE LOOKUP: Single condition, value appears verbatim in question
  - "What position does Amir Johnson play?" → Player = 'Amir Johnson'
  - "Who won the race in 2005?" → Year = '2005'
  - "What country has Paris as its capital?" → Capital = 'Paris'

- 5 COMPARISON: Numeric comparisons
  - "Which cities have a population over 1 million?" → Population > 1000000
  - "Players with more than 10 goals" → Goals > 10

- 4 MULTI-CONDITION: 2 conditions joined with AND
  - "Which guard plays for Duke?" → Position = 'Guard' AND School/Club Team = 'Duke'
  - "What 2010 films were rated PG?" → Year = '2010' AND Rating = 'PG'

- 2 IMPLIED COLUMN: The WHERE column is NOT directly named in the question — it's implied
  - "Who wears number 42?" → No. = '42' (question says "number", column is "No.")
  - "Where was the game with 50000 fans?" → Attendance = 50000 (question says "fans", column is "Attendance")

- 2 STRING-NOT-TEMPORAL: Questions with year-like values that are STRING matches, not date operations
  - "What was the result in the 2005-06 season?" → Season = '2005-06' (NOT a date range)
  - "Who won the 1998 tournament?" → Year = '1998' (simple string equality)

## For EACH example provide:

1. `english`: The natural English question as someone would ask looking at a table
2. `target_table`: The table name from the schema
3. `sql_where`: The SQL WHERE clause ONLY (no SELECT/FROM). Use double quotes around column names that contain spaces or special characters.
4. `requires_join`: false (always — these are single flat tables)
5. `join_clause`: null
6. `latent_variables`: An object containing:
   - `predicate_type`: one of ["simple", "comparison", "multi_condition", "implied_column", "string_not_temporal"]
   - `columns_referenced`: list of "table.column" strings
   - `operators_used`: list of SQL operators used
   - `conjunction_type`: "none" for single conditions, "and" for multi-condition
   - `has_temporal`: false (almost always — these are lookup tables)
   - `temporal_type`: null
   - `temporal_expression`: null
   - `has_negation`: false (unless the question uses "not"/"other than"/etc.)
   - `negation_scope`: null
   - `value_types`: list from ["string_literal", "number"]
   - `ambiguity_notes`: describe any ambiguity, or "none"
   - `column_reference_type`: one of ["direct", "synonym", "implied", "partial"]
     - "direct": column name appears in question ("What is the Capital of France?")
     - "synonym": a synonym of the column name appears ("Who wears number 42?" for "No." column)
     - "implied": column is implied by context ("Who played for Duke?" implies School/Club Team)
     - "partial": partial column name match ("What team?" for "Home team"/"Away team")
   - `value_extraction_type`: one of ["verbatim", "normalized", "computed"]
     - "verbatim": value appears exactly in the question
     - "normalized": value needs case/format normalization
     - "computed": value is derived (e.g., "over a million" → 1000000)

## Output Format
Return ONLY a JSON array of 25 objects. No markdown, no explanation, no preamble.

## IMPORTANT RULES
- The value in the WHERE clause should almost always appear VERBATIM in the English question
- Use the ACTUAL column names from the schema — wrap in double quotes if they contain spaces
- For text columns, the default operator is = (exact match), NOT LIKE
- Year-like values ("2005", "1996-97") should be treated as STRING = matches, not temporal operations
- Questions should sound natural: "Who...", "What...", "Where...", "How many...", "Which..."
- For implied column references: the question must NOT contain the column name, but a reader would understand which column is meant
- Make sure values are realistic for the column (don't put a person's name in a "Score" column)
