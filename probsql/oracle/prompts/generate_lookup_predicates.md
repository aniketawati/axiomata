You are a data generation oracle. Given a flat table, generate exactly 25 LOOKUP-STYLE questions with their SQL WHERE clauses.

## Input Schema
{schema_json}

## CRITICAL PATTERN: SELECT column vs WHERE column

In lookup questions, the question ASKS ABOUT one column but FILTERS ON a different column:

- "What POSITION does the player who played for BUTLER CC play?"
  → SELECT Position (asked about) | WHERE "School/Club Team" = 'Butler CC' (filtered on)

- "Who is the PLAYER that wears NUMBER 42?"
  → SELECT Player (asked about) | WHERE "No." = '42' (filtered on)

- "What COUNTRY has PARIS as its capital?"
  → SELECT Country (asked about) | WHERE Capital = 'Paris' (filtered on)

Your job is to generate the WHERE clause only. The WHERE column is the one being FILTERED ON, not the one being asked about.

## Requirements

Generate 25 examples with this distribution:

- 10 LOOKUP WITH PROPER NOUN VALUE: Question contains a proper noun/specific value that filters a column
  - "What position does Amir Johnson play?" → "Player" = 'Amir Johnson'
  - "What country is Tokyo in?" → Capital = 'Tokyo'
  - The value (Amir Johnson, Tokyo) appears verbatim in the question

- 5 LOOKUP WITH NUMBER VALUE: Question contains a number that filters a column
  - "Who wears number 42?" → "No." = '42'
  - "What team scored 3 goals?" → Goals = 3
  - Use the column that the NUMBER logically belongs to, not the column being asked about

- 5 LOOKUP WITH CATEGORY VALUE: Question filters on a category/type
  - "Which guards are from Duke?" → Position = 'Guard' AND "School/Club Team" = 'Duke'
  - "What films are rated PG-13?" → Rating = 'PG-13'

- 3 REVERSE LOOKUP: The question asks for the value of the SAME column type it filters on (less common)
  - "What is the score when the opponent was Lakers?" → Opponent = 'Lakers'
  - The SELECT and WHERE are on different columns but the structure is simpler

- 2 YEAR-AS-STRING: Question mentions a year/season that should be matched as string =, NOT as temporal
  - "Who won in 2005?" → Year = '2005' (string match)
  - "What was the result in the 2005-06 season?" → Season = '2005-06' (string match)

## For EACH example provide:

1. `english`: Natural English question (Who/What/Where/When/How many...)
2. `target_table`: The table name
3. `sql_where`: SQL WHERE clause ONLY. Use double quotes for column names with spaces/special chars. Use = for text matches.
4. `requires_join`: false
5. `join_clause`: null
6. `latent_variables`:
   - `predicate_type`: one of ["lookup_proper_noun", "lookup_number", "lookup_category", "reverse_lookup", "year_as_string"]
   - `columns_referenced`: list of "table.column" for WHERE columns
   - `select_column`: the column the question ASKS ABOUT (for training the SELECT vs WHERE distinction)
   - `operators_used`: list of operators
   - `conjunction_type`: "none" or "and"
   - `has_temporal`: false
   - `temporal_type`: null
   - `temporal_expression`: null
   - `has_negation`: false
   - `negation_scope`: null
   - `value_types`: ["string_literal"] or ["number"]
   - `ambiguity_notes`: describe any ambiguity
   - `column_reference_type`: "implied" or "direct" or "synonym"
   - `value_extraction_type`: "verbatim"
   - `value_in_question`: the exact substring from the question that is the WHERE value

## Output Format
Return ONLY a JSON array of 25 objects. No markdown, no explanation.

## RULES
- The WHERE value MUST appear verbatim in the question
- The WHERE column is DIFFERENT from what the question asks about
- Use = for all text comparisons (not LIKE)
- Year values like "2005", "1996-97" are STRING = matches
- Use actual column names from the schema, double-quoted if they contain spaces
- `value_in_question` must be the exact substring that should be extracted as the value
