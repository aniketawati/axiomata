You are labeling text-to-SQL examples with JOINT value-span and column assignments.

For each question, identify ALL value spans and which column each belongs to.

## Examples to analyze
{examples_json}

For EACH example, you have: question, headers, where_column (ground truth), where_value.

Provide:

1. `question`: (copy)
2. `value_column_pairs`: A list of ALL (value, column) pairs in this question:
   ```json
   [
     {"value_span": "los angeles rams", "column": "Opponent", "match_reason": "team_name_to_opponent"},
     {"value_span": "37,382", "column": "Attendance", "match_reason": "number_to_numeric_column"}
   ]
   ```
   Include ALL values, not just the primary WHERE condition.

3. For EACH pair, provide `match_reason` — one of:
   - `"name_to_name_column"`: Person name → Player/Name/Winner column
   - `"team_to_team_column"`: Team/org name → Team/Opponent/Club column
   - `"place_to_location_column"`: Place name → Location/Venue/Country column
   - `"number_to_numeric_column"`: Number → numeric column (Score, Points, Rank)
   - `"number_to_id_column"`: Number → ID-like column (No., Episode, Week)
   - `"year_to_year_column"`: Year → Year/Season/Date column
   - `"category_to_category_column"`: Category value → categorical column (Position, Result, Type)
   - `"text_to_matching_column"`: Value text matches column name keywords
   - `"context_implies"`: Verb/preposition context links value to column

4. `column_disambiguation_features`: When multiple columns could match, what distinguishes the correct one?
   - `"column_name_near_value"`: The correct column's name appears near the value in the question
   - `"verb_phrase_links"`: A verb/preposition links value to column ("played for X" → team column)
   - `"value_type_excludes"`: Value type rules out some columns (person name can't be a Date column)
   - `"position_in_question"`: Value's position hints at column (early → primary filter, late → secondary)
   - `"schema_compatibility"`: Only one column semantically accepts this value type

## Output Format
Return ONLY a JSON array. No markdown.

## RULES
- Include ALL value-column pairs, even for multi-condition questions
- value_span must be an exact substring of the question
- Focus on generalizable match_reason categories, not example-specific facts
