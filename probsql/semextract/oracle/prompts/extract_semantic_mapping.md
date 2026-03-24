You are a linguistic analyst extracting semantic mapping rules from text-to-SQL examples.

Given a question, a table schema, and the ground-truth SQL, analyze WHY each word/phrase in the question maps to a specific column. Extract the semantic reasoning as structured rules.

## Examples to analyze
{examples_json}

## For EACH example, provide:

1. `question`: The original question
2. `where_column`: The correct WHERE column
3. `where_value`: The correct WHERE value
4. `semantic_mappings`: A list of rules that explain the mapping:

   For each mapping rule, provide:
   - `trigger_phrase`: The exact words/phrase in the question that indicate the column
   - `trigger_type`: One of:
     - "verb_relation": A verb phrase implies a column ("played for" → school/team column)
     - "noun_synonym": A noun is a synonym for the column name ("number" → "No." column)
     - "value_type_match": The value's type matches the column's purpose ("Guard" is a position → Position column)
     - "preposition_signal": A preposition connects value to column ("from X" → country/location column, "in X" → year/season column)
     - "direct_mention": The column name appears directly in the question
     - "question_word_signal": The question word implies the SELECT column ("Who" → person/name, "Where" → location)
   - `column_pattern`: What kind of column names this rule applies to (e.g., "school|team|club|university")
   - `confidence`: How reliable this rule is (0.0-1.0)
   - `reasoning`: One sentence explaining WHY this mapping works

3. `select_column`: The column the question is asking about (SELECT target)
4. `select_signal`: What in the question indicates the SELECT column:
   - `signal_phrase`: The words that hint at the SELECT column
   - `signal_type`: "direct_mention" | "question_word" | "context_inference"

5. `value_boundaries`: How to identify where the value starts and ends in the question:
   - `left_boundary`: What comes right before the value ("played for [VALUE]", "number [VALUE]")
   - `right_boundary`: What comes right after the value ("[VALUE] play?", "[VALUE] in 2005")
   - `boundary_pattern`: A generalizable pattern for extracting this value type

## Output Format
Return ONLY a JSON array. No markdown, no explanation.

## IMPORTANT
- Focus on GENERALIZABLE rules, not example-specific facts
- "played for X" → school/team is a general rule. "Butler CC is a school" is not.
- The trigger_phrase should be the MINIMAL phrase needed to identify the mapping
- Think about what patterns a non-ML system could use to replicate this mapping
