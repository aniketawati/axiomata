You are labeling text-to-SQL examples with the semantic reasoning that connects the question to the correct WHERE column.

## Examples to label
{examples_json}

For EACH example, you are given: question, headers (column names), where_column (ground truth), where_value, select_column.

Provide a JSON object with:

1. `question`: (copy from input)
2. `where_column`: (copy from input)
3. `where_value`: (copy from input)

4. `why_this_column`: ONE of these reasoning categories:
   - `"value_matches_column_type"`: The value's semantic type matches the column's purpose. E.g., "Guard" is a position type → Position column.
   - `"trigger_phrase_indicates"`: A verb/preposition phrase in the question points to this column. E.g., "played for X" → school/team column.
   - `"column_name_mentioned"`: The column name (or close synonym) appears in the question near the value.
   - `"value_is_entity_name"`: The value is a proper noun and the column stores entity names. E.g., "Amir Johnson" → Player column.
   - `"process_of_elimination"`: The value doesn't obviously fit other columns; this is the best match.
   - `"number_matches_numeric_column"`: A numeric value matches a column that stores numbers of this kind.

5. `trigger_phrase`: If the reasoning is trigger-based, what EXACT phrase in the question signals the column? Must be 2+ words. null if not applicable.

6. `column_keyword`: What single word in the column name is the strongest signal? E.g., for "School/Club Team" the keyword is "team" or "school".

7. `value_span`: The EXACT substring of the question that is the WHERE value. Must appear verbatim.

8. `select_reasoning`: Why is the select_column the one being ASKED ABOUT? One of:
   - `"question_word_implies"`: "Who" → person, "Where" → location, "When" → date
   - `"column_name_after_question_word"`: "What position..." → Position
   - `"context_implies"`: The question context implies which column the answer is in

## Output Format
Return ONLY a JSON array. No markdown.

## RULES
- Be precise with value_span — it must be an exact substring of the question
- trigger_phrase must be at least 2 words
- Focus on the GENERALIZABLE reasoning, not the specific content
