You are extracting value span knowledge from text-to-SQL examples. For each question, identify the EXACT character span of the WHERE filter value, and explain the structural signals that mark its boundaries.

## Examples to analyze
{examples_json}

For EACH example, you are given: question, headers, where_column, where_value, select_column.

Provide:

1. `question`: (copy from input)
2. `where_value`: (copy from input)
3. `value_span_in_question`: The EXACT substring of the question that corresponds to the where_value. Must be a verbatim substring. If the value appears with different casing, use the question's casing.
4. `span_start_char`: Character index where the value starts in the question (0-based)
5. `span_end_char`: Character index where the value ends (exclusive)

6. `left_context`: What comes immediately before the value in the question (1-3 words)
7. `right_context`: What comes immediately after the value (1-3 words)

8. `start_signal`: Why does the value START at this position? One of:
   - `"after_trigger_verb"`: Value starts after a verb like "played", "directed", "scored"
   - `"after_preposition"`: Value starts after "for", "of", "in", "from", "at", "on", "by"
   - `"after_copula"`: Value starts after "is", "was", "are", "were"
   - `"after_column_name"`: Value starts after a column name mention ("Score of [VALUE]")
   - `"after_article"`: Value starts after "a", "an", "the"
   - `"at_proper_noun"`: Value starts at a capitalized word (proper noun)
   - `"at_number"`: Value starts at a digit
   - `"at_quoted"`: Value starts at a quote mark
   - `"start_of_phrase"`: Value is at the start of a clause/phrase

9. `end_signal`: Why does the value END at this position? One of:
   - `"before_verb"`: Value ends before a verb ("Butler CC [VALUE] play?")
   - `"before_question_mark"`: Value ends at "?"
   - `"before_comma_clause"`: Value ends at ", and" or ", with" etc.
   - `"before_preposition"`: Value ends before "in", "on", "at", "for"
   - `"at_end_of_proper_noun"`: Value ends when capitalized words stop
   - `"at_end_of_number"`: Value ends after last digit
   - `"before_filler"`: Value ends before function words (is, was, the, etc.)
   - `"end_of_question"`: Value runs to the end

10. `value_structure`: What type of value is this? One of:
    - `"proper_noun_sequence"`: Capitalized multi-word ("Butler CC (KS)", "Amir Johnson")
    - `"single_proper_noun"`: One capitalized word ("Guard", "Duke")
    - `"number"`: Numeric ("42", "100")
    - `"score_pattern"`: Score-like ("72-71-65=208", "4-6, 6-4, 6-3")
    - `"date_string"`: Date-like ("11 February 2008", "1996-97")
    - `"lowercase_phrase"`: Lowercase multi-word ("free agency", "did not qualify")
    - `"mixed_case"`: Mixed case with special chars ("w 48-10", "$42,845")
    - `"code_string"`: Alphanumeric code ("9ABX02", "2x Pro Bias")

## Output Format
Return ONLY a JSON array. No markdown.

## RULES
- value_span_in_question MUST be an exact substring of the question
- span_start_char and span_end_char must be correct character indices
- Focus on the STRUCTURAL signals — what general pattern identifies this value's boundaries?
- The same start_signal/end_signal categories should apply across many different values
