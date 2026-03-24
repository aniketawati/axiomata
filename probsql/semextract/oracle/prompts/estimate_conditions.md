You are analyzing text-to-SQL questions to determine how many WHERE conditions they require.

## Examples to analyze
{examples_json}

For EACH example, you are given: question, headers (column names).

Determine how many separate WHERE conditions this question needs, and explain why.

Provide:

1. `question`: (copy from input)
2. `n_conditions`: Integer 1-4. How many separate column=value conditions are needed?
3. `reasoning`: One sentence explaining why this many conditions.
4. `condition_signals`: List of signals in the question that indicate separate conditions:
   - `"explicit_and"`: "X and Y" connecting two conditions
   - `"comma_separated"`: "X, Y" listing conditions
   - `"with_clause"`: "with a X of Y" adding a condition
   - `"when_clause"`: "when X is Y" adding a condition
   - `"implicit_single"`: No conjunction signals → single condition
   - `"comparative"`: "more than X" adds a numeric condition
   - `"temporal_qualifier"`: "in 2005" adds a year/date condition
5. `question_features`:
   - `has_and`: boolean — contains "and" connecting clauses (not "and" within a value)
   - `has_comma_clause`: boolean — contains ", " separating conditions
   - `has_with`: boolean — contains "with a/an/the" introducing a condition
   - `has_when`: boolean — contains "when/where" introducing a condition
   - `word_count`: integer — total words in question
   - `column_mentions`: integer — how many column names from headers appear in the question

## Output Format
Return ONLY a JSON array. No markdown.

## RULES
- Count the MINIMUM number of conditions needed, not the maximum
- "What X has a Y of Z and a W of V?" → 2 conditions (Y=Z AND W=V)
- "What X is Y?" → 1 condition
- "What player played guard for toronto in 1996-97?" → 2 conditions (Position=Guard AND Years=1996-97)
- Don't count the SELECT column as a condition — "What position..." doesn't add a condition
