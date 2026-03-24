You are annotating question tokens with their semantic roles for training a Hidden Markov Model.

## Examples to annotate
{examples_json}

For EACH example, you are given: question, headers (column names), where_column (correct WHERE column), where_value, select_column.

Label EVERY token in the question with one of these roles:
- `QWORD`: Question words (What, Who, Where, When, How, Which, Name, List)
- `SELECT_HINT`: Words that indicate the SELECT/answer column (e.g., "position" in "What position does...")
- `FILLER`: Function words, articles, verbs connecting the question (is, are, the, does, did, of, for, etc.)
- `TRIGGER`: Verbs/prepositions that indicate the WHERE column relationship (played for, wears, directed by, from, against, etc.)
- `VALUE`: Words that are part of the WHERE filter value (Butler CC, 42, Guard, etc.)
- `CONTEXT`: Other contextual words that don't fit the above categories

## Output Format

Return a JSON array where each element is:
```json
{
  "question": "What position does the player who played for Butler CC (KS) play?",
  "tokens": [
    {"token": "What", "role": "QWORD"},
    {"token": "position", "role": "SELECT_HINT"},
    {"token": "does", "role": "FILLER"},
    {"token": "the", "role": "FILLER"},
    {"token": "player", "role": "FILLER"},
    {"token": "who", "role": "FILLER"},
    {"token": "played", "role": "TRIGGER"},
    {"token": "for", "role": "TRIGGER"},
    {"token": "Butler", "role": "VALUE"},
    {"token": "CC", "role": "VALUE"},
    {"token": "(KS)", "role": "VALUE"},
    {"token": "play", "role": "FILLER"}
  ]
}
```

## RULES
- Tokenize by splitting on whitespace and keeping punctuation attached to words
- VALUE tokens must correspond to the where_value — the actual filter value
- SELECT_HINT tokens should be words that hint at the select_column
- TRIGGER tokens are verbs/prepositions that semantically link to the WHERE column (not just any verb)
- FILLER is the default for function words, articles, and generic verbs
- Be consistent: the same word in the same context should always get the same role
- Multi-word values (proper nouns, dates) should have EACH word labeled as VALUE
