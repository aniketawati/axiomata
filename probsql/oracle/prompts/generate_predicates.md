You are a data generation oracle. Given a database schema, generate exactly 25
English predicate sentences that a human analyst might use to describe SQL WHERE
clause conditions, along with the corresponding SQL.

## Input Schema
{schema_json}

## Requirements

Generate 25 examples with this complexity distribution:
- 8 SIMPLE: Single condition on one column (e.g., "active users", "orders over $100")
- 7 COMPOUND: 2-3 conditions joined with AND/OR (e.g., "verified users who signed up last month")
- 5 TEMPORAL: Involve date/time reasoning (e.g., "created in the past 30 days", "before Q3 2024")
- 3 NEGATION: Involve NOT/exclusion (e.g., "users who haven't logged in", "excluding cancelled orders")
- 2 COMPLEX: 3+ conditions, possibly with nested logic or implicit joins

For EACH example, provide:

1. `english`: The natural English predicate as a non-technical person would say it
2. `target_table`: The primary table being filtered
3. `sql_where`: The exact SQL WHERE clause (just the clause, no SELECT/FROM)
4. `requires_join`: true/false — does this need a JOIN to resolve?
5. `join_clause`: If requires_join is true, the JOIN clause needed. null otherwise.
6. `latent_variables`: An object containing:
   - `predicate_type`: one of ["simple", "compound", "temporal", "negation", "complex"]
   - `columns_referenced`: list of "table.column" strings
   - `operators_used`: list of SQL operators (=, >, <, >=, <=, !=, LIKE, IN, IS NULL, IS NOT NULL, BETWEEN, NOT)
   - `conjunction_type`: one of ["none", "and", "or", "mixed", "nested"]
   - `has_temporal`: true/false
   - `temporal_type`: one of [null, "relative_to_now", "absolute_date", "relative_to_column", "date_range"]
   - `temporal_expression`: the English temporal phrase if any, null otherwise
   - `has_negation`: true/false
   - `negation_scope`: one of [null, "operator", "clause", "existence"]
   - `value_types`: list of value types present: ["string_literal", "number", "date", "boolean", "null", "enum", "computed"]
   - `ambiguity_notes`: string describing any ambiguity in the English (or "none")

## Output Format
Return ONLY a JSON array of 25 objects. No markdown, no explanation, no preamble.
Each object has the fields listed above.

## Important Rules
- Use realistic, natural English — not SQL-like English. Say "expensive products" not "products where price > 100".
- Temporal expressions should use natural language: "last month", "this year", "past 2 weeks", "since January", "before their first order"
- Use the ACTUAL column names and table names from the provided schema
- Assume the current date is 2025-01-15 for any relative temporal references
- For ENUM columns, use the actual enum values from the schema
- SQL must be syntactically valid and use the correct column/table names
- Include a variety of SQL operators, not just = and >
- Make the English sound like something a product manager or analyst would say in a Slack message
