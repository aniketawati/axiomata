You are extracting world knowledge about entity types. For common entities that appear in text-to-SQL questions, classify what TYPE each entity is.

## Task

For each entity listed below, provide its type. This is WORLD KNOWLEDGE — you know that Rome is a city, Guard is a sports position, Duke is a university, etc.

## Entity types (pick one):
- `person`: Individual human names (Michael Jordan, Jane Smith)
- `city`: City names (Rome, Paris, Tokyo, New York)
- `country`: Country names (France, Japan, Brazil, United States)
- `state_region`: State/province/region (California, Ontario, Bavaria)
- `team`: Sports team names (Lakers, 49ers, Manchester United, Red Sox)
- `school`: School/university names (Duke, Harvard, Butler CC, MIT)
- `position`: Sports/job position (Guard, Forward, Center, Midfielder)
- `sport`: Sport names (Football, Basketball, Tennis, Cricket)
- `genre`: Genre/category (Action, Comedy, Drama, Rock, Jazz)
- `status`: Status values (Active, Retired, Deceased, Win, Loss)
- `year`: Year values (2005, 1996-97)
- `number`: Plain numbers (42, 100, 3.5)
- `language`: Language names (English, Spanish, French)
- `organization`: Non-sports organizations (UN, NATO, Google, Apple)
- `other`: Anything else

## Input
{entities_json}

## Output Format
Return a JSON object mapping each entity to its type:
```json
{"Rome": "city", "Guard": "position", "Duke": "school", "Lakers": "team"}
```

No markdown, just the JSON object.

## RULES
- Use your world knowledge — don't just guess from the string format
- An entity can have multiple possible types (e.g., "Jordan" could be person or country) — pick the MOST COMMON usage in the context of data tables
- Short abbreviations: use context ("SF" → city for San Francisco, or team for San Francisco 49ers)
- If genuinely ambiguous, pick the most common usage
