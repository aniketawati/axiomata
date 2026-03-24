You are classifying database column headers into semantic types.

## Input
A JSON array of column header strings.

## Classify each into ONE of:
- `person`: Stores person names (Player, Winner, Name, Director, Driver, Pole Position, Singles, Doubles)
- `place`: Stores location names (Location, Venue, Country, City, Capital, Common of, Ground, Stadium)
- `team_org`: Stores team/org names (Team, Opponent, Club, School, Away team, Home team, Affiliate)
- `temporal`: Stores dates/years (Year, Season, Date, Founded, Air date, First elected)
- `numeric`: Stores numbers/scores (Score, Points, Goals, Attendance, Population, Rank, Total)
- `identifier`: Stores IDs/codes (No., Episode, Week, Round, Pick, Production code)
- `category`: Stores categorical values (Position, Result, Type, Status, Class, Division, Genre)
- `text`: Stores free text (Notes, Description, Title, Comments)
- `other`: Doesn't fit any category

## Output
Return a JSON object: {"column_name": "type", ...}

## RULES
- Use your knowledge of what these columns typically store in data tables
- "Pole Position" in racing → person (stores driver name)
- "Mixed doubles" in badminton → person (stores player names)
- "Common of" in Italian municipalities → place
- "Away team" → team_org (stores team names)
- When ambiguous, pick the most common usage
