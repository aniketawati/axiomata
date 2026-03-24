"""
Flat Schema Generator — Generates Wikipedia-style single-table schemas
with human-readable column names.

Covers the gap identified by WikiSQL benchmark: our original schemas
are all developer-style (snake_case, multi-table, FK relationships).
Real-world tables often have human-readable headers with spaces,
special characters, and capitalization.

Usage: python probsql/oracle/flat_schema_generator.py
"""

import json
import random
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
SCHEMAS_DIR = SCRIPT_DIR / "schemas"

# Domains modeled after actual WikiSQL/Wikipedia table patterns
FLAT_DOMAINS = {
    "sports_roster": {
        "templates": [
            {
                "headers": ["Player", "No.", "Nationality", "Position", "Years", "School/Club Team"],
                "types": ["text", "text", "text", "text", "text", "text"],
            },
            {
                "headers": ["Player", "Position", "Team", "Goals", "Assists", "Points"],
                "types": ["text", "text", "text", "real", "real", "real"],
            },
            {
                "headers": ["Name", "Number", "Position", "Height", "Weight", "College"],
                "types": ["text", "text", "text", "text", "text", "text"],
            },
            {
                "headers": ["Player", "Country", "Position", "Caps", "Goals", "Club"],
                "types": ["text", "text", "text", "real", "real", "text"],
            },
        ],
    },
    "sports_results": {
        "templates": [
            {
                "headers": ["Date", "Opponent", "Score", "Result", "Record", "Attendance"],
                "types": ["text", "text", "text", "text", "text", "real"],
            },
            {
                "headers": ["Week", "Date", "Opponent", "Result", "Score", "Venue", "Attendance"],
                "types": ["real", "text", "text", "text", "text", "text", "real"],
            },
            {
                "headers": ["Game", "Date", "Team", "Score", "High points", "High rebounds", "High assists", "Location"],
                "types": ["real", "text", "text", "text", "text", "text", "text", "text"],
            },
            {
                "headers": ["Round", "Date", "Home team", "Home team score", "Away team", "Away team score", "Venue", "Crowd"],
                "types": ["text", "text", "text", "text", "text", "text", "text", "real"],
            },
        ],
    },
    "geography": {
        "templates": [
            {
                "headers": ["Country", "Capital", "Population", "Area (km²)", "Official Language", "Currency"],
                "types": ["text", "text", "real", "real", "text", "text"],
            },
            {
                "headers": ["City", "State/Province", "Country", "Population", "Elevation (m)", "Time Zone"],
                "types": ["text", "text", "text", "real", "real", "text"],
            },
            {
                "headers": ["County", "Seat", "Population (2010)", "Area (sq mi)", "Founded"],
                "types": ["text", "text", "real", "real", "text"],
            },
            {
                "headers": ["District", "Headquarters", "Population", "Area (km²)", "Density (/km²)"],
                "types": ["text", "text", "real", "real", "real"],
            },
        ],
    },
    "politics_elections": {
        "templates": [
            {
                "headers": ["Candidate", "Party", "Votes", "Percentage", "Result"],
                "types": ["text", "text", "real", "text", "text"],
            },
            {
                "headers": ["District", "Incumbent", "Party", "First elected", "Result", "Candidates"],
                "types": ["text", "text", "text", "text", "text", "text"],
            },
            {
                "headers": ["Year", "Office", "Winner", "Party", "Margin", "Runner-up"],
                "types": ["text", "text", "text", "text", "text", "text"],
            },
            {
                "headers": ["Name", "Title/Office", "Term Start", "Term End", "Party", "State"],
                "types": ["text", "text", "text", "text", "text", "text"],
            },
        ],
    },
    "entertainment": {
        "templates": [
            {
                "headers": ["Title", "Director", "Year", "Genre", "Box Office ($M)", "Rating"],
                "types": ["text", "text", "text", "text", "real", "text"],
            },
            {
                "headers": ["Episode", "Title", "Directed by", "Written by", "Original air date", "Viewers (millions)"],
                "types": ["text", "text", "text", "text", "text", "real"],
            },
            {
                "headers": ["Song", "Artist", "Album", "Year", "Peak Position", "Weeks on Chart"],
                "types": ["text", "text", "text", "text", "real", "real"],
            },
            {
                "headers": ["Season", "Episodes", "Premiere Date", "Finale Date", "Network", "Viewers (millions)"],
                "types": ["text", "real", "text", "text", "text", "real"],
            },
        ],
    },
    "education": {
        "templates": [
            {
                "headers": ["School", "Location", "Founded", "Enrollment", "Type", "Conference"],
                "types": ["text", "text", "text", "real", "text", "text"],
            },
            {
                "headers": ["University", "City", "State", "Tuition ($)", "Acceptance Rate", "Ranking"],
                "types": ["text", "text", "text", "real", "text", "real"],
            },
            {
                "headers": ["Course", "Instructor", "Department", "Credits", "Enrollment", "Room"],
                "types": ["text", "text", "text", "real", "real", "text"],
            },
        ],
    },
    "transportation": {
        "templates": [
            {
                "headers": ["Station", "Line", "Location", "Opened", "Passengers (daily)", "Zone"],
                "types": ["text", "text", "text", "text", "real", "text"],
            },
            {
                "headers": ["Airport", "City", "Country", "IATA Code", "Passengers (2019)", "Runways"],
                "types": ["text", "text", "text", "text", "real", "real"],
            },
            {
                "headers": ["Route", "Origin", "Destination", "Distance (km)", "Operator", "Frequency"],
                "types": ["text", "text", "text", "real", "text", "text"],
            },
        ],
    },
    "science_tech": {
        "templates": [
            {
                "headers": ["Element", "Symbol", "Atomic Number", "Atomic Weight", "Category", "Discovery Year"],
                "types": ["text", "text", "real", "real", "text", "text"],
            },
            {
                "headers": ["Model", "Manufacturer", "Year", "Price ($)", "Rating", "Category"],
                "types": ["text", "text", "text", "real", "text", "text"],
            },
            {
                "headers": ["Species", "Family", "Conservation Status", "Population", "Habitat", "Region"],
                "types": ["text", "text", "text", "text", "text", "text"],
            },
        ],
    },
    "rankings_awards": {
        "templates": [
            {
                "headers": ["Rank", "Name", "Country", "Score", "Prize Money ($)", "Year"],
                "types": ["real", "text", "text", "real", "real", "text"],
            },
            {
                "headers": ["Year", "Winner", "Category", "Film/Work", "Country"],
                "types": ["text", "text", "text", "text", "text"],
            },
            {
                "headers": ["Position", "Company", "Industry", "Revenue ($B)", "Employees", "Headquarters"],
                "types": ["real", "text", "text", "real", "real", "text"],
            },
        ],
    },
    "military_history": {
        "templates": [
            {
                "headers": ["Ship", "Class", "Launched", "Displacement (tons)", "Status", "Fleet"],
                "types": ["text", "text", "text", "real", "text", "text"],
            },
            {
                "headers": ["Unit", "Branch", "Base", "Established", "Personnel", "Commander"],
                "types": ["text", "text", "text", "text", "real", "text"],
            },
            {
                "headers": ["Battle", "Date", "Location", "Result", "Casualties", "Commander"],
                "types": ["text", "text", "text", "text", "text", "text"],
            },
        ],
    },
}

SCHEMAS_PER_DOMAIN = 10  # 10 domains × 10 = 100 flat schemas


def generate_flat_schemas():
    """Generate 100 flat Wikipedia-style schemas."""
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(99)
    manifest_entries = []

    for domain, config in FLAT_DOMAINS.items():
        templates = config["templates"]
        for i in range(1, SCHEMAS_PER_DOMAIN + 1):
            template = templates[(i - 1) % len(templates)]
            schema_id = f"flat_{domain}_{i:03d}"
            table_name = f"table_{domain}_{i:03d}"

            # Optionally add/remove a column for variety
            headers = list(template["headers"])
            types = list(template["types"])
            if rng.random() > 0.5 and len(headers) > 5:
                # Randomly drop one optional column
                idx = rng.randint(2, len(headers) - 1)
                headers.pop(idx)
                types.pop(idx)

            columns = []
            for h, t in zip(headers, types):
                columns.append({
                    "name": h,
                    "type": "TEXT" if t == "text" else "REAL",
                })

            schema = {
                "schema_id": schema_id,
                "domain": f"flat_{domain}",
                "schema_style": "flat",
                "tables": [{
                    "name": table_name,
                    "columns": columns,
                }],
                "relationships": [],
            }

            out_path = SCHEMAS_DIR / f"{schema_id}.json"
            with open(out_path, "w") as f:
                json.dump(schema, f, indent=2)

            manifest_entries.append({"schema_id": schema_id, "domain": f"flat_{domain}"})

    # Update manifest
    manifest_path = SCHEMAS_DIR / "_manifest.json"
    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest = json.load(f)
    else:
        manifest = {"total": 0, "schemas": []}

    manifest["schemas"].extend(manifest_entries)
    manifest["total"] = len(manifest["schemas"])
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"Generated {len(manifest_entries)} flat schemas across {len(FLAT_DOMAINS)} domains")
    return manifest_entries


if __name__ == "__main__":
    generate_flat_schemas()
