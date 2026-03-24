"""
Entity-Aware Column Resolver — Uses world knowledge (entity types from
Wikidata/LLM extraction) to score value-column compatibility.

This is "probabilistic attention" — cross-relevance between value type
and column type via a compatibility table:

  P(column | entity_type(value), column_semantic_type)

The compatibility table is the equivalent of attention weights,
but as a static 2D probability lookup:

  entity_type × column_type → compatibility score

Example:
  entity_type("Rome") = "city"
  column_type("Location") = "place"
  compatibility("city", "place") = 0.95
"""

import json
import re
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent.parent / "knowledge" / "base"
SEMEXTRACT_KNOWLEDGE = Path(__file__).parent / "knowledge"

# Column semantic type classification based on column name keywords
COLUMN_SEMANTIC_TYPES = {
    "person": {"player", "name", "winner", "candidate", "person", "incumbent",
               "driver", "rider", "artist", "director", "coach", "manager",
               "captain", "author", "singer", "composer", "actor", "actress",
               "contestant", "representative", "minister", "commander",
               "doubles", "singles"},
    "place": {"location", "venue", "country", "city", "capital", "state",
              "district", "county", "ground", "stadium", "arena", "nation",
              "headquarters", "base", "region", "province", "hometown",
              "birthplace", "common", "municipality", "territory", "area"},
    "team_org": {"team", "opponent", "club", "school", "university", "college",
                 "company", "party", "network", "organization", "affiliate",
                 "carrier", "airline", "away", "home", "visitor"},
    "temporal": {"year", "season", "date", "time", "day", "month", "founded",
                 "established", "elected", "launched", "opened", "aired"},
    "numeric": {"score", "points", "goals", "attendance", "crowd", "population",
                "total", "rank", "votes", "viewers", "rating", "percentage",
                "wins", "losses", "draws"},
    "identifier": {"no", "number", "#", "episode", "week", "round", "game",
                    "pick", "code", "id"},
    "category": {"position", "result", "type", "status", "class", "division",
                 "genre", "league", "conference", "branch", "format", "role"},
}

# The "attention" table: P(compatibility | entity_type, column_type)
# This IS the cross-relevance score — what attention computes.
COMPATIBILITY_TABLE = {
    # entity_type → {column_type → score}
    "country": {"place": 0.95, "team_org": 0.30, "person": 0.02, "category": 0.10},
    "city": {"place": 0.95, "team_org": 0.15, "person": 0.02, "category": 0.05},
    "state_region": {"place": 0.90, "team_org": 0.10, "person": 0.02},
    "person": {"person": 0.90, "team_org": 0.10, "place": 0.02, "category": 0.05},
    "team": {"team_org": 0.90, "person": 0.05, "place": 0.10, "category": 0.10},
    "school": {"team_org": 0.90, "place": 0.15, "person": 0.02},
    "organization": {"team_org": 0.85, "person": 0.05, "place": 0.10},
    "position": {"category": 0.95, "person": 0.02, "team_org": 0.02},
    "sport": {"category": 0.80, "team_org": 0.15},
    "genre": {"category": 0.90},
    "status": {"category": 0.85, "numeric": 0.10},
    "year": {"temporal": 0.90, "numeric": 0.20, "identifier": 0.15},
    "number": {"numeric": 0.70, "identifier": 0.60, "temporal": 0.10},
    "language": {"category": 0.70, "place": 0.15},
    "other": {},  # no strong signal
}


class EntityResolver:
    """Resolves value to column using entity type knowledge.

    The core operation is a compatibility lookup:
      score = COMPATIBILITY_TABLE[entity_type(value)][column_type(column)]

    This is "probabilistic attention" without neural weights.
    """

    def __init__(self):
        self.entity_kb = {}    # value_lower → entity_type
        self.llm_entities = {} # from LLM classification

    def load_knowledge(self, knowledge_dir=None):
        """Load entity type knowledge bases."""
        # Load curated entity types
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "entity_types.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                for etype, entities in data.items():
                    for key, val in entities.items():
                        self.entity_kb[key.lower()] = etype

        # Load LLM-classified entities
        # Try multiple paths
        llm_candidates = [
            SEMEXTRACT_KNOWLEDGE / "entity_types_llm.json",
            Path(knowledge_dir).parent / "semextract" / "knowledge" / "entity_types_llm.json" if knowledge_dir else None,
            Path(knowledge_dir) / "entity_types_llm.json" if knowledge_dir else None,
        ]
        llm_path = None
        for p in llm_candidates:
            if p and p.exists():
                llm_path = p
                break
        if not llm_path:
            llm_path = SEMEXTRACT_KNOWLEDGE / "entity_types_llm.json"
        if llm_path.exists():
            with open(llm_path) as f:
                self.llm_entities = json.load(f)
                # Merge into main kb
                for val, etype in self.llm_entities.items():
                    self.entity_kb[val.lower()] = etype

    def get_entity_type(self, value):
        """Look up entity type for a value."""
        if not value:
            return "other"

        val_lower = value.lower().strip()

        # Direct lookup
        if val_lower in self.entity_kb:
            return self.entity_kb[val_lower]

        # Try without trailing punctuation
        val_clean = val_lower.rstrip(".,?!")
        if val_clean in self.entity_kb:
            return self.entity_kb[val_clean]

        # Number check
        try:
            float(val_clean.replace(",", "").replace("$", ""))
            if re.match(r'^\d{4}$', val_clean):
                return "year"
            return "number"
        except ValueError:
            pass

        return "other"

    def get_column_type(self, column_name):
        """Classify a column's semantic type from its name."""
        col_lower = column_name.lower()
        col_words = set(re.findall(r'\b\w+\b', col_lower))

        best_type = "other"
        best_overlap = 0

        for col_type, keywords in COLUMN_SEMANTIC_TYPES.items():
            overlap = len(col_words & keywords)
            if overlap > best_overlap:
                best_overlap = overlap
                best_type = col_type

        return best_type

    def score_compatibility(self, value, column_name):
        """Score compatibility between a value and a column.

        This is the "attention score" — how relevant is this value
        to this column, based on world knowledge.

        Returns float 0.0-1.0
        """
        entity_type = self.get_entity_type(value)
        column_type = self.get_column_type(column_name)

        # Look up in compatibility table
        type_scores = COMPATIBILITY_TABLE.get(entity_type, {})
        score = type_scores.get(column_type, 0.15)  # default: low compatibility

        return score

    def rank_columns(self, value, headers, exclude=None):
        """Rank all columns by compatibility with a value.

        Returns list of (column_name, score) sorted desc.
        """
        exclude_set = {c.lower() for c in (exclude or [])}
        scores = []
        for h in headers:
            if h.lower() in exclude_set:
                continue
            score = self.score_compatibility(value, h)
            scores.append((h, score))

        scores.sort(key=lambda x: -x[1])
        return scores


def build_llm_entity_table():
    """Compile LLM-classified entities into a lookup table."""
    SEMEXTRACT_KNOWLEDGE.mkdir(parents=True, exist_ok=True)
    oracle_dir = Path(__file__).parent / "oracle" / "dataset"

    all_entities = {}
    for f in sorted(oracle_dir.glob("entity_typed_*.json")):
        with open(f) as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                all_entities.update(data)

    if not all_entities:
        print("No LLM entity classifications found.")
        return

    with open(SEMEXTRACT_KNOWLEDGE / "entity_types_llm.json", "w") as f:
        json.dump(all_entities, f)

    from collections import Counter
    type_counts = Counter(all_entities.values())
    print(f"LLM entity table: {len(all_entities)} entities")
    for t, c in type_counts.most_common():
        print(f"  {t}: {c}")


if __name__ == "__main__":
    build_llm_entity_table()

    # Test
    er = EntityResolver()
    er.load_knowledge()
    tests = [
        ("Rome", ["Tournament", "Location", "Year", "Winner"]),
        ("Guard", ["Player", "No.", "Position", "School/Club Team"]),
        ("Duke", ["Player", "School", "Year", "Position"]),
        ("Lakers", ["Team", "Opponent", "Score", "Date"]),
        ("2005", ["Year", "Score", "Player", "Location"]),
        ("42", ["No.", "Score", "Year", "Player"]),
    ]
    for value, headers in tests:
        etype = er.get_entity_type(value)
        ranked = er.rank_columns(value, headers)
        print(f"  {value} (type={etype}) → {[(h, f'{s:.2f}') for h, s in ranked[:3]]}")
