"""
Column Matcher — Maps English phrases to database column names.

The MOST IMPORTANT component. "Users who signed up recently" must match
"signed up" to the created_at column.

Uses:
1. Semantic expansion dictionary (English phrases → column name patterns)
2. TF-IDF cosine similarity (from scratch, no libraries)
3. Type compatibility scoring
4. Column name pattern bonuses
"""

import json
import math
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

KNOWLEDGE_DIR = Path(__file__).parent.parent / "knowledge" / "base"


@dataclass
class ColumnCandidate:
    table_name: str
    column_name: str
    column_type: str
    score: float
    enum_values: list = field(default_factory=list)
    breakdown: dict = field(default_factory=dict)

    @property
    def full_name(self):
        return f"{self.table_name}.{self.column_name}"


class ColumnMatcher:
    def __init__(self):
        self.semantic_map = {}      # column_pattern -> [english_phrases]
        self.type_compat = {}       # (english_type, sql_type) -> score
        self.idf_weights = {}       # token -> idf weight
        self.corpus_size = 0
        self.weights = [0.35, 0.25, 0.20, 0.20]  # w1-w4 for scoring

    def load_knowledge(self, knowledge_dir=None):
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        self._load_semantic_map(kdir)
        self._load_type_compatibility(kdir)
        self._load_tfidf(kdir)
        self._build_reverse_index()

    def _load_semantic_map(self, kdir):
        path = kdir / "column_semantic_map.json"
        if path.exists():
            with open(path) as f:
                self.semantic_map = json.load(f)
        else:
            self.semantic_map = self._default_semantic_map()

    def _load_type_compatibility(self, kdir):
        path = kdir / "type_compatibility.json"
        if path.exists():
            with open(path) as f:
                self.type_compat = json.load(f)
        else:
            self.type_compat = self._default_type_compat()

    def _build_reverse_index(self):
        """Build reverse index: phrase_keyword -> set of column patterns that contain it."""
        self._phrase_to_patterns = {}  # phrase_word -> [(pattern, phrase)]
        for pattern, phrases in self.semantic_map.items():
            for phrase in phrases:
                phrase_lower = phrase.lower()
                for word in phrase_lower.split():
                    if len(word) > 2:
                        self._phrase_to_patterns.setdefault(word, []).append((pattern, phrase_lower))

    def _load_tfidf(self, kdir):
        path = kdir / "tfidf_vectors.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.idf_weights = data.get("idf_weights", {})
                self.corpus_size = data.get("corpus_size", 0)

    def match(self, english_phrase, schema):
        """Match an English phrase to the best column(s) in the schema.

        Returns list of ColumnCandidate sorted by score descending.
        """
        candidates = self._get_all_columns(schema)
        phrase_lower = english_phrase.lower()
        phrase_tokens = self._tokenize(phrase_lower)

        scored = []
        for cand in candidates:
            s1 = self._keyword_match_score(phrase_lower, phrase_tokens, cand)
            s2 = self._tfidf_cosine(phrase_tokens, cand)
            s3 = self._type_compatibility_score(phrase_lower, cand)
            s4 = self._pattern_bonus(phrase_lower, phrase_tokens, cand)

            w = self.weights
            total = w[0] * s1 + w[1] * s2 + w[2] * s3 + w[3] * s4
            cand.score = total
            cand.breakdown = {"keyword": s1, "tfidf": s2, "type_compat": s3, "pattern": s4}
            scored.append(cand)

        scored.sort(key=lambda c: c.score, reverse=True)
        return scored[:5]

    def _get_all_columns(self, schema):
        """Extract all columns from schema as ColumnCandidate objects."""
        candidates = []
        tables = schema.get("tables", [])
        for table in tables:
            for col in table.get("columns", []):
                candidates.append(ColumnCandidate(
                    table_name=table["name"],
                    column_name=col["name"],
                    column_type=col.get("type", "VARCHAR"),
                    score=0.0,
                    enum_values=col.get("enum_values", []),
                ))
        return candidates

    def _keyword_match_score(self, phrase_lower, phrase_tokens, cand):
        """Score based on semantic expansion dictionary match."""
        col_name = cand.column_name
        best_score = 0.0

        # Direct column name in phrase (handles both "created_at" and "School/Club Team")
        col_lower = col_name.lower()
        if col_lower in phrase_lower:
            best_score = max(best_score, 1.0)
        elif col_name.replace("_", " ").lower() in phrase_lower:
            best_score = max(best_score, 1.0)

        # Tokenize column name — handle both snake_case and human-readable
        if "_" in col_name and " " not in col_name:
            col_tokens = col_name.lower().split("_")
        else:
            col_tokens = re.findall(r'\b[a-z]+\b', col_lower)

        # Check each token of column name
        skip_tokens = {"id", "at", "is", "has", "of", "the", "and", "in", "for", "a", "an"}
        meaningful_col_tokens = [t for t in col_tokens if t not in skip_tokens and len(t) > 1]
        if meaningful_col_tokens:
            col_token_matches = sum(1 for t in meaningful_col_tokens if t in phrase_tokens)
            token_ratio = col_token_matches / len(meaningful_col_tokens)
            best_score = max(best_score, min(token_ratio, 1.0))

        # Partial column name match for multi-word headers
        # e.g., "Home team" matches question containing "home team" or just "team"
        if " " in col_name and len(meaningful_col_tokens) >= 2:
            for token in meaningful_col_tokens:
                if len(token) >= 4 and token in phrase_tokens:
                    best_score = max(best_score, 0.6)

        # Check semantic expansion via reverse index
        if hasattr(self, '_phrase_to_patterns'):
            for token in phrase_tokens:
                for pattern, sem_phrase in self._phrase_to_patterns.get(token, []):
                    if self._col_matches_pattern(col_name, pattern) and sem_phrase in phrase_lower:
                        best_score = max(best_score, 0.9)
                        break
                if best_score >= 0.9:
                    break
        else:
            for pattern, phrases in self.semantic_map.items():
                if self._col_matches_pattern(col_name, pattern):
                    for phrase in phrases:
                        if phrase.lower() in phrase_lower:
                            best_score = max(best_score, 0.9)
                            break

        # Check enum values
        for ev in cand.enum_values:
            if ev.lower() in phrase_lower:
                best_score = max(best_score, 0.85)
                break

        return best_score

    def _col_matches_pattern(self, col_name, pattern):
        """Check if a column name matches a pattern like 'created_at', '*_at', 'is_*'."""
        if pattern == col_name:
            return True
        if pattern.startswith("*") and col_name.endswith(pattern[1:]):
            return True
        if pattern.endswith("*") and col_name.startswith(pattern[:-1]):
            return True
        # Generic name patterns
        base = col_name.rstrip("_").split("_")
        if any(b == pattern for b in base):
            return True
        return False

    def _tfidf_cosine(self, phrase_tokens, cand):
        """Compute TF-IDF cosine similarity between phrase and column."""
        col_tokens = cand.column_name.split("_") + [cand.table_name]
        col_tokens_set = set(t.lower() for t in col_tokens if t)

        # Fast path: check overlap first
        overlap = phrase_tokens & col_tokens_set
        if not overlap:
            return 0.0

        # Simplified: weighted overlap ratio
        default_idf = math.log(max(self.corpus_size, 100))
        score = sum(self.idf_weights.get(t, default_idf) for t in overlap)
        max_possible = sum(self.idf_weights.get(t, default_idf) for t in col_tokens_set) or 1
        return min(score / max_possible, 1.0)

    def _type_compatibility_score(self, phrase_lower, cand):
        """Score based on inferred English type vs SQL column type."""
        inferred = self._infer_english_type(phrase_lower)
        col_type = cand.column_type.upper()

        compat_map = {
            ("temporal", "TIMESTAMP"): 1.0,
            ("temporal", "DATE"): 1.0,
            ("numeric", "INT"): 0.9,
            ("numeric", "BIGINT"): 0.9,
            ("numeric", "FLOAT"): 0.9,
            ("numeric", "DECIMAL"): 0.9,
            ("boolean", "BOOLEAN"): 1.0,
            ("string", "VARCHAR"): 0.7,
            ("string", "TEXT"): 0.7,
            ("enum", "VARCHAR"): 0.8,
            ("monetary", "DECIMAL"): 1.0,
            ("monetary", "FLOAT"): 0.7,
        }

        # Normalize col_type
        base_type = col_type.split("(")[0]

        for (et, st), score in compat_map.items():
            if et == inferred and base_type.startswith(st):
                return score

        # Check type_compat from loaded knowledge
        key = f"{inferred}|{base_type}"
        if key in self.type_compat:
            return self.type_compat[key]

        return 0.3  # default low compatibility

    def _infer_english_type(self, phrase):
        """Infer what type of value the English phrase is describing."""
        temporal_words = {"today", "yesterday", "week", "month", "year", "date", "time",
                         "ago", "since", "before", "after", "recent", "recently", "past",
                         "last", "this", "next", "january", "february", "march", "april",
                         "may", "june", "july", "august", "september", "october", "november",
                         "december", "q1", "q2", "q3", "q4"}
        monetary_words = {"$", "dollar", "price", "cost", "amount", "revenue", "spent",
                         "paid", "fee", "charge", "balance", "salary", "budget", "worth",
                         "value", "expensive", "cheap", "affordable"}
        boolean_words = {"active", "inactive", "verified", "unverified", "enabled", "disabled",
                        "true", "false", "yes", "no", "available", "unavailable", "premium",
                        "featured", "published", "private", "public", "remote"}
        numeric_words = {"more than", "less than", "at least", "at most", "greater", "fewer",
                        "over", "under", "above", "below", "between", "count", "number",
                        "quantity", "rating", "score", "total", "average"}

        phrase_words = set(phrase.split())

        if any(w in phrase for w in monetary_words):
            return "monetary"
        if phrase_words & temporal_words:
            return "temporal"
        if phrase_words & boolean_words:
            return "boolean"
        if any(w in phrase for w in numeric_words) or re.search(r'\d+', phrase):
            return "numeric"
        return "string"

    def _pattern_bonus(self, phrase_lower, phrase_tokens, cand):
        """Bonus score based on column naming patterns."""
        col = cand.column_name
        col_type = cand.column_type.upper()
        score = 0.0

        # Temporal columns get bonus for temporal phrases
        temporal_indicators = {"ago", "since", "before", "after", "recent", "recently",
                              "last", "past", "week", "month", "year", "today", "yesterday",
                              "date", "time", "when", "during"}
        if (col.endswith("_at") or col.endswith("_date") or "DATE" in col_type or "TIMESTAMP" in col_type):
            if phrase_tokens & temporal_indicators:
                score = max(score, 0.8)

        # Boolean columns for boolean-like phrases
        boolean_phrases = {"active", "inactive", "verified", "available", "enabled",
                          "premium", "featured", "published", "remote", "deleted",
                          "private", "public", "paid", "free"}
        if col.startswith("is_") or col.startswith("has_"):
            adj = col.split("_", 1)[1] if "_" in col else ""
            if adj in phrase_lower:
                score = max(score, 0.95)
            if phrase_tokens & boolean_phrases:
                score = max(score, 0.5)

        # Status/enum columns for status-like phrases
        if col == "status" or col == "type" or col == "role" or col == "tier" or col == "level":
            for ev in cand.enum_values:
                if ev.lower() in phrase_lower:
                    score = max(score, 0.95)

        # ID columns should be deprioritized unless explicitly referenced
        if col.endswith("_id") or col == "id":
            if col not in phrase_lower:
                score = max(score - 0.3, 0.0)

        # Name columns for name-like phrases
        if col in ("name", "title", "email", "username", "display_name"):
            name_indicators = {"named", "called", "name", "email", "titled"}
            if phrase_tokens & name_indicators:
                score = max(score, 0.7)

        return score

    def _tokenize(self, text):
        """Tokenize text into words."""
        return set(re.findall(r'\b[a-z]+\b', text.lower()))

    @staticmethod
    def _default_semantic_map():
        """Default semantic expansion dictionary."""
        return {
            "*_at": ["created", "signed up", "registered", "joined", "started", "posted",
                     "submitted", "modified", "changed", "updated", "last", "recent",
                     "logged in", "purchased", "ordered", "scheduled", "completed",
                     "shipped", "delivered", "cancelled", "expired"],
            "created_at": ["signed up", "registered", "joined", "created", "since", "started",
                          "new", "recent", "age"],
            "updated_at": ["modified", "changed", "edited", "updated", "last changed"],
            "deleted_at": ["deleted", "removed", "archived"],
            "total_amount": ["spent", "total", "worth", "cost", "amount", "value", "priced",
                            "revenue", "expensive", "cheap", "order value"],
            "price": ["costs", "priced", "expensive", "cheap", "affordable", "worth",
                     "budget", "premium"],
            "amount": ["amount", "spent", "paid", "charged", "cost", "value", "worth"],
            "balance": ["balance", "funds", "money", "remaining"],
            "salary": ["salary", "compensation", "pay", "earning", "income", "wage"],
            "status": ["active", "inactive", "status", "state", "currently", "pending",
                      "cancelled", "completed", "suspended", "expired"],
            "is_active": ["active", "enabled", "live"],
            "is_verified": ["verified", "confirmed", "validated", "checked"],
            "is_premium": ["premium", "paid", "pro", "upgraded"],
            "is_featured": ["featured", "highlighted", "promoted"],
            "email": ["email", "email address", "mail"],
            "name": ["name", "called", "named", "titled"],
            "phone": ["phone", "telephone", "mobile", "cell"],
            "address": ["address", "location", "where they live"],
            "rating": ["rating", "rated", "stars", "score", "review score"],
            "quantity": ["quantity", "count", "number of", "how many"],
            "description": ["description", "about", "details"],
            "city": ["city", "town", "located in"],
            "country": ["country", "nation", "from"],
            "role": ["role", "position", "job"],
            "type": ["type", "kind", "category", "classification"],
            "category": ["category", "group", "classification", "type"],
            "department": ["department", "team", "division", "unit"],
        }

    @staticmethod
    def _default_type_compat():
        """Default type compatibility scores."""
        return {
            "temporal|TIMESTAMP": 1.0,
            "temporal|DATE": 1.0,
            "numeric|INT": 0.9,
            "numeric|BIGINT": 0.9,
            "numeric|FLOAT": 0.85,
            "numeric|DECIMAL": 0.9,
            "monetary|DECIMAL": 1.0,
            "monetary|FLOAT": 0.7,
            "monetary|INT": 0.5,
            "boolean|BOOLEAN": 1.0,
            "string|VARCHAR": 0.7,
            "string|TEXT": 0.7,
            "enum|VARCHAR": 0.85,
        }


def build_knowledge_from_oracle(oracle_path, output_dir=None):
    """Extract column matching knowledge from oracle dataset."""
    output_dir = Path(output_dir) if output_dir else KNOWLEDGE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(oracle_path) as f:
        data = json.load(f)
    examples = data if isinstance(data, list) else data.get("examples", [])

    # Build semantic map from oracle data
    col_to_phrases = {}
    token_doc_freq = Counter()
    total_docs = 0

    for ex in examples:
        english = ex.get("english", "")
        cols = ex.get("latent_variables", {}).get("columns_referenced", [])
        tokens = set(re.findall(r'\b[a-z]+\b', english.lower()))
        total_docs += 1

        for t in tokens:
            token_doc_freq[t] += 1

        for col_ref in cols:
            parts = col_ref.split(".")
            if len(parts) == 2:
                col_name = parts[1]
                col_to_phrases.setdefault(col_name, []).append(english.lower())

    # Compute IDF
    idf = {}
    for token, df in token_doc_freq.items():
        idf[token] = math.log((total_docs + 1) / (df + 1))

    # Save TF-IDF data
    tfidf_path = output_dir / "tfidf_vectors.json"
    with open(tfidf_path, "w") as f:
        json.dump({"idf_weights": idf, "corpus_size": total_docs}, f)

    # Build semantic map from collected phrases
    semantic_map = ColumnMatcher._default_semantic_map()
    for col_name, phrases in col_to_phrases.items():
        # Extract distinctive words for this column
        word_counts = Counter()
        for phrase in phrases:
            for word in re.findall(r'\b[a-z]+\b', phrase):
                if len(word) > 2 and word not in ("the", "and", "for", "with", "that", "this", "are", "was", "were"):
                    word_counts[word] += 1
        top_words = [w for w, c in word_counts.most_common(15) if c >= 2]
        if top_words:
            existing = semantic_map.get(col_name, [])
            semantic_map[col_name] = list(set(existing + top_words))

    sem_path = output_dir / "column_semantic_map.json"
    with open(sem_path, "w") as f:
        json.dump(semantic_map, f, indent=2)

    print(f"Column matching knowledge saved to {output_dir}")
    print(f"  Semantic map: {len(semantic_map)} column patterns")
    print(f"  TF-IDF: {len(idf)} tokens, {total_docs} documents")

    return semantic_map, idf


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        build_knowledge_from_oracle(sys.argv[1])
    else:
        print("Usage: python column_matcher.py <oracle_dataset_path>")
        print("Or import and use ColumnMatcher class directly.")
