"""
Conjunction Parser — Splits compound English predicates into a tree of atomic predicates.

"active users who signed up last month and have placed at least 3 orders"
→ AND(AND("active users", "signed up last month"), "placed at least 3 orders")
"""

import re
from dataclasses import dataclass
from typing import Union


@dataclass
class LeafPredicate:
    """An atomic, unsplit predicate phrase."""
    text: str

    def __repr__(self):
        return f'Leaf("{self.text}")'


@dataclass
class ConjunctionNode:
    """A compound predicate with AND/OR."""
    conjunction: str  # "AND" or "OR"
    left: 'PredicateTree'
    right: 'PredicateTree'

    def __repr__(self):
        return f'{self.conjunction}({self.left}, {self.right})'


PredicateTree = Union[LeafPredicate, ConjunctionNode]


# Conjunction patterns ordered by specificity
CONJUNCTION_PATTERNS = [
    (r'\s+and\s+also\s+', 'AND'),
    (r'\s+as\s+well\s+as\s+', 'AND'),
    (r'\s+in\s+addition\s+to\s+', 'AND'),
    (r'\s+but\s+also\s+', 'AND'),
    (r'\s+or\s+', 'OR'),
    (r'\s+but\s+', 'AND'),
    (r'\s+and\s+', 'AND'),
]

# Phrases that indicate this is still one atomic predicate (don't split)
NO_SPLIT_PATTERNS = [
    r'\bbetween\s+\S+\s+and\s+\S+',  # "between X and Y"
    r'\bboth\s+\S+\s+and\s+\S+',  # "both X and Y" (could be IN list)
]


class ConjunctionParser:
    def parse(self, english_text):
        """Parse English text into a predicate tree.

        Args:
            english_text: The English predicate sentence

        Returns:
            PredicateTree (LeafPredicate or ConjunctionNode)
        """
        text = english_text.strip()
        if not text:
            return LeafPredicate("")

        return self._parse_recursive(text)

    def _parse_recursive(self, text):
        """Recursively split text at conjunction points."""
        text = text.strip()

        # Check for "either X or Y" pattern
        either_match = re.match(r'^either\s+(.+?)\s+or\s+(.+)$', text, re.IGNORECASE)
        if either_match:
            left = self._parse_recursive(either_match.group(1))
            right = self._parse_recursive(either_match.group(2))
            return ConjunctionNode("OR", left, right)

        # Try each conjunction pattern
        for pattern, conj_type in CONJUNCTION_PATTERNS:
            parts = self._smart_split(text, pattern)
            if parts and len(parts) == 2:
                left_text, right_text = parts
                if self._is_valid_split(left_text, right_text, text):
                    left = self._parse_recursive(left_text)
                    right = self._parse_recursive(right_text)
                    return ConjunctionNode(conj_type, left, right)

        # Try comma-separated splitting (implicit AND)
        if ", " in text:
            parts = self._comma_split(text)
            if parts and len(parts) >= 2:
                return self._build_tree(parts, "AND")

        # Try "who/that/which" relative clause splitting
        rel_parts = self._relative_clause_split(text)
        if rel_parts and len(rel_parts) >= 2:
            return self._build_tree(rel_parts, "AND")

        # Base case: atomic predicate
        return LeafPredicate(text)

    def _smart_split(self, text, pattern):
        """Split text at a conjunction pattern, respecting no-split zones."""
        # Check if the text contains a no-split pattern at the conjunction point
        for ns_pattern in NO_SPLIT_PATTERNS:
            if re.search(ns_pattern, text, re.IGNORECASE):
                # Check if the conjunction is within the no-split zone
                ns_match = re.search(ns_pattern, text, re.IGNORECASE)
                conj_match = re.search(pattern, text, re.IGNORECASE)
                if conj_match and ns_match:
                    if ns_match.start() <= conj_match.start() <= ns_match.end():
                        return None

        # Find all matches
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        if not matches:
            return None

        # Use the last match to split (handles "X and Y and Z" → "X and Y" | "Z")
        # Actually, use the first match for simpler left-to-right parsing
        m = matches[0]
        left = text[:m.start()].strip()
        right = text[m.end():].strip()

        if left and right:
            return [left, right]
        return None

    def _is_valid_split(self, left, right, original):
        """Check if a split produces valid predicate parts."""
        # Both parts should have at least 2 words
        if len(left.split()) < 2 or len(right.split()) < 1:
            return False

        # Right side shouldn't start with a preposition that belongs to left
        right_lower = right.lower()
        if right_lower.startswith(("than ", "to ", "from ", "of ", "with ")):
            return False

        return True

    def _comma_split(self, text):
        """Split at commas if they separate distinct predicates."""
        parts = [p.strip() for p in text.split(", ")]
        # Filter out empty parts and very short fragments
        parts = [p for p in parts if len(p.split()) >= 2]

        if len(parts) < 2:
            return None

        # Check if last part starts with "and" — then it's a list
        if parts[-1].lower().startswith("and "):
            parts[-1] = parts[-1][4:].strip()

        return parts if len(parts) >= 2 else None

    def _relative_clause_split(self, text):
        """Split at relative clause boundaries (who, that, which, where)."""
        # "active users who signed up last month" → ["active users", "signed up last month"]
        m = re.search(r'\b(who|that|which|where|whose)\s+', text, re.IGNORECASE)
        if m:
            left = text[:m.start()].strip()
            right = text[m.end():].strip()
            if left and right and len(left.split()) >= 1 and len(right.split()) >= 2:
                return [left, right]
        return None

    def _build_tree(self, parts, conjunction):
        """Build a balanced tree from a list of parts."""
        if len(parts) == 1:
            return LeafPredicate(parts[0])
        if len(parts) == 2:
            return ConjunctionNode(
                conjunction,
                self._parse_recursive(parts[0]),
                self._parse_recursive(parts[1]),
            )
        # For 3+ parts, left-associate
        left = self._parse_recursive(parts[0])
        right = self._build_tree(parts[1:], conjunction)
        return ConjunctionNode(conjunction, left, right)

    def get_leaves(self, tree):
        """Extract all leaf predicates from a tree."""
        if isinstance(tree, LeafPredicate):
            return [tree.text]
        return self.get_leaves(tree.left) + self.get_leaves(tree.right)

    def get_conjunction_type(self, tree):
        """Determine the overall conjunction type."""
        if isinstance(tree, LeafPredicate):
            return "none"
        conjunctions = set()
        self._collect_conjunctions(tree, conjunctions)
        if len(conjunctions) == 0:
            return "none"
        if len(conjunctions) == 1:
            return list(conjunctions)[0].lower()
        return "mixed"

    def _collect_conjunctions(self, tree, conjunctions):
        if isinstance(tree, ConjunctionNode):
            conjunctions.add(tree.conjunction)
            self._collect_conjunctions(tree.left, conjunctions)
            self._collect_conjunctions(tree.right, conjunctions)


if __name__ == "__main__":
    parser = ConjunctionParser()
    tests = [
        "active users",
        "active users who signed up last month",
        "verified users who signed up last month and have placed at least 3 orders",
        "orders over $100 or orders with free shipping",
        "customers in New York, Los Angeles, and Chicago",
        "products between $10 and $50",
        "either premium or enterprise users",
        "active employees who are remote and have a rating above 4",
    ]
    for text in tests:
        tree = parser.parse(text)
        print(f"\n  Input: {text}")
        print(f"  Tree:  {tree}")
        print(f"  Leaves: {parser.get_leaves(tree)}")
        print(f"  Conjunction: {parser.get_conjunction_type(tree)}")
