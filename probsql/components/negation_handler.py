"""
Negation Handler — Detects negation in English phrases and determines scope.

"users who are NOT premium"         → negate operator: != 'premium'
"users who have NOT placed orders"  → negate existence: NOT EXISTS
"non-premium users"                 → prefix negation: != 'premium'
"excluding cancelled orders"        → filter negation: status != 'cancelled'
"orders without a shipping address" → IS NULL
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class NegationInfo:
    has_negation: bool
    negation_type: Optional[str]  # "operator", "existence", "null_check", "prefix", "filter"
    negation_word: Optional[str]  # the word/phrase that triggered negation
    cleaned_phrase: str  # phrase with negation removed for further processing


# Negation patterns and their scope types
NEGATION_PATTERNS = [
    # Null-check negation (highest priority)
    (r'\bwithout\s+(?:a\s+|an\s+)?', "null_check", "without"),
    (r'\bmissing\s+', "null_check", "missing"),
    (r'\blacking\s+', "null_check", "lacking"),
    (r'\bno\s+\w+\s+(?:set|provided|given|specified)', "null_check", "no...set"),

    # Filter negation
    (r'\bexcluding\s+', "filter", "excluding"),
    (r'\bexcept\s+(?:for\s+)?', "filter", "except"),
    (r'\bother\s+than\s+', "filter", "other than"),
    (r'\bbesides?\s+', "filter", "besides"),

    # Prefix negation
    (r'\bnon-(\w+)', "prefix", "non-"),
    (r'\bun(\w+)', "prefix", "un"),

    # Operator negation (contractions and explicit)
    (r"\bisn'?t\b", "operator", "isn't"),
    (r"\baren'?t\b", "operator", "aren't"),
    (r"\bdoesn'?t\b", "operator", "doesn't"),
    (r"\bdon'?t\b", "operator", "don't"),
    (r"\bhasn'?t\b", "operator", "hasn't"),
    (r"\bhaven'?t\b", "operator", "haven't"),
    (r"\bwasn'?t\b", "operator", "wasn't"),
    (r"\bweren'?t\b", "operator", "weren't"),
    (r'\bnot\b', "operator", "not"),

    # Existence negation
    (r'\bno\s+', "existence", "no"),
    (r'\bnone\b', "existence", "none"),
    (r'\bzero\b', "existence", "zero"),
    (r'\bnever\b', "existence", "never"),
]


class NegationHandler:
    def detect(self, english_phrase):
        """Detect negation in a phrase and return NegationInfo.

        Args:
            english_phrase: The English text to analyze

        Returns:
            NegationInfo with negation details
        """
        phrase_lower = english_phrase.lower().strip()

        for pattern, neg_type, neg_word in NEGATION_PATTERNS:
            m = re.search(pattern, phrase_lower)
            if m:
                # Clean the phrase by removing the negation
                cleaned = re.sub(pattern, ' ', phrase_lower, count=1).strip()
                cleaned = re.sub(r'\s+', ' ', cleaned)

                return NegationInfo(
                    has_negation=True,
                    negation_type=neg_type,
                    negation_word=neg_word,
                    cleaned_phrase=cleaned,
                )

        return NegationInfo(
            has_negation=False,
            negation_type=None,
            negation_word=None,
            cleaned_phrase=english_phrase,
        )

    def apply_negation(self, operator, value, negation_info):
        """Apply negation to an operator/value pair.

        Args:
            operator: The SQL operator (e.g., "=", ">", "LIKE")
            value: The SQL value
            negation_info: NegationInfo from detect()

        Returns:
            tuple: (negated_operator, negated_value)
        """
        if not negation_info.has_negation:
            return operator, value

        neg_type = negation_info.negation_type

        if neg_type == "null_check":
            return "IS NULL", None

        if neg_type == "filter":
            return self._negate_operator(operator), value

        if neg_type == "prefix":
            return self._negate_operator(operator), value

        if neg_type == "operator":
            return self._negate_operator(operator), value

        if neg_type == "existence":
            # "no orders" → count = 0 or NOT EXISTS
            return self._negate_operator(operator), value

        return operator, value

    def _negate_operator(self, operator):
        """Return the negated form of an operator."""
        negation_map = {
            "=": "!=",
            "!=": "=",
            ">": "<=",
            "<": ">=",
            ">=": "<",
            "<=": ">",
            "LIKE": "NOT LIKE",
            "NOT LIKE": "LIKE",
            "IN": "NOT IN",
            "NOT IN": "IN",
            "IS NULL": "IS NOT NULL",
            "IS NOT NULL": "IS NULL",
            "BETWEEN": "NOT BETWEEN",
            "NOT BETWEEN": "BETWEEN",
        }
        return negation_map.get(operator, f"NOT {operator}")


if __name__ == "__main__":
    handler = NegationHandler()
    tests = [
        "users who are NOT premium",
        "users who haven't logged in",
        "non-premium users",
        "excluding cancelled orders",
        "orders without a shipping address",
        "no orders placed",
        "active users",  # no negation
        "products that aren't in stock",
        "customers other than VIPs",
        "missing email address",
    ]
    for text in tests:
        info = handler.detect(text)
        print(f"\n  Input: {text}")
        print(f"  Negation: {info.has_negation}, type={info.negation_type}, word='{info.negation_word}'")
        print(f"  Cleaned: '{info.cleaned_phrase}'")
        if info.has_negation:
            neg_op, _ = handler.apply_negation("=", "'premium'", info)
            print(f"  Negated = from '=' → '{neg_op}'")
