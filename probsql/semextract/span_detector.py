"""
Value Span Detector — Identifies WHERE value spans in questions
using boundary signal probabilities from Opus labels.

Instead of token-by-token HMM classification (which fragments values),
this detects span BOUNDARIES:
  1. Find candidate start positions
  2. Find candidate end positions
  3. Score each (start, end) span
  4. Return the highest-scoring span

Probabilities computed from 1000 Opus-labeled value spans:
  P(start | left_word) — e.g., P(start | "of") = 0.25
  P(end | right_word) — e.g., P(end | "?") = 0.40
  P(structure | span_content) — e.g., P(proper_noun | capitalized) = 0.24
"""

import re
from dataclasses import dataclass
from typing import Optional

# P(value starts after this word) — empirical from 1000 Opus labels
# Normalized to represent "how likely is this word to precede a value"
START_TRIGGER_WORDS = {
    "of": 0.90, "is": 0.75, "than": 0.85, "the": 0.30,
    "for": 0.70, "on": 0.65, "was": 0.70, "has": 0.60,
    "in": 0.55, "a": 0.40, "when": 0.50, "did": 0.45,
    "with": 0.55, "from": 0.70, "an": 0.40, "were": 0.65,
    "are": 0.65, "by": 0.70, "at": 0.65, "as": 0.50,
    "named": 0.85, "called": 0.85, "titled": 0.85,
}

# Words that END a value span (value stops BEFORE these)
END_SIGNAL_WORDS = {
    "?": 0.95, ",": 0.70, "and": 0.75, "as": 0.65,
    "is": 0.60, "with": 0.60, "in": 0.50, "was": 0.55,
    "play": 0.70, "have": 0.60, "had": 0.60, "on": 0.50,
    "for": 0.45, "did": 0.55, "does": 0.55, "do": 0.55,
    "the": 0.40, "a": 0.40, "an": 0.40,
}

# Question words that are never part of a value
NEVER_VALUE = {
    "what", "which", "who", "where", "when", "how", "name", "list",
    "tell", "find", "give", "many", "much",
}


@dataclass
class SpanCandidate:
    text: str
    start: int
    end: int
    score: float
    start_signal: str
    end_signal: str


class ValueSpanDetector:
    """Detects value spans using boundary probabilities."""

    def detect(self, question, headers=None):
        """Find the most likely value span in a question.

        Args:
            question: The question text
            headers: Column headers (to avoid selecting column names as values)

        Returns:
            SpanCandidate or None
        """
        q = question.strip()
        tokens = q.split()
        n = len(tokens)

        if n < 2:
            return None

        header_words = set()
        if headers:
            for h in headers:
                header_words.update(w.lower() for w in re.findall(r'\b\w+\b', h))

        # Build character-to-token mapping
        token_starts = []
        token_ends = []
        pos = 0
        for tok in tokens:
            idx = q.find(tok, pos)
            token_starts.append(idx)
            token_ends.append(idx + len(tok))
            pos = idx + len(tok)

        # Find candidate start positions
        starts = []
        for i in range(n):
            score, signal = self._score_start(tokens, i, header_words)
            if score > 0.2:
                starts.append((i, score, signal))

        # Find candidate end positions
        ends = []
        for i in range(n):
            score, signal = self._score_end(tokens, i, n)
            if score > 0.2:
                ends.append((i, score, signal))

        # Score all valid (start, end) spans
        candidates = []
        for si, s_score, s_signal in starts:
            for ei, e_score, e_signal in ends:
                if ei < si:
                    continue
                span_len = ei - si + 1
                if span_len < 1 or span_len > 10:
                    continue

                # Extract span text
                span_text = " ".join(tokens[si:ei + 1])
                # Clean trailing punctuation
                span_text = span_text.rstrip("?,.")

                if not span_text:
                    continue

                # Strip leading articles/determiners
                strip_leading = {"the", "a", "an", "this", "that", "these", "those"}
                while span_text.split() and span_text.split()[0].lower() in strip_leading:
                    first = span_text.split()[0]
                    span_text = span_text[len(first):].strip()
                    si_adj = si + 1
                    if si_adj <= ei:
                        si = si_adj

                # Strip trailing common verbs/fillers
                strip_trailing = {"play", "plays", "played", "score", "scored", "have",
                                  "has", "had", "is", "are", "was", "were", "as", "the",
                                  "a", "an", "in", "on", "at", "for", "with", "by",
                                  "does", "did", "do", "result", "team"}
                while span_text.split() and span_text.split()[-1].lower().rstrip("?,") in strip_trailing:
                    last = span_text.split()[-1]
                    span_text = span_text[:-(len(last))].strip().rstrip()
                    ei_adj = ei - 1
                    if ei_adj >= si:
                        ei = ei_adj
                    else:
                        break

                span_text = span_text.strip().rstrip("?,. ")
                if not span_text:
                    continue

                # Score the span content
                content_score = self._score_content(span_text, tokens, si, ei, header_words)

                # Combined score
                total = s_score * 0.35 + e_score * 0.35 + content_score * 0.30
                candidates.append(SpanCandidate(
                    text=span_text,
                    start=token_starts[si],
                    end=token_ends[ei],
                    score=total,
                    start_signal=s_signal,
                    end_signal=e_signal,
                ))

        if not candidates:
            return None

        # Return highest scoring
        candidates.sort(key=lambda c: -c.score)
        return candidates[0]

    def detect_multiple(self, question, headers=None, max_spans=4):
        """Find multiple non-overlapping value spans.

        For multi-condition queries like:
        "What X has a Y of Z and a W of V?"
        → two spans: Z and V

        Returns list of SpanCandidate, sorted by position in question.
        """
        q = question.strip()
        tokens = q.split()
        n = len(tokens)

        if n < 2:
            return []

        header_words = set()
        if headers:
            for h in headers:
                header_words.update(w.lower() for w in re.findall(r'\b\w+\b', h))

        # Build token positions
        token_starts = []
        token_ends = []
        pos = 0
        for tok in tokens:
            idx = q.find(tok, pos)
            token_starts.append(idx)
            token_ends.append(idx + len(tok))
            pos = idx + len(tok)

        # Get ALL scored spans
        all_candidates = []
        starts = [(i, *self._score_start(tokens, i, header_words)) for i in range(n)]
        ends = [(i, *self._score_end(tokens, i, n)) for i in range(n)]

        for si, s_score, s_signal in starts:
            if s_score <= 0.2:
                continue
            for ei, e_score, e_signal in ends:
                if e_score <= 0.2 or ei < si:
                    continue
                span_len = ei - si + 1
                if span_len < 1 or span_len > 10:
                    continue

                span_text = " ".join(tokens[si:ei + 1])
                # Strip leading/trailing
                orig_si, orig_ei = si, ei
                strip_leading = {"the", "a", "an", "this", "that", "these", "those"}
                while span_text.split() and span_text.split()[0].lower() in strip_leading:
                    first = span_text.split()[0]
                    span_text = span_text[len(first):].strip()
                    si += 1
                strip_trailing = {"play", "plays", "played", "score", "scored", "have",
                                  "has", "had", "is", "are", "was", "were", "as", "the",
                                  "a", "an", "in", "on", "at", "for", "with", "by",
                                  "does", "did", "do", "result", "team"}
                while span_text.split() and span_text.split()[-1].lower().rstrip("?,") in strip_trailing:
                    last = span_text.split()[-1]
                    span_text = span_text[:-(len(last))].strip()
                    ei -= 1
                    if ei < si:
                        break

                span_text = span_text.strip().rstrip("?,. ")
                if not span_text:
                    continue

                content_score = self._score_content(span_text, tokens, si, ei, header_words)
                total = s_score * 0.35 + e_score * 0.35 + content_score * 0.30

                all_candidates.append(SpanCandidate(
                    text=span_text,
                    start=token_starts[si] if si < len(token_starts) else 0,
                    end=token_ends[min(ei, len(token_ends)-1)],
                    score=total,
                    start_signal=s_signal,
                    end_signal=e_signal,
                ))

        if not all_candidates:
            return []

        # Greedy non-overlapping selection: pick best, remove overlapping, repeat
        all_candidates.sort(key=lambda c: -c.score)
        selected = []
        used_chars = set()

        for cand in all_candidates:
            # Check overlap with already selected spans
            cand_chars = set(range(cand.start, cand.end))
            if cand_chars & used_chars:
                continue
            # Don't select duplicate texts
            if any(cand.text.lower() == s.text.lower() for s in selected):
                continue
            selected.append(cand)
            used_chars.update(cand_chars)
            if len(selected) >= max_spans:
                break

        # Sort by position in question
        selected.sort(key=lambda c: c.start)
        return selected

    def _score_start(self, tokens, i, header_words):
        """Score position i as a value start."""
        tok = tokens[i]
        tok_lower = tok.lower().rstrip("?,.")

        # Never start on question words
        if tok_lower in NEVER_VALUE:
            return 0.0, "none"

        # Check previous word trigger
        if i > 0:
            prev = tokens[i - 1].lower().rstrip(",?.!")
            if prev in START_TRIGGER_WORDS:
                return START_TRIGGER_WORDS[prev], f"after_{prev}"

        # Capitalized word (proper noun start)
        if tok[0].isupper() and tok_lower not in NEVER_VALUE:
            # But not if it's a column header word in early position
            if i <= 3 and tok_lower in header_words:
                return 0.15, "column_word"
            return 0.65, "capitalized"

        # Number
        if re.match(r'\d', tok):
            return 0.70, "number"

        # Quoted
        if tok.startswith('"') or tok.startswith("'"):
            return 0.85, "quoted"

        # After preposition/copula at any position
        if i > 0:
            prev = tokens[i - 1].lower().rstrip(",?.!")
            if prev in {"of", "is", "was", "for", "from", "by", "at", "on", "in",
                        "than", "with", "named", "called", "has", "have", "had",
                        "are", "were"}:
                return 0.50, f"after_{prev}"

        return 0.1, "none"

    def _score_end(self, tokens, i, n):
        """Score position i as a value end (inclusive)."""
        tok = tokens[i]
        tok_lower = tok.lower()

        # Question mark at end
        if tok.endswith("?"):
            return 0.90, "question_mark"

        # Last token
        if i == n - 1:
            return 0.75, "end_of_question"

        # Check next word
        if i < n - 1:
            next_tok = tokens[i + 1].lower().rstrip("?,.")
            if next_tok in END_SIGNAL_WORDS:
                return END_SIGNAL_WORDS[next_tok], f"before_{next_tok}"
            # Next word is a verb
            if next_tok in {"play", "plays", "played", "score", "scored",
                           "have", "has", "had", "does", "did", "do",
                           "win", "won", "lose", "lost"}:
                return 0.70, f"before_verb_{next_tok}"

        # Comma
        if tok.endswith(","):
            return 0.65, "at_comma"

        return 0.15, "none"

    def _score_content(self, span_text, tokens, si, ei, header_words):
        """Score the content of a span — does it look like a value?"""
        score = 0.3  # base

        # Proper noun sequence (all capitalized)
        words = span_text.split()
        if words and all(w[0].isupper() for w in words if w.isalpha()):
            score = max(score, 0.8)

        # Contains numbers
        if re.search(r'\d', span_text):
            score = max(score, 0.7)

        # Contains special chars (scores, codes)
        if re.search(r'[-–/().=$#]', span_text):
            score = max(score, 0.6)

        # Is a single common word that's likely a column value
        if len(words) == 1 and words[0].lower() in header_words:
            score *= 0.5  # penalty — probably a column name, not a value

        # Very short spans (1-2 chars) — less likely to be meaningful
        if len(span_text) <= 2 and not re.match(r'\d+$', span_text):
            score *= 0.5

        # Too long (>8 words) — probably captured too much
        if len(words) > 8:
            score *= 0.5

        return score


if __name__ == "__main__":
    detector = ValueSpanDetector()

    tests = [
        ("What position does the player who played for Butler CC (KS) play?",
         ["Player", "No.", "Position", "School/Club Team"]),
        ("Who is the player that wears number 42?",
         ["Player", "No.", "Position"]),
        ("What school/club team is Amir Johnson on?",
         ["Player", "School/Club Team"]),
        ("Name the total number of represents for clary sermina delgado cid",
         ["Contestant", "Represents"]),
        ("What is the highest Week when the opponent was the los angeles rams?",
         ["Week", "Opponent", "Result"]),
        ("What is the outcome of the 4–6, 6–4, 6–3, 7–6 (7–2) score?",
         ["Score", "Outcome"]),
        ("How many attendances have w 48-10 as the result?",
         ["Attendance", "Result"]),
    ]

    for q, headers in tests:
        span = detector.detect(q, headers)
        if span:
            print(f"Q: {q[:65]}")
            print(f"  Span: \"{span.text}\" (score={span.score:.2f})")
            print(f"  Start: {span.start_signal}, End: {span.end_signal}")
            print()
        else:
            print(f"Q: {q[:65]}")
            print(f"  No span found")
            print()
