"""
Temporal Expression Parser — Recursive descent parser for English temporal
expressions, producing SQL date conditions.

Handles:
- Relative to now: "today", "last month", "past 30 days"
- Absolute: "in January 2024", "Q3 2024", "on Jan 15"
- Relative to column: "within 7 days of signup"
- Composite: "between Jan and March 2024"
"""

import re
from dataclasses import dataclass
from typing import Optional

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

WORD_NUMBERS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "twenty": 20, "thirty": 30, "sixty": 60, "ninety": 90,
}

UNIT_MAP = {
    "day": "DAY", "days": "DAY",
    "week": "WEEK", "weeks": "WEEK",
    "month": "MONTH", "months": "MONTH",
    "year": "YEAR", "years": "YEAR",
    "hour": "HOUR", "hours": "HOUR",
    "minute": "MINUTE", "minutes": "MINUTE",
}

# SQLite-compatible date functions
SQLITE_MODE = True


@dataclass
class TemporalResult:
    sql_condition: str
    temporal_type: str  # relative_to_now, absolute_date, relative_to_column, date_range
    confidence: float
    parsed_expression: str


class TemporalParser:
    def __init__(self, current_date="2025-01-15", dialect="sqlite"):
        self.current_date = current_date
        self.dialect = dialect

    def is_temporal(self, english_phrase):
        """Check if a phrase contains temporal expressions."""
        phrase = english_phrase.lower()
        temporal_indicators = [
            r"\btoday\b", r"\byesterday\b", r"\btomorrow\b",
            r"\bthis\s+(week|month|year)\b", r"\blast\s+(week|month|year|day|hour|\d+)\b",
            r"\bpast\s+\d+\b", r"\brecently\b", r"\brecent\b",
            r"\bsince\b", r"\bbefore\b", r"\bafter\b",
            r"\bin\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\b",
            r"\bin\s+\d{4}\b", r"\bin\s+q[1-4]\b",
            r"\bon\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\b",
            r"\b\d{4}-\d{2}\b", r"\b\d{4}-\d{2}-\d{2}\b",
            r"\bago\b", r"\bwithin\b",
            r"\bthis\s+quarter\b", r"\blast\s+quarter\b",
        ]
        return any(re.search(p, phrase) for p in temporal_indicators)

    def parse(self, english_phrase, column_name="created_at"):
        """Parse a temporal expression and return SQL condition.

        Args:
            english_phrase: The English temporal expression
            column_name: The column to apply the condition to

        Returns:
            TemporalResult with SQL condition
        """
        phrase = english_phrase.lower().strip()

        # Try each parser in order
        parsers = [
            self._parse_today_yesterday,
            self._parse_this_unit,
            self._parse_last_unit,
            self._parse_past_n_units,
            self._parse_recently,
            self._parse_between_dates,
            self._parse_absolute_quarter,
            self._parse_absolute_month_year,
            self._parse_absolute_year,
            self._parse_on_date,
            self._parse_since,
            self._parse_before_after,
            self._parse_within_of_column,
        ]

        for parser in parsers:
            result = parser(phrase, column_name)
            if result:
                return result

        # Fallback: if has a date-like pattern, try generic
        return TemporalResult(
            sql_condition=f"{column_name} IS NOT NULL",
            temporal_type="unknown",
            confidence=0.2,
            parsed_expression=phrase,
        )

    def _parse_today_yesterday(self, phrase, col):
        if "today" in phrase:
            sql = self._date_eq(col, "date('now')")
            return TemporalResult(sql, "relative_to_now", 0.95, "today")
        if "yesterday" in phrase:
            sql = self._date_eq(col, "date('now', '-1 day')")
            return TemporalResult(sql, "relative_to_now", 0.95, "yesterday")
        return None

    def _parse_this_unit(self, phrase, col):
        m = re.search(r"\bthis\s+(week|month|year|quarter)\b", phrase)
        if not m:
            return None
        unit = m.group(1)
        if unit == "week":
            sql = f"{col} >= date('now', 'weekday 0', '-7 days')"
        elif unit == "month":
            sql = f"{col} >= date('now', 'start of month')"
        elif unit == "year":
            sql = f"{col} >= date('now', 'start of year')"
        elif unit == "quarter":
            sql = self._current_quarter_sql(col)
        else:
            return None
        return TemporalResult(sql, "relative_to_now", 0.9, f"this {unit}")

    def _parse_last_unit(self, phrase, col):
        # "last week", "last month", "last year"
        m = re.search(r"\blast\s+(week|month|year|quarter)\b", phrase)
        if not m:
            return None
        unit = m.group(1)
        if unit == "week":
            start = "date('now', 'weekday 0', '-14 days')"
            end = "date('now', 'weekday 0', '-7 days')"
        elif unit == "month":
            start = "date('now', 'start of month', '-1 month')"
            end = "date('now', 'start of month')"
        elif unit == "year":
            start = "date('now', 'start of year', '-1 year')"
            end = "date('now', 'start of year')"
        elif unit == "quarter":
            start, end = self._last_quarter_range()
        else:
            return None
        sql = f"{col} >= {start} AND {col} < {end}"
        return TemporalResult(sql, "relative_to_now", 0.9, f"last {unit}")

    def _parse_past_n_units(self, phrase, col):
        # "past 30 days", "last 7 days", "past 2 weeks", "last three months"
        m = re.search(r"\b(?:past|last)\s+(\d+|" + "|".join(WORD_NUMBERS.keys()) + r")\s+(days?|weeks?|months?|years?|hours?|minutes?)\b", phrase)
        if not m:
            return None
        n_str = m.group(1)
        unit = m.group(2)

        n = WORD_NUMBERS.get(n_str, None)
        if n is None:
            try:
                n = int(n_str)
            except ValueError:
                return None

        sql_unit = UNIT_MAP.get(unit, "DAY")
        if sql_unit == "DAY":
            sql = f"{col} >= date('now', '-{n} days')"
        elif sql_unit == "WEEK":
            sql = f"{col} >= date('now', '-{n * 7} days')"
        elif sql_unit == "MONTH":
            sql = f"{col} >= date('now', '-{n} months')"
        elif sql_unit == "YEAR":
            sql = f"{col} >= date('now', '-{n} years')"
        elif sql_unit == "HOUR":
            sql = f"{col} >= datetime('now', '-{n} hours')"
        elif sql_unit == "MINUTE":
            sql = f"{col} >= datetime('now', '-{n} minutes')"
        else:
            sql = f"{col} >= date('now', '-{n} days')"

        return TemporalResult(sql, "relative_to_now", 0.9, f"past {n} {unit}")

    def _parse_recently(self, phrase, col):
        if re.search(r"\brecently\b|\brecent\b", phrase):
            sql = f"{col} >= date('now', '-30 days')"
            return TemporalResult(sql, "relative_to_now", 0.7, "recently (default 30 days)")
        return None

    def _parse_between_dates(self, phrase, col):
        # "between January and March 2024", "from Jan to Mar"
        m = re.search(
            r"\b(?:between|from)\s+(\w+)\s+(?:and|to)\s+(\w+)(?:\s+(\d{4}))?\b",
            phrase
        )
        if not m:
            return None
        start_str = m.group(1).lower()
        end_str = m.group(2).lower()
        year = m.group(3) or self.current_date[:4]

        start_month = MONTH_NAMES.get(start_str)
        end_month = MONTH_NAMES.get(end_str)

        if start_month and end_month:
            start_date = f"{year}-{start_month:02d}-01"
            # End is start of month after end_month
            if end_month == 12:
                end_date = f"{int(year) + 1}-01-01"
            else:
                end_date = f"{year}-{end_month + 1:02d}-01"
            sql = f"{col} >= '{start_date}' AND {col} < '{end_date}'"
            return TemporalResult(sql, "date_range", 0.85, f"between {start_str} and {end_str} {year}")

        return None

    def _parse_absolute_quarter(self, phrase, col):
        m = re.search(r"\bq([1-4])\s*(\d{4})?\b", phrase)
        if not m:
            return None
        q = int(m.group(1))
        year = m.group(2) or self.current_date[:4]
        start_month = (q - 1) * 3 + 1
        end_month = start_month + 3
        start_date = f"{year}-{start_month:02d}-01"
        if end_month > 12:
            end_date = f"{int(year) + 1}-01-01"
        else:
            end_date = f"{year}-{end_month:02d}-01"
        sql = f"{col} >= '{start_date}' AND {col} < '{end_date}'"
        return TemporalResult(sql, "absolute_date", 0.9, f"Q{q} {year}")

    def _parse_absolute_month_year(self, phrase, col):
        # "in January 2024", "in Jan", "January 2024"
        for month_name, month_num in MONTH_NAMES.items():
            pattern = rf"\b(?:in\s+)?{month_name}\s+(\d{{4}})\b"
            m = re.search(pattern, phrase)
            if m:
                year = m.group(1)
                start_date = f"{year}-{month_num:02d}-01"
                if month_num == 12:
                    end_date = f"{int(year) + 1}-01-01"
                else:
                    end_date = f"{year}-{month_num + 1:02d}-01"
                sql = f"{col} >= '{start_date}' AND {col} < '{end_date}'"
                return TemporalResult(sql, "absolute_date", 0.9, f"{month_name} {year}")

        # Just month name without year
        for month_name, month_num in MONTH_NAMES.items():
            pattern = rf"\bin\s+{month_name}\b"
            m = re.search(pattern, phrase)
            if m:
                year = self.current_date[:4]
                start_date = f"{year}-{month_num:02d}-01"
                if month_num == 12:
                    end_date = f"{int(year) + 1}-01-01"
                else:
                    end_date = f"{year}-{month_num + 1:02d}-01"
                sql = f"{col} >= '{start_date}' AND {col} < '{end_date}'"
                return TemporalResult(sql, "absolute_date", 0.8, f"in {month_name} (inferred {year})")

        return None

    def _parse_absolute_year(self, phrase, col):
        m = re.search(r"\bin\s+(\d{4})\b", phrase)
        if not m:
            return None
        year = m.group(1)
        sql = f"{col} >= '{year}-01-01' AND {col} < '{int(year) + 1}-01-01'"
        return TemporalResult(sql, "absolute_date", 0.9, f"in {year}")

    def _parse_on_date(self, phrase, col):
        # "on January 15, 2024" or "on Jan 15"
        for month_name, month_num in MONTH_NAMES.items():
            pattern = rf"\bon\s+{month_name}\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s+(\d{{4}}))?\b"
            m = re.search(pattern, phrase)
            if m:
                day = int(m.group(1))
                year = m.group(2) or self.current_date[:4]
                date_str = f"{year}-{month_num:02d}-{day:02d}"
                sql = f"DATE({col}) = '{date_str}'"
                return TemporalResult(sql, "absolute_date", 0.95, f"on {month_name} {day}, {year}")

        # ISO date: "on 2024-01-15"
        m = re.search(r"\bon\s+(\d{4}-\d{2}-\d{2})\b", phrase)
        if m:
            date_str = m.group(1)
            sql = f"DATE({col}) = '{date_str}'"
            return TemporalResult(sql, "absolute_date", 0.95, f"on {date_str}")

        return None

    def _parse_since(self, phrase, col):
        # "since January", "since 2023", "since January 2024"
        m = re.search(r"\bsince\s+(\d{4})\b", phrase)
        if m:
            year = m.group(1)
            sql = f"{col} >= '{year}-01-01'"
            return TemporalResult(sql, "absolute_date", 0.9, f"since {year}")

        for month_name, month_num in MONTH_NAMES.items():
            m = re.search(rf"\bsince\s+{month_name}(?:\s+(\d{{4}}))?\b", phrase)
            if m:
                year = m.group(1) or self.current_date[:4]
                sql = f"{col} >= '{year}-{month_num:02d}-01'"
                return TemporalResult(sql, "absolute_date", 0.85, f"since {month_name} {year}")

        return None

    def _parse_before_after(self, phrase, col):
        # "before March 2024", "after June 1st"
        direction = None
        if re.search(r"\bbefore\b", phrase):
            direction = "before"
        elif re.search(r"\bafter\b", phrase):
            direction = "after"
        if not direction:
            return None

        op = "<" if direction == "before" else ">"

        # Month + year
        for month_name, month_num in MONTH_NAMES.items():
            m = re.search(rf"\b{direction}\s+{month_name}(?:\s+(\d{{4}}))?\b", phrase)
            if m:
                year = m.group(1) or self.current_date[:4]
                date_str = f"{year}-{month_num:02d}-01"
                sql = f"{col} {op} '{date_str}'"
                return TemporalResult(sql, "absolute_date", 0.85, f"{direction} {month_name} {year}")

        # Year only
        m = re.search(rf"\b{direction}\s+(\d{{4}})\b", phrase)
        if m:
            year = m.group(1)
            date_str = f"{year}-01-01"
            sql = f"{col} {op} '{date_str}'"
            return TemporalResult(sql, "absolute_date", 0.85, f"{direction} {year}")

        return None

    def _parse_within_of_column(self, phrase, col):
        # "within 7 days of signup", "30 days after creation"
        m = re.search(
            r"\bwithin\s+(\d+)\s+(days?|weeks?|months?)\s+(?:of|from)\s+(\w+)",
            phrase
        )
        if m:
            n = int(m.group(1))
            unit = m.group(2)
            ref_col = m.group(3)
            sql_unit = UNIT_MAP.get(unit, "DAY")
            if sql_unit == "DAY":
                sql = f"{col} <= date({ref_col}, '+{n} days')"
            elif sql_unit == "WEEK":
                sql = f"{col} <= date({ref_col}, '+{n * 7} days')"
            elif sql_unit == "MONTH":
                sql = f"{col} <= date({ref_col}, '+{n} months')"
            else:
                sql = f"{col} <= date({ref_col}, '+{n} days')"
            return TemporalResult(sql, "relative_to_column", 0.7, f"within {n} {unit} of {ref_col}")

        # "N days after column"
        m = re.search(
            r"\b(\d+)\s+(days?|weeks?|months?)\s+(after|before)\s+(\w+)",
            phrase
        )
        if m:
            n = int(m.group(1))
            unit = m.group(2)
            direction = m.group(3)
            ref_col = m.group(4)
            sign = "+" if direction == "after" else "-"
            sql_unit = UNIT_MAP.get(unit, "DAY")
            if sql_unit == "DAY":
                sql = f"{col} >= date({ref_col}, '{sign}{n} days')"
            elif sql_unit == "MONTH":
                sql = f"{col} >= date({ref_col}, '{sign}{n} months')"
            else:
                sql = f"{col} >= date({ref_col}, '{sign}{n} days')"
            return TemporalResult(sql, "relative_to_column", 0.7, f"{n} {unit} {direction} {ref_col}")

        return None

    def _date_eq(self, col, date_expr):
        return f"DATE({col}) = {date_expr}"

    def _current_quarter_sql(self, col):
        # Approximate: use current month to determine quarter start
        return f"{col} >= date('now', 'start of month', '-' || ((cast(strftime('%m', 'now') as integer) - 1) % 3) || ' months')"

    def _last_quarter_range(self):
        # Simplified: go back 3 months from current quarter start
        start = "date('now', 'start of month', '-' || ((cast(strftime('%m', 'now') as integer) - 1) % 3 + 3) || ' months')"
        end = "date('now', 'start of month', '-' || ((cast(strftime('%m', 'now') as integer) - 1) % 3) || ' months')"
        return start, end


if __name__ == "__main__":
    parser = TemporalParser()
    test_phrases = [
        "today",
        "yesterday",
        "this month",
        "last week",
        "past 30 days",
        "last 7 days",
        "recently",
        "in January 2024",
        "in Q3 2024",
        "since 2023",
        "before March 2024",
        "on January 15, 2024",
        "between January and March 2024",
        "in 2024",
    ]
    for phrase in test_phrases:
        result = parser.parse(phrase)
        print(f"  {phrase:40s} -> {result.sql_condition}")
        print(f"    type={result.temporal_type}, confidence={result.confidence}")
