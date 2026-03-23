"""Custom exceptions for ProbSQL engine."""


class ProbSQLError(Exception):
    """Base exception for ProbSQL."""
    pass


class SchemaError(ProbSQLError):
    """Invalid or missing schema."""
    pass


class ParseError(ProbSQLError):
    """Failed to parse English predicate."""
    pass


class ColumnMatchError(ProbSQLError):
    """Failed to match any column."""
    pass


class ValueExtractionError(ProbSQLError):
    """Failed to extract value from English phrase."""
    pass
