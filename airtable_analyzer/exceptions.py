"""Custom exceptions for the Airtable Analyzer application."""


class AirtableClientError(Exception):
    """Base exception for Airtable client failures."""


class AirtableAuthenticationError(AirtableClientError):
    """Raised when Airtable rejects authentication credentials."""


class AirtableRateLimitError(AirtableClientError):
    """Raised when Airtable rate limits requests despite internal handling."""


class AirtableNotFoundError(AirtableClientError):
    """Raised when a requested Airtable resource could not be found."""


class GeminiClientError(Exception):
    """Raised for Gemini integration failures."""


class ReportGenerationError(Exception):
    """Raised when report generation fails."""
