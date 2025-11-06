"""Run the Airtable analyzer against a real base."""

from __future__ import annotations

import logging
from pathlib import Path

from airtable_analyzer.airtable_client import AirtableClient
from airtable_analyzer.config import get_settings
from airtable_analyzer.gemini_client import GeminiClient
from airtable_analyzer.logging_config import configure_logging
from airtable_analyzer.report_builder import ReportBuilder
from airtable_analyzer.schema_processor import SchemaProcessor
from airtable_analyzer.service import AirtableAnalysisService


def main() -> None:
    """Execute the full analysis workflow using environment configuration."""
    configure_logging(logging.INFO)
    settings = get_settings()

    airtable_client = AirtableClient(
        access_token=settings.get_airtable_token(),
        timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retry_attempts,
        initial_backoff_seconds=settings.initial_backoff_seconds,
    )
    schema_processor = SchemaProcessor()
    gemini_client = GeminiClient(
        api_key=settings.get_gemini_api_key(),
        model_name=settings.gemini_model,
    )
    report_builder = ReportBuilder()
    service = AirtableAnalysisService(
        settings=settings,
        airtable_client=airtable_client,
        schema_processor=schema_processor,
        gemini_client=gemini_client,
        report_builder=report_builder,
    )

    output_path = Path("reports") / f"{settings.airtable_base_id}_duplication.md"
    report, saved_path = service.generate_report(output_path=output_path)
    print(report)
    print(f"\nReport saved to: {saved_path}")


if __name__ == "__main__":
    main()
