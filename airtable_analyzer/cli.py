"""Command-line interface for the Airtable Analyzer."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer

from .airtable_client import AirtableClient
from .config import Settings, get_settings
from .gemini_client import GeminiClient
from .logging_config import configure_logging
from .report_builder import ReportBuilder
from .schema_processor import SchemaProcessor
from .service import AirtableAnalysisService

app = typer.Typer(help="Generate Airtable base duplication guides powered by Gemini.")


@app.command()
def analyze(  # pragma: no cover - CLI glue
    base_id: Optional[str] = typer.Option(
        None,
        "--base-id",
        "-b",
        help="Override the Airtable base identifier.",
        envvar="AIRTABLE_BASE_ID",
    ),
    model: Optional[str] = typer.Option(
        None,
        "--model",
        "-m",
        help="Override the Gemini model (gemini-2.5-pro or gemini-2.5-flash).",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Optional path where the markdown report should be written.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging.",
    ),
) -> None:
    """Generate the duplication report from the command line."""
    configure_logging(logging.DEBUG if verbose else logging.INFO)
    settings = _resolve_settings(base_id=base_id, model=model)

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

    report, saved_path = service.generate_report(output_path=output)

    # Print the report to console
    typer.echo(report)

    # Print the saved file location
    typer.echo(f"\n{'='*60}")
    typer.echo(f"Report saved to: {saved_path}")
    typer.echo(f"{'='*60}")


def _resolve_settings(
    base_id: Optional[str], model: Optional[str]
) -> Settings:  # pragma: no cover - simple helper
    settings = get_settings()
    overrides = {}
    if base_id:
        overrides["airtable_base_id"] = base_id
    if model:
        overrides["gemini_model"] = model
    if overrides:
        return settings.model_copy(update=overrides)
    return settings


def main() -> None:  # pragma: no cover - CLI entrypoint
    app()


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()
