"""High-level orchestration for the Airtable Analyzer."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from .airtable_client import AirtableClient
from .config import Settings
from .gemini_client import GeminiClient
from .models import AirtableBaseSchema, DuplicationGuide, SchemaAnalysis
from .report_builder import ReportBuilder
from .schema_processor import SchemaProcessor

LOGGER = logging.getLogger(__name__)


class AirtableAnalysisService:
    """Coordinate data retrieval, analysis, and report generation."""

    def __init__(
        self,
        settings: Settings,
        airtable_client: AirtableClient,
        schema_processor: SchemaProcessor,
        gemini_client: GeminiClient,
        report_builder: ReportBuilder,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Create a new analysis service."""
        self.settings = settings
        self.airtable_client = airtable_client
        self.schema_processor = schema_processor
        self.gemini_client = gemini_client
        self.report_builder = report_builder
        self.logger = logger or LOGGER

    def generate_report(self, output_path: Optional[Path] = None) -> str:
        """Generate the markdown duplication guide.

        Args:
            output_path: Optional path where the markdown report should be written.

        Returns:
            Markdown document as a string.
        """
        schema = self._fetch_schema(self.settings.airtable_base_id)
        analysis = self._analyze_schema(schema)
        guide = self._invoke_gemini(analysis)
        report = self.report_builder.build_report(schema, analysis, guide)

        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report, encoding="utf-8")
            self.logger.info("Report written to %s", output_path)

        return report

    def _fetch_schema(self, base_id: str) -> AirtableBaseSchema:
        self.logger.info("Fetching Airtable schema for base %s", base_id)
        return self.airtable_client.fetch_base_schema(base_id)

    def _analyze_schema(self, schema: AirtableBaseSchema) -> SchemaAnalysis:
        self.logger.info("Analyzing Airtable schema")
        return self.schema_processor.analyze_schema(schema)

    def _invoke_gemini(self, analysis: SchemaAnalysis) -> DuplicationGuide:
        self.logger.info("Generating duplication guidance with Gemini")
        return self.gemini_client.generate_duplication_guide(analysis)
