"""End-to-end service tests with mocks."""

from __future__ import annotations

from pathlib import Path

from airtable_analyzer.config import Settings
from airtable_analyzer.models import AirtableBaseSchema, DuplicationGuide, SchemaAnalysis
from airtable_analyzer.report_builder import ReportBuilder
from airtable_analyzer.schema_processor import SchemaProcessor
from airtable_analyzer.service import AirtableAnalysisService


class FakeAirtableClient:
    """Stub Airtable client returning static schema."""

    def __init__(self, schema: AirtableBaseSchema) -> None:
        self.schema = schema

    def fetch_base_schema(self, base_id: str) -> AirtableBaseSchema:
        assert base_id == self.schema.id
        return self.schema


class FakeGeminiClient:
    """Stub Gemini client returning a predetermined guide."""

    def __init__(self, guide: DuplicationGuide) -> None:
        self.guide = guide

    def generate_duplication_guide(self, _analysis: SchemaAnalysis) -> DuplicationGuide:
        return self.guide


def test_analysis_service_end_to_end(
    tmp_path: Path,
    sample_schema: AirtableBaseSchema,
    sample_duplication_guide: DuplicationGuide,
) -> None:
    settings = Settings(
        AIRTABLE_ACCESS_TOKEN="token",
        AIRTABLE_BASE_ID=sample_schema.id,
        GEMINI_API_KEY="api-key",
        GEMINI_MODEL="gemini-2.5-pro",
    )

    service = AirtableAnalysisService(
        settings=settings,
        airtable_client=FakeAirtableClient(sample_schema),
        schema_processor=SchemaProcessor(),
        gemini_client=FakeGeminiClient(sample_duplication_guide),
        report_builder=ReportBuilder(),
    )

    output_file = tmp_path / "report.md"
    report = service.generate_report(output_path=output_file)

    assert "Airtable Base Duplication Guide" in report
    assert output_file.exists()
    assert output_file.read_text(encoding="utf-8")
