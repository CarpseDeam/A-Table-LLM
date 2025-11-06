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
    report, saved_path = service.generate_report(output_path=output_file)

    assert "Airtable Base Duplication Guide" in report
    assert output_file.exists()
    assert output_file.read_text(encoding="utf-8")
    assert saved_path == output_file


def test_analysis_service_auto_save(
    sample_schema: AirtableBaseSchema,
    sample_duplication_guide: DuplicationGuide,
) -> None:
    """Test that reports are automatically saved when no output path is provided."""
    import os
    from pathlib import Path

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

    # Clean up any existing reports directory
    reports_dir = Path("reports")
    if reports_dir.exists():
        for file in reports_dir.glob("*.md"):
            file.unlink()

    try:
        # Generate report without providing output_path
        report, saved_path = service.generate_report()

        # Verify the report content
        assert "Airtable Base Duplication Guide" in report

        # Verify the file was saved
        assert saved_path is not None
        assert saved_path.exists()
        assert saved_path.parent.name == "reports"
        assert saved_path.name.endswith(".md")
        assert sample_schema.name.replace(" ", "_") in saved_path.name or "Sample" in saved_path.name

        # Verify the content was written correctly
        saved_content = saved_path.read_text(encoding="utf-8")
        assert saved_content == report
    finally:
        # Clean up
        if saved_path and saved_path.exists():
            saved_path.unlink()
