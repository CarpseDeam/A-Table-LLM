"""Tests for report generation."""

from airtable_analyzer.models import AirtableBaseSchema, DuplicationGuide, SchemaAnalysis
from airtable_analyzer.report_builder import ReportBuilder


def test_report_builder_generates_markdown(
    sample_schema: AirtableBaseSchema,
    sample_analysis: SchemaAnalysis,
    sample_duplication_guide: DuplicationGuide,
) -> None:
    report = ReportBuilder().build_report(
        schema=sample_schema,
        analysis=sample_analysis,
        guide=sample_duplication_guide,
    )

    assert "## Quick Reference" in report
    assert "## Table Breakdown" in report
    assert "## Relationships & Flow" in report
    assert "### Projects" in report
    assert "### Tasks" in report
    assert "- [ ]" in report  # checklists should be present
    assert "```json" not in report  # simple fields should avoid raw JSON blocks
