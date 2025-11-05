"""Tests for the schema processor."""

from airtable_analyzer.models import AirtableBaseSchema
from airtable_analyzer.schema_processor import SchemaProcessor


def test_schema_processor_relationships(sample_schema: AirtableBaseSchema) -> None:
    processor = SchemaProcessor()
    analysis = processor.analyze_schema(sample_schema)

    assert analysis.suggested_table_creation_order[0] == "Projects"
    assert len(analysis.relationships) == 1
    relationship = analysis.relationships[0]
    assert relationship.from_table_name == "Tasks"
    assert relationship.to_table_name == "Projects"
    assert "linkedTableId" in relationship.configuration


def test_schema_processor_field_metadata(sample_schema: AirtableBaseSchema) -> None:
    analysis = SchemaProcessor().analyze_schema(sample_schema)
    projects_table = next(table for table in analysis.tables if table.name == "Projects")
    field = next(field for field in projects_table.fields if field.name == "Stage")
    assert field.configuration["choices"][0]["name"] == "Idea"
    assert not field.is_primary
