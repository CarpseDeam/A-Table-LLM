"""Demonstrate the analyzer workflow using mocked data."""

from __future__ import annotations

import logging
from airtable_analyzer.models import (
    AirtableBaseSchema,
    DuplicationGuide,
    DuplicationStep,
    DuplicationTableDetail,
)
from airtable_analyzer.report_builder import ReportBuilder
from airtable_analyzer.schema_processor import SchemaProcessor

SAMPLE_SCHEMA = {
    "id": "appDemo123",
    "name": "Sample Product Ops",
    "tables": [
        {
            "id": "tblProjects",
            "name": "Projects",
            "description": "High-level initiatives.",
            "primaryFieldId": "fldProjectName",
            "fields": [
                {
                    "id": "fldProjectName",
                    "name": "Project Name",
                    "type": "singleLineText",
                    "description": "The canonical project title.",
                    "isPrimaryField": True,
                },
                {
                    "id": "fldProjectStage",
                    "name": "Stage",
                    "type": "singleSelect",
                    "options": {
                        "choices": [
                            {"name": "Ideation"},
                            {"name": "Planning"},
                            {"name": "Execution"},
                            {"name": "Complete"},
                        ]
                    },
                },
                {
                    "id": "fldProjectLead",
                    "name": "Project Lead",
                    "type": "singleLineText",
                },
            ],
            "views": [
                {
                    "id": "viwProjectsDefault",
                    "name": "All Projects",
                    "type": "grid",
                    "fieldOrder": {
                        "fieldIds": [
                            "fldProjectName",
                            "fldProjectStage",
                            "fldProjectLead",
                        ]
                    },
                }
            ],
        },
        {
            "id": "tblTasks",
            "name": "Tasks",
            "description": "Detailed work items linked to projects.",
            "primaryFieldId": "fldTaskName",
            "fields": [
                {
                    "id": "fldTaskName",
                    "name": "Task",
                    "type": "singleLineText",
                    "isPrimaryField": True,
                },
                {
                    "id": "fldTaskProject",
                    "name": "Project",
                    "type": "multipleRecordLinks",
                    "options": {"linkedTableId": "tblProjects"},
                },
                {
                    "id": "fldTaskAssignee",
                    "name": "Assignee",
                    "type": "singleLineText",
                },
                {
                    "id": "fldTaskStatus",
                    "name": "Status",
                    "type": "singleSelect",
                    "options": {
                        "choices": [
                            {"name": "Not Started"},
                            {"name": "In Progress"},
                            {"name": "Blocked"},
                            {"name": "Done"},
                        ]
                    },
                },
            ],
            "views": [
                {
                    "id": "viwTaskByStatus",
                    "name": "Kanban",
                    "type": "kanban",
                    "fieldOrder": {
                        "fieldIds": [
                            "fldTaskName",
                            "fldTaskProject",
                            "fldTaskStatus",
                            "fldTaskAssignee",
                        ]
                    },
                    "sorts": [{"fieldId": "fldTaskStatus", "direction": "asc"}],
                }
            ],
        },
    ],
}


def build_mock_guide() -> DuplicationGuide:
    """Return a static duplication guide useful for demos."""
    return DuplicationGuide(
        base_overview="Two-table base that tracks projects and tasks with linked records.",
        key_considerations=[
            "Respect the Projects → Tasks dependency so lookups resolve correctly.",
            "Configure single select options before importing data to prevent mismatches.",
        ],
        table_details=[
            DuplicationTableDetail(
                table_name="Projects",
                summary="Create core project metadata fields and set default views.",
                field_instructions=[
                    "Add `Project Name` as the primary single line text field.",
                    "Configure `Stage` single select with the stages Ideation → Complete.",
                ],
                view_instructions=[
                    "Grid view should surface name, stage, and project lead fields."
                ],
                sequencing_notes=[
                    "Create this table before Tasks to satisfy linked record dependencies."
                ],
            ),
            DuplicationTableDetail(
                table_name="Tasks",
                summary="Track actionable tasks linked back to projects.",
                field_instructions=[
                    "Add a linked record field to Projects and allow multiple records.",
                    "Mirror the status single select values from the source base.",
                ],
                view_instructions=["Configure a Kanban view grouped by Status."],
                sequencing_notes=[
                    "Link to Projects after the Projects table is fully configured."
                ],
            ),
        ],
        relationships=[
            "Tasks.Project links each task to its parent project.",
            "Consider rollups or lookups on Projects to summarize task health.",
        ],
        duplication_steps=[
            DuplicationStep(
                order=1,
                title="Create the Projects table",
                description="Add core fields, select options, and grid view configuration.",
                prerequisites=[],
            ),
            DuplicationStep(
                order=2,
                title="Create the Tasks table",
                description="Define fields then link Project to the Projects table.",
                prerequisites=["Projects table created"],
            ),
            DuplicationStep(
                order=3,
                title="Configure lookups and validations",
                description="Verify linked records, single selects, and view filters.",
                prerequisites=["Tasks table created"],
            ),
        ],
        post_duplication_checks=[
            "Test creating a new project and associated tasks to validate linked records.",
            "Confirm Kanban view groups tasks by the correct select options.",
        ],
    )


def main() -> None:
    """Generate a markdown report without any external dependencies."""
    logging.basicConfig(level=logging.INFO)
    schema = AirtableBaseSchema.model_validate(SAMPLE_SCHEMA)
    processor = SchemaProcessor()
    analysis = processor.analyze_schema(schema)
    guide = build_mock_guide()
    report = ReportBuilder().build_report(schema, analysis, guide)
    print(report)


if __name__ == "__main__":
    main()
