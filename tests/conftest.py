"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from airtable_analyzer.models import (  # noqa: E402
    AirtableBaseSchema,
    DuplicationGuide,
    DuplicationStep,
    DuplicationTableDetail,
)
from airtable_analyzer.schema_processor import SchemaProcessor


@pytest.fixture
def sample_schema_payload() -> dict:
    """Return a representative Airtable schema payload."""
    return {
        "id": "appSample",
        "name": "Sample Operations",
        "tables": [
            {
                "id": "tblProjects",
                "name": "Projects",
                "description": "Projects table",
                "primaryFieldId": "fldProjectName",
                "fields": [
                    {
                        "id": "fldProjectName",
                        "name": "Project Name",
                        "type": "singleLineText",
                        "isPrimaryField": True,
                    },
                    {
                        "id": "fldProjectStage",
                        "name": "Stage",
                        "type": "singleSelect",
                        "options": {
                            "choices": [
                                {"name": "Idea"},
                                {"name": "Active"},
                                {"name": "Done"},
                            ]
                        },
                    },
                ],
                "views": [
                    {
                        "id": "viwProjects",
                        "name": "All Projects",
                        "type": "grid",
                        "fieldOrder": {"fieldIds": ["fldProjectName", "fldProjectStage"]},
                    }
                ],
            },
            {
                "id": "tblTasks",
                "name": "Tasks",
                "description": "Tasks table",
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
                ],
                "views": [
                    {
                        "id": "viwTasks",
                        "name": "Board",
                        "type": "kanban",
                        "fieldOrder": {"fieldIds": ["fldTaskName", "fldTaskStatus"]},
                    }
                ],
            },
        ],
    }


@pytest.fixture
def sample_schema(sample_schema_payload: dict) -> AirtableBaseSchema:
    """Return a validated Airtable base schema."""
    return AirtableBaseSchema.model_validate(sample_schema_payload)


@pytest.fixture
def sample_analysis(sample_schema: AirtableBaseSchema):
    """Return processed schema analysis."""
    return SchemaProcessor().analyze_schema(sample_schema)


@pytest.fixture
def sample_duplication_guide() -> DuplicationGuide:
    """Return a representative Gemini response."""
    return DuplicationGuide(
        base_overview="Overview text.",
        key_considerations=["Keep dependencies intact."],
        table_details=[
            DuplicationTableDetail(
                table_name="Projects",
                summary="Create Projects table.",
                field_instructions=["Add Stage select options."],
                view_instructions=["Ensure grid view shows stage."],
                sequencing_notes=["Build Projects before Tasks."],
            ),
            DuplicationTableDetail(
                table_name="Tasks",
                summary="Create Tasks table.",
                field_instructions=["Link to Projects table."],
                view_instructions=["Set up Kanban view."],
                sequencing_notes=["Create after Projects."],
            ),
        ],
        relationships=["Tasks.Project links to Projects."],
        duplication_steps=[
            DuplicationStep(
                order=1,
                title="Create Projects",
                description="Add fields.",
                prerequisites=[],
            ),
            DuplicationStep(
                order=2,
                title="Create Tasks",
                description="Add linked record.",
                prerequisites=["Projects complete"],
            ),
        ],
        post_duplication_checks=[
            "Create sample project and task to validate relations."
        ],
    )
