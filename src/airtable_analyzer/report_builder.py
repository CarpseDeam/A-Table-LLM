"""Markdown report generation."""

from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional

from .exceptions import ReportGenerationError
from .models import (
    AirtableBaseSchema,
    DuplicationGuide,
    DuplicationTableDetail,
    FieldSummary,
    SchemaAnalysis,
    TableSummary,
)

LOGGER = logging.getLogger(__name__)


class ReportBuilder:
    """Build a human-readable markdown duplication guide."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """Create a report builder."""
        self.logger = logger or LOGGER

    def build_report(
        self,
        schema: AirtableBaseSchema,
        analysis: SchemaAnalysis,
        guide: DuplicationGuide,
    ) -> str:
        """Create a markdown report from schema analysis and Gemini output.

        Args:
            schema: Original Airtable base schema.
            analysis: Normalized schema analysis.
            guide: Gemini-generated duplication guidance.

        Returns:
            Markdown document containing the duplication instructions.
        """
        try:
            lines: List[str] = []
            lines.append(f"# Airtable Base Duplication Guide: {schema.name}")
            lines.append("")
            lines.append("## Base Overview")
            lines.append("")
            lines.append(guide.base_overview)
            lines.append("")

            if guide.key_considerations:
                lines.append("### Key Considerations")
                lines.extend([f"- {item}" for item in guide.key_considerations])
                lines.append("")

            table_detail_lookup = {
                detail.table_name: detail for detail in guide.table_details
            }

            lines.append("## Table Breakdown")
            lines.append("")
            for table in analysis.tables:
                lines.extend(self._format_table_section(table, table_detail_lookup))
                lines.append("")

            lines.append("## Relationships")
            lines.append("")
            if guide.relationships:
                lines.append("### LLM Insights")
                lines.extend([f"- {item}" for item in guide.relationships])
                lines.append("")

            if analysis.relationships:
                lines.append("### Detected Links")
                for relationship in analysis.relationships:
                    detail = (
                        f"- `{relationship.from_table_name}`.`{relationship.from_field_name}` "
                        f"→ `{relationship.to_table_name}` ({relationship.relationship_type})"
                    )
                    lines.append(detail)
                    if relationship.configuration:
                        configuration = json.dumps(
                            relationship.configuration, indent=2, ensure_ascii=False
                        )
                        lines.append("")
                        lines.append("```json")
                        lines.append(configuration)
                        lines.append("```")
                        lines.append("")
                lines.append("")

            lines.append("## Views Overview")
            lines.append("")
            for table in analysis.tables:
                if not table.views:
                    continue
                lines.append(f"### {table.name}")
                for view in table.views:
                    lines.append(f"- **{view.name}** ({view.type or 'custom'} view)")
                    if view.description:
                        lines.append(f"  - {view.description}")
                    if view.visible_fields:
                        visible = ", ".join(f"`{field}`" for field in view.visible_fields)
                        lines.append(f"  - Visible fields: {visible}")
                    if view.filters:
                        lines.append("  - Filters:")
                        lines.append("```json")
                        lines.append(json.dumps(view.filters, indent=2, ensure_ascii=False))
                        lines.append("```")
                    if view.sorts:
                        lines.append("  - Sorts:")
                        lines.append("```json")
                        lines.append(json.dumps(view.sorts, indent=2, ensure_ascii=False))
                        lines.append("```")
                    if view.groups:
                        lines.append("  - Groups:")
                        lines.append("```json")
                        lines.append(json.dumps(view.groups, indent=2, ensure_ascii=False))
                        lines.append("```")
                lines.append("")

            if guide.duplication_steps:
                lines.append("## Duplication Steps")
                lines.append("")
                for step in sorted(guide.duplication_steps, key=lambda s: s.order):
                    lines.append(f"### Step {step.order}: {step.title}")
                    lines.append("")
                    lines.append(step.description)
                    lines.append("")
                    if step.prerequisites:
                        lines.append("**Prerequisites**")
                        lines.extend([f"- {item}" for item in step.prerequisites])
                        lines.append("")

            if guide.post_duplication_checks:
                lines.append("## Post-duplication Validation")
                lines.extend([f"- {item}" for item in guide.post_duplication_checks])

            return "\n".join(lines).strip()
        except Exception as exc:  # noqa: BLE001
            raise ReportGenerationError("Failed to build markdown report.") from exc

    def _format_table_section(
        self,
        table: TableSummary,
        table_detail_lookup: Dict[str, DuplicationTableDetail],
    ) -> List[str]:
        lines: List[str] = [f"### {table.name}"]
        detail = table_detail_lookup.get(table.name)
        if detail:
            lines.append(detail.summary)

        if table.dependencies:
            dependencies = ", ".join(table.dependencies)
            lines.append(f"- Depends on: {dependencies}")

        if detail and detail.sequencing_notes:
            lines.append("- Sequencing notes:")
            lines.extend([f"  - {note}" for note in detail.sequencing_notes])

        lines.append("")
        lines.append("#### Fields")
        lines.extend(self._format_fields(table.fields))
        lines.append("")

        if detail and detail.field_instructions:
            lines.append("##### Gemini Guidance")
            lines.extend([f"- {instruction}" for instruction in detail.field_instructions])
            lines.append("")

        if detail and detail.view_instructions:
            lines.append("#### View Notes")
            lines.extend([f"- {instruction}" for instruction in detail.view_instructions])

        return lines

    def _format_fields(self, fields: List[FieldSummary]) -> List[str]:
        lines: List[str] = []
        for field in fields:
            header = f"- `{field.name}` ({field.type})"
            if field.description:
                header += f": {field.description}"
            lines.append(header)
            if field.configuration:
                configuration = json.dumps(field.configuration, indent=2, ensure_ascii=False)
                lines.append("```json")
                lines.append(configuration)
                lines.append("```")
            if field.linked_table_name:
                lines.append(
                    f"  - Links to `{field.linked_table_name}` ({field.linked_table_id})"
                )
        return lines
