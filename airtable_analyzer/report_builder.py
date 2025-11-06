
"""Markdown report generation."""

from __future__ import annotations

import json
import logging
import math
import re
from collections import Counter, OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .exceptions import ReportGenerationError
from .models import (
    AirtableBaseSchema,
    DuplicationGuide,
    DuplicationStep,
    DuplicationTableDetail,
    FieldSummary,
    RelationshipSummary,
    SchemaAnalysis,
    TableSummary,
    ViewSummary,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class ReportMetrics:
    """Calculated metrics used to enrich the report."""

    table_count: int
    field_count: int
    relationship_count: int
    formula_count: int
    lookup_count: int
    rollup_count: int
    linked_count: int
    single_select_count: int
    relationship_counter: Counter
    dependencies_by_target: Dict[str, List[str]]
    complexity_score: float
    complexity_label: str
    estimated_time_minutes: int
    critical_dependencies: List[str]


class ReportBuilder:
    """Build a human-readable markdown duplication guide."""

    SIMPLE_FIELD_TYPES = {
        "singleLineText",
        "multilineText",
        "email",
        "url",
        "phoneNumber",
        "number",
        "currency",
        "percent",
        "checkbox",
        "date",
        "dateTime",
        "duration",
        "rating",
    }
    COMPLEX_FIELD_TYPES = {
        "formula",
        "lookup",
        "rollup",
        "multipleRecordLinks",
        "singleRecordLink",
        "linkedRecord",
        "singleSelect",
        "multipleSelects",
    }
    RELATIONSHIP_FIELD_TYPES = {
        "multipleRecordLinks",
        "singleRecordLink",
        "linkedRecord",
    }
    ASSIGNMENT_FIELD_TYPES = {
        "user",
        "collaborator",
        "multipleCollaborators",
    }
    METADATA_FIELD_TYPES = {
        "createdTime",
        "lastModifiedTime",
        "createdBy",
        "lastModifiedBy",
        "autoNumber",
        "formula",
        "rollup",
        "lookup",
    }
    STATUS_FIELD_TYPES = {
        "singleSelect",
        "multipleSelects",
        "checkbox",
    }
    RATING_FIELD_TYPES = {"rating"}
    COLLAPSIBLE_THRESHOLD = 12

    STATUS_KEYWORDS = ("status", "stage", "state", "phase", "progress")
    ASSIGNMENT_KEYWORDS = ("assign", "owner", "lead", "manager", "responsible")
    METADATA_KEYWORDS = (
        "created",
        "updated",
        "modified",
        "timestamp",
        "notes",
        "description",
        "comment",
    )

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """Create a report builder."""
        self.logger = logger or LOGGER

    def build_report(
        self,
        schema: AirtableBaseSchema,
        analysis: SchemaAnalysis,
        guide: DuplicationGuide,
    ) -> str:
        """Create a markdown report from schema analysis and Gemini output."""
        try:
            table_lookup = {table.id: table.name for table in schema.tables}
            field_lookup = self._build_field_lookup(analysis)
            metrics = self._compute_metrics(analysis)

            lines: List[str] = []
            lines.append(f"# Airtable Base Duplication Guide: {schema.name}")
            lines.append("")

            lines.extend(
                self._format_quick_reference(
                    analysis=analysis,
                    metrics=metrics,
                    creation_order=analysis.suggested_table_creation_order,
                )
            )
            lines.append("")
            lines.append("---")
            lines.append("")

            lines.append("## Base Overview")
            lines.append("")
            lines.append(guide.base_overview.strip())
            lines.append("")

            if guide.key_considerations:
                lines.append("### Key Considerations")
                lines.extend([f"- {item}" for item in guide.key_considerations])
                lines.append("")

            lines.append("---")
            lines.append("")
            lines.extend(
                self._format_relationship_section(
                    analysis=analysis,
                    guide=guide,
                )
            )
            lines.append("")
            lines.append("---")
            lines.append("")

            table_detail_lookup = {
                detail.table_name: detail for detail in guide.table_details
            }

            lines.append("## Table Breakdown")
            lines.append("")
            for table in analysis.tables:
                lines.extend(
                    self._format_table_section(
                        table=table,
                        table_detail_lookup=table_detail_lookup,
                        table_lookup=table_lookup,
                        field_lookup=field_lookup,
                        metrics=metrics,
                    )
                )
                lines.append("")

            if guide.duplication_steps:
                lines.append("---")
                lines.append("")
                lines.extend(self._format_duplication_steps(guide.duplication_steps))

            if guide.post_duplication_checks:
                lines.append("")
                lines.append("## Post-duplication Validation")
                lines.extend([f"- {item}" for item in guide.post_duplication_checks])

            return "\n".join(lines).strip()
        except Exception as exc:  # noqa: BLE001
            raise ReportGenerationError("Failed to build markdown report.") from exc

    def _format_quick_reference(
        self,
        analysis: SchemaAnalysis,
        metrics: ReportMetrics,
        creation_order: Sequence[str],
    ) -> List[str]:
        lines = ["## Quick Reference", ""]
        relationships_by_type = ", ".join(
            f"{self._humanize_relationship_type(rel_type)} x {count}"
            for rel_type, count in metrics.relationship_counter.most_common()
        )

        calculated_summary = (
            f"{metrics.formula_count} formulas, {metrics.lookup_count} lookups, "
            f"{metrics.rollup_count} rollups"
        )

        lines.append(
            "- **Structure:** "
            f"{metrics.table_count} tables · {metrics.field_count} fields · "
            f"{metrics.relationship_count} relationships"
        )
        lines.append(f"- **Calculated fields:** {calculated_summary}")
        lines.append(
            f"- **Complexity:** {metrics.complexity_label} "
            f"(score {int(round(metrics.complexity_score))})"
        )
        lines.append(
            f"- **Estimated duplication time:** "
            f"{self._format_time_estimate(metrics.estimated_time_minutes)}"
        )
        if relationships_by_type:
            lines.append(f"- **Relationships by type:** {relationships_by_type}")

        if creation_order:
            lines.append("")
            lines.append("**Table creation sequence**")
            for index, table_name in enumerate(creation_order, start=1):
                lines.append(f"{index}. {table_name}")

        if metrics.critical_dependencies:
            lines.append("")
            lines.append("**Critical dependencies**")
            for dependency in metrics.critical_dependencies:
                lines.append(f"- {dependency}")

        if not metrics.relationship_count:
            lines.append("")
            lines.append("_No cross-table relationships detected._")

        has_views = any(table.views for table in analysis.tables)
        if not has_views:
            lines.append("")
            lines.append(
                "_View configurations were not returned by the API; capture key views manually._"
            )

        return lines
    def _format_relationship_section(
        self,
        analysis: SchemaAnalysis,
        guide: DuplicationGuide,
    ) -> List[str]:
        lines: List[str] = ["## Relationships & Flow", ""]
        diagram_lines = self._build_relationship_diagram_lines(analysis.relationships)
        if diagram_lines:
            lines.append("```")
            lines.extend(diagram_lines)
            lines.append("```")
        else:
            lines.append("_No relationships to visualize._")

        key_relationships = self._build_key_relationship_summaries(analysis.relationships)
        if key_relationships:
            lines.append("")
            lines.append("**Key relationships**")
            lines.extend([f"- {item}" for item in key_relationships])

        if guide.relationships:
            lines.append("")
            lines.append("**LLM insights**")
            lines.extend([f"- {item}" for item in guide.relationships])

        return lines

    def _format_table_section(
        self,
        table: TableSummary,
        table_detail_lookup: Dict[str, DuplicationTableDetail],
        table_lookup: Dict[str, str],
        field_lookup: Dict[str, Tuple[str, str]],
        metrics: ReportMetrics,
    ) -> List[str]:
        lines: List[str] = [f"### {table.name}"]
        if table.description:
            lines.append(table.description)

        detail = table_detail_lookup.get(table.name)
        if detail:
            lines.append(detail.summary)

        dependency_names = [table_lookup.get(dep, dep) for dep in table.dependencies]
        dependency_names = [name for name in dependency_names if name]
        if dependency_names:
            lines.append(f"- Depends on: {', '.join(dependency_names)}")

        dependents = metrics.dependencies_by_target.get(table.name, [])
        if dependents:
            unique_dependents = sorted(set(dependents))
            lines.append(f"- Supports: {', '.join(unique_dependents)}")

        if detail and detail.sequencing_notes:
            lines.append("- Sequencing notes:")
            lines.extend([f"  - {note}" for note in detail.sequencing_notes])

        lines.append("")
        lines.extend(
            self._format_fields_section(
                table=table,
                table_lookup=table_lookup,
                field_lookup=field_lookup,
            )
        )

        if detail and detail.field_instructions:
            lines.append("")
            lines.append("#### Gemini Guidance")
            lines.extend([f"- {instruction}" for instruction in detail.field_instructions])

        view_lines = self._format_table_views(table.views)
        if view_lines:
            lines.append("")
            lines.extend(view_lines)

        if detail and detail.view_instructions:
            if view_lines:
                lines.append("")
                lines.append("**Gemini view notes**")
            else:
                lines.append("")
                lines.append("#### View Notes")
            lines.extend([f"- {instruction}" for instruction in detail.view_instructions])

        return lines
    def _format_fields_section(
        self,
        table: TableSummary,
        table_lookup: Dict[str, str],
        field_lookup: Dict[str, Tuple[str, str]],
    ) -> List[str]:
        fields = table.fields
        field_groups = self._group_fields(fields)
        field_count = len(fields)

        lines: List[str] = ["#### Fields"]
        content: List[str] = []
        for group_name, group_fields in field_groups.items():
            if not group_fields:
                continue
            content.append(f"**{group_name}**")
            for field in group_fields:
                content.extend(
                    self._format_field_entry(
                        field=field,
                        table_lookup=table_lookup,
                        field_lookup=field_lookup,
                    )
                )
            content.append("")

        while content and not content[-1]:
            content.pop()

        if field_count > self.COLLAPSIBLE_THRESHOLD and content:
            lines.append(
                "<details>\n<summary><strong>Field groups "
                f"({field_count} fields)</strong></summary>"
            )
            lines.append("")
            lines.extend(content)
            lines.append("</details>")
        else:
            lines.extend(content)

        return lines

    def _format_field_entry(
        self,
        field: FieldSummary,
        table_lookup: Dict[str, str],
        field_lookup: Dict[str, Tuple[str, str]],
    ) -> List[str]:
        display_type = self._humanize_field_type(field.type)
        inline_highlights = self._inline_configuration_highlights(field)
        inline_suffix = f" | {', '.join(inline_highlights)}" if inline_highlights else ""
        header = f"- `{field.name}` ({display_type}){inline_suffix}"
        lines = [header]

        if field.description:
            lines.append(f"  - {field.description}")

        if field.linked_table_name:
            lines.append(f"  - Links to `{field.linked_table_name}`")

        lines.extend(
            self._format_field_specific_details(
                field=field,
                table_lookup=table_lookup,
                field_lookup=field_lookup,
            )
        )

        return lines

    def _format_field_specific_details(
        self,
        field: FieldSummary,
        table_lookup: Dict[str, str],
        field_lookup: Dict[str, Tuple[str, str]],
    ) -> List[str]:
        config = field.configuration or {}
        field_type = field.type
        lines: List[str] = []

        if field_type in {"singleSelect", "multipleSelects"}:
            options = self._extract_select_options(config)
            if options:
                lines.append(f"  - Options: {', '.join(options)}")

        if field_type in self.RELATIONSHIP_FIELD_TYPES:
            relationship_details = self._describe_linked_record(field, config)
            if relationship_details:
                lines.append(f"  - {relationship_details}")

        if field_type == "lookup":
            lookup_details = self._describe_lookup(field, config, field_lookup, table_lookup)
            if lookup_details:
                lines.append(f"  - {lookup_details}")

        if field_type == "rollup":
            rollup_details = self._describe_rollup(field, config, field_lookup, table_lookup)
            if rollup_details:
                lines.append(f"  - {rollup_details}")

        if field_type == "formula":
            lines.extend(self._format_formula_details(config))

        if field_type not in self.COMPLEX_FIELD_TYPES:
            inline_notes = self._render_simple_configuration_notes(config)
            lines.extend([f"  - {note}" for note in inline_notes])

        if not lines and config:
            serialized = json.dumps(config, indent=2, ensure_ascii=False)
            lines.append("  - Configuration:")
            lines.append("    ```json")
            for config_line in serialized.splitlines():
                lines.append(f"    {config_line}")
            lines.append("    ```")

        return lines

    def _format_formula_details(self, config: Dict[str, object]) -> List[str]:
        lines: List[str] = []
        formula = config.get("formula")
        if isinstance(formula, str) and formula.strip():
            lines.append("  - Formula:")
            lines.append("    ```text")
            for formula_line in formula.strip().splitlines():
                lines.append(f"    {formula_line}")
            lines.append("    ```")
            description = self._describe_formula(formula)
            if description:
                lines.append(f"  - Purpose: {description}")
            referenced_fields = self._extract_referenced_fields(formula)
            if referenced_fields:
                field_list = ", ".join(f"`{name}`" for name in referenced_fields)
                lines.append(f"  - Uses: {field_list}")
        return lines

    def _format_table_views(
        self,
        views: Sequence[ViewSummary],
    ) -> List[str]:
        if not views:
            return []

        lines: List[str] = ["#### Views"]
        for view in views:
            summary = f"- `{view.name}` ({view.type or 'custom'})"
            lines.append(summary)
            if view.description:
                lines.append(f"  - {view.description}")
            if view.visible_fields:
                fields_display = ", ".join(f"`{field}`" for field in view.visible_fields[:12])
                if len(view.visible_fields) > 12:
                    fields_display += ", ..."
                lines.append(f"  - Visible fields: {fields_display}")
            if view.sorts:
                sort_summary = self._format_view_sort(view.sorts)
                if sort_summary:
                    lines.append(f"  - Sort: {sort_summary}")
            if view.filters:
                filter_summary = self._summarize_filter(view.filters)
                lines.append(f"  - Filters: {filter_summary}")
            if view.groups:
                group_fields = ", ".join(
                    f"`{group.get('fieldId', 'field')}`"
                    for group in view.groups
                    if isinstance(group, dict)
                )
                if group_fields:
                    lines.append(f"  - Grouped by: {group_fields}")

        return lines
    def _format_duplication_steps(self, steps: Sequence[DuplicationStep]) -> List[str]:
        lines: List[str] = ["## Duplication Steps", ""]
        ordered_steps = sorted(steps, key=lambda step: step.order)
        total_steps = len(ordered_steps)

        for index, step in enumerate(ordered_steps, start=1):
            lines.extend(self._format_duplication_step(step, total_steps, index))
            if index < total_steps:
                lines.append("")

        return lines

    def _format_duplication_step(
        self,
        step: DuplicationStep,
        total_steps: int,
        sequence_index: int,
    ) -> List[str]:
        lines: List[str] = [f"### Step {step.order}: {step.title}", ""]
        complexity = self._classify_step_complexity(step)
        estimated_time = self._estimate_step_time(step, complexity)
        execution_note = self._build_execution_note(step)

        lines.append(f"- **Complexity:** {complexity}")
        lines.append(f"- **Estimated time:** {self._format_time_estimate(estimated_time)}")
        lines.append(f"- **Execution:** {execution_note}")

        tasks = self._extract_tasks(step.description)
        if tasks:
            lines.append("")
            lines.append("Tasks:")
            lines.extend([f"- [ ] {task}" for task in tasks])

        if step.description:
            lines.append("")
            lines.append(step.description.strip())

        if step.prerequisites:
            lines.append("")
            lines.append("**Prerequisites**")
            lines.extend([f"- {item}" for item in step.prerequisites])

        return lines

    def _build_field_lookup(self, analysis: SchemaAnalysis) -> Dict[str, Tuple[str, str]]:
        lookup: Dict[str, Tuple[str, str]] = {}
        for table in analysis.tables:
            for field in table.fields:
                lookup[field.id] = (table.name, field.name)
        return lookup

    def _compute_metrics(
        self,
        analysis: SchemaAnalysis,
    ) -> ReportMetrics:
        table_count = len(analysis.tables)
        field_count = sum(len(table.fields) for table in analysis.tables)

        formula_count = 0
        lookup_count = 0
        rollup_count = 0
        linked_count = 0
        single_select_count = 0

        for table in analysis.tables:
            for field in table.fields:
                field_type = field.type
                if field_type == "formula":
                    formula_count += 1
                elif field_type == "lookup":
                    lookup_count += 1
                elif field_type == "rollup":
                    rollup_count += 1
                if field_type in self.RELATIONSHIP_FIELD_TYPES:
                    linked_count += 1
                if field_type == "singleSelect":
                    single_select_count += 1

        relationships = analysis.relationships
        relationship_counter = Counter(rel.relationship_type for rel in relationships)
        relationship_count = len(relationships)

        dependencies_by_target: Dict[str, List[str]] = defaultdict(list)
        for rel in relationships:
            dependencies_by_target[rel.to_table_name].append(rel.from_table_name)

        complexity_score = (
            table_count * 5
            + field_count * 0.6
            + relationship_count * 4
            + formula_count * 6
            + lookup_count * 4
            + rollup_count * 4.5
            + linked_count * 3
        )

        complexity_label = self._classify_complexity(complexity_score)

        estimated_time_minutes = max(
            30,
            int(
                table_count * 20
                + field_count * 1.2
                + relationship_count * 5
                + formula_count * 8
                + lookup_count * 6
                + rollup_count * 6
            ),
        )

        critical_dependencies = self._identify_critical_dependencies(dependencies_by_target)

        return ReportMetrics(
            table_count=table_count,
            field_count=field_count,
            relationship_count=relationship_count,
            formula_count=formula_count,
            lookup_count=lookup_count,
            rollup_count=rollup_count,
            linked_count=linked_count,
            single_select_count=single_select_count,
            relationship_counter=relationship_counter,
            dependencies_by_target=dependencies_by_target,
            complexity_score=complexity_score,
            complexity_label=complexity_label,
            estimated_time_minutes=estimated_time_minutes,
            critical_dependencies=critical_dependencies,
        )
    def _classify_complexity(self, score: float) -> str:
        if score < 60:
            return "Low"
        if score < 120:
            return "Moderate"
        if score < 180:
            return "High"
        return "Very High"

    def _identify_critical_dependencies(
        self, dependencies_by_target: Dict[str, List[str]]
    ) -> List[str]:
        entries: List[Tuple[str, List[str]]] = [
            (table_name, sources)
            for table_name, sources in dependencies_by_target.items()
            if sources
        ]
        entries.sort(key=lambda item: len(set(item[1])), reverse=True)

        critical: List[str] = []
        for table_name, sources in entries:
            unique_sources = sorted(set(sources))
            if not unique_sources:
                continue
            critical.append(f"{table_name} referenced by {', '.join(unique_sources)}")
            if len(critical) >= 5:
                break
        return critical

    def _build_relationship_diagram_lines(
        self, relationships: Sequence[RelationshipSummary]
    ) -> List[str]:
        if not relationships:
            return []

        adjacency: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        all_tables = set()
        for rel in relationships:
            adjacency[rel.from_table_name].append(
                (rel.to_table_name, self._humanize_relationship_type(rel.relationship_type))
            )
            all_tables.add(rel.from_table_name)
            all_tables.add(rel.to_table_name)

        diagram_lines: List[str] = []
        for table_name in sorted(all_tables):
            diagram_lines.append(f"[{table_name}]")
            edges = sorted(adjacency.get(table_name, []), key=lambda item: item[0])
            if edges:
                for idx, (target, rel_type) in enumerate(edges):
                    prefix = "  |--" if idx < len(edges) - 1 else "  '--"
                    diagram_lines.append(f"{prefix}({rel_type})--> [{target}]")
            else:
                diagram_lines.append("  '-- no outgoing links")
            diagram_lines.append("")

        while diagram_lines and not diagram_lines[-1]:
            diagram_lines.pop()

        return diagram_lines

    def _build_key_relationship_summaries(
        self, relationships: Sequence[RelationshipSummary]
    ) -> List[str]:
        if not relationships:
            return []

        summary_map: Dict[Tuple[str, str], Counter] = defaultdict(Counter)
        for rel in relationships:
            summary_map[(rel.from_table_name, rel.to_table_name)][
                self._humanize_relationship_type(rel.relationship_type)
            ] += 1

        items: List[str] = []
        for (source, target), counter in sorted(summary_map.items()):
            type_segments = [
                f"{rel_type} x {count}" if count > 1 else rel_type
                for rel_type, count in sorted(counter.items())
            ]
            items.append(f"{source} -> {target} ({', '.join(type_segments)})")

        return items

    def _group_fields(self, fields: Sequence[FieldSummary]) -> "OrderedDict[str, List[FieldSummary]]":
        grouped: "OrderedDict[str, List[FieldSummary]]" = OrderedDict(
            (
                ("Core Fields", []),
                ("Relationship Fields", []),
                ("Assignment Fields", []),
                ("Status Management", []),
                ("Rating Fields", []),
                ("Calculated Fields", []),
                ("Metadata Fields", []),
                ("Other Fields", []),
            )
        )

        for field in fields:
            category = self._categorize_field(field)
            grouped[category].append(field)

        return OrderedDict((name, items) for name, items in grouped.items() if items)

    def _categorize_field(self, field: FieldSummary) -> str:
        field_type = field.type
        name_lower = field.name.lower()

        if field.is_primary or field_type in {"singleLineText", "multilineText"}:
            return "Core Fields"
        if field_type in self.RELATIONSHIP_FIELD_TYPES or field.linked_table_name:
            return "Relationship Fields"
        if field_type in self.ASSIGNMENT_FIELD_TYPES or any(
            keyword in name_lower for keyword in self.ASSIGNMENT_KEYWORDS
        ):
            return "Assignment Fields"
        if field_type in self.STATUS_FIELD_TYPES or any(
            keyword in name_lower for keyword in self.STATUS_KEYWORDS
        ):
            return "Status Management"
        if field_type in self.RATING_FIELD_TYPES or "rating" in name_lower:
            return "Rating Fields"
        if field_type in {"formula", "lookup", "rollup"}:
            return "Calculated Fields"
        if field_type in self.METADATA_FIELD_TYPES or any(
            keyword in name_lower for keyword in self.METADATA_KEYWORDS
        ):
            return "Metadata Fields"
        return "Other Fields"

    def _inline_configuration_highlights(self, field: FieldSummary) -> List[str]:
        config = field.configuration or {}
        field_type = field.type
        highlights: List[str] = []

        if field.is_primary:
            highlights.append("primary")

        if field_type in {"number", "currency", "percent"}:
            precision = config.get("precision")
            if isinstance(precision, (int, float)):
                highlights.append(f"precision {precision}")
            symbol = config.get("symbol")
            if isinstance(symbol, str):
                highlights.append(f"symbol '{symbol}'")

        if field_type == "checkbox":
            color = config.get("color")
            if isinstance(color, str):
                highlights.append(f"color {color}")

        if field_type == "rating":
            max_value = config.get("max")
            if isinstance(max_value, (int, float)):
                highlights.append(f"max {max_value}")

        if field_type in {"date", "dateTime"}:
            format_str = config.get("format")
            if isinstance(format_str, dict):
                name = format_str.get("name") or format_str.get("format")
                if isinstance(name, str):
                    highlights.append(f"format {name}")

        return highlights

    def _describe_linked_record(self, field: FieldSummary, config: Dict[str, object]) -> str:
        allow_multiple = config.get("allowMultipleRecords")
        prefers_single = config.get("prefersSingleRecordLink")

        if allow_multiple is True:
            cardinality = "multiple records"
        elif allow_multiple is False or prefers_single:
            cardinality = "one record"
        else:
            cardinality = "one or many records"

        return f"Stores {cardinality} from the linked table"

    def _describe_lookup(
        self,
        field: FieldSummary,
        config: Dict[str, object],
        field_lookup: Dict[str, Tuple[str, str]],
        table_lookup: Dict[str, str],
    ) -> str:
        lookup_config = config.get("lookup")
        if isinstance(lookup_config, dict):
            lookup_field_id = lookup_config.get("fieldId") or lookup_config.get("lookupFieldId")
            relationship_field_id = lookup_config.get("relationshipFieldId")
            linked_table_id = lookup_config.get("linkedTableId")
        else:
            lookup_field_id = config.get("lookupFieldId") or config.get("fieldId")
            relationship_field_id = config.get("recordLinkFieldId")
            linked_table_id = config.get("linkedTableId")

        if lookup_field_id in field_lookup:
            table_name, field_name = field_lookup[lookup_field_id]
            source_desc = f"{table_name} -> {field_name}"
        else:
            source_desc = f"field {lookup_field_id}" if lookup_field_id else "linked records"

        if relationship_field_id in field_lookup:
            _, relation_field_name = field_lookup[relationship_field_id]
            relation_desc = f"via {relation_field_name}"
        else:
            relation_desc = "via linked records"

        if linked_table_id and linked_table_id in table_lookup:
            target_table = table_lookup[linked_table_id]
            source_desc = f"{target_table} -> {source_desc.split('->')[-1].strip()}"

        return f"Pulls values from {source_desc} {relation_desc}".strip()

    def _describe_rollup(
        self,
        field: FieldSummary,
        config: Dict[str, object],
        field_lookup: Dict[str, Tuple[str, str]],
        table_lookup: Dict[str, str],
    ) -> str:
        rollup_config: Dict[str, object]
        if isinstance(config.get("rollup"), dict):
            rollup_config = config["rollup"]  # type: ignore[assignment]
        else:
            rollup_config = config

        aggregation = rollup_config.get("aggregation")
        agg_display = aggregation.replace("_", " ") if isinstance(aggregation, str) else "aggregation"

        field_id = rollup_config.get("fieldId")
        relation_field_id = rollup_config.get("recordLinkFieldId")
        linked_table_id = rollup_config.get("linkedTableId")

        if field_id in field_lookup:
            table_name, field_name = field_lookup[field_id]  # type: ignore[index]
            target_desc = f"{table_name} -> {field_name}"
        else:
            target_desc = f"field {field_id}" if field_id else "the linked table"

        if relation_field_id in field_lookup:
            _, relation_name = field_lookup[relation_field_id]  # type: ignore[index]
            relation_desc = f"via {relation_name}"
        else:
            relation_desc = "via linked records"

        if linked_table_id and linked_table_id in table_lookup:
            table_name = table_lookup[linked_table_id]
            target_desc = f"{table_name} -> {target_desc.split('->')[-1].strip()}"

        return f"Rolls up {target_desc} using {agg_display} {relation_desc}".strip()

    def _render_simple_configuration_notes(self, config: Dict[str, object]) -> List[str]:
        notes: List[str] = []
        if not isinstance(config, dict):
            return notes

        for key in ("defaultValue", "allowNegativeNumbers", "useThousandsSeparator"):
            value = config.get(key)
            if value not in (None, ""):
                pretty_key = key.replace("_", " ").replace("allow", "allow ")
                notes.append(f"{pretty_key}: {value}")

        return notes

    def _extract_select_options(self, config: Dict[str, object]) -> List[str]:
        choices = config.get("choices")
        if not isinstance(choices, list):
            return []
        options: List[str] = []
        for choice in choices:
            if isinstance(choice, dict):
                name = choice.get("name")
                if isinstance(name, str):
                    options.append(name)
        return options

    def _format_view_sort(self, sorts: Optional[Sequence[Dict[str, object]]]) -> str:
        if not sorts:
            return ""
        segments: List[str] = []
        for sort in sorts:
            if not isinstance(sort, dict):
                continue
            field_id = sort.get("fieldId") or sort.get("field")
            direction = sort.get("direction", "asc")
            if field_id:
                segments.append(f"{field_id} {direction}")
        return ", ".join(segments)

    def _summarize_filter(self, filters: Dict[str, object]) -> str:
        if not isinstance(filters, dict):
            return "custom logic"
        formula = filters.get("formula")
        if isinstance(formula, dict):
            return formula.get("text", "formula filter")
        if isinstance(formula, str):
            return formula
        return "configured"

    def _classify_step_complexity(self, step: DuplicationStep) -> str:
        description = step.description or ""
        word_count = len(re.findall(r"\w+", description))
        score = word_count / 12 + len(step.prerequisites) * 0.75
        if score < 2.5:
            return "Low"
        if score < 4.5:
            return "Moderate"
        return "High"

    def _estimate_step_time(self, step: DuplicationStep, complexity: str) -> int:
        description = step.description or ""
        word_count = len(re.findall(r"\w+", description))
        base = 20 + word_count * 1.1 + len(step.prerequisites) * 10
        complexity_bonus = {"Low": 10, "Moderate": 25, "High": 45}.get(complexity, 20)
        estimate = base + complexity_bonus
        return max(15, int(math.ceil(estimate / 5.0)) * 5)

    def _build_execution_note(self, step: DuplicationStep) -> str:
        if not step.prerequisites:
            return "Parallel-friendly once base access is granted"
        if len(step.prerequisites) == 1:
            return f"Sequential - wait for {step.prerequisites[0]}"
        prereq_list = ", ".join(step.prerequisites)
        return f"Sequential - depends on {prereq_list}"

    def _extract_tasks(self, description: str) -> List[str]:
        if not description:
            return []
        fragments = [frag.strip() for frag in re.split(r"[.;]", description) if frag.strip()]
        tasks = [frag[0].upper() + frag[1:] if len(frag) > 1 else frag for frag in fragments]
        return [task for task in tasks if len(task.split()) > 2]

    def _humanize_relationship_type(self, rel_type: str) -> str:
        clean = rel_type.replace("_", " ")
        return clean.lower()

    def _humanize_field_type(self, field_type: str) -> str:
        spaced = re.sub(r"(?<!^)(?=[A-Z])", " ", field_type)
        spaced = spaced.replace("_", " ")
        return spaced.lower()

    def _format_time_estimate(self, minutes: int) -> str:
        hours, mins = divmod(minutes, 60)
        if hours and mins:
            return f"~{hours} hr {mins} min"
        if hours:
            unit = "hr" if hours == 1 else "hrs"
            return f"~{hours} {unit}"
        return f"~{minutes} min"

    def _describe_formula(self, formula: str) -> str:
        uppercase = formula.upper()
        descriptors: List[str] = []
        if "IF(" in uppercase or "SWITCH(" in uppercase:
            descriptors.append("Evaluates conditions to choose outputs")
        if any(keyword in uppercase for keyword in ("SUM(", "AVERAGE(", "COUNT(", "MIN(", "MAX(")):
            descriptors.append("Aggregates numeric values")
        if any(keyword in uppercase for keyword in ("DATETIME", "DATE", "NOW(", "TODAY(")):
            descriptors.append("Works with dates or times")
        if any(keyword in uppercase for keyword in ("FIND(", "SEARCH(", "REGEX")):
            descriptors.append("Checks text content")
        if "+" in formula or "&" in formula:
            descriptors.append("Combines multiple fields")
        if not descriptors:
            return "Derives a calculated value from the referenced fields"
        return "; ".join(descriptors)

    def _extract_referenced_fields(self, formula: str) -> List[str]:
        matches = re.findall(r"\{([^}]+)\}", formula)
        seen = set()
        ordered: List[str] = []
        for match in matches:
            if match not in seen:
                seen.add(match)
                ordered.append(match)
        return ordered
