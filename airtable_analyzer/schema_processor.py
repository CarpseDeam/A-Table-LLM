"""Schema processing utilities for Airtable metadata."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .models import (
    AirtableBaseSchema,
    AirtableTable,
    FieldSummary,
    RelationshipSummary,
    SchemaAnalysis,
    TableSummary,
    ViewSummary,
)

LOGGER = logging.getLogger(__name__)


class SchemaProcessor:
    """Process Airtable schemas into analysis artifacts."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """Create a new schema processor."""
        self.logger = logger or LOGGER

    def analyze_schema(self, schema: AirtableBaseSchema) -> SchemaAnalysis:
        """Generate a normalized schema analysis from Airtable metadata.

        Args:
            schema: Airtable base schema.

        Returns:
            SchemaAnalysis containing normalized metadata for downstream components.
        """
        table_lookup = {table.id: table.name for table in schema.tables}
        table_summaries: List[TableSummary] = []
        relationships: List[RelationshipSummary] = []
        dependency_graph: Dict[str, Set[str]] = defaultdict(set)

        for table in schema.tables:
            field_summaries, field_relationships, dependencies = self._process_fields(
                table, table_lookup
            )
            relationships.extend(field_relationships)
            dependency_graph[table.id].update(dependencies)

            view_summaries = [self._process_view(view) for view in table.views]
            table_summary = TableSummary(
                id=table.id,
                name=table.name,
                description=table.description,
                primary_field_id=table.primary_field_id,
                fields=field_summaries,
                views=view_summaries,
                dependencies=sorted(dependencies),
            )
            table_summaries.append(table_summary)

        creation_order = self._derive_creation_order(
            dependency_graph, table_lookup, schema.tables
        )

        return SchemaAnalysis(
            base_id=schema.id,
            base_name=schema.name,
            tables=table_summaries,
            relationships=relationships,
            suggested_table_creation_order=creation_order,
        )

    def _process_fields(
        self, table: AirtableTable, table_lookup: Dict[str, str]
    ) -> Tuple[List[FieldSummary], List[RelationshipSummary], Set[str]]:
        field_summaries: List[FieldSummary] = []
        relationships: List[RelationshipSummary] = []
        dependencies: Set[str] = set()

        for field in table.fields:
            configuration = self._normalize_configuration(field.options or {})
            linked_table_id = self._extract_linked_table_id(configuration)
            linked_table_name = table_lookup.get(linked_table_id)

            field_summary = FieldSummary(
                id=field.id,
                name=field.name,
                type=field.type,
                description=field.description,
                is_primary=field.is_primary_field or field.id == table.primary_field_id,
                configuration=configuration,
                linked_table_id=linked_table_id,
                linked_table_name=linked_table_name,
            )
            field_summaries.append(field_summary)

            if linked_table_id and linked_table_id != table.id:
                dependencies.add(linked_table_id)
                relationships.append(
                    RelationshipSummary(
                        from_table_id=table.id,
                        from_table_name=table.name,
                        from_field_id=field.id,
                        from_field_name=field.name,
                        to_table_id=linked_table_id,
                        to_table_name=linked_table_name or linked_table_id,
                        relationship_type=self._determine_relationship_type(field.type),
                        configuration=configuration,
                    )
                )

        return field_summaries, relationships, dependencies

    def _process_view(self, view: Any) -> ViewSummary:
        visible_fields = self._extract_visible_fields(view)
        return ViewSummary(
            id=view.id,
            name=view.name,
            type=view.type,
            description=view.description,
            visible_fields=visible_fields,
            filters=view.filters,
            sorts=view.sorts,
            groups=view.groups,
        )

    def _extract_visible_fields(self, view: Any) -> List[str]:
        order = getattr(view, "field_order", None)
        if isinstance(order, dict):
            field_ids = order.get("fieldIds")
            if isinstance(field_ids, list):
                return [field_id for field_id in field_ids if isinstance(field_id, str)]
        return []

    def _normalize_configuration(self, options: Dict[str, Any]) -> Dict[str, Any]:
        sanitized: Dict[str, Any] = {}
        for key, value in options.items():
            if value is None:
                continue
            if isinstance(value, list):
                sanitized[key] = value
            elif isinstance(value, dict):
                sanitized[key] = value
            else:
                sanitized[key] = value
        return sanitized

    def _extract_linked_table_id(self, configuration: Dict[str, Any]) -> Optional[str]:
        candidates = [
            configuration.get("linkedTableId"),
            configuration.get("foreignTableId"),
            configuration.get("recordLinkTableId"),
        ]
        for candidate in candidates:
            if isinstance(candidate, str):
                return candidate
        rollup = configuration.get("rollup")
        if isinstance(rollup, dict):
            linked = rollup.get("linkedTableId")
            if isinstance(linked, str):
                return linked
        lookup = configuration.get("lookup")
        if isinstance(lookup, dict):
            linked = lookup.get("linkedTableId")
            if isinstance(linked, str):
                return linked
        return None

    def _determine_relationship_type(self, field_type: str) -> str:
        relational_types = {
            "linkedRecord": "linked_record",
            "multipleRecordLinks": "linked_record",
            "rollup": "rollup",
            "lookup": "lookup",
        }
        return relational_types.get(field_type, field_type)

    def _derive_creation_order(
        self,
        graph: Dict[str, Set[str]],
        table_lookup: Dict[str, str],
        tables: Iterable[AirtableTable],
    ) -> List[str]:
        indegree: Dict[str, int] = {}
        adjacency: Dict[str, Set[str]] = defaultdict(set)

        for table in tables:
            table_id = table.id
            indegree.setdefault(table_id, 0)
            adjacency.setdefault(table_id, set())

        for table_id, dependencies in graph.items():
            for dependency in dependencies:
                if dependency not in indegree:
                    indegree[dependency] = 0
                adjacency[dependency].add(table_id)
                indegree[table_id] = indegree.get(table_id, 0) + 1

        queue: Deque[str] = deque(sorted([t for t, deg in indegree.items() if deg == 0]))
        ordered: List[str] = []

        while queue:
            current = queue.popleft()
            ordered.append(table_lookup.get(current, current))
            for neighbor in sorted(adjacency[current]):
                indegree[neighbor] -= 1
                if indegree[neighbor] == 0:
                    queue.append(neighbor)

        if len(ordered) != len(indegree):
            # Cycles detected; fall back to alphabetical order.
            self.logger.warning(
                "Cyclic dependencies detected in Airtable schema. Using fallback order."
            )
            return [table_lookup.get(table.id, table.id) for table in sorted(tables, key=lambda t: t.name)]

        return ordered
