"""Integration with Google's Gemini models."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

import google.generativeai as genai
from google.generativeai import types as genai_types

from .exceptions import GeminiClientError
from .models import DuplicationGuide, SchemaAnalysis

LOGGER = logging.getLogger(__name__)


class GeminiClient:
    """Client responsible for interacting with Gemini 2.5."""

    def __init__(
        self,
        api_key: str,
        model_name: str,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the Gemini client.

        Args:
            api_key: Gemini API key.
            model_name: Gemini model name to use (e.g., gemini-2.5-pro).
            logger: Optional logger for diagnostics.
        """
        genai.configure(api_key=api_key)
        self.logger = logger or LOGGER
        generation_config = genai_types.GenerationConfig(
            temperature=0.25,
            top_p=0.9,
            top_k=40,
            candidate_count=1,
            response_mime_type="application/json",
        )
        self.model = genai.GenerativeModel(
            model_name=model_name,
            generation_config=generation_config,
        )

    def generate_duplication_guide(self, analysis: SchemaAnalysis) -> DuplicationGuide:
        """Generate a duplication guide using Gemini.

        Args:
            analysis: Preprocessed schema analysis.

        Returns:
            Structured duplication guide.

        Raises:
            GeminiClientError: When Gemini returns an invalid or malformed response.
        """
        payload = self._build_prompt_payload(analysis)
        prompt = self._format_prompt(payload)

        try:
            response = self.model.generate_content([prompt])
        except Exception as exc:  # noqa: BLE001
            raise GeminiClientError("Gemini API request failed.") from exc

        response_text = self._extract_text(response)
        parsed = self._parse_json_response(response_text)
        return DuplicationGuide.model_validate(parsed)

    def _build_prompt_payload(self, analysis: SchemaAnalysis) -> Dict[str, Any]:
        return {
            "base": {
                "id": analysis.base_id,
                "name": analysis.base_name,
                "suggested_table_creation_order": analysis.suggested_table_creation_order,
                "tables": [
                    {
                        "id": table.id,
                        "name": table.name,
                        "description": table.description,
                        "primary_field_id": table.primary_field_id,
                        "dependencies": table.dependencies,
                        "fields": [
                            {
                                "id": field.id,
                                "name": field.name,
                                "type": field.type,
                                "is_primary": field.is_primary,
                                "description": field.description,
                                "configuration": field.configuration,
                                "linked_table_id": field.linked_table_id,
                                "linked_table_name": field.linked_table_name,
                            }
                            for field in table.fields
                        ],
                        "views": [
                            {
                                "id": view.id,
                                "name": view.name,
                                "type": view.type,
                                "description": view.description,
                                "visible_fields": view.visible_fields,
                                "filters": view.filters,
                                "sorts": view.sorts,
                                "groups": view.groups,
                            }
                            for view in table.views
                        ],
                    }
                    for table in analysis.tables
                ],
                "relationships": [
                    {
                        "from_table": rel.from_table_name,
                        "from_field": rel.from_field_name,
                        "to_table": rel.to_table_name,
                        "relationship_type": rel.relationship_type,
                        "configuration": rel.configuration,
                    }
                    for rel in analysis.relationships
                ],
            }
        }

    def _format_prompt(self, payload: Dict[str, Any]) -> str:
        guide_schema = {
            "base_overview": "string",
            "key_considerations": ["string"],
            "table_details": [
                {
                    "table_name": "string",
                    "summary": "string",
                    "field_instructions": ["string"],
                    "view_instructions": ["string"],
                    "sequencing_notes": ["string"],
                }
            ],
            "relationships": ["string"],
            "duplication_steps": [
                {
                    "order": "integer",
                    "title": "string",
                    "description": "string",
                    "prerequisites": ["string"],
                }
            ],
            "post_duplication_checks": ["string"],
        }
        prompt = (
            "You are an Airtable expert helping engineers recreate complex bases.\n"
            "Analyze the provided base schema and produce a structured JSON object "
            "that strictly matches the following schema:\n"
            f"{json.dumps(guide_schema, indent=2)}\n\n"
            "Guidance:\n"
            "- Provide a concise overview emphasizing critical configuration areas.\n"
            "- Include detailed table instructions covering fields, formulas, lookups, "
            "rollups, select options, and formatting requirements.\n"
            "- Outline relationships and dependencies so tables can be created in the "
            "correct order.\n"
            "- Supply a sequential duplication plan using the base's dependencies.\n"
            "- End with validation steps to confirm parity with the original base.\n\n"
            "Schema payload:\n"
            f"{json.dumps(payload, indent=2)}\n\n"
            "Output only the JSON object."
        )
        return prompt

    def _extract_text(self, response: Any) -> str:
        content = getattr(response, "text", None)
        if isinstance(content, str):
            return content

        candidates: list[str] = []
        for candidate in getattr(response, "candidates", []):
            content_obj = getattr(candidate, "content", None)
            parts = getattr(content_obj, "parts", []) if content_obj else []
            for part in parts:
                text = getattr(part, "text", None)
                if isinstance(text, str):
                    candidates.append(text)
        if candidates:
            return "\n".join(candidates)

        raise GeminiClientError("Gemini response did not contain text content.")

    def _parse_json_response(self, response_text: str) -> Dict[str, Any]:
        try:
            return json.loads(response_text)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            self.logger.error("Gemini returned invalid JSON: %s", response_text)
            raise GeminiClientError("Gemini response was not valid JSON.") from exc
