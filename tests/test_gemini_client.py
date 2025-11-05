"""Tests for Gemini integration."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from airtable_analyzer.exceptions import GeminiClientError
from airtable_analyzer.gemini_client import GeminiClient
from airtable_analyzer.models import SchemaAnalysis


def test_generate_duplication_guide_success(
    monkeypatch: pytest.MonkeyPatch,
    sample_analysis: SchemaAnalysis,
    sample_duplication_guide,
) -> None:
    monkeypatch.setattr("google.generativeai.configure", lambda **_: None)
    mock_model = Mock()
    mock_model.generate_content.return_value = SimpleNamespace(
        text=json.dumps(sample_duplication_guide.model_dump())
    )
    monkeypatch.setattr("google.generativeai.GenerativeModel", lambda **_: mock_model)

    client = GeminiClient(api_key="token", model_name="gemini-2.5-pro")
    guide = client.generate_duplication_guide(sample_analysis)

    assert guide.base_overview == sample_duplication_guide.base_overview
    assert guide.duplication_steps[0].order == 1


def test_generate_duplication_guide_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
    sample_analysis: SchemaAnalysis,
) -> None:
    monkeypatch.setattr("google.generativeai.configure", lambda **_: None)
    mock_model = Mock()
    mock_model.generate_content.return_value = SimpleNamespace(text="not json")
    monkeypatch.setattr("google.generativeai.GenerativeModel", lambda **_: mock_model)

    client = GeminiClient(api_key="token", model_name="gemini-2.5-pro")

    with pytest.raises(GeminiClientError):
        client.generate_duplication_guide(sample_analysis)
