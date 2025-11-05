"""Tests for the Airtable metadata client."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import pytest

from airtable_analyzer.airtable_client import AirtableClient
from airtable_analyzer.exceptions import (
    AirtableAuthenticationError,
    AirtableRateLimitError,
)


class FakeResponse:
    """Simple stand-in for requests.Response."""

    def __init__(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self) -> Dict[str, Any]:
        return self._payload


class FakeSession:
    """Session that yields predefined responses."""

    def __init__(self, responses: List[FakeResponse]) -> None:
        self.responses = responses
        self.headers: Dict[str, str] = {}
        self.requests: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []

    def request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> FakeResponse:
        self.requests.append((method, url, params))
        if not self.responses:
            raise AssertionError("No more fake responses configured.")
        return self.responses.pop(0)


def _build_happy_path_session() -> FakeSession:
    base_response = FakeResponse({"base": {"id": "app123", "name": "Demo Base"}})
    tables_response = FakeResponse(
        {
            "tables": [
                {
                    "id": "tblProjects",
                    "name": "Projects",
                    "primaryFieldId": "fldProjectName",
                    "fields": [
                        {
                            "id": "fldProjectName",
                            "name": "Project Name",
                            "type": "singleLineText",
                            "isPrimaryField": True,
                        },
                        {
                            "id": "fldProjectLink",
                            "name": "Project Link",
                            "type": "multipleRecordLinks",
                            "options": {"linkedTableId": "tblTasks"},
                        },
                    ],
                },
                {
                    "id": "tblTasks",
                    "name": "Tasks",
                    "primaryFieldId": "fldTaskName",
                    "fields": [
                        {
                            "id": "fldTaskName",
                            "name": "Task Name",
                            "type": "singleLineText",
                            "isPrimaryField": True,
                        }
                    ],
                },
            ]
        }
    )
    project_views = FakeResponse({"views": []})
    task_views = FakeResponse({"views": []})
    return FakeSession([base_response, tables_response, project_views, task_views])


def test_fetch_base_schema_success(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _build_happy_path_session()
    client = AirtableClient(
        access_token="token",
        timeout_seconds=5,
        max_retries=2,
        initial_backoff_seconds=0.01,
        session=session,
    )
    client.rate_limiter.acquire = lambda: None  # type: ignore[assignment]
    monkeypatch.setattr("time.sleep", lambda *_: None)

    schema = client.fetch_base_schema("app123")

    assert schema.id == "app123"
    assert schema.name == "Demo Base"
    assert len(schema.tables) == 2
    assert schema.tables[0].fields[1].options == {"linkedTableId": "tblTasks"}


def test_fetch_base_schema_auth_failure() -> None:
    session = FakeSession([FakeResponse({}, status_code=401)])
    client = AirtableClient(
        access_token="token",
        timeout_seconds=5,
        max_retries=1,
        initial_backoff_seconds=0.01,
        session=session,
    )
    client.rate_limiter.acquire = lambda: None  # type: ignore[assignment]

    with pytest.raises(AirtableAuthenticationError):
        client.fetch_base_schema("app123")


def test_fetch_base_schema_rate_limit_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    session = FakeSession([FakeResponse({}, status_code=429), FakeResponse({}, status_code=429)])
    client = AirtableClient(
        access_token="token",
        timeout_seconds=5,
        max_retries=1,
        initial_backoff_seconds=0.0,
        session=session,
    )
    client.rate_limiter.acquire = lambda: None  # type: ignore[assignment]
    monkeypatch.setattr("time.sleep", lambda *_: None)

    with pytest.raises(AirtableRateLimitError):
        client.fetch_base_schema("app123")
