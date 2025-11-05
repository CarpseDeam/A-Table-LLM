"""Wrapper around the Airtable Metadata API."""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional

import requests
from requests import Response, Session

from .exceptions import (
    AirtableAuthenticationError,
    AirtableClientError,
    AirtableNotFoundError,
    AirtableRateLimitError,
)
from .models import AirtableBaseSchema, AirtableTable, AirtableView

LOGGER = logging.getLogger(__name__)


class RateLimiter:
    """Simple thread-safe rate limiter implementing a leaky bucket algorithm."""

    def __init__(self, max_calls: int, period_seconds: float) -> None:
        """Initialize the rate limiter.

        Args:
            max_calls: Maximum number of requests permitted in the period.
            period_seconds: Time window for rate limiting in seconds.
        """
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self._timestamps: Deque[float] = deque()

    def acquire(self) -> None:
        """Block until another request is permitted."""
        while True:
            now = time.monotonic()
            while self._timestamps and now - self._timestamps[0] > self.period_seconds:
                self._timestamps.popleft()

            if len(self._timestamps) < self.max_calls:
                self._timestamps.append(now)
                return

            sleep_duration = self.period_seconds - (now - self._timestamps[0])
            if sleep_duration <= 0:
                # The loop will clean up on the next iteration.
                continue

            time.sleep(sleep_duration)


class AirtableClient:
    """Client responsible for retrieving Airtable metadata."""

    API_ROOT = "https://api.airtable.com/v0"

    def __init__(
        self,
        access_token: str,
        timeout_seconds: int,
        max_retries: int,
        initial_backoff_seconds: float,
        session: Optional[Session] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the client with authentication and retry configuration."""
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
        )
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.initial_backoff_seconds = initial_backoff_seconds
        self.rate_limiter = RateLimiter(max_calls=5, period_seconds=1.0)
        self.logger = logger or LOGGER

    def fetch_base_schema(self, base_id: str) -> AirtableBaseSchema:
        """Retrieve the full base schema including tables and views.

        Args:
            base_id: Airtable base identifier.

        Returns:
            Validated Airtable base schema model.

        Raises:
            AirtableClientError: For unrecoverable Airtable API failures.
        """
        base_info = self._fetch_base_information(base_id)
        raw_tables = self._fetch_tables(base_id)
        tables: List[AirtableTable] = []

        for raw_table in raw_tables:
            table_id = raw_table["id"]
            try:
                raw_views = self._fetch_views(base_id, table_id)
            except AirtableNotFoundError:
                raw_views = []

            table = AirtableTable.model_validate(
                {
                    **raw_table,
                    "views": [AirtableView.model_validate(view) for view in raw_views],
                }
            )
            tables.append(table)

        base_name = base_info.get("name") or base_info.get("id") or base_id

        return AirtableBaseSchema.model_validate(
            {"id": base_info.get("id", base_id), "name": base_name, "tables": tables}
        )

    def _fetch_base_information(self, base_id: str) -> Dict[str, Any]:
        response = self._request("GET", f"/meta/bases/{base_id}")
        body = self._parse_json(response)
        if "base" in body:
            return body["base"]
        return body

    def _fetch_tables(self, base_id: str) -> List[Dict[str, Any]]:
        return self._fetch_paginated(
            f"/meta/bases/{base_id}/tables", collection_key="tables"
        )

    def _fetch_views(self, base_id: str, table_id: str) -> List[Dict[str, Any]]:
        return self._fetch_paginated(
            f"/meta/bases/{base_id}/tables/{table_id}/views", collection_key="views"
        )

    def _fetch_paginated(
        self, path: str, collection_key: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        offset: Optional[str] = None

        while True:
            query = dict(params or {})
            if offset:
                query["offset"] = offset

            response = self._request("GET", path, params=query)
            payload = self._parse_json(response)
            batch = payload.get(collection_key, [])
            if not isinstance(batch, list):
                raise AirtableClientError(
                    f"Unexpected response format: '{collection_key}' is not a list."
                )
            items.extend(batch)
            offset = payload.get("offset")
            if not offset:
                break

        return items

    def _request(
        self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None
    ) -> Response:
        url = f"{self.API_ROOT}{path}"
        attempt = 0

        while True:
            self.rate_limiter.acquire()
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    timeout=self.timeout_seconds,
                )
            except requests.Timeout as exc:
                self._log_debug("Request timeout encountered.", attempt, exc)
                if attempt >= self.max_retries:
                    raise AirtableClientError("Airtable API request timed out.") from exc
                self._sleep_backoff(attempt)
                attempt += 1
                continue

            if response.status_code == 429:
                self._log_debug("Rate limit response received.", attempt, None)
                if attempt >= self.max_retries:
                    raise AirtableRateLimitError(
                        "Exceeded Airtable rate limit despite retries."
                    )
                self._sleep_backoff(attempt)
                attempt += 1
                continue

            if response.status_code in {500, 502, 503, 504}:
                self._log_debug("Server error received.", attempt, None)
                if attempt >= self.max_retries:
                    raise AirtableClientError(
                        f"Airtable server error ({response.status_code})."
                    )
                self._sleep_backoff(attempt)
                attempt += 1
                continue

            if response.status_code in {401, 403}:
                raise AirtableAuthenticationError(
                    "Airtable authentication failed. Verify access token and scopes."
                )

            if response.status_code == 404:
                raise AirtableNotFoundError(
                    "Airtable resource not found. Verify the base identifier."
                )

            if response.status_code >= 400:
                raise AirtableClientError(
                    f"Airtable API error ({response.status_code}): {response.text}"
                )

            return response

    def _parse_json(self, response: Response) -> Dict[str, Any]:
        try:
            return response.json()
        except ValueError as exc:
            raise AirtableClientError("Failed to parse Airtable response as JSON.") from exc

    def _sleep_backoff(self, attempt: int) -> None:
        backoff = self.initial_backoff_seconds * (2 ** attempt)
        time.sleep(backoff)

    def _log_debug(
        self, message: str, attempt: int, exception: Optional[Exception]
    ) -> None:
        if exception:
            self.logger.debug("%s Attempt %s. Error: %s", message, attempt + 1, exception)
        else:
            self.logger.debug("%s Attempt %s.", message, attempt + 1)
