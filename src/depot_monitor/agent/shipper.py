"""Send events to the API over HTTPS. Never raises on network trouble: it reports failure."""

from __future__ import annotations

import logging
from collections.abc import Sequence

import httpx

log = logging.getLogger(__name__)


class Shipper:
    def __init__(
        self,
        api_url: str,
        api_key: str,
        *,
        client: httpx.Client | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._base = api_url.rstrip("/")
        self._headers = {"X-API-Key": api_key}
        self._client = client or httpx.Client(timeout=timeout)

    def _post(self, path: str, payload: dict) -> bool:
        try:
            response = self._client.post(self._base + path, json=payload, headers=self._headers)
        except httpx.HTTPError as exc:
            log.warning("API unreachable (%s): %s", type(exc).__name__, exc)
            return False
        if response.is_success:
            return True
        log.warning("API refused %s: HTTP %s %s", path, response.status_code, response.text[:200])
        return False

    def send(self, events: Sequence[dict]) -> bool:
        """True only when the API confirmed it stored the whole batch."""
        return self._post("/api/v1/events", {"events": list(events)})

    def heartbeat(self, source: str, pending: int, version: str) -> bool:
        return self._post(
            "/api/v1/heartbeat", {"source": source, "pending": pending, "version": version}
        )

    def close(self) -> None:
        self._client.close()
