"""Codeforces API client.

Failure classification matters more than the happy path here. An unattended run
of twelve hours will overlap at least one contest, during which the API degrades
and returns transient failures; treating those as user-level errors would burn
through the cohort silently.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import requests

from .limiter import RateLimiter

INITIAL_BACKOFF = 5.0
MAX_BACKOFF = 300.0


class CodeforcesError(Exception):
    """Base for every failure this client raises."""


class RateLimited(CodeforcesError):
    """Call limit exceeded and retries were exhausted."""


class Unavailable(CodeforcesError):
    """Transport failure, or Codeforces reporting itself temporarily down."""


class HandleNotFound(CodeforcesError):
    """The handle no longer exists - renamed or deleted. Permanent, not retried."""


class ApiFailed(CodeforcesError):
    """A FAILED response we do not know how to classify."""


class CodeforcesClient:
    def __init__(
        self,
        base_url: str,
        limiter: RateLimiter,
        *,
        timeout: float = 60.0,
        max_attempts: int = 10,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._limiter = limiter
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._session = session or requests.Session()
        self._sleep = sleep
        self._session.headers.setdefault(
            "User-Agent",
            "codrona-lens/0.1 (+https://github.com/codronahq/lens)",
        )

    def _get(self, method: str, params: dict[str, Any]) -> Any:
        backoff = INITIAL_BACKOFF
        last_detail = ""

        for attempt in range(1, self._max_attempts + 1):
            self._limiter.acquire()
            try:
                response = self._session.get(
                    f"{self._base_url}/{method}",
                    params=params,
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                last_detail = f"transport: {exc}"
                if attempt >= self._max_attempts:
                    raise Unavailable(last_detail) from exc
                self._sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            payload: Any = None
            try:
                payload = response.json()
            except ValueError:
                payload = None

            if isinstance(payload, dict):
                status = payload.get("status")
                comment = str(payload.get("comment", ""))
            else:
                status = None
                comment = response.text[:200]

            if status == "OK":
                return payload["result"]

            lowered = comment.lower()
            last_detail = comment or f"HTTP {response.status_code}"

            if "not found" in lowered:
                raise HandleNotFound(last_detail)

            transient = (
                "call limit exceeded" in lowered
                or "temporarily unavailable" in lowered
                or response.status_code >= 500
                or response.status_code == 403
            )
            if transient:
                if attempt >= self._max_attempts:
                    if "call limit exceeded" in lowered:
                        raise RateLimited(last_detail)
                    raise Unavailable(last_detail)
                self._sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            raise ApiFailed(last_detail)

        raise Unavailable(last_detail or "retries exhausted")

    def user_rated_list(
        self,
        *,
        active_only: bool = True,
        include_retired: bool = False,
    ) -> list[dict[str, Any]]:
        result = self._get(
            "user.ratedList",
            {
                "activeOnly": str(active_only).lower(),
                "includeRetired": str(include_retired).lower(),
            },
        )
        return list(result)

    def user_status(self, handle: str, *, from_: int, count: int) -> list[dict[str, Any]]:
        """One page of a user's submissions, newest first (descending id)."""
        result = self._get(
            "user.status",
            {"handle": handle, "from": from_, "count": count},
        )
        return list(result)
