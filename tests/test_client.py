"""Tests for the Codeforces API client.

This file exists because client.py was the one module in the collection path
with no tests, and a method was just added to it. The failure classification is
the part worth gating: an unattended run overlaps at least one contest, during
which Codeforces returns HTTP 200 with a FAILED status and a comment, and
misreading that comment is the difference between skipping one dead handle and
burning through the whole cohort. None of that is visible from the happy path.

Sleeps are injected, so the backoff paths assert in milliseconds rather than
taking the real five-minute ceiling.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

from typing import Any, cast

import pytest
import requests

from codrona_lens.codeforces.client import (
    ApiFailed,
    CodeforcesClient,
    HandleNotFound,
    RateLimited,
    Unavailable,
)
from codrona_lens.codeforces.limiter import RateLimiter


class FakeResponse:
    def __init__(
        self,
        payload: Any = None,
        *,
        status_code: int = 200,
        text: str = "",
        raises: Exception | None = None,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = text
        self._raises = raises

    def json(self) -> Any:
        if self._raises is not None:
            raise self._raises
        return self._payload


class FakeSession:
    """Records every call so params can be asserted, not inferred."""

    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any], timeout: float) -> Any:
        self.calls.append((url, dict(params)))
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


class SpyLimiter(RateLimiter):
    def __init__(self) -> None:
        super().__init__(2.0, clock=lambda: 0.0, sleep=lambda _s: None)
        self.acquired = 0

    def acquire(self) -> None:
        self.acquired += 1


def build(
    responses: list[Any],
    *,
    max_attempts: int = 3,
) -> tuple[CodeforcesClient, FakeSession, SpyLimiter, list[float]]:
    session = FakeSession(responses)
    limiter = SpyLimiter()
    slept: list[float] = []
    client = CodeforcesClient(
        "https://codeforces.com/api",
        limiter,
        max_attempts=max_attempts,
        session=cast(requests.Session, session),
        sleep=slept.append,
    )
    return client, session, limiter, slept


def ok(result: Any) -> FakeResponse:
    return FakeResponse({"status": "OK", "result": result})


EMPTY: dict[str, Any] = {"problems": [], "problemStatistics": []}
LIMIT = "Call limit exceeded"


def failed(comment: str, *, status_code: int = 200) -> FakeResponse:
    body = {"status": "FAILED", "comment": comment}
    return FakeResponse(body, status_code=status_code)


# --- the new method --------------------------------------------------------


def test_mainline_request_sends_no_problemset_name() -> None:
    """The parameter must be ABSENT, not empty: an empty value is not the default."""
    client, session, _limiter, _slept = build([ok(EMPTY)])
    client.problemset_problems()
    url, params = session.calls[0]
    assert url.endswith("/problemset.problems")
    assert params == {}


def test_named_request_sends_the_problemset_name() -> None:
    client, session, _limiter, _slept = build([ok(EMPTY)])
    client.problemset_problems(problemset_name="acmsguru")
    assert session.calls[0][1] == {"problemsetName": "acmsguru"}


def test_problemset_returns_the_result_dict() -> None:
    payload = {"problems": [{"index": "A"}], "problemStatistics": []}
    client, _session, _limiter, _slept = build([ok(payload)])
    assert client.problemset_problems() == payload


def test_problemset_rejects_a_non_dict_result() -> None:
    """user.status returns a list; a list here means the shape assumption broke."""
    client, _session, _limiter, _slept = build([ok([1, 2, 3])])
    with pytest.raises(ApiFailed, match="returned list"):
        client.problemset_problems()


def test_problemset_passes_through_the_limiter() -> None:
    client, _session, limiter, _slept = build([ok(EMPTY)])
    client.problemset_problems()
    assert limiter.acquired == 1


# --- failure classification ------------------------------------------------


def test_not_found_is_permanent_and_not_retried() -> None:
    responses = [failed("handle: User with handle x not found")]
    client, session, _limiter, slept = build(responses)
    with pytest.raises(HandleNotFound):
        client.user_status("x", from_=1, count=10)
    assert len(session.calls) == 1
    assert slept == []


def test_call_limit_is_transient_and_retried_to_success() -> None:
    client, session, _limiter, slept = build([failed(LIMIT), ok([{"id": 1}])])
    assert client.user_status("x", from_=1, count=10) == [{"id": 1}]
    assert len(session.calls) == 2
    assert slept == [5.0]


def test_call_limit_exhausted_raises_rate_limited() -> None:
    client, _session, _limiter, slept = build([failed(LIMIT)] * 3, max_attempts=3)
    with pytest.raises(RateLimited):
        client.user_status("x", from_=1, count=10)
    assert slept == [5.0, 10.0]


def test_temporarily_unavailable_exhausted_raises_unavailable() -> None:
    down = failed("Codeforces is temporarily unavailable")
    client, _session, _limiter, _slept = build([down] * 3, max_attempts=3)
    with pytest.raises(Unavailable):
        client.problemset_problems()


def test_server_error_is_transient_even_without_a_comment() -> None:
    boom = FakeResponse(None, status_code=503, text="bad gateway")
    client, session, _limiter, _slept = build([boom, ok({"problems": []})])
    assert client.problemset_problems() == {"problems": []}
    assert len(session.calls) == 2


def test_unclassifiable_failure_raises_api_failed_without_retrying() -> None:
    client, session, _limiter, _slept = build([failed("something entirely new")])
    with pytest.raises(ApiFailed, match="something entirely new"):
        client.problemset_problems()
    assert len(session.calls) == 1


def test_transport_error_retries_then_raises_unavailable() -> None:
    boom = requests.ConnectionError("reset")
    client, _session, _limiter, slept = build([boom, boom], max_attempts=2)
    with pytest.raises(Unavailable, match="transport"):
        client.problemset_problems()
    assert slept == [5.0]


def test_non_json_body_is_classified_from_its_text() -> None:
    """A challenge page is HTML, not JSON, and must not read as a clean result."""
    blocked = "<html>blocked</html>"
    html = FakeResponse(None, status_code=403, text=blocked, raises=ValueError())
    client, _session, _limiter, _slept = build([html] * 2, max_attempts=2)
    with pytest.raises(Unavailable):
        client.problemset_problems()


def test_descriptive_user_agent_is_set() -> None:
    """LEGAL.md commits us to identifying the caller; that lives here."""
    client, session, _limiter, _slept = build([ok({"problems": []})])
    assert "codrona" in session.headers["User-Agent"].lower()
    assert "github.com/codronahq" in session.headers["User-Agent"]
    client.problemset_problems()


def test_backoff_doubles_between_attempts() -> None:
    client, _session, _limiter, slept = build([failed(LIMIT)] * 5, max_attempts=5)
    with pytest.raises(RateLimited):
        client.problemset_problems()
    assert slept == [5.0, 10.0, 20.0, 40.0]
