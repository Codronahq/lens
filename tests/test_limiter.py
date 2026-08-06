"""The rate limit is an operational commitment, so it gets a test rather than a comment."""

from __future__ import annotations

import itertools

from codrona_lens.codeforces.limiter import RateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.now: float = 0.0
        self.waits: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


def test_first_acquire_does_not_sleep() -> None:
    clock = FakeClock()
    limiter = RateLimiter(2.1, clock=clock.time, sleep=clock.sleep)
    limiter.acquire()
    assert clock.waits == []


def test_no_two_requests_closer_than_the_interval() -> None:
    clock = FakeClock()
    limiter = RateLimiter(2.1, clock=clock.time, sleep=clock.sleep)

    stamps: list[float] = []
    for _ in range(10):
        limiter.acquire()
        stamps.append(clock.now)
        clock.now += 0.05  # the request itself takes some time

    gaps = [b - a for a, b in itertools.pairwise(stamps)]
    assert all(gap >= 2.1 - 1e-9 for gap in gaps), gaps


def test_slow_request_consumes_the_interval() -> None:
    clock = FakeClock()
    limiter = RateLimiter(2.1, clock=clock.time, sleep=clock.sleep)
    limiter.acquire()
    clock.now += 5.0
    limiter.acquire()
    assert clock.waits == []


def test_rejects_non_positive_interval() -> None:
    try:
        RateLimiter(0.0)
    except ValueError:
        return
    raise AssertionError("expected ValueError")
