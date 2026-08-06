"""The single global chokepoint through which every Codeforces request passes.

Clock and sleep are injectable so the interval guarantee can be asserted in a
test without the test taking as long as the interval.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class RateLimiter:
    """Enforces a minimum interval between successive acquire() calls."""

    def __init__(
        self,
        min_interval: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if min_interval <= 0:
            raise ValueError("min_interval must be positive")
        self._min_interval = min_interval
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._last: float | None = None

    @property
    def min_interval(self) -> float:
        return self._min_interval

    def acquire(self) -> None:
        with self._lock:
            if self._last is not None:
                wait = self._min_interval - (self._clock() - self._last)
                if wait > 0:
                    self._sleep(wait)
            self._last = self._clock()
