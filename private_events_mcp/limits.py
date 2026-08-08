from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock


class RateLimitExceeded(RuntimeError):
    pass


class SlidingWindowLimiter:
    def __init__(self) -> None:
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def allow(self, key: str, *, limit: int, window_seconds: float) -> bool:
        now = time.monotonic()
        cutoff = now - window_seconds
        with self._lock:
            queue = self._events[key]
            while queue and queue[0] <= cutoff:
                queue.popleft()
            if len(queue) >= limit:
                return False
            queue.append(now)
            if len(self._events) > 2048:
                stale = [k for k, q in self._events.items() if not q or q[-1] <= cutoff]
                for item in stale[:512]:
                    self._events.pop(item, None)
            return True


@dataclass(slots=True)
class _BudgetWindow:
    started_at: float
    used: int


class HourlyByteBudget:
    def __init__(self, limit: int) -> None:
        self.limit = max(1, int(limit))
        self._window = _BudgetWindow(time.monotonic(), 0)
        self._lock = Lock()

    def reserve(self, amount: int) -> bool:
        amount = max(0, int(amount))
        now = time.monotonic()
        with self._lock:
            if now - self._window.started_at >= 3600:
                self._window = _BudgetWindow(now, 0)
            if self._window.used + amount > self.limit:
                return False
            self._window.used += amount
            return True


class AdmissionController:
    def __init__(self, *, concurrency: int, egress_limit: int) -> None:
        self.semaphore = asyncio.Semaphore(max(1, concurrency))
        self.rate = SlidingWindowLimiter()
        self.egress = HourlyByteBudget(egress_limit)

    async def __aenter__(self) -> "AdmissionController":
        try:
            await asyncio.wait_for(self.semaphore.acquire(), timeout=0.25)
        except asyncio.TimeoutError as exc:
            raise RateLimitExceeded("server_busy") from exc
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        self.semaphore.release()
        return False
