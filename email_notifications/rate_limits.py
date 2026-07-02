from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EmailRateLimitConfig:
    max_per_hour: int = 100
    max_per_day: int = 1000
    max_per_recipient_per_day: int = 6
    max_per_recipient_event_per_day: int = 2
    cancel_batch_per_minute: int = 30


@dataclass(frozen=True)
class RateLimitCounters:
    sender_hour: int = 0
    sender_day: int = 0
    recipient_day: int = 0
    recipient_event_day: int = 0
    cancel_minute: int = 0


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    reason: str | None = None
    defer_seconds: int = 0


class EmailRateLimiter:
    def __init__(self, config: EmailRateLimitConfig | None = None):
        self.config = config or EmailRateLimitConfig()

    def check(self, *, kind: str, counters: RateLimitCounters) -> RateLimitDecision:
        if counters.sender_hour >= self.config.max_per_hour:
            return RateLimitDecision(False, "sender_hour_limit", 3600)
        if counters.sender_day >= self.config.max_per_day:
            return RateLimitDecision(False, "sender_day_limit", 86400)
        if counters.recipient_day >= self.config.max_per_recipient_per_day:
            return RateLimitDecision(False, "recipient_day_limit", 86400)
        if counters.recipient_event_day >= self.config.max_per_recipient_event_per_day:
            return RateLimitDecision(False, "recipient_event_day_limit", 86400)
        if kind == "event_cancelled" and counters.cancel_minute >= self.config.cancel_batch_per_minute:
            return RateLimitDecision(False, "cancel_batch_minute_limit", 60)
        return RateLimitDecision(True)
