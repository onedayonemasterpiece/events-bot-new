"""Transactional event email notification foundation."""

from .core import (
    CANCELLATION_DISCLAIMER,
    EmailDeliveryEvent,
    EmailOutboxItem,
    EventFollower,
    EventSnapshot,
    build_follow_outbox,
    build_lifecycle_outbox,
    detect_event_changes,
    email_hash,
    render_email,
)
from .postbox import PostboxConfig, PostboxSmtpSender
from .rate_limits import EmailRateLimitConfig, EmailRateLimiter, RateLimitCounters
from .ydb_stats import EmailStatsEvent, RecordingYDBStatsSink, YDBStatsConfig, YDBStatsSink

__all__ = [
    "CANCELLATION_DISCLAIMER",
    "EmailDeliveryEvent",
    "EmailOutboxItem",
    "EventFollower",
    "EventSnapshot",
    "build_follow_outbox",
    "build_lifecycle_outbox",
    "detect_event_changes",
    "email_hash",
    "render_email",
    "PostboxConfig",
    "PostboxSmtpSender",
    "EmailRateLimitConfig",
    "EmailRateLimiter",
    "RateLimitCounters",
    "EmailStatsEvent",
    "RecordingYDBStatsSink",
    "YDBStatsConfig",
    "YDBStatsSink",
]
