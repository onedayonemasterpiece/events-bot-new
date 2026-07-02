from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class EmailStatsEvent:
    event_type: str
    kind: str | None = None
    status: str | None = None
    event_id: int | None = None
    user_id: str | None = None
    recipient_email_hash: str | None = None
    provider_message_id: str | None = None
    dry_run: bool = True
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "kind": self.kind,
            "status": self.status,
            "event_id": self.event_id,
            "user_id": self.user_id,
            "recipient_email_hash": self.recipient_email_hash,
            "provider_message_id": self.provider_message_id,
            "dry_run": self.dry_run,
            "reason": self.reason,
            "metadata": self.metadata,
            "occurred_at": self.occurred_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        }


@dataclass(frozen=True)
class YDBStatsConfig:
    endpoint: str
    database: str
    table_path: str
    service_account_key_file: str | None = None
    enabled: bool = False

    @classmethod
    def from_env(cls) -> "YDBStatsConfig":
        prefix = (os.getenv("EMAIL_YDB_TABLE_PREFIX") or "/email_notifications").rstrip("/")
        return cls(
            endpoint=os.getenv("EMAIL_YDB_ENDPOINT") or "",
            database=os.getenv("EMAIL_YDB_DATABASE") or "",
            table_path=os.getenv("EMAIL_YDB_STATS_TABLE") or f"{prefix}/delivery_events",
            service_account_key_file=os.getenv("EMAIL_YDB_SERVICE_ACCOUNT_KEY_FILE") or None,
            enabled=(os.getenv("EMAIL_YDB_STATS_ENABLED") or "0").strip().lower() in {"1", "true", "yes"},
        )


class RecordingYDBStatsSink:
    def __init__(self) -> None:
        self.events: list[EmailStatsEvent] = []

    def record(self, event: EmailStatsEvent) -> None:
        self.events.append(event)


class YDBStatsSink:
    """YDB stats adapter contract; fails visibly until real YDB is provisioned."""

    def __init__(self, config: YDBStatsConfig | None = None) -> None:
        self.config = config or YDBStatsConfig.from_env()

    def record(self, event: EmailStatsEvent) -> None:
        if not self.config.enabled:
            raise RuntimeError("YDB stats sink is disabled; keep Postbox in dry-run until EMAIL_YDB_STATS_ENABLED=1")
        if not self.config.endpoint or not self.config.database or not self.config.table_path:
            raise RuntimeError("YDB stats sink is missing endpoint/database/table config")
        try:
            import ydb  # type: ignore  # noqa: F401
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("official ydb Python SDK is required for EMAIL_YDB_STATS_ENABLED=1") from exc
        payload = json.dumps(event.as_dict(), ensure_ascii=False, sort_keys=True)
        if not payload:
            raise RuntimeError("empty YDB stats payload")
        raise NotImplementedError("YDB UPSERT wiring is gated until YDB credentials/table are provisioned")
