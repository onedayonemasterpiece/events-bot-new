from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Any


class AvailabilityStatus(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"


class MonitorRunStatus(StrEnum):
    PASS = "PASS"
    WARN_NO_LIVE_SUPPLY = "WARN_NO_LIVE_SUPPLY"
    PARTIAL = "PARTIAL"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def canonical_json_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(slots=True, frozen=True)
class VolunteerOpportunity:
    source_type: str
    source_external_id: str
    canonical_url: str
    title: str
    organizer_name: str | None
    region: str | None
    city: str | None
    venue: str | None
    location_text: str | None
    application_open_at: date | None
    application_close_at: date | None
    event_start_at: date | None
    event_end_at: date | None
    roles: tuple[str, ...]
    external_links: tuple[str, ...]
    source_excerpt: str
    availability_status: AvailabilityStatus
    availability_reason: str
    checked_at: datetime
    semantic_hash: str
    availability_hash: str

    def to_dict(self) -> dict[str, Any]:
        raw = asdict(self)
        for key in (
            "application_open_at",
            "application_close_at",
            "event_start_at",
            "event_end_at",
        ):
            raw[key] = raw[key].isoformat() if raw[key] else None
        raw["checked_at"] = self.checked_at.isoformat()
        raw["availability_status"] = self.availability_status.value
        raw["roles"] = list(self.roles)
        raw["external_links"] = list(self.external_links)
        return raw


@dataclass(slots=True, frozen=True)
class MonitorResult:
    mode: str
    generated_at: datetime
    opportunities: tuple[VolunteerOpportunity, ...]
    source_pages_seen: int
    run_status: MonitorRunStatus = MonitorRunStatus.PASS
    warnings: tuple[str, ...] = ()
    outside_region_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        rows = [item.to_dict() for item in self.opportunities]
        status_counts = {status.value: 0 for status in AvailabilityStatus}
        for item in self.opportunities:
            status_counts[item.availability_status.value] += 1
        body: dict[str, Any] = {
            "schema_version": "volunteer-monitor-result-v1",
            "mode": self.mode,
            "run_status": self.run_status.value,
            "generated_at": self.generated_at.isoformat(),
            "source_pages_seen": self.source_pages_seen,
            "outside_region_count": self.outside_region_count,
            "opportunity_count": len(rows),
            "status_counts": status_counts,
            "warnings": list(self.warnings),
            "opportunities": rows,
        }
        body["result_sha256"] = canonical_json_hash(body)
        return body
