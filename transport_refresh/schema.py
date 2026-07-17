from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

SCHEMA_VERSION = "kenigevents.transport_provider.v1"
COMBINED_SCHEMA_VERSION = "kenigevents.transport_combined.v1"
POINTER_SCHEMA_VERSION = "kenigevents.transport_pointer.v1"
PROVIDERS = ("kppk", "bus")
TIMEZONE = "Europe/Kaliningrad"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ManifestValidationError(ValueError):
    def __init__(self, reasons: list[str]):
        self.reasons = tuple(dict.fromkeys(reasons))
        super().__init__("; ".join(self.reasons))


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def parse_datetime(value: Any, field: str, reasons: list[str]) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        reasons.append(f"{field}:invalid_datetime")
        return None
    if parsed.tzinfo is None:
        reasons.append(f"{field}:timezone_required")
        return None
    return parsed


def parse_date(value: Any, field: str, reasons: list[str]) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        reasons.append(f"{field}:invalid_date")
        return None


def _named_coordinates(value: Any, field: str, reasons: list[str]) -> None:
    if not isinstance(value, dict):
        reasons.append(f"{field}:object_required")
        return
    if not str(value.get("name") or "").strip():
        reasons.append(f"{field}.name:required")
    try:
        lat, lon = float(value.get("lat")), float(value.get("lon"))
        if not (math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError
    except (TypeError, ValueError):
        reasons.append(f"{field}:valid_coordinates_required")


def semantic_provider_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    """Fields that change rendered transport guidance.

    Retrieval timestamps and snapshot ids deliberately do not participate. A
    successful nightly re-check of byte-for-byte equivalent service must not
    rebuild the whole static site.
    """

    return {
        "schema_version": manifest.get("schema_version"),
        "provider": manifest.get("provider"),
        "timezone": manifest.get("timezone"),
        "validity": manifest.get("validity"),
        "source": {"url": (manifest.get("source") or {}).get("url")},
        "services": sorted(manifest.get("services") or [], key=lambda item: str(item.get("service_id") or "")),
    }


def validate_provider_manifest(
    manifest: dict[str, Any],
    *,
    expected_provider: str | None = None,
    now: datetime | None = None,
    max_age_hours: int = 72,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(manifest, dict):
        raise ManifestValidationError(["manifest:object_required"])
    provider = str(manifest.get("provider") or "").strip()
    if manifest.get("schema_version") != SCHEMA_VERSION:
        reasons.append("schema_version:unsupported")
    if provider not in PROVIDERS:
        reasons.append("provider:unsupported")
    if expected_provider and provider != expected_provider:
        reasons.append("provider:mismatch")
    if manifest.get("timezone") != TIMEZONE:
        reasons.append("timezone:must_be_Europe_Kaliningrad")
    if not str(manifest.get("snapshot_id") or "").strip():
        reasons.append("snapshot_id:required")

    fetched_at = parse_datetime(manifest.get("fetched_at"), "fetched_at", reasons)
    source_fetched_at = None
    source = manifest.get("source")
    if not isinstance(source, dict):
        reasons.append("source:object_required")
    else:
        source_url = str(source.get("url") or "").strip()
        parsed_url = urlparse(source_url)
        if parsed_url.scheme != "https" or not parsed_url.netloc:
            reasons.append("source.url:https_required")
        source_fetched_at = parse_datetime(source.get("fetched_at"), "source.fetched_at", reasons)
        if not SHA256_RE.fullmatch(str(source.get("document_sha256") or "")):
            reasons.append("source.document_sha256:invalid")

    validity = manifest.get("validity")
    valid_from = valid_until = None
    if not isinstance(validity, dict):
        reasons.append("validity:object_required")
    else:
        valid_from = parse_date(validity.get("valid_from"), "validity.valid_from", reasons)
        valid_until = parse_date(validity.get("valid_until"), "validity.valid_until", reasons)
        if valid_from and valid_until and valid_from > valid_until:
            reasons.append("validity:inverted_range")
        if validity.get("status") != "valid":
            reasons.append("validity.status:not_valid")

    services = manifest.get("services")
    if not isinstance(services, list) or not services:
        reasons.append("services:empty")
        services = []
    seen_ids: set[str] = set()
    tz = ZoneInfo(TIMEZONE)
    for index, service in enumerate(services):
        prefix = f"services[{index}]"
        if not isinstance(service, dict):
            reasons.append(f"{prefix}:object_required")
            continue
        service_id = str(service.get("service_id") or "").strip()
        if not service_id:
            reasons.append(f"{prefix}.service_id:required")
        elif service_id in seen_ids:
            reasons.append(f"{prefix}.service_id:duplicate")
        seen_ids.add(service_id)
        expected_mode = "rail" if provider == "kppk" else "bus"
        if service.get("mode") != expected_mode:
            reasons.append(f"{prefix}.mode:provider_mismatch")
        if not str(service.get("trip_id") or "").strip():
            reasons.append(f"{prefix}.trip_id:required")
        route = service.get("route")
        if not isinstance(route, dict) or not str(route.get("id") or "").strip() or not str(route.get("name") or "").strip():
            reasons.append(f"{prefix}.route:id_and_name_required")
        stops = service.get("stops")
        if not isinstance(stops, list) or len(stops) < 2:
            reasons.append(f"{prefix}.stops:at_least_two_required")
            stops = []
        stop_ids: set[str] = set()
        for stop_index, stop in enumerate(stops):
            _named_coordinates(stop, f"{prefix}.stops[{stop_index}]", reasons)
            stop_id = str((stop or {}).get("id") or "").strip() if isinstance(stop, dict) else ""
            if not stop_id:
                reasons.append(f"{prefix}.stops[{stop_index}].id:required")
            stop_ids.add(stop_id)
        service_date = parse_date(service.get("service_date"), f"{prefix}.service_date", reasons)
        if service_date and valid_from and service_date < valid_from or service_date and valid_until and service_date > valid_until:
            reasons.append(f"{prefix}.service_date:outside_validity")
        departure = service.get("departure")
        arrival = service.get("arrival")
        dep_dt = arr_dt = None
        for label, leg in (("departure", departure), ("arrival", arrival)):
            if not isinstance(leg, dict):
                reasons.append(f"{prefix}.{label}:object_required")
                continue
            if str(leg.get("stop_id") or "") not in stop_ids:
                reasons.append(f"{prefix}.{label}.stop_id:unknown")
            parsed = parse_datetime(leg.get("time"), f"{prefix}.{label}.time", reasons)
            if label == "departure":
                dep_dt = parsed
            else:
                arr_dt = parsed
        if service_date and dep_dt and dep_dt.astimezone(tz).date() != service_date:
            reasons.append(f"{prefix}.departure:wrong_service_date")
        if dep_dt and arr_dt and not dep_dt < arr_dt:
            reasons.append(f"{prefix}:arrival_not_after_departure")
        binding = service.get("binding")
        if not isinstance(binding, dict):
            reasons.append(f"{prefix}.binding:object_required")
        else:
            if not str(binding.get("city") or "").strip():
                reasons.append(f"{prefix}.binding.city:required")
            _named_coordinates(binding.get("venue"), f"{prefix}.binding.venue", reasons)
            event_ids = binding.get("event_ids")
            if not isinstance(event_ids, list):
                reasons.append(f"{prefix}.binding.event_ids:list_required")
        if service.get("direction") not in {"outbound", "return"}:
            reasons.append(f"{prefix}.direction:unsupported")
        if not isinstance(service.get("source"), dict) or urlparse(str((service.get("source") or {}).get("url") or "")).scheme != "https":
            reasons.append(f"{prefix}.source.url:https_required")

    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if fetched_at:
        fetched_utc = fetched_at.astimezone(timezone.utc)
        if fetched_utc > now:
            reasons.append("freshness:fetched_in_future")
        elif (now - fetched_utc).total_seconds() > max_age_hours * 3600:
            reasons.append("freshness:source_stale")
    if source_fetched_at:
        source_fetched_utc = source_fetched_at.astimezone(timezone.utc)
        if source_fetched_utc > now:
            reasons.append("freshness:source_fetched_in_future")
        elif (now - source_fetched_utc).total_seconds() > max_age_hours * 3600:
            reasons.append("freshness:source_document_stale")
        if fetched_at and source_fetched_utc > fetched_at.astimezone(timezone.utc):
            reasons.append("source.fetched_at:after_manifest_fetch")
    if valid_until and now.astimezone(ZoneInfo(TIMEZONE)).date() > valid_until:
        reasons.append("freshness:validity_expired")
    if reasons:
        raise ManifestValidationError(reasons)
    normalized = json.loads(canonical_json(manifest))
    normalized.pop("content_hash", None)
    normalized.pop("snapshot_hash", None)
    normalized["content_hash"] = digest(semantic_provider_payload(normalized))
    normalized["snapshot_hash"] = digest(normalized)
    return normalized
