"""Daily service-share card data, selection and immutable asset contracts.

This module is deliberately independent from the static-site publisher.  It
reads the accepted Fly/SQLite catalogue and emits local, versioned artifacts;
moving ``current/manifest.json`` remains a separate compare-and-swap publish
operation.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from PIL import Image

from db import Database
from reaction_counter_sync import aggregate_source_reaction_counters


SERVICE_SHARE_TZ = ZoneInfo("Europe/Kaliningrad")
PROMO_SURFACE_SERVICE_SHARE_CARD = "service_share_card"
CANONICAL_URL = "https://kenigevents.ru/"
PUBLIC_MANIFEST_PATH = "/service-share/current/manifest.json"
MANIFEST_SCHEMA_VERSION = "service_share_asset_manifest_v1"
SHARE_TEXT = "Полюбить Калининград Анонсы — события всего региона"
_WS = re.compile(r"\s+")


def _clean(value: Any) -> str:
    return _WS.sub(" ", str(value or "").strip())


def _normalize_city(value: Any) -> tuple[str, str]:
    display = _clean(value)
    display = re.sub(r"^(?:пос(?:ёлок|елок)?\.?|пгт)\s+", "", display, flags=re.IGNORECASE)
    display = _clean(display)
    return display.casefold(), display


def _parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        raw = value
    else:
        try:
            raw = json.loads(value or "[]")
        except Exception:
            raw = []
    return [_clean(item) for item in raw if _clean(item)] if isinstance(raw, list) else []


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def event_is_public_current(event: dict[str, Any], *, local_date: str) -> bool:
    """Fail-closed projection used by the card's accepted-catalog snapshot."""

    if not event.get("id") or _clean(event.get("identity_status") or "canonical") != "canonical":
        return False
    if event.get("merged_into_event_id") not in (None, "", 0):
        return False
    if bool(event.get("silent")) or _clean(event.get("lifecycle_status") or "") != "active":
        return False
    try:
        date.fromisoformat(_clean(event.get("date") or event.get("start_date")))
        last_day = date.fromisoformat(_clean(event.get("end_date") or event.get("date") or event.get("start_date")))
    except ValueError:
        return False
    return last_day.isoformat() >= local_date


def _canonical_event(event: dict[str, Any]) -> dict[str, Any]:
    poster_url = _clean(event.get("poster_url"))
    # Public cards use only the accepted event-media projection.  Legacy
    # ``event.photo_urls`` may predate duplicate/semantic review.
    image_url = poster_url
    return {
        "id": int(event["id"]),
        "title": _clean(event.get("title")),
        "start_date": _clean(event.get("date") or event.get("start_date")),
        "end_date": _clean(event.get("end_date")) or None,
        "start_time": _clean(event.get("time") or event.get("start_time")),
        "city": _clean(event.get("city")),
        "festival": _clean(event.get("festival")) or None,
        "image_url": image_url,
        "safe_crop": bool(event.get("safe_crop", True)),
        "image_text_mode": event.get("image_text_mode"),
        "image_object_position": event.get("image_object_position"),
        "added_at": _parse_datetime(event.get("added_at")).isoformat() if _parse_datetime(event.get("added_at")) else None,
        "lifecycle_status": "active",
    }


def build_catalog_snapshot(
    events: Iterable[dict[str, Any]], *, measured_at: datetime
) -> dict[str, Any]:
    measured_at = measured_at.astimezone(timezone.utc) if measured_at.tzinfo else measured_at.replace(tzinfo=timezone.utc)
    local_date = measured_at.astimezone(SERVICE_SHARE_TZ).date().isoformat()
    accepted_by_id: dict[int, dict[str, Any]] = {}
    for raw in events:
        if event_is_public_current(raw, local_date=local_date):
            accepted_by_id[int(raw["id"])] = _canonical_event(raw)
    accepted = [accepted_by_id[key] for key in sorted(accepted_by_id)]
    city_by_key: dict[str, str] = {}
    city_frequency: dict[str, int] = {}
    for event in accepted:
        city_key, city = _normalize_city(event.get("city"))
        if city:
            city_by_key.setdefault(city_key, city)
            city_frequency[city_key] = city_frequency.get(city_key, 0) + 1
    cutoff = measured_at - timedelta(days=7)
    recent = sum(1 for event in accepted if (_parse_datetime(event.get("added_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff)
    hash_rows = [
        {key: event.get(key) for key in ("id", "title", "start_date", "end_date", "start_time", "city", "festival", "image_url")}
        for event in accepted
    ]
    catalog_hash = hashlib.sha256(
        json.dumps(hash_rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema_version": "service_share_catalog_snapshot_v1",
        "measured_at": measured_at.isoformat().replace("+00:00", "Z"),
        "timezone": str(SERVICE_SHARE_TZ),
        "local_date": local_date,
        "eligible_event_count": len(accepted),
        "city_count": len(city_by_key),
        "recent_added_count": recent,
        "city_names": [city_by_key[key] for key in sorted(city_by_key, key=lambda key: (-city_frequency[key], key))],
        "catalog_hash": catalog_hash,
        "events": accepted,
    }


async def load_catalog_snapshot(db: Database, *, measured_at: datetime | None = None) -> dict[str, Any]:
    """Read a stable catalogue projection without mutating event or promo state."""

    measured_at = measured_at or datetime.now(timezone.utc)
    async with db.raw_conn() as conn:
        cursor = await conn.execute(
            """
            SELECT e.id, e.title, e.date, e.end_date, e.time, e.city, e.festival,
                   e.photo_urls, e.added_at, e.identity_status, e.merged_into_event_id,
                   e.silent, e.lifecycle_status,
                   (SELECT COALESCE(NULLIF(ep.supabase_url,''), NULLIF(ep.thumbnail_512_url,''), NULLIF(ep.thumbnail_256_url,''))
                      FROM eventposter ep
                     WHERE ep.event_id=e.id
                       AND ep.duplicate_of_id IS NULL
                       AND ep.review_status='approved'
                     ORDER BY ep.display_order, ep.id LIMIT 1) AS poster_url,
                   (SELECT ep.safe_crop FROM eventposter ep
                     WHERE ep.event_id=e.id AND ep.duplicate_of_id IS NULL
                     ORDER BY ep.display_order, ep.id LIMIT 1) AS safe_crop,
                   (SELECT ep.image_text_mode FROM eventposter ep
                     WHERE ep.event_id=e.id AND ep.duplicate_of_id IS NULL
                     ORDER BY ep.display_order, ep.id LIMIT 1) AS image_text_mode
              FROM event e
            """
        )
        columns = [item[0] for item in cursor.description]
        rows = [dict(zip(columns, row)) for row in await cursor.fetchall()]
        await cursor.close()
    return build_catalog_snapshot(rows, measured_at=measured_at)


async def load_active_promo_candidates(
    db: Database,
    *,
    snapshot: dict[str, Any],
    measured_at: datetime,
    surface: str = PROMO_SURFACE_SERVICE_SHARE_CARD,
) -> list[dict[str, Any]]:
    """Resolve already-configured exact-surface promo targets, read-only.

    No campaign ``ensure`` helper is called. Unsupported target types simply
    produce no candidate and remain visible in the returned audit counts.
    """

    now_utc = measured_at.astimezone(timezone.utc) if measured_at.tzinfo else measured_at.replace(tzinfo=timezone.utc)
    events = {int(row["id"]): row for row in snapshot["events"] if row.get("image_url")}
    async with db.raw_conn() as conn:
        cursor = await conn.execute(
            """
            SELECT c.id AS campaign_id, c.priority, c.starts_at, c.ends_at,
                   a.id AS activity_id, a.max_per_publish,
                   t.id AS target_id, t.target_type, t.event_id, t.festival_name, t.query_text
              FROM promo_campaign c
              JOIN promo_activity a ON a.campaign_id=c.id
              JOIN promo_target t ON t.campaign_id=c.id
             WHERE c.status='active' AND a.enabled=1 AND a.surface=?
             ORDER BY c.priority DESC, c.id, a.id, t.id
            """,
            (surface,),
        )
        columns = [item[0] for item in cursor.description]
        targets = [dict(zip(columns, row)) for row in await cursor.fetchall()]
        await cursor.close()
        provenance = "exact_surface_activity"
        # Preview bootstrap: production currently has active explicit campaign
        # targets but no service_share_card activity. Resolve only explicit
        # event/festival targets read-only; broad targets are never inherited.
        if not targets:
            cursor = await conn.execute(
                """
                SELECT c.id AS campaign_id, c.priority, c.starts_at, c.ends_at,
                       0 AS activity_id, 999 AS max_per_publish,
                       t.id AS target_id, t.target_type, t.event_id, t.festival_name, t.query_text
                  FROM promo_campaign c
                  JOIN promo_target t ON t.campaign_id=c.id
                 WHERE c.status='active' AND t.target_type IN ('event','festival')
                 ORDER BY c.priority DESC, c.id, t.id
                """
            )
            columns = [item[0] for item in cursor.description]
            targets = [dict(zip(columns, row)) for row in await cursor.fetchall()]
            await cursor.close()
            provenance = "explicit_target_preview_fallback_no_surface_activity"
    output: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    per_activity: dict[int, int] = {}
    for target in targets:
        starts = _parse_datetime(target.get("starts_at"))
        ends = _parse_datetime(target.get("ends_at"))
        if (starts and starts > now_utc) or (ends and ends < now_utc):
            continue
        activity_id = int(target["activity_id"])
        cap = max(0, int(target.get("max_per_publish") or 0))
        if cap and per_activity.get(activity_id, 0) >= cap:
            continue
        target_type = _clean(target.get("target_type"))
        if target_type == "event" and target.get("event_id"):
            ids = [int(target["event_id"])]
        elif target_type == "festival" and _clean(target.get("festival_name")):
            festival = _clean(target["festival_name"])
            ids = [event_id for event_id, event in events.items() if _clean(event.get("festival")) == festival]
        else:
            ids = []
        for event_id in sorted(ids):
            key = (activity_id, event_id)
            if event_id not in events or key in seen or (cap and per_activity.get(activity_id, 0) >= cap):
                continue
            seen.add(key)
            per_activity[activity_id] = per_activity.get(activity_id, 0) + 1
            output.append({
                "event_id": event_id,
                "campaign_id": int(target["campaign_id"]),
                "activity_id": activity_id,
                "target_id": int(target["target_id"]),
                "priority": int(target.get("priority") or 0),
                "provenance": provenance,
            })
    output.sort(key=lambda row: (-row["priority"], row["campaign_id"], row["activity_id"], row["event_id"]))
    return output


async def enrich_snapshot_metrics(db: Database, snapshot: dict[str, Any]) -> None:
    counters = await aggregate_source_reaction_counters(db, event_ids=[row["id"] for row in snapshot["events"]])
    by_id = {int(counter.event_id): counter for counter in counters}
    for event in snapshot["events"]:
        counter = by_id.get(int(event["id"]))
        event.update({
            "source_likes_count": int(counter.source_likes_count) if counter else 0,
            "source_views_count": int(counter.source_views_count) if counter else 0,
            "source_engagement_sources_count": int(counter.source_engagement_sources_count) if counter else 0,
            "shares_count": 0,
            "service_likes_count": 0,
        })


def _file_record(path: Path, *, url: str, mime_type: str) -> dict[str, Any]:
    data = path.read_bytes()
    return {
        "url": url,
        "mime_type": mime_type,
        "byte_size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "filename": path.name,
    }


def export_asset_bundle(
    *,
    master_png: Path,
    output_dir: Path,
    visual_payload: dict[str, Any],
    selection: dict[str, Any],
    snapshot: dict[str, Any],
    composition: dict[str, Any],
    bundle_sha256: str,
    result_sha256: str,
) -> Path:
    """Export true PNG/WebP and the UI-owned current-manifest schema locally."""

    payload_hash = hashlib.sha256(
        json.dumps(visual_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    asset_version = f"{snapshot['local_date'].replace('-', '')}-{payload_hash[:16]}"
    version_root = output_dir / "versions" / asset_version
    version_root.mkdir(parents=True, exist_ok=True)
    png_path = version_root / f"service-share-{asset_version}.png"
    webp_path = version_root / f"service-share-{asset_version}.webp"
    image = Image.open(master_png).convert("RGB")
    image.save(png_path, format="PNG", optimize=True)
    image.save(webp_path, format="WEBP", quality=88, method=6)
    if png_path.read_bytes()[:8] != b"\x89PNG\r\n\x1a\n":
        raise RuntimeError("PNG signature mismatch")
    if webp_path.read_bytes()[:4] != b"RIFF" or webp_path.read_bytes()[8:12] != b"WEBP":
        raise RuntimeError("WebP signature mismatch")
    # Relative to ``service-share/current/manifest.json`` so a preview build
    # keeps its build-id prefix while production resolves to the same root tree.
    base_url = f"../versions/{asset_version}"
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "asset_version": asset_version,
        "visual_payload_hash": payload_hash,
        "canonical_url": CANONICAL_URL,
        "share_text": SHARE_TEXT,
        "assets": {
            "webp": _file_record(webp_path, url=f"{base_url}/{webp_path.name}", mime_type="image/webp"),
            "png": _file_record(png_path, url=f"{base_url}/{png_path.name}", mime_type="image/png"),
        },
        "measured_at": snapshot["measured_at"],
        "timezone": snapshot["timezone"],
        "local_date": snapshot["local_date"],
        "metrics": {key: snapshot[key] for key in ("eligible_event_count", "city_count", "recent_added_count", "catalog_hash")},
        "selection": {
            "event_ids": [int(row["event_id"]) for row in selection["events"]],
            "groups": [row["selection_group"] for row in selection["events"]],
            "actual_mix": selection.get("actual_mix"),
            "promo_status": selection.get("promo_status"),
            "promo_shortfall": selection.get("promo_shortfall"),
        },
        "composition": composition,
        "bundle_sha256": bundle_sha256,
        "result_sha256": result_sha256,
    }
    version_manifest = version_root / "manifest.json"
    version_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    current_manifest = output_dir / "current" / "manifest.json"
    current_manifest.parent.mkdir(parents=True, exist_ok=True)
    current_manifest.write_text(version_manifest.read_text())
    return current_manifest
