"""Deterministic daily service-share asset built from the accepted event snapshot.

The renderer is intentionally CPU/Pillow-only.  It is executed by the existing
coalesced StaticSiteBuilder job before Astro builds, never from a share click.
"""
from __future__ import annotations

import hashlib
import json
import os
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont


SCHEMA_VERSION = "service_share_asset_manifest_v2"
SELECTOR_VERSION = "service_share_daily_selector_v1"
RENDERER_VERSION = "service_share_daily_pillow_1080x1350_v1"
COPY_VERSION = "service_share_product_copy_v2"
CANONICAL_URL = "https://kenigevents.ru/"
TIME_ZONE = ZoneInfo("Europe/Kaliningrad")
SIZE = (1080, 1350)
MAX_AGE_SECONDS = 24 * 60 * 60


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf"),
    ]
    for candidate in candidates:
        if candidate.is_file():
            return ImageFont.truetype(str(candidate), size=size)
    return ImageFont.load_default()


def _event_score(event: dict[str, Any]) -> int:
    return (
        int(event.get("source_views_count") or 0)
        + 12 * int(event.get("source_likes_count") or 0)
        + 20 * int(event.get("service_likes_count") or 0)
    )


def _event_fingerprint(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(event["id"]),
        "title": str(event.get("title") or "").strip(),
        "start_date": str(event.get("start_date") or event.get("date") or "").strip(),
        "end_date": str(event.get("end_date") or "").strip(),
        "start_time": str(event.get("start_time") or event.get("time") or "").strip(),
        "city": str(event.get("city") or "").strip(),
        "venue_name": str(event.get("venue_name") or "").strip(),
        "lifecycle_status": str(event.get("lifecycle_status") or "active").strip(),
    }


def select_daily_events(
    events: list[dict[str, Any]], *, local_date: str, count: int = 8
) -> dict[str, Any]:
    """Select popular plus stable-daily events without mutating source state."""

    eligible: list[dict[str, Any]] = []
    seen: set[int] = set()
    for event in events:
        try:
            event_id = int(event.get("id") or 0)
        except (TypeError, ValueError):
            continue
        end_date = str(
            event.get("end_date") or event.get("start_date") or event.get("date") or ""
        )[:10]
        if (
            event_id <= 0
            or event_id in seen
            or end_date < local_date
            or str(event.get("lifecycle_status") or "active") != "active"
            or bool(event.get("silent"))
            or not str(event.get("title") or "").strip()
        ):
            continue
        seen.add(event_id)
        eligible.append(event)

    catalog_rows = [_event_fingerprint(event) for event in sorted(eligible, key=lambda row: int(row["id"]))]
    catalog_hash = _sha256_bytes(_canonical_json(catalog_rows))
    popular = sorted(
        eligible, key=lambda row: (-_event_score(row), str(row.get("start_date") or ""), int(row["id"]))
    )
    chosen: list[dict[str, Any]] = popular[: min(3, count)]
    chosen_ids = {int(row["id"]) for row in chosen}
    stable = sorted(
        (row for row in eligible if int(row["id"]) not in chosen_ids),
        key=lambda row: (
            _sha256_bytes(f"{local_date}:{catalog_hash}:{int(row['id'])}".encode("utf-8")),
            int(row["id"]),
        ),
    )
    chosen.extend(stable[: max(0, count - len(chosen))])
    selection_rows = [
        {
            **_event_fingerprint(event),
            "bucket": "popular" if index < min(3, len(chosen)) else "stable_daily",
        }
        for index, event in enumerate(chosen)
    ]
    selection_hash = _sha256_bytes(_canonical_json(selection_rows))
    city_names = sorted(
        {
            str(event.get("city") or "").strip()
            for event in eligible
            if str(event.get("city") or "").strip()
        },
        key=str.casefold,
    )
    return {
        "schema_version": "service_share_daily_selection_v1",
        "selector_version": SELECTOR_VERSION,
        "local_date": local_date,
        "catalog_hash": catalog_hash,
        "selection_hash": selection_hash,
        "eligible_event_count": len(eligible),
        "events_floor": (len(eligible) // 10) * 10,
        "city_count": len(city_names),
        "city_names": city_names,
        "event_ids": [int(row["id"]) for row in selection_rows],
        "events": selection_rows,
    }


def _wrapped(text: str, *, width: int, lines: int) -> list[str]:
    values = textwrap.wrap(" ".join(str(text or "").split()), width=width)
    return values[:lines] or ["Событие"]


def _render(selection: dict[str, Any]) -> Image.Image:
    image = Image.new("RGB", SIZE, "#f2e6d2")
    draw = ImageDraw.Draw(image)
    # Deterministic warm paper/cyclorama bands.
    for y in range(SIZE[1]):
        ratio = y / SIZE[1]
        draw.line((0, y, SIZE[0], y), fill=(242 - int(18 * ratio), 230 - int(12 * ratio), 210 - int(8 * ratio)))
    draw.rounded_rectangle((64, 62, 1016, 1288), radius=54, fill="#fffaf0", outline="#1f1b17", width=4)
    draw.text((104, 106), "ПОЛЮБИТЬ КАЛИНИНГРАД", font=_font(34, bold=True), fill="#a64b2a")
    draw.text((102, 154), "Анонсы", font=_font(88, bold=True), fill="#17120e")
    metric = int(selection.get("events_floor") or 0)
    metric_text = f"{metric}+ актуальных событий" if metric > 0 else "События Калининграда и области"
    draw.text((106, 258), metric_text, font=_font(31, bold=True), fill="#54483e")

    palette = ("#d9643a", "#e4a23c", "#466c5a", "#755c8a", "#306a84", "#b6505b", "#69743f", "#ba7b45")
    positions = [
        (104, 354, 506, 545), (552, 334, 974, 548),
        (94, 580, 448, 768), (482, 588, 982, 782),
        (112, 814, 544, 1008), (582, 814, 976, 1010),
        (92, 1042, 450, 1202), (488, 1038, 978, 1214),
    ]
    for index, row in enumerate(selection.get("events") or []):
        if index >= len(positions):
            break
        x1, y1, x2, y2 = positions[index]
        color = palette[index % len(palette)]
        draw.rounded_rectangle((x1, y1, x2, y2), radius=26, fill=color)
        title_lines = _wrapped(str(row.get("title") or ""), width=max(16, (x2 - x1) // 19), lines=3)
        y = y1 + 24
        for line in title_lines:
            draw.text((x1 + 24, y), line, font=_font(25, bold=True), fill="white")
            y += 31
        date_value = str(row.get("start_date") or "")
        end_value = str(row.get("end_date") or "")
        if date_value < str(selection.get("local_date") or "") <= end_value:
            date_value = f"до {end_value}"
        time_value = str(row.get("start_time") or "")
        location = str(row.get("venue_name") or row.get("city") or "")
        detail = " · ".join(value for value in (date_value, time_value, location) if value)
        detail_font = _font(18)
        max_width = x2 - x1 - 48
        raw_detail = detail
        while raw_detail and draw.textlength(detail, font=detail_font) > max_width:
            raw_detail = raw_detail[:-1].rstrip(" .·")
            detail = raw_detail + "…"
        draw.text((x1 + 24, y2 - 43), detail, font=detail_font, fill="#fff8e9")

    draw.text((106, 1240), "kenigevents.ru", font=_font(28, bold=True), fill="#17120e")
    draw.text((706, 1243), selection["local_date"], font=_font(22), fill="#67594d")
    return image


def build_daily_service_share(
    *,
    events: list[dict[str, Any]],
    public_root: Path,
    build_id: str,
    measured_at: str,
    source_snapshot_id: str | None,
    source_snapshot_hash: str | None,
    input_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Render immutable assets and atomically replace only the current pointer."""

    measured = datetime.fromisoformat(str(measured_at).replace("Z", "+00:00"))
    if measured.tzinfo is None:
        measured = measured.replace(tzinfo=timezone.utc)
    measured = measured.astimezone(timezone.utc)
    local_date = measured.astimezone(TIME_ZONE).date().isoformat()
    selection = select_daily_events(events, local_date=local_date)
    if not selection["events"]:
        raise RuntimeError("service share renderer has no eligible current events")

    version_basis = {
        "renderer_version": RENDERER_VERSION,
        "local_date": local_date,
        "catalog_hash": selection["catalog_hash"],
        "selection_hash": selection["selection_hash"],
    }
    visual_payload_hash = _sha256_bytes(_canonical_json(version_basis))
    asset_version = f"{local_date.replace('-', '')}-{visual_payload_hash[:16]}"
    version_dir = Path(public_root) / "service-share" / "versions" / asset_version
    version_dir.mkdir(parents=True, exist_ok=True)
    png_name = f"service-share-{asset_version}.png"
    webp_name = f"service-share-{asset_version}.webp"
    png_path = version_dir / png_name
    webp_path = version_dir / webp_name
    rendered = _render(selection)
    rendered.save(png_path, format="PNG", optimize=True)
    rendered.save(webp_path, format="WEBP", quality=82, method=6)
    if Image.open(png_path).size != SIZE or Image.open(webp_path).size != SIZE:
        raise RuntimeError("service share renderer emitted unexpected dimensions")

    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "asset_version": asset_version,
        "renderer_version": RENDERER_VERSION,
        "selector_version": SELECTOR_VERSION,
        "copy_version": COPY_VERSION,
        "visual_payload_hash": visual_payload_hash,
        "canonical_url": CANONICAL_URL,
        "share_text": "События Калининграда и области. Найдите своё событие быстрее.",
        "local_date": local_date,
        "timezone": str(TIME_ZONE),
        "measured_at": measured.isoformat().replace("+00:00", "Z"),
        "fresh_until": (measured + timedelta(seconds=MAX_AGE_SECONDS))
        .isoformat()
        .replace("+00:00", "Z"),
        "build_id": build_id,
        "source_snapshot_id": source_snapshot_id,
        "source_snapshot_hash": source_snapshot_hash,
        "input_fingerprint": input_fingerprint,
        "metrics": {
            "eligible_event_count": selection["eligible_event_count"],
            "events_floor": selection["events_floor"],
            "city_count": selection["city_count"],
            "catalog_hash": selection["catalog_hash"],
        },
        "selection": {
            "selection_hash": selection["selection_hash"],
            "event_ids": selection["event_ids"],
            "events": selection["events"],
        },
        "assets": {
            "png": {
                "url": f"../versions/{asset_version}/{png_name}",
                "filename": png_name,
                "mime_type": "image/png",
                "width": SIZE[0],
                "height": SIZE[1],
                "byte_size": png_path.stat().st_size,
                "sha256": _sha256_file(png_path),
            },
            "webp": {
                "url": f"../versions/{asset_version}/{webp_name}",
                "filename": webp_name,
                "mime_type": "image/webp",
                "width": SIZE[0],
                "height": SIZE[1],
                "byte_size": webp_path.stat().st_size,
                "sha256": _sha256_file(webp_path),
            },
        },
    }
    manifest["manifest_payload_hash"] = _sha256_bytes(_canonical_json(manifest))
    immutable_manifest = version_dir / "manifest.json"
    _atomic_json(immutable_manifest, manifest)
    _atomic_json(Path(public_root) / "service-share" / "current" / "manifest.json", manifest)
    return manifest
