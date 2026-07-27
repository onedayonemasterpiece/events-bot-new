"""Resolve and render standalone graphical medallions for Telegram event posts.

The source of truth is the same curated static-site asset inventory used by the
event pages.  Telegram gets a deterministic opaque graphite strip so it stays
readable in both light and dark client themes; it does not rebuild logos and it
does not use custom-emoji mosaics.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from PIL import Image, ImageDraw, ImageFilter


ROOT = Path(__file__).resolve().parent
PUBLIC_ASSET_ROOT = ROOT / "site" / "public"
ORGANIZER_MANIFEST = ROOT / "site" / "src" / "data" / "organizerMedallions.json"
FESTIVAL_MANIFEST = ROOT / "site" / "src" / "data" / "festivalMedallions.json"

STRIP_WIDTH = 1300
STRIP_HEIGHT = 330
STRIP_BACKGROUND = "#202830"
STRIP_RENDER_VERSION = "tg_graphic_medallion_strip_v1_1300x330_260"
MAX_VISUAL_HEIGHT = 260
MAX_REGULAR_WIDTH = 260
MAX_WIDE_WIDTH = 310
MAX_ITEMS_DEFAULT = 5
GAP = 36
SIDE_SAFE_AREA = 40

_ALNUM_BOUNDARY = re.compile(r"[0-9a-zа-я]", re.IGNORECASE)
_FESTIVAL_OR_PROGRAM_SLUGS = {"kgd80", "kantata-festival"}


def _norm(value: Any) -> str:
    return re.sub(
        r"\s+",
        " ",
        re.sub(r"[«»\"'`´’‘.,!?()[\]{}:;—–-]+", " ", str(value or "").casefold().replace("ё", "е")),
    ).strip()


def _bounded_match(alias: str, haystack: str) -> bool:
    needle = _norm(alias)
    if not needle or not haystack:
        return False
    start = haystack.find(needle)
    while start >= 0:
        end = start + len(needle)
        before = haystack[start - 1] if start else ""
        after = haystack[end] if end < len(haystack) else ""
        if not _ALNUM_BOUNDARY.match(before) and not _ALNUM_BOUNDARY.match(after):
            return True
        start = haystack.find(needle, start + 1)
    return False


def _load_items(path: Path, kind: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    items = payload.get("items") if isinstance(payload, dict) else []
    return [{**item, "manifest_kind": kind} for item in (items or []) if isinstance(item, dict)]


@lru_cache(maxsize=1)
def graphic_medallion_catalog() -> tuple[dict[str, Any], ...]:
    return tuple(
        [
            *_load_items(ORGANIZER_MANIFEST, "organizer"),
            *_load_items(FESTIVAL_MANIFEST, "festival"),
        ]
    )


def reset_graphic_medallion_catalog_cache() -> None:
    graphic_medallion_catalog.cache_clear()


def _event_text(event: Any, fields: Iterable[str]) -> str:
    values: list[str] = []
    for field in fields:
        value = getattr(event, field, None)
        if isinstance(value, (list, tuple)):
            values.extend(str(item or "") for item in value)
        elif value:
            values.append(str(value))
    return _norm(" | ".join(values))


def _aliases(item: dict[str, Any]) -> list[str]:
    return [
        str(value)
        for value in [
            item.get("name"),
            item.get("shortName"),
            item.get("short_name"),
            item.get("slug"),
            *(item.get("aliases") or []),
        ]
        if str(value or "").strip()
    ]


def _public_asset_path(url: str | None) -> Path | None:
    raw = str(url or "").strip()
    if not raw.startswith("/assets/"):
        return None
    path = PUBLIC_ASSET_ROOT / raw.lstrip("/")
    return path if path.exists() else None


def _asset_for(item: dict[str, Any]) -> Path | None:
    candidates = [item.get("fallbackPngUrl"), item.get("avatarUrl")]
    avatar = str(item.get("avatarUrl") or "")
    if avatar.endswith(".svg"):
        candidates.insert(1, avatar[:-4] + ".png")
    for candidate in candidates:
        path = _public_asset_path(candidate)
        if path and path.suffix.lower() in {".png", ".webp", ".jpg", ".jpeg"}:
            return path
    return None


def resolve_event_graphic_medallions(
    event: Any,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Resolve source-grounded medallions in stable visual order.

    Venue/organizer aliases only match explicit location/source identity fields;
    festival/program marks only match ``event.festival``.  The KGD80 partnership
    is a curated exception: both the festival and Znanie marks are required.
    """

    max_items = max(1, int(limit or os.getenv("TG_GRAPHIC_MEDALLION_MAX_ITEMS", MAX_ITEMS_DEFAULT)))
    catalog = list(graphic_medallion_catalog())
    by_slug = {str(item.get("slug") or ""): item for item in catalog}
    location = _event_text(event, ("location_name", "location_address", "city"))
    organizers = _event_text(event, ("organizer_names",))
    identity = _event_text(
        event,
        ("tg_source_author", "source_post_url", "source_vk_post_url", "source_urls"),
    )
    festival_value = _norm(getattr(event, "festival", None))
    is_kgd80 = "80 истор" in festival_value and "главн" in festival_value
    selected: list[dict[str, Any]] = []
    selected_keys: set[str] = set()
    selected_slugs: set[str] = set()

    def add(item: dict[str, Any] | None, reason: str, *, semantic_key: str | None = None) -> None:
        if not item:
            return
        asset = _asset_for(item)
        if not asset:
            return
        slug = str(item.get("slug") or "").strip()
        key = semantic_key or slug
        if not slug or slug in selected_slugs or key in selected_keys:
            return
        selected_keys.add(key)
        selected_slugs.add(slug)
        selected.append({**item, "slug": slug, "asset_path": str(asset), "reason": reason})

    # Organizer/venue comes first. Do not let prose mentions or venue names
    # manufacture an organizer identity.
    for item in catalog:
        slug = str(item.get("slug") or "")
        if item.get("manifest_kind") != "organizer" or slug in _FESTIVAL_OR_PROGRAM_SLUGS:
            continue
        aliases = _aliases(item)
        category = str(item.get("category") or "organizer")
        if category == "venue_brand" and any(_bounded_match(alias, location) for alias in aliases):
            add(item, "location_alias")
        elif category == "organizer" and any(
            _bounded_match(alias, organizers) for alias in aliases
        ):
            add(item, "organizer_field")
        elif not (slug == "znanie-russia" and is_kgd80) and any(
            _bounded_match(alias, identity) for alias in aliases
        ):
            add(item, "source_identity")

    if is_kgd80:
        # Prefer the dedicated festival asset, fall back to the legacy combined
        # organizer manifest item when a deployment predates festivalMedallions.
        add(
            by_slug.get("kgd80-80-stories") or by_slug.get("kgd80"),
            "kgd80_festival_curated",
            semantic_key="festival:kgd80",
        )
        add(by_slug.get("znanie-russia"), "kgd80_znanie_curated", semantic_key="partner:znanie")
    elif festival_value:
        # Current curated festival/program manifest.  Require the structured
        # festival field rather than broad description/title keyword matching.
        for item in catalog:
            if item.get("manifest_kind") != "festival":
                continue
            aliases = _aliases(item)
            if any(_bounded_match(alias, festival_value) for alias in aliases):
                add(item, "festival_field", semantic_key=f"festival:{item.get('slug')}")
        kantata = by_slug.get("kantata-festival")
        if kantata and "кантата" in festival_value:
            add(kantata, "festival_field", semantic_key="festival:kantata")

    if bool(getattr(event, "pushkin_card", False)):
        pushkin_item = {
            "slug": "pushkin-card",
            "name": "Пушкинская карта",
            "avatarUrl": "/assets/badges/pushkin-card-medallion.webp",
            "fallbackPngUrl": "/assets/badges/pushkin-card-medallion.png",
            "manifest_kind": "program",
        }
        add(pushkin_item, "pushkin_card", semantic_key="program:pushkin-card")

    return selected[:max_items]


def graphic_medallion_signature(event: Any) -> str:
    parts = [STRIP_RENDER_VERSION]
    for item in resolve_event_graphic_medallions(event):
        path = Path(str(item["asset_path"]))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        parts.append(f"{item['slug']}:{digest}")
    return "\n".join(parts)


def _scaled_asset(item: dict[str, Any]) -> Image.Image:
    image = Image.open(str(item["asset_path"])).convert("RGBA")
    wide = str(item.get("slug") or "") == "pushkin-card"
    max_width = MAX_WIDE_WIDTH if wide else MAX_REGULAR_WIDTH
    scale = min(max_width / image.width, MAX_VISUAL_HEIGHT / image.height, 1.0)
    if scale < 1:
        image = image.resize(
            (max(1, round(image.width * scale)), max(1, round(image.height * scale))),
            Image.Resampling.LANCZOS,
        )
    return image


def render_event_graphic_medallion_strip(medallions: list[dict[str, Any]]) -> bytes:
    if not medallions:
        raise ValueError("at least one graphical medallion is required")
    images = [_scaled_asset(item) for item in medallions]
    available = STRIP_WIDTH - SIDE_SAFE_AREA * 2 - GAP * (len(images) - 1)
    total_width = sum(image.width for image in images)
    if total_width > available:
        scale = available / total_width
        images = [
            image.resize(
                (max(1, round(image.width * scale)), max(1, round(image.height * scale))),
                Image.Resampling.LANCZOS,
            )
            for image in images
        ]
    canvas = Image.new("RGB", (STRIP_WIDTH, STRIP_HEIGHT), STRIP_BACKGROUND)
    group_width = sum(image.width for image in images) + GAP * (len(images) - 1)
    x = (STRIP_WIDTH - group_width) // 2
    for image in images:
        y = (STRIP_HEIGHT - image.height) // 2
        alpha = image.getchannel("A")
        shadow = Image.new("RGBA", image.size, (0, 0, 0, 0))
        shadow.putalpha(
            alpha.filter(ImageFilter.GaussianBlur(10)).point(lambda value: round(value * 0.32))
        )
        canvas.paste(shadow, (x + 4, y + 7), shadow)
        canvas.paste(image, (x, y), image)
        x += image.width + GAP
    draw = ImageDraw.Draw(canvas)
    draw.line((0, 1, STRIP_WIDTH, 1), fill="#35414C", width=2)
    draw.line((0, STRIP_HEIGHT - 2, STRIP_WIDTH, STRIP_HEIGHT - 2), fill="#151B21", width=2)
    output = io.BytesIO()
    canvas.save(output, format="JPEG", quality=96, subsampling=0, optimize=True)
    return output.getvalue()
