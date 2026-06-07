from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from PIL import Image, ImageDraw, ImageFont, ImageOps
from sqlalchemy import select

from db import Database
from models import Festival

logger = logging.getLogger(__name__)

WIDE_SIZE = (1920, 768)
MOBILE_SIZE = (1080, 1920)
MAX_MOBILE_COVERS = 5
DEFAULT_TTL_DAYS = 7

SETTINGS_ENABLED = "vk_dynamic_cover_enabled"
SETTINGS_ACTIVE_UNTIL = "vk_dynamic_cover_active_until"
SETTINGS_LAST_STATE = "vk_dynamic_cover_last_state"
SETTINGS_HISTORY = "vk_dynamic_cover_history"
SETTINGS_DEFAULT_STATE = "vk_dynamic_cover_default_state"

REFERENCE_LOGO = Path("docs/backlog/features/vk-dynamic-cover/photo_2025-02-02_11-08-25.jpg")

PALETTES = [
    {
        "id": "deep_wine_ivory",
        "background": "#4A0F1E",
        "text": "#FFF4DF",
        "accent": "#FFB000",
        "accent_text": "#000000",
    },
    {
        "id": "prussian_cream",
        "background": "#102A43",
        "text": "#FFF3D6",
        "accent": "#F26B38",
        "accent_text": "#000000",
    },
    {
        "id": "baltic_navy_sand",
        "background": "#071B33",
        "text": "#F5E6C8",
        "accent": "#3DD6D0",
        "accent_text": "#000000",
    },
    {
        "id": "pine_black_mint",
        "background": "#063B35",
        "text": "#E9FFF7",
        "accent": "#FF6B6B",
        "accent_text": "#000000",
    },
    {
        "id": "plum_citron",
        "background": "#30104D",
        "text": "#FFF8EA",
        "accent": "#D6FF3F",
        "accent_text": "#111111",
    },
]

MASTER_PALETTE = {
    "background": "#101114",
    "panel_1": "#071B33",
    "panel_2": "#102A43",
    "panel_3": "#063B35",
    "text": "#FFF4DF",
    "muted": "#B9C5C6",
    "accent": "#FFB000",
    "accent_text": "#101114",
}

FALLBACK_FESTIVALS = (
    ("80 историй о главном", "июнь 2026", "истории города"),
    ("Кантата", "лето 2026", "музыка и места"),
    ("Русская музыка на Балтике", "июнь 2026", "концерты фестиваля"),
)


@dataclass(frozen=True, slots=True)
class CoverItem:
    title: str
    period: str
    subtitle: str = ""
    image_url: str | None = None
    vk_url: str | None = None


@dataclass(frozen=True, slots=True)
class CoverRenderResult:
    wide_path: Path
    mobile_paths: list[Path]
    items: list[CoverItem]
    title: str
    subtitle: str


def _hex_to_rgb(value: str) -> tuple[int, int, int]:
    raw = value.strip().lstrip("#")
    return (
        int(raw[0:2], 16),
        int(raw[2:4], 16),
        int(raw[4:6], 16),
    )


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/usr/share/fonts/truetype/montserrat/Montserrat-Bold.ttf" if bold else "/usr/share/fonts/truetype/montserrat/Montserrat-Regular.ttf",
        "/usr/share/fonts/truetype/inter/Inter-Bold.ttf" if bold else "/usr/share/fonts/truetype/inter/Inter-Regular.ttf",
        "/usr/share/fonts/truetype/manrope/Manrope-Bold.ttf" if bold else "/usr/share/fonts/truetype/manrope/Manrope-Regular.ttf",
        "/usr/share/fonts/truetype/ibm-plex/IBMPlexSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/ibm-plex/IBMPlexSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "Montserrat-Bold.ttf" if bold else "Montserrat-Regular.ttf",
        "Inter-Bold.ttf" if bold else "Inter-Regular.ttf",
        "Manrope-Bold.ttf" if bold else "Manrope-Regular.ttf",
        "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _text_bbox(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> tuple[int, int]:
    if not text:
        return (0, 0)
    box = draw.multiline_textbbox((0, 0), text, font=font, spacing=8)
    return (box[2] - box[0], box[3] - box[1])


def _wrap_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> str:
    words = re.split(r"\s+", str(text or "").strip())
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        width, _height = _text_bbox(draw, candidate, font)
        if current and width > max_width:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return "\n".join(lines)


def _fit_wrapped_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    box: tuple[int, int],
    *,
    max_size: int,
    min_size: int,
    bold: bool = True,
    max_lines: int = 4,
) -> tuple[str, ImageFont.ImageFont]:
    width, height = box
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    for size in range(max_size, min_size - 1, -2):
        font = _font(size, bold=bold)
        wrapped = _wrap_text(draw, clean, font, width)
        lines = wrapped.splitlines()
        tw, th = _text_bbox(draw, wrapped, font)
        if len(lines) <= max_lines and tw <= width and th <= height:
            return wrapped, font
    font = _font(min_size, bold=bold)
    wrapped = _wrap_text(draw, clean, font, width)
    lines = wrapped.splitlines()
    if len(lines) > max_lines:
        wrapped = "\n".join(lines[:max_lines]).rstrip()
        if not wrapped.endswith("..."):
            wrapped = wrapped.rstrip(". ") + "..."
    return wrapped, font


def _load_logo(size: int) -> Image.Image | None:
    if not REFERENCE_LOGO.exists():
        return None
    try:
        logo = Image.open(REFERENCE_LOGO).convert("RGBA")
    except Exception:
        return None
    logo.thumbnail((size, size), Image.Resampling.LANCZOS)
    mask = Image.new("L", logo.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, logo.size[0] - 1, logo.size[1] - 1), radius=max(24, size // 12), fill=255)
    logo.putalpha(mask)
    return logo


def _draw_badge(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    *,
    palette: dict[str, str],
    font_size: int,
) -> tuple[int, int, int, int]:
    x, y = xy
    font = _font(font_size, bold=True)
    label = str(text or "").strip().upper()[:24]
    tw, th = _text_bbox(draw, label, font)
    pad_x = 22
    pad_y = 10
    box = (x, y, x + tw + pad_x * 2, y + th + pad_y * 2 - 4)
    draw.rounded_rectangle(box, radius=16, fill=_hex_to_rgb(palette["accent"]))
    draw.text((x + pad_x, y + pad_y - 3), label, font=font, fill=_hex_to_rgb(palette["accent_text"]))
    return box


def _date_label(start: str | None, end: str | None) -> str:
    def _parse(v: str | None) -> date | None:
        raw = str(v or "").strip()
        if not raw:
            return None
        try:
            return date.fromisoformat(raw[:10])
        except Exception:
            return None

    s = _parse(start)
    e = _parse(end)
    if s and e and s != e:
        if s.month == e.month:
            return f"{s.day}-{e.day}.{e.month:02d}"
        return f"{s.day}.{s.month:02d}-{e.day}.{e.month:02d}"
    if s:
        return f"{s.day}.{s.month:02d}"
    if e:
        return f"до {e.day}.{e.month:02d}"
    return "сейчас"


def _festival_score(festival: Festival, today: date) -> tuple[int, str]:
    start_raw = str(getattr(festival, "start_date", "") or "")
    end_raw = str(getattr(festival, "end_date", "") or "")
    try:
        start = date.fromisoformat(start_raw[:10]) if start_raw else None
    except Exception:
        start = None
    try:
        end = date.fromisoformat(end_raw[:10]) if end_raw else None
    except Exception:
        end = None
    is_active = bool((not start or start <= today) and (not end or end >= today))
    is_future = bool(start and start > today)
    has_media = bool(getattr(festival, "photo_url", None) or getattr(festival, "photo_urls", None))
    has_vk = bool(getattr(festival, "vk_url", None) or getattr(festival, "vk_post_url", None))
    score = 0
    if is_active:
        score += 100
    if is_future:
        score += 50
    if has_media:
        score += 10
    if has_vk:
        score += 5
    sort_date = start_raw or end_raw or "9999-99-99"
    return (-score, sort_date)


async def select_cover_items(db: Database, *, limit: int = 3, now: datetime | None = None) -> list[CoverItem]:
    today = (now or datetime.now(timezone.utc)).date()
    async with db.get_session() as session:
        rows = (await session.execute(select(Festival))).scalars().all()
    candidates: list[Festival] = []
    for fest in rows:
        name = str(getattr(fest, "name", "") or "").strip()
        if not name:
            continue
        end_raw = str(getattr(fest, "end_date", "") or "")
        if end_raw:
            try:
                if date.fromisoformat(end_raw[:10]) < today - timedelta(days=7):
                    continue
            except Exception:
                pass
        candidates.append(fest)
    candidates.sort(key=lambda f: _festival_score(f, today))
    items: list[CoverItem] = []
    for fest in candidates[:limit]:
        period = _date_label(getattr(fest, "start_date", None), getattr(fest, "end_date", None))
        subtitle = (
            str(getattr(fest, "location_name", "") or "").strip()
            or str(getattr(fest, "city", "") or "").strip()
            or "фестиваль в афише"
        )
        photo_urls = getattr(fest, "photo_urls", None) or []
        image_url = getattr(fest, "photo_url", None) or (photo_urls[0] if photo_urls else None)
        items.append(
            CoverItem(
                title=str(getattr(fest, "name", "") or "").strip(),
                period=period,
                subtitle=subtitle,
                image_url=str(image_url).strip() if image_url else None,
                vk_url=str(getattr(fest, "vk_url", "") or getattr(fest, "vk_post_url", "") or "").strip() or None,
            )
        )
    if items:
        return items
    return [CoverItem(title=t, period=p, subtitle=s) for t, p, s in FALLBACK_FESTIVALS[:limit]]


def _draw_brand_block(draw: ImageDraw.ImageDraw, img: Image.Image, box: tuple[int, int, int, int], palette: dict[str, str]) -> None:
    x1, y1, x2, y2 = box
    bg = _hex_to_rgb(palette["background"])
    accent = _hex_to_rgb(palette["accent"])
    text = _hex_to_rgb(palette["text"])
    muted = _hex_to_rgb(palette.get("muted", "#B9C5C6"))
    draw.rectangle(box, fill=bg)
    draw.polygon([(x1, y2), (x2, y1 + 80), (x2, y2)], fill=tuple(max(0, c - 18) for c in bg))
    logo = _load_logo(238)
    if logo:
        img.alpha_composite(logo, (x1 + 76, y1 + 74))
    draw.line((x1 + 76, y2 - 206, x2 - 72, y2 - 206), fill=accent, width=4)
    draw.text((x1 + 76, y2 - 170), "ПОЛЮБИТЬ", font=_font(34, bold=True), fill=text)
    draw.text((x1 + 76, y2 - 126), "КАЛИНИНГРАД", font=_font(34, bold=True), fill=text)
    draw.text((x1 + 78, y2 - 70), "афиша города и области", font=_font(23), fill=muted)


def render_wide_cover(items: list[CoverItem], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    palette = MASTER_PALETTE
    img = Image.new("RGBA", WIDE_SIZE, _hex_to_rgb(palette["background"]) + (255,))
    draw = ImageDraw.Draw(img)
    brand_w = 460
    draw.rectangle((brand_w, 0, WIDE_SIZE[0], WIDE_SIZE[1]), fill=_hex_to_rgb(palette["background"]))
    usable = WIDE_SIZE[0] - brand_w
    count = max(1, min(3, len(items)))
    cell_w = math.ceil(usable / count)
    text_layout: list[tuple[int, int, CoverItem, int]] = []
    overlay = Image.new("RGBA", WIDE_SIZE, (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    panel_colors = [palette["panel_1"], palette["panel_2"], palette["panel_3"]]
    for idx, item in enumerate(items[:count]):
        x = brand_w + idx * cell_w
        x2 = min(WIDE_SIZE[0], x + cell_w)
        bg = _hex_to_rgb(panel_colors[idx % len(panel_colors)])
        accent = _hex_to_rgb(palette["accent"])
        skew = 70
        left_top = x if idx == 0 else x - skew
        left_bottom = x if idx == 0 else x - 2 * skew
        poly = [(left_top, 0), (x2 + skew, 0), (x2 - skew, WIDE_SIZE[1]), (left_bottom, WIDE_SIZE[1])]
        draw.polygon(poly, fill=bg)
        overlay_draw.polygon(
            [(left_top, 0), (x2 + skew, 0), (x2 + 8, 245), (left_top + 78, 350)],
            fill=(255, 255, 255, 16),
        )
        overlay_draw.polygon(
            [(left_bottom, WIDE_SIZE[1]), (x2 - skew, WIDE_SIZE[1]), (x2 - 10, 525), (left_bottom + 70, 620)],
            fill=(0, 0, 0, 38),
        )
        if idx > 0:
            draw.line([(x - skew, 38), (x - 2 * skew, WIDE_SIZE[1] - 38)], fill=accent, width=4)
        safe_x = max(brand_w + 52, x + 54)
        safe_w = max(250, min(cell_w - 160, WIDE_SIZE[0] - safe_x - 86))
        text_layout.append((safe_x, safe_w, item, idx))
    img.alpha_composite(overlay)

    _draw_brand_block(draw, img, (0, 0, brand_w, WIDE_SIZE[1]), palette)

    for safe_x, safe_w, item, idx in text_layout:
        text = _hex_to_rgb(palette["text"])
        muted = _hex_to_rgb(palette["muted"])
        _draw_badge(draw, (safe_x, 94), item.period, palette=palette, font_size=23)
        draw.text((safe_x, 164), f"{idx + 1:02d} / ФЕСТИВАЛЬ", font=_font(18, bold=True), fill=muted)
        title, font = _fit_wrapped_text(draw, item.title, (safe_w, 292), max_size=60, min_size=38, max_lines=3)
        draw.multiline_text((safe_x, 190), title, font=font, fill=text, spacing=6)
        subtitle = item.subtitle or "в афише"
        draw.line((safe_x, 552, safe_x + min(210, safe_w), 552), fill=_hex_to_rgb(palette["accent"]), width=3)
        sub, sub_font = _fit_wrapped_text(draw, subtitle, (safe_w, 54), max_size=26, min_size=20, bold=False, max_lines=1)
        draw.multiline_text((safe_x, 578), sub, font=sub_font, fill=muted, spacing=4)
    img.convert("RGB").save(output_path, "PNG", optimize=True)
    return output_path


def render_mobile_covers(items: list[CoverItem], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    slides: list[Path] = []
    palette = MASTER_PALETTE
    panel_colors = [palette["panel_1"], palette["panel_2"], palette["panel_3"]]
    brand_items = [CoverItem("Главные фестивали недели", "афиша", "Калининград и область")]
    for idx, item in enumerate((brand_items + items)[:MAX_MOBILE_COVERS]):
        bg_hex = palette["background"] if idx == 0 else panel_colors[(idx - 1) % len(panel_colors)]
        img = Image.new("RGBA", MOBILE_SIZE, _hex_to_rgb(bg_hex) + (255,))
        draw = ImageDraw.Draw(img)
        bg = _hex_to_rgb(bg_hex)
        accent = _hex_to_rgb(palette["accent"])
        text = _hex_to_rgb(palette["text"])
        muted = _hex_to_rgb(palette["muted"])
        draw.polygon([(0, 0), (MOBILE_SIZE[0], 0), (MOBILE_SIZE[0], 540), (0, 820)], fill=tuple(min(255, c + 16) for c in bg))
        draw.polygon([(0, 1480), (MOBILE_SIZE[0], 1160), (MOBILE_SIZE[0], MOBILE_SIZE[1]), (0, MOBILE_SIZE[1])], fill=tuple(max(0, c - 22) for c in bg))
        logo = _load_logo(190)
        if logo:
            img.alpha_composite(logo, (88, 84))
        _draw_badge(draw, (96, 328), item.period, palette=palette, font_size=27)
        if idx == 0:
            title, font = _fit_wrapped_text(draw, item.title, (850, 430), max_size=102, min_size=66, max_lines=3)
            draw.multiline_text((96, 560), title, font=font, fill=text, spacing=6)
            y = 1040
            for row in items[:3]:
                period = row.period
                name, name_font = _fit_wrapped_text(draw, row.title, (780, 74), max_size=36, min_size=28, max_lines=1)
                draw.text((96, y), period, font=_font(25, bold=True), fill=accent)
                draw.text((96, y + 44), name, font=name_font, fill=text)
                y += 142
        else:
            draw.text((96, 432), f"{idx:02d} / ФЕСТИВАЛЬ", font=_font(24, bold=True), fill=muted)
            title, font = _fit_wrapped_text(draw, item.title, (860, 610), max_size=98, min_size=54, max_lines=4)
            draw.multiline_text((96, 540), title, font=font, fill=text, spacing=6)
            subtitle = item.subtitle or "смотри в афише"
            draw.line((96, 1288, 360, 1288), fill=accent, width=6)
            sub, sub_font = _fit_wrapped_text(draw, subtitle, (820, 140), max_size=42, min_size=30, bold=False, max_lines=2)
            draw.multiline_text((96, 1332), sub, font=sub_font, fill=muted, spacing=4)
        draw.line((96, 1694, 984, 1694), fill=accent, width=4)
        draw.text((96, 1738), "Полюбить Калининград", font=_font(38, bold=True), fill=text)
        draw.text((98, 1790), "афиша города и области", font=_font(27), fill=muted)
        path = output_dir / f"vk-cover-mobile-{idx + 1}.png"
        img.convert("RGB").save(path, "PNG", optimize=True)
        slides.append(path)
    return slides


def render_cover_pack(items: list[CoverItem], output_dir: Path | None = None) -> CoverRenderResult:
    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix="vk-dynamic-cover-"))
    output_dir.mkdir(parents=True, exist_ok=True)
    wide = render_wide_cover(items, output_dir / "vk-cover-wide.png")
    mobile = render_mobile_covers(items, output_dir)
    return CoverRenderResult(
        wide_path=wide,
        mobile_paths=mobile,
        items=items,
        title="VK dynamic cover",
        subtitle=", ".join(i.title for i in items[:3]),
    )


async def build_cover_pack(db: Database, *, limit: int = 3, output_dir: Path | None = None) -> CoverRenderResult:
    items = await select_cover_items(db, limit=limit)
    return render_cover_pack(items, output_dir=output_dir)


def resolve_cover_group_id() -> str:
    raw = (
        os.getenv("VK_DYNAMIC_COVER_GROUP_ID")
        or os.getenv("VK_EVENTS_GROUP_ID")
        or os.getenv("VK_AFISHA_GROUP_ID")
        or ""
    )
    return str(raw).strip().lstrip("-")


def cover_storage_dir() -> Path:
    raw = str(os.getenv("VK_DYNAMIC_COVER_STORAGE_DIR") or "").strip()
    if raw:
        return Path(raw)
    if Path("/data").exists():
        return Path("/data/vk_dynamic_cover")
    return Path("artifacts/codex/vk-dynamic-cover")


def default_cover_path(group_id: str) -> Path:
    gid = str(group_id or "").strip().lstrip("-") or "default"
    return cover_storage_dir() / f"default-cover-{gid}.jpg"


def _normalize_vk_response(data: Any) -> dict[str, Any]:
    if isinstance(data, dict) and "response" in data and isinstance(data["response"], dict):
        return dict(data["response"])
    if isinstance(data, dict):
        return dict(data)
    return {}


def _extract_vk_groups(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict) and "response" in data:
        data = data["response"]
    if isinstance(data, dict) and isinstance(data.get("groups"), list):
        return [g for g in data["groups"] if isinstance(g, dict)]
    if isinstance(data, list):
        return [g for g in data if isinstance(g, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _best_cover_image_url(group: dict[str, Any]) -> str | None:
    cover = group.get("cover") if isinstance(group, dict) else None
    if not isinstance(cover, dict):
        return None
    images = cover.get("images")
    if not isinstance(images, list):
        return None
    candidates: list[tuple[int, str]] = []
    for image in images:
        if not isinstance(image, dict):
            continue
        url = str(image.get("url") or "").strip()
        if not url:
            continue
        try:
            width = int(image.get("width") or 0)
            height = int(image.get("height") or 0)
        except Exception:
            width = height = 0
        candidates.append((width * height, url))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


async def fetch_current_owner_cover_url(
    group_id: str,
    *,
    db: Database | None = None,
    bot: Any = None,
) -> str | None:
    import main as main_mod

    token = str(getattr(main_mod, "VK_USER_TOKEN", "") or os.getenv("VK_ACCESS_TOKEN4") or "").strip()
    vk_api_call = getattr(main_mod, "_vk_api")
    gid = str(group_id).strip().lstrip("-")
    if not gid:
        raise RuntimeError("VK_DYNAMIC_COVER_GROUP_ID/VK_EVENTS_GROUP_ID is not configured")
    raw = await vk_api_call(
        "groups.getById",
        {"group_id": gid, "fields": "cover"},
        db,
        bot,
        token=token or None,
        token_kind="user" if token else "group",
    )
    groups = _extract_vk_groups(raw)
    for group in groups:
        url = _best_cover_image_url(group)
        if url:
            return url
    return None


async def upload_owner_cover(
    group_id: str,
    image_path: Path,
    *,
    db: Database | None = None,
    bot: Any = None,
) -> dict[str, Any]:
    import main as main_mod

    token = str(getattr(main_mod, "VK_USER_TOKEN", "") or os.getenv("VK_ACCESS_TOKEN4") or "").strip()
    if not token:
        raise RuntimeError("VK_USER_TOKEN/VK_ACCESS_TOKEN4 is required for cover upload")
    vk_api_call = getattr(main_mod, "_vk_api")
    gid = str(group_id).strip().lstrip("-")
    if not gid:
        raise RuntimeError("VK_DYNAMIC_COVER_GROUP_ID/VK_EVENTS_GROUP_ID is not configured")
    upload_server = await vk_api_call(
        "photos.getOwnerCoverPhotoUploadServer",
        {
            "group_id": gid,
            "crop_x": 0,
            "crop_y": 0,
            "crop_x2": WIDE_SIZE[0],
            "crop_y2": WIDE_SIZE[1],
        },
        db,
        bot,
        token=token,
        token_kind="user",
    )
    upload_data = _normalize_vk_response(upload_server)
    upload_url = str(upload_data.get("upload_url") or "").strip()
    if not upload_url:
        raise RuntimeError("VK did not return owner cover upload_url")

    def _post_file() -> dict[str, Any]:
        with image_path.open("rb") as fh:
            resp = requests.post(upload_url, files={"file": fh}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    uploaded = await asyncio.to_thread(_post_file)
    saved = await vk_api_call(
        "photos.saveOwnerCoverPhoto",
        uploaded,
        db,
        bot,
        token=token,
        token_kind="user",
    )
    logger.info("vk_dynamic_cover.uploaded group_id=%s path=%s", gid, image_path)
    return _normalize_vk_response(saved) or {"ok": True}


async def _get_setting(db: Database, key: str) -> str | None:
    import main as main_mod

    return await main_mod.get_setting_value(db, key)


async def _set_setting(db: Database, key: str, value: str | None) -> None:
    import main as main_mod

    await main_mod.set_setting_value(db, key, value)


async def is_dynamic_cover_enabled(db: Database) -> bool:
    raw = await _get_setting(db, SETTINGS_ENABLED)
    return str(raw if raw is not None else "1").strip() != "0"


async def set_dynamic_cover_enabled(db: Database, enabled: bool) -> None:
    await _set_setting(db, SETTINGS_ENABLED, "1" if enabled else "0")


async def append_cover_history(db: Database, entry: dict[str, Any], *, limit: int = 20) -> None:
    raw = await _get_setting(db, SETTINGS_HISTORY)
    try:
        history = json.loads(raw or "[]")
        if not isinstance(history, list):
            history = []
    except Exception:
        history = []
    history.insert(0, entry)
    await _set_setting(db, SETTINGS_HISTORY, json.dumps(history[:limit], ensure_ascii=False))


async def load_cover_history(db: Database) -> list[dict[str, Any]]:
    raw = await _get_setting(db, SETTINGS_HISTORY)
    try:
        history = json.loads(raw or "[]")
    except Exception:
        return []
    return history if isinstance(history, list) else []


async def load_default_cover_state(db: Database) -> dict[str, Any] | None:
    raw = await _get_setting(db, SETTINGS_DEFAULT_STATE)
    try:
        state = json.loads(raw or "{}")
    except Exception:
        return None
    return state if isinstance(state, dict) and state else None


def _download_cover_to_default_path(url: str, path: Path) -> dict[str, Any]:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    with Image.open(BytesIO(resp.content)) as source:
        image = source.convert("RGB")
        original_size = image.size
        if image.size != WIDE_SIZE:
            image = ImageOps.fit(image, WIDE_SIZE, method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))
        path.parent.mkdir(parents=True, exist_ok=True)
        image.save(path, "JPEG", quality=94, optimize=True)
    return {
        "path": str(path),
        "source_url_host": urlparse(url).netloc,
        "original_width": original_size[0],
        "original_height": original_size[1],
        "width": WIDE_SIZE[0],
        "height": WIDE_SIZE[1],
    }


async def save_current_cover_as_default(
    db: Database,
    *,
    bot: Any = None,
    operator_id: int | None = None,
) -> dict[str, Any]:
    group_id = resolve_cover_group_id()
    if not group_id:
        raise RuntimeError("VK_DYNAMIC_COVER_GROUP_ID/VK_EVENTS_GROUP_ID is not configured")
    url = await fetch_current_owner_cover_url(group_id, db=db, bot=bot)
    if not url:
        raise RuntimeError("Current VK cover image was not found")
    path = default_cover_path(group_id)
    meta = await asyncio.to_thread(_download_cover_to_default_path, url, path)
    now = datetime.now(timezone.utc)
    state = {
        "kind": "default_saved",
        "group_id": group_id,
        "path": str(path),
        "saved_at": now.isoformat(),
        "operator_id": operator_id,
        "source_url": url,
        **meta,
    }
    await _set_setting(db, SETTINGS_DEFAULT_STATE, json.dumps(state, ensure_ascii=False))
    await append_cover_history(db, state)
    logger.info("vk_dynamic_cover.default_saved group_id=%s path=%s", group_id, path)
    return state


async def restore_saved_default_cover(
    db: Database,
    *,
    bot: Any = None,
    force: bool = False,
    reason: str = "manual_restore",
) -> bool:
    if not force and not await is_dynamic_cover_enabled(db):
        return False
    state = await load_default_cover_state(db)
    if not state:
        logger.warning("vk_dynamic_cover.default_not_saved")
        return False
    path = Path(str(state.get("path") or ""))
    if not path.exists():
        logger.warning("vk_dynamic_cover.default_missing path=%s", path)
        return False
    group_id = str(state.get("group_id") or resolve_cover_group_id()).strip().lstrip("-")
    if not group_id:
        raise RuntimeError("VK_DYNAMIC_COVER_GROUP_ID/VK_EVENTS_GROUP_ID is not configured")
    upload_result = await upload_owner_cover(group_id, path, db=db, bot=bot)
    now = datetime.now(timezone.utc)
    restored_state = {
        "kind": "default_saved_restore",
        "group_id": group_id,
        "wide_path": str(path),
        "restored_at": now.isoformat(),
        "saved_at": state.get("saved_at"),
        "reason": reason,
        "published": True,
        "upload_result": upload_result or {},
    }
    await _set_setting(db, SETTINGS_LAST_STATE, json.dumps(restored_state, ensure_ascii=False))
    await _set_setting(db, SETTINGS_ACTIVE_UNTIL, None)
    await append_cover_history(db, restored_state)
    logger.info("vk_dynamic_cover.restored_saved_default force=%s path=%s", force, path)
    return True


async def apply_dynamic_cover(
    db: Database,
    *,
    bot: Any = None,
    operator_id: int | None = None,
    reason: str = "manual",
    ttl_days: int = DEFAULT_TTL_DAYS,
    publish: bool = True,
) -> CoverRenderResult:
    if not await is_dynamic_cover_enabled(db):
        raise RuntimeError("VK dynamic cover is disabled")
    pack = await build_cover_pack(db)
    group_id = resolve_cover_group_id()
    upload_result: dict[str, Any] | None = None
    if publish:
        upload_result = await upload_owner_cover(group_id, pack.wide_path, db=db, bot=bot)
    now = datetime.now(timezone.utc)
    active_until = now + timedelta(days=max(1, ttl_days))
    state = {
        "kind": "festival_mvp",
        "group_id": group_id,
        "wide_path": str(pack.wide_path),
        "mobile_paths": [str(p) for p in pack.mobile_paths],
        "items": [asdict(i) for i in pack.items],
        "applied_at": now.isoformat(),
        "active_until": active_until.isoformat(),
        "reason": reason,
        "operator_id": operator_id,
        "published": bool(publish),
        "upload_result": upload_result or {},
    }
    await _set_setting(db, SETTINGS_LAST_STATE, json.dumps(state, ensure_ascii=False))
    await _set_setting(db, SETTINGS_ACTIVE_UNTIL, active_until.isoformat())
    await append_cover_history(db, state)
    return pack


async def restore_default_cover_if_expired(db: Database, *, bot: Any = None, force: bool = False) -> bool:
    if not force and not await is_dynamic_cover_enabled(db):
        return False
    raw_until = await _get_setting(db, SETTINGS_ACTIVE_UNTIL)
    if not force:
        if not raw_until:
            return False
        try:
            until = datetime.fromisoformat(raw_until)
            if until.tzinfo is None:
                until = until.replace(tzinfo=timezone.utc)
        except Exception:
            return False
        if until > datetime.now(timezone.utc):
            return False
    return await restore_saved_default_cover(
        db,
        bot=bot,
        force=force,
        reason="expired" if not force else "manual_restore",
    )


async def dynamic_cover_expiry_scheduler(db: Database, bot: Any = None) -> None:
    try:
        changed = await restore_default_cover_if_expired(db, bot=bot, force=False)
        if changed:
            logger.info("vk_dynamic_cover.scheduler_restored_default")
    except Exception:
        logger.exception("vk_dynamic_cover.scheduler_failed")
