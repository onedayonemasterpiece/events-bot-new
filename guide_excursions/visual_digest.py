"""Production visual schedule digest for guide excursions.

The visual digest is a separate VK-first publication: one 1080x1350 schedule
card with up to five future excursions.  The renderer is deterministic (Pillow,
local assets only) so the daily production job can reproduce the approved VK
prototype without relying on paid image generation.
"""

from __future__ import annotations

import io
import html
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiosqlite
from aiogram.types import BufferedInputFile
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

from db import Database
from markup import format_tel_link_for_display
from telegram_sources import canonicalize_tg_url, parse_tg_post_url

from .dedup import deduplicate_occurrence_rows
from .digest import RU_MONTH_GEN, format_date_time
from .digest_writer import VK_NAME_MARK_RE, ask_vk_narrative
from .parser import collapse_ws, extract_date_iso, names_for_source_role, normalize_title_key, occurrence_fare, price_for_publication, split_occurrence_blocks
from .route_history import load_materialized_history, load_cached_background, needs_background

logger = logging.getLogger(__name__)

VISUAL_DIGEST_FAMILY = "visual_schedule"
VISUAL_DIGEST_CARD_LIMIT = 5
VISUAL_DIGEST_MAX_CARDS_DEFAULT = max(
    1,
    min(int((os.getenv("GUIDE_VISUAL_DIGEST_MAX_CARDS") or "1") or 1), 10),
)
VISUAL_DIGEST_WINDOW_DAYS = max(
    3,
    min(
        int((os.getenv("GUIDE_VISUAL_DIGEST_WINDOW_DAYS") or os.getenv("GUIDE_DIGEST_WINDOW_DAYS") or "45") or 45),
        120,
    ),
)
VISUAL_DIGEST_VK_TARGET = collapse_ws(os.getenv("GUIDE_VISUAL_DIGEST_VK_TARGET") or os.getenv("GUIDE_DIGEST_VK_TARGET") or "uhtykaliningrad")
VISUAL_DIGEST_TG_MAX_URL = collapse_ws(
    os.getenv("GUIDE_VISUAL_DIGEST_MAX_URL")
    or os.getenv("GUIDE_DIGEST_MAX_URL")
    or "https://max.ru/channel_uh_kaliningrad"
)
VISUAL_DIGEST_TG_VK_URL = collapse_ws(
    os.getenv("GUIDE_VISUAL_DIGEST_VK_URL")
    or os.getenv("GUIDE_DIGEST_PUBLIC_VK_URL")
    or "https://vk.ru/uhtykaliningrad"
)
VISUAL_DIGEST_VK_TARGET_GROUP_ID = collapse_ws(
    os.getenv("GUIDE_VISUAL_DIGEST_VK_TARGET_GROUP_ID") or os.getenv("GUIDE_DIGEST_VK_TARGET_GROUP_ID")
)
VISUAL_DIGEST_VK_DELAY_SECONDS = max(
    60,
    min(int((os.getenv("GUIDE_VISUAL_DIGEST_VK_DELAY_SECONDS") or "600") or 600), 86400),
)
VISUAL_DIGEST_VK_STORY_DELAY_SECONDS = max(
    60,
    min(int((os.getenv("GUIDE_VISUAL_DIGEST_VK_STORY_DELAY_SECONDS") or "900") or 900), 86400),
)
VISUAL_DIGEST_REVIEW_DELAY_DAYS = max(
    1,
    min(int((os.getenv("GUIDE_VISUAL_DIGEST_REVIEW_DELAY_DAYS") or "3") or 3), 30),
)
VISUAL_DIGEST_TG_TARGET_CHATS_RAW = (
    os.getenv("GUIDE_VISUAL_DIGEST_TARGET_CHATS")
    or os.getenv("GUIDE_VISUAL_DIGEST_TARGET_CHAT")
    or os.getenv("GUIDE_DIGEST_TARGET_CHATS")
    or os.getenv("GUIDE_DIGEST_TARGET_CHAT")
    or "@wheretogo39"
)
VISUAL_DIGEST_TELEGRAM_ENABLED = (
    (os.getenv("ENABLE_GUIDE_VISUAL_DIGEST_TELEGRAM") or "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
# The production compose enables confirmed personal mentions. Test posts substitute their recipient.
VISUAL_DIGEST_VK_MENTIONS_ENABLED = (
    (os.getenv("GUIDE_VISUAL_DIGEST_VK_MENTIONS") or "").strip().lower()
    in {"1", "true", "yes", "on"}
)
VISUAL_DIGEST_STORIES_ENABLED = (
    (os.getenv("ENABLE_GUIDE_VISUAL_DIGEST_VK_STORIES") or "").strip().lower()
    in {"1", "true", "yes", "on"}
)

_MODULE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _MODULE_DIR.parent
_FONT_DIR = _REPO_ROOT / "assets" / "fonts"
_ASSET_DIR = _MODULE_DIR / "assets"
_ICON_DIR = _ASSET_DIR / "visual_digest_icons"
_AVATAR_DIR = _ASSET_DIR / "visual_digest_avatars"
_BRAND_DIR = _ASSET_DIR / "visual_digest_brand"
_BRAND_LOCKUP_FILE = _BRAND_DIR / "uhty_kaliningrad_v18.png"
_MAIN_FONT = _FONT_DIR / "Cygre-ExtraBold.ttf"
_BOLD_FONT = _FONT_DIR / "Cygre-Bold.ttf"
_SEMI_FONT = _FONT_DIR / "Cygre-SemiBold.ttf"
_DEJAVU_BOLD = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")
_NUMERO_FONT = _FONT_DIR.resolve().parents[1] / "video_announce" / "assets" / "DrukCyr-Bold.ttf"

W, H = 1080, 1350
RU_MONTH_SHORT = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}
RU_WEEK_SHORT = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
RU_WEEK_FULL = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]


@dataclass(frozen=True)
class VisualPalette:
    key: str
    name: str
    bg1: str
    bg2: str
    halo: str
    decor: str
    issue: str
    text: str
    sub: str
    count: str
    card: str
    card_stroke: str
    date_fill: str
    date_stroke: str
    route: str
    guide: str
    focus: str
    divider: str
    shadow: str
    icon_stroke: str
    accents: tuple[str, ...]


PALETTES: tuple[VisualPalette, ...] = (
    VisualPalette(
        "sage-rose-carbon", "sage rose + carbon", "#fff8f5", "#e6efde", "#e7aaa1", "#738c5a", "#33433f",
        "#192522", "#667064", "#596f4e", "#fffefa", "#d7d9c3", "#fff3ef", "#e7c6bd",
        "#bb5c55", "#52675b", "#6d715f", "#c2c9ad", "#33433f", "#d8d9c2",
        ("#33433f", "#c76558", "#7f9a5b", "#e5ad62", "#8b5f73"),
    ),
    VisualPalette(
        "sky-butter-petrol", "sky butter + petrol", "#f4fbff", "#fff2ca", "#75c9e9", "#f3c84f", "#1e5572",
        "#132b3a", "#596a76", "#216f8c", "#fffffb", "#c5d8de", "#f1fbff", "#c0d9e3",
        "#cf6b43", "#456270", "#6e7379", "#aac9d3", "#1e5572", "#bfd8e1",
        ("#1e5572", "#f3c84f", "#cf6b43", "#28a59a", "#6a58ad"),
    ),
    VisualPalette(
        "rose-matcha-ink", "rose matcha + ink", "#fff7f4", "#e8efd9", "#e9a1a1", "#9bbf64", "#30463f",
        "#1d2825", "#657061", "#536e4d", "#fffdfa", "#d9d6bd", "#fff2ef", "#e7c2bd",
        "#c76450", "#4c6056", "#6b705d", "#c7cfad", "#30463f", "#d9d6bd",
        ("#c76450", "#789650", "#30463f", "#e5a85c", "#a95f77"),
    ),
    VisualPalette(
        "papaya-sky-denim", "papaya sky + denim", "#fff5eb", "#d9f0ff", "#ffac73", "#2e6c8f", "#275c7f",
        "#152b3a", "#526a78", "#2d7696", "#fffdf9", "#bed6e5", "#fff3e8", "#eccab0",
        "#df6a43", "#3d6073", "#67747a", "#a9cadb", "#275c7f", "#bfd7e6",
        ("#df6a43", "#2d7696", "#ffba64", "#164b67", "#35a99d"),
    ),
    VisualPalette(
        "champagne-cypress", "champagne + cypress", "#fffaf0", "#e7eadc", "#dbc083", "#23584a", "#315d4f",
        "#1d2b25", "#626e60", "#47705d", "#fffefa", "#d6d7c0", "#fff7e8", "#e4cfa4",
        "#aa6a35", "#496659", "#6f6e59", "#c3c7a9", "#315d4f", "#d8d7bd",
        ("#315d4f", "#d08a3e", "#7a9b63", "#b3534a", "#6f6e59"),
    ),
    VisualPalette(
        "vanilla-ultramarine", "vanilla + ultramarine", "#fffdf1", "#e3e7ff", "#ffe680", "#2b47c7", "#263b9a",
        "#171d42", "#5b6078", "#344fb1", "#fffefa", "#cfd4ec", "#fff9df", "#e7d89d",
        "#db6b35", "#4b5575", "#70717f", "#b9c0e0", "#263b9a", "#c9d0ec",
        ("#263b9a", "#f5c34e", "#db6b35", "#189b99", "#6d55b8"),
    ),
    VisualPalette(
        "clay-mint-graphite", "clay mint + graphite", "#fff6ef", "#dff2e7", "#77d7bd", "#d47b58", "#3d5050",
        "#172726", "#5d6b66", "#456e6b", "#fffefa", "#c8dfd5", "#fff2e8", "#e3c8b5",
        "#bf6048", "#465f5c", "#6d6a61", "#b1d1c3", "#3d5050", "#c8dfd5",
        ("#3d5050", "#d47b58", "#62bfa8", "#e0ad4d", "#6e5a72"),
    ),
    VisualPalette(
        "sea-glass-mango", "sea glass + mango", "#f3fff9", "#d7f1e8", "#3fd1bf", "#ffb34a", "#087076",
        "#112c2d", "#526b66", "#087a80", "#fffffb", "#b5ded4", "#f6fffb", "#b7ded2",
        "#d66d2f", "#3b615e", "#687163", "#9bd1c7", "#087076", "#acdcd1",
        ("#087a80", "#ff8f36", "#27b7a7", "#17465a", "#f6bd4f"),
    ),
    VisualPalette(
        "butter-cobalt", "butter + cobalt", "#fffceb", "#dce8ff", "#ffd65c", "#2451c6", "#25479b",
        "#17234b", "#58617a", "#3159b4", "#fffef8", "#cbd7f2", "#fff8df", "#ead898",
        "#d46c3d", "#465776", "#6b7080", "#b5c2e4", "#25479b", "#c6d4ef",
        ("#3159b4", "#ffd35d", "#ff7b4a", "#1d8c8f", "#7b5bc9"),
    ),
)

_AVATAR_FILES = {
    "twometerguide": "twometerguide.jpg",
    "natakkaz": "natakkaz.jpg",
    "katimartihobby": "katimartihobby.jpg",
    "katya_kostyugova": "katya_kostyugova.jpg",
    "gid_zelenogradsk": "gid_zelenogradsk.jpg",
    "murnikovat": "murnikovat.jpg",
    "ruin_keepers": "ruin_keepers.jpg",
    "vkaliningrade": "vkaliningrade.jpg",
    "valeravezet": "valeravezet.jpg",
    "amber_fringilla": "amber_fringilla.jpg",
    "balticsyndicate": "balticsyndicate.jpg",
    "tatyana_udovenko_face": "tatyana_udovenko_face.jpg",
}
_ICON_FILES = {
    "walk": "walk.png",
    "route": "route.png",
    "bus": "bus.png",
    "boat": "boat.png",
    "tram": "tram.png",
}


def _visual_tz() -> ZoneInfo:
    name = collapse_ws(os.getenv("GUIDE_EXCURSIONS_TZ") or "Europe/Kaliningrad")
    try:
        return ZoneInfo(name or "Europe/Kaliningrad")
    except ZoneInfoNotFoundError:
        logger.warning("guide_visual_digest_invalid_tz value=%r; using UTC", name)
        return ZoneInfo("UTC")


def _visual_today() -> date:
    return datetime.now(_visual_tz()).date()


def _visual_start_iso(today: date | None = None) -> str:
    """Earliest date for a visual schedule digest.

    Same-day excursions are intentionally skipped: a daily VK digest should leave
    at least overnight decision time, so the production window starts tomorrow
    in the guide monitoring timezone.
    """

    base = today or _visual_today()
    return (base + timedelta(days=1)).isoformat()


def _future_cutoff_iso(today: date | None = None) -> str:
    base = today or _visual_today()
    return (base + timedelta(days=VISUAL_DIGEST_WINDOW_DAYS)).isoformat()


def _parse_target_chats(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if isinstance(value, str):
        raw = re.split(r"[\n,;]+", value)
    elif isinstance(value, Sequence):
        raw = [str(item) for item in value]
    else:
        raw = []
    seen: set[str] = set()
    out: list[str] = []
    for item in raw:
        target = collapse_ws(item)
        if not target or target in seen:
            continue
        seen.add(target)
        out.append(target)
    return tuple(out or ["@wheretogo39"])


VISUAL_DIGEST_TG_TARGET_CHATS = _parse_target_chats(VISUAL_DIGEST_TG_TARGET_CHATS_RAW)


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_load(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return fallback


def _parse_json_array(value: Any) -> list[Any]:
    data = _json_load(value, [])
    return data if isinstance(data, list) else []


def _safe_json_object(value: Any) -> dict[str, Any]:
    data = _json_load(value, {})
    return data if isinstance(data, dict) else {}


def _plain(value: Any) -> str:
    text = collapse_ws("" if value is None else str(value))
    if text.lower() in {"не указано", "not specified", "not_specified", "none", "null", "unknown", "n/a"}:
        return ""
    return text


def _fit(text: Any, limit: int) -> str:
    value = _plain(text)
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "…"


def _font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(path), size)


def _fallback_font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    if path.is_file():
        return _font(path, size)
    return _font(_MAIN_FONT, size)


def _hex(value: str) -> tuple[int, int, int]:
    raw = value.strip().lstrip("#")
    if len(raw) == 3:
        raw = "".join(ch * 2 for ch in raw)
    return int(raw[0:2], 16), int(raw[2:4], 16), int(raw[4:6], 16)


def _rgba(value: str, alpha: int = 255) -> tuple[int, int, int, int]:
    r, g, b = _hex(value)
    return r, g, b, alpha


def _gradient(size: tuple[int, int], c1: str, c2: str) -> Image.Image:
    w, h = size
    a = _hex(c1)
    b = _hex(c2)
    img = Image.new("RGB", size, a)
    pix = img.load()
    for y in range(h):
        for x in range(w):
            t = (x / max(1, w - 1) + y / max(1, h - 1)) / 2
            pix[x, y] = tuple(int(a[i] * (1 - t) + b[i] * t) for i in range(3))
    return img


def _alpha_circle_layer(size: tuple[int, int], bbox: tuple[int, int, int, int], fill: str, alpha: int) -> Image.Image:
    layer = Image.new("RGBA", size, (0, 0, 0, 0))
    ImageDraw.Draw(layer).ellipse(bbox, fill=_rgba(fill, alpha))
    return layer


def _card_text_runs(text: str, font: ImageFont.FreeTypeFont):
    for word in re.split("(№)", text):
        if word:
            yield word, _font(_NUMERO_FONT, font.size) if word == "№" else font


def _draw_card_line(draw, xy, text, font, fill):
    if "№" not in text:
        draw.text(xy, text, font=font, fill=fill)
        return
    x, y = xy
    baseline = y + font.getmetrics()[0]
    for run, face in _card_text_runs(text, font):
        draw.text((x, baseline), run, font=face, fill=fill, anchor="ls")
        x += draw.textlength(run, font=face)


def _wrap_by_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_width: int,
                   max_lines: int, *, use_numero: bool = False) -> list[str]:
    def width(line: str) -> float:
        if use_numero and "№" in line:
            return sum(draw.textlength(run, font=face) for run, face in _card_text_runs(line, font))
        return draw.textlength(line, font=font)

    words = _plain(text).split()
    if not words:
        return []
    lines: list[str] = []
    current = ""
    truncated = False
    consumed = 0
    for idx, word in enumerate(words):
        candidate = f"{current} {word}".strip()
        if not current or width(candidate) <= max_width:
            current = candidate
            consumed = idx + 1
            continue
        if len(lines) >= max_lines - 1:
            truncated = True
            break
        lines.append(current)
        current = word
        consumed = idx + 1
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) == max_lines and (truncated or consumed < len(words)):
        # Ensure visibly truncated titles end with ellipsis, not an abrupt word.
        last = lines[-1]
        while last and width(last + "…") > max_width:
            last = last[:-1].rstrip()
        lines[-1] = (last + "…") if last else "…"
    return lines


def _draw_text_stroked(
    draw: ImageDraw.ImageDraw,
    xy: tuple[float, float],
    text: str,
    *,
    font: ImageFont.FreeTypeFont,
    fill: str,
    stroke_fill: str,
    stroke_width: int = 3,
) -> None:
    draw.text(xy, text, font=font, fill=_hex(fill), stroke_width=stroke_width, stroke_fill=_hex(stroke_fill))


def _skew_text_layer(layer: Image.Image, *, skew: float = -0.10) -> Image.Image:
    w, h = layer.size
    xshift = abs(skew) * h
    new_w = int(w + xshift + 4)
    if skew < 0:
        coeff = (1, skew, xshift, 0, 1, 0)
    else:
        coeff = (1, skew, 0, 0, 1, 0)
    return layer.transform((new_w, h), Image.Transform.AFFINE, coeff, resample=Image.Resampling.BICUBIC)


@lru_cache(maxsize=1)
def _brand_lockup_asset() -> Image.Image | None:
    if not _BRAND_LOCKUP_FILE.is_file():
        return None
    return Image.open(_BRAND_LOCKUP_FILE).convert("RGBA")


def _brand_lockup(scale: float = 1.0) -> Image.Image:
    """Return the accepted `Ух ты, Калининград!` lockup.

    The lockup is intentionally a committed bitmap rendered from the previously
    approved v18 SVG prototype.  Do not recreate it with Pillow text transforms:
    different rasterizers changed the italic direction, stroke weight and
    `УХ ТЫ` proportions in review.
    """

    asset = _brand_lockup_asset()
    if asset is not None:
        img = asset.copy()
        if scale != 1:
            img = img.resize((int(img.width * scale), int(img.height * scale)), Image.Resampling.LANCZOS)
        return img

    base_w, base_h = 380, 160
    img = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    halo = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    hd = ImageDraw.Draw(halo)
    hd.ellipse((0, 2, 354, 134), fill=(255, 190, 90, 92))
    hd.ellipse((42, 8, 330, 130), fill=(15, 220, 210, 68))
    halo = halo.filter(ImageFilter.GaussianBlur(10))
    img.alpha_composite(halo)

    shape = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shape)
    sd.polygon([(38, 14), (266, 14), (252, 54), (22, 54)], fill=(242, 103, 39, 255))
    sd.polygon([(58, 5), (312, 5), (296, 28), (42, 28)], fill=(255, 190, 50, 240))
    sd.polygon([(6, 47), (328, 47), (304, 119), (30, 119)], fill=(5, 72, 82, 255))
    sd.polygon([(10, 42), (316, 42), (308, 60), (7, 60)], fill=(14, 205, 205, 248))
    sd.polygon([(268, 88), (348, 88), (320, 130), (246, 130)], fill=(233, 91, 54, 255))
    shadow = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    shadow.alpha_composite(shape, (0, 8))
    shadow = shadow.filter(ImageFilter.GaussianBlur(8))
    # darken shadow alpha only
    sh = Image.new("RGBA", (base_w, base_h), (36, 18, 11, 0))
    sh.putalpha(shadow.split()[-1].point(lambda v: int(v * 0.30)))
    img.alpha_composite(sh)
    img.alpha_composite(shape)

    text_layer = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    td = ImageDraw.Draw(text_layer)
    top_font = _font(_MAIN_FONT, 35)
    main_font = _font(_MAIN_FONT, 32)
    # glow strokes on a separate blurred layer
    glow = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.text((64, 23), "УХ ТЫ,", font=top_font, fill=(0, 0, 0, 0), stroke_width=10, stroke_fill=(255, 218, 92, 106))
    gd.text((42, 78), "КАЛИНИНГРАД!", font=main_font, fill=(0, 0, 0, 0), stroke_width=10, stroke_fill=(14, 224, 214, 96))
    glow = glow.filter(ImageFilter.GaussianBlur(4))
    text_layer.alpha_composite(glow)
    td.text((64, 23), "УХ ТЫ,", font=top_font, fill=(255, 250, 241, 255), stroke_width=4, stroke_fill=(72, 35, 23, 255))
    td.text((42, 78), "КАЛИНИНГРАД!", font=main_font, fill=(255, 250, 241, 255), stroke_width=4, stroke_fill=(72, 35, 23, 255))
    skewed = _skew_text_layer(text_layer, skew=-0.14)
    img.alpha_composite(skewed.crop((0, 0, base_w, base_h)))
    img = img.rotate(-6, resample=Image.Resampling.BICUBIC, expand=False)
    if scale != 1:
        img = img.resize((int(base_w * scale), int(base_h * scale)), Image.Resampling.LANCZOS)
    return img


def _palette_for_issue(issue_id: int) -> VisualPalette:
    return PALETTES[abs(int(issue_id or 0)) % len(PALETTES)]


def _date_parts(date_iso: Any) -> tuple[str, str, str, str]:
    raw = _plain(date_iso)
    try:
        d = date.fromisoformat(raw)
    except Exception:
        return "", "", "", ""
    return str(d.day), RU_MONTH_SHORT.get(d.month, ""), RU_WEEK_SHORT[d.weekday()], RU_WEEK_FULL[d.weekday()]


def _period_of(rows: Sequence[Mapping[str, Any]]) -> str:
    dates: list[date] = []
    for row in rows:
        try:
            dates.append(date.fromisoformat(_plain(row.get("date"))))
        except Exception:
            pass
    dates = sorted(set(dates))
    if not dates:
        return ""
    start, end = dates[0], dates[-1]
    if start == end:
        return f"{start.day} {RU_MONTH_SHORT.get(start.month, '')}".strip()
    if start.year == end.year and start.month == end.month:
        return f"{start.day}–{end.day} {RU_MONTH_SHORT.get(start.month, '')}".strip()
    return f"{start.day} {RU_MONTH_SHORT.get(start.month, '')}–{end.day} {RU_MONTH_SHORT.get(end.month, '')}".strip()


def _row_source_blob(row: Mapping[str, Any]) -> str:
    guides = " ".join(_plain(x) for x in (row.get("guide_names") or []) if _plain(x)) if isinstance(row.get("guide_names"), list) else ""
    orgs = " ".join(_plain(x) for x in (row.get("organizer_names") or []) if _plain(x)) if isinstance(row.get("organizer_names"), list) else ""
    values = [
        row.get("source_username"),
        row.get("primary_source_username"),
        row.get("source_title"),
        row.get("primary_source_title"),
        row.get("guide_line"),
        row.get("organizer_line"),
        row.get("guide_profile_marketing_name"),
        row.get("booking_url"),
        row.get("source_post_url"),
        row.get("channel_url"),
        guides,
        orgs,
    ]
    return " ".join(_plain(value) for value in values if _plain(value)).lower().replace("ё", "е")


def _is_katya_kostyugova(row: Mapping[str, Any]) -> bool:
    src = _row_source_blob(row)
    return bool(
        re.search(
            r"progulki_s_katey|katerinakostiugova|kostiugova|прогулки с катей|катя костюгова|екатерина костюгова",
            src,
        )
    )


def _source_name(row: Mapping[str, Any]) -> str:
    if people := row.get("participant_medallions"):
        return ", ".join(dict.fromkeys(_plain(p["name"]) for p in people))
    row = {**row, **{k: v for k, v in (row.get("fact_pack") or {}).items() if k in ("guide_names", "organizer_names")}}
    guides = [_plain(x) for x in (row.get("guide_names") or []) if _plain(x)] if isinstance(row.get("guide_names"), list) else []
    orgs = [_plain(x) for x in (row.get("organizer_names") or []) if _plain(x)] if isinstance(row.get("organizer_names"), list) else []
    if guides and _is_katya_kostyugova(row):
        normalized = [g.lower().replace("ё", "е") for g in guides]
        if normalized == ["катя"] or normalized == ["екатерина"] or normalized == ["катерина"]:
            return "Катя Костюгова"
    if guides:
        return ", ".join(guides)
    if orgs:
        return ", ".join(orgs)
    if row.get("source_roles_checked"):
        return ""
    return _plain(row.get("guide_line") or row.get("organizer_line") or row.get("guide_profile_marketing_name") or row.get("source_title"))


def _fact(row: Mapping[str, Any], key: str) -> Any:
    pack = row.get("fact_pack") if isinstance(row.get("fact_pack"), Mapping) else {}
    return pack.get(key) if isinstance(pack, Mapping) else None


def _route_raw(row: Mapping[str, Any]) -> str:
    return _plain(_fact(row, "route_summary") or row.get("route_summary") or row.get("route_line"))


def _short_meeting_point(value: Any) -> str:
    text = _plain(value)
    if not text:
        return ""
    text = re.sub(r"\s*\([^)]{24,}\)", "", text)
    text = re.sub(r"(?iu)памятник\s+", "пам. ", text)
    text = re.sub(r"(?iu)Угол Карла Маркса и Комсомольской", "угол Карла Маркса/Комсомольской", text)
    return _fit(text, 42)


_PLACE_CASE_ENDINGS = (
    r"(?:а|я|у|ю|е|и|ы|о|ой|ей|ом|ем|ам|ям|ах|ях|ов|ев|ый|ий|ая|яя|ое|ее|"
    r"ую|юю|ые|ие|ого|его|ому|ему|ым|им|ых|их)?"
)


def _place_phrase(value: str) -> re.Pattern[str]:
    """Match the same consecutive words with ordinary Russian case endings."""
    words = []
    for word in re.findall(r"[а-яa-z0-9]+", value.casefold().replace("ё", "е")):
        stem = re.sub(r"(?:ый|ий|ая|яя|ое|ее|а|я|о|е|ы|и)$", "", word)
        if len(stem) >= 4 and re.fullmatch("[а-я]+", word):
            words.append(re.escape(stem) + _PLACE_CASE_ENDINGS)
        else:
            words.append(re.escape(word))
    return re.compile(r"(?<!\w)" + r"""[\s«»„“”’'":,–—-]+""".join(words) + r"(?!\w)")


def _card_source_blocks(row: Mapping[str, Any], title: str) -> Iterator[str]:
    day = _plain(row.get("date"))
    posts = row.get("linked_posts")
    if posts is None:
        posts = [{"text": row.get("dedup_source_text"), "post_date": day}]
    title_match = _place_phrase(title)
    for post in posts:
        text = str(post.get("text") or "").casefold().replace("ё", "е")
        if not text:
            continue
        try:
            published = datetime.fromisoformat(str(post.get("post_date") or day))
        except ValueError:
            continue
        blocks = split_occurrence_blocks(text, post_date=published, keep_lines=True)
        for block in blocks:
            block_day = extract_date_iso(block, post_date=published)
            linked_single = not block_day and len(blocks) == 1 and post.get("post_kind") == "announce_single"
            if (block_day == day or linked_single) and title_match.search(block):
                yield block


def _confirmed_card_labels(row: Mapping[str, Any], labels: Sequence[str]) -> list[str]:
    """Choose labels from a single source block in the order they appear there."""
    patterns = [(label, _place_phrase(label)) for label in dict.fromkeys(labels) if re.search(r"\w", label)]
    title = _plain(row.get("canonical_title"))
    if not title or not patterns:
        return []
    best = []
    for block in _card_source_blocks(row, title):
        matches = [(m.start(), label) for label, pattern in patterns if (m := pattern.search(block))]
        if len(matches) > len(best):
            best = sorted(matches)
    return [label for _, label in best[:3]]


def _place_line(row: Mapping[str, Any]) -> str:
    meeting = _short_meeting_point(_fact(row, "meeting_point") or row.get("meeting_point"))
    if meeting:
        return re.sub(r"(?iu)^от\s+", "", meeting)
    title = _plain(row.get("canonical_title"))
    route = _route_raw(row)
    if title and route and normalize_title_key(route) != normalize_title_key(title):
        parts = [p.strip() for p in re.split(r"\s*(?:[,;→·]|\s[–—]\s)\s*", route) if p.strip()]
        if confirmed := _confirmed_card_labels(row, parts):
            return " · ".join(confirmed)
    return _plain(_fact(row, "city") or row.get("city")) or "детали в тексте"


def _schedule_attributes_day_to_other_title(post: Mapping[str, Any], title: str, day: str) -> bool:
    """A named outing in the introduction is not automatically the first dated entry."""
    text = str(post.get("text") or "").casefold().replace("ё", "е")
    sections = re.split(r"(?m)^\s*расписание\s*:\s*", text, maxsplit=1)
    if len(sections) != 2 or not title or not day:
        return False
    intro, schedule = sections
    name = _place_phrase(title)
    try:
        anchor = datetime.fromisoformat(str(post.get("post_date") or day))
    except ValueError:
        return False
    if not name.search(intro) or any(extract_date_iso(line, post_date=anchor) == day for line in intro.splitlines()):
        return False
    entries = [line for line in schedule.splitlines() if extract_date_iso(line, post_date=anchor) == day]
    if not entries or any(name.search(line) for line in schedule.splitlines()):
        return False
    # Require an explicitly titled schedule entry. A terse time or a location alone
    # gives no reason to override the extracted date.
    return all(re.search(r"[—–-]\s*[«\"“][^»\"”]+[»\"”]", entry) for entry in entries)


def _opening_has_only_published_time(post: Mapping[str, Any], clock: str) -> bool:
    text = str(post.get("text") or "")
    if not re.fullmatch(r"\d{1,2}:\d{2}", clock) or text.count(clock) != 1:
        return False
    return bool(re.search(
        r"\b(?:открытие|церемония)\b[^.!?\n]*?\bв\s+" + re.escape(clock) +
        r"[.!]\s*В\s+(?:этот|тот)\s+(?:же\s+)?день\b[^.!?\n]*?\b(?:экскурси|прогулк)",
        text, re.I,
    ))


def _apply_published_facts(row: dict[str, Any], posts: Sequence[Mapping[str, Any]]) -> None:
    """Recover the outing's extracted roles and fare before loading portraits or composing a post."""
    title = _plain(row.get("canonical_title"))
    day = _plain(row.get("date"))
    title_key = normalize_title_key(title).removeprefix("на ")
    pack = row.setdefault("fact_pack", {})
    roles: dict[str, set[str]] = {}
    current_price = price_for_publication(row.get("price_line") or row.get("price_text") or pack.get("price_text"))
    fares = set()
    for post in posts:
        text = post.get("text") or ""
        extractions = _parse_json_array(post.get("raw_facts_json"))
        programme = len({e.get("canonical_title") or e.get("title") for e in extractions
                         if isinstance(e, Mapping)}) > 1 or post.get("post_kind") == "announce_multi"
        for item in extractions:
            if not isinstance(item, Mapping) or (item.get("date") or item.get("date_iso")) != day:
                continue
            if normalize_title_key(item.get("canonical_title") or item.get("title") or "").removeprefix("на ") != title_key:
                continue
            if row.get("time") and item.get("time") and row["time"] != item["time"]:
                continue
            for key in ("guide_names", "organizer_names"):
                if not isinstance(item.get(key), list):
                    continue
                names = roles.setdefault(key, set())
                for name in item[key]:
                    if not isinstance(name, str):
                        continue
                    if key == "organizer_names" and programme:
                        event_lines = [line.casefold() for line in text.splitlines() if title.casefold() in line.casefold()]
                        other_guide = any(name.casefold() in line.casefold() and "гид" in line.casefold()
                                          for line in text.splitlines() if title.casefold() not in line.casefold())
                        named_here = any(name.casefold() in line and "организ" in line for line in event_lines)
                        if other_guide and not named_here:
                            continue
                    names.add(name.casefold())
        if not current_price or current_price.casefold() == "по запросу":
            try:
                anchor = datetime.fromisoformat(post.get("post_date") or day).replace(tzinfo=timezone.utc)
            except (TypeError, ValueError):
                continue
            fare = occurrence_fare(text, title=title, date_iso=day, published=anchor)
            if fare:
                fares.add(fare)

    for key, supported in roles.items():
        names = [n for n in pack.get(key, row.get(key, [])) or [] if isinstance(n, str) and n.casefold() in supported]
        row[key] = pack[key] = names
    # Some historical rows have no retained structured extraction. The original post
    # still distinguishes the booking contact and past projects from today's participants.
    original_texts = [post.get("text") for post in posts if post.get("text")]
    corrected = False
    for key in ("guide_names", "organizer_names"):
        names = pack.get(key, row.get(key)) or []
        checked = names_for_source_role(names, original_texts, organizer=key == "organizer_names")
        if checked != names:
            row.setdefault("unconfirmed_source_roles", {})[key] = [name for name in names if name not in checked]
            row[key] = pack[key] = checked
            corrected = True
    if roles or corrected:
        row["source_roles_checked"] = True
        # Frozen editions can already contain resolved portraits. Normal editions resolve them next.
        if isinstance(row.get("participant_medallions"), list):
            people = []
            for person in row["participant_medallions"]:
                allowed = [role for role in person.get("roles", []) if
                           (role == "Гид" and person["name"] in row.get("guide_names", [])) or
                           (role == "Организатор" and person["name"] in row.get("organizer_names", []))]
                if allowed:
                    people.append(dict(person, roles=allowed))
            row["participant_medallions"] = people
    if len(fares) == 1:
        current_price = fares.pop()
    row["price_text"] = pack["price_text"] = current_price
    if "price_line" in row:
        row["price_line"] = current_price


def _card_description(row: Mapping[str, Any]) -> str:
    """Keep the source wording of the hook intact on the card and in writer input."""
    return _plain(_fact(row, "main_hook") or row.get("main_hook"))


def _focus_line(row: Mapping[str, Any]) -> str:
    if _card_description(row):
        return ""
    audience = (row.get("fact_pack") or {}).get("audience_fit", row.get("audience_fit"))
    if not isinstance(audience, list):
        return ""
    labels = [_plain(label) for label in audience if isinstance(label, str) and re.search("[а-яё]", label, re.I)]
    return " · ".join(_confirmed_card_labels(row, labels))


def _modes_for(row: Mapping[str, Any]) -> list[str]:
    transport = row.get("transport") or _fact(row, "transport") or {}
    if isinstance(transport, Mapping) and "modes" in transport:
        return [mode for mode in transport["modes"] if mode in _ICON_FILES][:2]
    legacy = _plain(_fact(row, "group_format") or row.get("group_format")).casefold()
    legacy = " ".join(re.sub(r"[_-]+", " ", legacy).split())
    # Explicit types already stored by older imports; never infer from a title.
    return {
        **dict.fromkeys(("walking", "walking tour", "guided walk", "guided walking tour",
                         "пешком", "пешеходная", "пешеходная экскурсия",
                         "пешеходная прогулка экскурсия", "пешая прогулка"), ["walk"]),
        **dict.fromkeys(("bus", "bus tour", "автобус", "автобусная",
                         "автобусная экскурсия", "автобусная группа"), ["bus"]),
        "автобусно пешеходная": ["bus", "walk"],
        "джип тур": ["route"], "автомобильный выезд": ["route"],
        "tram": ["tram"], "boat": ["boat"],
    }.get(legacy, [])


def _seats_text(row: Mapping[str, Any]) -> str:
    facts = row.get("fact_pack") or {}
    text = _plain(facts.get("seats_text") if "seats_text" in facts else (row.get("seats_line") or row.get("seats_text")))
    if not text:
        return ""
    m = re.search(r"\d+", text)
    if not m:
        return ""
    n = int(m.group(0))
    if n % 10 == 1 and n % 100 != 11:
        word = "место"
    elif n % 10 in {2, 3, 4} and n % 100 not in {12, 13, 14}:
        word = "места"
    else:
        word = "мест"
    return f"{n} {word}"


def _seats_count(value: Any) -> int | None:
    text = _plain(value)
    if not text:
        return None
    m = re.search(r"\d+", text)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def _visual_item_state(row: Mapping[str, Any]) -> dict[str, Any]:
    """Fact subset that defines whether a visual digest repeat is meaningful."""

    seats = _seats_text(row) or _plain(row.get("seats_text") or row.get("seats_line"))
    state = {
        "title": _plain(row.get("canonical_title")),
        "date": _plain(row.get("date")),
        "time": _plain(row.get("time")),
        "status": _plain(row.get("status")),
        "city": _plain(_fact(row, "city") or row.get("city")),
        "meeting_point": _plain(_fact(row, "meeting_point") or row.get("meeting_point")),
        "route": _route_raw(row),
        "booking_url": _visual_booking_url(row),
        "price_text": _plain(row.get("price_text")),
        "seats_text": seats,
        "seats_count": _seats_count(seats),
        "is_last_call": bool(int(row.get("is_last_call") or 0)) if str(row.get("is_last_call") or "").strip().isdigit() else bool(row.get("is_last_call")),
    }
    return {key: value for key, value in state.items() if value not in ("", None, False)}


def _visual_publication_states(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    states = {}
    for row in rows:
        states.update(row.get("covered_visual_states") or {})
        if row.get("id"):
            states.setdefault(str(row["id"]), _visual_item_state(row))
    return states


def _visual_issue_state_map(media_items_json: Any) -> dict[str, dict[str, Any]]:
    media = _safe_json_object(media_items_json)
    raw = media.get("item_states")
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for key, value in raw.items():
        if isinstance(value, Mapping):
            out[str(key)] = dict(value)
    return out


def _is_low_seats_count(value: int | None) -> bool:
    return value is not None and value <= 4


def _visual_selection_reason(row: Mapping[str, Any]) -> str:
    """Return why this occurrence should enter the daily visual digest.

    A row is eligible if it has never appeared in the visual digest, or if the
    stored visual fact snapshot differs in a product-important way.  Plain
    `updated_at` churn is intentionally not enough: it otherwise burns the
    five daily slots on already-shown excursions without viewer-facing change.
    """

    published_id = int(row.get("published_visual_digest_issue_id") or 0)
    if published_id <= 0:
        return "new"

    current = _visual_item_state(row)
    previous = row.get("published_visual_digest_state")
    previous = dict(previous) if isinstance(previous, Mapping) else {}
    current_seats = current.get("seats_count")
    previous_seats = previous.get("seats_count")
    try:
        previous_seats_i = int(previous_seats) if previous_seats is not None else None
    except Exception:
        previous_seats_i = None
    try:
        current_seats_i = int(current_seats) if current_seats is not None else None
    except Exception:
        current_seats_i = None

    if not previous:
        if current.get("is_last_call"):
            return "last_call"
        if _is_low_seats_count(current_seats_i):
            return "low_seats"
        return ""

    # Old snapshots saved the rendered place line as well as the underlying route
    # and meeting point. Compare source fields so a renderer update cannot replay them.
    if "place" not in previous and current.get("city") != previous.get("city"):
        return "changed_place"
    if current.get("route") != previous.get("route"):
        return "changed_place"
    for key in ("date", "time", "status", "title", "meeting_point", "booking_url", "price_text"):
        if current.get(key) != previous.get(key):
            return f"changed_{key}"
    if current.get("is_last_call") and not previous.get("is_last_call"):
        return "last_call"
    if _is_low_seats_count(current_seats_i) and (previous_seats_i is None or current_seats_i < previous_seats_i):
        return "low_seats"
    return ""


def _avatar_key(row: Mapping[str, Any]) -> str:
    src = _row_source_blob(row)
    checks = [
        (r"татьяна удовенко|tanja_from_koenigsberg", "tatyana_udovenko_face"),
        (r"progulki_s_katey|katerinakostiugova|kostiugova|прогулки с катей|катя костюгова|екатерина костюгова", "katya_kostyugova"),
        (r"twometerguide|двухметров", "twometerguide"),
        (r"natakkaz|наталья казакова", "natakkaz"),
        (r"katimartihobby|катя марти|шаги кати", "katimartihobby"),
        (r"gid_zelenogradsk|кот[оа]ва наталья|наталья котова|зеленоградск", "gid_zelenogradsk"),
        (r"murnikovat|мурникова", "murnikovat"),
        (r"ruin[._-]?keepers|хранители руин", "ruin_keepers"),
        (r"vkaliningrade|narodexcursovod|народный экскурсовод", "vkaliningrade"),
        (r"valeravezet|автобус валер", "valeravezet"),
        (r"amber_fringilla|amber fringilla|пруссии|юлия гришанова", "amber_fringilla"),
        (r"balticsyndicate|baltic syndicate|балтийский синдикат|евгений мосиенко", "balticsyndicate"),
    ]
    for pattern, key in checks:
        if re.search(pattern, src):
            return key
    return ""


def _initials(row: Mapping[str, Any]) -> str:
    base = _source_name(row) or _plain(row.get("source_title")) or "Гид"
    letters = [part[0] for part in re.split(r"\s+", base.strip()) if part]
    return "".join(letters[:2]).upper()[:2] or "Г"


def _crop_circle(img: Image.Image, size: int) -> Image.Image:
    src = ImageOps.fit(img.convert("RGB"), (size, size), Image.Resampling.LANCZOS)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    out.paste(src, (0, 0), mask)
    return out


def _medallion_row(person: Mapping[str, Any], *, registered: bool = False) -> dict[str, Any]:
    return {"guide_names": [person["name"]], "source_title": person["name"], "avatar_roles_resolved": registered,
            "source_avatar_path": person.get("path") or ""}


def _shown_medallions(people: Sequence[Mapping[str, Any]], *, registered: bool = False) -> list[Mapping[str, Any]]:
    """People with a picture; when nobody has one, a single initials badge (the guide first)."""
    def pictured(person: Mapping[str, Any]) -> bool:
        row = _medallion_row(person)
        bundled = "" if registered else _AVATAR_FILES.get(_avatar_key(row), "")
        return Path(str(row["source_avatar_path"])).is_file() or bool(bundled and (_AVATAR_DIR / bundled).is_file())

    shown = [person for person in people if pictured(person)]
    if shown or not people:
        return shown
    return [next((p for p in people if "Гид" in (p.get("roles") or [])), people[0])]


def _draw_avatar(draw_img: Image.Image, row: Mapping[str, Any], x: int, y: int, size: int, pal: VisualPalette, accent: str) -> None:
    layer = Image.new("RGBA", draw_img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    # subtle shadow
    d.ellipse((x - 4, y + 4, x + size + 4, y + size + 12), fill=(0, 0, 0, 34))
    d.ellipse((x - 4, y - 4, x + size + 4, y + size + 4), fill=_rgba(pal.card, 255), outline=_rgba(accent, 255), width=5)
    key = "" if row.get("avatar_roles_resolved") else _avatar_key(row)
    path = _AVATAR_DIR / _AVATAR_FILES.get(key, "")
    dynamic = Path(str(row.get("source_avatar_path") or ""))
    if dynamic.is_file():
        path = dynamic
    if path.is_file():
        avatar = _crop_circle(Image.open(path), size - 4)
        layer.alpha_composite(avatar, (x + 2, y + 2))
        d.ellipse((x + 2, y + 2, x + size - 2, y + size - 2), outline=(255, 255, 255, 235), width=4)
    else:
        d.ellipse((x, y, x + size, y + size), fill=_rgba(accent, 235))
        f = _font(_MAIN_FONT, 24)
        txt = _initials(row)
        bb = d.textbbox((0, 0), txt, font=f)
        d.text((x + size / 2 - (bb[2] - bb[0]) / 2, y + size / 2 - (bb[3] - bb[1]) / 2 - 2), txt, font=f, fill=(255, 255, 255, 255))
    draw_img.alpha_composite(layer)


def _draw_icon_badges(img: Image.Image, modes: Sequence[str], x: int, y: int, pal: VisualPalette) -> None:
    layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    box = 50
    for idx, mode in enumerate(list(modes)[:2]):
        yy = y + idx * 56
        d.ellipse((x, yy, x + box, yy + box), fill=_rgba(pal.date_fill, 255), outline=_rgba(pal.icon_stroke, 255), width=3)
        icon_path = _ICON_DIR / _ICON_FILES.get(mode, "")
        if icon_path.is_file():
            icon = Image.open(icon_path).convert("RGBA")
            size = 34 if mode == "route" else 32
            icon = ImageOps.contain(icon, (size, size), Image.Resampling.LANCZOS)
            layer.alpha_composite(icon, (x + (box - icon.width) // 2, yy + (box - icon.height) // 2))
    img.alpha_composite(layer)


def _draw_chip(draw: ImageDraw.ImageDraw, text: str, x: int, y: int, accent: str) -> None:
    if not text:
        return
    f = _font(_MAIN_FONT, 17)
    w = max(92, int(draw.textlength(text, font=f)) + 28)
    draw.rounded_rectangle((x, y, x + w, y + 34), radius=17, fill=_hex(accent))
    bb = draw.textbbox((0, 0), text, font=f)
    draw.text((x + w / 2 - (bb[2] - bb[0]) / 2, y + 17 - (bb[3] - bb[1]) / 2 - 2), text, font=f, fill=(255, 255, 255))


def _draw_date_tile(draw: ImageDraw.ImageDraw, row: Mapping[str, Any], row_h: int, accent: str, pal: VisualPalette) -> None:
    day, mon, wd, wdf = _date_parts(row.get("date"))
    time_text = _plain(row.get("time"))
    tile_h = row_h - 28
    x, y, w = 22, 14, 166
    bar_h = 46
    draw.rounded_rectangle((x, y, x + w, y + tile_h), radius=28, fill=_hex(pal.date_fill), outline=_hex(pal.date_stroke), width=1)
    # Top-only rounded bar.  Drawing a full rounded rectangle and covering its
    # lower corners creates small "ears" on the date tile sides; build the bar
    # from explicit top corner arcs instead.
    r = 28
    draw.rectangle((x, y + r, x + w, y + bar_h), fill=_hex(accent))
    draw.rectangle((x + r, y, x + w - r, y + r), fill=_hex(accent))
    draw.pieslice((x, y, x + 2 * r, y + 2 * r), 180, 270, fill=_hex(accent))
    draw.pieslice((x + w - 2 * r, y, x + w, y + 2 * r), 270, 360, fill=_hex(accent))
    f_mon = _font(_MAIN_FONT, 23 if len(mon) > 4 else 27)
    f_day = _font(_MAIN_FONT, 68 if row_h <= 190 else 70)
    f_meta = _font(_MAIN_FONT, 21 if time_text else 18)
    mon_txt = mon.upper()
    bb = draw.textbbox((0, 0), mon_txt, font=f_mon)
    draw.text((x + w / 2 - (bb[2] - bb[0]) / 2, y + 12 - (bb[1] / 2)), mon_txt, font=f_mon, fill=(255, 255, 255))
    prefix = _plain(row.get("date_prefix"))
    meta = _plain(row.get("daily_hours")) if prefix == "до" else (" · ".join(p for p in (wd, time_text) if p) if time_text else wdf)
    day_bb = draw.textbbox((0, 0), day, font=f_day)
    meta_bb = draw.textbbox((0, 0), meta, font=f_meta)
    day_h, meta_h = day_bb[3] - day_bb[1], meta_bb[3] - meta_bb[1]
    gap = 8 if meta else 0
    top = y + bar_h + (tile_h - bar_h - day_h - gap - meta_h) / 2
    prefix_w = draw.textlength(prefix + " ", font=f_meta) if prefix else 0
    day_x = x + (w - (day_bb[2] - day_bb[0]) - prefix_w) / 2
    if prefix:
        prefix_bb = draw.textbbox((0, 0), prefix, font=f_meta)
        draw.text((day_x, top + day_h - prefix_bb[3]), prefix, font=f_meta, fill=_hex(pal.text))
    draw.text((day_x + prefix_w - day_bb[0], top - day_bb[1]), day, font=f_day, fill=_hex(pal.text))
    if meta:
        draw.text((x + (w - meta_bb[2] + meta_bb[0]) / 2 - meta_bb[0], top + day_h + gap - meta_bb[1]), meta, font=f_meta, fill=_hex(pal.route))


def render_visual_digest_card(
    rows: Sequence[Mapping[str, Any]],
    *,
    issue_id: int,
    card_index: int = 1,
    total_cards: int = 1,
    all_rows: Sequence[Mapping[str, Any]] | None = None,
    palette: VisualPalette | None = None,
) -> bytes:
    """Render one approved 4:5 VK schedule card and return JPEG bytes."""
    page_rows = [dict(r) for r in rows[:VISUAL_DIGEST_CARD_LIMIT]]
    if not page_rows:
        raise ValueError("visual digest card requires at least one row")
    palette = palette or _palette_for_issue(issue_id + card_index - 1)
    img = _gradient((W, H), palette.bg1, palette.bg2).convert("RGBA")
    img.alpha_composite(_alpha_circle_layer((W, H), (700, -62, 1160, 398), palette.halo, 42))
    img.alpha_composite(_alpha_circle_layer((W, H), (-238, 1035, 222, 1495), palette.decor, 24))
    draw = ImageDraw.Draw(img)

    # Header badge and title.
    shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    issue_x, issue_y, issue_w, issue_h = 55, 58, 196, 92
    sd.rounded_rectangle((issue_x, issue_y + 8, issue_x + issue_w, issue_y + issue_h + 8), radius=30, fill=(0, 0, 0, 30))
    shadow = shadow.filter(ImageFilter.GaussianBlur(8))
    img.alpha_composite(shadow)
    draw.rounded_rectangle((issue_x, issue_y, issue_x + issue_w, issue_y + issue_h), radius=30, fill=_hex(palette.issue))
    issue_label_font = _font(_MAIN_FONT, 18)
    issue_label = "ВЫПУСК"
    label_bb = draw.textbbox((0, 0), issue_label, font=issue_label_font)
    # No numero sign: a compact number-only badge reads cleaner in VK preview.
    issue_font = _font(_MAIN_FONT, 54)
    issue_text = f"{int(issue_id)}"
    issue_bb = draw.textbbox((0, 0), issue_text, font=issue_font)
    label_w = label_bb[2] - label_bb[0]
    label_h = label_bb[3] - label_bb[1]
    num_w = issue_bb[2] - issue_bb[0]
    num_h = issue_bb[3] - issue_bb[1]
    group_gap = 3
    group_h = label_h + group_gap + num_h
    group_y = issue_y + (issue_h - group_h) / 2 - 1
    draw.text(
        (issue_x + (issue_w - label_w) / 2 - label_bb[0], group_y - label_bb[1]),
        issue_label,
        font=issue_label_font,
        fill=(255, 255, 255),
    )
    draw.text(
        (issue_x + (issue_w - num_w) / 2 - issue_bb[0], group_y + label_h + group_gap - issue_bb[1]),
        issue_text,
        font=issue_font,
        fill=(255, 255, 255),
    )
    title_font = _font(_MAIN_FONT, 54)
    draw.text((320, 54), "Дайджест", font=title_font, fill=_hex(palette.text))
    draw.text((320, 109), "экскурсий", font=title_font, fill=_hex(palette.text))
    draw.text((322, 178), "куда · с кем · как", font=_font(_MAIN_FONT, 23), fill=_hex(palette.sub))
    # Exact accepted v18 brand asset: lower than the native VK carousel counter,
    # with the original right-leaning italic lockup and stroke weight.
    img.alpha_composite(_brand_lockup(1.0), (640, 120))

    page_period = _period_of(page_rows)
    draw.text((58, 220), page_period, font=_font(_MAIN_FONT, 56), fill=_hex(palette.text))
    # Do not show "1–5 из 15": in VK each carousel card must read as a
    # self-contained mini-digest, not as a fragment of a technical list.
    draw.line((58, 296, 1022, 296), fill=_rgba(palette.divider, 180), width=2)

    expanded = len(page_rows) <= 4
    row_h = 228 if expanded else 174
    gap = 18 if expanded else 12
    if not expanded:
        row_h = 180
    y0 = 318 if expanded else 314
    title_font = _font(_MAIN_FONT, 36 if expanded else 32)
    guide_font = _font(_MAIN_FONT, 24 if expanded else 21)
    route_font = _font(_MAIN_FONT, 23 if expanded else 20)

    for i, row in enumerate(page_rows):
        y = y0 + i * (row_h + gap)
        accent = palette.accents[i % len(palette.accents)]
        # Card shadow
        sh = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        sdraw = ImageDraw.Draw(sh)
        sdraw.rounded_rectangle((42, y + 8, 1038, y + row_h + 8), radius=30, fill=_rgba(palette.shadow, 36))
        sh = sh.filter(ImageFilter.GaussianBlur(9))
        img.alpha_composite(sh)
        draw.rounded_rectangle((42, y, 1038, y + row_h), radius=30, fill=_hex(palette.card), outline=_hex(palette.card_stroke), width=1)
        draw.rounded_rectangle((42, y, 52, y + row_h), radius=5, fill=_hex(accent))
        row_layer = Image.new("RGBA", (996, row_h), (0, 0, 0, 0))
        rd = ImageDraw.Draw(row_layer)
        _draw_date_tile(rd, row, row_h, accent, palette)
        tx = 238
        participants = _shown_medallions(row.get("participant_medallions") or [], registered=bool(row.get("avatar_roles_resolved")))
        av_size = 140
        avatar_step = round(av_size * 0.8)
        av_x = 820 - max(0, len(participants) - 1) * avatar_step
        text_width = min(515, av_x - 42 - tx - 22)
        ty = (18 if len(participants) >= 3 else 24) if len(participants) > 1 else (42 if expanded else 28)
        row_title_font = _font(_MAIN_FONT, 30) if len(participants) >= 3 else title_font
        title_text = _plain(row.get("canonical_title")) or "Экскурсия"
        title_lines = _wrap_by_width(rd, title_text, row_title_font, text_width, 3 if len(participants) >= 3 else 2, use_numero=True)
        for line in title_lines:
            _draw_card_line(rd, (tx, ty), line, row_title_font, _hex(palette.text))
            ty += 34 if len(participants) >= 3 else (40 if expanded else 34)
        guide = _source_name(row)
        if guide:
            row_guide_font = _font(_MAIN_FONT, 20) if len(participants) >= 3 else guide_font
            for name_line in _wrap_by_width(rd, guide, row_guide_font, text_width, max(1, len(participants))):
                rd.text((tx, ty + 2), name_line, font=row_guide_font, fill=_hex(palette.guide))
                ty += 23 if len(participants) >= 3 else (28 if len(participants) > 1 else (34 if expanded else 27))
        place = _place_line(row)
        if place and place.casefold() not in _plain(row.get("canonical_title")).casefold():
            labels = place.split(" · ")
            while len(labels) > 1 and rd.textlength(" · ".join(labels), font=route_font) > text_width:
                labels.pop()
            rd.text((tx, ty + 2), "".join(_wrap_by_width(rd, " · ".join(labels), route_font, text_width, 1)),
                    font=route_font, fill=_hex(palette.route))
            ty += 34 if expanded else 26
        description = _card_description(row) or _focus_line(row)
        description_font = _font(_MAIN_FONT, 21 if expanded else 18)
        line_h = 25 if expanded else 22
        available_lines = max(0, min(2, (row_h - ty - (54 if _seats_text(row) else 12)) // line_h))
        for line in _wrap_by_width(rd, description, description_font, text_width, available_lines, use_numero=True) if description and available_lines else []:
            _draw_card_line(rd, (tx, ty + 2), line, description_font, _hex(palette.route))
            ty += line_h
        seats = _seats_text(row)
        if seats:
            chip_width = max(92, int(rd.textlength(seats, font=_font(_MAIN_FONT, 17))) + 28)
            chip_x = min(654, av_x - 42 - chip_width - 14)
            _draw_chip(rd, seats, chip_x, row_h - (54 if expanded else 52), accent)
        img.alpha_composite(row_layer, (42, y))
        av_y = y + round((row_h - av_size) / 2)
        if participants:
            for idx, person in enumerate(participants):
                px = av_x + idx * avatar_step
                _draw_avatar(img, _medallion_row(person, registered=bool(row.get("avatar_roles_resolved"))), px, av_y, av_size, palette, accent)
        elif not row.get("source_roles_checked"):
            _draw_avatar(img, row, av_x, av_y, av_size, palette, accent)
        modes = _modes_for(row)
        _draw_icon_badges(img, modes, 42 + 920, av_y + (12 if len(modes) > 1 else 76), palette)

    draw.text((58, 1290), "Подробности и запись — в тексте поста ↓", font=_font(_MAIN_FONT, 24), fill=_hex(palette.text))

    rgb = img.convert("RGB")
    buf = io.BytesIO()
    rgb.save(buf, format="JPEG", quality=94, optimize=True)
    return buf.getvalue()


async def ensure_visual_digest_schema(conn: aiosqlite.Connection) -> None:
    cur = await conn.execute("PRAGMA table_info(guide_occurrence)")
    cols = {str(row[1]) for row in await cur.fetchall()}
    if "published_visual_digest_issue_id" not in cols:
        await conn.execute("ALTER TABLE guide_occurrence ADD COLUMN published_visual_digest_issue_id INTEGER")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_guide_occurrence_visual_digest "
        "ON guide_occurrence(digest_eligible, published_visual_digest_issue_id, updated_at)"
    )


async def _enable_row_factory(conn: aiosqlite.Connection) -> None:
    conn.row_factory = aiosqlite.Row


async def _load_transport(conn: aiosqlite.Connection, row: dict[str, Any]) -> None:
    """Resolve direct evidence first; only explicitly reusable claims may cross dates."""
    cur = await conn.execute(
        """
        SELECT c.entity_id, c.fact_key, c.fact_value, c.provenance_json, p.source_url, p.post_date,
               member_post.source_url AS program_source_url
        FROM guide_fact_claim c
        JOIN guide_monitor_post p ON p.id=c.source_post_id
        LEFT JOIN guide_fact_claim member
          ON c.fact_key='transport' AND json_extract(c.provenance_json,'$.program')=member.fact_value
         AND member.entity_kind='occurrence' AND member.entity_id=? AND member.fact_key='program'
         AND substr(member.fact_value,-7)=substr(?,1,7)
        LEFT JOIN guide_monitor_post member_post ON member_post.id=member.source_post_id
        WHERE c.entity_kind='occurrence' AND c.fact_key IN ('transport', 'group_format')
          AND (c.entity_id=? OR member_post.id IS NOT NULL OR
               (c.fact_key='transport' AND json_extract(c.provenance_json, '$.scope')='route'
                AND c.entity_id IN (SELECT id FROM guide_occurrence WHERE template_id=?)))
        ORDER BY (c.entity_id=?) DESC, p.post_date DESC, c.id DESC
        """,
        (row["id"], row.get("date"), row["id"], row.get("template_id"), row["id"]),
    )
    candidates = []
    group_format = None
    for claim in await cur.fetchall():
        if claim["fact_key"] == "transport":
            modes = _parse_json_array(claim["fact_value"])
        else:
            modes = _modes_for({"group_format": claim["fact_value"]})
            if not modes and group_format is None:
                group_format = claim["fact_value"]
        if modes:
            candidates.append((claim, modes))
    if group_format:
        row["group_format"] = group_format
    if candidates:
        first, modes = candidates[0]
        direct = first["entity_id"] == row["id"]
        for claim, other_modes in candidates[1:]:
            if direct and (claim["entity_id"] != row["id"] or claim["post_date"] != first["post_date"]):
                break
            if set(other_modes) != set(modes):
                modes = []
                break
        row["transport"] = {**_safe_json_object(first["provenance_json"]), "modes": modes,
                            "source_url": first["source_url"], "program_source_url": first["program_source_url"]}


_VISUAL_ROW_QUERY = """
        SELECT
            go.*,
            gs.username AS source_username,
            gs.platform AS source_platform,
            gs.title AS source_title,
            gs.source_kind AS source_kind,
            gs.flags_json AS source_flags_json,
            gs.about_text AS source_about_text,
            gs.about_links_json AS source_about_links_json,
            gs.priority_weight AS priority_weight,
            gp.display_name AS guide_profile_display_name,
            gp.marketing_name AS guide_profile_marketing_name,
            gp.summary_short AS guide_profile_summary,
            gp.facts_rollup_json AS guide_profile_facts_rollup_json,
            gdi.media_items_json AS visual_issue_media_items_json
        FROM guide_occurrence go
        LEFT JOIN guide_source gs ON gs.id = go.primary_source_id
        LEFT JOIN guide_profile gp ON gp.id = gs.primary_profile_id
        LEFT JOIN guide_digest_issue gdi ON gdi.id = go.published_visual_digest_issue_id
"""


def _shared_departure_identity(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    def names(row: Mapping[str, Any], key: str) -> set[str]:
        values = row.get(key)
        if values is None:
            values = _parse_json_array(row.get(f"{key}_json"))
        return {_plain(name).casefold() for name in values if _plain(name)}
    guides = names(left, "guide_names")
    return bool(left.get("date") and left["date"] == right.get("date")
                and left.get("city") and left["city"] == right.get("city")
                and guides and guides == names(right, "guide_names")
                and names(left, "organizer_names").intersection(names(right, "organizer_names")))


def _same_visual_departure(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    if not left.get("date") or left["date"] != right.get("date"):
        return False
    # A schedule may use the public route name in its route field, while a
    # detailed announcement uses that very name as the main title.
    title = _plain(left.get("canonical_title")).casefold()
    other = _plain(right.get("canonical_title")).casefold()
    if not title or not other:
        return False
    linked_urls = {p["url"] for p in left["linked_posts"] if p["url"]}
    same_post = any(p["url"] in linked_urls for p in right["linked_posts"])
    guides, other_guides = set(left.get("guide_names") or ()), set(right.get("guide_names") or ())
    same_organizers = set(left.get("organizer_names") or ()) & set(right.get("organizer_names") or ())
    start = _plain(left.get("time")).split("-")[0].strip()
    other_start = _plain(right.get("time")).split("-")[0].strip()
    same_slot = bool(start and start == other_start and same_organizers
                     and (not guides or not other_guides or guides & other_guides))
    if same_post and (_shared_departure_identity(left, right) or same_slot) and (
        title == other or _place_phrase(title).search(other) or _place_phrase(other).search(title)
    ):
        return True
    if _shared_departure_identity(left, right) and (
        title == _plain(right.get("route_summary")).casefold() or other == _plain(left.get("route_summary")).casefold()
    ):
        return True
    # Reposts can have no extracted guide, a different city scope, and different
    # titles. Two identical reservation phones and the same named landmark identify
    # the offer without treating a shared operator alone as a match.
    phones, places = _departure_booking_evidence(left)
    other_phones, other_places = _departure_booking_evidence(right)
    return len(phones & other_phones) >= 2 and bool(places & other_places)


def _departure_booking_evidence(row: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    sources = [str(p.get("text") or "") for p in row.get("linked_posts") or []]
    booking = "\n".join([str(row.get("booking_text") or ""), *(line for text in sources for line in text.splitlines()
                           if re.search(r"брон|телефон|запис", line, re.I))])
    phones = {re.sub(r"\D", "", m) for m in re.findall(r"(?<!\d)(?:\d{2}-\d{2}-\d{2}|\d{3}-\d{3})(?!\d)", booking)}
    route = " ".join(_plain(row.get(key)) for key in ("canonical_title", "route_summary"))
    places = {m.group().casefold() for m in re.finditer(
        r"\b(?i:замок|монастырь|кирха|усадьба|музей|городище|парк)\s+[«“\"]?[А-ЯЁ][А-Яа-яЁё-]{2,}\b", route)}
    return phones, places


def _crosspost_fare(rows: Sequence[Mapping[str, Any]]) -> str:
    """Recover the full labelled tariff of one jointly advertised, dated departure."""
    terms = set()
    for row in rows:
        phones, places = _departure_booking_evidence(row)
        if len(phones) < 2 or not places:
            continue
        for post in row.get("linked_posts") or []:
            text = str(post.get("text") or "")
            try:
                anchor = datetime.fromisoformat(str(post.get("post_date") or row["date"]))
            except ValueError:
                continue
            lines = text.splitlines()
            dates = {day for line in lines if (day := extract_date_iso(line, post_date=anchor))}
            if dates != {row["date"]} or not any(place in text.casefold() for place in places):
                continue
            for line in lines:
                price = re.search(r"(?:^|[.!?]\s+)[^\w\n]*(?:стоимость|цена)(?:\s+(?:участия|поездки))?\s*[:—–-]\s*(\d[^\n]+)", line, re.I)
                if price and re.search(r"руб|₽", price[1], re.I):
                    terms.add(price[1].strip().rstrip("."))
    return terms.pop() if len(terms) == 1 else ""


_FARE_LINE = re.compile(r"^\s*(?:стоимость|цена)(?: участия)?\s*:\s*(\d[^.!?\n]+)", re.I | re.M)
_SUPPLEMENT = re.compile(
    r"(\b(?:дополнительно|отдельно) оплачивается [а-яё-]+)\s*[—–-]\s*"
    r"(\d[\d \u00a0]*)\s*(?:руб(?:лей|ля|ль)?|₽)(?: с человека)?", re.I,
)


def _agreed_base_fare(rows: Sequence[Mapping[str, Any]]) -> str:
    """Remove a disputed supplement only when the two published offers otherwise agree."""
    terms: dict[str, set[str]] = {}
    base_amounts: set[str] = set()
    for row in rows:
        stored = re.match(r"\d[\d\s]*", _plain(row.get("price_line") or row.get("price_text")))
        if stored:
            base_amounts.add("".join(stored[0].split()))
        for post in row.get("linked_posts", []):
            text = str(post.get("text") or "")
            prices = _FARE_LINE.findall(text)
            if len(prices) != 1:
                continue
            price = prices[0].strip()
            base_amounts.add("".join(re.match(r"\d[\d\s]*", price)[0].split()))
            supplement = _SUPPLEMENT.search(price)
            if supplement and supplement.group(2).strip():
                common = _SUPPLEMENT.sub(r"\1", price, count=1)
                terms.setdefault(common, set()).add("".join(supplement.group(2).split()))
    if len(terms) == 1 and len(base_amounts) == 1:
        common, amounts = next(iter(terms.items()))
        if len(amounts) > 1:
            return common
    return ""


def _agreed_fare_source(text: str, agreed_fare: str) -> str:
    def reconcile(match: re.Match[str]) -> str:
        price = match[1].strip()
        if _SUPPLEMENT.sub(r"\1", price, count=1) == agreed_fare:
            return match[0].replace(match[1], agreed_fare)
        return match[0]
    return _FARE_LINE.sub(reconcile, text)


def _conflicting_departure_logistics(rows: Sequence[Mapping[str, Any]], *, agreed_fare: str = "") -> bool:
    times = [_plain(r.get("time")) for r in rows if _plain(r.get("time"))]
    spans = {re.sub(r"\s*[-–—]\s*", "-", t) for t in times}
    starts = {span.split("-")[0] for span in spans}
    fares = {_plain(r.get("price_text")).casefold() for r in rows}
    known_fares = sorted(fares - {"", "по запросу"}, key=len)
    # Preserve the fuller fare ("4000 рублей с человека ...") and a compatible
    # end time. Incompatible prices or starts could describe two separate offers.
    return (len(starts) > 1 or len({t for t in spans if "-" in t}) > 1
            or (not agreed_fare and any(not known_fares[-1].startswith(f + " ") for f in known_fares[:-1])))


def _select_documented_departures(requested: Sequence[dict[str, Any]], peers: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = []
    seen = set()
    for row in requested:
        if row["id"] in seen:
            continue
        # A detailed announcement may connect two source rows through a third
        # row that carries both original post links.
        members = [row]
        member_ids = {row["id"]}
        for member in members:
            for peer in peers:
                if peer["id"] not in member_ids and _same_visual_departure(member, peer):
                    member_ids.add(peer["id"])
                    members.append(peer)
        agreed_fare = _agreed_base_fare(members)
        incompatible = _conflicting_departure_logistics(members, agreed_fare=agreed_fare)
        if incompatible or any(m["id"] in seen for m in members):
            members = [row]
            agreed_fare = _agreed_base_fare(members)
        source = max(members, key=lambda m: (
            bool(m.get("time") and _plain(m.get("price_text"))), len(_plain(m.get("time"))),
            len(_plain(m.get("route_summary"))), -int(m["id"])))
        representative = dict(source)
        if len(members) > 1:
            representative["fact_pack"] = dict(source.get("fact_pack") or {})
            if _plain(price_for_publication(representative.get("price_text"))).casefold() in ("", "по запросу"):
                if tariff := _crosspost_fare(members):
                    representative["price_line"] = representative["price_text"] = representative["fact_pack"]["price_text"] = tariff
            for role in ("guide_names", "organizer_names"):
                names = list(dict.fromkeys(n for m in [source, *members] for n in m.get(role) or []))
                representative[role] = representative["fact_pack"][role] = names
            transport = [m["transport"] for m in members if (m.get("transport") or {}).get("modes")]
            if not representative.get("transport") and len({tuple(t["modes"]) for t in transport}) == 1:
                representative["transport"] = transport[0]
            # Portraits already computed by an older caller belong to only one
            # row. Normal loading resolves portraits after the group is chosen.
            if "participant_medallions" in representative:
                people = {}
                for member in [source, *members]:
                    for person in member.get("participant_medallions") or []:
                        key = (person.get("owner"), person["name"])
                        merged = people.setdefault(key, dict(person))
                        merged["roles"] = list(dict.fromkeys([*merged["roles"], *person["roles"]]))
                representative["participant_medallions"] = list(people.values())
        if len(members) > 1:
            representative["covered_visual_states"] = {str(m["id"]): _visual_item_state(m) for m in members}
        representative["covered_visual_ids"] = sorted(m["id"] for m in members)
        representative["linked_posts"] = [post for member in members for post in member["linked_posts"]]
        if len(members) > 1:
            # A guide's past project may also explicitly co-organize this same departure
            # in another linked announcement. Reconcile after resolving the dated peers.
            for role in ("guide_names", "organizer_names"):
                deferred = [n for m in members for n in (m.get("unconfirmed_source_roles") or {}).get(role, [])]
                if deferred:
                    blocks = (block for member in members
                              for block in _card_source_blocks(member, member["canonical_title"]))
                    names = names_for_source_role(deferred, blocks, organizer=role == "organizer_names",
                                                  require_mention=True)
                    representative[role] = representative["fact_pack"][role] = list(
                        dict.fromkeys([*representative.get(role, []), *names]))
        if agreed_fare:
            representative["price_line"] = representative["price_text"] = agreed_fare
            representative["fact_pack"] = {**(representative.get("fact_pack") or {}), "price_text": agreed_fare}
            # Preserve the originals for provenance while excluding the disputed figure from writer input.
            representative["linked_posts"] = [
                {**post, "writer_text": _agreed_fare_source(post.get("text") or "", agreed_fare)}
                for post in representative["linked_posts"]
            ]
        representative["visual_selection_reason"] = row.get("visual_selection_reason")
        seen.update(representative["covered_visual_ids"])
        selected.append(representative)
    return selected


async def _load_visual_rows(conn: aiosqlite.Connection, records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not records:
        return []
    ids = [int(row["id"]) for row in records]
    params = ",".join("?" for _ in ids)
    cur = await conn.execute(f"""
        SELECT gos.occurrence_id, gmp.id, gmp.text, gmp.post_date, gmp.source_url AS url, gmp.source_url,
               gmp.post_kind, gmp.raw_facts_json
        FROM guide_occurrence_source gos
        JOIN guide_monitor_post gmp ON gmp.id = gos.post_id
        WHERE gos.occurrence_id IN ({params})
        ORDER BY gos.occurrence_id, CASE WHEN gos.role='primary' THEN 0 ELSE 1 END,
                 gmp.post_date DESC, gmp.id DESC
    """, tuple(ids))
    linked: dict[int, list[dict[str, Any]]] = {}
    for post in await cur.fetchall():
        linked.setdefault(post["occurrence_id"], []).append(dict(post))
    histories = await load_materialized_history(conn, records, linked)
    out = []
    for row in records:
        item = dict(row)
        item["guide_names"] = _parse_json_array(item.get("guide_names_json"))
        item["organizer_names"] = _parse_json_array(item.get("organizer_names_json"))
        item["audience_fit"] = _parse_json_array(item.get("audience_fit_json"))
        item["fact_pack"] = _safe_json_object(item.get("fact_pack_json"))
        item["guide_profile_facts"] = _safe_json_object(item.get("guide_profile_facts_rollup_json"))
        for key in (
            "canonical_title",
            "date",
            "time",
            "duration_text",
            "city",
            "meeting_point",
            "route_summary",
            "price_text",
            "booking_text",
            "booking_url",
            "status",
            "seats_text",
            "summary_one_liner",
            "digest_blurb",
            "availability_mode",
            "post_kind",
            "group_format",
        ):
            if not item.get(key) and item["fact_pack"].get(key) is not None:
                item[key] = item["fact_pack"].get(key)
        item["source_flags"] = _safe_json_object(item.get("source_flags_json"))
        state_map = _visual_issue_state_map(item.get("visual_issue_media_items_json"))
        item["published_visual_digest_state"] = state_map.get(str(item.get("id") or ""))
        posts = linked.get(item["id"], [])
        if posts and all(_schedule_attributes_day_to_other_title(post, _plain(item.get("canonical_title")),
                                                                _plain(item.get("date"))) for post in posts):
            continue
        clock = _plain(item.get("time"))
        if posts and clock and all(_opening_has_only_published_time(post, clock) for post in posts):
            item["time"] = item["fact_pack"]["time"] = None
            for key in ("summary_one_liner", "digest_blurb", "main_hook", "main_hook_evidence", "route_summary"):
                if clock in str(item.get(key) or "") or clock in str(item["fact_pack"].get(key) or ""):
                    item.pop(key, None)
                    item["fact_pack"].pop(key, None)
        item["linked_posts"] = posts
        history = histories[item["id"]]
        item["earlier_route_posts"] = history["earlier"]
        item["route_history"] = {key: history[key] for key in ("kind", "dated_departures", "description_sufficient")}
        primary = posts[0] if posts else {}
        item["dedup_source_text"] = collapse_ws(primary.get("text") or "")
        item["dedup_source_url"] = primary.get("url")
        item["source_post_url"] = _source_post_url(item["fact_pack"].get("source_post_url"),
                                                  item["dedup_source_url"] or item.get("channel_url"))
        await _load_transport(conn, item)
        _apply_published_facts(item, posts)
        out.append(item)
    earliest_check = datetime.now(timezone.utc) - timedelta(days=14)
    sparse = [item for item in out if needs_background(histories[item["id"]])]
    route_background = {}
    for item in sparse:
        website = histories[item["id"]].get("website")
        if website and datetime.fromisoformat(website["retrieved_at"]) >= earliest_check:
            route_background[item["id"]] = website
    route_background.update(await load_cached_background(conn, sparse))
    for item in out:
        if item["id"] in route_background:
            item["route_background"] = route_background[item["id"]]
    return out


async def _include_documented_departures(conn: aiosqlite.Connection, requested: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not requested:
        return []
    # Older issues may contain only the programme row even though a detailed
    # announcement for the same departure has since been imported.
    days = sorted({row["date"] for row in requested if row.get("date")})
    if not days:
        return requested
    cur = await conn.execute(_VISUAL_ROW_QUERY + f"""
        WHERE go.date IN ({",".join("?" for _ in days)}) AND go.digest_eligible = 1
          AND COALESCE(go.status, 'scheduled') != 'cancelled'
    """, tuple(days))
    by_id = {row["id"]: row for row in requested}
    # A secondary VK announcement need not contain the same extracted city or
    # personal name. Hydrate its actual dated source evidence before comparison.
    extras = [dict(record) for record in await cur.fetchall() if record["id"] not in by_id]
    peers = [*requested, *await _load_visual_rows(conn, extras)]
    return _select_documented_departures(requested, peers)


async def _fetch_visual_candidates(conn: aiosqlite.Connection, *, limit: int, as_of: date | None = None) -> list[dict[str, Any]]:
    if as_of is None:
        await ensure_visual_digest_schema(conn)
    sql = _VISUAL_ROW_QUERY + """
        WHERE go.digest_eligible = 1
          AND go.date IS NOT NULL
          AND go.date GLOB '????-??-??'
          AND go.date >= ?
          AND go.date <= ?
          AND COALESCE(go.status, 'scheduled') != 'cancelled'
          AND (
            go.published_visual_digest_issue_id IS NULL
            OR datetime(go.updated_at) > datetime(COALESCE(gdi.published_at, gdi.created_at, '1970-01-01 00:00:00'))
          )
        ORDER BY go.date ASC, COALESCE(go.time, '99:99') ASC, go.updated_at DESC
        LIMIT ?
    """
    cur = await conn.execute(sql, (_visual_start_iso(as_of), _future_cutoff_iso(as_of), int(limit)))
    candidates = []
    for item in await _load_visual_rows(conn, await cur.fetchall()):
        reason = _visual_selection_reason(item)
        if reason:
            item["visual_selection_reason"] = reason
            candidates.append(item)
    return await _include_documented_departures(conn, candidates)


def _is_tg_post_url(url: str | None) -> bool:
    return parse_tg_post_url(_plain(url)) is not None


def _source_post_url(payload_url: Any, fallback_url: Any) -> str | None:
    primary = _plain(payload_url)
    fallback = _plain(fallback_url)
    if _is_tg_post_url(primary):
        return primary
    if _is_tg_post_url(fallback):
        return fallback
    return primary or fallback or None


async def select_visual_digest_rows(db: Database, *, max_items: int = VISUAL_DIGEST_CARD_LIMIT,
                                    as_of: date | None = None, previous_ids: Sequence[int] = ()) -> list[dict[str, Any]]:
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
        raw_limit = max(int(max_items) * 4, 48)
        if as_of is None:
            rows = await _fetch_visual_candidates(conn, limit=raw_limit)
        else:
            rows = await _fetch_visual_candidates(conn, limit=raw_limit + len(previous_ids), as_of=as_of)
        candidates = {r["id"] for r in rows}
        if previous_ids and rows:
            # Compare today's candidates with yesterday's departures using the
            # normal deduplicator, even when a prior row has since been marked.
            previous = await _fetch_rows_by_ids(conn, previous_ids)
            previous_ids = tuple(set(previous_ids).union(
                i for row in previous for i in row.get("covered_visual_ids", [row["id"]])))
            rows.extend(row for row in previous if row["id"] not in candidates)
    if not rows:
        return []
    dedup = await deduplicate_occurrence_rows(rows, family=VISUAL_DIGEST_FAMILY, limit=max(len(rows), int(max_items)))
    display_rows = [dict(row) for row in dedup.display_rows]
    # The ordinary deduplicator may choose among candidates more broadly. Keep
    # distinct rows when a single apparent route has conflicting published starts.
    by_id = {r["id"]: r for r in rows}
    covered = {}
    for canonical_id, member_ids in dedup.coverage_by_display_id.items():
        group = [by_id[i] for i in member_ids]
        if len(group) > 1 and _conflicting_departure_logistics(group):
            display_rows.extend(dict(r) for r in group if r["id"] != canonical_id)
        elif as_of is not None:
            covered[canonical_id] = sorted({i for r in group for i in r.get("covered_visual_ids", [r["id"]])})
    if as_of is not None:
        shown = set(previous_ids)
        remaining = []
        for row in display_rows:
            ids = covered.get(row["id"], row.get("covered_visual_ids", [row["id"]]))
            if row["id"] in candidates and not shown.intersection(ids):
                row["covered_visual_ids"] = ids
                remaining.append(row)
        display_rows = remaining
    display_rows.sort(key=lambda r: (str(r.get("date") or ""), str(r.get("time") or "99:99"), int(r.get("id") or 0)))
    return display_rows[: int(max_items)]


async def build_visual_digest_issue(
    db: Database,
    *,
    max_cards: int = VISUAL_DIGEST_MAX_CARDS_DEFAULT,
    card_limit: int = VISUAL_DIGEST_CARD_LIMIT,
    run_id: int | None = None,
) -> dict[str, Any]:
    max_items = max(1, min(int(max_cards), 10)) * max(1, int(card_limit))
    rows = await select_visual_digest_rows(db, max_items=max_items)
    from source_avatars import prepare_guide_avatars
    await prepare_guide_avatars(rows, db)
    media_payload = {
        "kind": "visual_schedule_card",
        "card_limit": int(card_limit),
        "item_states": _visual_publication_states(rows),
        "selection_reasons": {str(int(row["id"])): _plain(row.get("visual_selection_reason")) for row in rows if int(row.get("id") or 0) > 0},
    }
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
        await ensure_visual_digest_schema(conn)
        cur = await conn.execute(
            """
            INSERT INTO guide_digest_issue(family, status, target_chat, title, text, items_json, media_items_json, run_id)
            VALUES(?, 'preview', ?, ?, ?, ?, ?, ?)
            """,
            (
                VISUAL_DIGEST_FAMILY,
                VISUAL_DIGEST_VK_TARGET,
                f"Дайджест экскурсий · {len(rows)}" if rows else "Дайджест экскурсий · пусто",
                "",
                _json_dump([int(row["id"]) for row in rows if int(row.get("id") or 0) > 0]),
                _json_dump(media_payload),
                int(run_id) if run_id is not None else None,
            ),
        )
        issue_id = int(cur.lastrowid or 0)
        if not rows:
            await conn.execute("UPDATE guide_digest_issue SET status='empty' WHERE id=?", (issue_id,))
        await conn.commit()
    return {"issue_id": issue_id, "family": VISUAL_DIGEST_FAMILY, "items": rows, "published": False, "reason": "no_items" if not rows else None}


async def _fetch_rows_by_ids(conn: aiosqlite.Connection, occurrence_ids: Sequence[int]) -> list[dict[str, Any]]:
    ids = [int(x) for x in occurrence_ids if int(x or 0) > 0]
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    cur = await conn.execute(_VISUAL_ROW_QUERY + f" WHERE go.id IN ({placeholders})", tuple(ids))
    by_id = {row["id"]: row for row in await _load_visual_rows(conn, await cur.fetchall())}
    return await _include_documented_departures(conn, [by_id[i] for i in ids if i in by_id])


def _issue_occurrence_ids(items_json: Any) -> list[int]:
    data = _json_load(items_json, [])
    if not isinstance(data, list):
        return []
    out: list[int] = []
    for item in data:
        try:
            value = int(item)
        except Exception:
            continue
        if value > 0:
            out.append(value)
    return out


async def load_visual_digest_issue(db: Database, issue_id: int | None = None, *, selection_only: bool = False) -> dict[str, Any] | None:
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
        if not selection_only:
            await ensure_visual_digest_schema(conn)
        if issue_id is not None:
            cur = await conn.execute(
                "SELECT * FROM guide_digest_issue WHERE id=? AND family=?",
                (int(issue_id), VISUAL_DIGEST_FAMILY),
            )
        else:
            cur = await conn.execute(
                "SELECT * FROM guide_digest_issue WHERE family=? AND status IN ('preview','empty') ORDER BY id DESC LIMIT 1",
                (VISUAL_DIGEST_FAMILY,),
            )
        issue = await cur.fetchone()
        if not issue:
            return None
        ids = _issue_occurrence_ids(issue["items_json"])
        rows = await _fetch_rows_by_ids(conn, ids)
    payload = dict(issue)
    if not selection_only:
        from source_avatars import prepare_guide_avatars
        await prepare_guide_avatars(rows, db)
    payload["items"] = rows
    return payload


def chunk_visual_rows(rows: Sequence[Mapping[str, Any]], *, card_limit: int = VISUAL_DIGEST_CARD_LIMIT) -> list[list[dict[str, Any]]]:
    items = [dict(r) for r in rows]
    return [items[i : i + int(card_limit)] for i in range(0, len(items), int(card_limit))]


def render_visual_digest_cards(rows: Sequence[Mapping[str, Any]], *, issue_id: int) -> list[bytes]:
    chunks = chunk_visual_rows(rows)
    total = len(chunks)
    cards: list[bytes] = []
    for idx, chunk in enumerate(chunks, start=1):
        cards.append(render_visual_digest_card(chunk, issue_id=issue_id, card_index=idx, total_cards=total, all_rows=rows))
    return cards


def render_visual_digest_story_image(card_jpeg: bytes) -> bytes:
    """Prepare the card for VK Stories without shrinking the schedule text.

    The schedule card is already 1080px wide.  For story readability we keep it
    at native width and place it inside a 1080×1920 canvas below the VK top UI
    chrome instead of scaling it down to fit a full-bleed story.
    """

    card = Image.open(io.BytesIO(card_jpeg)).convert("RGBA")
    if card.width != W:
        card = card.resize((W, int(card.height * (W / max(1, card.width)))), Image.Resampling.LANCZOS)
    canvas = _gradient((1080, 1920), "#f7fbff", "#fff4e9").convert("RGBA")
    canvas.alpha_composite(_alpha_circle_layer((1080, 1920), (632, -140, 1210, 438), "#f1c9b8", 38))
    canvas.alpha_composite(_alpha_circle_layer((1080, 1920), (-280, 1360, 260, 1960), "#b9dceb", 26))
    y = 165
    shadow = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rounded_rectangle((22, y + 16, 1058, y + card.height + 16), radius=34, fill=(0, 0, 0, 42))
    shadow = shadow.filter(ImageFilter.GaussianBlur(18))
    canvas.alpha_composite(shadow)
    canvas.alpha_composite(card, (0, y))
    draw = ImageDraw.Draw(canvas)
    footer = "Подробности и запись — по ссылке в истории"
    f = _font(_MAIN_FONT, 34)
    bb = draw.textbbox((0, 0), footer, font=f)
    draw.text((540 - (bb[2] - bb[0]) / 2, 1605), footer, font=f, fill=(28, 42, 48))
    rgb = canvas.convert("RGB")
    buf = io.BytesIO()
    rgb.save(buf, format="JPEG", quality=94, optimize=True)
    return buf.getvalue()


def _tg_public_target_key(target: str) -> str:
    return f"tg:{collapse_ws(target)}:visual"


async def _update_visual_issue_publication_state(
    db: Database,
    *,
    issue_id: int,
    target_chat: str,
    text: str | None,
    targets_update: Mapping[str, Mapping[str, Any]],
    occurrence_ids: Sequence[int],
    status: str = "published",
) -> None:
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
        await ensure_visual_digest_schema(conn)
        cur = await conn.execute("SELECT published_targets_json FROM guide_digest_issue WHERE id=?", (int(issue_id),))
        current = await cur.fetchone()
        targets_raw = _json_load(current["published_targets_json"] if current else None, {})
        targets = dict(targets_raw) if isinstance(targets_raw, Mapping) else {}
        for key, payload in targets_update.items():
            targets[str(key)] = dict(payload)
        primary_payload = next(iter(targets_update.values()), {})
        primary_message_ids = [int(v) for v in (primary_payload.get("message_ids") or []) if str(v).strip()]
        await conn.execute(
            """
            UPDATE guide_digest_issue
            SET status=?, target_chat=?, text=COALESCE(?, text), published_at=CURRENT_TIMESTAMP,
                published_message_ids_json=?, published_targets_json=?
            WHERE id=?
            """,
            (
                status,
                target_chat,
                text,
                _json_dump(primary_message_ids),
                _json_dump(targets),
                int(issue_id),
            ),
        )
        ids = [int(x) for x in occurrence_ids if int(x or 0) > 0]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            await conn.execute(
                f"UPDATE guide_occurrence SET published_visual_digest_issue_id=? WHERE id IN ({placeholders})",
                (int(issue_id), *ids),
            )
        await conn.commit()


async def publish_visual_digest_to_telegram(
    db: Database,
    bot: Any | None,
    *,
    issue_id: int | None = None,
    max_cards: int = 1,
    target_chats: str | Sequence[str] | None = None,
) -> dict[str, Any]:
    if bot is None:
        return {"published": False, "reason": "missing_bot", "family": VISUAL_DIGEST_FAMILY}
    if issue_id is None:
        built = await build_visual_digest_issue(db, max_cards=max_cards)
        issue_id = int(built["issue_id"])
        rows = list(built.get("items") or [])
    else:
        issue = await load_visual_digest_issue(db, issue_id)
        if not issue:
            return {"published": False, "reason": "no_issue", "issue_id": issue_id, "family": VISUAL_DIGEST_FAMILY}
        rows = list(issue.get("items") or [])
    rows = rows[: max(1, min(int(max_cards), 1)) * VISUAL_DIGEST_CARD_LIMIT]
    if not rows:
        return {"published": False, "reason": "no_items", "issue_id": int(issue_id), "family": VISUAL_DIGEST_FAMILY}

    targets = list(_parse_target_chats(target_chats if target_chats is not None else VISUAL_DIGEST_TG_TARGET_CHATS))
    card = render_visual_digest_cards(rows, issue_id=int(issue_id))[0]
    from .max_delivery import run_legacy_delivery
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT published_targets_json FROM guide_digest_issue WHERE id=?", (int(issue_id),))
        saved = await cur.fetchone()
        existing_targets = _json_load(saved[0] if saved else None, {})
    occurrence_ids = list(dict.fromkeys(i for row in rows for i in row.get("covered_visual_ids", [row["id"]])))
    published_targets: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    primary_caption = ""
    for target in targets:
        caption = await build_visual_digest_telegram_text(rows, issue_id=int(issue_id), target_chat=target)
        if not primary_caption:
            primary_caption = caption
        target_key = _tg_public_target_key(target)
        if (existing_targets.get(target_key) or {}).get("message_ids"):
            published_targets[target_key] = existing_targets[target_key]
            continue
        try:
            async def send_one():
                upload = BufferedInputFile(card, filename=f"guide_visual_digest_{int(issue_id)}.jpg")
                msg = await bot.send_photo(target, upload, caption=caption, parse_mode="HTML")
                message_id = int(getattr(msg, "message_id", 0) or 0)
                return {"published": bool(message_id), "message_ids": [message_id] if message_id else []}
            delivered = await run_legacy_delivery(db, issue_id=int(issue_id), provider="telegram", target_id=target, send=send_one)
            if not delivered.get("published"):
                errors[target] = str(delivered.get("reason") or delivered.get("state"))
                continue
            message_id = int(delivered["message_ids"][0])
            published_targets[target_key] = {
                "message_ids": [message_id] if message_id else [],
                "text_message_ids": [],
                "media_message_ids": [message_id] if message_id else [],
                "post_urls": [],
                "target_chat": target,
                "transport": "telegram_photo",
                "attachments_count": 1,
                "cards_count": 1,
            }
            await _update_visual_issue_publication_state(db, issue_id=int(issue_id), target_chat=target,
                text=caption, targets_update={target_key: published_targets[target_key]},
                occurrence_ids=occurrence_ids, status="partial")
        except Exception as exc:
            logger.exception("guide_visual_digest_tg_publish_failed issue_id=%s target=%s", issue_id, target)
            errors[target] = str(exc) or type(exc).__name__
    if published_targets and any(existing_targets.get(key) != value for key, value in published_targets.items()):
        await _update_visual_issue_publication_state(
            db,
            issue_id=int(issue_id),
            target_chat=targets[0],
            text=primary_caption,
            targets_update=published_targets,
            occurrence_ids=occurrence_ids,
            status="partial" if errors else "published",
        )
    return {
        "published": bool(published_targets),
        "reason": "partial_failed" if published_targets and errors else ("send_failed" if errors else None),
        "issue_id": int(issue_id),
        "family": VISUAL_DIGEST_FAMILY,
        "target_chats": targets,
        "published_targets": published_targets,
        "message_ids": [mid for payload in published_targets.values() for mid in (payload.get("message_ids") or [])],
        "errors": errors,
        "text": primary_caption,
        "occurrence_ids": occurrence_ids,
    }


def _is_vk_url(url: str | None) -> bool:
    raw = _plain(url).lower()
    return bool(re.match(r"^(?:https?://)?(?:m\.)?vk\.(?:com|ru)/", raw))


def _vk_wall_ref(url: str | None) -> tuple[int, int] | None:
    raw = _plain(url)
    match = re.search(r"(?:vk\.(?:com|ru)/)?wall(-?\d+)_(\d+)", raw, flags=re.I)
    if not match:
        return None
    try:
        return int(match.group(1)), int(match.group(2))
    except Exception:
        return None


def _is_empty_booking_text(value: str | None) -> bool:
    raw = _plain(value).lower().replace("ё", "е")
    return raw in {
        "",
        "not specified",
        "не указано",
        "нет",
        "n/a",
        "none",
    }


def _looks_preliminary_schedule_source(row: Mapping[str, Any]) -> bool:
    source_text = _plain(row.get("dedup_source_text")).lower().replace("ё", "е")
    if not source_text:
        return False
    return bool(
        re.search(r"предварительн\w*\s+расписан", source_text)
        or (
            "расписан" in source_text
            and re.search(r"анонс\w*\s+.*кажд", source_text)
        )
    )


def _visual_booking_url(row: Mapping[str, Any]) -> str:
    """Return the viewer-facing booking URL after visual-digest safety checks.

    Some guide sources publish a fresh multi-event schedule where each repeated
    route title points to an old detailed wall post.  Those links are useful
    route context, but they are unsafe as primary booking/details links for the
    future occurrence.  In that shape the current schedule post is the safer
    public source until a fresh occurrence-specific booking link appears.
    """

    booking_url = _plain(row.get("booking_url"))
    if not booking_url:
        return ""
    # Bare Telegram usernames cannot contain a dot. Keep post, invite and deep links.
    if re.fullmatch(r"(?:https?://)?(?:t\.me|telegram\.me)/[a-z0-9_]+(?:\.[a-z0-9_]+)+/?(?:[?#].*)?",
                    booking_url, re.I):
        return ""
    source = _plain(row.get("source_post_url") or row.get("channel_url"))
    booking_ref = _vk_wall_ref(booking_url)
    source_ref = _vk_wall_ref(source)
    if (
        booking_ref
        and source_ref
        and booking_ref[0] == source_ref[0]
        and booking_ref[1] != source_ref[1]
        and _plain(row.get("post_kind") or _fact(row, "post_kind")) == "announce_multi"
        and _is_empty_booking_text(_plain(row.get("booking_text") or row.get("booking_line")))
        and _looks_preliminary_schedule_source(row)
    ):
        return ""
    return booking_url


def _is_phoneish(value: str | None) -> bool:
    raw = _plain(value).lower()
    if raw.startswith("tel:"):
        return True
    digits = re.sub(r"\D", "", raw)
    return len(digits) in {10, 11} and (digits.startswith("7") or digits.startswith("8") or len(digits) == 10)


def _normalize_phone(value: str | None) -> str | None:
    raw = _plain(value)
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("7"):
        return format_tel_link_for_display(f"tel:+{digits}") or f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
    return raw if raw.startswith("+") else None


def _telegram_phone_auto_link_text(value: str | None) -> str | None:
    raw = _plain(value)
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("7"):
        return f"+{digits}"
    return raw if raw.startswith("+") else None


def _ensure_https(url: str) -> str:
    raw = _plain(url)
    if not raw:
        return ""
    canonical_tg = canonicalize_tg_url(raw)
    if canonical_tg:
        return canonical_tg
    if raw.lower().startswith(("http://", "https://", "tel:")):
        return raw
    if re.match(r"^(?:t\.me|telegram\.me|vk\.(?:com|ru)|www\.)/", raw, flags=re.I):
        return "https://" + raw
    return raw


def _vk_link(label: str, url: str) -> str:
    safe_label = label.replace("]", ")").replace("|", "—")
    return f"[{_ensure_https(url)}|{safe_label}]"


async def _shorten_external_for_vk(
    url: str | None,
    *,
    db: Database | None,
    bot: Any | None,
    vk_api_fn: Callable[..., Awaitable[Any]] | None,
) -> str | None:
    raw = _ensure_https(_plain(url))
    if not raw:
        return None
    if _is_phoneish(raw):
        return _normalize_phone(raw)
    if _is_vk_url(raw):
        return raw
    if raw.lower().startswith(("vk.cc/", "https://vk.cc/", "http://vk.cc/")):
        return re.sub(r"^https?://", "", raw, flags=re.I)
    if not raw.lower().startswith(("http://", "https://")):
        return raw
    if vk_api_fn is None:
        return raw
    try:
        response = await vk_api_fn("utils.getShortLink", {"url": raw}, db, bot)
    except Exception:
        logger.warning("guide_visual_digest_shortlink_failed url=%s", raw, exc_info=True)
        return raw
    payload = response.get("response", response) if isinstance(response, dict) else response
    short_url = ""
    if isinstance(payload, Mapping):
        short_url = _plain(payload.get("short_url"))
        key = _plain(payload.get("key"))
        if not short_url and key:
            short_url = f"https://vk.cc/{key}"
    return re.sub(r"^https?://", "", short_url, flags=re.I) if short_url else raw


def _primary_link(row: Mapping[str, Any]) -> tuple[str | None, bool]:
    # Return (link_or_phone, is_phone). Booking is the action link; source post is fallback.
    booking_url = _visual_booking_url(row)
    booking_text = _plain(row.get("booking_text") or row.get("booking_line"))
    if booking_url:
        return booking_url, _is_phoneish(booking_url)
    if booking_text and _is_phoneish(booking_text):
        return booking_text, True
    source = _plain(row.get("source_post_url") or row.get("channel_url"))
    if source:
        return source, False
    if booking_text and re.search(
        r"https?://|t\.me|telegram\.me|vk\.com|vk\.ru", booking_text, re.I
    ):
        return booking_text, False
    return None, False


_HASHTAG_LOCATIONS: tuple[str, ...] = (
    "Калининград",
    "Зеленоградск",
    "Балтийск",
    "Светлогорск",
    "Пионерский",
    "Янтарный",
    "Черняховск",
    "Гвардейск",
    "Гусев",
    "Советск",
    "Неман",
    "Полесск",
    "Багратионовск",
    "Ладушкин",
    "Мамоново",
    "Правдинск",
    "Железнодорожный",
    "Гурьевск",
    "Светлый",
    "Приморск",
    "Куршская коса",
    "Балтийская коса",
    "Амалиенау",
)


def _hashtag_token(label: str) -> str:
    parts = re.findall(r"[0-9A-Za-zА-Яа-яЁё]+", label)
    if not parts:
        return ""
    return "#" + "".join(part[:1].upper() + part[1:] for part in parts)


def _digest_hashtags(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    tags: list[str] = ["#экскурсия", "#Калининград", "#УхтыКалининград"]
    blob_parts: list[str] = []
    for row in rows:
        blob_parts.extend(
            [
                _plain(row.get("canonical_title")),
                _plain(row.get("city")),
                _plain(row.get("route_summary")),
                _plain(row.get("meeting_point")),
                _route_raw(row),
                _place_line(row),
            ]
        )
    blob = " ".join(part for part in blob_parts if part).lower().replace("ё", "е")
    seen = {tag.lower().replace("ё", "е") for tag in tags}
    for name in _HASHTAG_LOCATIONS:
        normalized = name.lower().replace("ё", "е")
        if normalized not in blob:
            continue
        tag = _hashtag_token(name)
        key = tag.lower().replace("ё", "е")
        if tag and key not in seen:
            tags.append(tag)
            seen.add(key)
        if len(tags) >= 9:
            break
    return tags


def _tg_link(label: str, url: str) -> str:
    href = html.escape(_ensure_https(url), quote=True)
    text = html.escape(label, quote=False)
    return f'<a href="{href}">{text}</a>'


def _telegram_target_public_url(target_chat: str | int | None) -> str | None:
    raw = collapse_ws("" if target_chat is None else str(target_chat)).rstrip("/")
    if not raw:
        return None
    if raw.startswith("@") and len(raw) > 1:
        return f"https://t.me/{raw[1:]}"
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", raw):
        return f"https://t.me/{raw}"
    match = re.fullmatch(
        r"https?://(?:t\.me|telegram\.me)/([A-Za-z0-9_]+)", raw, flags=re.I
    )
    if match:
        return f"https://t.me/{match.group(1)}"
    return None


def _visual_digest_telegram_footer(target_chat: str | int | None = None) -> str:
    subscribe_url = (
        _telegram_target_public_url(target_chat)
        or _telegram_target_public_url(VISUAL_DIGEST_TG_TARGET_CHATS[0] if VISUAL_DIGEST_TG_TARGET_CHATS else None)
        or "https://t.me/wheretogo39"
    )
    return " · ".join(
        [
            _tg_link("Подписаться", subscribe_url),
            _tg_link("Max", VISUAL_DIGEST_TG_MAX_URL),
            _tg_link("Вконтакте", VISUAL_DIGEST_TG_VK_URL),
        ]
    )


async def build_visual_digest_telegram_text(
    rows: Sequence[Mapping[str, Any]],
    *,
    issue_id: int,
    target_chat: str | int | None = None,
    include_footer: bool = True,
) -> str:
    items = [dict(r) for r in rows]
    period = _period_of(items)
    lines = [
        f"<b>Дайджест экскурсий №{int(issue_id)}</b>",
        "",
        html.escape(
            f"Коротко: {len(items)} будущих экскурсий" + (f" на {period}" if period else "") + ". Смотрите карточку, детали и запись — ниже.",
            quote=False,
        ),
        "",
    ]
    for idx, row in enumerate(items, start=1):
        title = _plain(row.get("canonical_title")) or "Экскурсия"
        link, is_phone = _primary_link(row)
        if link and is_phone:
            source_link = _plain(row.get("source_post_url") or row.get("channel_url"))
            if source_link:
                line = f"{idx}. {_tg_link(title, source_link)}"
            else:
                display = _telegram_phone_auto_link_text(link) or _plain(link)
                line = f"{idx}. {html.escape(title, quote=False)} — {html.escape(display, quote=False)}"
        elif link:
            line = f"{idx}. {_tg_link(title, link)}"
        else:
            line = f"{idx}. {html.escape(title, quote=False)}"
        lines.append(line)
    footer_tail = ["", " ".join(_digest_hashtags(items))]
    if include_footer:
        footer_tail.extend(["", _visual_digest_telegram_footer(target_chat)])
    lines.extend(footer_tail)
    text = "\n".join(lines).strip()
    if len(text) <= 1024:
        return text
    # Caption hard-limit guard: keep linked titles, hashtags and the social footer; shorten only intro/overflow.
    compact_lines = [lines[0], ""]
    for idx, row in enumerate(items, start=1):
        title = _fit(_plain(row.get("canonical_title")) or "Экскурсия", 44)
        link, is_phone = _primary_link(row)
        if link and not is_phone:
            compact_lines.append(f"{idx}. {_tg_link(title, link)}")
        elif link:
            source_link = _plain(row.get("source_post_url") or row.get("channel_url"))
            if source_link:
                compact_lines.append(f"{idx}. {_tg_link(title, source_link)}")
            else:
                display = _telegram_phone_auto_link_text(link) or _plain(link)
                compact_lines.append(f"{idx}. {html.escape(title, quote=False)} — {html.escape(display, quote=False)}")
        else:
            compact_lines.append(f"{idx}. {html.escape(title, quote=False)}")
    compact_lines.extend(footer_tail)
    footer_len = len(footer_tail)
    while len("\n".join(compact_lines).strip()) > 1024 and len(compact_lines) > footer_len + 2:
        tail = compact_lines[-footer_len:]
        body = compact_lines[:-footer_len]
        body.pop()
        compact_lines = body + tail
    return "\n".join(compact_lines).strip()


_VK_MOVEMENT = {"walk": "пешком", "bus": "автобус", "tram": "трамвай", "boat": "по воде"}


def _vk_narrative_payload(items: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], dict[int, tuple[int, str]]]:
    """Writer input for the whole edition plus {person number: (personal VK id, name)} for confirmed guides."""
    people: dict[str, dict[str, Any]] = {}
    mention_ids: dict[int, tuple[int, str]] = {}
    sources: dict[tuple[str, str], dict[str, Any]] = {}
    earlier_posts: dict[str, dict[str, Any]] = {}
    excursions: list[dict[str, Any]] = []
    for row in items:
        row_id = int(row.get("id") or 0)
        participants = row.get("participant_medallions")
        if not isinstance(participants, list):
            participants = [{"name": name, "roles": [role]} for field, role in
                            (("guide_names", "Гид"), ("organizer_names", "Организатор"))
                            for name in ((row.get("fact_pack") or {}).get(field, row.get(field)) or [])]
        guides = []
        organizers = []
        for person in participants:
            roles = [role for role in person.get("roles") or [] if role != "Источник"]
            name = _plain(person.get("name"))
            if not roles or not name:
                continue
            if "Гид" in roles:
                guides.append(name)
            if "Организатор" in roles:
                organizers.append(name)
            owner = int(person.get("owner") or 0)
            entry = people.setdefault(f"vk:{owner}" if owner else name.casefold(),
                                      {"n": len(people) + 1, "name": name, "roles": [], "excursions": []})
            entry["roles"] = sorted({*entry["roles"], *roles})
            entry["excursions"].append(row_id)
            if owner > 0 and "Гид" in roles:
                mention_ids[entry["n"]] = (owner, name)
        posts = row.get("linked_posts")
        if posts is None:
            posts = [{"url": row.get("dedup_source_url", row.get("source_post_url")),
                      "text": row.get("dedup_source_text")}]
        for post in posts:
            source_text = (post.get("writer_text") or post.get("text") or "").strip()
            if not source_text:
                continue
            source_url = _plain(post.get("url"))
            source = sources.setdefault((source_url, source_text), {
                "url": source_url, "text": source_text, "excursions": [],
            })
            if post.get("post_date"):
                source["post_date"] = _plain(post["post_date"])[:10]
            if row_id not in source["excursions"]:
                source["excursions"].append(row_id)
        own_texts = {(post.get("text") or "").strip() for post in posts}
        for post in row.get("earlier_route_posts") or []:
            text = (post.get("text") or "").strip()
            if not text or text in own_texts:
                continue
            earlier = earlier_posts.setdefault(text, {
                "url": _plain(post.get("url")), "post_date": _plain(post.get("post_date"))[:10],
                "text": text, "excursions": [],
            })
            if row_id not in earlier["excursions"]:
                earlier["excursions"].append(row_id)
        try:
            weekday = RU_WEEK_FULL[date.fromisoformat(_plain(row.get("date"))).weekday()]
        except ValueError:
            weekday = ""
        facts = {key: value for key in ("summary_one_liner", "digest_blurb", "route_summary", "meeting_point",
                                        "duration_text", "group_format", "audience_fit", "main_hook", "main_hook_evidence")
                 if (value := _fact(row, key) or row.get(key)) and (not isinstance(value, str) or _plain(value))}
        caption = _card_description(row)
        if caption:
            facts["card_caption"] = caption
        profile = {"kind": _plain(row.get("source_kind")), "title": _plain(row.get("source_title")),
                   "about": _plain(row.get("source_about_text"))[:600],
                   "summary": _plain(row.get("guide_profile_summary")), "facts": row.get("guide_profile_facts") or {}}
        if row.get("source_roles_checked") and row.get("source_kind") == "guide_personal":
            profile_names = {_plain(row.get(k)).casefold() for k in
                             ("source_title", "guide_profile_display_name", "guide_profile_marketing_name")}
            profile_owner = (row.get("guide_profile_facts") or {}).get("vk_user_id")
            matched = any(p.get("roles") and (
                (profile_owner and str(p.get("owner")) == str(profile_owner))
                or _plain(p.get("name")).casefold() in profile_names) for p in participants)
            if not matched:
                profile = {}
        excursions.append({
            "occurrence_id": row_id, "title": _plain(row.get("canonical_title")),
            "date": _plain(row.get("date")), "time": _plain(row.get("time")), "weekday": weekday,
            "city": _plain(row.get("city")),
            "movement": [_VK_MOVEMENT[mode] for mode in _modes_for(row) if mode in _VK_MOVEMENT],
            "guides": guides, "organizers": organizers,
            "facts": facts, "source_profile": {key: value for key, value in profile.items() if value},
            **({"route_background": row["route_background"]} if row.get("route_background") else {}),
        })
    payload = {"excursions": excursions, "people": list(people.values()), "sources": list(sources.values())}
    if earlier_posts:
        payload["earlier_announcements"] = list(earlier_posts.values())
    return payload, mention_ids


def _vk_schedule_lines(details: Sequence[tuple[str, str, str, str]]) -> list[str]:
    """Combine compatible descriptions of one start; keep conflicting times and prices visible."""
    slots: list[tuple[str, str, str, str]] = []
    for when, price, contact, destination in details:
        for i, (prev_when, prev_price, prev_contact, prev_dest) in enumerate(slots):
            if not when or not destination or destination != prev_dest:
                continue
            times = sorted((prev_when, when), key=len)
            prices = sorted((prev_price, price), key=len)
            same_start = times[0] == times[1] or re.fullmatch(re.escape(times[0]) + r"\s*[-–—]\s*\d{1,2}:\d{2}", times[1])
            same_fare = prices[0] == prices[1] or (prices[0] and prices[1].startswith(prices[0] + " "))
            if not (same_start and same_fare):
                continue
            booking = next((c for c in (prev_contact, contact) if "Запись" in c), contact)
            slots[i] = (times[1], prices[1], booking, destination)
            break
        else:
            slots.append((when, price, contact, destination))
    if not slots:
        return []
    if len({(p, c) for _, p, c, _ in slots}) == 1:
        price, contact = slots[0][1:3]
        return [" · ".join(w for w, _, _, _ in slots if w),
                " · ".join(x for x in (f"💳 {price}" if price else "", contact) if x)]
    return [" · ".join(x for x in (w, f"💳 {p}" if p else "", c) if x) for w, p, c, _ in slots]


def _same_person(label: str, name: str) -> bool:
    """True when the label is the stored name in any grammatical case ("Нозы Усмановой" for "Ноза Усманова")."""
    words = collapse_ws(label).casefold().replace("ё", "е").split()
    parts = collapse_ws(name).casefold().replace("ё", "е").split()
    return bool(parts) and len(words) == len(parts) and all(
        word[:max(3, len(part) - 2)] == part[:max(3, len(part) - 2)] for word, part in zip(words, parts))


def _vk_time_label(row: Mapping[str, Any]) -> str:
    clock = _plain(row.get("time"))
    if not re.fullmatch(r"\d{1,2}:\d{2}", clock):
        return clock
    day = _plain(row.get("date"))
    primary = row.get("source_post_url") or row.get("dedup_source_url")
    for post in row.get("linked_posts") or []:
        if not primary or (post.get("url") or post.get("source_url")) != primary:
            continue
        text = post.get("text") or ""
        if not re.search(r"\bвыезд\s+(?:в\s+)?" + re.escape(clock) + r"(?!\d)", text, re.I):
            continue
        try:
            published = datetime.fromisoformat(str(post.get("post_date") or day))
        except ValueError:
            continue
        if len(split_occurrence_blocks(text, post_date=published)) == 1 and extract_date_iso(text, post_date=published) == day:
            return f"выезд в {clock}"
    return clock


async def _assemble_vk_narrative(
    items: Sequence[Mapping[str, Any]],
    narrative: Mapping[str, Any],
    *,
    period: str,
    mention_ids: Mapping[int, tuple[int, str]],
    mentions_enabled: bool,
    shorten: Callable[[str], Awaitable[str]],
    test_mention: int | None = None,
    mention_coverage: dict[str, int] | None = None,
) -> str | None:
    """Wrap the written prose with exact titles, dates, prices, contacts and hashtags from the rows."""
    texts = [narrative["lead"], narrative.get("outro") or "", *(p["text"] for p in narrative["paragraphs"])]
    if any(re.search(r"[\[\]]", VK_NAME_MARK_RE.sub(lambda m: m[2], text)) for text in texts):
        return None

    test_tags: set[str] = set()
    tagged_guides: set[int] = set()
    wall_links: set[str] = set()

    def mention(m: re.Match[str]) -> str:
        label = m[2]
        owner, name = mention_ids.get(int(m[1]), (0, ""))
        if not owner or not _same_person(label, name):
            return label
        if test_mention is not None:
            # Every identified guide is rewritten to the approved test recipient, so a review
            # post shows exactly the names that production would turn into mentions.
            tagged_guides.add(owner)
            test_tags.add(f"[id{test_mention}|{label}]")
            return f"[id{test_mention}|{label}]"
        return f"[id{owner}|{label}]" if mentions_enabled else label

    def names(text: str) -> str:
        return VK_NAME_MARK_RE.sub(mention, text)

    by_id = {int(row.get("id") or 0): row for row in items}
    tags = _digest_hashtags(items)
    lead = re.sub(r"(?<!\w)#(?=\w)", "", narrative["lead"])
    blocks = [f"ЭКСКУРСИИ {period.upper()}".strip() + "\n" + names(lead)]
    for paragraph in narrative["paragraphs"]:
        rows = sorted((by_id[i] for i in paragraph["occurrence_ids"]),
                      key=lambda r: (_plain(r.get("date")), _plain(r.get("time")) or "99:99"))
        details: list[tuple[str, str, str, str]] = []
        for row in rows:
            try:
                day = date.fromisoformat(_plain(row.get("date")))
                when = f"{day.day} {RU_MONTH_GEN.get(day.month, '')} ({RU_WEEK_SHORT[day.weekday()]})"
            except ValueError:
                when = ""
            if clock := _vk_time_label(row):
                when = f"{when}, {clock}".strip(", ")
            link, is_phone = _primary_link(row)
            source_url = _plain(row.get("source_post_url"))
            booking_note = _plain(row.get("booking_text") or _fact(row, "booking_text"))
            two_steps = source_url and re.search(
                r"регистраци\w*\s+на\s+сайте\b[^\n]*?\s+и\s+заполн\w+\s+(?:форм\w*|анкет\w*)",
                booking_note, re.I,
            )
            if two_steps:
                link, is_phone = source_url, False
            destination = ""
            if link and is_phone:
                destination = _normalize_phone(link) or _plain(link)
                contact = f"☎ Запись по телефону: {destination}"
            elif link:
                is_booking = _plain(link) != _plain(row.get("source_post_url") or row.get("channel_url"))
                label = "Запись" if is_booking else "Подробности"
                if two_steps:
                    label = booking_note
                url = link if _is_vk_url(link) else await shorten(link)
                destination = url
                if re.fullmatch(r"(?:https?://)?(?:m\.)?vk\.(?:com|ru)/wall-?\d+_\d+/?", url, re.I):
                    contact = _vk_link(label, url)
                    wall_links.add(contact)
                else:
                    contact = f"{label}: {url}"
                contact = "🔗 " + contact
            else:
                contact = ""
            price = _plain(row.get("price_line") or row.get("price_text"))
            # A short URL may represent both a booking link and the source post.
            details.append((when, price, contact, destination))
        lines = _vk_schedule_lines(details)
        title = _plain(rows[0].get("canonical_title")) or "Экскурсия"
        heading = title.upper()
        body = paragraph["text"]
        first_line, separator, rest = body.partition("\n")
        if separator and first_line.strip() == heading:
            body = rest
        body = collapse_ws(body)
        blocks.append("\n\n".join(x for x in (
            heading, names(body), "\n".join(line for line in lines if line)
        ) if x))
    if narrative.get("outro"):
        blocks.append(names(narrative["outro"]))
    blocks.append("Это дайджест экскурсий по Калининградской области от «Ух ты, Калининград!».")
    blocks.append(" ".join(tags))
    text = "\n\n".join(blocks)
    if mention_coverage is not None:
        mention_coverage.update(eligible=len({owner for owner, _ in mention_ids.values()}),
                                substituted=len(tagged_guides))
    if test_mention is not None:
        remaining = text
        for link_markup in (*test_tags, *wall_links):
            remaining = remaining.replace(link_markup, "")
        if re.search(r"[\[\]@]|\*[\w.-]+", remaining):
            raise ValueError("Unexpected VK mention markup in test digest")
        if not test_tags:
            text += f"\n\nПроверка уведомления: [id{test_mention}|тестовый получатель]"
    return text


async def build_visual_digest_vk_text(
    rows: Sequence[Mapping[str, Any]],
    *,
    issue_id: int,
    db: Database | None = None,
    bot: Any | None = None,
    vk_api_fn: Callable[..., Awaitable[Any]] | None = None,
    test_mention: int | None = None,
    review: dict[str, Any] | None = None,
    mention_coverage: dict[str, int] | None = None,
) -> str:
    items = [dict(r) for r in rows]
    period = _period_of(items)
    short_cache: dict[str, str | None] = {}

    async def shorten(link: str) -> str:
        cache_key = _ensure_https(_plain(link))
        if cache_key not in short_cache:
            short_cache[cache_key] = await _shorten_external_for_vk(link, db=db, bot=bot, vk_api_fn=vk_api_fn)
        return short_cache[cache_key] or link

    payload, mention_ids = _vk_narrative_payload(items)
    if review is not None:
        review["input"] = payload
    writer_options = {"review": review} if review is not None else {}
    narrative = await ask_vk_narrative(payload, issue_id=int(issue_id), **writer_options) if items else None
    if narrative:
        text = await _assemble_vk_narrative(items, narrative, period=period, mention_ids=mention_ids,
                                            mentions_enabled=VISUAL_DIGEST_VK_MENTIONS_ENABLED,
                                            shorten=shorten, test_mention=test_mention, mention_coverage=mention_coverage)
        if text:
            return text
        logger.warning("guide_vk_narrative issue_id=%s outcome=rejected reason=stray_markup", issue_id)
    if test_mention is not None:
        raise ValueError("No narrative was generated for the requested test post")
    lines = [
        f"Дайджест экскурсий №{int(issue_id)}",
        "",
        f"Коротко: {len(items)} будущих экскурсий" + (f" на {period}" if period else "") + ". Смотрите карточку, а детали и запись — ниже.",
        "",
    ]
    for idx, row in enumerate(items, start=1):
        title = _plain(row.get("canonical_title")) or "Экскурсия"
        link, is_phone = _primary_link(row)
        if link and is_phone:
            display = _normalize_phone(link) or _plain(link)
            line = f"{idx}. {title} — {display}"
        elif link and _is_vk_url(link):
            line = f"{idx}. {_vk_link(title, link)}"
        elif link:
            line = f"{idx}. {title} — {await shorten(link)}"
        else:
            line = f"{idx}. {title}"
        lines.append(line)
    lines.extend(["", " ".join(_digest_hashtags(items))])
    return "\n".join(lines).strip()


async def _resolve_vk_group_id(
    *,
    db: Database | None,
    bot: Any | None,
    group_id: str | int | None = None,
    target: str | None = None,
    vk_api_fn: Callable[..., Awaitable[Any]] | None = None,
) -> int:
    explicit = collapse_ws(group_id) or VISUAL_DIGEST_VK_TARGET_GROUP_ID
    if explicit:
        raw = explicit.lstrip("-")
        if raw.isdigit():
            return int(raw)
        target = raw
    screen_name = collapse_ws(target) or VISUAL_DIGEST_VK_TARGET
    screen_name = re.sub(r"^https?://vk\.(?:com|ru)/", "", screen_name, flags=re.I).strip("/")
    if not screen_name:
        raise RuntimeError("GUIDE_VISUAL_DIGEST_VK_TARGET or GUIDE_VISUAL_DIGEST_VK_TARGET_GROUP_ID is required")
    if vk_api_fn is None:
        import main

        vk_api_fn = main._vk_api  # type: ignore[attr-defined]
    response = await vk_api_fn("utils.resolveScreenName", {"screen_name": screen_name}, db, bot)
    payload = response.get("response", response) if isinstance(response, dict) else response
    if not isinstance(payload, Mapping):
        raise RuntimeError(f"VK target resolve failed for {screen_name}: invalid response")
    if _plain(payload.get("type")) != "group" or payload.get("object_id") is None:
        raise RuntimeError(f"VK target {screen_name} resolved as {_plain(payload.get('type')) or 'unknown'}, expected group")
    return int(payload["object_id"])


def _vk_public_target_key(*, group_id: int, target: str | None = None) -> str:
    return f"vk:{collapse_ws(target) or group_id}:visual"


def _extract_wall_post_id(url: str | None) -> int | None:
    m = re.search(r"wall-?\d+_(\d+)", _plain(url))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def default_review_publish_date(*, delay_days: int = VISUAL_DIGEST_REVIEW_DELAY_DAYS) -> int:
    now = datetime.now(timezone.utc) + timedelta(days=max(1, int(delay_days)))
    rounded = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return int(rounded.timestamp())


async def publish_visual_digest_to_vk(
    db: Database, bot: Any | None = None, *, issue_id: int | None = None,
    max_cards: int = VISUAL_DIGEST_MAX_CARDS_DEFAULT, group_id: str | int | None = None,
    target: str | None = None, publish_date: int | None = None,
    vk_api_fn: Callable[..., Awaitable[Any]] | None = None,
    post_to_vk_fn: Callable[..., Awaitable[str | None]] | None = None,
    upload_vk_photo_bytes_fn: Callable[..., Awaitable[str | None]] | None = None,
    publish_stories: bool = False,
    test_mention: int | None = None,
    review_dir: Path | None = None,
    review_rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    from .max_delivery import run_legacy_delivery
    if review_dir is not None and test_mention is None:
        raise ValueError("A VK review capture requires a test publication")
    if review_rows is not None and (test_mention is None or issue_id is None):
        raise ValueError("Projected rows require a numbered test publication")
    if test_mention is not None and (
        str(group_id).lstrip("-") != "243343951" or test_mention not in (858363572, 1134828279)
    ):
        raise ValueError("Choose the review community and one of the two approved test recipients")
    if issue_id is None:
        built = await build_visual_digest_issue(db, max_cards=max_cards)
        issue_id = int(built["issue_id"])
    group = await _resolve_vk_group_id(db=db, bot=bot, group_id=group_id, target=target, vk_api_fn=vk_api_fn)
    async def send_one():
        return await _publish_visual_digest_to_vk_once(
            db, bot, issue_id=issue_id, max_cards=max_cards, group_id=group, target=target,
            publish_date=publish_date, vk_api_fn=vk_api_fn, post_to_vk_fn=post_to_vk_fn,
            upload_vk_photo_bytes_fn=upload_vk_photo_bytes_fn, publish_stories=publish_stories, test_mention=test_mention,
            review_dir=review_dir, review_rows=review_rows)
    if test_mention is not None:
        # Each deliberate review run is a new post, including for the same issue.
        return await send_one()

    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT published_targets_json FROM guide_digest_issue WHERE id=?", (issue_id,))
        saved = await cur.fetchone()
    for key, receipt in _json_load(saved[0] if saved else None, {}).items():
        if key.startswith("vk:") and int(receipt.get("group_id") or 0) == group and receipt.get("post_urls"):
            return dict(receipt, published=True, issue_id=issue_id, target=key, url=receipt["post_urls"][0])
    return await run_legacy_delivery(db, issue_id=issue_id, provider="vk", target_id=str(group), send=send_one)


async def _publish_visual_digest_to_vk_once(
    db: Database,
    bot: Any | None = None,
    *,
    issue_id: int | None = None,
    max_cards: int = VISUAL_DIGEST_MAX_CARDS_DEFAULT,
    group_id: str | int | None = None,
    target: str | None = None,
    publish_date: int | None = None,
    vk_api_fn: Callable[..., Awaitable[Any]] | None = None,
    post_to_vk_fn: Callable[..., Awaitable[str | None]] | None = None,
    upload_vk_photo_bytes_fn: Callable[..., Awaitable[str | None]] | None = None,
    publish_stories: bool = False,
    test_mention: int | None = None,
    review_dir: Path | None = None,
    review_rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    if review_rows is not None and (test_mention is None or issue_id is None):
        raise ValueError("Projected rows require a numbered test publication")
    if vk_api_fn is None:
        import main

        vk_api_fn = main._vk_api  # type: ignore[attr-defined]
    if post_to_vk_fn is None:
        import main

        post_to_vk_fn = main.post_to_vk  # type: ignore[attr-defined]
    if upload_vk_photo_bytes_fn is None:
        import main

        upload_vk_photo_bytes_fn = main.upload_vk_photo_bytes  # type: ignore[attr-defined]

    if review_rows is not None:
        rows = list(review_rows)
    elif issue_id is None:
        built = await build_visual_digest_issue(db, max_cards=max_cards)
        issue_id = int(built["issue_id"])
        rows = list(built.get("items") or [])
    else:
        issue = await load_visual_digest_issue(db, issue_id)
        if not issue:
            return {"published": False, "reason": "no_issue", "issue_id": issue_id}
        rows = list(issue.get("items") or [])
    if not rows:
        return {"published": False, "reason": "no_items", "issue_id": issue_id, "family": VISUAL_DIGEST_FAMILY}

    rows = rows[: max(1, min(int(max_cards), 10)) * VISUAL_DIGEST_CARD_LIMIT]
    group = await _resolve_vk_group_id(db=db, bot=bot, group_id=group_id, target=target, vk_api_fn=vk_api_fn)
    review = {"issue_id": int(issue_id)} if review_dir is not None else None
    coverage: dict[str, int] = {}
    try:
        text = await build_visual_digest_vk_text(
            rows, issue_id=int(issue_id), db=db, bot=bot, vk_api_fn=vk_api_fn,
            test_mention=test_mention, review=review, mention_coverage=coverage if test_mention is not None else None,
        )
    finally:
        if review_dir is not None:
            (review_dir / "generation.json").write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    if review_dir is not None:
        (review_dir / "wall-text.txt").write_text(text, encoding="utf-8")
    card_bytes = render_visual_digest_cards(rows, issue_id=int(issue_id))
    attachments: list[str] = []
    for idx, payload in enumerate(card_bytes, start=1):
        # VK rejected the full JPEG card but accepted the same pixels as PNG.
        # Keep the text at its rendered resolution instead of resizing it away.
        with Image.open(io.BytesIO(payload)) as card:
            png = io.BytesIO()
            card.save(png, format="PNG")
            logger.info("guide_visual_digest_vk_card issue_id=%s card=%s format=PNG width=%s height=%s bytes=%s",
                        issue_id, idx, card.width, card.height, png.tell())
        if review_dir is not None:
            (review_dir / f"card-{idx}.png").write_bytes(png.getvalue())
        att = await upload_vk_photo_bytes_fn(str(group), png.getvalue(), db, bot, filename=f"guide_visual_digest_{issue_id}_{idx}.png")
        if not att:
            raise RuntimeError(f"Guide visual digest VK upload failed for card {idx}/{len(card_bytes)}")
        attachments.append(att)
    url = await post_to_vk_fn(str(group), text, db, bot, attachments=attachments, carousel=True, publish_date=publish_date)
    if not url:
        raise RuntimeError("Guide visual digest VK wall.post failed")

    if test_mention is not None:
        # A review post leaves the issue available for regular daily publication.
        return {"published": True, "issue_id": issue_id, "url": url, "group_id": group,
                "text": text, "cards_count": len(card_bytes), "attachments_count": len(attachments),
                "guide_mentions": coverage}

    post_id = _extract_wall_post_id(url)
    target_key = _vk_public_target_key(group_id=group, target=target or VISUAL_DIGEST_VK_TARGET)
    story_payload: dict[str, Any] | None = None
    if publish_stories or VISUAL_DIGEST_STORIES_ENABLED:
        if publish_date:
            story_payload = {
                "published": False,
                "pending": True,
                "reason": "wall_post_is_postponed",
                "due_at": int(publish_date) + VISUAL_DIGEST_VK_STORY_DELAY_SECONDS,
                "delay_seconds": VISUAL_DIGEST_VK_STORY_DELAY_SECONDS,
            }
        else:
            from promo import VKStoryUploadError, _publish_vk_story_photo

            try:
                story_payload = await _publish_vk_story_photo(
                    db,
                    bot,
                    target_group_id=int(group),
                    image_bytes=render_visual_digest_story_image(card_bytes[0]),
                    source_url=url,
                    link_text="more",
                    include_source_link=True,
                )
                story_payload["published"] = True
            except Exception as exc:  # pragma: no cover - production integration
                logger.warning("guide_visual_digest_story_failed issue_id=%s: %s", issue_id, exc, exc_info=True)
                story_payload = {"published": False, "reason": str(exc) or type(exc).__name__}
                if isinstance(exc, VKStoryUploadError):
                    story_payload.update(pending=True, attempts=1, retry_after=int(time.time()) + 900)

    occurrence_ids = list(dict.fromkeys(i for row in rows for i in row.get("covered_visual_ids", [row["id"]])))
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
        await ensure_visual_digest_schema(conn)
        cur = await conn.execute("SELECT published_targets_json, media_items_json FROM guide_digest_issue WHERE id=?", (int(issue_id),))
        current = await cur.fetchone()
        targets_raw = _json_load(current["published_targets_json"] if current else None, {})
        targets = dict(targets_raw) if isinstance(targets_raw, Mapping) else {}
        targets[target_key] = {
            "message_ids": [int(post_id)] if post_id else [],
            "text_message_ids": [int(post_id)] if post_id else [],
            "media_message_ids": [int(post_id)] if post_id else [],
            "post_urls": [url],
            "group_id": int(group),
            "transport": "vk_wall",
            "attachments_count": len(attachments),
            "cards_count": len(card_bytes),
            "publish_date": int(publish_date) if publish_date else None,
            "story": story_payload,
        }
        media_raw = _json_load(current["media_items_json"] if current else None, {})
        media_payload = dict(media_raw) if isinstance(media_raw, Mapping) else {}
        media_payload.update(
            {
                "kind": "visual_schedule_card",
                "card_limit": VISUAL_DIGEST_CARD_LIMIT,
                "item_states": _visual_publication_states(rows),
                "selection_reasons": {str(int(row["id"])): _plain(row.get("visual_selection_reason")) for row in rows if int(row.get("id") or 0) > 0},
            }
        )
        await conn.execute(
            """
            UPDATE guide_digest_issue
            SET status='published', target_chat=COALESCE(NULLIF(target_chat, ''), ?), text=?,
                published_at=CURRENT_TIMESTAMP, published_targets_json=?, media_items_json=?
            WHERE id=?
            """,
            (target or VISUAL_DIGEST_VK_TARGET, text, _json_dump(targets), _json_dump(media_payload), int(issue_id)),
        )
        if occurrence_ids:
            placeholders = ",".join("?" for _ in occurrence_ids)
            await conn.execute(
                f"UPDATE guide_occurrence SET published_visual_digest_issue_id=? WHERE id IN ({placeholders})",
                (int(issue_id), *occurrence_ids),
            )
        await conn.commit()
    return {
        "published": True,
        "issue_id": int(issue_id),
        "family": VISUAL_DIGEST_FAMILY,
        "target": target_key,
        "group_id": int(group),
        "url": url,
        "attachments_count": len(attachments),
        "cards_count": len(card_bytes),
        "occurrence_ids": occurrence_ids,
        "text": text,
        "story": story_payload,
    }


def default_vk_publish_date(*, delay_seconds: int = VISUAL_DIGEST_VK_DELAY_SECONDS) -> int:
    return int(time.time()) + max(60, int(delay_seconds))


async def publish_visual_digest_daily(
    db: Database,
    bot: Any | None = None,
    *,
    max_cards: int = 1,
    target_chats: str | Sequence[str] | None = None,
    vk_group_id: str | int | None = None,
    vk_target: str | None = None,
    vk_delay_seconds: int = VISUAL_DIGEST_VK_DELAY_SECONDS,
    publish_stories: bool | None = None,
    resume_issue_id: int | None = None,
) -> dict[str, Any]:
    """Resume today's edition and independently fan out provider deliveries."""
    from .max_digest import existing_daily_issue_id, publish_visual_digest_to_max, max_enabled

    existing_id = int(resume_issue_id) if resume_issue_id is not None else await existing_daily_issue_id(db)
    if existing_id is not None:
        issue = await load_visual_digest_issue(db, existing_id)
        built = {"issue_id": existing_id, "items": (issue or {}).get("items", [])}
    else:
        built = await build_visual_digest_issue(db, max_cards=max_cards, card_limit=VISUAL_DIGEST_CARD_LIMIT)
    issue_id = int(built["issue_id"])
    rows = list(built.get("items") or [])
    if not rows:
        return {"published": False, "reason": "no_items", "issue_id": issue_id, "family": VISUAL_DIGEST_FAMILY}
    results: dict[str, Any] = {}
    if VISUAL_DIGEST_TELEGRAM_ENABLED:
        try:
            results["telegram"] = await publish_visual_digest_to_telegram(db, bot, issue_id=issue_id,
                                                                          max_cards=1, target_chats=target_chats)
        except Exception:
            logger.error("guide_visual_provider_failed issue_id=%s provider=telegram", issue_id)
            results["telegram"] = {"published": False, "reason": "provider_failure"}
    vk_publish_date = default_vk_publish_date(delay_seconds=vk_delay_seconds)
    try:
        results["vk"] = await publish_visual_digest_to_vk(
            db, bot, issue_id=issue_id, max_cards=1, group_id=vk_group_id, target=vk_target,
            publish_date=vk_publish_date,
            publish_stories=VISUAL_DIGEST_STORIES_ENABLED if publish_stories is None else bool(publish_stories))
    except Exception:
        logger.error("guide_visual_provider_failed issue_id=%s provider=vk", issue_id)
        results["vk"] = {"published": False, "reason": "provider_failure"}
    if max_enabled():
        results["max"] = await publish_visual_digest_to_max(db, issue_id=issue_id)
    owner_confirmed = any(result.get("state") == "owner_confirmed_present" for result in results.values())
    complete = all((result.get("published") or result.get("state") == "owner_confirmed_present")
                   and not result.get("errors") for result in results.values())
    state = "complete_with_owner_confirmation" if complete and owner_confirmed else ("complete" if complete else "partial")
    return {
        "published": any(result.get("published") for result in results.values()),
        "complete": complete, "state": state,
        "issue_id": issue_id, "family": VISUAL_DIGEST_FAMILY, "items": len(rows),
        **results, "vk_publish_date": vk_publish_date,
        "occurrence_ids": [int(row["id"]) for row in rows if int(row.get("id") or 0) > 0],
    }


async def _resolve_live_vk_wall_post_id(
    db: Database,
    bot: Any | None,
    *,
    owner_id: int,
    post_id: int,
    vk_api_fn: Callable[..., Awaitable[Any]] | None = None,
) -> int | None:
    if vk_api_fn is None:
        import main

        vk_api_fn = main._vk_api  # type: ignore[attr-defined]
    def _items(payload: Any) -> list[Any]:
        if isinstance(payload, Mapping):
            value = payload.get("items") or []
            return list(value) if isinstance(value, list) else []
        return list(payload) if isinstance(payload, list) else []

    try:
        import main

        response = await vk_api_fn(
            "wall.getById",
            {"posts": f"{int(owner_id)}_{int(post_id)}", "extended": 0},
            db,
            bot,
            token=main._vk_user_token(),
            token_kind="user",
        )
    except Exception:
        logger.warning("guide_visual_digest_story_live_probe_failed owner_id=%s post_id=%s", owner_id, post_id, exc_info=True)
    else:
        payload = response.get("response", response) if isinstance(response, dict) else response
        for item in _items(payload):
            if isinstance(item, Mapping) and int(item.get("id") or 0) == int(post_id):
                return int(post_id)
    # VK postponed posts can receive a new wall id when they become public while
    # retaining the original id as `postponed_id`.  Resolve that live id before
    # attaching the Story link, otherwise `wall-..._<postponed_id>` may 404.
    try:
        import main

        response = await vk_api_fn(
            "wall.get",
            {"owner_id": int(owner_id), "filter": "owner", "count": 100},
            db,
            bot,
            token=main._vk_user_token(),
            token_kind="user",
        )
    except Exception:
        logger.warning("guide_visual_digest_story_wall_scan_failed owner_id=%s post_id=%s", owner_id, post_id, exc_info=True)
        return None
    payload = response.get("response", response) if isinstance(response, dict) else response
    for item in _items(payload):
        if not isinstance(item, Mapping):
            continue
        item_id = int(item.get("id") or 0)
        postponed_id = int(item.get("postponed_id") or 0)
        if item_id == int(post_id) or postponed_id == int(post_id):
            return item_id or int(post_id)
    return None


async def publish_due_visual_digest_vk_stories(
    db: Database,
    bot: Any | None = None,
    *,
    now_ts: int | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Publish pending VK Stories for visual digest wall posts that are live."""

    if not VISUAL_DIGEST_STORIES_ENABLED:
        return {"published": False, "reason": "disabled", "stories": []}
    now_value = int(now_ts or time.time())
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
        cur = await conn.execute(
            """
            SELECT id, published_targets_json
            FROM guide_digest_issue
            WHERE family=? AND status IN ('published','partial')
              AND published_targets_json IS NOT NULL
            ORDER BY id DESC
            LIMIT ?
            """,
            (VISUAL_DIGEST_FAMILY, max(1, int(limit))),
        )
        issues = await cur.fetchall()

    published: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for issue in issues:
        issue_id = int(issue["id"])
        targets = _json_load(issue["published_targets_json"], {})
        if not isinstance(targets, Mapping):
            continue
        next_targets = dict(targets)
        changed = False
        issue_rows: list[dict[str, Any]] | None = None
        issue_card: bytes | None = None
        for target_key, payload_raw in list(targets.items()):
            payload = dict(payload_raw) if isinstance(payload_raw, Mapping) else {}
            story = dict(payload.get("story") or {}) if isinstance(payload.get("story"), Mapping) else {}
            if not story or story.get("published") or not story.get("pending"):
                continue
            due_at = int(story.get("due_at") or 0)
            retry_after = int(story.get("retry_after") or 0)
            if due_at > now_value or retry_after > now_value:
                continue
            post_urls = [collapse_ws(x) for x in (payload.get("post_urls") or []) if collapse_ws(x)]
            post_url = post_urls[0] if post_urls else ""
            post_id = _extract_wall_post_id(post_url)
            group_id = int(payload.get("group_id") or 0)
            if not post_url or not post_id or not group_id:
                story.update({"pending": False, "published": False, "reason": "missing_post_url_or_group"})
                payload["story"] = story
                next_targets[str(target_key)] = payload
                changed = True
                continue
            owner_id = -abs(int(group_id))
            actual_post_id = await _resolve_live_vk_wall_post_id(db, bot, owner_id=owner_id, post_id=int(post_id))
            if not actual_post_id:
                story["retry_after"] = now_value + 300
                story["reason"] = "wall_post_not_live_yet"
                payload["story"] = story
                next_targets[str(target_key)] = payload
                changed = True
                skipped.append({"issue_id": issue_id, "target": str(target_key), "reason": "wall_post_not_live_yet"})
                continue
            if int(actual_post_id) != int(post_id):
                post_url = f"https://vk.com/wall{int(owner_id)}_{int(actual_post_id)}"
                payload["post_urls"] = [post_url]
                payload["message_ids"] = [int(actual_post_id)]
                payload["text_message_ids"] = [int(actual_post_id)]
                payload["media_message_ids"] = [int(actual_post_id)]
            if issue_rows is None:
                loaded = await load_visual_digest_issue(db, issue_id)
                issue_rows = list((loaded or {}).get("items") or [])[:VISUAL_DIGEST_CARD_LIMIT]
            if not issue_rows:
                story["retry_after"] = now_value + 900
                story["reason"] = "no_issue_rows"
                payload["story"] = story
                next_targets[str(target_key)] = payload
                changed = True
                continue
            if issue_card is None:
                issue_card = render_visual_digest_cards(issue_rows, issue_id=issue_id)[0]
            from promo import VKStoryUploadError, _publish_vk_story_photo

            try:
                result = await _publish_vk_story_photo(
                    db,
                    bot,
                    target_group_id=group_id,
                    image_bytes=render_visual_digest_story_image(issue_card),
                    source_url=post_url,
                    link_text="more",
                    include_source_link=True,
                )
                story.update(result)
                story.update({"published": True, "pending": False, "published_at": now_value})
                payload["story"] = story
                next_targets[str(target_key)] = payload
                changed = True
                published.append({"issue_id": issue_id, "target": str(target_key), "url": result.get("url")})
            except Exception as exc:  # pragma: no cover - production integration
                logger.warning("guide_visual_digest_due_story_failed issue_id=%s target=%s: %s", issue_id, target_key, exc, exc_info=True)
                story["pending"] = isinstance(exc, VKStoryUploadError)
                story["published"] = False
                story["last_error"] = str(exc) or type(exc).__name__
                story["attempts"] = int(story.get("attempts") or 0) + 1
                story["retry_after"] = now_value + 900
                payload["story"] = story
                next_targets[str(target_key)] = payload
                changed = True
        if changed:
            async with db.raw_conn() as conn:
                await conn.execute(
                    "UPDATE guide_digest_issue SET published_targets_json=? WHERE id=?",
                    (_json_dump(next_targets), issue_id),
                )
                await conn.commit()
    return {"published": bool(published), "stories": published, "skipped": skipped}


def visual_digest_enabled() -> bool:
    return (os.getenv("ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED") or "").strip().lower() in {"1", "true", "yes", "on"}
