"""Production visual schedule digest for guide excursions.

The visual digest is a separate VK-first publication: one 1080x1350 schedule
card with up to five future excursions.  The renderer is deterministic (Pillow,
local assets only) so the daily production job can reproduce the approved VK
prototype without relying on paid image generation.
"""

from __future__ import annotations

import io
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Sequence

import aiosqlite
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

from db import Database

from .dedup import deduplicate_occurrence_rows
from .digest import RU_MONTH_GEN, format_date_time
from .parser import collapse_ws

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
VISUAL_DIGEST_VK_TARGET_GROUP_ID = collapse_ws(
    os.getenv("GUIDE_VISUAL_DIGEST_VK_TARGET_GROUP_ID") or os.getenv("GUIDE_DIGEST_VK_TARGET_GROUP_ID")
)
VISUAL_DIGEST_REVIEW_DELAY_DAYS = max(
    1,
    min(int((os.getenv("GUIDE_VISUAL_DIGEST_REVIEW_DELAY_DAYS") or "3") or 3), 30),
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
_MAIN_FONT = _FONT_DIR / "Cygre-ExtraBold.ttf"
_BOLD_FONT = _FONT_DIR / "Cygre-Bold.ttf"
_SEMI_FONT = _FONT_DIR / "Cygre-SemiBold.ttf"
_DEJAVU_BOLD = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")

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


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _future_cutoff_iso() -> str:
    return (datetime.now(timezone.utc).date() + timedelta(days=VISUAL_DIGEST_WINDOW_DAYS)).isoformat()


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
    if text.lower() in {"не указано", "not specified", "none", "null", "unknown", "n/a"}:
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


def _wrap_by_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_width: int, max_lines: int) -> list[str]:
    words = _plain(text).split()
    if not words:
        return []
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if not current or draw.textlength(candidate, font=font) <= max_width:
            current = candidate
            continue
        lines.append(current)
        current = word
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) == max_lines and words:
        # Ensure the last line does not overflow; trim character-wise if needed.
        last = lines[-1]
        while last and draw.textlength(last + "…", font=font) > max_width:
            last = last[:-1].rstrip()
        lines[-1] = (last + "…") if last != lines[-1] else last
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


def _brand_lockup(scale: float = 0.96) -> Image.Image:
    base_w, base_h = 350, 146
    img = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    halo = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    hd = ImageDraw.Draw(halo)
    hd.ellipse((0, 4, 330, 124), fill=(255, 190, 90, 80))
    hd.ellipse((36, 10, 310, 120), fill=(15, 220, 210, 55))
    halo = halo.filter(ImageFilter.GaussianBlur(8))
    img.alpha_composite(halo)

    shape = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shape)
    sd.polygon([(34, 12), (244, 12), (232, 48), (20, 48)], fill=(242, 103, 39, 255))
    sd.polygon([(52, 4), (286, 4), (270, 24), (38, 24)], fill=(255, 190, 50, 238))
    sd.polygon([(6, 42), (300, 42), (278, 108), (28, 108)], fill=(5, 72, 82, 255))
    sd.polygon([(10, 38), (288, 38), (281, 53), (7, 53)], fill=(14, 205, 205, 248))
    sd.polygon([(244, 80), (318, 80), (292, 118), (226, 118)], fill=(233, 91, 54, 255))
    shadow = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    shadow.alpha_composite(shape, (0, 7))
    shadow = shadow.filter(ImageFilter.GaussianBlur(5))
    # darken shadow alpha only
    sh = Image.new("RGBA", (base_w, base_h), (36, 18, 11, 0))
    sh.putalpha(shadow.split()[-1].point(lambda v: int(v * 0.36)))
    img.alpha_composite(sh)
    img.alpha_composite(shape)

    text_layer = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    td = ImageDraw.Draw(text_layer)
    top_font = _font(_MAIN_FONT, 32)
    main_font = _font(_MAIN_FONT, 29)
    # glow strokes on a separate blurred layer
    glow = Image.new("RGBA", (base_w, base_h), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.text((58, 20), "УХ ТЫ,", font=top_font, fill=(0, 0, 0, 0), stroke_width=8, stroke_fill=(255, 216, 109, 90))
    gd.text((36, 70), "КАЛИНИНГРАД!", font=main_font, fill=(0, 0, 0, 0), stroke_width=8, stroke_fill=(17, 226, 213, 80))
    glow = glow.filter(ImageFilter.GaussianBlur(3))
    text_layer.alpha_composite(glow)
    td.text((58, 20), "УХ ТЫ,", font=top_font, fill=(255, 250, 241, 255), stroke_width=3, stroke_fill=(75, 34, 22, 255))
    td.text((36, 70), "КАЛИНИНГРАД!", font=main_font, fill=(255, 250, 241, 255), stroke_width=3, stroke_fill=(75, 34, 22, 255))
    skewed = _skew_text_layer(text_layer, skew=-0.08)
    img.alpha_composite(skewed.crop((0, 0, base_w, base_h)))
    img = img.rotate(-2, resample=Image.Resampling.BICUBIC, expand=False)
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


def _source_name(row: Mapping[str, Any]) -> str:
    guides = [_plain(x) for x in (row.get("guide_names") or []) if _plain(x)] if isinstance(row.get("guide_names"), list) else []
    orgs = [_plain(x) for x in (row.get("organizer_names") or []) if _plain(x)] if isinstance(row.get("organizer_names"), list) else []
    if guides:
        return ", ".join(guides)
    if orgs:
        return ", ".join(orgs)
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


def _place_line(row: Mapping[str, Any]) -> str:
    title = _plain(row.get("canonical_title"))
    low = title.lower().replace("ё", "е")
    meeting = _short_meeting_point(_fact(row, "meeting_point") or row.get("meeting_point"))
    route = _route_raw(row)
    if meeting:
        point = re.sub(r"(?iu)^от\s+", "", meeting)
        if "железнодорожн" in low and "ворот" in low:
            return "Железнодорожные ворота" if re.search("Железнодорожные ворота", point, re.I) else f"{point} → Железнодорожные ворота"
        if "трамва" in low or "зеленоградск" in low:
            return point
        if "культурная революция: туда" in low:
            return f"{point} → Хуфен"
        if re.search(r"(?iu)^(Гвардейский|Железнодорожный$|Железнодорожные ворота$)", point):
            return point
        return point
    if re.search(r"(?iu)балтийск|балткос", low):
        return "Балтийск → Балтийская коса"
    if re.search(r"(?iu)куршск|дюн", low):
        return "Куршская коса"
    if re.search(r"(?iu)коса, коты|сыр", low):
        return "побережье области"
    if "амалиенау" in low:
        return "Калининград · Амалиенау"
    if "понарт" in low:
        return "Калининград · Понарт"
    if "проспект мира" in low:
        return "Калининград · проспект Мира"
    if "фрунзе" in low:
        return "Калининград · улица Фрунзе"
    if "вармийск" in low:
        return "Ушаково → Ладушкин → Багратионовск"
    if "самб" in low:
        return "Варген → Куменен → берег Балтики"
    if "река города к" in low:
        return "Кнайпхоф → мосты → порт → гавани"
    if "трамва" in low:
        return "Калининград · трамвайная история"
    if "обзорная прогулка" in low:
        return "Калининград · обзорный маршрут"
    if route and len(route) < 58 and route.lower() != low:
        return route
    return _plain(row.get("city") or _fact(row, "city")) or "детали в тексте"


def _focus_line(row: Mapping[str, Any]) -> str:
    text = (_plain(row.get("canonical_title")) + " " + _route_raw(row)).lower().replace("ё", "е")
    if re.search(r"амалиенау|вилл", text):
        return "архитектура · виллы · детали"
    if "вармийск" in text:
        return "руины · кирхи · юго-запад области"
    if re.search(r"куршск|дюн", text):
        return "дюны · природа · море"
    if re.search(r"балтийск|балткос|порт", text):
        return "форты · порт · море"
    if re.search(r"коты|сыр", text):
        return "море · коты · сыр"
    if re.search(r"река|сплав|анграп", text):
        return "река · природа · активный формат"
    if "трамва" in text:
        return "городской транспорт · история"
    if re.search(r"понарт|фрунзе|проспект мира|железнодорожн", text):
        return "городская история · архитектура"
    if "культурная революция" in text:
        return "городская история · культура"
    audience = row.get("audience_fit") if isinstance(row.get("audience_fit"), list) else []
    ru_audience = [
        _plain(x)
        for x in audience[:3]
        if _plain(x) and re.search(r"[А-Яа-яЁё]", _plain(x))
    ]
    if ru_audience:
        return " · ".join(ru_audience)[:48]
    return ""


def _modes_for(row: Mapping[str, Any]) -> list[str]:
    fp_group = _plain(_fact(row, "group_format") or row.get("group_format"))
    text = " ".join([_plain(row.get("canonical_title")), _route_raw(row), fp_group, _plain(row.get("duration_text"))]).lower().replace("ё", "е")
    modes: list[str] = []
    if "трамва" in text:
        modes.append("tram")
    if re.search(r"сплав|байдар|лодк|катер|корабл|яхт|водн|река", text):
        modes.append("boat")
    if "автобус" in fp_group.lower() or "автобус" in text:
        modes.append("bus")
    road_like = re.search(r"кольц|коса|куршск|балтийск|балткос|самбия|вармийск|выезд|8 часов|10 часов", text)
    if road_like and "bus" not in modes:
        modes.append("route")
    if re.search(r"пешеход|пеш|прогулк|район|улица|амалиенау|понарт|проспект|фрунзе|ворот|городск|железнодорожн|зеленоградск|культурная", text):
        modes.append("walk")
    out: list[str] = []
    for mode in modes:
        if mode not in out:
            out.append(mode)
    return out[:2]


def _seats_text(row: Mapping[str, Any]) -> str:
    text = _plain(row.get("seats_line") or row.get("seats_text"))
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


def _avatar_key(row: Mapping[str, Any]) -> str:
    guides = " ".join(_plain(x) for x in (row.get("guide_names") or []) if _plain(x)) if isinstance(row.get("guide_names"), list) else ""
    orgs = " ".join(_plain(x) for x in (row.get("organizer_names") or []) if _plain(x)) if isinstance(row.get("organizer_names"), list) else ""
    src = " ".join(
        [
            _plain(row.get("source_username")),
            _plain(row.get("primary_source_username")),
            _plain(row.get("source_title")),
            _plain(row.get("primary_source_title")),
            guides,
            orgs,
        ]
    ).lower().replace("ё", "е")
    checks = [
        (r"татьяна удовенко|tanja_from_koenigsberg", "tatyana_udovenko_face"),
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


def _draw_avatar(draw_img: Image.Image, row: Mapping[str, Any], x: int, y: int, size: int, pal: VisualPalette, accent: str) -> None:
    layer = Image.new("RGBA", draw_img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    # subtle shadow
    d.ellipse((x - 4, y + 4, x + size + 4, y + size + 12), fill=(0, 0, 0, 34))
    d.ellipse((x - 4, y - 4, x + size + 4, y + size + 4), fill=_rgba(pal.card, 255), outline=_rgba(accent, 255), width=5)
    key = _avatar_key(row)
    path = _AVATAR_DIR / _AVATAR_FILES.get(key, "")
    if key and path.is_file():
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
    # Top bar with rounded top corners.
    draw.rounded_rectangle((x, y, x + w, y + bar_h + 14), radius=28, fill=_hex(accent))
    draw.rectangle((x, y + bar_h, x + w, y + bar_h + 14), fill=_hex(accent))
    f_mon = _font(_MAIN_FONT, 23 if len(mon) > 4 else 27)
    f_day = _font(_MAIN_FONT, 70)
    f_meta = _font(_MAIN_FONT, 22 if time_text else 20)
    mon_txt = mon.upper()
    bb = draw.textbbox((0, 0), mon_txt, font=f_mon)
    draw.text((x + w / 2 - (bb[2] - bb[0]) / 2, y + 12 - (bb[1] / 2)), mon_txt, font=f_mon, fill=(255, 255, 255))
    day_y = y + (110 if row_h <= 190 else 124)
    bb = draw.textbbox((0, 0), day, font=f_day)
    draw.text((x + w / 2 - (bb[2] - bb[0]) / 2, day_y - (bb[3] - bb[1]) / 2 - 16), day, font=f_day, fill=_hex(pal.text))
    meta = " · ".join(p for p in (wd, time_text) if p) if time_text else wdf
    bb = draw.textbbox((0, 0), meta, font=f_meta)
    meta_y = y + (150 if row_h <= 190 else 174)
    draw.text((x + w / 2 - (bb[2] - bb[0]) / 2, meta_y - (bb[3] - bb[1]) / 2 - 2), meta, font=f_meta, fill=_hex(pal.route))


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
    all_items = [dict(r) for r in (all_rows or page_rows)]
    palette = palette or _palette_for_issue(issue_id + card_index - 1)
    img = _gradient((W, H), palette.bg1, palette.bg2).convert("RGBA")
    img.alpha_composite(_alpha_circle_layer((W, H), (700, -62, 1160, 398), palette.halo, 42))
    img.alpha_composite(_alpha_circle_layer((W, H), (-238, 1035, 222, 1495), palette.decor, 24))
    draw = ImageDraw.Draw(img)

    # Header badge and title.
    shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rounded_rectangle((55, 62, 291, 160), radius=30, fill=(0, 0, 0, 30))
    shadow = shadow.filter(ImageFilter.GaussianBlur(8))
    img.alpha_composite(shadow)
    draw.rounded_rectangle((55, 54, 291, 152), radius=30, fill=_hex(palette.issue))
    draw.text((83, 72), "ВЫПУСК", font=_font(_MAIN_FONT, 22), fill=(255, 255, 255))
    # Cygre lacks the numero glyph on the production image; draw it with a
    # system fallback and keep the digits in the brand font.
    no_font = _fallback_font(_DEJAVU_BOLD, 52)
    num_font = _font(_MAIN_FONT, 58)
    draw.text((80, 96), "№", font=no_font, fill=(255, 255, 255))
    draw.text((146, 100), f"{int(issue_id)}", font=num_font, fill=(255, 255, 255))
    title_font = _font(_MAIN_FONT, 50)
    draw.text((320, 56), "Дайджест", font=title_font, fill=_hex(palette.text))
    draw.text((320, 108), "экскурсий", font=title_font, fill=_hex(palette.text))
    draw.text((322, 173), "куда · с кем · как", font=_font(_MAIN_FONT, 23), fill=_hex(palette.sub))
    # Keep the native VK carousel counter area (top-right) free: the logo stays
    # large but ends before the usual overlay pill.
    img.alpha_composite(_brand_lockup(.96), (600, 124))

    page_period = _period_of(page_rows)
    full_period = _period_of(all_items)
    draw.text((58, 223), page_period, font=_font(_MAIN_FONT, 52), fill=_hex(palette.text))
    start_no = (card_index - 1) * VISUAL_DIGEST_CARD_LIMIT + 1
    end_no = min(start_no + len(page_rows) - 1, len(all_items))
    if total_cards > 1:
        count = f"{start_no}–{end_no} из {len(all_items)}"
    else:
        count = f"{len(page_rows)} из {len(all_items)}"
    draw.text((58, 268), count, font=_font(_MAIN_FONT, 26), fill=_hex(palette.count))
    extra = ""
    if len(all_items) > end_no and total_cards <= card_index:
        extra = f"ещё {len(all_items) - end_no} в тексте"
    elif total_cards <= 1 and full_period and full_period != page_period:
        extra = full_period
    if extra:
        draw.text((178, 268), extra, font=_font(_MAIN_FONT, 26), fill=_hex(palette.count))
    draw.line((58, 306, 1022, 306), fill=_rgba(palette.divider, 180), width=2)

    expanded = len(page_rows) <= 4
    row_h = 236 if expanded else 174
    gap = 18 if expanded else 12
    y0 = 326 if expanded else 332
    title_font = _font(_MAIN_FONT, 34 if expanded else 29)
    guide_font = _font(_MAIN_FONT, 24 if expanded else 21)
    route_font = _font(_MAIN_FONT, 23 if expanded else 20)
    focus_font = _font(_BOLD_FONT, 20 if expanded else 18)

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
        ty = 42 if expanded else 34
        title_lines = _wrap_by_width(rd, _plain(row.get("canonical_title")) or "Экскурсия", title_font, 555, 2)
        for line in title_lines:
            rd.text((tx, ty), line, font=title_font, fill=_hex(palette.text))
            ty += 39 if expanded else 32
        guide = _source_name(row)
        if guide:
            rd.text((tx, ty + 2), _fit(guide, 48 if expanded else 44), font=guide_font, fill=_hex(palette.guide))
            ty += 34 if expanded else 27
        place = _place_line(row)
        rd.text((tx, ty + 2), _fit(place, 58 if expanded else 52), font=route_font, fill=_hex(palette.route))
        ty += 34 if expanded else 26
        focus = _focus_line(row)
        if focus:
            rd.text((tx, ty + 2), _fit(focus, 54 if expanded else 44), font=focus_font, fill=_hex(palette.focus))
        seats = _seats_text(row)
        if seats:
            _draw_chip(rd, seats, 654, row_h - (54 if expanded else 52), accent)
        img.alpha_composite(row_layer, (42, y))
        av_size = 118
        av_x = 42 + 806
        av_y = y + round((row_h - av_size) / 2)
        _draw_avatar(img, row, av_x, av_y, av_size, palette, accent)
        modes = _modes_for(row)
        _draw_icon_badges(img, modes, 42 + 918, av_y + (8 if len(modes) > 1 else 68), palette)

    foot_y = y0 + len(page_rows) * (row_h + gap) + 10
    if len(all_items) > end_no and total_cards <= card_index:
        draw.text(
            (58, min(1260, foot_y)),
            "Остальные даты — ниже в посте. В карточке только ближайшие/дефицитные.",
            font=_font(_MAIN_FONT, 20),
            fill=_hex(palette.count),
        )
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


async def _fetch_visual_candidates(conn: aiosqlite.Connection, *, limit: int) -> list[dict[str, Any]]:
    await ensure_visual_digest_schema(conn)
    sql = """
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
            gp.facts_rollup_json AS guide_profile_facts_rollup_json
        FROM guide_occurrence go
        LEFT JOIN guide_source gs ON gs.id = go.primary_source_id
        LEFT JOIN guide_profile gp ON gp.id = gs.primary_profile_id
        LEFT JOIN guide_digest_issue gdi ON gdi.id = go.published_visual_digest_issue_id
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
    cur = await conn.execute(sql, (_today_iso(), _future_cutoff_iso(), int(limit)))
    rows = await cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["guide_names"] = _parse_json_array(item.get("guide_names_json"))
        item["organizer_names"] = _parse_json_array(item.get("organizer_names_json"))
        item["audience_fit"] = _parse_json_array(item.get("audience_fit_json"))
        item["fact_pack"] = _safe_json_object(item.get("fact_pack_json"))
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
        post_cur = await conn.execute(
            """
            SELECT gmp.text, gmp.source_url
            FROM guide_occurrence_source gos
            JOIN guide_monitor_post gmp ON gmp.id = gos.post_id
            WHERE gos.occurrence_id=?
            ORDER BY CASE WHEN gos.role='primary' THEN 0 ELSE 1 END, gmp.post_date DESC, gmp.id DESC
            LIMIT 1
            """,
            (int(item.get("id") or 0),),
        )
        post_row = await post_cur.fetchone()
        item["dedup_source_text"] = collapse_ws(str((post_row["text"] if post_row else "") or ""))
        payload_url = item["fact_pack"].get("source_post_url") if isinstance(item["fact_pack"], dict) else None
        item["source_post_url"] = _source_post_url(payload_url, (post_row["source_url"] if post_row else None) or item.get("channel_url"))
        out.append(item)
    return out


def _is_tg_post_url(url: str | None) -> bool:
    raw = _plain(url)
    if not raw.startswith("https://t.me/"):
        return False
    parts = raw.rstrip("/").split("/")
    return len(parts) >= 5 and parts[-1].isdigit()


def _source_post_url(payload_url: Any, fallback_url: Any) -> str | None:
    primary = _plain(payload_url)
    fallback = _plain(fallback_url)
    if _is_tg_post_url(primary):
        return primary
    if _is_tg_post_url(fallback):
        return fallback
    return primary or fallback or None


async def select_visual_digest_rows(db: Database, *, max_items: int = VISUAL_DIGEST_CARD_LIMIT) -> list[dict[str, Any]]:
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
        raw_limit = max(int(max_items) * 4, 48)
        rows = await _fetch_visual_candidates(conn, limit=raw_limit)
    if not rows:
        return []
    dedup = await deduplicate_occurrence_rows(rows, family=VISUAL_DIGEST_FAMILY, limit=max(len(rows), int(max_items)))
    display_rows = [dict(row) for row in dedup.display_rows]
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
                _json_dump({"kind": "visual_schedule_card", "card_limit": int(card_limit)}),
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
    cur = await conn.execute(
        f"""
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
            gp.facts_rollup_json AS guide_profile_facts_rollup_json
        FROM guide_occurrence go
        LEFT JOIN guide_source gs ON gs.id = go.primary_source_id
        LEFT JOIN guide_profile gp ON gp.id = gs.primary_profile_id
        WHERE go.id IN ({placeholders})
        """,
        tuple(ids),
    )
    rows = await cur.fetchall()
    by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        item["guide_names"] = _parse_json_array(item.get("guide_names_json"))
        item["organizer_names"] = _parse_json_array(item.get("organizer_names_json"))
        item["audience_fit"] = _parse_json_array(item.get("audience_fit_json"))
        item["fact_pack"] = _safe_json_object(item.get("fact_pack_json"))
        for key in (
            "canonical_title", "date", "time", "duration_text", "city", "meeting_point", "route_summary",
            "price_text", "booking_text", "booking_url", "status", "seats_text", "summary_one_liner",
            "digest_blurb", "availability_mode", "post_kind", "group_format",
        ):
            if not item.get(key) and item["fact_pack"].get(key) is not None:
                item[key] = item["fact_pack"].get(key)
        post_cur = await conn.execute(
            """
            SELECT gmp.text, gmp.source_url
            FROM guide_occurrence_source gos
            JOIN guide_monitor_post gmp ON gmp.id = gos.post_id
            WHERE gos.occurrence_id=?
            ORDER BY CASE WHEN gos.role='primary' THEN 0 ELSE 1 END, gmp.post_date DESC, gmp.id DESC
            LIMIT 1
            """,
            (int(item.get("id") or 0),),
        )
        post_row = await post_cur.fetchone()
        item["dedup_source_text"] = collapse_ws(str((post_row["text"] if post_row else "") or ""))
        payload_url = item["fact_pack"].get("source_post_url") if isinstance(item["fact_pack"], dict) else None
        item["source_post_url"] = _source_post_url(payload_url, (post_row["source_url"] if post_row else None) or item.get("channel_url"))
        by_id[int(item["id"])] = item
    return [by_id[i] for i in ids if i in by_id]


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


async def load_visual_digest_issue(db: Database, issue_id: int | None = None) -> dict[str, Any] | None:
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
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


def _is_vk_url(url: str | None) -> bool:
    raw = _plain(url).lower()
    return bool(re.match(r"^(?:https?://)?(?:m\.)?vk\.(?:com|ru)/", raw))


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
        return f"+7 {digits[1:4]} {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
    return raw if raw.startswith("+") else None


def _ensure_https(url: str) -> str:
    raw = _plain(url)
    if not raw:
        return ""
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
    booking_url = _plain(row.get("booking_url"))
    booking_text = _plain(row.get("booking_text") or row.get("booking_line"))
    if booking_url:
        return booking_url, _is_phoneish(booking_url)
    if booking_text and _is_phoneish(booking_text):
        return booking_text, True
    source = _plain(row.get("source_post_url") or row.get("channel_url"))
    if source:
        return source, False
    if booking_text and re.search(r"https?://|t\.me|vk\.com|vk\.ru", booking_text, re.I):
        return booking_text, False
    return None, False


async def build_visual_digest_vk_text(
    rows: Sequence[Mapping[str, Any]],
    *,
    issue_id: int,
    db: Database | None = None,
    bot: Any | None = None,
    vk_api_fn: Callable[..., Awaitable[Any]] | None = None,
) -> str:
    items = [dict(r) for r in rows]
    period = _period_of(items)
    lines = [
        f"Дайджест экскурсий №{int(issue_id)}",
        "",
        f"Коротко: {len(items)} будущих экскурсий" + (f" на {period}" if period else "") + ". Смотрите карточку, а детали и запись — ниже.",
        "",
    ]
    short_cache: dict[str, str | None] = {}
    for idx, row in enumerate(items, start=1):
        title = _plain(row.get("canonical_title")) or "Экскурсия"
        link, is_phone = _primary_link(row)
        if link and is_phone:
            display = _normalize_phone(link) or _plain(link)
            line = f"{idx}. {title} — {display}"
        elif link and _is_vk_url(link):
            line = f"{idx}. {_vk_link(title, link)}"
        elif link:
            cache_key = _ensure_https(_plain(link))
            if cache_key not in short_cache:
                short_cache[cache_key] = await _shorten_external_for_vk(link, db=db, bot=bot, vk_api_fn=vk_api_fn)
            short = short_cache.get(cache_key)
            line = f"{idx}. {title} — {short or link}"
        else:
            line = f"{idx}. {title}"
        lines.append(line)
    lines.extend(["", "#экскурсии #Калининград #УхтыКалининград"])
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
) -> dict[str, Any]:
    if vk_api_fn is None:
        import main

        vk_api_fn = main._vk_api  # type: ignore[attr-defined]
    if post_to_vk_fn is None:
        import main

        post_to_vk_fn = main.post_to_vk  # type: ignore[attr-defined]
    if upload_vk_photo_bytes_fn is None:
        import main

        upload_vk_photo_bytes_fn = main.upload_vk_photo_bytes  # type: ignore[attr-defined]

    if issue_id is None:
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
    text = await build_visual_digest_vk_text(rows, issue_id=int(issue_id), db=db, bot=bot, vk_api_fn=vk_api_fn)
    card_bytes = render_visual_digest_cards(rows, issue_id=int(issue_id))
    attachments: list[str] = []
    for idx, payload in enumerate(card_bytes, start=1):
        att = await upload_vk_photo_bytes_fn(str(group), payload, db, bot, filename=f"guide_visual_digest_{issue_id}_{idx}.jpg")
        if not att:
            raise RuntimeError(f"Guide visual digest VK upload failed for card {idx}/{len(card_bytes)}")
        attachments.append(att)
    url = await post_to_vk_fn(str(group), text, db, bot, attachments=attachments, carousel=True, publish_date=publish_date)
    if not url:
        raise RuntimeError("Guide visual digest VK wall.post failed")

    post_id = _extract_wall_post_id(url)
    target_key = _vk_public_target_key(group_id=group, target=target or VISUAL_DIGEST_VK_TARGET)
    story_payload: dict[str, Any] | None = None
    if publish_stories or VISUAL_DIGEST_STORIES_ENABLED:
        if publish_date:
            story_payload = {"published": False, "reason": "wall_post_is_postponed"}
        else:
            try:
                from promo import _publish_vk_story_photo

                story_payload = await _publish_vk_story_photo(
                    db,
                    bot,
                    target_group_id=int(group),
                    image_bytes=card_bytes[0],
                    source_url=url,
                    link_text="К дайджесту",
                    include_source_link=True,
                )
                story_payload["published"] = True
            except Exception as exc:  # pragma: no cover - production integration
                logger.warning("guide_visual_digest_story_failed issue_id=%s: %s", issue_id, exc, exc_info=True)
                story_payload = {"published": False, "reason": str(exc) or type(exc).__name__}

    occurrence_ids = [int(row["id"]) for row in rows if int(row.get("id") or 0) > 0]
    async with db.raw_conn() as conn:
        await _enable_row_factory(conn)
        await ensure_visual_digest_schema(conn)
        cur = await conn.execute("SELECT published_targets_json FROM guide_digest_issue WHERE id=?", (int(issue_id),))
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
        await conn.execute(
            """
            UPDATE guide_digest_issue
            SET status='published', target_chat=?, text=?, published_at=CURRENT_TIMESTAMP, published_targets_json=?
            WHERE id=?
            """,
            (target or VISUAL_DIGEST_VK_TARGET, text, _json_dump(targets), int(issue_id)),
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


def visual_digest_enabled() -> bool:
    return (os.getenv("ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED") or "").strip().lower() in {"1", "true", "yes", "on"}
