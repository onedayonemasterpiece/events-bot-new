from __future__ import annotations

import json
import io
import os
import re
import shutil
import subprocess
import sys
import zipfile
import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from PIL import Image, ImageColor, ImageDraw, ImageEnhance, ImageFilter, ImageFont


def resolve_root() -> Path:
    candidates = [
        os.environ.get("CHERRYFLASH_ROOT"),
        os.environ.get("PROJECT_ROOT"),
    ]
    for candidate in candidates:
        if candidate:
            path = Path(candidate).expanduser().resolve()
            if path.exists():
                return path
    return Path(__file__).resolve().parents[1]


ROOT = resolve_root()
ARTIFACTS_ROOT = Path(os.environ.get("CHERRYFLASH_ARTIFACTS_ROOT", str(ROOT / "artifacts"))).expanduser()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from video_announce.cherryflash_text import event_count_label


def first_existing_path(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def resolve_blender() -> Path:
    candidates = [
        os.environ.get("BLENDER_BIN"),
        shutil.which("blender"),
        str(ROOT / ".cache" / "blender" / "blender-4.5.4-linux-x64" / "blender"),
        "/tmp/blender-4.5.4-linux-x64/blender",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return Path(candidate)
    raise FileNotFoundError("Blender binary not found. Set BLENDER_BIN or install Blender into .cache/blender.")


BLENDER = resolve_blender()
BLENDER_SCRIPT = first_existing_path(
    ROOT / "mobilefeed_intro_still.py",
    ROOT / "kaggle" / "CherryFlash" / "mobilefeed_intro_still.py",
)
PHONE_MODEL = first_existing_path(
    ROOT / "assets" / "iphone_16_pro_max.glb",
    ROOT / "kaggle" / "CherryFlash" / "assets" / "iphone_16_pro_max.glb",
    ROOT / "docs" / "reference" / "iphone_16_pro_max.glb",
)
HDRI = Path(
    os.environ.get(
        "CHERRYFLASH_HDRI",
        str(ROOT / "docs" / "reference" / "phone_shop_1k.exr"),
    )
).expanduser()

OUT_DIR = ARTIFACTS_ROOT / "codex" / "mobilefeed_intro_still"
TEXTURE_DIR = OUT_DIR / "textures"
CONFIG_DIR = OUT_DIR / "configs"

W = 1080
H = 1920
SAFE_TOP = 204
SAFE_SIDE = 84
SAFE_BOTTOM = 260

DRUK_SUPER = first_existing_path(
    ROOT / "video_announce" / "assets" / "DrukCyr-Super.ttf",
    ROOT.parent / "assets" / "DrukCyr-Super.ttf",
    ROOT / "assets" / "DrukCyr-Super.ttf",
)
DRUK_BOLD = first_existing_path(
    ROOT / "video_announce" / "assets" / "DrukCyr-Bold.ttf",
    ROOT.parent / "assets" / "DrukCyr-Bold.ttf",
    ROOT / "assets" / "DrukCyr-Bold.ttf",
)
AKROBAT_BLACK = first_existing_path(
    ROOT / "video_announce" / "assets" / "Akrobat-Black.otf",
    ROOT.parent / "assets" / "Akrobat-Black.otf",
    ROOT / "assets" / "Akrobat-Black.otf",
)
AKROBAT_BOLD = first_existing_path(
    ROOT / "video_announce" / "assets" / "Akrobat-Bold.otf",
    ROOT.parent / "assets" / "Akrobat-Bold.otf",
    ROOT / "assets" / "Akrobat-Bold.otf",
)
AKROBAT_REGULAR = first_existing_path(
    ROOT / "video_announce" / "assets" / "Akrobat-Regular.otf",
    ROOT.parent / "assets" / "Akrobat-Regular.otf",
    ROOT / "assets" / "Akrobat-Regular.otf",
)
RO_ZNANIE_ZIP = first_existing_path(
    ROOT / "assets" / "ro_znanie.zip",
    ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie.zip",
    ROOT / "docs" / "reference" / "шрифт РО Знание.zip",
)
RO_ZNANIE_FONT_DIR = first_existing_path(
    ROOT / "assets" / "ro_znanie_fonts",
    ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts",
)
FONT_CACHE_DIR = TEXTURE_DIR / "_font_cache"
POSTER_CACHE_DIR = TEXTURE_DIR / "_poster_cache"

POSTER_DIR = first_existing_path(
    ROOT / "assets" / "posters",
    ROOT / "kaggle" / "CherryFlash" / "assets" / "posters",
    ROOT / "artifacts" / "codex" / "mobilefeed_posters" / "sparse_april",
)
SELECTION_MANIFEST_PATH = first_existing_path(
    ROOT / "assets" / "cherryflash_selection.json",
    ROOT / "kaggle" / "CherryFlash" / "assets" / "cherryflash_selection.json",
)


@dataclass(frozen=True)
class Poster:
    event_id: int
    title: str
    date: str
    city: str
    file_name: str
    file_candidates: tuple[str, ...] = ()
    time: str = ""
    location_name: str = ""

    @property
    def image_path(self) -> Path:
        candidates = list(self.file_candidates) or [self.file_name]
        for candidate in candidates:
            raw = str(candidate or "").strip()
            if not raw:
                continue
            if raw.startswith("http://") or raw.startswith("https://"):
                resolved = _download_runtime_poster(raw)
                if resolved is not None:
                    return resolved
                continue
            local_name = Path(raw).name
            local_path = POSTER_DIR / local_name
            if local_path.exists():
                return local_path
        fallback = POSTER_DIR / self.file_name
        if fallback.exists():
            return fallback
        raise FileNotFoundError(
            f"CherryFlash poster asset is missing event_id={self.event_id} "
            f"file_name={self.file_name} candidates={candidates}"
        )


@dataclass(frozen=True)
class Variant:
    slug: str
    title_lines: tuple[str, ...]
    kicker: str
    date_copy: str
    period_copy: str
    city_copy: str
    screen_top: str
    screen_bottom: str


def unique_in_order(values):
    seen = set()
    ordered = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _download_runtime_poster(url: str) -> Path | None:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        suffix = ".jpg"
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    cached = POSTER_CACHE_DIR / f"{digest}{suffix}"
    if cached.exists():
        return cached
    POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        },
    )
    try:
        with urlopen(req, timeout=20) as resp:
            data = resp.read()
    except Exception:
        return None
    if not data:
        return None
    cached.write_bytes(data)
    return cached


MONTHS_NOMINATIVE = {
    1: "ЯНВАРЬ",
    2: "ФЕВРАЛЬ",
    3: "МАРТ",
    4: "АПРЕЛЬ",
    5: "МАЙ",
    6: "ИЮНЬ",
    7: "ИЮЛЬ",
    8: "АВГУСТ",
    9: "СЕНТЯБРЬ",
    10: "ОКТЯБРЬ",
    11: "НОЯБРЬ",
    12: "ДЕКАБРЬ",
}


def _fallback_posters() -> dict[int, Poster]:
    return {
        3666: Poster(
            3666,
            "Встреча киноклуба «Декалог»",
            "2026-04-06",
            "Калининград",
            "3666.jpg",
            time="19:30",
            location_name="Бар Суспирия, Коперника 21, Калининград",
        ),
        3616: Poster(
            3616,
            'Кинолекторий "Знать и помнить": Штурм Кёнигсберга',
            "2026-04-07",
            "Черняховск",
            "3616.jpg",
            time="14:00",
            location_name="Библиротека им. Лунина, Калинина 4, Черняховск",
        ),
        3664: Poster(
            3664,
            "Открытая встреча с директором Калининградского зоопарка",
            "2026-04-08",
            "Калининград",
            "3664.jpg",
            time="16:00",
            location_name="Музей Мирового океана, наб. Петра Великого 1, Калининград",
        ),
        3565: Poster(
            3565,
            "Апрельский штурм",
            "2026-04-09",
            "Калининград",
            "3565.jpg",
            time="15:00",
            location_name="Историко-художественный музей, Клиническая 21, Калининград",
        ),
        3292: Poster(
            3292,
            "Стендап-концерт Саши Малого",
            "2026-04-10",
            "Калининград",
            "3292.jpg",
            time="20:00",
            location_name="Дом железнодорожников (ДКЖ), Железнодорожная 2, Калининград",
        ),
        3670: Poster(
            3670,
            "Гуси-Лебеди",
            "2026-04-12",
            "Калининград",
            "3670.jpg",
            time="12:00",
            location_name="Театр кукол, Победы 1А, Калининград",
        ),
    }


def _parse_iso_date(value: str) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception:
        return None


def _month_period_copy(dates: list[date]) -> str:
    months = unique_in_order(MONTHS_NOMINATIVE.get(item.month, "") for item in dates if item)
    months = [item for item in months if item]
    return " • ".join(months) if months else "АПРЕЛЬ"


def _date_copy(dates: list[date]) -> str:
    grouped: list[str] = []
    current_month: int | None = None
    current_days: list[str] = []
    for item in dates:
        if current_month is None:
            current_month = item.month
        if item.month != current_month:
            if current_days:
                grouped.append(" • ".join(current_days))
            current_month = item.month
            current_days = []
        current_days.append(str(item.day))
    if current_days:
        grouped.append(" • ".join(current_days))
    return " / ".join(grouped) if grouped else "6 • 7 • 8 • 9 • 10 • 12"


def _type_cluster(items: list[dict]) -> str:
    mapping = {
        "концерт": "концерты",
        "лекция": "лекции",
        "спектакль": "театр",
        "образование": "образование",
        "экскурсия": "экскурсии",
        "встреча": "встречи",
        "кино": "кино",
    }
    labels: list[str] = []
    for item in items:
        raw = str(item.get("event_type") or "").strip().lower()
        if not raw:
            continue
        label = mapping.get(raw, raw)
        if label not in labels:
            labels.append(label)
        if len(labels) >= 4:
            break
    if not labels:
        return "популярное сейчас"
    return " • ".join(labels)


def _default_variants(
    *,
    scene_count: int,
    date_copy: str,
    period_copy: str,
    city_copy: str,
    screen_top: str,
    screen_bottom: str,
) -> tuple[Variant, ...]:
    return (
        Variant(
            slug="v1_choose_event",
            title_lines=("ВЫБЕРИ", "СОБЫТИЕ"),
            kicker="ПОПУЛЯРНОЕ",
            date_copy=date_copy,
            period_copy=period_copy,
            city_copy=city_copy,
            screen_top=screen_top,
            screen_bottom=screen_bottom,
        ),
        Variant(
            slug="v2_dont_miss",
            title_lines=("ЧТО НЕ", "ПРОПУСТИТЬ"),
            kicker="ПОПУЛЯРНОЕ",
            date_copy=date_copy,
            period_copy=period_copy,
            city_copy=city_copy,
            screen_top=screen_top,
            screen_bottom=screen_bottom,
        ),
    )


def _load_runtime_selection() -> tuple[dict[int, Poster], list[int], int, tuple[Variant, ...], dict]:
    fallback_posters = _fallback_posters()
    fallback_ribbon = [3565, 3292, 3616, 3664, 3666, 3670]
    fallback_focus = 3292
    fallback_scene_count = len(fallback_ribbon)
    fallback_city_ui = " · ".join(
        unique_in_order(fallback_posters[event_id].city for event_id in fallback_ribbon)
    )
    fallback_variants = _default_variants(
        scene_count=fallback_scene_count,
        date_copy="6 • 7 • 8 • 9 • 10 • 12",
        period_copy="АПРЕЛЬ",
        city_copy="КАЛИНИНГРАД / ЧЕРНЯХОВСК",
        screen_top=f"{event_count_label(fallback_scene_count)}\nкино • лекции • встречи • экскурсии",
        screen_bottom=fallback_city_ui,
    )
    if not SELECTION_MANIFEST_PATH.exists():
        return fallback_posters, fallback_ribbon, fallback_focus, fallback_variants, {}

    raw = json.loads(SELECTION_MANIFEST_PATH.read_text(encoding="utf-8"))
    events = list(raw.get("events") or [])
    posters: dict[int, Poster] = {}
    for item in events:
        try:
            event_id = int(item["event_id"])
        except Exception:
            continue
        poster_file = str(item.get("poster_file") or "").strip()
        if not poster_file:
            poster_file = f"{event_id}.jpg"
        poster_candidates = item.get("poster_candidates") or []
        if isinstance(poster_candidates, str):
            poster_candidates = [poster_candidates]
        cleaned_candidates = tuple(
            str(candidate or "").strip()
            for candidate in poster_candidates
            if str(candidate or "").strip()
        )
        raw_date = str(item.get("date_iso") or item.get("date") or "").strip()
        posters[event_id] = Poster(
            event_id=event_id,
            title=str(item.get("title") or "").strip() or f"event {event_id}",
            date=raw_date,
            city=str(item.get("city") or "").strip(),
            file_name=Path(poster_file).name,
            file_candidates=cleaned_candidates,
            time=str(item.get("time") or "").strip(),
            location_name=str(item.get("location_name") or "").strip(),
        )
    ribbon_order = [int(item) for item in list(raw.get("ribbon_order") or []) if int(item) in posters]
    focus_event_id = int(raw.get("focus_event_id") or 0)
    if not ribbon_order:
        ribbon_order = list(fallback_ribbon)
    if focus_event_id not in posters:
        focus_event_id = ribbon_order[1] if len(ribbon_order) > 1 and ribbon_order[1] in posters else (
            next(iter(posters)) if posters else fallback_focus
        )
    if not posters:
        return fallback_posters, fallback_ribbon, fallback_focus, fallback_variants, {}

    ordered_events = [posters[event_id] for event_id in ribbon_order if event_id in posters]
    date_points = [item for item in (_parse_iso_date(poster.date) for poster in ordered_events) if item]
    city_ui = " · ".join(unique_in_order(poster.city for poster in ordered_events if poster.city))
    city_copy = " / ".join(unique_in_order(poster.city.upper() for poster in ordered_events if poster.city))
    type_cluster = _type_cluster(events)
    scene_count = len(ribbon_order)
    variant_overrides = dict(raw.get("variant") or {})
    variants = _default_variants(
        scene_count=scene_count,
        date_copy=str(variant_overrides.get("date_copy") or _date_copy(date_points)),
        period_copy=str(variant_overrides.get("period_copy") or _month_period_copy(date_points)),
        city_copy=str(variant_overrides.get("city_copy") or city_copy or "КАЛИНИНГРАД"),
        screen_top=str(
            variant_overrides.get("screen_top")
            or f"{event_count_label(scene_count)}\n{type_cluster}"
        ),
        screen_bottom=str(variant_overrides.get("screen_bottom") or city_ui or "Калининград"),
    )
    return posters, ribbon_order, focus_event_id, variants, raw


POSTERS, RIBBON_ORDER, FOCUS_EVENT_ID, VARIANTS, SELECTION_MANIFEST = _load_runtime_selection()
SCENE_COUNT = len(RIBBON_ORDER)
CITY_CLUSTER = " • ".join(unique_in_order(POSTERS[event_id].city.upper() for event_id in RIBBON_ORDER))
CITY_CLUSTER_UI = " · ".join(unique_in_order(POSTERS[event_id].city for event_id in RIBBON_ORDER))
SCREEN_PERIOD_CLUSTER = VARIANTS[0].period_copy
SCREEN_TYPE_CLUSTER = VARIANTS[0].screen_top.split("\n", 1)[1] if "\n" in VARIANTS[0].screen_top else ""
SCREEN_TOP_COPY = VARIANTS[0].screen_top


def font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(path), size=size)


def ensure_dirs():
    TEXTURE_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    FONT_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def ensure_ro_znanie_fonts() -> tuple[Path, Path, Path, Path, Path, Path]:
    FONT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    regular = FONT_CACHE_DIR / "Cygre-Regular.ttf"
    book = FONT_CACHE_DIR / "Cygre-Book.ttf"
    medium = FONT_CACHE_DIR / "Cygre-Medium.ttf"
    semibold = FONT_CACHE_DIR / "Cygre-SemiBold.ttf"
    bold = FONT_CACHE_DIR / "Cygre-Bold.ttf"
    extra_bold = FONT_CACHE_DIR / "Cygre-ExtraBold.ttf"
    if all(path.exists() for path in (regular, book, medium, semibold, bold, extra_bold)):
        return regular, book, medium, semibold, bold, extra_bold
    extracted_mapping = {
        RO_ZNANIE_FONT_DIR / "Cygre-Regular.ttf": regular,
        RO_ZNANIE_FONT_DIR / "Cygre-Book.ttf": book,
        RO_ZNANIE_FONT_DIR / "Cygre-Medium.ttf": medium,
        RO_ZNANIE_FONT_DIR / "Cygre-SemiBold.ttf": semibold,
        RO_ZNANIE_FONT_DIR / "Cygre-Bold.ttf": bold,
        RO_ZNANIE_FONT_DIR / "Cygre-ExtraBold.ttf": extra_bold,
    }
    if all(src.exists() for src in extracted_mapping):
        for src, dest in extracted_mapping.items():
            if not dest.exists():
                dest.write_bytes(src.read_bytes())
        return regular, book, medium, semibold, bold, extra_bold
    if not RO_ZNANIE_ZIP.exists():
        return AKROBAT_REGULAR, AKROBAT_REGULAR, AKROBAT_BOLD, AKROBAT_BOLD, AKROBAT_BLACK, AKROBAT_BLACK
    with zipfile.ZipFile(RO_ZNANIE_ZIP) as outer:
        nested = outer.read("шрифт/cygre_default.zip")
    with zipfile.ZipFile(io.BytesIO(nested)) as inner:
        mapping = {
            "cygre_default/Cygre-Regular.ttf": regular,
            "cygre_default/Cygre-Book.ttf": book,
            "cygre_default/Cygre-Medium.ttf": medium,
            "cygre_default/Cygre-SemiBold.ttf": semibold,
            "cygre_default/Cygre-Bold.ttf": bold,
            "cygre_default/Cygre-ExtraBold.ttf": extra_bold,
        }
        for archive_name, output_path in mapping.items():
            if output_path.exists():
                continue
            output_path.write_bytes(inner.read(archive_name))
    return regular, book, medium, semibold, bold, extra_bold


CYGRE_REGULAR, CYGRE_BOOK, CYGRE_MEDIUM, CYGRE_SEMIBOLD, CYGRE_BOLD, CYGRE_EXTRA_BOLD = ensure_ro_znanie_fonts()


def atlas_and_meta():
    target_height = 2400
    resized = []
    total_width = 0
    for event_id in RIBBON_ORDER:
        poster = POSTERS[event_id]
        with Image.open(poster.image_path).convert("RGB") as image:
            width, height = image.size
            scaled_width = round(width * target_height / height)
            resized_image = image.resize((scaled_width, target_height), Image.Resampling.LANCZOS)
            resized_image = ImageEnhance.Color(resized_image).enhance(1.44)
            resized_image = ImageEnhance.Contrast(resized_image).enhance(1.18)
            resized_image = ImageEnhance.Brightness(resized_image).enhance(1.06)
            resized_image = ImageEnhance.Sharpness(resized_image).enhance(1.18)
            resized_image = resized_image.filter(ImageFilter.UnsharpMask(radius=1.2, percent=138, threshold=2))
            resized.append((poster, resized_image))
            total_width += scaled_width
    atlas = Image.new("RGB", (total_width, target_height), (255, 255, 255))
    x = 0
    panels = []
    focus = None
    for poster, image in resized:
        atlas.paste(image, (x, 0))
        panel = {
            "event_id": poster.event_id,
            "title": poster.title,
            "date": poster.date,
            "city": poster.city,
            "x": x,
            "width": image.width,
            "height": image.height,
        }
        panels.append(panel)
        if poster.event_id == FOCUS_EVENT_ID:
            focus = panel
        x += image.width
    if focus is None:
        raise RuntimeError("Focus event not found in ribbon order")
    atlas_path = TEXTURE_DIR / "mobilefeed_sparse_april_atlas.png"
    atlas.save(atlas_path)
    meta = {
        "atlas_width": atlas.width,
        "atlas_height": atlas.height,
        "focus_event_id": FOCUS_EVENT_ID,
        "focus_x": focus["x"],
        "focus_width": focus["width"],
        "focus_height": focus["height"],
        "focus_center_u": (focus["x"] + focus["width"] / 2.0) / atlas.width,
        "focus_width_u": focus["width"] / atlas.width,
        "focus_aspect": focus["width"] / focus["height"],
        "panels": panels,
    }
    return atlas_path, meta


def make_shadow_texture(path: Path):
    w, h = 2200, 1400
    image = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    cx, cy = w // 2, h // 2
    # Outer soft halo — subtle, just reinforces contact zone
    draw.ellipse((cx - 900, cy - 480, cx + 900, cy + 580), fill=(0, 0, 0, 50))
    # Middle gradient
    draw.ellipse((cx - 600, cy - 340, cx + 600, cy + 340), fill=(0, 0, 0, 95))
    # Contact core — tight AO reinforcement
    draw.ellipse((cx - 380, cy - 220, cx + 380, cy + 140), fill=(0, 0, 0, 140))
    # Contact line — thin dark band right at surface contact
    draw.ellipse((cx - 260, cy - 140, cx + 260, cy + 40), fill=(0, 0, 0, 170))
    image = image.filter(ImageFilter.GaussianBlur(20))
    image.save(path)


def text_size(draw: ImageDraw.ImageDraw, text: str, fnt: ImageFont.FreeTypeFont):
    left, top, right, bottom = draw.textbbox((0, 0), text, font=fnt)
    return right - left, bottom - top


def draw_right(draw: ImageDraw.ImageDraw, x: int, y: int, text: str, fnt, fill):
    width, _ = text_size(draw, text, fnt)
    draw.text((x - width, y), text, font=fnt, fill=fill)


def make_overlay_texture(variant: Variant, out_path: Path):
    image = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    max_title_width = 620
    max_title_height = 360
    title_size = 188
    while title_size >= 118:
        probe_font = font(DRUK_SUPER, title_size)
        widths = [text_size(draw, line, probe_font)[0] for line in variant.title_lines]
        heights = [text_size(draw, line, probe_font)[1] for line in variant.title_lines]
        total_height = sum(heights) + max(0, len(variant.title_lines) - 1) * max(6, title_size // 18)
        if max(widths) <= max_title_width and total_height <= max_title_height:
            break
        title_size -= 8
    title_font = font(DRUK_SUPER, title_size)
    kicker_font = font(AKROBAT_BLACK, 56)
    dates_font = font(AKROBAT_BLACK, 40)
    city_font = font(CYGRE_MEDIUM, 30)
    accent = ImageColor.getrgb("#F45B1F")
    ink = ImageColor.getrgb("#11110F")
    x = SAFE_SIDE
    y = SAFE_TOP
    line_gap = max(10, title_size // 16)
    for line in variant.title_lines:
        draw.text((x, y), line, font=title_font, fill=ink)
        _, h = text_size(draw, line, title_font)
        y += h + line_gap
    kicker_y = y + 14
    draw.text((x + 4, kicker_y), variant.kicker, font=kicker_font, fill=accent)
    draw.rectangle((x + 4, kicker_y + 68, x + 300, kicker_y + 80), fill=accent)
    draw_right(draw, W - SAFE_SIDE, SAFE_TOP + 14, variant.date_copy, dates_font, fill=ink)
    draw_right(draw, W - SAFE_SIDE, SAFE_TOP + 64, variant.period_copy, kicker_font, fill=accent)
    draw_right(draw, W - SAFE_SIDE, H - SAFE_BOTTOM - 18, variant.city_copy, city_font, fill=ink)
    image.save(out_path)


def make_screen_texture(variant: Variant, out_path: Path):
    w, h = 1320, 2860
    image = Image.new("RGBA", (w, h), ImageColor.getrgb("#07090D") + (255,))
    draw = ImageDraw.Draw(image)
    accent = ImageColor.getrgb("#F45B1F")
    glow_cool = ImageColor.getrgb("#1A2431")
    glow_soft = ImageColor.getrgb("#0F1520")
    divider = ImageColor.getrgb("#2B3644")
    text_support = ImageColor.getrgb("#A9B6C7")

    top_glow = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    top_draw = ImageDraw.Draw(top_glow)
    top_draw.ellipse((-180, -120, w + 120, 720), fill=glow_cool + (72,))
    top_glow = top_glow.filter(ImageFilter.GaussianBlur(110))
    image = Image.alpha_composite(image, top_glow)

    bottom_glow = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    bottom_draw = ImageDraw.Draw(bottom_glow)
    bottom_draw.ellipse((-120, h - 720, w + 160, h + 120), fill=glow_soft + (58,))
    bottom_glow = bottom_glow.filter(ImageFilter.GaussianBlur(130))
    image = Image.alpha_composite(image, bottom_glow)

    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((w // 2 - 116, 82, w // 2 + 116, 162), radius=40, fill=ImageColor.getrgb("#020304"))
    draw.rounded_rectangle((106, 230, 194, 242), radius=6, fill=accent)
    draw.rectangle((106, 304, w - 106, 308), fill=divider)
    draw.rectangle((106, h - 306, w - 106, h - 302), fill=divider)
    draw.rounded_rectangle((106, h - 242, 420, h - 230), radius=6, fill=text_support)
    image = image.convert("RGB")
    image.save(out_path)


def make_screen_label_texture(text: str, out_path: Path, *, position: str):
    if position not in {"top", "bottom"}:
        raise ValueError(f"Unsupported screen label position: {position}")
    texture_scale = 2
    base_w, base_h = (1180, 352) if position == "top" else (1180, 170)
    w, h = base_w * texture_scale, base_h * texture_scale
    image = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    ink = ImageColor.getrgb("#F3F7FE")
    muted = ImageColor.getrgb("#A8B5C6")
    accent = ImageColor.getrgb("#F45B1F")
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    def s(value: int) -> int:
        return round(value * texture_scale)

    def fit_font(line: str, font_path: Path, start_size: int, min_size: int, max_width: int):
        size = start_size
        while size >= min_size:
            candidate = font(font_path, size)
            if text_size(draw, line, candidate)[0] <= max_width:
                return candidate
            size -= 4
        return font(font_path, min_size)

    if position == "top":
        if len(lines) <= 1:
            label_font = fit_font(text, CYGRE_BOLD, s(130), s(92), w - s(156))
            draw.rounded_rectangle((s(26), s(28), s(54), s(56)), radius=s(14), fill=accent)
            draw.text((s(74), 0), text, font=label_font, fill=ink)
        else:
            line_1 = lines[0]
            line_2 = lines[1]
            draw.rounded_rectangle((s(26), s(28), s(54), s(56)), radius=s(14), fill=accent)
            count_font = fit_font(line_1, CYGRE_BOLD, s(128), s(90), w - s(184))
            types_font = fit_font(line_2, CYGRE_SEMIBOLD, s(50), s(34), w - s(184))
            draw.text((s(74), 0), line_1, font=count_font, fill=ink)
            line_1_h = text_size(draw, line_1, count_font)[1]
            draw.text((s(74), max(s(8), s(4) + line_1_h)), line_2, font=types_font, fill=muted)
    else:
        if len(text) <= 22:
            size = 96
        elif len(text) <= 34:
            size = 82
        else:
            size = 70
        label_font = fit_font(text, CYGRE_BOLD, s(size), s(48), w - s(110))
        draw.rounded_rectangle((s(18), s(22), s(94), s(34)), radius=s(6), fill=accent)
        draw.text((s(18), s(42)), text, font=label_font, fill=ink)
    image.save(out_path)


def build_variant_config(
    variant: Variant,
    *,
    atlas_path: Path,
    ribbon_meta: dict,
    shadow_path: Path,
    overlay_path: Path,
    screen_path: Path,
    screen_top_label_path: Path,
    screen_bottom_label_path: Path,
    output_path: Path,
):
    return {
        "res_x": W,
        "res_y": H,
        "samples": 36,
        "use_denoising": True,
        "denoiser": "OPENIMAGEDENOISE",
        "use_adaptive_sampling": True,
        "adaptive_threshold": 0.014,
        "sample_clamp_direct": 5.0,
        "sample_clamp_indirect": 1.0,
        "view_transform": "AgX",
        "look": "High Contrast",
        "exposure": -0.22,
        "phone_model": str(PHONE_MODEL),
        "hdri_path": str(HDRI),
        "hdri_strength": 0.010,
        "hdri_rot_deg": 116.0,
        "overlay_texture": str(overlay_path),
        "screen_texture": str(screen_path),
        "screen_top_label_texture": str(screen_top_label_path),
        "screen_bottom_label_texture": str(screen_bottom_label_path),
        "ribbon_texture": str(atlas_path),
        "ribbon_meta": ribbon_meta,
        "shadow_texture": str(shadow_path),
        "backdrop_location": (0.0, 0.0, -0.032),
        "backdrop_rotation_deg": (0.0, 0.0, 0.0),
        "backdrop_scale": (120.0, 120.0, 1.0),
        "backdrop_color": "#E7E6E1",
        "camera_background_color": "#E7E6E1",
        "backdrop_emission_strength": 0.0,
        "backdrop_roughness": 1.0,
        "backdrop_bump_strength": 0.0,
        "tonal_transition_key_multiplier": 0.72,
        "tonal_transition_fill_multiplier": 0.10,
        "tonal_transition_top_multiplier": 0.20,
        "tonal_transition_edge_multiplier": 1.10,
        "max_bounces": 4,
        "diffuse_bounces": 1,
        "glossy_bounces": 3,
        "transmission_bounces": 2,
        "transparent_max_bounces": 4,
        "overlay_location": (1.70, 0.0, 0.42),
        "overlay_scale": (4.9, 7.15, 1.0),
        "scene_overlay_in_blender": False,
        "camera_location": (2.26, -3.86, 3.52),
        "camera_target": (0.04, -0.64, 0.05),
        "camera_lens_mm": 64.0,
        "key_light_location": (-4.20, -2.04, 2.18),
        "key_light_target": (0.18, -0.76, 0.04),
        "key_light_energy": 5200,
        "key_light_size": (0.82, 0.66),
        "key_light_color": "#FFF1DE",
        "fill_light_location": (5.2, -4.8, 3.8),
        "fill_light_target": (0.08, -0.78, 0.02),
        "fill_light_energy": 2.5,
        "fill_light_size": (5.8, 4.8),
        "fill_light_color": "#DBE5F1",
        "top_light_location": (-0.1, 2.7, 4.8),
        "top_light_target": (0.04, -0.52, 0.03),
        "top_light_energy": 90,
        "top_light_size": (4.6, 4.1),
        "top_light_color": "#FFF2E0",
        "edge_light_location": (4.55, 1.42, 2.36),
        "edge_light_target": (0.22, -0.67, 0.12),
        "edge_light_energy": 860,
        "edge_light_size": (0.64, 0.24),
        "edge_light_color": "#EDF4FF",
        "phone_location": (0.03, -0.62, 0.04),
        "phone_rotation_deg": (8.0, 90.0, 154.0),
        "phone_scale": (1.90, 1.90, 1.90),
        "ribbon_camber": 0.012,
        "ribbon_fold_amplitude": 0.020,
        "ribbon_vertical_flutter": 0.006,
        "ribbon_left_sag": 0.072,
        "ribbon_right_sag": 0.076,
        "ribbon_screen_cushion": 0.006,
        "ribbon_desk_gap": 0.006,
        "ribbon_screen_gap": 0.018,
        "ribbon_tension_bias": 0.22,
        "ribbon_flat_run_scale": 0.78,
        "ribbon_lift_run_scale": 0.16,
        "ribbon_rise_height": 0.020,
        "ribbon_cols": 180,
        "ribbon_rows": 10,
        "ribbon_thickness": 0.0044,
        "ribbon_subdiv": 2,
        "screen_label_width_ratio": 0.62,
        "screen_top_label_height_ratio": 0.126,
        "screen_bottom_label_height_ratio": 0.086,
        "screen_top_label_center_ratio": 0.442,
        "screen_bottom_label_center_ratio": 0.452,
        "screen_label_normal_offset": 0.012,
        "screen_top_label_x_offset_ratio": -0.145,
        "screen_bottom_label_x_offset_ratio": -0.08,
        "screen_top_label_lift_ratio": 0.026,
        "screen_bottom_label_lift_ratio": 0.0,
        "screen_notch_safe_ratio": 0.138,
        "phone_clearance_margin_u": 0.055,
        "phone_clearance_margin_v": 0.072,
        "phone_clearance_n": 0.022,
        "use_shadow_plane": True,
        "shadow_scale_x": 1.72,
        "shadow_scale_y": 1.18,
        "shadow_offset_x": 0.06,
        "shadow_offset_y": -0.03,
        "shadow_z_offset": 0.0008,
        "shadow_rotation_deg": -21.5,
        "animation": {
            "tonal_transition_start_frame": 78,
            "tonal_transition_end_frame": 106,
            "tonal_transition_from": "#E7E6E1",
            "tonal_transition_to": "#050608",
            "tonal_transition_emission_strength_end": 0.0,
        },
        "output": str(output_path),
    }


def render_variant(variant: Variant, atlas_path: Path, ribbon_meta: dict, shadow_path: Path, *, config_overrides: dict | None = None):
    overlay_path = TEXTURE_DIR / f"{variant.slug}_overlay.png"
    screen_path = TEXTURE_DIR / f"{variant.slug}_screen.png"
    screen_top_label_path = TEXTURE_DIR / f"{variant.slug}_screen_top.png"
    screen_bottom_label_path = TEXTURE_DIR / f"{variant.slug}_screen_bottom.png"
    output_path = OUT_DIR / f"{variant.slug}.png"
    config_path = CONFIG_DIR / f"{variant.slug}.json"
    make_overlay_texture(variant, overlay_path)
    make_screen_texture(variant, screen_path)
    make_screen_label_texture(variant.screen_top, screen_top_label_path, position="top")
    make_screen_label_texture(variant.screen_bottom, screen_bottom_label_path, position="bottom")

    cfg = build_variant_config(
        variant,
        atlas_path=atlas_path,
        ribbon_meta=ribbon_meta,
        shadow_path=shadow_path,
        overlay_path=overlay_path,
        screen_path=screen_path,
        screen_top_label_path=screen_top_label_path,
        screen_bottom_label_path=screen_bottom_label_path,
        output_path=output_path,
    )
    if config_overrides:
        cfg.update(config_overrides)
    config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    cmd = [
        str(BLENDER),
        "-b",
        "-P",
        str(BLENDER_SCRIPT),
        "--",
        "--config",
        str(config_path),
    ]
    subprocess.run(cmd, check=True)
    with Image.open(output_path).convert("RGBA") as render_im:
        with Image.open(overlay_path).convert("RGBA") as overlay_im:
            if overlay_im.size != render_im.size:
                overlay_im = overlay_im.resize(render_im.size, Image.Resampling.LANCZOS)
            render_im = Image.alpha_composite(render_im, overlay_im)
        render_im.save(output_path)
    return output_path


def make_index(paths: list[Path]):
    thumbs = []
    labels = []
    for path in paths:
        with Image.open(path).convert("RGB") as image:
            thumbs.append(image.resize((340, round(340 * image.height / image.width)), Image.Resampling.LANCZOS))
        labels.append(path.stem)
    label_font = font(AKROBAT_BOLD, 28)
    margin = 40
    label_h = 48
    total_width = margin + len(thumbs) * (340 + margin)
    max_h = max(im.height for im in thumbs)
    canvas = Image.new("RGB", (total_width, margin * 2 + max_h + label_h), ImageColor.getrgb("#F1ECE5"))
    draw = ImageDraw.Draw(canvas)
    x = margin
    for thumb, label in zip(thumbs, labels):
        canvas.paste(thumb, (x, margin))
        draw.text((x, margin + max_h + 8), label, font=label_font, fill=ImageColor.getrgb("#11110F"))
        x += 340 + margin
    out = OUT_DIR / "index.png"
    canvas.save(out)
    return out


def write_manifest(atlas_meta: dict):
    source_db = str(SELECTION_MANIFEST.get("db_snapshot") or "runtime payload")
    out = OUT_DIR / f"selection_manifest_{date.today().isoformat()}.md"
    lines = [
        "# MobileFeed Intro Real Selection",
        "",
        f"- DB / source: `{source_db}`",
        f"- Payload topology: `{SELECTION_MANIFEST.get('date_topology') or 'runtime'}`",
        f"- Date copy: `{VARIANTS[0].date_copy}` / `{VARIANTS[0].period_copy}`",
        "- Ribbon order is art-directed but uses only real selected events.",
        f"- Focus / handoff poster: `{FOCUS_EVENT_ID}`",
        "",
        "## Ribbon order",
        "",
    ]
    for panel in atlas_meta["panels"]:
        lines.append(f"- `{panel['event_id']}` · `{panel['date']}` · {panel['title']}")
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    import sys
    slug_filter = sys.argv[1] if len(sys.argv) > 1 else None
    ensure_dirs()
    if not PHONE_MODEL.exists():
        raise FileNotFoundError(f"Phone model not found: {PHONE_MODEL}")
    atlas_path, ribbon_meta = atlas_and_meta()
    shadow_path = TEXTURE_DIR / "mobilefeed_shadow.png"
    make_shadow_texture(shadow_path)
    variants = [v for v in VARIANTS if v.slug == slug_filter] if slug_filter else list(VARIANTS)
    outputs = [render_variant(variant, atlas_path, ribbon_meta, shadow_path) for variant in variants]
    make_index(outputs)
    write_manifest(ribbon_meta)


if __name__ == "__main__":
    main()
