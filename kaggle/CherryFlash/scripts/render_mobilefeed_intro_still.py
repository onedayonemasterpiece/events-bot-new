from __future__ import annotations

import json
import io
import os
import shutil
import subprocess
import zipfile
from dataclasses import dataclass
from pathlib import Path

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

DRUK_SUPER = ROOT / "video_announce" / "assets" / "DrukCyr-Super.ttf"
DRUK_BOLD = ROOT / "video_announce" / "assets" / "DrukCyr-Bold.ttf"
AKROBAT_BLACK = ROOT / "video_announce" / "assets" / "Akrobat-Black.otf"
AKROBAT_BOLD = ROOT / "video_announce" / "assets" / "Akrobat-Bold.otf"
AKROBAT_REGULAR = ROOT / "video_announce" / "assets" / "Akrobat-Regular.otf"
RO_ZNANIE_ZIP = first_existing_path(
    ROOT / "assets" / "ro_znanie.zip",
    ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie.zip",
    ROOT / "docs" / "reference" / "шрифт РО Знание.zip",
)
FONT_CACHE_DIR = TEXTURE_DIR / "_font_cache"

POSTER_DIR = first_existing_path(
    ROOT / "assets" / "posters",
    ROOT / "kaggle" / "CherryFlash" / "assets" / "posters",
    ROOT / "artifacts" / "codex" / "mobilefeed_posters" / "sparse_april",
)


@dataclass(frozen=True)
class Poster:
    event_id: int
    title: str
    date: str
    city: str
    file_name: str

    @property
    def image_path(self) -> Path:
        return POSTER_DIR / self.file_name


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


POSTERS = {
    3666: Poster(3666, "Встреча киноклуба «Декалог»", "2026-04-06", "Калининград", "3666.jpg"),
    3616: Poster(3616, 'Кинолекторий "Знать и помнить": Штурм Кёнигсберга', "2026-04-07", "Черняховск", "3616.jpg"),
    3664: Poster(3664, "Открытая встреча с директором Калининградского зоопарка", "2026-04-08", "Калининград", "3664.jpg"),
    3565: Poster(3565, "Апрельский штурм", "2026-04-09", "Калининград", "3565.jpg"),
    3292: Poster(3292, "Стендап-концерт Саши Малого", "2026-04-10", "Калининград", "3292.jpg"),
    3670: Poster(3670, "Гуси-Лебеди", "2026-04-12", "Калининград", "3670.jpg"),
}

RIBBON_ORDER = [3565, 3292, 3616, 3664, 3666, 3670]
FOCUS_EVENT_ID = 3292
SCENE_COUNT = len(RIBBON_ORDER)
CITY_CLUSTER = " • ".join(unique_in_order(POSTERS[event_id].city.upper() for event_id in RIBBON_ORDER))
CITY_CLUSTER_UI = " · ".join(unique_in_order(POSTERS[event_id].city for event_id in RIBBON_ORDER))
SCREEN_PERIOD_CLUSTER = "Апрель"
SCREEN_TYPE_CLUSTER = "кино • лекции • встречи • экскурсии"
SCREEN_TOP_COPY = f"{SCENE_COUNT} событий\n{SCREEN_TYPE_CLUSTER}"

VARIANTS = (
    Variant(
        slug="v1_choose_event",
        title_lines=("ВЫБЕРИ", "СОБЫТИЕ"),
        kicker="ПОПУЛЯРНОЕ",
        date_copy="6 • 7 • 8 • 9 • 10 • 12",
        period_copy="АПРЕЛЬ",
        city_copy="КАЛИНИНГРАД / ЧЕРНЯХОВСК",
        screen_top=SCREEN_TOP_COPY,
        screen_bottom=CITY_CLUSTER_UI,
    ),
    Variant(
        slug="v2_dont_miss",
        title_lines=("ЧТО НЕ", "ПРОПУСТИТЬ"),
        kicker="ПОПУЛЯРНОЕ",
        date_copy="6 • 7 • 8 • 9 • 10 • 12",
        period_copy="АПРЕЛЬ",
        city_copy="КАЛИНИНГРАД / ЧЕРНЯХОВСК",
        screen_top=SCREEN_TOP_COPY,
        screen_bottom=CITY_CLUSTER_UI,
    ),
)


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
            label_font = fit_font(text, CYGRE_SEMIBOLD, s(122), s(84), w - s(148))
            draw.rounded_rectangle((s(26), s(28), s(54), s(56)), radius=s(14), fill=accent)
            draw.text((s(74), 0), text, font=label_font, fill=ink)
        else:
            line_1 = lines[0]
            line_2 = lines[1]
            draw.rounded_rectangle((s(26), s(28), s(54), s(56)), radius=s(14), fill=accent)
            count_font = fit_font(line_1, CYGRE_SEMIBOLD, s(118), s(80), w - s(170))
            types_font = fit_font(line_2, CYGRE_BOOK, s(42), s(28), w - s(170))
            draw.text((s(74), 0), line_1, font=count_font, fill=ink)
            line_1_h = text_size(draw, line_1, count_font)[1]
            draw.text((s(74), max(s(6), s(2) + line_1_h)), line_2, font=types_font, fill=muted)
    else:
        if len(text) <= 22:
            size = 88
        elif len(text) <= 34:
            size = 74
        else:
            size = 62
        label_font = fit_font(text, CYGRE_BOOK, s(size), s(42), w - s(110))
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
    out = OUT_DIR / "selection_manifest_2026-04-06.md"
    lines = [
        "# MobileFeed Intro Real Selection",
        "",
        "- DB: `/workspaces/events-bot-new/artifacts/db/db_prod_snapshot_2026-04-06_114841.sqlite`",
        "- Payload topology: `sparse_same_month`",
        "- Date copy: `6 • 7 • 8 • 9 • 10 • 12` / `АПРЕЛЬ`",
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
