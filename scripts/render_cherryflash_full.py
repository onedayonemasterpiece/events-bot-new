from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from PIL import Image, ImageColor, ImageDraw, ImageFilter, ImageOps

AudioFileClip = None
MultiplyVolume = None
volumex_fx = None


def resolve_root() -> Path:
    for candidate in (
        os.environ.get("CHERRYFLASH_ROOT"),
        os.environ.get("PROJECT_ROOT"),
    ):
        if candidate:
            path = Path(candidate).expanduser().resolve()
            if path.exists():
                return path
    return Path(__file__).resolve().parents[1]


ROOT = resolve_root()
ARTIFACTS_ROOT = Path(
    os.environ.get("CHERRYFLASH_ARTIFACTS_ROOT", str(ROOT / "artifacts"))
).expanduser()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import render_mobilefeed_intro_scene1_approval as approval
from video_announce.cherryflash_text import event_count_label


FINAL_MODE = "--final" in sys.argv
MODE_SLUG = "final" if FINAL_MODE else "preview"
OUT_DIR = ARTIFACTS_ROOT / "codex" / f"cherryflash_full_{MODE_SLUG}"
RAW_FRAMES_DIR = OUT_DIR / "frames_raw"
FRAMES_DIR = OUT_DIR / "frames"
PREVIEW_FRAMES_DIR = OUT_DIR / "preview_frames"

W = approval.W
H = approval.H
FPS = approval.FPS
SPLIT_Y = approval.SPLIT_Y
SCENE_TOTAL_LOCAL = approval.SCENE1_TOTAL_LOCAL
SCENE_TEXT_START = approval.SCENE1_TEXT_START
GUIDE_EXCURSION_TOTAL_LOCAL = 6.1
INTRO_END_FRAME = approval.INTRO_END_FRAME
FINAL_CARD_DURATION = 3.5
FINAL_CARD_FADE_IN = 0.3
AUDIO_BITRATE = "128k"
AUDIO_SAMPLE_RATE = "48000"
FIRST_PRIMARY_SCENE_START_LOCAL = approval.SCENE1_START_LOCAL
FINAL_VIDEO_CODEC = "libx265"
FINAL_VIDEO_PRESET = "medium"
FINAL_VIDEO_BITRATE = "1300k"
FINAL_VIDEO_MAXRATE = "1600k"
FINAL_VIDEO_BUFSIZE = "3200k"
FINAL_VIDEO_TAG = "hvc1"
FINAL_VIDEO_X265_PARAMS = "keyint=30:min-keyint=30:scenecut=0:open-gop=0:repeat-headers=1"
PREVIEW_VIDEO_CODEC = "libx264"
PREVIEW_VIDEO_PRESET = "slow"
PREVIEW_VIDEO_CRF = "23"

PRIMARY_TITLE_FONT = approval.BEBAS_BOLD
DESCRIPTION_FONT = approval.AKROBAT_BOLD
TITLE_COLOR = approval.TITLE_COLOR
ACCENT_COLOR = approval.ACCENT_COLOR
DETAIL_COLOR = approval.DETAIL_COLOR
BG_BLACK = approval.BG_BLACK
OUTRO_BG = BG_BLACK
OUTRO_STRIP = ImageColor.getrgb("#F1E44B")
OUTRO_TEXT = ImageColor.getrgb("#100E0E")
LEGACY_STRIP = (80, 20, 140, 255)
OUTRO_CONFIG: dict = {}

KNOWN_CITY_NAMES = {
    "Калининград",
    "Светлогорск",
    "Зеленоградск",
    "Янтарный",
    "Черняховск",
    "Балтийск",
}

@dataclass(frozen=True)
class RenderScene:
    index: int
    variant: str
    title: str
    date_line: str
    location_line: str
    description: str
    image_path: Path
    start_local: float
    festival_line: str = ""
    price_line: str = ""
    image_paths: tuple[Path, ...] = ()
    guide_excursion: dict | None = None


def _ensure_dirs() -> None:
    for directory in (OUT_DIR, RAW_FRAMES_DIR, FRAMES_DIR, PREVIEW_FRAMES_DIR):
        directory.mkdir(parents=True, exist_ok=True)
        for child in directory.glob("*.png"):
            child.unlink()


def _load_payload() -> dict:
    direct = ROOT / "payload.json"
    if direct.exists():
        return json.loads(direct.read_text(encoding="utf-8"))
    matches = sorted(ROOT.rglob("payload.json"))
    if matches:
        return json.loads(matches[0].read_text(encoding="utf-8"))
    raise FileNotFoundError("CherryFlash payload.json not found")


def _hex_rgb(value: str | None, fallback: tuple[int, int, int]) -> tuple[int, int, int]:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    try:
        return ImageColor.getrgb(raw)
    except Exception:
        return fallback


def _configure_outro(payload: dict) -> None:
    global OUTRO_CONFIG
    raw = payload.get("outro") if isinstance(payload, dict) else None
    OUTRO_CONFIG = raw if isinstance(raw, dict) else {}


def _outro_lines() -> list[dict]:
    raw_lines = OUTRO_CONFIG.get("lines") if isinstance(OUTRO_CONFIG, dict) else None
    if isinstance(raw_lines, list) and raw_lines:
        lines: list[dict] = []
        for idx, item in enumerate(raw_lines):
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            lines.append(
                {
                    "text": text.upper(),
                    "scale": float(item.get("scale") or 1.0),
                    "side": str(item.get("side") or ("left" if idx % 2 == 0 else "right")),
                    "delay": float(item.get("delay") or idx * 0.4),
                }
            )
        if lines:
            return lines
    return [
        {"text": "ПОЛЮБИТЬ", "side": "left", "delay": 0.0, "scale": 1.0},
        {"text": "КАЛИНИНГРАД", "side": "right", "delay": 0.4, "scale": 1.0},
        {"text": "АНОНСЫ", "side": "left", "delay": 0.8, "scale": 1.0},
    ]


def _candidate_audio_path() -> Path:
    for candidate in (
        ROOT / "assets" / "Pulsarium.mp3",
        ROOT / "video_announce" / "assets" / "Pulsarium.mp3",
        ROOT / "assets" / "Pulsarium_scene1_clip.mp3",
        ROOT / "video_announce" / "assets" / "The_xx_-_Intro.mp3",
    ):
        if candidate.exists():
            return candidate
    raise FileNotFoundError("CherryFlash audio asset not found")


def _audio_start_seconds(audio_path: Path) -> float:
    return 0.0 if audio_path.name == "Pulsarium_scene1_clip.mp3" else 294.0


def _resolve_image_path(value: str | None) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if path.is_absolute() and path.exists():
        return path
    for candidate in (
        ROOT / raw,
        ROOT / "assets" / raw,
        ROOT / "assets" / "posters" / raw,
        ROOT / path.name,
    ):
        if candidate.exists():
            return candidate
    matches = sorted(ROOT.rglob(path.name))
    return matches[0] if matches else None


def _scene_description(raw_scene: dict) -> str:
    return " ".join(
        str(
            raw_scene.get("description")
            or raw_scene.get("search_digest")
            or raw_scene.get("short_description")
            or ""
        ).split()
    )


def _normalize_text(value: str | None) -> str:
    return " ".join(str(value or "").split()).strip()


def _format_display_date(raw_date: str | None, raw_time: str | None = None) -> str:
    date_line = _normalize_text(raw_date)
    if date_line:
        try:
            dt = date.fromisoformat(date_line)
        except ValueError:
            dt = None
        if dt is not None:
            date_line = f"{dt.day} {approval.MONTHS_GENITIVE[dt.month]}"
    time_line = _normalize_text(raw_time)
    if time_line and time_line != "00:00" and time_line not in date_line:
        date_line = f"{date_line} • {time_line}" if date_line else time_line
    return date_line.upper()


def _extract_city_from_location(raw_location: str | None) -> str:
    parts = [part.strip() for part in str(raw_location or "").split(",") if part.strip()]
    for part in reversed(parts):
        if part in KNOWN_CITY_NAMES:
            return part
    return ""


def _format_display_location(
    *,
    location_name: str | None = None,
    city: str | None = None,
    raw_location: str | None = None,
    address: str | None = None,
) -> str:
    """Single-line venue label in the bot's canonical form:

        ЛОКАЦИЯ • АДРЕС • ГОРОД

    `address` is the optional street address (e.g. ``Мира 9``); when present
    it slots between the location name and the city. If the address is empty
    the line falls back to the legacy ``LOC • CITY`` shape.
    """
    explicit_location = _normalize_text(location_name)
    explicit_city = _normalize_text(city).split(",")[0].strip()
    raw_location_norm = _normalize_text(raw_location)
    fallback_location = raw_location_norm.split(",")[0].strip() if raw_location_norm else ""
    city_value = explicit_city or _extract_city_from_location(raw_location_norm)
    # Strip the city tail that some VK extractors append to address strings
    # (e.g. ``Мира 9, Калининград``) — the city is already in its own slot.
    address_value = _normalize_text(address).split(",")[0].strip()
    parts = [
        part.upper()
        for part in (explicit_location or fallback_location, address_value, city_value)
        if part
    ]
    if parts:
        return " • ".join(dict.fromkeys(parts))
    return raw_location_norm.upper()


def _format_display_price(scene_data: dict) -> str:
    price_min = scene_data.get("ticket_price_min")
    price_max = scene_data.get("ticket_price_max")
    is_free = bool(scene_data.get("is_free"))

    def _to_int(value) -> int | None:
        if value is None or value == "":
            return None
        try:
            ivalue = int(round(float(value)))
        except (TypeError, ValueError):
            return None
        return ivalue if ivalue >= 0 else None

    pmin = _to_int(price_min)
    pmax = _to_int(price_max)
    # Use «руб.» instead of the ₽ glyph: round-3 operator feedback
    # (2026-05-17) reported render artefacts on the price line because the
    # CherryFlash title font (Bebas Neue) does not include the ruble sign
    # codepoint and falls back to `.notdef`. «руб.» renders cleanly.
    if pmin is not None and pmin > 0:
        if pmax is not None and pmax > pmin:
            return f"{pmin}–{pmax} руб."
        return f"ОТ {pmin} руб." if pmax is None else f"{pmin} руб."
    if pmax is not None and pmax > 0:
        return f"ДО {pmax} руб."
    if is_free or (pmin == 0 and pmax in (None, 0)) or (pmax == 0 and pmin in (None, 0)):
        return "БЕСПЛАТНО"
    # No objective price info — don't claim free. The original round-1 KONB
    # override defaulted to "БЕСПЛАТНО" because library events are mostly free,
    # but that leaked into the general cherryflash flow and mislabelled paid
    # events.
    return ""


def _resolve_final_card_path() -> Path | None:
    for candidate in (
        ROOT / "Final.png",
        ROOT / "assets" / "Final.png",
        ROOT / "video_announce" / "crumple_references" / "Final.png",
        Path(__file__).resolve().parents[1] / "video_announce" / "crumple_references" / "Final.png",
    ):
        if candidate.exists():
            return candidate
    return None


def ease_out_expo(t: float) -> float:
    t = max(0.0, min(1.0, t))
    if t == 1.0:
        return 1.0
    return 1.0 - 2.0 ** (-10.0 * t)


def _paste_rgba_subpixel(
    canvas: Image.Image,
    overlay: Image.Image,
    x: float,
    y: float,
) -> None:
    ix = math.floor(x)
    iy = math.floor(y)
    fx = x - ix
    fy = y - iy
    shifted = overlay
    if abs(fx) > 1e-6 or abs(fy) > 1e-6:
        shifted = overlay.transform(
            overlay.size,
            Image.AFFINE,
            (1, 0, -fx, 0, 1, -fy),
            resample=Image.Resampling.BICUBIC,
        )
    canvas.paste(shifted, (ix, iy), shifted)


def _build_outro_strip(
    text: str,
    *,
    font_path: Path,
    font_size: int,
    strip_height: int,
    strip_color: tuple[int, int, int] = OUTRO_STRIP,
    text_color: tuple[int, int, int] = OUTRO_TEXT,
    max_width: int | None = None,
) -> Image.Image:
    font_size = max(18, int(font_size))
    max_width = max_width or W
    font = approval.font(font_path, font_size)
    bbox = font.getbbox(text)
    text_w = int(math.ceil(font.getlength(text)))
    text_h = bbox[3] - bbox[1]
    pad_x = max(18, round(25 * (W / approval.BASE_W)))
    while text_w + pad_x * 2 > max_width and font_size > 18:
        font_size -= 2
        font = approval.font(font_path, font_size)
        bbox = font.getbbox(text)
        text_w = int(math.ceil(font.getlength(text)))
        text_h = bbox[3] - bbox[1]
    strip_w = text_w + pad_x * 2
    text_y = int((strip_height - text_h) / 2 - bbox[1])
    image = Image.new("RGBA", (strip_w, strip_height), (*strip_color, 255))
    draw = ImageDraw.Draw(image)
    draw.text((pad_x, text_y), text, font=font, fill=(*text_color, 255))
    return image


# Strip leading characters the title font (Bebas Neue) cannot render. Bebas
# Neue has no emoji/pictograph coverage, so a leading emoji rasterizes as
# `.notdef` ("unknown glyph" / empty box) in the CherryFlash video title.
# Until we ship a real emoji-fallback rasterizer, drop the leading emoji and
# any trailing whitespace so the title reads cleanly.
_LEADING_NON_TEXT_RE = re.compile(
    r"^["
    r"\U0001F300-\U0001FAFF"   # symbols & pictographs (incl. emoji)
    r"\U0001F600-\U0001F6FF"   # emoticons / transport
    r"\U0001F700-\U0001F77F"
    r"\U00002600-\U000027BF"   # misc symbols & dingbats
    r"\U0001F900-\U0001F9FF"   # supplemental symbols & pictographs
    r"\U0001FA70-\U0001FAFF"
    r"\U0001F1E6-\U0001F1FF"   # regional indicators (flags)
    r"‍️"              # ZWJ + variation selector
    r"]+\s*",
    flags=re.UNICODE,
)


def _strip_leading_emoji(title: str) -> str:
    raw = (title or "").strip()
    if not raw:
        return ""
    cleaned = _LEADING_NON_TEXT_RE.sub("", raw, count=1)
    return cleaned.strip()


def _build_render_scenes(payload: dict) -> list[RenderScene]:
    _configure_outro(payload)
    scenes_data = payload.get("scenes") or []
    scenes: list[RenderScene] = []
    first_primary_assigned = False
    for idx, raw_scene in enumerate(scenes_data, start=1):
        if not isinstance(raw_scene, dict):
            continue
        scene_variant = str(raw_scene.get("scene_variant") or "primary")
        images = raw_scene.get("images") or []
        if isinstance(images, str):
            images = [images]
        image_path = None
        image_paths: list[Path] = []
        for candidate in images:
            resolved = _resolve_image_path(candidate)
            if resolved is not None:
                image_paths.append(resolved)
            if image_path is None and resolved is not None:
                image_path = resolved
        if scene_variant == "guide_excursion_promo":
            promo = raw_scene.get("guide_excursion") if isinstance(raw_scene.get("guide_excursion"), dict) else {}
            avatar_images = promo.get("avatar_images") if isinstance(promo, dict) else []
            if isinstance(avatar_images, str):
                avatar_images = [avatar_images]
            for candidate in avatar_images or []:
                resolved = _resolve_image_path(candidate)
                if resolved is not None and resolved not in image_paths:
                    image_paths.append(resolved)
                if image_path is None and resolved is not None:
                    image_path = resolved
            if image_path is None:
                continue
            scenes.append(
                RenderScene(
                    index=idx,
                    variant=scene_variant,
                    title=str(raw_scene.get("title") or "").strip(),
                    festival_line="СКОРО ЭКСКУРСИЯ",
                    date_line=_format_display_date(
                        raw_scene.get("date_iso") or raw_scene.get("date"),
                        raw_scene.get("time"),
                    ),
                    location_line="",
                    price_line="",
                    description="",
                    image_path=image_path,
                    image_paths=tuple(image_paths[:3]),
                    guide_excursion=dict(promo),
                    start_local=0.0,
                )
            )
            continue
        for resolved in image_paths:
            image_path = resolved
            if image_path is not None:
                break
        if image_path is None:
            continue
        start_local = 0.0
        if scene_variant == "primary" and not first_primary_assigned:
            start_local = FIRST_PRIMARY_SCENE_START_LOCAL
            first_primary_assigned = True
        scenes.append(
            RenderScene(
                index=idx,
                variant=scene_variant,
                title=_strip_leading_emoji(
                    str(raw_scene.get("about") or raw_scene.get("title") or "")
                ),
                festival_line=_normalize_text(raw_scene.get("festival")).upper(),
                date_line=_format_display_date(
                    raw_scene.get("date_iso") or raw_scene.get("date"),
                    raw_scene.get("time"),
                ),
                location_line=_format_display_location(
                    location_name=raw_scene.get("location_name"),
                    city=raw_scene.get("city"),
                    raw_location=raw_scene.get("location"),
                    address=raw_scene.get("location_address"),
                ),
                price_line=_format_display_price(raw_scene),
                description=_scene_description(raw_scene),
                image_path=image_path,
                start_local=start_local,
            )
        )
    final_card_path = _resolve_final_card_path()
    if final_card_path is not None:
        scenes.append(
            RenderScene(
                index=len(scenes) + 1,
                variant="brand_outro",
                title="Полюбить Калининград Анонсы",
                festival_line="",
                date_line="",
                location_line="",
                description="",
                image_path=final_card_path,
                start_local=0.0,
            )
        )
    if not scenes:
        raise RuntimeError("CherryFlash payload does not contain renderable scenes")
    return scenes


def _render_intro_frames() -> None:
    cmd = [sys.executable, str(ROOT / "scripts" / "render_mobilefeed_intro_scene1_approval.py")]
    if FINAL_MODE:
        cmd.append("--final")
    subprocess.run(cmd, check=True)
    intro_dir = approval.OUT_DIR / "frames"
    if not intro_dir.exists():
        raise RuntimeError(f"CherryFlash intro frames directory not found: {intro_dir}")
    for frame_num in range(1, INTRO_END_FRAME + 1):
        src = intro_dir / f"frame_{frame_num:04d}.png"
        if not src.exists():
            raise RuntimeError(f"Missing intro frame: {src}")
        shutil.copy2(src, RAW_FRAMES_DIR / f"frame_{frame_num:04d}.png")


def _primary_geometry(local_t: float, poster_size: tuple[int, int]) -> tuple[int, int, int, int]:
    src_w, src_h = poster_size
    if local_t < approval.T_ENTRY:
        scale = 0.4 + (0.5 * approval.ease_out_cubic(local_t / approval.T_ENTRY))
        y_mode: float | str = "center"
    elif local_t < (approval.T_ENTRY + approval.T_HOLD):
        scale = 0.9 + (0.1 * ((local_t - approval.T_ENTRY) / approval.T_HOLD))
        y_mode = "center"
    elif local_t < (approval.T_ENTRY + approval.T_HOLD + approval.T_MOVE):
        scale = 1.0
        progress = (local_t - (approval.T_ENTRY + approval.T_HOLD)) / approval.T_MOVE
        start_y = H / 2
        end_y = SPLIT_Y / 2
        y_mode = start_y + (end_y - start_y) * approval.ease_in_out_cubic(progress)
    elif local_t < (SCENE_TOTAL_LOCAL - approval.T_EXIT):
        scale = 1.0
        elapsed = local_t - (approval.T_ENTRY + approval.T_HOLD + approval.T_MOVE)
        y_mode = (SPLIT_Y / 2.0) - (10.0 * elapsed)
    else:
        scale = 1.0
        progress = (local_t - (SCENE_TOTAL_LOCAL - approval.T_EXIT)) / approval.T_EXIT
        start_y = SPLIT_Y / 2
        y_mode = start_y + (-src_h - start_y) * approval.ease_in_cubic(progress)
    poster_w = round(W * scale)
    poster_h = round(poster_w * src_h / src_w)
    x = (W - poster_w) / 2.0
    y = (H - poster_h) / 2.0 if y_mode == "center" else (float(y_mode) - poster_h / 2.0)
    return x, y, poster_w, poster_h


def _followup_geometry(local_t: float, poster_size: tuple[int, int]) -> tuple[int, int, int, int]:
    src_w, src_h = poster_size
    if local_t < approval.T_ENTRY:
        scale = 0.4 + (0.5 * approval.ease_out_cubic(local_t / approval.T_ENTRY))
        x_center = W / 2
    elif local_t < (approval.T_ENTRY + approval.T_HOLD):
        scale = 0.9 + (0.1 * ((local_t - approval.T_ENTRY) / approval.T_HOLD))
        x_center = W / 2
    elif local_t < (approval.T_ENTRY + approval.T_HOLD + approval.T_MOVE):
        scale = 1.0
        progress = (local_t - (approval.T_ENTRY + approval.T_HOLD)) / approval.T_MOVE
        x_center = (W / 2) + ((-W * 0.18) * approval.ease_in_out_cubic(progress))
    elif local_t < (SCENE_TOTAL_LOCAL - approval.T_EXIT):
        scale = 1.0
        elapsed = local_t - (approval.T_ENTRY + approval.T_HOLD + approval.T_MOVE)
        x_center = (W / 2) - (W * 0.18) - (W * 0.02 * elapsed)
    else:
        scale = 1.0
        progress = (local_t - (SCENE_TOTAL_LOCAL - approval.T_EXIT)) / approval.T_EXIT
        x_center = (W / 2) - (W * 0.20) + ((-W * 0.55) * approval.ease_in_cubic(progress))
    poster_w = round(W * scale)
    poster_h = round(poster_w * src_h / src_w)
    x = x_center - poster_w / 2.0
    y = (H - poster_h) / 2.0
    return x, y, poster_w, poster_h



GUIDE_PALETTES = {
    "prussian_cream": {
        "bg": "#121A2F", "bg2": "#21345B", "ink": "#FFF6E3", "muted": "#D8C9AA",
        "accent": "#FF6B35", "accent2": "#F6D365", "cta": "#FFF1CF", "cta_text": "#15213A",
        "bubble1": "#2F5597", "bubble2": "#F58F29", "bubble3": "#8CC6B5",
    },
    "deep_wine_ivory": {
        "bg": "#2A0F1D", "bg2": "#4B1736", "ink": "#FFF4E8", "muted": "#E7C7C5",
        "accent": "#FF7A45", "accent2": "#F2C36B", "cta": "#FFF7E9", "cta_text": "#321323",
        "bubble1": "#6A2450", "bubble2": "#E85D3F", "bubble3": "#C89B5A",
    },
    "museum_green_ivory": {
        "bg": "#10251F", "bg2": "#204E42", "ink": "#FFF8E7", "muted": "#D1E0CC",
        "accent": "#E9703E", "accent2": "#A8E06E", "cta": "#F4F0D8", "cta_text": "#10251F",
        "bubble1": "#2E7B67", "bubble2": "#E7A543", "bubble3": "#88B894",
    },
    "black_lime": {
        "bg": "#0C0D0E", "bg2": "#202226", "ink": "#F6F4EC", "muted": "#BFC3BB",
        "accent": "#F2653B", "accent2": "#D9F45F", "cta": "#E8FF65", "cta_text": "#111214",
        "bubble1": "#2B2E35", "bubble2": "#F2653B", "bubble3": "#647B46",
    },
}


def _guide_pal(scene: RenderScene) -> dict[str, str]:
    promo = scene.guide_excursion or {}
    name = str(promo.get("palette") or "prussian_cream")
    return GUIDE_PALETTES.get(name, GUIDE_PALETTES["prussian_cream"])


def _rgb(value: str) -> tuple[int, int, int]:
    return ImageColor.getrgb(value)


def _mix(c1: tuple[int, int, int], c2: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    return tuple(round(a + (b - a) * t) for a, b in zip(c1, c2))


def _alpha_layer(img: Image.Image, alpha: float) -> Image.Image:
    alpha = max(0.0, min(1.0, alpha))
    if alpha >= 0.999:
        return img
    return approval._alpha_image(img.copy(), int(255 * alpha))


def _crop_avatar_circle(path: Path, size: int) -> Image.Image:
    with Image.open(path).convert("RGB") as src:
        fitted = ImageOps.fit(src, (size, size), Image.Resampling.LANCZOS, centering=(0.5, 0.42))
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    mask = Image.new("L", (size, size), 0)
    md = ImageDraw.Draw(mask)
    md.ellipse((2, 2, size - 3, size - 3), fill=255)
    out.paste(fitted, (0, 0), mask)
    return out


def _fit_font(path: Path, text: str, max_width: int, max_size: int, min_size: int) -> tuple:
    size = max_size
    while size > min_size:
        font = approval.font(path, size)
        if ImageDraw.Draw(Image.new("RGB", (1, 1))).textlength(text, font=font) <= max_width:
            return font, size
        size -= 2
    return approval.font(path, min_size), min_size


def _wrap_text(text: str, font_path: Path, max_width: int, max_size: int, min_size: int, max_lines: int) -> tuple[list[str], object, int]:
    text = " ".join(str(text or "").split())
    words = text.split()
    draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    for size in range(max_size, min_size - 1, -2):
        font = approval.font(font_path, size)
        lines: list[str] = []
        current = ""
        for word in words:
            test = f"{current} {word}".strip()
            if draw.textlength(test, font=font) <= max_width or not current:
                current = test
            else:
                lines.append(current)
                current = word
        if current:
            lines.append(current)
        if len(lines) <= max_lines and all(draw.textlength(line, font=font) <= max_width for line in lines):
            return lines, font, size
    font = approval.font(font_path, min_size)
    lines = []
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        if draw.textlength(test, font=font) <= max_width or not current:
            current = test
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        while lines[-1] and draw.textlength(lines[-1] + "…", font=font) > max_width:
            lines[-1] = lines[-1][:-1].rstrip()
        lines[-1] = lines[-1] + "…"
    return lines, font, min_size


def _draw_centered_lines(canvas: Image.Image, lines: list[str], font, *, center_x: float, top: float, fill: tuple[int, int, int], line_gap: int = 8, alpha: float = 1.0) -> float:
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    y = top
    for line in lines:
        bbox = d.textbbox((0, 0), line, font=font)
        w = d.textlength(line, font=font)
        h = bbox[3] - bbox[1]
        d.text((center_x - w / 2, y - bbox[1]), line, font=font, fill=(*fill, int(255 * alpha)))
        y += h + line_gap
    canvas.alpha_composite(layer)
    return y


def _draw_building_icon(size: int, color: tuple[int, int, int], accent: tuple[int, int, int]) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    s = size
    d.polygon([(s*.18, s*.34), (s*.50, s*.14), (s*.82, s*.34)], fill=(*accent, 255))
    d.rounded_rectangle((s*.20, s*.36, s*.80, s*.78), radius=round(s*.06), outline=(*color, 255), width=max(3, round(s*.055)))
    for x in (0.32, 0.50, 0.68):
        d.rounded_rectangle((s*(x-.035), s*.44, s*(x+.035), s*.72), radius=round(s*.02), fill=(*color, 255))
    d.rounded_rectangle((s*.15, s*.78, s*.85, s*.88), radius=round(s*.035), fill=(*color, 255))
    return img


def _draw_walk_icon(size: int, color: tuple[int, int, int], accent: tuple[int, int, int]) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    w = max(4, round(size * 0.065))
    d.ellipse((size*.42, size*.10, size*.62, size*.30), fill=(*accent, 255))
    d.line((size*.50, size*.31, size*.43, size*.53, size*.33, size*.82), fill=(*color,255), width=w)
    d.line((size*.46, size*.42, size*.25, size*.55), fill=(*color,255), width=w)
    d.line((size*.46, size*.43, size*.66, size*.56), fill=(*color,255), width=w)
    d.line((size*.43, size*.53, size*.68, size*.84), fill=(*color,255), width=w)
    return img


def _draw_route_icon(size: int, color: tuple[int, int, int], accent: tuple[int, int, int]) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    w = max(4, round(size * 0.055))
    d.line((size*.24, size*.72, size*.45, size*.35, size*.73, size*.65), fill=(*color,255), width=w)
    for x, y, c in ((.24,.72,accent),(.45,.35,color),(.73,.65,accent)):
        d.ellipse((size*(x-.08), size*(y-.08), size*(x+.08), size*(y+.08)), fill=(*c,255))
    return img


def _draw_water_icon(size: int, color: tuple[int, int, int], accent: tuple[int, int, int]) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    w = max(4, round(size * 0.055))
    d.arc((size*.18, size*.28, size*.55, size*.66), 180, 360, fill=(*accent,255), width=w)
    d.arc((size*.45, size*.28, size*.82, size*.66), 180, 360, fill=(*color,255), width=w)
    d.line((size*.18, size*.72, size*.82, size*.72), fill=(*color,255), width=w)
    return img


def _guide_icon(kind: str, size: int, color: tuple[int, int, int], accent: tuple[int, int, int]) -> Image.Image:
    if kind == "building":
        return _draw_building_icon(size, color, accent)
    if kind == "route":
        return _draw_route_icon(size, color, accent)
    if kind == "water":
        return _draw_water_icon(size, color, accent)
    return _draw_walk_icon(size, color, accent)


def _guide_avatar_state(local_t: float) -> tuple[float, float]:
    # returns scale and center-y. P1 keeps the avatar on visual center, P2 moves
    # it to the approved central-focus composition.
    if local_t < 0.28:
        return 0.35, H / 2
    if local_t < 0.78:
        p = approval.ease_out_cubic((local_t - 0.28) / 0.50)
        return 0.38 + 0.52 * p, H / 2
    if local_t < 1.90:
        p = (local_t - 0.78) / 1.12
        return 0.90 + 0.10 * p, H / 2
    if local_t < 2.70:
        p = approval.ease_in_out_cubic((local_t - 1.90) / 0.80)
        return 1.0, (H / 2) + ((H * 0.37) - (H / 2)) * p
    if local_t < 4.95:
        return 1.0, H * 0.37
    p = approval.ease_in_cubic(min(1.0, (local_t - 4.95) / 0.82))
    return 1.0, H * 0.37 - H * 0.75 * p


def _guide_exit_offset(local_t: float, start: float, distance: float) -> float:
    if local_t < start:
        return 0.0
    return -distance * approval.ease_in_cubic(min(1.0, (local_t - start) / (GUIDE_EXCURSION_TOTAL_LOCAL - start)))


def _fade_slide(local_t: float, start: float, duration: float, slide: float = 34.0) -> tuple[float, float]:
    if local_t < start:
        return 0.0, slide
    p = min(1.0, (local_t - start) / duration)
    e = approval.ease_out_cubic(p)
    return e, slide * (1.0 - e)


def _render_guide_excursion_frame(scene: RenderScene, local_t: float) -> Image.Image:
    pal = _guide_pal(scene)
    bg = _rgb(pal["bg"]); bg2 = _rgb(pal["bg2"]); ink = _rgb(pal["ink"]); muted = _rgb(pal["muted"])
    accent = _rgb(pal["accent"]); accent2 = _rgb(pal["accent2"]); cta = _rgb(pal["cta"]); cta_text = _rgb(pal["cta_text"])
    scale = W / 720
    reveal = approval.ease_out_cubic(min(1.0, local_t / 0.28))
    canvas = Image.new("RGBA", (W, H), (*_mix((0, 0, 0), bg, reveal), 255))
    d = ImageDraw.Draw(canvas)
    for y in range(H):
        t = y / max(1, H - 1)
        color = _mix(bg, bg2, t * 0.75)
        d.line((0, y, W, y), fill=(*_mix(bg, color, reveal), 255))

    # Far 3D bubbles: they participate in the fast+slow P1 zoom at ~30% depth,
    # shift up less in P2 and exit later/slower in P3. Top orange strip remains
    # visually in front, so bubbles never cross above it.
    bubble_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    bd = ImageDraw.Draw(bubble_layer)
    if local_t < 0.78:
        bp = approval.ease_out_cubic(max(0.0, (local_t - 0.28) / 0.50))
        bscale = 0.86 + 0.105 * bp
    elif local_t < 1.90:
        bp = (local_t - 0.78) / 1.12
        bscale = 0.965 + 0.035 * bp
    else:
        bscale = 1.0
    bmove = 0.0
    if local_t >= 1.90:
        bmove = -46 * approval.ease_in_out_cubic(min(1.0, (local_t - 1.90) / 0.80))
    bmove += _guide_exit_offset(local_t, 5.22, H * 0.25)
    bubbles = [
        (-78, 210, 250, pal["bubble1"], 54, -22),
        (528, 190, 225, pal["bubble2"], 48, 18),
        (486, 615, 300, pal["bubble3"], 35, 28),
        (-120, 825, 270, pal["bubble2"], 30, -28),
    ]
    top_guard = round(112 * scale)
    for x, y, size, color_hex, alpha, drift in bubbles:
        cx = W / 2 + (x + size / 2 - W / 2) * bscale + drift * (bscale - 0.86) + _guide_exit_offset(local_t, 5.28, W * 0.04)
        cy = H / 2 + (y + size / 2 - H / 2) * bscale + bmove
        r = size * bscale / 2
        cy = max(cy, top_guard + r)
        bd.ellipse((cx-r, cy-r, cx+r, cy+r), fill=(*_rgb(color_hex), int(alpha * reveal)))
    bubble_layer = bubble_layer.filter(ImageFilter.GaussianBlur(radius=max(0.6, 1.8 * scale)))
    canvas.alpha_composite(bubble_layer)

    # Top accent strip is frontmost for background composition.
    strip_h = round(70 * scale)
    strip_y = round(58 * scale) + _guide_exit_offset(local_t, 5.38, H * 0.10)
    d.rounded_rectangle((-20, strip_y, W + 20, strip_y + strip_h), radius=round(18 * scale), fill=(*accent, int(255 * reveal)))

    avatar_scale, avatar_cy = _guide_avatar_state(local_t)
    avatar_paths = scene.image_paths or (scene.image_path,)
    n = max(1, min(3, len(avatar_paths)))
    base_size = round((360 if n == 1 else 258) * scale * avatar_scale)
    overlap = base_size * (0.58 if n == 2 else 0.54)
    group_w = base_size + (n - 1) * overlap
    avatar_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ad = ImageDraw.Draw(avatar_layer)
    ax0 = W / 2 - group_w / 2
    ay = avatar_cy - base_size / 2
    shadow_blur = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow_blur)
    sd.ellipse((W/2 - group_w/2 - 18*scale, ay + 14*scale, W/2 + group_w/2 + 18*scale, ay + base_size + 32*scale), fill=(0,0,0,85))
    shadow_blur = shadow_blur.filter(ImageFilter.GaussianBlur(radius=18*scale))
    avatar_layer.alpha_composite(shadow_blur)
    for i, path in enumerate(avatar_paths[:n]):
        size_i = base_size
        x = ax0 + i * overlap
        if n > 1 and i % 2 == 0:
            y = ay + 8 * scale
        else:
            y = ay - 4 * scale
        ad.ellipse((x - 8*scale, y - 8*scale, x + size_i + 8*scale, y + size_i + 8*scale), fill=(*accent2, 255))
        ad.ellipse((x - 3*scale, y - 3*scale, x + size_i + 3*scale, y + size_i + 3*scale), fill=(*ink, 255))
        avatar = _crop_avatar_circle(path, size_i)
        _paste_rgba_subpixel(avatar_layer, avatar, x, y)
    canvas.alpha_composite(avatar_layer)

    promo = scene.guide_excursion or {}
    # Kicker + icon appear during P2, then leave with a slight delay in P3.
    k_alpha, k_slide = _fade_slide(local_t, 2.03, 0.45, 24 * scale)
    k_y = round(165 * scale) + k_slide + _guide_exit_offset(local_t, 5.08, H * 0.52)
    icon_size = round(84 * scale)
    icon = _guide_icon(str(promo.get("icon_kind") or "walk"), icon_size, ink, accent2)
    _paste_rgba_subpixel(canvas, _alpha_layer(icon, k_alpha), W - round(126 * scale), k_y - round(22 * scale))
    k_font = approval.font(approval.AKROBAT_BLACK, max(24, round(29 * scale)))
    kd = ImageDraw.Draw(canvas)
    kicker = "СКОРО\nЭКСКУРСИЯ"
    kk_layer = Image.new("RGBA", canvas.size, (0,0,0,0))
    kld = ImageDraw.Draw(kk_layer)
    for j, line in enumerate(kicker.split("\n")):
        kld.text((round(72*scale), k_y + j*round(31*scale)), line, font=k_font, fill=(*muted, int(255*k_alpha)))
    canvas.alpha_composite(kk_layer)

    title_alpha, title_slide = _fade_slide(local_t, 2.32, 0.55, 50 * scale)
    title_exit = _guide_exit_offset(local_t, 5.03, H * 0.68)
    title_top = (avatar_cy + base_size/2 + round(48*scale)) + title_slide + title_exit
    title_lines, title_font, _ = _wrap_text(scene.title, approval.DRUK_SUPER, round(W*0.84), max(54, round(70*scale)), max(34, round(42*scale)), 3)
    _draw_centered_lines(canvas, [line.upper() for line in title_lines], title_font, center_x=W/2, top=title_top, fill=ink, line_gap=round(8*scale), alpha=title_alpha)

    date_alpha, date_slide = _fade_slide(local_t, 2.82, 0.45, 34 * scale)
    date_exit = _guide_exit_offset(local_t, 5.12, H * 0.62)
    text_height_est = len(title_lines) * round(60*scale)
    date_top = title_top + text_height_est + round(24*scale) + date_slide + date_exit
    date_font, _ = _fit_font(approval.AKROBAT_BLACK, scene.date_line, round(W*0.76), max(36, round(44*scale)), max(26, round(30*scale)))
    _draw_centered_lines(canvas, [scene.date_line], date_font, center_x=W/2, top=date_top, fill=accent2, line_gap=0, alpha=date_alpha)

    cta_alpha, cta_slide = _fade_slide(local_t, 3.20, 0.45, 34 * scale)
    cta_exit = _guide_exit_offset(local_t, 5.20, H * 0.55)
    contact = str(promo.get("contact") or "Полюбить Калининград").strip()
    contact_label = str(promo.get("contact_label") or "запись").strip().upper()
    cta_w = round(W * 0.78)
    cta_h = round(104 * scale)
    cta_x = (W - cta_w) / 2
    cta_y = date_top + round(72 * scale) + cta_slide + cta_exit
    halo_p = 0.0
    if 3.78 <= local_t <= 4.38:
        halo_p = math.sin((local_t - 3.78) / 0.60 * math.pi)
    cta_layer = Image.new("RGBA", canvas.size, (0,0,0,0))
    cd = ImageDraw.Draw(cta_layer)
    if halo_p > 0:
        cd.rounded_rectangle((cta_x-18*scale, cta_y-14*scale, cta_x+cta_w+18*scale, cta_y+cta_h+14*scale), radius=round(42*scale), fill=(*accent2, int(85*halo_p*cta_alpha)))
        cta_layer = cta_layer.filter(ImageFilter.GaussianBlur(radius=12*scale))
        cd = ImageDraw.Draw(cta_layer)
    cd.rounded_rectangle((cta_x, cta_y, cta_x+cta_w, cta_y+cta_h), radius=round(32*scale), fill=(*cta, int(245*cta_alpha)))
    label_font = approval.font(approval.AKROBAT_BOLD, max(20, round(24*scale)))
    value_font, _ = _fit_font(approval.AKROBAT_BLACK, contact, round(cta_w - 70*scale), max(30, round(38*scale)), max(20, round(24*scale)))
    cd.text((cta_x+round(34*scale), cta_y+round(18*scale)), contact_label, font=label_font, fill=(*_mix(cta_text, accent, 0.35), int(255*cta_alpha)))
    cd.text((cta_x+round(34*scale), cta_y+round(52*scale)), contact, font=value_font, fill=(*cta_text, int(255*cta_alpha)))
    canvas.alpha_composite(cta_layer)

    if local_t >= 5.55:
        fade = approval.ease_in_cubic(min(1.0, (local_t - 5.55) / 0.55))
        overlay = Image.new("RGBA", (W, H), (*bg, int(255 * fade)))
        canvas.alpha_composite(overlay)
    return canvas


def _build_primary_blocks(scene: RenderScene):
    blocks: list = []
    scale = W / approval.BASE_W
    title_blocks, next_y = approval._build_text_blocks(
        scene.title,
        font_path=PRIMARY_TITLE_FONT,
        font_size=max(36, round(90 * scale)),
        text_color=TITLE_COLOR,
        start_time=SCENE_TEXT_START,
        duration=approval.T_INFO + 1.0,
        start_y=SPLIT_Y + round(30 * scale),
    )
    blocks.extend(title_blocks)
    if scene.festival_line:
        festival_blocks, next_y = approval._build_text_blocks(
            scene.festival_line,
            font_path=PRIMARY_TITLE_FONT,
            font_size=max(22, round(42 * scale)),
            text_color=DETAIL_COLOR,
            start_time=SCENE_TEXT_START + 0.15,
            duration=approval.T_INFO + 1.0,
            start_y=next_y + round(22 * scale),
        )
        blocks.extend(festival_blocks)
    date_blocks, next_y = approval._build_text_blocks(
        scene.date_line,
        font_path=PRIMARY_TITLE_FONT,
        font_size=max(24, round(50 * scale)),
        text_color=ACCENT_COLOR,
        start_time=SCENE_TEXT_START + 0.2,
        duration=approval.T_INFO + 1.0,
        start_y=next_y + round(30 * scale),
    )
    blocks.extend(date_blocks)
    if scene.price_line:
        price_blocks, next_y = approval._build_text_blocks(
            scene.price_line,
            font_path=PRIMARY_TITLE_FONT,
            font_size=max(20, round(40 * scale)),
            text_color=DETAIL_COLOR,
            start_time=SCENE_TEXT_START + 0.3,
            duration=approval.T_INFO + 1.0,
            start_y=next_y + round(18 * scale),
        )
        blocks.extend(price_blocks)
    location_blocks, _ = approval._build_text_blocks(
        scene.location_line,
        font_path=PRIMARY_TITLE_FONT,
        font_size=max(22, round(45 * scale)),
        text_color=DETAIL_COLOR,
        start_time=SCENE_TEXT_START + 0.4,
        duration=approval.T_INFO + 1.0,
        start_y=next_y + round(22 * scale),
    )
    blocks.extend(location_blocks)
    return blocks


def _build_followup_blocks(scene: RenderScene):
    description = scene.description or scene.title
    return approval._build_text_blocks(
        description,
        font_path=DESCRIPTION_FONT,
        font_size=max(24, round(38 * (W / approval.BASE_W))),
        text_color=TITLE_COLOR,
        start_time=SCENE_TEXT_START + 0.12,
        duration=approval.T_INFO + 1.0,
        start_y=SPLIT_Y + round(46 * (W / approval.BASE_W)),
    )[0]


def _render_scene_frame(scene: RenderScene, local_t: float, text_blocks) -> Image.Image:
    if scene.variant == "brand_outro":
        return _render_brand_outro_frame(local_t)
    if scene.variant == "guide_excursion_promo":
        return _render_guide_excursion_frame(scene, local_t)
    with Image.open(scene.image_path).convert("RGBA") as poster_src:
        canvas = Image.new("RGBA", (W, H), (*BG_BLACK, 255))
        geometry_fn = _followup_geometry if scene.variant == "followup_image" else _primary_geometry
        x, y, poster_w, poster_h = geometry_fn(local_t, poster_src.size)
        poster = approval._resize_poster_clean(poster_src, (poster_w, poster_h))
        _paste_rgba_subpixel(canvas, poster, x, y)

    draw = ImageDraw.Draw(canvas)
    if local_t >= (approval.T_ENTRY + approval.T_HOLD):
        progress = min(
            1.0,
            max(0.0, (local_t - (approval.T_ENTRY + approval.T_HOLD)) / approval.T_MOVE),
        )
        curtain_y = (
            int(H + (SPLIT_Y - H) * approval.ease_in_out_cubic(progress))
            if scene.variant == "followup_image"
            else int(H + (SPLIT_Y - H) * approval.ease_in_out_quint(progress))
        )
        if curtain_y < H:
            draw.rectangle((0, curtain_y, W, H), fill=(*BG_BLACK, 255))

    for block in text_blocks:
        positioned = approval._block_position(block, local_t)
        if positioned is None:
            continue
        bx, by, alpha = positioned
        _paste_rgba_subpixel(
            canvas,
            approval._alpha_image(block.image.copy(), alpha),
            bx,
            by,
        )
    return canvas


def _render_brand_outro_frame(local_t: float) -> Image.Image:
    scale = W / approval.BASE_W
    # `global_scale` lets the outro config shrink every stripe + font + gap
    # uniformly (per КОНБ round-2 feedback: outro is 30% smaller end-to-end).
    global_outro_scale = 1.0
    if isinstance(OUTRO_CONFIG, dict):
        try:
            global_outro_scale = float(OUTRO_CONFIG.get("global_scale") or 1.0)
        except (TypeError, ValueError):
            global_outro_scale = 1.0
    global_outro_scale = max(0.3, min(1.5, global_outro_scale))
    base_strip_height = max(40, round(210 * scale * global_outro_scale))
    gap = max(6, round(20 * scale * global_outro_scale))
    base_font_size = max(36, round(160 * scale * global_outro_scale))
    slide_duration = 0.8
    words_conf = _outro_lines()
    strip_color = _hex_rgb(
        OUTRO_CONFIG.get("strip_color") if isinstance(OUTRO_CONFIG, dict) else None,
        OUTRO_STRIP,
    )
    text_color = _hex_rgb(
        OUTRO_CONFIG.get("text_color") if isinstance(OUTRO_CONFIG, dict) else None,
        OUTRO_TEXT,
    )
    heights = [
        max(30, round(base_strip_height * max(0.32, min(1.2, float(item.get("scale") or 1.0)))))
        for item in words_conf
    ]
    # Per-item extra gap BEFORE the stripe, as a multiplier of the base gap.
    # Used by КОНБ outro to add `2x` empty space above AND below «при поддержке»
    # — the stripe itself sets `extra_gap_before=1.0` (so total gap above is
    # 2× normal), and the next stripe (Полюбить) also sets it to 1.0
    # (so total gap below «при поддержке» is also 2× normal).
    extra_before: list[float] = [
        max(0.0, float(item.get("extra_gap_before") or 0.0)) for item in words_conf
    ]
    extra_gap_px = [round(gap * mult) for mult in extra_before]
    total_block_h = (
        sum(heights)
        + gap * max(0, len(words_conf) - 1)
        + sum(extra_gap_px[1:])
    )
    start_y_block = (H - total_block_h) / 2.0
    canvas = Image.new("RGBA", (W, H), (*OUTRO_BG, 255))

    current_y = start_y_block
    for idx, item in enumerate(words_conf):
        if idx > 0:
            current_y += extra_gap_px[idx]
        strip_height = heights[idx]
        font_size = max(20, round(base_font_size * max(0.35, min(1.1, float(item.get("scale") or 1.0)))))
        strip = _build_outro_strip(
            item["text"],
            font_path=PRIMARY_TITLE_FONT,
            font_size=font_size,
            strip_height=strip_height,
            strip_color=strip_color,
            text_color=text_color,
            max_width=round(W * 0.94),
        )
        final_x = (W - strip.width) / 2.0
        final_y = current_y
        start_x = -strip.width - round(100 * scale) if item["side"] == "left" else W + round(100 * scale)
        if local_t < item["delay"]:
            x = float(start_x)
        else:
            progress = min(1.0, max(0.0, (local_t - item["delay"]) / slide_duration))
            x = start_x + (final_x - start_x) * ease_out_expo(progress)
        _paste_rgba_subpixel(canvas, strip, x, final_y)
        current_y += strip_height + gap

    if local_t < FINAL_CARD_FADE_IN:
        alpha = int(255 * approval.ease_out_cubic(max(0.0, local_t / FINAL_CARD_FADE_IN)))
        faded = Image.new("RGBA", (W, H), (0, 0, 0, 255))
        faded.alpha_composite(approval._alpha_image(canvas, alpha), (0, 0))
        return faded
    return canvas


def _render_scene_frames(scenes: list[RenderScene]) -> int:
    frame_num = INTRO_END_FRAME + 1
    preview_points = {
        INTRO_END_FRAME + 1,
        INTRO_END_FRAME + 18,
        INTRO_END_FRAME + 54,
    }
    for scene_idx, scene in enumerate(scenes, start=1):
        blocks = (
            []
            if scene.variant == "guide_excursion_promo"
            else (
                _build_followup_blocks(scene)
                if scene.variant == "followup_image"
                else _build_primary_blocks(scene)
            )
        )
        local_t = float(scene.start_local)
        scene_total_local = (
            FINAL_CARD_DURATION
            if scene.variant == "brand_outro"
            else (
                GUIDE_EXCURSION_TOTAL_LOCAL
                if scene.variant == "guide_excursion_promo"
                else SCENE_TOTAL_LOCAL
            )
        )
        while local_t <= scene_total_local + (0.5 / FPS):
            frame = _render_scene_frame(scene, local_t, blocks)
            out_path = RAW_FRAMES_DIR / f"frame_{frame_num:04d}.png"
            frame.save(out_path)
            if frame_num in preview_points:
                shutil.copy2(out_path, PREVIEW_FRAMES_DIR / out_path.name)
            frame_num += 1
            local_t += 1.0 / FPS
        print(
            f"Rendered scene {scene_idx}/{len(scenes)} variant={scene.variant} image={scene.image_path.name}",
            flush=True,
        )
    return frame_num - 1


def _dedupe_exact_frames(*, audio_anchor_frame: int, dedupe_end_frame: int) -> tuple[int, int, int]:
    raw_paths = sorted(RAW_FRAMES_DIR.glob("frame_*.png"))
    if not raw_paths:
        raise RuntimeError("CherryFlash full render produced no raw frames")
    previous_digest: str | None = None
    removed_before_anchor = 0
    removed_total = 0
    kept_frame_num = 1
    for raw_path in raw_paths:
        original_frame_num = int(raw_path.stem.split("_")[-1])
        if original_frame_num <= dedupe_end_frame:
            with Image.open(raw_path).convert("RGBA") as frame:
                digest = hashlib.sha1(frame.tobytes()).hexdigest()
            if digest == previous_digest:
                removed_total += 1
                if original_frame_num < audio_anchor_frame:
                    removed_before_anchor += 1
                continue
            previous_digest = digest
        else:
            previous_digest = None
        shutil.copy2(raw_path, FRAMES_DIR / f"frame_{kept_frame_num:04d}.png")
        kept_frame_num += 1
    return kept_frame_num - 1, removed_total, removed_before_anchor


def _write_manifest(
    scenes: list[RenderScene],
    *,
    final_frame: int,
    removed_duplicates_total: int,
    removed_duplicates_before_anchor: int,
    audio_shift_seconds: float,
    output_path: Path,
) -> Path:
    manifest = OUT_DIR / f"manifest_{MODE_SLUG}.md"
    event_scenes = [scene for scene in scenes if scene.variant != "brand_outro"]
    has_final_card = any(scene.variant == "brand_outro" for scene in scenes)
    lines = [
        f"# CherryFlash Full Render {MODE_SLUG.title()}",
        "",
        f"- Canvas: `{W}x{H}`",
        f"- FPS: `{FPS}`",
        f"- Event scenes: `{len(event_scenes)}`",
        f"- Animated brand outro: `{'yes' if has_final_card else 'no'}`",
        f"- Intro end frame: `{INTRO_END_FRAME}`",
        f"- Final frame: `{final_frame}`",
        f"- Removed exact duplicate frames: `{removed_duplicates_total}`",
        f"- Removed before move-up anchor: `{removed_duplicates_before_anchor}`",
        f"- Audio shift before anchor: `{audio_shift_seconds:.4f}s`",
        f"- Output: `{output_path.name}`",
        "",
        "## Scenes",
        "",
    ]
    for scene in scenes:
        lines.append(
            f"- `{scene.index}` `{scene.variant}` `{scene.image_path.name}` :: {scene.title or scene.description[:80]}"
        )
    manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest


def _scale_audio_volume(audio: object, factor: float) -> object:
    if hasattr(audio, "with_volume_scaled"):
        return audio.with_volume_scaled(factor)
    if hasattr(audio, "volumex"):
        return audio.volumex(factor)
    if MultiplyVolume and hasattr(audio, "with_effects"):
        return audio.with_effects([MultiplyVolume(factor)])
    if volumex_fx:
        if hasattr(audio, "fx"):
            return audio.fx(volumex_fx, factor)
        return volumex_fx(audio, factor)
    print(
        "CherryFlash: moviepy build has no supported volume-scaling API; keeping original level.",
        file=sys.stderr,
    )
    return audio


def _load_moviepy_audio_clip():
    global AudioFileClip, MultiplyVolume, volumex_fx
    if AudioFileClip is None:
        try:
            from moviepy import AudioFileClip as audio_file_clip
        except ImportError:
            from moviepy.audio.io.AudioFileClip import AudioFileClip as audio_file_clip
        AudioFileClip = audio_file_clip
    if MultiplyVolume is None:
        try:
            from moviepy.audio.fx.MultiplyVolume import MultiplyVolume as multiply_volume
        except ImportError:
            multiply_volume = False
        MultiplyVolume = multiply_volume
    if volumex_fx is None:
        try:
            from moviepy.audio.fx.volumex import volumex as loaded_volumex
        except ImportError:
            try:
                from moviepy.audio.fx.all import volumex as loaded_volumex
            except ImportError:
                loaded_volumex = False
        volumex_fx = loaded_volumex
    return AudioFileClip


def _ffmpeg_bin() -> str:
    """Return path to the ffmpeg binary (imageio-ffmpeg bundled copy)."""
    try:
        from imageio_ffmpeg import get_ffmpeg_exe

        return get_ffmpeg_exe()
    except ImportError:
        return "ffmpeg"


def _encode_profile() -> dict[str, str | list[str]]:
    if FINAL_MODE:
        return {
            "video_codec": FINAL_VIDEO_CODEC,
            "preset": FINAL_VIDEO_PRESET,
            "audio_bitrate": AUDIO_BITRATE,
            "audio_sample_rate": AUDIO_SAMPLE_RATE,
            "video_args": [
                "-b:v",
                FINAL_VIDEO_BITRATE,
                "-maxrate",
                FINAL_VIDEO_MAXRATE,
                "-bufsize",
                FINAL_VIDEO_BUFSIZE,
            ],
            "extra_args": [
                "-tag:v",
                FINAL_VIDEO_TAG,
                "-x265-params",
                FINAL_VIDEO_X265_PARAMS,
            ],
        }
    return {
        "video_codec": PREVIEW_VIDEO_CODEC,
        "preset": PREVIEW_VIDEO_PRESET,
        "audio_bitrate": AUDIO_BITRATE,
        "audio_sample_rate": AUDIO_SAMPLE_RATE,
        "video_args": [
            "-crf",
            PREVIEW_VIDEO_CRF,
        ],
        "extra_args": [],
    }


def _encode_video(*, final_frame: int, audio_shift_seconds: float) -> Path:
    # --- 1. Prepare trimmed + volume-scaled audio as a temp WAV ----------
    audio_path = _candidate_audio_path()
    audio_clip = _load_moviepy_audio_clip()
    audio = audio_clip(str(audio_path))
    start_seconds = _audio_start_seconds(audio_path) + max(0.0, audio_shift_seconds)
    if audio.duration > start_seconds:
        if hasattr(audio, "subclipped"):
            audio = audio.subclipped(start_seconds)
        else:
            audio = audio.subclip(start_seconds)
    video_duration = final_frame / FPS
    if hasattr(audio, "with_duration"):
        audio = audio.with_duration(video_duration)
    else:
        audio = audio.set_duration(video_duration)
    audio = _scale_audio_volume(audio, 0.45)
    tmp_audio = FRAMES_DIR / "_audio_tmp.wav"
    audio.write_audiofile(str(tmp_audio), fps=int(AUDIO_SAMPLE_RATE), logger=None)
    audio.close()

    # --- 2. Encode video + audio with ffmpeg directly --------------------
    # MoviePy's ImageSequenceClip can introduce timestamp jitter that causes
    # certain decoded frames to map back to the previous source PNG.  Calling
    # ffmpeg directly with image2 + -framerate preserves exact 1/FPS timing.
    out_path = OUT_DIR / f"cherryflash_full_{MODE_SLUG}.mp4"
    profile = _encode_profile()
    cmd = [
        _ffmpeg_bin(),
        "-y",
        "-framerate",
        str(FPS),
        "-i",
        str(FRAMES_DIR / "frame_%04d.png"),
        "-i",
        str(tmp_audio),
        "-c:v",
        str(profile["video_codec"]),
        "-preset",
        str(profile["preset"]),
        *list(profile["video_args"]),
        "-pix_fmt",
        "yuv420p",
        *list(profile["extra_args"]),
        "-c:a",
        "aac",
        "-b:a",
        str(profile["audio_bitrate"]),
        "-ac",
        "2",
        "-ar",
        str(profile["audio_sample_rate"]),
        "-shortest",
        "-movflags",
        "+faststart",
        str(out_path),
    ]
    subprocess.run(cmd, check=True)
    tmp_audio.unlink(missing_ok=True)
    return out_path


def _write_cover_frame(scenes: list[RenderScene]) -> Path:
    image = Image.new("RGB", (W, H), ImageColor.getrgb("#0B0C10"))
    draw = ImageDraw.Draw(image)
    ink = ImageColor.getrgb("#F3F3F0")
    accent = ImageColor.getrgb("#F45B1F")
    headline = event_count_label(len([scene for scene in scenes if scene.variant == "primary"]))
    draw.text((84, 120), "CHERRYFLASH", font=approval.font(approval.AKROBAT_BLACK, 48), fill=accent)
    draw.text((84, 220), headline, font=approval.font(approval.DRUK_SUPER, 120), fill=ink)
    first = scenes[0]
    draw.text((84, 378), first.title or "Сцена 1", font=approval.font(approval.AKROBAT_BLACK, 38), fill=ink)
    draw.text((84, 438), first.date_line or first.description[:80], font=approval.font(approval.AKROBAT_BOLD, 30), fill=ink)
    out_path = OUT_DIR / "approval_cover.png"
    image.save(out_path)
    return out_path


def main() -> None:
    _ensure_dirs()
    payload = _load_payload()
    scenes = _build_render_scenes(payload)
    _render_intro_frames()
    _render_scene_frames(scenes)
    final_frame, removed_total, removed_before_anchor = _dedupe_exact_frames(
        audio_anchor_frame=INTRO_END_FRAME + 1,
        dedupe_end_frame=INTRO_END_FRAME,
    )
    audio_shift_seconds = removed_before_anchor / FPS
    output_path = _encode_video(
        final_frame=final_frame,
        audio_shift_seconds=audio_shift_seconds,
    )
    manifest = _write_manifest(
        scenes,
        final_frame=final_frame,
        removed_duplicates_total=removed_total,
        removed_duplicates_before_anchor=removed_before_anchor,
        audio_shift_seconds=audio_shift_seconds,
        output_path=output_path,
    )
    cover = _write_cover_frame(scenes)
    print(output_path, flush=True)
    print(manifest, flush=True)
    print(cover, flush=True)


if __name__ == "__main__":
    main()
