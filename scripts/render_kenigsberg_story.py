from __future__ import annotations

import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

try:
    import cv2
except Exception:  # pragma: no cover - Kaggle installs it in the notebook.
    cv2 = None

try:
    import librosa
    import numpy as np
except Exception:  # pragma: no cover - Kaggle installs it in the notebook.
    librosa = None
    np = None


W = 720
H = 1280
FPS = 30
MAIN_DURATION = 18.0
OUTRO_SCREEN_DURATION = 3.5
STORY_MAX_DURATION = 60.0
POETRY_MIN_OUTRO_SCREEN_DURATION = 1.8
TRANSITION_FRAMES = 2
WATERMARK_TEXT = "Мост в Кёнигсберг"
OUTRO_BG = (0, 0, 0)
OUTRO_STRIP = (241, 228, 75)
OUTRO_TEXT = (16, 14, 14)
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}
AUDIO_EXTS = {".flac", ".mp3", ".wav", ".m4a", ".aac", ".ogg"}
MIN_STRONG_MAIN_DURATION = 15.0
MAX_STRIPE_LINES = 7

MUSIC_RANGES = {
    "the promise": [(402.0, None)],
    "wyatt earth": [(0.0, 108.0)],
    "save me": [(0.0, 38.0)],
    "manuela": [(183.0, 217.0)],
    "one truth": [(96.0, 110.0)],
    "terminal": [(220.0, 266.0)],
    "elegy": [(378.0, 406.0)],
}
VIDEO_DATASETS = [
    {
        "period_key": "1919-1940",
        "dataset": "zigomaro/koenigsberg19191940",
        "aliases": ["koenigsberg19191940", "19191940"],
    },
    {
        "period_key": "winter",
        "dataset": "zigomaro/koenigsberg-winter",
        "aliases": ["koenigsberg-winter", "winter"],
    },
]


def log(message: str) -> None:
    print(message, flush=True)


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(cmd, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            log(exc.stdout[-4000:])
        if exc.stderr:
            log(exc.stderr[-4000:])
        raise


def ffprobe_duration(path: Path) -> float:
    try:
        result = run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ]
        )
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def find_session_root(input_root: Path) -> Path:
    candidates = []
    for item in input_root.iterdir() if input_root.exists() else []:
        if not item.is_dir():
            continue
        if item.name.startswith("kenigsberg-session-"):
            candidates.append(item)
    if not candidates:
        raise RuntimeError("kenigsberg-session-* dataset is not mounted")
    candidates.sort(key=lambda p: p.name, reverse=True)
    return candidates[0]


def find_dataset_dir(input_root: Path, aliases: list[str]) -> Path | None:
    normalized = [item.casefold() for item in aliases]
    roots = [input_root]
    datasets_root = input_root / "datasets"
    if datasets_root.exists():
        roots.insert(0, datasets_root)
    candidates: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        candidates.extend(item for item in root.rglob("*") if item.is_dir())
    candidates.extend(item for item in input_root.iterdir() if input_root.exists() and item.is_dir())
    for item in sorted(set(candidates)):
        if not item.is_dir():
            continue
        rel = item.relative_to(input_root).as_posix().casefold()
        if any(alias in rel for alias in normalized):
            return item
    return None


def mounted_input_dirs(input_root: Path) -> list[str]:
    if not input_root.exists():
        return []
    dirs = []
    for item in sorted(input_root.rglob("*")):
        if item.is_dir():
            try:
                dirs.append(item.relative_to(input_root).as_posix())
            except ValueError:
                dirs.append(str(item))
        if len(dirs) >= 80:
            dirs.append("...")
            break
    return dirs


def choose_video_dataset(
    input_root: Path,
    rng: random.Random,
    *,
    forced_dataset: str = "",
) -> tuple[Path, str, str]:
    options = []
    for spec in VIDEO_DATASETS:
        path = find_dataset_dir(input_root, list(spec["aliases"]))
        if path:
            video_count = len(iter_files(path, VIDEO_EXTS))
            if video_count > 0:
                options.append((path, str(spec["period_key"]), str(spec["dataset"]), video_count))
    forced = str(forced_dataset or "").strip().casefold()
    if forced:
        forced_aliases = [forced, Path(forced).name.casefold()]
        for spec in VIDEO_DATASETS:
            spec_aliases = {str(alias).casefold() for alias in spec["aliases"]}
            spec_aliases.add(str(spec["dataset"]).casefold())
            spec_aliases.add(Path(str(spec["dataset"])).name.casefold())
            if any(alias in spec_aliases for alias in forced_aliases):
                forced_aliases.extend(spec_aliases)
                break
        for path, period_key, dataset_slug, _video_count in options:
            if (
                dataset_slug.casefold() in forced_aliases
                or Path(dataset_slug).name.casefold() in forced_aliases
                or path.name.casefold() in forced_aliases
            ):
                return path, period_key, dataset_slug
        path = find_dataset_dir(input_root, list({alias for alias in forced_aliases if alias}))
        if path and iter_files(path, VIDEO_EXTS):
            return path, forced, forced
        raise RuntimeError(
            "Pinned poetry video dataset is not mounted or empty: "
            f"requested={forced_dataset!r} available_inputs={mounted_input_dirs(input_root)}"
        )
    if options:
        total = sum(item[3] for item in options)
        threshold = rng.uniform(0, float(total))
        cursor = 0.0
        for path, period_key, dataset_slug, video_count in options:
            cursor += float(video_count)
            if threshold <= cursor:
                return path, period_key, dataset_slug
        path, period_key, dataset_slug, _video_count = options[-1]
        return path, period_key, dataset_slug

    datasets_root = input_root / "datasets"
    scan_root = datasets_root if datasets_root.exists() else input_root
    fallback = []
    for item in sorted(scan_root.rglob("*")) if scan_root.exists() else []:
        if not item.is_dir():
            continue
        rel = item.relative_to(input_root).as_posix().casefold()
        if "kenigsberg-session-" in rel or "music" in rel:
            continue
        if iter_files(item, VIDEO_EXTS):
            fallback.append((item, item.name, item.name))
    if fallback:
        return rng.choice(fallback)
    raise RuntimeError(
        "No mounted Kenigsberg video dataset contains supported video files; "
        f"available_inputs={mounted_input_dirs(input_root)}"
    )


def iter_files(root: Path, exts: set[str]) -> list[Path]:
    return sorted(path for path in root.rglob("*") if path.is_file() and path.suffix.casefold() in exts)


def normalize_music_key(value: str) -> str:
    stem = Path(value).stem if Path(value).suffix else value
    return re.sub(r"[^0-9a-zа-яё]+", " ", stem.casefold()).strip()


def normalize_media_match_key(value: str) -> str:
    return re.sub(r"[^0-9a-zа-яё]+", " ", Path(str(value)).stem.casefold()).strip()


def find_poem_voice_audio(input_root: Path, payload: dict[str, Any]) -> Path | None:
    poem_id = str(payload.get("poem_audio") or payload.get("poem_id") or "").strip()
    if not poem_id:
        return None
    expected = normalize_media_match_key(poem_id)
    if not expected:
        return None
    candidates: list[Path] = []
    for path in input_root.rglob("*") if input_root.exists() else []:
        if not path.is_file() or path.suffix.casefold() not in AUDIO_EXTS:
            continue
        rel = path.relative_to(input_root).as_posix().casefold()
        if "koenigsberg-music" in rel or "kenigsberg-music" in rel:
            continue
        key = normalize_media_match_key(path.name)
        if expected == key or expected in key:
            candidates.append(path)
    if not candidates:
        return None
    candidates.sort(key=lambda item: (len(item.name), item.name))
    return candidates[0]


def parse_music_range_time(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().casefold()
    if not text or text in {"end", "конец", "track_end"}:
        return None
    parts = text.split(":")
    if len(parts) == 1:
        return float(parts[0])
    seconds = 0.0
    for part in parts:
        seconds = seconds * 60.0 + float(part)
    return seconds


def parse_music_ranges_payload(raw_ranges: Any) -> list[tuple[float, float | None]]:
    ranges: list[tuple[float, float | None]] = []
    if not isinstance(raw_ranges, list):
        return ranges
    for item in raw_ranges:
        try:
            if isinstance(item, dict):
                start = parse_music_range_time(item.get("start", 0.0)) or 0.0
                end = parse_music_range_time(item.get("end"))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                start = parse_music_range_time(item[0]) or 0.0
                end = parse_music_range_time(item[1])
            else:
                continue
        except Exception:
            continue
        if end is not None and end <= start:
            continue
        ranges.append((round(start, 3), None if end is None else round(end, 3)))
    return ranges


def load_music_range_manifest(music_dir: Path) -> dict[str, list[tuple[float, float | None]]]:
    manifest: dict[str, list[tuple[float, float | None]]] = {}
    for name in ("kenigsberg_music_ranges.json", "music_ranges.json"):
        path = music_dir / name
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            log(f"Music range manifest {path.name} could not be read: {type(exc).__name__}: {exc}")
            continue
        tracks = data.get("tracks", data) if isinstance(data, dict) else {}
        if not isinstance(tracks, dict):
            continue
        for track_name, raw_ranges in tracks.items():
            ranges = parse_music_ranges_payload(raw_ranges)
            if ranges:
                manifest[normalize_music_key(str(track_name))] = ranges
    return manifest


def allowed_music_ranges(
    track: Path,
    range_manifest: dict[str, list[tuple[float, float | None]]] | None = None,
) -> list[tuple[float, float | None]]:
    track_key = normalize_music_key(track.name)
    for configured_key, ranges in (range_manifest or {}).items():
        key = normalize_music_key(configured_key)
        if track_key == key or key in track_key:
            return ranges
    for configured_key, ranges in MUSIC_RANGES.items():
        key = normalize_music_key(configured_key)
        if track_key == key or key in track_key:
            return ranges
    return []


def font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        Path("assets/ro_znanie_fonts/Cygre-SemiBold.ttf"),
        Path("assets/ro_znanie_fonts/Cygre-Medium.ttf"),
        Path("assets/Akrobat-Bold.otf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
    ]
    for path in candidates:
        if path.exists():
            return ImageFont.truetype(str(path), size=size)
    return ImageFont.load_default()


def font_from_candidates(size: int, candidates: list[Path]) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for path in candidates:
        if path.exists():
            return ImageFont.truetype(str(path), size=size)
    return font(size)


def fit_font_for_width(
    texts: list[str],
    candidates: list[Path],
    *,
    start_size: int,
    min_size: int,
    max_width: int,
) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    probe = Image.new("RGBA", (W, H))
    draw = ImageDraw.Draw(probe)
    for size in range(start_size, min_size - 1, -2):
        fnt = font_from_candidates(size, candidates)
        if all(draw.textlength(text, font=fnt) <= max_width for text in texts):
            return fnt
    return font_from_candidates(min_size, candidates)


def wrap_text(text: str, draw: ImageDraw.ImageDraw, fnt: ImageFont.ImageFont, max_width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if draw.textlength(candidate, font=fnt) <= max_width or not current:
            current = candidate
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def stripe_text_layout(
    text: str,
    draw: ImageDraw.ImageDraw,
    *,
    max_lines: int = MAX_STRIPE_LINES,
) -> tuple[ImageFont.ImageFont, list[str], int, int]:
    max_width = W - 120
    for size in range(44, 29, -2):
        headline_font = font(size)
        lines = wrap_text(text, draw, headline_font, max_width=max_width)
        if not lines:
            return headline_font, [], 0, 0
        stripe_h = max(48, int(size * 1.42))
        gap = max(6, int(size * 0.16))
        total_h = len(lines) * stripe_h + max(0, len(lines) - 1) * gap
        if len(lines) <= max_lines and total_h <= 540:
            return headline_font, lines, stripe_h, gap
    min_font = font(30)
    lines = wrap_text(text, draw, min_font, max_width=max_width)
    raise RuntimeError(
        "Kenigsberg text screen is too dense for stripe typography: "
        f"wrapped_lines={len(lines)} max_lines={max_lines} text={text!r}"
    )


def ease_out_cubic(t: float) -> float:
    t = max(0.0, min(1.0, t))
    return 1 - (1 - t) ** 3


def ease_in_cubic(t: float) -> float:
    t = max(0.0, min(1.0, t))
    return t**3


def ease_in_out_cubic(t: float) -> float:
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return 4.0 * t * t * t
    return 1.0 - ((-2.0 * t + 2.0) ** 3) / 2.0


def ease_in_out_quint(t: float) -> float:
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return 16.0 * t**5
    return 1.0 - ((-2.0 * t + 2.0) ** 5) / 2.0


def ease_out_expo(t: float) -> float:
    t = max(0.0, min(1.0, t))
    if t == 1.0:
        return 1.0
    return 1.0 - 2.0 ** (-10.0 * t)


def paste_rgba_subpixel(canvas: Image.Image, overlay: Image.Image, x: float, y: float) -> None:
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


def ease_out_back(t: float, overshoot: float = 0.9) -> float:
    t = max(0.0, min(1.0, t))
    return 1.0 + (overshoot + 1.0) * (t - 1.0) ** 3 + overshoot * (t - 1.0) ** 2


def ease_in_back(t: float, overshoot: float = 0.75) -> float:
    t = max(0.0, min(1.0, t))
    return (overshoot + 1.0) * t**3 - overshoot * t**2


def draw_stripes(
    image: Image.Image,
    text: str,
    *,
    t: float,
    scene_duration: float,
) -> None:
    draw = ImageDraw.Draw(image, "RGBA")
    headline_font, lines, stripe_h, gap = stripe_text_layout(text, draw)
    if not lines:
        return
    x0 = 54
    y0 = 300
    stripe_in_duration = 0.72
    text_in_delay = 0.34
    text_in_duration = 0.42
    out_start = max(0.9, scene_duration - 0.52)
    for idx, line in enumerate(lines):
        local_delay = idx * 0.08
        stripe_p = ease_out_back((t - local_delay) / stripe_in_duration, overshoot=0.78)
        text_p = ease_out_back((t - local_delay - text_in_delay) / text_in_duration, overshoot=0.28)
        out_p = ease_in_back((t - out_start - idx * 0.055) / 0.34, overshoot=0.62)
        text_w = int(draw.textlength(line, font=headline_font))
        stripe_w = min(W - x0 - 44, text_w + 46)
        visible_w = int(stripe_w * (1.0 - out_p) * min(1.045, max(0.0, stripe_p)))
        y = y0 + idx * (stripe_h + gap)
        if visible_w <= 1:
            continue
        draw.rectangle((x0, y, x0 + visible_w, y + stripe_h), fill=(241, 228, 75, 230))
        bbox = headline_font.getbbox(line)
        text_h = bbox[3] - bbox[1]
        text_y_final = int(y + (stripe_h - text_h) / 2 - bbox[1])
        text_y = text_y_final + int((1.0 - min(1.0, max(0.0, text_p))) * stripe_h) + int(out_p * 28)
        mask = Image.new("L", image.size, 0)
        md = ImageDraw.Draw(mask)
        md.rectangle((x0, y, x0 + visible_w, y + stripe_h), fill=255)
        text_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
        td = ImageDraw.Draw(text_layer)
        td.text((x0 + 23, text_y), line, font=headline_font, fill=(16, 14, 14, 255))
        image.alpha_composite(Image.composite(text_layer, Image.new("RGBA", image.size), mask))


def poem_block_layout(
    lines: list[str],
    draw: ImageDraw.ImageDraw,
    *,
    group_breaks: set[int] | None = None,
) -> tuple[ImageFont.ImageFont, int, int, int]:
    max_width = W - 96
    max_height = 720
    clean = [line for line in lines if line.strip()]
    breaks = {idx for idx in (group_breaks or set()) if 0 <= idx < len(clean) - 1}
    for size in range(42, 23, -2):
        fnt = font(size)
        line_h = max(44, int(size * 1.34))
        gap = max(8, int(size * 0.22))
        group_extra = max(18, int(size * 0.6))
        total_h = (
            len(clean) * line_h
            + max(0, len(clean) - 1) * gap
            + len(breaks) * group_extra
        )
        if total_h > max_height:
            continue
        if all(draw.textlength(line, font=fnt) <= max_width - 36 for line in clean):
            return fnt, line_h, gap, group_extra
    fnt = font(24)
    too_wide = [line for line in clean if draw.textlength(line, font=fnt) > max_width - 36]
    if too_wide:
        raise RuntimeError(
            "Poetry line does not fit without wrapping at minimum font size: "
            + json.dumps(too_wide[:3], ensure_ascii=False)
        )
    return fnt, 36, 7, 18


def draw_poem_block(
    image: Image.Image,
    cue: dict[str, Any] | list[str],
    *,
    t: float,
    scene_duration: float,
) -> None:
    if isinstance(cue, dict):
        groups_raw = cue.get("groups")
        if not isinstance(groups_raw, list) or not groups_raw:
            groups_raw = [cue.get("lines") or []]
        style = str(cue.get("style") or "normal")
    else:
        groups_raw = [cue]
        style = "normal"
    groups: list[list[str]] = []
    for group in groups_raw:
        if not isinstance(group, list):
            continue
        cleaned = [str(line).strip() for line in group if str(line).strip()]
        if cleaned:
            groups.append(cleaned)
    if not groups:
        return
    flat: list[str] = []
    group_breaks: set[int] = set()
    group_index_per_line: list[int] = []
    for gi, group in enumerate(groups):
        for line in group:
            flat.append(line)
            group_index_per_line.append(gi)
        if gi < len(groups) - 1:
            group_breaks.add(len(flat) - 1)
    draw = ImageDraw.Draw(image, "RGBA")
    fnt, line_h, gap, group_extra = poem_block_layout(flat, draw, group_breaks=group_breaks)
    extras = [group_extra if (idx - 1) in group_breaks else 0 for idx in range(len(flat))]
    block_h = (
        len(flat) * line_h
        + max(0, len(flat) - 1) * gap
        + sum(extras)
    )
    x0 = 48
    y0 = int((H - block_h) * 0.46)
    is_title = style == "title"
    if is_title:
        in_duration = min(0.32, max(0.12, scene_duration * 0.08))
        out_start = max(0.85, scene_duration - 0.32)
        out_duration = 0.24
    else:
        in_duration = min(0.58, max(0.24, scene_duration * 0.22))
        out_start = max(0.65, scene_duration - min(0.44, scene_duration * 0.18))
        out_duration = 0.28
    cursor_y = y0
    for idx, line in enumerate(flat):
        if is_title:
            in_p = ease_out_cubic(t / in_duration) if in_duration > 0 else 1.0
            out_p = ease_in_cubic((t - out_start) / out_duration) if out_duration > 0 else 0.0
            stripe_progress = 1.0
        else:
            local_delay = idx * 0.055
            in_p = ease_out_cubic((t - local_delay) / in_duration)
            out_p = ease_in_cubic((t - out_start - idx * 0.035) / out_duration)
            stripe_progress = max(0.0, min(1.0, in_p)) * (1.0 - max(0.0, min(1.0, out_p)))
        alpha = int(232 * max(0.0, min(1.0, in_p)) * (1.0 - max(0.0, min(1.0, out_p))))
        if alpha <= 0:
            cursor_y += line_h + (gap if idx < len(flat) - 1 else 0) + extras[idx]
            continue
        text_w = int(draw.textlength(line, font=fnt))
        stripe_w = min(W - x0 - 34, text_w + 36)
        visible_w = int(stripe_w * stripe_progress) if not is_title else stripe_w
        y = cursor_y
        draw.rounded_rectangle(
            (x0, y, x0 + visible_w, y + line_h),
            radius=5,
            fill=(241, 228, 75, alpha),
        )
        if visible_w >= text_w + 20:
            bbox = fnt.getbbox(line)
            text_h = bbox[3] - bbox[1]
            text_y = int(y + (line_h - text_h) / 2 - bbox[1])
            draw.text((x0 + 18, text_y), line, font=fnt, fill=(16, 14, 14, min(255, alpha + 20)))
        cursor_y += line_h + (gap if idx < len(flat) - 1 else 0) + extras[idx]


def build_outro_strip(text: str, fnt: ImageFont.ImageFont, strip_height: int) -> Image.Image:
    bbox = fnt.getbbox(text)
    text_w = int(math.ceil(ImageDraw.Draw(Image.new("RGBA", (1, 1))).textlength(text, font=fnt)))
    text_h = bbox[3] - bbox[1]
    pad_x = 20
    strip_w = text_w + pad_x * 2
    text_y = int((strip_height - text_h) / 2 - bbox[1])
    image = Image.new("RGBA", (strip_w, strip_height), (*OUTRO_STRIP, 255))
    draw = ImageDraw.Draw(image)
    draw.text((pad_x, text_y), text, font=fnt, fill=(*OUTRO_TEXT, 255))
    return image


def draw_cherryflash_outro_screen(
    local_t: float,
    lines: list[str],
    *,
    sides: list[str] | None = None,
) -> Image.Image:
    scale = W / 1080.0
    strip_height = max(56, round(210 * scale))
    gap = max(8, round(20 * scale))
    font_candidates = [
        Path("assets/BebasNeue-Bold.ttf"),
        Path("video_announce/assets/BebasNeue-Bold.ttf"),
        Path("assets/ro_znanie_fonts/Cygre-Bold.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
    ]
    fnt = fit_font_for_width(
        lines,
        font_candidates,
        start_size=max(48, round(160 * scale)),
        min_size=56,
        max_width=W - 64,
    )
    slide_duration = 0.8
    step_y = strip_height + gap
    total_block_h = len(lines) * step_y - gap
    start_y_block = (H - total_block_h) / 2.0
    canvas = Image.new("RGBA", (W, H), (*OUTRO_BG, 255))
    sides = sides or ["left", "right", "left"]
    for idx, text in enumerate(lines):
        strip = build_outro_strip(text, fnt, strip_height)
        final_x = (W - strip.width) / 2.0
        final_y = start_y_block + idx * step_y
        side = sides[idx % len(sides)]
        start_x = -strip.width - round(100 * scale) if side == "left" else W + round(100 * scale)
        delay = idx * 0.4
        if local_t < delay:
            x = float(start_x)
        else:
            progress = min(1.0, max(0.0, (local_t - delay) / slide_duration))
            x = start_x + (final_x - start_x) * ease_out_expo(progress)
        paste_rgba_subpixel(canvas, strip, x, final_y)
    if local_t < 0.3:
        alpha = int(255 * ease_out_cubic(local_t / 0.3))
        faded = Image.new("RGBA", (W, H), (0, 0, 0, 255))
        layer = canvas.copy()
        layer.putalpha(alpha)
        faded.alpha_composite(layer, (0, 0))
        return faded
    return canvas


def draw_watermark(image: Image.Image) -> None:
    draw = ImageDraw.Draw(image, "RGBA")
    fnt = font(24)
    text_w = draw.textlength(WATERMARK_TEXT, font=fnt)
    x = int((W - text_w) / 2)
    y = H - 78
    draw.rounded_rectangle((x - 18, y - 9, x + text_w + 18, y + 32), radius=6, fill=(0, 0, 0, 92))
    draw.text((x, y), WATERMARK_TEXT, font=fnt, fill=(255, 255, 255, 188))


def mask_bottom_source_strip(image: Image.Image, mask_px: int) -> None:
    if mask_px <= 0:
        return
    mask_px = max(1, min(mask_px, H // 8))
    y0 = H - mask_px
    sample_top = max(0, y0 - 28)
    sample = image.crop((0, sample_top, W, y0)).resize((1, 1), Image.Resampling.BILINEAR)
    r, g, b, *_ = sample.getpixel((0, 0))
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    fill = (int(r), int(g), int(b), 255) if luminance < 72 else (16, 16, 16, 255)
    ImageDraw.Draw(image, "RGBA").rectangle((0, y0, W, H), fill=fill)


def blend_transition_frame(
    image: Image.Image,
    transition_tail: list[Image.Image],
    local_frame: int,
) -> Image.Image:
    if local_frame >= TRANSITION_FRAMES or not transition_tail:
        return image
    prev_idx = min(local_frame, len(transition_tail) - 1)
    alpha = ease_in_out_cubic((local_frame + 1) / (TRANSITION_FRAMES + 1))
    return Image.blend(transition_tail[prev_idx], image, alpha)


def crop_veo_bottom(frame: Any, crop_px: int) -> Any:
    if crop_px <= 0:
        return frame
    h = frame.shape[0]
    return frame[: max(1, h - crop_px), :, :]


def cover_resize(frame: Any) -> Image.Image:
    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    src = Image.fromarray(rgb).convert("RGBA")
    scale = max(W / src.width, H / src.height)
    new_size = (math.ceil(src.width * scale), math.ceil(src.height * scale))
    src = src.resize(new_size, Image.Resampling.LANCZOS)
    left = max(0, (src.width - W) // 2)
    top = max(0, (src.height - H) // 2)
    return src.crop((left, top, left + W, top + H))


def extract_segment_frames(
    segment: dict[str, Any],
    segment_dir: Path,
    *,
    frame_count: int,
    crop_px: int,
) -> tuple[list[Path], dict[str, Any]]:
    """Decode a source segment as CFR 30fps frames for stable story timing."""
    if segment_dir.exists():
        shutil.rmtree(segment_dir)
    segment_dir.mkdir(parents=True, exist_ok=True)
    source_start = float(segment["source_start"])
    duration = max(0.05, float(segment["timeline_end"]) - float(segment["timeline_start"]))
    filters: list[str] = []
    if crop_px > 0:
        crop_h = f"if(gt(ih\\,{crop_px})\\,ih-{crop_px}\\,ih)"
        filters.append(f"crop=iw:{crop_h}:0:0")
    filters.extend(
        [
            f"scale={W}:{H}:force_original_aspect_ratio=increase:flags=lanczos",
            f"crop={W}:{H}",
            "setsar=1",
            f"fps={FPS}",
        ]
    )
    pattern = segment_dir / "source_%05d.jpg"
    run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            f"{source_start:.3f}",
            "-i",
            str(segment["source_path"]),
            "-t",
            f"{duration + (1.0 / FPS):.3f}",
            "-vf",
            ",".join(filters),
            "-an",
            "-frames:v",
            str(frame_count),
            "-q:v",
            "3",
            str(pattern),
        ]
    )
    frames = sorted(segment_dir.glob("source_*.jpg"))
    decoded_count = len(frames)
    if not frames:
        raise RuntimeError(f"Could not decode source frames from {segment['source_file']}")
    pad_count = 0
    while len(frames) < frame_count:
        pad_count += 1
        dest = segment_dir / f"source_{len(frames) + 1:05d}.jpg"
        shutil.copy2(frames[-1], dest)
        frames.append(dest)
    if len(frames) > frame_count:
        frames = frames[:frame_count]
    return frames, {
        "decoded_frame_count": decoded_count,
        "expected_frame_count": frame_count,
        "pad_frame_count": pad_count,
        "decode_strategy": "ffmpeg_cfr_30fps",
    }


def payload_scene_lines(payload: dict[str, Any], fallback_thought: str, count: int) -> list[str]:
    raw = payload.get("scene_lines")
    if isinstance(raw, list):
        lines: list[str] = []
        for item in raw:
            line = " ".join(str(item or "").split())
            if not line:
                continue
            lines.append(line)
        if lines:
            too_long = [line for line in lines if len(line) > 160 or len(line.split()) > 34]
            if too_long:
                raise RuntimeError("Payload scene_lines contain overlong screens; LLM split is required")
            return lines
    raise RuntimeError("Payload scene_lines are required; Kaggle text fallback is disabled")


def scene_text_for_segment(lines: list[str], seg_idx: int, total_segments: int) -> str:
    if not lines:
        return ""
    if total_segments <= 0:
        return lines[0]
    line_idx = min(len(lines) - 1, int(seg_idx * len(lines) / total_segments))
    return lines[line_idx]


def build_text_cues(lines: list[str], total_duration: float) -> list[dict[str, Any]]:
    clean = [" ".join(line.split()) for line in lines if " ".join(line.split())]
    if not clean:
        return []
    gap = 0.16
    start = 0.45
    available = max(1.0, total_duration - start - 0.55)
    durations = [
        min(4.3, max(2.45, 1.55 + len(line.split()) * 0.22))
        for line in clean
    ]
    total = sum(durations) + gap * max(0, len(durations) - 1)
    if total > available:
        scale = available / total
        durations = [max(1.85, dur * scale) for dur in durations]
        total = sum(durations) + gap * max(0, len(durations) - 1)
    extra = max(0.0, available - total)
    if durations:
        add = extra / len(durations)
        durations = [dur + add for dur in durations]
    cues: list[dict[str, Any]] = []
    cursor = start
    for line, duration in zip(clean, durations):
        cues.append(
            {
                "text": line,
                "start": round(cursor, 3),
                "end": round(cursor + duration, 3),
            }
        )
        cursor += duration + gap
    return cues


def voice_pause_times(voice_path: Path | None, duration: float) -> list[float]:
    if voice_path is None or librosa is None or np is None or not voice_path.exists():
        return []
    try:
        y, sr = librosa.load(str(voice_path), sr=16000, mono=True, duration=duration)
        if y is None or len(y) < sr:
            return []
        rms = librosa.feature.rms(y=y, frame_length=1024, hop_length=256)[0]
        times = librosa.frames_to_time(range(len(rms)), sr=sr, hop_length=256)
        threshold = max(float(np.percentile(rms, 28)) * 1.12, float(np.max(rms)) * 0.055)
        pauses: list[float] = []
        start: float | None = None
        for value, ts in zip(rms, times):
            t = float(ts)
            if float(value) <= threshold:
                if start is None:
                    start = t
            elif start is not None:
                if t - start >= 0.16:
                    pauses.append(round((start + t) / 2.0, 3))
                start = None
        return [pause for pause in pauses if 0.5 <= pause <= duration - 0.5]
    except Exception as exc:
        log(f"Poetry voice pause analysis failed: {type(exc).__name__}: {exc}")
        return []


def _nearest_pause(target: float, pauses: list[float], *, min_t: float, max_t: float) -> float:
    candidates = [pause for pause in pauses if min_t <= pause <= max_t and abs(pause - target) <= 0.9]
    if not candidates:
        return target
    return min(candidates, key=lambda pause: abs(pause - target))


def build_poetry_text_cues(
    blocks: list[list[str]],
    total_duration: float,
    *,
    voice_path: Path | None = None,
    signature_lines: list[str] | None = None,
) -> list[dict[str, Any]]:
    clean = [[str(line).strip() for line in block if str(line).strip()] for block in blocks]
    clean = [block for block in clean if block]
    if not clean:
        return []
    signature = [str(line).strip() for line in (signature_lines or []) if str(line).strip()]
    start_pad = 0.18
    end_pad = 0.18
    available = max(1.0, total_duration - start_pad - end_pad)
    weights = [max(8, sum(len(line) for line in block)) for block in clean]
    total_weight = float(sum(weights)) or 1.0
    pauses = voice_pause_times(voice_path, total_duration)
    cues: list[dict[str, Any]] = []
    cursor = start_pad
    for idx, (block, weight) in enumerate(zip(clean, weights)):
        if idx == len(clean) - 1:
            end = total_duration - end_pad
        else:
            target_end = cursor + available * (weight / total_weight)
            min_end = cursor + 1.8
            max_end = min(total_duration - end_pad - (len(clean) - idx - 1) * 1.55, target_end + 1.05)
            end = _nearest_pause(target_end, pauses, min_t=min_end, max_t=max_end)
        if end <= cursor:
            end = min(total_duration - end_pad, cursor + 1.8)
        groups = [block]
        style = "normal"
        if idx == 0 and len(block) == 1:
            style = "title"
        if idx == len(clean) - 1 and signature:
            groups = [block, signature]
            style = "final"
        cues.append(
            {
                "text": "\n".join(block),
                "lines": block,
                "groups": groups,
                "style": style,
                "start": round(cursor, 3),
                "end": round(end, 3),
            }
        )
        cursor = end + 0.08
    return cues


def text_cue_at(cues: list[dict[str, Any]], timeline_t: float) -> tuple[str, float, float] | None:
    for cue in cues:
        start = float(cue.get("start") or 0.0)
        end = float(cue.get("end") or 0.0)
        if start <= timeline_t < end:
            return str(cue.get("text") or ""), timeline_t - start, max(0.1, end - start)
    return None


def poetry_cue_at(cues: list[dict[str, Any]], timeline_t: float) -> tuple[dict[str, Any], float, float] | None:
    for cue in cues:
        start = float(cue.get("start") or 0.0)
        end = float(cue.get("end") or 0.0)
        if start <= timeline_t < end:
            return cue, timeline_t - start, max(0.1, end - start)
    return None


def _music_names_match(left: str, right: str) -> bool:
    left_key = normalize_music_key(left)
    right_key = normalize_music_key(right)
    return bool(left_key and right_key and (left_key in right_key or right_key in left_key))


def _music_ranges_overlap(start: float, end: float, recent_start: float, recent_end: float, *, margin: float = 4.0) -> bool:
    return max(start, recent_start - margin) < min(end, recent_end + margin)


def _recent_music_items(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    items: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        file_name = str(item.get("file") or item.get("music_file") or "").strip()
        if not file_name:
            continue
        try:
            start = float(item.get("start") if item.get("start") is not None else item.get("music_start") or 0.0)
            end = float(item.get("end") if item.get("end") is not None else item.get("music_end") or 0.0)
        except Exception:
            continue
        if end <= start:
            continue
        items.append({"file": file_name, "start": start, "end": end, "issue_number": item.get("issue_number")})
    return items


def estimate_voice_risk(track: Path, start: float, duration: float) -> float:
    if os.getenv("KENIGSBERG_STORIES_MUSIC_VOICE_ANALYSIS", "1").strip() in {"0", "false", "False"}:
        return 0.0
    if librosa is None or np is None:
        return 0.0
    try:
        sample_duration = min(12.0, max(6.0, duration * 0.45))
        sample_start = max(0.0, start + max(0.0, duration - sample_duration) * 0.5)
        y, sr = librosa.load(str(track), sr=16000, mono=True, offset=sample_start, duration=sample_duration)
        if y is None or len(y) < sr:
            return 0.0
        y = np.asarray(y, dtype=float)
        rms = librosa.feature.rms(y=y)[0]
        if not rms.size or float(np.max(rms)) <= 1e-5:
            return 0.0
        harmonic, percussive = librosa.effects.hpss(y)
        harmonic_energy = float(np.mean(np.abs(harmonic)))
        percussive_energy = float(np.mean(np.abs(percussive))) + 1e-8
        harmonic_ratio = harmonic_energy / (harmonic_energy + percussive_energy)
        stft = np.abs(librosa.stft(y, n_fft=2048, hop_length=512))
        pitches, magnitudes = librosa.piptrack(S=stft, sr=sr, fmin=90, fmax=700)
        max_magnitudes = magnitudes.max(axis=0) if magnitudes.size else np.asarray([])
        if not max_magnitudes.size:
            pitched_ratio = 0.0
        else:
            threshold = max(float(np.percentile(max_magnitudes, 70)) * 0.55, 1e-6)
            pitched_ratio = float(np.mean(max_magnitudes > threshold))
        freqs = librosa.fft_frequencies(sr=sr, n_fft=2048)
        total_energy = float(np.sum(stft)) + 1e-8
        vocal_band = (freqs >= 180) & (freqs <= 3400)
        vocal_band_ratio = float(np.sum(stft[vocal_band, :]) / total_energy) if stft.size else 0.0
        flatness = librosa.feature.spectral_flatness(S=stft)[0]
        tonal_score = max(0.0, 1.0 - float(np.median(flatness)) * 8.0)
        risk = (0.36 * pitched_ratio) + (0.28 * harmonic_ratio) + (0.24 * vocal_band_ratio) + (0.12 * tonal_score)
        return round(max(0.0, min(1.0, risk)), 4)
    except Exception as exc:
        log(f"Music voice risk analysis failed for {track.name}: {type(exc).__name__}: {exc}")
        return 0.0


def choose_music(
    music_dir: Path,
    rng: random.Random,
    *,
    recent_music: list[dict[str, Any]] | None = None,
    total_duration: float | None = None,
) -> tuple[Path, float, float, dict[str, Any]]:
    tracks = iter_files(music_dir, AUDIO_EXTS)
    if not tracks:
        raise RuntimeError(f"No audio tracks found in {music_dir}")
    total_duration = float(total_duration or (MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION))
    rng.shuffle(tracks)
    skipped: list[str] = []
    recent = _recent_music_items(recent_music or [])
    latest_recent_issue = max((int(item.get("issue_number") or 0) for item in recent), default=0)
    range_manifest = load_music_range_manifest(music_dir)
    candidates: list[dict[str, Any]] = []
    for track in tracks:
        track_has_candidate = False
        ranges = allowed_music_ranges(track, range_manifest)
        if not ranges:
            skipped.append(f"{track.name}: no_allowed_range")
            continue
        duration = ffprobe_duration(track)
        for start, end in ranges:
            usable_end = duration if end is None else min(duration, end)
            if usable_end - start >= total_duration:
                max_start = usable_end - total_duration
                starts = [start] if max_start <= start else [rng.uniform(start, max_start) for _ in range(6)]
                if max_start > start:
                    starts.extend([start, max_start])
                for selected_start in starts:
                    selected_start = round(selected_start, 3)
                    selected_end = round(selected_start + total_duration, 3)
                    recent_same_track = [
                        item for item in recent if _music_names_match(track.name, str(item.get("file") or ""))
                    ]
                    latest_same_issue = max(
                        (int(item.get("issue_number") or 0) for item in recent_same_track),
                        default=0,
                    )
                    issue_gap = max(0, latest_recent_issue - latest_same_issue)
                    same_track_count = len(recent_same_track)
                    overlaps_recent = any(
                        _music_ranges_overlap(
                            selected_start,
                            selected_end,
                            float(item.get("start") or 0.0),
                            float(item.get("end") or 0.0),
                        )
                        for item in recent_same_track
                    )
                    voice_risk = estimate_voice_risk(track, selected_start, total_duration)
                    track_fatigue = (
                        max(0, 6 - issue_gap) * 5.0 if latest_same_issue else 0.0
                    ) + min(8, same_track_count) * 1.35
                    overlap_penalty = 35.0 if overlaps_recent else 0.0
                    voice_penalty = max(0.0, voice_risk - 0.58) * 9.0
                    candidates.append(
                        {
                            "track": track,
                            "start": selected_start,
                            "end": selected_end,
                            "allowed_start": start,
                            "allowed_end": usable_end,
                            "allowed_end_is_track_end": end is None,
                            "track_duration": duration,
                            "voice_risk": voice_risk,
                            "recent_same_track": bool(recent_same_track),
                            "recent_same_track_count": same_track_count,
                            "latest_same_track_issue": latest_same_issue,
                            "same_track_issue_gap": issue_gap,
                            "track_fatigue": round(track_fatigue, 4),
                            "overlaps_recent": overlaps_recent,
                            "score": (
                                rng.random()
                                + voice_risk * 2.0
                                + voice_penalty
                                + overlap_penalty
                                + track_fatigue
                            ),
                        }
                    )
                    track_has_candidate = True
        if not track_has_candidate:
            skipped.append(f"{track.name}: ranges_too_short_for_{total_duration:.1f}s")
    if candidates:
        clean = [
            item
            for item in candidates
            if not item["overlaps_recent"]
            and not item["recent_same_track"]
            and float(item.get("voice_risk") or 0.0) <= 0.68
        ]
        fresh_track = [
            item for item in candidates if not item["overlaps_recent"] and not item["recent_same_track"]
        ]
        low_voice_non_overlap = [
            item for item in candidates if not item["overlaps_recent"] and float(item.get("voice_risk") or 0.0) <= 0.68
        ]
        non_overlapping = [item for item in candidates if not item["overlaps_recent"]]
        low_voice = [item for item in candidates if float(item.get("voice_risk") or 0.0) <= 0.68]
        if clean:
            pool = clean
            selection_tier = "clean_fresh_low_voice"
        elif low_voice_non_overlap:
            pool = low_voice_non_overlap
            selection_tier = "same_track_low_voice_non_overlap_fallback"
        elif fresh_track:
            pool = fresh_track
            selection_tier = "fresh_track_voice_fallback"
        elif non_overlapping:
            pool = non_overlapping
            selection_tier = "non_overlap_voice_fallback"
        elif low_voice:
            pool = low_voice
            selection_tier = "overlap_low_voice_emergency"
        else:
            pool = candidates
            selection_tier = "full_emergency"
        selected = min(pool, key=lambda item: float(item["score"]))
        track = selected["track"]
        return track, float(selected["start"]), total_duration, {
            "allowed_start": selected["allowed_start"],
            "allowed_end": selected["allowed_end"],
            "allowed_end_is_track_end": selected["allowed_end_is_track_end"],
            "selected_end": selected["end"],
            "track_duration": selected["track_duration"],
            "voice_risk": selected["voice_risk"],
            "music_selection_score": round(float(selected["score"]), 4),
            "music_selection_tier": selection_tier,
            "recent_same_track": bool(selected["recent_same_track"]),
            "recent_same_track_count": selected["recent_same_track_count"],
            "latest_same_track_issue": selected["latest_same_track_issue"],
            "same_track_issue_gap": selected["same_track_issue_gap"],
            "track_fatigue": selected["track_fatigue"],
            "overlaps_recent": bool(selected["overlaps_recent"]),
            "candidate_count": len(candidates),
            "pool_count": len(pool),
            "tracks_with_allowed_ranges": len({str(item["track"]) for item in candidates}),
            "range_manifest_loaded": bool(range_manifest),
            "skipped_audio_count": len(skipped),
        }
    raise RuntimeError(
        "No audio track has an allowed instrumental range long enough for the full story; "
        f"required_duration={total_duration:.1f}s skipped={skipped[:20]}"
    )


def rhythm_slots_from_strong_beats(
    strong_beats: list[float],
    duration: float,
    rng: random.Random,
) -> list[tuple[float, float]]:
    anchors = [beat for beat in strong_beats if 0.35 <= beat < duration - 0.25]
    if len(anchors) < 4:
        raise RuntimeError("Not enough strong beats to build a rhythm grid")
    slots: list[tuple[float, float]] = []
    current = 0.0
    idx = 0
    # First change may be a partial interval from the selected audio start to
    # the first detected strong beat; all later changes land on strong beats.
    first = anchors[0]
    if first - current >= 0.45:
        slots.append((current, first))
        current = first
        idx = 1
    while idx < len(anchors) and duration - current >= 0.6:
        span = rng.choices([1, 2], weights=[0.56, 0.44], k=1)[0]
        target_idx = min(len(anchors) - 1, idx + span - 1)
        target = anchors[target_idx]
        if target - current < 0.45:
            idx += 1
            continue
        if duration - target < 0.6:
            break
        slots.append((current, target))
        current = target
        idx = target_idx + 1
    if current < min(MIN_STRONG_MAIN_DURATION, duration - 0.5) and duration - current >= 0.45:
        # Keep the MVP resilient: when the detected strong-beat sequence is too
        # short, preserve the established story length instead of publishing a
        # sharply shortened clip. The manifest marks this as a target-duration
        # fallback so it is visible in audit logs.
        slots.append((current, duration))
    return [(round(start, 3), round(end, 3)) for start, end in slots]


def approximate_rhythm_slots(duration: float, rng: random.Random) -> list[tuple[float, float]]:
    slots: list[tuple[float, float]] = []
    current = 0.0
    base_interval = rng.uniform(1.62, 2.12)
    while duration - current >= 0.45:
        span = rng.choices([1, 2], weights=[0.58, 0.42], k=1)[0]
        target = min(duration, current + base_interval * span)
        if duration - target < 0.6:
            target = duration
        if target - current < 0.45:
            break
        slots.append((round(current, 3), round(target, 3)))
        current = target
    return slots


def detect_strong_beats(
    music_path: Path,
    music_start: float,
    duration: float,
) -> tuple[list[float], dict[str, Any]]:
    if librosa is None or np is None:
        raise RuntimeError("librosa is required for Kenigsberg beat-synced scene cuts")
    pad = 0.25
    y, sr = librosa.load(
        str(music_path),
        sr=22050,
        mono=True,
        offset=max(0.0, music_start - pad),
        duration=duration + pad + 0.75,
    )
    if y is None or len(y) < sr:
        raise RuntimeError("Could not load enough audio for beat detection")
    onset_env = librosa.onset.onset_strength(y=y, sr=sr)
    tempo, beat_frames = librosa.beat.beat_track(y=y, sr=sr, onset_envelope=onset_env, units="frames")
    beat_frames = np.asarray(beat_frames, dtype=int)
    if beat_frames.size < 6:
        raise RuntimeError("Beat detection returned too few beats")
    beat_times_all = librosa.frames_to_time(beat_frames, sr=sr)
    beat_pairs: list[tuple[float, float]] = []
    for frame, absolute_time in zip(beat_frames, beat_times_all):
        rel_time = float(absolute_time - pad)
        if not (0.0 < rel_time < duration):
            continue
        strength_idx = min(max(int(frame), 0), len(onset_env) - 1)
        beat_pairs.append((rel_time, float(onset_env[strength_idx])))
    if len(beat_pairs) < 6:
        raise RuntimeError("Beat detection returned too few in-range beats")
    beat_times = [item[0] for item in beat_pairs]
    beat_strengths = [item[1] for item in beat_pairs]
    intervals = np.diff(np.asarray(beat_times, dtype=float))
    median_interval = float(np.median(intervals)) if intervals.size else 0.0
    if median_interval <= 0:
        raise RuntimeError("Beat detection returned invalid beat intervals")
    beats_per_strong = max(1, min(4, int(round(1.85 / median_interval))))
    offset_scores: list[tuple[float, int]] = []
    for offset in range(beats_per_strong):
        strengths = beat_strengths[offset::beats_per_strong]
        if strengths:
            offset_scores.append((float(np.mean(strengths)), offset))
    strong_offset = max(offset_scores)[1] if offset_scores else 0
    strong_beats = [
        beat
        for idx, beat in enumerate(beat_times)
        if idx % beats_per_strong == strong_offset
    ]
    meta = {
        "tempo_bpm": float(np.asarray(tempo).reshape(-1)[0]) if np.asarray(tempo).size else float(tempo),
        "beat_count": len(beat_times),
        "strong_beat_count": len(strong_beats),
        "median_beat_interval": round(median_interval, 4),
        "beats_per_strong": beats_per_strong,
        "strong_offset": strong_offset,
        "first_strong_beat": round(strong_beats[0], 3) if strong_beats else None,
        "beat_times": [round(beat, 3) for beat in beat_times],
        "strong_beat_times": [round(beat, 3) for beat in strong_beats],
    }
    return strong_beats, meta


def beat_slots(
    duration: float,
    rng: random.Random,
    *,
    music_path: Path | None = None,
    music_start: float = 0.0,
) -> tuple[list[tuple[float, float]], dict[str, Any]]:
    if music_path is None:
        raise RuntimeError("music_path is required for beat-synced Kenigsberg rhythm slots")
    try:
        strong_beats, meta = detect_strong_beats(music_path, music_start, duration)
        slots = rhythm_slots_from_strong_beats(strong_beats, duration, rng)
        last_end = slots[-1][1] if slots else 0.0
        ends_on_strong = any(abs(last_end - beat) <= 0.025 for beat in strong_beats)
        meta["rhythm_end_mode"] = "strong_beat" if ends_on_strong else "target_duration_fallback"
        if not ends_on_strong:
            meta["fallback_reason"] = "strong_beats_ended_before_target_duration"
    except Exception as exc:
        slots = approximate_rhythm_slots(duration, rng)
        meta = {
            "rhythm_end_mode": "approximate_fallback",
            "fallback_reason": f"{type(exc).__name__}: {exc}",
            "beat_times": [],
            "strong_beat_times": [],
        }
    meta["slots"] = slots
    return slots, meta


def poetry_timeline_plan(voice_audio: Path | None, payload: dict[str, Any]) -> tuple[float, list[dict[str, Any]], dict[str, Any]]:
    if str(payload.get("content_mode") or "").strip() != "poetry":
        return MAIN_DURATION, [
            {"lines": ["МОСТ", "В КЁНИГСБЕРГ"], "duration": OUTRO_SCREEN_DURATION},
            {"lines": ["ЗНАЙ ПРОШЛОЕ", "СТРОЙ БУДУЩЕЕ"], "duration": OUTRO_SCREEN_DURATION},
        ], {"mode": "thought_default"}
    if voice_audio is not None and voice_audio.exists():
        main_duration = max(8.0, ffprobe_duration(voice_audio))
    else:
        blocks = payload.get("poem_blocks") if isinstance(payload.get("poem_blocks"), list) else []
        line_count = sum(len(block) for block in blocks if isinstance(block, list))
        main_duration = min(46.0, max(24.0, line_count * 2.2))
    remaining = max(0.0, STORY_MAX_DURATION - main_duration)
    outro_screens: list[dict[str, Any]] = []
    if remaining >= POETRY_MIN_OUTRO_SCREEN_DURATION:
        first_duration = min(OUTRO_SCREEN_DURATION, remaining)
        outro_screens.append({"lines": ["МОСТ", "В КЁНИГСБЕРГ"], "duration": round(first_duration, 3)})
        remaining -= first_duration
    if remaining >= POETRY_MIN_OUTRO_SCREEN_DURATION + 0.2:
        second_duration = min(OUTRO_SCREEN_DURATION, remaining)
        outro_screens.append({"lines": ["ЗНАЙ ПРОШЛОЕ", "СТРОЙ БУДУЩЕЕ"], "duration": round(second_duration, 3)})
    return round(main_duration, 3), outro_screens, {
        "mode": "poetry",
        "voice_audio": voice_audio.name if voice_audio else "",
        "voice_duration": round(ffprobe_duration(voice_audio), 3) if voice_audio else 0.0,
        "outro_screens": outro_screens,
    }


def normalize_source_bans(raw: Any, *, dataset_slug: str = "") -> dict[str, list[dict[str, Any]]]:
    if isinstance(raw, dict):
        return {
            str(key): [item for item in value if isinstance(item, dict)]
            for key, value in raw.items()
            if isinstance(value, list)
        }
    grouped: dict[str, list[dict[str, Any]]] = {}
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            ban_dataset = str(item.get("dataset") or "").strip()
            if dataset_slug and ban_dataset and ban_dataset != dataset_slug:
                continue
            source_file = str(item.get("source_file") or "").strip()
            if source_file:
                grouped.setdefault(source_file, []).append(item)
    return grouped


def overlaps_ban(source_file: str, start: float, end: float, bans: dict[str, list[dict[str, Any]]]) -> bool:
    file_bans = bans.get(source_file) or bans.get(Path(source_file).name) or []
    for ban in file_bans:
        ban_start = float(ban.get("start", ban.get("source_start", 0.0)) or 0.0)
        ban_end = float(ban.get("end", ban.get("source_end", 0.0)) or 0.0)
        if start < ban_end and ban_start < end:
            return True
    return False


def split_hard_soft_source_bans(
    bans: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    hard: dict[str, list[dict[str, Any]]] = {}
    soft: dict[str, list[dict[str, Any]]] = {}
    for source_file, file_bans in bans.items():
        for ban in file_bans:
            reason = str(ban.get("reason") or "").strip()
            target = soft if reason == "recent_generation" else hard
            target.setdefault(source_file, []).append(ban)
    return hard, soft


def pick_video_segments(
    videos: list[Path],
    slots: list[tuple[float, float]],
    *,
    rng: random.Random,
    dataset_slug: str,
    crop_px: int,
    source_bans: Any = None,
) -> list[dict[str, Any]]:
    durations = {path: ffprobe_duration(path) for path in videos}
    usable = [path for path in videos if durations.get(path, 0.0) >= 2.0]
    if not usable:
        raise RuntimeError("No usable video files found")
    segments = []
    recent: list[Path] = []
    normalized_bans = normalize_source_bans(source_bans, dataset_slug=dataset_slug)
    hard_bans, soft_bans = split_hard_soft_source_bans(normalized_bans)
    run_bans: dict[str, list[dict[str, Any]]] = {}
    run_margin = 0.75
    for idx, (timeline_start, timeline_end) in enumerate(slots):
        dur = timeline_end - timeline_start
        unused_pool = [path for path in usable if path not in recent]
        pool = unused_pool or [path for path in usable if path not in recent[-2:]] or usable
        selected: tuple[Path, float, bool, bool, str] | None = None

        def try_pick(
            candidate_pool: list[Path],
            *,
            avoid_recent: bool,
            avoid_current: bool,
            mode: str,
        ) -> tuple[Path, float, bool, bool, str] | None:
            for _ in range(80):
                path = rng.choice(candidate_pool)
                max_start = max(0.0, durations[path] - dur - 0.2)
                source_start = rng.uniform(0.0, max_start) if max_start > 0 else 0.0
                source_end = source_start + dur
                if overlaps_ban(path.name, source_start, source_end, hard_bans):
                    continue
                overlaps_soft = overlaps_ban(path.name, source_start, source_end, soft_bans)
                overlaps_run = overlaps_ban(path.name, source_start, source_end, run_bans)
                if avoid_recent and overlaps_soft:
                    continue
                if avoid_current and overlaps_run:
                    continue
                return (path, source_start, overlaps_soft, overlaps_run, mode)

            for path in candidate_pool:
                max_start = max(0.0, durations[path] - dur - 0.2)
                candidate = 0.0
                while candidate <= max_start:
                    candidate_end = candidate + dur
                    if overlaps_ban(path.name, candidate, candidate_end, hard_bans):
                        candidate += 0.5
                        continue
                    overlaps_soft = overlaps_ban(path.name, candidate, candidate_end, soft_bans)
                    overlaps_run = overlaps_ban(path.name, candidate, candidate_end, run_bans)
                    if avoid_recent and overlaps_soft:
                        candidate += 0.5
                        continue
                    if avoid_current and overlaps_run:
                        candidate += 0.5
                        continue
                    return (path, candidate, overlaps_soft, overlaps_run, mode)
                    candidate += 0.5
            return None

        selected = try_pick(pool, avoid_recent=True, avoid_current=True, mode="fresh")
        if selected is None:
            fallback_pool = pool if pool == usable else usable
            selected = try_pick(fallback_pool, avoid_recent=False, avoid_current=True, mode="recent_soft_fallback")
        if selected is None:
            fallback_pool = pool if pool == usable else usable
            selected = try_pick(fallback_pool, avoid_recent=False, avoid_current=False, mode="current_emergency_fallback")
        if selected is None:
            raise RuntimeError(f"No source segment can avoid bans for slot {idx + 1}")
        path, source_start, overlaps_soft_ban, overlaps_run_ban, selection_mode = selected
        if overlaps_soft_ban or overlaps_run_ban:
            log(
                "Source segment repeat fallback: "
                + json.dumps(
                    {
                        "slot": idx + 1,
                        "source_file": path.name,
                        "source_start": round(source_start, 3),
                        "source_end": round(source_start + dur, 3),
                        "overlaps_recent_generation": overlaps_soft_ban,
                        "overlaps_current_generation": overlaps_run_ban,
                        "selection_mode": selection_mode,
                    },
                    ensure_ascii=False,
                )
            )
        segments.append(
            {
                "index": idx + 1,
                "dataset": dataset_slug,
                "source_file": path.name,
                "source_path": str(path),
                "source_start": round(source_start, 3),
                "source_end": round(source_start + dur, 3),
                "timeline_start": round(timeline_start, 3),
                "timeline_end": round(timeline_end, 3),
                "crop_bottom_px": crop_px,
                "score": 1.0,
                "strategy": "heuristic_v1",
                "source_soft_repeat_fallback": bool(overlaps_soft_ban or overlaps_run_ban),
                "source_overlaps_recent_generation": bool(overlaps_soft_ban),
                "source_overlaps_current_generation": bool(overlaps_run_ban),
                "source_selection_mode": selection_mode,
            }
        )
        run_bans.setdefault(path.name, []).append(
            {
                "source_start": max(0.0, source_start - run_margin),
                "source_end": min(durations[path], source_start + dur + run_margin),
                "reason": "current_generation",
            }
        )
        recent.append(path)
    return segments


def render_frames(
    payload: dict[str, Any],
    video_dir: Path,
    out_dir: Path,
    rng: random.Random,
    slots: list[tuple[float, float]],
) -> list[dict[str, Any]]:
    if cv2 is None:
        raise RuntimeError("opencv-python is not installed")
    crop_px = int(payload.get("crop_bottom_px") or 96)
    bottom_mask_px = int(payload.get("bottom_mask_px") or 34)
    videos = iter_files(video_dir, VIDEO_EXTS)
    rng.shuffle(videos)
    log(
        "Rhythm slots: "
        + json.dumps(
            {
                "seed": payload.get("seed"),
                "slots": slots,
                "rhythm_meta": payload.get("rhythm_meta") or {},
            },
            ensure_ascii=False,
        )
    )
    segments = pick_video_segments(
        videos,
        slots,
        rng=rng,
        dataset_slug=str(payload.get("dataset") or video_dir.name),
        crop_px=crop_px,
        source_bans=payload.get("source_bans") or {},
    )
    main_duration = float(slots[-1][1]) if slots else MAIN_DURATION
    thought = str(payload.get("thought_text") or "").strip()
    is_poetry = str(payload.get("content_mode") or "").strip() == "poetry"
    if is_poetry:
        raw_blocks = payload.get("poem_blocks") or []
        blocks = [
            [str(line).strip() for line in block if str(line).strip()]
            for block in raw_blocks
            if isinstance(block, list)
        ]
        signature_lines = [
            str(line).strip()
            for line in (payload.get("poem_signature_lines") or [])
            if str(line).strip() and not str(line).strip().startswith("@")
        ]
        lines = ["\n".join(block) for block in blocks]
        voice_path_raw = str(payload.get("voice_audio_path") or "").strip()
        text_cues = build_poetry_text_cues(
            blocks,
            main_duration,
            voice_path=Path(voice_path_raw) if voice_path_raw else None,
            signature_lines=signature_lines,
        )
    else:
        lines = payload_scene_lines(payload, thought, max(4, min(7, len(segments))))
        text_cues = build_text_cues(lines, main_duration)
    log(
        "Story text: "
        + json.dumps(
            {
                "thought_id": payload.get("thought_id") or "",
                "thought_text": thought,
                "scene_lines": lines,
                "text_cues": text_cues,
            },
            ensure_ascii=False,
        )
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    frame_no = 1
    transition_tail: list[Image.Image] = []
    for seg_idx, segment in enumerate(segments):
        frame_count = max(1, int(round((segment["timeline_end"] - segment["timeline_start"]) * FPS)))
        source_frames, decode_meta = extract_segment_frames(
            segment,
            out_dir / f"_segment_{seg_idx + 1:02d}",
            frame_count=frame_count,
            crop_px=crop_px,
        )
        segment.update(decode_meta)
        if decode_meta["pad_frame_count"]:
            log(
                "Segment decode padded frames: "
                + json.dumps(
                    {
                        "segment": segment["index"],
                        "source_file": segment["source_file"],
                        "decoded": decode_meta["decoded_frame_count"],
                        "expected": decode_meta["expected_frame_count"],
                        "padded": decode_meta["pad_frame_count"],
                    },
                    ensure_ascii=False,
                )
            )
        midpoint = (float(segment["timeline_start"]) + float(segment["timeline_end"])) / 2.0
        active_midpoint = text_cue_at(text_cues, midpoint)
        segment["text"] = active_midpoint[0] if active_midpoint else ""
        segment_tail: list[Image.Image] = []
        for local_frame in range(frame_count):
            with Image.open(source_frames[local_frame]) as src_image:
                image = src_image.convert("RGBA")
            timeline_t = float(segment["timeline_start"]) + (local_frame / FPS)
            if is_poetry:
                active_poem = poetry_cue_at(text_cues, timeline_t)
                if active_poem:
                    cue_dict, cue_t, cue_duration = active_poem
                    draw_poem_block(image, cue_dict, t=cue_t, scene_duration=cue_duration)
            else:
                active_text = text_cue_at(text_cues, timeline_t)
                if active_text:
                    text, cue_t, cue_duration = active_text
                    draw_stripes(image, text, t=cue_t, scene_duration=cue_duration)
            mask_bottom_source_strip(image, bottom_mask_px)
            draw_watermark(image)
            if seg_idx > 0:
                image = blend_transition_frame(image, transition_tail, local_frame)
            segment_tail.append(image.copy())
            if len(segment_tail) > TRANSITION_FRAMES:
                segment_tail.pop(0)
            image.convert("RGB").save(out_dir / f"frame_{frame_no:05d}.jpg", quality=92)
            frame_no += 1
        transition_tail = segment_tail
    outro_screens = payload.get("outro_screens") or [
        {"lines": ["МОСТ", "В КЁНИГСБЕРГ"], "duration": OUTRO_SCREEN_DURATION},
        {"lines": ["ЗНАЙ ПРОШЛОЕ", "СТРОЙ БУДУЩЕЕ"], "duration": OUTRO_SCREEN_DURATION},
    ]
    for screen in outro_screens:
        if isinstance(screen, dict):
            lines_for_screen = [str(line) for line in screen.get("lines") or []]
            screen_duration = float(screen.get("duration") or OUTRO_SCREEN_DURATION)
        else:
            lines_for_screen = [str(line) for line in screen]
            screen_duration = OUTRO_SCREEN_DURATION
        if not lines_for_screen or screen_duration <= 0:
            continue
        total = max(1, int(screen_duration * FPS))
        for i in range(total):
            image = draw_cherryflash_outro_screen(
                i / FPS,
                lines_for_screen,
                sides=["left", "right"],
            )
            image.convert("RGB").save(out_dir / f"frame_{frame_no:05d}.jpg", quality=92)
            frame_no += 1
    payload["text_cues"] = text_cues
    payload["main_duration"] = main_duration
    return segments


def encode_video(
    frames_dir: Path,
    music: Path,
    music_start: float,
    total_duration: float,
    out_path: Path,
    *,
    voice_audio: Path | None = None,
    main_duration: float | None = None,
    voice_music_gain: float = 0.08,
) -> None:
    audio_tmp = out_path.parent / "_kenigsberg_audio.m4a"
    if voice_audio is not None and voice_audio.exists():
        voice_duration = float(main_duration or ffprobe_duration(voice_audio) or total_duration)
        safe_music_gain = max(0.0, min(float(voice_music_gain), 0.12))
        run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                str(music_start),
                "-t",
                str(total_duration),
                "-i",
                str(music),
                "-i",
                str(voice_audio),
                "-filter_complex",
                (
                    f"[0:a]volume={safe_music_gain:.3f},atrim=0:"
                    f"{total_duration:.3f},asetpts=PTS-STARTPTS[m];"
                    "[1:a]loudnorm=I=-16:TP=-2.0:LRA=9,"
                    f"atrim=0:{voice_duration:.3f},asetpts=PTS-STARTPTS[v];"
                    "[m][v]amix=inputs=2:duration=longest:dropout_transition=0,"
                    "alimiter=limit=0.96[a]"
                ),
                "-map",
                "[a]",
                "-vn",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-ar",
                "48000",
                "-ac",
                "2",
                str(audio_tmp),
            ]
        )
    else:
        run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                str(music_start),
                "-t",
                str(total_duration),
                "-i",
                str(music),
                "-vn",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-ar",
                "48000",
                "-ac",
                "2",
                str(audio_tmp),
            ]
        )
    run(
        [
            "ffmpeg",
            "-y",
            "-framerate",
            str(FPS),
            "-i",
            str(frames_dir / "frame_%05d.jpg"),
            "-i",
            str(audio_tmp),
            "-c:v",
            "libx265",
            "-preset",
            "medium",
            "-b:v",
            "1300k",
            "-maxrate",
            "1600k",
            "-bufsize",
            "3200k",
            "-pix_fmt",
            "yuv420p",
            "-tag:v",
            "hvc1",
            "-x265-params",
            "keyint=30:min-keyint=30:scenecut=0:open-gop=0:repeat-headers=1",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-shortest",
            "-movflags",
            "+faststart",
            str(out_path),
        ]
    )
    audio_tmp.unlink(missing_ok=True)


def export_preview_frames(frames_dir: Path, working: Path) -> list[str]:
    frames = sorted(frames_dir.glob("frame_*.jpg"))
    if not frames:
        return []
    preview_dir = working / "kenigsberg_previews"
    if preview_dir.exists():
        shutil.rmtree(preview_dir)
    preview_dir.mkdir(parents=True, exist_ok=True)
    selected = [frames[0], frames[len(frames) // 2], frames[-1]]
    exported: list[str] = []
    for idx, src in enumerate(selected, start=1):
        dest = preview_dir / f"preview_{idx:02d}_{src.name}"
        shutil.copy2(src, dest)
        exported.append(dest.relative_to(working).as_posix())
    return exported


def main() -> None:
    input_root = Path(os.getenv("KAGGLE_INPUT_ROOT", "/kaggle/input"))
    working = Path(os.getenv("KAGGLE_WORKING_DIR", "/kaggle/working"))
    session_root = find_session_root(input_root)
    payload = json.loads((session_root / "payload.json").read_text(encoding="utf-8"))
    seed = int(payload.get("seed") or 1)
    rng = random.Random(seed)
    voice_audio = find_poem_voice_audio(input_root, payload)
    main_duration_target, outro_screens, timeline_meta = poetry_timeline_plan(voice_audio, payload)
    total_duration_target = main_duration_target + sum(float(screen.get("duration") or 0.0) for screen in outro_screens)
    forced_video_dataset = str(payload.get("forced_video_dataset") or "").strip()
    video_dir, period_key, dataset_slug = choose_video_dataset(
        input_root,
        rng,
        forced_dataset=forced_video_dataset,
    )
    log(
        f"Selected video dataset: period={period_key} path={video_dir}"
        + (f" forced={forced_video_dataset}" if forced_video_dataset else "")
    )
    mounted_video_files = iter_files(video_dir, VIDEO_EXTS)
    music_dir = find_dataset_dir(input_root, ["koenigsberg-music", "music"])
    if music_dir is None:
        raise RuntimeError(
            f"Music dataset is not mounted; available_inputs={mounted_input_dirs(input_root)}"
        )
    music, music_start, total_duration, music_meta = choose_music(
        music_dir,
        rng,
        recent_music=payload.get("recent_music") or [],
        total_duration=total_duration_target,
    )
    log(
        "Selected music: "
        f"file={music.name} start={music_start:.3f} end={float(music_meta.get('selected_end') or music_start + total_duration):.3f} "
        f"tier={music_meta.get('music_selection_tier')} voice_risk={float(music_meta.get('voice_risk') or 0.0):.3f} "
        f"same_track_count={music_meta.get('recent_same_track_count')} "
        f"same_track_gap={music_meta.get('same_track_issue_gap')} "
        f"overlaps_recent={music_meta.get('overlaps_recent')} "
        f"candidate_count={music_meta.get('candidate_count')} pool_count={music_meta.get('pool_count')} "
        f"tracks_with_ranges={music_meta.get('tracks_with_allowed_ranges')} "
        f"range_manifest_loaded={music_meta.get('range_manifest_loaded')} "
        f"skipped_audio={music_meta.get('skipped_audio_count')}"
    )
    rhythm_slots, rhythm_meta = beat_slots(
        main_duration_target,
        rng,
        music_path=music,
        music_start=music_start,
    )
    frames_dir = working / "kenigsberg_frames"
    if frames_dir.exists():
        shutil.rmtree(frames_dir)
    render_payload = dict(payload)
    render_payload["dataset"] = dataset_slug
    render_payload["rhythm_meta"] = rhythm_meta
    render_payload["outro_screens"] = outro_screens
    render_payload["voice_audio_path"] = str(voice_audio) if voice_audio else ""
    segments = render_frames(render_payload, video_dir, frames_dir, rng, rhythm_slots)
    main_duration = float(render_payload.get("main_duration") or MAIN_DURATION)
    total_duration = main_duration + sum(float(screen.get("duration") or 0.0) for screen in outro_screens)
    music_meta["selected_end"] = round(music_start + total_duration, 3)
    voice_music_gain = 0.08
    if voice_audio is not None and voice_audio.exists():
        music_voice_risk = float(music_meta.get("voice_risk") or 0.0)
        selection_tier = str(music_meta.get("music_selection_tier") or "")
        if music_voice_risk >= 0.68 or "emergency" in selection_tier:
            voice_music_gain = 0.045
        elif music_voice_risk >= 0.58:
            voice_music_gain = 0.06
    music_meta["voice_mix_music_gain"] = voice_music_gain
    out_path = working / "kenigsberg_story_final.mp4"
    encode_video(
        frames_dir,
        music,
        music_start,
        total_duration,
        out_path,
        voice_audio=voice_audio,
        main_duration=main_duration,
        voice_music_gain=voice_music_gain,
    )
    preview_frames = export_preview_frames(frames_dir, working)
    frame_count = len(list(frames_dir.glob("frame_*.jpg")))
    shutil.rmtree(frames_dir, ignore_errors=True)
    manifest = {
        "issue_number": int(payload.get("issue_number") or 0),
        "dataset": dataset_slug,
        "period_key": period_key,
        "thought_id": str(payload.get("thought_id") or ""),
        "thought_text": str(payload.get("thought_text") or ""),
        "music_file": music.name,
        "music_start": music_start,
        "music_end": music_meta.get("selected_end"),
        "music_allowed_range": {
            "start": music_meta.get("allowed_start"),
            "end": music_meta.get("allowed_end"),
            "end_is_track_end": music_meta.get("allowed_end_is_track_end"),
        },
        "music_voice_risk": music_meta.get("voice_risk"),
        "voice_mix_music_gain": music_meta.get("voice_mix_music_gain"),
        "content_mode": str(payload.get("content_mode") or "thought"),
        "poetry_mode": str(payload.get("poetry_mode") or ""),
        "poem_id": str(payload.get("poem_id") or ""),
        "poem_title": str(payload.get("poem_title") or ""),
        "poem_author": str(payload.get("poem_author") or ""),
        "poem_handle": str(payload.get("poem_handle") or ""),
        "voice_audio_file": voice_audio.name if voice_audio else "",
        "timeline_plan": timeline_meta,
        "outro_screens": outro_screens,
        "music_selection": {
            "score": music_meta.get("music_selection_score"),
            "tier": music_meta.get("music_selection_tier"),
            "candidate_count": music_meta.get("candidate_count"),
            "pool_count": music_meta.get("pool_count"),
            "tracks_with_allowed_ranges": music_meta.get("tracks_with_allowed_ranges"),
            "recent_same_track": music_meta.get("recent_same_track"),
            "recent_same_track_count": music_meta.get("recent_same_track_count"),
            "latest_same_track_issue": music_meta.get("latest_same_track_issue"),
            "same_track_issue_gap": music_meta.get("same_track_issue_gap"),
            "track_fatigue": music_meta.get("track_fatigue"),
            "overlaps_recent": music_meta.get("overlaps_recent"),
            "range_manifest_loaded": music_meta.get("range_manifest_loaded"),
            "skipped_audio_count": music_meta.get("skipped_audio_count"),
            "voice_mix_music_gain": music_meta.get("voice_mix_music_gain"),
        },
        "total_duration": total_duration,
        "main_duration": main_duration,
        "seed": int(payload.get("seed") or 0),
        "strategy": "heuristic_v1",
        "frame_count": frame_count,
        "preview_frames": preview_frames,
        "hook": str(render_payload.get("hook") or ""),
        "scene_lines": render_payload.get("scene_lines") or [],
        "text_cues": render_payload.get("text_cues") or [],
        "segments": segments,
        "rhythm_meta": rhythm_meta,
        "rhythm_slots": [
            [segment["timeline_start"], segment["timeline_end"]]
            for segment in segments
        ],
        "output": out_path.name,
        "output_size_bytes": out_path.stat().st_size if out_path.exists() else 0,
    }
    render_log = {
        "selected_video_dataset": {
            "period_key": period_key,
            "dataset": dataset_slug,
            "path": str(video_dir),
            "video_count": len(mounted_video_files),
            "video_files_sample": [path.name for path in mounted_video_files[:30]],
        },
        "timeline_plan": timeline_meta,
        "selected_music": {
            "file": music.name,
            "path": str(music),
            "start": music_start,
            "end": music_meta.get("selected_end"),
            "allowed_range": {
                "start": music_meta.get("allowed_start"),
                "end": music_meta.get("allowed_end"),
                "end_is_track_end": music_meta.get("allowed_end_is_track_end"),
            },
            "track_duration": music_meta.get("track_duration"),
            "duration": total_duration,
            "voice_risk": music_meta.get("voice_risk"),
            "selection_score": music_meta.get("music_selection_score"),
            "selection_tier": music_meta.get("music_selection_tier"),
            "candidate_count": music_meta.get("candidate_count"),
            "pool_count": music_meta.get("pool_count"),
            "tracks_with_allowed_ranges": music_meta.get("tracks_with_allowed_ranges"),
            "recent_same_track": music_meta.get("recent_same_track"),
            "recent_same_track_count": music_meta.get("recent_same_track_count"),
            "latest_same_track_issue": music_meta.get("latest_same_track_issue"),
            "same_track_issue_gap": music_meta.get("same_track_issue_gap"),
            "track_fatigue": music_meta.get("track_fatigue"),
            "overlaps_recent": music_meta.get("overlaps_recent"),
            "range_manifest_loaded": music_meta.get("range_manifest_loaded"),
            "skipped_audio_count": music_meta.get("skipped_audio_count"),
            "voice_mix_music_gain": music_meta.get("voice_mix_music_gain"),
        },
        "text": {
            "content_mode": str(payload.get("content_mode") or "thought"),
            "poem_id": str(payload.get("poem_id") or ""),
            "thought_id": manifest["thought_id"],
            "thought_text": manifest["thought_text"],
            "hook": manifest["hook"],
            "scene_lines": manifest["scene_lines"],
            "text_cues": manifest["text_cues"],
        },
        "segments": segments,
        "rhythm_meta": rhythm_meta,
        "rhythm_slots": manifest["rhythm_slots"],
        "output": {
            "file": out_path.name,
            "size_bytes": manifest["output_size_bytes"],
            "frame_count": frame_count,
            "preview_frames": preview_frames,
        },
    }
    (working / "kenigsberg_issue_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (working / "kenigsberg_render_log.json").write_text(
        json.dumps(render_log, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (working / "kenigsberg_manifest.md").write_text(
        "# Kenigsberg Story\n\n"
        f"- Issue: #{manifest['issue_number']}\n"
        f"- Period: {period_key}\n"
        f"- Music: {music.name} @ {music_start:.2f}-{float(music_meta.get('selected_end') or 0.0):.2f}s "
        f"(allowed {float(music_meta.get('allowed_start') or 0.0):.2f}-{float(music_meta.get('allowed_end') or 0.0):.2f}s, "
        f"voice_risk={float(music_meta.get('voice_risk') or 0.0):.2f}, "
        f"recent_same_track={bool(music_meta.get('recent_same_track'))}, "
        f"overlaps_recent={bool(music_meta.get('overlaps_recent'))})\n"
        f"- Thought: {manifest['thought_text']}\n"
        f"- Segments: {len(segments)}\n",
        encoding="utf-8",
    )
    log(
        "Render summary: "
        + json.dumps(
            {
                "period": period_key,
                "music": music.name,
                "voice_audio": voice_audio.name if voice_audio else "",
                "music_start": music_start,
                "music_end": music_meta.get("selected_end"),
                "music_allowed_range": manifest["music_allowed_range"],
                "music_voice_risk": music_meta.get("voice_risk"),
                "music_selection_score": music_meta.get("music_selection_score"),
                "music_recent_same_track": music_meta.get("recent_same_track"),
                "music_overlaps_recent": music_meta.get("overlaps_recent"),
                "segments": len(segments),
                "frames": frame_count,
                "output_size_bytes": manifest["output_size_bytes"],
                "preview_frames": preview_frames,
            },
            ensure_ascii=False,
        )
    )
    log(str(out_path))


if __name__ == "__main__":
    main()
