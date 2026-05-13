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
TRANSITION_FRAMES = 2
WATERMARK_TEXT = "Мост в Кёнигсберг"
OUTRO_BG = (0, 0, 0)
OUTRO_STRIP = (241, 228, 75)
OUTRO_TEXT = (16, 14, 14)
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}
AUDIO_EXTS = {".flac", ".mp3", ".wav", ".m4a", ".aac", ".ogg"}
MIN_STRONG_MAIN_DURATION = 15.0

MUSIC_RANGES = {
    "the promise": [(224.0, 266.0), (402.0, None)],
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
    return subprocess.run(cmd, check=True, text=True, capture_output=True)


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


def choose_video_dataset(input_root: Path, rng: random.Random) -> tuple[Path, str, str]:
    options = []
    for spec in VIDEO_DATASETS:
        path = find_dataset_dir(input_root, list(spec["aliases"]))
        if path and iter_files(path, VIDEO_EXTS):
            options.append((path, str(spec["period_key"]), str(spec["dataset"])))
    if options:
        return rng.choice(options)

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


def allowed_music_ranges(track: Path) -> list[tuple[float, float | None]]:
    track_key = normalize_music_key(track.name)
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
    return lines[:4]


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
    headline_font = font(44)
    lines = wrap_text(text, draw, headline_font, max_width=W - 120)
    if not lines:
        return
    stripe_h = 62
    gap = 8
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
        text_y_final = y + 8
        text_y = text_y_final + int((1.0 - min(1.0, max(0.0, text_p))) * stripe_h) + int(out_p * 28)
        mask = Image.new("L", image.size, 0)
        md = ImageDraw.Draw(mask)
        md.rectangle((x0, y, x0 + visible_w, y + stripe_h), fill=255)
        text_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
        td = ImageDraw.Draw(text_layer)
        td.text((x0 + 23, text_y), line, font=headline_font, fill=(16, 14, 14, 255))
        image.alpha_composite(Image.composite(text_layer, Image.new("RGBA", image.size), mask))


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
            too_long = [line for line in lines if len(line) > 118 or len(line.split()) > 19]
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


def text_cue_at(cues: list[dict[str, Any]], timeline_t: float) -> tuple[str, float, float] | None:
    for cue in cues:
        start = float(cue.get("start") or 0.0)
        end = float(cue.get("end") or 0.0)
        if start <= timeline_t < end:
            return str(cue.get("text") or ""), timeline_t - start, max(0.1, end - start)
    return None


def choose_music(music_dir: Path, rng: random.Random) -> tuple[Path, float, float, dict[str, Any]]:
    tracks = iter_files(music_dir, AUDIO_EXTS)
    if not tracks:
        raise RuntimeError(f"No audio tracks found in {music_dir}")
    total_duration = MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION
    rng.shuffle(tracks)
    skipped: list[str] = []
    for track in tracks:
        ranges = allowed_music_ranges(track)
        if not ranges:
            skipped.append(f"{track.name}: no_allowed_range")
            continue
        duration = ffprobe_duration(track)
        for start, end in ranges:
            usable_end = duration if end is None else min(duration, end)
            if usable_end - start >= total_duration:
                max_start = usable_end - total_duration
                selected_start = start if max_start <= start else rng.uniform(start, max_start)
                selected_start = round(selected_start, 3)
                return track, selected_start, total_duration, {
                    "allowed_start": start,
                    "allowed_end": usable_end,
                    "allowed_end_is_track_end": end is None,
                    "selected_end": round(selected_start + total_duration, 3),
                    "track_duration": duration,
                }
        skipped.append(f"{track.name}: ranges_too_short_for_{total_duration:.1f}s")
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
    run_bans: dict[str, list[dict[str, Any]]] = {}
    run_margin = 0.75
    for idx, (timeline_start, timeline_end) in enumerate(slots):
        dur = timeline_end - timeline_start
        unused_pool = [path for path in usable if path not in recent]
        pool = unused_pool or [path for path in usable if path not in recent[-2:]] or usable
        selected: tuple[Path, float] | None = None
        for _ in range(80):
            path = rng.choice(pool)
            max_start = max(0.0, durations[path] - dur - 0.2)
            source_start = rng.uniform(0.0, max_start) if max_start > 0 else 0.0
            source_end = source_start + dur
            if not overlaps_ban(path.name, source_start, source_end, normalized_bans) and not overlaps_ban(
                path.name,
                source_start,
                source_end,
                run_bans,
            ):
                selected = (path, source_start)
                break
        if selected is None:
            for path in pool:
                max_start = max(0.0, durations[path] - dur - 0.2)
                candidate = 0.0
                while candidate <= max_start:
                    candidate_end = candidate + dur
                    if not overlaps_ban(path.name, candidate, candidate_end, normalized_bans) and not overlaps_ban(
                        path.name,
                        candidate,
                        candidate_end,
                        run_bans,
                    ):
                        selected = (path, candidate)
                        break
                    candidate += 0.5
                if selected is not None:
                    break
        if selected is None:
            raise RuntimeError(f"No source segment can avoid bans for slot {idx + 1}")
        path, source_start = selected
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
    outro_screens = [
        ["МОСТ", "В КЁНИГСБЕРГ"],
        ["ЗНАЙ ПРОШЛОЕ", "СТРОЙ БУДУЩЕЕ"],
    ]
    for lines_for_screen in outro_screens:
        total = int(OUTRO_SCREEN_DURATION * FPS)
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


def encode_video(frames_dir: Path, music: Path, music_start: float, total_duration: float, out_path: Path) -> None:
    audio_tmp = out_path.parent / "_kenigsberg_audio.m4a"
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
    video_dir, period_key, dataset_slug = choose_video_dataset(input_root, rng)
    log(f"Selected video dataset: period={period_key} path={video_dir}")
    mounted_video_files = iter_files(video_dir, VIDEO_EXTS)
    music_dir = find_dataset_dir(input_root, ["koenigsberg-music", "music"])
    if music_dir is None:
        raise RuntimeError(
            f"Music dataset is not mounted; available_inputs={mounted_input_dirs(input_root)}"
        )
    music, music_start, total_duration, music_meta = choose_music(music_dir, rng)
    rhythm_slots, rhythm_meta = beat_slots(
        MAIN_DURATION,
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
    segments = render_frames(render_payload, video_dir, frames_dir, rng, rhythm_slots)
    main_duration = float(render_payload.get("main_duration") or MAIN_DURATION)
    total_duration = main_duration + 2 * OUTRO_SCREEN_DURATION
    music_meta["selected_end"] = round(music_start + total_duration, 3)
    out_path = working / "kenigsberg_story_final.mp4"
    encode_video(frames_dir, music, music_start, total_duration, out_path)
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
        },
        "text": {
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
        f"(allowed {float(music_meta.get('allowed_start') or 0.0):.2f}-{float(music_meta.get('allowed_end') or 0.0):.2f}s)\n"
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
                "music_start": music_start,
                "music_end": music_meta.get("selected_end"),
                "music_allowed_range": manifest["music_allowed_range"],
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
