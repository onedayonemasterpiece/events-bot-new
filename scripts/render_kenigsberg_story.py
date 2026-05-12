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


W = 720
H = 1280
FPS = 30
MAIN_DURATION = 18.0
OUTRO_SCREEN_DURATION = 3.5
WATERMARK_TEXT = "Мост в Кёнигсберг"
OUTRO_BG = (0, 0, 0)
OUTRO_STRIP = (241, 228, 75)
OUTRO_TEXT = (16, 14, 14)
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}
AUDIO_EXTS = {".flac", ".mp3", ".wav", ".m4a", ".aac", ".ogg"}

MUSIC_RANGES = {
    "the promise.flac": [(224.0, 266.0), (402.0, None)],
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


def split_scene_lines(thought: str, count: int) -> list[str]:
    text = " ".join(str(thought or "").split())
    if not text:
        return ["Кёнигсберг помнит больше, чем кажется."]
    text = re.sub(
        r"^(?:а\s+вы\s+знали\??|знали\s+ли\s+вы,\s+что|возможно\s+вы\s+не\s+знали,?|сегодня\s+вы\s+узнаете,\s+что|теперь\s+вы\s+будете\s+знать,\s+что)\s+",
        "",
        text,
        flags=re.I,
    ).strip()
    raw_parts: list[str] = []
    for sentence in re.split(r"(?<=[.!?])\s+", text):
        sentence = sentence.strip()
        if not sentence:
            continue
        raw_parts.extend(part.strip(" .") for part in re.split(r"\s+[—:]\s+", sentence) if part.strip())
    parts: list[str] = []
    for part in raw_parts:
        words = part.split()
        if len(words) <= 7:
            parts.append(part)
            continue
        chunk = 5
        for i in range(0, len(words), chunk):
            piece = " ".join(words[i : i + chunk]).strip()
            if piece:
                parts.append(piece)
    cleaned: list[str] = []
    seen: set[str] = set()
    for part in parts:
        key = part.casefold()
        if key and key not in seen:
            cleaned.append(part)
            seen.add(key)
    return cleaned[: max(1, count)]


def payload_scene_lines(payload: dict[str, Any], fallback_thought: str, count: int) -> list[str]:
    raw = payload.get("scene_lines")
    if isinstance(raw, list):
        lines = [
            " ".join(str(item or "").split())
            for item in raw
            if " ".join(str(item or "").split())
        ]
        if lines:
            return lines[: max(1, count)]
    return split_scene_lines(fallback_thought, count)


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
        add = min(0.45, extra / len(durations))
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


def choose_music(music_dir: Path, rng: random.Random) -> tuple[Path, float, float]:
    tracks = iter_files(music_dir, AUDIO_EXTS)
    if not tracks:
        raise RuntimeError(f"No audio tracks found in {music_dir}")
    rng.shuffle(tracks)
    for track in tracks:
        key = track.stem.casefold()
        key_with_ext = track.name.casefold()
        ranges = MUSIC_RANGES.get(key_with_ext) or MUSIC_RANGES.get(key) or [(0.0, None)]
        duration = ffprobe_duration(track)
        for start, end in ranges:
            usable_end = duration if end is None else min(duration, end)
            if usable_end - start >= MAIN_DURATION:
                max_start = usable_end - MAIN_DURATION
                selected_start = start if max_start <= start else rng.uniform(start, max_start)
                return track, round(selected_start, 3), MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION
    fallback = tracks[0]
    return fallback, 0.0, min(max(MAIN_DURATION, ffprobe_duration(fallback)), MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION)


def beat_slots(duration: float, rng: random.Random) -> list[tuple[float, float]]:
    # MVP fallback grid: musical enough for first smoke, replaced by librosa in v1.1.
    base = rng.choice([1.75, 2.0, 2.25])
    starts = [0.0]
    t = base
    while t < duration - 0.5:
        starts.append(t)
        t += base * rng.choice([1, 2])
    starts.append(duration)
    return [(starts[i], starts[i + 1]) for i in range(len(starts) - 1) if starts[i + 1] - starts[i] >= 0.45]


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
    for idx, (timeline_start, timeline_end) in enumerate(slots):
        dur = timeline_end - timeline_start
        pool = [path for path in usable if path not in recent[-2:]] or usable
        selected: tuple[Path, float] | None = None
        for _ in range(80):
            path = rng.choice(pool)
            max_start = max(0.0, durations[path] - dur - 0.2)
            source_start = rng.uniform(0.0, max_start) if max_start > 0 else 0.0
            if not overlaps_ban(path.name, source_start, source_start + dur, normalized_bans):
                selected = (path, source_start)
                break
        if selected is None:
            for path in pool:
                max_start = max(0.0, durations[path] - dur - 0.2)
                candidate = 0.0
                while candidate <= max_start:
                    if not overlaps_ban(path.name, candidate, candidate + dur, normalized_bans):
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
        recent.append(path)
    return segments


def render_frames(payload: dict[str, Any], video_dir: Path, out_dir: Path, rng: random.Random) -> list[dict[str, Any]]:
    if cv2 is None:
        raise RuntimeError("opencv-python is not installed")
    crop_px = int(payload.get("crop_bottom_px") or 96)
    videos = iter_files(video_dir, VIDEO_EXTS)
    rng.shuffle(videos)
    slots = beat_slots(MAIN_DURATION, rng)
    segments = pick_video_segments(
        videos,
        slots,
        rng=rng,
        dataset_slug=str(payload.get("dataset") or video_dir.name),
        crop_px=crop_px,
        source_bans=payload.get("source_bans") or {},
    )
    thought = str(payload.get("thought_text") or "").strip()
    lines = payload_scene_lines(payload, thought, max(4, min(7, len(segments))))
    text_cues = build_text_cues(lines, MAIN_DURATION)
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
    for seg_idx, segment in enumerate(segments):
        cap = cv2.VideoCapture(segment["source_path"])
        cap.set(cv2.CAP_PROP_POS_MSEC, float(segment["source_start"]) * 1000.0)
        frame_count = max(1, int(round((segment["timeline_end"] - segment["timeline_start"]) * FPS)))
        midpoint = (float(segment["timeline_start"]) + float(segment["timeline_end"])) / 2.0
        active_midpoint = text_cue_at(text_cues, midpoint)
        segment["text"] = active_midpoint[0] if active_midpoint else ""
        last_frame = None
        for local_frame in range(frame_count):
            ok, frame = cap.read()
            if ok:
                last_frame = frame
            elif last_frame is not None:
                frame = last_frame
            else:
                raise RuntimeError(f"Could not read frame from {segment['source_file']}")
            frame = crop_veo_bottom(frame, crop_px)
            image = cover_resize(frame)
            timeline_t = float(segment["timeline_start"]) + (local_frame / FPS)
            active_text = text_cue_at(text_cues, timeline_t)
            if local_frame < 3 and frame_no > 1:
                overlay = Image.new("RGBA", (W, H), (0, 0, 0, int(120 * (1 - local_frame / 3))))
                image.alpha_composite(overlay)
            if active_text:
                text, cue_t, cue_duration = active_text
                draw_stripes(image, text, t=cue_t, scene_duration=cue_duration)
            draw_watermark(image)
            image.convert("RGB").save(out_dir / f"frame_{frame_no:05d}.jpg", quality=92)
            frame_no += 1
        cap.release()
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
    music_dir = find_dataset_dir(input_root, ["koenigsberg-music", "music"])
    if music_dir is None:
        raise RuntimeError(
            f"Music dataset is not mounted; available_inputs={mounted_input_dirs(input_root)}"
        )
    music, music_start, total_duration = choose_music(music_dir, rng)
    frames_dir = working / "kenigsberg_frames"
    if frames_dir.exists():
        shutil.rmtree(frames_dir)
    render_payload = dict(payload)
    render_payload["dataset"] = dataset_slug
    segments = render_frames(render_payload, video_dir, frames_dir, rng)
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
        "total_duration": total_duration,
        "strategy": "heuristic_v1",
        "frame_count": frame_count,
        "preview_frames": preview_frames,
        "hook": str(render_payload.get("hook") or ""),
        "scene_lines": render_payload.get("scene_lines") or [],
        "text_cues": render_payload.get("text_cues") or [],
        "segments": segments,
        "output": out_path.name,
        "output_size_bytes": out_path.stat().st_size if out_path.exists() else 0,
    }
    render_log = {
        "selected_video_dataset": {
            "period_key": period_key,
            "dataset": dataset_slug,
            "path": str(video_dir),
            "video_count": len(iter_files(video_dir, VIDEO_EXTS)),
        },
        "selected_music": {
            "file": music.name,
            "path": str(music),
            "start": music_start,
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
        f"- Music: {music.name} @ {music_start:.2f}s\n"
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
