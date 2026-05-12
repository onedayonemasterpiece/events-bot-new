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
OUTRO_SCREEN_DURATION = 2.2
WATERMARK_TEXT = "Мост в Кёнигсберг"
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
    for item in sorted(input_root.iterdir()) if input_root.exists() else []:
        if not item.is_dir():
            continue
        name = item.name.casefold()
        if any(alias in name for alias in normalized):
            return item
    return None


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
    y0 = 160
    appear = min(1.0, max(0.0, t / 0.55))
    disappear = min(1.0, max(0.0, (t - (scene_duration - 0.45)) / 0.45))
    for idx, line in enumerate(lines):
        local_delay = idx * 0.09
        in_p = ease_out_cubic(max(0.0, min(1.0, (appear - local_delay) / 0.6)))
        out_p = ease_in_cubic(max(0.0, min(1.0, (disappear - local_delay) / 0.5)))
        text_w = int(draw.textlength(line, font=headline_font))
        stripe_w = min(W - x0 - 44, text_w + 46)
        visible_w = int(stripe_w * (1.0 - out_p) * in_p)
        y = y0 + idx * (stripe_h + gap)
        if visible_w <= 1:
            continue
        draw.rectangle((x0, y, x0 + visible_w, y + stripe_h), fill=(241, 228, 75, 230))
        text_y_final = y + 8
        text_y = text_y_final + int((1.0 - in_p) * stripe_h) + int(out_p * 22)
        mask = Image.new("L", image.size, 0)
        md = ImageDraw.Draw(mask)
        md.rectangle((x0, y, x0 + visible_w, y + stripe_h), fill=255)
        text_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
        td = ImageDraw.Draw(text_layer)
        td.text((x0 + 23, text_y), line, font=headline_font, fill=(16, 14, 14, 255))
        image.alpha_composite(Image.composite(text_layer, Image.new("RGBA", image.size), mask))


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
    sentences = re.split(r"(?<=[.!?])\s+", " ".join(thought.split()))
    sentences = [s.strip() for s in sentences if s.strip()]
    if not sentences:
        return ["Кёнигсберг помнит больше, чем кажется."]
    if len(sentences) >= count:
        return sentences[:count]
    words = thought.split()
    chunk = max(5, math.ceil(len(words) / count))
    lines = [" ".join(words[i : i + chunk]) for i in range(0, len(words), chunk)]
    return [line for line in lines if line][:count]


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
    lines = split_scene_lines(thought, max(4, min(7, len(segments))))
    out_dir.mkdir(parents=True, exist_ok=True)
    frame_no = 1
    for seg_idx, segment in enumerate(segments):
        cap = cv2.VideoCapture(segment["source_path"])
        cap.set(cv2.CAP_PROP_POS_MSEC, float(segment["source_start"]) * 1000.0)
        frame_count = max(1, int(round((segment["timeline_end"] - segment["timeline_start"]) * FPS)))
        text = lines[min(seg_idx, len(lines) - 1)]
        for local_frame in range(frame_count):
            ok, frame = cap.read()
            if not ok:
                cap.set(cv2.CAP_PROP_POS_MSEC, float(segment["source_start"]) * 1000.0)
                ok, frame = cap.read()
            if not ok:
                raise RuntimeError(f"Could not read frame from {segment['source_file']}")
            frame = crop_veo_bottom(frame, crop_px)
            image = cover_resize(frame)
            local_t = local_frame / FPS
            scene_duration = frame_count / FPS
            if local_frame < 3 and frame_no > 1:
                overlay = Image.new("RGBA", (W, H), (0, 0, 0, int(120 * (1 - local_frame / 3))))
                image.alpha_composite(overlay)
            draw_stripes(image, text, t=local_t, scene_duration=scene_duration)
            draw_watermark(image)
            image.convert("RGB").save(out_dir / f"frame_{frame_no:05d}.jpg", quality=92)
            frame_no += 1
        cap.release()
    for text in ("Мост в Кёнигсберг", "Знай прошлое — строй будущее"):
        total = int(OUTRO_SCREEN_DURATION * FPS)
        for i in range(total):
            image = Image.new("RGBA", (W, H), (11, 12, 16, 255))
            draw_stripes(image, text, t=i / FPS, scene_duration=OUTRO_SCREEN_DURATION)
            image.convert("RGB").save(out_dir / f"frame_{frame_no:05d}.jpg", quality=92)
            frame_no += 1
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


def main() -> None:
    input_root = Path(os.getenv("KAGGLE_INPUT_ROOT", "/kaggle/input"))
    working = Path(os.getenv("KAGGLE_WORKING_DIR", "/kaggle/working"))
    session_root = find_session_root(input_root)
    payload = json.loads((session_root / "payload.json").read_text(encoding="utf-8"))
    seed = int(payload.get("seed") or 1)
    rng = random.Random(seed)
    period_key = str(payload.get("period_key") or "1919-1940")
    video_dir = (
        find_dataset_dir(input_root, ["koenigsberg-winter"])
        if period_key == "winter"
        else find_dataset_dir(input_root, ["koenigsberg19191940", "19191940"])
    )
    if video_dir is None:
        raise RuntimeError(f"Video dataset for period={period_key!r} is not mounted")
    music_dir = find_dataset_dir(input_root, ["koenigsberg-music", "music"])
    if music_dir is None:
        raise RuntimeError("Music dataset is not mounted")
    music, music_start, total_duration = choose_music(music_dir, rng)
    frames_dir = working / "kenigsberg_frames"
    if frames_dir.exists():
        shutil.rmtree(frames_dir)
    segments = render_frames(payload, video_dir, frames_dir, rng)
    out_path = working / "kenigsberg_story_final.mp4"
    encode_video(frames_dir, music, music_start, total_duration, out_path)
    manifest = {
        "issue_number": int(payload.get("issue_number") or 0),
        "dataset": str(payload.get("dataset") or video_dir.name),
        "period_key": period_key,
        "thought_id": str(payload.get("thought_id") or ""),
        "thought_text": str(payload.get("thought_text") or ""),
        "music_file": music.name,
        "music_start": music_start,
        "strategy": "heuristic_v1",
        "segments": segments,
        "output": out_path.name,
    }
    (working / "kenigsberg_issue_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (working / "kenigsberg_manifest.md").write_text(
        "# Kenigsberg Story\n\n"
        f"- Issue: #{manifest['issue_number']}\n"
        f"- Period: {period_key}\n"
        f"- Music: {music.name} @ {music_start:.2f}s\n"
        f"- Segments: {len(segments)}\n",
        encoding="utf-8",
    )
    log(str(out_path))


if __name__ == "__main__":
    main()
