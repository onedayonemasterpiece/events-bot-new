from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from PIL import Image, ImageColor, ImageDraw

try:
    from moviepy import AudioFileClip
except ImportError:
    from moviepy.audio.io.AudioFileClip import AudioFileClip

try:
    from moviepy.audio.fx.MultiplyVolume import MultiplyVolume
except ImportError:
    MultiplyVolume = None

try:
    from moviepy.audio.fx.volumex import volumex as volumex_fx
except ImportError:
    try:
        from moviepy.audio.fx.all import volumex as volumex_fx
    except ImportError:
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
INTRO_END_FRAME = approval.INTRO_END_FRAME
FINAL_CARD_DURATION = 3.5
FINAL_CARD_FADE_IN = 0.3
AUDIO_BITRATE = "128k"
FIRST_PRIMARY_SCENE_START_LOCAL = approval.SCENE1_START_LOCAL
FINAL_VIDEO_CODEC = "libx265"
FINAL_VIDEO_TAG = "hvc1"
FINAL_VIDEO_PRESET = "slow"
FINAL_VIDEO_CRF = "28"
FINAL_X265_PARAMS = (
    f"keyint={FPS}:min-keyint={FPS}:scenecut=0:no-open-gop=1:"
    "repeat-headers=1:aq-mode=3:vbv-maxrate=3000:vbv-bufsize=6000"
)
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
) -> str:
    explicit_location = _normalize_text(location_name)
    explicit_city = _normalize_text(city).split(",")[0].strip()
    raw_location_norm = _normalize_text(raw_location)
    fallback_location = raw_location_norm.split(",")[0].strip() if raw_location_norm else ""
    city_value = explicit_city or _extract_city_from_location(raw_location_norm)
    parts = [
        part.upper()
        for part in (explicit_location or fallback_location, city_value)
        if part
    ]
    if parts:
        return " • ".join(dict.fromkeys(parts))
    return raw_location_norm.upper()


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
) -> Image.Image:
    font = approval.font(font_path, font_size)
    bbox = font.getbbox(text)
    text_w = int(math.ceil(font.getlength(text)))
    text_h = bbox[3] - bbox[1]
    pad_x = max(18, round(25 * (W / 1080.0)))
    strip_w = text_w + pad_x * 2
    text_y = int((strip_height - text_h) / 2 - bbox[1])
    image = Image.new("RGBA", (strip_w, strip_height), (*OUTRO_STRIP, 255))
    draw = ImageDraw.Draw(image)
    draw.text((pad_x, text_y), text, font=font, fill=(*OUTRO_TEXT, 255))
    return image


def _build_render_scenes(payload: dict) -> list[RenderScene]:
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
        for candidate in images:
            image_path = _resolve_image_path(candidate)
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
                title=str(raw_scene.get("about") or raw_scene.get("title") or "").strip(),
                date_line=_format_display_date(
                    raw_scene.get("date_iso") or raw_scene.get("date"),
                    raw_scene.get("time"),
                ),
                location_line=_format_display_location(
                    location_name=raw_scene.get("location_name"),
                    city=raw_scene.get("city"),
                    raw_location=raw_scene.get("location"),
                ),
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


def _build_primary_blocks(scene: RenderScene):
    blocks: list = []
    scale = W / 1080.0
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
    location_blocks, _ = approval._build_text_blocks(
        scene.location_line,
        font_path=PRIMARY_TITLE_FONT,
        font_size=max(22, round(45 * scale)),
        text_color=DETAIL_COLOR,
        start_time=SCENE_TEXT_START + 0.4,
        duration=approval.T_INFO + 1.0,
        start_y=next_y + round(52 * scale),
    )
    blocks.extend(location_blocks)
    return blocks


def _build_followup_blocks(scene: RenderScene):
    description = scene.description or scene.title
    return approval._build_text_blocks(
        description,
        font_path=DESCRIPTION_FONT,
        font_size=max(24, round(38 * (W / 1080.0))),
        text_color=TITLE_COLOR,
        start_time=SCENE_TEXT_START + 0.12,
        duration=approval.T_INFO + 1.0,
        start_y=SPLIT_Y + round(46 * (W / 1080.0)),
    )[0]


def _render_scene_frame(scene: RenderScene, local_t: float, text_blocks) -> Image.Image:
    if scene.variant == "brand_outro":
        return _render_brand_outro_frame(local_t)
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
    scale = W / 1080.0
    strip_height = max(56, round(210 * scale))
    gap = max(8, round(20 * scale))
    font_size = max(48, round(160 * scale))
    slide_duration = 0.8
    words_conf = [
        {"text": "ПОЛЮБИТЬ", "side": "left", "delay": 0.0},
        {"text": "КАЛИНИНГРАД", "side": "right", "delay": 0.4},
        {"text": "АНОНСЫ", "side": "left", "delay": 0.8},
    ]
    step_y = strip_height + gap
    total_block_h = len(words_conf) * step_y - gap
    start_y_block = (H - total_block_h) / 2.0
    canvas = Image.new("RGBA", (W, H), (*OUTRO_BG, 255))

    for idx, item in enumerate(words_conf):
        strip = _build_outro_strip(
            item["text"],
            font_path=PRIMARY_TITLE_FONT,
            font_size=font_size,
            strip_height=strip_height,
        )
        final_x = (W - strip.width) / 2.0
        final_y = start_y_block + idx * step_y
        start_x = -strip.width - round(100 * scale) if item["side"] == "left" else W + round(100 * scale)
        if local_t < item["delay"]:
            x = float(start_x)
        else:
            progress = min(1.0, max(0.0, (local_t - item["delay"]) / slide_duration))
            x = start_x + (final_x - start_x) * ease_out_expo(progress)
        _paste_rgba_subpixel(canvas, strip, x, final_y)

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
            _build_followup_blocks(scene)
            if scene.variant == "followup_image"
            else _build_primary_blocks(scene)
        )
        local_t = float(scene.start_local)
        scene_total_local = (
            FINAL_CARD_DURATION if scene.variant == "brand_outro" else SCENE_TOTAL_LOCAL
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
    if MultiplyVolume is not None and hasattr(audio, "with_effects"):
        return audio.with_effects([MultiplyVolume(factor)])
    if volumex_fx is not None:
        if hasattr(audio, "fx"):
            return audio.fx(volumex_fx, factor)
        return volumex_fx(audio, factor)
    print(
        "CherryFlash: moviepy build has no supported volume-scaling API; keeping original level.",
        file=sys.stderr,
    )
    return audio


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
            "crf": FINAL_VIDEO_CRF,
            "audio_bitrate": AUDIO_BITRATE,
            "extra_args": [
                "-tag:v",
                FINAL_VIDEO_TAG,
                "-g",
                str(FPS),
                "-x265-params",
                FINAL_X265_PARAMS,
            ],
        }
    return {
        "video_codec": PREVIEW_VIDEO_CODEC,
        "preset": PREVIEW_VIDEO_PRESET,
        "crf": PREVIEW_VIDEO_CRF,
        "audio_bitrate": AUDIO_BITRATE,
        "extra_args": [],
    }


def _encode_video(*, final_frame: int, audio_shift_seconds: float) -> Path:
    # --- 1. Prepare trimmed + volume-scaled audio as a temp WAV ----------
    audio_path = _candidate_audio_path()
    audio = AudioFileClip(str(audio_path))
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
    audio.write_audiofile(str(tmp_audio), fps=44100, logger=None)
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
        "-crf",
        str(profile["crf"]),
        "-pix_fmt",
        "yuv420p",
        *list(profile["extra_args"]),
        "-c:a",
        "aac",
        "-b:a",
        str(profile["audio_bitrate"]),
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
