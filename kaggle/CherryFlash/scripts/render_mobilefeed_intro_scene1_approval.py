from __future__ import annotations

import json
import math
import os
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from PIL import Image, ImageColor, ImageDraw, ImageFilter, ImageFont

try:
    from moviepy import AudioFileClip, ImageSequenceClip
except ImportError:
    from moviepy.audio.io.AudioFileClip import AudioFileClip
    from moviepy.video.io.ImageSequenceClip import ImageSequenceClip

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

from scripts.render_mobilefeed_intro_still import (
    AKROBAT_BOLD,
    AKROBAT_BLACK,
    BLENDER,
    BLENDER_SCRIPT,
    DRUK_SUPER,
    FOCUS_EVENT_ID,
    POSTERS,
    VARIANTS,
    atlas_and_meta,
    build_variant_config,
    ensure_dirs,
    make_overlay_texture,
    make_screen_label_texture,
    make_screen_texture,
    make_shadow_texture,
)

FINAL_MODE = "--final" in sys.argv
MODE_SLUG = "final" if FINAL_MODE else "preview"

OUT_DIR = ARTIFACTS_ROOT / "codex" / f"mobilefeed_intro_scene1_{MODE_SLUG}"
TEXTURE_DIR = OUT_DIR / "textures"
CONFIG_DIR = OUT_DIR / "configs"
INTRO_RAW_FRAMES_DIR = OUT_DIR / "intro_frames_raw"
FRAMES_DIR = OUT_DIR / "frames"
PREVIEW_FRAMES_DIR = OUT_DIR / "preview_frames"
INTRO_SPARSE_DIR = OUT_DIR / "intro_frames_sparse"
INTRO_INTERP_DIR = OUT_DIR / "intro_frames_interp"

W = 1080 if FINAL_MODE else 360
H = 1920 if FINAL_MODE else 640
FPS = 30
# Motion approval must use real per-frame 3D renders; otherwise synthetic
# in-between frames hide the true camera rhythm and reintroduce fake stutter.
INTRO_RENDER_STEP = 1
SPLIT_RATIO = 0.6
SPLIT_Y = int(H * SPLIT_RATIO)

ACTIVE_VARIANT = VARIANTS[0]
INTRO_START_FRAME = 1
INTRO_END_FRAME = 106
SCENE1_START_FRAME = INTRO_END_FRAME + 1
STRONG_BEAT_SEC = 3.576

T_ENTRY = 0.45
T_HOLD = 1.2
T_MOVE = 0.75
T_INFO = 2.5
T_EXIT = 0.45
SCENE1_TOTAL_LOCAL = T_ENTRY + T_HOLD + T_MOVE + T_INFO + T_EXIT
SCENE1_START_LOCAL = 1.80
HANDOFF_BLEND_START = 105 if FINAL_MODE else 104
HANDOFF_BLEND_END = 106
DENSE_TAIL_START = 88 if FINAL_MODE else 96
SCENE1_TEXT_START = T_ENTRY + T_HOLD + (T_MOVE * 0.2)

TITLE_COLOR = ImageColor.getrgb("#FFFFFF")
ACCENT_COLOR = ImageColor.getrgb("#F1C40F")
DETAIL_COLOR = ImageColor.getrgb("#BDC3C7")
BG_BLACK = ImageColor.getrgb("#000000")
BG_MILKY = ImageColor.getrgb("#E7E6E1")
DISABLE_DENOISE = str(os.environ.get("CHERRYFLASH_DISABLE_DENOISE", "")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

AUDIO_PATH = next(
    (
        candidate
        for candidate in (
            ROOT / "assets" / "Pulsarium_scene1_clip.mp3",
            ROOT / "kaggle" / "CherryFlash" / "assets" / "Pulsarium_scene1_clip.mp3",
            ROOT / "video_announce" / "assets" / "Pulsarium.mp3",
        )
        if candidate.exists()
    ),
    ROOT / "video_announce" / "assets" / "Pulsarium.mp3",
)
AUDIO_START_SEC = 0 if AUDIO_PATH.name == "Pulsarium_scene1_clip.mp3" else 294
DB_SNAPSHOT = ROOT / "artifacts" / "db" / "db_prod_snapshot_2026-04-06_114841.sqlite"
BEBAS_BOLD = ROOT / "video_announce" / "assets" / "BebasNeue-Bold.ttf"


MONTHS_GENITIVE = {
    1: "ЯНВАРЯ",
    2: "ФЕВРАЛЯ",
    3: "МАРТА",
    4: "АПРЕЛЯ",
    5: "МАЯ",
    6: "ИЮНЯ",
    7: "ИЮЛЯ",
    8: "АВГУСТА",
    9: "СЕНТЯБРЯ",
    10: "ОКТЯБРЯ",
    11: "НОЯБРЯ",
    12: "ДЕКАБРЯ",
}


@dataclass(frozen=True)
class ScenePayload:
    title: str
    date_line: str
    location_line: str
    poster_path: Path


@dataclass(frozen=True)
class WordBlock:
    image: Image.Image
    x: int
    y: int
    start_time: float
    life_duration: float


def ease_out_cubic(t: float) -> float:
    t = max(0.0, min(1.0, t))
    return 1.0 - (1.0 - t) ** 3


def ease_in_cubic(t: float) -> float:
    t = max(0.0, min(1.0, t))
    return t * t * t


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


def _lerp_rgb(a: tuple[int, int, int], b: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    return tuple(round(x + (y - x) * t) for x, y in zip(a, b))


def ensure_local_dirs(*, preserve_raw: bool = False) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    TEXTURE_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    INTRO_RAW_FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    INTRO_SPARSE_DIR.mkdir(parents=True, exist_ok=True)
    INTRO_INTERP_DIR.mkdir(parents=True, exist_ok=True)
    FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    PREVIEW_FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    purge_dirs = [INTRO_SPARSE_DIR, INTRO_INTERP_DIR, FRAMES_DIR, PREVIEW_FRAMES_DIR]
    if not preserve_raw:
        purge_dirs.append(INTRO_RAW_FRAMES_DIR)
    for directory in purge_dirs:
        for child in directory.glob("*.png"):
            child.unlink()


def font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(path), size=size)


def _draw_text_size(draw: ImageDraw.ImageDraw, text: str, fnt: ImageFont.FreeTypeFont) -> tuple[int, int]:
    left, top, right, bottom = draw.textbbox((0, 0), text, font=fnt)
    return right - left, bottom - top


def _fit_font(text: str, font_path: Path, max_size: int, min_size: int, max_width: int) -> ImageFont.FreeTypeFont:
    probe = ImageDraw.Draw(Image.new("RGB", (32, 32)))
    size = max_size
    while size >= min_size:
        current = font(font_path, size)
        if _draw_text_size(probe, text, current)[0] <= max_width:
            return current
        size -= 2
    return font(font_path, min_size)


def _alpha_image(base: Image.Image, alpha: float) -> Image.Image:
    alpha = max(0.0, min(1.0, alpha))
    if alpha >= 0.999:
        return base
    image = base.copy()
    alpha_channel = image.getchannel("A").point(lambda value: int(value * alpha))
    image.putalpha(alpha_channel)
    return image


def _resize_poster_clean(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    resized = image.resize(size, Image.Resampling.LANCZOS)
    return resized.filter(ImageFilter.UnsharpMask(radius=1.2, percent=120, threshold=2))


def _safe_stem(name: str) -> str:
    return (
        name.replace("(", "")
        .replace(")", "")
        .replace('"', "")
        .replace("«", "")
        .replace("»", "")
        .replace("/", "-")
        .strip()
    )


def load_scene_payload() -> ScenePayload:
    poster = POSTERS[FOCUS_EVENT_ID]
    title = poster.title
    date_line = "10 АПРЕЛЯ"
    location_line = poster.city.upper()
    if DB_SNAPSHOT.exists():
        connection = sqlite3.connect(DB_SNAPSHOT)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                "SELECT title, date, time, location_name, city FROM event WHERE id = ?",
                (FOCUS_EVENT_ID,),
            ).fetchone()
        finally:
            connection.close()
        if row:
            title = row["title"] or title
            raw_date = row["date"] or poster.date
            raw_time = row["time"] or ""
            dt = date.fromisoformat(raw_date)
            date_line = f"{dt.day} {MONTHS_GENITIVE[dt.month]}"
            if raw_time and raw_time != "00:00":
                date_line = f"{date_line} • {raw_time}"
            location_name = (row["location_name"] or row["city"] or poster.city or "").split(",")[0].strip()
            city = (row["city"] or poster.city or "").strip()
            parts = [part.upper() for part in (location_name, city) if part]
            if parts:
                location_line = " • ".join(dict.fromkeys(parts))
    return ScenePayload(
        title=title,
        date_line=date_line,
        location_line=location_line,
        poster_path=poster.image_path,
    )


def render_intro_frames() -> None:
    ensure_dirs()
    reuse_raw = str(os.environ.get("CHERRYFLASH_REUSE_RAW_INTRO", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    rerender_tail_from_env = os.environ.get("CHERRYFLASH_RERENDER_TAIL_FROM", "").strip()
    rerender_tail_from = int(rerender_tail_from_env) if rerender_tail_from_env else None
    ensure_local_dirs(preserve_raw=reuse_raw)
    atlas_path, ribbon_meta = atlas_and_meta()
    shadow_path = TEXTURE_DIR / "mobilefeed_shadow.png"
    make_shadow_texture(shadow_path)

    overlay_path = TEXTURE_DIR / f"{ACTIVE_VARIANT.slug}_approval_overlay.png"
    screen_path = TEXTURE_DIR / f"{ACTIVE_VARIANT.slug}_approval_screen.png"
    screen_top_label_path = TEXTURE_DIR / f"{ACTIVE_VARIANT.slug}_approval_screen_top.png"
    screen_bottom_label_path = TEXTURE_DIR / f"{ACTIVE_VARIANT.slug}_approval_screen_bottom.png"
    output_anchor = OUT_DIR / "handoff_anchor.png"
    config_path = CONFIG_DIR / f"{ACTIVE_VARIANT.slug}_scene1_approval.json"

    make_overlay_texture(ACTIVE_VARIANT, overlay_path)
    make_screen_texture(ACTIVE_VARIANT, screen_path)
    make_screen_label_texture(ACTIVE_VARIANT.screen_top, screen_top_label_path, position="top")
    make_screen_label_texture(ACTIVE_VARIANT.screen_bottom, screen_bottom_label_path, position="bottom")

    cfg = build_variant_config(
        ACTIVE_VARIANT,
        atlas_path=atlas_path,
        ribbon_meta=ribbon_meta,
        shadow_path=shadow_path,
        overlay_path=overlay_path,
        screen_path=screen_path,
        screen_top_label_path=screen_top_label_path,
        screen_bottom_label_path=screen_bottom_label_path,
        output_path=output_anchor,
    )
    cfg.update(
        {
            "res_x": W,
            "res_y": H,
            "render_engine": "CYCLES",
            "samples": 48 if FINAL_MODE else 1,
            "allow_cycles_gpu": True,
            "use_denoising": not DISABLE_DENOISE,
            "denoiser": "OPENIMAGEDENOISE",
            "use_adaptive_sampling": True,
            "adaptive_threshold": 0.014 if FINAL_MODE else 0.12,
            "sample_clamp_direct": 5.0 if FINAL_MODE else 3.0,
            "sample_clamp_indirect": 1.0 if FINAL_MODE else 0.8,
            "max_bounces": 4 if FINAL_MODE else 3,
            "diffuse_bounces": 1,
            "glossy_bounces": 4 if FINAL_MODE else 2,
            "transmission_bounces": 2 if FINAL_MODE else 1,
            "transparent_max_bounces": 4 if FINAL_MODE else 3,
            "hdri_strength": 0.010,
            "hdri_rot_deg": 116.0,
            "backdrop_color": "#E7E6E1",
            "camera_background_color": "#E7E6E1",
            "backdrop_emission_strength": 0.0,
            "backdrop_roughness": 1.0,
            "backdrop_bump_strength": 0.0,
            "tonal_transition_key_multiplier": 0.72,
            "tonal_transition_fill_multiplier": 0.10,
            "tonal_transition_top_multiplier": 0.18,
            "tonal_transition_edge_multiplier": 1.12,
            "key_light_energy": 5200,
            "key_light_size": (0.82, 0.66),
            "fill_light_energy": 2.5,
            "fill_light_size": (5.8, 4.8),
            "top_light_energy": 90,
            "top_light_size": (4.6, 4.1),
            "use_motion_blur": False,
            "motion_blur_shutter": 0.0,
            "camera_location": (3.52, -5.96, 4.74),
            "camera_target": (0.05, -0.74, 0.07),
            "screen_label_normal_offset": 0.0032,
            "use_shadow_plane": True,
            "shadow_location_x": 1.72,
            "shadow_scale_y": 2.04,
            "shadow_scale_z": 1.20,
            "shadow_offset_y": 0.18,
            "render_animation": True,
            "output_pattern": str(INTRO_RAW_FRAMES_DIR / "frame_"),
            "animation": {
                "fps": FPS,
                "frame_step": INTRO_RENDER_STEP,
                "frame_start": INTRO_START_FRAME,
                "frame_end": INTRO_END_FRAME,
                "timing_warp_control_points": [
                    [0.0, 0.0],
                    [0.32, 0.32],
                    [0.38, 0.392],
                    [0.42, 0.434],
                    [0.50, 0.510],
                    [0.54, 0.556],
                    [0.59, 0.610],
                    [0.76, 0.770],
                    [0.82, 0.846],
                    [0.88, 0.898],
                    [0.94, 0.950],
                    [1.0, 1.0],
                ],
                "combo_mid_frame": 36,
                "sync_start_frame": 72,
                "sync_mid_frame": 96,
                "combo_lens_mm": 62.0,
                "sync_start_lens_mm": 74.0,
                "sync_mid_lens_mm": 82.0,
                "end_lens_mm": 92.0,
                "combo_fill": 0.38,
                "sync_start_fill": 0.68,
                "sync_mid_fill": 0.92,
                "scene1_end_scale": 0.97,
                "combo_height_offset": 0.18,
                "combo_side_offset": 0.020,
                "sync_start_height_offset": 0.050,
                "sync_start_side_offset": 0.006,
                "sync_mid_height_offset": 0.010,
                "sync_mid_side_offset": 0.000,
                "end_height_offset": -0.002,
                "start_up_blend": 0.10,
                "combo_up_blend": 0.62,
                "sync_up_blend": 0.972,
                "focus_target_normal_offset": 0.010,
                "tonal_transition_start_frame": 76,
                "tonal_transition_end_frame": 106,
                "tonal_transition_from": "#E7E6E1",
                "tonal_transition_to": "#040507",
                "tonal_transition_emission_strength_end": 0.0,
                "screen_bottom_label_exit_start": 101,
                "screen_bottom_label_exit_end": 106,
                "screen_top_label_exit_start": 103,
                "screen_top_label_exit_end": 106,
            },
        }
    )
    def _run_blender_with_config(config: dict, path: Path) -> None:
        path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        subprocess.run(
            [
                str(BLENDER),
                "-b",
                "-P",
                str(BLENDER_SCRIPT),
                "--",
                "--config",
                str(path),
            ],
            check=True,
        )

    if not reuse_raw:
        _run_blender_with_config(cfg, config_path)

    raw_frames = sorted(INTRO_RAW_FRAMES_DIR.glob("frame_*.png"))
    if not raw_frames:
        raise RuntimeError("No raw intro frames were rendered by Blender")
    rendered_frames: dict[int, Path] = {}
    for raw_path in raw_frames:
        frame_num = int(raw_path.stem.split("_")[-1])
        sparse_path = INTRO_SPARSE_DIR / f"frame_{frame_num:04d}.png"
        shutil.copy2(raw_path, sparse_path)
        rendered_frames[frame_num] = sparse_path

    if INTRO_END_FRAME not in rendered_frames:
        end_cfg = json.loads(json.dumps(cfg))
        end_cfg["animation"]["frame_start"] = INTRO_END_FRAME
        end_cfg["animation"]["frame_end"] = INTRO_END_FRAME
        end_cfg["animation"]["frame_step"] = 1
        end_cfg["animation"]["keyframe_start_frame"] = INTRO_START_FRAME
        end_cfg["output_path"] = str(output_anchor)
        _run_blender_with_config(end_cfg, CONFIG_DIR / f"{ACTIVE_VARIANT.slug}_scene1_approval_endframe.json")

        end_raw = INTRO_RAW_FRAMES_DIR / f"frame_{INTRO_END_FRAME:04d}.png"
        if not end_raw.exists():
            raise RuntimeError(f"Expected end frame was not rendered: {end_raw}")
        sparse_path = INTRO_SPARSE_DIR / f"frame_{INTRO_END_FRAME:04d}.png"
        shutil.copy2(end_raw, sparse_path)
        rendered_frames[INTRO_END_FRAME] = sparse_path

    tail_start = rerender_tail_from if rerender_tail_from is not None else DENSE_TAIL_START
    should_rerender_tail = tail_start <= INTRO_END_FRAME and (INTRO_RENDER_STEP > 1 or rerender_tail_from is not None)
    if should_rerender_tail:
        dense_cfg = json.loads(json.dumps(cfg))
        dense_cfg["animation"]["frame_start"] = tail_start
        dense_cfg["animation"]["frame_end"] = INTRO_END_FRAME
        dense_cfg["animation"]["frame_step"] = 1
        dense_cfg["animation"]["keyframe_start_frame"] = INTRO_START_FRAME
        dense_cfg["output_path"] = str(output_anchor)
        _run_blender_with_config(dense_cfg, CONFIG_DIR / f"{ACTIVE_VARIANT.slug}_scene1_approval_dense_tail.json")
        for frame_num in range(tail_start, INTRO_END_FRAME + 1):
            dense_raw = INTRO_RAW_FRAMES_DIR / f"frame_{frame_num:04d}.png"
            if not dense_raw.exists():
                raise RuntimeError(f"Expected dense tail frame was not rendered: {dense_raw}")
            sparse_path = INTRO_SPARSE_DIR / f"frame_{frame_num:04d}.png"
            shutil.copy2(dense_raw, sparse_path)
            rendered_frames[frame_num] = sparse_path

    interp_frames = {}
    if FINAL_MODE:
        for frame_num in range(INTRO_START_FRAME, INTRO_END_FRAME + 1):
            if frame_num not in rendered_frames:
                raise RuntimeError(
                    f"Final mode requires a real rendered intro frame for every step; missing f{frame_num}"
                )
            dst = INTRO_INTERP_DIR / f"frame_{frame_num:04d}.png"
            shutil.copy2(rendered_frames[frame_num], dst)
            interp_frames[frame_num] = dst
    else:
        rendered_indices = sorted(rendered_frames)
        for frame_num in range(INTRO_START_FRAME, INTRO_END_FRAME + 1):
            dst = INTRO_INTERP_DIR / f"frame_{frame_num:04d}.png"
            if frame_num in rendered_frames:
                shutil.copy2(rendered_frames[frame_num], dst)
                interp_frames[frame_num] = dst
                continue
            prev_idx = max(idx for idx in rendered_indices if idx < frame_num)
            next_idx = min(idx for idx in rendered_indices if idx > frame_num)
            t = (frame_num - prev_idx) / max(1.0, next_idx - prev_idx)
            with Image.open(rendered_frames[prev_idx]).convert("RGBA") as prev_im, Image.open(rendered_frames[next_idx]).convert("RGBA") as next_im:
                blended = Image.blend(prev_im, next_im, t)
                blended.save(dst)
            interp_frames[frame_num] = dst

    overlay = Image.open(overlay_path).convert("RGBA")
    if overlay.size != (W, H):
        overlay = overlay.resize((W, H), Image.Resampling.LANCZOS)

    for frame_num in range(INTRO_START_FRAME, INTRO_END_FRAME + 1):
        frame_path = interp_frames.get(frame_num)
        if frame_path is None:
            frame_path = interp_frames[max(interp_frames)]
        with Image.open(frame_path).convert("RGBA") as frame:
            canvas = frame.copy()
        if frame_num <= 40:
            if frame_num <= 12:
                overlay_alpha = 1.0
                dy = 0
            else:
                progress = ease_in_out_cubic((frame_num - 12) / 28.0)
                overlay_alpha = 1.0 - progress
                dy = round(-72 * progress)
            overlay_layer = _alpha_image(overlay.copy(), overlay_alpha)
            layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            layer.alpha_composite(overlay_layer, (0, dy))
            canvas = Image.alpha_composite(canvas, layer)
        out_path = FRAMES_DIR / f"frame_{frame_num:04d}.png"
        canvas.save(out_path)

    shutil.copy2(FRAMES_DIR / f"frame_{INTRO_END_FRAME:04d}.png", OUT_DIR / "last_intro_frame.png")


def _poster_geometry(local_t: float, poster_size: tuple[int, int]) -> tuple[int, int, int, int]:
    src_w, src_h = poster_size
    if local_t < T_ENTRY:
        scale = 0.4 + (0.5 * ease_out_cubic(local_t / T_ENTRY))
        y_mode = "center"
    elif local_t < (T_ENTRY + T_HOLD):
        scale = 0.9 + (0.1 * ((local_t - T_ENTRY) / T_HOLD))
        y_mode = "center"
    elif local_t < (T_ENTRY + T_HOLD + T_MOVE):
        scale = 1.0
        progress = (local_t - (T_ENTRY + T_HOLD)) / T_MOVE
        start_y = H / 2
        end_y = SPLIT_Y / 2
        y_mode = int(start_y + (end_y - start_y) * ease_in_out_cubic(progress))
    elif local_t < (SCENE1_TOTAL_LOCAL - T_EXIT):
        scale = 1.0
        y_mode = int(SPLIT_Y / 2 - 10 * (local_t - (T_ENTRY + T_HOLD + T_MOVE)))
    else:
        scale = 1.0
        progress = (local_t - (SCENE1_TOTAL_LOCAL - T_EXIT)) / T_EXIT
        start_y = SPLIT_Y / 2 - 10 * T_INFO
        y_mode = int(start_y + (-src_h - start_y) * ease_in_cubic(progress))
    poster_w = round(W * scale)
    poster_h = round(poster_w * src_h / src_w)
    x = (W - poster_w) // 2
    y = (H - poster_h) // 2 if y_mode == "center" else int(y_mode - poster_h / 2)
    return x, y, poster_w, poster_h


def _render_block_image(word: str, font_path: Path, font_size: int, text_color: tuple[int, int, int]) -> Image.Image:
    fnt = font(font_path, font_size)
    bbox = fnt.getbbox(word)
    word_width = round(fnt.getlength(word))
    pad_x = 12
    sample_bbox = fnt.getbbox("HgЙj")
    sample_h = sample_bbox[3] - sample_bbox[1]
    block_h = int(sample_h * 1.25)
    canvas_w = word_width + pad_x * 2
    image = Image.new("RGBA", (canvas_w, block_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, canvas_w, block_h), fill=(10, 10, 10, 240))
    text_y = (block_h - sample_h) // 2 - sample_bbox[1]
    draw.text((pad_x, text_y), word, font=fnt, fill=text_color)
    return image


def _build_text_blocks(
    text: str,
    *,
    font_path: Path,
    font_size: int,
    text_color: tuple[int, int, int],
    start_time: float,
    duration: float,
    start_y: int,
) -> tuple[list[WordBlock], int]:
    fnt = font(font_path, font_size)
    space_w = round(fnt.getlength(" "))
    max_w = W - 100
    lines: list[list[str]] = []
    for raw_line in (text.split("\n") if text else [""]):
        words = [word for word in raw_line.split(" ") if word]
        if not words:
            lines.append([])
            continue
        current_line: list[str] = []
        current_width = 0
        for word in words:
            word_width = round(fnt.getlength(word))
            extra = word_width if not current_line else word_width + space_w
            if current_line and current_width + extra > max_w:
                lines.append(current_line)
                current_line = [word]
                current_width = word_width
            else:
                current_line.append(word)
                current_width += extra
        if current_line:
            lines.append(current_line)

    sample_bbox = fnt.getbbox("HgЙj")
    sample_h = sample_bbox[3] - sample_bbox[1]
    clip_h = int(sample_h * 1.25)
    word_blocks: list[WordBlock] = []
    global_index = 0
    current_y = start_y
    for line in lines:
        current_x = 50
        for word in line:
            image = _render_block_image(word, font_path, font_size, text_color)
            delay = global_index * 0.05
            life_duration = max(duration - delay, 0.5)
            word_blocks.append(
                WordBlock(
                    image=image,
                    x=current_x,
                    y=current_y,
                    start_time=start_time + delay,
                    life_duration=life_duration,
                )
            )
            current_x += image.width + 5
            global_index += 1
        current_y += clip_h + 2
    return word_blocks, current_y


def build_scene_text_blocks(payload: ScenePayload) -> list[WordBlock]:
    scale = W / 1080.0
    blocks: list[WordBlock] = []
    title_blocks, next_y = _build_text_blocks(
        payload.title,
        font_path=BEBAS_BOLD,
        font_size=max(36, round(90 * scale)),
        text_color=TITLE_COLOR,
        start_time=SCENE1_TEXT_START,
        duration=T_INFO + 1.0,
        start_y=SPLIT_Y + round(30 * scale),
    )
    blocks.extend(title_blocks)
    date_blocks, next_y = _build_text_blocks(
        payload.date_line,
        font_path=BEBAS_BOLD,
        font_size=max(24, round(50 * scale)),
        text_color=ACCENT_COLOR,
        start_time=SCENE1_TEXT_START + 0.2,
        duration=T_INFO + 1.0,
        start_y=next_y + round(30 * scale),
    )
    blocks.extend(date_blocks)
    location_blocks, _ = _build_text_blocks(
        payload.location_line,
        font_path=BEBAS_BOLD,
        font_size=max(22, round(45 * scale)),
        text_color=DETAIL_COLOR,
        start_time=SCENE1_TEXT_START + 0.4,
        duration=T_INFO + 1.0,
        start_y=next_y + round(52 * scale),
    )
    blocks.extend(location_blocks)
    return blocks


def _block_position(block: WordBlock, local_t: float) -> tuple[int, int, float] | None:
    fly_in_dur = 0.7
    fly_in_dist = 200
    fall_dur = 0.4
    if local_t < block.start_time:
        return None
    elapsed = local_t - block.start_time
    if elapsed > block.life_duration:
        return None
    if elapsed < fly_in_dur:
        progress = elapsed / fly_in_dur
        y = (block.y + fly_in_dist) - (fly_in_dist * ease_out_cubic(progress))
    elif elapsed > (block.life_duration - fall_dur):
        fall_t = min((elapsed - (block.life_duration - fall_dur)) / fall_dur, 1.0)
        y = block.y + (250 * ease_in_cubic(fall_t))
    else:
        y = block.y
    alpha_in = min(1.0, elapsed / 0.2)
    alpha_out = min(1.0, max(0.0, block.life_duration - elapsed) / fall_dur)
    alpha = min(alpha_in, alpha_out)
    return block.x, int(y), alpha


def render_scene1_frame(local_t: float, payload: ScenePayload, text_blocks: list[WordBlock]) -> Image.Image:
    with Image.open(payload.poster_path).convert("RGBA") as poster_src:
        scene_bg = BG_BLACK
        canvas = Image.new("RGBA", (W, H), (*scene_bg, 255))
        x, y, poster_w, poster_h = _poster_geometry(local_t, poster_src.size)
        poster = _resize_poster_clean(poster_src, (poster_w, poster_h))
        canvas.paste(poster, (x, y), poster)

    draw = ImageDraw.Draw(canvas)
    if local_t >= (T_ENTRY + T_HOLD):
        progress = min(1.0, max(0.0, (local_t - (T_ENTRY + T_HOLD)) / T_MOVE))
        curtain_y = H if progress <= 0 else int(H + (SPLIT_Y - H) * ease_in_out_quint(progress))
        if curtain_y < H:
            draw.rectangle((0, curtain_y, W, H), fill=(*BG_BLACK, 255))

    for block in text_blocks:
        positioned = _block_position(block, local_t)
        if positioned is None:
            continue
        bx, by, alpha = positioned
        canvas.alpha_composite(_alpha_image(block.image.copy(), alpha), (bx, by))

    return canvas


def render_scene1_frames(payload: ScenePayload) -> int:
    text_blocks = build_scene_text_blocks(payload)
    local_t = SCENE1_START_LOCAL
    frame_num = SCENE1_START_FRAME
    while local_t <= SCENE1_TOTAL_LOCAL + (0.5 / FPS):
        frame = render_scene1_frame(local_t, payload, text_blocks)
        out_path = FRAMES_DIR / f"frame_{frame_num:04d}.png"
        frame.save(out_path)
        if frame_num in {SCENE1_START_FRAME, SCENE1_START_FRAME + 18, SCENE1_START_FRAME + 54}:
            shutil.copy2(out_path, PREVIEW_FRAMES_DIR / out_path.name)
        frame_num += 1
        local_t += 1.0 / FPS
    first_2d = FRAMES_DIR / f"frame_{SCENE1_START_FRAME:04d}.png"
    shutil.copy2(first_2d, OUT_DIR / "first_scene_frame.png")
    return frame_num - 1


def _handoff_blend_mask(alpha: float) -> Image.Image:
    mask = Image.new("L", (W, H), 0)
    draw = ImageDraw.Draw(mask)
    ramp_h = round(H * 0.14)
    draw.rectangle((0, 0, W, H), fill=int(255 * alpha))
    draw.rectangle((0, 0, W, ramp_h), fill=int(255 * alpha * 0.82))
    return mask.filter(ImageFilter.GaussianBlur(radius=18))


def soften_handoff(payload: ScenePayload) -> None:
    # Keep the handoff as a true cut. The tonal continuity is now handled inside
    # the early 2D frames, which prevents the ugly whole-frame hybrid images that
    # looked like fake interpolation during the previous blend-based pass.
    shutil.copy2(FRAMES_DIR / f"frame_{INTRO_END_FRAME:04d}.png", OUT_DIR / "last_intro_frame.png")
    shutil.copy2(FRAMES_DIR / f"frame_{SCENE1_START_FRAME:04d}.png", OUT_DIR / "first_scene_frame.png")


def write_storyboard(final_frame: int) -> Path:
    selected = [
        1,
        12,
        28,
        44,
        60,
        76,
        92,
        INTRO_END_FRAME,
        SCENE1_START_FRAME,
        SCENE1_START_FRAME + 18,
        SCENE1_START_FRAME + 48,
        final_frame,
    ]
    selected = sorted({frame for frame in selected if 1 <= frame <= final_frame})
    thumbs: list[tuple[int, Image.Image]] = []
    for frame_num in selected:
        path = FRAMES_DIR / f"frame_{frame_num:04d}.png"
        with Image.open(path).convert("RGB") as image:
            thumbs.append((frame_num, image.resize((220, round(220 * image.height / image.width)), Image.Resampling.LANCZOS)))
    label_font = font(AKROBAT_BOLD, 28)
    margin = 28
    label_h = 44
    max_h = max(image.height for _, image in thumbs)
    canvas = Image.new(
        "RGB",
        (margin + len(thumbs) * (220 + margin), margin * 2 + max_h + label_h),
        ImageColor.getrgb("#F1ECE5"),
    )
    draw = ImageDraw.Draw(canvas)
    x = margin
    for frame_num, thumb in thumbs:
        canvas.paste(thumb, (x, margin))
        draw.text((x, margin + max_h + 8), f"f{frame_num}", font=label_font, fill=ImageColor.getrgb("#11110F"))
        x += 220 + margin
    out = OUT_DIR / "storyboard.png"
    canvas.save(out)
    return out


def write_manifest(payload: ScenePayload, final_frame: int) -> Path:
    manifest = OUT_DIR / f"scene1_manifest_{date.today().isoformat()}.md"
    lines = [
        f"# MobileFeed Intro {MODE_SLUG.title()} Clip",
        "",
        f"- Variant: `{ACTIVE_VARIANT.slug}`",
        f"- Canvas: `{W}x{H}`",
        f"- FPS: `{FPS}`",
        f"- Render mode: `{MODE_SLUG}`",
        f"- Intro frames: `{INTRO_START_FRAME}-{INTRO_END_FRAME}`",
        f"- Scene 1 starts at frame: `{SCENE1_START_FRAME}`",
        f"- Final frame: `{final_frame}`",
        f"- Music: `{AUDIO_PATH}` from `{AUDIO_START_SEC}s` with legacy `video_afisha` offset",
        f"- Strong beat target: `~{STRONG_BEAT_SEC:.2f}s` from clip start",
        f"- Focus event: `{FOCUS_EVENT_ID}`",
        f"- Scene 1 title: `{payload.title}`",
        f"- Scene 1 date line: `{payload.date_line}`",
        f"- Scene 1 location line: `{payload.location_line}`",
        "",
        "## Notes",
        "",
        f"- This artifact is a `{MODE_SLUG}` export of `intro + scene1`; geometry, sync, and visual quality should match the current handoff contract for this mode.",
        "- Screen CTA remains attached to the phone surface inside the 3D intro and fades there before the 2D cut.",
        "- The intro duration is stretched so the strongest early music accent lands on the start of the first upward move in the 2D scene.",
        "- The final clip includes only the intro and the first legacy-style 2D scene.",
        f"- Intro render step: `{INTRO_RENDER_STEP}`; final mode must not synthesize in-between intro frames via image blending.",
    ]
    manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest


def encode_video(final_frame: int) -> Path:
    frame_paths = [str(FRAMES_DIR / f"frame_{frame_num:04d}.png") for frame_num in range(1, final_frame + 1)]
    clip = ImageSequenceClip(frame_paths, fps=FPS)
    audio = AudioFileClip(str(AUDIO_PATH))
    if audio.duration > AUDIO_START_SEC:
        if hasattr(audio, "subclipped"):
            audio = audio.subclipped(AUDIO_START_SEC)
        else:
            audio = audio.subclip(AUDIO_START_SEC)
    if hasattr(audio, "with_duration"):
        audio = audio.with_duration(clip.duration)
    else:
        audio = audio.set_duration(clip.duration)
    if hasattr(audio, "with_volume_scaled"):
        audio = audio.with_volume_scaled(0.45)
    if hasattr(clip, "with_audio"):
        clip = clip.with_audio(audio)
    else:
        clip = clip.set_audio(audio)
    out_path = OUT_DIR / f"mobilefeed_intro_scene1_{MODE_SLUG}.mp4"
    clip.write_videofile(
        str(out_path),
        fps=FPS,
        codec="libx264",
        audio_codec="aac",
        preset="slow",
        audio_bitrate="96k",
        ffmpeg_params=[
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-crf",
            "20" if FINAL_MODE else "23",
        ],
        logger="bar",
    )
    audio.close()
    clip.close()
    return out_path


def write_cover_frame(payload: ScenePayload) -> Path:
    image = Image.new("RGB", (W, H), ImageColor.getrgb("#F1ECE5"))
    draw = ImageDraw.Draw(image)
    ink = ImageColor.getrgb("#11110F")
    accent = ImageColor.getrgb("#F45B1F")
    title_font = _fit_font(payload.title.upper(), DRUK_SUPER, 120, 74, 880)
    meta_font = font(AKROBAT_BLACK, 44)
    draw.text((84, 154), "MOBILEFEED INTRO", font=meta_font, fill=accent)
    draw.text((84, 236), payload.title.upper(), font=title_font, fill=ink)
    draw.text((84, 366), payload.date_line, font=meta_font, fill=ink)
    draw.text((84, 424), payload.location_line, font=font(AKROBAT_BOLD, 30), fill=ink)
    out = OUT_DIR / "approval_cover.png"
    image.save(out)
    return out


def main() -> None:
    payload = load_scene_payload()
    render_intro_frames()
    final_frame = render_scene1_frames(payload)
    soften_handoff(payload)
    video_path = encode_video(final_frame)
    storyboard = write_storyboard(final_frame)
    manifest = write_manifest(payload, final_frame)
    cover = write_cover_frame(payload)
    print(video_path)
    print(storyboard)
    print(manifest)
    print(cover)


if __name__ == "__main__":
    main()
