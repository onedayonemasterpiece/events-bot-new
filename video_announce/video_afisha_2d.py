from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFont

try:
    from moviepy import ColorClip, CompositeVideoClip, ImageClip
except ImportError:
    from moviepy.editor import ColorClip, CompositeVideoClip, ImageClip


@dataclass(frozen=True)
class VideoAfisha2DConfig:
    width: int
    height: int
    font_path: Path | str | None
    title_color: str | tuple[int, int, int] = "white"
    accent_color: str | tuple[int, int, int] = "#f1c40f"
    detail_color: str | tuple[int, int, int] = "#bdc3c7"
    strip_color: tuple[int, int, int, int] = (80, 20, 140, 255)
    background_color: tuple[int, int, int] = (0, 0, 0)
    split_ratio: float = 0.6

    @property
    def split_y(self) -> int:
        return int(self.height * self.split_ratio)


def ease_out_cubic(t: float) -> float:
    return 1.0 - (1.0 - t) ** 3


def ease_in_cubic(t: float) -> float:
    return t * t * t


def ease_in_out_quint(t: float) -> float:
    if t < 0.5:
        return 16.0 * t * t * t * t * t
    return 1.0 - pow(-2.0 * t + 2.0, 5) / 2.0


def ease_out_expo(t: float) -> float:
    if t == 1:
        return 1.0
    return 1.0 - pow(2.0, -10.0 * t)


def get_font_object(font_path: Path | str | None, fontsize: int):
    try:
        return (
            ImageFont.truetype(str(font_path), fontsize)
            if font_path
            else ImageFont.load_default()
        )
    except Exception:
        return ImageFont.load_default()


def clip_set_start(clip, value: float):
    if hasattr(clip, "with_start"):
        return clip.with_start(value)
    return clip.set_start(value)


def clip_set_duration(clip, value: float):
    if hasattr(clip, "with_duration"):
        return clip.with_duration(value)
    return clip.set_duration(value)


def clip_set_position(clip, value):
    if hasattr(clip, "with_position"):
        return clip.with_position(value)
    return clip.set_position(value)


def clip_resize_width(clip, width: int):
    if hasattr(clip, "resized"):
        return clip.resized(width=width)
    return clip.resize(width=width)


def clip_subclip(clip, start: float = 0.0, end: float | None = None):
    if hasattr(clip, "subclipped"):
        if end is None:
            return clip.subclipped(start)
        return clip.subclipped(start, end)
    if end is None:
        return clip.subclip(start)
    return clip.subclip(start, end)


def clip_crossfadein(clip, duration: float):
    if hasattr(clip, "crossfadein"):
        return clip.crossfadein(duration)
    return clip


def clip_crossfadeout(clip, duration: float):
    if hasattr(clip, "crossfadeout"):
        return clip.crossfadeout(duration)
    return clip


def rgba_image_to_clip(img: Image.Image):
    rgba = np.array(img.convert("RGBA"))
    rgb = rgba[:, :, :3]
    clip = ImageClip(rgb)
    if rgba.shape[2] == 4:
        alpha = rgba[:, :, 3].astype("float32") / 255.0
        try:
            mask = ImageClip(alpha, ismask=True)
        except TypeError:
            mask = ImageClip(alpha)
            if hasattr(mask, "with_is_mask"):
                mask = mask.with_is_mask(True)
            else:
                mask.is_mask = True
        if hasattr(clip, "with_mask"):
            clip = clip.with_mask(mask)
        else:
            clip = clip.set_mask(mask)
    return clip


def create_word_clip_fixed_height(word, font, color, fixed_height, baseline_offset):
    w_text = font.getlength(word)
    pad_x = 10
    w_canvas = int(w_text + pad_x * 2)
    img = Image.new("RGBA", (w_canvas, fixed_height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    rect_top = 5
    rect_bottom = fixed_height - 5
    draw.rectangle([0, rect_top, w_canvas, rect_bottom], fill=(10, 10, 10, 240))
    draw.text((pad_x, baseline_offset), word, font=font, fill=color)
    return rgba_image_to_clip(img)


def generate_kinetic_text(
    text: str,
    *,
    config: VideoAfisha2DConfig,
    fontsize: int,
    color,
    start_time: float,
    duration: float,
    start_y: int,
):
    font = get_font_object(config.font_path, fontsize)
    try:
        left, top, right, bottom = font.getbbox("HgЙj")
        ascent, descent = -top, bottom
        line_height = ascent + descent + 5
        baseline_y = ascent + 2
        clip_height = int(line_height + 10)
    except Exception:
        clip_height = int(fontsize * 1.2)
        baseline_y = int(fontsize * 0.2)

    raw_lines = text.split("\n") if text else [""]
    lines = []
    space_w = font.getlength(" ")
    max_w = config.width - 100

    for raw_line in raw_lines:
        if not raw_line.strip():
            lines.append([])
            continue
        words = [w for w in raw_line.split(" ") if w]
        current_line = []
        current_w = 0
        for word in words:
            word_w = font.getlength(word)
            if current_line and current_w + word_w > max_w:
                lines.append(current_line)
                current_line = [word]
                current_w = word_w + space_w
            else:
                current_line.append(word)
                current_w += word_w + space_w
        if current_line:
            lines.append(current_line)

    word_clips = []
    current_y_pos = start_y
    word_global_index = 0

    fly_in_dur = 0.7
    fly_in_dist = 200
    fall_dur = 0.4

    for line in lines:
        current_x = 50
        for word in line:
            clip = create_word_clip_fixed_height(word, font, color, clip_height, baseline_y)

            delay = word_global_index * 0.05
            word_start = start_time + delay

            life_duration = duration - delay
            if life_duration < (fall_dur + 0.1):
                life_duration = fall_dur + 0.1

            final_x = current_x
            final_y = current_y_pos

            def pos_func(t, fx=final_x, fy=final_y, dur=life_duration):
                if t < fly_in_dur:
                    prog = t / fly_in_dur
                    eased = ease_out_cubic(prog)
                    curr_y = (fy + fly_in_dist) - (fly_in_dist * eased)
                    return (fx, int(curr_y))
                if t > (dur - fall_dur):
                    fall_t = t - (dur - fall_dur)
                    prog = min(fall_t / fall_dur, 1.0)
                    eased = ease_in_cubic(prog)
                    curr_y = fy + (250 * eased)
                    return (fx, int(curr_y))
                return (fx, int(fy))

            clip = clip_set_start(clip, word_start)
            clip = clip_set_duration(clip, life_duration)
            clip = clip_set_position(clip, pos_func)
            clip = clip_crossfadein(clip, 0.2)
            clip = clip_crossfadeout(clip, fall_dur)

            word_clips.append(clip)
            current_x += clip.w + 5
            word_global_index += 1

        current_y_pos += clip_height + 2

    return word_clips, current_y_pos


def create_single_word_clip(
    word: str,
    *,
    config: VideoAfisha2DConfig,
    font_size: int,
    bg_color,
    text_color,
    target_height: int | None = None,
):
    font = get_font_object(config.font_path, font_size)
    pad_x = 25

    if target_height:
        canvas_h = target_height
        w_text = font.getlength(word)
        bbox = font.getbbox(word)
        h_text_real = bbox[3] - bbox[1]
        pad_top = (canvas_h - h_text_real) // 2 - bbox[1]
        y_pos = pad_top + 10
        canvas_w = int(w_text + pad_x * 2)
    else:
        bbox_sample = font.getbbox("Hg")
        sample_h = bbox_sample[3] - bbox_sample[1]
        fixed_h = int(sample_h * 1.25)
        canvas_h = fixed_h
        w_text = font.getlength(word)
        canvas_w = int(w_text + pad_x * 2)
        y_pos = (fixed_h - sample_h) // 2 - bbox_sample[1]

    img = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 0, canvas_w, canvas_h], fill=bg_color)
    draw.text((pad_x, y_pos), word, font=font, fill=text_color)

    return rgba_image_to_clip(img), canvas_w, canvas_h


def create_advanced_scene(
    image_path: str | Path,
    text_data: dict[str, str],
    *,
    config: VideoAfisha2DConfig,
    start_delay: float = 0.0,
):
    T_ENTRY = 0.45
    T_HOLD = 1.2
    T_MOVE = 0.75
    T_INFO = 2.5
    T_EXIT = 0.45
    total_duration = start_delay + T_ENTRY + T_HOLD + T_MOVE + T_INFO + T_EXIT

    image_path = str(image_path)
    if not Path(image_path).exists():
        img = clip_set_duration(
            ColorClip(size=(config.width, config.height), color=(30, 30, 30)),
            total_duration,
        )
    else:
        img_pil = Image.open(image_path).convert("RGBA")
        img = clip_resize_width(rgba_image_to_clip(img_pil), config.width)

    def transform_func(t):
        if t < start_delay:
            return 0.4, "center"
        adj_t = t - start_delay
        if adj_t < T_ENTRY:
            return 0.4 + (0.5 * ease_out_cubic(adj_t / T_ENTRY)), "center"
        if adj_t < (T_ENTRY + T_HOLD):
            return 0.9 + (0.1 * ((adj_t - T_ENTRY) / T_HOLD)), "center"
        if adj_t < (T_ENTRY + T_HOLD + T_MOVE):
            prog = (adj_t - (T_ENTRY + T_HOLD)) / T_MOVE
            sy, ey = config.height / 2, config.split_y / 2
            return 1.0, int(sy + (ey - sy) * ease_in_out_quint(prog) - img.h / 2)
        if adj_t < (total_duration - start_delay - T_EXIT):
            return 1.0, int(config.split_y / 2 - 10 * (adj_t - (T_ENTRY + T_HOLD + T_MOVE)) - img.h / 2)
        prog = (adj_t - (total_duration - start_delay - T_EXIT)) / T_EXIT
        sy = config.split_y / 2 - 10 * T_INFO
        return 1.0, int(sy + (-img.h - sy) * ease_in_cubic(prog) - img.h / 2)

    moving_img = clip_resize_width(img, config.width)
    moving_img = moving_img.resize(lambda t: transform_func(t)[0])
    moving_img = clip_set_position(moving_img, lambda t: ("center", transform_func(t)[1]))
    moving_img = clip_set_duration(moving_img, total_duration)

    curtain_h = config.height - config.split_y
    curtain = ColorClip(size=(config.width, curtain_h), color=(10, 10, 10))
    move_start_time = start_delay + T_ENTRY + T_HOLD

    def curtain_pos(t):
        if t < move_start_time:
            return ("center", config.height)
        if t < move_start_time + T_MOVE:
            prog = (t - move_start_time) / T_MOVE
            return ("center", int(config.height + (config.split_y - config.height) * ease_in_out_quint(prog)))
        return ("center", config.split_y)

    curtain = clip_set_start(curtain, 0)
    curtain = clip_set_duration(curtain, total_duration)
    curtain = clip_set_position(curtain, curtain_pos)

    text_start = move_start_time + T_MOVE * 0.2
    sy = config.split_y + 30

    txts = []
    t1, ny = generate_kinetic_text(
        text_data["title"],
        config=config,
        fontsize=90,
        color=config.title_color,
        start_time=text_start,
        duration=T_INFO + 1.0,
        start_y=sy,
    )
    txts.extend(t1)
    t2, ny = generate_kinetic_text(
        text_data["date"],
        config=config,
        fontsize=50,
        color=config.accent_color,
        start_time=text_start + 0.2,
        duration=T_INFO + 1.0,
        start_y=ny + 30,
    )
    txts.extend(t2)
    t3, _ = generate_kinetic_text(
        text_data["location"],
        config=config,
        fontsize=45,
        color=config.detail_color,
        start_time=text_start + 0.4,
        duration=T_INFO + 1.0,
        start_y=ny + 15,
    )
    txts.extend(t3)

    bg = clip_set_duration(ColorClip((config.width, config.height), config.background_color), total_duration)
    return CompositeVideoClip([bg, moving_img, curtain] + txts)


def make_slide_anim(sx: int, ex: int, fy: int, delay: float):
    def slide_func(t):
        if t < delay:
            return (sx, fy)
        anim_time = t - delay
        anim_dur = 0.8
        if anim_time >= anim_dur:
            return (ex, fy)
        prog = anim_time / anim_dur
        val = ease_out_expo(prog)
        curr_x = sx + (ex - sx) * val
        return (int(curr_x), fy)

    return slide_func


def create_outro_slide_in(
    *,
    config: VideoAfisha2DConfig,
    words_conf: list[dict[str, str | float]] | None = None,
):
    duration = 3.5
    bg = clip_set_duration(ColorClip(size=(config.width, config.height), color=config.background_color), duration)

    if words_conf is None:
        words_conf = [
            {"text": "ПОЛЮБИТЬ", "side": "left", "delay": 0.0},
            {"text": "КАЛИНИНГРАД", "side": "right", "delay": 0.4},
            {"text": "АНОНСЫ", "side": "left", "delay": 0.8},
        ]

    clips = []
    strip_h = 210
    gap = 20
    step_y = strip_h + gap
    total_block_h = len(words_conf) * step_y - gap
    start_y_block = (config.height - total_block_h) / 2

    for i, item in enumerate(words_conf):
        clp, cw, _ = create_single_word_clip(
            str(item["text"]),
            config=config,
            font_size=160,
            bg_color=config.strip_color,
            text_color=config.title_color,
            target_height=strip_h,
        )

        final_x = (config.width - cw) // 2
        final_y = int(start_y_block + i * step_y)
        start_x_pos = -cw - 100 if item["side"] == "left" else config.width + 100
        pos_func = make_slide_anim(start_x_pos, final_x, final_y, float(item["delay"]))

        final = clip_set_duration(clp, duration)
        final = clip_set_start(final, 0)
        final = clip_set_position(final, pos_func)
        clips.append(final)

    return CompositeVideoClip([bg] + clips)
