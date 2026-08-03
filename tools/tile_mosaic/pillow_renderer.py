"""Deterministic Pillow renderer for rapid tile-mosaic previews.

The Pillow backend is not a fake screenshot.  It consumes the same immutable
scene plan as Blender, keeps the image projection continuous across tiles, and
implements the same state/roughness/lighting contract.  It exists so geometry
and art direction can be reviewed without a 265 MiB Blender installation.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import random
from typing import Any, Mapping

from PIL import (
    Image,
    ImageChops,
    ImageDraw,
    ImageEnhance,
    ImageFilter,
    ImageOps,
)

try:
    RESAMPLING = Image.Resampling
except AttributeError:  # pragma: no cover - Pillow < 9
    RESAMPLING = Image


@dataclass(frozen=True)
class PixelProjection:
    pixels_per_world: float
    camera_left_world: float
    camera_top_world: float
    grid_left_world: float
    grid_top_world: float
    tile_size_px: float
    gap_px: float


def compute_pixel_projection(plan: Mapping[str, Any]) -> PixelProjection:
    canvas = plan["canvas"]
    grid = plan["grid"]
    camera = plan["camera"]
    width = int(canvas["width"])
    height = int(canvas["height"])
    grid_width = float(grid["width_world"])
    grid_height = float(grid["height_world"])
    overscan = max(0.0, float(camera.get("overscan", 0.0)))
    camera_height = grid_height * (1.0 + 2.0 * overscan)
    pixels_per_world = height / camera_height
    camera_width = camera_height * width / height
    center_x = float(camera.get("offset_x", 0.0)) * grid_width
    center_y = float(camera.get("offset_y", 0.0)) * grid_height
    camera_left = center_x - camera_width / 2.0
    camera_top = center_y + camera_height / 2.0
    grid_left = -grid_width / 2.0
    grid_top = grid_height / 2.0
    tile_size_px = float(grid["tile_size_world"]) * pixels_per_world
    gap_px = float(grid["gap_world"]) * pixels_per_world
    return PixelProjection(
        pixels_per_world=pixels_per_world,
        camera_left_world=camera_left,
        camera_top_world=camera_top,
        grid_left_world=grid_left,
        grid_top_world=grid_top,
        tile_size_px=tile_size_px,
        gap_px=gap_px,
    )


def _linear_gradient(size: tuple[int, int], start: int, end: int, *, vertical: bool) -> Image.Image:
    width, height = size
    length = height if vertical else width
    if length <= 1:
        values = [start]
    else:
        values = [round(start + (end - start) * i / (length - 1)) for i in range(length)]
    strip = Image.new("L", (1, length))
    strip.putdata(values)
    if vertical:
        return strip.resize((width, height))
    return strip.resize((width, height)).transpose(Image.Transpose.ROTATE_90).resize((width, height))


def _radial_gradient(
    size: tuple[int, int],
    *,
    center_x: float,
    center_y: float,
    radius: float,
    inner: int,
    outer: int,
) -> Image.Image:
    width, height = size
    small_w = max(32, min(320, width // 4))
    small_h = max(32, min(180, height // 4))
    pixels: list[int] = []
    safe_radius = max(1e-6, radius)
    aspect = width / max(1, height)
    for y in range(small_h):
        ny = (y + 0.5) / small_h
        for x in range(small_w):
            nx = (x + 0.5) / small_w
            dx = (nx - center_x) * aspect
            dy = ny - center_y
            distance = math.sqrt(dx * dx + dy * dy) / safe_radius
            t = min(1.0, max(0.0, distance))
            # Smoothstep keeps the light broad and avoids CGI-looking rings.
            t = t * t * (3.0 - 2.0 * t)
            pixels.append(round(inner + (outer - inner) * t))
    gradient = Image.new("L", (small_w, small_h))
    gradient.putdata(pixels)
    return gradient.resize((width, height), RESAMPLING.BICUBIC)


def _deterministic_noise(
    size: tuple[int, int],
    *,
    seed: int,
    coarse: int,
    contrast: float = 1.0,
    blur: float = 0.0,
) -> Image.Image:
    width, height = size
    sample_w = max(8, min(coarse, width))
    sample_h = max(8, min(coarse, height))
    rng = random.Random(seed)
    values = []
    for _ in range(sample_w * sample_h):
        # Averaging three uniforms gives a soft, material-like bell curve.
        value = (rng.random() + rng.random() + rng.random()) / 3.0
        value = 128 + (value - 0.5) * 126 * contrast
        values.append(max(0, min(255, round(value))))
    image = Image.new("L", (sample_w, sample_h))
    image.putdata(values)
    image = image.resize((width, height), RESAMPLING.BICUBIC)
    if blur > 0:
        image = image.filter(ImageFilter.GaussianBlur(blur))
    return image


def _apply_color_state(image: Image.Image, state: Mapping[str, Any]) -> Image.Image:
    result = ImageEnhance.Color(image).enhance(float(state["saturation"]))
    result = ImageEnhance.Brightness(result).enhance(float(state["brightness"]))
    contrast = float(state.get("contrast", 1.0))
    if not math.isclose(contrast, 1.0):
        result = ImageEnhance.Contrast(result).enhance(contrast)
    return result


def build_state_textures(
    source_texture: Image.Image,
    plan: Mapping[str, Any],
) -> dict[str, Image.Image]:
    textures: dict[str, Image.Image] = {}
    render_scale = source_texture.width / max(1, int(plan["canvas"]["width"]))
    for state_name, state in plan["states"].items():
        texture = source_texture
        blur_radius = float(state.get("blur_px", 0.0)) * render_scale
        if blur_radius > 0:
            texture = texture.filter(ImageFilter.GaussianBlur(blur_radius))
        textures[state_name] = _apply_color_state(texture, state)
    return textures


def _rounded_mask(size: tuple[int, int], radius: int) -> Image.Image:
    mask = Image.new("L", size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle(
        (0, 0, size[0] - 1, size[1] - 1),
        radius=max(1, radius),
        fill=255,
    )
    return mask


def _matte_surface(
    tile: Image.Image,
    *,
    mask: Image.Image,
    state: Mapping[str, Any],
    seed: int,
    brightness_multiplier: float,
    light_bias: tuple[float, float],
) -> Image.Image:
    tile = tile.convert("RGB")
    tile = ImageEnhance.Brightness(tile).enhance(max(0.1, brightness_multiplier))
    width, height = tile.size

    micro = _deterministic_noise(
        tile.size,
        seed=seed,
        coarse=max(48, min(132, width // 2)),
        contrast=0.72,
        blur=0.35,
    )
    macro = _deterministic_noise(
        tile.size,
        seed=seed ^ 0x4D415454,
        coarse=max(12, min(30, width // 7)),
        contrast=0.58,
        blur=max(0.8, width / 220),
    )
    grain = ImageChops.multiply(micro, macro)
    grain = ImageEnhance.Contrast(grain).enhance(0.72)
    grain_rgb = Image.merge("RGB", (grain, grain, grain))
    tile = ImageChops.soft_light(tile, grain_rgb)

    # Local warm reflection.  Rough tiles get a broad, weak highlight while a
    # glint stays more compact and visible.
    roughness = max(0.0, min(1.0, float(state["roughness"])))
    glint = float(state.get("glint", 0.0))
    radius = 0.42 + roughness * 0.34
    light = _radial_gradient(
        tile.size,
        center_x=light_bias[0],
        center_y=light_bias[1],
        radius=radius,
        inner=round(48 + 100 * glint),
        outer=0,
    )
    warm = Image.new("RGB", tile.size, (245, 118, 68))
    warm.putalpha(light.point(lambda value: round(value * (0.15 + 0.38 * glint))))
    tile_rgba = tile.convert("RGBA")
    tile_rgba = Image.alpha_composite(tile_rgba, warm)

    # Matte veil and a restrained lower-right occlusion produce the same
    # tactile reading as a rough Principled BSDF without flattening the photo.
    veil_alpha = round(255 * float(state.get("veil_alpha", 0.0)))
    if veil_alpha:
        veil = Image.new("RGBA", tile.size, tuple(state.get("veil_rgb", [6, 8, 9])) + (veil_alpha,))
        tile_rgba = Image.alpha_composite(tile_rgba, veil)

    diagonal = Image.new("L", tile.size)
    diagonal_pixels: list[int] = []
    for y in range(height):
        for x in range(width):
            nx = x / max(1, width - 1)
            ny = y / max(1, height - 1)
            value = max(0.0, min(1.0, (nx + ny - 0.7) / 1.3))
            diagonal_pixels.append(round(value * 88))
    diagonal.putdata(diagonal_pixels)
    occlusion = Image.new("RGBA", tile.size, (0, 0, 0, 0))
    occlusion.putalpha(diagonal)
    tile_rgba = Image.alpha_composite(tile_rgba, occlusion)

    # Inner bevel: bright upper-left edge, dark lower-right edge.
    inner = mask.filter(ImageFilter.GaussianBlur(max(0.6, width / 260)))
    shifted_light = ImageChops.offset(inner, 2, 2)
    shifted_dark = ImageChops.offset(inner, -2, -2)
    top_left_edge = ImageChops.subtract(inner, shifted_light)
    bottom_right_edge = ImageChops.subtract(inner, shifted_dark)
    top_left_edge = top_left_edge.point(lambda value: round(value * 0.27))
    bottom_right_edge = bottom_right_edge.point(lambda value: round(value * 0.67))
    highlight_layer = Image.new("RGBA", tile.size, (255, 233, 215, 0))
    highlight_layer.putalpha(top_left_edge)
    shadow_layer = Image.new("RGBA", tile.size, (0, 0, 0, 0))
    shadow_layer.putalpha(bottom_right_edge)
    tile_rgba = Image.alpha_composite(tile_rgba, highlight_layer)
    tile_rgba = Image.alpha_composite(tile_rgba, shadow_layer)

    # A one-pixel neutral rim keeps tiles legible against completely dark grout.
    outline = Image.new("RGBA", tile.size, (0, 0, 0, 0))
    outline_draw = ImageDraw.Draw(outline)
    outline_draw.rounded_rectangle(
        (0, 0, width - 1, height - 1),
        radius=max(1, round(width * 0.045)),
        outline=(255, 255, 255, 17),
        width=max(1, round(width / 150)),
    )
    tile_rgba = Image.alpha_composite(tile_rgba, outline)
    tile_rgba.putalpha(mask)
    return tile_rgba


def _add_global_light(canvas: Image.Image, plan: Mapping[str, Any]) -> Image.Image:
    width, height = canvas.size
    lighting = plan["lighting"]
    key = lighting["key"]
    center = key.get("screen_position", [0.68, 0.38])
    strength = float(key.get("screen_strength", 0.16))
    gradient = _radial_gradient(
        canvas.size,
        center_x=float(center[0]),
        center_y=float(center[1]),
        radius=float(key.get("screen_radius", 0.46)),
        inner=255,
        outer=0,
    )
    color = tuple(round(float(channel) * 255) for channel in key["color"])
    glow = Image.new("RGBA", canvas.size, color + (0,))
    glow.putalpha(gradient.point(lambda value: round(value * strength)))
    return Image.alpha_composite(canvas.convert("RGBA"), glow)


def _apply_vignette(canvas: Image.Image, *, strength: float) -> Image.Image:
    if strength <= 0:
        return canvas
    width, height = canvas.size
    mask = _radial_gradient(
        canvas.size,
        center_x=0.5,
        center_y=0.47,
        radius=0.78,
        inner=255,
        outer=round(255 * (1.0 - min(0.95, strength))),
    )
    # _radial_gradient goes inner->outer, exactly the multiplier needed here.
    multiplier = Image.merge("RGB", (mask, mask, mask))
    rgb = ImageChops.multiply(canvas.convert("RGB"), multiplier)
    if canvas.mode == "RGBA":
        rgb.putalpha(canvas.getchannel("A"))
    return rgb


def render_pillow(
    *,
    plan: Mapping[str, Any],
    source_texture: Image.Image,
    output_path: str | Path,
) -> dict[str, Any]:
    canvas_width = int(plan["canvas"]["width"])
    canvas_height = int(plan["canvas"]["height"])
    background_rgb = tuple(plan["canvas"].get("background_rgb", [3, 5, 6]))
    canvas = Image.new("RGBA", (canvas_width, canvas_height), background_rgb + (255,))
    projection = compute_pixel_projection(plan)
    state_textures = build_state_textures(source_texture, plan)

    columns = int(plan["grid"]["columns"])
    rows = int(plan["grid"]["rows"])
    texture_tile_width = source_texture.width / columns
    texture_tile_height = source_texture.height / rows
    grid_left = projection.grid_left_world
    grid_top = projection.grid_top_world
    tile_world = float(plan["grid"]["tile_size_world"])
    step_world = tile_world + float(plan["grid"]["gap_world"])
    base_tile_px = projection.tile_size_px
    tile_px = max(8, round(base_tile_px))
    corner_radius = max(2, round(tile_px * float(plan["grid"].get("corner_radius_ratio", 0.045))))

    for tile_plan in plan["tiles"]:
        col = int(tile_plan["column"])
        row = int(tile_plan["row"])
        state_name = str(tile_plan["state"])
        state = plan["states"][state_name]

        source = state_textures[state_name]
        crop = (
            round(col * texture_tile_width),
            round(row * texture_tile_height),
            round((col + 1) * texture_tile_width),
            round((row + 1) * texture_tile_height),
        )
        tile_image = source.crop(crop).resize((tile_px, tile_px), RESAMPLING.LANCZOS)
        mask = _rounded_mask((tile_px, tile_px), corner_radius)

        tile_center_x = grid_left + col * step_world + tile_world / 2.0
        tile_center_y = grid_top - row * step_world - tile_world / 2.0
        x_px = (tile_center_x - projection.camera_left_world) * projection.pixels_per_world
        y_px = (projection.camera_top_world - tile_center_y) * projection.pixels_per_world
        light_x = float(plan["lighting"]["key"].get("screen_position", [0.68, 0.38])[0])
        light_y = float(plan["lighting"]["key"].get("screen_position", [0.68, 0.38])[1])
        local_light_x = min(1.2, max(-0.2, light_x - (x_px / canvas_width - 0.5) * 0.24))
        local_light_y = min(1.2, max(-0.2, light_y - (y_px / canvas_height - 0.5) * 0.24))

        surface = _matte_surface(
            tile_image,
            mask=mask,
            state=state,
            seed=int(tile_plan["roughness_seed"]),
            brightness_multiplier=float(tile_plan["brightness_multiplier"]),
            light_bias=(local_light_x, local_light_y),
        )

        rotation = float(tile_plan.get("rotation_degrees", 0.0))
        if abs(rotation) > 1e-6:
            surface = surface.rotate(
                rotation,
                resample=RESAMPLING.BICUBIC,
                expand=True,
                fillcolor=(0, 0, 0, 0),
            )

        depth_offset = float(tile_plan.get("depth_offset", 0.0))
        depth_px = round(depth_offset * projection.pixels_per_world * 0.6)
        paste_x = round(x_px - surface.width / 2 + depth_px * 0.25)
        paste_y = round(y_px - surface.height / 2 - depth_px * 0.35)

        shadow_alpha = 112 + min(82, abs(depth_px) * 4)
        shadow_mask = surface.getchannel("A").filter(
            ImageFilter.GaussianBlur(max(2.0, tile_px / 42.0))
        )
        shadow_mask = shadow_mask.point(lambda value: round(value * shadow_alpha / 255))
        shadow = Image.new("RGBA", surface.size, (0, 0, 0, 0))
        shadow.putalpha(shadow_mask)
        canvas.alpha_composite(shadow, (paste_x + max(2, tile_px // 48), paste_y + max(3, tile_px // 36)))
        canvas.alpha_composite(surface, (paste_x, paste_y))

    canvas = _add_global_light(canvas, plan)
    canvas = _apply_vignette(canvas, strength=float(plan["canvas"].get("vignette", 0.46)))

    # Restrained full-frame grain prevents the grout/background from looking
    # digitally empty while preserving the per-tile material hierarchy.
    global_noise = _deterministic_noise(
        canvas.size,
        seed=int(plan["seed"]) ^ 0x5343454E,
        coarse=180,
        contrast=0.38,
        blur=0.2,
    )
    grain_alpha = global_noise.point(lambda value: abs(value - 128) // 7)
    grain_rgb = Image.merge("RGBA", (global_noise, global_noise, global_noise, grain_alpha))
    canvas = Image.alpha_composite(canvas.convert("RGBA"), grain_rgb)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(output, format="PNG", optimize=True)
    return {
        "backend": "pillow",
        "output": str(output),
        "width": canvas_width,
        "height": canvas_height,
        "tile_size_px": round(projection.tile_size_px, 4),
        "gap_px": round(projection.gap_px, 4),
    }
