"""Deterministic second-stage material studies built from a frozen golden render.

This stage exists for art-direction iteration.  It never overwrites the golden
`Kafel Classic` result.  Instead it reuses the committed tile surfaces and scene
plan, then applies bounded matte, blur, corner-light and micro-geometry changes.
That makes every laboratory variant reproducible even when the original remote
photo is unavailable.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from hashlib import sha256
import json
import math
from pathlib import Path
import random
from typing import Any, Mapping, Sequence

from PIL import Image, ImageChops, ImageDraw, ImageEnhance, ImageFilter

from .core import artifact_name, canonical_json_bytes, load_scene_plan, reproducible_timestamp
from .pillow_renderer import (
    RESAMPLING,
    _add_global_light,
    _apply_vignette,
    _deterministic_noise,
    _radial_gradient,
    _rounded_mask,
    compute_pixel_projection,
)
from .prepare import load_image, sha256_file

REFINER_VERSION = "1.0.0"
CORNER_ORDER = ("top_left", "top_right", "bottom_right", "bottom_left")


class RefinementError(ValueError):
    pass


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _number(
    mapping: Mapping[str, Any],
    key: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RefinementError(f"{key} must be numeric")
    number = float(value)
    if not math.isfinite(number) or not minimum <= number <= maximum:
        raise RefinementError(f"{key} must be in {minimum}..{maximum}")
    return number


def load_refinement(path: str | Path) -> dict[str, Any]:
    profile = json.loads(Path(path).read_text(encoding="utf-8"))
    if int(profile.get("schema_version", 0)) != 1:
        raise RefinementError("unsupported refinement schema_version")
    name = str(profile.get("name") or "").strip()
    if not name:
        raise RefinementError("refinement name is required")
    profile["name"] = name
    profile["seed_offset"] = int(profile.get("seed_offset", 0))

    matte = profile.get("matte")
    blur = profile.get("blur")
    geometry = profile.get("geometry")
    corner_light = profile.get("corner_light")
    edge = profile.get("edge")
    shadow = profile.get("shadow")
    global_fx = profile.get("global")
    for key, value in {
        "matte": matte,
        "blur": blur,
        "geometry": geometry,
        "corner_light": corner_light,
        "edge": edge,
        "shadow": shadow,
        "global": global_fx,
    }.items():
        if not isinstance(value, dict):
            raise RefinementError(f"{key} must be an object")

    for key, minimum, maximum in (
        ("strength", 0.0, 1.0),
        ("micro_contrast", 0.0, 3.0),
        ("macro_contrast", 0.0, 3.0),
        ("desaturate", 0.0, 1.0),
        ("brightness", 0.2, 2.0),
        ("roughness_haze", 0.0, 1.0),
    ):
        matte[key] = _number(matte, key, minimum=minimum, maximum=maximum)

    blur["fraction"] = _number(blur, "fraction", minimum=0.0, maximum=1.0)
    blur["min_px"] = _number(blur, "min_px", minimum=0.0, maximum=20.0)
    blur["max_px"] = _number(blur, "max_px", minimum=blur["min_px"], maximum=30.0)
    states = blur.get("states", ["dim", "sleeping", "revealed"])
    if not isinstance(states, list) or not all(isinstance(item, str) for item in states):
        raise RefinementError("blur.states must be an array of state names")
    blur["states"] = states

    for key, minimum, maximum in (
        ("active_fraction", 0.0, 1.0),
        ("max_rotation_degrees", 0.0, 3.0),
        ("max_tilt_degrees", 0.0, 5.0),
        ("max_depth_px", 0.0, 20.0),
        ("max_corner_lift_px", 0.0, 16.0),
        ("max_shift_px", 0.0, 10.0),
        ("inset_px", 0.0, 12.0),
    ):
        geometry[key] = _number(geometry, key, minimum=minimum, maximum=maximum)

    corner_light["fraction"] = _number(corner_light, "fraction", minimum=0.0, maximum=1.0)
    corner_light["strength"] = _number(corner_light, "strength", minimum=0.0, maximum=1.5)
    corner_light["radius"] = _number(corner_light, "radius", minimum=0.05, maximum=2.0)
    color = corner_light.get("color")
    if not isinstance(color, list) or len(color) != 3 or not all(isinstance(channel, int) for channel in color):
        raise RefinementError("corner_light.color must contain three integer channels")
    corner_light["color"] = [max(0, min(255, channel)) for channel in color]

    for key in ("highlight_alpha", "shadow_alpha", "outline_alpha"):
        edge[key] = _number(edge, key, minimum=0.0, maximum=1.0)
    edge["radius_ratio"] = _number(edge, "radius_ratio", minimum=0.0, maximum=0.35)

    shadow["alpha"] = _number(shadow, "alpha", minimum=0.0, maximum=1.0)
    shadow["blur_px"] = _number(shadow, "blur_px", minimum=0.0, maximum=30.0)
    shadow["offset_x_px"] = _number(shadow, "offset_x_px", minimum=-20.0, maximum=20.0)
    shadow["offset_y_px"] = _number(shadow, "offset_y_px", minimum=-20.0, maximum=20.0)

    global_fx["vignette_delta"] = _number(global_fx, "vignette_delta", minimum=-0.5, maximum=0.5)
    global_fx["grain_strength"] = _number(global_fx, "grain_strength", minimum=0.0, maximum=1.0)
    global_fx["warm_glow_multiplier"] = _number(
        global_fx,
        "warm_glow_multiplier",
        minimum=0.0,
        maximum=3.0,
    )
    profile["profile_sha256"] = sha256(canonical_json_bytes({k: v for k, v in profile.items() if k != "profile_sha256"})).hexdigest()
    return profile


def _solve_linear(matrix: list[list[float]], values: list[float]) -> list[float]:
    """Solve a small dense linear system with partial pivoting."""

    n = len(values)
    augmented = [row[:] + [values[index]] for index, row in enumerate(matrix)]
    for column in range(n):
        pivot = max(range(column, n), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-10:
            raise RefinementError("degenerate perspective transform")
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(n):
            if row == column:
                continue
            factor = augmented[row][column]
            if factor == 0:
                continue
            augmented[row] = [
                value - factor * pivot_value
                for value, pivot_value in zip(augmented[row], augmented[column])
            ]
    return [augmented[row][-1] for row in range(n)]


def _perspective_coefficients(
    source: Sequence[tuple[float, float]],
    destination: Sequence[tuple[float, float]],
) -> tuple[float, ...]:
    """Return Pillow inverse mapping coefficients destination -> source."""

    matrix: list[list[float]] = []
    values: list[float] = []
    for (sx, sy), (dx, dy) in zip(source, destination):
        matrix.append([dx, dy, 1.0, 0.0, 0.0, 0.0, -sx * dx, -sx * dy])
        values.append(sx)
        matrix.append([0.0, 0.0, 0.0, dx, dy, 1.0, -sy * dx, -sy * dy])
        values.append(sy)
    return tuple(_solve_linear(matrix, values))


def _plane_gradient(size: tuple[int, int], tilt_x: float, tilt_y: float) -> Image.Image:
    width, height = size
    sample_w = max(24, min(128, width // 2))
    sample_h = max(24, min(128, height // 2))
    values: list[int] = []
    magnitude = max(1e-6, abs(tilt_x) + abs(tilt_y))
    for y in range(sample_h):
        ny = (y / max(1, sample_h - 1)) - 0.5
        for x in range(sample_w):
            nx = (x / max(1, sample_w - 1)) - 0.5
            plane = (nx * tilt_y - ny * tilt_x) / magnitude
            values.append(round(max(0.0, min(1.0, 0.5 + plane * 0.48)) * 255))
    gradient = Image.new("L", (sample_w, sample_h))
    gradient.putdata(values)
    return gradient.resize(size, RESAMPLING.BICUBIC)


def _apply_matte(
    tile: Image.Image,
    *,
    profile: Mapping[str, Any],
    seed: int,
) -> Image.Image:
    matte = profile["matte"]
    tile = tile.convert("RGB")
    desaturate = float(matte["desaturate"])
    if desaturate:
        tile = ImageEnhance.Color(tile).enhance(1.0 - desaturate)
    tile = ImageEnhance.Brightness(tile).enhance(float(matte["brightness"]))

    strength = float(matte["strength"])
    if strength <= 0:
        return tile
    width, height = tile.size
    micro = _deterministic_noise(
        tile.size,
        seed=seed ^ 0x6D696372,
        coarse=max(44, min(150, width // 2)),
        contrast=float(matte["micro_contrast"]),
        blur=0.28,
    )
    macro = _deterministic_noise(
        tile.size,
        seed=seed ^ 0x6D616372,
        coarse=max(10, min(34, width // 7)),
        contrast=float(matte["macro_contrast"]),
        blur=max(0.8, width / 240),
    )
    grain = ImageChops.multiply(micro, macro)
    grain = ImageEnhance.Contrast(grain).enhance(0.72 + strength * 0.7)
    grain_rgb = Image.merge("RGB", (grain, grain, grain))
    softened = ImageChops.soft_light(tile, grain_rgb)
    tile = Image.blend(tile, softened, min(0.92, strength * 1.5))

    haze = float(matte["roughness_haze"])
    if haze:
        neutral = Image.new("RGB", tile.size, (118, 121, 123))
        haze_noise = _deterministic_noise(
            tile.size,
            seed=seed ^ 0x68617A65,
            coarse=max(18, min(48, width // 4)),
            contrast=0.42,
            blur=max(0.7, width / 260),
        )
        haze_alpha = haze_noise.point(lambda value: round(abs(value - 128) * haze * 1.45))
        neutral.putalpha(haze_alpha)
        tile_rgba = Image.alpha_composite(tile.convert("RGBA"), neutral)
        tile = tile_rgba.convert("RGB")
    return tile


def _add_tilt_shading(tile: Image.Image, tilt_x: float, tilt_y: float, strength: float) -> Image.Image:
    if abs(tilt_x) + abs(tilt_y) < 1e-6 or strength <= 0:
        return tile
    gradient = _plane_gradient(tile.size, tilt_x, tilt_y)
    highlight = gradient.point(lambda value: round(max(0, value - 128) * strength))
    shadow = gradient.point(lambda value: round(max(0, 128 - value) * strength * 1.25))
    light_layer = Image.new("RGBA", tile.size, (255, 230, 209, 0))
    light_layer.putalpha(highlight)
    dark_layer = Image.new("RGBA", tile.size, (0, 0, 0, 0))
    dark_layer.putalpha(shadow)
    result = Image.alpha_composite(tile.convert("RGBA"), light_layer)
    return Image.alpha_composite(result, dark_layer)


def _corner_position(corner: str) -> tuple[float, float]:
    return {
        "top_left": (0.08, 0.08),
        "top_right": (0.92, 0.08),
        "bottom_right": (0.92, 0.92),
        "bottom_left": (0.08, 0.92),
    }[corner]


def _add_corner_light(tile: Image.Image, profile: Mapping[str, Any], corner: str) -> Image.Image:
    config = profile["corner_light"]
    center_x, center_y = _corner_position(corner)
    alpha = _radial_gradient(
        tile.size,
        center_x=center_x,
        center_y=center_y,
        radius=float(config["radius"]),
        inner=255,
        outer=0,
    )
    strength = float(config["strength"])
    alpha = alpha.point(lambda value: round(value * strength))
    color = tuple(int(channel) for channel in config["color"])
    light = Image.new("RGBA", tile.size, color + (0,))
    light.putalpha(alpha)
    return Image.alpha_composite(tile.convert("RGBA"), light)


def _add_edges(tile: Image.Image, mask: Image.Image, profile: Mapping[str, Any]) -> Image.Image:
    edge = profile["edge"]
    width = tile.width
    inner = mask.filter(ImageFilter.GaussianBlur(max(0.55, width / 300)))
    shifted_light = ImageChops.offset(inner, 2, 2)
    shifted_dark = ImageChops.offset(inner, -2, -2)
    top_left = ImageChops.subtract(inner, shifted_light).point(
        lambda value: round(value * float(edge["highlight_alpha"]))
    )
    bottom_right = ImageChops.subtract(inner, shifted_dark).point(
        lambda value: round(value * float(edge["shadow_alpha"]))
    )
    highlight = Image.new("RGBA", tile.size, (255, 235, 220, 0))
    highlight.putalpha(top_left)
    shadow = Image.new("RGBA", tile.size, (0, 0, 0, 0))
    shadow.putalpha(bottom_right)
    result = Image.alpha_composite(tile.convert("RGBA"), highlight)
    result = Image.alpha_composite(result, shadow)
    outline = Image.new("RGBA", tile.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(outline)
    draw.rounded_rectangle(
        (0, 0, tile.width - 1, tile.height - 1),
        radius=max(1, round(tile.width * float(edge["radius_ratio"]))),
        outline=(255, 255, 255, round(255 * float(edge["outline_alpha"]))),
        width=max(1, round(tile.width / 155)),
    )
    result = Image.alpha_composite(result, outline)
    result.putalpha(mask)
    return result


def _destination_quad(
    tile_size: int,
    pad: int,
    *,
    tilt_x: float,
    tilt_y: float,
    lift_corner: str | None,
    lift_px: float,
) -> list[tuple[float, float]]:
    left = float(pad)
    top = float(pad)
    right = float(pad + tile_size - 1)
    bottom = float(pad + tile_size - 1)
    points = [
        [left, top],
        [right, top],
        [right, bottom],
        [left, bottom],
    ]
    perspective = tile_size * 0.0028
    tx = max(-5.0, min(5.0, tilt_x)) * perspective
    ty = max(-5.0, min(5.0, tilt_y)) * perspective
    # X tilt changes vertical compression; Y tilt changes horizontal compression.
    points[0][1] += max(0.0, tx)
    points[1][1] += max(0.0, tx)
    points[2][1] -= min(0.0, tx)
    points[3][1] -= min(0.0, tx)
    points[0][0] += max(0.0, ty)
    points[3][0] += max(0.0, ty)
    points[1][0] -= min(0.0, ty)
    points[2][0] -= min(0.0, ty)

    if lift_corner and lift_px > 0:
        index = CORNER_ORDER.index(lift_corner)
        outward = ((-1, -1), (1, -1), (1, 1), (-1, 1))[index]
        points[index][0] += outward[0] * lift_px * 0.35
        points[index][1] += outward[1] * lift_px * 0.35
        # The adjacent corners move less, so the tile appears flexed/tilted rather
        # than translated as a whole.
        points[(index - 1) % 4][0] += outward[0] * lift_px * 0.08
        points[(index + 1) % 4][1] += outward[1] * lift_px * 0.08
    return [(x, y) for x, y in points]


def _warp_tile(
    tile: Image.Image,
    *,
    tilt_x: float,
    tilt_y: float,
    lift_corner: str | None,
    lift_px: float,
    rotation_degrees: float,
) -> Image.Image:
    pad = max(8, round(tile.width * 0.08 + lift_px))
    output_size = (tile.width + pad * 2, tile.height + pad * 2)
    source = [
        (0.0, 0.0),
        (float(tile.width - 1), 0.0),
        (float(tile.width - 1), float(tile.height - 1)),
        (0.0, float(tile.height - 1)),
    ]
    destination = _destination_quad(
        tile.width,
        pad,
        tilt_x=tilt_x,
        tilt_y=tilt_y,
        lift_corner=lift_corner,
        lift_px=lift_px,
    )
    coefficients = _perspective_coefficients(source, destination)
    warped = tile.transform(
        output_size,
        Image.Transform.PERSPECTIVE,
        coefficients,
        resample=RESAMPLING.BICUBIC,
        fillcolor=(0, 0, 0, 0),
    )
    if abs(rotation_degrees) > 1e-6:
        warped = warped.rotate(
            rotation_degrees,
            resample=RESAMPLING.BICUBIC,
            expand=True,
            fillcolor=(0, 0, 0, 0),
        )
    return warped


def _apply_global_finish(canvas: Image.Image, plan: Mapping[str, Any], profile: Mapping[str, Any]) -> Image.Image:
    global_fx = profile["global"]
    adjusted = deepcopy(dict(plan))
    adjusted["lighting"] = deepcopy(plan["lighting"])
    adjusted["lighting"]["key"] = deepcopy(plan["lighting"]["key"])
    adjusted["lighting"]["key"]["screen_strength"] = min(
        1.0,
        float(plan["lighting"]["key"].get("screen_strength", 0.115))
        * float(global_fx["warm_glow_multiplier"]),
    )
    canvas = _add_global_light(canvas, adjusted)
    vignette = min(
        0.95,
        max(0.0, float(plan["canvas"].get("vignette", 0.42)) + float(global_fx["vignette_delta"])),
    )
    canvas = _apply_vignette(canvas, strength=vignette)
    grain_strength = float(global_fx["grain_strength"])
    if grain_strength > 0:
        noise = _deterministic_noise(
            canvas.size,
            seed=int(plan["seed"]) ^ int(profile["seed_offset"]) ^ 0x52454649,
            coarse=190,
            contrast=0.34,
            blur=0.22,
        )
        alpha = noise.point(lambda value: round(abs(value - 128) * grain_strength / 2.8))
        grain = Image.merge("RGBA", (noise, noise, noise, alpha))
        canvas = Image.alpha_composite(canvas.convert("RGBA"), grain)
    return canvas


def render_refinement(
    *,
    base_render: str | Path,
    base_plan: str | Path,
    profile_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    base_path = Path(base_render).expanduser().resolve()
    plan_path = Path(base_plan).expanduser().resolve()
    profile_source = Path(profile_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    base = load_image(base_path)
    plan = load_scene_plan(plan_path)
    profile = load_refinement(profile_source)
    expected_size = (int(plan["canvas"]["width"]), int(plan["canvas"]["height"]))
    if base.size != expected_size:
        raise RefinementError(f"base render size {base.size} does not match plan {expected_size}")

    background_rgb = tuple(int(value) for value in plan["canvas"].get("background_rgb", [3, 5, 6]))
    canvas = Image.new("RGBA", expected_size, background_rgb + (255,))
    projection = compute_pixel_projection(plan)
    columns = int(plan["grid"]["columns"])
    grid_left = projection.grid_left_world
    grid_top = projection.grid_top_world
    tile_world = float(plan["grid"]["tile_size_world"])
    step_world = tile_world + float(plan["grid"]["gap_world"])
    tile_px = max(8, round(projection.tile_size_px))
    inset_px = max(0, round(float(profile["geometry"]["inset_px"])))
    radius = max(2, round(tile_px * float(profile["edge"]["radius_ratio"])))
    base_seed = int(plan["seed"]) ^ int(profile["seed_offset"])

    prepared_tiles: list[tuple[float, int, Image.Image, int, int, dict[str, Any]]] = []
    for tile_plan in plan["tiles"]:
        index = int(tile_plan["index"])
        row = int(tile_plan["row"])
        column = int(tile_plan["column"])
        state_name = str(tile_plan["state"])
        tile_center_x = grid_left + column * step_world + tile_world / 2.0
        tile_center_y = grid_top - row * step_world - tile_world / 2.0
        center_x = (tile_center_x - projection.camera_left_world) * projection.pixels_per_world
        center_y = (projection.camera_top_world - tile_center_y) * projection.pixels_per_world
        left = round(center_x - tile_px / 2)
        top = round(center_y - tile_px / 2)
        crop_box = (left + inset_px, top + inset_px, left + tile_px - inset_px, top + tile_px - inset_px)
        cropped = base.crop(crop_box).resize((tile_px, tile_px), RESAMPLING.LANCZOS)

        rng = random.Random(base_seed ^ int(tile_plan.get("roughness_seed", index + 1)) ^ (index * 0x9E3779B1))
        geometry_active = rng.random() < float(profile["geometry"]["active_fraction"])
        blur_active = state_name in set(profile["blur"]["states"]) and rng.random() < float(profile["blur"]["fraction"])
        corner_light_active = rng.random() < float(profile["corner_light"]["fraction"])

        if blur_active:
            radius_px = rng.uniform(float(profile["blur"]["min_px"]), float(profile["blur"]["max_px"]))
            cropped = cropped.filter(ImageFilter.GaussianBlur(radius_px))
        surface = _apply_matte(cropped, profile=profile, seed=rng.randrange(1, 2**31 - 1))

        tilt_x = rng.uniform(-float(profile["geometry"]["max_tilt_degrees"]), float(profile["geometry"]["max_tilt_degrees"])) if geometry_active else 0.0
        tilt_y = rng.uniform(-float(profile["geometry"]["max_tilt_degrees"]), float(profile["geometry"]["max_tilt_degrees"])) if geometry_active else 0.0
        rotation = rng.uniform(-float(profile["geometry"]["max_rotation_degrees"]), float(profile["geometry"]["max_rotation_degrees"])) if geometry_active else 0.0
        depth_px = rng.uniform(0.0, float(profile["geometry"]["max_depth_px"])) if geometry_active else 0.0
        shift_x = rng.uniform(-float(profile["geometry"]["max_shift_px"]), float(profile["geometry"]["max_shift_px"])) if geometry_active else 0.0
        shift_y = rng.uniform(-float(profile["geometry"]["max_shift_px"]), float(profile["geometry"]["max_shift_px"])) if geometry_active else 0.0
        lift_corner = rng.choice(CORNER_ORDER) if geometry_active and float(profile["geometry"]["max_corner_lift_px"]) > 0 else None
        lift_px = rng.uniform(0.25, 1.0) * float(profile["geometry"]["max_corner_lift_px"]) if lift_corner else 0.0

        surface = _add_tilt_shading(
            surface,
            tilt_x,
            tilt_y,
            strength=0.14 + float(profile["matte"]["strength"]) * 0.28,
        )
        if corner_light_active:
            surface = _add_corner_light(surface, profile, rng.choice(CORNER_ORDER))
        mask = _rounded_mask((tile_px, tile_px), radius)
        surface = _add_edges(surface, mask, profile)
        warped = _warp_tile(
            surface,
            tilt_x=tilt_x,
            tilt_y=tilt_y,
            lift_corner=lift_corner,
            lift_px=lift_px,
            rotation_degrees=rotation,
        )
        paste_x = round(center_x - warped.width / 2 + shift_x - depth_px * 0.08)
        paste_y = round(center_y - warped.height / 2 + shift_y - depth_px * 0.14)
        prepared_tiles.append(
            (
                depth_px,
                index,
                warped,
                paste_x,
                paste_y,
                {
                    "state": state_name,
                    "blur": blur_active,
                    "corner_light": corner_light_active,
                    "tilt_x_degrees": round(tilt_x, 6),
                    "tilt_y_degrees": round(tilt_y, 6),
                    "rotation_degrees": round(rotation, 6),
                    "depth_px": round(depth_px, 6),
                    "lifted_corner": lift_corner,
                    "corner_lift_px": round(lift_px, 6),
                    "shift_x_px": round(shift_x, 6),
                    "shift_y_px": round(shift_y, 6),
                },
            )
        )

    # Draw tiles closer to the wall first.  Lifted tiles naturally overlap only
    # by a few pixels, matching the restrained reference rather than a card fan.
    prepared_tiles.sort(key=lambda item: (item[0], item[1]))
    tile_records: list[dict[str, Any]] = []
    for depth_px, index, surface, paste_x, paste_y, record in prepared_tiles:
        shadow_config = profile["shadow"]
        shadow_mask = surface.getchannel("A").filter(
            ImageFilter.GaussianBlur(float(shadow_config["blur_px"]) + depth_px * 0.35)
        )
        alpha_multiplier = min(1.0, float(shadow_config["alpha"]) + depth_px * 0.018)
        shadow_mask = shadow_mask.point(lambda value: round(value * alpha_multiplier))
        shadow = Image.new("RGBA", surface.size, (0, 0, 0, 0))
        shadow.putalpha(shadow_mask)
        shadow_x = paste_x + round(float(shadow_config["offset_x_px"]) + depth_px * 0.28)
        shadow_y = paste_y + round(float(shadow_config["offset_y_px"]) + depth_px * 0.45)
        canvas.alpha_composite(shadow, (shadow_x, shadow_y))
        canvas.alpha_composite(surface, (paste_x, paste_y))
        tile_records.append({"index": index, **record})

    canvas = _apply_global_finish(canvas, plan, profile)
    canvas.convert("RGB").save(output, format="PNG", optimize=True)
    manifest = {
        "schema_version": 1,
        "generated_at": reproducible_timestamp(),
        "generator": "events-bot tile-mosaic material lab refinement",
        "generator_version": REFINER_VERSION,
        "base": {
            "render": artifact_name(base_path),
            "render_sha256": sha256_file(base_path),
            "plan": artifact_name(plan_path),
            "plan_sha256": plan.get("plan_sha256"),
            "legacy_plan_sha256": plan.get("legacy_plan_sha256"),
        },
        "profile": {
            "name": profile["name"],
            "path": f"refinements/{profile_source.name}",
            "sha256": profile["profile_sha256"],
        },
        "output": {
            "path": artifact_name(output),
            "sha256": sha256_file(output),
            "size_bytes": output.stat().st_size,
            "width": output.stat().st_size and expected_size[0],
            "height": expected_size[1],
        },
        "tile_records": tile_records,
    }
    _write_json(output.with_suffix(output.suffix + ".manifest.json"), manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refine a frozen Kafel Classic render into a lab variant")
    parser.add_argument("--base-render", required=True)
    parser.add_argument("--base-plan", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    manifest = render_refinement(
        base_render=args.base_render,
        base_plan=args.base_plan,
        profile_path=args.profile,
        output_path=args.output,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
