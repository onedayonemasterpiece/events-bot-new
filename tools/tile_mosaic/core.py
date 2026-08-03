"""Deterministic scene-plan construction for the tile mosaic material lab.

The renderers deliberately consume a serialisable, immutable plan.  All random
choices are resolved here, so Pillow and Blender receive the same tile states,
material variation and geometry.  A frozen plan can be committed as a golden
contract and re-rendered without depending on future changes to the planner.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import random
from typing import Any, Mapping, MutableMapping

STATE_NAMES = ("sealed", "dim", "sleeping", "revealed", "glint")
SCHEMA_VERSION = 1


class PresetError(ValueError):
    """Raised when a material preset or scene plan violates the contract."""




def reproducible_timestamp() -> str:
    """Return a stable UTC timestamp when SOURCE_DATE_EPOCH is supplied.

    Render pixels never depend on wall-clock time, but manifests committed as
    review evidence must also be reproducible.  GitHub Actions pins
    ``SOURCE_DATE_EPOCH``; interactive runs retain a truthful current timestamp.
    """

    explicit = str(os.getenv("TILE_MOSAIC_GENERATED_AT") or "").strip()
    if explicit:
        parsed = datetime.fromisoformat(explicit.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    epoch = str(os.getenv("SOURCE_DATE_EPOCH") or "").strip()
    if epoch:
        try:
            value = int(epoch)
        except ValueError as exc:
            raise PresetError("SOURCE_DATE_EPOCH must be an integer") from exc
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def artifact_name(path: str | Path) -> str:
    """Return a portable manifest reference instead of a runner-local path."""

    return Path(path).name

def canonical_json_bytes(payload: Any) -> bytes:
    """Return stable UTF-8 JSON bytes used for hashes and golden sidecars."""

    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_payload(payload: Any) -> str:
    return sha256(canonical_json_bytes(payload)).hexdigest()


def _require_mapping(value: Any, name: str) -> MutableMapping[str, Any]:
    if not isinstance(value, MutableMapping):
        raise PresetError(f"{name} must be an object")
    return value


def _require_number(
    mapping: Mapping[str, Any],
    key: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PresetError(f"{key} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise PresetError(f"{key} must be finite")
    if minimum is not None and number < minimum:
        raise PresetError(f"{key} must be >= {minimum}")
    if maximum is not None and number > maximum:
        raise PresetError(f"{key} must be <= {maximum}")
    return number


def _require_int(
    mapping: Mapping[str, Any],
    key: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PresetError(f"{key} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        bound = f"..{maximum}" if maximum is not None else "+"
        raise PresetError(f"{key} must be in {minimum}{bound}")
    return value


def _validate_rgb(value: Any, name: str, *, normalized: bool) -> list[float] | list[int]:
    if not isinstance(value, list) or len(value) != 3:
        raise PresetError(f"{name} must contain exactly three channels")
    result: list[float] | list[int]
    if normalized:
        result = []
        for channel in value:
            if isinstance(channel, bool) or not isinstance(channel, (int, float)):
                raise PresetError(f"{name} channels must be numeric")
            channel_float = float(channel)
            if not 0.0 <= channel_float <= 1.0:
                raise PresetError(f"{name} channels must be in 0..1")
            result.append(channel_float)
    else:
        result = []
        for channel in value:
            if isinstance(channel, bool) or not isinstance(channel, int):
                raise PresetError(f"{name} channels must be integers")
            if not 0 <= channel <= 255:
                raise PresetError(f"{name} channels must be in 0..255")
            result.append(channel)
    return result


def validate_preset(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalise a full material preset."""

    preset = deepcopy(dict(raw))
    if int(preset.get("schema_version", 0)) != SCHEMA_VERSION:
        raise PresetError(f"unsupported preset schema_version: {preset.get('schema_version')!r}")
    name = str(preset.get("name") or "").strip()
    if not name:
        raise PresetError("preset name is required")
    preset["name"] = name
    preset["seed"] = int(preset.get("seed", 20260901))

    canvas = _require_mapping(preset.get("canvas"), "canvas")
    canvas["width"] = _require_int(canvas, "width", minimum=320, maximum=8192)
    canvas["height"] = _require_int(canvas, "height", minimum=320, maximum=8192)
    canvas["background_rgb"] = _validate_rgb(
        canvas.get("background_rgb", [3, 5, 6]),
        "canvas.background_rgb",
        normalized=False,
    )
    canvas["vignette"] = _require_number(canvas, "vignette", minimum=0.0, maximum=0.95)

    grid = _require_mapping(preset.get("grid"), "grid")
    grid["columns"] = _require_int(grid, "columns", minimum=1, maximum=64)
    grid["rows"] = _require_int(grid, "rows", minimum=1, maximum=64)
    grid["gap_ratio"] = _require_number(grid, "gap_ratio", minimum=0.0, maximum=0.3)
    grid["tile_thickness"] = _require_number(grid, "tile_thickness", minimum=0.001, maximum=1.0)
    grid["bevel_radius"] = _require_number(grid, "bevel_radius", minimum=0.0, maximum=0.25)
    grid["corner_radius_ratio"] = _require_number(
        grid,
        "corner_radius_ratio",
        minimum=0.0,
        maximum=0.35,
    )
    grid["texture_tile_px"] = _require_int(
        grid,
        "texture_tile_px",
        minimum=32,
        maximum=2048,
    )

    camera = _require_mapping(preset.get("camera"), "camera")
    camera["overscan"] = _require_number(camera, "overscan", minimum=0.0, maximum=1.0)
    for key in ("offset_x", "offset_y"):
        camera[key] = _require_number(camera, key, minimum=-2.0, maximum=2.0)
    for key in ("tilt_x_degrees", "tilt_y_degrees"):
        camera[key] = _require_number(camera, key, minimum=-25.0, maximum=25.0)
    camera["ortho"] = bool(camera.get("ortho", True))

    jitter = _require_mapping(preset.get("geometry_jitter"), "geometry_jitter")
    jitter["depth"] = _require_number(jitter, "depth", minimum=0.0, maximum=0.5)
    jitter["tilt_degrees"] = _require_number(
        jitter,
        "tilt_degrees",
        minimum=0.0,
        maximum=8.0,
    )
    jitter["rotation_degrees"] = _require_number(
        jitter,
        "rotation_degrees",
        minimum=0.0,
        maximum=6.0,
    )
    jitter["brightness"] = _require_number(jitter, "brightness", minimum=0.0, maximum=0.5)
    jitter.setdefault("active_fraction", 1.0)
    jitter["active_fraction"] = _require_number(
        jitter,
        "active_fraction",
        minimum=0.0,
        maximum=1.0,
    )
    jitter.setdefault("corner_lift", 0.0)
    jitter["corner_lift"] = _require_number(
        jitter,
        "corner_lift",
        minimum=0.0,
        maximum=0.25,
    )

    material = _require_mapping(preset.get("material"), "material")
    for key in (
        "base_roughness",
        "micro_strength",
        "macro_strength",
        "roughness_variation",
        "coat_weight",
        "coat_roughness",
    ):
        material[key] = _require_number(material, key, minimum=0.0, maximum=1.0)
    for key in ("micro_scale", "macro_scale"):
        material[key] = _require_number(material, key, minimum=0.01, maximum=1000.0)
    material["side_rgb"] = _validate_rgb(material.get("side_rgb"), "material.side_rgb", normalized=True)
    material["grout_rgb"] = _validate_rgb(material.get("grout_rgb"), "material.grout_rgb", normalized=True)
    material.setdefault("corner_highlight_fraction", 0.0)
    material.setdefault("corner_highlight_strength", 0.0)
    material.setdefault("corner_highlight_radius", 0.42)
    material["corner_highlight_fraction"] = _require_number(
        material,
        "corner_highlight_fraction",
        minimum=0.0,
        maximum=1.0,
    )
    material["corner_highlight_strength"] = _require_number(
        material,
        "corner_highlight_strength",
        minimum=0.0,
        maximum=1.0,
    )
    material["corner_highlight_radius"] = _require_number(
        material,
        "corner_highlight_radius",
        minimum=0.05,
        maximum=2.0,
    )

    states = _require_mapping(preset.get("states"), "states")
    if set(states) != set(STATE_NAMES):
        missing = sorted(set(STATE_NAMES) - set(states))
        extra = sorted(set(states) - set(STATE_NAMES))
        raise PresetError(f"state contract mismatch; missing={missing}, extra={extra}")
    fraction_total = 0.0
    for state_name in STATE_NAMES:
        state = _require_mapping(states[state_name], f"states.{state_name}")
        state["fraction"] = _require_number(state, "fraction", minimum=0.0, maximum=1.0)
        fraction_total += state["fraction"]
        for key in ("brightness", "saturation", "contrast"):
            state[key] = _require_number(state, key, minimum=0.0, maximum=3.0)
        state["roughness"] = _require_number(state, "roughness", minimum=0.0, maximum=1.0)
        state["blur_px"] = _require_number(state, "blur_px", minimum=0.0, maximum=40.0)
        state["image_mix"] = _require_number(state, "image_mix", minimum=0.0, maximum=1.0)
        state["veil_alpha"] = _require_number(state, "veil_alpha", minimum=0.0, maximum=1.0)
        state["veil_rgb"] = _validate_rgb(state.get("veil_rgb"), f"states.{state_name}.veil_rgb", normalized=False)
        state["glint"] = _require_number(state, "glint", minimum=0.0, maximum=2.0)
        state.setdefault("local_blur_fraction", 0.0)
        state.setdefault("local_blur_extra_px", 0.0)
        state["local_blur_fraction"] = _require_number(
            state,
            "local_blur_fraction",
            minimum=0.0,
            maximum=1.0,
        )
        state["local_blur_extra_px"] = _require_number(
            state,
            "local_blur_extra_px",
            minimum=0.0,
            maximum=20.0,
        )
        if "coat_weight" in state:
            state["coat_weight"] = _require_number(state, "coat_weight", minimum=0.0, maximum=1.0)
        if "coat_roughness" in state:
            state["coat_roughness"] = _require_number(
                state,
                "coat_roughness",
                minimum=0.0,
                maximum=1.0,
            )
    if not math.isclose(fraction_total, 1.0, abs_tol=1e-6):
        raise PresetError(f"state fractions must sum to 1.0, got {fraction_total}")

    lighting = _require_mapping(preset.get("lighting"), "lighting")
    for light_name in ("key", "fill", "rim"):
        light = _require_mapping(lighting.get(light_name), f"lighting.{light_name}")
        light["type"] = str(light.get("type") or "AREA")
        light["energy"] = _require_number(light, "energy", minimum=0.0, maximum=100000.0)
        light["size"] = _require_number(light, "size", minimum=0.01, maximum=1000.0)
        position = light.get("position")
        if not isinstance(position, list) or len(position) != 3:
            raise PresetError(f"lighting.{light_name}.position must contain three values")
        light["position"] = [float(value) for value in position]
        light["color"] = _validate_rgb(light.get("color"), f"lighting.{light_name}.color", normalized=True)
    key = lighting["key"]
    key.setdefault("screen_position", [0.68, 0.38])
    key.setdefault("screen_radius", 0.46)
    key.setdefault("screen_strength", 0.12)
    if not isinstance(key["screen_position"], list) or len(key["screen_position"]) != 2:
        raise PresetError("lighting.key.screen_position must contain two values")
    key["screen_position"] = [float(value) for value in key["screen_position"]]
    key["screen_radius"] = _require_number(key, "screen_radius", minimum=0.01, maximum=4.0)
    key["screen_strength"] = _require_number(key, "screen_strength", minimum=0.0, maximum=1.0)

    render = _require_mapping(preset.get("render"), "render")
    render["samples"] = _require_int(render, "samples", minimum=1, maximum=8192)
    render["transparent"] = bool(render.get("transparent", False))
    render["file_format"] = str(render.get("file_format") or "PNG")
    render["color_mode"] = str(render.get("color_mode") or "RGB")
    render["compression"] = _require_int(render, "compression", minimum=0, maximum=100)
    return preset


def load_preset(path: str | Path) -> dict[str, Any]:
    preset_path = Path(path)
    raw = json.loads(preset_path.read_text(encoding="utf-8"))
    return validate_preset(raw)


def _largest_remainder_counts(fractions: Mapping[str, float], total: int) -> dict[str, int]:
    exact = {name: float(fraction) * total for name, fraction in fractions.items()}
    counts = {name: int(math.floor(value)) for name, value in exact.items()}
    remaining = total - sum(counts.values())
    order = sorted(
        exact,
        key=lambda name: (exact[name] - counts[name], -STATE_NAMES.index(name)),
        reverse=True,
    )
    for name in order[:remaining]:
        counts[name] += 1
    return counts


def _clamp_unit(value: float, name: str) -> float:
    if not math.isfinite(value):
        raise PresetError(f"{name} must be finite")
    return min(1.0, max(0.0, value))


def _round(value: float) -> float:
    return round(float(value), 6)


def _weighted_state_assignment(
    *,
    columns: int,
    rows: int,
    counts: Mapping[str, int],
    focal_x: float,
    focal_y: float,
    rng: random.Random,
) -> dict[int, str]:
    """Assign exact state counts while biasing legible states to the focal area."""

    scores: list[tuple[int, float, float]] = []
    aspect = columns / max(1, rows)
    for row in range(rows):
        for column in range(columns):
            index = row * columns + column
            x = (column + 0.5) / columns
            y = (row + 0.5) / rows
            dx = (x - focal_x) * aspect
            dy = y - focal_y
            distance = math.sqrt(dx * dx + dy * dy)
            # Independent jitter avoids visible rings and gives the seed real
            # artistic influence without allowing it to change exact counts.
            jitter = rng.uniform(-0.11, 0.11)
            scores.append((index, distance + jitter, rng.random()))

    remaining = {index for index, _, _ in scores}
    assignment: dict[int, str] = {}

    # Bright states are selected closest to the focal point.  The less legible
    # states are then selected increasingly far away, leaving sealed as the
    # remainder.  This creates a coherent reveal instead of random confetti.
    for state_name in ("glint", "revealed", "sleeping", "dim"):
        count = counts[state_name]
        if count <= 0:
            continue
        candidates = [item for item in scores if item[0] in remaining]
        if state_name in {"glint", "revealed"}:
            candidates.sort(key=lambda item: (item[1], item[2]))
        elif state_name == "sleeping":
            candidates.sort(key=lambda item: (abs(item[1] - 0.42), item[2]))
        else:
            candidates.sort(key=lambda item: (-item[1], item[2]))
        for index, _, _ in candidates[:count]:
            assignment[index] = state_name
            remaining.remove(index)

    if len(remaining) != counts["sealed"]:
        raise AssertionError("state assignment did not preserve exact counts")
    for index in sorted(remaining):
        assignment[index] = "sealed"
    return assignment


def build_scene_plan(
    preset: Mapping[str, Any],
    *,
    focal_x: float = 0.5,
    focal_y: float = 0.5,
    seed: int | None = None,
) -> dict[str, Any]:
    """Resolve a full deterministic scene plan from a validated preset."""

    normalized = validate_preset(preset)
    focal_x = _clamp_unit(float(focal_x), "focal_x")
    focal_y = _clamp_unit(float(focal_y), "focal_y")
    resolved_seed = int(normalized["seed"] if seed is None else seed)
    rng = random.Random(resolved_seed)

    columns = int(normalized["grid"]["columns"])
    rows = int(normalized["grid"]["rows"])
    tile_count = columns * rows
    tile_size_world = 1.0
    gap_world = tile_size_world * float(normalized["grid"]["gap_ratio"])
    width_world = columns * tile_size_world + (columns - 1) * gap_world
    height_world = rows * tile_size_world + (rows - 1) * gap_world
    texture_tile_px = int(normalized["grid"]["texture_tile_px"])

    fractions = {
        name: float(normalized["states"][name]["fraction"])
        for name in STATE_NAMES
    }
    state_counts = _largest_remainder_counts(fractions, tile_count)
    assignments = _weighted_state_assignment(
        columns=columns,
        rows=rows,
        counts=state_counts,
        focal_x=focal_x,
        focal_y=focal_y,
        rng=rng,
    )

    jitter = normalized["geometry_jitter"]
    material = normalized["material"]
    tiles: list[dict[str, Any]] = []
    corner_names = ("top_left", "top_right", "bottom_right", "bottom_left")
    for index in range(tile_count):
        row, column = divmod(index, columns)
        active = rng.random() <= float(jitter.get("active_fraction", 1.0))
        amplitude = 1.0 if active else 0.0
        local_blur_fraction = float(normalized["states"][assignments[index]].get("local_blur_fraction", 0.0))
        local_blur = local_blur_fraction > 0.0 and rng.random() < local_blur_fraction
        corner_highlight = (
            float(material.get("corner_highlight_fraction", 0.0)) > 0.0
            and rng.random() < float(material["corner_highlight_fraction"])
        )
        corner_lift = float(jitter.get("corner_lift", 0.0)) * amplitude
        tiles.append(
            {
                "index": index,
                "row": row,
                "column": column,
                "state": assignments[index],
                "roughness_seed": rng.randrange(1, 2**31 - 1),
                "brightness_multiplier": _round(
                    1.0 + rng.uniform(-float(jitter["brightness"]), float(jitter["brightness"]))
                ),
                "depth_offset": _round(
                    rng.uniform(-float(jitter["depth"]), float(jitter["depth"])) * amplitude
                ),
                "tilt_x_degrees": _round(
                    rng.uniform(-float(jitter["tilt_degrees"]), float(jitter["tilt_degrees"]))
                    * amplitude
                ),
                "tilt_y_degrees": _round(
                    rng.uniform(-float(jitter["tilt_degrees"]), float(jitter["tilt_degrees"]))
                    * amplitude
                ),
                "rotation_degrees": _round(
                    rng.uniform(
                        -float(jitter["rotation_degrees"]),
                        float(jitter["rotation_degrees"]),
                    )
                    * amplitude
                ),
                "lifted_corner": rng.choice(corner_names) if corner_lift > 0.0 else None,
                "corner_lift": _round(corner_lift * rng.uniform(0.45, 1.0)) if corner_lift > 0.0 else 0.0,
                "corner_highlight": rng.choice(corner_names) if corner_highlight else None,
                "local_blur_extra_px": _round(
                    float(normalized["states"][assignments[index]].get("local_blur_extra_px", 0.0))
                    * rng.uniform(0.65, 1.0)
                )
                if local_blur
                else 0.0,
            }
        )

    preset_hash = sha256_payload(normalized)
    plan: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "renderer_contract": {"pillow_geometry_version": 2, "material_version": 2},
        "preset_name": normalized["name"],
        "preset_sha256": preset_hash,
        "seed": resolved_seed,
        "focal_point": {"x": _round(focal_x), "y": _round(focal_y)},
        "canvas": deepcopy(normalized["canvas"]),
        "grid": {
            **deepcopy(normalized["grid"]),
            "tile_count": tile_count,
            "tile_size_world": tile_size_world,
            "gap_world": _round(gap_world),
            "width_world": _round(width_world),
            "height_world": _round(height_world),
            "texture_width": columns * texture_tile_px,
            "texture_height": rows * texture_tile_px,
        },
        "camera": deepcopy(normalized["camera"]),
        "material": deepcopy(normalized["material"]),
        "states": deepcopy(normalized["states"]),
        "lighting": deepcopy(normalized["lighting"]),
        "render": deepcopy(normalized["render"]),
        "state_counts": state_counts,
        "tiles": tiles,
    }
    plan["plan_sha256"] = sha256_payload(plan)
    return plan


def validate_scene_plan(raw: Mapping[str, Any]) -> dict[str, Any]:
    plan = deepcopy(dict(raw))
    if int(plan.get("schema_version", 0)) != SCHEMA_VERSION:
        raise PresetError(f"unsupported scene plan schema_version: {plan.get('schema_version')!r}")
    grid = _require_mapping(plan.get("grid"), "grid")
    columns = int(grid.get("columns", 0))
    rows = int(grid.get("rows", 0))
    tiles = plan.get("tiles")
    if not isinstance(tiles, list) or len(tiles) != columns * rows:
        raise PresetError("scene plan tile count does not match rows × columns")
    seen = set()
    for tile in tiles:
        if not isinstance(tile, dict):
            raise PresetError("scene plan tiles must be objects")
        index = int(tile.get("index", -1))
        if index in seen or not 0 <= index < columns * rows:
            raise PresetError(f"duplicate or invalid tile index: {index}")
        seen.add(index)
        if tile.get("state") not in STATE_NAMES:
            raise PresetError(f"invalid tile state: {tile.get('state')!r}")
    supplied_hash = str(plan.get("plan_sha256") or "")
    unhashed = deepcopy(plan)
    unhashed.pop("plan_sha256", None)
    actual_hash = sha256_payload(unhashed)
    # Legacy v1 plans did not include preset_name/render in exactly the same
    # canonical payload.  Keep them loadable but surface the computed hash.
    if supplied_hash and supplied_hash != actual_hash:
        plan["legacy_plan_sha256"] = supplied_hash
        plan["plan_sha256"] = actual_hash
    elif not supplied_hash:
        plan["plan_sha256"] = actual_hash
    return plan


def load_scene_plan(path: str | Path) -> dict[str, Any]:
    return validate_scene_plan(json.loads(Path(path).read_text(encoding="utf-8")))
