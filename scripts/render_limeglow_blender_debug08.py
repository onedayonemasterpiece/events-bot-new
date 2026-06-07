#!/usr/bin/env python3
"""Render Limeglow debug-08 from structured planning packs.

This is a low-cost motion preview renderer. It deliberately renders a full PNG
sequence first so the result can be audited frame-by-frame before any final
quality pass.

Example run:

  docker run --rm -v /home/dev:/home/dev -w /home/dev/projects/events-bot-new node:22-bookworm-slim bash -lc '
    apt-get update >/dev/null
    apt-get install -y --no-install-recommends xvfb xauth x11-xkb-utils fonts-dejavu-core libx11-6 libxrender1 libxext6 libxi6 libxrandr2 libxfixes3 libxcursor1 libxinerama1 libxxf86vm1 libgl1-mesa-dri libglx-mesa0 libegl1 libxkbcommon0 libsm6 libice6 ca-certificates ffmpeg >/dev/null
    LD_LIBRARY_PATH=/home/dev/.local/opt/blender-4.5.0-linux-x64/lib xvfb-run -a /home/dev/.local/opt/blender-4.5.0-linux-x64/blender --gpu-backend opengl --python scripts/render_limeglow_blender_debug08.py -- --width 270 --height 480 --fps 15
  '
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
from pathlib import Path

import bpy
from mathutils import Vector


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PACK_DIR = ROOT / "artifacts/codex/limeglow-debug08-packs"
DEFAULT_OUT = ROOT / "artifacts/codex/limeglow-blender-debug-08"
FONT_BOLD = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")
FONT_REGULAR = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")


def parse_args() -> argparse.Namespace:
    raw = sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else []
    parser = argparse.ArgumentParser()
    parser.add_argument("--pack-dir", type=Path, default=DEFAULT_PACK_DIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--width", type=int, default=270)
    parser.add_argument("--height", type=int, default=480)
    parser.add_argument("--fps", type=int, default=15)
    parser.add_argument("--duration", type=float, default=22.0)
    parser.add_argument(
        "--variant",
        choices=["collage_cutout", "tram_background"],
        default="collage_cutout",
        help="Scene treatment variant. tram_background keeps the tram photo as a full-scene background plate.",
    )
    parser.add_argument("--skip-frames", action="store_true")
    return parser.parse_args(raw)


def resolve(path: str | Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else ROOT / p


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def rgba(hex_color: str, alpha: float = 1.0) -> tuple[float, float, float, float]:
    h = hex_color.lstrip("#")
    return (
        int(h[0:2], 16) / 255.0,
        int(h[2:4], 16) / 255.0,
        int(h[4:6], 16) / 255.0,
        alpha,
    )


def clean_scene() -> None:
    bpy.ops.object.select_all(action="SELECT")
    bpy.ops.object.delete()


def make_color_mat(name: str, color: str, alpha: float = 1.0):
    mat = bpy.data.materials.new(name)
    mat.use_nodes = True
    mat.blend_method = "BLEND" if alpha < 0.999 else "OPAQUE"
    mat.show_transparent_back = alpha < 0.999
    nodes = mat.node_tree.nodes
    nodes.clear()
    out = nodes.new("ShaderNodeOutputMaterial")
    emission = nodes.new("ShaderNodeEmission")
    emission.inputs["Color"].default_value = rgba(color, alpha)
    emission.inputs["Strength"].default_value = 1.0
    mat.node_tree.links.new(emission.outputs["Emission"], out.inputs["Surface"])
    mat.diffuse_color = rgba(color, alpha)
    return mat


def make_image_mat(name: str, path: Path, opacity: float = 1.0):
    mat = bpy.data.materials.new(name)
    mat.use_nodes = True
    mat.blend_method = "BLEND"
    mat.show_transparent_back = True
    nodes = mat.node_tree.nodes
    nodes.clear()
    out = nodes.new("ShaderNodeOutputMaterial")
    tex = nodes.new("ShaderNodeTexImage")
    tex.image = bpy.data.images.load(str(path))
    transparent = nodes.new("ShaderNodeBsdfTransparent")
    emission = nodes.new("ShaderNodeEmission")
    mix = nodes.new("ShaderNodeMixShader")
    alpha_mul = nodes.new("ShaderNodeMath")
    alpha_mul.name = "opacity_alpha"
    alpha_mul.operation = "MULTIPLY"
    alpha_mul.inputs[1].default_value = opacity
    emission.inputs["Strength"].default_value = 1.0
    mat.node_tree.links.new(tex.outputs["Color"], emission.inputs["Color"])
    mat.node_tree.links.new(tex.outputs["Alpha"], alpha_mul.inputs[0])
    mat.node_tree.links.new(alpha_mul.outputs["Value"], mix.inputs["Fac"])
    mat.node_tree.links.new(transparent.outputs["BSDF"], mix.inputs[1])
    mat.node_tree.links.new(emission.outputs["Emission"], mix.inputs[2])
    mat.node_tree.links.new(mix.outputs["Shader"], out.inputs["Surface"])
    mat.diffuse_color = (1, 1, 1, opacity)
    return mat, tex.image


def add_plane(
    name: str,
    x: float,
    y: float,
    z: float,
    width: float,
    height: float,
    mat,
    rot_z: float = 0.0,
):
    bpy.ops.mesh.primitive_plane_add(
        size=1,
        location=(x, y, z),
        rotation=(math.radians(90), 0, math.radians(rot_z)),
    )
    obj = bpy.context.object
    obj.name = name
    obj.scale = (width, height, 1)
    obj.data.materials.append(mat)
    return obj


def add_color_plane(
    name: str,
    x: float,
    y: float,
    z: float,
    width: float,
    height: float,
    color: str,
    alpha: float = 1.0,
    rot_z: float = 0.0,
):
    return add_plane(name, x, y, z, width, height, make_color_mat(f"{name}_mat", color, alpha), rot_z)


def add_image_plane(
    name: str,
    path: Path,
    x: float,
    y: float,
    z: float,
    width: float,
    opacity: float = 1.0,
    rot_z: float = 0.0,
):
    mat, image = make_image_mat(f"{name}_mat", path, opacity)
    aspect = image.size[1] / image.size[0] if image.size[0] else 1.0
    return add_plane(name, x, y, z, width, width * aspect, mat, rot_z)


def add_text(
    name: str,
    body: str,
    x: float,
    y: float,
    z: float,
    size: float,
    color: str = "#F4F4F4",
    alpha: float = 1.0,
    align: str = "CENTER",
    rot_z: float = 0.0,
):
    bpy.ops.object.text_add(location=(x, y, z), rotation=(math.radians(90), 0, math.radians(rot_z)))
    obj = bpy.context.object
    obj.name = name
    obj.data.body = body
    obj.data.align_x = align
    obj.data.align_y = "CENTER"
    obj.data.size = size
    obj.data.space_line = 0.92
    font_path = FONT_BOLD if FONT_BOLD.exists() else FONT_REGULAR
    if font_path.exists():
        obj.data.font = bpy.data.fonts.load(str(font_path))
    obj.data.materials.append(make_color_mat(f"{name}_mat", color, alpha))
    return obj


def add_route(
    name: str,
    points: list[tuple[float, float, float]],
    color: str,
    bevel: float = 0.015,
    alpha: float = 1.0,
):
    curve = bpy.data.curves.new(name, "CURVE")
    curve.dimensions = "3D"
    curve.resolution_u = 2
    curve.bevel_depth = bevel
    curve.bevel_resolution = 3
    poly = curve.splines.new("POLY")
    poly.points.add(len(points) - 1)
    for point, (x, y, z) in zip(poly.points, points):
        point.co = (x, y, z, 1)
    obj = bpy.data.objects.new(name, curve)
    bpy.context.collection.objects.link(obj)
    obj.data.materials.append(make_color_mat(f"{name}_mat", color, alpha))
    return obj


def look_at(camera, target: tuple[float, float, float]) -> None:
    direction = Vector(target) - camera.location
    camera.rotation_euler = direction.to_track_quat("-Z", "Y").to_euler()


def key_camera(camera, frame: int, loc: tuple[float, float, float], target: tuple[float, float, float], lens: float) -> None:
    bpy.context.scene.frame_set(frame)
    camera.location = loc
    camera.data.lens = lens
    look_at(camera, target)
    camera.keyframe_insert(data_path="location", frame=frame)
    camera.keyframe_insert(data_path="rotation_euler", frame=frame)
    camera.data.keyframe_insert(data_path="lens", frame=frame)


def _bezier(a: float, b: float, c: float, t: float) -> float:
    inv = 1.0 - t
    return 3.0 * inv * inv * t * a + 3.0 * inv * t * t * b + t * t * t * c


def css_cubic_bezier(x1: float, y1: float, x2: float, y2: float, x: float) -> float:
    """Return y for a CSS-like cubic bezier at time x.

    Blender's default BEZIER keyframes were too soft for the editorial
    reference, so camera motion is baked every frame through this curve.
    """
    x = max(0.0, min(1.0, x))
    lo, hi = 0.0, 1.0
    t = x
    for _ in range(18):
        current = _bezier(x1, x2, 1.0, t)
        if current < x:
            lo = t
        else:
            hi = t
        t = (lo + hi) * 0.5
    return _bezier(y1, y2, 1.0, t)


def ease_camera(t: float) -> float:
    # Matches the requirements: short acceleration, fast travel, firm settle.
    return css_cubic_bezier(0.76, 0.0, 0.24, 1.0, t)


def ease_slide(t: float) -> float:
    return css_cubic_bezier(0.65, 0.0, 0.35, 1.0, t)


def ease_pop(t: float) -> float:
    t = max(0.0, min(1.0, t))
    # Back-out approximation for small tags/effects.
    c1 = 1.70158
    c3 = c1 + 1.0
    return 1.0 + c3 * (t - 1.0) ** 3 + c1 * (t - 1.0) ** 2


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def lerp_tuple(a: tuple[float, ...], b: tuple[float, ...], t: float) -> tuple[float, ...]:
    return tuple(lerp(x, y, t) for x, y in zip(a, b))


def bake_camera_route(camera, route: list[dict], frame_end: int) -> None:
    """Bake camera every frame so the editorial easing is exact and visible."""
    if len(route) < 2:
        return
    for idx, item in enumerate(route[:-1]):
        nxt = route[idx + 1]
        start = int(item["frame"])
        end = int(nxt["frame"])
        duration = max(1, end - start)
        for frame in range(start, end + 1):
            raw = (frame - start) / duration
            p = ease_camera(raw)
            loc = lerp_tuple(item["loc"], nxt["loc"], p)
            target = lerp_tuple(item["target"], nxt["target"], p)
            lens = lerp(float(item["lens"]), float(nxt["lens"]), p)
            key_camera(camera, frame, loc, target, lens)
    last = route[-1]
    key_camera(camera, frame_end, last["loc"], last["target"], float(last["lens"]))


def key_object(obj, frame: int, *, loc=None, scale=None) -> None:
    bpy.context.scene.frame_set(frame)
    if loc is not None:
        obj.location = loc
        obj.keyframe_insert(data_path="location", frame=frame)
    if scale is not None:
        obj.scale = scale
        obj.keyframe_insert(data_path="scale", frame=frame)


def key_visibility(obj, frame: int, visible: bool) -> None:
    bpy.context.scene.frame_set(frame)
    obj.hide_viewport = not visible
    obj.hide_render = not visible
    obj.keyframe_insert(data_path="hide_viewport", frame=frame)
    obj.keyframe_insert(data_path="hide_render", frame=frame)


def key_image_opacity(obj, frame: int, opacity: float) -> None:
    bpy.context.scene.frame_set(frame)
    for mat in obj.data.materials:
        if not mat or not mat.use_nodes:
            continue
        for node in mat.node_tree.nodes:
            if node.name == "opacity_alpha":
                node.inputs[1].default_value = opacity
                node.inputs[1].keyframe_insert(data_path="default_value", frame=frame)


def fade_image(obj, start: int, end: int, from_opacity: float, to_opacity: float) -> None:
    key_image_opacity(obj, start, from_opacity)
    key_image_opacity(obj, end, to_opacity)


def show_between(obj, start: int, end: int, frame_end: int) -> None:
    keys = {
        1: False,
        max(1, start - 1): False,
        start: True,
        end: True,
        min(frame_end, end + 1): False,
        frame_end: False,
    }
    if start <= 1:
        keys[1] = True
    if end >= frame_end:
        keys[frame_end] = True
    for frame in sorted(keys):
        key_visibility(obj, frame, keys[frame])


def show_prefixes_between(prefixes: tuple[str, ...], start: int, end: int, frame_end: int) -> None:
    for obj in bpy.data.objects:
        if obj.name.startswith(prefixes):
            show_between(obj, start, end, frame_end)


def set_easing() -> None:
    for action in bpy.data.actions:
        for fc in action.fcurves:
            for kp in fc.keyframe_points:
                if fc.data_path in {"hide_viewport", "hide_render"}:
                    kp.interpolation = "CONSTANT"
                else:
                    kp.interpolation = "BEZIER"
                    kp.easing = "EASE_IN_OUT"


def setup_render(out_dir: Path, width: int, height: int, fps: int, frame_end: int) -> None:
    scene = bpy.context.scene
    scene.render.engine = "BLENDER_WORKBENCH"
    scene.display.shading.light = "FLAT"
    scene.display.shading.color_type = "TEXTURE"
    scene.render.resolution_x = width
    scene.render.resolution_y = height
    scene.render.fps = fps
    scene.frame_start = 1
    scene.frame_end = frame_end
    scene.world = bpy.data.worlds.new("limeglow_world") if scene.world is None else scene.world
    scene.world.color = (0.025, 0.025, 0.04)
    scene.view_settings.view_transform = "Standard"
    scene.view_settings.look = "Medium High Contrast"
    scene.render.film_transparent = False
    scene.render.image_settings.file_format = "PNG"
    scene.render.filepath = str(out_dir / "frames" / "frame_")


def asset_path(input_pack: dict, asset_id: str) -> Path:
    for speaker in input_pack["selected_speaker_assets"]:
        if speaker["asset_id"] == asset_id:
            return resolve(speaker["path"])
    for excursion in input_pack["selected_excursions"]:
        for asset in excursion["visual_assets"]:
            if asset["asset_id"] == asset_id:
                return resolve(asset["path"])
    raise KeyError(asset_id)


def add_window_blocks(prefix: str, base_x: float, base_z: float, color: str, *, y: float = 2.75, alpha: float = 0.28) -> None:
    for idx, (dx, dz, scale) in enumerate(
        [(-0.6, 0.55, 1.0), (-0.18, 0.7, 0.78), (0.28, 0.42, 0.9), (0.64, 0.82, 0.65), (-0.42, -0.18, 0.72)]
    ):
        add_color_plane(f"{prefix}_window_{idx}", base_x + dx, y, base_z + dz, 0.22 * scale, 0.32 * scale, color, alpha, rot_z=2)


def add_route_dots(prefix: str, base_x: float, base_z: float, color: str) -> None:
    for idx, (dx, dz) in enumerate([(-0.7, -0.45), (-0.28, -0.18), (0.18, 0.08), (0.62, 0.34)]):
        bpy.ops.mesh.primitive_uv_sphere_add(segments=24, ring_count=12, radius=0.055 + idx * 0.005, location=(base_x + dx, -0.62, base_z + dz))
        dot = bpy.context.object
        dot.name = f"{prefix}_dot_{idx}"
        dot.data.materials.append(make_color_mat(f"{dot.name}_mat", color, 1.0))


def build_scene(
    packs: dict,
    out_dir: Path,
    width: int,
    height: int,
    fps: int,
    frame_end: int,
    variant: str,
) -> dict:
    clean_scene()
    setup_render(out_dir, width, height, fps, frame_end)

    input_pack = packs["input_pack"]
    product_pack = packs["product_pack"]
    hook_pack = packs["hook_pack"]

    speaker_a = asset_path(input_pack, "speaker_debug_a")
    speaker_b = asset_path(input_pack, "speaker_debug_b")
    prepared_dir = ROOT / "artifacts/codex/limeglow-kaggle-cutout-probe-v3/limeglow_cutouts"
    house = prepared_dir / "amalienau_brick_house_paper_object.png"
    if not house.exists():
        house = asset_path(input_pack, "amalienau_paper_house")
    if not house.exists():
        raw_house = asset_path(input_pack, "amalienau_raw_brick_house")
        house = raw_house if raw_house.exists() else ROOT / "docs/backlog/features/limeglow/images-test/VSmFaL8hxMRoF3-zNzQbUMCIleFDiz9Gmhc9s3MlSZxKQCdAS2LLSTLAwEnkXukHuJEWqEQnBN9cSR63NnAtzb8q.jpg"
    tram = prepared_dir / "tram_corridor_exact_cutout.png"
    if not tram.exists():
        tram = asset_path(input_pack, "tram_vehicle_cutout")
    tram_asset_available = tram.exists()
    tram_background = prepared_dir / "tram_corridor_source.jpg"
    if not tram_background.exists():
        raw_tram = asset_path(input_pack, "tram_raw_corridor")
        tram_background = raw_tram if raw_tram.exists() else tram

    a = product_pack["excursions"][0]
    b = product_pack["excursions"][1]
    hook_a = hook_pack["hooks"][0]["primary_hook"]
    hook_b = hook_pack["hooks"][1]["primary_hook"]
    cta = product_pack["outro"]["preferred_cta"]

    pal = {
        "bg": "#07070D",
        "paper": "#F2EEE5",
        "text": "#F4F4F4",
        "muted": "#777A85",
        "magenta": "#D02050",
        "blue": "#0060F0",
        "lime": "#D8F15C",
        "oxide": "#A74934",
        "ink": "#101014",
    }

    # Far semantic typography: visible for long stretches so camera movement
    # reveals depth instead of switching slides.
    add_text("far_intro_excursions", "ЭКСКУРСИИ", -0.1, 3.8, 2.65, 0.46, "#232735", 0.72, rot_z=-2)
    add_text("far_intro_walks", "ПРОГУЛКА", -1.15, 3.6, 1.42, 0.34, "#232735", 0.58, rot_z=4)
    add_text("far_a_amalienau", "АМАЛИЕНАУ", -2.05, 3.75, 0.2, 0.38, "#252937", 0.72, rot_z=90)
    add_text("far_a_walk", "ПРОГУЛКА", -0.8, 3.45, -0.35, 0.34, "#242835", 0.65, rot_z=-4)
    add_text("far_b_tram", "ТРАМВАЙ", 1.35, 3.7, -2.7, 0.42, "#252A3A", 0.76, rot_z=-2)
    add_text("far_b_rails", "РЕЛЬСЫ", 2.45, 3.55, -3.55, 0.33, "#252A3A", 0.68, rot_z=90)
    add_text("far_b_excursion", "ЭКСКУРСИЯ", 0.2, 3.45, -3.95, 0.32, "#252A3A", 0.62, rot_z=3)
    add_text("far_final_digest", "ДАЙДЖЕСТ", 0.0, 3.8, -5.55, 0.52, "#262A38", 0.75, rot_z=-2)

    # Intro: humans first, not small cards.
    intro_a = add_image_plane("intro_speaker_a_crop", speaker_a, -1.12, -0.05, 0.28, 3.02, 0.95, -4)
    intro_b = add_image_plane("intro_speaker_b_crop", speaker_b, 1.28, 0.08, 0.18, 2.78, 0.65, 3)
    add_text("intro_title", "ЭКСКУРСИИ\nНЕДЕЛИ", 0.0, -0.82, 2.72, 0.38, pal["text"], 1.0)
    add_color_plane("intro_lime_pill", 0.0, -1.02, 2.2, 1.35, 0.16, pal["lime"], 1.0)
    add_text("intro_kicker", "два маршрута из дайджеста", 0.0, -1.06, 2.2, 0.08, pal["ink"], 1.0)
    if variant != "tram_background":
        add_route("intro_route_hint", [(-1.3, -0.78, 1.15), (-0.2, -0.86, 0.95), (0.8, -0.8, 0.52)], pal["lime"], 0.012, 0.55)

    # Amalienau: guide, animated echo, hook, house promise, product tag.
    add_image_plane("a_guide_hero", speaker_a, -2.28, -0.08, -0.28, 2.9, 1.0, 0)
    echo_a1 = add_image_plane("echo_a_1", speaker_a, -2.62, 0.88, 0.08, 2.42, 0.26, -2)
    echo_a2 = add_image_plane("echo_a_2", speaker_a, -1.9, 1.05, -0.04, 2.24, 0.16, 1)
    add_window_blocks("a", -0.75, 0.2, pal["oxide"])
    add_color_plane("a_hook_back", -2.1, -0.78, -0.55, 1.75, 0.88, pal["ink"], 0.84, -1)
    add_text("a_hook", "Что скрывает\nнемецкая вилла?", -2.1, -0.88, -0.54, 0.18, pal["text"], 1.0)
    add_text("a_hook_meta", "14 ИЮНЯ · ИГОРЬ ЛЯШУК", -2.1, -0.91, -0.86, 0.115, pal["lime"], 1.0, rot_z=-1)
    add_image_plane("a_visual_house", house, -0.7, 0.02, -0.14, 2.25, 1.0, -2)
    add_text("a_title", "АМАЛИЕНАУ", -0.72, -0.94, -0.82, 0.23, pal["text"], 1.0)
    add_color_plane("a_guide_name_back", -0.72, -1.06, -1.08, 1.7, 0.28, pal["ink"], 0.9)
    add_text("a_guide_name", "ИГОРЬ ЛЯШУК", -0.72, -1.1, -1.08, 0.125, pal["text"], 1.0)
    add_color_plane("a_date_back", -0.72, -1.07, -1.42, 1.52, 0.34, pal["ink"], 0.9)
    add_text("a_date", "14 ИЮНЯ", -0.72, -1.11, -1.42, 0.17, pal["lime"], 1.0)
    if variant != "tram_background":
        add_route("a_roofline_wipe", [(-1.8, -0.9, -0.95), (-1.1, -0.92, -0.76), (-0.25, -0.95, -0.95), (0.55, -0.92, -0.72)], pal["magenta"], 0.014, 0.72)
    add_color_plane("occluder_architecture_pushin", -0.7, -1.16, -0.08, 2.35, 2.35, pal["ink"], 0.97, -2)

    # Semantic bridge: rails start before the tram scene.
    if variant != "tram_background":
        add_route("transition_rail_1", [(-0.9, -0.95, -1.25), (0.0, -1.05, -1.65), (0.9, -1.02, -2.15), (1.7, -1.0, -2.65)], pal["blue"], 0.01, 0.58)
        add_route("transition_rail_2", [(-0.75, -0.75, -1.12), (0.08, -0.82, -1.52), (1.0, -0.78, -2.0), (1.82, -0.76, -2.5)], pal["lime"], 0.006, 0.58)
    add_text("transition_word", "МАРШРУТ", 0.55, 2.9, -1.65, 0.28, "#242936", 0.7, rot_z=-8)

    # Tram: guide, hook, route dots, tram object, product tag.
    add_image_plane("b_guide_hero", speaker_b, 2.15, -0.08, -3.0, 2.92, 1.0, 0)
    add_route_dots("b_route", 2.0, -2.8, pal["lime"])
    add_color_plane("b_hook_back", 2.2, -0.78, -2.85, 1.78, 1.05, pal["ink"], 0.85, 1)
    add_text("b_hook", "Куда ведут\nрельсы старого\nКёнигсберга?", 2.2, -0.9, -2.82, 0.16, pal["text"], 1.0)
    add_text("b_hook_meta", "27 ИЮНЯ 17:30 · ДИНА ЛЯХ", 2.2, -0.93, -3.28, 0.1, pal["lime"], 1.0, rot_z=1)
    if variant == "tram_background":
        add_image_plane("b_visual_tram", tram_background, 0.66, 2.85, -3.42, 5.5, 0.46, 0)
        add_color_plane("b_tram_photo_veil", 0.66, 2.55, -3.42, 4.8, 4.0, pal["bg"], 0.24, 0)
    elif tram_asset_available:
        add_image_plane("b_visual_tram", tram, 0.58, 0.0, -3.24, 1.95, 1.0, 2)
    else:
        # Fallback when one-off cutout artifacts were cleaned: keep the motion
        # plan renderable with a deliberately graphic tram proxy.
        add_color_plane("b_visual_tram_body", 0.62, 0.0, -3.1, 1.05, 1.25, pal["blue"], 0.92, 2)
        add_color_plane("b_visual_tram_window", 0.62, -0.04, -2.84, 0.56, 0.36, pal["paper"], 0.88, 2)
        add_color_plane("b_visual_tram_front", 0.62, -0.05, -3.5, 0.62, 0.28, pal["oxide"], 0.96, 2)
        add_text("b_visual_tram", "435", 0.62, -0.08, -3.38, 0.11, pal["paper"], 1.0, rot_z=2)
    if variant != "tram_background":
        add_route("b_rail_foreground", [(0.2, -1.0, -3.95), (0.55, -1.0, -3.35), (0.98, -1.0, -2.75), (1.35, -1.0, -2.25)], pal["blue"], 0.01, 0.48)
    add_text("b_title", "КЁНИГСБЕРГСКИЙ\nТРАМВАЙ", 0.5, -0.98, -4.05, 0.17, pal["text"], 1.0)
    add_color_plane("b_guide_name_back", 0.52, -1.08, -4.38, 1.44, 0.28, pal["ink"], 0.9)
    add_text("b_guide_name", "ДИНА ЛЯХ", 0.52, -1.12, -4.38, 0.14, pal["text"], 1.0)
    add_color_plane("b_date_back", 0.52, -1.08, -4.74, 1.86, 0.34, pal["ink"], 0.9)
    add_text("b_date", "27 ИЮНЯ 17:30", 0.52, -1.12, -4.74, 0.13, pal["lime"], 1.0)

    # Digest bridge and CTA: humans large, visual fragments small/supporting.
    add_image_plane("final_guide_a", speaker_a, -1.55, -0.04, -6.08, 3.0, 1.0, -1)
    add_image_plane("final_guide_b", speaker_b, 1.55, -0.04, -6.04, 2.88, 1.0, 1)
    add_image_plane("final_house_fragment", house, -1.72, 0.92, -4.8, 0.9, 0.35, -6)
    if tram_asset_available:
        add_image_plane("final_tram_fragment", tram, 1.78, 0.86, -4.86, 0.68, 0.42, 7)
    else:
        add_color_plane("final_tram_fragment", 1.78, 0.86, -4.86, 0.46, 0.42, pal["blue"], 0.6, 7)
    add_color_plane("final_cta_back", 0.0, -1.0, -6.05, 2.55, 0.8, pal["ink"], 0.96, 0)
    add_text("final_cta", "ПРОЧИТАТЬ\nДАЙДЖЕСТ", 0.0, -1.12, -6.02, 0.31, pal["lime"], 1.0)
    add_text("final_sub", "гиды, даты и маршруты — в подборке", 0.0, -1.22, -6.62, 0.08, pal["text"], 0.9)

    # Scene objects switch only around push-in/covered beats. This prevents the
    # v2 problem where future boards were visible from the intro.
    show_prefixes_between(("intro_",), 1, 42, frame_end)
    show_prefixes_between(("a_",), 34, 184, frame_end)
    show_prefixes_between(("transition_",), 154, 188, frame_end)
    show_prefixes_between(("b_",), 174, 310, frame_end)
    show_prefixes_between(("final_",), 286, frame_end, frame_end)
    show_prefixes_between(("far_intro",), 1, 42, frame_end)
    show_prefixes_between(("far_a",), 34, 170, frame_end)
    show_prefixes_between(("far_b",), 176, 292, frame_end)
    show_prefixes_between(("far_final",), 286, frame_end, frame_end)
    show_between(echo_a1, 50, 86, frame_end)
    show_between(echo_a2, 54, 82, frame_end)
    show_between(bpy.data.objects["occluder_architecture_pushin"], 156, 164, frame_end)
    show_between(bpy.data.objects["a_guide_hero"], 34, 120, frame_end)
    for name in ("a_hook_back", "a_hook", "a_hook_meta"):
        show_between(bpy.data.objects[name], 88, 138, frame_end)
    for name in ("a_visual_house", "a_title"):
        show_between(bpy.data.objects[name], 122, 184, frame_end)
    for name in ("a_guide_name_back", "a_guide_name", "a_date_back", "a_date"):
        show_between(bpy.data.objects[name], 110, 184, frame_end)
    show_between(bpy.data.objects["b_guide_hero"], 174, 306, frame_end)
    for name in ("b_hook_back", "b_hook", "b_hook_meta"):
        show_between(bpy.data.objects[name], 198, 254, frame_end)
    if variant == "tram_background":
        show_between(bpy.data.objects["b_visual_tram"], 174, 320, frame_end)
        show_between(bpy.data.objects["b_tram_photo_veil"], 174, 320, frame_end)
        show_between(bpy.data.objects["b_title"], 236, 310, frame_end)
    else:
        for name in ("b_visual_tram", "b_title"):
            show_between(bpy.data.objects[name], 208, 292, frame_end)
    for name in ("b_guide_name_back", "b_guide_name", "b_date_back", "b_date"):
        show_between(bpy.data.objects[name], 222, 310, frame_end)

    # Fade the intro humans as the camera leaves the cover. They should feel
    # carried out by the travel, not cut away.
    fade_image(intro_a, 24, 38, 0.95, 0.0)
    fade_image(intro_b, 24, 38, 0.65, 0.0)
    fade_image(bpy.data.objects["a_guide_hero"], 96, 120, 1.0, 0.0)
    fade_image(bpy.data.objects["b_guide_hero"], 286, 306, 1.0, 0.0)

    # Echo beat: source portrait first, then two clones split out from it.
    for obj, start, dx, dz in [
        (echo_a1, 50, -0.34, 0.08),
        (echo_a2, 54, 0.28, -0.02),
    ]:
        final_loc = obj.location.copy()
        final_scale = obj.scale.copy()
        source = bpy.data.objects["a_guide_hero"]
        key_object(obj, start, loc=source.location, scale=source.scale * 0.9)
        fade_image(obj, start, start + 5, 0.0, 0.26 if obj == echo_a1 else 0.16)
        key_object(obj, start + 15, loc=final_loc, scale=final_scale)

    for name, start in [("a_guide_hero", 31), ("a_visual_house", 92), ("b_guide_hero", 164), ("b_visual_tram", 212), ("final_cta", 250)]:
        obj = bpy.data.objects[name]
        final_scale = obj.scale.copy()
        key_object(obj, start, scale=final_scale * 0.92)
        key_object(obj, start + 12, scale=final_scale)
    if variant == "tram_background":
        fade_image(bpy.data.objects["b_visual_tram"], 174, 218, 0.08, 0.3)
        fade_image(bpy.data.objects["b_visual_tram"], 218, 250, 0.3, 0.72)
        fade_image(bpy.data.objects["b_visual_tram"], 292, 320, 0.72, 0.18)

    # Camera route: baked every frame through editorial cubic-bezier. Includes
    # a push-in through the architecture object before the tram transition.
    bpy.ops.object.camera_add(location=(0.0, -8.6, 2.45))
    camera = bpy.context.object
    camera.name = "LimeglowCamera"
    bpy.context.scene.camera = camera
    camera_route = [
        {"frame": 1, "loc": (0.0, -8.8, 2.08), "target": (0.0, 0.0, 1.84), "lens": 34, "purpose": "human-led digest intro"},
        {"frame": 24, "loc": (0.0, -8.4, 1.92), "target": (0.0, 0.0, 1.78), "lens": 38, "purpose": "intro settle with grounded speakers"},
        {"frame": 38, "loc": (-2.18, -6.25, 0.02), "target": (-2.18, 0.0, -0.02), "lens": 58, "purpose": "fast editorial move to Amalienau guide"},
        {"frame": 54, "loc": (-2.18, -6.0, 0.02), "target": (-2.18, 0.0, -0.02), "lens": 62, "purpose": "Amalienau guide identity hold"},
        {"frame": 70, "loc": (-2.05, -5.85, -0.08), "target": (-2.05, 0.0, -0.1), "lens": 62, "purpose": "separate echo beat"},
        {"frame": 92, "loc": (-2.05, -5.45, -0.54), "target": (-2.05, 0.0, -0.54), "lens": 68, "purpose": "Amalienau hook hold"},
        {"frame": 118, "loc": (-0.78, -5.35, -0.2), "target": (-0.78, 0.0, -0.2), "lens": 64, "purpose": "architecture visual promise"},
        {"frame": 140, "loc": (-0.72, -5.95, -1.18), "target": (-0.72, 0.0, -1.16), "lens": 56, "purpose": "Amalienau guide/date/title read"},
        {"frame": 158, "loc": (-0.72, -4.15, -0.08), "target": (-0.72, 0.0, -0.08), "lens": 84, "purpose": "push-in transition into architecture object"},
        {"frame": 174, "loc": (0.1, -6.1, -1.25), "target": (0.08, 0.0, -1.34), "lens": 48, "purpose": "emerge through route geometry"},
        {"frame": 194, "loc": (2.12, -6.35, -2.68), "target": (2.12, 0.0, -2.66), "lens": 58, "purpose": "tram guide hold"},
        {"frame": 218, "loc": (2.12, -5.45, -2.86), "target": (2.12, 0.0, -2.86), "lens": 68, "purpose": "tram hook hold"},
        {"frame": 242, "loc": (0.72, -5.7, -3.3), "target": (0.68, 0.0, -3.24), "lens": 64, "purpose": "push-in to tram background photo"},
        {"frame": 268, "loc": (0.72, -6.2, -4.28), "target": (0.68, 0.0, -4.26), "lens": 54, "purpose": "tram guide/date/title read"},
        {"frame": 292, "loc": (0.0, -7.4, -5.35), "target": (0.0, 0.0, -5.45), "lens": 46, "purpose": "digest bridge pull-back"},
        {"frame": 312, "loc": (0.0, -8.1, -5.92), "target": (0.0, 0.0, -5.92), "lens": 42, "purpose": "CTA lock"},
        {"frame": frame_end, "loc": (0.0, -8.55, -6.05), "target": (0.0, 0.0, -6.03), "lens": 39, "purpose": "CTA settle with micro motion"},
    ]
    bake_camera_route(camera, camera_route, frame_end)
    set_easing()

    return {
        "camera_keyframes": [
            {"frame": item["frame"], "purpose": item["purpose"]} for item in camera_route
        ],
        "selected_assets": {
            "speaker_debug_a": str(speaker_a.relative_to(ROOT)),
            "speaker_debug_b": str(speaker_b.relative_to(ROOT)),
            "amalienau_visual_object": str(house.relative_to(ROOT)),
            "tram_vehicle_cutout": str(tram.relative_to(ROOT)) if tram_asset_available else None,
        },
        "debug_caveats": [
            "480p preview is for motion/composition, not final text-readability judgment.",
            "Speaker assets are debug placeholders and are not fact-bound.",
            "Hooks still need validation against full source descriptions before production.",
            "If prior one-off image cutouts are missing, renderer falls back to local architecture image and a graphic tram proxy.",
        ],
    }


def render_frames(out_dir: Path) -> None:
    frames_dir = out_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    bpy.context.scene.render.filepath = str(frames_dir / "frame_")
    try:
        bpy.ops.render.opengl(animation=True, view_context=False)
    except RuntimeError as exc:
        if "Cannot use OpenGL render in background mode" not in str(exc):
            raise
        bpy.ops.render.render(animation=True)


def copy_keyframes(out_dir: Path, frames: list[int]) -> list[str]:
    key_dir = out_dir / "keyframes"
    key_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for frame in frames:
        src = out_dir / "frames" / f"frame_{frame:04d}.png"
        dst = key_dir / f"kf-{frame:04d}.png"
        if src.exists():
            shutil.copyfile(src, dst)
            copied.append(str(dst.relative_to(ROOT)))
    return copied


def main() -> None:
    args = parse_args()
    pack_dir = resolve(args.pack_dir)
    out_dir = resolve(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    frame_end = int(round(args.fps * args.duration))
    packs = {
        "input_pack": read_json(pack_dir / "input_pack.json"),
        "product_pack": read_json(pack_dir / "product_pack.json"),
        "hook_pack": read_json(pack_dir / "hook_pack.json"),
        "asset_treatment_plan": read_json(pack_dir / "asset_treatment_plan.json"),
        "grammar_selection": read_json(pack_dir / "grammar_selection.json"),
        "object_map_seed": read_json(pack_dir / "object_map_seed.json"),
    }

    meta = build_scene(packs, out_dir, args.width, args.height, args.fps, frame_end, args.variant)
    if not args.skip_frames:
        render_frames(out_dir)

    keyframes = copy_keyframes(out_dir, [1, 15, 30, 45, 58, 72, 82, 98, 116, 141, 166, 192, 214, 240])

    director_plan = {
        "product": "limeglow",
        "version": "blender-debug-08",
        "renderer": "scripts/render_limeglow_blender_debug08.py",
        "packs": str(pack_dir.relative_to(ROOT)),
        "resolution": [args.width, args.height],
        "fps": args.fps,
        "duration_sec": args.duration,
        "variant": args.variant,
        "frame_end": frame_end,
        "render_mode": "full PNG sequence first, low-cost 2.5D Blender Workbench/OpenGL",
        "motion_principles": [
            "one primary accent per beat",
            "large grounded speakers",
            "visual object tied to guide/hook/date",
            "far/mid/hero/foreground depth planes",
            "baked editorial camera easing: cubicBezier(0.76, 0.00, 0.24, 1.00)",
            "push-in transition through architecture/tram objects",
            "no ice cream or skyline/pipes in this two-excursion render",
        ],
        **meta,
        "copied_keyframes": keyframes,
        "frames_dir": str((out_dir / "frames").relative_to(ROOT)),
    }
    (out_dir / "director_plan.json").write_text(json.dumps(director_plan, ensure_ascii=False, indent=2), encoding="utf-8")

    render_manifest = {
        "run_id": "limeglow-blender-debug-08",
        "status": "frames_rendered" if not args.skip_frames else "scene_built_skip_frames",
        "frame_sequence_pattern": str((out_dir / "frames" / "frame_%04d.png").relative_to(ROOT)),
        "expected_video_path": str((out_dir / "motion_preview_15fps_480p.mp4").relative_to(ROOT)),
        "expected_audit_sheet_pattern": str((out_dir / "audit" / "page-%02d.jpg").relative_to(ROOT)),
        "next_shell_steps": [
            "ffmpeg -y -framerate 15 -i artifacts/codex/limeglow-blender-debug-08/frames/frame_%04d.png -c:v libx264 -pix_fmt yuv420p artifacts/codex/limeglow-blender-debug-08/motion_preview_15fps_480p.mp4",
            "ffmpeg -y -framerate 15 -i artifacts/codex/limeglow-blender-debug-08/frames/frame_%04d.png -vf scale=135:240,tile=5x4 artifacts/codex/limeglow-blender-debug-08/audit/page-%02d.jpg",
        ],
    }
    (out_dir / "render_manifest.json").write_text(json.dumps(render_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {out_dir / 'director_plan.json'}")
    print(f"wrote {out_dir / 'render_manifest.json'}")
    print(f"frames: {frame_end} at {args.width}x{args.height} {args.fps}fps")
    bpy.ops.wm.quit_blender()


if __name__ == "__main__":
    main()
