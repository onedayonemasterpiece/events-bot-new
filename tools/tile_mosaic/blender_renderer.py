"""Blender-side physical tile renderer.

Run only through Blender:

    blender -b --factory-startup --python blender_renderer.py -- \
      --plan scene-plan.json --textures-dir textures --output render.png
"""

from __future__ import annotations

import argparse
import ctypes.util
import json
import math
from pathlib import Path
import sys
from typing import Any

import bpy
from mathutils import Vector


STATE_NAMES = ("sealed", "dim", "sleeping", "revealed", "glint")


def _args() -> argparse.Namespace:
    argv = sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else []
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", required=True)
    parser.add_argument("--textures-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--engine", choices=("eevee", "cycles"), default="eevee")
    return parser.parse_args(argv)


def _set_input(node: Any, names: tuple[str, ...], value: Any) -> None:
    for name in names:
        socket = node.inputs.get(name)
        if socket is not None:
            socket.default_value = value
            return


def _clean_scene() -> None:
    if bpy.context.object and bpy.context.object.mode != "OBJECT":
        bpy.ops.object.mode_set(mode="OBJECT")
    bpy.ops.object.select_all(action="SELECT")
    bpy.ops.object.delete(use_global=False)
    for collection in (
        bpy.data.meshes,
        bpy.data.curves,
        bpy.data.materials,
        bpy.data.cameras,
        bpy.data.lights,
    ):
        for block in list(collection):
            collection.remove(block)


def _rgb(value: list[float] | tuple[float, ...], alpha: float = 1.0) -> tuple[float, float, float, float]:
    return (float(value[0]), float(value[1]), float(value[2]), alpha)


def _look_at(obj: Any, target: tuple[float, float, float]) -> None:
    direction = Vector(target) - obj.location
    obj.rotation_euler = direction.to_track_quat("-Z", "Y").to_euler()


def _make_simple_material(name: str, rgb: list[float], roughness: float) -> Any:
    material = bpy.data.materials.new(name)
    material.use_nodes = True
    nodes = material.node_tree.nodes
    nodes.clear()
    output = nodes.new("ShaderNodeOutputMaterial")
    shader = nodes.new("ShaderNodeBsdfPrincipled")
    _set_input(shader, ("Base Color",), _rgb(rgb))
    _set_input(shader, ("Roughness",), roughness)
    _set_input(shader, ("Metallic",), 0.0)
    material.node_tree.links.new(shader.outputs["BSDF"], output.inputs["Surface"])
    return material


def _make_state_material(
    *,
    state_name: str,
    state: dict[str, Any],
    material_config: dict[str, Any],
    texture_path: Path,
) -> Any:
    material = bpy.data.materials.new(f"Tile_{state_name}")
    material.use_nodes = True
    nodes = material.node_tree.nodes
    links = material.node_tree.links
    nodes.clear()

    output = nodes.new("ShaderNodeOutputMaterial")
    output.location = (900, 0)
    shader = nodes.new("ShaderNodeBsdfPrincipled")
    shader.location = (650, 0)
    links.new(shader.outputs["BSDF"], output.inputs["Surface"])

    image = bpy.data.images.load(str(texture_path), check_existing=True)
    image.colorspace_settings.name = "sRGB"
    tex = nodes.new("ShaderNodeTexImage")
    tex.image = image
    tex.interpolation = "Cubic"
    tex.projection = "FLAT"
    tex.location = (-760, 260)

    uv = nodes.new("ShaderNodeUVMap")
    uv.location = (-980, 260)
    links.new(uv.outputs["UV"], tex.inputs["Vector"])

    dark = tuple(float(channel) / 255.0 for channel in state.get("veil_rgb", [5, 7, 8]))
    mix = nodes.new("ShaderNodeMixRGB")
    mix.blend_type = "MIX"
    mix.inputs[0].default_value = float(state.get("image_mix", 1.0))
    mix.inputs[1].default_value = (*dark, 1.0)
    mix.location = (120, 280)
    links.new(tex.outputs["Color"], mix.inputs[2])

    object_info = nodes.new("ShaderNodeObjectInfo")
    object_info.location = (-1000, -260)

    object_brightness = nodes.new("ShaderNodeMixRGB")
    object_brightness.blend_type = "MULTIPLY"
    object_brightness.inputs[0].default_value = 1.0
    object_brightness.location = (360, 260)
    links.new(mix.outputs["Color"], object_brightness.inputs[1])
    links.new(object_info.outputs["Color"], object_brightness.inputs[2])
    links.new(object_brightness.outputs["Color"], shader.inputs["Base Color"])

    micro = nodes.new("ShaderNodeTexNoise")
    micro.noise_dimensions = "4D"
    micro.inputs["Scale"].default_value = float(material_config["micro_scale"])
    micro.inputs["Detail"].default_value = 3.2
    micro.inputs["Roughness"].default_value = 0.72
    micro.location = (-650, -180)
    links.new(object_info.outputs["Random"], micro.inputs["W"])

    macro = nodes.new("ShaderNodeTexNoise")
    macro.noise_dimensions = "4D"
    macro.inputs["Scale"].default_value = float(material_config["macro_scale"])
    macro.inputs["Detail"].default_value = 2.0
    macro.inputs["Roughness"].default_value = 0.62
    macro.location = (-650, -430)
    links.new(object_info.outputs["Random"], macro.inputs["W"])

    add_noise = nodes.new("ShaderNodeMixRGB")
    add_noise.blend_type = "MULTIPLY"
    add_noise.inputs[0].default_value = 0.68
    add_noise.location = (-350, -250)
    links.new(micro.outputs["Fac"], add_noise.inputs[1])
    links.new(macro.outputs["Fac"], add_noise.inputs[2])

    bump = nodes.new("ShaderNodeBump")
    bump.inputs["Strength"].default_value = float(material_config["micro_strength"])
    bump.inputs["Distance"].default_value = 0.065
    bump.location = (350, -175)
    links.new(add_noise.outputs["Color"], bump.inputs["Height"])
    links.new(bump.outputs["Normal"], shader.inputs["Normal"])

    roughness = float(state.get("roughness", material_config["base_roughness"]))
    variation = float(material_config.get("roughness_variation", 0.06))
    ramp = nodes.new("ShaderNodeValToRGB")
    ramp.location = (-20, -365)
    ramp.color_ramp.elements[0].position = 0.18
    ramp.color_ramp.elements[0].color = (max(0.0, roughness - variation),) * 3 + (1.0,)
    ramp.color_ramp.elements[1].position = 0.82
    ramp.color_ramp.elements[1].color = (min(1.0, roughness + variation),) * 3 + (1.0,)
    links.new(macro.outputs["Fac"], ramp.inputs["Fac"])
    links.new(ramp.outputs["Color"], shader.inputs["Roughness"])

    _set_input(shader, ("Metallic",), 0.0)
    _set_input(shader, ("IOR",), 1.45)
    _set_input(
        shader,
        ("Coat Weight", "Clearcoat"),
        float(state.get("coat_weight", material_config.get("coat_weight", 0.0))),
    )
    _set_input(
        shader,
        ("Coat Roughness", "Clearcoat Roughness"),
        float(state.get("coat_roughness", material_config.get("coat_roughness", 0.5))),
    )
    return material


def _make_tile_mesh(
    *,
    name: str,
    half: float,
    thickness: float,
    uv_bounds: tuple[float, float, float, float],
) -> Any:
    z_back = -thickness / 2.0
    z_front = thickness / 2.0
    vertices = [
        (-half, -half, z_back),
        (half, -half, z_back),
        (half, half, z_back),
        (-half, half, z_back),
        (-half, -half, z_front),
        (half, -half, z_front),
        (half, half, z_front),
        (-half, half, z_front),
    ]
    # Front first so polygon 0 receives the projected image material.
    faces = [
        (4, 5, 6, 7),
        (1, 0, 3, 2),
        (0, 4, 7, 3),
        (5, 1, 2, 6),
        (3, 7, 6, 2),
        (0, 1, 5, 4),
    ]
    mesh = bpy.data.meshes.new(name)
    mesh.from_pydata(vertices, [], faces)
    mesh.update()
    # Blender 4.0 requires Auto Smooth for hardened bevel normals and the
    # Weighted Normal modifier. Newer Blender lines may remove this RNA
    # property, so the modifier path below is conditional on its presence.
    if hasattr(mesh, "use_auto_smooth"):
        mesh.use_auto_smooth = True
        if hasattr(mesh, "auto_smooth_angle"):
            mesh.auto_smooth_angle = math.radians(60.0)
    uv_layer = mesh.uv_layers.new(name="UVMap")
    u0, v0, u1, v1 = uv_bounds
    front_uv = ((u0, v0), (u1, v0), (u1, v1), (u0, v1))
    for polygon in mesh.polygons:
        polygon.material_index = 0 if polygon.index == 0 else 1
        polygon.use_smooth = False
        for local_index, loop_index in enumerate(polygon.loop_indices):
            uv_layer.data[loop_index].uv = front_uv[local_index] if polygon.index == 0 else (0.5, 0.5)
    return mesh


def _create_tile(
    *,
    tile_plan: dict[str, Any],
    plan: dict[str, Any],
    state_materials: dict[str, Any],
    side_material: Any,
) -> Any:
    grid = plan["grid"]
    columns = int(grid["columns"])
    rows = int(grid["rows"])
    col = int(tile_plan["column"])
    row = int(tile_plan["row"])
    tile_size = float(grid["tile_size_world"])
    gap = float(grid["gap_world"])
    step = tile_size + gap
    grid_width = float(grid["width_world"])
    grid_height = float(grid["height_world"])
    thickness = float(grid["tile_thickness"])
    name = f"Tile_{row:02d}_{col:02d}_{tile_plan['state']}"

    u0 = col / columns
    u1 = (col + 1) / columns
    v1 = 1.0 - row / rows
    v0 = 1.0 - (row + 1) / rows
    mesh = _make_tile_mesh(
        name=name,
        half=tile_size / 2.0,
        thickness=thickness,
        uv_bounds=(u0, v0, u1, v1),
    )
    obj = bpy.data.objects.new(name, mesh)
    bpy.context.scene.collection.objects.link(obj)
    obj.data.materials.append(state_materials[str(tile_plan["state"])])
    obj.data.materials.append(side_material)

    tilt_x = float(tile_plan.get("tilt_x_degrees", 0.0))
    tilt_y = float(tile_plan.get("tilt_y_degrees", 0.0))
    depth = float(tile_plan.get("depth_offset", 0.0))
    corner_lift = float(tile_plan.get("corner_lift", 0.0) or 0.0)
    lifted_corner = tile_plan.get("lifted_corner")
    if corner_lift > 0 and lifted_corner:
        lift_degrees = corner_lift * 18.0
        signs = {
            "top_left": (-1.0, 1.0),
            "top_right": (-1.0, -1.0),
            "bottom_right": (1.0, -1.0),
            "bottom_left": (1.0, 1.0),
        }.get(str(lifted_corner), (0.0, 0.0))
        tilt_x += signs[0] * lift_degrees
        tilt_y += signs[1] * lift_degrees
        depth += corner_lift * 0.35

    obj.location = (
        -grid_width / 2.0 + tile_size / 2.0 + col * step,
        grid_height / 2.0 - tile_size / 2.0 - row * step,
        depth,
    )
    obj.rotation_euler = (
        math.radians(tilt_x),
        math.radians(tilt_y),
        math.radians(float(tile_plan.get("rotation_degrees", 0.0))),
    )
    brightness = max(0.35, min(1.65, float(tile_plan.get("brightness_multiplier", 1.0))))
    obj.color = (brightness, brightness, brightness, 1.0)

    bevel = obj.modifiers.new("Rounded physical edge", "BEVEL")
    bevel.width = float(grid["bevel_radius"])
    bevel.segments = 4
    bevel.limit_method = "ANGLE"
    auto_smooth_enabled = bool(getattr(mesh, "use_auto_smooth", False))
    if hasattr(bevel, "harden_normals"):
        bevel.harden_normals = auto_smooth_enabled
    if hasattr(bevel, "material"):
        bevel.material = 1

    # Keep weighted normals only where the corresponding mesh contract exists.
    # This removes Blender 4.0 modifier warnings without flattening the bevel.
    if auto_smooth_enabled:
        weighted = obj.modifiers.new("Weighted normals", "WEIGHTED_NORMAL")
        if hasattr(weighted, "keep_sharp"):
            weighted.keep_sharp = True
    return obj


def _create_area_light(name: str, config: dict[str, Any]) -> Any:
    data = bpy.data.lights.new(name, type="AREA")
    data.energy = float(config["energy"])
    data.color = tuple(float(channel) for channel in config["color"])
    data.shape = "DISK"
    data.size = float(config.get("size", 4.0))
    data.use_shadow = True
    obj = bpy.data.objects.new(name, data)
    bpy.context.scene.collection.objects.link(obj)
    obj.location = tuple(float(value) for value in config["position"])
    _look_at(obj, (0.0, 0.0, 0.0))
    return obj


def _resolve_engine(requested: str) -> str:
    if requested != "eevee":
        return requested
    # EEVEE needs a working EGL/OpenGL runtime even in Blender background mode.
    # GitHub runners and minimal containers may intentionally omit libEGL.
    # Cycles CPU is slower but fully headless and preserves the physical material
    # contract, so use it as a deterministic portability fallback.
    if ctypes.util.find_library("EGL"):
        return requested
    print(json.dumps({
        "event": "blender_engine_fallback",
        "requested": requested,
        "effective": "cycles",
        "reason": "libEGL.so.1 not available",
    }, ensure_ascii=False), flush=True)
    return "cycles"


def _configure_scene(plan: dict[str, Any], output: Path, engine: str) -> Any:
    scene = bpy.context.scene
    canvas = plan["canvas"]
    render = plan.get("render", {})
    scene.render.resolution_x = int(canvas["width"])
    scene.render.resolution_y = int(canvas["height"])
    scene.render.resolution_percentage = 100
    scene.render.film_transparent = bool(render.get("transparent", False))
    scene.render.image_settings.file_format = str(render.get("file_format", "PNG"))
    scene.render.image_settings.color_mode = str(render.get("color_mode", "RGB"))
    if hasattr(scene.render.image_settings, "compression"):
        scene.render.image_settings.compression = int(render.get("compression", 18))
    scene.render.filepath = str(output)
    scene.render.use_file_extension = True

    if engine == "cycles":
        scene.render.engine = "CYCLES"
        scene.cycles.samples = int(render.get("samples", 64))
        scene.cycles.use_denoising = True
        scene.cycles.device = "CPU"
    else:
        # Blender 4.0 exposes EEVEE as ``BLENDER_EEVEE`` while newer releases
        # renamed the same renderer to ``BLENDER_EEVEE_NEXT``. Probe the RNA
        # enum instead of coupling the laboratory to one minor Blender line.
        eevee_engine = None
        for candidate in ("BLENDER_EEVEE_NEXT", "BLENDER_EEVEE"):
            try:
                scene.render.engine = candidate
                eevee_engine = candidate
                break
            except TypeError:
                continue
        if eevee_engine is None:
            supported = tuple(
                item.identifier
                for item in scene.render.bl_rna.properties["engine"].enum_items
            )
            raise RuntimeError(
                "No supported EEVEE render engine is available; "
                f"Blender reports {supported!r}"
            )
        samples = int(render.get("samples", 64))
        eevee = getattr(scene, "eevee", None)
        if eevee is not None:
            for attribute in ("taa_render_samples", "taa_samples"):
                if hasattr(eevee, attribute):
                    setattr(eevee, attribute, samples)

    scene.render.engine = scene.render.engine
    scene.render.image_settings.color_depth = "8"
    scene.view_settings.view_transform = "AgX"
    for look in ("AgX - Medium High Contrast", "AgX - Medium Low Contrast", "None"):
        try:
            scene.view_settings.look = look
            break
        except TypeError:
            continue
    scene.view_settings.exposure = -0.25
    scene.view_settings.gamma = 1.0

    world = scene.world or bpy.data.worlds.new("World")
    scene.world = world
    world.use_nodes = True
    background = world.node_tree.nodes.get("Background")
    if background is not None:
        background.inputs["Color"].default_value = (0.0015, 0.0022, 0.0028, 1.0)
        background.inputs["Strength"].default_value = 0.08
    return scene


def _create_camera(plan: dict[str, Any]) -> Any:
    grid = plan["grid"]
    camera_config = plan["camera"]
    grid_width = float(grid["width_world"])
    grid_height = float(grid["height_world"])
    center_x = float(camera_config.get("offset_x", 0.0)) * grid_width
    center_y = float(camera_config.get("offset_y", 0.0)) * grid_height
    camera_data = bpy.data.cameras.new("Camera")
    camera = bpy.data.objects.new("Camera", camera_data)
    bpy.context.scene.collection.objects.link(camera)
    camera_data.type = "ORTHO"
    camera_data.ortho_scale = grid_height * (1.0 + 2.0 * float(camera_config.get("overscan", 0.0)))
    camera_data.lens = 70.0
    tilt_x = math.radians(float(camera_config.get("tilt_x_degrees", 0.0)))
    tilt_y = math.radians(float(camera_config.get("tilt_y_degrees", 0.0)))
    distance = 14.0
    camera.location = (
        center_x + math.tan(tilt_y) * distance,
        center_y - math.tan(tilt_x) * distance,
        distance,
    )
    _look_at(camera, (center_x, center_y, 0.0))
    bpy.context.scene.camera = camera
    return camera


def build(plan: dict[str, Any], textures_dir: Path, output: Path, engine: str) -> None:
    _clean_scene()
    scene = _configure_scene(plan, output, engine)
    material_config = plan["material"]
    side_material = _make_simple_material(
        "Tile sides",
        list(material_config["side_rgb"]),
        0.93,
    )
    grout_material = _make_simple_material(
        "Deep matte grout",
        list(material_config["grout_rgb"]),
        0.96,
    )
    state_materials = {
        state_name: _make_state_material(
            state_name=state_name,
            state=plan["states"][state_name],
            material_config=material_config,
            texture_path=textures_dir / f"texture-{state_name}.png",
        )
        for state_name in STATE_NAMES
    }

    for tile_plan in plan["tiles"]:
        _create_tile(
            tile_plan=tile_plan,
            plan=plan,
            state_materials=state_materials,
            side_material=side_material,
        )

    grid = plan["grid"]
    bpy.ops.mesh.primitive_cube_add(
        location=(0.0, 0.0, -float(grid["tile_thickness"]) / 2.0 - 0.09),
        scale=(float(grid["width_world"]) * 0.58, float(grid["height_world"]) * 0.62, 0.07),
    )
    grout = bpy.context.active_object
    grout.name = "Opaque grout and backing"
    grout.data.materials.append(grout_material)
    bevel = grout.modifiers.new("Backing bevel", "BEVEL")
    bevel.width = 0.06
    bevel.segments = 3

    _create_camera(plan)
    for light_name in ("key", "fill", "rim"):
        _create_area_light(light_name.capitalize(), plan["lighting"][light_name])

    # A very broad front fill keeps sealed tiles tactile instead of collapsing
    # into mathematically flat black.
    front = bpy.data.lights.new("Front ambient", type="AREA")
    front.energy = 105.0
    front.color = (0.33, 0.38, 0.43)
    front.shape = "RECTANGLE"
    front.size = float(grid["width_world"]) * 0.9
    front.size_y = float(grid["height_world"]) * 0.9
    front_obj = bpy.data.objects.new("Front ambient", front)
    scene.collection.objects.link(front_obj)
    front_obj.location = (0.0, 0.0, 8.0)
    _look_at(front_obj, (0.0, 0.0, 0.0))

    output.parent.mkdir(parents=True, exist_ok=True)
    bpy.ops.wm.save_as_mainfile(filepath=str(output.with_suffix(".blend")))
    bpy.ops.render.render(write_still=True)
    if not output.exists():
        candidate = output.with_suffix(output.suffix + ".png")
        if candidate.exists():
            candidate.replace(output)
    if not output.exists():
        raise RuntimeError(f"Blender did not create {output}")


def main() -> None:
    args = _args()
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    effective_engine = _resolve_engine(args.engine)
    build(plan, Path(args.textures_dir), Path(args.output), effective_engine)
    print(json.dumps({
        "status": "ok",
        "output": str(Path(args.output).resolve()),
        "requested_engine": args.engine,
        "engine": effective_engine,
        "plan_sha256": plan.get("plan_sha256"),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
