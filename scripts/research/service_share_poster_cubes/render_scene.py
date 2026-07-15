#!/usr/bin/env python3
"""Portable Blender renderer for the approved service-share cube scene."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import bpy
from bpy_extras.object_utils import world_to_camera_view
from mathutils import Vector

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
from layout_contract import resolve_layout


def srgb(value: str) -> tuple[float, float, float, float]:
    value = value.lstrip("#")
    encoded = [int(value[index:index + 2], 16) / 255 for index in (0, 2, 4)]
    linear = [channel / 12.92 if channel <= .04045 else ((channel + .055) / 1.055) ** 2.4 for channel in encoded]
    return tuple(linear) + (1,)


def clean() -> None:
    bpy.ops.object.select_all(action="SELECT")
    bpy.ops.object.delete(use_global=False)


def matte(name, color, roughness=.58):
    material = bpy.data.materials.new(name)
    material.use_nodes = True
    shader = material.node_tree.nodes.get("Principled BSDF")
    shader.inputs["Base Color"].default_value = color
    shader.inputs["Metallic"].default_value = 0
    shader.inputs["Roughness"].default_value = roughness
    if "Specular IOR Level" in shader.inputs:
        shader.inputs["Specular IOR Level"].default_value = .24
    return material


def diffuse(name, color, roughness=.9):
    material = bpy.data.materials.new(name)
    material.use_nodes = True
    nodes = material.node_tree.nodes
    links = material.node_tree.links
    nodes.clear()
    shader = nodes.new("ShaderNodeBsdfDiffuse")
    shader.inputs["Color"].default_value = color
    shader.inputs["Roughness"].default_value = roughness
    output = nodes.new("ShaderNodeOutputMaterial")
    links.new(shader.outputs["BSDF"], output.inputs["Surface"])
    return material


def image_material(name: str, path: Path, strength=.98):
    material = bpy.data.materials.new(name)
    material.use_nodes = True
    nodes = material.node_tree.nodes
    links = material.node_tree.links
    nodes.clear()
    uv = nodes.new("ShaderNodeTexCoord")
    texture = nodes.new("ShaderNodeTexImage")
    texture.image = bpy.data.images.load(str(path.resolve()), check_existing=True)
    texture.image.colorspace_settings.name = "sRGB"
    texture.extension = "CLIP"
    texture.interpolation = "Linear"
    emission = nodes.new("ShaderNodeEmission")
    emission.inputs["Strength"].default_value = strength
    output = nodes.new("ShaderNodeOutputMaterial")
    links.new(uv.outputs["UV"], texture.inputs["Vector"])
    links.new(texture.outputs["Color"], emission.inputs["Color"])
    links.new(emission.outputs["Emission"], output.inputs["Surface"])
    return material


def cube(name, location, size, material, rotation=(0, 0, 0)):
    bpy.ops.mesh.primitive_cube_add(size=size, location=location, rotation=rotation)
    obj = bpy.context.object
    obj.name = name
    obj.data.materials.append(material)
    bevel = obj.modifiers.new("Soft premium bevel", "BEVEL")
    bevel.width = min(.09, size * .035)
    bevel.segments = 7
    bevel.limit_method = "ANGLE"
    return obj


def plane(name, location, size, face, material, inset=.9, rotation=(0, 0, 0)):
    half = size / 2
    extent = size * inset / 2
    offset = .007
    if face == "front":
        vertices = [(-extent,-half-offset,-extent),(extent,-half-offset,-extent),(extent,-half-offset,extent),(-extent,-half-offset,extent)]
    elif face == "left":
        vertices = [(-half-offset,half-half*(1-inset),-extent),(-half-offset,-half+half*(1-inset),-extent),(-half-offset,-half+half*(1-inset),extent),(-half-offset,half-half*(1-inset),extent)]
    elif face == "top":
        vertices = [(-extent,-half+half*(1-inset),half+offset),(extent,-half+half*(1-inset),half+offset),(extent,half-half*(1-inset),half+offset),(-extent,half-half*(1-inset),half+offset)]
    else:
        raise ValueError(face)
    mesh = bpy.data.meshes.new(name + "_Mesh")
    mesh.from_pydata(vertices, [], [(0, 1, 2, 3)])
    mesh.update()
    uv = mesh.uv_layers.new(name="UVMap")
    for loop, coordinate in zip(mesh.loops, [(0,0),(1,0),(1,1),(0,1)]):
        uv.data[loop.index].uv = coordinate
    obj = bpy.data.objects.new(name, mesh)
    bpy.context.collection.objects.link(obj)
    obj.location = location
    obj.rotation_euler = rotation
    obj.data.materials.append(material)
    obj.visible_shadow = False
    return obj


def cyclorama(name, material):
    import math
    profile = [(-15, -3.7), (4, -3.7)]
    radius = 5.0
    for index in range(1, 13):
        angle = (index / 12) * (math.pi / 2)
        profile.append((4 + radius * math.sin(angle), -3.7 + radius * (1 - math.cos(angle))))
    profile.append((9, 14))
    vertices = []
    for x in (-25, 25):
        vertices.extend([(x, y, z) for y, z in profile])
    size = len(profile)
    faces = [(index, index + 1, size + index + 1, size + index) for index in range(size - 1)]
    mesh = bpy.data.meshes.new(name + "_Mesh")
    mesh.from_pydata(vertices, [], faces)
    mesh.update()
    obj = bpy.data.objects.new(name, mesh)
    bpy.context.collection.objects.link(obj)
    obj.data.materials.append(material)
    for polygon in mesh.polygons:
        polygon.use_smooth = True
    return obj


def point(obj, target):
    obj.rotation_euler = (Vector(target) - obj.location).to_track_quat("-Z", "Y").to_euler()


def area(name, location, energy, size, color, target):
    light = bpy.data.lights.new(name, "AREA")
    light.energy = energy
    light.shape = "DISK"
    light.size = size
    light.color = color
    obj = bpy.data.objects.new(name, light)
    bpy.context.collection.objects.link(obj)
    obj.location = location
    point(obj, target)


def _projected_bbox(scene, camera, obj) -> dict:
    points = [world_to_camera_view(scene, camera, obj.matrix_world @ Vector(corner)) for corner in obj.bound_box]
    xs = [point.x for point in points]
    ys = [point.y for point in points]
    bbox = [min(xs), min(ys), max(xs), max(ys)]
    clipped = [max(0, bbox[0]), max(0, bbox[1]), min(1, bbox[2]), min(1, bbox[3])]
    clipped_area = max(0, clipped[2] - clipped[0]) * max(0, clipped[3] - clipped[1])
    return {
        "bbox": [round(value, 5) for value in bbox],
        "clipped_bbox": [round(value, 5) for value in clipped],
        "clipped_area": round(clipped_area, 6),
        "center": [round((bbox[0] + bbox[2]) / 2, 5), round((bbox[1] + bbox[3]) / 2, 5)],
    }


def _bbox_gap(first: list[float], second: list[float]) -> float:
    gap_x = max(0.0, first[0] - second[2], second[0] - first[2])
    gap_y = max(0.0, first[1] - second[3], second[1] - first[3])
    return (gap_x * gap_x + gap_y * gap_y) ** .5


def _bbox_overlap_ratio(first: list[float], second: list[float]) -> float:
    overlap_x = max(0.0, min(first[2], second[2]) - max(first[0], second[0]))
    overlap_y = max(0.0, min(first[3], second[3]) - max(first[1], second[1]))
    overlap = overlap_x * overlap_y
    first_area = max(0.0, first[2] - first[0]) * max(0.0, first[3] - first[1])
    second_area = max(0.0, second[2] - second[0]) * max(0.0, second[3] - second[1])
    return overlap / max(1e-9, min(first_area, second_area))


def validate_composition(scene, camera, objects: list, *, family: str, seed: int) -> dict:
    projected = {obj.name.removeprefix("Cube_"): _projected_bbox(scene, camera, obj) for obj in objects}
    errors = []
    safe_x = .435
    for name, metrics in projected.items():
        if metrics["bbox"][0] < safe_x:
            errors.append(f"{name}: product safe-zone intrusion x={metrics['bbox'][0]:.3f}")
    hero = projected["HERO"]
    bridge = projected["BRIDGE"]
    if hero["bbox"][2] < 1.0 or hero["bbox"][1] > 0.0:
        errors.append("HERO: must exit both right and bottom frame edges")
    if not (.16 <= hero["clipped_area"] <= .42):
        errors.append(f"HERO: clipped area outside dominance band ({hero['clipped_area']:.3f})")
    if hero["clipped_area"] < bridge["clipped_area"] * 2.0:
        errors.append("HERO: insufficient dominance over BRIDGE")
    for name, threshold in {"BRIDGE":.018, "A":.0025, "B":.0025, "C":.0018}.items():
        if projected[name]["clipped_area"] < threshold:
            errors.append(f"{name}: visible area below {threshold}")
    chain = ["HERO", "BRIDGE", "A", "B", "C"]
    gaps = {}
    overlaps = {}
    world_ratios = {}
    object_map = {obj.name.removeprefix("Cube_"):obj for obj in objects}
    for first, second in zip(chain, chain[1:]):
        gap = _bbox_gap(projected[first]["bbox"], projected[second]["bbox"])
        overlap = _bbox_overlap_ratio(projected[first]["bbox"], projected[second]["bbox"])
        first_object, second_object = object_map[first], object_map[second]
        mean_size = (first_object.dimensions.x + second_object.dimensions.x) / 2
        world_ratio = (first_object.location - second_object.location).length / max(.001, mean_size)
        gaps[f"{first}->{second}"] = round(gap, 6)
        overlaps[f"{first}->{second}"] = round(overlap, 6)
        world_ratios[f"{first}->{second}"] = round(world_ratio, 6)
        if gap > .005:
            errors.append(f"{first}->{second}: disconnected screen-space gap {gap:.3f}")
        if overlap < .05:
            errors.append(f"{first}->{second}: insufficient screen-space overlap {overlap:.3f}")
        if world_ratio > 2.4:
            errors.append(f"{first}->{second}: excessive 3D distance ratio {world_ratio:.3f}")
    if errors:
        raise RuntimeError("composition rejected: " + "; ".join(errors) + " projected=" + json.dumps(projected, sort_keys=True))
    return {
        "family": family,
        "seed": seed,
        "safe_zone_x": safe_x,
        "hero_exits": ["right", "bottom"],
        "projected": projected,
        "chain_gaps": gaps,
        "chain_overlap_ratios": overlaps,
        "chain_world_distance_ratios": world_ratios,
        "gates_passed": True,
        "seamless_cyclorama": True,
    }


def configure_device(scene, requested: str) -> dict:
    if requested == "CPU":
        scene.cycles.device = "CPU"
        return {"requested": requested, "actual": "CPU", "devices": ["CPU"]}
    preferences = bpy.context.preferences.addons["cycles"].preferences
    errors = []
    for backend in ("OPTIX", "CUDA"):
        try:
            preferences.compute_device_type = backend
            preferences.get_devices()
            enabled = []
            for device in preferences.devices:
                device.use = device.type != "CPU"
                if device.use:
                    enabled.append(f"{device.type}:{device.name}")
            if enabled:
                scene.cycles.device = "GPU"
                return {"requested": requested, "actual": "GPU", "backend": backend, "devices": enabled}
        except Exception as exc:
            errors.append(f"{backend}:{exc}")
    raise RuntimeError(f"GPU requested but no Cycles GPU device enabled: {errors}")


def build(config_path: Path, bundle_root: Path, output_dir: Path) -> None:
    config = json.loads(config_path.read_text())
    manifest = json.loads((bundle_root / "faces" / "face_manifest.json").read_text())
    faces = manifest["faces"]
    if len(faces) < 8:
        raise RuntimeError("at least eight face textures are required")
    materials = [image_material(f"EventFace_{row['event_id']}_{index}", bundle_root / "faces" / row["face_path"]) for index, row in enumerate(faces)]
    clean()
    graphite = matte("Deep graphite", srgb("#1f2322"), .55)
    graphite_2 = matte("Quiet graphite", srgb("#292d2c"), .62)
    floor = diffuse("Seamless warm cyclorama", srgb("#d8d4cc"), .92)
    family, composition_seed, layout = resolve_layout(config)
    material_map = {"graphite":graphite, "graphite_2":graphite_2}
    cubes = [(name, location, size, material_map[material], rotation) for name, location, size, material, rotation in layout]
    cube_objects = []
    for index, (name, location, size, body, rotation) in enumerate(cubes):
        cube_objects.append(cube("Cube_" + name, location, size, body, rotation))
        plane(f"EventFront_{name}_{faces[index]['event_id']}", location, size, "front", materials[index], .90, rotation)
    plane(f"EventLeft_HERO_{faces[5]['event_id']}", cubes[0][1], cubes[0][2], "left", materials[5], .87, cubes[0][4])
    plane(f"EventTop_HERO_{faces[6]['event_id']}", cubes[0][1], cubes[0][2], "top", materials[6], .87, cubes[0][4])
    plane(f"EventLeft_BRIDGE_{faces[7]['event_id']}", cubes[1][1], cubes[1][2], "left", materials[7], .87, cubes[1][4])
    plane(f"EventTop_BRIDGE_{faces[0]['event_id']}", cubes[1][1], cubes[1][2], "top", materials[0], .87, cubes[1][4])
    cyclorama("InfinityCyclorama", floor)
    area("Large warm key", (-5,-7,12), 1450, 7.5, (1,.94,.87), (2,0,.3))
    area("Broad cool fill", (9,-5,7), 720, 8, (.84,.9,1), (3,0,.2))
    area("Rim", (7,5,10), 1150, 6, (1,.76,.57), (3,1,1))
    camera_data = bpy.data.cameras.new("Camera")
    camera = bpy.data.objects.new("Camera", camera_data)
    bpy.context.collection.objects.link(camera)
    camera.location = (0,-13.6,2.35)
    camera_data.lens = 28
    camera_data.sensor_width = 36
    camera_data.dof.use_dof = False
    point(camera, (1.85,.2,.25))
    scene = bpy.context.scene
    scene.camera = camera
    scene.render.resolution_x = scene.render.resolution_y = int(config["resolution"])
    scene.render.resolution_percentage = 100
    bpy.context.view_layer.update()
    composition = validate_composition(scene, camera, cube_objects, family=family, seed=composition_seed)
    scene.render.engine = "CYCLES"
    scene.cycles.use_denoising = True
    scene.cycles.use_adaptive_sampling = True
    scene.cycles.adaptive_threshold = float(config.get("adaptive_threshold", .025))
    scene.cycles.adaptive_min_samples = int(config.get("adaptive_min_samples", 16))
    scene.cycles.samples = int(config["samples"])
    scene.cycles.max_bounces = 3
    scene.cycles.diffuse_bounces = 1
    scene.cycles.glossy_bounces = 1
    scene.cycles.transmission_bounces = 0
    scene.cycles.volume_bounces = 0
    scene.cycles.use_reflective_caustics = False
    scene.cycles.use_refractive_caustics = False
    scene.cycles.sample_clamp_indirect = 2.0
    device = configure_device(scene, str(config["device"]).upper())
    scene.render.image_settings.file_format = "PNG"
    scene.render.image_settings.color_mode = "RGB"
    scene.view_settings.view_transform = "Standard"
    scene.view_settings.look = "None"
    scene.view_settings.exposure = 0
    scene.view_settings.gamma = 1
    scene.world.use_nodes = True
    scene.world.node_tree.nodes["Background"].inputs["Color"].default_value = srgb("#d8d4cc")
    scene.world.node_tree.nodes["Background"].inputs["Strength"].default_value = .22
    output_dir.mkdir(parents=True, exist_ok=True)
    scene.render.filepath = str(output_dir / config["base_filename"])
    blend_path = output_dir / config["blend_filename"]
    scene["research_only"] = True
    scene["selection_mix"] = json.dumps(manifest["selection"]["requested_mix"], ensure_ascii=False)
    scene["texture_extension"] = "CLIP"
    scene["no_dof"] = True
    scene["render_profile"] = config["profile"]
    scene["render_device"] = device["actual"]
    scene["composition_family"] = family
    scene["composition_seed"] = str(composition_seed)
    scene["composition_gates_passed"] = True
    print("SERVICE_SHARE_COMPOSITION " + json.dumps(composition, ensure_ascii=False, sort_keys=True), flush=True)
    print("SERVICE_SHARE_PREFLIGHT " + json.dumps({"device": device, "profile": config["profile"], "resolution": config["resolution"], "samples": config["samples"]}), flush=True)
    if config.get("preflight_only"):
        return
    if config.get("keep_blend", True):
        bpy.context.preferences.filepaths.save_version = 0
        bpy.ops.wm.save_as_mainfile(filepath=str(blend_path))
        bpy.ops.file.pack_all()
        bpy.ops.wm.save_as_mainfile(filepath=str(blend_path))
    bpy.ops.render.render(write_still=True)
    print("SERVICE_SHARE_RENDER_DONE " + json.dumps({"base": scene.render.filepath, "blend": str(blend_path) if config.get("keep_blend", True) else None, "device": device}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--output-dir", required=True)
    argv = sys.argv[sys.argv.index("--") + 1:] if "--" in sys.argv else sys.argv[1:]
    args = parser.parse_args(argv)
    build(Path(args.config), Path(args.bundle_root), Path(args.output_dir))


if __name__ == "__main__":
    main()
