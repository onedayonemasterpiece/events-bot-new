#!/usr/bin/env python3
"""Portable Blender renderer for the approved service-share cube scene."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import bpy
from mathutils import Vector


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
    cubes = [
        ("HERO", (4.20,-5.35,-1.45), 4.25, graphite, (0,0,0)),
        ("BRIDGE", (3.25,-1.58,.82), 2.32, graphite_2, (.02,-.015,-.085)),
        ("A", (2.10,.72,3.25), 1.16, graphite_2, (-.025,.015,-.065)),
        ("B", (3.50,1.18,2.92), 1.36, graphite, (.035,-.02,.075)),
        ("C", (4.82,1.90,2.28), 1.25, graphite_2, (-.03,.02,-.095)),
    ]
    for index, (name, location, size, body, rotation) in enumerate(cubes):
        cube("Cube_" + name, location, size, body, rotation)
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
    scene.render.resolution_x = scene.render.resolution_y = int(config["resolution"])
    scene.render.resolution_percentage = 100
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
    bpy.ops.wm.save_as_mainfile(filepath=str(blend_path))
    bpy.ops.file.pack_all()
    bpy.ops.wm.save_as_mainfile(filepath=str(blend_path))
    print("SERVICE_SHARE_PREFLIGHT " + json.dumps({"device": device, "profile": config["profile"], "resolution": config["resolution"], "samples": config["samples"]}), flush=True)
    bpy.ops.render.render(write_still=True)
    print("SERVICE_SHARE_RENDER_DONE " + json.dumps({"base": scene.render.filepath, "blend": str(blend_path), "device": device}), flush=True)


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
