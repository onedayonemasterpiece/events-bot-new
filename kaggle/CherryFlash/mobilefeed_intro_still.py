import json
import math
import sys
from pathlib import Path

import bpy
from mathutils import Matrix, Vector


ROOT = Path(__file__).resolve().parents[2]


def _parse_args():
    argv = sys.argv
    if "--" not in argv:
        return {}
    args = argv[argv.index("--") + 1 :]
    out = {}
    i = 0
    while i < len(args):
        key = args[i]
        if not key.startswith("--"):
            i += 1
            continue
        key = key[2:]
        value = True
        if i + 1 < len(args) and not args[i + 1].startswith("--"):
            value = args[i + 1]
            i += 1
        out[key] = value
        i += 1
    return out


def _hex_to_rgba(color: str, alpha: float = 1.0):
    color = color.lstrip("#")
    return (
        int(color[0:2], 16) / 255.0,
        int(color[2:4], 16) / 255.0,
        int(color[4:6], 16) / 255.0,
        alpha,
    )


def _set_socket(node, socket_name: str, value):
    if socket_name in node.inputs:
        node.inputs[socket_name].default_value = value


def _try_set_enum(obj, attr: str, value) -> bool:
    try:
        prop = obj.bl_rna.properties[attr]
    except Exception:
        return False
    if value in prop.enum_items.keys():
        setattr(obj, attr, value)
        return True
    return False


def _new_material(name: str):
    mat = bpy.data.materials.new(name=name)
    mat.use_nodes = True
    nt = mat.node_tree
    for node in list(nt.nodes):
        nt.nodes.remove(node)
    return mat, nt


def _look_at(obj, target):
    direction = Vector(target) - obj.location
    if direction.length < 1e-6:
        return
    obj.rotation_euler = direction.to_track_quat("-Z", "Y").to_euler()


def _world_bbox(obj):
    corners = [obj.matrix_world @ Vector(corner) for corner in obj.bound_box]
    min_x = min(v.x for v in corners)
    max_x = max(v.x for v in corners)
    min_y = min(v.y for v in corners)
    max_y = max(v.y for v in corners)
    min_z = min(v.z for v in corners)
    max_z = max(v.z for v in corners)
    return {
        "min_x": min_x,
        "max_x": max_x,
        "min_y": min_y,
        "max_y": max_y,
        "min_z": min_z,
        "max_z": max_z,
        "center": Vector(((min_x + max_x) / 2.0, (min_y + max_y) / 2.0, (min_z + max_z) / 2.0)),
        "size": Vector((max_x - min_x, max_y - min_y, max_z - min_z)),
    }


def _projected_bounds(objects, center: Vector, axis_u: Vector, axis_v: Vector, axis_n: Vector):
    mins = [float("inf"), float("inf"), float("inf")]
    maxs = [float("-inf"), float("-inf"), float("-inf")]
    for obj in objects:
        if getattr(obj, "type", None) != "MESH":
            continue
        for corner in obj.bound_box:
            point = obj.matrix_world @ Vector(corner)
            rel = point - center
            coords = (rel.dot(axis_u), rel.dot(axis_v), rel.dot(axis_n))
            for i, coord in enumerate(coords):
                mins[i] = min(mins[i], coord)
                maxs[i] = max(maxs[i], coord)
    return {
        "min_u": mins[0],
        "max_u": maxs[0],
        "min_v": mins[1],
        "max_v": maxs[1],
        "min_n": mins[2],
        "max_n": maxs[2],
    }


def _unit_axis(index: int):
    axes = (Vector((1.0, 0.0, 0.0)), Vector((0.0, 1.0, 0.0)), Vector((0.0, 0.0, 1.0)))
    return axes[index]


def _smoothstep(t: float):
    t = max(0.0, min(1.0, t))
    return t * t * (3.0 - 2.0 * t)


def _hermite(p0: Vector, p1: Vector, m0: Vector, m1: Vector, t: float):
    t2 = t * t
    t3 = t2 * t
    h00 = 2 * t3 - 3 * t2 + 1
    h10 = t3 - 2 * t2 + t
    h01 = -2 * t3 + 3 * t2
    h11 = t3 - t2
    return h00 * p0 + h10 * m0 + h01 * p1 + h11 * m1


def _camera_rotation(location: Vector, target: Vector, up_hint: Vector):
    forward = Vector(target) - Vector(location)
    if forward.length < 1e-6:
        return Vector((0.0, 0.0, 0.0)).to_track_quat("-Z", "Y").to_euler()
    forward.normalize()
    up = Vector(up_hint)
    if up.length < 1e-6:
        up = Vector((0.0, 0.0, 1.0))
    up.normalize()
    if abs(forward.dot(up)) > 0.985:
        up = Vector((1.0, 0.0, 0.0)) if abs(forward.x) < 0.8 else Vector((0.0, 1.0, 0.0))
    right = forward.cross(up)
    if right.length < 1e-6:
        right = forward.orthogonal()
    right.normalize()
    true_up = right.cross(forward)
    if true_up.length < 1e-6:
        true_up = Vector((0.0, 0.0, 1.0))
    else:
        true_up.normalize()
    rot = Matrix((right, true_up, -forward)).transposed()
    return rot.to_euler()


def _screen_basis(screen_obj, *, view_hint=None, camera_target=None):
    bbox_local = [Vector(corner) for corner in screen_obj.bound_box]
    mins = Vector((min(v.x for v in bbox_local), min(v.y for v in bbox_local), min(v.z for v in bbox_local)))
    maxs = Vector((max(v.x for v in bbox_local), max(v.y for v in bbox_local), max(v.z for v in bbox_local)))
    extents = [maxs[i] - mins[i] for i in range(3)]
    center_local = (mins + maxs) * 0.5
    thickness_idx = min(range(3), key=lambda i: extents[i])
    flat_axes = [i for i in range(3) if i != thickness_idx]
    width_idx, height_idx = sorted(flat_axes, key=lambda i: extents[i])
    matrix = screen_obj.matrix_world.to_3x3()
    center_world = screen_obj.matrix_world @ center_local
    width_vec = matrix @ _unit_axis(width_idx)
    height_vec = matrix @ _unit_axis(height_idx)
    normal_vec = matrix @ _unit_axis(thickness_idx)
    width_len = max(width_vec.length, 1e-6)
    height_len = max(height_vec.length, 1e-6)
    normal_len = max(normal_vec.length, 1e-6)
    width_axis = width_vec / width_len
    height_axis = height_vec / height_len
    normal = normal_vec / normal_len
    if view_hint is not None and normal.dot(Vector(view_hint) - center_world) < 0:
        normal.negate()
    if view_hint is not None and camera_target is not None:
        cam_loc = Vector(view_hint)
        cam_target = Vector(camera_target)
        cam_forward = (cam_target - cam_loc)
        if cam_forward.length > 1e-6:
            cam_forward.normalize()
            world_up = Vector((0.0, 0.0, 1.0))
            cam_right = cam_forward.cross(world_up)
            if cam_right.length < 1e-6:
                cam_right = Vector((1.0, 0.0, 0.0))
            else:
                cam_right.normalize()
            cam_up = cam_right.cross(cam_forward)
            if cam_up.length > 1e-6:
                cam_up.normalize()
            if width_axis.dot(cam_right) < 0:
                width_axis.negate()
            if height_axis.dot(cam_up) < 0:
                height_axis.negate()
    else:
        if width_axis.y > 0:
            width_axis.negate()
        if height_axis.z > 0:
            height_axis.negate()
    return {
        "center": center_world,
        "width_axis": width_axis,
        "height_axis": height_axis,
        "normal": normal,
        "width": extents[width_idx] * width_len,
        "height": extents[height_idx] * height_len,
        "thickness": extents[thickness_idx] * normal_len,
    }


def _load_image(path):
    return bpy.data.images.load(str(Path(path)), check_existing=True)


def make_principled_material(
    name: str,
    *,
    base_color: str,
    metallic: float = 0.0,
    roughness: float = 0.5,
    specular: float = 0.5,
    coat: float = 0.0,
    anisotropy: float = 0.0,
    anisotropy_rotation: float = 0.0,
    emission_strength: float = 0.0,
):
    mat, nt = _new_material(name)
    nodes = nt.nodes
    links = nt.links
    out = nodes.new("ShaderNodeOutputMaterial")
    bsdf = nodes.new("ShaderNodeBsdfPrincipled")
    bsdf.inputs["Base Color"].default_value = _hex_to_rgba(base_color)
    bsdf.inputs["Metallic"].default_value = metallic
    bsdf.inputs["Roughness"].default_value = roughness
    _set_socket(bsdf, "Specular IOR Level", specular)
    _set_socket(bsdf, "Specular", specular)
    _set_socket(bsdf, "Coat Weight", coat)
    _set_socket(bsdf, "Coat", coat)
    _set_socket(bsdf, "Coat Roughness", max(0.02, roughness * 0.55))
    _set_socket(bsdf, "Anisotropic", anisotropy)
    _set_socket(bsdf, "Anisotropy", anisotropy)
    _set_socket(bsdf, "Anisotropic Rotation", anisotropy_rotation)
    _set_socket(bsdf, "Anisotropy Rotation", anisotropy_rotation)
    if emission_strength > 0:
        emission = nodes.new("ShaderNodeEmission")
        emission.inputs["Color"].default_value = _hex_to_rgba(base_color)
        emission.inputs["Strength"].default_value = emission_strength
        add = nodes.new("ShaderNodeAddShader")
        links.new(bsdf.outputs["BSDF"], add.inputs[0])
        links.new(emission.outputs["Emission"], add.inputs[1])
        links.new(add.outputs["Shader"], out.inputs["Surface"])
    else:
        links.new(bsdf.outputs["BSDF"], out.inputs["Surface"])
    return mat


def make_backdrop_material(
    name: str,
    color: str,
    *,
    emission_strength: float = 0.34,
    roughness: float = 0.985,
    bump_strength: float = 0.004,
):
    mat, nt = _new_material(name)
    nodes = nt.nodes
    links = nt.links
    out = nodes.new("ShaderNodeOutputMaterial")
    bsdf = nodes.new("ShaderNodeBsdfPrincipled")
    bsdf.name = "BackdropBSDF"
    bsdf.label = "BackdropBSDF"
    emission = nodes.new("ShaderNodeEmission")
    emission.name = "BackdropEmission"
    emission.label = "BackdropEmission"
    add = nodes.new("ShaderNodeAddShader")
    noise = nodes.new("ShaderNodeTexNoise")
    bump = nodes.new("ShaderNodeBump")
    coord = nodes.new("ShaderNodeTexCoord")
    mapping = nodes.new("ShaderNodeMapping")
    bsdf.inputs["Base Color"].default_value = _hex_to_rgba(color)
    bsdf.inputs["Roughness"].default_value = roughness
    _set_socket(bsdf, "Specular IOR Level", 0.03)
    _set_socket(bsdf, "Specular", 0.03)
    emission.inputs["Color"].default_value = _hex_to_rgba(color)
    emission.inputs["Strength"].default_value = emission_strength
    noise.inputs["Scale"].default_value = 18.0
    noise.inputs["Detail"].default_value = 2.0
    bump.inputs["Strength"].default_value = bump_strength
    bump.inputs["Distance"].default_value = 0.12
    links.new(coord.outputs["Object"], mapping.inputs["Vector"])
    links.new(mapping.outputs["Vector"], noise.inputs["Vector"])
    links.new(noise.outputs["Fac"], bump.inputs["Height"])
    links.new(bump.outputs["Normal"], bsdf.inputs["Normal"])
    links.new(bsdf.outputs["BSDF"], add.inputs[0])
    links.new(emission.outputs["Emission"], add.inputs[1])
    links.new(add.outputs["Shader"], out.inputs["Surface"])
    return mat


def make_overlay_material(name: str, image_path: Path):
    mat, nt = _new_material(name)
    nodes = nt.nodes
    links = nt.links
    out = nodes.new("ShaderNodeOutputMaterial")
    transparent = nodes.new("ShaderNodeBsdfTransparent")
    bsdf = nodes.new("ShaderNodeBsdfPrincipled")
    mix_shader = nodes.new("ShaderNodeMixShader")
    tex = nodes.new("ShaderNodeTexImage")
    opacity = nodes.new("ShaderNodeValue")
    opacity.name = "OpacityControl"
    opacity.label = "OpacityControl"
    opacity.outputs[0].default_value = 1.0
    multiply = nodes.new("ShaderNodeMath")
    multiply.operation = "MULTIPLY"
    multiply.use_clamp = True
    tex.image = _load_image(image_path)
    tex.interpolation = "Cubic"
    tex.extension = "CLIP"
    bsdf.inputs["Roughness"].default_value = 0.92
    _set_socket(bsdf, "Specular IOR Level", 0.0)
    _set_socket(bsdf, "Specular", 0.0)
    links.new(tex.outputs["Color"], bsdf.inputs["Base Color"])
    links.new(tex.outputs["Alpha"], multiply.inputs[0])
    links.new(opacity.outputs[0], multiply.inputs[1])
    links.new(multiply.outputs[0], mix_shader.inputs["Fac"])
    links.new(transparent.outputs["BSDF"], mix_shader.inputs[1])
    links.new(bsdf.outputs["BSDF"], mix_shader.inputs[2])
    links.new(mix_shader.outputs["Shader"], out.inputs["Surface"])
    mat.blend_method = "BLEND"
    if hasattr(mat, "shadow_method"):
        mat.shadow_method = "NONE"
    return mat


def make_unlit_overlay_material(name: str, image_path: Path):
    mat, nt = _new_material(name)
    nodes = nt.nodes
    links = nt.links
    out = nodes.new("ShaderNodeOutputMaterial")
    transparent = nodes.new("ShaderNodeBsdfTransparent")
    emission = nodes.new("ShaderNodeEmission")
    mix_shader = nodes.new("ShaderNodeMixShader")
    tex = nodes.new("ShaderNodeTexImage")
    opacity = nodes.new("ShaderNodeValue")
    opacity.name = "OpacityControl"
    opacity.label = "OpacityControl"
    opacity.outputs[0].default_value = 1.0
    multiply = nodes.new("ShaderNodeMath")
    multiply.operation = "MULTIPLY"
    multiply.use_clamp = True
    tex.image = _load_image(image_path)
    tex.interpolation = "Cubic"
    tex.extension = "CLIP"
    emission.inputs["Strength"].default_value = 1.0
    links.new(tex.outputs["Color"], emission.inputs["Color"])
    links.new(tex.outputs["Alpha"], multiply.inputs[0])
    links.new(opacity.outputs[0], multiply.inputs[1])
    links.new(multiply.outputs[0], mix_shader.inputs["Fac"])
    links.new(transparent.outputs["BSDF"], mix_shader.inputs[1])
    links.new(emission.outputs["Emission"], mix_shader.inputs[2])
    links.new(mix_shader.outputs["Shader"], out.inputs["Surface"])
    mat.blend_method = "BLEND"
    if hasattr(mat, "shadow_method"):
        mat.shadow_method = "NONE"
    return mat


def make_screen_material(name: str, image_path: Path):
    mat, nt = _new_material(name)
    nodes = nt.nodes
    links = nt.links
    out = nodes.new("ShaderNodeOutputMaterial")
    bsdf = nodes.new("ShaderNodeBsdfPrincipled")
    bsdf.name = "ScreenBSDF"
    bsdf.label = "ScreenBSDF"
    tex = nodes.new("ShaderNodeTexImage")
    tex.image = _load_image(image_path)
    tex.interpolation = "Cubic"
    tex.extension = "EXTEND"
    bsdf.inputs["Roughness"].default_value = 0.038
    _set_socket(bsdf, "Specular IOR Level", 0.54)
    _set_socket(bsdf, "Specular", 0.54)
    _set_socket(bsdf, "Coat Weight", 0.18)
    _set_socket(bsdf, "Coat", 0.18)
    _set_socket(bsdf, "Coat Roughness", 0.04)
    emit = nodes.new("ShaderNodeEmission")
    emit.name = "ScreenEmission"
    emit.label = "ScreenEmission"
    emit.inputs["Strength"].default_value = 0.08
    add = nodes.new("ShaderNodeAddShader")
    links.new(tex.outputs["Color"], bsdf.inputs["Base Color"])
    links.new(tex.outputs["Color"], emit.inputs["Color"])
    links.new(bsdf.outputs["BSDF"], add.inputs[0])
    links.new(emit.outputs["Emission"], add.inputs[1])
    links.new(add.outputs["Shader"], out.inputs["Surface"])
    return mat


def make_glass_material(name: str):
    mat, nt = _new_material(name)
    nodes = nt.nodes
    links = nt.links
    out = nodes.new("ShaderNodeOutputMaterial")
    transparent = nodes.new("ShaderNodeBsdfTransparent")
    bsdf = nodes.new("ShaderNodeBsdfPrincipled")
    mix = nodes.new("ShaderNodeMixShader")
    bsdf.inputs["Base Color"].default_value = _hex_to_rgba("#0A0D12")
    bsdf.inputs["Roughness"].default_value = 0.028
    _set_socket(bsdf, "Specular IOR Level", 0.78)
    _set_socket(bsdf, "Specular", 0.78)
    _set_socket(bsdf, "Transmission Weight", 0.0)
    _set_socket(bsdf, "Transmission", 0.0)
    _set_socket(bsdf, "Coat Weight", 0.58)
    _set_socket(bsdf, "Coat", 0.58)
    _set_socket(bsdf, "Coat Roughness", 0.028)
    mix.inputs["Fac"].default_value = 0.16
    links.new(transparent.outputs["BSDF"], mix.inputs[1])
    links.new(bsdf.outputs["BSDF"], mix.inputs[2])
    links.new(mix.outputs["Shader"], out.inputs["Surface"])
    mat.blend_method = "BLEND"
    if hasattr(mat, "shadow_method"):
        mat.shadow_method = "NONE"
    return mat


def make_ribbon_material(name: str, atlas_path: Path):
    mat, nt = _new_material(name)
    nodes = nt.nodes
    links = nt.links
    out = nodes.new("ShaderNodeOutputMaterial")
    bsdf = nodes.new("ShaderNodeBsdfPrincipled")
    emit = nodes.new("ShaderNodeEmission")
    add = nodes.new("ShaderNodeAddShader")
    tex = nodes.new("ShaderNodeTexImage")
    hue = nodes.new("ShaderNodeHueSaturation")
    bright = nodes.new("ShaderNodeBrightContrast")
    tex.image = _load_image(atlas_path)
    tex.interpolation = "Cubic"
    tex.extension = "EXTEND"
    noise = nodes.new("ShaderNodeTexNoise")
    bump = nodes.new("ShaderNodeBump")
    coord = nodes.new("ShaderNodeTexCoord")
    mapping = nodes.new("ShaderNodeMapping")
    bsdf.inputs["Roughness"].default_value = 0.24
    _set_socket(bsdf, "Specular IOR Level", 0.36)
    _set_socket(bsdf, "Specular", 0.36)
    _set_socket(bsdf, "Coat Weight", 0.08)
    _set_socket(bsdf, "Coat", 0.08)
    _set_socket(bsdf, "Coat Roughness", 0.04)
    emit.inputs["Strength"].default_value = 0.005
    hue.inputs["Saturation"].default_value = 1.24
    hue.inputs["Value"].default_value = 1.02
    bright.inputs["Bright"].default_value = 0.01
    bright.inputs["Contrast"].default_value = 0.16
    noise.inputs["Scale"].default_value = 240.0
    noise.inputs["Detail"].default_value = 2.4
    bump.inputs["Strength"].default_value = 0.0006
    bump.inputs["Distance"].default_value = 0.02
    links.new(coord.outputs["UV"], tex.inputs["Vector"])
    links.new(coord.outputs["Object"], mapping.inputs["Vector"])
    links.new(mapping.outputs["Vector"], noise.inputs["Vector"])
    links.new(noise.outputs["Fac"], bump.inputs["Height"])
    links.new(tex.outputs["Color"], hue.inputs["Color"])
    links.new(hue.outputs["Color"], bright.inputs["Color"])
    links.new(bright.outputs["Color"], bsdf.inputs["Base Color"])
    links.new(bright.outputs["Color"], emit.inputs["Color"])
    links.new(bump.outputs["Normal"], bsdf.inputs["Normal"])
    links.new(bsdf.outputs["BSDF"], add.inputs[0])
    links.new(emit.outputs["Emission"], add.inputs[1])
    links.new(add.outputs["Shader"], out.inputs["Surface"])
    return mat


def _configure_cycles_device(scene):
    if not hasattr(scene, "cycles"):
        return "CPU"
    try:
        prefs = bpy.context.preferences.addons["cycles"].preferences
    except Exception:
        scene.cycles.device = "CPU"
        return "CPU"

    for device_type in ("OPTIX", "CUDA", "HIP", "ONEAPI", "METAL"):
        try:
            prefs.compute_device_type = device_type
        except Exception:
            continue
        try:
            prefs.get_devices()
        except Exception:
            pass
        devices = list(getattr(prefs, "devices", []) or [])
        gpu_devices = []
        for device in devices:
            dtype = str(getattr(device, "type", "") or "").upper()
            use_gpu = bool(dtype) and dtype != "CPU"
            try:
                device.use = use_gpu
            except Exception:
                pass
            if use_gpu:
                gpu_devices.append(dtype)
        if gpu_devices:
            scene.cycles.device = "GPU"
            return f"{device_type}:{','.join(gpu_devices)}"

    scene.cycles.device = "CPU"
    return "CPU"


def _ensure_cycles(scene):
    try:
        scene.render.engine = "CYCLES"
    except Exception:
        try:
            bpy.ops.preferences.addon_enable(module="cycles")
        except Exception:
            pass
        try:
            scene.render.engine = "CYCLES"
        except Exception:
            pass


def setup_scene(cfg):
    bpy.ops.wm.read_factory_settings(use_empty=True)
    scene = bpy.context.scene
    requested_engine = str(cfg.get("render_engine", "CYCLES"))
    if requested_engine == "CYCLES":
        _ensure_cycles(scene)
    elif not _try_set_enum(scene.render, "engine", requested_engine):
        _try_set_enum(scene.render, "engine", "BLENDER_EEVEE_NEXT")
    print(f"Render engine requested={requested_engine} active={scene.render.engine}")
    scene.render.resolution_x = int(cfg["res_x"])
    scene.render.resolution_y = int(cfg["res_y"])
    scene.render.resolution_percentage = 100
    scene.render.image_settings.file_format = "PNG"
    scene.render.image_settings.color_mode = "RGBA"
    scene.render.film_transparent = False
    if hasattr(scene.render, "use_persistent_data"):
        scene.render.use_persistent_data = bool(cfg.get("use_persistent_data", True))
    if scene.render.engine == "CYCLES":
        scene.cycles.samples = int(cfg.get("samples", 12))
        if bool(cfg.get("allow_cycles_gpu", False)):
            _configure_cycles_device(scene)
        else:
            scene.cycles.device = "CPU"
        if hasattr(scene.cycles, "use_denoising"):
            scene.cycles.use_denoising = bool(cfg.get("use_denoising", False))
        if hasattr(scene.cycles, "denoiser") and cfg.get("denoiser"):
            _try_set_enum(scene.cycles, "denoiser", str(cfg["denoiser"]))
        if hasattr(scene.cycles, "use_adaptive_sampling"):
            scene.cycles.use_adaptive_sampling = bool(cfg.get("use_adaptive_sampling", False))
        if hasattr(scene.cycles, "adaptive_threshold") and cfg.get("adaptive_threshold") is not None:
            scene.cycles.adaptive_threshold = float(cfg["adaptive_threshold"])
        if hasattr(scene.cycles, "sample_clamp_direct") and cfg.get("sample_clamp_direct") is not None:
            scene.cycles.sample_clamp_direct = float(cfg["sample_clamp_direct"])
        if hasattr(scene.cycles, "sample_clamp_indirect") and cfg.get("sample_clamp_indirect") is not None:
            scene.cycles.sample_clamp_indirect = float(cfg["sample_clamp_indirect"])
        for attr, value in (
            ("max_bounces", int(cfg.get("max_bounces", 3))),
            ("diffuse_bounces", int(cfg.get("diffuse_bounces", 1))),
            ("glossy_bounces", int(cfg.get("glossy_bounces", 2))),
            ("transmission_bounces", int(cfg.get("transmission_bounces", 2))),
            ("transparent_max_bounces", int(cfg.get("transparent_max_bounces", 4))),
        ):
            if hasattr(scene.cycles, attr):
                setattr(scene.cycles, attr, value)
    elif scene.render.engine == "BLENDER_EEVEE_NEXT":
        eevee = scene.eevee
        if hasattr(eevee, "taa_render_samples"):
            eevee.taa_render_samples = int(cfg.get("eevee_taa_render_samples", 64))
        if hasattr(eevee, "taa_samples"):
            eevee.taa_samples = int(cfg.get("eevee_taa_samples", 32))
        if hasattr(eevee, "use_taa_reprojection"):
            eevee.use_taa_reprojection = bool(cfg.get("eevee_use_taa_reprojection", True))
        if hasattr(eevee, "use_shadows"):
            eevee.use_shadows = bool(cfg.get("eevee_use_shadows", True))
        if hasattr(eevee, "shadow_ray_count"):
            eevee.shadow_ray_count = int(cfg.get("eevee_shadow_ray_count", 4))
        if hasattr(eevee, "shadow_step_count"):
            eevee.shadow_step_count = int(cfg.get("eevee_shadow_step_count", 8))
        if hasattr(eevee, "shadow_resolution_scale"):
            eevee.shadow_resolution_scale = float(cfg.get("eevee_shadow_resolution_scale", 1.0))
        if hasattr(eevee, "shadow_pool_size"):
            _try_set_enum(eevee, "shadow_pool_size", str(cfg.get("eevee_shadow_pool_size", "1024")))
        if hasattr(eevee, "use_raytracing"):
            eevee.use_raytracing = bool(cfg.get("eevee_use_raytracing", True))
        if hasattr(eevee, "ray_tracing_method") and cfg.get("eevee_ray_tracing_method"):
            _try_set_enum(eevee, "ray_tracing_method", str(cfg["eevee_ray_tracing_method"]))
        rt = getattr(eevee, "ray_tracing_options", None)
        if rt is not None:
            if hasattr(rt, "resolution_scale") and cfg.get("eevee_ray_resolution_scale"):
                _try_set_enum(rt, "resolution_scale", str(cfg["eevee_ray_resolution_scale"]))
            if hasattr(rt, "screen_trace_quality") and cfg.get("eevee_screen_trace_quality") is not None:
                rt.screen_trace_quality = float(cfg["eevee_screen_trace_quality"])
            if hasattr(rt, "screen_trace_thickness") and cfg.get("eevee_screen_trace_thickness") is not None:
                rt.screen_trace_thickness = float(cfg["eevee_screen_trace_thickness"])
            if hasattr(rt, "trace_max_roughness") and cfg.get("eevee_trace_max_roughness") is not None:
                rt.trace_max_roughness = float(cfg["eevee_trace_max_roughness"])
            if hasattr(rt, "use_denoise"):
                rt.use_denoise = bool(cfg.get("eevee_use_denoise", True))
            if hasattr(rt, "denoise_spatial"):
                rt.denoise_spatial = bool(cfg.get("eevee_denoise_spatial", True))
            if hasattr(rt, "denoise_temporal"):
                rt.denoise_temporal = bool(cfg.get("eevee_denoise_temporal", True))
        if hasattr(scene.render, "use_motion_blur"):
            scene.render.use_motion_blur = bool(cfg.get("use_motion_blur", False))
        if hasattr(scene.render, "motion_blur_shutter") and cfg.get("motion_blur_shutter") is not None:
            scene.render.motion_blur_shutter = float(cfg.get("motion_blur_shutter"))
    try:
        scene.view_settings.view_transform = str(cfg.get("view_transform", "AgX"))
        scene.view_settings.look = str(cfg.get("look", "Medium High Contrast"))
        scene.view_settings.exposure = float(cfg.get("exposure", 0.0))
    except Exception:
        pass
    world = scene.world or bpy.data.worlds.new("World")
    scene.world = world
    world.use_nodes = True
    nt = world.node_tree
    for node in list(nt.nodes):
        nt.nodes.remove(node)
    bg = nt.nodes.new("ShaderNodeBackground")
    out = nt.nodes.new("ShaderNodeOutputWorld")
    camera_bg = nt.nodes.new("ShaderNodeBackground")
    camera_bg.name = "CameraBG"
    camera_bg.label = "CameraBG"
    mix = nt.nodes.new("ShaderNodeMixShader")
    light_path = nt.nodes.new("ShaderNodeLightPath")
    camera_bg.inputs["Color"].default_value = _hex_to_rgba(cfg.get("camera_background_color", cfg.get("backdrop_color", "#EEE7DE")))
    camera_bg.inputs["Strength"].default_value = float(cfg.get("camera_background_strength", 1.0))
    if cfg.get("hdri_path") and Path(cfg["hdri_path"]).exists():
        env = nt.nodes.new("ShaderNodeTexEnvironment")
        env.image = _load_image(cfg["hdri_path"])
        mapping = nt.nodes.new("ShaderNodeMapping")
        tex_coord = nt.nodes.new("ShaderNodeTexCoord")
        links = nt.links
        mapping.inputs["Rotation"].default_value[2] = math.radians(float(cfg.get("hdri_rot_deg", 145.0)))
        bg.inputs["Strength"].default_value = float(cfg.get("hdri_strength", 0.22))
        bg.name = "WorldBG"
        bg.label = "WorldBG"
        links.new(tex_coord.outputs["Generated"], mapping.inputs["Vector"])
        links.new(mapping.outputs["Vector"], env.inputs["Vector"])
        links.new(env.outputs["Color"], bg.inputs["Color"])
    else:
        bg.name = "WorldBG"
        bg.label = "WorldBG"
        bg.inputs["Color"].default_value = _hex_to_rgba(cfg.get("world_bg", cfg.get("backdrop_color", "#EEE7DE")))
        bg.inputs["Strength"].default_value = float(cfg.get("world_strength", 0.2))
    nt.links.new(light_path.outputs["Is Camera Ray"], mix.inputs["Fac"])
    nt.links.new(camera_bg.outputs["Background"], mix.inputs[1])
    nt.links.new(bg.outputs["Background"], mix.inputs[2])
    nt.links.new(mix.outputs["Shader"], out.inputs["Surface"])
    return scene


def add_camera(cfg):
    bpy.ops.object.camera_add(location=tuple(cfg["camera_location"]))
    cam = bpy.context.object
    bpy.context.scene.camera = cam
    cam.data.type = "PERSP"
    cam.data.lens = float(cfg.get("camera_lens_mm", 58.0))
    cam.data.sensor_width = 36.0
    cam.data.sensor_fit = "HORIZONTAL"
    cam.data.clip_start = 0.01
    cam.data.clip_end = 100.0
    _look_at(cam, cfg["camera_target"])
    return cam


def add_area_light(name, location, target, *, energy, size, color):
    bpy.ops.object.light_add(type="AREA", location=location)
    light = bpy.context.object
    light.name = name
    light.data.energy = energy
    light.data.shape = "RECTANGLE"
    light.data.size = size[0]
    light.data.size_y = size[1]
    light.data.color = _hex_to_rgba(color)[:3]
    _look_at(light, target)
    return light


def import_phone_model(path: Path):
    before = set(bpy.data.objects)
    if path.suffix.lower() == ".glb":
        bpy.ops.import_scene.gltf(filepath=str(path))
    elif path.suffix.lower() == ".fbx":
        bpy.ops.import_scene.fbx(filepath=str(path))
    else:
        raise ValueError(f"Unsupported phone model: {path}")
    imported = [obj for obj in bpy.data.objects if obj not in before]
    cleanup_prefixes = (
        "Plane.013_",
        "Plane.014_",
    )
    cleaned = []
    for obj in imported:
        if any(obj.name.startswith(prefix) for prefix in cleanup_prefixes):
            bpy.data.objects.remove(obj, do_unlink=True)
            continue
        cleaned.append(obj)
    return cleaned


def create_phone_root(objects):
    root = bpy.data.objects.new("PhoneRoot", None)
    bpy.context.scene.collection.objects.link(root)
    root.empty_display_type = "PLAIN_AXES"
    for obj in objects:
        if obj.parent is None:
            obj.parent = root
    return root


def create_backdrop(cfg):
    rot = tuple(math.radians(v) for v in cfg.get("backdrop_rotation_deg", (0.0, 90.0, 0.0)))
    bpy.ops.mesh.primitive_plane_add(location=tuple(cfg["backdrop_location"]), rotation=rot)
    plane = bpy.context.object
    plane.name = "BackdropPlane"
    plane.scale = tuple(cfg["backdrop_scale"])
    plane.data.materials.append(
        make_backdrop_material(
            "MobileFeedBackdrop",
            cfg["backdrop_color"],
            emission_strength=float(cfg.get("backdrop_emission_strength", 0.34)),
            roughness=float(cfg.get("backdrop_roughness", 0.985)),
            bump_strength=float(cfg.get("backdrop_bump_strength", 0.004)),
        )
    )
    return plane


def create_overlay_plane(name, image_path: Path, *, location, scale):
    bpy.ops.mesh.primitive_plane_add(location=location, rotation=(0.0, math.radians(90.0), 0.0))
    plane = bpy.context.object
    plane.name = name
    plane.scale = scale
    plane.data.materials.append(make_overlay_material(f"{name}Mat", image_path))
    return plane


def create_floor_overlay_plane(name, image_path: Path, *, location, scale):
    bpy.ops.mesh.primitive_plane_add(location=location, rotation=(0.0, 0.0, 0.0))
    plane = bpy.context.object
    plane.name = name
    plane.scale = scale
    plane.data.materials.append(make_unlit_overlay_material(f"{name}Mat", image_path))
    return plane


def create_aligned_plane(name, image_path: Path, *, center, width, height, width_axis, height_axis, normal, parent=None):
    half_w = width * 0.5
    half_h = height * 0.5
    offset = normal * 0.0005
    verts_world = [
        center - width_axis * half_w - height_axis * half_h + offset,
        center + width_axis * half_w - height_axis * half_h + offset,
        center + width_axis * half_w + height_axis * half_h + offset,
        center - width_axis * half_w + height_axis * half_h + offset,
    ]
    if parent is not None:
        parent_inv = parent.matrix_world.inverted()
        verts = [parent_inv @ v for v in verts_world]
    else:
        verts = verts_world
    mesh = bpy.data.meshes.new(name)
    obj = bpy.data.objects.new(name, mesh)
    bpy.context.scene.collection.objects.link(obj)
    if parent is not None:
        obj.parent = parent
        obj.matrix_parent_inverse = Matrix.Identity(4)
    mesh.from_pydata(verts, [], [(0, 1, 2, 3)])
    mesh.update()
    uv_layer = mesh.uv_layers.new(name="UVMap")
    uvs = ((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0))
    for loop_idx, uv in enumerate(uvs):
        uv_layer.data[loop_idx].uv = uv
    obj.data.materials.append(make_unlit_overlay_material(f"{name}Mat", image_path))
    return obj


def apply_phone_materials(objects, screen_texture_path: Path):
    body = make_principled_material(
        "PhoneBody",
        base_color="#2D333B",
        metallic=0.84,
        roughness=0.18,
        specular=0.48,
        coat=0.14,
    )
    black = make_principled_material(
        "PhoneBlack",
        base_color="#010203",
        metallic=0.0,
        roughness=0.10,
        specular=0.22,
        coat=0.14,
    )
    metal = make_principled_material(
        "PhoneMetal",
        base_color="#B0B8C2",
        metallic=1.0,
        roughness=0.15,
        specular=0.78,
        coat=0.0,
        anisotropy=0.42,
        anisotropy_rotation=0.18,
    )
    logo = make_principled_material(
        "PhoneLogo",
        base_color="#AEB4BC",
        metallic=0.6,
        roughness=0.14,
        specular=0.38,
        coat=0.0,
    )
    lens = make_principled_material(
        "PhoneLens",
        base_color="#12151B",
        metallic=0.2,
        roughness=0.08,
        specular=0.56,
        coat=0.20,
    )
    glass = make_glass_material("PhoneGlass")
    screen = make_screen_material("PhoneScreen", screen_texture_path)

    screen_obj = None
    for obj in objects:
        if obj.type != "MESH":
            continue
        lowered = obj.name.lower()
        if "screen" in lowered:
            obj.data.materials.clear()
            obj.data.materials.append(screen)
            screen_obj = obj
            continue
        if "glass" in lowered:
            obj.data.materials.clear()
            obj.data.materials.append(glass)
            continue
        if "metalframe" in lowered or "metall" in lowered or "metal" in lowered:
            obj.data.materials.clear()
            obj.data.materials.append(metal)
            continue
        if "logo" in lowered:
            obj.data.materials.clear()
            obj.data.materials.append(logo)
            continue
        if "lensinglass" in lowered or "lens" in lowered:
            obj.data.materials.clear()
            obj.data.materials.append(lens)
            continue
        if "basecolor" in lowered or "gray" in lowered:
            obj.data.materials.clear()
            obj.data.materials.append(body)
            continue
        obj.data.materials.clear()
        obj.data.materials.append(black)
    if screen_obj is None:
        raise RuntimeError("Phone screen object not found in imported model")
    return screen_obj


def build_ribbon_mesh(root, screen_obj, atlas_path: Path, meta, cfg, phone_objects=None):
    atlas_aspect = float(meta["atlas_width"]) / max(1.0, float(meta["atlas_height"]))
    focus_aspect = float(meta["focus_aspect"])
    focus_width_u = float(meta["focus_width_u"])
    focus_center_u = float(meta["focus_center_u"])
    focus_start_u = focus_center_u - focus_width_u * 0.5
    focus_end_u = focus_center_u + focus_width_u * 0.5
    basis = _screen_basis(screen_obj, view_hint=cfg.get("camera_location"), camera_target=cfg.get("camera_target"))
    screen_width = float(basis["width"])
    screen_center = basis["center"]
    width_axis = basis["width_axis"]
    height_axis = basis["height_axis"]
    normal = basis["normal"]
    phone_bounds = None
    if phone_objects:
        phone_bounds = _projected_bounds(phone_objects, screen_center, width_axis, height_axis, normal)
    if phone_bounds:
        focus_target_width = max(1e-6, float(phone_bounds["max_u"] - phone_bounds["min_u"]))
        focus_center_offset_u = float((phone_bounds["min_u"] + phone_bounds["max_u"]) * 0.5)
        focus_left_u = float(phone_bounds["min_u"])
        focus_right_u = float(phone_bounds["max_u"])
    else:
        focus_target_width = screen_width
        focus_center_offset_u = 0.0
        focus_left_u = -screen_width * 0.5
        focus_right_u = screen_width * 0.5
    ribbon_height = focus_target_width / max(0.001, focus_aspect)
    ribbon_width = ribbon_height * atlas_aspect
    desk_up = Vector((0.0, 0.0, 1.0))
    desk_z = float(cfg.get("desk_z", -0.02))
    desk_gap = float(cfg.get("ribbon_desk_gap", 0.005))
    screen_gap = float(cfg.get("ribbon_screen_gap", 0.010))
    desk_dir = Vector((width_axis.x, width_axis.y, 0.0))
    if desk_dir.length < 1e-5:
        desk_dir = width_axis.copy()
    desk_dir.normalize()
    if width_axis.dot(desk_dir) < 0:
        desk_dir.negate()
    desk_cross = Vector((-desk_dir.y, desk_dir.x, 0.0))
    if desk_cross.length < 1e-6:
        desk_cross = height_axis.copy()
    else:
        desk_cross.normalize()
    if desk_cross.dot(height_axis) < 0:
        desk_cross.negate()
    left_edge = screen_center + width_axis * focus_left_u + normal * screen_gap
    right_edge = screen_center + width_axis * focus_right_u + normal * screen_gap
    left_tail_len = ribbon_width * focus_start_u
    right_tail_len = ribbon_width * (1.0 - focus_end_u)
    panels = list(meta.get("panels", []) or [])
    focus_event_id = meta.get("focus_event_id")
    focus_index = next((idx for idx, panel in enumerate(panels) if panel.get("event_id") == focus_event_id), None)
    left_neighbor_share = 1.0
    right_neighbor_share = 1.0
    if focus_index is not None and focus_index > 0 and focus_start_u > 1e-6:
        left_neighbor_share = min(
            1.0,
            (float(panels[focus_index - 1]["width"]) / max(1.0, float(meta["atlas_width"]))) / focus_start_u,
        )
    if focus_index is not None and focus_index + 1 < len(panels) and (1.0 - focus_end_u) > 1e-6:
        right_neighbor_share = min(
            1.0,
            (float(panels[focus_index + 1]["width"]) / max(1.0, float(meta["atlas_width"]))) / (1.0 - focus_end_u),
        )
    far_left = left_edge - desk_dir * left_tail_len
    far_right = right_edge + desk_dir * right_tail_len
    far_left.z = desk_z + desk_gap
    far_right.z = desk_z + desk_gap
    cols = int(cfg.get("ribbon_cols", 160))
    rows = int(cfg.get("ribbon_rows", 8))
    mesh = bpy.data.meshes.new("MobileFeedRibbon")
    obj = bpy.data.objects.new("MobileFeedRibbon", mesh)
    bpy.context.scene.collection.objects.link(obj)
    if root is not None:
        obj.parent = root
    obj.rotation_euler = (0.0, 0.0, 0.0)
    verts = []
    faces = []
    camber = float(cfg.get("ribbon_camber", 0.006))
    min_z = desk_z + desk_gap
    left_sag = float(cfg.get("ribbon_left_sag", 0.06))
    right_sag = float(cfg.get("ribbon_right_sag", 0.06))
    phone_margin_u = float(cfg.get("phone_clearance_margin_u", 0.05))
    phone_margin_v = float(cfg.get("phone_clearance_margin_v", 0.06))
    phone_clearance_n = float(cfg.get("phone_clearance_n", 0.010))

    def lift_above_phone(pos: Vector):
        if not phone_bounds:
            return pos
        rel = pos - screen_center
        u = rel.dot(width_axis)
        v = rel.dot(height_axis)
        n = rel.dot(normal)
        if (
            phone_bounds["min_u"] - phone_margin_u
            <= u
            <= phone_bounds["max_u"] + phone_margin_u
            and phone_bounds["min_v"] - phone_margin_v
            <= v
            <= phone_bounds["max_v"] + phone_margin_v
        ):
            min_n = phone_bounds["max_n"] + phone_clearance_n
            if n < min_n:
                pos = pos + normal * (min_n - n)
        return pos

    rise_height = float(cfg.get("ribbon_rise_height", 0.028))
    edge_arc_up = max(rise_height * 1.30, 0.024)
    edge_arc_n = max(phone_clearance_n * 0.62, 0.012)
    left_bend_start = max(0.48, 1.0 - left_neighbor_share * 0.56)
    right_bend_end = min(0.52, max(0.22, right_neighbor_share * 1.18))
    left_bend_anchor = far_left.lerp(left_edge, left_bend_start)
    right_bend_anchor = right_edge.lerp(far_right, right_bend_end)
    left_bend_anchor.z = min_z
    right_bend_anchor.z = min_z

    def centerline_position(u: float):
        if u <= focus_start_u:
            t = 0.0 if focus_start_u <= 1e-6 else u / focus_start_u
            if t <= left_bend_start:
                pos = far_left.lerp(left_bend_anchor, _smoothstep(t / max(1e-6, left_bend_start)))
                pos.z = min_z
                return pos, 0.0
            tt = (t - left_bend_start) / max(1e-6, 1.0 - left_bend_start)
            pos = _hermite(
                left_bend_anchor,
                left_edge,
                desk_dir * max(left_tail_len * 0.28, focus_target_width * 0.18) + desk_up * (edge_arc_up * 0.35),
                desk_dir * max(focus_target_width * 0.22, 0.08) + normal * (edge_arc_n * 0.18),
                tt,
            )
            pos += normal * (edge_arc_n * math.sin(math.pi * tt * 0.5) ** 1.05)
            pos += desk_up * (edge_arc_up * math.sin(math.pi * tt * 0.5) ** 1.18)
            pos.z -= left_sag * 0.06 * math.sin(math.pi * tt) ** 1.2
            pos.z = max(pos.z, min_z)
            return lift_above_phone(pos), _smoothstep(tt)
        if u >= focus_end_u:
            t = 0.0 if (1.0 - focus_end_u) <= 1e-6 else (u - focus_end_u) / (1.0 - focus_end_u)
            if t <= right_bend_end:
                tt = t / max(1e-6, right_bend_end)
                pos = _hermite(
                    right_edge,
                    right_bend_anchor,
                    desk_dir * max(focus_target_width * 0.22, 0.08) + normal * (edge_arc_n * 0.18),
                    desk_dir * max(right_tail_len * 0.28, focus_target_width * 0.18) + desk_up * (edge_arc_up * 0.35),
                    tt,
                )
                pos += normal * (edge_arc_n * math.sin(math.pi * (1.0 - tt) * 0.5) ** 1.05)
                pos += desk_up * (edge_arc_up * math.sin(math.pi * (1.0 - tt) * 0.5) ** 1.18)
                pos.z -= right_sag * 0.06 * math.sin(math.pi * tt) ** 1.2
                pos.z = max(pos.z, min_z)
                return lift_above_phone(pos), 1.0 - _smoothstep(tt)
            tt = (t - right_bend_end) / max(1e-6, 1.0 - right_bend_end)
            pos = right_bend_anchor.lerp(far_right, _smoothstep(tt))
            pos.z = min_z
            pos.z = max(pos.z, min_z)
            return pos, 0.0
        local = (u - focus_start_u) / max(1e-6, focus_width_u)
        x = focus_center_offset_u + (local - 0.5) * focus_target_width
        pos = screen_center + width_axis * x + normal * screen_gap
        pos += normal * float(cfg.get("ribbon_screen_cushion", 0.002)) * math.cos((local - 0.5) * math.pi)
        return lift_above_phone(pos), 1.0

    for iy in range(rows + 1):
        v = iy / rows
        local_v = 0.5 - v
        for ix in range(cols + 1):
            u = ix / cols
            center, screen_blend = centerline_position(u)
            left_point, _ = centerline_position(max(0.0, u - 1.0 / cols))
            right_point, _ = centerline_position(min(1.0, u + 1.0 / cols))
            tangent = (right_point - left_point).normalized()
            cross_mix = _smoothstep(min(1.0, screen_blend * 1.18))
            cross_axis = desk_cross * (1.0 - cross_mix) + height_axis * cross_mix
            if cross_axis.length < 1e-6:
                cross_axis = height_axis.copy()
            cross_axis.normalize()
            normal_axis = tangent.cross(cross_axis)
            if normal_axis.length < 1e-6:
                normal_axis = normal.copy()
            else:
                normal_axis.normalize()
            if normal_axis.dot(normal) < 0:
                normal_axis.negate()
            fold_profile = (1.0 - (abs(local_v) / 0.5) ** 1.9) if abs(local_v) < 0.5 else 0.0
            pos = center + cross_axis * (local_v * ribbon_height)
            pos += normal_axis * camber * fold_profile
            pos.z = max(pos.z, min_z)
            pos = lift_above_phone(pos)
            verts.append(tuple(pos))
    stride = cols + 1
    for iy in range(rows):
        for ix in range(cols):
            a = iy * stride + ix
            b = a + 1
            c = a + stride + 1
            d = a + stride
            faces.append((a, b, c, d))
    mesh.from_pydata(verts, [], faces)
    mesh.update()
    uv_layer = mesh.uv_layers.new(name="RibbonUV")
    for poly in mesh.polygons:
        for loop_idx in poly.loop_indices:
            vert_idx = mesh.loops[loop_idx].vertex_index
            iy = vert_idx // stride
            ix = vert_idx % stride
            u = ix / cols
            v = iy / rows
            uv_layer.data[loop_idx].uv = (u, 1.0 - v)
    solidify = obj.modifiers.new(name="Solidify", type="SOLIDIFY")
    solidify.thickness = float(cfg.get("ribbon_thickness", 0.005))
    solidify.offset = 1.0
    subsurf = obj.modifiers.new(name="Subsurf", type="SUBSURF")
    subsurf.levels = int(cfg.get("ribbon_subdiv", 2))
    subsurf.render_levels = int(cfg.get("ribbon_subdiv", 2))
    obj.data.materials.append(make_ribbon_material("MobileFeedRibbonMaterial", atlas_path))
    if hasattr(obj, "visible_shadow"):
        obj.visible_shadow = True
    bpy.context.view_layer.objects.active = obj
    bpy.ops.object.shade_smooth()
    return obj


def create_screen_label_planes(root, screen_obj, cfg):
    basis = _screen_basis(screen_obj, view_hint=cfg.get("camera_location"), camera_target=cfg.get("camera_target"))
    normal_offset = float(cfg.get("screen_label_normal_offset", 0.014))
    width = basis["width"] * float(cfg.get("screen_label_width_ratio", 0.76))
    phone_parent = screen_obj.parent
    phone_objects = []
    if phone_parent is not None:
        phone_objects = [
            obj
            for obj in bpy.context.scene.objects
            if getattr(obj, "type", None) == "MESH" and obj.parent == phone_parent
        ]
    phone_bounds = _projected_bounds(phone_objects, basis["center"], basis["width_axis"], basis["height_axis"], basis["normal"]) if phone_objects else None
    focus_aspect = float(cfg.get("ribbon_meta", {}).get("focus_aspect", 0.78))
    focus_target_width = float(phone_bounds["max_u"] - phone_bounds["min_u"]) if phone_bounds else float(basis["width"])
    ribbon_height = focus_target_width / max(0.001, focus_aspect)
    clear_band = max(0.001, (basis["height"] - ribbon_height) * 0.5)
    top_height = min(basis["height"] * float(cfg.get("screen_top_label_height_ratio", 0.12)), clear_band * 0.78)
    bottom_height = min(basis["height"] * float(cfg.get("screen_bottom_label_height_ratio", 0.10)), clear_band * 0.58)
    edge_inset = max(0.012, basis["height"] * 0.026)
    notch_safe = max(edge_inset, basis["height"] * float(cfg.get("screen_notch_safe_ratio", 0.10)))
    top_x_offset = basis["width_axis"] * (basis["width"] * float(cfg.get("screen_top_label_x_offset_ratio", -0.08)))
    bottom_x_offset = basis["width_axis"] * (basis["width"] * float(cfg.get("screen_bottom_label_x_offset_ratio", -0.03)))
    top_center = (
        basis["center"]
        + top_x_offset
        + basis["height_axis"] * (basis["height"] * 0.5 - notch_safe - top_height * 0.5)
        + basis["height_axis"] * (basis["height"] * float(cfg.get("screen_top_label_lift_ratio", 0.0)))
        + basis["normal"] * normal_offset
    )
    bottom_center = (
        basis["center"]
        + bottom_x_offset
        - basis["height_axis"] * (basis["height"] * 0.5 - edge_inset - bottom_height * 0.5)
        + basis["height_axis"] * (basis["height"] * float(cfg.get("screen_bottom_label_lift_ratio", 0.0)))
        + basis["normal"] * normal_offset
    )
    top_path = cfg.get("screen_top_label_texture")
    bottom_path = cfg.get("screen_bottom_label_texture")
    created = []
    if top_path and Path(top_path).exists():
        created.append(
            create_aligned_plane(
            "ScreenTopLabel",
            Path(top_path),
            center=top_center,
            width=width,
            height=top_height,
            width_axis=basis["width_axis"],
            height_axis=basis["height_axis"],
            normal=basis["normal"],
            parent=screen_obj,
            )
        )
    if bottom_path and Path(bottom_path).exists():
        created.append(
            create_aligned_plane(
            "ScreenBottomLabel",
            Path(bottom_path),
            center=bottom_center,
            width=width * 0.96,
            height=bottom_height,
            width_axis=basis["width_axis"],
            height_axis=basis["height_axis"],
            normal=basis["normal"],
            parent=screen_obj,
            )
        )
    return created


def _set_auto_clamped_curves(id_data):
    if not id_data.animation_data or not id_data.animation_data.action:
        return
    for fcurve in id_data.animation_data.action.fcurves:
        for point in fcurve.keyframe_points:
            point.interpolation = "BEZIER"
            point.handle_left_type = "AUTO_CLAMPED"
            point.handle_right_type = "AUTO_CLAMPED"


def _set_linear_curves(id_data):
    if not id_data.animation_data or not id_data.animation_data.action:
        return
    for fcurve in id_data.animation_data.action.fcurves:
        for point in fcurve.keyframe_points:
            point.interpolation = "LINEAR"


def _value_add(a, b):
    return a + b


def _value_sub(a, b):
    return a - b


def _value_scale(value, scalar: float):
    return value * scalar


def _normalize_if_vector(value):
    if hasattr(value, "length"):
        if value.length > 1e-8:
            value.normalize()
    return value


def _smootherstep(t: float) -> float:
    t = max(0.0, min(1.0, t))
    return t * t * t * (t * (t * 6.0 - 15.0) + 10.0)


def _ease_in_out_cubic_value(t: float) -> float:
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return 4.0 * t * t * t
    return 1.0 - ((-2.0 * t + 2.0) ** 3) / 2.0


def _luxury_progress(t: float) -> float:
    t = max(0.0, min(1.0, t))
    # Keep the move premium and eased, but do not let the tail velocity die to
    # zero before the 2D handoff. A small linear component removes the visible
    # micro-plateau that made the approach feel like it was stopping.
    return (
        0.18 * t
        + 0.62 * _smootherstep(t)
        + 0.20 * (1.0 - (1.0 - t) ** 3)
    )


def _apply_timing_warp(t: float, control_points) -> float:
    t = max(0.0, min(1.0, t))
    if not control_points:
        return t
    points = []
    for point in control_points:
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            continue
        src = max(0.0, min(1.0, float(point[0])))
        dst = max(0.0, min(1.0, float(point[1])))
        points.append((src, dst))
    if not points:
        return t
    points.sort(key=lambda item: item[0])
    if points[0][0] > 0.0:
        points.insert(0, (0.0, 0.0))
    if points[-1][0] < 1.0:
        points.append((1.0, 1.0))
    for idx in range(len(points) - 1):
        x0, y0 = points[idx]
        x1, y1 = points[idx + 1]
        if t <= x1:
            if x1 <= x0:
                return y1
            u = (t - x0) / (x1 - x0)
            return y0 + (y1 - y0) * u
    return points[-1][1]


def _bezier_point(values, t: float):
    current = [value.copy() if hasattr(value, "copy") else value for value in values]
    t = max(0.0, min(1.0, t))
    while len(current) > 1:
        next_values = []
        for idx in range(len(current) - 1):
            a = current[idx]
            b = current[idx + 1]
            next_values.append(_value_add(_value_scale(a, 1.0 - t), _value_scale(b, t)))
        current = next_values
    return current[0]


def _hermite_sample(times, values, sample_time: float, tension: float = 0.42):
    if sample_time <= times[0]:
        return values[0].copy() if hasattr(values[0], "copy") else values[0]
    if sample_time >= times[-1]:
        return values[-1].copy() if hasattr(values[-1], "copy") else values[-1]

    tangents = []
    for idx, value in enumerate(values):
        if idx == 0:
            dt = max(1.0, times[1] - times[0])
            tangent = _value_scale(_value_sub(values[1], values[0]), (1.0 - tension) / dt)
        elif idx == len(values) - 1:
            dt = max(1.0, times[-1] - times[-2])
            tangent = _value_scale(_value_sub(values[-1], values[-2]), (1.0 - tension) / dt)
        else:
            dt = max(1.0, times[idx + 1] - times[idx - 1])
            tangent = _value_scale(_value_sub(values[idx + 1], values[idx - 1]), (1.0 - tension) / dt)
        if len(values) >= 5:
            if idx == 1:
                tangent = _value_scale(tangent, 1.85)
            elif idx == 2:
                tangent = _value_scale(tangent, 1.30)
            elif idx == len(values) - 2:
                tangent = _value_scale(tangent, 1.10)
        tangents.append(tangent)

    seg_idx = 0
    for idx in range(len(times) - 1):
        if times[idx] <= sample_time <= times[idx + 1]:
            seg_idx = idx
            break

    t0 = times[seg_idx]
    t1 = times[seg_idx + 1]
    p0 = values[seg_idx]
    p1 = values[seg_idx + 1]
    m0 = tangents[seg_idx]
    m1 = tangents[seg_idx + 1]
    dt = max(1.0, t1 - t0)
    u = (sample_time - t0) / dt
    u2 = u * u
    u3 = u2 * u
    h00 = 2.0 * u3 - 3.0 * u2 + 1.0
    h10 = u3 - 2.0 * u2 + u
    h01 = -2.0 * u3 + 3.0 * u2
    h11 = u3 - u2
    value = _value_add(
        _value_add(_value_scale(p0, h00), _value_scale(m0, h10 * dt)),
        _value_add(_value_scale(p1, h01), _value_scale(m1, h11 * dt)),
    )
    return value


def setup_label_exit_animation(label_objects, screen_obj, cfg):
    if not label_objects:
        return
    anim = cfg.get("animation")
    if not anim:
        return
    frame_start = int(anim.get("frame_start", 1))
    label_keys = {
        "label_exit_start",
        "label_exit_end",
        "screen_top_label_exit_start",
        "screen_top_label_exit_end",
        "screen_bottom_label_exit_start",
        "screen_bottom_label_exit_end",
    }
    if not any(key in anim for key in label_keys):
        return
    exit_start = int(anim.get("label_exit_start", frame_start + 30))
    exit_end = int(anim.get("label_exit_end", exit_start + 4))

    for obj in label_objects:
        if "Top" in obj.name:
            obj_exit_start = int(anim.get("screen_top_label_exit_start", exit_start))
            obj_exit_end = int(anim.get("screen_top_label_exit_end", exit_end))
        elif "Bottom" in obj.name:
            obj_exit_start = int(anim.get("screen_bottom_label_exit_start", exit_start))
            obj_exit_end = int(anim.get("screen_bottom_label_exit_end", exit_end))
        else:
            obj_exit_start = exit_start
            obj_exit_end = exit_end
        for material in obj.data.materials:
            if material is None or not material.use_nodes or material.node_tree is None:
                continue
            opacity = material.node_tree.nodes.get("OpacityControl")
            if opacity is None:
                continue
            socket = opacity.outputs[0]
            socket.default_value = 1.0
            socket.keyframe_insert(data_path="default_value", frame=frame_start)
            socket.keyframe_insert(data_path="default_value", frame=obj_exit_start)
            socket.default_value = 0.0
            socket.keyframe_insert(data_path="default_value", frame=obj_exit_end)
            _set_auto_clamped_curves(material.node_tree)


def setup_tonal_transition_animation(scene, backdrop_obj, cfg, light_objects=None):
    anim = cfg.get("animation")
    if not anim:
        return
    start_frame = int(anim.get("tonal_transition_start_frame", 0))
    end_frame = int(anim.get("tonal_transition_end_frame", 0))
    if start_frame <= 0 or end_frame <= start_frame:
        return

    start_color = _hex_to_rgba(anim.get("tonal_transition_from", cfg.get("backdrop_color", "#E7E6E1")))
    end_color = _hex_to_rgba(anim.get("tonal_transition_to", "#06080C"))
    world = scene.world
    if world and world.use_nodes and world.node_tree:
        nodes = world.node_tree.nodes
        camera_bg = nodes.get("CameraBG")
        if camera_bg is not None:
            socket = camera_bg.inputs["Color"]
            socket.default_value = start_color
            socket.keyframe_insert(data_path="default_value", frame=start_frame - 1)
            socket.keyframe_insert(data_path="default_value", frame=start_frame)
            socket.default_value = end_color
            socket.keyframe_insert(data_path="default_value", frame=end_frame)
            _set_auto_clamped_curves(world.node_tree)

    if backdrop_obj and backdrop_obj.data.materials:
        backdrop_mat = backdrop_obj.data.materials[0]
        if backdrop_mat and backdrop_mat.use_nodes and backdrop_mat.node_tree:
            nodes = backdrop_mat.node_tree.nodes
            bsdf = nodes.get("BackdropBSDF")
            emission = nodes.get("BackdropEmission")
            if bsdf is not None:
                socket = bsdf.inputs["Base Color"]
                socket.default_value = start_color
                socket.keyframe_insert(data_path="default_value", frame=start_frame - 1)
                socket.keyframe_insert(data_path="default_value", frame=start_frame)
                socket.default_value = end_color
                socket.keyframe_insert(data_path="default_value", frame=end_frame)
            if emission is not None:
                color_socket = emission.inputs["Color"]
                strength_socket = emission.inputs["Strength"]
                color_socket.default_value = start_color
                color_socket.keyframe_insert(data_path="default_value", frame=start_frame - 1)
                color_socket.keyframe_insert(data_path="default_value", frame=start_frame)
                color_socket.default_value = end_color
                color_socket.keyframe_insert(data_path="default_value", frame=end_frame)
                start_strength = float(cfg.get("backdrop_emission_strength", 0.0))
                end_strength = float(anim.get("tonal_transition_emission_strength_end", 0.0))
                strength_socket.default_value = start_strength
                strength_socket.keyframe_insert(data_path="default_value", frame=start_frame - 1)
                strength_socket.keyframe_insert(data_path="default_value", frame=start_frame)
                strength_socket.default_value = end_strength
                strength_socket.keyframe_insert(data_path="default_value", frame=end_frame)
            _set_auto_clamped_curves(backdrop_mat.node_tree)

    if light_objects:
        for light_obj, multiplier in light_objects:
            if light_obj is None:
                continue
            light_data = light_obj.data
            base_energy = float(light_data.energy)
            light_data.energy = base_energy
            light_data.keyframe_insert(data_path="energy", frame=start_frame - 1)
            light_data.keyframe_insert(data_path="energy", frame=start_frame)
            light_data.energy = base_energy * multiplier
            light_data.keyframe_insert(data_path="energy", frame=end_frame)
            _set_auto_clamped_curves(light_data)


def setup_handoff_animation(scene, cam, screen_obj, cfg):
    anim = cfg.get("animation")
    if not anim:
        return
    fps = int(anim.get("fps", 30))
    frame_step = int(anim.get("frame_step", 1))
    frame_start = int(anim.get("frame_start", 1))
    frame_end = int(anim.get("frame_end", 45))
    keyframe_start_frame = int(anim.get("keyframe_start_frame", frame_start))
    combo_mid_frame = int(anim.get("combo_mid_frame", frame_start + 10))
    sync_start_frame = int(anim.get("sync_start_frame", combo_mid_frame + 10))
    sync_mid_frame = int(anim.get("sync_mid_frame", frame_end - 10))
    scene.render.fps = fps
    scene.frame_start = frame_start
    scene.frame_end = frame_end
    scene.frame_step = max(1, frame_step)
    basis = _screen_basis(screen_obj, view_hint=cfg.get("camera_location"), camera_target=cfg.get("camera_target"))
    focus_center = basis["center"] + basis["normal"] * float(anim.get("focus_target_normal_offset", 0.012))
    start_target = Vector(cfg["camera_target"])

    start_loc = Vector(cfg["camera_location"])
    start_lens = float(cfg.get("camera_lens_mm", 58.0))
    combo_lens = float(anim.get("combo_lens_mm", start_lens + 8.0))
    sync_start_lens = float(anim.get("sync_start_lens_mm", combo_lens + 10.0))
    sync_mid_lens = float(anim.get("sync_mid_lens_mm", sync_start_lens + 4.0))
    end_lens = float(anim.get("end_lens_mm", sync_mid_lens + 2.0))

    def distance_for_fill(fill_ratio: float, lens_mm: float):
        h_fov = 2.0 * math.atan(cam.data.sensor_width / (2.0 * lens_mm))
        return (basis["width"] * 0.5) / max(1e-6, math.tan(h_fov * 0.5) * max(1e-6, fill_ratio))

    combo_fill = float(anim.get("combo_fill", 0.58))
    sync_start_fill = float(anim.get("sync_start_fill", 0.90))
    sync_mid_fill = float(anim.get("sync_mid_fill", 0.97))
    end_fill = float(anim.get("scene1_end_scale", 1.0))

    start_fill = float(anim.get("start_fill", 0.26))

    combo_loc = focus_center + basis["normal"] * distance_for_fill(combo_fill, combo_lens)
    combo_loc += basis["height_axis"] * float(anim.get("combo_height_offset", 0.10))
    combo_loc += basis["width_axis"] * float(anim.get("combo_side_offset", 0.020))

    sync_start_loc = focus_center + basis["normal"] * distance_for_fill(sync_start_fill, sync_start_lens)
    sync_start_loc += basis["height_axis"] * float(anim.get("sync_start_height_offset", 0.030))
    sync_start_loc += basis["width_axis"] * float(anim.get("sync_start_side_offset", 0.006))

    sync_mid_loc = focus_center + basis["normal"] * distance_for_fill(sync_mid_fill, sync_mid_lens)
    sync_mid_loc += basis["height_axis"] * float(anim.get("sync_mid_height_offset", 0.008))
    sync_mid_loc += basis["width_axis"] * float(anim.get("sync_mid_side_offset", 0.001))

    end_loc = focus_center + basis["normal"] * distance_for_fill(end_fill, end_lens)
    end_loc += basis["height_axis"] * float(anim.get("end_height_offset", 0.0))
    end_loc += basis["width_axis"] * float(anim.get("end_side_offset", 0.0))

    start_up = Vector((0.0, 0.0, 1.0)).lerp(basis["height_axis"], float(anim.get("start_up_blend", 0.22)))
    combo_up = Vector((0.0, 0.0, 1.0)).lerp(basis["height_axis"], float(anim.get("combo_up_blend", 0.72)))
    sync_up = Vector((0.0, 0.0, 1.0)).lerp(basis["height_axis"], float(anim.get("sync_up_blend", 0.96)))
    end_up = basis["height_axis"]

    lens_bezier = [start_lens, combo_lens, sync_start_lens, end_lens]
    up_bezier = [start_up, combo_up, sync_up, end_up]
    target_bezier = [
        start_target,
        start_target.lerp(focus_center, 0.36),
        start_target.lerp(focus_center, 0.82),
        focus_center,
    ]
    start_anchor = focus_center + basis["normal"] * distance_for_fill(start_fill, start_lens)
    start_height = (start_loc - start_anchor).dot(basis["height_axis"])
    start_side = (start_loc - start_anchor).dot(basis["width_axis"])
    loc_bezier = [
        start_loc,
        combo_loc,
        sync_start_loc.lerp(sync_mid_loc, 0.32),
        end_loc,
    ]
    height_bezier = [
        start_height,
        float(anim.get("combo_height_offset", 0.10)),
        float(anim.get("sync_start_height_offset", 0.030)),
        float(anim.get("end_height_offset", 0.0)),
    ]
    side_bezier = [
        start_side,
        float(anim.get("combo_side_offset", 0.020)),
        float(anim.get("sync_start_side_offset", 0.006)),
        float(anim.get("end_side_offset", 0.0)),
    ]

    total_frames = max(1, frame_end - keyframe_start_frame)
    timing_warp_points = anim.get("timing_warp_control_points") or []

    for frame_num in range(keyframe_start_frame, frame_end + 1):
        linear_u = (frame_num - keyframe_start_frame) / total_frames
        warped_u = _apply_timing_warp(linear_u, timing_warp_points)
        move_u = _luxury_progress(warped_u)
        lens_value = float(_bezier_point(lens_bezier, move_u))
        loc_value = _bezier_point(loc_bezier, move_u)
        # Keep a subtle residual height/side cleanup tied to the same progress
        # curve, so the camera never appears to switch between competing moves.
        height_value = float(_bezier_point(height_bezier, move_u))
        side_value = float(_bezier_point(side_bezier, move_u))
        loc_anchor = focus_center + basis["normal"] * max(
            0.0, (loc_value - focus_center).dot(basis["normal"])
        )
        loc_value = loc_anchor + basis["height_axis"] * height_value + basis["width_axis"] * side_value
        up_value = _bezier_point(up_bezier, move_u)
        target_value = _bezier_point(target_bezier, move_u)
        up_value = _normalize_if_vector(up_value)

        cam.location = loc_value
        cam.rotation_euler = _camera_rotation(loc_value, target_value, up_value)
        cam.keyframe_insert(data_path="location", frame=frame_num)
        cam.keyframe_insert(data_path="rotation_euler", frame=frame_num)
        cam.data.lens = lens_value
        cam.data.keyframe_insert(data_path="lens", frame=frame_num)

    _set_linear_curves(cam)
    _set_linear_curves(cam.data)


def main():
    args = _parse_args()
    cfg_path = args.get("config")
    if not cfg_path:
        raise SystemExit("Expected --config path")
    cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
    setup_scene(cfg)
    backdrop = create_backdrop(cfg)
    if cfg.get("scene_overlay_in_blender", True):
        create_overlay_plane(
            "BackgroundOverlay",
            Path(cfg["overlay_texture"]),
            location=tuple(cfg["overlay_location"]),
            scale=tuple(cfg["overlay_scale"]),
        )
    cam = add_camera(cfg)
    key_light = add_area_light(
        "KeyLight",
        tuple(cfg["key_light_location"]),
        tuple(cfg["key_light_target"]),
        energy=float(cfg["key_light_energy"]),
        size=tuple(cfg["key_light_size"]),
        color=cfg["key_light_color"],
    )
    fill_light = add_area_light(
        "FillLight",
        tuple(cfg["fill_light_location"]),
        tuple(cfg["fill_light_target"]),
        energy=float(cfg["fill_light_energy"]),
        size=tuple(cfg["fill_light_size"]),
        color=cfg["fill_light_color"],
    )
    top_light = add_area_light(
        "TopLight",
        tuple(cfg["top_light_location"]),
        tuple(cfg["top_light_target"]),
        energy=float(cfg["top_light_energy"]),
        size=tuple(cfg["top_light_size"]),
        color=cfg["top_light_color"],
    )
    edge_light = None
    if cfg.get("edge_light_location") and cfg.get("edge_light_target"):
        edge_light = add_area_light(
            "EdgeLight",
            tuple(cfg["edge_light_location"]),
            tuple(cfg["edge_light_target"]),
            energy=float(cfg.get("edge_light_energy", 0.0)),
            size=tuple(cfg.get("edge_light_size", (2.0, 2.0))),
            color=cfg.get("edge_light_color", "#FFFFFF"),
        )

    phone_path = Path(cfg["phone_model"])
    imported = import_phone_model(phone_path)
    root = create_phone_root(imported)
    screen_obj = apply_phone_materials(imported, Path(cfg["screen_texture"]))

    root.location = Vector(cfg["phone_location"])
    root.rotation_euler = tuple(math.radians(v) for v in cfg["phone_rotation_deg"])
    root.scale = tuple(cfg["phone_scale"])
    bpy.context.view_layer.update()

    build_ribbon_mesh(None, screen_obj, Path(cfg["ribbon_texture"]), cfg["ribbon_meta"], cfg, phone_objects=imported)
    label_objects = create_screen_label_planes(None, screen_obj, cfg)

    if cfg.get("use_shadow_plane", False):
        bbox = _world_bbox(root)
        shadow_plane = create_floor_overlay_plane(
            "PhoneShadow",
            Path(cfg["shadow_texture"]),
            location=(
                bbox["center"].x + float(cfg.get("shadow_offset_x", 0.0)),
                bbox["center"].y + float(cfg.get("shadow_offset_y", 0.0)),
                bbox["min_z"] + float(cfg.get("shadow_z_offset", 0.0008)),
            ),
            scale=(float(cfg["shadow_scale_x"]), float(cfg["shadow_scale_y"]), 1.0),
        )
        shadow_plane.rotation_euler = (
            0.0,
            0.0,
            math.radians(float(cfg.get("shadow_rotation_deg", 0.0))),
        )

    setup_handoff_animation(bpy.context.scene, cam, screen_obj, cfg)
    setup_label_exit_animation(label_objects, screen_obj, cfg)
    setup_tonal_transition_animation(
        bpy.context.scene,
        backdrop,
        cfg,
        light_objects=[
            (key_light, float(cfg.get("tonal_transition_key_multiplier", 0.74))),
            (fill_light, float(cfg.get("tonal_transition_fill_multiplier", 0.10))),
            (top_light, float(cfg.get("tonal_transition_top_multiplier", 0.22))),
            (edge_light, float(cfg.get("tonal_transition_edge_multiplier", 1.08))),
        ],
    )

    if cfg.get("render_animation"):
        bpy.context.scene.render.filepath = str(Path(cfg["output_pattern"]).resolve())
        bpy.ops.render.render(animation=True)
    else:
        bpy.context.scene.render.filepath = str(Path(cfg["output"]).resolve())
        bpy.ops.render.render(write_still=True)


if __name__ == "__main__":
    main()
