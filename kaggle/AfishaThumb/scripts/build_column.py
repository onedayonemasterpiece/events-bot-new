"""Parametric AfishaThumb column geometry + materials.

Implements the Litfaßsäule / афишная тумба described in
`docs/backlog/features/afishathumb/afishathumb-requirements.md` (sections 1–4).
Designed to run inside Blender (4.x) via `bpy`. Reuse:

    import bpy
    from build_column import build_column, reset_scene
    reset_scene()
    column = build_column(D=1.0, location=(0, 0, 0))

The function returns a dict of named objects so downstream layout code
(`layout_posters.py`) can reference the body cylinder (where posters get
glued) and the cornice/dome (where they must not).

All vertical proportions are expressed as multiples of `D` (the diameter
of the main cylindrical body). Total column height ≈ 4.5 * D.
"""

from __future__ import annotations

import math
from typing import Dict, Tuple

import bpy  # type: ignore[import-not-found]
from mathutils import Vector  # type: ignore[import-not-found]


# Canonical colours from requirements section "Material and colour"
CAST_IRON_HEX = "#2A3B2C"
PLASTER_HEX = "#B0B5B0"
PLAQUE_INNER_HEX = "#D9DDD8"
VERDIGRIS_HEX = "#6C8E75"


# --------------------------------------------------------------------------- #
# Colour / material helpers
# --------------------------------------------------------------------------- #


def _hex_to_linear_rgb(hex_color: str) -> Tuple[float, float, float, float]:
    h = hex_color.lstrip("#")
    r_srgb = int(h[0:2], 16) / 255.0
    g_srgb = int(h[2:4], 16) / 255.0
    b_srgb = int(h[4:6], 16) / 255.0

    def _to_linear(c: float) -> float:
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    return (_to_linear(r_srgb), _to_linear(g_srgb), _to_linear(b_srgb), 1.0)


def _ensure_material(
    name: str,
    base_hex: str,
    roughness: float = 0.85,
    metallic: float = 0.0,
    bump_strength: float = 0.0,
    bump_scale: float = 18.0,
    roughness_variation: float = 0.0,
    color_variation: float = 0.0,
) -> bpy.types.Material:
    mat = bpy.data.materials.get(name)
    if mat is not None:
        return mat
    mat = bpy.data.materials.new(name=name)
    mat.use_nodes = True
    nt = mat.node_tree
    for n in list(nt.nodes):
        nt.nodes.remove(n)

    out = nt.nodes.new("ShaderNodeOutputMaterial")
    bsdf = nt.nodes.new("ShaderNodeBsdfPrincipled")
    bsdf.inputs["Base Color"].default_value = _hex_to_linear_rgb(base_hex)
    bsdf.inputs["Roughness"].default_value = roughness
    bsdf.inputs["Metallic"].default_value = metallic
    nt.links.new(bsdf.outputs["BSDF"], out.inputs["Surface"])

    tex_coord = nt.nodes.new("ShaderNodeTexCoord")

    # Color variation: lighter wear highlights + darker recess shadows so
    # painted-iron reads as worn metal rather than a flat colour fill.
    if color_variation > 0.0:
        cv_noise = nt.nodes.new("ShaderNodeTexNoise")
        cv_noise.inputs["Scale"].default_value = max(2.5, bump_scale * 0.15)
        cv_noise.inputs["Detail"].default_value = 6.0
        cv_noise.inputs["Roughness"].default_value = 0.7
        ramp = nt.nodes.new("ShaderNodeValToRGB")
        ramp.color_ramp.interpolation = "EASE"
        # Slightly darker shadow on the dark side, slightly lighter wear on highlights.
        base_lin = _hex_to_linear_rgb(base_hex)
        def _shift(c, k):
            return tuple(max(0.0, min(1.0, ch * k)) for ch in c[:3]) + (1.0,)
        dark = _shift(base_lin, 0.55)
        mid = base_lin
        light = _shift(base_lin, 1.55)
        ramp.color_ramp.elements[0].position = 0.3
        ramp.color_ramp.elements[0].color = dark
        ramp.color_ramp.elements[1].position = 0.85
        ramp.color_ramp.elements[1].color = light
        mid_el = ramp.color_ramp.elements.new(0.6)
        mid_el.color = mid
        mix = nt.nodes.new("ShaderNodeMixRGB")
        mix.blend_type = "MIX"
        mix.inputs["Fac"].default_value = color_variation
        mix.inputs["Color1"].default_value = base_lin
        nt.links.new(tex_coord.outputs["Object"], cv_noise.inputs["Vector"])
        nt.links.new(cv_noise.outputs["Fac"], ramp.inputs["Fac"])
        nt.links.new(ramp.outputs["Color"], mix.inputs["Color2"])
        nt.links.new(mix.outputs["Color"], bsdf.inputs["Base Color"])

    # Roughness variation: worn highlights are smoother (shinier) than the
    # painted bulk. Real metal-paint surfaces are never uniformly matte.
    if roughness_variation > 0.0:
        r_noise = nt.nodes.new("ShaderNodeTexNoise")
        r_noise.inputs["Scale"].default_value = max(3.0, bump_scale * 0.18)
        r_noise.inputs["Detail"].default_value = 5.0
        r_ramp = nt.nodes.new("ShaderNodeValToRGB")
        r_ramp.color_ramp.elements[0].position = 0.35
        r_ramp.color_ramp.elements[0].color = (1, 1, 1, 1)
        r_ramp.color_ramp.elements[1].position = 0.85
        r_ramp.color_ramp.elements[1].color = (0, 0, 0, 1)
        r_mix = nt.nodes.new("ShaderNodeMixRGB")
        r_mix.blend_type = "MIX"
        r_mix.inputs["Fac"].default_value = roughness_variation
        r_mix.inputs["Color1"].default_value = (roughness, roughness, roughness, 1.0)
        nt.links.new(tex_coord.outputs["Object"], r_noise.inputs["Vector"])
        nt.links.new(r_noise.outputs["Fac"], r_ramp.inputs["Fac"])
        nt.links.new(r_ramp.outputs["Color"], r_mix.inputs["Color2"])
        nt.links.new(r_mix.outputs["Color"], bsdf.inputs["Roughness"])

    if bump_strength > 0.0:
        # Procedural micro-detail: noise-driven bump so cast iron / plaster
        # don't read as plastic under raking light.
        noise = nt.nodes.new("ShaderNodeTexNoise")
        noise.inputs["Scale"].default_value = bump_scale
        noise.inputs["Detail"].default_value = 6.0
        noise.inputs["Roughness"].default_value = 0.55
        bump = nt.nodes.new("ShaderNodeBump")
        bump.inputs["Strength"].default_value = bump_strength
        nt.links.new(tex_coord.outputs["Object"], noise.inputs["Vector"])
        nt.links.new(noise.outputs["Fac"], bump.inputs["Height"])
        nt.links.new(bump.outputs["Normal"], bsdf.inputs["Normal"])

    return mat


def _ensure_body_iron_material(
    name: str = "AfishaThumb.Body.WeatheredIron",
) -> bpy.types.Material:
    """Bare cast iron for the poster-receiving cylinder.

    Round-4 user feedback: the previous "weathered iron with vertical
    rust streaks" rendered as visible HORIZONTAL stripes because the
    Object-space gradient interacted with the cylinder UV in a
    band-producing way. New approach is intentionally minimal:

      - Uniform dark warm-grey base (`#3E3C39`), no gradients at all.
      - A coarse Voronoi cell pattern with very small color delta
        between cells (≤ 8% brightness change). Reads as the irregular
        "cellular" surface of foundry-cast iron, no banding.
      - Fine noise bump for the cast-sand texture.
      - Metallic 0.62, roughness 0.58 — semi-matte real cast iron, not
        polished steel and not mirror.
    """
    mat = bpy.data.materials.get(name)
    if mat is not None:
        return mat
    mat = bpy.data.materials.new(name=name)
    mat.use_nodes = True
    nt = mat.node_tree
    for n in list(nt.nodes):
        nt.nodes.remove(n)

    out = nt.nodes.new("ShaderNodeOutputMaterial")
    bsdf = nt.nodes.new("ShaderNodeBsdfPrincipled")
    bsdf.inputs["Metallic"].default_value = 0.62
    bsdf.inputs["Roughness"].default_value = 0.58
    nt.links.new(bsdf.outputs["BSDF"], out.inputs["Surface"])

    coord = nt.nodes.new("ShaderNodeTexCoord")

    base = _hex_to_linear_rgb("#3E3C39")
    base_light = _hex_to_linear_rgb("#4A4744")
    base_dark = _hex_to_linear_rgb("#322F2D")

    # Fine voronoi: scale 22 → cells of ~0.045 in object space,
    # ~5° angularly on the cylinder. Reads as a fine cast-surface micro
    # variation rather than the "crocodile-skin patches" the previous
    # scale=7 produced. Narrower tone delta keeps the body uniformly dark.
    vor = nt.nodes.new("ShaderNodeTexVoronoi")
    vor.feature = "F1"
    vor.distance = "EUCLIDEAN"
    vor.inputs["Scale"].default_value = 22.0
    vor.inputs["Randomness"].default_value = 1.0

    # Narrow tone delta: only ±5% brightness around the base colour.
    def _shade(c, k):
        return tuple(max(0.0, min(1.0, ch * k)) for ch in c[:3]) + (1.0,)
    ramp = nt.nodes.new("ShaderNodeValToRGB")
    ramp.color_ramp.interpolation = "LINEAR"
    ramp.color_ramp.elements[0].position = 0.0
    ramp.color_ramp.elements[0].color = _shade(base, 0.88)
    ramp.color_ramp.elements[1].position = 1.0
    ramp.color_ramp.elements[1].color = _shade(base, 1.10)
    mid = ramp.color_ramp.elements.new(0.5)
    mid.color = base

    nt.links.new(coord.outputs["Object"], vor.inputs["Vector"])
    nt.links.new(vor.outputs["Distance"], ramp.inputs["Fac"])
    nt.links.new(ramp.outputs["Color"], bsdf.inputs["Base Color"])

    # Cast surface micro-bump — high-frequency noise gives the gritty
    # "foundry surface" feel without any directional pattern.
    bump_noise = nt.nodes.new("ShaderNodeTexNoise")
    bump_noise.inputs["Scale"].default_value = 180.0
    bump_noise.inputs["Detail"].default_value = 4.0
    bump_noise.inputs["Roughness"].default_value = 0.6
    bump = nt.nodes.new("ShaderNodeBump")
    bump.inputs["Strength"].default_value = 0.28
    nt.links.new(coord.outputs["Object"], bump_noise.inputs["Vector"])
    nt.links.new(bump_noise.outputs["Fac"], bump.inputs["Height"])
    nt.links.new(bump.outputs["Normal"], bsdf.inputs["Normal"])

    return mat


def _verdigris_material(name: str = "AfishaThumb.Verdigris") -> bpy.types.Material:
    """Patinated copper with AO-darkened seams + lighter wear highlights."""
    mat = bpy.data.materials.get(name)
    if mat is not None:
        return mat
    mat = bpy.data.materials.new(name=name)
    mat.use_nodes = True
    nt = mat.node_tree
    for n in list(nt.nodes):
        nt.nodes.remove(n)

    out = nt.nodes.new("ShaderNodeOutputMaterial")
    bsdf = nt.nodes.new("ShaderNodeBsdfPrincipled")
    bsdf.inputs["Roughness"].default_value = 0.7
    bsdf.inputs["Metallic"].default_value = 0.15
    nt.links.new(bsdf.outputs["BSDF"], out.inputs["Surface"])

    # Two-tone colour mix: deep verdigris with lighter wear on highlights.
    tex_coord = nt.nodes.new("ShaderNodeTexCoord")
    noise = nt.nodes.new("ShaderNodeTexNoise")
    noise.inputs["Scale"].default_value = 12.0
    noise.inputs["Detail"].default_value = 8.0
    color_ramp = nt.nodes.new("ShaderNodeValToRGB")
    color_ramp.color_ramp.elements[0].position = 0.35
    color_ramp.color_ramp.elements[0].color = _hex_to_linear_rgb("#4E6F58")
    color_ramp.color_ramp.elements[1].position = 0.75
    color_ramp.color_ramp.elements[1].color = _hex_to_linear_rgb("#9CB8A2")
    nt.links.new(tex_coord.outputs["Generated"], noise.inputs["Vector"])
    nt.links.new(noise.outputs["Fac"], color_ramp.inputs["Fac"])
    nt.links.new(color_ramp.outputs["Color"], bsdf.inputs["Base Color"])

    # Bump from a second noise to keep matte verdigris feel.
    noise2 = nt.nodes.new("ShaderNodeTexNoise")
    noise2.inputs["Scale"].default_value = 60.0
    noise2.inputs["Detail"].default_value = 4.0
    bump = nt.nodes.new("ShaderNodeBump")
    bump.inputs["Strength"].default_value = 0.35
    nt.links.new(tex_coord.outputs["Object"], noise2.inputs["Vector"])
    nt.links.new(noise2.outputs["Fac"], bump.inputs["Height"])
    nt.links.new(bump.outputs["Normal"], bsdf.inputs["Normal"])

    return mat


# --------------------------------------------------------------------------- #
# Scene management
# --------------------------------------------------------------------------- #


def reset_scene() -> None:
    """Remove every object, mesh, curve, material and image from the scene.

    Used at the top of one-shot test renders so the column always builds
    against an empty file. Not for live production where assets accumulate.
    """
    for obj in list(bpy.data.objects):
        bpy.data.objects.remove(obj, do_unlink=True)
    for coll in (bpy.data.meshes, bpy.data.curves, bpy.data.materials, bpy.data.images):
        for item in list(coll):
            coll.remove(item, do_unlink=True)


def _new_empty(name: str, location: Vector) -> bpy.types.Object:
    empty = bpy.data.objects.new(name, None)
    empty.location = location
    bpy.context.collection.objects.link(empty)
    return empty


def _add_cylinder(
    name: str,
    radius: float,
    depth: float,
    location: Vector,
    segments: int = 96,
    material: bpy.types.Material | None = None,
) -> bpy.types.Object:
    bpy.ops.mesh.primitive_cylinder_add(
        radius=radius, depth=depth, vertices=segments, location=location
    )
    obj = bpy.context.active_object
    obj.name = name
    if material is not None:
        obj.data.materials.append(material)
    # Force smooth shading on the side faces. The default
    # `primitive_cylinder_add` leaves them flat, which renders as visible
    # vertical "facet" stripes on a curved body once a camera moves
    # across them. Top + bottom caps stay flat.
    for poly in obj.data.polygons:
        # The two end caps point along ±Z and have many vertices (ring
        # fan); side quads have only 4 vertices and normals perpendicular
        # to Z. Smooth-shade only the side quads.
        if len(poly.vertices) == 4:
            poly.use_smooth = True
    obj.data.update()
    return obj


def _add_torus_ring(
    name: str,
    major_radius: float,
    minor_radius: float,
    location: Vector,
    major_seg: int = 96,
    minor_seg: int = 16,
    material: bpy.types.Material | None = None,
) -> bpy.types.Object:
    bpy.ops.mesh.primitive_torus_add(
        major_radius=major_radius,
        minor_radius=minor_radius,
        major_segments=major_seg,
        minor_segments=minor_seg,
        location=location,
    )
    obj = bpy.context.active_object
    obj.name = name
    if material is not None:
        obj.data.materials.append(material)
    return obj


def _spin_profile(
    name: str,
    profile_points: list[Tuple[float, float]],
    z_offset: float,
    segments: int = 96,
    material: bpy.types.Material | None = None,
) -> bpy.types.Object:
    """Build a surface of revolution from a (r, z) profile rotated around Z.

    `profile_points` is a list of `(radius, z)` pairs in the X+/Z plane.
    The profile is sampled densely between supplied points and revolved
    `segments` times around the world Z axis. The mesh origin sits at the
    base of the profile, then the whole object is translated by `z_offset`.
    """
    mesh = bpy.data.meshes.new(name)
    obj = bpy.data.objects.new(name, mesh)
    bpy.context.collection.objects.link(obj)

    verts: list[Tuple[float, float, float]] = []
    faces: list[Tuple[int, int, int, int]] = []
    n = len(profile_points)
    for s in range(segments):
        theta = (s / segments) * 2.0 * math.pi
        cos_t, sin_t = math.cos(theta), math.sin(theta)
        for r, z in profile_points:
            verts.append((r * cos_t, r * sin_t, z))

    for s in range(segments):
        s_next = (s + 1) % segments
        for i in range(n - 1):
            a = s * n + i
            b = s * n + i + 1
            c = s_next * n + i + 1
            d = s_next * n + i
            faces.append((a, b, c, d))

    mesh.from_pydata(verts, [], faces)
    mesh.update()
    # Smooth shading for the curved profile; flat shading would betray facets.
    for poly in mesh.polygons:
        poly.use_smooth = True

    obj.location = Vector((0.0, 0.0, z_offset))
    if material is not None:
        mesh.materials.append(material)
    return obj


# --------------------------------------------------------------------------- #
# Sub-builders
# --------------------------------------------------------------------------- #


def _build_base(D: float, parent: bpy.types.Object) -> dict:
    """Two stepped cast-iron tiers — section 1 of the spec."""
    # Painted cast iron: high metallic so the surface reflects light like
    # real iron, mid roughness for the gloss of old layered paint, plus
    # noise-driven color and roughness variation so the surface reads as
    # worn metalwork instead of a flat-painted plastic column.
    iron = _ensure_material(
        "AfishaThumb.CastIron",
        CAST_IRON_HEX,
        roughness=0.42,
        metallic=0.72,
        bump_strength=0.55,
        bump_scale=22.0,
        roughness_variation=0.55,
        color_variation=0.55,
    )

    # Lower plinth: D=1.1, h=0.08, strict square top edge.
    plinth_h = 0.08 * D
    plinth = _add_cylinder(
        "Tumba.Base.Plinth",
        radius=0.55 * D,
        depth=plinth_h,
        location=Vector((0.0, 0.0, plinth_h / 2.0)),
        material=iron,
    )

    # Upper tier: D=1.05, h=0.1, with 45° bevel on the top edge that narrows
    # the diameter to exactly D=1.0 at the top, handing off to the body.
    upper_h = 0.10 * D
    upper_bottom_z = plinth_h
    upper = _add_cylinder(
        "Tumba.Base.UpperTier",
        radius=0.525 * D,
        depth=upper_h,
        location=Vector((0.0, 0.0, upper_bottom_z + upper_h / 2.0)),
        material=iron,
    )
    # Bevel only the top loop so the bottom stays at 1.05D and the top arrives at ~1.0D.
    bpy.context.view_layer.objects.active = upper
    bpy.ops.object.mode_set(mode="EDIT")
    bpy.ops.mesh.select_all(action="DESELECT")
    bpy.ops.object.mode_set(mode="OBJECT")
    for v in upper.data.vertices:
        v.select = v.co.z > 0.0
    bpy.ops.object.mode_set(mode="EDIT")
    bpy.ops.mesh.bevel(
        offset=(0.525 - 0.50) * D,
        segments=4,
        affect="VERTICES",
    )
    bpy.ops.object.mode_set(mode="OBJECT")

    plinth.parent = parent
    upper.parent = parent

    return {"plinth": plinth, "upper": upper, "top_z": upper_bottom_z + upper_h}


def _build_body(D: float, base_top_z: float, parent: bpy.types.Object) -> dict:
    """Main poster-carrying cylinder — section 2.

    User override of the spec's `#B0B5B0` plaster surface: the body is the
    surface posters get glued to, so it would NOT be painted (paint would
    fight adhesion + look ugly when paper peels). We render it as exposed
    weathered iron — neutral grey with rust streaks, no green — while the
    decorative parts (base / cornice / dome) keep the painted cast-iron
    look. This is the third iteration on this surface; previous passes
    used the green cast-iron hex and read as plastic-painted plastic.
    """
    body_iron = _ensure_body_iron_material("AfishaThumb.Body.WeatheredIron")

    body_h = 2.5 * D
    body = _add_cylinder(
        "Tumba.Body",
        radius=0.50 * D,
        depth=body_h,
        location=Vector((0.0, 0.0, base_top_z + body_h / 2.0)),
        material=body_iron,
    )
    body.parent = parent

    # Cylindrical UVs are what poster placement needs — Smart UV Project
    # would break that. Manually unwrap by Cube/Cylinder Project so U maps
    # to angle and V maps to height. layout_posters.py will read body.data
    # to project posters onto the surface.
    bpy.context.view_layer.objects.active = body
    bpy.ops.object.mode_set(mode="EDIT")
    bpy.ops.mesh.select_all(action="SELECT")
    bpy.ops.uv.cylinder_project(direction="VIEW_ON_EQUATOR", align="POLAR_ZX")
    bpy.ops.object.mode_set(mode="OBJECT")

    return {"body": body, "top_z": base_top_z + body_h, "radius": 0.50 * D}


def _build_cornice_belt(
    D: float, body_top_z: float, parent: bpy.types.Object
) -> dict:
    """Molding + frieze + plaque + bell capitel — section 3."""
    iron = bpy.data.materials["AfishaThumb.CastIron"]
    plaque_inner = _ensure_material(
        "AfishaThumb.PlaqueInner",
        PLAQUE_INNER_HEX,
        roughness=0.5,
    )

    # 1. Molding torus: thin ring, polukruglyy profile, slightly proud of the body.
    molding_h = 0.04 * D
    molding = _add_torus_ring(
        "Tumba.Cornice.Molding",
        major_radius=0.515 * D,
        minor_radius=molding_h / 2.0,
        location=Vector((0.0, 0.0, body_top_z + molding_h / 2.0)),
        material=iron,
    )
    molding.parent = parent

    # 2. Frieze cylinder: same D=1.0 as the body, height 0.4D.
    frieze_h = 0.40 * D
    frieze_bottom = body_top_z + molding_h
    frieze = _add_cylinder(
        "Tumba.Cornice.Frieze",
        radius=0.50 * D,
        depth=frieze_h,
        location=Vector((0.0, 0.0, frieze_bottom + frieze_h / 2.0)),
        material=iron,
    )
    frieze.parent = parent

    # 3. Information plaque: a slightly recessed rectangle on the front face.
    # MVP: a thin plane offset from the cylinder surface, oriented +Y.
    plaque_w = 0.40 * D
    plaque_h = 0.25 * D
    bpy.ops.mesh.primitive_plane_add(size=1.0, location=Vector((0.0, 0.0, 0.0)))
    plaque = bpy.context.active_object
    plaque.name = "Tumba.Cornice.Plaque"
    plaque.scale = Vector((plaque_w, plaque_h, 1.0))
    plaque.rotation_euler = (math.radians(90.0), 0.0, 0.0)
    plaque.location = Vector(
        (0.0, -0.502 * D, frieze_bottom + frieze_h / 2.0)
    )
    plaque.data.materials.append(plaque_inner)
    plaque.parent = parent

    # 4. Bell-shaped supporting cornice (carniz). Profile expands from 1.0D
    # at the bottom to 1.2D at the top, height 0.25D, with three subtle
    # horizontal grooves modelled by inward dips in the profile.
    carniz_h = 0.25 * D
    carniz_bottom = frieze_bottom + frieze_h
    profile: list[Tuple[float, float]] = []
    n_samples = 48
    for i in range(n_samples + 1):
        t = i / n_samples  # 0..1 bottom→top
        # Bell/concave-then-convex: gentle expansion via shifted cosine.
        r = 0.50 * D + (0.10 * D) * (1.0 - math.cos(t * math.pi)) / 2.0
        # Two extra D-units of expansion concentrated in the upper half
        # to land at 1.2D at the top.
        if t > 0.55:
            r += 0.10 * D * ((t - 0.55) / 0.45) ** 1.3
        # Three thin grooves between t=[0.2, 0.45, 0.7].
        for groove_center in (0.20, 0.45, 0.70):
            r -= 0.012 * D * math.exp(-((t - groove_center) ** 2) / 0.0008)
        z = t * carniz_h
        profile.append((r, z))
    carniz = _spin_profile(
        "Tumba.Cornice.Carniz",
        profile_points=profile,
        z_offset=carniz_bottom,
        material=iron,
    )
    carniz.parent = parent

    return {
        "molding": molding,
        "frieze": frieze,
        "plaque": plaque,
        "carniz": carniz,
        "top_z": carniz_bottom + carniz_h,
        "top_radius": profile[-1][0],
    }


def _build_dome_and_spire(
    D: float, cornice_top_z: float, parent: bpy.types.Object
) -> dict:
    """Ogee dome with fish-scale shader + verdigris spire — section 4."""
    verdigris = _verdigris_material()

    # Ogee dome: starts at radius 0.6D (the cornice top is at 0.6D), bulges
    # slightly outward at t~0.25, then sweeps inward to a near-zero apex.
    dome_h = 0.70 * D
    profile: list[Tuple[float, float]] = []
    n_samples = 64
    for i in range(n_samples + 1):
        t = i / n_samples  # 0..1 base→apex
        # Bulge factor: peak at t≈0.25.
        bulge = 0.03 * D * math.sin(min(1.0, t / 0.35) * math.pi)
        # Inward sweep: stronger above t=0.4, easing into the apex.
        if t < 0.4:
            base_r = 0.60 * D - (0.10 * D) * (t / 0.4) ** 1.8
        else:
            base_r = 0.50 * D * (1.0 - (t - 0.4) / 0.6) ** 1.6
        r = max(0.0, base_r + bulge)
        z = t * dome_h
        profile.append((r, z))
    dome = _spin_profile(
        "Tumba.Dome",
        profile_points=profile,
        z_offset=cornice_top_z,
        material=verdigris,
    )
    dome.parent = parent

    # Fish-scale pattern is left as a shader-level concern for now: a
    # voronoi-based normal map driven by surface UVs. We add the nodes
    # on the verdigris material variant so the dome shows scales while
    # the spire stays smooth. Implementing geometry-real scales is a
    # follow-up once stills validate the silhouette.

    # Spire: small ring, flattened bulb, pointed cone.
    spire_total_h = 0.35 * D
    spire_bottom = cornice_top_z + dome_h

    ring_h = 0.04 * D
    ring = _add_cylinder(
        "Tumba.Spire.Ring",
        radius=0.075 * D,
        depth=ring_h,
        location=Vector((0.0, 0.0, spire_bottom + ring_h / 2.0)),
        segments=48,
        material=verdigris,
    )
    ring.parent = parent

    bulb_h = 0.10 * D
    bpy.ops.mesh.primitive_uv_sphere_add(
        radius=0.065 * D,
        segments=48,
        ring_count=24,
        location=Vector((0.0, 0.0, spire_bottom + ring_h + bulb_h / 2.0)),
    )
    bulb = bpy.context.active_object
    bulb.name = "Tumba.Spire.Bulb"
    bulb.scale = Vector((1.0, 1.0, 0.55))  # flatten on Z
    bulb.data.materials.append(verdigris)
    bulb.parent = parent

    needle_h = spire_total_h - ring_h - bulb_h
    bpy.ops.mesh.primitive_cone_add(
        radius1=0.020 * D,
        radius2=0.0,
        depth=needle_h,
        vertices=24,
        location=Vector(
            (0.0, 0.0, spire_bottom + ring_h + bulb_h + needle_h / 2.0)
        ),
    )
    needle = bpy.context.active_object
    needle.name = "Tumba.Spire.Needle"
    needle.data.materials.append(verdigris)
    needle.parent = parent

    return {
        "dome": dome,
        "spire_ring": ring,
        "spire_bulb": bulb,
        "spire_needle": needle,
        "top_z": spire_bottom + spire_total_h,
    }


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def build_column(
    D: float = 1.0,
    location: Tuple[float, float, float] = (0.0, 0.0, 0.0),
    name: str = "AfishaThumb.Column",
) -> Dict[str, object]:
    """Build the full advertising column at `location` with body diameter `D`.

    Returns a flat dict of named objects:
      - `root`: parent Empty
      - `base.plinth`, `base.upper`
      - `body`
      - `cornice.molding`, `cornice.frieze`, `cornice.plaque`, `cornice.carniz`
      - `dome`, `spire.ring`, `spire.bulb`, `spire.needle`
      - `metrics`: dict with `body_top_z`, `cornice_top_z`, `total_height`,
        `body_radius` for downstream consumers (poster placement,
        camera-path planner).
    """
    root = _new_empty(name, Vector(location))

    base = _build_base(D, root)
    body = _build_body(D, base["top_z"], root)
    cornice = _build_cornice_belt(D, body["top_z"], root)
    dome_spire = _build_dome_and_spire(D, cornice["top_z"], root)

    return {
        "root": root,
        "base.plinth": base["plinth"],
        "base.upper": base["upper"],
        "body": body["body"],
        "cornice.molding": cornice["molding"],
        "cornice.frieze": cornice["frieze"],
        "cornice.plaque": cornice["plaque"],
        "cornice.carniz": cornice["carniz"],
        "dome": dome_spire["dome"],
        "spire.ring": dome_spire["spire_ring"],
        "spire.bulb": dome_spire["spire_bulb"],
        "spire.needle": dome_spire["spire_needle"],
        "metrics": {
            "D": D,
            "body_radius": body["radius"],
            "base_top_z": base["top_z"],
            "body_top_z": body["top_z"],
            "cornice_top_z": cornice["top_z"],
            "total_height": dome_spire["top_z"],
        },
    }


if __name__ == "__main__":  # pragma: no cover — invoked via `blender --python`
    reset_scene()
    result = build_column(D=1.0)
    print(f"AfishaThumb column built. Total height = {result['metrics']['total_height']:.3f}")
