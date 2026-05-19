"""Wrap posters and stickers onto the AfishaThumb column.

Each glued artifact is a small mesh — a curved plane that hugs the
cylindrical body surface — with the poster or sticker image bound as
its base-color texture. Geometry features:

- Wrap: every plane is generated as a strip of `n_u` subdivisions along
  the circumferential direction, so it curves to follow the cylinder.
- Float: planes sit at `cylinder_radius + paper_offset`, so the paper
  looks glued, not embedded.
- Peel: an optional corner is lifted off the surface by displacing its
  vertices radially outward + a small extra Z lift, producing the
  curled-corner read from the requirements.
- Wrinkle: a low-intensity displacement modifier driven by a stable
  noise texture; afishathumb uses a near-flat amount so most posters
  stay tidy.

This module is Blender-side (uses `bpy`). Texture creation lives in
`typography.py` and the Stage-2 driver writes both kinds of PNGs into
a working directory before calling these placement helpers.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import bpy  # type: ignore[import-not-found]
from mathutils import Vector  # type: ignore[import-not-found]


@dataclass
class PaperPlacement:
    """One paper artifact glued onto the column."""

    image_path: Path
    anchor_angle_deg: float          # circumferential position on the column
    anchor_z: float                  # vertical centre of the artifact
    width: float                     # arc-length width along the cylinder
    height: float                    # vertical height
    tilt_deg: float = 0.0            # in-plane rotation around the surface normal
    peel_corners: tuple[bool, bool, bool, bool] = (False, False, False, False)
    # (top-left, top-right, bottom-left, bottom-right) in the paper's local frame.
    peel_intensity: float = 1.0      # multiplier on the standard peel displacement
    wrinkle: float = 0.0             # 0..1; 0 = flat, 0.2 ≈ requirements default
    paper_offset: float = 0.004      # gap above the cylinder surface; the
                                     # placement engine increments this
                                     # per sticker so overlapping papers
                                     # don't z-fight against each other
                                     # (visible as "torn" vertical stripes
                                     # through the offending sticker).
    name: str = "AfishaThumb.Paper"


def _ensure_image_material(name: str, image_path: Path) -> bpy.types.Material:
    """Create a material that displays `image_path` on the base color.

    Important: the sticker PNGs from `typography.py` ship with transparent
    pixels outside the rounded card. We wire `image.Alpha → BSDF.Alpha`
    and set `blend_method=HASHED` so those pixels render *transparent*
    against the column instead of opaque black, which is what caused the
    visible black rectangles around tilted stickers in v1.

    Each invocation produces a unique material so two posters sharing the
    same image filename don't collide on node-tree edits.
    """
    mat = bpy.data.materials.new(name=name)
    mat.use_nodes = True
    # Cycles uses `blend_method` indirectly through the alpha output; setting
    # this also keeps Eevee renders coherent if we switch later.
    mat.blend_method = "HASHED"
    nt = mat.node_tree
    for n in list(nt.nodes):
        nt.nodes.remove(n)

    out = nt.nodes.new("ShaderNodeOutputMaterial")
    bsdf = nt.nodes.new("ShaderNodeBsdfPrincipled")
    bsdf.inputs["Roughness"].default_value = 0.78
    bsdf.inputs["Metallic"].default_value = 0.0
    nt.links.new(bsdf.outputs["BSDF"], out.inputs["Surface"])

    tex = nt.nodes.new("ShaderNodeTexImage")
    img = bpy.data.images.load(str(image_path), check_existing=True)
    # PNGs from typography.py carry alpha; force the image colorspace to
    # sRGB on color and Non-Color where appropriate.
    img.alpha_mode = "STRAIGHT"
    tex.image = img
    tex.interpolation = "Cubic"
    nt.links.new(tex.outputs["Color"], bsdf.inputs["Base Color"])
    if "Alpha" in tex.outputs:
        nt.links.new(tex.outputs["Alpha"], bsdf.inputs["Alpha"])

    # Subtle paper micro-bump so glued sheets don't look like decals.
    coord = nt.nodes.new("ShaderNodeTexCoord")
    noise = nt.nodes.new("ShaderNodeTexNoise")
    noise.inputs["Scale"].default_value = 220.0
    noise.inputs["Detail"].default_value = 4.0
    bump = nt.nodes.new("ShaderNodeBump")
    bump.inputs["Strength"].default_value = 0.08
    nt.links.new(coord.outputs["Object"], noise.inputs["Vector"])
    nt.links.new(noise.outputs["Fac"], bump.inputs["Height"])
    nt.links.new(bump.outputs["Normal"], bsdf.inputs["Normal"])
    return mat


def _paper_position(
    u: float, v: float, anchor_angle: float, anchor_z: float,
    cyl_radius: float, paper_offset: float, peel_lift: float,
    cos_t: float, sin_t: float,
) -> Vector:
    """Map a flat-paper (u,v) into world space wrapped on the cylinder."""
    theta = anchor_angle + (u / cyl_radius)
    r = cyl_radius + paper_offset + peel_lift
    return Vector((
        r * math.cos(theta),
        r * math.sin(theta),
        anchor_z + v,
    ))


def place_paper(
    placement: PaperPlacement,
    cyl_radius: float,
    parent: Optional[bpy.types.Object] = None,
    n_u: int = 24,
    n_v: int = 12,
) -> bpy.types.Object:
    """Build one wrapped-paper mesh + bind the image texture.

    Returns the placed object; caller can re-parent or post-process.
    """
    anchor_angle = math.radians(placement.anchor_angle_deg)
    tilt = math.radians(placement.tilt_deg)

    # Generate vertices on a regular (n_u+1) x (n_v+1) grid.
    half_w = placement.width / 2.0
    half_h = placement.height / 2.0
    verts: list[tuple[float, float, float]] = []
    uvs: list[tuple[float, float]] = []

    cos_t = math.cos(tilt)
    sin_t = math.sin(tilt)

    # Peel corner mask: returns extra radial lift for a (i, j) grid point.
    pin_uv = lambda i, j: (i / n_u, j / n_v)

    def _peel_lift(i: int, j: int) -> float:
        if placement.peel_intensity <= 0.0:
            return 0.0
        tl, tr, bl, br = placement.peel_corners
        # Map grid index to corner falloff.
        # Top-left ≈ (i small, j large), bottom-right ≈ (i large, j small) etc.
        u_norm = i / n_u
        v_norm = j / n_v
        lift = 0.0
        if tl:
            lift = max(lift, max(0.0, 1.0 - u_norm * 3.5) * max(0.0, v_norm * 1.0 - 0.55) * 4.0)
        if tr:
            lift = max(lift, max(0.0, u_norm * 3.5 - 2.5) * max(0.0, v_norm * 1.0 - 0.55) * 4.0)
        if bl:
            lift = max(lift, max(0.0, 1.0 - u_norm * 3.5) * max(0.0, 0.45 - v_norm) * 4.0)
        if br:
            lift = max(lift, max(0.0, u_norm * 3.5 - 2.5) * max(0.0, 0.45 - v_norm) * 4.0)
        return lift * 0.035 * placement.peel_intensity

    for j in range(n_v + 1):
        # v ∈ [-half_h, +half_h], where v=+half_h is the visual "top" of the paper.
        v_paper = -half_h + (j / n_v) * placement.height
        for i in range(n_u + 1):
            u_paper = -half_w + (i / n_u) * placement.width
            # Apply tilt: a tilt rotates the (u,v) frame around the surface normal,
            # which here we approximate as the radial vector. We just rotate the
            # (u,v) coordinates in the plane before wrapping.
            u_rot = u_paper * cos_t - v_paper * sin_t
            v_rot = u_paper * sin_t + v_paper * cos_t
            lift = _peel_lift(i, j)
            p = _paper_position(
                u_rot, v_rot, anchor_angle, placement.anchor_z,
                cyl_radius, placement.paper_offset, lift,
                cos_t, sin_t,
            )
            verts.append((p.x, p.y, p.z))
            uvs.append(pin_uv(i, j))

    faces: list[tuple[int, int, int, int]] = []
    n_cols = n_u + 1
    for j in range(n_v):
        for i in range(n_u):
            a = j * n_cols + i
            b = j * n_cols + i + 1
            c = (j + 1) * n_cols + i + 1
            d = (j + 1) * n_cols + i
            # Outward-facing (towards viewer) winding.
            faces.append((a, b, c, d))

    mesh = bpy.data.meshes.new(placement.name + ".mesh")
    obj = bpy.data.objects.new(placement.name, mesh)
    bpy.context.collection.objects.link(obj)
    mesh.from_pydata(verts, [], faces)
    mesh.update()
    for poly in mesh.polygons:
        poly.use_smooth = True

    # UV layer.
    uv_layer = mesh.uv_layers.new(name="paper_uv")
    for poly in mesh.polygons:
        for loop_idx in poly.loop_indices:
            v_idx = mesh.loops[loop_idx].vertex_index
            uv_layer.data[loop_idx].uv = uvs[v_idx]

    # Material.
    mat = _ensure_image_material(placement.name + ".mat", placement.image_path)
    mesh.materials.append(mat)

    # Optional wrinkle: low-amplitude displacement modifier.
    if placement.wrinkle > 0.0:
        tex = bpy.data.textures.new(placement.name + ".wrinkle", type="STUCCI")
        tex.noise_scale = 0.18
        disp = obj.modifiers.new(name="Wrinkle", type="DISPLACE")
        disp.texture = tex
        disp.strength = 0.0035 * placement.wrinkle
        disp.mid_level = 0.5
        disp.direction = "NORMAL"

    if parent is not None:
        obj.parent = parent
    return obj
