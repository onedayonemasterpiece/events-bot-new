"""Smoke test: build the AfishaThumb column and render a single still.

Invoked through `scripts/afishathumb/blender_run.sh`.

Outputs:
    artifacts/afishathumb/smoke_column_<orbit>.png

Renders three angles (0°, 90°, 180° orbit around Z) so we can verify the
silhouette, materials, and proportions before adding posters/stickers.
"""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "kaggle" / "AfishaThumb" / "scripts"))

import bpy  # type: ignore[import-not-found]
from mathutils import Vector  # type: ignore[import-not-found]

from build_column import build_column, reset_scene  # noqa: E402

OUT_DIR = REPO_ROOT / "artifacts" / "afishathumb"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _setup_lighting() -> None:
    """Natural daylight key + cool fill + bounce. Tuned so the dark-green
    cast iron of the base/cornice reads as green (not black) and the
    verdigris dome keeps its blue-green character without clipping."""
    bpy.ops.object.light_add(type="SUN", location=(0, 0, 10))
    key = bpy.context.active_object
    key.name = "Key.Sun"
    key.rotation_euler = (math.radians(55.0), 0.0, math.radians(35.0))
    key.data.energy = 5.5
    key.data.angle = math.radians(2.5)
    key.data.color = (1.0, 0.97, 0.92)

    bpy.ops.object.light_add(type="AREA", location=(6, -3, 4))
    fill = bpy.context.active_object
    fill.name = "Fill.Area"
    fill.rotation_euler = (math.radians(75.0), 0.0, math.radians(120.0))
    fill.data.energy = 600.0
    fill.data.size = 6.0
    fill.data.color = (0.85, 0.90, 1.0)

    # Low bounce from the front, so the cast-iron front face is not pure shadow.
    bpy.ops.object.light_add(type="AREA", location=(0, -5.5, 1.2))
    bounce = bpy.context.active_object
    bounce.name = "Bounce.Front"
    bounce.rotation_euler = (math.radians(90.0), 0.0, 0.0)
    bounce.data.energy = 280.0
    bounce.data.size = 4.5
    bounce.data.color = (0.92, 0.95, 1.0)

    world = bpy.context.scene.world
    if world is None:
        world = bpy.data.worlds.new("World")
        bpy.context.scene.world = world
    world.use_nodes = True
    bg = world.node_tree.nodes.get("Background")
    if bg is not None:
        bg.inputs["Color"].default_value = (0.65, 0.72, 0.80, 1.0)
        bg.inputs["Strength"].default_value = 0.55


def _setup_camera(total_height: float, orbit_deg: float) -> bpy.types.Object:
    """Wide camera framing the whole column with headroom above the spire
    and visible cobblestones below the plinth. The orbit angle rotates the
    camera around Z while the target sits on the column axis."""
    # Pull back further than v0 and use a slightly wider lens so the full
    # 4.42-unit silhouette fits with margin on a 1080x1572 frame.
    radius = 7.5
    angle = math.radians(orbit_deg)
    cam_z = total_height * 0.50
    cam_loc = Vector(
        (
            radius * math.cos(angle),
            radius * math.sin(angle),
            cam_z,
        )
    )
    target = Vector((0.0, 0.0, total_height * 0.42))
    bpy.ops.object.camera_add(location=cam_loc)
    cam = bpy.context.active_object
    cam.name = f"Cam.Orbit{int(orbit_deg):03d}"
    cam.data.lens = 42.0
    cam.data.sensor_width = 36.0
    direction = (target - cam_loc).normalized()
    rot_quat = direction.to_track_quat("-Z", "Y")
    cam.rotation_euler = rot_quat.to_euler()
    bpy.context.scene.camera = cam
    return cam


def _render_to(path: Path) -> None:
    scene = bpy.context.scene
    # Cycles CPU only: EEVEE Next needs a real GPU/EGL context, which we
    # don't have under Xvfb. Cycles in CPU mode renders without a working
    # GL backend.
    scene.render.engine = "CYCLES"
    scene.cycles.device = "CPU"
    scene.cycles.samples = 48  # smoke pass — quality not the goal yet
    scene.cycles.use_denoising = True
    scene.render.image_settings.file_format = "PNG"
    scene.render.image_settings.color_mode = "RGB"
    scene.render.resolution_x = 1080
    scene.render.resolution_y = 1572
    scene.render.resolution_percentage = 50  # half-res for smoke speed
    scene.render.filepath = str(path)
    bpy.ops.render.render(write_still=True)


def main() -> None:
    reset_scene()
    column = build_column(D=1.0)
    metrics = column["metrics"]  # type: ignore[index]
    total_h = float(metrics["total_height"])  # type: ignore[index]
    print(f"[smoke] column total_height = {total_h:.3f}")

    _setup_lighting()

    for orbit_deg in (0.0, 90.0, 180.0):
        # Remove any prior camera to start fresh per angle.
        for obj in [o for o in bpy.data.objects if o.type == "CAMERA"]:
            bpy.data.objects.remove(obj, do_unlink=True)
        _setup_camera(total_h, orbit_deg)
        out_path = OUT_DIR / f"smoke_column_orbit_{int(orbit_deg):03d}.png"
        _render_to(out_path)
        print(f"[smoke] rendered {out_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 — surface inside Blender stdout
        print(f"[smoke] FAILED: {exc!r}")
        raise
