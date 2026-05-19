"""Stage-2 in-Blender driver: render a single slot close-up.

Reads `artifacts/afishathumb/slot_<event_id>/manifest.json` (produced by
`scripts/afishathumb/prepare_slot.py`), builds the column from
`kaggle/AfishaThumb/scripts/build_column.py`, glues the poster + sticker
papers via `layout_posters.place_paper`, sets a close-up camera, and
renders to `slot_close.png` + a back-off wide angle to `slot_wide.png`
for visual reference.
"""

from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path

import bpy  # type: ignore[import-not-found]
from bpy_extras.object_utils import world_to_camera_view  # type: ignore[import-not-found]
from mathutils import Vector  # type: ignore[import-not-found]

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "kaggle" / "AfishaThumb" / "scripts"))

from build_column import build_column, reset_scene  # noqa: E402
from layout_posters import PaperPlacement, place_paper  # noqa: E402


def _event_id() -> str:
    """Returns the slot-folder suffix (e.g. `"4131"` or `"4131_llm"`).
    Used purely to locate `artifacts/afishathumb/slot_<id>/manifest.json`."""
    val = os.environ.get("AFISHATHUMB_SLOT_EVENT_ID")
    if val is None or not val.strip():
        raise SystemExit("AFISHATHUMB_SLOT_EVENT_ID env var is required")
    return val.strip()


def _setup_lighting() -> None:
    """Daylight setup tuned to keep poster colours saturated (no
    washout). Previous round used a studio-style 4.5-energy sun + huge
    fill which read as overlit / faded; we now use a calmer sun + a
    cooler sky ambient that leaves poster pigments alone."""
    bpy.ops.object.light_add(type="SUN", location=(0, 0, 10))
    key = bpy.context.active_object
    key.name = "Key.Sun"
    key.rotation_euler = (math.radians(58.0), 0.0, math.radians(28.0))
    key.data.energy = 2.4
    key.data.angle = math.radians(3.5)
    key.data.color = (1.0, 0.95, 0.88)  # warmer afternoon sun

    bpy.ops.object.light_add(type="AREA", location=(3.5, -2.5, 2.5))
    fill = bpy.context.active_object
    fill.name = "Fill.Area"
    fill.rotation_euler = (math.radians(80.0), 0.0, math.radians(115.0))
    fill.data.energy = 180.0
    fill.data.size = 4.0
    fill.data.color = (0.78, 0.86, 1.0)  # cool sky-blue ambient

    bpy.ops.object.light_add(type="AREA", location=(2.0, -2.8, 1.3))
    bounce = bpy.context.active_object
    bounce.name = "Bounce.Front"
    bounce.rotation_euler = (math.radians(85.0), 0.0, math.radians(70.0))
    bounce.data.energy = 110.0
    bounce.data.size = 3.0
    bounce.data.color = (0.95, 0.92, 0.86)  # warm ground bounce

    world = bpy.context.scene.world
    if world is None:
        world = bpy.data.worlds.new("World")
        bpy.context.scene.world = world
    world.use_nodes = True
    bg = world.node_tree.nodes.get("Background")
    if bg is not None:
        bg.inputs["Color"].default_value = (0.72, 0.78, 0.84, 1.0)
        bg.inputs["Strength"].default_value = 0.35

    # Keep exposure low enough that bright poster whites don't clip.
    scene = bpy.context.scene
    if hasattr(scene.view_settings, "exposure"):
        scene.view_settings.exposure = -0.20
    if hasattr(scene.view_settings, "look"):
        # Slight contrast to preserve saturation against bright key.
        # Blender 4.5 uses AgX view transforms; pick the closest medium
        # contrast variant available without hard-coding for a single
        # Blender version.
        try:
            scene.view_settings.look = "AgX - Medium High Contrast"
        except (TypeError, AttributeError):
            pass


def _setup_camera(angle_deg: float, focus_z: float, radius: float,
                  lens_mm: float, target_offset_z: float, name: str) -> bpy.types.Object:
    for obj in [o for o in bpy.data.objects if o.type == "CAMERA"]:
        bpy.data.objects.remove(obj, do_unlink=True)
    angle = math.radians(angle_deg)
    cam_loc = Vector(
        (radius * math.cos(angle), radius * math.sin(angle), focus_z)
    )
    target = Vector((0.0, 0.0, focus_z + target_offset_z))
    bpy.ops.object.camera_add(location=cam_loc)
    cam = bpy.context.active_object
    cam.name = name
    cam.data.lens = lens_mm
    cam.data.sensor_width = 36.0
    direction = (target - cam_loc).normalized()
    rot_quat = direction.to_track_quat("-Z", "Y")
    cam.rotation_euler = rot_quat.to_euler()
    bpy.context.scene.camera = cam
    return cam


def _render_to(path: Path, samples: int = 64, res_pct: int = 60) -> None:
    scene = bpy.context.scene
    scene.render.engine = "CYCLES"
    scene.cycles.device = "CPU"
    scene.cycles.samples = samples
    scene.cycles.use_denoising = True
    scene.render.image_settings.file_format = "PNG"
    scene.render.image_settings.color_mode = "RGB"
    scene.render.resolution_x = 1080
    scene.render.resolution_y = 1572
    scene.render.resolution_percentage = res_pct
    scene.render.filepath = str(path)
    bpy.ops.render.render(write_still=True)


def main() -> None:
    event_id = _event_id()
    slot_dir = REPO_ROOT / "artifacts" / "afishathumb" / f"slot_{event_id}"
    manifest_path = slot_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"manifest not found: {manifest_path}")
    with manifest_path.open("r", encoding="utf-8") as f:
        m = json.load(f)

    reset_scene()
    col = build_column(D=1.0)
    body_radius = float(col["metrics"]["body_radius"])  # type: ignore[index]

    for paper in m["papers"]:
        placement = PaperPlacement(
            image_path=Path(paper["image"]),
            anchor_angle_deg=float(paper["anchor_angle_deg"]),
            anchor_z=float(paper["anchor_z"]),
            width=float(paper["width"]),
            height=float(paper["height"]),
            tilt_deg=float(paper.get("tilt_deg", 0.0)),
            peel_corners=tuple(paper.get("peel_corners", (False,)*4)),  # type: ignore[arg-type]
            peel_intensity=float(paper.get("peel_intensity", 1.0)),
            wrinkle=float(paper.get("wrinkle", 0.0)),
            paper_offset=float(paper.get("paper_offset", 0.004)),
            name=str(paper.get("name", "Paper")),
        )
        place_paper(placement, cyl_radius=body_radius, parent=col["root"])  # type: ignore[arg-type]

    _setup_lighting()

    focus_angle = float(m["camera_focus_angle_deg"])
    focus_z = float(m["camera_focus_z"])

    # Compute the 3D world centre of each paper for later screen-space
    # projection. Cylinder body radius is 0.5 + small paper_offset; we
    # ignore wrap curvature because the centre is on the line from axis
    # to the anchor angle.
    body_radius_for_paper = body_radius + 0.005
    paper_world_centers: dict[str, Vector] = {}
    for paper in m["papers"]:
        angle = math.radians(float(paper["anchor_angle_deg"]))
        z = float(paper["anchor_z"])
        paper_world_centers[paper["name"]] = Vector((
            body_radius_for_paper * math.cos(angle),
            body_radius_for_paper * math.sin(angle),
            z,
        ))
    paper_world_centers["cluster"] = Vector((
        body_radius_for_paper * math.cos(math.radians(focus_angle)),
        body_radius_for_paper * math.sin(math.radians(focus_angle)),
        focus_z,
    ))

    # Three beats per slot, mirroring how the camera will actually move
    # in the final flight: a wide cluster-overview when the camera lands,
    # the main poster-lookup beat with the afisha near full-screen, and
    # an info-read close-up on the date+location stickers.

    # Overview camera now sits closer (was 4.2 — cluster was a tiny patch in
    # the frame, hard to overlay readable beat dots on). At 3.0 with a 38mm
    # lens the column still reads "establishing", but the cluster fills
    # 60–70% of the frame so the trace overlay has real space to work.
    overview_cam = _setup_camera(
        angle_deg=focus_angle,
        focus_z=focus_z + 0.10,
        radius=3.0,
        lens_mm=38.0,
        target_offset_z=-0.05,
        name="Cam.Slot.Overview",
    )
    # Force the depsgraph to evaluate the freshly-created camera's
    # matrix_world before projecting — otherwise `world_to_camera_view`
    # returns garbage (x/y in [−25, +25] etc., observed on first run).
    bpy.context.view_layer.update()
    scene = bpy.context.scene
    screen_coords: dict[str, dict] = {}
    for name, wpos in paper_world_centers.items():
        ndc = world_to_camera_view(scene, overview_cam, wpos)
        screen_coords[name] = {
            "x_norm": float(ndc.x),
            "y_norm": 1.0 - float(ndc.y),
            "depth": float(ndc.z),
        }
    with (slot_dir / "screen_coords.json").open("w", encoding="utf-8") as f:
        json.dump(screen_coords, f, ensure_ascii=False, indent=2)
    _render_to(slot_dir / "slot_overview.png", samples=48, res_pct=55)

    # Main lookup beat: poster fits the frame with ~12% headroom; side
    # stickers peek at the edges. Camera distance is precomputed in the
    # manifest based on the poster's actual aspect.
    _setup_camera(
        angle_deg=focus_angle,
        focus_z=focus_z,
        radius=float(m["camera_radius"]),
        lens_mm=float(m["camera_lens_mm"]),
        target_offset_z=float(m["camera_target_offset_z"]),
        name="Cam.Slot.Main",
    )
    _render_to(slot_dir / "slot_main.png", samples=64, res_pct=60)

    # Info-readout beat: pull back ~30% from the main distance and aim at
    # the geometric mid-point of the cluster so both date and location
    # cards stay in frame. Slightly wider lens to keep more cluster in
    # view without falling into "wide overview" territory.
    info_radius = max(1.20, float(m["camera_radius"]) * 0.78)
    _setup_camera(
        angle_deg=focus_angle,
        focus_z=focus_z,
        radius=info_radius,
        lens_mm=55.0,
        target_offset_z=0.0,
        name="Cam.Slot.Info",
    )
    _render_to(slot_dir / "slot_info.png", samples=64, res_pct=60)

    print(f"[slot] rendered overview/main/info for event {event_id}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[slot] FAILED: {exc!r}")
        raise
