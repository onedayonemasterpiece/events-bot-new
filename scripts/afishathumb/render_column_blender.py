"""Phase F1: render the full tour column with ALL events glued in place.

Reads `artifacts/afishathumb/tour_today.json` to get the per-event
`(cylinder_angle_deg, cylinder_z_offset)` placement chosen by LLM-C,
then for each event loads its own slot manifest and shifts every paper
by that placement so the entire tour lives on one shared cylinder.

Renders three reference views:
  - `column_front.png`  — 0° establishing wide shot
  - `column_side.png`   — 120° establishing wide shot
  - `column_back.png`   — 240° establishing wide shot

The three angles together cover all six event clusters so the operator
can confirm the placement before motion is added.
"""

from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path

import bpy  # type: ignore[import-not-found]
from mathutils import Vector  # type: ignore[import-not-found]

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "kaggle" / "AfishaThumb" / "scripts"))

from build_column import build_column, reset_scene  # noqa: E402
from layout_posters import PaperPlacement, place_paper  # noqa: E402


def _slot_dir_for(event_id: int) -> Path:
    """The slot folder we should pull papers from. Pipeline writes
    `slot_<id>_llm` when LLM-A composed the layout; the older
    deterministic-only path writes `slot_<id>`. Prefer the LLM one."""
    art = REPO_ROOT / "artifacts" / "afishathumb"
    llm = art / f"slot_{event_id}_llm"
    if (llm / "manifest.json").exists():
        return llm
    return art / f"slot_{event_id}"


def _setup_lighting() -> None:
    bpy.ops.object.light_add(type="SUN", location=(0, 0, 10))
    key = bpy.context.active_object
    key.name = "Key.Sun"
    key.rotation_euler = (math.radians(58.0), 0.0, math.radians(28.0))
    key.data.energy = 2.4
    key.data.angle = math.radians(3.5)
    key.data.color = (1.0, 0.95, 0.88)

    bpy.ops.object.light_add(type="AREA", location=(3.5, -2.5, 2.5))
    fill = bpy.context.active_object
    fill.name = "Fill.Area"
    fill.rotation_euler = (math.radians(80.0), 0.0, math.radians(115.0))
    fill.data.energy = 180.0
    fill.data.size = 4.0
    fill.data.color = (0.78, 0.86, 1.0)

    bpy.ops.object.light_add(type="AREA", location=(2.0, -2.8, 1.3))
    bounce = bpy.context.active_object
    bounce.name = "Bounce.Front"
    bounce.rotation_euler = (math.radians(85.0), 0.0, math.radians(70.0))
    bounce.data.energy = 110.0
    bounce.data.size = 3.0
    bounce.data.color = (0.95, 0.92, 0.86)

    world = bpy.context.scene.world
    if world is None:
        world = bpy.data.worlds.new("World")
        bpy.context.scene.world = world
    world.use_nodes = True
    bg = world.node_tree.nodes.get("Background")
    if bg is not None:
        bg.inputs["Color"].default_value = (0.72, 0.78, 0.84, 1.0)
        bg.inputs["Strength"].default_value = 0.35

    scene = bpy.context.scene
    if hasattr(scene.view_settings, "exposure"):
        scene.view_settings.exposure = -0.20
    if hasattr(scene.view_settings, "look"):
        try:
            scene.view_settings.look = "AgX - Medium High Contrast"
        except (TypeError, AttributeError):
            pass


def _setup_camera(angle_deg: float, focus_z: float, radius: float,
                  lens_mm: float, name: str) -> bpy.types.Object:
    for obj in [o for o in bpy.data.objects if o.type == "CAMERA"]:
        bpy.data.objects.remove(obj, do_unlink=True)
    angle = math.radians(angle_deg)
    cam_loc = Vector(
        (radius * math.cos(angle), radius * math.sin(angle), focus_z)
    )
    target = Vector((0.0, 0.0, focus_z - 0.05))
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


def _render_to(path: Path, samples: int = 56, res_pct: int = 55) -> None:
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
    art = REPO_ROOT / "artifacts" / "afishathumb"
    tour_path = art / "tour_today.json"
    if not tour_path.exists():
        raise SystemExit(f"tour plan not found: {tour_path}")
    with tour_path.open("r", encoding="utf-8") as f:
        tour = json.load(f)

    reset_scene()
    col = build_column(D=1.0)
    body_radius = float(col["metrics"]["body_radius"])  # type: ignore[index]

    # Each per-event slot was authored with its cluster centered on
    # anchor_z=1.65. To pack all 6 events on one cylinder the planner
    # gives us `cylinder_z_offset` (signed shift relative to that
    # 1.65 center) and `cylinder_angle_deg` (the spoke direction).
    paper_count = 0
    placed_log: list[dict] = []
    for slot in tour["slots"]:
        event_id = int(slot["event_id"])
        angle_offset = float(slot.get("cylinder_angle_deg", 0.0))
        z_offset = float(slot.get("cylinder_z_offset", 0.0))
        slot_dir = _slot_dir_for(event_id)
        manifest_path = slot_dir / "manifest.json"
        if not manifest_path.exists():
            print(f"[column] WARN: missing manifest for event {event_id} at {manifest_path}")
            continue
        with manifest_path.open("r", encoding="utf-8") as f:
            m = json.load(f)
        for paper in m["papers"]:
            placement = PaperPlacement(
                image_path=Path(paper["image"]),
                anchor_angle_deg=float(paper["anchor_angle_deg"]) + angle_offset,
                anchor_z=float(paper["anchor_z"]) + z_offset,
                width=float(paper["width"]),
                height=float(paper["height"]),
                tilt_deg=float(paper.get("tilt_deg", 0.0)),
                peel_corners=tuple(paper.get("peel_corners", (False,)*4)),  # type: ignore[arg-type]
                peel_intensity=float(paper.get("peel_intensity", 1.0)),
                wrinkle=float(paper.get("wrinkle", 0.0)),
                paper_offset=float(paper.get("paper_offset", 0.004)),
                name=f"{paper.get('name', 'Paper')}.E{event_id}",
            )
            place_paper(placement, cyl_radius=body_radius, parent=col["root"])  # type: ignore[arg-type]
            paper_count += 1
        placed_log.append({
            "event_id": event_id,
            "cylinder_angle_deg": angle_offset,
            "cylinder_z_offset": z_offset,
            "papers": len(m["papers"]),
        })

    _setup_lighting()

    # Three establishing shots 120° apart, each catching two clusters.
    # Wide lens + tall radius so the whole 2.7 D body fits with the
    # cornice and dome visible — the operator wants to see the column
    # as the viewer will see it, not a cropped poster wall.
    focus_z = 1.65
    for label, view_angle_deg in [("front", 0.0), ("side", 120.0), ("back", 240.0)]:
        _setup_camera(
            angle_deg=view_angle_deg,
            focus_z=focus_z,
            radius=5.6,
            lens_mm=42.0,
            name=f"Cam.Column.{label}",
        )
        _render_to(art / f"column_{label}.png", samples=56, res_pct=55)

    with (art / "column_placement.json").open("w", encoding="utf-8") as f:
        json.dump({"slots": placed_log, "paper_count": paper_count}, f,
                  ensure_ascii=False, indent=2)

    print(f"[column] placed {paper_count} papers across {len(placed_log)} events")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[column] FAILED: {exc!r}")
        raise
