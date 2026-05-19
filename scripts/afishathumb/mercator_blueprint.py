"""Mercator-style flat blueprint of the AfishaThumb cylinder.

Unrolls the body cylinder into a flat 2D map (X = angle 0..360°, Y = z
in D-units) and renders:

  - every event's papers (poster + extras + stickers) at the absolute
    (cylinder_angle, z) position assigned by LLM-C,
  - per-event LLM-B camera beats as numbered dots,
  - inter-scene transitions as arcs connecting consecutive events,
  - a sidebar with the visit order + brief narrative per scene.

This is the operator's "blueprint approval" view — looked at BEFORE
the 3D cylinder render of the full tour.

Inputs (from `artifacts/afishathumb/`):
  - `selection_today.json` — the 6 events picked today
  - `tour_today.json`      — LLM-C tour plan
  - `slot_<id>_llm/manifest.json` for each event
  - poster + sticker PNGs inside each `slot_<id>_llm/`

Output:
  - `artifacts/afishathumb/mercator_today.png`
"""

from __future__ import annotations

import json
import math
import sys
from typing import Optional
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnnotationBbox, OffsetImage
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[2]
ART = REPO_ROOT / "artifacts" / "afishathumb"

sys.path.insert(0, str(REPO_ROOT / "kaggle" / "AfishaThumb" / "scripts"))
from tour_llm import plan_tour  # noqa: E402


CYL_RADIUS = 0.5
ARC_FULL_D = 2.0 * math.pi * CYL_RADIUS  # ~3.1416  full unrolled length

def _arc_deg(width_d: float) -> float:
    return math.degrees(width_d / CYL_RADIUS)  # cylinder body radius = 0.5

def _deg_to_arclen_d(angle_deg: float) -> float:
    """Convert an angle on the cylinder to its arc length in D-units
    along the unrolled surface."""
    return math.radians(angle_deg) * CYL_RADIUS


def _build_event_brief_from_manifest(slot_dir: Path) -> dict:
    m = json.loads((slot_dir / "manifest.json").read_text(encoding="utf-8"))
    scene_dur = sum(float(b.get("dwell_s", 0)) for b in m.get("beats", []))
    a_min, a_max, z_min, z_max = _cluster_extent_deg(m)
    return {
        "event_id": int(m["event_id"]),
        "title": m["title"],
        "date": m["date_iso"],
        "time": m["time_text"],
        "cost_text": ("БЕСПЛАТНО" if m.get("is_free") else (m.get("price_text") or "уточняется")),
        "hook_short": (m.get("search_digest") or "")[:60],
        "scene_duration_s": scene_dur or 5.5,
        "is_promo": False,
        "cluster_arc_deg": a_max - a_min,
        "cluster_h_d": z_max - z_min,
        "cluster_v_center_offset": ((z_min + z_max) / 2.0) - 1.65,
    }


def _draw_paper(ax, image_path: Path, x_centre: float, y_centre: float,
                w_x: float, h_y: float, tilt_deg: float = 0.0, zorder: int = 5) -> None:
    """Draw the paper image at (x_centre, y_centre) in axis coords.
    Y axis grows DOWNWARD on the mercator canvas (z high = top).
    """
    try:
        img = Image.open(image_path).convert("RGBA")
    except Exception:
        return
    extent = (x_centre - w_x / 2, x_centre + w_x / 2,
              y_centre - h_y / 2, y_centre + h_y / 2)
    # `aspect="equal"` (matching the axis) keeps image pixels square in
    # data coords — so the source image is never stretched even when the
    # figure is wide. `aspect="auto"` previously let matplotlib stretch
    # images to fill extent in figure-pixel space, which read as
    # horizontal stretching of faces and text on the unrolled blueprint.
    ax.imshow(img, extent=extent, aspect="equal", zorder=zorder, alpha=0.95)
    # Outline so paper edges read against the column body.
    ax.add_patch(mpatches.Rectangle(
        (extent[0], extent[2]), w_x, h_y,
        fill=False, edgecolor="#16161A", linewidth=0.8, zorder=zorder + 1,
    ))


def _cluster_extent_deg(manifest: dict) -> tuple[float, float, float, float]:
    """Returns (min_angle, max_angle, min_z, max_z) over all papers."""
    a_min = a_max = z_min = z_max = None
    for p in manifest["papers"]:
        a = p["anchor_angle_deg"]
        z = p["anchor_z"]
        half_arc = _arc_deg(p["width"]) / 2.0
        half_h = p["height"] / 2.0
        l = a - half_arc; r = a + half_arc
        b = z - half_h; t = z + half_h
        a_min = l if a_min is None else min(a_min, l)
        a_max = r if a_max is None else max(a_max, r)
        z_min = b if z_min is None else min(z_min, b)
        z_max = t if z_max is None else max(z_max, t)
    return a_min or 0.0, a_max or 0.0, z_min or 0.0, z_max or 0.0


def render_mercator() -> Path:
    selection = json.loads((ART / "selection_today.json").read_text(encoding="utf-8"))

    # 1) Gather event briefs from cached per-slot manifests.
    briefs: list[dict] = []
    slot_dirs: dict[int, Path] = {}
    manifests: dict[int, dict] = {}
    for ev in selection:
        sd = ART / f"slot_{ev['id']}_llm"
        if not (sd / "manifest.json").exists():
            print(f"[mercator] skip {ev['id']} — no manifest yet")
            continue
        briefs.append(_build_event_brief_from_manifest(sd))
        slot_dirs[int(ev["id"])] = sd
        manifests[int(ev["id"])] = json.loads((sd / "manifest.json").read_text(encoding="utf-8"))

    if not briefs:
        raise SystemExit("no cached LLM manifests; run llm_full_pipeline.py first")

    # 2) Plan the multi-scene tour with LLM-C.
    tour = plan_tour(briefs, cache_path=ART / "tour_today.json", force_refresh=True)
    if not tour.slots:
        raise SystemExit("LLM-C returned no tour")
    print(f"[mercator] LLM-C narrative: {tour.narrative}")

    # Validate LLM-C placement: bounding boxes must NOT overlap.
    # If overlap detected, fall back to a deterministic 2D packer that
    # honours LLM-C visit ORDER but recomputes (angle, z) so events fit.
    extents: dict[int, tuple[float, float, float, float]] = {}
    for eid, m in manifests.items():
        extents[eid] = _cluster_extent_deg(m)

    def _llm_bboxes_overlap(slots) -> list[tuple[int, int]]:
        boxes = []
        for s in slots:
            if s.event_id not in extents:
                continue
            a_min, a_max, z_min, z_max = extents[s.event_id]
            half_arc = (a_max - a_min) / 2.0
            half_h = (z_max - z_min) / 2.0
            cx = s.cylinder_angle_deg
            cz = 1.65 + s.cylinder_z_offset
            boxes.append((s.event_id, cx - half_arc, cx + half_arc, cz - half_h, cz + half_h))
        bad: list[tuple[int, int]] = []
        for i in range(len(boxes)):
            for j in range(i + 1, len(boxes)):
                a = boxes[i]; b = boxes[j]
                # consider wrap-around at 360°
                arc_overlap = max(0.0, min(a[2], b[2]) - max(a[1], b[1]))
                z_overlap = max(0.0, min(a[4], b[4]) - max(a[3], b[3]))
                if arc_overlap > 5.0 and z_overlap > 0.10:
                    bad.append((a[0], b[0]))
        return bad

    overlaps = _llm_bboxes_overlap(tour.slots)
    if overlaps:
        print(f"[mercator] LLM-C produced {len(overlaps)} overlapping pairs; running deterministic packer")
        # Deterministic packer: try 2-row first, then 3-row, picking
        # whichever fits without overlap. Each row uses the cluster's
        # natural width + ANG_GAP° gap and pre-checks if 360° / row count
        # has enough room for all events on that row.
        order_sorted = sorted(tour.slots, key=lambda s: s.visit_order)

        def _pack_rows(n_rows: int, gap_deg: float) -> bool:
            """Mutates tour.slots in-place. Returns True if fits.
            Round-13 update: pack CONSECUTIVE visits into the SAME row
            instead of alternating. With visits 1..N split across rows
            as [1,2,3] upper + [4,5,6] lower the camera sweeps each row
            cleanly, no zigzag between z-tiers.
            """
            if n_rows == 2:
                z_offsets = [+0.55, -0.55]
            elif n_rows == 3:
                z_offsets = [+0.70, 0.0, -0.70]
            else:
                z_offsets = [+0.80, +0.27, -0.27, -0.80]
            # Split visits in continuous chunks per row.
            n = len(order_sorted)
            chunk = (n + n_rows - 1) // n_rows
            chunks: list[list] = [order_sorted[i:i + chunk] for i in range(0, n, chunk)]
            for row, chunk_slots in enumerate(chunks):
                cursor = 0.0
                for s in chunk_slots:
                    if s.event_id not in extents:
                        continue
                    a_min, a_max, _, _ = extents[s.event_id]
                    arc = a_max - a_min
                    if cursor + arc + gap_deg > 360.0:
                        return False
                    s.cylinder_z_offset = z_offsets[row]
                    cluster_local_centre = (a_min + a_max) / 2.0
                    left_edge = cursor + gap_deg / 2.0
                    new_centre = left_edge + (cluster_local_centre - a_min)
                    s.cylinder_angle_deg = new_centre % 360.0
                    cursor = left_edge + arc + gap_deg / 2.0
            return True

        for n_rows in (2, 3, 4):
            if _pack_rows(n_rows, gap_deg=12.0):
                print(f"[mercator] packed into {n_rows} z-rows")
                break
        else:
            print("[mercator] could not pack 6 events even in 4 rows — clusters too wide")
        still = _llm_bboxes_overlap(tour.slots)
        if still:
            print(f"[mercator] residual overlaps after pack: {len(still)}")
    else:
        print("[mercator] LLM-C 2D packing passed overlap check")

    # 3) Render the blueprint.
    # X axis = arc length in D-units (0..ARC_FULL_D ≈ 3.14). With
    # `aspect="equal"` this guarantees identical visual scale between
    # the unrolled blueprint and the 3D render (1 D on the cylinder
    # surface = 1 D on the canvas, both horizontally and vertically).
    # Figure aspect matches the unrolled cylinder data aspect (≈1.12)
    # plus the sidebar — so `aspect="equal"` on the axes shows the
    # blueprint without compression or stretching.
    fig = plt.figure(figsize=(16, 11), facecolor="#16161A")
    gs = fig.add_gridspec(1, 2, width_ratios=[1.0, 0.30])
    ax = fig.add_subplot(gs[0, 0])
    ax.set_facecolor("#1F1F23")
    ax.set_xlim(0, ARC_FULL_D)
    ax.set_ylim(0.0, 2.8)
    ax.set_aspect("equal")
    ax.set_xlabel("Длина арки вокруг тумбы (D)", color="#D9D5C8")
    ax.set_ylabel("Высота z (D)", color="#D9D5C8")
    # Top secondary axis with degrees for the operator.
    def _arc_to_deg(arc):
        import numpy as np  # noqa: WPS433
        return arc / CYL_RADIUS * (180.0 / math.pi)
    def _deg_to_arc(deg):
        return deg / (180.0 / math.pi) * CYL_RADIUS
    secax = ax.secondary_xaxis("top", functions=(_arc_to_deg, _deg_to_arc))
    secax.set_xlabel("Угол (°)", color="#D9D5C8")
    secax.tick_params(colors="#9FA3AB")
    ax.tick_params(colors="#9FA3AB")
    for spine in ax.spines.values():
        spine.set_color("#3A3530")
    ax.grid(True, linestyle=":", alpha=0.25, color="#7A7060")

    # Body painted bands (visual cue of where on the column we sit).
    ax.axhspan(0.00, 0.18, facecolor="#2A3B2C", alpha=0.25, zorder=1)   # base
    ax.axhspan(2.68, 2.80, facecolor="#3A4C3F", alpha=0.25, zorder=1)   # cornice band

    # Draw each event's cluster region (background band) + papers,
    # shifted by BOTH the LLM-C angular slot AND the z_offset.
    event_palette = ["#1B2D5E", "#0E7A6B", "#9F2933", "#9F8B1F", "#7A3F8B", "#306B5C"]
    for idx, slot in enumerate(tour.slots):
        if slot.event_id not in slot_dirs:
            continue
        sd = slot_dirs[slot.event_id]
        m = manifests[slot.event_id]
        offset_deg = slot.cylinder_angle_deg
        z_offset = slot.cylinder_z_offset
        # Region rectangle so it's visible where the event lives.
        a_min, a_max, z_min, z_max = extents[slot.event_id]
        region_w_deg = a_max - a_min
        region_h = z_max - z_min
        # Shift extent to the slot. Convert to D-arc units for X.
        region_x = _deg_to_arclen_d(offset_deg - region_w_deg / 2.0)
        region_w = _deg_to_arclen_d(region_w_deg)
        region_y = (z_min + z_offset)
        ax.add_patch(mpatches.Rectangle(
            (region_x, region_y), region_w, region_h,
            facecolor=event_palette[idx % len(event_palette)],
            edgecolor="#FFFCF4", alpha=0.10, linewidth=1.2,
            zorder=2,
        ))
        # Draw papers within the region.
        for paper in m["papers"]:
            ang_centre = (paper["anchor_angle_deg"] + offset_deg) % 360
            x = ang_centre
            y = paper["anchor_z"] + z_offset
            h_y = paper["height"]
            _draw_paper(ax, Path(paper["image"]), _deg_to_arclen_d(x), y, paper["width"], h_y,
                        tilt_deg=paper.get("tilt_deg", 0.0))
        # Big visible event label BELOW the cluster on the canvas itself.
        label_y = region_y - 0.10
        if label_y < 0.05:
            label_y = region_y + region_h + 0.10
        ax.text(_deg_to_arclen_d(offset_deg), label_y,
                f"#{slot.visit_order} · {m['title'][:30]}",
                color="#FFFCF4", fontsize=10, fontweight="bold",
                ha="center", va="top" if label_y > region_y else "bottom",
                bbox=dict(facecolor=event_palette[idx % len(event_palette)],
                          edgecolor="#FFFCF4", lw=0.8, alpha=0.95, pad=4))

    # Camera path: visit slots in order. Beats inside ONE event are
    # connected by thin blue arrows; transitions BETWEEN events get the
    # transit-type colour from LLM-C.
    order_sorted = sorted(tour.slots, key=lambda s: s.visit_order)
    last_event_exit_point: tuple[float, float] | None = None
    last_event_id: Optional[int] = None
    beat_idx_global = 0
    transit_color = {
        "slow_ease": "#5DBB66",
        "fast_flyby": "#E0A82E",
        "crane_up": "#E04640",
        "crane_down": "#E04640",
    }
    transitions_by_pair = {
        (t.from_event, t.to_event): t for t in tour.transitions
    }
    for slot in order_sorted:
        if slot.event_id not in slot_dirs:
            continue
        m = manifests[slot.event_id]
        offset_deg = slot.cylinder_angle_deg
        z_offset = slot.cylinder_z_offset
        first_pt_of_event = None
        last_pt_of_event = None
        for b in m.get("beats", []):
            beat_idx_global += 1
            x = _deg_to_arclen_d((b["angle_deg"] + offset_deg) % 360)
            y = b["z"] + z_offset
            color = "#E04640" if b.get("is_focus_push") else "#7BC4E2"
            ax.scatter([x], [y], s=110 + 60 * float(b.get("dwell_s", 0.5)),
                       facecolor=color, edgecolor="#FFFCF4", linewidths=1.0,
                       zorder=20)
            ax.text(x, y, str(beat_idx_global), ha="center", va="center",
                    color="#FFFCF4", fontsize=7.5, fontweight="bold", zorder=21)
            if last_pt_of_event is not None:
                ax.annotate("", xy=(x, y), xytext=last_pt_of_event,
                            arrowprops=dict(arrowstyle="->", color="#7BC4E2",
                                            lw=1.2, alpha=0.7,
                                            connectionstyle="arc3,rad=0.04"),
                            zorder=15)
            if first_pt_of_event is None:
                first_pt_of_event = (x, y)
            last_pt_of_event = (x, y)
        # Inter-event transition arrow from previous event's exit to
        # this event's first beat, coloured by LLM-C's transit type.
        if (last_event_exit_point is not None and first_pt_of_event is not None
                and last_event_id is not None):
            tr = transitions_by_pair.get((last_event_id, slot.event_id))
            t_type = tr.transit if tr else "slow_ease"
            color = transit_color.get(t_type, "#5DBB66")
            ls = (0, (4, 3)) if t_type == "fast_flyby" else "-"
            lw = 2.4 if t_type != "fast_flyby" else 1.4
            ax.annotate("", xy=first_pt_of_event, xytext=last_event_exit_point,
                        arrowprops=dict(arrowstyle="->", color=color, lw=lw,
                                        ls=ls, alpha=0.95,
                                        connectionstyle="arc3,rad=0.20"),
                        zorder=16)
        last_event_exit_point = last_pt_of_event
        last_event_id = slot.event_id

    ax.set_title(
        "AfishaThumb · Mercator-развёртка тумбы · сегодняшняя выборка 6 событий\n"
        f"LLM-C: {tour.narrative}",
        color="#F2C14E", fontsize=12, pad=10,
    )

    # Sidebar with the visit order + scene briefs.
    side = fig.add_subplot(gs[0, 1])
    side.set_facecolor("#1F1F23")
    side.set_xticks([]); side.set_yticks([])
    for spine in side.spines.values():
        spine.set_color("#3A3530")
    side.set_xlim(0, 1); side.set_ylim(0, 1)
    y = 0.97
    side.text(0.04, y, "Маршрут камеры (порядок)", color="#F2C14E",
              fontsize=13, fontweight="bold", va="top",
              transform=side.transAxes)
    y -= 0.07
    for slot in order_sorted:
        brief = next((b for b in briefs if b["event_id"] == slot.event_id), None)
        if not brief:
            continue
        side.text(0.04, y,
                  f"#{slot.visit_order} • {brief['event_id']} • {slot.cylinder_angle_deg:5.0f}°",
                  color="#F2C14E", fontsize=10, fontweight="bold",
                  transform=side.transAxes, va="top")
        side.text(0.04, y - 0.025, f"  {brief['title'][:46]}",
                  color="#D9D5C8", fontsize=9, transform=side.transAxes, va="top")
        side.text(0.04, y - 0.045,
                  f"  {brief['date']} {brief['time']} · {brief['cost_text']} · сцена ≈ {brief['scene_duration_s']:.1f}с",
                  color="#9FA3AB", fontsize=8.5, transform=side.transAxes, va="top")
        y -= 0.085
    y -= 0.01
    side.text(0.04, y, "Обозначения:", color="#9FA3AB", fontsize=10,
              transform=side.transAxes, va="top")
    y -= 0.025
    legend = [
        ("● синяя точка — beat внутри сцены (LLM-B)", "#7BC4E2"),
        ("● красная — focus push (читаем инфо)", "#E04640"),
        ("→ зелёная линия — slow ease между событиями", "#5DBB66"),
        ("→ оранжевый пунктир — fast fly-by", "#E0A82E"),
    ]
    for txt, col in legend:
        side.text(0.04, y, txt, color=col, fontsize=8.5,
                  transform=side.transAxes, va="top")
        y -= 0.022

    fig.tight_layout()
    out = ART / "mercator_today.png"
    fig.savefig(out, dpi=130, facecolor="#16161A")
    plt.close(fig)
    return out


if __name__ == "__main__":
    print(f"[mercator] wrote {render_mercator()}")
