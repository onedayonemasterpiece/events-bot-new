"""Render the AfishaThumb camera-flight plan diagram.

Produces a single PNG (`artifacts/afishathumb/camera_plan.png`) that
visualises one full release as a "carta of meanings" — six slots glued
around the cylindrical column plus intro/outro, with the camera path
laid over them. The diagram uses two synchronised views:

  - top-down (XY): the column is a circle at origin; slot anchors sit
    on its circumference; the camera path is a polyline that orbits +
    pushes in / out around each slot.
  - side (height vs time): vertical camera moves as a function of beat
    index, so it's clear when the camera rides up to a high promo slot
    and drops to read a low one.

Annotations encode "what the camera is doing":
  - a filled circle on a path node = a dwell moment (focus, slow read).
    Circle radius scales with dwell duration.
  - a hollow ring around a dwell node = a strong "focus push" (camera
    moves closer than the cluster radius — used for info-readout beats
    where the poster does not fit and information must be read).
  - thick line segment = slow eased camera move (between adjacent slots
    or inside a slot).
  - thin dashed line = fast fly-by (long traversal around the cylinder,
    no info read on the way).
  - red ring + "★" badge = promo slot («обратите внимание»). Per the
    requirements: never first, never two in a row — visualised here as
    the second-anchor slot only.

Run:
    .venv/bin/python scripts/afishathumb/camera_plan.py
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, Circle, Wedge

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = REPO_ROOT / "artifacts" / "afishathumb"


@dataclass
class Beat:
    label: str        # short caption rendered at the node
    angle_deg: float  # circumferential position on the column
    radius: float     # camera-to-axis distance (cluster = ~2.2, focus = ~1.4)
    height: float     # camera z on the column (in D-units)
    dwell: float      # seconds; 0 = transit waypoint
    is_focus_push: bool = False
    is_promo: bool = False
    is_intro: bool = False
    is_outro: bool = False
    transit: str = "ease"   # "ease" (thick solid) or "flyby" (thin dashed)


# Plan: 6 main slots at angular positions { 0, 55, 130, 190, 250, 310 } around
# the cylinder, plus an intro wide and a two-sticker outro. Slot 2 carries
# the promo «обратите внимание» tag (second position, per requirements).
SLOT_ANGLES = (0.0, 55.0, 130.0, 190.0, 250.0, 310.0)
SLOT_HEIGHTS = (1.65, 2.05, 1.45, 1.78, 1.85, 1.55)

BEATS: list[Beat] = [
    # --- Intro: wide establishing shot, column fills almost the full frame
    Beat("intro · широкий", angle_deg=-90.0, radius=6.5, height=2.10,
         dwell=2.0, is_intro=True, transit="ease"),
    # --- Slot 1 (angle 0°)
    Beat("1 · landing", angle_deg=SLOT_ANGLES[0] - 14, radius=4.2,
         height=SLOT_HEIGHTS[0] + 0.35, dwell=0.8, transit="ease"),
    Beat("1 · main", angle_deg=SLOT_ANGLES[0], radius=2.2,
         height=SLOT_HEIGHTS[0], dwell=2.1, transit="ease"),
    Beat("1 · info push", angle_deg=SLOT_ANGLES[0] + 3, radius=1.4,
         height=SLOT_HEIGHTS[0] + 0.10, dwell=1.6, is_focus_push=True,
         transit="ease"),
    # --- Slot 2 (promo, adjacent angle 55°)
    Beat("2 · promo · «обратите внимание»",
         angle_deg=SLOT_ANGLES[1], radius=2.2, height=SLOT_HEIGHTS[1],
         dwell=2.3, is_promo=True, transit="ease"),
    Beat("2 · info push", angle_deg=SLOT_ANGLES[1] + 3, radius=1.35,
         height=SLOT_HEIGHTS[1] - 0.05, dwell=1.6, is_focus_push=True,
         transit="ease"),
    # --- Fast fly-by past angle 90°, around to slot 3 (angle 130°)
    Beat("→ облёт", angle_deg=90.0, radius=3.4, height=1.95,
         dwell=0.0, transit="flyby"),
    Beat("3 · main", angle_deg=SLOT_ANGLES[2], radius=2.2,
         height=SLOT_HEIGHTS[2], dwell=2.0, transit="flyby"),
    Beat("3 · info push", angle_deg=SLOT_ANGLES[2] + 3, radius=1.4,
         height=SLOT_HEIGHTS[2] - 0.05, dwell=1.5, is_focus_push=True,
         transit="ease"),
    # --- Slow ease through angle 170° to slot 4 (angle 190°)
    Beat("4 · main", angle_deg=SLOT_ANGLES[3], radius=2.2,
         height=SLOT_HEIGHTS[3], dwell=2.0, transit="ease"),
    Beat("4 · info push", angle_deg=SLOT_ANGLES[3] + 4, radius=1.4,
         height=SLOT_HEIGHTS[3] + 0.05, dwell=1.4, is_focus_push=True,
         transit="ease"),
    # --- Fast fly-by past angle 220° to slot 5 (angle 250°)
    Beat("→ облёт", angle_deg=220.0, radius=3.2, height=1.80,
         dwell=0.0, transit="flyby"),
    Beat("5 · main", angle_deg=SLOT_ANGLES[4], radius=2.2,
         height=SLOT_HEIGHTS[4], dwell=2.0, transit="ease"),
    Beat("5 · info push", angle_deg=SLOT_ANGLES[4] + 3, radius=1.45,
         height=SLOT_HEIGHTS[4] - 0.08, dwell=1.4, is_focus_push=True,
         transit="ease"),
    # --- Slot 6 (angle 310°)
    Beat("6 · main", angle_deg=SLOT_ANGLES[5], radius=2.2,
         height=SLOT_HEIGHTS[5], dwell=2.0, transit="ease"),
    Beat("6 · info push", angle_deg=SLOT_ANGLES[5] + 3, radius=1.4,
         height=SLOT_HEIGHTS[5] + 0.05, dwell=1.4, is_focus_push=True,
         transit="ease"),
    # --- Outro: pull back, two side-by-side stickers stay co-visible
    Beat("outro · «Полюбить КГД Анонсы» + «Мост в Кёнигсберг»",
         angle_deg=345.0, radius=2.6, height=1.30, dwell=2.4,
         is_outro=True, transit="ease"),
]


def _xy(angle_deg: float, radius: float) -> tuple[float, float]:
    a = math.radians(angle_deg)
    return radius * math.cos(a), radius * math.sin(a)


def _arc_path(b0: Beat, b1: Beat, n: int = 24) -> list[tuple[float, float]]:
    """Interpolate the camera path between two beats. We sweep the angle
    (mod 360, choosing the shorter direction) and lerp the radius/height
    linearly, so the path naturally orbits the cylinder."""
    a0, a1 = b0.angle_deg % 360.0, b1.angle_deg % 360.0
    # Always pick the shorter circumferential direction.
    delta = (a1 - a0 + 540) % 360 - 180
    pts: list[tuple[float, float]] = []
    for i in range(n + 1):
        t = i / n
        a = a0 + delta * t
        r = b0.radius * (1 - t) + b1.radius * t
        pts.append(_xy(a, r))
    return pts


def render_plan(out_path: Path) -> Path:
    fig, (ax_top, ax_side) = plt.subplots(
        1, 2, figsize=(18, 10),
        gridspec_kw={"width_ratios": [1.4, 1.0]},
        facecolor="#F5F2EA",
    )
    fig.suptitle(
        "AfishaThumb · карта рассказа: 6 слотов вокруг тумбы + intro + outro",
        fontsize=16, fontweight="bold", color="#16161A",
    )

    # ---------- TOP-DOWN VIEW ----------
    ax_top.set_aspect("equal")
    ax_top.set_facecolor("#F5F2EA")
    ax_top.set_xlim(-7.5, 7.5)
    ax_top.set_ylim(-7.5, 7.5)
    ax_top.set_title("Вид сверху · орбита камеры вокруг тумбы (D = 1)",
                     fontsize=12, color="#2A2A30")

    # Column footprint (radius 0.5 in D-units).
    column = Circle((0, 0), radius=0.5, facecolor="#2A3B2C",
                    edgecolor="#10180E", linewidth=2.0, zorder=2)
    ax_top.add_patch(column)
    # Cluster zone — the band of comfortable "main" radii.
    for r in (2.2, 1.4):
        ax_top.add_patch(Circle((0, 0), radius=r, facecolor="none",
                                edgecolor="#D9D5C8", linewidth=0.8,
                                linestyle=":", zorder=1))

    # Slot anchor markers on the cylinder surface.
    for i, a in enumerate(SLOT_ANGLES, 1):
        x, y = _xy(a, 0.5)
        nx, ny = _xy(a, 0.85)
        ax_top.plot([x, nx], [y, ny], color="#16161A", linewidth=1.2, zorder=3)
        ax_top.text(nx * 1.18, ny * 1.18, f"slot {i}",
                    ha="center", va="center", fontsize=10,
                    color="#16161A", fontweight="bold", zorder=4)

    # Camera path: walk through beats, draw segments.
    for b0, b1 in zip(BEATS[:-1], BEATS[1:]):
        pts = _arc_path(b0, b1)
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        if b1.transit == "flyby":
            ax_top.plot(xs, ys, color="#7A7060", linewidth=1.0,
                        linestyle=(0, (3, 3)), zorder=5)
        else:
            ax_top.plot(xs, ys, color="#1B2D5E", linewidth=2.2,
                        alpha=0.9, zorder=5)

    # Beat nodes.
    for b in BEATS:
        x, y = _xy(b.angle_deg, b.radius)
        node_color = "#1B2D5E"
        edge_color = "#1B2D5E"
        if b.is_intro:
            node_color = "#0E7A6B"
            edge_color = "#0E7A6B"
        if b.is_outro:
            node_color = "#9F2933"
            edge_color = "#9F2933"
        if b.is_promo:
            node_color = "#E0A82E"
            edge_color = "#9F2933"

        radius = max(0.05, 0.05 + 0.08 * b.dwell)
        ax_top.add_patch(Circle((x, y), radius=radius,
                                facecolor=node_color, edgecolor=edge_color,
                                linewidth=1.4, zorder=6))
        if b.is_focus_push:
            ax_top.add_patch(Circle((x, y), radius=radius + 0.10,
                                    facecolor="none", edgecolor="#9F2933",
                                    linewidth=1.8, linestyle="-", zorder=7))
        if b.is_promo:
            ax_top.text(x, y - 0.02, "★", color="#9F2933",
                        ha="center", va="center", fontsize=12,
                        fontweight="bold", zorder=8)

        # Label placement: push outward along the radial.
        lx = x * 1.18 + (0.6 if abs(x) < 0.3 else 0.0)
        ly = y * 1.18
        ax_top.text(lx, ly, b.label, color="#16161A", fontsize=8.5,
                    ha="center", va="center",
                    bbox=dict(facecolor="#FFFCF4", edgecolor="none",
                              alpha=0.85, pad=2.0),
                    zorder=9)

    # Top-down legend.
    legend_elems = [
        Line2D([0], [0], color="#1B2D5E", linewidth=2.2,
               label="slow ease (между соседями / внутри слота)"),
        Line2D([0], [0], color="#7A7060", linewidth=1.4,
               linestyle=(0, (3, 3)),
               label="fast fly-by (длинный перелёт без чтения)"),
        Circle((0, 0), 0.3, facecolor="#1B2D5E", edgecolor="#1B2D5E",
               label="dwell · фокус (радиус = время)"),
        Circle((0, 0), 0.3, facecolor="none", edgecolor="#9F2933",
               linewidth=1.8, label="focus push (info readout, ближе чем main)"),
        Circle((0, 0), 0.3, facecolor="#E0A82E", edgecolor="#9F2933",
               linewidth=1.4, label="promo слот · «обратите внимание»"),
        Circle((0, 0), 0.3, facecolor="#0E7A6B", edgecolor="#0E7A6B",
               label="intro / outro"),
    ]
    ax_top.legend(handles=legend_elems, loc="lower left",
                  fontsize=9, frameon=True, facecolor="#FFFCF4",
                  edgecolor="#D9D5C8")

    # ---------- SIDE VIEW: HEIGHT × BEAT TIME ----------
    ax_side.set_facecolor("#F5F2EA")
    ax_side.set_title("Профиль высоты камеры по таймлайну",
                      fontsize=12, color="#2A2A30")
    ax_side.set_xlabel("beat #", fontsize=10)
    ax_side.set_ylabel("z (D-units)", fontsize=10)

    # Column z-extents.
    ax_side.axhspan(0, 0.18, facecolor="#1B2620", alpha=0.30, label="цоколь")
    ax_side.axhspan(0.18, 2.68, facecolor="#2A3B2C", alpha=0.18, label="тело")
    ax_side.axhspan(2.68, 3.50, facecolor="#3A4C3F", alpha=0.18, label="карниз")
    ax_side.axhspan(3.50, 4.42, facecolor="#6C8E75", alpha=0.18, label="купол + шпиль")

    xs = list(range(len(BEATS)))
    ys = [b.height for b in BEATS]

    # Segment styling: separate ease vs flyby.
    for i in range(len(BEATS) - 1):
        seg_color = "#7A7060" if BEATS[i + 1].transit == "flyby" else "#1B2D5E"
        seg_ls = (0, (3, 3)) if BEATS[i + 1].transit == "flyby" else "-"
        seg_lw = 1.2 if BEATS[i + 1].transit == "flyby" else 2.0
        ax_side.plot([xs[i], xs[i + 1]], [ys[i], ys[i + 1]],
                     color=seg_color, linewidth=seg_lw, linestyle=seg_ls)

    for x, y, b in zip(xs, ys, BEATS):
        node_color = "#1B2D5E"
        if b.is_intro:
            node_color = "#0E7A6B"
        elif b.is_outro:
            node_color = "#9F2933"
        elif b.is_promo:
            node_color = "#E0A82E"
        size = 50 + 110 * b.dwell
        ax_side.scatter([x], [y], s=size, color=node_color,
                        edgecolors="#16161A", linewidths=0.8, zorder=5)
        if b.is_focus_push:
            ax_side.scatter([x], [y], s=size + 220, facecolors="none",
                            edgecolors="#9F2933", linewidths=1.6, zorder=6)
        if b.is_promo:
            ax_side.annotate("★ promo", (x, y), xytext=(0, 12),
                             textcoords="offset points", ha="center",
                             fontsize=9, color="#9F2933", fontweight="bold")
        # Stagger labels above/below to avoid overlap.
        offset = 16 if x % 2 == 0 else -22
        ax_side.annotate(b.label.split("·")[0].strip(),
                         (x, y), xytext=(0, offset),
                         textcoords="offset points", ha="center",
                         fontsize=7.5, color="#16161A")

    ax_side.set_xlim(-0.5, len(BEATS) - 0.5)
    ax_side.set_ylim(0, 4.6)
    ax_side.set_xticks(xs)
    ax_side.set_xticklabels([str(i) for i in xs], fontsize=8)
    ax_side.grid(True, axis="y", linestyle=":", alpha=0.4)

    fig.tight_layout(rect=[0, 0, 1, 0.96])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, facecolor="#F5F2EA")
    plt.close(fig)
    return out_path


if __name__ == "__main__":
    out = render_plan(OUT_DIR / "camera_plan.png")
    print(f"[plan] wrote {out}")
