"""Overlay the per-slot camera attention trace on top of the rendered
`slot_overview.png`. Produces `slot_trace.png` per slot.

Reads:
  - `artifacts/afishathumb/slot_<id>/slot_overview.png` (the cluster
    photographed from the cinematic overview camera)
  - `manifest.json`                                    (carries the beat
    sequence + coverage tests + paper anchor list)
  - `screen_coords.json`                               (each paper's
    centre projected through the overview camera, normalised 0..1)

Renders:
  - filled circle per beat — colour = beat role / dwell-aware size
  - lines connecting consecutive beats — the attention path
  - red ring around focus-push beats (info readouts)
  - small label per dot (role + dwell)
  - a sidebar listing the coverage tests with pass/fail ticks

Run:
    .venv/bin/python scripts/afishathumb/slot_trace.py 4594
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.lines import Line2D
from PIL import Image


REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_slot(event_id) -> tuple[Image.Image, dict, dict]:
    slot_dir = REPO_ROOT / "artifacts" / "afishathumb" / f"slot_{event_id}"
    overview = Image.open(slot_dir / "slot_overview.png")
    with (slot_dir / "manifest.json").open("r", encoding="utf-8") as f:
        manifest = json.load(f)
    with (slot_dir / "screen_coords.json").open("r", encoding="utf-8") as f:
        coords = json.load(f)
    return overview, manifest, coords


def render_trace(event_id: int) -> Path:
    slot_dir = REPO_ROOT / "artifacts" / "afishathumb" / f"slot_{event_id}"
    overview, manifest, coords = _load_slot(event_id)
    W, H = overview.size

    fig = plt.figure(figsize=(W / 100 * 1.85, H / 100), facecolor="#16161A")
    gs = fig.add_gridspec(1, 2, width_ratios=[1.0, 0.42])

    ax_img = fig.add_subplot(gs[0, 0])
    ax_img.imshow(overview, extent=(0, W, H, 0))
    ax_img.set_xlim(0, W)
    ax_img.set_ylim(H, 0)
    ax_img.set_xticks([])
    ax_img.set_yticks([])
    ax_img.set_facecolor("#16161A")
    for spine in ax_img.spines.values():
        spine.set_color("#3A3530")

    # Resolve each beat's anchor to a pixel (x,y). Beats that target the
    # SAME anchor get a small spiral offset so the dots don't pile on top
    # of each other when the camera reads multiple info pieces off the
    # same poster (image + title + essence on a dense poster, etc.).
    beats = manifest.get("beats", [])
    anchor_counts: dict[str, int] = {}
    raw_pts: list[tuple[float, float]] = []
    for b in beats:
        a = b.get("target_anchor", "cluster")
        i = anchor_counts.get(a, 0)
        anchor_counts[a] = i + 1
        cx = coords.get(a, coords["cluster"])["x_norm"] * W
        cy = coords.get(a, coords["cluster"])["y_norm"] * H
        # Spiral offset: 0, 36px right-up, 36px left-up, 36px right-down...
        import math as _m
        ox = _m.cos(i * 1.25) * (24 if i > 0 else 0)
        oy = _m.sin(i * 1.25) * (24 if i > 0 else 0)
        raw_pts.append((cx + ox, cy + oy))
    pts = raw_pts

    # Path lines between consecutive beats.
    for (x0, y0), (x1, y1), b1 in zip(pts[:-1], pts[1:], beats[1:]):
        is_push = bool(b1.get("is_focus_push"))
        color = "#F2C14E" if is_push else "#7BC4E2"
        lw = 3.6 if is_push else 2.4
        # Drawn beneath the dots.
        ax_img.annotate(
            "", xy=(x1, y1), xytext=(x0, y0),
            arrowprops=dict(arrowstyle="->", color=color, lw=lw,
                            connectionstyle="arc3,rad=0.12",
                            shrinkA=14, shrinkB=14),
            zorder=3,
        )

    # Dots per beat.
    for i, (b, (x, y)) in enumerate(zip(beats, pts)):
        is_push = bool(b.get("is_focus_push"))
        # Dot size encodes dwell time.
        dwell = float(b.get("dwell_s", 0.5))
        r = 18 + dwell * 16
        color = str(b.get("color") or "#7BC4E2")
        ax_img.add_patch(patches.Circle((x, y), radius=r + 3,
                                        facecolor="#16161A",
                                        edgecolor="none", zorder=4))
        ax_img.add_patch(patches.Circle((x, y), radius=r,
                                        facecolor=color, edgecolor="#FFFCF4",
                                        linewidth=2.4, zorder=5))
        if is_push:
            ax_img.add_patch(patches.Circle((x, y), radius=r + 10,
                                            facecolor="none",
                                            edgecolor="#E04640",
                                            linewidth=2.4, zorder=6))
        # Beat number in the dot.
        ax_img.text(x, y, str(i + 1), ha="center", va="center",
                    color="#FFFCF4", fontsize=12, fontweight="bold", zorder=7)
        # Label outside the dot.
        label = f"{b.get('label', b.get('role'))}\n{dwell:.1f}s"
        # Position label radially away from the cluster centre so it
        # never sits on the dot itself.
        cluster_x = coords["cluster"]["x_norm"] * W
        cluster_y = coords["cluster"]["y_norm"] * H
        dx, dy = x - cluster_x, y - cluster_y
        norm = (dx * dx + dy * dy) ** 0.5 or 1.0
        ox = x + (dx / norm) * (r + 36)
        oy = y + (dy / norm) * (r + 30)
        ax_img.annotate(
            label, xy=(x, y), xytext=(ox, oy),
            ha="center", va="center",
            color="#FFFCF4", fontsize=9, fontweight="medium",
            bbox=dict(facecolor="#16161A", edgecolor=color, lw=1.2, pad=4.0),
            zorder=8,
        )

    ax_img.set_title(
        f"Слот {event_id} · «{manifest.get('title','')}»\n"
        f"плотность текста на афише = {manifest.get('poster_text_density', 0):.2f}",
        color="#FFFCF4", fontsize=11, pad=10,
    )

    # ---------- Sidebar: coverage tests, beat schedule, source data ----------
    ax_side = fig.add_subplot(gs[0, 1])
    ax_side.set_facecolor("#1F1F23")
    ax_side.set_xticks([])
    ax_side.set_yticks([])
    for spine in ax_side.spines.values():
        spine.set_color("#3A3530")
    ax_side.set_xlim(0, 1)
    ax_side.set_ylim(0, 1)

    y_cursor = 0.97

    def write(text: str, color: str = "#FFFCF4", size: int = 11,
              weight: str = "normal", indent: float = 0.04) -> None:
        nonlocal y_cursor
        ax_side.text(indent, y_cursor, text, color=color, fontsize=size,
                     fontweight=weight, va="top", transform=ax_side.transAxes,
                     family="DejaVu Sans")
        y_cursor -= 0.043 if size <= 11 else 0.058

    write("История о событии", color="#F2C14E", size=13, weight="bold")
    y_cursor -= 0.005

    cov = manifest.get("coverage_tests", {})
    write("Зритель получит:", color="#9FA3AB", size=9.5)
    icons = [
        ("образ", cov.get("image")),
        ("название", cov.get("title")),
        ("суть", cov.get("essence")),
        ("когда", cov.get("when")),
        ("где", cov.get("where")),
        ("стоимость", cov.get("cost")),
    ]
    for label, ok in icons:
        write(f"  {'✓' if ok else '✗'}  {label}",
              color=("#5DBB66" if ok else "#E04640"), size=9.5)
    y_cursor -= 0.008

    write("Беаты камеры:", color="#9FA3AB", size=9.5)
    total = 0.0
    for i, b in enumerate(beats):
        marker = "●" if not b.get("is_focus_push") else "◉"
        line = f"  {i+1}. {marker} {b.get('label','')[:18]} · {b.get('dwell_s',0):.1f}с · {int(b.get('lens_mm',50))}мм"
        write(line, color="#D9D5C8", size=8.5, indent=0.02)
        total += float(b.get("dwell_s", 0))
    y_cursor -= 0.008
    write(f"≈ {total:.1f}с / сцена", color="#F2C14E", size=10.0, weight="bold")

    y_cursor -= 0.012
    write("Обозначения:", color="#9FA3AB", size=9.5)
    write("● = дотс акцента (size = время)", color="#D9D5C8", size=8.5, indent=0.02)
    write("◉ + кольцо = focus push (ключ.)", color="#D9D5C8", size=8.5, indent=0.02)
    write("→ стрелка = ease (синяя) / push (жёлтая)", color="#D9D5C8", size=8.5, indent=0.02)

    fig.tight_layout()
    out_path = slot_dir / "slot_trace.png"
    fig.savefig(out_path, dpi=110, facecolor="#16161A")
    plt.close(fig)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("event_id")  # may be plain int or e.g. "4131_llm"
    args = ap.parse_args()
    out = render_trace(args.event_id)
    print(f"[trace] wrote {out}")


if __name__ == "__main__":
    main()
