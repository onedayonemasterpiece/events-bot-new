"""Round-11 Mercator unwrap of the afishathumb cylinder.

Takes per-event Bento blocks (already-rendered flat previews) plus the
column placement from `column_layout.py` and stitches them onto a
single canvas representing the unrolled cylinder body.

Output: `artifacts/afishathumb/mercator_today.png`

This is the operator's "blueprint" view — no 3D, no perspective, just
the flat layout so it's easy to see all events at once and judge
overall composition before committing to a Blender render.
"""

from __future__ import annotations

import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from scripts.afishathumb.bento_slot import BentoSlot
from scripts.afishathumb.column_layout import (
    BODY_ARC_D,
    BODY_Z_MAX,
    BODY_Z_MIN,
    BODY_Z_RANGE,
)
from scripts.afishathumb.flythrough import Flythrough
from scripts.afishathumb.sticker_render import PX_PER_D

CANVAS_PAD_PX = 80
COLUMN_BG = (52, 54, 48)


def _try_truetype(size: int) -> ImageFont.FreeTypeFont:
    candidates = [
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    ]
    for p in candidates:
        if p.exists():
            try:
                return ImageFont.truetype(str(p), size=size)
            except (OSError, IOError):
                pass
    return ImageFont.load_default()


def render_mercator(slots: list[BentoSlot],
                    block_previews: dict[int, Path],
                    out_path: Path,
                    title: str = "AfishaThumb · Mercator-развёртка тумбы",
                    flythrough: list[Flythrough] | None = None) -> None:
    """Render the unrolled cylinder to `out_path`."""
    arc_px = int(BODY_ARC_D * PX_PER_D)
    body_px = int(BODY_Z_RANGE * PX_PER_D)
    W = arc_px + 2 * CANVAS_PAD_PX
    H = body_px + 2 * CANVAS_PAD_PX + 120  # extra top space for title
    img = Image.new("RGB", (W, H), (24, 26, 22))
    d = ImageDraw.Draw(img)

    title_font = _try_truetype(36)
    d.text((CANVAS_PAD_PX, 30), title, font=title_font, fill=(220, 220, 210))

    body_x0 = CANVAS_PAD_PX
    body_y0 = CANVAS_PAD_PX + 90
    body_x1 = body_x0 + arc_px
    body_y1 = body_y0 + body_px
    d.rectangle([body_x0, body_y0, body_x1, body_y1], fill=COLUMN_BG)

    # Subtle horizontal grain to evoke a cylinder body
    for y in range(body_y0, body_y1, 8):
        d.line([(body_x0, y), (body_x1, y)], fill=(56, 58, 52), width=1)

    # First paint the flythrough afishas (so target blocks layer above
    # them visually — flythrough is a background of "city posters").
    for ft in (flythrough or []):
        try:
            poster = Image.open(ft.image_path).convert("RGBA")
        except (OSError, IOError):
            continue
        block_w_px = int(ft.w_d * PX_PER_D)
        block_h_px = int(ft.h_d * PX_PER_D)
        poster = poster.resize((block_w_px, block_h_px), Image.LANCZOS)
        if abs(ft.tilt_deg) > 0.1:
            poster = poster.rotate(ft.tilt_deg, expand=True, resample=Image.BICUBIC)
        cx_arc_d = (ft.anchor_angle_deg / 360.0) * BODY_ARC_D
        cx_px = body_x0 + int(cx_arc_d * PX_PER_D)
        cz_norm = (ft.anchor_z - BODY_Z_MIN) / BODY_Z_RANGE
        cy_px = body_y1 - int(cz_norm * body_px)
        x = cx_px - poster.width // 2
        y = cy_px - poster.height // 2
        img.paste(poster, (x, y), poster)

    # Place each event block at its (angle, z) anchor.
    for slot in slots:
        preview_path = block_previews.get(slot.event_id)
        if preview_path is None or not preview_path.exists():
            continue
        preview = Image.open(preview_path).convert("RGBA")
        # Block dimensions in pixels
        block_w_px = int(slot.block_w_d * PX_PER_D)
        block_h_px = int(slot.block_h_d * PX_PER_D)
        # Trim the preview's outer padding (we left 0.10 D margin) to
        # keep alignment honest.
        margin_px = int(0.10 * PX_PER_D)
        inner = preview.crop((margin_px, margin_px,
                              preview.width - margin_px,
                              preview.height - margin_px))
        # Re-scale if needed to match the requested block size exactly.
        if inner.size != (block_w_px, block_h_px):
            inner = inner.resize((block_w_px, block_h_px), Image.LANCZOS)
        # Map (angle, z) → (x, y) on the canvas. Arc 0..360° maps to
        # x 0..arc_px linearly; z = 0..2.5 D maps to y bottom..top.
        cx_arc_d = (slot.anchor_angle_deg / 360.0) * BODY_ARC_D
        cx_px = body_x0 + int(cx_arc_d * PX_PER_D)
        cz_norm = (slot.anchor_z - BODY_Z_MIN) / BODY_Z_RANGE
        cy_px = body_y1 - int(cz_norm * body_px)
        x = cx_px - block_w_px // 2
        y = cy_px - block_h_px // 2
        # Clamp to canvas — wrap is not implemented for this prototype;
        # column_layout places spokes so blocks don't straddle the 0°
        # seam.
        img.paste(inner, (x, y), inner if inner.mode == "RGBA" else None)
        # block label tag
        tag_font = _try_truetype(24)
        d.text((x + 6, y + 6), f"#{slot.event_id}", font=tag_font,
               fill=(255, 230, 120))

    # Axis labels
    axis_font = _try_truetype(20)
    for deg in (0, 60, 120, 180, 240, 300, 360):
        x = body_x0 + int((deg / 360.0) * arc_px)
        d.line([(x, body_y1), (x, body_y1 + 8)], fill=(200, 200, 200), width=2)
        d.text((x - 14, body_y1 + 12), f"{deg}°", font=axis_font,
               fill=(200, 200, 200))
    for z_d, label in [(0.50, "0.5 D"), (1.50, "1.5 D"), (2.50, "2.5 D")]:
        y = body_y1 - int(((z_d) / BODY_Z_RANGE) * body_px)
        d.line([(body_x0 - 8, y), (body_x0, y)], fill=(200, 200, 200), width=2)
        d.text((4, y - 12), label, font=axis_font, fill=(200, 200, 200))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG")
