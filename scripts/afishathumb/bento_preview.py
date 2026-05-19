"""Round-11 flat preview of one event's Bento block.

Stitches the composed cells (primary, extras, stickers) onto a single
2D canvas at the same `PX_PER_D` scale as the stickers — no Blender,
no cylinder projection — so the operator can see the bento composition
quickly during iteration.

Each cell is rasterised at its target D-units, IR1 tilt is applied via
PIL rotation, and small drop shadows give the "glued paper" feel
without requiring the full Blender render.

Output: `artifacts/afishathumb/preview_<event_id>.png`
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scripts.afishathumb.bento_slot import BentoSlot, Cell, compose_bento
from scripts.afishathumb.sticker_render import (
    PX_PER_D,
    render_cost,
    render_date,
    render_date_stacked,
    render_digest,
    render_info,
    render_location,
    render_title,
)


def _load_image(path: str) -> Optional[Image.Image]:
    if not path:
        return None
    try:
        return Image.open(path).convert("RGBA")
    except (OSError, IOError):
        return None


def _fit_image(img: Image.Image, w_px: int, h_px: int) -> Image.Image:
    src_asp = img.width / img.height
    dst_asp = w_px / h_px
    if abs(src_asp - dst_asp) < 0.02:
        return img.resize((w_px, h_px), Image.LANCZOS)
    # Inscribe: shrink to fit so we preserve aspect, then center on
    # a white background of the target size — matches what the
    # cylinder render does via `canvas_to_world` aspect inscribe.
    if src_asp > dst_asp:
        new_w = w_px
        new_h = int(w_px / src_asp)
    else:
        new_h = h_px
        new_w = int(h_px * src_asp)
    inner = img.resize((new_w, new_h), Image.LANCZOS)
    bg = Image.new("RGBA", (w_px, h_px), (240, 235, 225, 255))
    bg.paste(inner, ((w_px - new_w) // 2, (h_px - new_h) // 2), inner)
    return bg


def _render_cell(cell: Cell, *, is_free_event: bool) -> Image.Image:
    w_px = int(cell.w_d * PX_PER_D)
    h_px = int(cell.h_d * PX_PER_D)
    if cell.role == "title":
        # decide lines from height heuristic — anything taller than the
        # 1-line cap+padding budget is 2 lines
        from scripts.afishathumb.master_sizes import (
            STICKER_PAD_V_BOTTOM_D,
            STICKER_PAD_V_TOP_D,
            TITLE_CAP_H_D,
        )
        one_line_h = TITLE_CAP_H_D + STICKER_PAD_V_TOP_D + STICKER_PAD_V_BOTTOM_D
        lines = 1 if cell.h_d <= one_line_h * 1.10 else 2
        return render_title(cell.text, cell.w_d, cell.h_d, lines=lines)
    if cell.role == "date":
        if cell.extra.get("day"):
            return render_date_stacked(
                cell.extra.get("day", ""),
                cell.extra.get("month", ""),
                cell.extra.get("time", ""),
                cell.w_d, cell.h_d,
            )
        return render_date(cell.text, cell.w_d, cell.h_d)
    if cell.role == "location":
        return render_location(cell.text, cell.w_d, cell.h_d,
                               parts=cell.extra.get("parts"))
    if cell.role == "cost":
        return render_cost(cell.text, cell.w_d, cell.h_d, is_free=is_free_event)
    if cell.role == "digest":
        return render_digest(cell.text, cell.w_d, cell.h_d)
    # image cells (primary / extras)
    img = _load_image(cell.image_path or "")
    if img is None:
        # placeholder
        img = Image.new("RGBA", (w_px, h_px), (200, 60, 60, 255))
    return _fit_image(img, w_px, h_px)


def _paste_with_tilt(canvas: Image.Image, cell_img: Image.Image,
                     center_x: int, center_y: int, tilt_deg: float) -> None:
    """Paste with rotation around center. Tilts are small (`±3..±8°`)
    so we expand the rotated bbox slightly to avoid corner clipping."""
    if abs(tilt_deg) > 0.1:
        rotated = cell_img.rotate(tilt_deg, expand=True, resample=Image.BICUBIC)
    else:
        rotated = cell_img
    x = center_x - rotated.width // 2
    y = center_y - rotated.height // 2
    canvas.paste(rotated, (x, y), rotated)


def render_bento_preview(slot: BentoSlot, *, is_free_event: bool,
                         out_path: Path) -> None:
    margin_d = 0.10
    canvas_w_d = slot.block_w_d + 2 * margin_d
    canvas_h_d = slot.block_h_d + 2 * margin_d
    canvas_w_px = int(canvas_w_d * PX_PER_D)
    canvas_h_px = int(canvas_h_d * PX_PER_D)
    # warm grey-green background to evoke the cylinder body
    canvas = Image.new("RGBA", (canvas_w_px, canvas_h_px), (52, 54, 48, 255))
    # subtle column-body grain
    d = ImageDraw.Draw(canvas)
    for y in range(0, canvas_h_px, 6):
        d.line([(0, y), (canvas_w_px, y)], fill=(56, 58, 52), width=1)

    block_origin_x = int(margin_d * PX_PER_D)
    block_origin_y = int(margin_d * PX_PER_D)

    for cell in slot.cells:
        cell_img = _render_cell(cell, is_free_event=is_free_event)
        cx = block_origin_x + int((cell.x_d + cell.w_d / 2) * PX_PER_D)
        cy = block_origin_y + int((cell.y_d + cell.h_d / 2) * PX_PER_D)
        _paste_with_tilt(canvas, cell_img, cx, cy, cell.tilt_deg)

    canvas.convert("RGB").save(out_path, "PNG")


def main() -> None:
    # Quick demo on event 3977 using its existing slot folder for image
    # paths. Real per-event composition is driven by `build_today.py`.
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-id", type=int, default=3977)
    args = parser.parse_args()

    slot_dir = REPO_ROOT / "artifacts" / "afishathumb" / f"slot_{args.event_id}_llm"
    if not slot_dir.exists():
        slot_dir = REPO_ROOT / "artifacts" / "afishathumb" / f"slot_{args.event_id}"

    def _aspect(p: Path) -> float:
        im = Image.open(p)
        return im.width / im.height

    if args.event_id == 3977:
        primary_path = slot_dir / "poster.png"
        extras = [
            (str(slot_dir / "poster_extra_1.png"), _aspect(slot_dir / "poster_extra_1.png")),
            (str(slot_dir / "poster_extra_2.png"), _aspect(slot_dir / "poster_extra_2.png")),
            (str(slot_dir / "poster_extra_3.png"), _aspect(slot_dir / "poster_extra_3.png")),
        ]
        slot = compose_bento(
            event_id=3977,
            primary_image_path=str(primary_path),
            primary_aspect=_aspect(primary_path),
            title_text="Дурак и солнце",
            day_text="16",
            month_text="мая",
            time_text="19:00",
            location_text="Театр Третий этаж",
            is_free=False,
            price_text="",
            digest_text='Спектакль "Дурак и солнце" в Театре Третий этаж — история о дураке и солнце, полная глубокого смысла.',
            extra_image_paths=extras,
        )
        is_free = False
    else:
        raise SystemExit(f"--event-id {args.event_id} not wired yet; use 3977 for the demo")

    out = REPO_ROOT / "artifacts" / "afishathumb" / f"preview_{args.event_id}.png"
    render_bento_preview(slot, is_free_event=is_free, out_path=out)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
