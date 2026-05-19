"""Round-11 per-event Bento composer.

Replaces the round-7..10 loose-cluster approach. Produces an
`event-block`: a rectangular region on the cylinder containing the
event's primary poster, info stickers (title / date / location /
cost), optional extras and digest, all sized by `master_sizes` and
placed in one of a few canonical Bento templates picked by aspect +
seeded variant.

Output is a `BentoSlot` with absolute (angle_deg, z) placements for
every paper, ready to feed into the Blender render driver — same
`PaperPlacement` schema as before.

The event-block bbox is centered on the per-event anchor angle/z;
column-level placement (where the block sits on the cylinder) is
decided later by `column_llm.py`.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Optional

from scripts.afishathumb.master_sizes import (
    DATE_CAP_H_D,
    COST_CAP_H_D,
    DIGEST_CAP_H_D,
    EVENT_BLOCK_MAX_ARC_DEG,
    EVENT_BLOCK_MAX_H_D,
    EXTRA_LINEAR_RATIO,
    LOCATION_CAP_H_D,
    MASTER_PRIMARY_AREA_D2,
    PrimaryDims,
    info_sticker_dims,
    primary_dims_from_aspect,
    seeded_peel,
    seeded_tilt_deg,
    seeded_wrinkle,
    title_cell_dims,
)


def extra_dims_linear_half(primary: PrimaryDims, source_aspect: float) -> PrimaryDims:
    """Extras at linear-half of primary (operator wording «уменьшенный
    в 2 раза размер»). Source aspect drives cell shape so the rasterised
    image fills the cell without white margins.
    """
    half_h = primary.h_d * EXTRA_LINEAR_RATIO
    half_w = half_h * source_aspect
    return PrimaryDims(w_d=half_w, h_d=half_h)


@dataclass
class Cell:
    """One bento cell inside the event-block, with bbox in D-units
    relative to the block's bottom-left corner."""
    role: str          # "primary" | "extra_1..3" | "title" | "date" | "location" | "cost" | "digest"
    x_d: float
    y_d: float
    w_d: float
    h_d: float
    text: str = ""     # for sticker cells
    image_path: Optional[str] = None  # for image cells
    tilt_deg: float = 0.0
    peel_corners: tuple[bool, bool, bool, bool] = (False, False, False, False)
    peel_intensity: float = 0.0
    wrinkle: float = 0.0
    # Structured payload for non-trivial stickers (date stacked, etc.)
    extra: dict = field(default_factory=dict)


@dataclass
class BentoSlot:
    event_id: int
    anchor_angle_deg: float       # center of the block on the cylinder
    anchor_z: float               # center of the block in D-units
    block_w_d: float              # block width (arc length in D-units)
    block_h_d: float              # block height in D-units
    cells: list[Cell] = field(default_factory=list)
    template_name: str = ""


def _arc_deg_per_d(cyl_radius: float) -> float:
    """Convert arc length in D-units to degrees: arc_d = θ × R, so
    θ_deg = arc_d × 360 / (2 π R) = arc_d × 57.296 / R."""
    return 360.0 / (2.0 * math.pi * cyl_radius)


def compose_bento(
    *,
    event_id: int,
    primary_image_path: str,
    primary_aspect: float,
    title_text: str,
    day_text: str,
    month_text: str,
    time_text: str,
    location_text: str,
    is_free: bool,
    price_text: str = "",
    digest_text: Optional[str] = None,
    extra_image_paths: Optional[list[tuple[str, float]]] = None,
    rng_seed: Optional[str] = None,
) -> BentoSlot:
    """Build the per-event bento block.

    Args:
        primary_aspect: w/h of the primary poster image.
        extra_image_paths: optional list of `(path, aspect)` tuples for
            extras; up to 3 are used.
        rng_seed: seed for the layout-variant pick (defaults to
            `f"{event_id}"`).
    """
    seed = rng_seed or f"{event_id}"
    rng = random.Random(seed + ":bento")

    # --- size every cell deterministically -----------------------------
    primary = primary_dims_from_aspect(primary_aspect)
    extras: list[tuple[str, float, PrimaryDims]] = []
    for path, asp in (extra_image_paths or [])[:4]:
        extras.append((path, asp, extra_dims_linear_half(primary, asp)))

    # Info stickers. Date is the narrow vertical stack
    # (day big / month / time) so it lands as a compact rectangle next
    # to the primary, the style operator remembered from earlier rounds.
    info_cells: list[tuple[str, str, float, float, dict]] = []  # (role, text, w, h, extra)
    if day_text and month_text:
        # Narrow vertical card sized to fit the LONGEST line. Months
        # like «ИЮЛЯ» / «ОКТЯБРЯ» are wider than «МАЯ» — width adapts.
        longest = max(day_text, month_text.upper(), time_text, key=len)
        date_w = max(0.22, len(longest) * (DATE_CAP_H_D * 0.55) + 2 * 0.012)
        date_h = 0.30
        info_cells.append(("date", f"{day_text} {month_text} {time_text}".strip(),
                           date_w, date_h,
                           {"day": day_text, "month": month_text, "time": time_text}))
    # Right-column stickers. Width scaled with STICKER_SCALE so the
    # event block stays bounded. Renderers shrink font and wrap text
    # inside to fit.
    from scripts.afishathumb.master_sizes import STICKER_SCALE
    RIGHT_COL_W = 0.32 * STICKER_SCALE
    if location_text:
        loc_parts = [p.strip() for p in location_text.split(",") if p.strip()][:3]
        loc_h = len(loc_parts) * LOCATION_CAP_H_D * 1.20 + 0.028
        info_cells.append(("location", location_text,
                           RIGHT_COL_W, loc_h, {"parts": loc_parts}))
    if is_free:
        info_cells.append(("cost", "БЕСПЛАТНО",
                           RIGHT_COL_W, COST_CAP_H_D + 0.024, {}))
    elif price_text.strip():
        info_cells.append(("cost", price_text,
                           RIGHT_COL_W, COST_CAP_H_D + 0.024, {}))
    if digest_text:
        n_words = len(digest_text.split())
        # At RIGHT_COL_W 0.32, font 0.034: ≈ 2.5 words per line.
        # Give 6-10 lines so 16-22 word digests never clip.
        line_count = max(6, min(10, (n_words + 2) // 2))
        digest_h = line_count * DIGEST_CAP_H_D * 1.30 + 0.024
        info_cells.append(("digest", digest_text,
                           RIGHT_COL_W, digest_h, {}))

    # --- single unified template (round-11c) ----------------------------
    # All events use the portrait-style layout regardless of source
    # aspect: primary on the left, info stickers stacked on the right,
    # extras row(s) below. Landscape primaries become wide-and-short
    # on the left side of the block; the right-column geometry is the
    # same for everyone, so stickers always fit and the cylinder reads
    # as a coherent bento composition.
    template_family = "portrait"
    variant = rng.choice(["A", "B"])
    template_name = f"unified_{variant}"

    # --- build the block ----------------------------------------------
    # Title cell width is bounded by the planned block width. For portrait
    # we plan a narrower block; for landscape, a wider one.
    if template_family == "portrait":
        block_w_d = min(EVENT_BLOCK_MAX_ARC_DEG / 100.0, primary.w_d + 0.35 + 0.10)
    elif template_family == "landscape":
        block_w_d = min(1.10, primary.w_d + 0.05)
    else:
        block_w_d = min(1.00, primary.w_d + 0.35 + 0.10)

    title_w, title_h, title_lines = title_cell_dims(len(title_text), block_w_d)

    # Layout each template
    cells: list[Cell] = []
    block_h_d = 0.0

    if template_family == "landscape":
        # ┌──────────── title ────────────┐
        # │           primary              │
        # ├──────┬─────────┬───────────────┤
        # │ date │  loc    │  cost         │   (info row)
        # ├──────┴─────────┴───────────────┤
        # │   extras (if any)              │
        cells.append(Cell("title", x_d=0.0, y_d=0.0,
                          w_d=block_w_d, h_d=title_h, text=title_text))
        cells.append(Cell("primary",
                          x_d=(block_w_d - primary.w_d) / 2.0,
                          y_d=title_h + 0.020,
                          w_d=primary.w_d, h_d=primary.h_d,
                          image_path=primary_image_path))
        info_y = title_h + 0.020 + primary.h_d + 0.015
        info_x = 0.0
        max_info_h = 0.0
        for role, text, w, h, extra in info_cells:
            cells.append(Cell(role, x_d=info_x, y_d=info_y,
                              w_d=w, h_d=h, text=text, extra=extra))
            info_x += w + 0.018
            max_info_h = max(max_info_h, h)
        extras_y = info_y + max_info_h + 0.020
        if extras:
            extras_x = 0.0
            for i, (path, asp, ed) in enumerate(extras):
                cells.append(Cell(f"extra_{i+1}",
                                  x_d=extras_x, y_d=extras_y,
                                  w_d=ed.w_d, h_d=ed.h_d,
                                  image_path=path))
                extras_x += ed.w_d + 0.020
            block_h_d = extras_y + max(e[2].h_d for e in extras)
        else:
            block_h_d = info_y + max_info_h

    elif template_family == "portrait":
        # Portrait template, round-11b layout:
        #   Title (full block width)
        #   Primary (left, larger after +30%) | right column of stickers
        #   Extras row(s) below: 2-in-row fill 100% block width;
        #     3 extras = 2-in-row + 1 below at same cell size.
        from scripts.afishathumb.master_sizes import CELL_GAP_D
        gap = CELL_GAP_D
        # Block width = primary + gap + right column. The right column
        # width is the WIDEST info sticker (location / cost / digest are
        # all sized to right_col_w = 0.42 by us above).
        right_col_w = max((w for _, _, w, _, _ in info_cells), default=0.28)
        block_w_d = primary.w_d + gap + right_col_w

        title_w, title_h, title_lines = title_cell_dims(len(title_text), block_w_d)
        cells.append(Cell("title", x_d=0.0, y_d=0.0,
                          w_d=block_w_d, h_d=title_h, text=title_text))
        primary_y = title_h + gap
        cells.append(Cell("primary", x_d=0.0, y_d=primary_y,
                          w_d=primary.w_d, h_d=primary.h_d,
                          image_path=primary_image_path))
        info_stack_x = primary.w_d + gap
        info_stack_y = primary_y
        for role, text, w, h, extra in info_cells:
            cells.append(Cell(role, x_d=info_stack_x, y_d=info_stack_y,
                              w_d=w, h_d=h, text=text, extra=extra))
            info_stack_y += h + gap
        info_stack_bottom = info_stack_y - gap

        extras_y = max(primary_y + primary.h_d, info_stack_bottom) + gap
        n_extras = len(extras)
        if n_extras == 0:
            block_h_d = max(primary_y + primary.h_d, info_stack_bottom)
        elif n_extras == 1:
            path, asp, ed = extras[0]
            cell_w = block_w_d
            cell_h = cell_w / max(0.30, asp)
            cells.append(Cell("extra_1",
                              x_d=0.0, y_d=extras_y,
                              w_d=cell_w, h_d=cell_h, image_path=path))
            block_h_d = extras_y + cell_h
        elif n_extras == 2:
            # Two extras side-by-side filling 100% of block width
            # (operator round-11b «когда 2 в ряд они занимают 100%
            # импровизированного блока»).
            cell_w = (block_w_d - gap) / 2.0
            row_h = 0.0
            for i, (path, asp, ed) in enumerate(extras):
                cell_h = cell_w / max(0.30, asp)
                cells.append(Cell(f"extra_{i+1}",
                                  x_d=i * (cell_w + gap), y_d=extras_y,
                                  w_d=cell_w, h_d=cell_h, image_path=path))
                row_h = max(row_h, cell_h)
            block_h_d = extras_y + row_h
        elif n_extras == 3:
            # 2-up + 1-below (operator round-11c: «третья афиша должна
            # уходить ниже следующей строкой»). Row 1 fills 100% block
            # at two equal cells; row 2 has the third tile at the same
            # cell size, centred. Multi-image blocks therefore become
            # tall — column LLM-C gives them a dedicated spoke.
            cell_w = (block_w_d - gap) / 2.0
            row1_h = 0.0
            for i in (0, 1):
                path, asp, ed = extras[i]
                cell_h = cell_w / max(0.30, asp)
                cells.append(Cell(f"extra_{i+1}",
                                  x_d=i * (cell_w + gap), y_d=extras_y,
                                  w_d=cell_w, h_d=cell_h, image_path=path))
                row1_h = max(row1_h, cell_h)
            row2_y = extras_y + row1_h + gap
            path, asp, ed = extras[2]
            cell_h = cell_w / max(0.30, asp)
            cells.append(Cell("extra_3",
                              x_d=(block_w_d - cell_w) / 2.0, y_d=row2_y,
                              w_d=cell_w, h_d=cell_h, image_path=path))
            block_h_d = row2_y + cell_h
        else:
            # 4 extras in 2×2 grid. Operator round-11c: «четвёртая
            # афиша должна уходить ниже следующей строкой».
            cell_w = (block_w_d - gap) / 2.0
            row1_h = 0.0
            for i in (0, 1):
                path, asp, ed = extras[i]
                cell_h = cell_w / max(0.30, asp)
                cells.append(Cell(f"extra_{i+1}",
                                  x_d=i * (cell_w + gap), y_d=extras_y,
                                  w_d=cell_w, h_d=cell_h, image_path=path))
                row1_h = max(row1_h, cell_h)
            row2_y = extras_y + row1_h + gap
            row2_h = 0.0
            for i in (2, 3):
                path, asp, ed = extras[i]
                cell_h = cell_w / max(0.30, asp)
                cells.append(Cell(f"extra_{i+1}",
                                  x_d=(i - 2) * (cell_w + gap), y_d=row2_y,
                                  w_d=cell_w, h_d=cell_h, image_path=path))
                row2_h = max(row2_h, cell_h)
            block_h_d = row2_y + row2_h

    else:  # square
        # ┌────── title ──────┐
        # │ ┌primary┐  ┌date─┐│
        # │ │       │  └─────┘│
        # │ │       │  ┌─cost┐│
        # │ └───────┘  └─────┘│
        # │  ┌─location───┐    │
        # │  └────────────┘    │
        # │  extras (if any)   │
        cells.append(Cell("title", x_d=0.0, y_d=0.0,
                          w_d=block_w_d, h_d=title_h, text=title_text))
        primary_y = title_h + 0.020
        cells.append(Cell("primary", x_d=0.0, y_d=primary_y,
                          w_d=primary.w_d, h_d=primary.h_d,
                          image_path=primary_image_path))
        # right column: date + cost
        right_x = primary.w_d + 0.030
        right_y = primary_y
        right_h_total = 0.0
        for role, text, w, h, extra in info_cells:
            if role == "location":
                continue
            cells.append(Cell(role, x_d=right_x, y_d=right_y,
                              w_d=w, h_d=h, text=text, extra=extra))
            right_y += h + 0.018
            right_h_total += h + 0.018
        # location below primary, full block width
        loc_y = primary_y + primary.h_d + 0.020
        loc_cell = next((c for c in info_cells if c[0] == "location"), None)
        if loc_cell is not None:
            _, ltext, lw, lh, lextra = loc_cell
            cells.append(Cell("location", x_d=0.0, y_d=loc_y,
                              w_d=lw, h_d=lh, text=ltext, extra=lextra))
            loc_y += lh + 0.020
        # extras below location
        if extras:
            extras_x = 0.0
            for i, (path, asp, ed) in enumerate(extras):
                cells.append(Cell(f"extra_{i+1}",
                                  x_d=extras_x, y_d=loc_y,
                                  w_d=ed.w_d, h_d=ed.h_d,
                                  image_path=path))
                extras_x += ed.w_d + 0.020
            block_h_d = loc_y + max(e[2].h_d for e in extras)
        else:
            block_h_d = loc_y

    # Apply IR1 imperfection. Posters glued straight (tilt 0); title
    # has a narrow tilt range (≤1.2°); other stickers wider.
    for cell in cells:
        is_poster_cell = (cell.role == "primary" or cell.role.startswith("extra"))
        is_title_cell = (cell.role == "title")
        cell.tilt_deg = seeded_tilt_deg(
            f"{event_id}:{cell.role}",
            is_poster=is_poster_cell,
            is_title=is_title_cell,
        )
        # Mild peel only on posters; stickers stay flat so typography
        # reads cleanly.
        if is_poster_cell:
            corners, intensity = seeded_peel(f"{event_id}:{cell.role}")
            cell.peel_corners = corners
            cell.peel_intensity = intensity
        cell.wrinkle = seeded_wrinkle(f"{event_id}:{cell.role}")

    # block_h_d is whatever the content needs; column-level packer
    # (column_llm.py) is the one that decides whether two such blocks
    # fit vertically on the cylinder.

    return BentoSlot(
        event_id=event_id,
        anchor_angle_deg=0.0,    # column LLM-C sets later
        anchor_z=1.65,           # default mid; column LLM-C overrides
        block_w_d=block_w_d,
        block_h_d=block_h_d,
        cells=cells,
        template_name=template_name,
    )


def bento_to_papers(slot: BentoSlot,
                    cyl_radius: float) -> list[dict]:
    """Translate a `BentoSlot` into the `paper` list shape expected by
    `render_slot_blender.py` / `render_column_blender.py`. Anchor
    `(angle_deg, z)` is at each cell's centre on the cylinder.

    The downstream `layout_posters.place_paper` contract: `width` and
    `height` are arc-length / vertical extent in D-units; the placer
    converts arc-length to an angle via `theta = arc / R` internally.
    `anchor_angle_deg` is the absolute angular position on the column.
    """
    arc_per_d = _arc_deg_per_d(cyl_radius)
    # Block bbox is anchored on its centre (anchor_angle, anchor_z);
    # the block extends ±block_w/2 in arc and ±block_h/2 in z. Cells
    # carry their offsets from the block's bottom-left corner.
    block_left_arc_d = -slot.block_w_d / 2.0
    block_bottom_z = slot.anchor_z - slot.block_h_d / 2.0
    papers: list[dict] = []
    for cell in slot.cells:
        cell_cx_arc_d = block_left_arc_d + cell.x_d + cell.w_d / 2.0
        cell_cy_z = block_bottom_z + cell.y_d + cell.h_d / 2.0
        anchor_angle = slot.anchor_angle_deg + cell_cx_arc_d * arc_per_d
        papers.append({
            "image": cell.image_path or "",
            "text": cell.text,
            "role": cell.role,
            "anchor_angle_deg": anchor_angle,
            "anchor_z": cell_cy_z,
            "width": cell.w_d,
            "height": cell.h_d,
            "tilt_deg": cell.tilt_deg,
            "peel_corners": list(cell.peel_corners),
            "peel_intensity": cell.peel_intensity,
            "wrinkle": cell.wrinkle,
            "name": f"{cell.role.capitalize()}.{slot.event_id}",
            "paper_offset": 0.004 + 0.001 * abs(hash(cell.role) % 10),
        })
    return papers
