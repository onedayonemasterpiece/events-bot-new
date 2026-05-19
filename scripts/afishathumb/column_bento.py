"""Round-11 final — REAL global Bento packer for the cylinder.

Replaces the spoke-based `column_layout.place_blocks` with a 2D grid
packer that snaps every tile (target event-block OR ambient flythrough
afisha) into a shared modular grid. Tiles share edges, the grid is
visible as a deliberate composition on the Mercator unwrap, and gaps
are intentional (small, between tiles) rather than huge swaths of
empty cylinder.

Grid: GRID_COLS × GRID_ROWS on the unrolled cylinder.
  col_w_d = BODY_ARC_D / GRID_COLS
  row_h_d = BODY_Z_RANGE / GRID_ROWS

Each tile has a footprint `(cell_w, cell_h)` derived from its
`(block_w_d, block_h_d)`:
  cell_w = ceil(block_w_d / col_w_d)
  cell_h = ceil(block_h_d / row_h_d)

Tiles are placed greedy, biggest-area first. Each grid cell is
either occupied by one tile or free. Wrap-around at the 0°/360° seam
is allowed (a tile can span across the seam).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from scripts.afishathumb.bento_slot import BentoSlot
from scripts.afishathumb.column_layout import (
    BODY_ARC_D,
    BODY_Z_MAX,
    BODY_Z_MIN,
    BODY_Z_RANGE,
    Placement,
)
from scripts.afishathumb.flythrough import Flythrough

GRID_COLS = 6
GRID_ROWS = 4
COL_W_D = BODY_ARC_D / GRID_COLS    # ≈ 0.523
ROW_H_D = BODY_Z_RANGE / GRID_ROWS  # ≈ 0.611


@dataclass
class Tile:
    """A single bento tile on the cylinder grid."""
    kind: str   # "target" or "flythrough"
    ref: object  # BentoSlot or Flythrough
    block_w_d: float
    block_h_d: float
    cell_w: int = 1
    cell_h: int = 1
    grid_col: int = -1
    grid_row: int = -1

    @property
    def event_id(self) -> int:
        return getattr(self.ref, "event_id", -1)

    @property
    def area_cells(self) -> int:
        return self.cell_w * self.cell_h


def _footprint(block_w_d: float, block_h_d: float) -> tuple[int, int]:
    """Map a tile's D-unit bbox to its grid cell footprint."""
    cell_w = max(1, math.ceil(block_w_d / COL_W_D - 0.10))  # tolerate 10% over
    cell_h = max(1, math.ceil(block_h_d / ROW_H_D - 0.10))
    cell_w = min(cell_w, GRID_COLS)
    cell_h = min(cell_h, GRID_ROWS)
    return cell_w, cell_h


def build_tiles(target_slots: list[BentoSlot],
                flythrough: list[Flythrough]) -> list[Tile]:
    """Build the tile list from targets + flythrough afishas."""
    tiles: list[Tile] = []
    for s in target_slots:
        cw, ch = _footprint(s.block_w_d, s.block_h_d)
        tiles.append(Tile(kind="target", ref=s,
                          block_w_d=s.block_w_d, block_h_d=s.block_h_d,
                          cell_w=cw, cell_h=ch))
    for f in flythrough:
        cw, ch = _footprint(f.w_d, f.h_d)
        tiles.append(Tile(kind="flythrough", ref=f,
                          block_w_d=f.w_d, block_h_d=f.h_d,
                          cell_w=cw, cell_h=ch))
    return tiles


def pack(tiles: list[Tile]) -> tuple[list[Tile], list[Tile]]:
    """Greedy bin-packer on a GRID_ROWS × GRID_COLS grid (wrap-around
    on the angular axis). Returns `(placed, unplaced)`."""
    grid: list[list[Optional[Tile]]] = [[None] * GRID_COLS
                                         for _ in range(GRID_ROWS)]
    # Sort by area (descending), then targets first within same area
    # so they get the prime grid positions.
    sorted_tiles = sorted(tiles,
                          key=lambda t: (-t.area_cells, 0 if t.kind == "target" else 1))
    placed: list[Tile] = []
    unplaced: list[Tile] = []
    for tile in sorted_tiles:
        ok = False
        # Try every (row, col) start position. Allow wrap-around on col.
        for row_start in range(GRID_ROWS - tile.cell_h + 1):
            for col_start in range(GRID_COLS):
                # Check footprint cells free (with col wrap)
                free = True
                for dr in range(tile.cell_h):
                    for dc in range(tile.cell_w):
                        c = (col_start + dc) % GRID_COLS
                        if grid[row_start + dr][c] is not None:
                            free = False
                            break
                    if not free:
                        break
                if free:
                    for dr in range(tile.cell_h):
                        for dc in range(tile.cell_w):
                            c = (col_start + dc) % GRID_COLS
                            grid[row_start + dr][c] = tile
                    tile.grid_col = col_start
                    tile.grid_row = row_start
                    placed.append(tile)
                    ok = True
                    break
            if ok:
                break
        if not ok:
            unplaced.append(tile)
    return placed, unplaced


def apply_placements(placed: list[Tile]) -> None:
    """Write the resulting `(angle_deg, z)` back onto each tile's ref
    (BentoSlot or Flythrough)."""
    deg_per_col = 360.0 / GRID_COLS
    for tile in placed:
        # Centre of footprint
        center_col = tile.grid_col + tile.cell_w / 2.0
        center_row = tile.grid_row + tile.cell_h / 2.0
        angle = (center_col * deg_per_col) % 360.0
        z = BODY_Z_MAX - center_row * ROW_H_D
        if tile.kind == "target":
            tile.ref.anchor_angle_deg = angle
            tile.ref.anchor_z = z
        else:
            tile.ref.anchor_angle_deg = angle
            tile.ref.anchor_z = z


def grid_summary(placed: list[Tile]) -> str:
    """ASCII art of the grid layout for debug logs."""
    grid: list[list[str]] = [["·"] * GRID_COLS for _ in range(GRID_ROWS)]
    for tile in placed:
        label = f"T{tile.event_id}" if tile.kind == "target" else f"a{tile.event_id}"
        for dr in range(tile.cell_h):
            for dc in range(tile.cell_w):
                c = (tile.grid_col + dc) % GRID_COLS
                grid[tile.grid_row + dr][c] = label[:4].ljust(4)
    out = []
    for row in grid:
        out.append(" ".join(cell.ljust(4) for cell in row))
    return "\n".join(out)
