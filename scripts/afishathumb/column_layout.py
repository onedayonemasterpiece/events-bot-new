"""Round-11 column-level placement.

Takes a list of BentoSlots from `build_today.py` and assigns
`(anchor_angle_deg, anchor_z)` to each so blocks pack the cylinder
densely without overlap.

This first implementation is **deterministic** — greedy bin-pack with
two-row vertical bias for short blocks and dedicated spokes for tall
multi-image blocks. The LLM-C variant lands in a follow-up commit
that uses gemini-3.1-flash-lite to pick aesthetically-pleasing
placements; the deterministic packer remains as validator + fallback.

Cylinder body: arc length 2π · 0.5 = π D (≈ 3.14 D total around 360°),
height z ∈ [0.18, 2.68] (≈ 2.5 D).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from scripts.afishathumb.bento_slot import BentoSlot
from scripts.afishathumb.flythrough import Flythrough

# Cylinder body geometry constants — match `build_column.py` defaults.
BODY_RADIUS = 0.5
BODY_Z_MIN = 0.20
BODY_Z_MAX = 2.65
BODY_Z_RANGE = BODY_Z_MAX - BODY_Z_MIN
BODY_ARC_D = math.pi * 2 * BODY_RADIUS    # arc length in D-units
SPOKE_ANGULAR_GAP_DEG = 8.0               # min angular separation between spokes
INTRA_SPOKE_GAP_D = 0.10                  # min vertical gap when stacking 2 blocks on one spoke
TALL_BLOCK_THRESHOLD_D = 1.50             # blocks taller than this get a dedicated spoke


@dataclass
class Placement:
    event_id: int
    angle_deg: float    # spoke azimuth (centre of block)
    z: float            # centre of block on cylinder body


def _arc_d_to_deg(arc_d: float) -> float:
    return arc_d * 360.0 / BODY_ARC_D


def place_blocks(slots: list[BentoSlot]) -> list[Placement]:
    """Width-aware greedy packer. Tall blocks own a spoke each; short
    blocks pair-stack two per spoke. Spoke angular spacing accounts
    for each block's arc-width so neighbours don't overlap. If total
    arc demand exceeds 360°, the operator's «3-4 целевых» bound
    applies — extra blocks are dropped from the column (they become
    flythrough candidates in a later pass).

    Cylinder body height: 2.45 D. If a multi-image block exceeds this
    we still place it; the renderer will let it slightly exceed the
    body bounds — column-density is a worse problem than block-height.
    """
    # 1. Classify blocks into "tall" (dedicated spoke) and "short" (paired).
    tall = sorted([s for s in slots if s.block_h_d > TALL_BLOCK_THRESHOLD_D],
                  key=lambda s: -s.block_h_d)
    short = sorted([s for s in slots if s.block_h_d <= TALL_BLOCK_THRESHOLD_D],
                   key=lambda s: -s.block_h_d)

    # 2. Pair shorts into spokes (greedy two-up per spoke).
    paired_spokes: list[list[BentoSlot]] = []
    used: set[int] = set()
    for i, s in enumerate(short):
        if id(s) in used:
            continue
        pair = [s]
        used.add(id(s))
        # find a partner whose block_h_d + s.block_h_d + INTRA_SPOKE_GAP_D <= BODY_Z_RANGE
        for j, t in enumerate(short[i + 1:], start=i + 1):
            if id(t) in used:
                continue
            if s.block_h_d + t.block_h_d + INTRA_SPOKE_GAP_D <= BODY_Z_RANGE:
                pair.append(t)
                used.add(id(t))
                break
        paired_spokes.append(pair)

    # 3. Total angular demand per spoke = max(block_w in arc-degrees)
    # for the items on that spoke, plus a fixed gap. If demand > 360,
    # drop spokes from the END (lowest priority) until we fit.
    def spoke_demand_deg(spoke_contents: list[BentoSlot]) -> float:
        max_w_d = max(s.block_w_d for s in spoke_contents)
        return _arc_d_to_deg(max_w_d) + SPOKE_ANGULAR_GAP_DEG

    spokes: list[list[BentoSlot]] = [[s] for s in tall] + paired_spokes
    total_deg = sum(spoke_demand_deg(s) for s in spokes)
    while total_deg > 360.0 and len(spokes) > 1:
        # remove the LAST spoke (lowest priority) to fit demand
        dropped = spokes.pop()
        for s in dropped:
            print(f"[column_layout] dropping event {s.event_id} — "
                  f"cylinder full (total demand {total_deg:.0f}°)")
        total_deg = sum(spoke_demand_deg(s) for s in spokes)

    # Allocate angular slots proportional to each spoke's demand.
    spokes_count = len(spokes)
    if spokes_count == 0:
        return []
    demands = [spoke_demand_deg(s) for s in spokes]
    total_demand = sum(demands)
    # If total_demand < 360, distribute remaining gap evenly so spokes
    # land at varied, non-uniform positions that still don't overlap.
    extra_per = (360.0 - total_demand) / spokes_count if total_demand < 360 else 0.0
    angles: list[float] = []
    cursor = 0.0
    for d_deg in demands:
        # spoke centre is cursor + half the demand (so neighbours don't
        # overlap)
        angles.append(cursor + d_deg / 2.0)
        cursor += d_deg + extra_per

    # 4. Assign (angle, z) to each block on its spoke. Spokes already
    # ordered (tall first, then paired). Angles computed above are
    # width-aware non-overlapping centres.
    placements: list[Placement] = []
    for spoke_idx, contents in enumerate(spokes):
        angle = angles[spoke_idx]
        if len(contents) == 1:
            slot = contents[0]
            # Centre vertically on the body, with a small upward bias so
            # the cornice + dome have breathing room.
            z = BODY_Z_MIN + BODY_Z_RANGE / 2.0 - 0.05
            slot.anchor_angle_deg = angle
            slot.anchor_z = z
            placements.append(Placement(event_id=slot.event_id,
                                         angle_deg=angle, z=z))
        else:
            top, bottom = contents
            total_h = top.block_h_d + bottom.block_h_d + INTRA_SPOKE_GAP_D
            slack = BODY_Z_RANGE - total_h
            # Centre the pair on the body
            top_y = BODY_Z_MIN + slack / 2.0 + bottom.block_h_d + INTRA_SPOKE_GAP_D
            bottom_y = BODY_Z_MIN + slack / 2.0
            top.anchor_angle_deg = angle
            top.anchor_z = top_y + top.block_h_d / 2.0
            bottom.anchor_angle_deg = angle
            bottom.anchor_z = bottom_y + bottom.block_h_d / 2.0
            placements.append(Placement(event_id=top.event_id,
                                         angle_deg=angle, z=top.anchor_z))
            placements.append(Placement(event_id=bottom.event_id,
                                         angle_deg=angle, z=bottom.anchor_z))

    return placements


def place_flythrough(flythrough: list[Flythrough],
                     target_placements: list[Placement],
                     target_block_widths: dict[int, float],
                     target_block_heights: dict[int, float] | None = None,
                     ) -> list[Flythrough]:
    """Drop flythrough afishas into the cylinder, avoiding collisions
    with target blocks both angularly AND vertically.

    For each flythrough we scan candidate (angle, z) cells on a grid;
    a cell is valid if its bbox does not overlap any already-placed
    target block or previously-placed flythrough.
    """
    if not flythrough:
        return []
    target_heights = target_block_heights or {}
    # Build occupancy list: each target is (angle_deg, z, half_w_deg, half_h)
    occupied: list[tuple[float, float, float, float]] = []
    for p in target_placements:
        w_d = target_block_widths.get(p.event_id, 0.6)
        h_d = target_heights.get(p.event_id, 1.0)
        half_w_deg = _arc_d_to_deg(w_d) / 2.0
        occupied.append((p.angle_deg, p.z, half_w_deg, h_d / 2.0))

    def collides(angle: float, z: float, half_w_deg: float, half_h: float) -> bool:
        for oa, oz, ohw, ohh in occupied:
            d_angle = min(abs(angle - oa), 360.0 - abs(angle - oa))
            if d_angle < (half_w_deg + ohw + 1.0) and \
               abs(z - oz) < (half_h + ohh + 0.04):
                return True
        return False

    # Candidate grid: angles every 5°, z at 4 bands across the body.
    z_bands = [BODY_Z_MIN + 0.30, BODY_Z_MIN + 0.85,
               BODY_Z_MIN + 1.40, BODY_Z_MIN + 1.95,
               BODY_Z_MIN + 2.30]
    # Distribute flythrough EVENLY around the cylinder — each one
    # gets a preferred starting angle from a round-robin pattern.
    placed: list[Flythrough] = []
    queue = sorted(flythrough, key=lambda f: -(f.w_d * f.h_d))
    n = len(queue) or 1
    for i, f in enumerate(queue):
        preferred_angle = (i * 360.0 / n) + 18.0  # offset from 0° seam
        half_w_deg = _arc_d_to_deg(f.w_d) / 2.0
        half_h = f.h_d / 2.0
        found = False
        # Sweep both directions from preferred_angle, growing the search
        # ring outward.
        for ring in range(0, 360, 5):
            for sign in (1, -1):
                a = (preferred_angle + sign * ring) % 360.0
                for z in z_bands:
                    if not collides(a, z, half_w_deg, half_h):
                        f.anchor_angle_deg = a
                        f.anchor_z = z
                        occupied.append((a, z, half_w_deg, half_h))
                        placed.append(f)
                        found = True
                        break
                if found:
                    break
            if found:
                break
    return placed
