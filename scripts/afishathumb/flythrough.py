"""Round-11 flythrough afishas — auxiliary posters on the cylinder
that the camera passes by but does NOT dwell on.

Per spec A1: «они тоже элементы бенто и вообще общего планирования,
разница только в том, что ты на них не останавливаешься камерой».

Sources, in priority order:
  1. Events from `selection_today.json` past the target cap (targets =
     first N events, flythrough = rest).
  2. Excursion digest (TODO — lands in a follow-up commit, requires
     wiring `docs/features/excursions/...` data source).

Each flythrough afisha is just the primary image scaled to a smaller
master size; no stickers, no title banner.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from scripts.afishathumb.master_sizes import (
    FLYTHROUGH_PRIMARY_AREA_D2,
    POSTER_TILT_RANGE_DEG,
    primary_dims_from_aspect,
    seeded_peel,
    seeded_tilt_deg,
)


@dataclass
class Flythrough:
    """A single non-dwell poster glued to the cylinder."""
    event_id: int
    image_path: str
    aspect: float
    w_d: float
    h_d: float
    anchor_angle_deg: float = 0.0   # set by column packer
    anchor_z: float = 1.65          # set by column packer
    tilt_deg: float = 0.0
    peel_corners: tuple[bool, bool, bool, bool] = (False, False, False, False)
    peel_intensity: float = 0.0


def build_flythrough(event_id: int, image_path: str, aspect: float,
                     *, rng_seed: Optional[str] = None) -> Flythrough:
    """Compose a flythrough afisha at FLYTHROUGH_PRIMARY_AREA_D2 size
    with mild glue imperfection."""
    p = primary_dims_from_aspect(aspect, area_d2=FLYTHROUGH_PRIMARY_AREA_D2)
    seed = rng_seed or f"flythrough:{event_id}"
    # Flythrough posters can have a tiny tilt — they're auxiliary, the
    # bill-poster wouldn't carefully straighten every one. Reuse the
    # poster (0°) range so they read as posters not stickers.
    tilt = seeded_tilt_deg(seed + ":tilt", is_poster=True)
    corners, intensity = seeded_peel(seed + ":peel")
    return Flythrough(
        event_id=event_id,
        image_path=image_path,
        aspect=aspect,
        w_d=p.w_d,
        h_d=p.h_d,
        tilt_deg=tilt,
        peel_corners=corners,
        peel_intensity=intensity,
    )
