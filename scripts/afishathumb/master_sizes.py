"""Round-11 master sizes — single source of truth for poster + sticker
dimensions on the afishathumb cylinder.

Why this module exists: rounds 1–10 had each module (scene_llm,
prepare_slot, typography) recompute its own size and font scale,
leading to title font sizes that jumped wildly between events and
extras that were sometimes too small to read. Per spec rows N3, N1,
R2 (round-10) all primaries are normalized to a shared **visible
area** and every sticker type uses one master cap height across all
events.

All sizes are in cylinder D-units (D = body diameter). The arc length
of the body is `π × D`, the body height is ≈ `2.5 × D`.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass


# Primary poster area (N3): every event's primary poster occupies this
# many D² of visible cylinder surface, regardless of its aspect. Picked
# so that a square primary (~0.55 × 0.55 D) covers ≈ 65% of the camera
# frame at the main-beat radius 2.0 / lens 50mm. Portrait posters become
# narrow-and-tall, landscape posters wide-and-short, but each event
# carries the same visual weight.
# SPLIT SCALE (round-11e) — posters and stickers scale independently
# so the poster-vs-sticker visual ratio is what the operator wants on
# the cylinder: posters BIG (the eye candy and the hook), stickers
# compact (still readable at camera zoom, but small on Mercator).
POSTER_SCALE = 0.65
STICKER_SCALE = 0.45

# Primary stays at master 0.30 D² area (full size at POSTER_SCALE=1.0).
MASTER_PRIMARY_AREA_D2 = 0.30 * (POSTER_SCALE ** 2)
MASTER_SCALE = STICKER_SCALE  # kept for back-compat with build_today

# Extra image area (N1 canonical): half the primary's area. Floor is
# 1/5 of primary (0.06 D²) — below that text on the extra cannot be
# read at the main-beat framing.
MASTER_EXTRA_AREA_D2 = MASTER_PRIMARY_AREA_D2 * 0.50
EXTRA_AREA_FLOOR_D2 = MASTER_PRIMARY_AREA_D2 * 0.20

# Flythrough primaries: smaller than target primaries so the eye can
# still tell which afishas are "the announced ones".
FLYTHROUGH_PRIMARY_AREA_D2 = MASTER_PRIMARY_AREA_D2 * 0.55

# Stickers — all scaled by STICKER_SCALE so they're compact on Mercator
# but still readable at camera zoom.
TITLE_CAP_H_D = 0.075 * STICKER_SCALE
DATE_CAP_H_D = 0.060 * STICKER_SCALE
COST_CAP_H_D = 0.060 * STICKER_SCALE
LOCATION_CAP_H_D = 0.048 * STICKER_SCALE
DIGEST_CAP_H_D = 0.034 * STICKER_SCALE

STICKER_PAD_H_D = 0.012 * STICKER_SCALE
STICKER_PAD_V_TOP_D = 0.010 * STICKER_SCALE
STICKER_PAD_V_BOTTOM_D = 0.014 * STICKER_SCALE

CELL_GAP_D = 0.012 * STICKER_SCALE

# Title cell can stretch across the event-block width (B1 corrected):
# short titles get a wide banner, long titles wrap to 2 lines at the
# same cap height. This is the MAX width a title cell is allowed to
# occupy on the cylinder (in D-units of arc).
TITLE_MAX_W_D = 0.95
TITLE_MIN_W_D = 0.30
TITLE_MAX_LINES = 2

# Imperfection (IR1, restored — but MILDER than round-10 first pass):
# Posters are glued straight — operator round-10b wording «афиши нужно
# было клеить ровно». Only stickers carry a slight tilt to imitate a
# bill-poster's hand. Bento composition still applies.
# Tilts are sampled uniformly across the role-specific range. Posters
# stay 0° («афиши нужно было клеить ровно»). Titles get a small range
# only — operator round-11c «качание заголовков можно минимизировать
# до небольших значений». Other stickers carry a wider range.
POSTER_TILT_RANGE_DEG = (0.0, 0.0)
TITLE_TILT_RANGE_DEG = (0.0, 1.2)
STICKER_TILT_RANGE_DEG = (0.0, 3.5)
PEEL_PROB_PER_PAPER = 0.25
PEEL_INTENSITY_RANGE = (0.06, 0.18)
WRINKLE_RANGE = (0.03, 0.10)

# Extras canonical = linear 1/2 of primary (operator wording «уменьшенный
# в 2 раза размер»). Area = 1/4 of primary then; ≥1/5 of primary's
# longer side remains the readability floor.
EXTRA_LINEAR_RATIO = 0.50

# Z-extent (vertical) of one event-block on the cylinder body. The body
# is z ∈ [0.18, 2.68] (≈ 2.5 D). Two rows of event-blocks with a small
# gap means each block can be at most ≈ 1.20 D tall; we leave 0.10 D
# above and below for the column to breathe.
EVENT_BLOCK_MAX_H_D = 1.20
EVENT_BLOCK_MAX_ARC_DEG = 110.0


@dataclass(frozen=True)
class PrimaryDims:
    """Shared visible-area normalization (N3)."""
    w_d: float
    h_d: float


def primary_dims_from_aspect(aspect: float,
                             area_d2: float = MASTER_PRIMARY_AREA_D2) -> PrimaryDims:
    """Returns `(width, height)` in D-units for a primary poster with
    the given aspect (w/h) so its visible area equals `area_d2`.

    Portrait posters (`aspect < 1`) come out narrow-and-tall; landscape
    posters (`aspect > 1`) come out wide-and-short — but `w × h` is
    always `area_d2`.
    """
    aspect = max(0.30, min(3.30, float(aspect)))  # clamp pathological inputs
    w = math.sqrt(area_d2 * aspect)
    h = math.sqrt(area_d2 / aspect)
    return PrimaryDims(w_d=w, h_d=h)


def extra_dims_from_aspect(aspect: float,
                           primary_area_d2: float = MASTER_PRIMARY_AREA_D2,
                           ratio: float = 0.50) -> PrimaryDims:
    """Extras at `ratio` of primary visible area (canonical = 0.50, per N1)."""
    return primary_dims_from_aspect(aspect, primary_area_d2 * ratio)


def flythrough_dims_from_aspect(aspect: float) -> PrimaryDims:
    return primary_dims_from_aspect(aspect, FLYTHROUGH_PRIMARY_AREA_D2)


def title_cell_dims(text_length_chars: int,
                    block_width_d: float) -> tuple[float, float, int]:
    """Returns `(width_d, height_d, line_count)` for a title cell at the
    master cap height. Width fits the text at one line if possible,
    otherwise wraps to two lines (R2 ceiling).

    Rough heuristic for advance width: Druk Cyr Heavy averages
    ≈ 0.55 × cap_height per glyph in D-units.
    """
    avg_advance = TITLE_CAP_H_D * 0.55
    one_line_w = text_length_chars * avg_advance + 2 * STICKER_PAD_H_D
    max_w = min(block_width_d, TITLE_MAX_W_D)
    if one_line_w <= max_w:
        # one line at master cap
        h = TITLE_CAP_H_D + STICKER_PAD_V_TOP_D + STICKER_PAD_V_BOTTOM_D
        w = max(TITLE_MIN_W_D, one_line_w)
        return w, h, 1
    # wrap to 2 lines
    two_line_w = max(TITLE_MIN_W_D, max_w)
    h = 2 * TITLE_CAP_H_D + STICKER_PAD_V_TOP_D + STICKER_PAD_V_BOTTOM_D + TITLE_CAP_H_D * 0.20
    return two_line_w, h, 2


def info_sticker_dims(text_length_chars: int,
                      cap_h_d: float,
                      max_w_d: float = 0.55) -> tuple[float, float]:
    """Returns `(width_d, height_d)` for a single-line info sticker
    sized to fit `text_length_chars` at `cap_h_d`."""
    avg_advance = cap_h_d * 0.55
    w = min(max_w_d, text_length_chars * avg_advance + 2 * STICKER_PAD_H_D)
    h = cap_h_d + STICKER_PAD_V_TOP_D + STICKER_PAD_V_BOTTOM_D
    return w, h


def seeded_tilt_deg(seed_key: str, *,
                    is_poster: bool = False,
                    is_title: bool = False) -> float:
    """Returns a small angular tilt deterministic per `seed_key`.

    Posters straight (0°). Title sticker has a narrow range (≤1.2°).
    Other stickers carry the wider range to imitate a bill-poster's
    hand.
    """
    r = random.Random(seed_key)
    sign = 1.0 if r.random() < 0.5 else -1.0
    if is_poster:
        rng = POSTER_TILT_RANGE_DEG
    elif is_title:
        rng = TITLE_TILT_RANGE_DEG
    else:
        rng = STICKER_TILT_RANGE_DEG
    if rng[1] <= 0.0:
        return 0.0
    mag = r.uniform(*rng)
    return sign * mag


def seeded_peel(seed_key: str) -> tuple[tuple[bool, bool, bool, bool], float]:
    """Returns `(peel_corners, peel_intensity)`. Peel probability is
    `PEEL_PROB_PER_PAPER` for ONE corner — never more than one corner
    per paper, never on stickers that carry critical typography."""
    r = random.Random(seed_key)
    if r.random() >= PEEL_PROB_PER_PAPER:
        return (False, False, False, False), 0.0
    corner_idx = r.randrange(4)
    corners = [False, False, False, False]
    corners[corner_idx] = True
    intensity = r.uniform(*PEEL_INTENSITY_RANGE)
    return tuple(corners), intensity  # type: ignore[return-value]


def seeded_wrinkle(seed_key: str) -> float:
    return random.Random(seed_key + ":wrinkle").uniform(*WRINKLE_RANGE)
