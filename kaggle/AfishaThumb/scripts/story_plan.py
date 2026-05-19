"""Plan the camera "story" through a single AfishaThumb slot.

We treat each slot as a `~5.7s` mini-narrative (same envelope as
CherryFlash scenes) that must deliver six pieces of information to the
viewer in time for them to decide whether to attend:

  1. **image**     — the poster as an emotional hook
  2. **title**     — what the event is called
  3. **essence**   — what happens (search_digest or richer poster text)
  4. **when**      — date + time
  5. **where**     — location, address, city
  6. **cost**      — free or price

Some of these may already be on the poster (title, sometimes date),
which we detect with `poster_analysis` + the event's database row. Beats
for redundant pieces are dropped instead of building a duplicate
sticker. The remaining beats are ordered so visual hooks come first,
hard facts last.

The output is consumed by:
  - `render_slot_blender.py` (Stage 3 will sample each beat as a key
    frame and tween between them with eased curves);
  - `slot_trace.py` (overlays the beat dots + camera path onto the
    rendered `slot_overview.png` for human review).

This module is deliberately deterministic — no LLM — because every
input is already structured (event row + poster text density). Future
LLM additions would only refine subtle cases like "is the poster title
legible enough to skip a title sticker".
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional


PAPER_ROLE_POSTER = "poster"
PAPER_ROLE_DATE = "date"
PAPER_ROLE_LOCATION = "location"
PAPER_ROLE_COST = "cost"
PAPER_ROLE_DIGEST = "digest"
PAPER_ROLE_TITLE = "title"
PAPER_ROLE_ATTENTION = "attention"


@dataclass
class StoryBeat:
    """One discrete attention moment in the slot's flight."""
    role: str                # one of PAPER_ROLE_* — which artifact to land on
    target_anchor: str       # paper.name in the manifest, or "cluster"
    label: str               # human-readable, shows on the trace overlay
    dwell_s: float           # seconds the camera lingers here
    lens_mm: float           # camera focal length at this beat
    radius_factor: float = 1.0   # multiplier on the cluster radius for this beat
    color: str = "#1B2D5E"   # overlay accent
    is_focus_push: bool = False  # "info readout" close-up (red ring on the overlay)


@dataclass
class StoryPlan:
    """Full beat sequence for a slot."""
    beats: list[StoryBeat] = field(default_factory=list)
    coverage_tests: dict = field(default_factory=dict)
    # Per-test boolean: did we manage to deliver this info piece?
    # Tests are: image, title, essence, when, where, cost.


def plan_story(
    event_id: int,
    *,
    attention_anchor_count: int = 2,
    has_title_on_poster: bool,
    has_date_on_poster: bool,
    has_location_on_poster: bool,
    has_essence_on_poster: bool,
    has_cost_on_poster: bool,
    has_digest_sticker: bool,
    has_date_sticker: bool,
    has_location_sticker: bool,
    has_cost_sticker: bool,
    has_title_sticker: bool,
    has_attention_sticker: bool,
    is_free: bool,
    poster_paper_name: str,
    date_paper_name: Optional[str],
    location_paper_name: Optional[str],
    cost_paper_name: Optional[str],
    digest_paper_name: Optional[str],
    title_paper_name: Optional[str],
    attention_paper_name: Optional[str],
) -> StoryPlan:
    """Single-arc beat sequence for one slot, S1-locked: 2 or 3
    attention anchors (image + 1 or 2 key info), all other info pieces
    delivered DRIVE-BY along the continuous arc (they sit in frame as
    the camera glides past, no explicit dwell).

    The total budget is ~5.7 s. Two-anchor version trades a slower image
    dwell for fewer cuts; three-anchor version trades dwell length for
    one more focused info push.
    """
    rng = random.Random(event_id)
    if attention_anchor_count not in (2, 3):
        attention_anchor_count = 2
    is_three = attention_anchor_count == 3

    beats: list[StoryBeat] = []

    # 0. Arrival / cluster overview.
    beats.append(StoryBeat(
        role="overview", target_anchor="cluster",
        label="приземление",
        dwell_s=0.50 + rng.uniform(-0.05, 0.05),
        lens_mm=42.0, radius_factor=1.95, color="#0E7A6B",
    ))

    # 1. Image dwell — slow continuous drift. Longer when only 2 anchors.
    beats.append(StoryBeat(
        role="image", target_anchor=poster_paper_name,
        label="образ",
        dwell_s=(2.65 if not is_three else 1.85) + rng.uniform(-0.10, 0.20),
        lens_mm=64.0, radius_factor=1.05, color="#1B2D5E",
    ))

    # 2..N. Key info anchors — ranked priority list of what's worth a
    # focus push. We pick the top (attention_anchor_count - 1) entries.
    priorities: list[tuple[str, str, str, str]] = []
    # (role, anchor_name, label, color)
    if has_attention_sticker and attention_paper_name:
        priorities.append(("attention", attention_paper_name, "обратите внимание", "#9F2933"))
    if has_cost_sticker and cost_paper_name:
        priorities.append((
            "cost", cost_paper_name,
            "бесплатно" if is_free else "сколько",
            "#9F8B1F" if is_free else "#1B2D5E",
        ))
    if has_date_sticker and date_paper_name:
        priorities.append(("when", date_paper_name, "когда", "#1B2D5E"))
    elif has_date_on_poster:
        priorities.append(("when", poster_paper_name, "когда (на афише)", "#1B2D5E"))
    if has_location_sticker and location_paper_name:
        priorities.append(("where", location_paper_name, "где", "#1B2D5E"))
    elif has_location_on_poster:
        priorities.append(("where", poster_paper_name, "где (на афише)", "#1B2D5E"))
    # Essence sticker as a fallback anchor (rarely picked unless nothing
    # higher-priority remains).
    if has_digest_sticker and digest_paper_name:
        priorities.append(("essence", digest_paper_name, "суть", "#1B2D5E"))

    n_key = attention_anchor_count - 1   # 1 or 2 key-info beats
    picked = priorities[:n_key]
    per_key_dwell = (1.55 if not is_three else 1.25)
    for role, anchor, label, color in picked:
        beats.append(StoryBeat(
            role=role, target_anchor=anchor, label=label,
            dwell_s=per_key_dwell + rng.uniform(-0.10, 0.10),
            lens_mm=78.0, radius_factor=0.82, color=color,
            is_focus_push=True,
        ))

    # Final. Exit glide toward the next slot — soft pull-back, NOT a
    # full establishing shot. ~0.6 s.
    beats.append(StoryBeat(
        role="exit", target_anchor=poster_paper_name,
        label="дальше →",
        dwell_s=0.55 + rng.uniform(-0.10, 0.10),
        lens_mm=50.0, radius_factor=1.35, color="#7A7060",
    ))

    # Coverage: image always; title/essence/when/where/cost are delivered
    # either by a focused anchor OR by drive-by passage along the arc.
    # Drive-by counts as covered IF the info is on the poster OR has a
    # sticker (both end up in-frame during the arc).
    def _covered(on_poster: bool, has_sticker: bool, picked_roles: set[str], role: str) -> bool:
        return on_poster or has_sticker or (role in picked_roles)

    picked_roles = {p[0] for p in picked}
    coverage = {
        "image": True,
        "title": has_title_on_poster or has_title_sticker,
        "essence": has_essence_on_poster or has_digest_sticker,
        "when": _covered(has_date_on_poster, has_date_sticker, picked_roles, "when"),
        "where": _covered(has_location_on_poster, has_location_sticker, picked_roles, "where"),
        "cost": _covered(has_cost_on_poster, has_cost_sticker, picked_roles, "cost"),
    }

    return StoryPlan(beats=beats, coverage_tests=coverage)


__all__ = ["StoryBeat", "StoryPlan", "plan_story"]
