"""LLM-C — multi-scene tour planner.

Given all per-event LLM-A + LLM-B plans (each event has its own
sticker layout + per-scene camera beats), asks Gemini-lite to:

  - decide the angular position of each event on the cylinder
    (placing 6 events around 360° so neighbours have visually
    natural transitions);
  - decide the reading ORDER (which event the camera visits first /
    second / …) — promo / "обратите внимание" events should not lead;
  - design the transitions between consecutive events: fast fly-by
    vs slow ease, where to crane up/down, how to glance at the
    in-between bare cylinder without dwelling.

Output: a tour plan with per-event `cylinder_angle_deg` slot, the
visit order, and a list of inter-scene transitions.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

DEFAULT_MODEL = "gemini-3.1-flash-lite"
MAX_RETRIES = 3
RETRY_DELAYS_SEC = (0.6, 1.6, 4.0)


@dataclass
class EventSlot:
    event_id: int
    cylinder_angle_deg: float   # which "spoke" of the cylinder this event sits on
    visit_order: int            # 1..N — first event read is 1, last is N
    cylinder_z_offset: float = 0.0   # vertical shift in D-units, relative
                                     # to anchor_z=1.65. Negative = below mid;
                                     # positive = above mid. Use the full
                                     # z-range of the body, not just one row.


@dataclass
class TourTransition:
    from_event: int
    to_event: int
    arc_deg: float             # how far the camera travels along the cylinder
    duration_s: float
    transit: str               # "slow_ease" | "fast_flyby" | "crane_up" | "crane_down"
    pass_by_event_ids: list[int] = field(default_factory=list)


@dataclass
class TourPlan:
    slots: list[EventSlot] = field(default_factory=list)
    transitions: list[TourTransition] = field(default_factory=list)
    narrative: str = ""
    model: str = ""
    source: str = "llm"


def _build_prompt(events_brief: list[dict]) -> str:
    lines = [
        "You are the director PLANNING the full camera tour over an advertising column (afishathumb) that carries N events. Each event has its own per-event layout + camera plan already designed.",
        "",
        "PRODUCT GOAL: the viewer sees the whole tour in ~ 35-40 seconds total. At the end they remember which events were on the column and have decided which to attend.",
        "",
        "CYLINDER GEOMETRY:",
        "  - Body wraps 360° around the cylinder axis.",
        "  - Body height z ∈ [0.18, 2.68] in D-units (≈ 2.5 D total).",
        "  - The whole surface is usable for events. DO NOT pack everything in one z-row.",
        "",
        "EACH EVENT IS A 2D RECTANGLE ON THIS SURFACE — you must place it AT a (cylinder_angle_deg, cylinder_z_offset) so that:",
        "  - its bounding box does not overlap any OTHER event's bounding box,",
        "  - at least 5° angular gap and 0.10 D vertical gap separates events,",
        "  - the FULL vertical space is used (e.g. 3 events in the upper half z≈1.9, 3 events in the lower half z≈1.0 is a perfectly good arrangement; or scattered).",
        "",
        "PER-EVENT INPUTS:",
    ]
    for i, e in enumerate(events_brief, start=1):
        cluster_arc = e.get("cluster_arc_deg", 90.0)
        cluster_h = e.get("cluster_h_d", 1.5)
        v_off = e.get("cluster_v_center_offset", 0.0)
        lines += [
            f"  [{i}] event_id={e['event_id']}",
            f"      title=«{e['title']}»",
            f"      hook={e['hook_short']}",
            f"      cluster_arc_deg={cluster_arc:.0f}  (angular width of the event's cluster)",
            f"      cluster_h_d={cluster_h:.2f}        (vertical height of the event's cluster)",
            f"      cluster_center_v_offset={v_off:.2f}  (where the cluster's center sits relative to its own anchor_z=1.65; use this as a hint of its natural balance)",
            f"      is_promo={e.get('is_promo', False)}",
        ]
    lines += [
        "",
        "YOUR JOB:",
        "  A) For EACH event pick a `cylinder_angle_deg` (0..360) AND a `cylinder_z_offset` (in D-units, signed). The event's cluster will be PLACED at (anchor_z=1.65 + cylinder_z_offset) on the cylinder.",
        "  B) Make sure event bounding boxes don't overlap. Use the FULL vertical extent of the cylinder — place some events higher (positive z_offset, up to +0.6) and some lower (negative, down to -0.6).",
        "  C) Pick the VISIT ORDER (1..N) — the order the camera reads events:",
        "     - never start the tour on a promo event;",
        "     - never put two promos consecutively;",
        "     - prefer a strong visual hook as the first read;",
        "     - leave the cheapest / free events as either the first 'hook' or a memorable closing beat.",
        "  D) For each consecutive transition (visit_order N → N+1), pick the transit:",
        "     - 'slow_ease' for neighbours (small arc difference);",
        "     - 'fast_flyby' for big angular jumps;",
        "     - 'crane_up' / 'crane_down' when the camera changes z by ≥ 0.3 D between events.",
        "",
        "OUTPUT — STRICT JSON, NO MARKDOWN:",
        '{',
        '  "narrative": "one sentence describing the order + cinematic intent",',
        '  "slots": [',
        '    {"event_id": 4834, "cylinder_angle_deg": 0,   "cylinder_z_offset":  0.45, "visit_order": 1},',
        '    {"event_id": 4832, "cylinder_angle_deg": 75,  "cylinder_z_offset": -0.50, "visit_order": 2},',
        '    {"event_id": 4828, "cylinder_angle_deg": 140, "cylinder_z_offset":  0.40, "visit_order": 3}',
        '  ],',
        '  "transitions": [',
        '    {"from_event": 4834, "to_event": 4832, "arc_deg": 75,  "duration_s": 0.8, "transit": "crane_down", "pass_by_event_ids": []},',
        '    {"from_event": 4832, "to_event": 4828, "arc_deg": 65,  "duration_s": 0.8, "transit": "crane_up",   "pass_by_event_ids": []}',
        '  ]',
        '}',
        "",
        "Return ONLY the JSON object.",
    ]
    return "\n".join(lines)


def _call(prompt: str, model: str) -> str:
    """One physical attempt through the mandatory shared gateway."""

    async def _generate() -> str:
        from google_ai import GoogleAIClient, SecretsProvider
        from google_ai.limiter_supabase import (
            build_google_ai_limiter_supabase_client,
        )

        client = GoogleAIClient(
            supabase_client=build_google_ai_limiter_supabase_client(
                require_configured=True,
            ),
            secrets_provider=SecretsProvider(),
            consumer="afishathumb.tour",
            default_env_var_name="GOOGLE_API_KEY",
        )
        client.allow_reserve_fallback = False
        client.allow_local_limiter_fallback = False
        client.allow_local_limiter_on_reserve_error = False
        client.max_retries = 1
        client.fallback_models = []
        response_text, _usage = await client.generate_content_async(
            model=model,
            prompt=prompt,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.4,
                "max_output_tokens": 3072,
            },
            max_output_tokens=3072,
        )
        return response_text

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_generate())
    raise RuntimeError("tour_llm synchronous API cannot run inside an active event loop")


def _parse(raw: str) -> TourPlan:
    text = raw.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    obj = json.loads(text)
    p = TourPlan(narrative=str(obj.get("narrative", "")))
    for s in obj.get("slots") or []:
        try:
            p.slots.append(EventSlot(
                event_id=int(s["event_id"]),
                cylinder_angle_deg=float(s["cylinder_angle_deg"]) % 360.0,
                visit_order=int(s["visit_order"]),
                cylinder_z_offset=float(s.get("cylinder_z_offset", 0.0)),
            ))
        except Exception:
            continue
    for t in obj.get("transitions") or []:
        try:
            p.transitions.append(TourTransition(
                from_event=int(t["from_event"]),
                to_event=int(t["to_event"]),
                arc_deg=float(t["arc_deg"]),
                duration_s=float(t["duration_s"]),
                transit=str(t.get("transit", "slow_ease")),
                pass_by_event_ids=[int(x) for x in (t.get("pass_by_event_ids") or [])],
            ))
        except Exception:
            continue
    return p


def plan_tour(
    events_brief: list[dict],
    *,
    cache_path: Optional[Path] = None,
    model: str = DEFAULT_MODEL,
    force_refresh: bool = False,
) -> TourPlan:
    if cache_path is not None and not force_refresh and cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            out = TourPlan(narrative=data.get("narrative", ""), source="cache")
            out.model = data.get("model", "")
            for s in data.get("slots", []):
                out.slots.append(EventSlot(**s))
            for t in data.get("transitions", []):
                out.transitions.append(TourTransition(**t))
            return out
        except Exception:
            pass

    prompt = _build_prompt(events_brief)
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            raw = _call(prompt, model)
            plan = _parse(raw)
            if not plan.slots:
                raise ValueError("empty tour plan")
            plan.model = model
            if cache_path is not None:
                serial = {
                    "narrative": plan.narrative,
                    "model": plan.model,
                    "slots": [s.__dict__ for s in plan.slots],
                    "transitions": [t.__dict__ for t in plan.transitions],
                }
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(serial, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
            return plan
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(RETRY_DELAYS_SEC[min(attempt, len(RETRY_DELAYS_SEC) - 1)])
    print(f"[tour_llm] giving up. last err: {last_err!r}")
    return TourPlan(source="fallback", model=model)


__all__ = ["EventSlot", "TourTransition", "TourPlan", "plan_tour"]
