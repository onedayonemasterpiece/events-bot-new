"""LLM-B per-scene camera planner.

Given a manifest produced by LLM-A (layout of poster + extras + stickers
on the cylinder, plus the event's metadata + LLM-detected info regions),
asks `gemini-3.1-flash-lite` to plan the camera flight through the
scene — start, attention dwells, transitions, exit.

The product goal: a viewer who watches the resulting ≈ 5.7 s flight
must end with NAME + WHEN + WHERE + COST + IMAGE understood — enough
to decide attend / skip.

Output is a list of `CameraBeat`s with absolute cylinder coords
(angle, z, radius, lens, tilt, dwell, transit), ready to drive
Blender keyframes (Stage 3) and the per-slot trace overlay.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


DEFAULT_MODEL = "gemini-3.1-flash-lite"
MAX_RETRIES = 3
RETRY_DELAYS_SEC = (0.6, 1.6, 4.0)


@dataclass
class CameraBeat:
    role: str          # "entry" | "image" | "title" | "when" | "where" | "cost" | "essence" | "exit"
    label: str         # short Russian label for the trace overlay
    target_anchor: str # paper.name to focus on (or "cluster" for overview)
    angle_deg: float   # camera azimuth around cylinder axis (deg)
    z: float           # camera height (D units)
    radius: float      # distance from cylinder axis (D units)
    lens_mm: float
    tilt_deg: float
    dwell_s: float
    transit: str = "ease"  # "ease" | "fast_arc" | "push" | "settle"
    is_focus_push: bool = False


@dataclass
class CameraPlan:
    beats: list[CameraBeat] = field(default_factory=list)
    narrative: str = ""
    model: str = ""
    source: str = "llm"


def _build_prompt(scene_brief: dict, papers: list[dict]) -> str:
    e = scene_brief
    lines = [
        "You are the cinematographer planning a one-shot camera flight over a single event scene on a cylindrical advertising column.",
        "",
        "PRODUCT GOAL: in ≈ 5.7 seconds the viewer must reliably READ:",
        "  - the event name (название)",
        "  - WHEN — date + time",
        "  - WHERE — venue + city",
        "  - COST — free / price",
        "  - the visual hook (image of the event)",
        "…enough to decide attend / skip.",
        "",
        "EVENT METADATA:",
        f"  title: «{e['title']}»",
        f"  date+time: {e['date']} {e['time']}",
        f"  where: {e['location']}" + (f", {e['city']}" if e.get('city') else ""),
        f"  cost: {e['cost_text']}",
        f"  description: {e.get('search_digest', '')[:140] or '—'}",
        "",
        "PAPERS PLACED ON THE CYLINDER (each is an object on the column surface):",
        "  Coordinates: angle_deg (azimuth on cylinder axis), z (height in D units, body span ≈ 0.18..2.68),",
        "  width/height in D units. Cylinder body radius = 0.5.",
        "",
    ]
    for p in papers:
        lines.append(
            f"  - {p['name']}: angle={p['anchor_angle_deg']:.1f}°  z={p['anchor_z']:.2f}  "
            f"w={p['width']:.2f}  h={p['height']:.2f}"
        )
    lines += [
        "",
        "CAMERA RULES (HARD — failed outputs are retried):",
        "  1. Total beats: between 3 and 4 (entry → 1-2 dwells → exit). NEVER more than 4. NEVER fewer than 3.",
        "  2. Angular sweep is MONOTONIC across beats (strictly non-decreasing OR strictly non-increasing). NO back-and-forth between consecutive beats.",
        "  3. Adjacent beats' angle_deg must differ by ≥ 10° (no two beats at near-identical angles — that would feel like the camera frozen).",
        "  4. The camera sweeps through ≤ 90° of angular range total (cluster reading, not orbit). Entry-to-exit angular delta ≤ 90°.",
        "  5. For info readout (date / location / cost / title focus) the camera PUSHES closer: radius ≤ 1.5. There must be EXACTLY ONE such focus push beat.",
        "  6. Eased motion. Combine multiple axes per beat (dolly + crane + tilt + slight off-radial approach). Avoid constant velocity.",
        "  7. Total dwell_s across all beats sums to 5.0..6.0.",
        "  8. Exit beat: soft pull-back; radius 2.3..3.0; tilt ≤ 5°.",
        "  9. The reading order is up to you, but the SCENE MUST DELIVER all 5 facts (name + when + where + cost + image). If something isn't on the primary, you MUST land on its sticker once.",
        "",
        "RADIUS GUIDE (cylinder radius = 0.5):",
        "  - Entry: 2.5–3.2 (medium-wide to frame the cluster).",
        "  - Main image / banner dwell: 1.8–2.2.",
        "  - Focus push on info: 1.2–1.5.",
        "  - Exit: 2.3–3.0.",
        "",
        "OUTPUT — STRICT JSON, NO MARKDOWN:",
        '{',
        '  "narrative": "one sentence describing the read order chosen",',
        '  "beats": [',
        '    {"role": "entry",  "label": "вход",       "target_anchor": "cluster",       "angle_deg": -18, "z": 1.85, "radius": 2.9, "lens_mm": 42, "tilt_deg": -3, "dwell_s": 0.6, "transit": "ease",  "is_focus_push": false},',
        '    {"role": "image",  "label": "образ",      "target_anchor": "Poster.4131",   "angle_deg": -3,  "z": 1.65, "radius": 2.0, "lens_mm": 60, "tilt_deg": -1, "dwell_s": 1.6, "transit": "ease",  "is_focus_push": false},',
        '    {"role": "when",   "label": "когда",      "target_anchor": "Date.4131",     "angle_deg": 18,  "z": 1.85, "radius": 1.4, "lens_mm": 80, "tilt_deg": -5, "dwell_s": 1.2, "transit": "push", "is_focus_push": true},',
        '    {"role": "where",  "label": "где",        "target_anchor": "Location.4131", "angle_deg": 24,  "z": 1.55, "radius": 1.4, "lens_mm": 78, "tilt_deg": 3,  "dwell_s": 1.1, "transit": "ease",  "is_focus_push": true},',
        '    {"role": "exit",   "label": "выход",      "target_anchor": "cluster",       "angle_deg": 38,  "z": 1.70, "radius": 2.6, "lens_mm": 50, "tilt_deg": 1,  "dwell_s": 0.6, "transit": "settle", "is_focus_push": false}',
        '  ]',
        '}',
        "",
        "`target_anchor` MUST be one of the paper names listed above, OR the literal string `cluster` (geometric centre of the placed papers).",
        "Return ONLY the JSON object.",
    ]
    return "\n".join(lines)


def _call_gemini(prompt: str, model: str) -> str:
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
            consumer="afishathumb.camera",
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
                "temperature": 0.35,
                "max_output_tokens": 2048,
            },
            max_output_tokens=2048,
        )
        return response_text

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_generate())
    raise RuntimeError("camera_llm synchronous API cannot run inside an active event loop")


def _parse_plan(raw: str, valid_anchor_names: set[str]) -> CameraPlan:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    obj = json.loads(text)
    out = CameraPlan(narrative=str(obj.get("narrative", "")))
    for b in obj.get("beats") or []:
        try:
            anchor = str(b["target_anchor"])
            if anchor != "cluster" and anchor not in valid_anchor_names:
                # Tolerate slight name drift — try matching by role.
                anchor = "cluster"
            out.beats.append(CameraBeat(
                role=str(b["role"]),
                label=str(b.get("label", b["role"])),
                target_anchor=anchor,
                angle_deg=float(b["angle_deg"]),
                z=float(b["z"]),
                radius=max(0.6, float(b["radius"])),
                lens_mm=max(20.0, min(120.0, float(b["lens_mm"]))),
                tilt_deg=max(-25.0, min(25.0, float(b.get("tilt_deg", 0.0)))),
                dwell_s=max(0.2, min(3.0, float(b["dwell_s"]))),
                transit=str(b.get("transit", "ease")),
                is_focus_push=bool(b.get("is_focus_push", False)),
            ))
        except Exception:
            continue
    return out


def _validate_plan(plan: CameraPlan) -> list[str]:
    msgs: list[str] = []
    n = len(plan.beats)
    if n < 3 or n > 4:
        msgs.append(f"beat count = {n}; must be 3 or 4")
    # Monotonic angular sweep
    angles = [b.angle_deg for b in plan.beats]
    incr = all(angles[i + 1] >= angles[i] for i in range(n - 1))
    decr = all(angles[i + 1] <= angles[i] for i in range(n - 1))
    if not (incr or decr):
        msgs.append("angular sweep is not monotonic (back-and-forth detected)")
    # Minimum spread between consecutive beats
    for i in range(n - 1):
        if abs(angles[i + 1] - angles[i]) < 10.0:
            msgs.append(
                f"beats {i + 1} and {i + 2} angle delta {abs(angles[i + 1] - angles[i]):.1f}° "
                "< 10° (camera appears stuck)"
            )
    # Total angular sweep
    if angles and (max(angles) - min(angles) > 90.0):
        msgs.append(f"total angular sweep {max(angles) - min(angles):.0f}° > 90°")
    # Exactly one focus push
    pushes = sum(1 for b in plan.beats if b.is_focus_push)
    if pushes != 1:
        msgs.append(f"focus_push count = {pushes}; must be exactly 1")
    # Dwell budget
    total = sum(b.dwell_s for b in plan.beats)
    if total < 5.0 or total > 6.0:
        msgs.append(f"total dwell_s = {total:.1f}; must be 5.0..6.0")
    return msgs


def plan_camera_for_scene(
    scene_brief: dict,
    papers: list[dict],
    *,
    cache_path: Optional[Path] = None,
    model: str = DEFAULT_MODEL,
    force_refresh: bool = False,
) -> CameraPlan:
    if cache_path is not None and not force_refresh and cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            out = CameraPlan(narrative=data.get("narrative", ""), source="cache")
            out.model = data.get("model", "")
            for b in data.get("beats", []):
                out.beats.append(CameraBeat(**b))
            return out
        except Exception:
            pass

    valid_anchors = {p["name"] for p in papers}
    prompt = _build_prompt(scene_brief, papers)
    last = None
    violations_history: list[list[str]] = []
    for attempt in range(MAX_RETRIES):
        try:
            this_prompt = prompt
            if violations_history:
                this_prompt = (prompt
                    + "\n\nPREVIOUS ATTEMPT VIOLATED these rules — fix EACH one:\n"
                    + "\n".join(f"  - {m}" for m in violations_history[-1])
                    + "\nReturn a CORRECTED JSON.")
            raw = _call_gemini(this_prompt, model)
            plan = _parse_plan(raw, valid_anchors)
            if not plan.beats:
                raise ValueError("LLM-B returned empty beats")
            violations = _validate_plan(plan)
            if violations:
                violations_history.append(violations)
                print(f"[camera_llm] attempt {attempt + 1} violated {len(violations)} rules; retrying")
                for m in violations[:5]:
                    print(f"  - {m}")
                last = ValueError("validation failed")
                continue
            plan.model = model
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                serial = {
                    "narrative": plan.narrative,
                    "model": plan.model,
                    "beats": [
                        {
                            "role": b.role, "label": b.label,
                            "target_anchor": b.target_anchor,
                            "angle_deg": b.angle_deg, "z": b.z,
                            "radius": b.radius, "lens_mm": b.lens_mm,
                            "tilt_deg": b.tilt_deg,
                            "dwell_s": b.dwell_s, "transit": b.transit,
                            "is_focus_push": b.is_focus_push,
                        } for b in plan.beats
                    ],
                }
                cache_path.write_text(json.dumps(serial, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
            return plan
        except Exception as exc:  # noqa: BLE001
            last = exc
            d = RETRY_DELAYS_SEC[min(attempt, len(RETRY_DELAYS_SEC) - 1)]
            print(f"[camera_llm] attempt {attempt + 1}/{MAX_RETRIES} failed: {exc!r}; retry in {d}s")
            time.sleep(d)
    print(f"[camera_llm] giving up. last err: {last!r}")
    return CameraPlan(source="fallback", model=model)


__all__ = ["CameraBeat", "CameraPlan", "plan_camera_for_scene"]
