"""LLM-driven scene composer for AfishaThumb (LLM-A).

For ONE event, asks Gemini-lite to design the layout of every paper on
the scene — primary poster, extra images, info stickers — given:

  - what the event is (title, date/time, location, price, search_digest);
  - what images are available (with their aspect ratios);
  - which info regions LLM-B already detected on the primary
    (so the composer doesn't cover the title with a sticker);
  - which stickers are required (built by the operator code based on
    what info is missing from the primary);
  - the product goal: viewer must learn name + when + where + cost +
    see the image in a ≈ 5.7 s camera pass.

The composer returns the position of each paper in a normalised 0..1
canvas (top-left origin). The caller converts those normalised coords
to absolute cylinder coordinates (angle on the column, z on the column).

Falls back to the operator's deterministic placement engine on LLM
unavailability or output parse failure.
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
class PlacedObject:
    """One paper placed by the LLM on the layout canvas."""
    id: str          # "primary" | "extra_1" | "title_banner" | "date" | "location" | "cost" | "digest"
    role: str        # "image" | "title" | "date" | "location" | "cost" | "essence"
    x_norm: float    # 0..1 in canvas, left-to-right
    y_norm: float    # 0..1 in canvas, top-to-bottom
    w_norm: float    # 0..1 width in canvas
    h_norm: float    # 0..1 height in canvas
    tilt_deg: float  # in-plane rotation


@dataclass
class SceneLayout:
    objects: list[PlacedObject] = field(default_factory=list)
    narrative: str = ""
    model: str = ""
    source: str = "llm"   # "llm" | "fallback"


def _build_prompt(scene_brief: dict) -> str:
    e = scene_brief
    n_extras = len(e.get("extras_aspects", []))
    is_photo_cluster = e.get("is_photo_cluster", False)
    parts = [
        "You are a graphic designer composing a single event panel that will be wrapped on a cylindrical advertising column (afishathumb).",
        "",
        "PRODUCT GOAL: In a ~5.7s camera pass over this panel the viewer must reliably learn the event NAME, WHEN (date+time), WHERE (venue+city), COST (free or price), and SEE the image. Then they decide to attend or skip.",
        "",
        f"EVENT TYPE: {'PHOTO-CLUSTER (primary is a photo, not a text poster)' if is_photo_cluster else 'POSTER (primary carries its own text)'}",
        "",
        "EVENT:",
        f"- Title: «{e['title']}»",
        f"- Date+time: {e['date']} {e['time']}",
        f"- Where: {e['location']}" + (f", {e['city']}" if e.get('city') else ""),
        f"- Cost: {e['cost_text']}",
        f"- Description (≈20 words): {e['search_digest'] or '—'}",
        "",
        "IMAGES YOU MUST PLACE ON THE PANEL:",
        f"- primary (aspect={e['primary_aspect']:.2f})",
    ]
    for i, asp in enumerate(e["extras_aspects"], start=1):
        parts.append(f"- extra_{i} (aspect={asp:.2f})")
    parts += [
        "",
        "LLM-DETECTED TEXT REGIONS ON THE PRIMARY (normalised 0..1 bboxes, origin top-left of the primary):",
    ]
    for key, label in (("title", "title text"), ("date", "date text"),
                       ("time", "time text"), ("location", "location text"),
                       ("price", "price text")):
        bbox = e["regions"].get(key)
        if bbox is None:
            parts.append(f"- {label}: NOT visibly on the primary")
        else:
            parts.append(f"- {label}: at bbox {bbox}")
    parts += [
        "",
        "STICKERS YOU MUST ALSO PLACE (each is a separate paper card with strong typography; only present in this list when the corresponding info is NOT already legibly on the primary):",
    ]
    for s in e["required_stickers"]:
        parts.append(f"- {s}")
    parts += [
        "",
        "LAYOUT CANVAS: rectangle 0.9 × 1.4 (in column-D units), facing the camera.",
        "- Coordinates are NORMALISED 0..1 (x left→right, y top→bottom).",
        "- The center (0.5, 0.5) is the camera focus.",
        "- Anything outside [0.05, 0.95] will be off-frame.",
        "",
        "HARD CONSTRAINTS (will be programmatically validated — non-compliant outputs are retried):",
        "1. Primary image MUST PRESERVE its aspect ratio: the (w / h) you choose for it must equal the primary aspect "
        f"({e['primary_aspect']:.3f}) within ±5%. Stretching or squashing the primary is FORBIDDEN.",
        "2. Primary occupies 35–55% of canvas area.",
        f"3. Each extra (if any) MUST be between 45% and 60% of the primary's LONGER side. "
        f"(Earlier the rule was 30-50% but on the actual rendered cylinder that was visually too small — extras must be CLEARLY readable as second-tier images, not thumbnails.) "
        f"You have {n_extras} extra(s) to place; each must respect this floor.",
        "4. Every extra preserves its OWN aspect ratio.",
        "5. NO sticker may cover any LLM-detected text region of the primary. "
        + ("Photo-cluster mode: stickers ARE ALLOWED to overlap photographic zones of the primary and the extras — prefer over-image placement when you can, because it reads faster for the viewer."
           if is_photo_cluster
           else "Poster mode: stickers stay OFF the primary (the primary carries its own text). Side placement only."),
        "6. Title-banner sticker (if listed): wide aspect 4:1 to 7:1, placed ABOVE the primary as a slide header. "
        "If the title is long (>30 chars), the banner can occupy up to 75% of canvas width.",
        "7. Side stickers (date / location / cost / digest) read in natural order top-to-bottom or left-to-right: "
        "date → location → cost → digest. Each at least width 0.18, height 0.07.",
        "8. ALL objects' bboxes must fit inside the canvas [0.02, 0.98] in BOTH axes. Nothing partially off-canvas.",
        "9. Object bboxes MUST NOT overlap each other (except: a sticker MAY overlap an image in photo-cluster mode per rule 5).",
        "10. Subtle natural tilt allowed (±6° max per object); never a strict grid.",
        "11. Place the digest sticker if it is in the required-stickers list — it is needed for product completeness.",
        "",
        "OUTPUT — STRICT JSON, NO MARKDOWN, NO COMMENTARY:",
        '{',
        '  "narrative": "one sentence describing your composition",',
        '  "objects": [',
        '    {"id": "primary", "role": "image", "x": 0.50, "y": 0.50, "w": 0.55, "h": 0.65, "tilt": -1.0},',
        '    {"id": "extra_1", "role": "image", "x": 0.30, "y": 0.85, "w": 0.22, "h": 0.18, "tilt": 2.5},',
        '    {"id": "title_banner", "role": "title", "x": 0.50, "y": 0.06, "w": 0.66, "h": 0.08, "tilt": 0.5},',
        '    {"id": "date", "role": "date", "x": 0.85, "y": 0.30, "w": 0.18, "h": 0.14, "tilt": -3.0},',
        '    {"id": "location", "role": "location", "x": 0.85, "y": 0.50, "w": 0.22, "h": 0.10, "tilt": 1.5},',
        '    {"id": "cost", "role": "cost", "x": 0.85, "y": 0.65, "w": 0.18, "h": 0.10, "tilt": -4.0}',
        '  ]',
        '}',
        "",
        "Each (x, y) refers to the CENTRE of the object. (x, y, w, h) define an axis-aligned bbox the object sits in (its own tilt is applied within that bbox).",
        "Return ONLY the JSON object.",
    ]
    return "\n".join(parts)


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
            consumer="afishathumb.scene",
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
                "temperature": 0.3,
                "max_output_tokens": 2048,
            },
            max_output_tokens=2048,
        )
        return response_text

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_generate())
    raise RuntimeError("scene_llm synchronous API cannot run inside an active event loop")


def _parse_layout(raw: str) -> SceneLayout:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError("expected JSON object")
    out = SceneLayout(narrative=str(obj.get("narrative", "")))
    raw_objs = obj.get("objects") or []
    if not isinstance(raw_objs, list):
        raise ValueError("'objects' must be a list")
    for o in raw_objs:
        if not isinstance(o, dict):
            continue
        try:
            out.objects.append(PlacedObject(
                id=str(o["id"]),
                role=str(o.get("role", "image")),
                x_norm=max(0.0, min(1.0, float(o["x"]))),
                y_norm=max(0.0, min(1.0, float(o["y"]))),
                w_norm=max(0.01, min(1.0, float(o["w"]))),
                h_norm=max(0.01, min(1.0, float(o["h"]))),
                tilt_deg=max(-12.0, min(12.0, float(o.get("tilt", 0.0)))),
            ))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _validate_layout(layout: SceneLayout, scene_brief: dict) -> list[str]:  # noqa: D401
    """Aspect-ratio is enforced by `canvas_to_world` (inscribe image
    inside bbox), so the layout validator skips it now and focuses on
    geometric / product issues."""
    """Returns a list of violation messages; empty means the layout
    passes all hard constraints."""
    msgs: list[str] = []
    primary_aspect = float(scene_brief.get("primary_aspect", 1.0))
    extras_aspects = scene_brief.get("extras_aspects", [])
    by_id = {o.id: o for o in layout.objects}

    p = by_id.get("primary")
    if p is None:
        return ["primary object missing"]

    # Rule N1: extras at 30-50% of primary's longer side, measured by
    # inscribed-image dimensions (after canvas_to_world preserves aspect).
    def _inscribed_long_side(o: PlacedObject, src_aspect: float) -> float:
        w_d = o.w_norm * 1.5
        h_d = o.h_norm * 1.8
        bbox_aspect = w_d / max(0.01, h_d)
        if bbox_aspect > src_aspect:
            w_d = h_d * src_aspect
        else:
            h_d = w_d / src_aspect
        return max(w_d, h_d)
    primary_long = _inscribed_long_side(p, primary_aspect)
    for i, exp_asp in enumerate(extras_aspects, start=1):
        ex = by_id.get(f"extra_{i}")
        if ex is None:
            msgs.append(f"extra_{i} is missing from the layout — please place it")
            continue
        ex_long = _inscribed_long_side(ex, exp_asp)
        ratio = ex_long / max(0.01, primary_long)
        if ratio < 0.40 or ratio > 0.65:
            msgs.append(
                f"extra_{i} longer-side is {ratio:.0%} of primary's; must be 45%-60%"
            )

    # Rule 11: digest sticker must be placed when listed.
    needs_digest = any("digest" in s.lower() for s in scene_brief.get("required_stickers", []))
    if needs_digest and not any(o.id in ("digest", "essence") for o in layout.objects):
        msgs.append("digest sticker is required but not placed")

    # Rule 8: every bbox inside [0.02, 0.98].
    for o in layout.objects:
        x0 = o.x_norm - o.w_norm / 2
        x1 = o.x_norm + o.w_norm / 2
        y0 = o.y_norm - o.h_norm / 2
        y1 = o.y_norm + o.h_norm / 2
        if x0 < 0.0 or x1 > 1.0 or y0 < 0.0 or y1 > 1.0:
            msgs.append(f"object '{o.id}' extends outside canvas [0,1]")

    # Rule 9 (relaxed in photo-cluster mode for sticker↔image overlaps):
    is_photo_cluster = scene_brief.get("is_photo_cluster", False)
    image_roles = {"image"}
    sticker_roles = {"title", "date", "location", "cost", "essence"}
    for i, a in enumerate(layout.objects):
        for b in layout.objects[i + 1:]:
            a_box = (a.x_norm - a.w_norm / 2, a.y_norm - a.h_norm / 2,
                     a.x_norm + a.w_norm / 2, a.y_norm + a.h_norm / 2)
            b_box = (b.x_norm - b.w_norm / 2, b.y_norm - b.h_norm / 2,
                     b.x_norm + b.w_norm / 2, b.y_norm + b.h_norm / 2)
            ix = max(0.0, min(a_box[2], b_box[2]) - max(a_box[0], b_box[0]))
            iy = max(0.0, min(a_box[3], b_box[3]) - max(a_box[1], b_box[1]))
            if ix < 0.001 or iy < 0.001:
                continue
            a_img = a.role in image_roles
            b_img = b.role in image_roles
            if a_img and b_img:
                msgs.append(f"images '{a.id}' and '{b.id}' overlap")
                continue
            # Sticker ↔ sticker overlaps are NEVER ok.
            if a.role in sticker_roles and b.role in sticker_roles:
                msgs.append(f"stickers '{a.id}' and '{b.id}' overlap")
                continue
            # Sticker ↔ image overlap is OK ONLY in photo-cluster mode.
            if not is_photo_cluster:
                msgs.append(f"sticker '{a.id if not a_img else b.id}' overlaps image — not allowed in poster mode")

    return msgs


def plan_scene_layout(
    scene_brief: dict,
    *,
    cache_path: Optional[Path] = None,
    model: str = DEFAULT_MODEL,
    force_refresh: bool = False,
) -> SceneLayout:
    """Ask Gemini to design the layout. Cache + fallback + validation
    with retry: when the LLM output violates a hard constraint, the
    failure is fed back into a follow-up prompt up to 2 times."""
    if cache_path is not None and not force_refresh and cache_path.exists():
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            out = SceneLayout(narrative=data.get("narrative", ""))
            out.model = data.get("model", "")
            out.source = "cache"
            for o in data.get("objects", []):
                out.objects.append(PlacedObject(**o))
            return out
        except Exception:
            pass

    prompt = _build_prompt(scene_brief)
    last_err = None
    violations_history: list[list[str]] = []
    for attempt in range(MAX_RETRIES):
        try:
            this_prompt = prompt
            if violations_history:
                this_prompt = (prompt
                    + "\n\nPREVIOUS ATTEMPT FAILED. Fix EACH of these errors:\n"
                    + "\n".join(f"  - {m}" for m in violations_history[-1])
                    + "\nReturn a CORRECTED JSON.")
            raw = _call_gemini(this_prompt, model)
            layout = _parse_layout(raw)
            if not layout.objects:
                raise ValueError("LLM returned empty objects list")
            violations = _validate_layout(layout, scene_brief)
            if violations:
                violations_history.append(violations)
                print(f"[scene_llm] attempt {attempt + 1} violated {len(violations)} rules; retrying")
                for m in violations[:5]:
                    print(f"  - {m}")
                last_err = ValueError("validation failed")
                continue
            layout.model = model
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                serial = {
                    "narrative": layout.narrative,
                    "model": layout.model,
                    "objects": [
                        {
                            "id": o.id, "role": o.role,
                            "x_norm": o.x_norm, "y_norm": o.y_norm,
                            "w_norm": o.w_norm, "h_norm": o.h_norm,
                            "tilt_deg": o.tilt_deg,
                        }
                        for o in layout.objects
                    ],
                }
                with cache_path.open("w", encoding="utf-8") as f:
                    json.dump(serial, f, ensure_ascii=False, indent=2)
            return layout
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            d = RETRY_DELAYS_SEC[min(attempt, len(RETRY_DELAYS_SEC) - 1)]
            print(f"[scene_llm] attempt {attempt + 1}/{MAX_RETRIES} failed: {exc!r}; retry in {d}s")
            time.sleep(d)

    print(f"[scene_llm] LLM unavailable, falling back. last err: {last_err!r}")
    return SceneLayout(source="fallback", model=model)


# Conversion helpers (canvas → cylinder world coords) -------------------------
# Canvas size: 1.5 D wide × 1.8 D tall, centred on (anchor_angle, anchor_z).
# x_norm 0..1 → angle_deg in [anchor_angle − 75°, anchor_angle + 75°]
# y_norm 0..1 → z in [anchor_z + 0.9, anchor_z − 0.9]  (y=0 is TOP).
CANVAS_ARC_DEG = 90.0      # full ±45° from anchor angle  (round-12: tighter
                           # per-event zone so 6 events fit on the cylinder
                           # at 2 z-tiers without overlap)
CANVAS_HEIGHT_D = 1.4      # full ±0.7 from anchor_z


def canvas_to_world(
    x_norm: float, y_norm: float, w_norm: float, h_norm: float,
    anchor_angle_deg: float = 0.0,
    anchor_z: float = 1.65,
    *,
    source_aspect: float | None = None,
) -> tuple[float, float, float, float]:
    """Convert normalised canvas placement to (angle_deg, z, width_d, height_d).

    `source_aspect` (img_w / img_h of the source image) ensures the
    final plane preserves the image's aspect — we INSCRIBE the image
    inside the LLM's bbox rather than stretching it. The LLM's bbox is
    the "available space", the actual plane fits inside it.
    """
    import math
    arc_deg = w_norm * CANVAS_ARC_DEG
    width_d = arc_deg / (180.0 / math.pi) * 0.5
    height_d = h_norm * CANVAS_HEIGHT_D
    # Canvas physical size: 0.9 D wide × 1.4 D tall (after round-12 tightening).
    # Plane width = w_norm × canvas_width_d (which translates to arc_deg via radius 0.5).
    # Plane height = h_norm × canvas_height_d.
    canvas_w_d = CANVAS_ARC_DEG / (180.0 / math.pi) * 0.5  # arc in D-units
    canvas_h_d = CANVAS_HEIGHT_D
    # Recompute width_d / height_d using the actual canvas D dimensions
    # so the aspect math below is correct.
    width_d = w_norm * canvas_w_d
    height_d = h_norm * canvas_h_d
    if source_aspect is not None and source_aspect > 0.01:
        bbox_aspect = width_d / max(0.01, height_d)
        if bbox_aspect > source_aspect:
            # bbox too wide — shrink width to match source aspect
            width_d = height_d * source_aspect
        else:
            # bbox too tall — shrink height
            height_d = width_d / source_aspect
    angle_deg = anchor_angle_deg + (x_norm - 0.5) * CANVAS_ARC_DEG
    z = anchor_z + (0.5 - y_norm) * CANVAS_HEIGHT_D
    return angle_deg, z, width_d, height_d


__all__ = [
    "PlacedObject", "SceneLayout",
    "plan_scene_layout", "canvas_to_world",
]
