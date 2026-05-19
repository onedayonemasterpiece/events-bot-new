"""A/B test for LLM-A scene composer.

Reads the cached slot_<id>/manifest.json + poster.png + sticker_*.png
for an event that was already prepared by the deterministic placer,
calls `scene_llm.plan_scene_layout`, and writes a fresh manifest into
`slot_<id>_llm/` so we can render the LLM placement side-by-side with
the deterministic one. Bypasses the DB.

Usage:
    .venv/bin/python scripts/afishathumb/llm_ab_test.py 4131
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
from dataclasses import asdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "kaggle" / "AfishaThumb" / "scripts"))

from PIL import Image  # noqa: E402

from camera_llm import CameraBeat, CameraPlan, plan_camera_for_scene  # noqa: E402
from scene_llm import (  # noqa: E402
    PlacedObject, SceneLayout, canvas_to_world, plan_scene_layout,
)


def _tighten_cluster(layout: "SceneLayout", scene_brief: dict) -> "SceneLayout":
    """Post-process LLM-A: keep its decisions about sides but enforce
    canonical sizes and tight grouping near the primary."""
    by_id = {o.id: o for o in layout.objects}
    primary = by_id.get("primary")
    if primary is None:
        return layout

    extras_aspects = scene_brief.get("extras_aspects", [])
    n_extras = len(extras_aspects)

    # Determine "sticker side" — was LLM-A leaning left or right of primary?
    sticker_ids = [o.id for o in layout.objects
                   if o.id not in ("primary",)
                   and not o.id.startswith("extra_")
                   and o.id != "title_banner"]
    side_score = 0.0
    for sid in sticker_ids:
        o = by_id.get(sid)
        if o:
            side_score += (1.0 if o.x_norm > primary.x_norm else -1.0)
    sticker_side = "R" if side_score >= 0 else "L"

    # Primary keeps its (x, y) but is centered-ish.
    primary.x_norm = 0.50
    primary.y_norm = 0.50

    # Resolve primary's box in normalised canvas coords (preserve aspect).
    # Approx: primary's longer side ≈ p.w_norm or p.h_norm in canvas units.
    primary_long_norm = max(primary.w_norm, primary.h_norm)
    # Extras at 50% of primary longer side.
    extras_long = primary_long_norm * 0.50
    # Place extras in a horizontal strip BELOW primary.
    p_bottom = primary.y_norm + primary.h_norm / 2.0
    extras_y = min(0.96, p_bottom + 0.04 + extras_long / 2.0)
    # Wrap aspects: each extra's normalised w,h follows its source aspect.
    # We treat extras_long as the longer side and derive shorter side.
    # In canvas (0.9 D × 1.4 D), longer side may be either width or height.
    # Canvas physical sizes (kept in sync with scene_llm.CANVAS_*):
    import math
    canvas_w_d = 90.0 / (180.0 / math.pi) * 0.5  # ≈ 0.785
    canvas_h_d = 1.4
    # PRIMARY LONGER SIDE AFTER aspect-inscribe (the actual rendered
    # size of the primary plane). Earlier the tightener used raw layout
    # dims which caused extras to be 89% of primary instead of 50%
    # because canvas_to_world later shrinks the primary to match source
    # aspect.
    src_p_asp = scene_brief.get("primary_aspect", 1.0)
    primary_w_d_raw = primary.w_norm * canvas_w_d
    primary_h_d_raw = primary.h_norm * canvas_h_d
    bbox_asp = primary_w_d_raw / max(0.01, primary_h_d_raw)
    if bbox_asp > src_p_asp:
        primary_w_d = primary_h_d_raw * src_p_asp
        primary_h_d = primary_h_d_raw
    else:
        primary_w_d = primary_w_d_raw
        primary_h_d = primary_w_d_raw / max(0.01, src_p_asp)
    primary_long_d = max(primary_w_d, primary_h_d)
    target_long_d = primary_long_d * 0.50

    for i, asp in enumerate(extras_aspects, start=1):
        ex = by_id.get(f"extra_{i}")
        if ex is None:
            ex = PlacedObject(id=f"extra_{i}", role="image",
                              x_norm=0.5, y_norm=0.5,
                              w_norm=0.2, h_norm=0.2, tilt_deg=0.0)
            layout.objects.append(ex)
            by_id[ex.id] = ex
        if asp >= 1.0:  # landscape
            w_d = target_long_d
            h_d = w_d / asp
        else:
            h_d = target_long_d
            w_d = h_d * asp
        ex.w_norm = max(0.05, min(0.95, w_d / canvas_w_d))
        ex.h_norm = max(0.05, min(0.95, h_d / canvas_h_d))
    # Lay them in a horizontal strip below primary, centered.
    extras_objs = [by_id[f"extra_{i}"] for i in range(1, n_extras + 1) if f"extra_{i}" in by_id]
    if extras_objs:
        total_w = sum(o.w_norm for o in extras_objs) + 0.03 * (len(extras_objs) - 1)
        x_cursor = 0.5 - total_w / 2.0
        for ex in extras_objs:
            ex.x_norm = x_cursor + ex.w_norm / 2.0
            ex.y_norm = extras_y
            ex.tilt_deg = 0.0
            x_cursor += ex.w_norm + 0.03

    # Stickers: vertical column on the chosen side, hugging primary edge.
    side_x = (primary.x_norm + primary.w_norm / 2.0 + 0.03
              if sticker_side == "R"
              else primary.x_norm - primary.w_norm / 2.0 - 0.03)
    # Width of sticker column: cap at canvas margin on chosen side.
    margin_w = (0.98 - side_x) if sticker_side == "R" else (side_x - 0.02)
    margin_w = max(0.12, min(0.30, margin_w))
    stack_top = max(0.04, primary.y_norm - primary.h_norm / 2.0)
    stack_bottom = min(extras_y - extras_long / 2.0 - 0.02, 0.95)
    sticker_order = []
    for kind in ("date", "location", "cost", "essence", "digest"):
        if kind in by_id:
            sticker_order.append(by_id[kind])
    if sticker_order:
        avail = stack_bottom - stack_top
        per_h = min(0.16, avail / len(sticker_order) - 0.02)
        y = stack_top + per_h / 2.0
        for o in sticker_order:
            o.w_norm = margin_w
            o.h_norm = per_h
            o.x_norm = side_x + (margin_w / 2.0 if sticker_side == "R" else -margin_w / 2.0)
            o.y_norm = y
            o.tilt_deg = 0.0
            y += per_h + 0.02

    # Title banner ABOVE primary if present.
    tb = by_id.get("title_banner") or by_id.get("title")
    if tb is not None:
        tb.w_norm = min(0.94, primary.w_norm * 1.10)
        tb.h_norm = 0.09
        tb.x_norm = primary.x_norm
        tb.y_norm = max(0.04 + tb.h_norm / 2.0,
                        primary.y_norm - primary.h_norm / 2.0 - 0.02 - tb.h_norm / 2.0)
        tb.tilt_deg = 0.0

    return layout

sys.path.insert(0, str(REPO_ROOT / "scripts" / "afishathumb"))
from prepare_slot import BeatPlan, PaperPlan, SlotManifest  # noqa: E402


def run(event_id: int) -> None:
    src_dir = REPO_ROOT / "artifacts" / "afishathumb" / f"slot_{event_id}"
    dst_dir = REPO_ROOT / "artifacts" / "afishathumb" / f"slot_{event_id}_llm"
    if not src_dir.exists():
        raise SystemExit(f"no cached slot dir: {src_dir}")
    dst_dir.mkdir(parents=True, exist_ok=True)

    # Reuse the source's poster/extras/stickers/regions/text-mask
    # without re-running the deterministic placer.
    for name in [
        "poster.png", "poster_regions.json", "poster_text_mask.png",
        "sticker_title.png", "sticker_date.png", "sticker_location.png",
        "sticker_free.png", "sticker_price.png", "sticker_digest.png",
    ]:
        p = src_dir / name
        if p.exists():
            shutil.copyfile(p, dst_dir / name)
    # Extras (poster_extra_*.png)
    extra_paths: list[Path] = []
    for p in sorted(src_dir.glob("poster_extra_*.png")):
        dst_p = dst_dir / p.name
        shutil.copyfile(p, dst_p)
        extra_paths.append(dst_p)

    # Pull existing manifest for metadata.
    src_manifest = json.loads((src_dir / "manifest.json").read_text(encoding="utf-8"))
    regions_data = json.loads((dst_dir / "poster_regions.json").read_text(encoding="utf-8"))

    # Aspect of primary + extras.
    def _aspect(p: Path) -> float:
        with Image.open(p) as im:
            return im.size[0] / max(1, im.size[1])
    primary_aspect = _aspect(dst_dir / "poster.png")
    extras_aspects = [_aspect(p) for p in extra_paths]

    # Decide which sticker assets exist.
    title_path = dst_dir / "sticker_title.png" if (dst_dir / "sticker_title.png").exists() else None
    date_path = dst_dir / "sticker_date.png" if (dst_dir / "sticker_date.png").exists() else None
    loc_path = dst_dir / "sticker_location.png" if (dst_dir / "sticker_location.png").exists() else None
    free_path = dst_dir / "sticker_free.png" if (dst_dir / "sticker_free.png").exists() else None
    price_path = dst_dir / "sticker_price.png" if (dst_dir / "sticker_price.png").exists() else None
    digest_path = dst_dir / "sticker_digest.png" if (dst_dir / "sticker_digest.png").exists() else None
    cost_path = free_path or price_path

    required_stickers: list[str] = []
    if title_path:
        required_stickers.append("title_banner: wide ribbon with the full event name")
    if date_path:
        required_stickers.append("date: a small card with day, month and start time")
    if loc_path:
        required_stickers.append("location: a small card with venue + city")
    if cost_path:
        if free_path:
            required_stickers.append("cost: a yellow «БЕСПЛАТНО» tag")
        else:
            required_stickers.append("cost: a small card with the price")
    if digest_path:
        required_stickers.append("digest: a small card with a short description")

    # photo-cluster vs poster: derive from how many real info regions LLM
    # detected on the primary. ≤ 2 regions = photo cluster (P3 mode).
    llm_non_null = sum(
        1 for k in ("title", "date", "time", "location", "price")
        if regions_data.get(k)
    )
    is_photo_cluster = llm_non_null < 3

    scene_brief = {
        "title": src_manifest["title"],
        "date": src_manifest["date_iso"],
        "time": src_manifest["time_text"],
        "location": src_manifest["location_name"],
        "city": src_manifest["city"],
        "cost_text": ("БЕСПЛАТНО" if src_manifest.get("is_free")
                      else (src_manifest.get("price_text") or "уточняется")),
        "search_digest": src_manifest.get("search_digest", ""),
        "primary_aspect": primary_aspect,
        "extras_aspects": extras_aspects,
        "regions": {
            "title": regions_data.get("title"),
            "date": regions_data.get("date"),
            "time": regions_data.get("time"),
            "location": regions_data.get("location"),
            "price": regions_data.get("price"),
        },
        "required_stickers": required_stickers,
        "is_photo_cluster": is_photo_cluster,
    }

    layout = plan_scene_layout(
        scene_brief, cache_path=dst_dir / "scene_llm_layout.json",
        force_refresh=True,
    )
    if not layout.objects:
        # Deterministic fallback: synthesize a minimal layout so the
        # subsequent _tighten_cluster step still produces a valid scene
        # even when LLM-A failed all retries.
        print("[ab] LLM-A failed; synthesizing deterministic seed layout")
        layout = SceneLayout(narrative="deterministic fallback")
        layout.source = "fallback"
        layout.objects.append(PlacedObject(
            id="primary", role="image",
            x_norm=0.50, y_norm=0.50, w_norm=0.55, h_norm=0.55, tilt_deg=0.0,
        ))
        for i, _asp in enumerate(extras_aspects, start=1):
            layout.objects.append(PlacedObject(
                id=f"extra_{i}", role="image",
                x_norm=0.20 + 0.20 * (i - 1), y_norm=0.85,
                w_norm=0.18, h_norm=0.14, tilt_deg=0.0,
            ))
        ticker_y = 0.30
        for sid in ("title_banner", "date", "location", "cost", "digest"):
            if any(sid == r for r in ("title_banner",)) and "title_banner" in [
                s.split(":", 1)[0].strip() for s in required_stickers
            ]:
                layout.objects.append(PlacedObject(
                    id="title_banner", role="title",
                    x_norm=0.50, y_norm=0.10, w_norm=0.50, h_norm=0.10, tilt_deg=0.0,
                ))
            elif sid in ("date", "location", "cost", "digest"):
                if any(sid in s.lower() for s in required_stickers):
                    layout.objects.append(PlacedObject(
                        id=sid, role=sid if sid != "digest" else "essence",
                        x_norm=0.85, y_norm=ticker_y,
                        w_norm=0.18, h_norm=0.10, tilt_deg=0.0,
                    ))
                    ticker_y += 0.14
    print(f"[ab] narrative: {layout.narrative}")
    print(f"[ab] objects: {len(layout.objects)}")

    # Round-14 hybrid: LLM-A decides COMPOSITION + sides; deterministic
    # _tighten_cluster ENFORCES extras=50% of primary, stickers stacked
    # tightly against the chosen primary side, title banner above primary.
    layout = _tighten_cluster(layout, scene_brief)
    print(f"[ab] post-tighten objects: {len(layout.objects)}")

    asset_map: dict[str, tuple[Path, str, bool]] = {
        "primary": (dst_dir / "poster.png", "image", False),
    }
    for i, ep in enumerate(extra_paths, start=1):
        asset_map[f"extra_{i}"] = (ep, "image", False)
    if title_path:
        asset_map["title_banner"] = (title_path, "title", True)
        asset_map["title"] = (title_path, "title", True)
    if date_path:
        asset_map["date"] = (date_path, "date", True)
    if loc_path:
        asset_map["location"] = (loc_path, "location", True)
    if cost_path:
        asset_map["cost"] = (cost_path, "cost", True)
        asset_map["price"] = (cost_path, "cost", True)
        asset_map["free"] = (cost_path, "cost", True)
    if digest_path:
        asset_map["digest"] = (digest_path, "essence", True)
        asset_map["essence"] = (digest_path, "essence", True)

    # Aspect lookup for the source images so canvas_to_world can inscribe.
    aspect_lookup: dict[str, float] = {"primary": primary_aspect}
    for i, asp in enumerate(extras_aspects, start=1):
        aspect_lookup[f"extra_{i}"] = asp

    papers: list[dict] = []
    for idx, obj in enumerate(layout.objects):
        if obj.id not in asset_map:
            print(f"[ab] skipped unknown id from LLM: {obj.id}")
            continue
        img_path, role, is_sticker = asset_map[obj.id]
        # Aspect preservation for images; stickers are free-form so we
        # let the LLM-chosen bbox stand.
        src_asp = aspect_lookup.get(obj.id) if not is_sticker else None
        # For sticker cards we still match the PNG's aspect so text isn't
        # squashed; lookup from the file.
        if src_asp is None and is_sticker:
            try:
                with Image.open(img_path) as _img:
                    src_asp = _img.size[0] / max(1, _img.size[1])
            except Exception:
                src_asp = None
        angle_deg, z, w_d, h_d = canvas_to_world(
            obj.x_norm, obj.y_norm, obj.w_norm, obj.h_norm,
            source_aspect=src_asp,
        )
        if is_sticker:
            p_offset = 0.025 + 0.003 * idx
        else:
            p_offset = 0.004 + 0.0006 * idx
        if obj.id == "primary":
            name = f"Poster.{event_id}"
        elif obj.id.startswith("extra_"):
            name = f"Image.{event_id}.{obj.id.split('_', 1)[1]}"
        else:
            name = f"{obj.id.capitalize()}.{event_id}"
        papers.append({
            "image": str(img_path),
            "anchor_angle_deg": angle_deg,
            "anchor_z": z,
            "width": max(0.05, w_d),
            "height": max(0.05, h_d),
            "tilt_deg": obj.tilt_deg,
            "peel_corners": [False, False, False, False],
            "peel_intensity": 0.0 if is_sticker else 0.25,
            "wrinkle": 0.04 if is_sticker else 0.10,
            "name": name,
            "paper_offset": p_offset,
        })

    # LLM-B: plan the camera flight given the LLM-A layout.
    camera_plan = plan_camera_for_scene(
        scene_brief, papers,
        cache_path=dst_dir / "scene_llm_camera.json",
        force_refresh=True,
    )
    if camera_plan.beats:
        print(f"[ab] LLM-B narrative: {camera_plan.narrative}")
        print(f"[ab] LLM-B beats ({len(camera_plan.beats)}): "
              + " → ".join(f"{b.role}({b.dwell_s:.1f}s, r{b.radius:.1f})" for b in camera_plan.beats))
    else:
        print("[ab] LLM-B failed; trace will use deterministic beats")

    beats_payload: list[dict] = []
    for b in camera_plan.beats:
        beats_payload.append({
            "role": b.role,
            "target_anchor": b.target_anchor,
            "label": b.label,
            "dwell_s": b.dwell_s,
            "lens_mm": b.lens_mm,
            "radius_factor": b.radius / 2.2,  # convert absolute → factor for legacy callers
            "color": "#9F2933" if b.is_focus_push else "#1B2D5E",
            "is_focus_push": b.is_focus_push,
            "angle_deg": b.angle_deg,
            "z": b.z,
            "radius": b.radius,
            "tilt_deg": b.tilt_deg,
            "transit": b.transit,
        })

    coverage = {
        "image": any(b["role"] == "image" for b in beats_payload),
        "title": any(b["role"] in ("title",) for b in beats_payload),
        "essence": any(b["role"] in ("essence", "image") for b in beats_payload),
        "when": any(b["role"] == "when" for b in beats_payload),
        "where": any(b["role"] == "where" for b in beats_payload),
        "cost": any(b["role"] == "cost" for b in beats_payload),
    }

    # Mimic SlotManifest serialisation (the render_slot_blender consumer
    # reads a flat dict).
    manifest = {
        "event_id": event_id,
        "title": src_manifest["title"],
        "date_iso": src_manifest["date_iso"],
        "time_text": src_manifest["time_text"],
        "location_name": src_manifest["location_name"],
        "address": src_manifest["address"],
        "city": src_manifest["city"],
        "is_free": src_manifest.get("is_free", False),
        "price_text": src_manifest.get("price_text", ""),
        "search_digest": src_manifest.get("search_digest", ""),
        "is_promo": False,
        "papers": papers,
        "camera_focus_angle_deg": 0.0,
        "camera_focus_z": 1.65,
        "camera_radius": 2.2,
        "camera_lens_mm": 70.0,
        "camera_target_offset_z": 0.0,
        "render_w": 1080,
        "render_h": 1572,
        "beats": beats_payload,
        "coverage_tests": coverage,
        "camera_narrative": camera_plan.narrative,
        "poster_text_density": src_manifest.get("poster_text_density", 0.0),
        "poster_regions": src_manifest.get("poster_regions", {}),
        "tightness_budget_deg": 0.0,
        "attention_anchor_count": 2,
    }
    (dst_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[ab] wrote LLM-A manifest -> {dst_dir / 'manifest.json'}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("event_id", type=int)
    args = ap.parse_args()
    run(args.event_id)


if __name__ == "__main__":
    main()
