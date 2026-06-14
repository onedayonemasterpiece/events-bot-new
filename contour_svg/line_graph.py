from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path

from .config import RunConfig
from .contracts import GuideSet, LineCandidate, LineGroup, LinePrimitive, MaskBundle
from .dependencies import require_module


def build_line_graph(guides: GuideSet, masks: MaskBundle, out_dir: str | Path, config: RunConfig) -> tuple[list[LineCandidate], list[LineGroup], Path]:
    candidates = _line_candidates_from_guides(guides, masks, config)
    groups = _group_line_candidates(candidates, config)
    overlay_path = _write_line_graph_artifacts(candidates, groups, masks, out_dir)
    return candidates, groups, overlay_path


def apply_line_group_actions(groups: list[LineGroup], actions: dict[str, dict[str, object]]) -> list[LineGroup]:
    by_id = {group.id: group for group in groups}
    for group_id, action in actions.items():
        group = by_id.get(group_id)
        if group is None:
            continue
        decision = str(action.get("action") or "").strip()
        if decision in {"keep", "simplify", "extend_across_occluder", "lower_priority"}:
            group.decision = "keep"
        elif decision == "drop":
            group.decision = "drop"
        elif decision == "merge_with":
            target = str(action.get("target_group_id") or "").strip()
            group.decision = "merge_with" if target in by_id and target != group_id else "keep"
            group.duplicate_group = target or None
        group.reason = str(action.get("reason") or group.reason)
        try:
            group.confidence = max(group.confidence, float(action.get("confidence") or 0.0))
        except (TypeError, ValueError):
            pass
    return groups


def _line_candidates_from_guides(guides: GuideSet, masks: MaskBundle, config: RunConfig) -> list[LineCandidate]:
    np = require_module("numpy", "numpy")
    Image = require_module("PIL.Image", "Pillow")
    object_visible = np.array(Image.open(masks.object_visible).convert("L")) > 32
    occluder = np.array(Image.open(masks.occluder).convert("L")) > 32 if masks.occluder else np.zeros_like(object_visible)
    background = np.array(Image.open(masks.background).convert("L")) > 32
    object_unknown = np.array(Image.open(masks.object_unknown).convert("L")) > 32

    out: list[LineCandidate] = []
    for idx, primitive in enumerate(guides.lines):
        if primitive.length < config.style.min_line_length:
            continue
        points = _sample_points(primitive)
        if not points:
            continue
        ratios = {
            "object_visible": _mask_ratio(points, object_visible),
            "occluder": _mask_ratio(points, occluder),
            "background": _mask_ratio(points, background),
            "object_unknown": _mask_ratio(points, object_unknown),
        }
        angle = _angle_deg(primitive)
        semantic = _semantic_guess(primitive, angle)
        vp_group = _vp_group(angle, semantic)
        length_score = min(1.0, primitive.length / 260.0)
        structure_bonus = 0.18 if semantic in {"silhouette", "roof_edge", "facade_edge", "cornice_band"} else 0.0
        raw_score = (
            0.48 * ratios["object_visible"]
            + 0.24 * length_score
            + structure_bonus
            + 0.10 * ratios["object_unknown"]
            - 0.52 * ratios["occluder"]
            - 0.30 * ratios["background"]
        )
        keep_probability = max(0.0, min(1.0, raw_score))
        out.append(
            LineCandidate(
                id=f"L_{idx:04d}",
                geometry=primitive,
                angle_deg=round(angle, 3),
                source=[primitive.source],
                vp_group=vp_group,
                object_visible_overlap=round(ratios["object_visible"], 4),
                occluder_overlap=round(ratios["occluder"], 4),
                background_overlap=round(ratios["background"], 4),
                object_unknown_overlap=round(ratios["object_unknown"], 4),
                junction_support=0.0,
                semantic_guess=semantic,
                raw_score=round(raw_score, 4),
                keep_probability=round(keep_probability, 4),
            )
        )
    return out


def _group_line_candidates(candidates: list[LineCandidate], config: RunConfig) -> list[LineGroup]:
    buckets: dict[tuple[str, str, int, int], list[LineCandidate]] = defaultdict(list)
    for candidate in candidates:
        geom = candidate.geometry
        if candidate.keep_probability < 0.12 and geom.role != "silhouette":
            continue
        rho = _rho_bucket(geom, candidate.angle_deg)
        angle_bucket = int(round(candidate.angle_deg / max(1.0, config.geometry.merge_angle_deg)))
        if candidate.semantic_guess == "silhouette":
            key = ("silhouette", candidate.id, 0, 0)
        else:
            key = (candidate.semantic_guess, candidate.vp_group, angle_bucket, rho)
        buckets[key].append(candidate)

    groups: list[LineGroup] = []
    for idx, bucket in enumerate(buckets.values()):
        bucket.sort(key=lambda c: (c.keep_probability, c.geometry.length), reverse=True)
        best = bucket[0]
        confidence = sum(c.keep_probability for c in bucket[:4]) / max(1, min(len(bucket), 4))
        importance = _importance(best.semantic_guess, confidence)
        decision = "keep" if importance == "primary" and confidence >= 0.25 else "candidate"
        groups.append(
            LineGroup(
                id=f"G_{best.semantic_guess}_{idx:03d}",
                members=[c.id for c in bucket],
                merged_geometry=best.geometry,
                semantic_label=best.semantic_guess,
                importance=importance,
                decision=decision,
                reason=f"{best.semantic_guess} from {','.join(best.source)}",
                confidence=round(confidence, 4),
                object_visible_overlap=round(sum(c.object_visible_overlap for c in bucket) / len(bucket), 4),
                occluder_overlap=round(sum(c.occluder_overlap for c in bucket) / len(bucket), 4),
                background_overlap=round(sum(c.background_overlap for c in bucket) / len(bucket), 4),
            )
        )
    groups.sort(
        key=lambda g: (
            {"primary": 0, "secondary": 1, "optional": 2}.get(g.importance, 3),
            -g.confidence,
            -g.merged_geometry.length,
            g.id,
        )
    )
    return groups


def _write_line_graph_artifacts(candidates: list[LineCandidate], groups: list[LineGroup], masks: MaskBundle, out_dir: str | Path) -> Path:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")

    debug = Path(out_dir) / "debug"
    debug.mkdir(parents=True, exist_ok=True)
    (debug / "line_candidates.jsonl").write_text(
        "\n".join(json.dumps(candidate.to_dict(), ensure_ascii=False) for candidate in candidates) + "\n",
        encoding="utf-8",
    )
    (debug / "line_groups.json").write_text(
        json.dumps([group.to_dict() for group in groups], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    overlay = np.array(Image.open(masks.overlay).convert("RGB"))
    palette = {
        "primary": (255, 255, 255),
        "secondary": (80, 220, 255),
        "optional": (170, 170, 170),
    }
    for idx, group in enumerate(groups[:120]):
        pts = [(int(x), int(y)) for x, y in group.merged_geometry.points]
        color = palette.get(group.importance, (160, 160, 160))
        for a, b in zip(pts, pts[1:]):
            cv2.line(overlay, a, b, color, 2, cv2.LINE_AA)
        if pts and idx < 45:
            cv2.putText(overlay, group.id, pts[0], cv2.FONT_HERSHEY_SIMPLEX, 0.38, (255, 230, 80), 1, cv2.LINE_AA)
    overlay_path = debug / "line_groups_overlay.png"
    Image.fromarray(overlay).save(overlay_path)
    return overlay_path


def _sample_points(line: LinePrimitive) -> list[tuple[int, int]]:
    points: list[tuple[int, int]] = []
    for a, b in zip(line.points, line.points[1:]):
        ax, ay = a
        bx, by = b
        steps = max(2, int((((bx - ax) ** 2 + (by - ay) ** 2) ** 0.5) / 6.0))
        for idx in range(steps + 1):
            t = idx / steps
            points.append((int(round(ax + (bx - ax) * t)), int(round(ay + (by - ay) * t))))
    return points


def _mask_ratio(points: list[tuple[int, int]], mask) -> float:
    if not points:
        return 0.0
    h, w = mask.shape[:2]
    inside = 0
    for x, y in points:
        if 0 <= x < w and 0 <= y < h and bool(mask[y, x]):
            inside += 1
    return inside / len(points)


def _angle_deg(line: LinePrimitive) -> float:
    if len(line.points) < 2:
        return 0.0
    x1, y1 = line.points[0]
    x2, y2 = line.points[-1]
    return math.degrees(math.atan2(y2 - y1, x2 - x1)) % 180.0


def _semantic_guess(line: LinePrimitive, angle: float) -> str:
    if line.role == "silhouette":
        return "silhouette"
    if line.role == "arc":
        return "arch_or_curve"
    horizontal = angle <= 10.0 or angle >= 170.0
    vertical = 80.0 <= angle <= 100.0
    shallow = 10.0 < angle < 38.0 or 142.0 < angle < 170.0
    y_mid = sum(y for _, y in line.points) / max(1, len(line.points))
    if shallow and y_mid < 420:
        return "roof_edge"
    if horizontal and y_mid < 520:
        return "cornice_band"
    if vertical:
        return "facade_edge"
    if horizontal:
        return "base_or_step"
    return "architectural_accent"


def _vp_group(angle: float, semantic: str) -> str:
    if semantic == "silhouette":
        return "object_hull"
    if angle <= 10.0 or angle >= 170.0:
        return "horizontal"
    if 80.0 <= angle <= 100.0:
        return "vertical"
    if angle < 90.0:
        return "left_vp"
    return "right_vp"


def _rho_bucket(line: LinePrimitive, angle: float) -> int:
    theta = math.radians(angle)
    nx = -math.sin(theta)
    ny = math.cos(theta)
    rho = sum((x * nx + y * ny) for x, y in line.points) / max(1, len(line.points))
    return int(round(rho / 12.0))


def _importance(semantic: str, confidence: float) -> str:
    if semantic in {"silhouette", "roof_edge", "facade_edge"}:
        return "primary"
    if semantic in {"cornice_band", "base_or_step", "arch_or_curve"} and confidence >= 0.18:
        return "secondary"
    return "optional"
