from __future__ import annotations

import json
import math
from pathlib import Path

from .config import RunConfig
from .contracts import (
    ArchPrimitive,
    Candidate,
    CandidateSVG,
    CompletionProposal,
    FacadeElement,
    LineGroup,
    LinePrimitive,
    MaskBundle,
    SemanticPlan,
)


FAMILY_BUDGETS = {
    "STRICT_VISIBLE": 86,
    "CONSERVATIVE_COMPLETION": 102,
    "POSTCARD_MINIMAL": 76,
    "BALANCED_ARCHITECTURAL": 118,
    "DETAILED_EDITORIAL": 170,
    "FEATURE_EMPHASIS_OPENINGS": 106,
}


def primitive_candidates_from_groups(
    groups: list[LineGroup],
    *,
    masks: MaskBundle,
    semantic_plan: SemanticPlan,
    out_dir: str | Path,
    config: RunConfig,
    facade_elements: list[FacadeElement] | None = None,
    completion_proposals: list[CompletionProposal] | None = None,
) -> list[Candidate]:
    facade_elements = facade_elements or []
    completion_proposals = completion_proposals or []
    primitives = [
        *_primitives_from_groups(groups, semantic_plan),
        *_primitives_from_elements(facade_elements),
        *_primitives_from_completion(completion_proposals),
    ]
    primitives = _order_primitives(primitives)
    _write_primitive_artifacts(primitives, out_dir, masks)
    if not any(primitive.render_role == "silhouette" for primitive in primitives):
        raise RuntimeError("Primitive renderer requires a global silhouette before final SVG ranking")

    candidates: list[Candidate] = []
    for family, budget in FAMILY_BUDGETS.items():
        limit = min(budget, config.geometry.max_paths_per_candidate, config.style.max_paths)
        selected = _select_primitives(_family_primitives(primitives, family), limit)
        if not selected:
            continue
        candidate_id = family.lower()
        lines = [
            LinePrimitive(
                primitive.geometry.points,
                role=primitive.render_role,
                priority=_line_priority(primitive),
                source=f"primitive:{primitive.primitive_type}",
                confidence=primitive.confidence,
            )
            for primitive in selected
        ]
        candidate_svg = CandidateSVG(
            candidate_id=candidate_id,
            family=family,
            primitive_ids=[primitive.id for primitive in selected],
            final_eligible=True,
        )
        candidate = Candidate(
            candidate_id=candidate_id,
            variant="B2",
            family=family,
            lines=lines,
            final_eligible=True,
            primitive_rendered=True,
            proposal_only=False,
            candidate_svg=candidate_svg,
        )
        candidate.parameters.update(
            {
                "primitive_renderer": True,
                "line_budget": limit,
                "primitive_count": len(selected),
                "primitive_ids": candidate_svg.primitive_ids,
                "mask_bundle": masks.to_dict(),
                "semantic_primary_object": semantic_plan.primary_object,
                "facade_element_count": len(facade_elements),
                "completion_proposal_count": len(completion_proposals),
                "family": family,
            }
        )
        if family == "POSTCARD_MINIMAL":
            candidate.parameters["stroke_width_scale"] = 0.68
        elif family in {"BALANCED_ARCHITECTURAL", "CONSERVATIVE_COMPLETION", "FEATURE_EMPHASIS_OPENINGS"}:
            candidate.parameters["stroke_width_scale"] = 0.56
        else:
            candidate.parameters["stroke_width_scale"] = 0.48
        candidates.append(candidate)
    return candidates


def _primitives_from_groups(groups: list[LineGroup], semantic_plan: SemanticPlan) -> list[ArchPrimitive]:
    useful = [group for group in groups if group.decision in {"keep", "candidate", "merge_with"}]
    useful.sort(
        key=lambda g: (
            {"primary": 0, "secondary": 1, "optional": 2}.get(g.importance, 3),
            -g.confidence,
            -g.merged_geometry.length,
        )
    )
    primitives: list[ArchPrimitive] = []
    for group in useful:
        primitive_type, render_role = _primitive_type(group)
        geometry = _regularized_geometry(group, primitive_type, render_role)
        if geometry is None:
            continue
        confidence = max(0.0, min(1.0, group.confidence))
        evidence = ["line_graph", f"semantic_label:{group.semantic_label}"]
        if semantic_plan.source:
            evidence.append(f"semantic_scene:{semantic_plan.source}")
        primitives.append(
            ArchPrimitive(
                id=f"P_{primitive_type}_{len(primitives):03d}",
                primitive_type=primitive_type,
                geometry=geometry,
                importance=group.importance,
                source_group_ids=[group.id],
                confidence=confidence,
                render_role=render_role,
                evidence=evidence,
            )
        )
    return primitives


def _primitives_from_elements(elements: list[FacadeElement]) -> list[ArchPrimitive]:
    primitives: list[ArchPrimitive] = []
    for element in elements:
        for line_idx, (primitive_type, role, points) in enumerate(_element_line_specs(element)):
            primitives.append(
                ArchPrimitive(
                    id=f"P_{primitive_type}_{element.id}_{line_idx:02d}",
                    primitive_type=primitive_type,
                    geometry=LinePrimitive(points, role=role, priority=_element_priority(element), source=f"facade_element:{element.source}", confidence=element.confidence),
                    importance=_element_importance(element),
                    source_group_ids=[],
                    confidence=element.confidence,
                    render_role=role,
                    evidence=[*element.evidence, f"element_id:{element.id}"],
                    completion_status=element.completion_status,
                )
            )
    return primitives


def _primitives_from_completion(completions: list[CompletionProposal]) -> list[ArchPrimitive]:
    primitives: list[ArchPrimitive] = []
    for proposal in completions:
        if not proposal.accepted:
            continue
        primitives.append(
            ArchPrimitive(
                id=f"P_completion_{proposal.id}",
                primitive_type=proposal.completion_type,
                geometry=proposal.geometry,
                importance="secondary",
                source_group_ids=proposal.source_group_ids,
                confidence=proposal.confidence,
                render_role=proposal.geometry.role if proposal.geometry.role in {"roofline", "structure", "arc", "accent"} else "structure",
                evidence=proposal.evidence,
                completion_status="interpolated",
            )
        )
    return primitives


def _order_primitives(primitives: list[ArchPrimitive]) -> list[ArchPrimitive]:
    role_order = {
        "silhouette": 0,
        "roofline": 1,
        "structure": 2,
        "arc": 3,
        "accent": 4,
    }
    type_boost = {
        "facade_corner": 0,
        "outer_silhouette": 0,
        "roof_edge": 1,
        "cornice": 2,
        "window": 3,
        "arched_window": 3,
        "door": 3,
        "balcony": 4,
    }
    return sorted(
        primitives,
        key=lambda p: (
            role_order.get(p.render_role, 9),
            {"primary": 0, "secondary": 1, "optional": 2}.get(p.importance, 3),
            type_boost.get(p.primitive_type, 5),
            p.completion_status == "interpolated",
            -p.confidence,
            -p.geometry.length,
            p.id,
        ),
    )


def _family_primitives(primitives: list[ArchPrimitive], family: str) -> list[ArchPrimitive]:
    design_primitives = [p for p in primitives if _is_reference_style_primitive(p)]
    if family == "STRICT_VISIBLE":
        return [
            p
            for p in design_primitives
            if p.completion_status != "interpolated" and p.render_role != "accent"
        ]
    if family == "CONSERVATIVE_COMPLETION":
        return [
            p
            for p in design_primitives
            if p.render_role != "accent" or p.confidence >= 0.65
        ]
    if family == "POSTCARD_MINIMAL":
        return [
            p
            for p in design_primitives
            if p.render_role in {"silhouette", "roofline", "structure", "arc"}
            and p.primitive_type
            in {
                "outer_silhouette",
                "roof_edge",
                "facade_edge",
                "cornice",
                "window",
                "arched_window",
                "door",
                "balcony",
                "pilaster",
            }
            and p.completion_status != "interpolated"
        ]
    if family == "FEATURE_EMPHASIS_OPENINGS":
        openings = [
            p
            for p in design_primitives
            if p.primitive_type in {"window", "arched_window", "door", "balcony"}
        ]
        rest = [
            p
            for p in design_primitives
            if p.primitive_type not in {"window", "arched_window", "door", "balcony"}
        ]
        return [*openings, *rest]
    if family == "DETAILED_EDITORIAL":
        return [
            p
            for p in primitives
            if _is_reference_style_primitive(p)
            or (p.primitive_type == "selected_detail" and p.confidence >= 0.68 and p.geometry.length <= 135.0)
        ]
    return design_primitives


def _select_primitives(primitives: list[ArchPrimitive], limit: int) -> list[ArchPrimitive]:
    selected: list[ArchPrimitive] = []
    role_caps = {
        "silhouette": 2,
        "roofline": min(max(6, limit // 11), 12),
        "structure": min(max(34, limit // 2), 72),
        "arc": min(max(7, limit // 9), 16),
        "accent": min(max(5, limit // 8), 14),
    }
    counts: dict[str, int] = {}
    buckets: set[tuple[str, int, int, int]] = set()
    for primitive in primitives:
        role = primitive.render_role
        if _is_cross_face_noise(primitive):
            continue
        count = counts.get(role, 0)
        if count >= role_caps.get(role, limit):
            continue
        bucket = _selection_bucket(primitive)
        if primitive.completion_status != "interpolated" and bucket in buckets and role in {"roofline", "structure"}:
            continue
        selected.append(primitive)
        buckets.add(bucket)
        counts[role] = count + 1
        if len(selected) >= limit:
            break
    return selected


def _element_line_specs(element: FacadeElement) -> list[tuple[str, str, list[tuple[float, float]]]]:
    x1, y1, x2, y2 = element.bbox_xyxy
    w = max(1.0, x2 - x1)
    h = max(1.0, y2 - y1)
    rect = [(x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)]
    if element.element_type == "window":
        if h > w * 1.35:
            arch = _arched_rect(x1, y1, x2, y2)
            return [("arched_window", "arc", arch), ("window", "structure", rect)]
        return [("window", "structure", rect)]
    if element.element_type == "door":
        return [("door", "structure", rect)]
    if element.element_type == "balcony":
        rail_y = y1 + h * 0.35
        return [
            ("balcony", "structure", [(x1, y1), (x2, y1), (x2, y2), (x1, y2)]),
            ("balcony", "accent", [(x1, rail_y), (x2, rail_y)]),
        ]
    if element.element_type == "pilaster":
        return [("pilaster", "structure", [(x1, y1), (x1, y2)]), ("pilaster", "structure", [(x2, y1), (x2, y2)])]
    if element.element_type in {"cornice", "molding", "sill"}:
        y = (y1 + y2) / 2
        return [("cornice", "structure", [(x1, y), (x2, y)])]
    if element.element_type == "wall_plane":
        return [("wall_plane", "structure", rect)]
    return [(element.element_type, "accent", rect)]


def _arched_rect(x1: float, y1: float, x2: float, y2: float) -> list[tuple[float, float]]:
    cx = (x1 + x2) / 2
    rx = max(1.0, (x2 - x1) / 2)
    arch_h = min((y2 - y1) * 0.42, rx * 1.15)
    base_y = y1 + arch_h
    points: list[tuple[float, float]] = [(x1, y2), (x1, base_y)]
    for idx in range(1, 8):
        t = math.pi * (1.0 - idx / 8.0)
        points.append((cx + math.cos(t) * rx, base_y - math.sin(t) * arch_h))
    points.extend([(x2, y2), (x1, y2)])
    return points


def _element_priority(element: FacadeElement) -> int:
    if element.element_type in {"door", "window", "balcony", "pilaster", "cornice"}:
        return 6
    if element.element_type in {"molding", "sill"}:
        return 5
    return 4


def _element_importance(element: FacadeElement) -> str:
    if element.element_type in {"door", "window", "balcony", "pilaster", "cornice"} and element.confidence >= 0.55:
        return "secondary"
    return "optional"


def _primitive_type(group: LineGroup) -> tuple[str, str]:
    label = group.semantic_label
    if label == "silhouette":
        return "outer_silhouette", "silhouette"
    if label == "roof_edge":
        return "roof_edge", "roofline"
    if label in {"facade_edge", "cornice_band", "base_or_step"}:
        return label, "structure"
    if label == "arch_or_curve":
        return "arch_or_ellipse", "arc"
    return "selected_detail", "accent"


def _line_priority(primitive: ArchPrimitive) -> int:
    if primitive.importance == "primary":
        return 9
    if primitive.importance == "secondary":
        return 6
    return 4


def _regularized_geometry(group: LineGroup, primitive_type: str, render_role: str) -> LinePrimitive | None:
    points = [(float(x), float(y)) for x, y in group.merged_geometry.points]
    if len(points) < 2:
        return None
    direct = _distance(points[0], points[-1])
    path = _path_length(points)
    angle = _angle(points[0], points[-1])
    y_mid = sum(y for _, y in points) / len(points)
    if render_role != "silhouette":
        min_direct = {
            "roofline": 72.0,
            "structure": 24.0,
            "arc": 0.0,
            "accent": 18.0,
        }.get(render_role, 32.0)
        if direct < min_direct:
            return None
        if render_role == "arc":
            if path < 34.0:
                return None
        else:
            min_efficiency = 0.30 if render_role == "accent" else 0.45
            if path > 0 and direct / path < min_efficiency:
                return None
        if group.occluder_overlap > 0.08 or group.background_overlap > 0.22:
            return None
        if primitive_type == "roof_edge" and not (_is_shallow_roof_angle(angle) and y_mid < 450):
            return None
        if primitive_type in {"cornice_band", "base_or_step"} and not _is_horizontal(angle):
            return None
        if primitive_type == "facade_edge" and not _is_vertical(angle):
            return None
        if primitive_type == "selected_detail" and (direct > 145.0 or not (_is_horizontal(angle) or _is_vertical(angle))):
            return None
    tolerance = {
        "silhouette": 18.0,
        "roofline": 3.5,
        "structure": 3.0,
        "arc": 2.0,
        "accent": 2.5,
    }.get(render_role, 3.0)
    simplified = _rdp(points, tolerance)
    if render_role in {"roofline", "structure", "accent"} and len(simplified) > 2:
        simplified = [simplified[0], simplified[-1]]
    if _path_length(simplified) < 16.0:
        return None
    return LinePrimitive(
        points=[(float(x), float(y)) for x, y in simplified],
        role=group.merged_geometry.role,
        priority=group.merged_geometry.priority,
        source=group.merged_geometry.source,
        confidence=group.merged_geometry.confidence,
    )


def _is_reference_style_primitive(primitive: ArchPrimitive) -> bool:
    if _is_cross_face_noise(primitive):
        return False
    if primitive.primitive_type in {
        "outer_silhouette",
        "roof_edge",
        "facade_edge",
        "cornice_band",
        "base_or_step",
        "cornice",
        "window",
        "arched_window",
        "door",
        "balcony",
        "pilaster",
    }:
        return True
    if primitive.primitive_type.endswith("_gap"):
        return primitive.confidence >= 0.40 and primitive.geometry.length <= 92.0
    if primitive.primitive_type == "repeated_window_placeholder":
        return primitive.confidence >= 0.35
    return False


def _is_cross_face_noise(primitive: ArchPrimitive) -> bool:
    points = primitive.geometry.points
    if len(points) < 2:
        return True
    angle = _angle(points[0], points[-1])
    length = primitive.geometry.length
    if primitive.primitive_type == "roof_edge":
        return length > 240.0 and not _is_shallow_roof_angle(angle)
    if primitive.render_role == "structure" and primitive.primitive_type not in {"window", "door", "arched_window", "balcony"}:
        return length > 155.0 and not (_is_horizontal(angle) or _is_vertical(angle))
    if primitive.completion_status == "interpolated":
        return length > 98.0 or not (_is_horizontal(angle) or _is_vertical(angle) or _is_shallow_roof_angle(angle))
    return False


def _angle(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.degrees(math.atan2(b[1] - a[1], b[0] - a[0])) % 180.0


def _is_horizontal(angle: float) -> bool:
    return angle <= 11.0 or angle >= 169.0


def _is_vertical(angle: float) -> bool:
    return 78.0 <= angle <= 102.0


def _is_shallow_roof_angle(angle: float) -> bool:
    return _is_horizontal(angle) or 13.0 <= angle <= 42.0 or 138.0 <= angle <= 167.0


def _selection_bucket(primitive: ArchPrimitive) -> tuple[str, int, int, int]:
    points = primitive.geometry.points
    if len(points) < 2:
        return (primitive.render_role, 0, 0, 0)
    x1, y1 = points[0]
    x2, y2 = points[-1]
    angle = math.degrees(math.atan2(y2 - y1, x2 - x1)) % 180.0
    theta = math.radians(angle)
    nx = -math.sin(theta)
    ny = math.cos(theta)
    mx = sum(x for x, _ in points) / len(points)
    my = sum(y for _, y in points) / len(points)
    rho = mx * nx + my * ny
    return (
        primitive.render_role,
        int(round(angle / 9.0)),
        int(round(rho / 28.0)),
        int(round(my / 72.0)),
    )


def _path_length(points: list[tuple[float, float]]) -> float:
    return sum(_distance(a, b) for a, b in zip(points, points[1:]))


def _distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(b[0] - a[0], b[1] - a[1])


def _rdp(points: list[tuple[float, float]], epsilon: float) -> list[tuple[float, float]]:
    if len(points) <= 2:
        return points
    start, end = points[0], points[-1]
    max_dist = -1.0
    split_idx = 0
    for idx, point in enumerate(points[1:-1], start=1):
        dist = _point_line_distance(point, start, end)
        if dist > max_dist:
            max_dist = dist
            split_idx = idx
    if max_dist > epsilon:
        left = _rdp(points[: split_idx + 1], epsilon)
        right = _rdp(points[split_idx:], epsilon)
        return left[:-1] + right
    return [start, end]


def _point_line_distance(
    point: tuple[float, float],
    start: tuple[float, float],
    end: tuple[float, float],
) -> float:
    sx, sy = start
    ex, ey = end
    px, py = point
    dx = ex - sx
    dy = ey - sy
    denom = math.hypot(dx, dy)
    if denom <= 1e-6:
        return math.hypot(px - sx, py - sy)
    return abs(dy * px - dx * py + ex * sy - ey * sx) / denom


def _write_primitive_artifacts(primitives: list[ArchPrimitive], out_dir: str | Path, masks: MaskBundle) -> None:
    debug = Path(out_dir) / "debug"
    debug.mkdir(parents=True, exist_ok=True)
    payload = {
        "mask_bundle": masks.to_dict(),
        "primitives": [primitive.to_dict() for primitive in primitives],
    }
    (debug / "arch_primitives.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
