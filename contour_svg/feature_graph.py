from __future__ import annotations

import json
import math
from pathlib import Path

from .config import RunConfig
from .contracts import (
    BuildingShell,
    Candidate,
    CandidateSVG,
    FacadeElement,
    FeatureGraph,
    FeatureNode,
    FeatureRow,
    LinePrimitive,
    MaskBundle,
    PlaneGraph,
)
from .dependencies import require_module
from .plane_graph import plane_graph_to_candidate


FEATURE_TYPES = {"window", "door", "balcony", "pilaster", "cornice", "molding", "sill"}


def build_feature_graph(
    *,
    image,
    masks: MaskBundle,
    facade_elements: list[FacadeElement],
    shell: BuildingShell,
    plane_graph: PlaneGraph,
    out_dir: str | Path,
    config: RunConfig,
) -> FeatureGraph:
    np = require_module("numpy", "numpy")
    Image = require_module("PIL.Image", "Pillow")

    debug = Path(out_dir) / "debug"
    debug.mkdir(parents=True, exist_ok=True)

    visible = np.array(Image.open(masks.object_visible).convert("L")) > 32
    unknown = np.array(Image.open(masks.object_unknown).convert("L")) > 32
    occluder = np.array(Image.open(masks.occluder).convert("L")) > 32 if masks.occluder else np.zeros_like(visible)

    features = _features_from_elements(
        facade_elements,
        shell=shell,
        plane_graph=plane_graph,
        visible=visible,
        unknown=unknown,
        occluder=occluder,
        config=config,
    )
    rows = _feature_rows(features)
    features = _attach_rows(features, rows)
    failure_flags = _failure_flags(features, rows)
    graph_confidence = _graph_confidence(features, rows, failure_flags)
    graph = FeatureGraph(
        features=features[:80],
        rows=rows[:24],
        graph_confidence=round(graph_confidence, 4),
        passed=graph_confidence >= 0.50 and not failure_flags,
        failure_flags=failure_flags,
    )
    (debug / "feature_graph.json").write_text(
        json.dumps(graph.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (debug / "feature_graph_score.json").write_text(
        json.dumps(
            {
                "graph_confidence": graph.graph_confidence,
                "passed": graph.passed,
                "failure_flags": graph.failure_flags,
                "feature_counts": _counts(feature.feature_type for feature in graph.features),
                "rows": len(graph.rows),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_feature_overlay(image=image, graph=graph, out_path=debug / "feature_graph_overlay.png")
    if not graph.passed:
        raise RuntimeError(f"FeatureGraph failed hard gate: {graph.failure_flags}")
    return graph


def feature_graph_to_candidate(
    shell: BuildingShell,
    plane_graph: PlaneGraph,
    feature_graph: FeatureGraph,
    config: RunConfig,
) -> Candidate:
    base = plane_graph_to_candidate(shell, plane_graph, config)
    lines = list(base.lines)
    primitive_ids = [f"plane_scaffold:{idx:03d}" for idx, _line in enumerate(base.lines)]
    selected_features = _select_features(feature_graph.features, limit=max(0, min(80, config.style.max_paths) - len(lines)))
    for feature in selected_features:
        for idx, line in enumerate(_feature_lines(feature)):
            lines.append(line)
            primitive_ids.append(f"{feature.id}:{idx:02d}")

    deduped = _dedupe_lines(lines, limit=min(110, config.style.max_paths))
    candidate = Candidate(
        candidate_id="feature_scaffold",
        variant="B2",
        family="FEATURE_SCAFFOLD",
        lines=deduped,
        final_eligible=True,
        primitive_rendered=True,
        proposal_only=False,
        candidate_svg=CandidateSVG(
            candidate_id="feature_scaffold",
            family="FEATURE_SCAFFOLD",
            primitive_ids=primitive_ids[: len(deduped)],
            final_eligible=True,
        ),
    )
    candidate.parameters.update(
        {
            "feature_scaffold": True,
            "feature_graph_score": feature_graph.graph_confidence,
            "feature_graph_passed": feature_graph.passed,
            "feature_graph_failure_flags": feature_graph.failure_flags,
            "feature_count": len(feature_graph.features),
            "feature_rows": len(feature_graph.rows),
            "line_budget": min(110, config.style.max_paths),
            "stroke_width_scale": 0.52,
            "building_shell": shell.to_dict(),
            "plane_graph": plane_graph.to_dict(),
            "feature_graph": feature_graph.to_dict(),
        }
    )
    return candidate


def _features_from_elements(
    elements: list[FacadeElement],
    *,
    shell: BuildingShell,
    plane_graph: PlaneGraph,
    visible,
    unknown,
    occluder,
    config: RunConfig,
) -> list[FeatureNode]:
    features: list[FeatureNode] = []
    for element in elements:
        if element.element_type not in FEATURE_TYPES:
            continue
        if not _inside_box(_center(element.bbox_xyxy), shell.bbox_xyxy, margin=18.0):
            continue
        if _area(element.bbox_xyxy) < 18.0:
            continue
        samples = _bbox_samples(element.bbox_xyxy)
        visible_ratio = _mask_ratio(samples, visible)
        unknown_ratio = _mask_ratio(samples, unknown)
        occluder_ratio = _mask_ratio(samples, occluder)
        if visible_ratio + unknown_ratio < 0.22 or occluder_ratio > 0.38:
            continue
        if element.element_type in {"window", "door"} and not _reasonable_opening(element.bbox_xyxy):
            continue
        feature_type = _canonical_feature_type(element)
        plane_id = _assign_plane(element.bbox_xyxy, plane_graph)
        confidence = min(0.92, element.confidence * 0.72 + visible_ratio * 0.18 + unknown_ratio * 0.05)
        features.append(
            FeatureNode(
                id=f"F_{feature_type}_{len(features):03d}",
                feature_type=feature_type,
                plane_id=plane_id,
                bbox_xyxy=element.bbox_xyxy,
                confidence=round(confidence, 4),
                source_element_ids=[element.id],
                completion_status=element.completion_status,
                evidence=[*element.evidence, f"plane:{plane_id}", "feature_graph:facade_parser"],
            )
        )
    features.sort(key=lambda f: (_feature_order(f.feature_type), f.bbox_xyxy[1], f.bbox_xyxy[0]))
    return features[:120]


def _feature_rows(features: list[FeatureNode]) -> list[FeatureRow]:
    row_features = [f for f in features if f.feature_type in {"window", "arched_window", "door"}]
    rows: list[FeatureRow] = []
    for plane_id in sorted({f.plane_id for f in row_features}):
        plane_items = [f for f in row_features if f.plane_id == plane_id]
        plane_items.sort(key=lambda f: (_center(f.bbox_xyxy)[1], _center(f.bbox_xyxy)[0]))
        buckets: list[list[FeatureNode]] = []
        for feature in plane_items:
            cy = _center(feature.bbox_xyxy)[1]
            height = feature.bbox_xyxy[3] - feature.bbox_xyxy[1]
            placed = False
            for bucket in buckets:
                bucket_y = sum(_center(item.bbox_xyxy)[1] for item in bucket) / len(bucket)
                bucket_h = sum(item.bbox_xyxy[3] - item.bbox_xyxy[1] for item in bucket) / len(bucket)
                if abs(cy - bucket_y) <= max(18.0, min(42.0, (height + bucket_h) * 0.58)):
                    bucket.append(feature)
                    placed = True
                    break
            if not placed:
                buckets.append([feature])
        for bucket in buckets:
            if len(bucket) < 2:
                continue
            y_center = sum(_center(item.bbox_xyxy)[1] for item in bucket) / len(bucket)
            confidence = sum(item.confidence for item in bucket) / len(bucket)
            rows.append(
                FeatureRow(
                    id=f"R_{plane_id.replace('.', '_')}_{len(rows):03d}",
                    plane_id=plane_id,
                    feature_type="opening_row",
                    member_ids=[item.id for item in bucket],
                    y_center=round(y_center, 2),
                    confidence=round(confidence, 4),
                )
            )
    return rows


def _attach_rows(features: list[FeatureNode], rows: list[FeatureRow]) -> list[FeatureNode]:
    row_by_member = {member_id: row.id for row in rows for member_id in row.member_ids}
    out: list[FeatureNode] = []
    for feature in features:
        if feature.id in row_by_member:
            feature.row_id = row_by_member[feature.id]
            feature.evidence.append("repetition_evidence:opening_row")
        out.append(feature)
    return out


def _select_features(features: list[FeatureNode], *, limit: int) -> list[FeatureNode]:
    selected: list[FeatureNode] = []
    caps = {
        "window": 18,
        "arched_window": 6,
        "door": 4,
        "balcony": 3,
        "pilaster": 8,
        "cornice": 8,
        "molding": 6,
        "sill": 8,
    }
    counts: dict[str, int] = {}
    for feature in sorted(features, key=lambda f: (f.row_id is None, _feature_order(f.feature_type), -f.confidence, f.bbox_xyxy[1], f.bbox_xyxy[0])):
        if counts.get(feature.feature_type, 0) >= caps.get(feature.feature_type, 4):
            continue
        selected.append(feature)
        counts[feature.feature_type] = counts.get(feature.feature_type, 0) + 1
        if len(selected) >= limit:
            break
    return selected


def _feature_lines(feature: FeatureNode) -> list[LinePrimitive]:
    x1, y1, x2, y2 = feature.bbox_xyxy
    w = max(1.0, x2 - x1)
    h = max(1.0, y2 - y1)
    confidence = feature.confidence
    source = f"feature_graph:{feature.feature_type}"
    if feature.feature_type == "arched_window":
        return [
            LinePrimitive(_arched_rect(x1, y1, x2, y2), role="arc", priority=6, source=source, confidence=confidence),
        ]
    if feature.feature_type == "window":
        rect = _rect(x1, y1, x2, y2)
        mullion_x = (x1 + x2) / 2
        lines = [LinePrimitive(rect, role="structure", priority=5, source=source, confidence=confidence)]
        if w >= 22 and h >= 26:
            lines.append(LinePrimitive([(mullion_x, y1 + h * 0.15), (mullion_x, y2 - h * 0.12)], role="accent", priority=3, source=source, confidence=confidence * 0.82))
        return lines
    if feature.feature_type == "door":
        return [LinePrimitive(_rect(x1, y1, x2, y2), role="structure", priority=6, source=source, confidence=confidence)]
    if feature.feature_type == "balcony":
        rail_y = y1 + h * 0.38
        return [
            LinePrimitive([(x1, y1), (x2, y1), (x2, y2), (x1, y2)], role="structure", priority=5, source=source, confidence=confidence),
            LinePrimitive([(x1, rail_y), (x2, rail_y)], role="accent", priority=3, source=source, confidence=confidence * 0.84),
        ]
    if feature.feature_type == "pilaster":
        return [
            LinePrimitive([(x1, y1), (x1, y2)], role="structure", priority=5, source=source, confidence=confidence),
            LinePrimitive([(x2, y1), (x2, y2)], role="structure", priority=5, source=source, confidence=confidence),
        ]
    if feature.feature_type in {"cornice", "molding", "sill"}:
        y = (y1 + y2) / 2
        return [LinePrimitive([(x1, y), (x2, y)], role="structure", priority=5, source=source, confidence=confidence)]
    return [LinePrimitive(_rect(x1, y1, x2, y2), role="accent", priority=3, source=source, confidence=confidence)]


def _failure_flags(features: list[FeatureNode], rows: list[FeatureRow]) -> list[str]:
    flags: list[str] = []
    openings = [feature for feature in features if feature.feature_type in {"window", "arched_window", "door"}]
    if len(features) < 3:
        flags.append("too_few_facade_features")
    if len(openings) < 2:
        flags.append("too_few_opening_features")
    if not rows and len(openings) >= 4:
        flags.append("missing_opening_rows")
    return flags


def _graph_confidence(features: list[FeatureNode], rows: list[FeatureRow], failure_flags: list[str]) -> float:
    openings = [feature for feature in features if feature.feature_type in {"window", "arched_window", "door"}]
    secondary = [feature for feature in features if feature.feature_type in {"balcony", "pilaster", "cornice", "molding", "sill"}]
    confidence = (
        0.16
        + 0.34 * min(1.0, len(openings) / 8.0)
        + 0.18 * min(1.0, len(secondary) / 6.0)
        + 0.18 * min(1.0, len(rows) / 2.0)
        + 0.14 * min(1.0, (sum(feature.confidence for feature in features) / max(1, len(features))) / 0.75)
    )
    confidence -= 0.13 * len(failure_flags)
    return max(0.0, min(1.0, confidence))


def _write_feature_overlay(*, image, graph: FeatureGraph, out_path: Path) -> Path:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")
    overlay = np.array(image.convert("RGB"))
    palette = {
        "window": (80, 220, 255),
        "arched_window": (140, 240, 255),
        "door": (80, 255, 140),
        "balcony": (255, 230, 80),
        "pilaster": (255, 150, 80),
        "cornice": (255, 255, 255),
        "molding": (210, 170, 255),
        "sill": (160, 255, 220),
    }
    for row in graph.rows:
        members = [feature for feature in graph.features if feature.id in row.member_ids]
        if len(members) >= 2:
            x1 = min(feature.bbox_xyxy[0] for feature in members)
            x2 = max(feature.bbox_xyxy[2] for feature in members)
            y = int(round(row.y_center))
            cv2.line(overlay, (int(x1), y), (int(x2), y), (255, 255, 255), 1, cv2.LINE_AA)
    for feature in graph.features:
        color = palette.get(feature.feature_type, (235, 235, 235))
        x1, y1, x2, y2 = [int(round(v)) for v in feature.bbox_xyxy]
        cv2.rectangle(overlay, (x1, y1), (x2, y2), color, 2, cv2.LINE_AA)
        cv2.putText(overlay, feature.feature_type[:10], (x1, max(12, y1 - 4)), cv2.FONT_HERSHEY_SIMPLEX, 0.36, color, 1, cv2.LINE_AA)
    cv2.putText(
        overlay,
        f"FeatureGraph {graph.graph_confidence:.2f}",
        (18, 24),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.68,
        (255, 255, 255),
        2,
        cv2.LINE_AA,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(overlay).save(out_path)
    return out_path


def _canonical_feature_type(element: FacadeElement) -> str:
    if element.element_type == "window":
        x1, y1, x2, y2 = element.bbox_xyxy
        if (y2 - y1) > (x2 - x1) * 1.38:
            return "arched_window"
    return element.element_type


def _reasonable_opening(box: tuple[float, float, float, float]) -> bool:
    x1, y1, x2, y2 = box
    w = max(1.0, x2 - x1)
    h = max(1.0, y2 - y1)
    aspect = w / h
    area = w * h
    return 0.14 <= aspect <= 4.2 and 30.0 <= area <= 30000.0


def _assign_plane(box: tuple[float, float, float, float], plane_graph: PlaneGraph) -> str:
    center = _center(box)
    best = None
    best_score = -1.0
    for plane in plane_graph.planes:
        score = 0.0
        if _inside_box(center, plane.bbox_xyxy, margin=24.0):
            score += 1.0
        px1, py1, px2, py2 = plane.bbox_xyxy
        score -= abs(center[0] - (px1 + px2) / 2) / max(1.0, px2 - px1) * 0.25
        if score > best_score:
            best_score = score
            best = plane.id
    return best or (plane_graph.planes[0].id if plane_graph.planes else "plane.unknown")


def _dedupe_lines(lines: list[LinePrimitive], *, limit: int) -> list[LinePrimitive]:
    out: list[LinePrimitive] = []
    seen: set[tuple[str, int, int, int]] = set()
    for line in sorted(lines, key=lambda item: (-item.priority, -item.confidence, -item.length)):
        if line.length < 8:
            continue
        key = _line_bucket(line)
        if key in seen:
            continue
        seen.add(key)
        out.append(line)
        if len(out) >= limit:
            break
    return out


def _line_bucket(line: LinePrimitive) -> tuple[str, int, int, int]:
    a = line.points[0]
    b = line.points[-1]
    angle = math.degrees(math.atan2(b[1] - a[1], b[0] - a[0])) % 180.0
    theta = math.radians(angle)
    nx = -math.sin(theta)
    ny = math.cos(theta)
    mx = (a[0] + b[0]) / 2
    my = (a[1] + b[1]) / 2
    rho = mx * nx + my * ny
    return (line.role, int(round(angle / 9.0)), int(round(rho / 18.0)), int(round(my / 42.0)))


def _bbox_samples(box: tuple[float, float, float, float]) -> list[tuple[int, int]]:
    x1, y1, x2, y2 = box
    samples: list[tuple[int, int]] = []
    for tx in [0.18, 0.5, 0.82]:
        for ty in [0.18, 0.5, 0.82]:
            samples.append((int(round(x1 + (x2 - x1) * tx)), int(round(y1 + (y2 - y1) * ty))))
    return samples


def _mask_ratio(points: list[tuple[int, int]], mask) -> float:
    if not points:
        return 0.0
    h, w = mask.shape[:2]
    inside = 0
    for x, y in points:
        if 0 <= x < w and 0 <= y < h and bool(mask[y, x]):
            inside += 1
    return inside / len(points)


def _rect(x1: float, y1: float, x2: float, y2: float) -> list[tuple[float, float]]:
    return [(x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)]


def _arched_rect(x1: float, y1: float, x2: float, y2: float) -> list[tuple[float, float]]:
    cx = (x1 + x2) / 2
    rx = max(1.0, (x2 - x1) / 2)
    arch_h = min((y2 - y1) * 0.40, rx * 1.10)
    base_y = y1 + arch_h
    points: list[tuple[float, float]] = [(x1, y2), (x1, base_y)]
    for idx in range(1, 8):
        t = math.pi * (1.0 - idx / 8.0)
        points.append((cx + math.cos(t) * rx, base_y - math.sin(t) * arch_h))
    points.extend([(x2, y2), (x1, y2)])
    return points


def _inside_box(point: tuple[float, float], box: tuple[float, float, float, float], *, margin: float = 0.0) -> bool:
    x, y = point
    x1, y1, x2, y2 = box
    return x1 - margin <= x <= x2 + margin and y1 - margin <= y <= y2 + margin


def _center(box: tuple[float, float, float, float]) -> tuple[float, float]:
    x1, y1, x2, y2 = box
    return (x1 + x2) / 2, (y1 + y2) / 2


def _area(box: tuple[float, float, float, float]) -> float:
    x1, y1, x2, y2 = box
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)


def _feature_order(feature_type: str) -> int:
    return {
        "door": 0,
        "arched_window": 1,
        "window": 2,
        "balcony": 3,
        "pilaster": 4,
        "cornice": 5,
        "molding": 6,
        "sill": 7,
    }.get(feature_type, 9)


def _counts(values) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        out[str(value)] = out.get(str(value), 0) + 1
    return out
