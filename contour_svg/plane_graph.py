from __future__ import annotations

import json
import math
from pathlib import Path

from .building_shell import building_shell_to_candidate
from .config import RunConfig
from .contracts import (
    BuildingShell,
    Candidate,
    CandidateSVG,
    EvidenceInventory,
    FacadeElement,
    FacadePlane,
    GuideSet,
    LinePrimitive,
    MaskBundle,
    PlaneGraph,
    PlaneSegment,
)
from .dependencies import require_module


def build_plane_graph(
    *,
    image,
    masks: MaskBundle,
    guides: GuideSet,
    facade_elements: list[FacadeElement],
    evidence: EvidenceInventory,
    shell: BuildingShell,
    out_dir: str | Path,
    config: RunConfig,
) -> PlaneGraph:
    np = require_module("numpy", "numpy")
    Image = require_module("PIL.Image", "Pillow")

    debug = Path(out_dir) / "debug"
    debug.mkdir(parents=True, exist_ok=True)

    visible = np.array(Image.open(masks.object_visible).convert("L")) > 32
    unknown = np.array(Image.open(masks.object_unknown).convert("L")) > 32
    occluder = np.array(Image.open(masks.occluder).convert("L")) > 32 if masks.occluder else np.zeros_like(visible)

    wall_bbox, wall_evidence = _wall_plane_bbox(facade_elements, shell)
    corner_x = _main_corner_x(shell, guides, wall_bbox)
    planes = _build_facade_planes(wall_bbox, corner_x, wall_evidence, evidence)
    guide_segments, perspective_groups = _segments_from_guides(
        guides=guides,
        shell=shell,
        visible=visible,
        unknown=unknown,
        occluder=occluder,
        corner_x=corner_x,
        config=config,
    )
    shell_segments = _segments_from_shell(shell, corner_x)
    all_segments = _dedupe_segments([*guide_segments, *shell_segments], limit=96)
    bands = [
        segment
        for segment in all_segments
        if segment.segment_type in {"roof_band", "cornice_band", "base_band"}
    ]
    vertical_edges = [segment for segment in all_segments if segment.segment_type == "vertical_edge"]
    failure_flags = _failure_flags(planes, bands, vertical_edges)
    graph_confidence = _graph_confidence(planes, bands, vertical_edges, failure_flags)
    graph = PlaneGraph(
        planes=planes,
        bands=bands[:28],
        vertical_edges=vertical_edges[:14],
        perspective_groups=perspective_groups,
        graph_confidence=round(graph_confidence, 4),
        passed=graph_confidence >= 0.54 and not failure_flags,
        failure_flags=failure_flags,
    )
    (debug / "plane_graph.json").write_text(
        json.dumps(graph.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (debug / "plane_graph_score.json").write_text(
        json.dumps(
            {
                "graph_confidence": graph.graph_confidence,
                "passed": graph.passed,
                "failure_flags": graph.failure_flags,
                "segment_counts": {
                    "planes": len(graph.planes),
                    "bands": len(graph.bands),
                    "vertical_edges": len(graph.vertical_edges),
                },
                "perspective_groups": graph.perspective_groups,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_plane_overlay(image=image, shell=shell, graph=graph, out_path=debug / "plane_graph_overlay.png")
    if not graph.passed:
        raise RuntimeError(f"PlaneGraph failed hard gate: {graph.failure_flags}")
    return graph


def plane_graph_to_candidate(shell: BuildingShell, graph: PlaneGraph, config: RunConfig) -> Candidate:
    base = building_shell_to_candidate(shell, config)
    lines: list[LinePrimitive] = list(base.lines)
    primitive_ids = [f"shell:{idx:03d}" for idx, _line in enumerate(base.lines)]
    for segment in [*graph.bands, *graph.vertical_edges]:
        role = "roofline" if segment.segment_type == "roof_band" else "structure"
        lines.append(
            LinePrimitive(
                segment.geometry.points,
                role=role,
                priority=8 if segment.segment_type in {"roof_band", "vertical_edge"} else 6,
                source=f"plane_graph:{segment.segment_type}",
                confidence=segment.confidence,
            )
        )
        primitive_ids.append(segment.id)

    deduped = _dedupe_lines(lines, limit=min(70, config.style.max_paths))
    candidate = Candidate(
        candidate_id="plane_scaffold",
        variant="B2",
        family="PLANE_SCAFFOLD",
        lines=deduped,
        final_eligible=True,
        primitive_rendered=True,
        proposal_only=False,
        candidate_svg=CandidateSVG(
            candidate_id="plane_scaffold",
            family="PLANE_SCAFFOLD",
            primitive_ids=primitive_ids[: len(deduped)],
            final_eligible=True,
        ),
    )
    candidate.parameters.update(
        {
            "plane_scaffold": True,
            "plane_graph_score": graph.graph_confidence,
            "plane_graph_passed": graph.passed,
            "plane_graph_failure_flags": graph.failure_flags,
            "line_budget": min(70, config.style.max_paths),
            "stroke_width_scale": 0.62,
            "building_shell": shell.to_dict(),
            "plane_graph": graph.to_dict(),
        }
    )
    return candidate


def _wall_plane_bbox(
    elements: list[FacadeElement],
    shell: BuildingShell,
) -> tuple[tuple[float, float, float, float], list[str]]:
    wall_planes = [element for element in elements if element.element_type == "wall_plane"]
    if not wall_planes:
        return shell.bbox_xyxy, shell.evidence_ids[:16]
    best = max(wall_planes, key=lambda element: _area(element.bbox_xyxy) * max(0.01, element.confidence))
    return best.bbox_xyxy, [best.id, *best.evidence]


def _main_corner_x(shell: BuildingShell, guides: GuideSet, bbox: tuple[float, float, float, float]) -> float:
    x1, _y1, x2, _y2 = bbox
    candidates: list[tuple[float, float]] = []
    for segment in shell.facade_corner_segments:
        xs = [point[0] for point in segment.geometry.points]
        x = sum(xs) / len(xs)
        if x1 <= x <= x2:
            candidates.append((segment.geometry.length * segment.confidence, x))
    for line in guides.lines:
        if len(line.points) < 2 or line.length < 90:
            continue
        angle = _angle(line.points[0], line.points[-1])
        if not _is_vertical(angle):
            continue
        x = sum(point[0] for point in line.points) / len(line.points)
        if x1 <= x <= x2:
            candidates.append((line.length * line.confidence * 0.75, x))
    if candidates:
        return max(candidates, key=lambda item: item[0])[1]
    return x1 + (x2 - x1) * 0.58


def _build_facade_planes(
    bbox: tuple[float, float, float, float],
    corner_x: float,
    evidence_ids: list[str],
    evidence: EvidenceInventory,
) -> list[FacadePlane]:
    x1, y1, x2, y2 = bbox
    width = max(1.0, x2 - x1)
    planes: list[FacadePlane] = []
    shell_evidence = [
        item.id
        for item in evidence.items
        if item.role_hint in {"plane", "shell", "structure"} or item.kind in {"wall_plane", "line_segment"}
    ][:24]
    if x1 + width * 0.22 <= corner_x <= x1 + width * 0.82:
        planes.append(
            FacadePlane(
                id="plane.front",
                plane_type="front_facade",
                polygon=[(x1, y1), (corner_x, y1), (corner_x, y2), (x1, y2), (x1, y1)],
                bbox_xyxy=(x1, y1, corner_x, y2),
                confidence=0.72,
                evidence_ids=[*evidence_ids, *shell_evidence],
                vanishing_group="left_vp",
            )
        )
        planes.append(
            FacadePlane(
                id="plane.side",
                plane_type="side_facade",
                polygon=[(corner_x, y1), (x2, y1 + width * 0.05), (x2, y2), (corner_x, y2), (corner_x, y1)],
                bbox_xyxy=(corner_x, y1, x2, y2),
                confidence=0.66,
                evidence_ids=[*evidence_ids, *shell_evidence],
                vanishing_group="right_vp",
            )
        )
    else:
        planes.append(
            FacadePlane(
                id="plane.main",
                plane_type="main_facade",
                polygon=[(x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)],
                bbox_xyxy=bbox,
                confidence=0.62,
                evidence_ids=[*evidence_ids, *shell_evidence],
                vanishing_group="frontal",
            )
        )
    return planes


def _segments_from_guides(
    *,
    guides: GuideSet,
    shell: BuildingShell,
    visible,
    unknown,
    occluder,
    corner_x: float,
    config: RunConfig,
) -> tuple[list[PlaneSegment], dict[str, int]]:
    segments: list[PlaneSegment] = []
    perspective_counts: dict[str, int] = {"horizontal": 0, "vertical": 0, "left_vp": 0, "right_vp": 0}
    min_len = max(36.0, config.style.min_line_length * 2.2)
    for idx, line in enumerate(guides.lines):
        if len(line.points) < 2 or line.length < min_len:
            continue
        if line.source not in {"mlsd", "deeplsd", "hawp", "hough", "lsd", "sam2_mask"}:
            continue
        if not _line_mid_inside_bbox(line, shell.bbox_xyxy, margin=36):
            continue
        samples = _sample_points(line)
        visible_ratio = _mask_ratio(samples, visible)
        unknown_ratio = _mask_ratio(samples, unknown)
        occluder_ratio = _mask_ratio(samples, occluder)
        if visible_ratio + unknown_ratio < 0.28 or occluder_ratio > 0.22:
            continue
        angle = _angle(line.points[0], line.points[-1])
        group = _perspective_group(angle)
        perspective_counts[group] = perspective_counts.get(group, 0) + 1
        segment_type = _segment_type(line, shell.bbox_xyxy)
        if segment_type is None:
            continue
        plane_id = "plane.front" if _line_mid_x(line) <= corner_x else "plane.side"
        confidence = min(0.92, 0.50 + line.confidence * 0.32 + visible_ratio * 0.10 + unknown_ratio * 0.04)
        role = "roofline" if segment_type == "roof_band" else "structure"
        segments.append(
            PlaneSegment(
                id=f"plane.guide.{idx:04d}",
                segment_type=segment_type,
                plane_id=plane_id,
                geometry=LinePrimitive(
                    [line.points[0], line.points[-1]],
                    role=role,
                    priority=8 if segment_type in {"roof_band", "vertical_edge"} else 6,
                    source=f"plane_graph:{line.source}",
                    confidence=line.confidence,
                ),
                confidence=round(confidence, 4),
                source_evidence_ids=[f"E_line_{idx:05d}"],
                completion_status="interpolated_band" if unknown_ratio > 0.26 else "visible",
            )
        )
    return segments, perspective_counts


def _segments_from_shell(shell: BuildingShell, corner_x: float) -> list[PlaneSegment]:
    out: list[PlaneSegment] = []
    for idx, source in enumerate([*shell.roof_segments, *shell.base_segments, *shell.facade_corner_segments]):
        if source.segment_type == "roofline":
            segment_type = "roof_band"
        elif source.segment_type == "base":
            segment_type = "base_band"
        elif source.segment_type == "facade_corner":
            segment_type = "vertical_edge"
        else:
            continue
        plane_id = "plane.front" if _line_mid_x(source.geometry) <= corner_x else "plane.side"
        out.append(
            PlaneSegment(
                id=f"plane.shell.{idx:03d}",
                segment_type=segment_type,
                plane_id=plane_id,
                geometry=source.geometry,
                confidence=max(0.52, source.confidence),
                source_evidence_ids=source.source_evidence_ids,
                completion_status=source.completion_status,
            )
        )
    return out


def _failure_flags(
    planes: list[FacadePlane],
    bands: list[PlaneSegment],
    vertical_edges: list[PlaneSegment],
) -> list[str]:
    flags: list[str] = []
    if not planes:
        flags.append("missing_facade_plane")
    if not any(segment.segment_type == "roof_band" for segment in bands):
        flags.append("missing_roof_band")
    if not any(segment.segment_type == "base_band" for segment in bands):
        flags.append("missing_base_band")
    if not vertical_edges:
        flags.append("missing_vertical_edges")
    if len(bands) + len(vertical_edges) < 5:
        flags.append("too_few_plane_segments")
    return flags


def _graph_confidence(
    planes: list[FacadePlane],
    bands: list[PlaneSegment],
    vertical_edges: list[PlaneSegment],
    failure_flags: list[str],
) -> float:
    confidence = (
        0.18
        + 0.18 * min(1.0, len(planes) / 2.0)
        + 0.20 * min(1.0, len([s for s in bands if s.segment_type == "roof_band"]) / 3.0)
        + 0.15 * min(1.0, len([s for s in bands if s.segment_type == "base_band"]) / 2.0)
        + 0.13 * min(1.0, len([s for s in bands if s.segment_type == "cornice_band"]) / 4.0)
        + 0.16 * min(1.0, len(vertical_edges) / 2.0)
    )
    confidence -= 0.12 * len(failure_flags)
    return max(0.0, min(1.0, confidence))


def _write_plane_overlay(*, image, shell: BuildingShell, graph: PlaneGraph, out_path: Path) -> Path:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")
    overlay = np.array(image.convert("RGB"))
    plane_layer = overlay.copy()
    colors = {
        "plane.front": (60, 120, 210),
        "plane.side": (80, 175, 130),
        "plane.main": (80, 130, 220),
    }
    for plane in graph.planes:
        pts = np.array([(int(round(x)), int(round(y))) for x, y in plane.polygon], dtype=np.int32)
        cv2.fillPoly(plane_layer, [pts], colors.get(plane.id, (80, 130, 220)))
        cv2.polylines(overlay, [pts], True, (255, 255, 255), 2, cv2.LINE_AA)
    overlay = cv2.addWeighted(overlay, 0.72, plane_layer, 0.28, 0)
    for segment in graph.bands:
        color = {
            "roof_band": (80, 240, 255),
            "cornice_band": (255, 255, 255),
            "base_band": (120, 255, 140),
        }.get(segment.segment_type, (220, 220, 220))
        _draw_line(overlay, segment.geometry, color, 3)
    for segment in graph.vertical_edges:
        _draw_line(overlay, segment.geometry, (255, 170, 80), 3)
    x1, y1, x2, y2 = [int(round(v)) for v in shell.bbox_xyxy]
    cv2.rectangle(overlay, (x1, y1), (x2, y2), (180, 180, 255), 2, cv2.LINE_AA)
    cv2.putText(
        overlay,
        f"PlaneGraph {graph.graph_confidence:.2f}",
        (x1, max(18, y1 - 8)),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.62,
        (255, 255, 255),
        2,
        cv2.LINE_AA,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(overlay).save(out_path)
    return out_path


def _draw_line(canvas, line: LinePrimitive, color: tuple[int, int, int], thickness: int) -> None:
    cv2 = require_module("cv2", "opencv-python-headless")
    pts = [(int(round(x)), int(round(y))) for x, y in line.points]
    for a, b in zip(pts, pts[1:]):
        cv2.line(canvas, a, b, color, thickness, cv2.LINE_AA)


def _dedupe_segments(segments: list[PlaneSegment], *, limit: int) -> list[PlaneSegment]:
    out: list[PlaneSegment] = []
    seen: set[tuple[str, str, int, int, int]] = set()
    for segment in sorted(segments, key=lambda s: (-s.confidence, -s.geometry.length, s.id)):
        key = _segment_bucket(segment)
        if key in seen:
            continue
        seen.add(key)
        out.append(segment)
        if len(out) >= limit:
            break
    return out


def _dedupe_lines(lines: list[LinePrimitive], *, limit: int) -> list[LinePrimitive]:
    out: list[LinePrimitive] = []
    seen: set[tuple[str, int, int, int]] = set()
    for line in sorted(lines, key=lambda item: (-item.priority, -item.confidence, -item.length)):
        if line.length < 12:
            continue
        key = _line_bucket(line)
        if key in seen:
            continue
        seen.add(key)
        out.append(line)
        if len(out) >= limit:
            break
    return out


def _segment_bucket(segment: PlaneSegment) -> tuple[str, str, int, int, int]:
    return (segment.plane_id, segment.segment_type, *_line_bucket(segment.geometry)[1:])


def _line_bucket(line: LinePrimitive) -> tuple[str, int, int, int]:
    a = line.points[0]
    b = line.points[-1]
    angle = _angle(a, b)
    theta = math.radians(angle)
    nx = -math.sin(theta)
    ny = math.cos(theta)
    mx = (a[0] + b[0]) / 2
    my = (a[1] + b[1]) / 2
    rho = mx * nx + my * ny
    return (line.role, int(round(angle / 9.0)), int(round(rho / 30.0)), int(round(my / 62.0)))


def _segment_type(line: LinePrimitive, bbox_xyxy: tuple[float, float, float, float]) -> str | None:
    angle = _angle(line.points[0], line.points[-1])
    y_mid = _line_mid_y(line)
    x1, y1, x2, y2 = bbox_xyxy
    height = max(1.0, y2 - y1)
    horizontal = _is_horizontal(angle)
    vertical = _is_vertical(angle)
    shallow = horizontal or 12.0 <= angle <= 42.0 or 138.0 <= angle <= 168.0
    if shallow and y_mid <= y1 + height * 0.42:
        return "roof_band"
    if horizontal and y1 + height * 0.24 <= y_mid <= y1 + height * 0.72:
        return "cornice_band"
    if horizontal and y_mid >= y1 + height * 0.55:
        return "base_band"
    if vertical and line.length >= height * 0.16:
        return "vertical_edge"
    return None


def _perspective_group(angle: float) -> str:
    if _is_horizontal(angle):
        return "horizontal"
    if _is_vertical(angle):
        return "vertical"
    if angle < 90.0:
        return "left_vp"
    return "right_vp"


def _sample_points(line: LinePrimitive) -> list[tuple[int, int]]:
    points: list[tuple[int, int]] = []
    for a, b in zip(line.points, line.points[1:]):
        ax, ay = a
        bx, by = b
        steps = max(2, int(_distance(a, b) / 7.0))
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


def _line_mid_inside_bbox(line: LinePrimitive, bbox_xyxy: tuple[float, float, float, float], *, margin: float) -> bool:
    x1, y1, x2, y2 = bbox_xyxy
    mx = _line_mid_x(line)
    my = _line_mid_y(line)
    return x1 - margin <= mx <= x2 + margin and y1 - margin <= my <= y2 + margin


def _line_mid_x(line: LinePrimitive) -> float:
    return sum(x for x, _y in line.points) / max(1, len(line.points))


def _line_mid_y(line: LinePrimitive) -> float:
    return sum(y for _x, y in line.points) / max(1, len(line.points))


def _area(box: tuple[float, float, float, float]) -> float:
    x1, y1, x2, y2 = box
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)


def _angle(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.degrees(math.atan2(b[1] - a[1], b[0] - a[0])) % 180.0


def _is_horizontal(angle: float) -> bool:
    return angle <= 12.0 or angle >= 168.0


def _is_vertical(angle: float) -> bool:
    return 78.0 <= angle <= 102.0


def _distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(b[0] - a[0], b[1] - a[1])
