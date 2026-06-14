from __future__ import annotations

import json
import math
from pathlib import Path

from .config import RunConfig
from .contracts import (
    BuildingShell,
    Candidate,
    CandidateSVG,
    EvidenceInventory,
    FacadeElement,
    GuideSet,
    LinePrimitive,
    MaskBundle,
    ShellSegment,
)
from .dependencies import require_module


def build_building_shell(
    *,
    image,
    masks: MaskBundle,
    guides: GuideSet,
    facade_elements: list[FacadeElement],
    evidence: EvidenceInventory,
    out_dir: str | Path,
    config: RunConfig,
) -> BuildingShell:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")

    debug = Path(out_dir) / "debug"
    debug.mkdir(parents=True, exist_ok=True)

    allowed = np.array(Image.open(masks.allowed_line_region).convert("L")) > 32
    visible = np.array(Image.open(masks.object_visible).convert("L")) > 32
    occluder = np.array(Image.open(masks.occluder).convert("L")) > 32 if masks.occluder else np.zeros_like(allowed)
    unknown = np.array(Image.open(masks.object_unknown).convert("L")) > 32
    shell_mask = _smooth_shell_mask(allowed, config)
    hull_polygon, bbox_xyxy = _largest_shell_polygon(shell_mask, config)

    visible_segments, completed_segments = _segments_from_hull(
        hull_polygon,
        visible=visible,
        unknown=unknown,
        occluder=occluder,
        evidence_ids=["E_mask_object_visible", "E_mask_object_unknown"],
    )
    structural_segments = _structural_segments_from_guides(
        guides,
        bbox_xyxy=bbox_xyxy,
        visible=visible,
        unknown=unknown,
        occluder=occluder,
        config=config,
    )
    roof_segments = _select_segments(structural_segments, "roofline", limit=9)
    base_segments = _select_segments(structural_segments, "base", limit=6)
    facade_corner_segments = _select_segments(structural_segments, "facade_corner", limit=7)
    # Keep hull-derived edges in the shell even when guide detectors miss a
    # region. This is the coarse object understanding gate, not a detail pass.
    hull_roof = _select_segments(visible_segments + completed_segments, "roofline", limit=5)
    hull_base = _select_segments(visible_segments + completed_segments, "base", limit=4)
    hull_corners = _select_segments(visible_segments + completed_segments, "facade_corner", limit=5)
    roof_segments = _dedupe_segments([*roof_segments, *hull_roof], limit=10)
    base_segments = _dedupe_segments([*base_segments, *hull_base], limit=7)
    facade_corner_segments = _dedupe_segments([*facade_corner_segments, *hull_corners], limit=8)

    occlusion_zones = _occlusion_zones(unknown | occluder)
    failure_flags = _shell_failure_flags(
        hull_polygon=hull_polygon,
        roof_segments=roof_segments,
        base_segments=base_segments,
        facade_corner_segments=facade_corner_segments,
        visible_segments=visible_segments,
        completed_segments=completed_segments,
    )
    shell_confidence = _shell_confidence(
        shell_mask=shell_mask,
        bbox_xyxy=bbox_xyxy,
        roof_segments=roof_segments,
        base_segments=base_segments,
        facade_corner_segments=facade_corner_segments,
        failure_flags=failure_flags,
    )
    passed = shell_confidence >= 0.52 and not failure_flags
    shell = BuildingShell(
        hull_polygon=hull_polygon,
        visible_hull_segments=_dedupe_segments(visible_segments, limit=18),
        completed_hull_segments=_dedupe_segments(completed_segments, limit=10),
        roof_segments=roof_segments,
        base_segments=base_segments,
        facade_corner_segments=facade_corner_segments,
        bbox_xyxy=bbox_xyxy,
        shell_confidence=round(shell_confidence, 4),
        occlusion_zones=occlusion_zones[:12],
        evidence_ids=_shell_evidence_ids(evidence, facade_elements),
        passed=passed,
        failure_flags=failure_flags,
    )
    (debug / "building_shell.json").write_text(
        json.dumps(shell.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (debug / "building_shell_score.json").write_text(
        json.dumps(
            {
                "shell_confidence": shell.shell_confidence,
                "passed": shell.passed,
                "failure_flags": shell.failure_flags,
                "segment_counts": {
                    "visible_hull": len(shell.visible_hull_segments),
                    "completed_hull": len(shell.completed_hull_segments),
                    "roof": len(shell.roof_segments),
                    "base": len(shell.base_segments),
                    "facade_corner": len(shell.facade_corner_segments),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_shell_overlay(
        image=image,
        shell=shell,
        shell_mask=shell_mask,
        occluder=occluder,
        out_path=debug / "building_shell_overlay.png",
    )
    if not shell.passed:
        raise RuntimeError(f"BuildingShell failed hard gate: {shell.failure_flags}")
    return shell


def building_shell_to_candidate(shell: BuildingShell, config: RunConfig) -> Candidate:
    segments = _dedupe_segments(
        [
            *shell.visible_hull_segments,
            *[
                segment
                for segment in shell.completed_hull_segments
                if segment.segment_type in {"roofline", "base", "facade_corner"}
            ],
            *shell.roof_segments,
            *shell.base_segments,
            *shell.facade_corner_segments,
        ],
        limit=35,
    )
    lines = [segment.geometry for segment in segments[:35]]
    candidate = Candidate(
        candidate_id="shell_only",
        variant="B2",
        family="SHELL_ONLY",
        lines=lines,
        final_eligible=True,
        primitive_rendered=True,
        proposal_only=False,
        candidate_svg=CandidateSVG(
            candidate_id="shell_only",
            family="SHELL_ONLY",
            primitive_ids=[segment.id for segment in segments[:35]],
            final_eligible=True,
        ),
    )
    candidate.parameters.update(
        {
            "shell_only": True,
            "shell_score": shell.shell_confidence,
            "shell_passed": shell.passed,
            "shell_failure_flags": shell.failure_flags,
            "line_budget": min(35, config.style.max_paths),
            "stroke_width_scale": 0.72,
            "building_shell": shell.to_dict(),
        }
    )
    return candidate


def _smooth_shell_mask(mask, config: RunConfig):
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    working = mask.astype("uint8") * 255
    if config.input.bbox_hint_xyxy:
        x1, y1, x2, y2 = [int(round(v)) for v in config.input.bbox_hint_xyxy]
        h, w = working.shape[:2]
        focus = np.zeros_like(working)
        focus[max(0, y1) : min(h, y2), max(0, x1) : min(w, x2)] = 255
        working = cv2.bitwise_and(working, focus)
    close_kernel = np.ones((29, 29), dtype=np.uint8)
    open_kernel = np.ones((5, 5), dtype=np.uint8)
    working = cv2.morphologyEx(working, cv2.MORPH_CLOSE, close_kernel, iterations=1)
    working = cv2.morphologyEx(working, cv2.MORPH_OPEN, open_kernel, iterations=1)
    return working > 32


def _largest_shell_polygon(mask, config: RunConfig) -> tuple[list[tuple[float, float]], tuple[float, float, float, float]]:
    cv2 = require_module("cv2", "opencv-python-headless")
    contours, _ = cv2.findContours(mask.astype("uint8") * 255, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        raise RuntimeError("BuildingShell requires a non-empty allowed object mask")
    contour = max(contours, key=cv2.contourArea)
    if cv2.contourArea(contour) < mask.shape[0] * mask.shape[1] * 0.02:
        raise RuntimeError("BuildingShell object mask is too small for shell extraction")
    epsilon = max(8.0, cv2.arcLength(contour, True) * 0.014)
    approx = cv2.approxPolyDP(contour, epsilon, True)
    points = [(float(x), float(y)) for [[x, y]] in approx]
    if len(points) < 4:
        x, y, w, h = cv2.boundingRect(contour)
        points = [(float(x), float(y)), (float(x + w), float(y)), (float(x + w), float(y + h)), (float(x), float(y + h))]
    if points[0] != points[-1]:
        points.append(points[0])
    xs = [x for x, _ in points]
    ys = [y for _, y in points]
    return points, (min(xs), min(ys), max(xs), max(ys))


def _segments_from_hull(
    hull: list[tuple[float, float]],
    *,
    visible,
    unknown,
    occluder,
    evidence_ids: list[str],
) -> tuple[list[ShellSegment], list[ShellSegment]]:
    visible_segments: list[ShellSegment] = []
    completed_segments: list[ShellSegment] = []
    bbox = _bbox_from_points(hull)
    for idx, (a, b) in enumerate(zip(hull, hull[1:])):
        line = LinePrimitive([a, b], role=_line_role_for_segment(a, b, bbox), priority=10, source="building_shell_mask", confidence=0.74)
        if line.length < 18:
            continue
        samples = _sample_points(line)
        unknown_ratio = _mask_ratio(samples, unknown)
        occluder_ratio = _mask_ratio(samples, occluder)
        segment_type = _segment_type(line, bbox)
        segment = ShellSegment(
            id=f"shell.hull.{idx:02d}",
            segment_type=segment_type,
            geometry=line,
            confidence=round(max(0.45, 0.78 - occluder_ratio * 0.18), 4),
            source_evidence_ids=evidence_ids,
            completion_status="interpolated_shell" if unknown_ratio > 0.20 or occluder_ratio > 0.25 else "visible",
        )
        if segment.completion_status == "visible":
            visible_segments.append(segment)
        else:
            completed_segments.append(segment)
    return visible_segments, completed_segments


def _structural_segments_from_guides(
    guides: GuideSet,
    *,
    bbox_xyxy: tuple[float, float, float, float],
    visible,
    unknown,
    occluder,
    config: RunConfig,
) -> list[ShellSegment]:
    segments: list[ShellSegment] = []
    x1, y1, x2, y2 = bbox_xyxy
    min_len = max(48.0, config.style.min_line_length * 2.6)
    for idx, line in enumerate(guides.lines):
        if len(line.points) < 2 or line.length < min_len:
            continue
        if line.source not in {"mlsd", "deeplsd", "hawp", "hough", "lsd", "sam2_mask"}:
            continue
        if not _line_mid_inside_bbox(line, bbox_xyxy, margin=32):
            continue
        segment_type = _segment_type(line, bbox_xyxy)
        if segment_type not in {"roofline", "base", "facade_corner"}:
            continue
        samples = _sample_points(line)
        visible_ratio = _mask_ratio(samples, visible)
        unknown_ratio = _mask_ratio(samples, unknown)
        occluder_ratio = _mask_ratio(samples, occluder)
        if visible_ratio + unknown_ratio < 0.35 or occluder_ratio > 0.18:
            continue
        role = "roofline" if segment_type == "roofline" else "structure"
        geometry = LinePrimitive(
            [line.points[0], line.points[-1]],
            role=role,
            priority=9 if segment_type in {"roofline", "facade_corner"} else 8,
            source=f"building_shell:{line.source}",
            confidence=line.confidence,
        )
        if segment_type == "roofline" and not (y1 - 10 <= _line_mid_y(geometry) <= y1 + (y2 - y1) * 0.48):
            continue
        if segment_type == "base" and not (_line_mid_y(geometry) >= y1 + (y2 - y1) * 0.54):
            continue
        segments.append(
            ShellSegment(
                id=f"shell.guide.{idx:04d}",
                segment_type=segment_type,
                geometry=geometry,
                confidence=round(min(0.92, 0.58 + line.confidence * 0.34 + visible_ratio * 0.08), 4),
                source_evidence_ids=[f"E_line_{idx:05d}"],
                completion_status="interpolated_shell" if unknown_ratio > 0.25 else "visible",
            )
        )
    return segments


def _select_segments(segments: list[ShellSegment], segment_type: str, *, limit: int) -> list[ShellSegment]:
    selected = [segment for segment in segments if segment.segment_type == segment_type]
    selected.sort(key=lambda s: (s.completion_status != "visible", -s.confidence, -s.geometry.length, s.id))
    return _dedupe_segments(selected, limit=limit)


def _dedupe_segments(segments: list[ShellSegment], *, limit: int) -> list[ShellSegment]:
    out: list[ShellSegment] = []
    buckets: set[tuple[str, int, int, int]] = set()
    for segment in sorted(segments, key=lambda s: (-s.confidence, -s.geometry.length, s.id)):
        bucket = _segment_bucket(segment)
        if bucket in buckets:
            continue
        buckets.add(bucket)
        out.append(segment)
        if len(out) >= limit:
            break
    return out


def _segment_bucket(segment: ShellSegment) -> tuple[str, int, int, int]:
    line = segment.geometry
    a = line.points[0]
    b = line.points[-1]
    angle = _angle(a, b)
    theta = math.radians(angle)
    nx = -math.sin(theta)
    ny = math.cos(theta)
    mx = (a[0] + b[0]) / 2
    my = (a[1] + b[1]) / 2
    rho = mx * nx + my * ny
    return (segment.segment_type, int(round(angle / 10.0)), int(round(rho / 38.0)), int(round(my / 70.0)))


def _shell_failure_flags(
    *,
    hull_polygon: list[tuple[float, float]],
    roof_segments: list[ShellSegment],
    base_segments: list[ShellSegment],
    facade_corner_segments: list[ShellSegment],
    visible_segments: list[ShellSegment],
    completed_segments: list[ShellSegment],
) -> list[str]:
    flags: list[str] = []
    if len(hull_polygon) < 4:
        flags.append("missing_hull_polygon")
    if not roof_segments:
        flags.append("missing_roofline")
    if not base_segments:
        flags.append("missing_base_profile")
    if not facade_corner_segments:
        flags.append("missing_facade_corner")
    if len(visible_segments) + len(completed_segments) < 4:
        flags.append("too_few_shell_segments")
    return flags


def _shell_confidence(
    *,
    shell_mask,
    bbox_xyxy: tuple[float, float, float, float],
    roof_segments: list[ShellSegment],
    base_segments: list[ShellSegment],
    facade_corner_segments: list[ShellSegment],
    failure_flags: list[str],
) -> float:
    x1, y1, x2, y2 = bbox_xyxy
    bbox_area = max(1.0, (x2 - x1) * (y2 - y1))
    mask_area_ratio = min(1.0, float(shell_mask.sum()) / bbox_area)
    confidence = (
        0.22
        + 0.18 * min(1.0, len(roof_segments) / 3.0)
        + 0.15 * min(1.0, len(base_segments) / 2.0)
        + 0.18 * min(1.0, len(facade_corner_segments) / 2.0)
        + 0.27 * mask_area_ratio
    )
    confidence -= 0.12 * len(failure_flags)
    return max(0.0, min(1.0, confidence))


def _occlusion_zones(mask) -> list[tuple[float, float, float, float]]:
    cv2 = require_module("cv2", "opencv-python-headless")
    contours, _ = cv2.findContours(mask.astype("uint8") * 255, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    zones: list[tuple[float, float, float, float]] = []
    for contour in sorted(contours, key=cv2.contourArea, reverse=True)[:12]:
        if cv2.contourArea(contour) < 80:
            continue
        x, y, w, h = cv2.boundingRect(contour)
        zones.append((float(x), float(y), float(x + w), float(y + h)))
    return zones


def _shell_evidence_ids(evidence: EvidenceInventory, facade_elements: list[FacadeElement]) -> list[str]:
    ids = [
        item.id
        for item in evidence.items
        if item.role_hint in {"shell", "structure", "plane", "negative_space", "completion_zone"}
        or item.kind in {"wall_plane", "mask_region"}
    ]
    for element in facade_elements:
        if element.element_type == "wall_plane":
            ids.append(element.id)
    return sorted(set(ids))[:120]


def _write_shell_overlay(*, image, shell: BuildingShell, shell_mask, occluder, out_path: Path) -> Path:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")
    overlay = np.array(image.convert("RGB"))
    mask_layer = overlay.copy()
    mask_layer[shell_mask] = (60, 95, 150)
    mask_layer[occluder] = (170, 50, 50)
    overlay = cv2.addWeighted(overlay, 0.66, mask_layer, 0.34, 0)

    def draw_segments(segments: list[ShellSegment], color: tuple[int, int, int], thickness: int = 3) -> None:
        for segment in segments:
            pts = [(int(round(x)), int(round(y))) for x, y in segment.geometry.points]
            for a, b in zip(pts, pts[1:]):
                cv2.line(overlay, a, b, color, thickness, cv2.LINE_AA)

    draw_segments(shell.visible_hull_segments, (245, 245, 245), 3)
    draw_segments(shell.completed_hull_segments, (255, 210, 80), 2)
    draw_segments(shell.roof_segments, (80, 240, 255), 3)
    draw_segments(shell.base_segments, (120, 255, 140), 3)
    draw_segments(shell.facade_corner_segments, (255, 160, 80), 3)
    x1, y1, x2, y2 = [int(round(v)) for v in shell.bbox_xyxy]
    cv2.rectangle(overlay, (x1, y1), (x2, y2), (180, 180, 255), 2, cv2.LINE_AA)
    cv2.putText(
        overlay,
        f"BuildingShell {shell.shell_confidence:.2f}",
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


def _segment_type(line: LinePrimitive, bbox_xyxy: tuple[float, float, float, float]) -> str:
    a = line.points[0]
    b = line.points[-1]
    angle = _angle(a, b)
    y_mid = (a[1] + b[1]) / 2
    x1, y1, x2, y2 = bbox_xyxy
    height = max(1.0, y2 - y1)
    horizontal = angle <= 12.0 or angle >= 168.0
    vertical = 78.0 <= angle <= 102.0
    shallow = horizontal or 12.0 < angle <= 43.0 or 137.0 <= angle < 168.0
    if shallow and y_mid <= y1 + height * 0.44:
        return "roofline"
    if horizontal and y_mid >= y1 + height * 0.56:
        return "base"
    if vertical and line.length >= height * 0.16:
        return "facade_corner"
    return "shell_edge"


def _line_role_for_segment(a: tuple[float, float], b: tuple[float, float], bbox_xyxy: tuple[float, float, float, float]) -> str:
    segment_type = _segment_type(LinePrimitive([a, b]), bbox_xyxy)
    if segment_type == "roofline":
        return "roofline"
    return "silhouette" if segment_type == "shell_edge" else "structure"


def _sample_points(line: LinePrimitive) -> list[tuple[int, int]]:
    points: list[tuple[int, int]] = []
    for a, b in zip(line.points, line.points[1:]):
        ax, ay = a
        bx, by = b
        steps = max(2, int(_distance(a, b) / 8.0))
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
    mx = sum(x for x, _ in line.points) / len(line.points)
    my = sum(y for _, y in line.points) / len(line.points)
    return x1 - margin <= mx <= x2 + margin and y1 - margin <= my <= y2 + margin


def _line_mid_y(line: LinePrimitive) -> float:
    return sum(y for _, y in line.points) / max(1, len(line.points))


def _bbox_from_points(points: list[tuple[float, float]]) -> tuple[float, float, float, float]:
    xs = [x for x, _ in points]
    ys = [y for _, y in points]
    return min(xs), min(ys), max(xs), max(ys)


def _angle(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.degrees(math.atan2(b[1] - a[1], b[0] - a[0])) % 180.0


def _distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(b[0] - a[0], b[1] - a[1])
