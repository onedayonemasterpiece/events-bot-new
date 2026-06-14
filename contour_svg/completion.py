from __future__ import annotations

import json
import math
from pathlib import Path

from .contracts import CompletionProposal, FacadeElement, LineGroup, LinePrimitive, MaskBundle
from .dependencies import require_module


def build_completion_proposals(
    groups: list[LineGroup],
    elements: list[FacadeElement],
    masks: MaskBundle,
    out_dir: str | Path,
) -> tuple[list[CompletionProposal], list[str]]:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")
    warnings: list[str] = []
    debug = Path(out_dir) / "debug"
    debug.mkdir(parents=True, exist_ok=True)

    unknown = np.array(Image.open(masks.object_unknown).convert("L")) > 32
    allowed = np.array(Image.open(masks.allowed_line_region).convert("L")) > 32
    h, w = unknown.shape[:2]
    proposals: list[CompletionProposal] = []
    proposals.extend(_line_gap_proposals(groups, unknown, allowed, w, h))
    proposals.extend(_repeated_element_proposals(elements, unknown, allowed, w, h))
    proposals = _dedupe_proposals(proposals)

    overlay = np.zeros((h, w, 3), dtype=np.uint8)
    overlay[allowed] = (40, 40, 40)
    overlay[unknown] = (80, 80, 120)
    for proposal in proposals:
        pts = [(int(x), int(y)) for x, y in proposal.geometry.points]
        color = (255, 255, 255) if proposal.accepted else (80, 80, 255)
        for a, b in zip(pts, pts[1:]):
            cv2.line(overlay, a, b, color, 2, cv2.LINE_AA)
        if pts:
            cv2.putText(overlay, proposal.completion_type[:12], pts[0], cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 220, 80), 1, cv2.LINE_AA)

    Image.fromarray(overlay).save(debug / "completion_overlay.png")
    (debug / "completion_proposals.json").write_text(
        json.dumps([proposal.to_dict() for proposal in proposals], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return proposals, warnings


def _line_gap_proposals(groups: list[LineGroup], unknown, allowed, width: int, height: int) -> list[CompletionProposal]:
    candidates = [
        group
        for group in groups
        if group.decision in {"keep", "candidate"}
        and group.semantic_label in {"roof_edge", "cornice_band", "base_or_step", "facade_edge"}
        and group.merged_geometry.length >= (45 if group.semantic_label != "cornice_band" else 28)
        and group.occluder_overlap <= 0.08
    ]
    out: list[CompletionProposal] = []
    for left_idx, left in enumerate(candidates):
        for right in candidates[left_idx + 1 :]:
            if left.semantic_label != right.semantic_label:
                continue
            if abs(_angle(left.merged_geometry) - _angle(right.merged_geometry)) > 7.0:
                continue
            line = _bridge_if_close(left.merged_geometry, right.merged_geometry, max_gap=92.0)
            if line is None:
                continue
            ratio = _mask_ratio(line.points, unknown | allowed, width, height)
            if ratio < 0.45:
                continue
            out.append(
                CompletionProposal(
                    id=f"C_gap_{len(out):03d}",
                    completion_type=f"{left.semantic_label}_gap",
                    geometry=line,
                    confidence=0.42,
                    source_group_ids=[left.id, right.id],
                    evidence=["completion_evidence:two_sided_line_gap", "perspective_evidence:angle_match"],
                )
            )
    return out[:14]


def _repeated_element_proposals(
    elements: list[FacadeElement],
    unknown,
    allowed,
    width: int,
    height: int,
) -> list[CompletionProposal]:
    visible_windows = [
        e for e in elements if e.element_type in {"window", "door"} and e.completion_status == "visible"
    ]
    if len(visible_windows) < 2:
        return []
    visible_windows.sort(key=lambda e: (_center(e.bbox_xyxy)[1], _center(e.bbox_xyxy)[0]))
    out: list[CompletionProposal] = []
    for a, b in zip(visible_windows, visible_windows[1:]):
        ax, ay = _center(a.bbox_xyxy)
        bx, by = _center(b.bbox_xyxy)
        if abs(ay - by) > max(24.0, (a.bbox_xyxy[3] - a.bbox_xyxy[1]) * 0.7):
            continue
        gap = bx - ax
        if not (45.0 <= gap <= 220.0):
            continue
        candidate_center = (ax - gap, ay)
        if not _point_in_mask(candidate_center, unknown | allowed, width, height):
            continue
        bw = ((a.bbox_xyxy[2] - a.bbox_xyxy[0]) + (b.bbox_xyxy[2] - b.bbox_xyxy[0])) / 2
        bh = ((a.bbox_xyxy[3] - a.bbox_xyxy[1]) + (b.bbox_xyxy[3] - b.bbox_xyxy[1])) / 2
        x1 = candidate_center[0] - bw / 2
        y1 = candidate_center[1] - bh / 2
        x2 = candidate_center[0] + bw / 2
        y2 = candidate_center[1] + bh / 2
        if x1 < 0 or y1 < 0 or x2 > width or y2 > height:
            continue
        pts = [(x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)]
        out.append(
            CompletionProposal(
                id=f"C_repeated_window_{len(out):03d}",
                completion_type="repeated_window_placeholder",
                geometry=LinePrimitive(pts, role="structure", priority=4, source="completion", confidence=0.38),
                confidence=0.38,
                source_element_ids=[a.id, b.id],
                evidence=["completion_evidence:two_confirmed_neighbors", "repetition_evidence:window_row"],
            )
        )
    return out[:12]


def _bridge_if_close(left: LinePrimitive, right: LinePrimitive, *, max_gap: float) -> LinePrimitive | None:
    endpoints = [(left.points[0], right.points[0]), (left.points[0], right.points[-1]), (left.points[-1], right.points[0]), (left.points[-1], right.points[-1])]
    a, b = min(endpoints, key=lambda pair: _distance(pair[0], pair[1]))
    gap = _distance(a, b)
    if gap <= 8.0 or gap > max_gap:
        return None
    role = "roofline" if left.role == "roofline" or right.role == "roofline" else "structure"
    return LinePrimitive([a, b], role=role, priority=4, source="completion", confidence=0.42)


def _mask_ratio(points: list[tuple[float, float]], mask, width: int, height: int) -> float:
    samples: list[tuple[int, int]] = []
    for a, b in zip(points, points[1:]):
        ax, ay = a
        bx, by = b
        steps = max(2, int(_distance(a, b) / 6))
        for idx in range(steps + 1):
            t = idx / steps
            samples.append((int(round(ax + (bx - ax) * t)), int(round(ay + (by - ay) * t))))
    if not samples:
        return 0.0
    inside = 0
    for x, y in samples:
        if 0 <= x < width and 0 <= y < height and bool(mask[y, x]):
            inside += 1
    return inside / len(samples)


def _point_in_mask(point: tuple[float, float], mask, width: int, height: int) -> bool:
    x, y = int(round(point[0])), int(round(point[1]))
    return 0 <= x < width and 0 <= y < height and bool(mask[y, x])


def _dedupe_proposals(proposals: list[CompletionProposal]) -> list[CompletionProposal]:
    seen: set[tuple[int, int, int, int, str]] = set()
    endpoint_counts: dict[tuple[str, int, int], int] = {}
    out: list[CompletionProposal] = []
    proposals = sorted(proposals, key=lambda proposal: (proposal.completion_type, proposal.geometry.length))
    for proposal in proposals:
        pts = proposal.geometry.points
        if len(pts) < 2:
            continue
        x1, y1 = pts[0]
        x2, y2 = pts[-1]
        if proposal.completion_type.endswith("_gap") and proposal.geometry.length > 96.0:
            continue
        key = (int(round(x1 / 10)), int(round(y1 / 10)), int(round(x2 / 10)), int(round(y2 / 10)), proposal.completion_type)
        if key in seen:
            continue
        endpoint_key_a = (proposal.completion_type, int(round(x1 / 28)), int(round(y1 / 28)))
        endpoint_key_b = (proposal.completion_type, int(round(x2 / 28)), int(round(y2 / 28)))
        if endpoint_counts.get(endpoint_key_a, 0) >= 2 or endpoint_counts.get(endpoint_key_b, 0) >= 2:
            continue
        seen.add(key)
        endpoint_counts[endpoint_key_a] = endpoint_counts.get(endpoint_key_a, 0) + 1
        endpoint_counts[endpoint_key_b] = endpoint_counts.get(endpoint_key_b, 0) + 1
        out.append(proposal)
    return out


def _angle(line: LinePrimitive) -> float:
    x1, y1 = line.points[0]
    x2, y2 = line.points[-1]
    return math.degrees(math.atan2(y2 - y1, x2 - x1)) % 180.0


def _center(box: tuple[float, float, float, float]) -> tuple[float, float]:
    x1, y1, x2, y2 = box
    return ((x1 + x2) / 2, (y1 + y2) / 2)


def _distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(b[0] - a[0], b[1] - a[1])
