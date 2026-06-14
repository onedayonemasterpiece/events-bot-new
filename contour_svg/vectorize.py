from __future__ import annotations

import math
from pathlib import Path

from .config import RunConfig
from .contracts import Candidate, LinePrimitive
from .dependencies import MissingDependencyError, optional_import


ROLE_ORDER = {
    "silhouette": 0,
    "roofline": 1,
    "structure": 2,
    "arc": 3,
    "accent": 4,
    "diffusion_lineart": 5,
}

ROLE_CAPS = {
    "silhouette": 6,
    "roofline": 40,
    "structure": 120,
    "arc": 55,
    "accent": 35,
}


def guide_lines_to_candidate(
    lines: list[LinePrimitive],
    *,
    candidate_id: str,
    variant: str,
    family: str,
    config: RunConfig,
) -> Candidate:
    ordered = sorted(
        [line for line in lines if line.length >= config.style.min_line_length],
        key=lambda line: (ROLE_ORDER.get(line.role, 9), -line.priority, -line.length),
    )
    selected: list[LinePrimitive] = []
    role_counts: dict[str, int] = {}
    for line in ordered:
        cap = ROLE_CAPS.get(line.role, config.geometry.max_paths_per_candidate)
        count = role_counts.get(line.role, 0)
        if count >= cap:
            continue
        selected.append(line)
        role_counts[line.role] = count + 1
        if len(selected) >= config.geometry.max_paths_per_candidate:
            break
    candidate = Candidate(
        candidate_id=candidate_id,
        variant=variant,
        family=family,
        lines=_dedupe_lines(selected)[: config.geometry.max_paths_per_candidate],
        final_eligible=False,
        primitive_rendered=False,
        proposal_only=True,
    )
    candidate.parameters["coarse_to_fine"] = True
    candidate.parameters["stroke_width_scale"] = 0.62
    candidate.parameters["silhouette_paths"] = sum(1 for line in candidate.lines if line.role == "silhouette")
    candidate.parameters["roofline_paths"] = sum(1 for line in candidate.lines if line.role == "roofline")
    return candidate


def hybrid_architecture_to_candidate(
    lines: list[LinePrimitive],
    detail_raster_path: Path,
    *,
    candidate_id: str,
    variant: str,
    family: str,
    config: RunConfig,
) -> Candidate:
    coarse = _merged_architecture_lines(lines, config)
    detail_config = _detail_config(config, max_paths=170)
    details = raster_to_candidate(
        detail_raster_path,
        candidate_id=f"{candidate_id}_detail_source",
        variant=variant,
        family=family,
        config=detail_config,
    )
    detail_lines: list[LinePrimitive] = []
    for line in details.lines:
        if line.length < config.style.min_line_length:
            continue
        # Keep detailed guide strokes thin and visually secondary.
        detail_lines.append(
            LinePrimitive(
                points=line.points,
                role="accent",
                priority=3,
                source="mlsd_detail",
                confidence=line.confidence,
            )
        )
    combined = _dedupe_lines([*coarse, *detail_lines])
    candidate = Candidate(
        candidate_id=candidate_id,
        variant=variant,
        family=family,
        lines=combined[: config.geometry.max_paths_per_candidate],
        raster_path=detail_raster_path,
        final_eligible=False,
        primitive_rendered=False,
        proposal_only=True,
    )
    candidate.parameters["coarse_to_fine"] = True
    candidate.parameters["hybrid_architecture"] = True
    candidate.parameters["stroke_width_scale"] = 0.52
    candidate.parameters["coarse_paths"] = len(coarse)
    candidate.parameters["detail_paths"] = min(len(detail_lines), max(0, config.geometry.max_paths_per_candidate - len(coarse)))
    candidate.parameters["silhouette_paths"] = sum(1 for line in candidate.lines if line.role == "silhouette")
    candidate.parameters["roofline_paths"] = sum(1 for line in candidate.lines if line.role == "roofline")
    return candidate


def raster_to_candidate(raster_path: Path, *, candidate_id: str, variant: str, family: str, config: RunConfig) -> Candidate:
    np = optional_import("numpy")
    cv2 = optional_import("cv2")
    Image = optional_import("PIL.Image")
    sk_morph = optional_import("skimage.morphology")
    if np is None or cv2 is None or Image is None:
        raise MissingDependencyError("Raster vectorization requires numpy, Pillow and opencv-python-headless")
    img = Image.open(raster_path).convert("L")
    arr = np.array(img)
    # ControlNet line-art checkpoints commonly emit dark strokes on a light
    # canvas, while extracted guide rasters are white strokes on black.
    threshold_mode = cv2.THRESH_BINARY_INV if float(arr.mean()) > 127.0 else cv2.THRESH_BINARY
    _, binary = cv2.threshold(arr, 210, 255, threshold_mode)
    if sk_morph is not None:
        skeleton = sk_morph.skeletonize(binary > 0)
        binary = (skeleton.astype("uint8") * 255)
    contours, _ = cv2.findContours(binary, cv2.RETR_LIST, cv2.CHAIN_APPROX_NONE)
    lines: list[LinePrimitive] = []
    for contour in contours[: config.geometry.max_paths_per_candidate * 3]:
        if cv2.arcLength(contour, False) < config.style.min_line_length:
            continue
        approx = cv2.approxPolyDP(contour, config.geometry.simplify_tolerance, False)
        pts = [(float(x), float(y)) for [[x, y]] in approx]
        if len(pts) >= 2:
            lines.append(LinePrimitive(pts, role="diffusion_lineart", priority=5, source=variant.lower(), confidence=0.55))
    lines = _dedupe_lines(lines)
    candidate = Candidate(
        candidate_id=candidate_id,
        variant=variant,
        family=family,
        lines=lines[: config.geometry.max_paths_per_candidate],
        raster_path=raster_path,
        final_eligible=False,
        primitive_rendered=False,
        proposal_only=True,
    )
    if variant == "B2":
        candidate.parameters["stroke_width_scale"] = 0.48
    return candidate


def _detail_config(config: RunConfig, *, max_paths: int) -> RunConfig:
    from dataclasses import replace

    return replace(
        config,
        geometry=replace(config.geometry, max_paths_per_candidate=max_paths),
    )


def _merged_architecture_lines(lines: list[LinePrimitive], config: RunConfig) -> list[LinePrimitive]:
    raw: list[LinePrimitive] = []
    for line in lines:
        if len(line.points) != 2 or line.role not in {"roofline", "structure"}:
            continue
        if line.length < max(50.0, config.style.min_line_length * 2.8):
            continue
        angle = _line_angle_deg(line)
        role = _architecture_role(line, angle)
        if role is None:
            continue
        if not _within_architecture_length_budget(line, angle, role):
            continue
        raw.append(LinePrimitive(line.points, role=role, priority=_role_priority(role), source=f"merged_{line.source}", confidence=line.confidence))
    merged = _thin_collinear(raw)
    merged.sort(key=lambda line: (ROLE_ORDER.get(line.role, 9), -line.priority, -line.length))
    selected: list[LinePrimitive] = []
    caps = {"roofline": 24, "structure": 52, "silhouette": 18}
    counts: dict[str, int] = {}
    for line in merged:
        count = counts.get(line.role, 0)
        if count >= caps.get(line.role, 40):
            continue
        selected.append(line)
        counts[line.role] = count + 1
    return selected


def _within_architecture_length_budget(line: LinePrimitive, angle: float, role: str) -> bool:
    if role == "roofline":
        return line.length <= 310.0
    vertical = 78.0 <= angle <= 102.0
    if vertical:
        return line.length <= 270.0
    return line.length <= 340.0


def _line_angle_deg(line: LinePrimitive) -> float:
    (x1, y1), (x2, y2) = line.points
    angle = math.degrees(math.atan2(y2 - y1, x2 - x1))
    return angle % 180.0


def _architecture_role(line: LinePrimitive, angle: float) -> str | None:
    ys = [p[1] for p in line.points]
    y_mid = sum(ys) / len(ys)
    horizontal = angle <= 12.0 or angle >= 168.0
    vertical = 78.0 <= angle <= 102.0
    shallow_roof = 12.0 < angle <= 36.0 or 144.0 <= angle < 168.0
    steep_roof = 36.0 < angle <= 64.0 or 116.0 <= angle < 144.0
    if horizontal or vertical:
        return "structure"
    if shallow_roof and y_mid < 370.0:
        return "roofline"
    if steep_roof and y_mid < 300.0:
        return "roofline"
    return None


def _role_priority(role: str) -> int:
    if role == "silhouette":
        return 10
    if role == "roofline":
        return 8
    return 6


def _thin_collinear(lines: list[LinePrimitive]) -> list[LinePrimitive]:
    clusters: dict[tuple[str, int, int], LinePrimitive] = {}
    for line in lines:
        angle = _line_angle_deg(line)
        theta = math.radians(angle)
        nx = -math.sin(theta)
        ny = math.cos(theta)
        rho = sum((x * nx + y * ny) for x, y in line.points) / len(line.points)
        angle_bucket = int(round(angle / 4.0))
        rho_bucket = int(round(rho / 10.0))
        key = (line.role, angle_bucket, rho_bucket)
        existing = clusters.get(key)
        if existing is None or line.length > existing.length:
            clusters[key] = line
    return _dedupe_lines(list(clusters.values()))


def _dedupe_lines(lines: list[LinePrimitive]) -> list[LinePrimitive]:
    seen: set[tuple[int, int, int, int, str]] = set()
    out: list[LinePrimitive] = []
    for line in lines:
        if len(line.points) < 2:
            continue
        a, b = line.points[0], line.points[-1]
        key = (round(a[0] / 4), round(a[1] / 4), round(b[0] / 4), round(b[1] / 4), line.role)
        rev = (key[2], key[3], key[0], key[1], key[4])
        if key in seen or rev in seen:
            continue
        seen.add(key)
        out.append(line)
    return out
