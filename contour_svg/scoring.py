from __future__ import annotations

from statistics import median
import math

from .contracts import Candidate
from .validators import validate_svg_file


def score_candidate(candidate: Candidate, *, expected_stroke: str) -> float:
    if not candidate.svg_path:
        candidate.failure_flags.append("missing_svg")
        candidate.accepted = False
        return 0.0
    validation = validate_svg_file(candidate.svg_path, expected_stroke=expected_stroke)
    if not validation.valid:
        candidate.failure_flags.extend(validation.flags)
        candidate.accepted = False
    lengths = [line.length for line in candidate.lines if line.length > 0]
    if not lengths:
        candidate.failure_flags.append("empty_line_set")
        candidate.accepted = False
        candidate.cv_score = 0.0
        return 0.0
    path_count = len(lengths)
    med = median(lengths)
    long_ratio = sum(l for l in lengths if l >= med * 1.35) / max(1.0, sum(lengths))
    short_noise_ratio = sum(l for l in lengths if l < 12) / max(1.0, sum(lengths))
    role_lengths = {
        role: sum(line.length for line in candidate.lines if line.role == role)
        for role in {"silhouette", "roofline", "structure", "arc"}
    }
    total_length = max(1.0, sum(lengths))
    structure_ratio = (role_lengths["silhouette"] + role_lengths["roofline"] + role_lengths["structure"]) / total_length
    diagonal_noise = sum(
        line.length
        for line in candidate.lines
        if line.role == "structure" and line.length > 135 and not _is_axis_aligned(line)
    ) / total_length
    has_silhouette = role_lengths["silhouette"] > 0
    has_roofline = role_lengths["roofline"] > 0
    global_structure_score = min(
        10.0,
        (4.0 if has_silhouette else 0.0)
        + (2.0 if has_roofline else 0.0)
        + min(4.0, structure_ratio * 6.0),
    )
    candidate.parameters["global_structure_score"] = round(global_structure_score, 3)
    if candidate.final_eligible:
        if not candidate.primitive_rendered:
            candidate.failure_flags.append("final_requires_primitive_renderer")
            candidate.accepted = False
        if candidate.raster_path is not None:
            candidate.failure_flags.append("final_rejects_raster_derived_candidate")
            candidate.accepted = False
        if not has_silhouette:
            candidate.failure_flags.append("missing_global_silhouette")
            candidate.accepted = False
        if global_structure_score < 3.0:
            candidate.failure_flags.append("weak_global_structure")
            candidate.accepted = False
        if diagonal_noise > 0.16:
            candidate.failure_flags.append("cross_face_diagonal_noise")
            candidate.accepted = False
    target_path_count = {
        "SHELL_ONLY": 30,
        "PLANE_SCAFFOLD": 48,
        "FEATURE_SCAFFOLD": 82,
    }.get(candidate.family, 92)
    count_band = {
        "SHELL_ONLY": 9.0,
        "PLANE_SCAFFOLD": 14.0,
        "FEATURE_SCAFFOLD": 18.0,
    }.get(candidate.family, 20.0)
    count_score = max(0.0, 10.0 - abs(path_count - target_path_count) / count_band)
    line_economy = max(0.0, 10.0 * long_ratio - 8.0 * short_noise_ratio)
    diagonal_score = max(0.0, 10.0 - diagonal_noise * 38.0)
    svg_score = 10.0 if validation.valid else max(0.0, 7.0 - len(validation.flags))
    candidate.parameters["diagonal_noise_ratio"] = round(diagonal_noise, 4)
    candidate.cv_score = round(
        max(
            0.0,
            min(
                10.0,
                0.25 * count_score
                + 0.20 * line_economy
                + 0.20 * svg_score
                + 0.20 * global_structure_score
                + 0.15 * diagonal_score,
            ),
        ),
        3,
    )
    return candidate.cv_score


def _is_axis_aligned(line) -> bool:
    if len(line.points) < 2:
        return True
    x1, y1 = line.points[0]
    x2, y2 = line.points[-1]
    angle = math.degrees(math.atan2(y2 - y1, x2 - x1)) % 180.0
    horizontal = angle <= 12.0 or angle >= 168.0
    vertical = 78.0 <= angle <= 102.0
    return horizontal or vertical
