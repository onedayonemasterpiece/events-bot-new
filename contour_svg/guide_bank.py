from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .dependencies import require_module


@dataclass(frozen=True)
class GuideBankConfig:
    source_image: Path
    out_dir: Path
    output_size: tuple[int, int] = (768, 768)


@dataclass(frozen=True)
class GuideBankResult:
    source_image: Path
    source_crop: Path
    object_mask: Path
    occluder_mask: Path
    guides: dict[str, Path]
    composite_guides: dict[str, Path]
    edge_mask: Path
    report_path: Path

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_image": str(self.source_image),
            "source_crop": str(self.source_crop),
            "object_mask": str(self.object_mask),
            "occluder_mask": str(self.occluder_mask),
            "edge_mask": str(self.edge_mask),
            "guides": {key: str(path) for key, path in self.guides.items()},
            "composite_guides": {key: str(path) for key, path in self.composite_guides.items()},
            "report_path": str(self.report_path),
        }


def build_guide_bank(config: GuideBankConfig) -> GuideBankResult:
    """Build the v0.2 guide bank from a source photo.

    The guides are saved as canonical black lines on a white background for
    ControlNet conditioning. Internally line masks use 255=line, 0=background.
    """

    Image = require_module("PIL.Image", "Pillow")
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")

    source_dir = config.out_dir / "source"
    guides_dir = config.out_dir / "guides"
    composite_dir = config.out_dir / "composite_guides"
    for directory in (source_dir, guides_dir, composite_dir):
        directory.mkdir(parents=True, exist_ok=True)

    source = Image.open(config.source_image).convert("RGB")
    source_crop = _fit_pad(source, size=config.output_size, fill=(255, 255, 255))
    source_crop_path = source_dir / "source_crop.png"
    source_crop.save(source_crop_path)

    rgb = np.array(source_crop)
    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
    blur = cv2.GaussianBlur(clahe, (3, 3), 0)
    raw_edges = cv2.Canny(blur, 45, 135)
    low_edges = cv2.Canny(blur, 24, 88)

    object_mask, object_meta = _estimate_object_mask(raw_edges, low_edges, rgb)
    occluder_mask, occluder_meta = _estimate_occluder_mask(rgb)

    object_mask_path = source_dir / "object_mask.png"
    occluder_mask_path = source_dir / "occluder_mask.png"
    _save_mask(object_mask, object_mask_path)
    _save_mask(occluder_mask, occluder_mask_path)
    # Compatibility aliases for consumers that expect the full pipeline names.
    _save_mask(object_mask, config.out_dir / "mask_object_visible.png")
    _save_mask(occluder_mask, config.out_dir / "mask_occluder.png")
    shutil = require_module("shutil", "shutil")
    shutil.copy2(source_crop_path, config.out_dir / "input_normalized.png")

    g1 = _binary(raw_edges)
    g2 = _binary(raw_edges)
    g3 = cv2.dilate(g2, _kernel(2), iterations=1)
    g3 = cv2.morphologyEx(g3, cv2.MORPH_CLOSE, _kernel(2), iterations=1)
    g4 = _remove_small_components(g3, min_area=14, min_side=2)
    g5 = _and(g4, object_mask)
    g6 = _and(g5, _not(occluder_mask))
    g7 = _morph_gradient(object_mask, k=9)
    g8 = _detail_edges(g6)
    g9 = _structure_edges(g6, g7)
    g10 = _fused_guide(g7, g9, g8, occluder_mask)

    guides = {
        "G1_edge_raw": g1,
        "G2_edge_binarized": g2,
        "G3_edge_thickened": g3,
        "G4_edge_cleaned": g4,
        "G5_edge_inside_object": g5,
        "G6_edge_minus_occluders": g6,
        "G7_silhouette_outline": g7,
        "G8_detail_edge": g8,
        "G9_structure_edge": g9,
        "G10_fused_guide": g10,
    }

    cg1 = _clean_union(g7, g9, min_area=22)
    cg2 = _clean_union(g9, g8, min_area=14)
    cg3 = _fused_guide(g7, g9, g8, occluder_mask)
    cg4 = _clean_union(g7, _major_lines(g9), min_area=28)
    composites = {
        "CG1_silhouette_plus_structure": cg1,
        "CG2_structure_plus_details": cg2,
        "CG3_fused_balanced": cg3,
        "CG4_minimal_clean": cg4,
    }

    guide_paths: dict[str, Path] = {}
    for guide_id, mask in guides.items():
        path = guides_dir / f"{guide_id}.png"
        _save_control(mask, path)
        guide_paths[guide_id] = path
    composite_paths: dict[str, Path] = {}
    for guide_id, mask in composites.items():
        path = composite_dir / f"{guide_id}.png"
        _save_control(mask, path)
        composite_paths[guide_id] = path

    edge_mask_path = config.out_dir / "edge_mask.png"
    _save_control(g10, edge_mask_path)
    _save_control(g6, config.out_dir / "edge_map.png")
    _save_control(g10, config.out_dir / "egde_mask.png")

    report = {
        "source_image": str(config.source_image),
        "output_size": list(config.output_size),
        "guide_policy": "v0.2 role-separated guide bank; black lines on white",
        "object_mask": object_meta,
        "occluder_mask": occluder_meta,
        "guides": {key: str(path) for key, path in guide_paths.items()},
        "composite_guides": {key: str(path) for key, path in composite_paths.items()},
        "edge_mask": str(edge_mask_path),
    }
    report_path = config.out_dir / "guide_bank_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return GuideBankResult(
        source_image=config.source_image,
        source_crop=source_crop_path,
        object_mask=object_mask_path,
        occluder_mask=occluder_mask_path,
        guides=guide_paths,
        composite_guides=composite_paths,
        edge_mask=edge_mask_path,
        report_path=report_path,
    )


def _fit_pad(image, *, size: tuple[int, int], fill: tuple[int, int, int]):
    Image = require_module("PIL.Image", "Pillow")
    resampling = getattr(Image, "Resampling", Image)
    width, height = image.size
    target_w, target_h = size
    scale = min(target_w / max(1, width), target_h / max(1, height))
    resized = image.resize((max(1, int(round(width * scale))), max(1, int(round(height * scale)))), resampling.LANCZOS)
    out = Image.new("RGB", size, fill)
    out.paste(resized, ((target_w - resized.width) // 2, (target_h - resized.height) // 2))
    return out


def _kernel(size: int):
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    side = max(1, int(size))
    return cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (side * 2 + 1, side * 2 + 1))


def _binary(mask):
    np = require_module("numpy", "numpy")
    return np.where(mask > 0, 255, 0).astype("uint8")


def _not(mask):
    cv2 = require_module("cv2", "opencv-python-headless")
    return cv2.bitwise_not(_binary(mask))


def _and(a, b):
    cv2 = require_module("cv2", "opencv-python-headless")
    return cv2.bitwise_and(_binary(a), _binary(b))


def _or(a, b):
    cv2 = require_module("cv2", "opencv-python-headless")
    return cv2.bitwise_or(_binary(a), _binary(b))


def _estimate_object_mask(raw_edges, low_edges, rgb):
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    h, w = raw_edges.shape[:2]
    seed = cv2.dilate(low_edges, _kernel(5), iterations=2)
    seed = cv2.morphologyEx(seed, cv2.MORPH_CLOSE, _kernel(13), iterations=2)
    contours, _ = cv2.findContours(seed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    mask = np.zeros((h, w), dtype="uint8")
    cx1, cy1, cx2, cy2 = int(w * 0.18), int(h * 0.10), int(w * 0.86), int(h * 0.92)
    kept = 0
    for contour in sorted(contours, key=cv2.contourArea, reverse=True)[:12]:
        area = cv2.contourArea(contour)
        if area < h * w * 0.004:
            continue
        x, y, cw, ch = cv2.boundingRect(contour)
        overlaps_center = not (x + cw < cx1 or x > cx2 or y + ch < cy1 or y > cy2)
        if overlaps_center or area > h * w * 0.025:
            cv2.drawContours(mask, [contour], -1, 255, thickness=-1)
            kept += 1
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, _kernel(11), iterations=2)
    mask = cv2.dilate(mask, _kernel(5), iterations=1)
    if cv2.countNonZero(mask) < h * w * 0.08:
        # Research-mode guard: keep the guide experiment running but record that
        # object segmentation was weak. Final SVG pipeline still fails loudly.
        mask[:, :] = 255
        method = "weak_edges_full_canvas_research_guard"
    else:
        method = "edge_components_center_weighted"
    return mask, {"method": method, "kept_components": kept, "coverage": round(float(cv2.countNonZero(mask)) / float(h * w), 5)}


def _estimate_occluder_mask(rgb):
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    hsv = cv2.cvtColor(rgb, cv2.COLOR_RGB2HSV)
    h, s, v = cv2.split(hsv)
    green = ((h >= 25) & (h <= 100) & (s >= 35) & (v >= 28)).astype("uint8") * 255
    # Very dark, high-saturation areas often correspond to leaf clusters in the
    # tower samples; keep them low-weight and clean aggressively.
    dark_sat = ((s >= 45) & (v <= 85)).astype("uint8") * 255
    mask = cv2.bitwise_or(green, dark_sat)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, _kernel(2), iterations=1)
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, _kernel(5), iterations=1)
    mask = _remove_small_components(mask, min_area=80, min_side=4)
    return mask, {"method": "hsv_green_dark_saturation", "coverage": round(float(cv2.countNonZero(mask)) / float(mask.size), 5)}


def _morph_gradient(mask, *, k: int):
    cv2 = require_module("cv2", "opencv-python-headless")
    side = max(1, int(k))
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (side, side))
    return cv2.morphologyEx(_binary(mask), cv2.MORPH_GRADIENT, kernel)


def _remove_small_components(mask, *, min_area: int, min_side: int = 1):
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    binary = _binary(mask)
    labels_count, labels, stats, _ = cv2.connectedComponentsWithStats(binary, connectivity=8)
    out = np.zeros_like(binary)
    for label in range(1, labels_count):
        x, y, w, h, area = stats[label]
        if area >= min_area and max(w, h) >= min_side:
            out[labels == label] = 255
    return out


def _major_lines(mask):
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    binary = _binary(mask)
    labels_count, labels, stats, _ = cv2.connectedComponentsWithStats(binary, connectivity=8)
    out = np.zeros_like(binary)
    for label in range(1, labels_count):
        x, y, w, h, area = stats[label]
        if area >= 45 or max(w, h) >= 34:
            out[labels == label] = 255
    return out


def _detail_edges(mask):
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    binary = _remove_small_components(mask, min_area=8, min_side=3)
    labels_count, labels, stats, _ = cv2.connectedComponentsWithStats(binary, connectivity=8)
    out = np.zeros_like(binary)
    for label in range(1, labels_count):
        x, y, w, h, area = stats[label]
        density = area / max(1, w * h)
        if area <= 900 and max(w, h) <= 180 and density <= 0.45:
            out[labels == label] = 255
    return out


def _structure_edges(mask, silhouette):
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    binary = _binary(mask)
    out = np.zeros_like(binary)
    lines = cv2.HoughLinesP(binary, 1, np.pi / 180, threshold=38, minLineLength=34, maxLineGap=8)
    if lines is not None:
        for line in lines[:, 0, :]:
            x1, y1, x2, y2 = [int(v) for v in line]
            cv2.line(out, (x1, y1), (x2, y2), 255, 2)
    out = _or(out, _major_lines(binary))
    out = _or(out, silhouette)
    return _remove_small_components(out, min_area=18, min_side=4)


def _clean_union(*masks, min_area: int):
    cv2 = require_module("cv2", "opencv-python-headless")
    if not masks:
        raise ValueError("at least one mask required")
    out = _binary(masks[0])
    for mask in masks[1:]:
        out = _or(out, mask)
    out = cv2.morphologyEx(out, cv2.MORPH_CLOSE, _kernel(1), iterations=1)
    return _remove_small_components(out, min_area=min_area, min_side=3)


def _fused_guide(silhouette, structure, detail, occluder):
    fused = _clean_union(silhouette, structure, detail, min_area=14)
    fused = _and(fused, _not(occluder))
    return _remove_small_components(fused, min_area=18, min_side=3)


def _save_mask(mask, path: Path) -> None:
    Image = require_module("PIL.Image", "Pillow")
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(_binary(mask)).save(path)


def _save_control(mask, path: Path) -> None:
    Image = require_module("PIL.Image", "Pillow")
    np = require_module("numpy", "numpy")
    path.parent.mkdir(parents=True, exist_ok=True)
    control = np.where(_binary(mask) > 0, 0, 255).astype("uint8")
    Image.fromarray(control).convert("RGB").save(path)
