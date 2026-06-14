from __future__ import annotations

import json
from pathlib import Path

from .config import RunConfig
from .contracts import FacadeElement, MaskBundle
from .dependencies import MissingDependencyError, has_cuda, require_module


FACADE_CLASS_ALIASES = {
    "facade": "wall_plane",
    "molding": "molding",
    "cornice": "cornice",
    "pillar": "pilaster",
    "window": "window",
    "door": "door",
    "sill": "sill",
    "blind": "window_blind",
    "balcony": "balcony",
    "shop": "shop",
    "deco": "deco",
}

ELEMENT_CLASSES = {
    "window",
    "door",
    "balcony",
    "pilaster",
    "cornice",
    "molding",
    "sill",
    "wall_plane",
}


def parse_facade_elements(
    image,
    masks: MaskBundle,
    out_dir: str | Path,
    config: RunConfig,
) -> tuple[list[FacadeElement], list[str]]:
    warnings: list[str] = []
    if not has_cuda():
        raise MissingDependencyError("CMP Facade SegFormer requires CUDA/GPU for contour_svg v0.3")
    torch = require_module("torch", "torch")
    transformers = require_module("transformers", "transformers")
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")

    debug = Path(out_dir) / "debug"
    class_dir = debug / "facade_classes"
    class_dir.mkdir(parents=True, exist_ok=True)

    model_id = "Xpitfire/segformer-finetuned-segments-cmp-facade"
    pipeline_kwargs = {"model": model_id, "device": 0}
    if config.runtime.dtype == "float16":
        pipeline_kwargs["torch_dtype"] = torch.float16
    pipe = transformers.pipeline("image-segmentation", **pipeline_kwargs)
    results = pipe(image)
    if not isinstance(results, list) or not results:
        raise RuntimeError("CMP Facade SegFormer produced no segmentation masks")

    object_visible = np.array(Image.open(masks.object_visible).convert("L")) > 32
    occluder = np.array(Image.open(masks.occluder).convert("L")) > 32 if masks.occluder else np.zeros_like(object_visible)
    h, w = object_visible.shape[:2]
    elements: list[FacadeElement] = []
    overlay = np.array(image.convert("RGB").resize((w, h)))
    palette = {
        "window": (80, 220, 255),
        "door": (80, 255, 140),
        "balcony": (255, 230, 80),
        "pilaster": (255, 140, 80),
        "cornice": (255, 255, 255),
        "molding": (220, 180, 255),
        "sill": (170, 255, 220),
        "wall_plane": (100, 140, 255),
    }

    for result in results:
        raw_label = str(result.get("label") or result.get("class") or "").strip().lower()
        element_type = FACADE_CLASS_ALIASES.get(raw_label, raw_label)
        if element_type not in ELEMENT_CLASSES:
            continue
        raw_mask = result.get("mask")
        if raw_mask is None:
            continue
        mask = np.array(raw_mask.convert("L").resize((w, h))) > 32
        mask &= object_visible
        mask &= ~occluder
        if not bool(mask.any()):
            continue
        mask_path = class_dir / f"{element_type}.png"
        Image.fromarray((mask.astype("uint8") * 255)).save(mask_path)
        elements.extend(
            _elements_from_binary_mask(
                mask,
                element_type=element_type,
                mask_path=mask_path,
                source="cmp_facade_segformer",
                confidence=float(result.get("score") or 0.72),
                image_area=w * h,
            )
        )

    elements = _dedupe_elements(elements)
    for element in elements:
        color = palette.get(element.element_type, (255, 255, 255))
        x1, y1, x2, y2 = [int(round(v)) for v in element.bbox_xyxy]
        cv2.rectangle(overlay, (x1, y1), (x2, y2), color, 2, cv2.LINE_AA)
        cv2.putText(overlay, element.element_type[:10], (x1, max(12, y1 - 4)), cv2.FONT_HERSHEY_SIMPLEX, 0.38, color, 1, cv2.LINE_AA)

    if not any(element.element_type in {"window", "door", "balcony", "cornice", "pilaster"} for element in elements):
        raise RuntimeError("CMP Facade SegFormer produced no drawable facade elements")

    Image.fromarray(overlay).save(debug / "elements_overlay.png")
    (debug / "facade_elements.json").write_text(
        json.dumps([element.to_dict() for element in elements], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return elements, warnings


def _elements_from_binary_mask(
    mask,
    *,
    element_type: str,
    mask_path: Path,
    source: str,
    confidence: float,
    image_area: int,
) -> list[FacadeElement]:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    num_labels, labels, stats, _centroids = cv2.connectedComponentsWithStats(mask.astype("uint8"), connectivity=8)
    out: list[FacadeElement] = []
    min_area = max(24, int(image_area * (0.00008 if element_type != "wall_plane" else 0.004)))
    max_area_ratio = 0.55 if element_type == "wall_plane" else 0.08
    for idx in range(1, num_labels):
        x, y, width, height, area = stats[idx]
        if area < min_area or area > image_area * max_area_ratio:
            continue
        if width < 4 or height < 4:
            continue
        aspect = width / max(1, height)
        if element_type in {"window", "door"} and not (0.18 <= aspect <= 3.8):
            continue
        element_id = f"E_{element_type}_{len(out):03d}_{int(x)}_{int(y)}"
        out.append(
            FacadeElement(
                id=element_id,
                element_type=element_type,
                bbox_xyxy=(float(x), float(y), float(x + width), float(y + height)),
                confidence=max(0.0, min(1.0, confidence)),
                source=source,
                evidence=["semantic_mask_evidence", f"class:{element_type}"],
                mask_path=mask_path,
            )
        )
    return out


def _dedupe_elements(elements: list[FacadeElement]) -> list[FacadeElement]:
    elements = sorted(elements, key=lambda e: (e.element_type, -e.confidence, _area(e.bbox_xyxy)))
    out: list[FacadeElement] = []
    for element in elements:
        if any(element.element_type == existing.element_type and _iou(element.bbox_xyxy, existing.bbox_xyxy) > 0.62 for existing in out):
            continue
        out.append(element)
    return out


def _area(box: tuple[float, float, float, float]) -> float:
    x1, y1, x2, y2 = box
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)


def _iou(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    inter = max(0.0, ix2 - ix1) * max(0.0, iy2 - iy1)
    return inter / max(1.0, _area(a) + _area(b) - inter)
