from __future__ import annotations

from pathlib import Path

from .contracts import MaskBundle
from .dependencies import require_module


def combine_masks(masks: list, size: tuple[int, int]):
    Image = require_module("PIL.Image", "Pillow")
    np = require_module("numpy", "numpy")
    if not masks:
        raise RuntimeError("combine_masks requires at least one neural mask")
    arr = np.zeros((size[1], size[0]), dtype=np.uint8)
    for mask in masks:
        arr = np.maximum(arr, np.array(mask.resize(size).convert("L")))
    return Image.fromarray(arr)


def build_mask_bundle(
    *,
    primary_mask_path: str | Path,
    occluder_mask_path: str | Path | None,
    out_dir: str | Path,
) -> MaskBundle:
    Image = require_module("PIL.Image", "Pillow")
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")

    debug = Path(out_dir) / "debug"
    debug.mkdir(parents=True, exist_ok=True)

    primary_img = Image.open(primary_mask_path).convert("L")
    primary = np.array(primary_img)
    if occluder_mask_path:
        occluder = np.array(Image.open(occluder_mask_path).convert("L").resize(primary_img.size))
    else:
        occluder = np.zeros_like(primary)

    object_raw = primary > 32
    occluder_raw = occluder > 32
    kernel = np.ones((15, 15), dtype=np.uint8)
    object_dilated = cv2.dilate(object_raw.astype("uint8"), kernel, iterations=1) > 0

    object_visible = object_raw & ~occluder_raw
    object_unknown = object_dilated & occluder_raw
    background = ~object_raw & ~occluder_raw
    allowed_line_region = object_visible | object_unknown

    warnings: list[str] = []
    object_area = int(object_raw.sum())
    visible_area = int(object_visible.sum())
    if object_area <= 0:
        warnings.append("empty_primary_object_mask")
    elif visible_area / max(1, object_area) < 0.25:
        warnings.append("primary_object_mostly_occluded")

    def save(mask, name: str) -> Path:
        path = debug / name
        Image.fromarray((mask.astype("uint8") * 255)).save(path)
        return path

    object_visible_path = save(object_visible, "mask_object_visible.png")
    occluder_path = save(occluder_raw, "mask_occluder.png") if occluder_mask_path else None
    background_path = save(background, "mask_background.png")
    object_unknown_path = save(object_unknown, "mask_object_unknown.png")
    allowed_path = save(allowed_line_region, "mask_allowed_line_region.png")

    overlay = np.zeros((*primary.shape, 3), dtype=np.uint8)
    overlay[background] = (24, 24, 24)
    overlay[object_visible] = (80, 150, 255)
    overlay[occluder_raw] = (255, 60, 70)
    overlay[object_unknown] = (255, 210, 60)
    overlay_path = debug / "masks_multistate_overlay.png"
    Image.fromarray(overlay).save(overlay_path)

    return MaskBundle(
        object_visible=object_visible_path,
        occluder=occluder_path,
        background=background_path,
        object_unknown=object_unknown_path,
        allowed_line_region=allowed_path,
        overlay=overlay_path,
        warnings=warnings,
    )
