from __future__ import annotations

from pathlib import Path

from .config import RunConfig
from .contracts import BoundingBox, MaskArtifact
from .dependencies import require_module
from .image_io import save_binary_mask


class Sam2Segmenter:
    def __init__(self, config: RunConfig):
        self.config = config

    def segment_bbox(self, image, bbox: BoundingBox, out_path: str | Path) -> MaskArtifact:
        warnings: list[str] = []
        torch = require_module("torch", "torch")
        sam_build = require_module("sam2.build_sam", "sam2")
        sam_predictor = require_module("sam2.sam2_image_predictor", "sam2")
        if not torch.cuda.is_available():
            raise RuntimeError("SAM2 requires CUDA/GPU for contour_svg")
        if not self.config.segmentation.sam2_checkpoint:
            raise RuntimeError("SAM2 checkpoint is required; set segmentation.sam2_checkpoint or CONTOUR_SAM2_CHECKPOINT")

        import numpy as np

        Image = require_module("PIL.Image", "Pillow")
        build_sam2 = sam_build.build_sam2
        predictor_cls = sam_predictor.SAM2ImagePredictor
        model = build_sam2(self.config.segmentation.sam2_model_cfg, self.config.segmentation.sam2_checkpoint)
        predictor = predictor_cls(model)
        arr = np.array(image.convert("RGB"))
        with torch.inference_mode():
            with torch.autocast("cuda", dtype=torch.bfloat16):
                predictor.set_image(arr)
                masks, scores, _ = predictor.predict(box=np.array(bbox.xyxy, dtype=np.float32))
        if len(scores) <= 0:
            raise RuntimeError("SAM2 returned no masks for GroundingDINO box")
        best_idx = int(np.argmax(scores))
        mask_arr = masks[best_idx].astype("uint8") * 255
        mask = Image.fromarray(mask_arr)
        save_binary_mask(mask, out_path)
        return MaskArtifact(Path(out_path), bbox=bbox, source="sam2", warnings=warnings)
