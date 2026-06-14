from __future__ import annotations

import inspect
import json
from pathlib import Path

from .config import RunConfig
from .contracts import BoundingBox, SemanticPlan
from .dependencies import MissingDependencyError, require_module


def _bbox_hint(image_size: tuple[int, int], config: RunConfig, semantic_plan: SemanticPlan) -> BoundingBox | None:
    w, h = image_size
    if config.input.bbox_hint_xyxy:
        return BoundingBox(tuple(float(v) for v in config.input.bbox_hint_xyxy), label=config.input.object_prompt, source="config_hint").clipped(w, h)
    hint = semantic_plan.primary_object.get("bbox_hint_xyxy_norm") if semantic_plan.primary_object else None
    if isinstance(hint, list) and len(hint) == 4:
        x1, y1, x2, y2 = hint
        return BoundingBox((x1 * w, y1 * h, x2 * w, y2 * h), label=config.input.object_prompt, score=0.75, source="gemini_hint").clipped(w, h)
    return None


class GroundingDinoDetector:
    def __init__(self, config: RunConfig):
        self.config = config

    def detect(self, image, prompts: list[str]) -> tuple[list[BoundingBox], list[str]]:
        warnings: list[str] = []
        torch = require_module("torch", "torch")
        transformers = require_module("transformers", "transformers")
        if not torch.cuda.is_available():
            raise MissingDependencyError("GroundingDINO requires CUDA/GPU for this pipeline")
        processor = transformers.AutoProcessor.from_pretrained(
            self.config.segmentation.grounding_model,
            cache_dir=self.config.runtime.hf_cache_dir,
        )
        model = transformers.AutoModelForZeroShotObjectDetection.from_pretrained(
            self.config.segmentation.grounding_model,
            cache_dir=self.config.runtime.hf_cache_dir,
        ).to("cuda:0")
        model.eval()
        text_prompt = " ".join(p if p.endswith(".") else f"{p}." for p in prompts)
        inputs = processor(images=image, text=text_prompt, return_tensors="pt").to("cuda:0")
        with torch.no_grad():
            outputs = model(**inputs)
        target_sizes = [(image.height, image.width)]
        postprocess = processor.post_process_grounded_object_detection
        postprocess_params = inspect.signature(postprocess).parameters
        postprocess_kwargs = {"target_sizes": target_sizes}
        if "box_threshold" in postprocess_params:
            postprocess_kwargs["box_threshold"] = self.config.segmentation.box_threshold
        elif "threshold" in postprocess_params:
            postprocess_kwargs["threshold"] = self.config.segmentation.box_threshold
        if "text_threshold" in postprocess_params:
            postprocess_kwargs["text_threshold"] = self.config.segmentation.text_threshold
        results = postprocess(outputs, inputs.input_ids, **postprocess_kwargs)[0]
        boxes: list[BoundingBox] = []
        for box, score, label in zip(results.get("boxes", []), results.get("scores", []), results.get("labels", [])):
            vals = tuple(float(v) for v in box.detach().cpu().tolist())
            boxes.append(BoundingBox(vals, label=str(label), score=float(score), source="groundingdino").clipped(image.width, image.height))
        boxes.sort(key=lambda b: b.score, reverse=True)
        return boxes, warnings


class YoloWorldDetector:
    def __init__(self, config: RunConfig):
        self.config = config

    def detect(self, image, prompts: list[str]) -> tuple[list[BoundingBox], list[str]]:
        warnings: list[str] = []
        torch = require_module("torch", "torch")
        ultralytics = require_module("ultralytics", "ultralytics")
        if not torch.cuda.is_available():
            raise MissingDependencyError("YOLO-World requires CUDA/GPU for this pipeline")
        YOLOWorld = getattr(ultralytics, "YOLOWorld", None)
        if YOLOWorld is None:
            from ultralytics.models.yolo.model import YOLOWorld as YOLOWorld
        model = YOLOWorld(self.config.segmentation.yoloworld_model)
        model.to("cuda")
        classes = _dedupe_prompts(prompts)
        model.set_classes(classes)
        results = model.predict(image, conf=float(self.config.segmentation.box_threshold), verbose=False)
        boxes: list[BoundingBox] = []
        if not results:
            return boxes, warnings
        result = results[0]
        names = getattr(result, "names", {}) or {}
        raw_boxes = getattr(result, "boxes", None)
        if raw_boxes is None:
            return boxes, warnings
        xyxy = getattr(raw_boxes, "xyxy", None)
        conf = getattr(raw_boxes, "conf", None)
        cls = getattr(raw_boxes, "cls", None)
        if xyxy is None or conf is None or cls is None:
            return boxes, warnings
        for box, score, class_id in zip(xyxy.detach().cpu().tolist(), conf.detach().cpu().tolist(), cls.detach().cpu().tolist()):
            label = str(names.get(int(class_id), classes[int(class_id)] if int(class_id) < len(classes) else class_id))
            boxes.append(
                BoundingBox(
                    tuple(float(v) for v in box),
                    label=label,
                    score=float(score),
                    source="yoloworld",
                ).clipped(image.width, image.height)
            )
        boxes.sort(key=lambda b: b.score, reverse=True)
        return boxes, warnings


class FlorenceDetector:
    def __init__(self, config: RunConfig):
        self.config = config

    def detect(self, image, prompts: list[str]) -> tuple[list[BoundingBox], list[str]]:
        warnings: list[str] = []
        torch = require_module("torch", "torch")
        transformers = require_module("transformers", "transformers")
        if not torch.cuda.is_available():
            raise MissingDependencyError("Florence-2 detection requires CUDA/GPU for this pipeline")
        model_id = self.config.segmentation.florence_model
        processor = transformers.AutoProcessor.from_pretrained(
            model_id,
            cache_dir=self.config.runtime.hf_cache_dir,
            trust_remote_code=True,
        )
        model_cls = getattr(transformers, "Florence2ForConditionalGeneration", None)
        if model_cls is None:
            model_cls = transformers.AutoModelForCausalLM
        model = model_cls.from_pretrained(
            model_id,
            torch_dtype=torch.float16 if self.config.runtime.dtype == "float16" else torch.float32,
            cache_dir=self.config.runtime.hf_cache_dir,
            trust_remote_code=True,
        ).to("cuda:0")
        model.eval()
        task_prompt = "<OD>"
        inputs = processor(text=task_prompt, images=image.convert("RGB"), return_tensors="pt").to("cuda:0")
        if self.config.runtime.dtype == "float16":
            for key, value in list(inputs.items()):
                if hasattr(value, "is_floating_point") and value.is_floating_point():
                    inputs[key] = value.to(torch.float16)
        with torch.no_grad():
            generated_ids = model.generate(
                **inputs,
                max_new_tokens=1024,
                num_beams=3,
            )
        generated_text = processor.batch_decode(generated_ids, skip_special_tokens=False)[0]
        parsed = processor.post_process_generation(generated_text, task=task_prompt, image_size=image.size)
        detections = parsed.get(task_prompt, {}) if isinstance(parsed, dict) else {}
        boxes_raw = detections.get("bboxes") or []
        labels_raw = detections.get("labels") or []
        prompt_terms = {term.lower() for prompt in prompts for term in str(prompt).replace("/", " ").split() if len(term) >= 3}
        boxes: list[BoundingBox] = []
        for box, label in zip(boxes_raw, labels_raw):
            label_text = str(label)
            label_terms = set(label_text.lower().replace("/", " ").split())
            score = 0.68 if prompt_terms & label_terms else 0.56
            if not isinstance(box, (list, tuple)) or len(box) != 4:
                continue
            boxes.append(
                BoundingBox(
                    tuple(float(v) for v in box),
                    label=label_text,
                    score=score,
                    source="florence2",
                ).clipped(image.width, image.height)
            )
        boxes.sort(key=lambda b: b.score, reverse=True)
        return boxes, warnings


def detect_open_vocabulary_boxes(
    image,
    config: RunConfig,
    prompts: list[str],
    *,
    debug_dir: Path | None = None,
    artifact_prefix: str = "detector",
) -> tuple[list[BoundingBox], list[str]]:
    warnings: list[str] = []
    grounding_boxes, grounding_warnings = GroundingDinoDetector(config).detect(image, prompts)
    warnings.extend(grounding_warnings)
    boxes = list(grounding_boxes)
    florence_boxes: list[BoundingBox] = []
    if config.segmentation.use_florence:
        florence_boxes, florence_warnings = FlorenceDetector(config).detect(image, prompts)
        warnings.extend(florence_warnings)
        boxes.extend(florence_boxes)
    yolo_boxes: list[BoundingBox] = []
    if config.segmentation.use_yoloworld:
        yolo_boxes, yolo_warnings = YoloWorldDetector(config).detect(image, prompts)
        warnings.extend(yolo_warnings)
        boxes.extend(yolo_boxes)
    boxes = _dedupe_boxes(boxes)
    boxes.sort(key=lambda b: (_source_weight(b.source), b.score, _area(b.xyxy)), reverse=True)
    if debug_dir is not None:
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / f"{artifact_prefix}_boxes.json").write_text(
            json.dumps(
                {
                    "prompts": prompts,
                    "groundingdino": [_box_to_dict(box) for box in grounding_boxes],
                    "florence2": [_box_to_dict(box) for box in florence_boxes],
                    "yoloworld": [_box_to_dict(box) for box in yolo_boxes],
                    "merged": [_box_to_dict(box) for box in boxes],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    return boxes, warnings


def choose_primary_bbox(image, config: RunConfig, semantic_plan: SemanticPlan, *, debug_dir: Path | None = None) -> tuple[BoundingBox, list[str]]:
    hint = _bbox_hint(image.size, config, semantic_plan)
    prompts = [config.input.object_prompt, *config.segmentation.primary_prompts]
    boxes, warnings = detect_open_vocabulary_boxes(image, config, prompts, debug_dir=debug_dir, artifact_prefix="primary_detector")
    if not boxes:
        raise RuntimeError(f"Open-vocabulary detectors returned no primary object boxes for prompts={prompts!r}")
    if hint is not None:
        fx1, fy1, fx2, fy2 = hint.xyxy
        best = max(boxes, key=lambda b: _iou((fx1, fy1, fx2, fy2), b.xyxy) + b.score * 0.2)
        return best, warnings
    return boxes[0], warnings


def _iou(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0.0, ix2 - ix1), max(0.0, iy2 - iy1)
    inter = iw * ih
    area_a = max(1.0, (ax2 - ax1) * (ay2 - ay1))
    area_b = max(1.0, (bx2 - bx1) * (by2 - by1))
    return inter / max(1.0, area_a + area_b - inter)


def _dedupe_prompts(prompts: list[str]) -> list[str]:
    out: list[str] = []
    for prompt in prompts:
        value = str(prompt).strip()
        if value and value not in out:
            out.append(value)
    return out or ["building"]


def _dedupe_boxes(boxes: list[BoundingBox]) -> list[BoundingBox]:
    out: list[BoundingBox] = []
    for box in sorted(boxes, key=lambda b: (_source_weight(b.source), b.score), reverse=True):
        if any(_iou(box.xyxy, existing.xyxy) > 0.72 for existing in out):
            continue
        out.append(box)
    return out


def _source_weight(source: str) -> float:
    if source == "groundingdino":
        return 1.0
    if source == "yoloworld":
        return 0.92
    if source == "florence2":
        return 0.82
    if source == "gemini_hint":
        return 0.8
    return 0.5


def _area(box: tuple[float, float, float, float]) -> float:
    x1, y1, x2, y2 = box
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)


def _box_to_dict(box: BoundingBox) -> dict[str, object]:
    return {
        "xyxy": list(box.xyxy),
        "label": box.label,
        "score": box.score,
        "source": box.source,
    }
