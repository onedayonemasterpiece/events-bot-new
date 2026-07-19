#!/usr/bin/env python3
"""Plan a no-letterbox crop from source/target geometry and protected boxes."""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Iterable


EPSILON = 1e-9

RATIO_TOKENS = {
    "P": 4 / 5,
    "S": 1.0,
    "W": 4 / 3,
    "L": 3 / 2,
    "H": 16 / 10,
    "OG": 1200 / 630,
}
SURFACE_TOKENS = {
    "card": ("P", "S", "W", "L"),
    "hero": ("P", "S", "W", "L", "H"),
    "share": ("P", "S", "OG"),
}
TOKEN_ALIASES = {
    "p": "P",
    "portrait-4x5": "P",
    "s": "S",
    "square-1x1": "S",
    "w": "W",
    "wide-4x3": "W",
    "l": "L",
    "landscape-3x2": "L",
    "h": "H",
    "hero-16x10": "H",
    "og": "OG",
    "og-40x21": "OG",
}


@dataclass(frozen=True)
class Box:
    x: float
    y: float
    w: float
    h: float

    @property
    def right(self) -> float:
        return self.x + self.w

    @property
    def bottom(self) -> float:
        return self.y + self.h


def parse_ratio(value: str) -> float:
    raw = value.strip()
    token = TOKEN_ALIASES.get(raw.lower())
    if token:
        return RATIO_TOKENS[token]
    if ":" in raw:
        left, right = raw.split(":", 1)
        ratio = float(left) / float(right)
    elif "/" in raw:
        left, right = raw.split("/", 1)
        ratio = float(left) / float(right)
    else:
        ratio = float(raw)
    if ratio <= 0:
        raise argparse.ArgumentTypeError("ratio must be positive")
    return ratio


def ratio_token(target_ratio: float, surface: str) -> str | None:
    if surface == "document":
        return None
    for token in SURFACE_TOKENS[surface]:
        if abs(RATIO_TOKENS[token] - target_ratio) <= EPSILON:
            return token
    return None


def parse_box(value: str) -> Box:
    try:
        box = Box(*(float(part.strip()) for part in value.split(",")))
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("box must be x,y,w,h in normalized 0..1 coordinates") from None
    if box.w <= 0 or box.h <= 0 or min(box.x, box.y) < 0 or box.right > 1 or box.bottom > 1:
        raise argparse.ArgumentTypeError("box must be positive and contained in normalized 0..1 space")
    return box


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _safe_window(
    *,
    axis: str,
    visible_fraction: float,
    boxes: Iterable[Box],
    focal: float,
    margin: float,
) -> tuple[float, float] | None:
    crop = 1 - visible_fraction
    protected = list(boxes)
    if axis == "vertical":
        starts = [box.y for box in protected]
        ends = [box.bottom for box in protected]
    else:
        starts = [box.x for box in protected]
        ends = [box.right for box in protected]
    lower = max(0.0, max((end + margin - visible_fraction for end in ends), default=0.0))
    upper = min(crop, min((start - margin for start in starts), default=crop))
    if lower > upper + EPSILON:
        return None
    desired = focal - visible_fraction / 2
    start = _clamp(desired, lower, upper)
    return start, start + visible_fraction


def plan_crop(
    *,
    width: int,
    height: int,
    target_ratio: float,
    image_text_mode: str,
    surface: str = "card",
    selection_reason: str = "surface-default",
    safe_crop: bool = False,
    ocr_boxes: Iterable[Box] = (),
    face_boxes: Iterable[Box] = (),
    focal_x: float = 0.5,
    focal_y: float = 0.5,
    margin: float = 0.01,
    max_ocr_crop: float = 0.20,
    max_visual_crop: float = 0.30,
) -> dict[str, object]:
    if width <= 0 or height <= 0:
        raise ValueError("width and height must be positive")
    if image_text_mode not in {"ocr_text", "visual_only", "unknown"}:
        raise ValueError("invalid image_text_mode")
    if surface not in {*SURFACE_TOKENS, "document"}:
        raise ValueError("invalid surface")
    selection_reason = selection_reason.strip()
    if not selection_reason:
        raise ValueError("selection_reason must be non-empty")
    target_token = ratio_token(target_ratio, surface)
    if surface != "document" and target_token is None:
        allowed = ", ".join(SURFACE_TOKENS[surface])
        raise ValueError(f"target ratio is not an approved {surface} token; use one of: {allowed}")
    if not (0 <= margin < 0.5 and 0 <= max_ocr_crop < 1 and 0 <= max_visual_crop < 1):
        raise ValueError("margins and crop limits must be within range")
    focal_x = _clamp(focal_x, 0, 1)
    focal_y = _clamp(focal_y, 0, 1)
    source_ratio = width / height
    relative = source_ratio / target_ratio
    loss = max(0.0, 1 - min(relative, 1 / relative))
    axis = "none" if loss <= EPSILON else ("vertical" if source_ratio < target_ratio else "horizontal")
    retained = 1 - loss
    ocr_boxes = list(ocr_boxes)
    face_boxes = list(face_boxes)

    base: dict[str, object] = {
        "source": {"width": width, "height": height, "ratio": round(source_ratio, 8)},
        "requested_target_ratio": round(target_ratio, 8),
        "target_token": target_token,
        "token_selection_reason": selection_reason,
        "surface": surface,
        "crop_axis": axis,
        "potential_crop_area_fraction": round(loss, 8),
        "retained_area_fraction": round(retained, 8),
        "image_text_mode": image_text_mode,
    }

    def natural(reason: str) -> dict[str, object]:
        if surface != "document":
            decision = {
                "card": "fallback-required",
                "hero": "route-to-document",
                "share": "composition-required",
            }[surface]
            return {
                **base,
                "decision": decision,
                "reason": reason,
                "container_ratio": round(target_ratio, 8),
                "render_source_image": False,
                "css": {
                    "container_aspect_ratio": str(round(target_ratio, 8)),
                    "image_layout": "do-not-render-source-in-this-normalized-frame",
                },
                "crop": {"top": 0, "right": 0, "bottom": 0, "left": 0},
            }
        return {
            **base,
            "decision": "natural-ratio",
            "reason": reason,
            "container_ratio": round(source_ratio, 8),
            "css": {
                "container_aspect_ratio": f"{width} / {height}",
                "image_layout": "display:block;width:100%;height:auto",
            },
            "crop": {"top": 0, "right": 0, "bottom": 0, "left": 0},
        }

    if axis == "none":
        if surface != "document":
            return {
                **base,
                "decision": "exact-ratio",
                "reason": "source already matches approved target token",
                "container_ratio": round(target_ratio, 8),
                "render_source_image": True,
                "crop": {"top": 0, "right": 0, "bottom": 0, "left": 0},
                "css": {
                    "container_aspect_ratio": str(round(target_ratio, 8)),
                    "image_layout": "display:block;width:100%;height:100%",
                },
            }
        return {
            **natural("source already matches target ratio"),
            "decision": "exact-ratio",
            "container_ratio": round(target_ratio, 8),
        }

    protected = face_boxes + (ocr_boxes if image_text_mode != "visual_only" else [])
    focal = focal_y if axis == "vertical" else focal_x
    window = _safe_window(
        axis=axis,
        visible_fraction=retained,
        boxes=protected,
        focal=focal,
        margin=margin,
    )

    if image_text_mode != "visual_only":
        if axis != "vertical":
            return natural("text-protected media never uses left/right crop")
        if loss > max_ocr_crop + EPSILON:
            return natural("vertical OCR crop would exceed the combined 20% area budget")
        if not ocr_boxes:
            return natural("OCR/unknown crop needs persisted OCR boxes")
        if window is None:
            return natural("no vertical crop window retains every OCR/face box")
        reason = "bounded vertical OCR crop retains every protected box"
    else:
        if not safe_crop:
            return natural("visual_only without safe_crop evidence cannot enter normalized cover")
        if loss > max_visual_crop + EPSILON:
            return natural("visual crop would exceed the configured visual loss budget")
        if window is None:
            return natural("no crop window retains every face box")
        reason = "verified visual-only crop stays within the safe envelope"

    start, end = window
    crop = {"top": 0.0, "right": 0.0, "bottom": 0.0, "left": 0.0}
    if axis == "vertical":
        crop["top"] = start
        crop["bottom"] = 1 - end
        object_position = f"50% {round((start / loss) * 100, 2)}%"
    else:
        crop["left"] = start
        crop["right"] = 1 - end
        object_position = f"{round((start / loss) * 100, 2)}% 50%"
    crop = {key: round(value, 8) for key, value in crop.items()}
    return {
        **base,
        "decision": "bounded-cover" if image_text_mode != "visual_only" else "cover",
        "reason": reason,
        "container_ratio": round(target_ratio, 8),
        "object_position": object_position,
        "crop": crop,
        "css": {
            "container_aspect_ratio": str(round(target_ratio, 8)),
            "image_layout": f"width:100%;height:100%;object-fit:cover;object-position:{object_position}",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--width", type=int, required=True)
    parser.add_argument("--height", type=int, required=True)
    parser.add_argument("--target", type=parse_ratio, required=True, dest="target_ratio")
    parser.add_argument("--image-text-mode", choices=("ocr_text", "visual_only", "unknown"), required=True)
    parser.add_argument("--surface", choices=("card", "hero", "share", "document"), default="card")
    parser.add_argument("--selection-reason", default="surface-default")
    parser.add_argument("--safe-crop", action="store_true")
    parser.add_argument("--ocr-box", action="append", default=[], type=parse_box, dest="ocr_boxes")
    parser.add_argument("--face-box", action="append", default=[], type=parse_box, dest="face_boxes")
    parser.add_argument("--focal-x", type=float, default=0.5)
    parser.add_argument("--focal-y", type=float, default=0.5)
    parser.add_argument("--margin", type=float, default=0.01)
    parser.add_argument("--max-ocr-crop", type=float, default=0.20)
    parser.add_argument("--max-visual-crop", type=float, default=0.30)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    values = vars(args)
    pretty = values.pop("pretty")
    result = plan_crop(**values)
    print(json.dumps(result, ensure_ascii=False, indent=2 if pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
