"""Analyse a poster image to find text-dense and free regions.

Lifted from the proven text-mask logic in
`kaggle/CrumpleVideo/poster_overlay.py` and shaped for AfishaThumb's
sticker-placement engine. Inputs are PNG/JPG poster images; outputs
include:

  - a binary text mask (where the poster carries text or fine detail)
  - an integral image for O(1) box-area density queries
  - a list of high-density "important text regions" (do-not-cover)
  - a list of "free zones" (low-density rectangles where overlay
    stickers are safe).

We expose one orchestration entrypoint, `analyze_poster(path)`, plus a
helper `find_free_zone(analysis, box_w_norm, box_h_norm, prefer)` for
picking sticker placement coordinates in *normalised* (0..1) poster
space.

Two callers in this codebase consume these:
  1. `prepare_slot.py` — decides whether to skip the redundant digest /
     date stickers when the poster already carries enough text;
  2. `slot_trace.py` — annotates which poster regions the story camera
     should dwell on (high-density region = "the viewer needs time to
     read this").
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import cv2  # type: ignore[import-not-found]
import numpy as np


@dataclass
class PosterAnalysis:
    """Result of `analyze_poster`. Coordinates are in normalised 0..1
    poster space (origin top-left) so the caller can scale to whatever
    sticker-placement coordinate system it uses."""
    width: int
    height: int
    text_density: float                       # 0..1 fraction of poster covered by text
    text_regions: list[tuple[float, float, float, float]] = field(default_factory=list)
    free_zones: list[tuple[float, float, float, float]] = field(default_factory=list)
    mask_path: Optional[Path] = None
    # The raw mask is kept for debug renders; not serialised into the
    # slot manifest.
    _mask: Optional[np.ndarray] = field(default=None, repr=False)
    _integral: Optional[np.ndarray] = field(default=None, repr=False)


def _read_bgr(path: Path) -> np.ndarray:
    raw = np.fromfile(str(path), dtype=np.uint8)
    img = cv2.imdecode(raw, cv2.IMREAD_COLOR)
    if img is None:
        raise FileNotFoundError(f"cannot decode poster image at {path}")
    return img


def _build_text_mask(gray: np.ndarray) -> np.ndarray:
    """Same shape as crumple's `_build_text_mask`: combine adaptive
    threshold, morphological gradient and Canny, then dilate so that
    text regions become solid blobs rather than thin strokes."""
    h, w = gray.shape[:2]
    base = float(min(w, h))
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    adaptive = cv2.adaptiveThreshold(
        blur, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY_INV, 31, 11,
    )
    gradient = cv2.morphologyEx(
        blur, cv2.MORPH_GRADIENT,
        cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)),
    )
    _, gradient_bin = cv2.threshold(
        gradient, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
    )
    edges = cv2.Canny(blur, 50, 150)
    mask = cv2.bitwise_or(adaptive, gradient_bin)
    mask = cv2.bitwise_or(mask, edges)
    close_kernel = cv2.getStructuringElement(
        cv2.MORPH_RECT,
        (max(3, int(round(base * 0.010))),
         max(3, int(round(base * 0.006)))),
    )
    expand_kernel = cv2.getStructuringElement(
        cv2.MORPH_RECT,
        (max(15, int(round(base * 0.032))),
         max(9, int(round(base * 0.020)))),
    )
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, close_kernel, iterations=1)
    mask = cv2.dilate(mask, expand_kernel, iterations=1)
    return mask


def _mask_fill_ratio(integral: np.ndarray, x0: int, y0: int, bw: int, bh: int) -> float:
    x1 = x0 + bw
    y1 = y0 + bh
    filled = (
        int(integral[y1, x1])
        - int(integral[y0, x1])
        - int(integral[y1, x0])
        + int(integral[y0, x0])
    )
    return filled / float(max(bw * bh, 1))


def _grid_regions(
    mask: np.ndarray, integral: np.ndarray, cell_norm: float = 0.18,
) -> tuple[list[tuple[float, float, float, float]], list[tuple[float, float, float, float]]]:
    """Sweep a coarse grid of cells across the mask. Cells with fill >= 0.35
    are tagged "text region"; cells with fill <= 0.05 are tagged "free".
    Returns both lists in normalised (x, y, w, h) coordinates."""
    h, w = mask.shape[:2]
    cell_w = max(8, int(w * cell_norm))
    cell_h = max(8, int(h * cell_norm))
    step_x = cell_w // 2
    step_y = cell_h // 2
    text_regs: list[tuple[float, float, float, float]] = []
    free_regs: list[tuple[float, float, float, float]] = []
    y = 0
    while y + cell_h <= h:
        x = 0
        while x + cell_w <= w:
            r = _mask_fill_ratio(integral, x, y, cell_w, cell_h)
            xn, yn = x / w, y / h
            wn, hn = cell_w / w, cell_h / h
            if r >= 0.35:
                text_regs.append((xn, yn, wn, hn))
            elif r <= 0.05:
                free_regs.append((xn, yn, wn, hn))
            x += step_x
        y += step_y
    return text_regs, free_regs


def analyze_poster(path: Path, save_debug_mask_to: Optional[Path] = None) -> PosterAnalysis:
    img = _read_bgr(path)
    h, w = img.shape[:2]
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    mask = _build_text_mask(gray)
    integral = cv2.integral((mask > 0).astype(np.uint8), sdepth=cv2.CV_32S)
    density = float(np.mean(mask > 0))
    text_regs, free_regs = _grid_regions(mask, integral)
    out = PosterAnalysis(
        width=w, height=h, text_density=density,
        text_regions=text_regs, free_zones=free_regs,
        _mask=mask, _integral=integral,
    )
    if save_debug_mask_to is not None:
        cv2.imwrite(str(save_debug_mask_to), mask)
        out.mask_path = save_debug_mask_to
    return out


def fill_ratio_at(
    analysis: PosterAnalysis,
    x_norm: float, y_norm: float, w_norm: float, h_norm: float,
) -> float:
    """Density of the mask under a normalised box on the poster."""
    if analysis._integral is None:
        return 0.0
    h_px, w_px = analysis.height, analysis.width
    x = max(0, min(w_px - 1, int(x_norm * w_px)))
    y = max(0, min(h_px - 1, int(y_norm * h_px)))
    bw = max(1, min(w_px - x, int(w_norm * w_px)))
    bh = max(1, min(h_px - y, int(h_norm * h_px)))
    return _mask_fill_ratio(analysis._integral, x, y, bw, bh)


def find_free_zone(
    analysis: PosterAnalysis,
    box_w_norm: float, box_h_norm: float,
    *, prefer: tuple[float, float] = (0.5, 0.5),
    avoid_other_boxes: Optional[list[tuple[float, float, float, float]]] = None,
    fill_max: float = 0.12,
    rng_seed: Optional[int] = None,
) -> Optional[tuple[float, float]]:
    """Locate a sticker-sized box on the poster whose mask fill stays
    below `fill_max`. Returns the top-left corner in normalised coords.

    `prefer` biases toward a target normalised centre; `avoid_other_boxes`
    forbids overlap with already-chosen sticker positions. Returns None
    when no acceptable position exists in the search grid.
    """
    rng = np.random.default_rng(rng_seed)
    candidates: list[tuple[float, float, float]] = []
    # Search grid.
    for x_norm in np.linspace(0.02, max(0.03, 1.0 - box_w_norm - 0.02), 12):
        for y_norm in np.linspace(0.02, max(0.03, 1.0 - box_h_norm - 0.02), 16):
            ratio = fill_ratio_at(analysis, x_norm, y_norm, box_w_norm, box_h_norm)
            if ratio > fill_max:
                continue
            collide = False
            if avoid_other_boxes:
                for (ox, oy, ow, oh) in avoid_other_boxes:
                    if not (x_norm + box_w_norm <= ox
                            or x_norm >= ox + ow
                            or y_norm + box_h_norm <= oy
                            or y_norm >= oy + oh):
                        collide = True
                        break
            if collide:
                continue
            cx = x_norm + box_w_norm / 2.0
            cy = y_norm + box_h_norm / 2.0
            pref_d = ((cx - prefer[0]) ** 2 + (cy - prefer[1]) ** 2) ** 0.5
            jitter = float(rng.uniform(-0.02, 0.02))
            candidates.append((ratio + pref_d * 0.3 + jitter, x_norm, y_norm))
    if not candidates:
        return None
    candidates.sort()
    _, x, y = candidates[0]
    return (x, y)


__all__ = [
    "PosterAnalysis",
    "analyze_poster",
    "fill_ratio_at",
    "find_free_zone",
]
