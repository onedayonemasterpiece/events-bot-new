"""Round-11 sticker renderer.

Replaces the per-card ad-hoc font ratios in
`kaggle/AfishaThumb/scripts/typography.py` with a single rule: every
sticker is rasterised at a fixed `PX_PER_D` (pixels per D-unit), and
the font size is `cap_h_d × PX_PER_D` regardless of the sticker's
pixel resolution. This guarantees that titles, dates, locations, and
costs all render with consistent visible typography across events on
the cylinder (R2).

Cards are simple white papers with a thin dark border and a 1-px
inner stroke; the text is laid out tight to the card edge with the
master padding from `master_sizes`.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Sequence

from PIL import Image, ImageDraw, ImageFont

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "kaggle" / "AfishaThumb" / "scripts"))

from typography import (  # noqa: E402
    FONT_BEBAS_BOLD,
    FONT_CYGRE_BOLD,
    FONT_CYGRE_MEDIUM,
    FONT_CYGRE_SEMIBOLD,
    FONT_DRUK_BOLD,
    FONT_DRUK_HEAVY,
    FONT_DRUK_MEDIUM,
    FONT_DRUK_SUPER,
)

from scripts.afishathumb.master_sizes import (
    COST_CAP_H_D,
    DATE_CAP_H_D,
    DIGEST_CAP_H_D,
    LOCATION_CAP_H_D,
    STICKER_PAD_H_D,
    STICKER_PAD_V_BOTTOM_D,
    STICKER_PAD_V_TOP_D,
    TITLE_CAP_H_D,
)

PX_PER_D = 1500


def _font(candidates: Sequence[Path], size_px: int) -> ImageFont.FreeTypeFont:
    for p in candidates:
        if p.exists():
            try:
                return ImageFont.truetype(str(p), size=size_px)
            except (OSError, IOError):
                continue
    return ImageFont.load_default()


def _measure(draw: ImageDraw.ImageDraw, text: str,
             font: ImageFont.FreeTypeFont) -> tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def _make_card(w_d: float, h_d: float,
               bg: tuple[int, int, int] = (250, 247, 240),
               border: tuple[int, int, int] = (40, 30, 25),
               border_px: int = 6) -> Image.Image:
    w_px = int(w_d * PX_PER_D)
    h_px = int(h_d * PX_PER_D)
    img = Image.new("RGBA", (w_px, h_px), bg + (255,))
    d = ImageDraw.Draw(img)
    d.rectangle([(0, 0), (w_px - 1, h_px - 1)], outline=border, width=border_px)
    return img


def render_title(text: str, w_d: float, h_d: float, lines: int = 1) -> Image.Image:
    img = _make_card(w_d, h_d)
    d = ImageDraw.Draw(img)
    upper = text.upper().strip()
    pad_x_px = int(STICKER_PAD_H_D * PX_PER_D)
    pad_top_px = int(STICKER_PAD_V_TOP_D * PX_PER_D)
    w_px = img.width
    h_px = img.height
    inner_w = w_px - 2 * pad_x_px

    # Word-wrap to up to 2 lines; if longest line doesn't fit at master
    # cap, shrink the cap until it does.
    cap_px = int(TITLE_CAP_H_D * PX_PER_D)
    chosen_lines: list[str] = []
    for attempt in range(10):
        font = _font(FONT_DRUK_HEAVY + FONT_DRUK_BOLD + FONT_DRUK_SUPER,
                     size_px=cap_px)
        words = upper.split()
        if lines == 1:
            tw, _ = _measure(d, upper, font)
            if tw <= inner_w:
                chosen_lines = [upper]
                break
            cap_px = int(cap_px * 0.92)
            continue
        # 2-line: greedy split that balances width
        best: tuple[str, str] | None = None
        for split in range(1, len(words)):
            l1 = " ".join(words[:split])
            l2 = " ".join(words[split:])
            tw1, _ = _measure(d, l1, font)
            tw2, _ = _measure(d, l2, font)
            if tw1 <= inner_w and tw2 <= inner_w:
                best = (l1, l2)
                # pick the most balanced split
                if abs(tw1 - tw2) < inner_w * 0.4:
                    break
        if best is not None:
            chosen_lines = list(best)
            break
        cap_px = int(cap_px * 0.92)

    if not chosen_lines:
        chosen_lines = [upper]

    font = _font(FONT_DRUK_HEAVY + FONT_DRUK_BOLD + FONT_DRUK_SUPER, size_px=cap_px)
    line_h_px = int(cap_px * 1.15)
    block_h = len(chosen_lines) * line_h_px
    y = pad_top_px + max(0, (h_px - 2 * pad_top_px - block_h) // 2)
    for line in chosen_lines:
        tw, _ = _measure(d, line, font)
        d.text(((w_px - tw) // 2, y), line, font=font, fill=(20, 16, 12))
        y += line_h_px
    return img


def render_info(text: str, cap_h_d: float, w_d: float, h_d: float,
                bg: tuple[int, int, int] = (250, 247, 240),
                fg: tuple[int, int, int] = (20, 16, 12),
                fonts: Iterable[Path] | None = None) -> Image.Image:
    img = _make_card(w_d, h_d, bg=bg)
    d = ImageDraw.Draw(img)
    font_px = int(cap_h_d * PX_PER_D)
    font = _font(fonts or (FONT_DRUK_BOLD + FONT_DRUK_HEAVY), size_px=font_px)
    pad_top_px = int(STICKER_PAD_V_TOP_D * PX_PER_D)
    w_px = img.width
    tw, th = _measure(d, text, font)
    d.text(((w_px - tw) // 2, pad_top_px), text, font=font, fill=fg)
    return img


def render_date(text: str, w_d: float, h_d: float) -> Image.Image:
    """Date+time sticker (e.g. `18 МАЯ · 19:00`). Single-line, Druk Heavy."""
    return render_info(text.upper(), DATE_CAP_H_D, w_d, h_d,
                       fonts=FONT_DRUK_HEAVY + FONT_DRUK_BOLD)


def render_date_stacked(day: str, month: str, time: str,
                        w_d: float, h_d: float) -> Image.Image:
    """Narrow vertical date sticker: big day number, smaller month
    below, time at the bottom — the "хорошая узкая наклейка" style
    operator remembered from earlier rounds."""
    img = _make_card(w_d, h_d)
    d = ImageDraw.Draw(img)
    w_px = img.width
    h_px = img.height
    pad_top = int(STICKER_PAD_V_TOP_D * PX_PER_D)
    pad_bottom = int(STICKER_PAD_V_BOTTOM_D * PX_PER_D)
    avail_h = h_px - pad_top - pad_bottom
    # Sized so day:month:time visually = 0.50 : 0.22 : 0.22 of avail_h,
    # with two small gaps between them.
    day_h_px = int(avail_h * 0.50)
    month_h_px = int(avail_h * 0.22)
    time_h_px = int(avail_h * 0.22)
    gap = (avail_h - day_h_px - month_h_px - time_h_px) // 2
    day_font = _font(FONT_DRUK_HEAVY + FONT_DRUK_BOLD, size_px=day_h_px)
    month_font = _font(FONT_DRUK_BOLD + FONT_DRUK_MEDIUM, size_px=month_h_px)
    time_font = _font(FONT_DRUK_BOLD, size_px=time_h_px)
    y = pad_top
    tw, _ = _measure(d, day, day_font)
    d.text(((w_px - tw) // 2, y), day, font=day_font, fill=(20, 16, 12))
    y += day_h_px + gap
    tw, _ = _measure(d, month.upper(), month_font)
    d.text(((w_px - tw) // 2, y), month.upper(), font=month_font, fill=(20, 16, 12))
    y += month_h_px + gap
    tw, _ = _measure(d, time, time_font)
    d.text(((w_px - tw) // 2, y), time, font=time_font, fill=(20, 16, 12))
    return img


def render_location(text: str, w_d: float, h_d: float,
                    parts: list[str] | None = None) -> Image.Image:
    """Location sticker. If `parts` is given (venue / address / city),
    each part renders on its own line. Font size is shrunk down if any
    line is wider than the card. Single-string mode falls back to a
    greedy word wrap.
    """
    img = _make_card(w_d, h_d)
    d = ImageDraw.Draw(img)
    pad_x = int(STICKER_PAD_H_D * PX_PER_D)
    pad_y = int(STICKER_PAD_V_TOP_D * PX_PER_D)
    inner_w = img.width - 2 * pad_x
    inner_h = img.height - 2 * pad_y

    if parts is None:
        parts_in = [text]
    else:
        parts_in = [p for p in parts if p]

    # Word-wrap each part to fit inner_w at the master cap height; if a
    # single word doesn't fit, shrink the cap until it does.
    cap_px = int(LOCATION_CAP_H_D * PX_PER_D)
    lines: list[str] = []
    for attempt in range(12):
        font = _font(FONT_CYGRE_BOLD + FONT_CYGRE_SEMIBOLD, size_px=cap_px)
        lines = []
        overflow = False
        for part in parts_in:
            words = part.split()
            cur = ""
            for w in words:
                candidate = (cur + " " + w).strip()
                tw, _ = _measure(d, candidate, font)
                if tw <= inner_w:
                    cur = candidate
                else:
                    if cur:
                        lines.append(cur)
                        cur = w
                    else:
                        # Single word wider than card — need shrink.
                        overflow = True
                        cur = w
                        break
            if cur and not overflow:
                lines.append(cur)
            if overflow:
                break
        if not overflow:
            break
        cap_px = int(cap_px * 0.90)
    line_h = int(cap_px * 1.18)
    block_h = len(lines) * line_h
    y = pad_y + max(0, (inner_h - block_h) // 2)
    for line in lines:
        tw, _ = _measure(d, line, font)
        d.text(((img.width - tw) // 2, y), line, font=font, fill=(20, 16, 12))
        y += line_h
    return img


def render_cost(text: str, w_d: float, h_d: float, *,
                is_free: bool) -> Image.Image:
    bg = (255, 232, 99) if is_free else (250, 247, 240)
    return render_info(text.upper(), COST_CAP_H_D, w_d, h_d,
                       bg=bg,
                       fonts=FONT_DRUK_HEAVY + FONT_DRUK_BOLD)


def render_digest(text: str, w_d: float, h_d: float) -> Image.Image:
    """Multi-line wrapped digest sticker (search_digest field, 16–20
    words). Wraps to fill the card; clips with `…` if it overflows."""
    img = _make_card(w_d, h_d)
    d = ImageDraw.Draw(img)
    font_px = int(DIGEST_CAP_H_D * PX_PER_D)
    font = _font(FONT_CYGRE_MEDIUM + FONT_CYGRE_SEMIBOLD, size_px=font_px)
    pad_x = int(STICKER_PAD_H_D * PX_PER_D)
    pad_y_top = int(STICKER_PAD_V_TOP_D * PX_PER_D)
    inner_w = img.width - 2 * pad_x
    inner_h = img.height - pad_y_top - int(STICKER_PAD_V_BOTTOM_D * PX_PER_D)
    line_h = int(font_px * 1.20)
    max_lines = max(1, inner_h // line_h)
    # naive greedy wrap by word
    words = text.split()
    lines: list[str] = []
    cur = ""
    for w in words:
        candidate = (cur + " " + w).strip()
        tw, _ = _measure(d, candidate, font)
        if tw <= inner_w:
            cur = candidate
        else:
            if cur:
                lines.append(cur)
            cur = w
            if len(lines) >= max_lines:
                break
    if cur and len(lines) < max_lines:
        lines.append(cur)
    if len(lines) >= max_lines and len(words) > sum(len(l.split()) for l in lines):
        # clip last line with ellipsis
        last = lines[-1]
        while last:
            test = last.rstrip() + "…"
            tw, _ = _measure(d, test, font)
            if tw <= inner_w:
                lines[-1] = test
                break
            last = last[:-1]
    y = pad_y_top
    for line in lines:
        d.text((pad_x, y), line, font=font, fill=(40, 32, 24))
        y += line_h
    return img
