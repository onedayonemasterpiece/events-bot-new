"""Sticker typography renderer for AfishaThumb.

Produces PNG textures for the info-stickers that get glued next to each
poster on the column. Runs outside Blender — Pillow only — so Stage 2
debug stills can pre-bake all sticker textures into `artifacts/afishathumb/`
before the Blender render loads them as image textures on small plane
meshes.

Style alignment:
- Strong typography family matches `video_announce/assets/` (Druk Cyr
  Bold/Heavy, Bebas Neue, Oswald) and the Cygre family from
  `kaggle/CherryFlash/assets/ro_znanie_fonts/`.
- Cards have a slightly off-white paper background by default (`#F4EFE3`),
  with deep ink black text. The card may be tinted (yellow promo card,
  black-on-cream free label, etc.) per `StickerStyle`.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from PIL import Image, ImageDraw, ImageFont

REPO_ROOT = Path(__file__).resolve().parents[3]

# Font sources. Order matters: first hit wins per family.
FONT_DRUK_BOLD = (
    REPO_ROOT / "video_announce" / "assets" / "DrukCyr-Bold.ttf",
)
FONT_DRUK_HEAVY = (
    REPO_ROOT / "video_announce" / "assets" / "DrukCyr-Heavy.ttf",
)
FONT_DRUK_SUPER = (
    REPO_ROOT / "video_announce" / "assets" / "DrukCyr-Super.ttf",
)
FONT_DRUK_MEDIUM = (
    REPO_ROOT / "video_announce" / "assets" / "DrukCyr-Medium.ttf",
)
FONT_BEBAS_BOLD = (
    REPO_ROOT / "video_announce" / "assets" / "BebasNeue-Bold.ttf",
)
FONT_OSWALD = (
    REPO_ROOT / "video_announce" / "assets" / "Oswald-VariableFont_wght.ttf",
)
FONT_BENZIN_BOLD = (
    REPO_ROOT / "video_announce" / "assets" / "Benzin-Bold.ttf",
)
FONT_CYGRE_BOLD = (
    REPO_ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts" / "Cygre-Bold.ttf",
)
FONT_CYGRE_SEMIBOLD = (
    REPO_ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts" / "Cygre-SemiBold.ttf",
)
FONT_CYGRE_MEDIUM = (
    REPO_ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts" / "Cygre-Medium.ttf",
)


RU_MONTHS_GEN = {
    1: "ЯНВАРЯ", 2: "ФЕВРАЛЯ", 3: "МАРТА", 4: "АПРЕЛЯ", 5: "МАЯ",
    6: "ИЮНЯ", 7: "ИЮЛЯ", 8: "АВГУСТА", 9: "СЕНТЯБРЯ", 10: "ОКТЯБРЯ",
    11: "НОЯБРЯ", 12: "ДЕКАБРЯ",
}


def _font(candidates: Sequence[Path], size: int) -> ImageFont.FreeTypeFont:
    for p in candidates:
        if p.exists():
            return ImageFont.truetype(str(p), size=size)
    raise FileNotFoundError(f"none of these font candidates exist: {[str(p) for p in candidates]}")


def _measure(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


@dataclass
class StickerStyle:
    bg: tuple[int, int, int, int] = (244, 239, 227, 255)  # cream paper
    fg: tuple[int, int, int, int] = (24, 24, 28, 255)
    border: tuple[int, int, int, int] | None = (24, 24, 28, 80)
    border_w: int = 0  # 0 = no border ring, just paper
    corner_radius: int = 18
    rotation_deg: float = 0.0
    paper_grain: bool = True


def _has_glyph(font: ImageFont.FreeTypeFont, ch: str) -> bool:
    """Detect whether the font actually has a glyph for `ch` rather than
    silently falling back to `.notdef` (which renders as the broken-square
    artifact we saw on the ruble symbol)."""
    try:
        ttfont = font.font  # FreeType wrapper
        # PIL exposes `getmask` per-glyph; if the resulting mask is empty
        # or matches the width of `.notdef`, the glyph is missing.
        from PIL import ImageFont as _IF  # noqa: F401
        cmap = getattr(ttfont, "getbest_cmap", None)
        if cmap is None:
            return True
        return ord(ch) in ttfont.getbest_cmap()
    except Exception:
        return True


def _money_text(text: str, font: ImageFont.FreeTypeFont) -> str:
    """Render ruble safely. If the chosen font lacks `₽`, substitute `руб.`
    so we never ship a `.notdef` square in the final video."""
    if "₽" in text and not _has_glyph(font, "₽"):
        text = text.replace("₽", "руб.")
    return text


def _paper_grain(img: Image.Image, intensity: int = 12) -> Image.Image:
    """Subtle noise so stickers don't read as digital flat fills."""
    import random
    random.seed(hash(img.tobytes()) & 0xFFFFFFFF)
    px = img.load()
    w, h = img.size
    for y in range(h):
        for x in range(0, w, 2):
            r, g, b, a = px[x, y]
            if a == 0:
                continue
            d = random.randint(-intensity, intensity)
            px[x, y] = (
                max(0, min(255, r + d)),
                max(0, min(255, g + d)),
                max(0, min(255, b + d)),
                a,
            )
    return img


def _rotate_about_center(img: Image.Image, deg: float) -> Image.Image:
    """DEPRECATED: PIL rotation produced rectangular PNGs with transparent
    bbox corners that Blender's image texture rendered as opaque black
    bands around tilted stickers. Tilt is now applied in 3D on the wrapped
    paper mesh (`PaperPlacement.tilt_deg`). Kept as a no-op for backward
    compatibility with old callers that still pass `rotation_deg`."""
    return img


def _draw_card(size: tuple[int, int], style: StickerStyle) -> Image.Image:
    w, h = size
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle(
        (0, 0, w - 1, h - 1),
        radius=style.corner_radius,
        fill=style.bg,
        outline=style.border if style.border_w > 0 else None,
        width=max(0, style.border_w),
    )
    if style.paper_grain:
        img = _paper_grain(img)
    return img


# --------------------------------------------------------------------------- #
# Sticker recipes (one card type per call)
# --------------------------------------------------------------------------- #


def render_date_card(
    out_path: Path,
    day: int,
    month: int,
    time_text: str,
    *,
    width: int = 720,
    height: int = 560,
    style: StickerStyle | None = None,
) -> Path:
    """Big day numeral + month + time, stacked vertically with real ink
    bounds (Druk numerals have huge ascender padding that the cheap
    bbox-anchored layout v1 was treating as visual height — the month
    string ended up sitting *inside* the day's glyph instead of below it).
    """
    s = style or StickerStyle()
    img = _draw_card((width, height), s)
    draw = ImageDraw.Draw(img)

    # Bigger month + slightly smaller day so «МАЯ / ИЮНЯ / ОКТЯБРЯ»
    # reads as fast as the day number itself. Round-7 feedback: the
    # previous 0.16-of-height month was unreadable at slot scales.
    day_font = _font(FONT_DRUK_SUPER + FONT_DRUK_HEAVY + FONT_DRUK_BOLD, size=int(height * 0.44))
    month_font = _font(FONT_DRUK_BOLD + FONT_DRUK_MEDIUM, size=int(height * 0.24))
    time_font = _font(FONT_DRUK_BOLD, size=int(height * 0.22))

    pad_x = int(width * 0.05)
    pad_y = int(height * 0.06)

    day_str = f"{day}"
    month_str = RU_MONTHS_GEN.get(month, "").upper()
    time_str = (time_text or "").strip()

    # Measure inkboxes precisely; PIL gives `(l, t, r, b)` in pixels.
    day_bbox = draw.textbbox((0, 0), day_str, font=day_font)
    month_bbox = draw.textbbox((0, 0), month_str, font=month_font) if month_str else (0, 0, 0, 0)
    time_bbox = draw.textbbox((0, 0), time_str, font=time_font) if time_str else (0, 0, 0, 0)

    day_ink_w = day_bbox[2] - day_bbox[0]
    day_ink_h = day_bbox[3] - day_bbox[1]
    month_ink_w = month_bbox[2] - month_bbox[0]
    month_ink_h = month_bbox[3] - month_bbox[1]
    time_ink_w = time_bbox[2] - time_bbox[0]
    time_ink_h = time_bbox[3] - time_bbox[1]

    gap_day_month = int(height * 0.04)
    gap_month_time = int(height * 0.04)

    # Vertical block height includes the inkboxes + two gaps.
    block_h = day_ink_h + gap_day_month + month_ink_h
    if time_str:
        block_h += gap_month_time + time_ink_h
    block_top = (height - block_h) // 2

    # Place each element so its *visible* top matches the running cursor,
    # then advance the cursor by the inkbox height + gap.
    cursor_y = block_top
    day_draw_y = cursor_y - day_bbox[1]
    day_x = (width - day_ink_w) // 2 - day_bbox[0]
    draw.text((day_x, day_draw_y), day_str, font=day_font, fill=s.fg)
    cursor_y += day_ink_h + gap_day_month

    if month_str:
        month_draw_y = cursor_y - month_bbox[1]
        month_x = (width - month_ink_w) // 2 - month_bbox[0]
        draw.text((month_x, month_draw_y), month_str, font=month_font, fill=s.fg)
        cursor_y += month_ink_h + gap_month_time

    if time_str:
        time_draw_y = cursor_y - time_bbox[1]
        time_x = (width - time_ink_w) // 2 - time_bbox[0]
        draw.text((time_x, time_draw_y), time_str, font=time_font, fill=s.fg)

    img = _rotate_about_center(img, s.rotation_deg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    return out_path


def render_location_card(
    out_path: Path,
    location_name: str,
    address: str | None,
    city: str | None,
    *,
    width: int | None = None,
    height: int | None = None,
    style: StickerStyle | None = None,
) -> Path:
    """Venue name (large) + address line + city. Druk Cyr Bold / Cygre.

    Auto-sizes the card to fit the actual text instead of locking to a
    fixed `880x380`. Width is derived from the longest measured line of
    text; height from the actual text stack. The caller's `width` /
    `height` arguments, when provided, act as *minimum* dimensions —
    we never shrink below them, but we will grow when text demands it.
    """
    s = style or StickerStyle()
    # Use a transparent throwaway draw to measure text geometry first.
    measure_canvas = Image.new("RGBA", (8, 8))
    mdraw = ImageDraw.Draw(measure_canvas)

    # Tentative font sizes anchored to a target 280px-tall card; we'll
    # rescale dimensions once we know the actual content.
    nominal_h = 280
    name_font = _font(FONT_DRUK_BOLD, size=int(nominal_h * 0.30))
    line_font = _font(FONT_CYGRE_SEMIBOLD + FONT_CYGRE_MEDIUM, size=int(nominal_h * 0.16))

    name = (location_name or "").strip().upper()
    extras = [p for p in (address, city) if p and p.strip()]
    extra_text = " · ".join(extras) if extras else ""

    # Pad ratios.
    pad_x_ratio = 0.06
    pad_y_ratio = 0.10

    # Probe candidate widths: try a "comfortable" width first; if the
    # name doesn't fit on 2 lines, expand. We measure widths at the
    # provisional font sizes — the final font sizes scale with the
    # eventual card height once we know the line count.
    best_w = 360
    while True:
        avail = best_w - 2 * int(best_w * pad_x_ratio)
        name_lines = _wrap(name, name_font, avail, mdraw)
        extras_lines = _wrap(extra_text, line_font, avail, mdraw) if extra_text else []
        if (len(name_lines) <= 2 and len(extras_lines) <= 2) or best_w >= 920:
            break
        best_w += 80

    # Re-measure final widths used by content.
    inner_w = max(
        max((_measure(mdraw, ln, name_font)[0] for ln in name_lines), default=0),
        max((_measure(mdraw, ln, line_font)[0] for ln in extras_lines), default=0),
    )
    card_w = max(width or 0, inner_w + 2 * int(best_w * pad_x_ratio))

    name_line_h = _measure(mdraw, "Mg", name_font)[1] if name_lines else 0
    extra_line_h = _measure(mdraw, "Mg", line_font)[1] if extras_lines else 0
    text_block_h = (
        len(name_lines) * int(name_line_h * 1.08)
        + (int(nominal_h * 0.04) if extras_lines else 0)
        + len(extras_lines) * int(extra_line_h * 1.15)
    )
    pad_y = int(nominal_h * pad_y_ratio)
    card_h = max(height or 0, text_block_h + 2 * pad_y)

    img = _draw_card((card_w, card_h), s)
    draw = ImageDraw.Draw(img)
    pad_x = int(card_w * pad_x_ratio)
    y = pad_y
    for line in name_lines[:2]:
        draw.text((pad_x, y), line, font=name_font, fill=s.fg)
        line_h = _measure(draw, line, name_font)[1]
        y += int(line_h * 1.08)
    if extras_lines:
        y += int(nominal_h * 0.04)
        for line in extras_lines[:2]:
            draw.text((pad_x, y), line, font=line_font, fill=s.fg)
            line_h = _measure(draw, line, line_font)[1]
            y += int(line_h * 1.15)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    return out_path


def render_free_card(
    out_path: Path,
    *,
    width: int = 520,
    height: int = 260,
    style: StickerStyle | None = None,
) -> Path:
    """The big yellow «БЕСПЛАТНО» tag for free events."""
    s = style or StickerStyle(
        bg=(244, 217, 71, 255),  # CherryFlash yellow `#F4D947`
        fg=(20, 14, 14, 255),
        corner_radius=18,
        paper_grain=True,
    )
    img = _draw_card((width, height), s)
    draw = ImageDraw.Draw(img)
    font = _font(FONT_DRUK_HEAVY + FONT_DRUK_BOLD, size=int(height * 0.55))
    text = "БЕСПЛАТНО"
    tw, th = _measure(draw, text, font)
    draw.text(((width - tw) // 2, (height - th) // 2 - int(height * 0.06)), text, font=font, fill=s.fg)
    img = _rotate_about_center(img, s.rotation_deg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    return out_path


def render_price_card(
    out_path: Path,
    price_text: str,
    *,
    width: int = 520,
    height: int = 240,
    style: StickerStyle | None = None,
) -> Path:
    s = style or StickerStyle()
    img = _draw_card((width, height), s)
    draw = ImageDraw.Draw(img)
    # Cygre Bold ships the ruble glyph in its modern Cyrillic spec; Bebas
    # Neue does not. Pick whichever supports the actual text being drawn.
    font = _font(FONT_CYGRE_BOLD + FONT_BEBAS_BOLD, size=int(height * 0.55))
    price_text = _money_text(price_text, font)
    tw, th = _measure(draw, price_text, font)
    draw.text(((width - tw) // 2, (height - th) // 2 - int(height * 0.05)), price_text, font=font, fill=s.fg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    return out_path


def render_title_card(
    out_path: Path,
    title: str,
    *,
    width: int | None = None,
    height: int | None = None,
    style: StickerStyle | None = None,
) -> Path:
    """Banner-style title sticker for multi-image / no-poster events.

    Round-8 contract: the card is a WIDE BANNER (aspect ≥ 3.5:1), not a
    near-square plate. The operator placement code lifts this banner
    ABOVE the primary image, like a PowerPoint slide header.

    Width and height adapt to the actual title text:
      - Target a single line at a large size when the title fits.
      - Allow up to two lines if the title is genuinely long.
      - Keep the aspect wide (≥ 3.5:1) so the 3D plane will look like
        a banner rather than a square card.
    """
    s = style or StickerStyle()
    measure_canvas = Image.new("RGBA", (8, 8))
    mdraw = ImageDraw.Draw(measure_canvas)

    text = title.upper().strip()
    # Probe a sequence of font sizes and pick the largest that lets the
    # title fit in 1 or 2 lines while keeping the banner aspect wide.
    pad_x_ratio = 0.04
    pad_y_ratio = 0.20

    def _candidate(font_size: int) -> tuple[int, int, list[str], ImageFont.FreeTypeFont]:
        font = _font(FONT_DRUK_HEAVY + FONT_DRUK_BOLD, size=font_size)
        ink_w, ink_h = _measure(mdraw, text, font)
        line_h = max(ink_h, _measure(mdraw, "Mg", font)[1])
        # One-line variant. The banner aspect is bounded BOTH below
        # (≥ 3.5 — otherwise reads as a square card, not a banner)
        # AND above (≤ 6.0 — anything wider stretches the typography
        # visually on the rendered cylinder / Mercator).
        card_w_1 = int(ink_w * (1 + 2 * pad_x_ratio))
        card_h_1 = int(line_h * (1 + 2 * pad_y_ratio))
        one_line_aspect = card_w_1 / max(1, card_h_1)
        if 3.5 <= one_line_aspect <= 6.0:
            return (card_w_1, card_h_1, [text], font)
        # Two-line wrap whenever the one-line aspect is outside [3.5, 6.0].
        # Pick a target aspect of 4.5 (mid of the allowed range) and let
        # the wrap fall out naturally.
        target_aspect = 4.5
        target_card_h = int(line_h * (2 * 1.05 + 2 * pad_y_ratio))
        target_card_w = int(target_card_h * target_aspect)
        avail = target_card_w - 2 * int(target_card_w * pad_x_ratio)
        lines = _wrap(text, font, avail, mdraw)
        if len(lines) <= 2:
            return (target_card_w, target_card_h, lines, font)
        return (0, 0, [], font)

    chosen: tuple[int, int, list[str], ImageFont.FreeTypeFont] | None = None
    for fs in (88, 72, 60, 50, 42, 36, 30):
        cand = _candidate(fs)
        if cand[2]:
            chosen = cand
            break
    if chosen is None:
        # Fallback: tiny size, force two lines.
        font = _font(FONT_DRUK_BOLD, size=26)
        avail = 900
        lines = _wrap(text, font, avail, mdraw)[:2]
        line_h = _measure(mdraw, "Mg", font)[1]
        card_w = avail + 2 * int(avail * pad_x_ratio)
        card_h = int(line_h * (len(lines) * 1.05 + 2 * pad_y_ratio))
        chosen = (card_w, card_h, lines, font)

    card_w, card_h, lines, font = chosen
    if width is not None:
        card_w = max(card_w, width)
    if height is not None:
        card_h = max(card_h, height)

    img = _draw_card((card_w, card_h), s)
    draw = ImageDraw.Draw(img)
    pad_x = int(card_w * pad_x_ratio)
    # Vertical-centre the line stack.
    line_h_render = _measure(mdraw, "Mg", font)[1]
    stack_h = len(lines) * int(line_h_render * 1.05)
    y = (card_h - stack_h) // 2
    for line in lines:
        line_w = _measure(draw, line, font)[0]
        x = (card_w - line_w) // 2  # centre each line horizontally
        draw.text((x, y), line, font=font, fill=s.fg)
        y += int(line_h_render * 1.05)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    return out_path


def render_digest_card(
    out_path: Path,
    digest_text: str,
    *,
    width: int = 920,
    height: int = 480,
    style: StickerStyle | None = None,
) -> Path:
    """`search_digest` fallback (16-20 word event description). Cygre.

    Uses a fixed line height derived from font metrics (`ascent + descent`)
    so the gap between lines stays constant — the previous bbox-per-line
    approach varied with descenders and read as broken pasted strips
    rather than a single continuous information card.
    """
    s = style or StickerStyle()
    img = _draw_card((width, height), s)
    draw = ImageDraw.Draw(img)
    pad_x = int(width * 0.07)
    pad_y = int(height * 0.10)
    font = _font(FONT_CYGRE_SEMIBOLD + FONT_CYGRE_MEDIUM, size=int(height * 0.10))
    lines = _wrap(digest_text, font, width - 2 * pad_x, draw)
    ascent, descent = font.getmetrics()
    line_h = ascent + descent
    line_step = int(line_h * 1.22)
    y = pad_y
    for line in lines[:6]:
        draw.text((pad_x, y), line, font=font, fill=s.fg)
        y += line_step
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    return out_path


def render_attention_card(
    out_path: Path,
    *,
    width: int = 660,
    height: int = 220,
    style: StickerStyle | None = None,
) -> Path:
    """Promo «обратите внимание» tag, condensed bold caps."""
    s = style or StickerStyle(
        bg=(220, 32, 38, 255),  # red attention
        fg=(255, 252, 240, 255),
        corner_radius=12,
        rotation_deg=-6.0,
        paper_grain=True,
    )
    img = _draw_card((width, height), s)
    draw = ImageDraw.Draw(img)
    font = _font(FONT_DRUK_HEAVY + FONT_DRUK_BOLD, size=int(height * 0.42))
    text = "ОБРАТИТЕ ВНИМАНИЕ"
    tw, th = _measure(draw, text, font)
    if tw > width - 32:
        font = _font(FONT_DRUK_BOLD, size=int(height * 0.32))
        tw, th = _measure(draw, text, font)
    draw.text(((width - tw) // 2, (height - th) // 2 - int(height * 0.05)), text, font=font, fill=s.fg)
    img = _rotate_about_center(img, s.rotation_deg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    return out_path


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _wrap(text: str, font: ImageFont.FreeTypeFont, max_w: int, draw: ImageDraw.ImageDraw) -> list[str]:
    """Greedy word-wrap. Returns a list of lines that each fit in max_w."""
    words = (text or "").split()
    if not words:
        return []
    lines: list[str] = []
    cur = words[0]
    for w in words[1:]:
        candidate = cur + " " + w
        cw, _ = _measure(draw, candidate, font)
        if cw <= max_w:
            cur = candidate
        else:
            lines.append(cur)
            cur = w
    lines.append(cur)
    return lines


__all__ = [
    "StickerStyle",
    "render_date_card",
    "render_location_card",
    "render_free_card",
    "render_price_card",
    "render_title_card",
    "render_digest_card",
    "render_attention_card",
]
