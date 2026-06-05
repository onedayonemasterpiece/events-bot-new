"""Render VK "engagement"/hook cards (1080x1080) for the guide excursions digest.

These cards carry a single marketing curiosity hook (a question or intriguing
phrase) drawn over a flat, high-contrast editorial background. They are attached
to the VK digest post alongside the real excursion afishas so that the VK grid
shows a catchy phrase straight from the image.

The module is intentionally pure: it takes a fully-resolved :class:`CardPalette`
plus the hook/subline strings and returns PNG bytes. All LLM/selection logic
lives in :mod:`guide_excursions.hook_cards`.
"""

from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Sequence

import math

from PIL import Image, ImageDraw, ImageFont, ImageOps

logger = logging.getLogger(__name__)

_MODULE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _MODULE_DIR.parent
_PALETTES_PATH = _MODULE_DIR / "assets" / "vk_hook_card_palettes.json"
_FONT_DIR = _REPO_ROOT / "assets" / "fonts"
_MAIN_FONT_PATH = _FONT_DIR / "Cygre-ExtraBold.ttf"
_SUB_FONT_PATH = _FONT_DIR / "Cygre-SemiBold.ttf"

# Canvas / layout (mirrors layout_defaults in the palettes JSON).
CANVAS = 1080
SAFE_X0 = 150
SAFE_Y0 = 160
SAFE_W = 780
SAFE_H = 760
SAFE_X1 = SAFE_X0 + SAFE_W
SAFE_Y1 = SAFE_Y0 + SAFE_H

MAIN_MAX_PX = 104
MAIN_MIN_PX = 56
SUB_FIXED_PX = 46          # footer is one fixed size for visual consistency
MAIN_MAX_LINES = 5
SUB_MAX_LINES = 2
MAIN_LINE_HEIGHT = 1.04
SUB_LINE_HEIGHT = 1.06
# Footer area is reserved uniformly so the main hook is fitted against the same
# height on every card — that keeps the main type size (and thus its apparent
# weight/width) identical across all cards in a publication.
SUB_AREA = 196


@dataclass(frozen=True)
class CardPalette:
    """A single validated colour palette for a hook card."""

    id: str
    family: str
    background: str
    text: str
    accent: str
    accent_text: str
    usage: str = ""


def _hex_to_rgb(value: str) -> tuple[int, int, int]:
    text = (value or "").strip().lstrip("#")
    if len(text) == 3:
        text = "".join(ch * 2 for ch in text)
    if len(text) != 6:
        raise ValueError(f"bad hex colour: {value!r}")
    return (int(text[0:2], 16), int(text[2:4], 16), int(text[4:6], 16))


@lru_cache(maxsize=1)
def load_palettes() -> tuple[CardPalette, ...]:
    """Load and cache the validated palette set shipped with the repo."""
    data = json.loads(_PALETTES_PATH.read_text(encoding="utf-8"))
    out: list[CardPalette] = []
    for item in data.get("palettes", []):
        try:
            out.append(
                CardPalette(
                    id=str(item["id"]),
                    family=str(item.get("family") or "default"),
                    background=str(item["background"]),
                    text=str(item["text"]),
                    accent=str(item.get("accent") or item["text"]),
                    accent_text=str(item.get("accent_text") or "#000000"),
                    usage=str(item.get("usage") or ""),
                )
            )
        except Exception:
            logger.warning("hook_card: skipping malformed palette %r", item.get("id"))
    if not out:
        raise RuntimeError("no usable hook-card palettes loaded")
    return tuple(out)


def palette_by_id(palette_id: str) -> CardPalette:
    for palette in load_palettes():
        if palette.id == palette_id:
            return palette
    raise KeyError(palette_id)


@lru_cache(maxsize=64)
def _font(path_str: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path_str, size)


def _wrap(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    """Greedy word wrap; hard-splits any single word wider than ``max_width``."""
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if draw.textlength(candidate, font=font) <= max_width or not current:
            if draw.textlength(candidate, font=font) <= max_width:
                current = candidate
                continue
            # single word too wide -> hard split by characters
            piece = ""
            for ch in word:
                trial = piece + ch
                if draw.textlength(trial, font=font) <= max_width or not piece:
                    piece = trial
                else:
                    lines.append(piece)
                    piece = ch
            current = piece
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def _block_height(lines: Sequence[str], font: ImageFont.FreeTypeFont, line_height: float) -> int:
    ascent, descent = font.getmetrics()
    line_px = ascent + descent
    if not lines:
        return 0
    return int(round(line_px * line_height * (len(lines) - 1) + line_px))


def _fit_lines(
    draw: ImageDraw.ImageDraw,
    text: str,
    font_path: str,
    *,
    max_width: int,
    max_height: int,
    max_px: int,
    min_px: int,
    max_lines: int,
    line_height: float,
) -> tuple[ImageFont.FreeTypeFont, list[str]]:
    """Pick the largest font size in ``[min_px, max_px]`` that fits the box."""
    best: tuple[ImageFont.FreeTypeFont, list[str]] | None = None
    for size in range(max_px, min_px - 1, -2):
        font = _font(font_path, size)
        lines = _wrap(draw, text, font, max_width)
        if len(lines) > max_lines:
            continue
        if _block_height(lines, font, line_height) > max_height:
            continue
        return font, lines
    # Nothing fit cleanly; fall back to the smallest size and accept clipping risk.
    font = _font(font_path, min_px)
    lines = _wrap(draw, text, font, max_width)
    best = (font, lines[:max_lines])
    return best


def _draw_centered_block(
    draw: ImageDraw.ImageDraw,
    lines: Sequence[str],
    font: ImageFont.FreeTypeFont,
    *,
    top: int,
    fill: tuple[int, int, int],
    line_height: float,
) -> int:
    ascent, descent = font.getmetrics()
    line_px = ascent + descent
    step = line_px * line_height
    y = top
    for line in lines:
        w = draw.textlength(line, font=font)
        x = SAFE_X0 + (SAFE_W - w) / 2
        draw.text((x, y), line, font=font, fill=fill)
        y += step
    return int(round(top + step * (len(lines) - 1) + line_px))


_SCRATCH_DRAW = ImageDraw.Draw(Image.new("RGB", (CANVAS, CANVAS)))


def main_fit_px(text: str) -> int:
    """Largest main-font size (≤ ``MAIN_MAX_PX``) that fits ``text`` in the box.

    Uses the uniform layout box (``SAFE_H − SUB_AREA``) so a shared size can be
    computed across a publication's cards.
    """
    text = (text or "").strip()
    if not text:
        return MAIN_MAX_PX
    font, _lines = _fit_lines(
        _SCRATCH_DRAW,
        text,
        str(_MAIN_FONT_PATH),
        max_width=SAFE_W,
        max_height=SAFE_H - SUB_AREA,
        max_px=MAIN_MAX_PX,
        min_px=MAIN_MIN_PX,
        max_lines=MAIN_MAX_LINES,
        line_height=MAIN_LINE_HEIGHT,
    )
    return int(font.size)


def render_hook_card(
    *,
    main_text: str,
    sub_text: str | None,
    palette: CardPalette,
    main_px: int | None = None,
) -> bytes:
    """Render a single hook card and return PNG bytes.

    ``main_px`` forces the main type size (so all cards in one post share it);
    when ``None`` the size is auto-fitted for this card alone.
    """
    main_text = (main_text or "").strip()
    if not main_text:
        raise ValueError("hook card requires non-empty main_text")
    sub_text = (sub_text or "").strip() or None

    bg = _hex_to_rgb(palette.background)
    fg = _hex_to_rgb(palette.text)
    accent = _hex_to_rgb(palette.accent)

    image = Image.new("RGB", (CANVAS, CANVAS), bg)
    draw = ImageDraw.Draw(image)

    # Footer (date · guide): one fixed size on every card.
    rule_gap = 28
    rule_h = 6
    rule_w = 132
    sub_reserved = 0
    sub_font = None
    sub_lines: list[str] = []
    if sub_text:
        sub_font = _font(str(_SUB_FONT_PATH), SUB_FIXED_PX)
        sub_lines = _wrap(draw, sub_text, sub_font, SAFE_W)[:SUB_MAX_LINES]
        sub_reserved = _block_height(sub_lines, sub_font, SUB_LINE_HEIGHT) + rule_gap + rule_h + rule_gap

    # Main hook: shared size across the post when main_px is given; the layout
    # box is the same on every card so the size — and apparent weight/width — match.
    size = int(main_px) if main_px else main_fit_px(main_text)
    main_font = _font(str(_MAIN_FONT_PATH), size)
    main_lines = _wrap(draw, main_text, main_font, SAFE_W)[:MAIN_MAX_LINES]

    main_h = _block_height(main_lines, main_font, MAIN_LINE_HEIGHT)
    total_h = main_h + sub_reserved
    top = SAFE_Y0 + max(0, (SAFE_H - total_h) // 2)

    bottom = _draw_centered_block(
        draw, main_lines, main_font, top=top, fill=fg, line_height=MAIN_LINE_HEIGHT
    )

    if sub_text and sub_font is not None:
        rule_y = bottom + rule_gap
        rule_x0 = SAFE_X0 + (SAFE_W - rule_w) / 2
        draw.rounded_rectangle(
            [rule_x0, rule_y, rule_x0 + rule_w, rule_y + rule_h],
            radius=rule_h / 2,
            fill=accent,
        )
        _draw_centered_block(
            draw,
            sub_lines,
            sub_font,
            top=int(rule_y + rule_h + rule_gap),
            fill=fg,
            line_height=SUB_LINE_HEIGHT,
        )

    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


# Multi-event "подборка" card is vertical 3:4 (1080×1440).
MULTI_W = 1080
MULTI_H = 1440
MULTI_MX = 104                # left/right margin
MULTI_TOP = 150
MULTI_BOTTOM = 110
MULTI_HOOK_MAX_PX = 88
MULTI_HOOK_MIN_PX = 48
MULTI_HOOK_MAX_LINES = 3
MULTI_HOOK_LINE_HEIGHT = 1.05
MULTI_FOOTER_PX = 34
MULTI_FOOTER_LINE_HEIGHT = 1.16
MULTI_CTA_PX = 42
DEFAULT_CTA = "Весь дайджест — в посте"


def render_multi_card(
    *,
    hooks: Sequence[str],
    footers: Sequence[str] = (),
    palette: CardPalette,
    cta: str = DEFAULT_CTA,
) -> bytes:
    """Render one vertical 3:4 "подборка" card with several question hooks.

    ``hooks`` are the question lines (1–3). ``footers`` (date · guide) are listed
    once near the bottom, not under each hook. ``cta`` is a short call-to-action
    pill at the very bottom. No title.
    """
    hooks = [h.strip() for h in hooks if h and h.strip()]
    if not hooks:
        raise ValueError("multi card requires at least one hook")
    footers = [f.strip() for f in footers if f and f.strip()]
    cta = (cta or "").strip()

    W, H = MULTI_W, MULTI_H
    sx0 = MULTI_MX
    sw = W - 2 * MULTI_MX

    bg = _hex_to_rgb(palette.background)
    fg = _hex_to_rgb(palette.text)
    accent = _hex_to_rgb(palette.accent)
    accent_text = _hex_to_rgb(palette.accent_text)

    image = Image.new("RGB", (W, H), bg)
    draw = ImageDraw.Draw(image)

    # --- CTA pill pinned to the bottom ---
    cta_top = None
    if cta:
        cta_font = _font(str(_MAIN_FONT_PATH), MULTI_CTA_PX)
        c_asc, c_desc = cta_font.getmetrics()
        pad_x, pad_y = 40, 22
        cta_w = draw.textlength(cta, font=cta_font)
        pill_w = min(cta_w + 2 * pad_x, sw)
        pill_h = (c_asc + c_desc) + 2 * pad_y
        cta_top = H - MULTI_BOTTOM - pill_h

    # --- footer (date · guide) block, once, above the CTA ---
    foot_font = _font(str(_SUB_FONT_PATH), MULTI_FOOTER_PX)
    fo_asc, fo_desc = foot_font.getmetrics()
    foot_line = fo_asc + fo_desc
    foot_block_h = (
        int(foot_line * MULTI_FOOTER_LINE_HEIGHT * (len(footers) - 1) + foot_line)
        if footers else 0
    )
    foot_gap = 48
    bottom_anchor = (cta_top if cta_top is not None else H - MULTI_BOTTOM)
    foot_top = bottom_anchor - (foot_gap + foot_block_h if footers else 0)

    # --- hooks block (auto-fit one size, left-aligned, centred in remaining) ---
    hooks_top = MULTI_TOP
    hooks_bot = foot_top - 48
    avail = hooks_bot - hooks_top
    entry_gap = 44

    def _layout(px: int):
        hf = _font(str(_MAIN_FONT_PATH), px)
        rows, total = [], 0
        for hook in hooks:
            lines = _wrap(draw, hook, hf, sw)
            if len(lines) > MULTI_HOOK_MAX_LINES:
                return None
            hh = _block_height(lines, hf, MULTI_HOOK_LINE_HEIGHT)
            rows.append((lines, hh))
            total += hh
        total += entry_gap * (len(hooks) - 1)
        return (hf, rows, total) if total <= avail else None

    chosen = None
    for px in range(MULTI_HOOK_MAX_PX, MULTI_HOOK_MIN_PX - 1, -2):
        chosen = _layout(px)
        if chosen:
            break
    if chosen is None:
        hf = _font(str(_MAIN_FONT_PATH), MULTI_HOOK_MIN_PX)
        rows, total = [], 0
        for hook in hooks:
            lines = _wrap(draw, hook, hf, sw)[:MULTI_HOOK_MAX_LINES]
            hh = _block_height(lines, hf, MULTI_HOOK_LINE_HEIGHT)
            rows.append((lines, hh))
            total += hh
        total += entry_gap * (len(hooks) - 1)
        chosen = (hf, rows, total)
    hf, rows, total = chosen

    asc, desc = hf.getmetrics()
    step = (asc + desc) * MULTI_HOOK_LINE_HEIGHT
    yy = hooks_top + max(0, (avail - total) // 2)
    for idx, (lines, hh) in enumerate(rows):
        ly = yy
        for line in lines:
            draw.text((sx0, ly), line, font=hf, fill=fg)
            ly += step
        yy += hh
        if idx < len(rows) - 1:
            yy += entry_gap / 2
            draw.rounded_rectangle([sx0, yy, sx0 + 70, yy + 5], radius=2.5, fill=accent)
            yy += entry_gap / 2

    # footers once, near the bottom
    if footers:
        fstep = foot_line * MULTI_FOOTER_LINE_HEIGHT
        fy = foot_top
        for f in footers:
            draw.text((sx0, fy), f, font=foot_font, fill=accent)
            fy += fstep

    # CTA pill (centred), accent fill + accent_text for guaranteed contrast
    if cta:
        px0 = (W - pill_w) / 2
        draw.rounded_rectangle(
            [px0, cta_top, px0 + pill_w, cta_top + pill_h], radius=pill_h / 2, fill=accent
        )
        cw = draw.textlength(cta, font=cta_font)
        draw.text((px0 + (pill_w - cw) / 2, cta_top + pad_y), cta, font=cta_font, fill=accent_text)

    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


# ----------------------------------------------------------------------------- #
# Carousel slides (for events whose source media is a plain photo without text).
# Each slide = photo on top + a bottom "ломаная геометрия" block carrying the
# question hook + date · guide. Non-last slides get a stylish curved "листай"
# swipe arrow; the last slide is an explicit CTA to read the digest.
# Posted as a VK carousel (no primary_attachments_mode=grid), so VK shows each
# slide full instead of center-cropping a 3:4 card to a square grid tile.
# ----------------------------------------------------------------------------- #
# Vertical 4:5 slide (1080×1350): photo fills the top band, a coloured block with a
# slanted broken edge sits below and carries the text. Block height is dynamic.
SLIDE_W = 1080
SLIDE_H = 1350
SLIDE_MX = 84
SLIDE_HOOK_FONT_PATH = _FONT_DIR / "Cygre-Bold.ttf"   # a touch lighter than ExtraBold
SLIDE_HOOK_MAX_PX = 80
SLIDE_HOOK_MIN_PX = 48
SLIDE_FOOTER_PX = 36
SLIDE_HOOK_LH = 1.1
DEFAULT_SLIDE_CTA_HEADLINE = "Куда сходить в Калининграде?"
DEFAULT_SLIDE_CTA = "Все экскурсии, даты и запись — в тексте поста ниже"


def _quad_bezier(p0, p1, p2, steps: int = 48):
    pts = []
    for i in range(steps + 1):
        t = i / steps
        mt = 1 - t
        x = mt * mt * p0[0] + 2 * mt * t * p1[0] + t * t * p2[0]
        y = mt * mt * p0[1] + 2 * mt * t * p1[1] + t * t * p2[1]
        pts.append((x, y))
    return pts


def _broken_edge(y_base: int) -> list[tuple[int, int]]:
    """An asymmetric rising slash with one kink — modern, not a mountain range."""
    W = SLIDE_W
    return [(0, y_base + 64), (int(W * 0.58), y_base + 2), (int(W * 0.64), y_base + 2), (W, y_base - 62)]


def _fit_block_text(draw, text, font_path, *, max_width, max_height, max_px, min_px, max_lines, line_height):
    for size in range(max_px, min_px - 1, -2):
        font = _font(font_path, size)
        lines = _wrap(draw, text, font, max_width)
        if len(lines) <= max_lines and _block_height(lines, font, line_height) <= max_height:
            return font, lines
    font = _font(font_path, min_px)
    return font, _wrap(draw, text, font, max_width)[:max_lines]


def _draw_listai(draw: ImageDraw.ImageDraw, right_x: int, mid_y: int, palette: CardPalette):
    """Small 'листай' + a clean curved arrow, right-aligned, inside the block."""
    accent = _hex_to_rgb(palette.accent)
    f = _font(str(_SUB_FONT_PATH), 34)
    label = "листай"
    lw = draw.textlength(label, font=f)
    fa, fd = f.getmetrics()
    aw = 60
    x0 = right_x - (lw + 16 + aw)
    draw.text((x0, mid_y - (fa + fd) / 2), label, font=f, fill=accent)
    ax0 = x0 + lw + 16
    p0 = (ax0, mid_y + 8)
    p1 = ((ax0 + right_x) / 2, mid_y - 14)
    p2 = (right_x, mid_y - 2)
    pts = _quad_bezier(p0, p1, p2, steps=32)
    draw.line(pts, fill=accent, width=9, joint="curve")
    hx, hy = pts[-1]
    gx, gy = pts[-5]
    ang = math.atan2(hy - gy, hx - gx)
    L, sp = 24, math.radians(34)
    a1 = (hx - L * math.cos(ang - sp), hy - L * math.sin(ang - sp))
    a2 = (hx - L * math.cos(ang + sp), hy - L * math.sin(ang + sp))
    draw.polygon([(hx, hy), a1, a2], fill=accent)


def _draw_down_squiggle(draw: ImageDraw.ImageDraw, cx: int, y0: int, y1: int, palette: CardPalette):
    """A playful dashed wavy arrow pointing down (towards the post text)."""
    accent = _hex_to_rgb(palette.accent)
    n = 56
    pts = [(cx + math.sin(i / n * math.pi * 3) * 26, y0 + (y1 - y0) * i / n) for i in range(n + 1)]
    for i in range(0, len(pts) - 2, 3):  # dashed
        draw.line([pts[i], pts[i + 1]], fill=accent, width=9)
    bx, by = pts[-1]
    draw.polygon([(bx, by + 8), (bx - 20, by - 22), (bx + 20, by - 22)], fill=accent)


def render_carousel_slide(
    *,
    photo: bytes,
    hook: str,
    footer: str | None,
    palette: CardPalette,
    swipe: bool = True,
) -> bytes:
    """Vertical 4:5 slide: photo band on top, dynamic-height angular text block below."""
    hook = (hook or "").strip()
    footer = (footer or "").strip() or None

    bg = _hex_to_rgb(palette.background)
    fg = _hex_to_rgb(palette.text)
    accent = _hex_to_rgb(palette.accent)

    inner_w = SLIDE_W - 2 * SLIDE_MX - 38  # leave room for the accent tab
    scratch = _SCRATCH_DRAW
    hook_font, hook_lines = _fit_block_text(
        scratch, hook, str(SLIDE_HOOK_FONT_PATH),
        max_width=inner_w, max_height=420,
        max_px=SLIDE_HOOK_MAX_PX, min_px=SLIDE_HOOK_MIN_PX,
        max_lines=3, line_height=SLIDE_HOOK_LH,
    )
    ha, hd = hook_font.getmetrics()
    hook_h = _block_height(hook_lines, hook_font, SLIDE_HOOK_LH)
    foot_font = _font(str(_SUB_FONT_PATH), SLIDE_FOOTER_PX)
    fa, fd = foot_font.getmetrics()
    footer_h = fa + fd

    pad_top = 86          # below the slash to the first hook line
    pad_gap = 30          # hook -> footer
    pad_bottom = 92
    block_h = pad_top + hook_h + pad_gap + footer_h + pad_bottom
    y_base = SLIDE_H - block_h
    y_base = max(int(SLIDE_H * 0.42), min(y_base, int(SLIDE_H * 0.72)))

    # photo as a top band (not behind the block) — balanced centre crop so the
    # subject is not swallowed by the block.
    band_h = y_base + 70
    image = Image.new("RGB", (SLIDE_W, SLIDE_H), bg)
    band = ImageOps.fit(Image.open(io.BytesIO(photo)).convert("RGB"), (SLIDE_W, band_h), method=Image.LANCZOS)
    image.paste(band, (0, 0))
    draw = ImageDraw.Draw(image)

    # angular block with a thin accent rim tracing the slash (depth)
    edge = _broken_edge(y_base)
    accent_edge = [(x, y - 14) for x, y in edge]
    draw.polygon(accent_edge + [(SLIDE_W, SLIDE_H), (0, SLIDE_H)], fill=accent)
    draw.polygon(edge + [(SLIDE_W, SLIDE_H), (0, SLIDE_H)], fill=bg)

    text_x = SLIDE_MX + 38
    hook_top = max(p[1] for p in edge) + pad_top - 24
    # accent tab to the left of the hook (replaces the old marker/diamonds)
    draw.rounded_rectangle([SLIDE_MX, hook_top + 6, SLIDE_MX + 12, hook_top + hook_h - 6], radius=6, fill=accent)

    step = (ha + hd) * SLIDE_HOOK_LH
    y = hook_top
    for line in hook_lines:
        draw.text((text_x, y), line, font=hook_font, fill=fg)
        y += step

    foot_y = hook_top + hook_h + pad_gap
    if footer:
        draw.text((text_x, foot_y), footer, font=foot_font, fill=accent)
    if swipe:
        _draw_listai(draw, SLIDE_W - SLIDE_MX, int(foot_y + footer_h / 2), palette)

    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def render_cta_slide(
    *,
    palette: CardPalette,
    headline: str = DEFAULT_SLIDE_CTA_HEADLINE,
    cta: str = DEFAULT_SLIDE_CTA,
) -> bytes:
    """Final carousel slide (vertical 4:5): explicit CTA + a wavy arrow down to the post."""
    bg = _hex_to_rgb(palette.background)
    fg = _hex_to_rgb(palette.text)
    accent = _hex_to_rgb(palette.accent)

    image = Image.new("RGB", (SLIDE_W, SLIDE_H), bg)
    draw = ImageDraw.Draw(image)
    inner_w = SLIDE_W - 2 * SLIDE_MX

    head_font, head_lines = _fit_block_text(
        draw, headline.strip(), str(_MAIN_FONT_PATH),
        max_width=inner_w, max_height=int(SLIDE_H * 0.34),
        max_px=100, min_px=56, max_lines=4, line_height=1.06,
    )
    cta_font, cta_lines = _fit_block_text(
        draw, cta.strip(), str(_SUB_FONT_PATH),
        max_width=inner_w, max_height=int(SLIDE_H * 0.2),
        max_px=50, min_px=34, max_lines=3, line_height=1.16,
    )
    head_h = _block_height(head_lines, head_font, 1.06)
    cta_h = _block_height(cta_lines, cta_font, 1.16)
    squiggle_h = 168
    gap = 70
    total = head_h + gap + cta_h + 64 + squiggle_h
    top = int(SLIDE_H * 0.30)

    ha, hd = head_font.getmetrics()
    hstep = (ha + hd) * 1.06
    y = top
    for line in head_lines:
        w = draw.textlength(line, font=head_font)
        draw.text((SLIDE_MX + (inner_w - w) / 2, y), line, font=head_font, fill=fg)
        y += hstep
    ry = top + head_h + gap // 2 - 4
    draw.rounded_rectangle([SLIDE_W / 2 - 70, ry, SLIDE_W / 2 + 70, ry + 7], radius=3.5, fill=accent)

    ca, cd = cta_font.getmetrics()
    cstep = (ca + cd) * 1.16
    y = top + head_h + gap
    for line in cta_lines:
        w = draw.textlength(line, font=cta_font)
        draw.text((SLIDE_MX + (inner_w - w) / 2, y), line, font=cta_font, fill=accent)
        y += cstep

    _draw_down_squiggle(draw, SLIDE_W // 2, int(y + 56), int(y + 56 + squiggle_h), palette)

    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()
