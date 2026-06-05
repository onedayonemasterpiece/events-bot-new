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

from PIL import Image, ImageDraw, ImageFont

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
SUB_MAX_PX = 56
SUB_MIN_PX = 40
MAIN_MAX_LINES = 5
SUB_MAX_LINES = 2
MAIN_LINE_HEIGHT = 1.04
SUB_LINE_HEIGHT = 1.06


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


def render_hook_card(
    *,
    main_text: str,
    sub_text: str | None,
    palette: CardPalette,
) -> bytes:
    """Render a single hook card and return PNG bytes."""
    main_text = (main_text or "").strip()
    if not main_text:
        raise ValueError("hook card requires non-empty main_text")
    sub_text = (sub_text or "").strip() or None

    bg = _hex_to_rgb(palette.background)
    fg = _hex_to_rgb(palette.text)
    accent = _hex_to_rgb(palette.accent)

    image = Image.new("RGB", (CANVAS, CANVAS), bg)
    draw = ImageDraw.Draw(image)

    # Reserve vertical room for an optional subline + accent rule.
    rule_gap = 28
    rule_h = 6
    rule_w = 132
    sub_reserved = 0
    sub_font = None
    sub_lines: list[str] = []
    if sub_text:
        sub_font, sub_lines = _fit_lines(
            draw,
            sub_text,
            str(_SUB_FONT_PATH),
            max_width=SAFE_W,
            max_height=int(SAFE_H * 0.22),
            max_px=SUB_MAX_PX,
            min_px=SUB_MIN_PX,
            max_lines=SUB_MAX_LINES,
            line_height=SUB_LINE_HEIGHT,
        )
        sub_reserved = _block_height(sub_lines, sub_font, SUB_LINE_HEIGHT) + rule_gap + rule_h + rule_gap

    main_font, main_lines = _fit_lines(
        draw,
        main_text,
        str(_MAIN_FONT_PATH),
        max_width=SAFE_W,
        max_height=SAFE_H - sub_reserved,
        max_px=MAIN_MAX_PX,
        min_px=MAIN_MIN_PX,
        max_lines=MAIN_MAX_LINES,
        line_height=MAIN_LINE_HEIGHT,
    )

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
