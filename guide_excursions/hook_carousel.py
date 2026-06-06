"""Build the VK *carousel* of slides for a guide excursions digest.

Three slide kinds, chosen per image:
- a media that is already an **afisha** (a finished poster with its own text) is
  shown in full with just a counter + a "листай" badge (``render_afisha_slide``);
- a **plain photo** (no text) gets a marketing question hook in a bottom block
  (``render_carousel_slide``);
- a strong hook for an event that has **no usable photo** becomes a text-only
  hook card (``render_hook_only_slide``).

The last slide is always a CTA pointing down to the post text. Afisha-vs-photo is
decided per image by a small vision classifier (GPT-4o vision) with a safe
fallback (treat as afisha → never cover text with a hook block).

Everything here is best-effort: callers must treat any exception as "no carousel"
and fall back to the plain afisha-grid digest.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Sequence

from .hook_card_render import (
    CardPalette,
    render_afisha_slide,
    render_carousel_slide,
    render_cta_slide,
    render_hook_only_slide,
)
from .hook_cards import generate_hook_cards, select_post_palette

logger = logging.getLogger(__name__)

# VK carousels render up to 10 images; keep a couple in reserve.
CAROUSEL_MAX_SLIDES = 10
CAROUSEL_MAX_TEXT_CARDS = 2


def _slide_jpeg(png: bytes) -> bytes:
    """Re-encode a rendered PNG slide to JPEG (smaller, fine for VK)."""
    from PIL import Image

    img = Image.open(io.BytesIO(png)).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=88, optimize=True)
    return buf.getvalue()


async def _default_classify_afisha(image_bytes: bytes) -> bool:
    """True if the image is an afisha/poster with its own overlaid text.

    Uses GPT-4o vision. On any error returns True (treat as afisha → we never
    cover existing text with a hook block).
    """
    token = (os.getenv("FOUR_O_TOKEN") or "").strip()
    if not token:
        return True
    url = os.getenv("FOUR_O_URL", "https://api.openai.com/v1/chat/completions")
    b64 = base64.b64encode(image_bytes).decode()
    payload = {
        "model": "gpt-4o",
        "max_tokens": 5,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Это афиша/постер с заметным наложенным текстом (заголовок, дата) "
                            "ИЛИ обычная фотография без текста? Ответь одним словом: АФИША или ФОТО."
                        ),
                    },
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                ],
            }
        ],
    }
    try:
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=payload,
                timeout=aiohttp.ClientTimeout(total=40),
            ) as resp:
                data = await resp.json()
                ans = (data["choices"][0]["message"]["content"] or "").strip().upper()
                return ("ФОТО" not in ans) or ("АФИША" in ans)
    except Exception as exc:
        logger.warning("hook_carousel: afisha classify failed: %s", exc)
        return True


def _media_photo_paths(media_items: Sequence[Mapping[str, Any]] | None) -> list[tuple[int, Path]]:
    out: list[tuple[int, Path]] = []
    for item in media_items or []:
        asset = item.get("media_asset") if isinstance(item.get("media_asset"), Mapping) else {}
        ref = item.get("media_ref") if isinstance(item.get("media_ref"), Mapping) else {}
        kind = (asset.get("kind") or ref.get("kind") or "photo")
        if str(kind) != "photo":
            continue
        path = asset.get("path")
        occ = int(item.get("occurrence_id") or 0)
        if not path or not occ:
            continue
        p = Path(str(path))
        if p.is_file():
            out.append((occ, p))
    return out


async def build_carousel_slides(
    rows: Sequence[Mapping[str, Any]],
    media_items: Sequence[Mapping[str, Any]] | None,
    *,
    seed: int = 0,
    ask_fn: Callable[..., Awaitable[str]] | None = None,
    classify_fn: Callable[[bytes], Awaitable[bool]] | None = None,
    max_slides: int = CAROUSEL_MAX_SLIDES,
    max_text_cards: int = CAROUSEL_MAX_TEXT_CARDS,
) -> list[bytes]:
    """Return ordered JPEG slide bytes for the carousel (photos+hooks, afishas,
    a few text-only hook cards, then a CTA), or ``[]`` if nothing usable."""
    classify_fn = classify_fn or _default_classify_afisha
    palette: CardPalette = select_post_palette(seed=seed)

    by_id: dict[int, Mapping[str, Any]] = {}
    for r in rows:
        oid = int(r.get("occurrence_id") or r.get("id") or 0)
        if oid:
            by_id[oid] = r

    media = _media_photo_paths(media_items)
    media_occ_ids = {occ for occ, _ in media}

    # classify each media image
    afisha_media: list[tuple[int, bytes]] = []
    photo_media: list[tuple[int, bytes]] = []
    for occ, path in media:
        try:
            data = path.read_bytes()
        except Exception:
            continue
        is_afisha = True
        try:
            is_afisha = await classify_fn(data)
        except Exception as exc:
            logger.warning("hook_carousel: classify error occ=%s: %s", occ, exc)
        (afisha_media if is_afisha else photo_media).append((occ, data))

    # hooks for the plain-photo events (sorted by strength)
    photo_rows = [by_id[occ] for occ, _ in photo_media if occ in by_id]
    photo_hooks: dict[int, tuple[str, str | None]] = {}
    if photo_rows:
        cards = await generate_hook_cards(
            photo_rows, existing_image_count=0, max_cards=len(photo_rows), seed=seed, ask_fn=ask_fn
        )
        for c in cards:
            photo_hooks[c.occurrence_id] = (c.main_text, c.sub_text)
        photo_strength_order = [c.occurrence_id for c in cards]
    else:
        photo_strength_order = []

    # text-only hook cards for the strongest image-less events
    imageless_rows = [
        r for oid, r in by_id.items() if oid not in media_occ_ids
    ]
    text_cards: list[Any] = []
    if imageless_rows and max_text_cards > 0:
        text_cards = await generate_hook_cards(
            imageless_rows, existing_image_count=0, max_cards=max_text_cards, seed=seed, ask_fn=ask_fn
        )

    data_by_occ = {occ: data for occ, data in (photo_media + afisha_media)}

    # ---- assemble ordered slide specs ----
    specs: list[tuple[str, Any]] = []
    for occ in photo_strength_order:               # plain photos with a hook
        specs.append(("photo", occ))
    for occ, _ in photo_media:                     # plain photos whose hook was rejected
        if occ not in photo_hooks:
            specs.append(("afisha", occ))
    for occ, _ in afisha_media:                    # real afishas
        specs.append(("afisha", occ))
    for c in text_cards:                           # text-only hook cards
        specs.append(("text", c))

    if not specs:
        return []

    # reserve one slot for the CTA
    specs = specs[: max(0, max_slides - 1)]
    total = len(specs) + 1

    slides: list[bytes] = []
    for idx, (kind, payload) in enumerate(specs, start=1):
        try:
            if kind == "photo":
                hook, footer = photo_hooks[payload]
                png = render_carousel_slide(
                    photo=data_by_occ[payload], hook=hook, footer=footer, palette=palette,
                    swipe=True, index=idx, total=total, edge_seed=payload,
                )
            elif kind == "afisha":
                png = render_afisha_slide(
                    afisha=data_by_occ[payload], palette=palette, index=idx, total=total, swipe=True
                )
            else:  # text-only hook card
                png = render_hook_only_slide(
                    hook=payload.main_text, footer=payload.sub_text, palette=palette,
                    swipe=True, index=idx, total=total,
                )
            slides.append(_slide_jpeg(png))
        except Exception as exc:
            logger.warning("hook_carousel: slide render failed kind=%s: %s", kind, exc)

    if len(slides) < 1:
        return []

    try:
        cta = render_cta_slide(palette=palette, index=total, total=total)
        slides.append(_slide_jpeg(cta))
    except Exception as exc:
        logger.warning("hook_carousel: CTA render failed: %s", exc)

    logger.info(
        "hook_carousel.built slides=%s photos=%s afishas=%s text=%s palette=%s",
        len(slides), len(photo_hooks), len(afisha_media), len(text_cards), palette.id,
    )
    return slides
