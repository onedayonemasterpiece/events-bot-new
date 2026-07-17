from __future__ import annotations

import html
import io
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Iterable, Sequence

from .service import PHOTO_ALLOWED_STATUSES

RENDERER_VERSION = "artist-arrival-card-v1"


@dataclass(frozen=True)
class RenderedArtistCard:
    item_key: str
    filename: str
    jpeg: bytes
    used_photo: bool


def _load_font(size: int, *, bold: bool = False):
    from PIL import ImageFont

    candidates = [
        "fonts/Cygre-Bold.ttf" if bold else "fonts/Cygre-Medium.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _wrap(draw, text: str, font, width: int, max_lines: int) -> list[str]:
    words = re.sub(r"\s+", " ", text).strip().split()
    lines: list[str] = []
    current = ""
    for word in words:
        probe = f"{current} {word}".strip()
        if not current or draw.textlength(probe, font=font) <= width:
            current = probe
        else:
            lines.append(current)
            current = word
            if len(lines) >= max_lines:
                break
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) == max_lines and len(" ".join(lines)) < len(" ".join(words)):
        lines[-1] = lines[-1].rstrip(".,") + "…"
    return lines


def _date_label(values: Sequence[str]) -> str:
    parsed = []
    for value in values:
        try:
            parsed.append(date.fromisoformat(str(value)[:10]))
        except ValueError:
            continue
    if not parsed:
        return "Дата — в событии"
    months = [
        "января", "февраля", "марта", "апреля", "мая", "июня",
        "июля", "августа", "сентября", "октября", "ноября", "декабря",
    ]
    if len(parsed) == 1:
        return f"{parsed[0].day} {months[parsed[0].month - 1]}"
    return f"{parsed[0].day}–{parsed[-1].day} {months[parsed[-1].month - 1]}"


def render_artist_arrival_card(
    item: dict[str, Any],
    *,
    source_image: bytes | None = None,
) -> RenderedArtistCard:
    """Render a deterministic shared TG/VK card.

    A photo is used only when the item carries an explicit artist↔media review
    and an allowed rights status. Event association alone is insufficient.
    """

    from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageOps

    width, height = 1080, 1350
    use_photo = bool(
        source_image
        and item.get("media_identity_status") == "verified"
        and item.get("photo_rights_status") in PHOTO_ALLOWED_STATUSES
        and item.get("photo_rights_evidence_ids")
    )
    if use_photo:
        with Image.open(io.BytesIO(source_image)) as opened:
            photo = ImageOps.exif_transpose(opened).convert("RGB")
        photo = ImageOps.fit(photo, (width, height), method=Image.Resampling.LANCZOS, centering=(0.5, 0.42))
        photo = ImageEnhance.Brightness(photo).enhance(0.74)
        canvas = photo
    else:
        canvas = Image.new("RGB", (width, height), "#F6EFE2")
        # Subtle deterministic studio texture, no generated imagery.
        layer = Image.new("L", (width, height), 0)
        ld = ImageDraw.Draw(layer)
        for i in range(12):
            radius = 260 + i * 42
            ld.ellipse((width - radius, -radius // 2, width + radius, radius * 2), fill=8 + i * 3)
        layer = layer.filter(ImageFilter.GaussianBlur(55))
        tint = Image.new("RGB", (width, height), "#B34A24")
        canvas = Image.composite(tint, canvas, layer)

    draw = ImageDraw.Draw(canvas, "RGBA")
    ink = (255, 250, 241, 255) if use_photo else (35, 42, 48, 255)
    accent = (239, 177, 70, 255) if use_photo else (175, 72, 31, 255)
    if use_photo:
        draw.rectangle((0, 0, width, height), fill=(18, 23, 30, 68))
        draw.rectangle((0, 760, width, height), fill=(18, 23, 30, 205))

    kind = "ЗАРУБЕЖНЫЙ ГОСТЬ" if item.get("arrival_kind") == "international" else "ГОСТЬ ИЗ РОССИИ"
    badge_font = _load_font(34, bold=True)
    badge_box = draw.textbbox((0, 0), kind, font=badge_font)
    badge_width = min(width - 140, (badge_box[2] - badge_box[0]) + 54)
    draw.rounded_rectangle((70, 72, 70 + badge_width, 138), radius=24, fill=accent)
    draw.text((96, 87), kind, font=badge_font, fill=(255, 250, 241, 255))

    name = str(item.get("artist_name") or "Артист")
    name_size = 92 if len(name) < 25 else 76
    name_font = _load_font(name_size, bold=True)
    name_lines = _wrap(draw, name, name_font, width - 140, 3)
    y = 580 if use_photo else 390
    for line in name_lines:
        draw.text((70, y), line, font=name_font, fill=ink)
        y += name_size + 8

    project = str(item.get("project_title") or "Событие")
    project_font = _load_font(45)
    for line in _wrap(draw, f"с проектом «{project}»", project_font, width - 140, 3):
        draw.text((70, y + 18), line, font=project_font, fill=ink)
        y += 58

    footer_y = height - 185
    draw.line((70, footer_y, width - 70, footer_y), fill=accent, width=5)
    meta_font = _load_font(34, bold=True)
    venues = list(item.get("venues") or [])
    cities = list(item.get("municipalities") or [])
    place = " · ".join([*(venues[:1]), *(cities[:1])])
    meta = _date_label(item.get("dates") or []) + (f" · {place}" if place else "")
    for line in _wrap(draw, meta, meta_font, width - 220, 2):
        draw.text((70, footer_y + 34), line, font=meta_font, fill=ink)
        footer_y += 42
    arrow_font = _load_font(52, bold=True)
    draw.text((width - 140, height - 128), "→", font=arrow_font, fill=accent)
    credit = str(item.get("photo_credit_text") or "").strip()
    if use_photo and credit:
        credit_font = _load_font(22)
        credit_label = f"Фото: {credit}"
        credit_line = _wrap(draw, credit_label, credit_font, width - 220, 1)[0]
        draw.text((70, height - 42), credit_line, font=credit_font, fill=ink)

    out = io.BytesIO()
    canvas.save(out, format="JPEG", quality=93, optimize=True)
    key = str(item.get("item_key") or "artist")
    safe = re.sub(r"[^0-9a-z_-]+", "-", key.casefold())[:70].strip("-") or "artist"
    return RenderedArtistCard(key, f"artist-arrival-{safe}.jpg", out.getvalue(), use_photo)


def build_telegram_slideshow_html(items: Sequence[dict[str, Any]]) -> str:
    if not 3 <= len(items) <= 10:
        raise ValueError("artist arrival Telegram slideshow requires 3..10 cards")
    media = "".join(f'<img src="tg://photo?id=artist-{idx}"/>' for idx in range(len(items)))
    lines = []
    for idx, item in enumerate(items, start=1):
        name = html.escape(str(item.get("artist_name") or ""))
        project = html.escape(str(item.get("project_title") or ""))
        when = html.escape(_date_label(item.get("dates") or []))
        url = html.escape(str(item.get("event_url") or ""), quote=True)
        label = f"{name} — {project}, {when}"
        lines.append(f'{idx}. <a href="{url}">{label}</a>' if url else f"{idx}. {label}")
    credits = []
    for item in items:
        credit = html.escape(str(item.get("photo_credit_text") or "").strip())
        source_url = html.escape(str(item.get("photo_source_url") or "").strip(), quote=True)
        if credit and source_url:
            credits.append(f'<a href="{source_url}">{credit}</a>')
    credit_block = (
        f"<p><small>Фото: {' · '.join(dict.fromkeys(credits))}</small></p>"
        if credits
        else ""
    )
    return (
        "<h2>Кто приедет в Калининградскую область</h2>"
        f"<tg-slideshow>{media}<figcaption>Артисты и проекты ближайших недель</figcaption></tg-slideshow>"
        f"<p>{'<br>'.join(lines)}</p>"
        f"{credit_block}"
        "<footer>Полюбить Калининград · Анонсы</footer>"
    )


def build_telegram_input_rich_message(
    items: Sequence[dict[str, Any]],
    cards: Sequence[RenderedArtistCard],
):
    from aiogram import types

    if len(items) != len(cards):
        raise ValueError("artist arrival items/cards mismatch")
    media = [
        {
            "id": f"artist-{idx}",
            "media": types.InputMediaPhoto(
                media=types.BufferedInputFile(card.jpeg, filename=card.filename)
            ),
        }
        for idx, card in enumerate(cards)
    ]
    return types.InputRichMessage(html=build_telegram_slideshow_html(items), media=media)


def build_vk_carousel_message(items: Sequence[dict[str, Any]]) -> str:
    lines = ["Кто приедет в Калининградскую область — ближайшие концерты и проекты:"]
    for idx, item in enumerate(items, start=1):
        when = _date_label(item.get("dates") or [])
        line = f"{idx}. {item.get('artist_name')} — {item.get('project_title')}, {when}"
        if item.get("event_url"):
            line += f"\n{item['event_url']}"
        lines.append(line)
    credits = []
    for item in items:
        credit = str(item.get("photo_credit_text") or "").strip()
        source_url = str(item.get("photo_source_url") or "").strip()
        if credit and source_url:
            credits.append(f"{credit} — {source_url}")
    if credits:
        lines.extend(["", "Фото: " + "; ".join(dict.fromkeys(credits))])
    lines.extend(["", "Полюбить Калининград · Анонсы"])
    return "\n\n".join(lines)


def render_issue_cards(items: Iterable[dict[str, Any]]) -> list[RenderedArtistCard]:
    # Media retrieval/identity verification is a separate reviewed step. A
    # deterministic text card is always available for shadow review.
    return [render_artist_arrival_card(item) for item in items]
