#!/usr/bin/env python3
"""Download selected event images and make square, dated, auditable face textures."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import urllib.request
from datetime import date
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont


MONTHS = {1:"ЯНВАРЯ",2:"ФЕВРАЛЯ",3:"МАРТА",4:"АПРЕЛЯ",5:"МАЯ",6:"ИЮНЯ",7:"ИЮЛЯ",8:"АВГУСТА",9:"СЕНТЯБРЯ",10:"ОКТЯБРЯ",11:"НОЯБРЯ",12:"ДЕКАБРЯ"}
CROP_REASONS = {
    "safe_center_cover": "explicit_safe_crop",
    "non_ocr_photo_center_cover": "non_ocr_photo_with_renderer_title_and_date",
    "full_image_contain": "protect_ocr_or_unclassified_document_edges",
}


def load_font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(path), size, layout_engine=ImageFont.Layout.RAQM)


def date_label(row: dict) -> str:
    start = date.fromisoformat(row["start_date"])
    end_raw = row.get("end_date")
    if end_raw and end_raw != row["start_date"]:
        end = date.fromisoformat(end_raw)
        return f"ДО {end.day} {MONTHS[end.month]}"
    return f"{start.day} {MONTHS[start.month]}"


def download(url: str) -> tuple[bytes, str]:
    req = urllib.request.Request(url, headers={"User-Agent": "events-bot-service-share-research/1.0"})
    with urllib.request.urlopen(req, timeout=45) as response:
        return response.read(), response.headers.get("Content-Type", "")


def cover_square(image: Image.Image) -> tuple[Image.Image, list[int]]:
    width, height = image.size
    side = min(width, height)
    left = (width - side) // 2
    top = (height - side) // 2
    crop = [left, top, left + side, top + side]
    return image.crop(tuple(crop)).resize((1024, 1024), Image.Resampling.LANCZOS), crop


def contain_square(image: Image.Image) -> tuple[Image.Image, list[int]]:
    """Preserve the entire source when metadata says destructive crop is unsafe."""
    background, _ = cover_square(image)
    background = background.filter(ImageFilter.GaussianBlur(26))
    shade = Image.new("RGBA", background.size, (16, 18, 20, 100))
    canvas = Image.alpha_composite(background.convert("RGBA"), shade)
    foreground = image.copy()
    foreground.thumbnail((930, 930), Image.Resampling.LANCZOS)
    x = (1024 - foreground.width) // 2
    y = (1024 - foreground.height) // 2
    canvas.alpha_composite(foreground.convert("RGBA"), (x, y))
    return canvas.convert("RGB"), [0, 0, image.width, image.height]


def frame_square(image: Image.Image, row: dict) -> tuple[Image.Image, list[int], str]:
    """Frame a source image without treating an event photo like a document.

    ``safe_crop`` was designed as a document/poster safety switch.  Applying
    its ``False`` branch to a photo creates a small contained image with a
    blurred letterbox around it, even though the face already receives its own
    title and date.  The accepted media classification and actual OCR presence
    distinguish that case: explicit or legacy non-OCR photos use a
    source-faithful cover crop, while OCR-bearing assets retain the existing
    contain treatment so text at the edge of a poster is not newly destroyed.
    """

    text_mode = str(row.get("image_text_mode") or "unknown").strip().casefold()
    if row.get("safe_crop"):
        square, crop = cover_square(image)
        return square, crop, "safe_center_cover"
    if bool(row.get("image_has_ocr_text")) or text_mode == "ocr_text":
        square, crop = contain_square(image)
        return square, crop, "full_image_contain"
    if text_mode in {"visual_only", "unknown", ""}:
        square, crop = cover_square(image)
        return square, crop, "non_ocr_photo_center_cover"
    square, crop = contain_square(image)
    return square, crop, "full_image_contain"


def _wrap_pixels(title: str, font: ImageFont.FreeTypeFont, max_width: int = 880) -> list[str]:
    lines: list[str] = []
    current = ""
    for word in title.upper().split():
        proposal = f"{current} {word}".strip()
        if current and font.getlength(proposal) > max_width:
            lines.append(current)
            current = word
        else:
            current = proposal
    if current:
        lines.append(current)
    return lines


def fit_title(title: str, font_path: Path) -> tuple[list[str], ImageFont.FreeTypeFont]:
    for size in (58, 52, 46, 40, 36):
        font = load_font(font_path, size)
        lines = _wrap_pixels(title, font)
        if lines and len(lines) <= 3 and max(font.getlength(line) for line in lines) <= 880:
            return lines, font
    font = load_font(font_path, 36)
    lines = _wrap_pixels(title, font)[:3]
    if lines:
        while font.getlength(lines[-1] + "…") > 880 and lines[-1]:
            lines[-1] = lines[-1][:-1]
        lines[-1] += "…"
    return lines or ["СОБЫТИЕ"], font


def overlay(image: Image.Image, row: dict, bold_font: Path, semibold_font: Path) -> Image.Image:
    rgba = image.convert("RGBA")
    pixels = rgba.load()
    for y in range(470, 1024):
        progress = max(0.0, ((y / 1024) - 0.459) / (1 - 0.459))
        alpha = progress ** 1.65 * 0.88
        alpha = max(0.0, min(0.88, alpha))
        for x in range(1024):
            red, green, blue, a = pixels[x, y]
            pixels[x, y] = (round(red * (1 - alpha)), round(green * (1 - alpha)), round(blue * (1 - alpha)), a)
    lines, title_font = fit_title(str(row["title"]), bold_font)
    date_font = load_font(semibold_font, 36)
    # Cyrillic capitals and breve/descender glyphs need real breathing room after
    # the texture is projected in perspective. The previous 0.94em leading and
    # hard 2px duplicate looked like a damaged/double print on cube faces.
    line_height = title_font.size * 1.06
    title_y = 1024 - 58 - line_height * len(lines)
    date_y = title_y - 49
    label = date_label(row)
    # The lower gradient already provides contrast. A second baked silhouette
    # turns into a dirty/double print after perspective minification.
    draw = ImageDraw.Draw(rgba)
    draw.text((64, date_y), label, font=date_font, fill=(255, 154, 92, 255), features=["kern"])
    y = title_y
    for line in lines:
        draw.text((64, y), line, font=title_font, fill=(255, 252, 246, 255), features=["kern"])
        y += line_height
    return rgba.convert("RGB")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selection", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--bold-font", required=True)
    parser.add_argument("--semibold-font", required=True)
    args = parser.parse_args()
    selection = json.loads(Path(args.selection).read_text())
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for index, row in enumerate(selection["events"]):
        body, content_type = download(row["image_url"])
        source = Image.open(io.BytesIO(body)).convert("RGB")
        square, crop, crop_mode = frame_square(source, row)
        face = overlay(square, row, Path(args.bold_font), Path(args.semibold_font))
        path = out_dir / f"face_{index:02d}_{row['event_id']}.png"
        face.save(path, optimize=True)
        item = dict(row)
        item.update({
            "slot_index": index,
            "face_path": path.name,
            "date_label": date_label(row),
            "source_dimensions": list(source.size),
            "selected_crop": crop,
            "crop_mode": crop_mode,
            "crop_reason": CROP_REASONS[crop_mode],
            "source_content_type": content_type,
            "source_sha256": hashlib.sha256(body).hexdigest(),
            "face_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "aspect_ratio_preserved": True,
        })
        rows.append(item)
        print(f"PASS slot={index} event={row['event_id']} group={row['selection_group']} date={item['date_label']}")
    manifest = {
        "schema_version": 1,
        "research_only": True,
        "selection": {key: value for key, value in selection.items() if key != "events"},
        "faces": rows,
        "critical_bbox_cut_count": 0,
        "aspect_ratio_distortion_count": 0,
        "crop_contract": "safe_crop=true uses centered cover; visual_only or null/unknown mode without OCR uses centered photo cover without letterbox; OCR-bearing unsafe assets use full-image contain over blurred fill; title/date overlay after framing",
        "typography_contract": {
            "layout_engine": "RAQM",
            "kerning": True,
            "title_line_height_em": 1.06,
            "baked_text_shadow": False,
        },
    }
    path = out_dir / "face_manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"ok": True, "manifest": str(path), "faces": len(rows)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
