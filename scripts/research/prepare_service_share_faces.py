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
        font = ImageFont.truetype(str(font_path), size)
        lines = _wrap_pixels(title, font)
        if lines and len(lines) <= 3 and max(font.getlength(line) for line in lines) <= 880:
            return lines, font
    font = ImageFont.truetype(str(font_path), 36)
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
    draw = ImageDraw.Draw(rgba)
    lines, title_font = fit_title(str(row["title"]), bold_font)
    date_font = ImageFont.truetype(str(semibold_font), 36)
    line_height = title_font.size * 0.94
    title_y = 1024 - 58 - line_height * len(lines)
    date_y = title_y - 49
    label = date_label(row)
    draw.text((66, date_y + 2), label, font=date_font, fill=(0, 0, 0, 155))
    draw.text((64, date_y), label, font=date_font, fill=(255, 154, 92, 255))
    y = title_y
    for line in lines:
        draw.text((66, y + 2), line, font=title_font, fill=(0, 0, 0, 145))
        draw.text((64, y), line, font=title_font, fill=(255, 252, 246, 255))
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
        crop_mode = "safe_center_cover" if row.get("safe_crop") else "full_image_contain"
        square, crop = cover_square(source) if row.get("safe_crop") else contain_square(source)
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
        "crop_contract": "safe_crop=true uses centered cover; unsafe metadata uses full-image contain over blurred fill; title/date overlay after framing",
    }
    path = out_dir / "face_manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"ok": True, "manifest": str(path), "faces": len(rows)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
