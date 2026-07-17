#!/usr/bin/env python3
"""Render paginated visual QA sheets for image-geometry JSONL results."""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path
import urllib.request

from PIL import Image, ImageDraw, ImageOps


def _read(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _download(url: str, timeout: float = 25.0) -> Image.Image:
    req = urllib.request.Request(url, headers={"User-Agent": "events-bot-geometry-qa/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        data = response.read(12_000_000)
    return ImageOps.exif_transpose(Image.open(io.BytesIO(data))).convert("RGB")


def _box_xyxy(box, width: int, height: int):
    ymin, xmin, ymax, xmax = [float(item) for item in box]
    return (xmin * width, ymin * height, xmax * width, ymax * height)


def _tile(row: dict, *, width: int, height: int) -> Image.Image:
    label_h = 58
    canvas = Image.new("RGB", (width, height), "#111318")
    draw = ImageDraw.Draw(canvas)
    try:
        image = _download(str(row.get("url") or ""))
        image.thumbnail((width - 8, height - label_h - 8), Image.Resampling.LANCZOS)
        x = (width - image.width) // 2
        y = label_h + (height - label_h - image.height) // 2
        canvas.paste(image, (x, y))
        overlay = ImageDraw.Draw(canvas)
        for face in list(row.get("face_boxes_yxyx") or []):
            left, top, right, bottom = _box_xyxy(face, image.width, image.height)
            overlay.rectangle((x + left, y + top, x + right, y + bottom), outline="#ff3b30", width=3)
        valuable = row.get("valuable_region_yxyx")
        if valuable:
            left, top, right, bottom = _box_xyxy(valuable, image.width, image.height)
            overlay.rectangle((x + left, y + top, x + right, y + bottom), outline="#32d74b", width=4)
    except Exception as exc:
        draw.text((8, label_h + 12), f"download error: {type(exc).__name__}", fill="#ff453a")
    faces = len(list(row.get("face_boxes_yxyx") or []))
    conf = row.get("valuable_region_confidence")
    draw.text(
        (7, 5),
        f"poster={row.get('poster_id')} event={row.get('event_id')} faces={faces} conf={conf}",
        fill="white",
    )
    draw.text(
        (7, 25),
        f"{row.get('reason_code') or row.get('status')}  red=faces green=value",
        fill="#c7c7cc",
    )
    return canvas


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--columns", type=int, default=5)
    parser.add_argument("--rows", type=int, default=5)
    parser.add_argument("--tile-width", type=int, default=320)
    parser.add_argument("--tile-height", type=int, default=300)
    args = parser.parse_args()

    values = [row for row in _read(Path(args.input)) if row.get("status") == "classified"]
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    page_size = max(1, args.columns * args.rows)
    for page_no, start in enumerate(range(0, len(values), page_size), 1):
        page_rows = values[start : start + page_size]
        sheet = Image.new(
            "RGB",
            (args.columns * args.tile_width, args.rows * args.tile_height),
            "#08090c",
        )
        for index, row in enumerate(page_rows):
            tile = _tile(row, width=args.tile_width, height=args.tile_height)
            x = (index % args.columns) * args.tile_width
            y = (index // args.columns) * args.tile_height
            sheet.paste(tile, (x, y))
        path = out / f"geometry-contact-sheet-{page_no:03d}.jpg"
        sheet.save(path, quality=90, optimize=True)
        print(path)
    print(f"classified={len(values)} pages={(len(values) + page_size - 1) // page_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
