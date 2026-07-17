#!/usr/bin/env python3
"""Build a labeled contact sheet from a JSON manifest of local real images."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont, ImageOps


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    ap.add_argument("--columns", type=int, default=4)
    ap.add_argument("--cell-width", type=int, default=380)
    ap.add_argument("--image-height", type=int, default=280)
    args = ap.parse_args()
    items = json.loads(args.manifest.read_text())
    if not isinstance(items, list) or not items:
        raise SystemExit("Manifest must be a non-empty JSON list")
    font = ImageFont.load_default(size=18)
    small = ImageFont.load_default(size=15)
    pad, caption_h = 14, 76
    cell_h = args.image_height + caption_h + pad * 2
    rows = math.ceil(len(items) / args.columns)
    sheet = Image.new("RGB", (args.columns * args.cell_width, rows * cell_h), "#e7e2d8")
    draw = ImageDraw.Draw(sheet)
    for index, item in enumerate(items):
        col, row = index % args.columns, index // args.columns
        x, y = col * args.cell_width, row * cell_h
        card = (x + 6, y + 6, x + args.cell_width - 6, y + cell_h - 6)
        draw.rounded_rectangle(card, radius=18, fill="#fffdf9")
        raw_path = Path(item.get("file") or item.get("path") or "")
        if not raw_path.is_absolute():
            candidate = args.manifest.parent / raw_path
            raw_path = candidate if candidate.exists() else raw_path
        with Image.open(raw_path) as source:
            image = ImageOps.contain(source.convert("RGB"), (args.cell_width - pad * 2, args.image_height))
        ix = x + (args.cell_width - image.width) // 2
        iy = y + pad + (args.image_height - image.height) // 2
        sheet.paste(image, (ix, iy))
        title = str(item.get("title") or raw_path.stem)
        event_id = item.get("event_id")
        label = f"#{event_id} {title}" if event_id is not None else title
        meta = " · ".join(str(item.get(key)) for key in ("orientation_label", "image_text_mode") if item.get(key))
        draw.text((x + pad, y + pad + args.image_height + 8), label[:44], fill="#1e1e1c", font=font)
        draw.text((x + pad, y + pad + args.image_height + 36), meta[:52], fill="#68645d", font=small)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(args.output)
    print(args.output)


if __name__ == "__main__":
    main()
