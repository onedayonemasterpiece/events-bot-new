#!/usr/bin/env python3
"""Prepare the same circular assets for website, VK and Telegram consumers."""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys

from PIL import Image, ImageChops, ImageDraw, ImageOps

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / 'site/src/data/organizerMedallions.json'


def normalize(source: Image.Image, ring: str = '#D9D5CB') -> Image.Image:
    image = source.convert('RGBA')
    bounds = image.getchannel('A').getbbox()
    corner = image.getpixel((0, 0))
    if image.getchannel('A').getextrema() == (255, 255) and min(corner[:3]) >= 245:
        difference = ImageChops.difference(image.convert('RGB'), Image.new('RGB', image.size, corner[:3]))
        ink = difference.convert('L').point(lambda value: 255 if value > 12 else 0)
        bounds = ink.getbbox()
        crop = image.crop(bounds) if bounds else image
        ink = ink.crop(bounds) if bounds else ink
        radius = max((math.hypot(x - crop.width / 2, y - crop.height / 2)
                      for y in range(crop.height) for x in range(crop.width) if ink.getpixel((x, y))), default=1)
        scale = min(500 / max(crop.size), 248 / radius)
        crop = crop.resize(tuple(max(1, round(side * scale)) for side in crop.size), Image.Resampling.LANCZOS)
        image = Image.new('RGBA', (512, 512), corner)
        image.alpha_composite(crop, ((512 - crop.width) // 2, (512 - crop.height) // 2))
    else:
        image = ImageOps.fit(image.crop(bounds) if bounds else image, (512, 512), method=Image.Resampling.LANCZOS)
    mask = Image.new('L', image.size)
    ImageDraw.Draw(mask).ellipse((0, 0, 511, 511), fill=255)
    image.putalpha(ImageChops.multiply(image.getchannel('A'), mask))
    ImageDraw.Draw(image).ellipse((1, 1, 510, 510), outline=ring, width=6)
    return image


def build(slugs: list[str]) -> None:
    for item in json.loads(MANIFEST.read_text())['items']:
        if item['slug'] not in slugs:
            continue
        urls = [item.get('fallbackPngUrl'), item['avatarUrl']]
        paths = [ROOT / 'site/public' / url.lstrip('/') for url in urls if url]
        with Image.open(paths[0]) as source:
            image = normalize(source, item.get('ring', '#D9D5CB'))
        for path in dict.fromkeys(paths):
            image.save(path, 'WEBP' if path.suffix == '.webp' else 'PNG', lossless=True)


if __name__ == '__main__':
    build(sys.argv[1:])
