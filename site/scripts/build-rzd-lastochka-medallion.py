#!/usr/bin/env python3
"""Build the deterministic RZD Lastochka lab medallion assets."""

from __future__ import annotations

from pathlib import Path

from PIL import Image


ROOT = Path(__file__).resolve().parents[2]
SOURCE = (
    ROOT
    / "docs"
    / "features"
    / "static-site-pages"
    / "medalions-free-ref"
    / "rzd-lastochka.png"
)
OUTPUT_DIR = ROOT / "site" / "public" / "assets" / "transport"
PNG_OUTPUT = OUTPUT_DIR / "rzd-lastochka-medallion.png"
WEBP_OUTPUT = OUTPUT_DIR / "rzd-lastochka-medallion.webp"

# Product and design review converged on the cab plus first passenger door:
# enough of the train remains visible to read as transport, while the red cab
# stays recognizable at the 90–112px QA sizes.
SOURCE_CROP = (80, 590, 800, 1310)
OUTPUT_SIZE = (768, 768)
BACKGROUND = (240, 243, 246)


def build() -> None:
    source = Image.open(SOURCE).convert("RGB")
    crop = source.crop(SOURCE_CROP).resize(OUTPUT_SIZE, Image.Resampling.LANCZOS)
    output = Image.new("RGB", OUTPUT_SIZE, BACKGROUND)

    source_pixels = crop.load()
    output_pixels = output.load()
    for y in range(crop.height):
        for x in range(crop.width):
            red, green, blue = source_pixels[x, y]
            # Replace the near-white studio field with the stable ice-grey token
            # background while retaining the train, antialiased edges and the
            # contact shadow below the wheels.
            distance_from_white = 255 - min(red, green, blue)
            alpha = max(0.0, min(1.0, (distance_from_white - 7.0) / 22.0))
            output_pixels[x, y] = tuple(
                round(BACKGROUND[channel] * (1 - alpha) + color * alpha)
                for channel, color in enumerate((red, green, blue))
            )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output.save(PNG_OUTPUT, optimize=True)
    output.save(WEBP_OUTPUT, "WEBP", lossless=True, method=6)


if __name__ == "__main__":
    build()
