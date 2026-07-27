#!/usr/bin/env python3
"""Generate deterministic launcher icons from the approved PWA reference."""

from pathlib import Path

from PIL import Image


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE = REPO_ROOT / "docs/reference/PKA-PWA2.png"
OUTPUT_DIR = REPO_ROOT / "site/public/assets/pwa"
SIZES = (192, 512)
MASKABLE_CONTENT_SCALE = 0.82
RESAMPLING = Image.Resampling.LANCZOS


def save_png(image: Image.Image, path: Path, size: int) -> None:
    resized = image.resize((size, size), RESAMPLING)
    resized.save(path, format="PNG", optimize=True, compress_level=9)


def padded_maskable(source: Image.Image) -> Image.Image:
    side = round(source.width * MASKABLE_CONTENT_SCALE)
    content = source.resize((side, side), RESAMPLING)
    background = Image.new("RGB", source.size, source.getpixel((0, 0)))
    offset = ((source.width - side) // 2, (source.height - side) // 2)
    background.paste(content, offset)
    return background


def main() -> None:
    source = Image.open(SOURCE).convert("RGB")
    if source.width != source.height:
        raise ValueError(f"PWA icon source must be square, got {source.size}")
    maskable = padded_maskable(source)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for size in SIZES:
        save_png(source, OUTPUT_DIR / f"announcements-brand-v2-{size}.png", size)
        save_png(
            maskable,
            OUTPUT_DIR / f"announcements-brand-v2-maskable-{size}.png",
            size,
        )


if __name__ == "__main__":
    main()
