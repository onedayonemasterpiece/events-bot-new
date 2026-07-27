#!/usr/bin/env python3
"""Generate deterministic launcher icons from the approved PWA reference."""

from pathlib import Path

from PIL import Image


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE = REPO_ROOT / "docs/reference/PWA-icon.png"
OUTPUT_DIR = REPO_ROOT / "site/public/assets/pwa"
SIZES = (192, 512)
MASKABLE_CROP_INSET = 60
RESAMPLING = Image.Resampling.LANCZOS


def save_png(image: Image.Image, path: Path, size: int) -> None:
    resized = image.resize((size, size), RESAMPLING)
    resized.save(path, format="PNG", optimize=True, compress_level=9)


def main() -> None:
    source = Image.open(SOURCE).convert("RGB")
    if source.width != source.height:
        raise ValueError(f"PWA icon source must be square, got {source.size}")
    if source.width <= MASKABLE_CROP_INSET * 2:
        raise ValueError(f"PWA icon source is too small for the maskable crop: {source.size}")

    maskable = source.crop(
        (
            MASKABLE_CROP_INSET,
            MASKABLE_CROP_INSET,
            source.width - MASKABLE_CROP_INSET,
            source.height - MASKABLE_CROP_INSET,
        )
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for size in SIZES:
        save_png(source, OUTPUT_DIR / f"announcements-brand-{size}.png", size)
        save_png(
            maskable,
            OUTPUT_DIR / f"announcements-brand-maskable-{size}.png",
            size,
        )


if __name__ == "__main__":
    main()
