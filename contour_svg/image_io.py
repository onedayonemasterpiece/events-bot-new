from __future__ import annotations

from pathlib import Path

from .dependencies import require_module


def load_image(path: str | Path):
    Image = require_module("PIL.Image", "Pillow")
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    img = Image.open(path)
    img = ImageOps.exif_transpose(img)
    return img.convert("RGB")


def normalize_image(input_path: str | Path, output_path: str | Path, *, max_side: int):
    img = load_image(input_path)
    if max(img.size) > max_side:
        img.thumbnail((max_side, max_side))
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    img.save(out)
    return img, out


def save_binary_mask(mask, path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    mask.save(out)
    return out
