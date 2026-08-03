"""Create deterministic labelled review sheets for laboratory outputs."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from PIL import Image, ImageDraw, ImageFont, ImageOps

try:
    RESAMPLING = Image.Resampling
except AttributeError:  # pragma: no cover
    RESAMPLING = Image


def _parse_item(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("items must use LABEL=PATH")
    label, path = value.split("=", 1)
    label = label.strip()
    if not label:
        raise argparse.ArgumentTypeError("item label is empty")
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise argparse.ArgumentTypeError(f"image does not exist: {source}")
    return label, source


def create_contact_sheet(
    items: Sequence[tuple[str, Path]],
    output: str | Path,
    *,
    columns: int = 2,
    cell_width: int = 960,
    label_height: int = 48,
) -> Path:
    if not items:
        raise ValueError("at least one image is required")
    columns = max(1, min(columns, len(items)))
    rows = (len(items) + columns - 1) // columns
    images = []
    for label, path in items:
        with Image.open(path) as opened:
            image = opened.convert("RGB")
        cell_height = round(cell_width * image.height / image.width)
        image = ImageOps.fit(image, (cell_width, cell_height), method=RESAMPLING.LANCZOS)
        images.append((label, image))
    cell_height = max(image.height for _, image in images)
    sheet = Image.new("RGB", (columns * cell_width, rows * (cell_height + label_height)), (5, 7, 8))
    draw = ImageDraw.Draw(sheet)
    font_path = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
    font = ImageFont.truetype(str(font_path), 22) if font_path.is_file() else ImageFont.load_default(size=22)
    for index, (label, image) in enumerate(images):
        row, column = divmod(index, columns)
        x = column * cell_width
        y = row * (cell_height + label_height)
        sheet.paste(image, (x, y))
        band_top = y + cell_height
        draw.rectangle((x, band_top, x + cell_width, band_top + label_height), fill=(10, 12, 13))
        draw.text((x + 18, band_top + 12), label, fill=(232, 229, 224), font=font)
    destination = Path(output).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(destination, format="JPEG", quality=92, optimize=True, progressive=True)
    return destination


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--item", action="append", type=_parse_item, required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--columns", type=int, default=2)
    parser.add_argument("--cell-width", type=int, default=960)
    return parser


def main() -> None:
    args = _parser().parse_args()
    print(create_contact_sheet(args.item, args.output, columns=args.columns, cell_width=args.cell_width))


if __name__ == "__main__":
    main()
