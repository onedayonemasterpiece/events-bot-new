#!/usr/bin/env python3
"""Deterministically render the Autopresenter control PWA PNG icons."""

from __future__ import annotations

import argparse
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageDraw


HERE = Path(__file__).resolve().parent
OUTPUTS = (
    ("icon-192.png", 192, False),
    ("icon-512.png", 512, False),
    ("icon-maskable-512.png", 512, True),
)


def render(size: int, *, maskable: bool) -> bytes:
    factor = 4
    side = size * factor

    def point(value: float) -> int:
        return round(value * side)

    image = Image.new("RGB", (side, side), "#090f1f")
    draw = ImageDraw.Draw(image, "RGB")
    if not maskable:
        draw.rounded_rectangle(
            (0, 0, side - 1, side - 1),
            radius=point(0.22),
            fill="#090f1f",
        )
    draw.ellipse(
        (-point(0.17), -point(0.18), point(0.38), point(0.37)),
        fill="#1e285c",
    )
    draw.ellipse(
        (point(0.62), point(0.61), point(1.18), point(1.18)),
        fill="#0d4e49",
    )
    draw.rounded_rectangle(
        (point(0.272), point(0.166), point(0.728), point(0.834)),
        radius=point(0.137),
        fill="#ecf0ff",
    )
    draw.rounded_rectangle(
        (point(0.344), point(0.240), point(0.656), point(0.330)),
        radius=point(0.045),
        fill="#202b50",
    )
    draw.polygon(
        (
            (point(0.439), point(0.400)),
            (point(0.439), point(0.600)),
            (point(0.611), point(0.500)),
        ),
        fill="#28cda6",
    )
    draw.ellipse(
        (point(0.451), point(0.670), point(0.549), point(0.768)),
        fill="#6574e8",
    )
    image = image.resize((size, size), Image.Resampling.LANCZOS)
    output = BytesIO()
    image.save(output, format="PNG", optimize=False, compress_level=9)
    return output.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if committed icons differ from deterministic output",
    )
    args = parser.parse_args()
    stale: list[str] = []
    for name, size, maskable in OUTPUTS:
        expected = render(size, maskable=maskable)
        target = HERE / name
        if args.check:
            if not target.is_file() or target.read_bytes() != expected:
                stale.append(name)
        else:
            target.write_bytes(expected)
            print(target.relative_to(HERE.parent))
    if stale:
        raise SystemExit(f"stale generated icons: {', '.join(stale)}")


if __name__ == "__main__":
    main()
