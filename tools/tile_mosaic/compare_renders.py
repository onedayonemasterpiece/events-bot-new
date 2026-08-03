"""Pixel-exact comparison independent of PNG compression metadata."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from PIL import Image, ImageChops, ImageStat


def compare(expected_path: str | Path, actual_path: str | Path) -> dict[str, Any]:
    expected_file = Path(expected_path).expanduser().resolve()
    actual_file = Path(actual_path).expanduser().resolve()
    with Image.open(expected_file) as opened:
        expected = opened.convert("RGB")
    with Image.open(actual_file) as opened:
        actual = opened.convert("RGB")
    same_size = expected.size == actual.size
    if same_size:
        difference = ImageChops.difference(expected, actual)
        extrema = difference.getextrema()
        max_difference = max(channel[1] for channel in extrema)
        mean_difference = sum(ImageStat.Stat(difference).mean) / 3.0
        expected_pixels = sha256(expected.tobytes()).hexdigest()
        actual_pixels = sha256(actual.tobytes()).hexdigest()
    else:
        max_difference = 255
        mean_difference = 255.0
        expected_pixels = sha256(expected.tobytes()).hexdigest()
        actual_pixels = sha256(actual.tobytes()).hexdigest()
    return {
        "schema_version": 1,
        "status": "pass" if same_size and expected_pixels == actual_pixels else "fail",
        "expected": {"path": str(expected_file), "size": list(expected.size), "pixel_sha256": expected_pixels},
        "actual": {"path": str(actual_file), "size": list(actual.size), "pixel_sha256": actual_pixels},
        "metrics": {"same_size": same_size, "max_channel_difference": max_difference, "mean_channel_difference": round(mean_difference, 8)},
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected", required=True)
    parser.add_argument("--actual", required=True)
    parser.add_argument("--report")
    return parser


def main() -> None:
    args = _parser().parse_args()
    report = compare(args.expected, args.actual)
    if args.report:
        destination = Path(args.report).expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
