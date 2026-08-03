"""Structural validation for tile-mosaic render artifacts."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any

from PIL import Image, ImageStat

from .core import artifact_name, load_scene_plan, reproducible_timestamp
from .prepare import sha256_file


def _entropy(image: Image.Image) -> float:
    histogram = image.convert("L").histogram()
    total = sum(histogram)
    if total <= 0:
        return 0.0
    result = 0.0
    for count in histogram:
        if not count:
            continue
        probability = count / total
        result -= probability * math.log2(probability)
    return result


def validate_render(image_path: str | Path, plan_path: str | Path) -> dict[str, Any]:
    image_file = Path(image_path).expanduser().resolve()
    plan_file = Path(plan_path).expanduser().resolve()
    plan = load_scene_plan(plan_file)
    with Image.open(image_file) as opened:
        image = opened.convert("RGB")
    expected = (int(plan["canvas"]["width"]), int(plan["canvas"]["height"]))
    gray = image.convert("L")
    extrema = gray.getextrema()
    histogram = gray.histogram()
    pixels = image.width * image.height
    dark_fraction = sum(histogram[:40]) / pixels
    highlight_fraction = sum(histogram[246:]) / pixels
    midtone_fraction = sum(histogram[70:190]) / pixels
    stat = ImageStat.Stat(image)
    channel_means = [round(value, 4) for value in stat.mean]
    channel_stddev = [round(value, 4) for value in stat.stddev]
    dynamic_range = int(extrema[1] - extrema[0])
    entropy = round(_entropy(image), 6)
    file_size = image_file.stat().st_size

    checks = {
        "dimensions": image.size == expected,
        "file_size": file_size >= 40_000,
        "dynamic_range": dynamic_range >= 120,
        "entropy": entropy >= 4.0,
        "dark_mass": 0.18 <= dark_fraction <= 0.92,
        "not_flat": mean(channel_stddev) >= 18.0,
        "highlights_bounded": highlight_fraction <= 0.18,
        "midtones_present": midtone_fraction >= 0.05,
    }
    return {
        "schema_version": 1,
        "generated_at": reproducible_timestamp(),
        "status": "pass" if all(checks.values()) else "fail",
        "image": {
            "path": artifact_name(image_file),
            "sha256": sha256_file(image_file),
            "size_bytes": file_size,
            "width": image.width,
            "height": image.height,
        },
        "plan": {
            "path": artifact_name(plan_file),
            "sha256": plan.get("plan_sha256"),
            "legacy_sha256": plan.get("legacy_plan_sha256"),
        },
        "metrics": {
            "dynamic_range": dynamic_range,
            "entropy": entropy,
            "dark_fraction": round(dark_fraction, 6),
            "highlight_fraction": round(highlight_fraction, 6),
            "midtone_fraction": round(midtone_fraction, 6),
            "channel_means": channel_means,
            "channel_stddev": channel_stddev,
        },
        "checks": checks,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--report", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    report = validate_render(args.image, args.plan)
    destination = Path(args.report).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
