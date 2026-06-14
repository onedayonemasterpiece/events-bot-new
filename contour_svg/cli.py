from __future__ import annotations

import argparse
import json

from .config import load_config
from .pipeline import ContourGenerator


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m contour_svg")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Generate contour SVG candidates")
    run.add_argument("--config", help="YAML config path")
    run.add_argument("--input", help="Input image path override")
    run.add_argument("--out", help="Output directory override")
    run.add_argument("--object", dest="object_prompt", help="Primary object prompt override")
    run.add_argument("--variants", help="Comma-separated variants, e.g. B1,B2,B3,B4,E1")
    run.add_argument("--stroke-color", help="Final SVG stroke color")
    args = parser.parse_args(argv)

    overrides = {}
    if args.input:
        overrides.setdefault("input", {})["image_path"] = args.input
    if args.out:
        overrides.setdefault("output", {})["output_dir"] = args.out
    if args.object_prompt:
        overrides.setdefault("input", {})["object_prompt"] = args.object_prompt
    if args.variants:
        overrides.setdefault("candidate_search", {})["variants"] = [v.strip() for v in args.variants.split(",") if v.strip()]
    if args.stroke_color:
        overrides.setdefault("style", {})["stroke_color"] = args.stroke_color
    config = load_config(args.config, overrides=overrides)
    result = ContourGenerator(config).run()
    print(
        json.dumps(
            {
                "status": "ok",
                "output_dir": str(result.output_dir),
                "final_svg": str(result.final_svg) if result.final_svg else None,
                "preview_png": str(result.preview_png) if result.preview_png else None,
                "candidate_count": len(result.candidates),
                "warnings": result.warnings,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
