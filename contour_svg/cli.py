from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import load_config
from .neural_branch import NeuralBranchConfig, run_neural_branch
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
    neural = sub.add_parser("neural-branch", help="Run the mask/edge neural line-art branch")
    neural.add_argument("--artifacts", required=True, help="Directory with edge_map.png and masks")
    neural.add_argument("--out", required=True, help="Output directory")
    neural.add_argument("--variants", default="A1,A3,C2,D1,E1", help="Comma-separated variants, e.g. A1,A3,C2,D1,E1")
    neural.add_argument("--seeds", default="42", help="Comma-separated integer seeds")
    neural.add_argument("--init-modes", default="line_init", help="Comma-separated init modes: line_init or photo_assisted")
    neural.add_argument("--source-image", help="Original source photo; required only for photo_assisted mode")
    neural.add_argument("--style-reference", help="Style reference image for E1")
    neural.add_argument("--run-neural", action="store_true", help="Actually run CUDA Diffusers img2img")
    neural.add_argument("--steps", type=int, default=24)
    neural.add_argument("--strength", type=float, default=0.60)
    neural.add_argument("--style-rewrite-strength", type=float, default=0.65)
    neural.add_argument("--guidance-scale", type=float, default=9.0)
    neural.add_argument("--control-scale", type=float, default=0.75)
    neural.add_argument("--style-reference-adapter-scale", type=float, default=0.55)
    args = parser.parse_args(argv)

    if args.command == "neural-branch":
        report = run_neural_branch(
            NeuralBranchConfig(
                artifact_dir=Path(args.artifacts),
                out_dir=Path(args.out),
                source_image=Path(args.source_image) if args.source_image else None,
                style_reference=Path(args.style_reference) if args.style_reference else None,
                variants=tuple(v.strip() for v in args.variants.split(",") if v.strip()),
                init_modes=tuple(v.strip() for v in args.init_modes.split(",") if v.strip()),
                seeds=tuple(int(v.strip()) for v in args.seeds.split(",") if v.strip()),
                run_neural=bool(args.run_neural),
                steps=args.steps,
                strength=args.strength,
                style_rewrite_strength=args.style_rewrite_strength,
                guidance_scale=args.guidance_scale,
                control_scale=args.control_scale,
                style_reference_adapter_scale=args.style_reference_adapter_scale,
            )
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return

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
