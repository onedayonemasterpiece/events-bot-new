"""One-command laboratory runner for the baseline and bounded variants."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
from types import SimpleNamespace
from typing import Any

from .contact_sheet import create_contact_sheet
from .core import artifact_name, reproducible_timestamp
from .generate import run as run_generator
from .refine_render import render_refinement
from .validate_render import validate_render
from .prepare import sha256_file

ROOT = Path(__file__).resolve().parent
DEFAULT_PRESET = ROOT / "presets" / "kafel_classic_v1.json"
REFINEMENT_NAMES = ("reference_balanced_v1", "matte_soft_v1", "microtilt_v1")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_lab(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    baseline = output_dir / "kafel-classic-v1.png"

    if args.frozen_base_render and args.frozen_base_plan:
        source_render = Path(args.frozen_base_render).expanduser().resolve()
        source_plan = Path(args.frozen_base_plan).expanduser().resolve()
        shutil.copyfile(source_render, baseline)
        shutil.copyfile(source_plan, baseline.with_suffix(".png.plan.json"))
        source_manifest = source_render.with_suffix(source_render.suffix + ".manifest.json")
        if source_manifest.is_file():
            shutil.copyfile(source_manifest, baseline.with_suffix(".png.manifest.json"))
        baseline_mode = "frozen-golden"
    else:
        if not args.input:
            raise ValueError("--input is required unless both frozen base files are supplied")
        manifest = run_generator(
            SimpleNamespace(
                input=args.input,
                output=str(baseline),
                preset=str(DEFAULT_PRESET),
                plan=args.plan,
                backend="pillow",
                focal_x=args.focal_x,
                focal_y=args.focal_y,
                seed=args.seed,
                work_dir=str(output_dir / "kafel-classic-work"),
                blender_bin=args.blender_bin,
                engine=args.engine,
                timeout_seconds=args.timeout_seconds,
                keep_work_dir=True,
            )
        )
        baseline_mode = manifest["plan"]["source"]

    baseline_plan = baseline.with_suffix(".png.plan.json")
    outputs: list[dict[str, Any]] = []
    baseline_validation = validate_render(baseline, baseline_plan)
    _write_json(output_dir / "kafel-classic-v1.validation.json", baseline_validation)
    outputs.append({
        "name": "kafel_classic_v1",
        "path": artifact_name(baseline),
        "sha256": sha256_file(baseline),
        "validation": baseline_validation["status"],
    })

    sheet_items = [("01 — Кафель / Kafel Classic", baseline)]
    labels = {
        "reference_balanced_v1": "02 — Reference Balanced",
        "matte_soft_v1": "03 — Matte Soft",
        "microtilt_v1": "04 — Microtilt",
    }
    for name in REFINEMENT_NAMES:
        output = output_dir / f"{name.replace('_', '-')}.png"
        profile = ROOT / "refinements" / f"{name}.json"
        manifest = render_refinement(
            base_render=baseline,
            base_plan=baseline_plan,
            profile_path=profile,
            output_path=output,
        )
        validation = validate_render(output, baseline_plan)
        _write_json(output_dir / f"{name.replace('_', '-')}.validation.json", validation)
        outputs.append({
            "name": name,
            "path": artifact_name(output),
            "sha256": manifest["output"]["sha256"],
            "validation": validation["status"],
        })
        sheet_items.append((labels[name], output))

    contact_sheet = create_contact_sheet(
        sheet_items,
        output_dir / "contact-sheet.jpg",
        columns=2,
        cell_width=960,
    )
    manifest = {
        "schema_version": 1,
        "generated_at": reproducible_timestamp(),
        "lab": "tile-mosaic-material-lab",
        "baseline_mode": baseline_mode,
        "baseline_plan": artifact_name(baseline_plan),
        "outputs": outputs,
        "contact_sheet": {
            "path": artifact_name(contact_sheet),
            "sha256": sha256_file(contact_sheet),
        },
    }
    _write_json(output_dir / "lab-manifest.json", manifest)
    if any(item["validation"] != "pass" for item in outputs):
        raise RuntimeError("one or more laboratory outputs failed structural validation")
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render Kafel Classic and three reproducible material studies")
    parser.add_argument("--input")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--focal-x", type=float, default=0.5)
    parser.add_argument("--focal-y", type=float, default=0.5)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--plan")
    parser.add_argument("--frozen-base-render")
    parser.add_argument("--frozen-base-plan")
    parser.add_argument("--blender-bin", default="blender")
    parser.add_argument("--engine", choices=("eevee", "cycles"), default="eevee")
    parser.add_argument("--timeout-seconds", type=int, default=1500)
    return parser


def main() -> None:
    args = _parser().parse_args()
    print(json.dumps(run_lab(args), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
