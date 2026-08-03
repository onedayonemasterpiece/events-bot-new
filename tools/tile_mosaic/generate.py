"""CLI orchestrator for deterministic matte tile-mosaic renders."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any

from .core import artifact_name, build_scene_plan, load_preset, load_scene_plan, reproducible_timestamp
from .pillow_renderer import render_pillow
from .prepare import load_image, materialize_input, prepare_textures, sha256_file

GENERATOR_VERSION = "2.0.0"
DEFAULT_PRESET = Path(__file__).with_name("presets") / "kafel_classic_v1.json"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Project one input image over a deterministic grid of tactile matte tiles.",
    )
    parser.add_argument("--input", required=True, help="Local image path or HTTP(S) URL")
    parser.add_argument("--output", required=True, help="Output PNG path")
    parser.add_argument("--preset", default=str(DEFAULT_PRESET))
    parser.add_argument(
        "--plan",
        help="Optional frozen scene-plan JSON. When supplied, planner changes cannot affect the render.",
    )
    parser.add_argument("--backend", choices=("pillow", "blender"), default="pillow")
    parser.add_argument("--focal-x", type=float, default=0.5)
    parser.add_argument("--focal-y", type=float, default=0.5)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--work-dir")
    parser.add_argument("--blender-bin", default="blender")
    parser.add_argument("--engine", choices=("eevee", "cycles"), default="eevee")
    parser.add_argument("--timeout-seconds", type=int, default=1500)
    parser.add_argument("--keep-work-dir", action="store_true")
    return parser


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _render_blender(
    *,
    blender_bin: str,
    plan_path: Path,
    textures_dir: Path,
    output: Path,
    engine: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    blender_script = Path(__file__).with_name("blender_renderer.py").resolve()
    command = [
        blender_bin,
        "-b",
        "--factory-startup",
        "--python",
        str(blender_script),
        "--",
        "--plan",
        str(plan_path),
        "--textures-dir",
        str(textures_dir),
        "--output",
        str(output),
        "--engine",
        engine,
    ]
    completed = subprocess.run(
        command,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout_seconds,
    )
    log_path = output.with_suffix(".blender.log")
    log_path.write_text(completed.stdout or "", encoding="utf-8")
    if completed.returncode != 0:
        tail = "\n".join((completed.stdout or "").splitlines()[-80:])
        raise RuntimeError(f"Blender exited with {completed.returncode}:\n{tail}")
    if not output.is_file() or output.stat().st_size < 10_000:
        raise RuntimeError(f"Blender output is missing or suspiciously small: {output}")
    return {
        "backend": "blender",
        "engine": engine,
        "command": command,
        "log": str(log_path),
        "blend": str(output.with_suffix(".blend")),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    preset_path = Path(args.preset).expanduser().resolve()
    preset = load_preset(preset_path)
    if args.plan:
        source_plan_path = Path(args.plan).expanduser().resolve()
        plan = load_scene_plan(source_plan_path)
        plan_source = "frozen"
    else:
        source_plan_path = None
        plan = build_scene_plan(
            preset,
            focal_x=args.focal_x,
            focal_y=args.focal_y,
            seed=args.seed,
        )
        plan_source = "planner"

    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    owned_tmp = args.work_dir is None
    work_dir = (
        Path(args.work_dir).expanduser().resolve()
        if args.work_dir
        else Path(tempfile.mkdtemp(prefix="tile-mosaic-"))
    )
    work_dir.mkdir(parents=True, exist_ok=True)
    plan_path = work_dir / "scene-plan.json"
    _write_json(plan_path, plan)

    input_path = materialize_input(args.input, work_dir / "input")
    prepared = prepare_textures(
        input_path=input_path,
        plan=plan,
        output_dir=work_dir / "textures",
    )

    if args.backend == "pillow":
        backend_result = render_pillow(
            plan=plan,
            source_texture=load_image(prepared.base_path),
            output_path=output,
        )
    else:
        backend_result = _render_blender(
            blender_bin=args.blender_bin,
            plan_path=plan_path,
            textures_dir=prepared.base_path.parent,
            output=output,
            engine=args.engine,
            timeout_seconds=max(60, int(args.timeout_seconds)),
        )

    sidecar_plan = output.with_suffix(output.suffix + ".plan.json")
    manifest = {
        "schema_version": 2,
        "generated_at": reproducible_timestamp(),
        "generator": "events-bot tile-mosaic material lab",
        "generator_version": GENERATOR_VERSION,
        "backend": args.backend,
        "backend_result": {**backend_result, "output": artifact_name(output)} if "output" in backend_result else backend_result,
        "input": {
            "reference": str(args.input),
            "sha256": prepared.input_sha256,
            "source_width": prepared.source_width,
            "source_height": prepared.source_height,
            "crop_box": list(prepared.crop_box),
        },
        "output": {
            "path": artifact_name(output),
            "sha256": sha256_file(output),
            "size_bytes": output.stat().st_size,
            "width": int(plan["canvas"]["width"]),
            "height": int(plan["canvas"]["height"]),
        },
        "preset": {
            "name": preset.get("name"),
            "path": f"presets/{preset_path.name}",
            "sha256": plan.get("preset_sha256"),
        },
        "plan": {
            "source": plan_source,
            "input_path": artifact_name(source_plan_path) if source_plan_path else None,
            "path": artifact_name(sidecar_plan),
            "sha256": plan["plan_sha256"],
            "legacy_sha256": plan.get("legacy_plan_sha256"),
            "seed": plan["seed"],
            "state_counts": plan["state_counts"],
        },
    }
    manifest_path = output.with_suffix(output.suffix + ".manifest.json")
    _write_json(manifest_path, manifest)

    shutil.copyfile(plan_path, sidecar_plan)

    if owned_tmp and not args.keep_work_dir:
        shutil.rmtree(work_dir, ignore_errors=True)
    else:
        manifest["work_dir"] = str(work_dir)
    return manifest


def main() -> None:
    args = _parser().parse_args()
    try:
        manifest = run(args)
    except Exception as exc:
        print(f"tile-mosaic generator failed: {exc}", file=sys.stderr)
        raise
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
