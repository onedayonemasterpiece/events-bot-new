#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import shutil
import tempfile
import time
import uuid
from pathlib import Path

try:
    from run_contour_svg_kaggle_sample import (
        DEFAULT_ENV_FILE,
        PROJECT_ROOT,
        apply_env_file,
        create_or_replace_dataset,
        delete_dataset,
        download_kernel_output,
        get_kaggle_api,
        kernel_status,
        poll_kernel,
        compact_unique_slug,
        project_relative,
        require_kaggle_username,
        slugify,
        wait_dataset_ready,
    )
except ModuleNotFoundError:  # pragma: no cover - import path used by pytest/package context
    from scripts.run_contour_svg_kaggle_sample import (
        DEFAULT_ENV_FILE,
        PROJECT_ROOT,
        apply_env_file,
        create_or_replace_dataset,
        delete_dataset,
        download_kernel_output,
        get_kaggle_api,
        kernel_status,
        poll_kernel,
        compact_unique_slug,
        project_relative,
        require_kaggle_username,
        slugify,
        wait_dataset_ready,
    )


logger = logging.getLogger("contour_svg_neural_branch_kaggle")

DEFAULT_KERNEL_PATH = PROJECT_ROOT / "kaggle" / "ContourSvgNeuralBranch"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "artifacts" / "codex" / "contour-svg-neural-branch-kaggle"
PAYLOAD_PREFIX = "csv-nn-payload"
DEFAULT_STYLE_REFERENCE = "docs/features/countur_svg_generator/samples/output/IMG_20260614_115550.webp"


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    shutil.copytree(src, dst, ignore=ignore)


def build_neural_run_config(
    *,
    run_id: str,
    artifact_dir: str | Path | None = None,
    style_reference: str | Path | None = DEFAULT_STYLE_REFERENCE,
    source_image: str | Path | None = None,
    variants: str | None = None,
    init_modes: str | None = None,
    seeds: str | None = None,
) -> dict:
    data: dict[str, str] = {"run_id": run_id}
    if artifact_dir:
        data["artifact_dir"] = "neural_artifacts"
    if style_reference:
        data["style_reference"] = project_relative(style_reference)
    if source_image:
        data["source_image"] = project_relative(source_image)
    if variants:
        data["variants"] = variants
    if init_modes:
        data["init_modes"] = init_modes
    if seeds:
        data["seeds"] = seeds
    return data


def write_payload_dataset(
    path: Path,
    *,
    run_id: str,
    artifact_dir: str | Path | None = None,
    style_reference: str | Path | None = DEFAULT_STYLE_REFERENCE,
    source_image: str | Path | None = None,
    variants: str | None = None,
    init_modes: str | None = None,
    seeds: str | None = None,
) -> None:
    repo = path / "repo_bundle"
    copy_tree(PROJECT_ROOT / "contour_svg", repo / "contour_svg")
    copy_tree(
        PROJECT_ROOT / "docs" / "features" / "countur_svg_generator",
        repo / "docs" / "features" / "countur_svg_generator",
    )
    if artifact_dir:
        artifact_path = Path(artifact_dir).expanduser()
        if not artifact_path.is_absolute():
            artifact_path = PROJECT_ROOT / artifact_path
        if not (artifact_path / "edge_map.png").exists():
            raise RuntimeError(f"artifact_dir must contain edge_map.png: {artifact_path}")
        copy_tree(artifact_path, repo / "neural_artifacts")
    (repo / "kaggle").mkdir(parents=True, exist_ok=True)
    shutil.copy2(PROJECT_ROOT / "kaggle" / "kaggle_status_client.py", repo / "kaggle" / "kaggle_status_client.py")
    (path / "kaggle_run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "kind": "contour_svg_neural_branch",
                "notebook": "ContourSvgNeuralBranch",
                "resource_leases": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (path / "neural_run_config.json").write_text(
        json.dumps(
            build_neural_run_config(
                run_id=run_id,
                artifact_dir=artifact_dir,
                style_reference=style_reference,
                source_image=source_image,
                variants=variants,
                init_modes=init_modes,
                seeds=seeds,
            ),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def push_kernel(api, *, kernel_slug: str, dataset_sources: list[str], accelerator: str, session_timeout_seconds: int) -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "ContourSvgNeuralBranch"
        shutil.copytree(DEFAULT_KERNEL_PATH, tmp_path)
        shutil.copy2(PROJECT_ROOT / "kaggle" / "kaggle_status_client.py", tmp_path / "kaggle_status_client.py")
        meta_path = tmp_path / "kernel-metadata.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        username = require_kaggle_username()
        meta["id"] = f"{username}/{kernel_slug}"
        meta["slug"] = kernel_slug
        meta["title"] = "Contour SVG Neural Branch"
        meta["dataset_sources"] = dataset_sources
        meta["enable_gpu"] = True
        meta["enable_internet"] = True
        meta["machine_shape"] = accelerator
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        api.kernels_push(str(tmp_path), timeout=str(session_timeout_seconds), acc=accelerator)
    return f"{require_kaggle_username()}/{kernel_slug}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-minutes", type=int, default=80)
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--accelerator", default="NvidiaTeslaT4")
    parser.add_argument("--kernel-slug", default="contour-svg-neural-branch")
    parser.add_argument("--run-label", default="contour-svg-neural")
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--style-reference", default=DEFAULT_STYLE_REFERENCE)
    parser.add_argument("--source-image")
    parser.add_argument("--variants", default="A1,A3,C2,D1,E1")
    parser.add_argument("--init-modes", default="line_init")
    parser.add_argument("--seeds", default="42")
    parser.add_argument("--session-timeout-seconds", type=int, default=5400)
    parser.add_argument("--keep-datasets", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    apply_env_file(DEFAULT_ENV_FILE)

    run_id = f"{slugify(args.run_label, max_len=32)}-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    run_slug = compact_unique_slug(run_id, max_len=32)
    username = require_kaggle_username()
    api = get_kaggle_api()

    payload_ref = create_or_replace_dataset(
        api,
        username,
        f"{PAYLOAD_PREFIX}-{run_slug}",
        f"CSV neural payload {run_slug[:24]}",
        lambda path: write_payload_dataset(
            path,
            run_id=run_id,
            artifact_dir=args.artifacts,
            style_reference=args.style_reference,
            source_image=args.source_image,
            variants=args.variants,
            init_modes=args.init_modes,
            seeds=args.seeds,
        ),
    )
    wait_dataset_ready(api, payload_ref, timeout_seconds=180, expected_files=["kaggle_run.json", "neural_run_config.json"])

    kernel_ref = push_kernel(
        api,
        kernel_slug=slugify(args.kernel_slug, max_len=48),
        dataset_sources=[payload_ref],
        accelerator=args.accelerator,
        session_timeout_seconds=args.session_timeout_seconds,
    )
    status = poll_kernel(api, kernel_ref, timeout_minutes=args.timeout_minutes, poll_interval_seconds=args.poll_interval_seconds)

    out_dir = args.output_root / run_slug
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded = download_kernel_output(api, kernel_ref, out_dir)
    summary = {
        "run_id": run_id,
        "kernel_ref": kernel_ref,
        "kernel_slug": slugify(args.kernel_slug, max_len=48),
        "artifacts": str(args.artifacts) if args.artifacts else None,
        "variants": args.variants,
        "init_modes": args.init_modes,
        "seeds": args.seeds,
        "kernel_status": status,
        "accelerator": args.accelerator,
        "session_timeout_seconds": args.session_timeout_seconds,
        "dataset_sources": [payload_ref],
        "download_dir": str(out_dir),
        "downloaded": downloaded,
        "latest_status": kernel_status(api, kernel_ref),
    }
    (out_dir / "local_run_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if not args.keep_datasets:
        try:
            delete_dataset(api, payload_ref)
        except Exception:
            logger.info("dataset cleanup failed dataset=%s", payload_ref, exc_info=True)


if __name__ == "__main__":
    main()
