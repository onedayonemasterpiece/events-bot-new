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
except ModuleNotFoundError:  # pragma: no cover
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

logger = logging.getLogger("contour_svg_line_art_batch_kaggle")
DEFAULT_KERNEL_PATH = PROJECT_ROOT / "kaggle" / "ContourSvgLineArtBatch"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "artifacts" / "codex" / "contour-svg-line-art-batch-kaggle"
PAYLOAD_PREFIX = "csv-lineart-payload"
DEFAULT_STYLE_REFERENCE = "docs/features/countur_svg_generator/samples/output/IMG_20260614_115550.webp"
DEFAULT_SOURCE_DIR = "docs/features/countur_svg_generator/to_do"


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"))


def _source_images_from_args(source_dir: str | Path, source_images: str | None) -> list[Path]:
    if source_images:
        items = [Path(item.strip()) for item in source_images.split(",") if item.strip()]
    else:
        root = Path(source_dir)
        if not root.is_absolute():
            root = PROJECT_ROOT / root
        items = sorted([*root.glob("*.jpg"), *root.glob("*.jpeg"), *root.glob("*.png"), *root.glob("*.webp")])
    out: list[Path] = []
    for item in items:
        path = item if item.is_absolute() else PROJECT_ROOT / item
        if not path.exists():
            raise RuntimeError(f"source image does not exist: {path}")
        out.append(path)
    if not out:
        raise RuntimeError("No source images found for line-art batch")
    return out


def write_payload_dataset(path: Path, *, run_id: str, source_images: list[Path], style_reference: str | Path | None, branches: str, guide_ids: str, seeds: str, steps: int, output_size: str, max_candidates_per_image: int | None) -> None:
    repo = path / "repo_bundle"
    copy_tree(PROJECT_ROOT / "contour_svg", repo / "contour_svg")
    copy_tree(PROJECT_ROOT / "docs" / "features" / "countur_svg_generator", repo / "docs" / "features" / "countur_svg_generator")
    (repo / "kaggle").mkdir(parents=True, exist_ok=True)
    shutil.copy2(PROJECT_ROOT / "kaggle" / "kaggle_status_client.py", repo / "kaggle" / "kaggle_status_client.py")
    config = {
        "run_id": run_id,
        "source_images": [project_relative(path) for path in source_images],
        "style_reference": project_relative(style_reference) if style_reference else None,
        "branches": branches,
        "guide_ids": guide_ids,
        "seeds": seeds,
        "steps": steps,
        "output_size": output_size,
    }
    if max_candidates_per_image is not None:
        config["max_candidates_per_image"] = int(max_candidates_per_image)
    (path / "line_art_batch_config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    (path / "kaggle_run.json").write_text(json.dumps({"run_id": run_id, "kind": "contour_svg_line_art_batch", "notebook": "ContourSvgLineArtBatch", "resource_leases": []}, ensure_ascii=False, indent=2), encoding="utf-8")


def push_kernel(api, *, kernel_slug: str, dataset_sources: list[str], accelerator: str, session_timeout_seconds: int) -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "ContourSvgLineArtBatch"
        shutil.copytree(DEFAULT_KERNEL_PATH, tmp_path)
        shutil.copy2(PROJECT_ROOT / "kaggle" / "kaggle_status_client.py", tmp_path / "kaggle_status_client.py")
        meta_path = tmp_path / "kernel-metadata.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        username = require_kaggle_username()
        meta["id"] = f"{username}/{kernel_slug}"
        meta["slug"] = kernel_slug
        meta["title"] = "Contour SVG Line Art Batch"
        meta["dataset_sources"] = dataset_sources
        meta["enable_gpu"] = True
        meta["enable_internet"] = True
        meta["machine_shape"] = accelerator
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        api.kernels_push(str(tmp_path), timeout=str(session_timeout_seconds), acc=accelerator)
    return f"{require_kaggle_username()}/{kernel_slug}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-minutes", type=int, default=150)
    parser.add_argument("--poll-interval-seconds", type=int, default=45)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--accelerator", default="NvidiaTeslaT4")
    parser.add_argument("--kernel-slug", default="contour-svg-line-art-batch")
    parser.add_argument("--run-label", default="contour-svg-line-art-batch")
    parser.add_argument("--source-dir", default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--source-images")
    parser.add_argument("--style-reference", default=DEFAULT_STYLE_REFERENCE)
    parser.add_argument("--branches", default="E1_lineart_control_only,E2_lineart_line_init,E3_scribble_control_only,E4_scribble_line_init")
    parser.add_argument("--guide-ids", default="G3_edge_thickened,G4_edge_cleaned,CG1_silhouette_plus_structure,CG3_fused_balanced,CG4_minimal_clean")
    parser.add_argument("--seeds", default="42")
    parser.add_argument("--steps", type=int, default=22)
    parser.add_argument("--output-size", default="768,768")
    parser.add_argument("--max-candidates-per-image", type=int)
    parser.add_argument("--session-timeout-seconds", type=int, default=7200)
    parser.add_argument("--keep-datasets", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    apply_env_file(DEFAULT_ENV_FILE)
    source_images = _source_images_from_args(args.source_dir, args.source_images)
    run_id = f"{slugify(args.run_label, max_len=32)}-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    run_slug = compact_unique_slug(run_id, max_len=40)
    username = require_kaggle_username()
    api = get_kaggle_api()
    payload_slug = compact_unique_slug(f"{PAYLOAD_PREFIX}-{run_slug}", max_len=50)

    payload_ref = create_or_replace_dataset(
        api,
        username,
        payload_slug,
        f"CSV line-art payload {run_slug[:24]}",
        lambda path: write_payload_dataset(
            path,
            run_id=run_id,
            source_images=source_images,
            style_reference=args.style_reference,
            branches=args.branches,
            guide_ids=args.guide_ids,
            seeds=args.seeds,
            steps=args.steps,
            output_size=args.output_size,
            max_candidates_per_image=args.max_candidates_per_image,
        ),
    )
    wait_dataset_ready(api, payload_ref, timeout_seconds=180, expected_files=["kaggle_run.json", "line_art_batch_config.json"])
    kernel_ref = push_kernel(api, kernel_slug=slugify(args.kernel_slug, max_len=48), dataset_sources=[payload_ref], accelerator=args.accelerator, session_timeout_seconds=args.session_timeout_seconds)
    status = poll_kernel(api, kernel_ref, timeout_minutes=args.timeout_minutes, poll_interval_seconds=args.poll_interval_seconds)

    out_dir = args.output_root / run_slug
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded = download_kernel_output(api, kernel_ref, out_dir)
    summary = {
        "run_id": run_id,
        "kernel_ref": kernel_ref,
        "kernel_slug": slugify(args.kernel_slug, max_len=48),
        "source_images": [str(path) for path in source_images],
        "branches": args.branches,
        "guide_ids": args.guide_ids,
        "seeds": args.seeds,
        "steps": args.steps,
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
