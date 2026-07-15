#!/usr/bin/env python3
"""Stage, run and audit the artifact-only service-share Blender still on Kaggle."""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import tarfile
import tempfile
import time
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.research.service_share_poster_cubes.layout_contract import (
    FAMILY_ORDER as COMPOSITION_FAMILIES,
    resolve_layout,
)
KERNEL_SOURCE = ROOT / "kaggle" / "ServiceShareStill"
RENDER_SOURCE = ROOT / "scripts" / "research" / "service_share_poster_cubes"
BRAND_SOURCE = RENDER_SOURCE / "assets" / "brand"
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts" / "codex" / "service-share-poster-cubes-research-v7" / "kaggle"
LOCK_PATH = ROOT / "artifacts" / "service-share-still-kaggle.lock"
PROFILES = {
    "debug-gpu": {"device":"GPU","resolution":512,"samples":24,"enable_gpu":True,"kernel_slug":"service-share-still-debug"},
    "final-cpu": {"device":"CPU","resolution":1024,"samples":256,"enable_gpu":False,"kernel_slug":"service-share-still-final"},
}


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_bundle_inputs(bundle_dir: Path, stage: Path) -> None:
    bundle = stage / "bundle"
    (bundle / "tools").mkdir(parents=True)
    shutil.copy2(RENDER_SOURCE / "render_scene.py", bundle / "tools" / "render_scene.py")
    shutil.copy2(RENDER_SOURCE / "composite_product.py", bundle / "tools" / "composite_product.py")
    shutil.copy2(RENDER_SOURCE / "layout_contract.py", bundle / "tools" / "layout_contract.py")
    brand_source = bundle_dir / "brand" if (bundle_dir / "brand").is_dir() else BRAND_SOURCE
    if not brand_source.is_dir():
        raise FileNotFoundError(brand_source)
    shutil.copytree(brand_source, bundle / "brand")
    fonts_source = bundle_dir / "fonts"
    if fonts_source.is_dir():
        shutil.copytree(fonts_source, bundle / "fonts")
    else:
        (bundle / "fonts").mkdir()
        for name, source in {
            "Cygre-ExtraBold.ttf": ROOT / "assets" / "fonts" / "Cygre-ExtraBold.ttf",
            "Cygre-SemiBold.ttf": ROOT / "assets" / "fonts" / "Cygre-SemiBold.ttf",
            "Cygre-Regular.ttf": ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts" / "Cygre-Regular.ttf",
        }.items():
            if not source.is_file():
                raise FileNotFoundError(source)
            shutil.copy2(source, bundle / "fonts" / name)
    required_fonts = {
        "Cygre-Bold.ttf": ROOT / "assets" / "fonts" / "Cygre-Bold.ttf",
        "Cygre-ExtraBold.ttf": ROOT / "assets" / "fonts" / "Cygre-ExtraBold.ttf",
        "Cygre-SemiBold.ttf": ROOT / "assets" / "fonts" / "Cygre-SemiBold.ttf",
        "Cygre-Regular.ttf": ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts" / "Cygre-Regular.ttf",
    }
    for name, source in required_fonts.items():
        target = bundle / "fonts" / name
        if not target.exists():
            if not source.is_file():
                raise FileNotFoundError(source)
            shutil.copy2(source, target)
    faces_source = bundle_dir / "kaggle_faces"
    manifest = faces_source / "face_manifest.json"
    if not manifest.exists():
        raise FileNotFoundError(manifest)
    (bundle / "faces").mkdir()
    shutil.copy2(manifest, bundle / "faces" / manifest.name)
    data = json.loads(manifest.read_text())
    for row in data.get("faces") or []:
        name = Path(row["face_path"]).name
        shutil.copy2(faces_source / name, bundle / "faces" / name)


def deterministic_tar(source_root: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for path in sorted(source_root.rglob("*"), key=lambda item: item.as_posix()):
                    relative = path.relative_to(source_root).as_posix()
                    info = archive.gettarinfo(str(path), arcname=relative)
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mtime = 0
                    if path.is_file():
                        with path.open("rb") as handle:
                            archive.addfile(info, handle)
                    else:
                        archive.addfile(info)


def build_bundle(bundle_dir: Path, staging_root: Path) -> tuple[Path, str, dict]:
    content = staging_root / "bundle-content"
    content.mkdir()
    copy_bundle_inputs(bundle_dir, content)
    # Neutral extension prevents Kaggle from auto-extracting the archive into the
    # input dataset and dropping the original bundle/hash boundary.
    tar_path = staging_root / "service_share_bundle.tarball"
    deterministic_tar(content, tar_path)
    manifest = json.loads((content / "bundle" / "faces" / "face_manifest.json").read_text())
    return tar_path, sha256(tar_path), manifest


def stage_kernel(staging: Path, *, username: str, profile: str) -> str:
    shutil.copytree(KERNEL_SOURCE, staging, dirs_exist_ok=True)
    metadata_path = staging / "kernel-metadata.json"
    metadata = json.loads(metadata_path.read_text())
    settings = PROFILES[profile]
    slug = settings["kernel_slug"]
    metadata.update({
        "id": f"{username}/{slug}",
        "slug": slug,
        # Kaggle's official metadata contract links title and slug: the slug is
        # the lowercased title with spaces replaced by dashes.
        "title": "Service Share Still Debug" if profile == "debug-gpu" else "Service Share Still Final",
        "enable_gpu": settings["enable_gpu"],
        "enable_internet": True,
        "is_private": True,
    })
    if settings["enable_gpu"]:
        metadata["machine_shape"] = "NvidiaTeslaT4"
    else:
        metadata.pop("machine_shape", None)
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    return f"{username}/{slug}"


def create_dataset(client, dataset_dir: Path) -> None:
    shutil.rmtree(Path(tempfile.gettempdir()) / ".kaggle" / "uploads", ignore_errors=True)
    client.create_dataset(dataset_dir, public=False, quiet=True)


def wait_dataset_ready(client, dataset_ref: str, expected_files: list[str], timeout=240) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            status = client.dataset_status(dataset_ref)
            files = [row["name"] for row in client.dataset_list_files(dataset_ref, page_size=30)]
            last = {"status":status,"files":files}
            print(f"[service-share-kaggle] dataset status={status} files={files}", flush=True)
            if str(status).lower() == "ready" and all(name in files for name in expected_files):
                return
        except Exception as exc:
            last = {"error":str(exc)}
        time.sleep(5)
    raise TimeoutError(f"dataset not ready: {dataset_ref} last={last}")


def live_status_lines(api, kernel_ref: str, seen: set[str]) -> None:
    try:
        logs = str(api.kernels_logs(kernel_ref) or "")
    except Exception as exc:
        print(f"[service-share-kaggle] logs unavailable: {exc}", flush=True)
        return
    for raw in logs.splitlines():
        line = raw.strip()
        if "[service_share_status]" not in line or line in seen:
            continue
        seen.add(line)
        print(f"[service-share-kaggle] internal {line[:2000]}", flush=True)


def validate_output(out_dir: Path, config: dict, *, require_bundle_sha: str) -> dict:
    result_path = out_dir / "service_share_render_result.json"
    if not result_path.exists():
        raise RuntimeError("service_share_render_result.json missing")
    result = json.loads(result_path.read_text())
    expected_size = int(config["resolution"])
    if not result.get("ok") or not result.get("artifact_only"):
        raise RuntimeError(f"invalid result contract: {result}")
    if result.get("bundle_sha256") != require_bundle_sha:
        raise RuntimeError("downloaded result bundle SHA mismatch")
    if result.get("resolution") != [expected_size, expected_size]:
        raise RuntimeError(f"wrong result resolution: {result.get('resolution')}")
    if result.get("global_snapshot_date_present") is not False or result.get("event_dates_on_faces") is not True:
        raise RuntimeError("event-date placement contract failed")
    if result.get("selection_mix") != config.get("selection_mix"):
        raise RuntimeError("selection mix contract failed")
    composition = result.get("composition") or {}
    if not composition.get("gates_passed"):
        raise RuntimeError("composition gates did not pass")
    if result.get("composition_date") != config.get("composition_date"):
        raise RuntimeError("composition date contract failed")
    if result.get("composition_family_requested") != config.get("composition_family"):
        raise RuntimeError("composition family contract failed")
    if result.get("composition_seed_input") != config.get("composition_seed"):
        raise RuntimeError("composition seed contract failed")
    expected_family, expected_seed, _ = resolve_layout(config)
    if composition.get("family") != expected_family or composition.get("seed") != expected_seed:
        raise RuntimeError("resolved composition identity failed")
    if config["profile"] == "debug-gpu" and (result.get("actual_device") or {}).get("actual") != "GPU":
        raise RuntimeError(f"GPU debug silently fell back: {result.get('actual_device')}")
    output = out_dir / result["output_filename"]
    if not output.exists() or sha256(output) != result["output_sha256"]:
        raise RuntimeError("output file/hash mismatch")
    return result


def create_status_input_dataset(
    client, *, username: str, run_id: str, kernel_ref: str, dataset_ref: str,
    status_db: str | None, callback_url: str | None,
) -> str | None:
    """Create a signed standard status input only when fully configured."""
    if not (status_db and callback_url):
        return None
    from db import Database
    from kaggle_status import create_kaggle_run_config, create_kaggle_status_dataset
    db = Database(status_db)

    async def build() -> dict | None:
        try:
            return await create_kaggle_run_config(
                db, run_id=run_id, session_id=None, kind="service_share_still",
                notebook="ServiceShareStill", kernel_ref=kernel_ref, dataset_ref=dataset_ref,
                callback_url=callback_url, resource_leases=["service_share:renderer"],
            )
        finally:
            await db.close()

    config = asyncio.run(build())
    return create_kaggle_status_dataset(
        client, username=username, slug_prefix="service-share-status", run_id=run_id, config=config,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--profile", choices=sorted(PROFILES), required=True)
    default_env = ROOT / ".env"
    if not default_env.exists() and Path("/home/dev/projects/events-bot-new/.env").exists():
        default_env = Path("/home/dev/projects/events-bot-new/.env")
    parser.add_argument("--env-file", default=str(default_env))
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--poll-interval", type=int, default=20)
    parser.add_argument("--timeout-minutes", type=int, default=60)
    parser.add_argument("--require-debug-result")
    parser.add_argument("--catalog-snapshot", required=True)
    parser.add_argument("--status-db")
    parser.add_argument("--status-callback-url")
    parser.add_argument(
        "--composition-date",
        default=datetime.now(ZoneInfo("Europe/Kaliningrad")).date().isoformat(),
        help="Local date used for stable daily composition rotation.",
    )
    parser.add_argument("--composition-family", choices=("auto", *COMPOSITION_FAMILIES), default="auto")
    parser.add_argument("--composition-seed", default="")
    parser.add_argument("--keep-dataset", action="store_true")
    parser.add_argument("--keep-staging", action="store_true")
    args = parser.parse_args()
    load_env(Path(args.env_file))
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME is required")
    profile = args.profile
    settings = PROFILES[profile]
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    phase = "debug" if profile == "debug-gpu" else "final"
    run_id = f"service-share:{args.composition_date}:{phase}:{run_stamp}"
    artifact_dir = Path(args.artifact_root) / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with tempfile.TemporaryDirectory(prefix="service-share-still-") as temp:
            temp_root = Path(temp)
            kernel_stage = temp_root / "kernel"
            dataset_dir = temp_root / "dataset"
            bundle_stage = temp_root / "bundle-stage"
            kernel_stage.mkdir()
            dataset_dir.mkdir()
            bundle_stage.mkdir()
            tar_path, bundle_hash, face_manifest = build_bundle(Path(args.bundle_dir), bundle_stage)
            snapshot = json.loads(Path(args.catalog_snapshot).read_text())
            if profile == "final-cpu":
                if not args.require_debug_result:
                    raise RuntimeError("final-cpu requires --require-debug-result")
                debug = json.loads(Path(args.require_debug_result).read_text())
                if not debug.get("ok") or debug.get("profile") != "debug-gpu" or debug.get("bundle_sha256") != bundle_hash:
                    raise RuntimeError("debug result does not approve this exact bundle")
                if (debug.get("actual_device") or {}).get("actual") != "GPU":
                    raise RuntimeError("debug result is not a verified GPU run")
                if debug.get("composition_date") != args.composition_date:
                    raise RuntimeError("debug result approves another composition date")
                if debug.get("composition_family_requested") != args.composition_family:
                    raise RuntimeError("debug result approves another composition family request")
                if debug.get("composition_seed_input") != args.composition_seed:
                    raise RuntimeError("debug result approves another composition seed")
                expected_family, expected_seed, _ = resolve_layout({
                    "composition_date": args.composition_date,
                    "composition_family": args.composition_family,
                    "composition_seed": args.composition_seed,
                })
                debug_composition = debug.get("composition") or {}
                if not debug_composition.get("gates_passed") or debug_composition.get("family") != expected_family or debug_composition.get("seed") != expected_seed:
                    raise RuntimeError("debug result did not pass this resolved composition")
            kernel_ref = stage_kernel(kernel_stage, username=username, profile=profile)
            dataset_slug = re.sub(r"[^a-z0-9-]+", "-", run_id.lower().replace("_", "-"))[:48].strip("-")
            dataset_ref = f"{username}/{dataset_slug}"
            shutil.copy2(tar_path, dataset_dir / tar_path.name)
            shutil.copy2(ROOT / "kaggle" / "kaggle_status_client.py", dataset_dir / "kaggle_status_client.py")
            config = {
                "run_id": run_id,
                "profile": profile,
                "device": settings["device"],
                "resolution": settings["resolution"],
                "samples": settings["samples"],
                "adaptive_threshold": .025,
                "adaptive_min_samples": 16,
                "bundle_sha256": bundle_hash,
                "selection_mix": face_manifest["selection"].get("actual_mix") or face_manifest["selection"]["requested_mix"],
                "selection_requested_mix": face_manifest["selection"]["requested_mix"],
                "promo_status": face_manifest["selection"].get("promo_status"),
                "promo_shortfall": face_manifest["selection"].get("promo_shortfall"),
                "composition_date": args.composition_date,
                "composition_family": args.composition_family,
                "composition_seed": args.composition_seed,
                "keep_blend": profile == "final-cpu",
                "base_filename": f"service_share_base_{profile}_{settings['resolution']}.png",
                "output_filename": f"service_share_card_{profile}_{settings['resolution']}.png",
                "thumbnail_filename": f"service_share_card_{profile}_360.png",
                "blend_filename": f"service_share_{profile}.blend",
                "current_event_count": int(snapshot["eligible_event_count"]),
                "new_event_count_7d": int(snapshot["recent_added_count"]),
                "city_count": int(snapshot["city_count"]),
                "city_names": list(snapshot.get("city_names") or []),
                "catalog_hash": snapshot["catalog_hash"],
                "measured_at": snapshot["measured_at"],
                "artifact_only": True,
            }
            (dataset_dir / "render_config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n")
            (dataset_dir / "dataset-metadata.json").write_text(json.dumps({
                "title": run_id,"id":dataset_ref,"licenses":[{"name":"other"}],
            }, ensure_ascii=False, indent=2) + "\n")
            if args.keep_staging:
                kept = artifact_dir / "staging"
                shutil.copytree(temp_root, kept)
            from video_announce.kaggle_client import KaggleClient
            client = KaggleClient()
            status_ref = None
            try:
                create_dataset(client, dataset_dir)
                wait_dataset_ready(client, dataset_ref, ["service_share_bundle.tarball","render_config.json","kaggle_status_client.py"])
                status_ref = create_status_input_dataset(
                    client, username=username, run_id=run_id, kernel_ref=kernel_ref, dataset_ref=dataset_ref,
                    status_db=args.status_db, callback_url=args.status_callback_url,
                )
                sources = [dataset_ref] + ([status_ref] if status_ref else [])
                client.push_kernel(kernel_path=kernel_stage, dataset_sources=sources)
                print(f"[service-share-kaggle] pushed kernel={kernel_ref} dataset={dataset_ref} bundle={bundle_hash}", flush=True)
                api = client._get_api()
                deadline = time.monotonic() + args.timeout_minutes * 60
                seen: set[str] = set()
                last = {}
                while time.monotonic() < deadline:
                    time.sleep(max(10, args.poll_interval))
                    last = client.get_kernel_status(kernel_ref)
                    state = str(last.get("status") or "").upper()
                    print(f"[service-share-kaggle] state={state} raw={last}", flush=True)
                    live_status_lines(api, kernel_ref, seen)
                    if state == "COMPLETE":
                        break
                    if state in {"ERROR","FAILED","CANCELLED","CANCEL_ACKNOWLEDGED"}:
                        raise RuntimeError(f"Kaggle kernel failed: {last}")
                else:
                    raise TimeoutError(f"Kaggle timeout last={last}")
                client.download_kernel_output(kernel_ref, path=artifact_dir, force=True)
                result = validate_output(artifact_dir, config, require_bundle_sha=bundle_hash)
                receipt = {"ok":True,"run_id":run_id,"kernel_ref":kernel_ref,"dataset_ref":dataset_ref,"status_dataset_ref":status_ref,"artifact_dir":str(artifact_dir),"result":result}
                (artifact_dir / "launcher_receipt.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
                print(json.dumps(receipt, ensure_ascii=False), flush=True)
                return 0
            finally:
                if not args.keep_dataset:
                    for cleanup_ref in (dataset_ref, status_ref):
                        if not cleanup_ref:
                            continue
                        try:
                            client.delete_dataset(cleanup_ref, no_confirm=True)
                            print(f"[service-share-kaggle] deleted dataset={cleanup_ref}", flush=True)
                        except Exception as exc:
                            print(f"[service-share-kaggle] dataset cleanup failed ref={cleanup_ref}: {exc}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
