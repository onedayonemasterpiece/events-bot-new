#!/usr/bin/env python3
"""Artifact-only Kaggle runtime for the service-share Blender still."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import tarfile
import time
import traceback
from pathlib import Path

from PIL import Image



def _load_status_client():
    """Load the shipped helper from code or a private input dataset."""
    try:
        from kaggle_status_client import load_status_client as loader
        return loader
    except Exception:
        pass
    roots = [Path("/kaggle/src"), Path("/kaggle/working"), Path("/kaggle/input")]
    candidates = []
    for root in roots:
        direct = root / "kaggle_status_client.py"
        if direct.is_file():
            candidates.append(direct)
        if root.exists():
            candidates.extend(sorted(root.rglob("kaggle_status_client.py")))
    for path in candidates:
        spec = importlib.util.spec_from_file_location("events_bot_kaggle_status_client", path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.load_status_client
    raise ModuleNotFoundError("kaggle_status_client.py not found in code or /kaggle/input")


load_status_client = _load_status_client()


WORK = Path("/kaggle/working")
INPUT = Path("/kaggle/input")
EXTRACTED = Path("/tmp/service-share-still")
BLENDER_ROOT = Path("/tmp/blender-4.5.0")
BLENDER = BLENDER_ROOT / "blender"
BLENDER_URL = "https://download.blender.org/release/Blender4.5/blender-4.5.0-linux-x64.tar.xz"
BLENDER_MIRROR_URL = "https://mirrors.ocf.berkeley.edu/blender/release/Blender4.5/blender-4.5.0-linux-x64.tar.xz"
STATUS = load_status_client(output_dir=WORK, log=lambda message: print(message, flush=True))
PROGRESS: dict = {"phase":"preflight","progress_percent":1,"progress_label":"подготовка"}
ACQUIRED_RESOURCES: list[str] = []


def find_input(name: str) -> Path:
    matches = sorted(path for path in INPUT.rglob(name) if path.is_file())
    if not matches:
        raise FileNotFoundError(f"{name} not found in /kaggle/input")
    return matches[0]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def emit(event: str, *, phase: str, status: str = "running", message: str | None = None, **progress) -> None:
    PROGRESS.update({key:value for key,value in progress.items() if value is not None})
    PROGRESS["phase"] = phase
    payload = {"event":event,"phase":phase,"status":status,"progress":dict(PROGRESS)}
    if message:
        payload["message"] = message
    print("[service_share_status] " + json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
    STATUS.event(event, phase=phase, status=status, progress=dict(PROGRESS), message=message)


def progress_snapshot() -> dict:
    return dict(PROGRESS)


def install_blender() -> None:
    if BLENDER.exists():
        return
    archive = Path("/tmp/blender-4.5.0.tar.xz")
    last_error = None
    for url in (BLENDER_URL, BLENDER_MIRROR_URL):
        print(f"Downloading {url}", flush=True)
        result = subprocess.run(
            ["wget", "-q", "--user-agent=Mozilla/5.0", "--timeout=60", "--tries=2", "-O", str(archive), url],
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 and archive.exists() and archive.stat().st_size > 100_000_000:
            break
        last_error = f"wget exit={result.returncode} stderr={result.stderr[-500:]}"
        archive.unlink(missing_ok=True)
    else:
        raise RuntimeError(f"Blender download failed: {last_error}")
    BLENDER_ROOT.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:xz") as tar:
        members = tar.getmembers()
        prefix = members[0].name.split("/", 1)[0]
        for member in members:
            if not member.name.startswith(prefix + "/"):
                continue
            member.name = member.name[len(prefix) + 1:]
            if member.name:
                tar.extract(member, BLENDER_ROOT, filter="data")
    archive.unlink(missing_ok=True)
    if not BLENDER.exists():
        raise RuntimeError("Blender 4.5.0 download/extract failed")


def run_blender(config_path: Path, bundle: Path, config: dict) -> dict:
    command = [
        str(BLENDER), "-b", "--factory-startup",
        "--python", str(bundle / "tools" / "render_scene.py"),
        "--", "--config", str(config_path), "--bundle-root", str(bundle), "--output-dir", str(WORK),
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    samples_total = int(config["samples"])
    last_emit = 0
    actual_device = None
    composition = None
    log_path = WORK / "render.log"
    with log_path.open("w", encoding="utf-8") as log:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            log.write(line)
            if line.startswith("SERVICE_SHARE_PREFLIGHT "):
                actual_device = json.loads(line.split(" ", 1)[1])["device"]
            if line.startswith("SERVICE_SHARE_COMPOSITION "):
                composition = json.loads(line.split(" ", 1)[1])
            match = re.search(r"Sample\s+(\d+)/(\d+)", line)
            if match:
                done = int(match.group(1))
                total = int(match.group(2)) or samples_total
                now = time.monotonic()
                step = 4 if samples_total <= 24 else 8
                if done == total or done - last_emit >= step or now - float(PROGRESS.get("last_sample_emit", 0)) >= 60:
                    last_emit = done
                    PROGRESS["last_sample_emit"] = now
                    percent = 20 + round(62 * min(1, done / total))
                    emit(
                        "alive", phase="render", status="alive",
                        samples_done=done, samples_total=total, progress_percent=percent,
                        progress_label=f"{config['profile']} · samples {done}/{total}",
                    )
    code = process.wait()
    if code != 0:
        raise RuntimeError(f"Blender failed with exit code {code}")
    if not composition or not composition.get("gates_passed"):
        raise RuntimeError("renderer did not return a passing composition contract")
    return {
        "device": actual_device or {"requested":config["device"],"actual":"unknown"},
        "composition": composition,
    }


def main() -> int:
    started = time.monotonic()
    config_path = find_input("render_config.json")
    tar_path = find_input("service_share_bundle.tarball")
    config = json.loads(config_path.read_text())
    bundle_hash = sha256(tar_path)
    if bundle_hash != config["bundle_sha256"]:
        raise RuntimeError("bundle SHA mismatch")
    emit("kernel_started", phase="preflight", progress_percent=2, progress_label="kernel started", render_profile=config["profile"], bundle_sha256=bundle_hash)
    for resource_key in STATUS.config.get("resource_leases") or []:
        if not STATUS.acquire_resource(str(resource_key), ttl_seconds=7200):
            raise RuntimeError(f"resource lease unavailable: {resource_key}")
        ACQUIRED_RESOURCES.append(str(resource_key))
    STATUS.start_alive(interval_seconds=60, progress_provider=progress_snapshot)
    shutil.rmtree(EXTRACTED, ignore_errors=True)
    EXTRACTED.mkdir(parents=True)
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(EXTRACTED, filter="data")
    bundle = EXTRACTED / "bundle"
    install_blender()
    version = subprocess.check_output([str(BLENDER), "--version"], text=True).splitlines()[0]
    emit("preflight_ok", phase="preflight", progress_percent=15, progress_label=f"{version} · {config['device']}", blender_version=version, requested_device=config["device"])
    emit("render_started", phase="render", progress_percent=20, progress_label=f"{config['profile']} · render started", samples_done=0, samples_total=config["samples"])
    runtime = run_blender(config_path, bundle, config)
    actual_device = runtime["device"]
    composition = runtime["composition"]
    emit("alive", phase="composite", status="alive", progress_percent=86, progress_label="компоновка продукта", actual_device=actual_device)
    output = WORK / config["output_filename"]
    subprocess.run([
        "python3", str(bundle / "tools" / "composite_product.py"),
        "--base", str(WORK / config["base_filename"]),
        "--output", str(output),
        "--bundle-root", str(bundle),
        "--config", str(config_path),
    ], check=True)
    emit("alive", phase="qa", status="alive", progress_percent=94, progress_label="проверка master")
    image = Image.open(output)
    if image.size != (int(config["resolution"]), int(config["resolution"])):
        raise RuntimeError(f"wrong output size: {image.size}")
    rgb = image.convert("RGB")
    near_black = sum(1 for red, green, blue in rgb.getdata() if max(red, green, blue) < 8)
    near_black_fraction = near_black / max(1, image.width * image.height)
    if near_black_fraction > .12:
        raise RuntimeError(f"alpha/composite QA failed: near_black_fraction={near_black_fraction:.6f}")
    output_hash = sha256(output)
    qa = {
        "artifact_only": bool(config.get("artifact_only", True)),
        "profile": config["profile"],
        "resolution": list(image.size),
        "samples": config["samples"],
        "requested_device": config["device"],
        "actual_device": actual_device,
        "bundle_sha256": bundle_hash,
        "output_sha256": output_hash,
        "texture_extension": "CLIP",
        "global_snapshot_date_present": False,
        "event_dates_on_faces": True,
        "selection_mix": config["selection_mix"],
        "selection_requested_mix": config.get("selection_requested_mix"),
        "promo_status": config.get("promo_status"),
        "catalog_hash": config.get("catalog_hash"),
        "measured_at": config.get("measured_at"),
        "composition_date": config["composition_date"],
        "composition_family_requested": config["composition_family"],
        "composition_seed_input": config.get("composition_seed", ""),
        "composition": composition,
        "near_black_fraction": round(near_black_fraction, 8),
    }
    (WORK / "scene_qa.json").write_text(json.dumps(qa, ensure_ascii=False, indent=2) + "\n")
    thumbnail = image.convert("RGB").resize((360,360), Image.Resampling.LANCZOS)
    thumbnail.save(WORK / config["thumbnail_filename"])
    emit("render_done", phase="qa", status="done", progress_percent=98, progress_label="master готов", output_bytes=output.stat().st_size, output_sha256=output_hash)
    result = {
        **qa,
        "ok": True,
        "pipeline_run_id": config["run_id"],
        "duration_seconds": round(time.monotonic() - started, 3),
        "output_filename": output.name,
        "base_filename": config["base_filename"],
        "blend_filename": config["blend_filename"],
    }
    (WORK / "service_share_render_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    if not config.get("keep_blend", True):
        (WORK / config["blend_filename"]).unlink(missing_ok=True)
        Path(str(WORK / config["blend_filename"]) + "1").unlink(missing_ok=True)
    emit("report_written", phase="report", status="done", progress_percent=100, progress_label="отчёт записан")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        emit("report_written", phase="failed", status="failed", progress_percent=100, progress_label="ошибка", message="".join(traceback.format_exception_only(type(exc), exc)).strip())
        raise
    finally:
        STATUS.stop_alive()
        for resource_key in reversed(ACQUIRED_RESOURCES):
            STATUS.release_resource(resource_key)
        shutil.rmtree(BLENDER_ROOT, ignore_errors=True)
        shutil.rmtree(EXTRACTED, ignore_errors=True)
