#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

try:
    from video_announce.kaggle_client import KaggleClient  # type: ignore  # noqa: E402
except Exception:  # pragma: no cover - lightweight fallback when app deps are missing
    KaggleClient = None  # type: ignore


class DirectKaggleClient:
    def __init__(self) -> None:
        from kaggle.api.kaggle_api_extended import KaggleApi
        self.api = KaggleApi()
        self.api.authenticate()

    def create_dataset(self, folder, *, public=False, quiet=True, convert_to_csv=False, dir_mode="zip") -> None:
        self.api.dataset_create_new(str(folder), public=public, quiet=quiet, convert_to_csv=convert_to_csv, dir_mode=dir_mode)

    def delete_dataset(self, dataset: str, *, no_confirm: bool = True) -> None:
        owner, slug = dataset.split("/", 1)
        self.api.dataset_delete(owner, slug, no_confirm=no_confirm)

    def push_kernel(self, *, kernel_path, dataset_sources=None) -> None:
        meta_path = Path(kernel_path) / "kernel-metadata.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if dataset_sources is not None:
            meta["dataset_sources"] = [str(x) for x in dataset_sources if str(x).strip()]
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        self.api.kernels_push(str(kernel_path))

    def get_kernel_status(self, kernel_ref: str) -> dict[str, Any]:
        response = self.api.kernels_status(kernel_ref)
        if hasattr(response, "to_dict"):
            out = response.to_dict()
        else:
            try:
                out = json.loads(str(response))
            except Exception:
                out = {}
        if not out.get("status"):
            status = getattr(response, "status", None)
            if status is not None:
                out["status"] = status.name if hasattr(status, "name") else str(status)
        failure = getattr(response, "failure_message", None) or getattr(response, "failureMessage", None)
        if failure and not out.get("failureMessage"):
            out["failureMessage"] = failure
        return out

    def download_kernel_output(self, kernel_ref: str, *, path, force=True) -> list[str]:
        files, _ = self.api.kernels_output(kernel_ref, path=str(path), force=force, quiet=False)
        return [str(x) for x in files]

KERNEL_PATH = PROJECT_ROOT / "kaggle" / "RegionTalkCandidateReport"
ARTIFACT_ROOT = PROJECT_ROOT / "artifacts" / "codex" / "kaggle" / "region-talk-candidate-report"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip(); value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def slugify(value: str, *, max_len: int = 48) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return (slug or uuid.uuid4().hex[:8])[:max_len].rstrip("-")


def create_or_replace_dataset(client: Any, username: str, slug: str, title: str, writer) -> str:
    dataset_ref = f"{username}/{slug}"
    with tempfile.TemporaryDirectory() as tmp_dir:
        folder = Path(tmp_dir)
        writer(folder)
        (folder / "dataset-metadata.json").write_text(
            json.dumps({"title": title, "id": dataset_ref, "licenses": [{"name": "CC0-1.0"}]}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        try:
            client.create_dataset(folder, public=False, quiet=True, convert_to_csv=False, dir_mode="zip")
        except Exception:
            try:
                client.delete_dataset(dataset_ref)
            except Exception:
                pass
            client.create_dataset(folder, public=False, quiet=True, convert_to_csv=False, dir_mode="zip")
    return dataset_ref


def build_input_datasets(client: Any, *, run_id: str, username: str) -> list[str]:
    from cryptography.fernet import Fernet
    safe_slug = slugify(run_id, max_len=32)
    env_config = {
        "REGION_TALK_RUN_ID": run_id,
        "REGION_TALK_DRY_RUN": "1",
        "REGION_TALK_DISABLE_PUBLISH": "1",
        "REGION_TALK_MAX_SOURCES": os.environ.get("REGION_TALK_MAX_SOURCES", "5"),
        "REGION_TALK_MAX_POSTS_PER_SOURCE": os.environ.get("REGION_TALK_MAX_POSTS_PER_SOURCE", "20"),
        "REGION_TALK_MAX_IMAGES_PER_POST": os.environ.get("REGION_TALK_MAX_IMAGES_PER_POST", "8"),
        "REGION_TALK_MAX_LLM_CALLS": os.environ.get("REGION_TALK_MAX_LLM_CALLS", "0"),
        "REGION_TALK_MAX_VLM_CALLS": os.environ.get("REGION_TALK_MAX_VLM_CALLS", "0"),
        "REGION_TALK_IMAGE_SCORING_MODE": os.environ.get("REGION_TALK_IMAGE_SCORING_MODE", "cv_only"),
        "REGION_TALK_AUTH_BUNDLE_ENV": os.environ.get("REGION_TALK_AUTH_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_DISCOVERY"),
        "REGION_TALK_OUTPUT_DIR": f"artifacts/region-talk/runs/{run_id}",
    }
    secret_names = [
        "TG_API_ID", "TG_API_HASH", "TELEGRAM_API_ID", "TELEGRAM_API_HASH",
        "TELEGRAM_AUTH_BUNDLE_DISCOVERY", "TELEGRAM_AUTH_BUNDLE_S22", "TG_SESSION", "TELEGRAM_SESSION",
    ]
    secrets = {name: os.environ.get(name) for name in secret_names if (os.environ.get(name) or "").strip()}
    key = Fernet.generate_key()
    encrypted = Fernet(key).encrypt(json.dumps(secrets, ensure_ascii=False).encode("utf-8"))

    def write_config(folder: Path) -> None:
        shutil.copy2(PROJECT_ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv", folder / "seed-sources-v1.csv")
        (folder / "region_talk_run_config.json").write_text(json.dumps({"run_id": run_id, "env": env_config, "seed_file": "seed-sources-v1.csv"}, ensure_ascii=False, indent=2), encoding="utf-8")

    def write_secret(folder: Path) -> None:
        (folder / "region_talk_secrets.enc").write_bytes(encrypted)

    def write_key(folder: Path) -> None:
        (folder / "region_talk_fernet.key").write_bytes(key)

    config_ref = create_or_replace_dataset(client, username, f"region-talk-config-{safe_slug}", f"Region Talk config {safe_slug}", write_config)
    secret_ref = create_or_replace_dataset(client, username, f"region-talk-secrets-{safe_slug}", f"Region Talk secrets {safe_slug}", write_secret)
    key_ref = create_or_replace_dataset(client, username, f"region-talk-key-{safe_slug}", f"Region Talk key {safe_slug}", write_key)
    return [config_ref, secret_ref, key_ref]


def kernel_ref_from_meta(kernel_path: Path, *, kernel_slug: str | None = None) -> str:
    meta = json.loads((kernel_path / "kernel-metadata.json").read_text(encoding="utf-8"))
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    raw_id = str(meta.get("id") or "").strip()
    slug = kernel_slug or str(meta.get("slug") or raw_id.split("/", 1)[-1] or kernel_path.name).strip()
    if username:
        return f"{username}/{slug}"
    return raw_id if "/" in raw_id else f"zigomaro/{slug}"


def prepared_kernel_path(*, run_id: str, kernel_slug: str | None) -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="region-talk-kernel-"))
    dst = tmp / KERNEL_PATH.name
    shutil.copytree(KERNEL_PATH, dst, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"))
    shutil.copy2(PROJECT_ROOT / "kaggle" / "kaggle_status_client.py", dst / "kaggle_status_client.py")
    meta_path = dst / "kernel-metadata.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    slug = kernel_slug or str(meta.get("slug") or "region-talk-candidate-report")
    if username:
        meta["id"] = f"{username}/{slug}"
    meta["slug"] = slug
    meta["title"] = f"Region Talk Candidate Report {run_id}"[:100]
    meta["enable_gpu"] = False
    meta["enable_internet"] = True
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst


def poll_kernel(client: Any, kernel_ref: str, *, timeout_minutes: int, poll_interval_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + max(60, timeout_minutes * 60)
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        time.sleep(max(5, poll_interval_seconds))
        status = client.get_kernel_status(kernel_ref)
        last = status
        raw = str(status.get("status") or "").upper()
        print(f"[region-talk-kaggle] status={raw} raw={status}", flush=True)
        if raw == "COMPLETE":
            return status
        if raw in {"ERROR", "FAILED", "CANCELLED"}:
            raise RuntimeError(f"Kaggle Region Talk run failed: {status}")
    raise TimeoutError(f"Kaggle Region Talk timeout; last_status={last}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run Region Talk Candidate Report on Kaggle")
    ap.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--kernel-slug", default="region-talk-candidate-report")
    ap.add_argument("--timeout-minutes", type=int, default=20)
    ap.add_argument("--poll-interval-seconds", type=int, default=20)
    ap.add_argument("--no-wait", action="store_true")
    ap.add_argument("--download-output", action="store_true", default=True)
    ap.add_argument("--max-sources", type=int, default=5)
    args = ap.parse_args()
    load_env_file(args.env_file)
    run_id = args.run_id or "region-talk-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ.setdefault("REGION_TALK_RUN_ID", run_id)
    os.environ.setdefault("REGION_TALK_DRY_RUN", "1")
    os.environ.setdefault("REGION_TALK_DISABLE_PUBLISH", "1")
    os.environ.setdefault("REGION_TALK_MAX_SOURCES", str(args.max_sources))
    os.environ.setdefault("REGION_TALK_MAX_POSTS_PER_SOURCE", "20")
    os.environ.setdefault("REGION_TALK_MAX_IMAGES_PER_POST", "8")
    os.environ.setdefault("REGION_TALK_MAX_LLM_CALLS", "0")
    os.environ.setdefault("REGION_TALK_MAX_VLM_CALLS", "0")
    os.environ.setdefault("REGION_TALK_IMAGE_SCORING_MODE", "cv_only")
    kernel_path = prepared_kernel_path(run_id=run_id, kernel_slug=args.kernel_slug)
    client = KaggleClient() if KaggleClient is not None else DirectKaggleClient()
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME is required")
    dataset_sources = build_input_datasets(client, run_id=run_id, username=username)
    kernel_ref = kernel_ref_from_meta(kernel_path, kernel_slug=args.kernel_slug)
    print(f"[region-talk-kaggle] pushing {kernel_ref} run_id={run_id} datasets={len(dataset_sources)}", flush=True)
    client.push_kernel(kernel_path=kernel_path, dataset_sources=dataset_sources)
    if args.no_wait:
        print(f"[region-talk-kaggle] pushed {kernel_ref}; not waiting", flush=True)
        return 0
    poll_kernel(client, kernel_ref, timeout_minutes=args.timeout_minutes, poll_interval_seconds=args.poll_interval_seconds)
    out_dir = ARTIFACT_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    files = client.download_kernel_output(kernel_ref, path=out_dir, force=True)
    print(f"[region-talk-kaggle] downloaded {len(files)} files to {out_dir}", flush=True)
    latest = out_dir / "candidates-latest.xlsx"
    if latest.exists():
        public_latest = PROJECT_ROOT / "artifacts" / "region-talk" / "candidates-latest.xlsx"
        public_latest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(latest, public_latest)
        print(f"[region-talk-kaggle] latest workbook: {public_latest}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
