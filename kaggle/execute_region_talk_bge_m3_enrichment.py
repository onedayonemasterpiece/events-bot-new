#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util

_candidate_executor_path = PROJECT_ROOT / "kaggle" / "execute_region_talk_candidate_report.py"
_spec = importlib.util.spec_from_file_location("region_talk_candidate_executor", _candidate_executor_path)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"cannot load {_candidate_executor_path}")
_candidate_executor = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_candidate_executor)

DirectKaggleClient = _candidate_executor.DirectKaggleClient
KaggleClient = _candidate_executor.KaggleClient
create_or_replace_dataset = _candidate_executor.create_or_replace_dataset
wait_dataset_ready = _candidate_executor.wait_dataset_ready
load_env_file = _candidate_executor.load_env_file
poll_kernel = _candidate_executor.poll_kernel
preflight_ydb_access = _candidate_executor.preflight_ydb_access
run_dataset_slug = _candidate_executor.run_dataset_slug

KERNEL_PATH = PROJECT_ROOT / "kaggle" / "RegionTalkBgeM3Enrichment"
OUT_ROOT = PROJECT_ROOT / "artifacts" / "codex" / "kaggle" / "region-talk-bge-m3-enrichment"
ACTIVE_KERNEL_STATUSES = {"RUNNING", "PENDING", "QUEUED", "INITIALIZING"}
DEFAULT_BGE_KAGGLE_MODEL_SOURCE = "andreasbis/baai-bge-m3/Transformers/default/1"


def getenv_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def slugify(value: str, *, max_len: int = 48) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return (slug or hashlib.sha1(value.encode("utf-8")).hexdigest()[:8])[:max_len].rstrip("-")


def bge_secret_names() -> list[str]:
    names = [
        "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON",
        "REGION_TALK_YDB_IAM_TOKEN",
        "YC_IAM_TOKEN",
        "YDB_ACCESS_TOKEN",
    ]
    extra = (os.environ.get("REGION_TALK_KAGGLE_SECRET_NAMES") or "").strip()
    if extra:
        names.extend([x.strip() for x in re.split(r"[,;\s]+", extra) if x.strip()])
    # BGE enrichment must not package Telegram auth bundles; it never opens Telethon.
    return [name for name in dict.fromkeys(names) if not name.startswith("TELEGRAM_AUTH_BUNDLE") and name not in {"TELEGRAM_SESSION", "TG_API_ID", "TG_API_HASH", "TELEGRAM_API_ID", "TELEGRAM_API_HASH"}]


def create_secret_bundle_dataset(client: Any, *, username: str, run_id: str) -> str:
    from cryptography.fernet import Fernet
    secret_names = bge_secret_names()
    secrets = {name: os.environ.get(name) for name in secret_names if (os.environ.get(name) or "").strip()}
    key = Fernet.generate_key()
    encrypted = Fernet(key).encrypt(json.dumps(secrets, ensure_ascii=False).encode("utf-8"))
    safe_slug = run_dataset_slug(run_id)

    def writer(folder: Path) -> None:
        (folder / "region_talk_secrets.enc").write_bytes(encrypted)
        (folder / "region_talk_fernet.key").write_bytes(key)

    return create_or_replace_dataset(client, username, f"rt-bge-secret-{safe_slug}", f"RT BGE sec {safe_slug}", writer)


def build_config_dataset(client: Any, *, username: str, run_id: str, args: argparse.Namespace) -> str:
    safe_slug = run_dataset_slug(run_id)
    try:
        git_sha = os.popen("git rev-parse HEAD 2>/dev/null").read().strip()
        git_branch = os.popen("git rev-parse --abbrev-ref HEAD 2>/dev/null").read().strip()
    except Exception:
        git_sha = ""
        git_branch = ""
    env_config = {
        "GIT_SHA": os.environ.get("GIT_SHA", git_sha),
        "GIT_BRANCH": os.environ.get("GIT_BRANCH", git_branch),
        "REGION_TALK_RUN_ID": run_id,
        "REGION_TALK_DRY_RUN": "1",
        "REGION_TALK_DISABLE_PUBLISH": "1",
        "REGION_TALK_STATE_BACKEND": "ydb",
        "REGION_TALK_REQUIRE_YDB_STATE": "1",
        "REGION_TALK_AUTH_BUNDLE_ENV": "REGION_TALK_NO_TELEGRAM_BUNDLE",
        "REGION_TALK_KAGGLE_SECRET_NAMES": os.environ.get("REGION_TALK_KAGGLE_SECRET_NAMES", ""),
        "REGION_TALK_YDB_ENDPOINT": os.environ.get("REGION_TALK_YDB_ENDPOINT", ""),
        "REGION_TALK_YDB_DATABASE": os.environ.get("REGION_TALK_YDB_DATABASE", ""),
        "REGION_TALK_YDB_NAMESPACE": os.environ.get("REGION_TALK_YDB_NAMESPACE", "region_talk_compact"),
        "REGION_TALK_YDB_SELECT_PAGE_SIZE": os.environ.get("REGION_TALK_YDB_SELECT_PAGE_SIZE", "200"),
        "REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS", "8"),
        "REGION_TALK_YDB_CONNECT_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_YDB_CONNECT_TIMEOUT_SECONDS", "20"),
        "REGION_TALK_BGE_BATCH_LIMIT": str(args.batch_limit),
        "REGION_TALK_BGE_BATCH_SIZE": str(args.batch_size),
        "REGION_TALK_BGE_MAX_LENGTH": str(args.max_length),
        "REGION_TALK_BGE_MAX_RUNTIME_SECONDS": str(args.max_runtime_seconds),
        "REGION_TALK_BGE_INPUT_KINDS": os.environ.get("REGION_TALK_BGE_INPUT_KINDS", args.input_kinds),
        "REGION_TALK_BGE_E5_ONLY": os.environ.get("REGION_TALK_BGE_E5_ONLY", "1"),
        "REGION_TALK_BGE_YDB_SCAN_LIMIT": os.environ.get("REGION_TALK_BGE_YDB_SCAN_LIMIT", str(max(args.batch_limit * 5, 20000))),
        "REGION_TALK_BGE_REPROCESS_EXISTING": "1" if args.reprocess_existing else os.environ.get("REGION_TALK_BGE_REPROCESS_EXISTING", "0"),
        "REGION_TALK_BGE_STORE_DENSE_VECTORS": "1" if args.store_dense_vectors else os.environ.get("REGION_TALK_BGE_STORE_DENSE_VECTORS", "1"),
        "REGION_TALK_BGE_STORE_VECTOR_MAX_ROWS": str(args.store_vector_max_rows),
        "REGION_TALK_BGE_BACKEND": os.environ.get("REGION_TALK_BGE_BACKEND", "flagembedding"),
        "REGION_TALK_AUTO_INSTALL": os.environ.get("REGION_TALK_AUTO_INSTALL", "1"),
        "REGION_TALK_HF_HUB_DOWNLOAD_TIMEOUT": os.environ.get("REGION_TALK_HF_HUB_DOWNLOAD_TIMEOUT", "60"),
        "REGION_TALK_HF_HUB_ETAG_TIMEOUT": os.environ.get("REGION_TALK_HF_HUB_ETAG_TIMEOUT", "20"),
        "REGION_TALK_HF_HUB_DISABLE_XET": os.environ.get("REGION_TALK_HF_HUB_DISABLE_XET", "1"),
        "REGION_TALK_BGE_KAGGLE_MODEL_SOURCE": os.environ.get("REGION_TALK_BGE_KAGGLE_MODEL_SOURCE", DEFAULT_BGE_KAGGLE_MODEL_SOURCE),
        "REGION_TALK_BGE_USE_KAGGLEHUB_FALLBACK": os.environ.get("REGION_TALK_BGE_USE_KAGGLEHUB_FALLBACK", "1"),
        "REGION_TALK_OUTPUT_DIR": f"artifacts/region-talk/runs/{run_id}",
        "REGION_TALK_PLACE_LEXICON_FILE": "kaliningrad-place-lexicon-v1.csv",
    }

    def writer(folder: Path) -> None:
        feature_dir = PROJECT_ROOT / "docs" / "features" / "region-talk-channel"
        shutil.copy2(feature_dir / "kaliningrad-place-lexicon-v1.csv", folder / "kaliningrad-place-lexicon-v1.csv")
        (folder / "region_talk_run_config.json").write_text(json.dumps({"run_id": run_id, "env": env_config}, ensure_ascii=False, indent=2), encoding="utf-8")

    return create_or_replace_dataset(client, username, f"rt-bge-config-{safe_slug}", f"RT BGE config {safe_slug}", writer)


def prepared_kernel_path(*, run_id: str, kernel_slug: str) -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="region-talk-bge-kernel-"))
    dst = tmp / KERNEL_PATH.name
    shutil.copytree(KERNEL_PATH, dst, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"))
    meta_path = dst / "kernel-metadata.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if username:
        meta["id"] = f"{username}/{kernel_slug}"
    meta["slug"] = kernel_slug
    meta["title"] = "Region Talk BGE-M3 Enrichment"
    meta["enable_gpu"] = getenv_bool("REGION_TALK_BGE_ENABLE_GPU", False)
    meta["enable_internet"] = True
    model_source = str(os.getenv("REGION_TALK_BGE_KAGGLE_MODEL_SOURCE") or DEFAULT_BGE_KAGGLE_MODEL_SOURCE).strip()
    model_sources = [str(item).strip() for item in (meta.get("model_sources") or []) if str(item).strip()]
    if model_source and model_source not in model_sources:
        model_sources.append(model_source)
    meta["model_sources"] = model_sources
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst


def kernel_ref(*, username: str, kernel_slug: str) -> str:
    return f"{username}/{kernel_slug}"


def assert_bge_kernel_slot_free(client: Any, kernel_ref: str, *, allow_active: bool = False) -> None:
    if allow_active or getenv_bool("REGION_TALK_BGE_ALLOW_ACTIVE_KAGGLE_OVERWRITE", False):
        print("[region-talk-bge] WARNING active BGE kernel guard bypassed by explicit override", flush=True)
        return
    try:
        raw = str(client.get_kernel_status(kernel_ref).get("status") or "").upper()
    except Exception as exc:
        if getenv_bool("REGION_TALK_ALLOW_UNVERIFIED_KAGGLE_SLOT", False):
            print(f"[region-talk-bge] WARNING could not verify own slot {kernel_ref}: {type(exc).__name__}: {str(exc)[:180]}", flush=True)
            return
        raise RuntimeError(f"Region Talk BGE launch refused: could not verify own kernel slot {kernel_ref}: {type(exc).__name__}: {str(exc)[:180]}") from exc
    if raw in ACTIVE_KERNEL_STATUSES:
        raise RuntimeError(f"Region Talk BGE launch refused: active own kernel detected {kernel_ref}={raw}")
    print(f"[region-talk-bge] active own slot check OK: {kernel_ref}={raw or 'UNKNOWN'}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run Region Talk BGE-M3 enrichment on Kaggle")
    ap.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--kernel-slug", default="region-talk-bge-m3-enrichment")
    ap.add_argument("--batch-limit", type=int, default=12)
    ap.add_argument("--batch-size", type=int, default=4)
    ap.add_argument("--max-length", type=int, default=2048)
    ap.add_argument("--max-runtime-seconds", type=int, default=25 * 60)
    ap.add_argument("--timeout-minutes", type=int, default=45)
    ap.add_argument("--poll-interval-seconds", type=int, default=30)
    ap.add_argument("--input-kinds", default="text_vector_enrichment_item,publication_candidate_item,candidate_memory_item,image_queue_item,processed_post_item,post_live_item")
    ap.add_argument("--store-dense-vectors", action="store_true", default=True)
    ap.add_argument("--no-store-dense-vectors", action="store_false", dest="store_dense_vectors")
    ap.add_argument("--store-vector-max-rows", type=int, default=100)
    ap.add_argument("--reprocess-existing", action="store_true")
    ap.add_argument("--keep-input-datasets", action="store_true")
    ap.add_argument("--allow-active-bge-kernel", action="store_true")
    ap.add_argument("--no-wait", action="store_true")
    args = ap.parse_args()
    load_env_file(args.env_file)
    run_id = args.run_id or "region-talk-bge-m3-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ["REGION_TALK_RUN_ID"] = run_id
    os.environ.setdefault("REGION_TALK_STATE_BACKEND", "ydb")
    os.environ.setdefault("REGION_TALK_REQUIRE_YDB_STATE", "1")
    os.environ.setdefault("REGION_TALK_AUTH_BUNDLE_ENV", "REGION_TALK_NO_TELEGRAM_BUNDLE")
    os.environ.setdefault("REGION_TALK_DRY_RUN", "1")
    os.environ.setdefault("REGION_TALK_DISABLE_PUBLISH", "1")
    preflight_ydb_access()
    client = KaggleClient() if KaggleClient is not None else DirectKaggleClient()
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME is required")
    kref = kernel_ref(username=username, kernel_slug=args.kernel_slug)
    assert_bge_kernel_slot_free(client, kref, allow_active=bool(args.allow_active_bge_kernel))
    config_ref = build_config_dataset(client, username=username, run_id=run_id, args=args)
    secret_ref = create_secret_bundle_dataset(client, username=username, run_id=run_id)
    wait_dataset_ready(client, config_ref, expected_files=["region_talk_run_config.json", "kaliningrad-place-lexicon-v1.csv"])
    wait_dataset_ready(client, secret_ref, expected_files=["region_talk_secrets.enc", "region_talk_fernet.key"])
    kernel_path = prepared_kernel_path(run_id=run_id, kernel_slug=args.kernel_slug)
    dataset_sources = [config_ref, secret_ref]
    print(f"[region-talk-bge] pushing {kref} run_id={run_id} batch_limit={args.batch_limit} batch_size={args.batch_size} datasets={len(dataset_sources)} no_telegram=1", flush=True)
    client.push_kernel(kernel_path=kernel_path, dataset_sources=dataset_sources)
    if args.no_wait:
        print(f"[region-talk-bge] pushed {kref}; not waiting; input datasets retained: {dataset_sources}", flush=True)
        return 0
    completed = False
    try:
        poll_kernel(client, kref, timeout_minutes=args.timeout_minutes, poll_interval_seconds=args.poll_interval_seconds)
        completed = True
        out_dir = OUT_ROOT / run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        pattern = os.environ.get("REGION_TALK_BGE_KAGGLE_OUTPUT_FILE_PATTERN", r"(^|/)(output\.json|stage_status\.json|region_talk_bge_events\.jsonl|bge_m3_enrichment_result\.json|bge_m3_enrichment_rows\.jsonl|[^/]+\.(json|jsonl|log))$")
        files = client.download_kernel_output(kref, path=out_dir, force=True, file_pattern=pattern)
        print(f"[region-talk-bge] downloaded {len(files)} files to {out_dir}", flush=True)
    finally:
        if not args.keep_input_datasets and completed:
            for ref in dataset_sources:
                try:
                    client.delete_dataset(ref)
                    print(f"[region-talk-bge] cleaned input dataset {ref}", flush=True)
                except Exception as exc:
                    print(f"[region-talk-bge] WARNING cleanup failed {ref}: {type(exc).__name__}: {exc}", flush=True)
        elif not completed:
            print(f"[region-talk-bge] keeping input datasets because run did not complete locally: {dataset_sources}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
