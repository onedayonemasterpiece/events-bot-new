#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
import uuid
import zlib
from datetime import datetime, timezone
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

    def dataset_status(self, dataset: str) -> str:
        return str(self.api.dataset_status(dataset))

    def dataset_list_files(self, dataset: str) -> list[str]:
        response = self.api.dataset_list_files(dataset, page_size=100)
        files = getattr(response, "files", response)
        return [str(getattr(item, "name", item)) for item in (files or [])]

    def push_kernel(self, *, kernel_path, dataset_sources=None) -> None:
        meta_path = Path(kernel_path) / "kernel-metadata.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if dataset_sources is not None:
            meta["dataset_sources"] = [str(x) for x in dataset_sources if str(x).strip()]
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            response = self.api.kernels_push(str(kernel_path))
        except Exception as exc:
            response = getattr(exc, "response", None)
            body = ""
            if response is not None:
                try:
                    body = str(response.text or "")[:1000]
                except Exception:
                    body = ""
            raise RuntimeError("Kaggle kernels_push failed" + (f": {body}" if body else "")) from exc
        response_dict = response.to_dict() if hasattr(response, "to_dict") else {}
        error = str(response_dict.get("error") or getattr(response, "error", "") or "").strip()
        if error:
            raise RuntimeError(f"Kaggle kernels_push failed: {error}")
        invalid_dataset_sources = [
            str(item).strip()
            for item in (
                response_dict.get("invalidDatasetSources")
                or response_dict.get("invalid_dataset_sources")
                or getattr(response, "invalidDatasetSources", None)
                or getattr(response, "invalid_dataset_sources", None)
                or []
            )
            if str(item).strip()
        ]
        invalid_model_sources = [
            str(item).strip()
            for item in (
                response_dict.get("invalidModelSources")
                or response_dict.get("invalid_model_sources")
                or getattr(response, "invalidModelSources", None)
                or getattr(response, "invalid_model_sources", None)
                or []
            )
            if str(item).strip()
        ]
        if invalid_dataset_sources:
            raise RuntimeError("Kaggle kernels_push rejected dataset sources: " + ", ".join(invalid_dataset_sources))
        if invalid_model_sources:
            raise RuntimeError("Kaggle kernels_push rejected model sources: " + ", ".join(invalid_model_sources))

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

    def download_kernel_output(self, kernel_ref: str, *, path, force=True, file_pattern: str | None = None) -> list[str]:
        files, _ = self.api.kernels_output(kernel_ref, path=str(path), force=force, quiet=False, file_pattern=file_pattern)
        return [str(x) for x in files]

KERNEL_PATH = PROJECT_ROOT / "kaggle" / "RegionTalkCandidateReport"
ARTIFACT_ROOT = PROJECT_ROOT / "artifacts" / "codex" / "kaggle" / "region-talk-candidate-report"
DEFAULT_E5_KAGGLE_MODEL_SOURCE = "ranaabdulrehman145/intfloatmultilingual-e5-base/Transformers/default/1"
KAGGLE_KERNEL_SOURCE_MAX_BYTES = 1_000_000
KAGGLE_KERNEL_SOURCE_SAFETY_BYTES = 950_000


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


REGION_TALK_TEMP_DATASET_PREFIXES = (
    "region-talk-config-",
    "rt-secret-bundle-",
    "rt-bge-config-",
    "rt-bge-secret-",
    "rt-img-diag-",
)


def cleanup_stale_region_talk_input_datasets(
    client: Any,
    *,
    protected_refs: set[str] | None = None,
    now: datetime | None = None,
) -> dict[str, int]:
    """Bounded emergency GC for leaked no-wait Kaggle input datasets.

    Normal launches retain private inputs while a remote kernel is running. If
    dataset creation fails twice, only inputs older than the safety TTL are
    eligible, so datasets of active (<6h by default) Region Talk kernels remain
    protected.
    """
    protected = {str(ref) for ref in (protected_refs or set())}
    ttl_seconds = max(3600, int(os.getenv("REGION_TALK_KAGGLE_INPUT_DATASET_TTL_SECONDS") or "21600"))
    max_delete = max(1, int(os.getenv("REGION_TALK_KAGGLE_INPUT_GC_MAX_DELETE") or "75"))
    api = getattr(client, "api", None)
    if api is None or not hasattr(api, "dataset_list"):
        return {"listed": 0, "eligible": 0, "deleted": 0, "failed": 0}
    now = now or datetime.now(timezone.utc)
    candidates: dict[str, datetime] = {}
    for page in range(1, 51):
        rows = api.dataset_list(mine=True, search="region-talk", page=page) or []
        if not rows:
            break
        for item in rows:
            data = item.to_dict() if hasattr(item, "to_dict") else {}
            ref = str(data.get("ref") or getattr(item, "ref", "") or "")
            slug = ref.split("/", 1)[-1]
            if not ref or not slug.startswith(REGION_TALK_TEMP_DATASET_PREFIXES):
                continue
            updated = str(data.get("lastUpdated") or getattr(item, "last_updated", "") or "")
            try:
                parsed = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            candidates[ref] = parsed.astimezone(timezone.utc)
    eligible = [
        ref for ref, updated in sorted(candidates.items(), key=lambda pair: pair[1])
        if ref not in protected and (now - updated).total_seconds() >= ttl_seconds
    ][:max_delete]
    deleted = failed = 0
    for ref in eligible:
        try:
            client.delete_dataset(ref)
            deleted += 1
        except Exception:
            failed += 1
    print(
        f"[region-talk-kaggle] stale input dataset GC listed={len(candidates)} "
        f"eligible={len(eligible)} deleted={deleted} failed={failed}",
        flush=True,
    )
    return {"listed": len(candidates), "eligible": len(eligible), "deleted": deleted, "failed": failed}


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
            try:
                client.create_dataset(folder, public=False, quiet=True, convert_to_csv=False, dir_mode="zip")
            except Exception:
                # Two equivalent CreateDataset failures indicate account-level
                # temporary-input accumulation, not a payload contract guess.
                # Run bounded TTL GC before the final documented create retry.
                cleanup_stale_region_talk_input_datasets(client, protected_refs={dataset_ref})
                client.create_dataset(folder, public=False, quiet=True, convert_to_csv=False, dir_mode="zip")
    return dataset_ref


def wait_dataset_ready(client: Any, dataset_ref: str, *, expected_files: list[str], timeout_seconds: int = 240) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_status = ""
    last_files: list[str] = []
    while time.monotonic() < deadline:
        try:
            last_status = str(client.dataset_status(dataset_ref))
            raw_files = client.dataset_list_files(dataset_ref)
            last_files = [
                str(x.get("name") or x) if isinstance(x, dict)
                else str(getattr(x, "name", x))
                for x in raw_files
            ]
            if last_status.lower() == "ready" and all(name in last_files for name in expected_files):
                return
        except Exception:
            pass
        time.sleep(5)
    raise TimeoutError(f"dataset not ready: {dataset_ref} status={last_status} files={last_files}")


def run_dataset_slug(run_id: str) -> str:
    """Stable Kaggle dataset slug with a hash suffix to avoid timestamp truncation collisions."""
    digest = hashlib.sha1(str(run_id).encode("utf-8")).hexdigest()[:8]
    base = slugify(run_id, max_len=21).strip("-") or "region-talk-run"
    return f"{base}-{digest}"[:31].strip("-")


def region_talk_secret_names(auth_bundle_env: str | None = None) -> list[str]:
    """Return the minimal secret allow-list needed for this Region Talk run.

    Telegram auth bundles are role-scoped. S22 is reserved for production
    monitoring, so Region Talk candidate/image runs must not ship it unless the
    caller explicitly selects it as the active REGION_TALK_AUTH_BUNDLE_ENV.
    """
    selected_auth_bundle = (auth_bundle_env or os.environ.get("REGION_TALK_AUTH_BUNDLE_ENV") or "TELEGRAM_AUTH_BUNDLE_DISCOVERY1").strip()
    names = [
        "TG_API_ID", "TG_API_HASH", "TELEGRAM_API_ID", "TELEGRAM_API_HASH",
        "GOOGLE_AI_LIMITER_SUPABASE_URL", "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY",
        "SUPABASE_URL", "SUPABASE_SERVICE_KEY", "SUPABASE_KEY", "SUPABASE_SCHEMA",
        "GOOGLE_API_KEY", "GOOGLE_API_KEY3", "GOOGLE_API_KEY_3",
        "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON", "REGION_TALK_YDB_IAM_TOKEN", "YC_IAM_TOKEN", "YDB_ACCESS_TOKEN",
        "VK_ACCESS_TOKEN", "VK_SERVICE_TOKEN", "VK_SERVICE_KEY", "VK_TOKEN",
    ]
    if selected_auth_bundle:
        names.append(selected_auth_bundle)
    return list(dict.fromkeys(names))


def getenv_bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def preflight_ydb_access() -> None:
    if (os.environ.get("REGION_TALK_STATE_BACKEND") or "").strip().lower() != "ydb":
        return
    endpoint = (os.environ.get("REGION_TALK_YDB_ENDPOINT") or "").strip().split("?")[0].rstrip("/")
    database = (os.environ.get("REGION_TALK_YDB_DATABASE") or "").strip()
    if not endpoint or not database:
        raise RuntimeError(
            "Region Talk YDB preflight failed: REGION_TALK_STATE_BACKEND=ydb "
            "requires REGION_TALK_YDB_ENDPOINT/REGION_TALK_YDB_DATABASE before "
            "pushing a Kaggle run; refusing json_fallback live run"
        )
    if getenv_bool("REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL", False) and not (os.environ.get("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip():
        if getenv_bool("REGION_TALK_ALLOW_KAGGLE_YDB_SECRET", False):
            print("[region-talk-kaggle] YDB local preflight skipped: explicit REGION_TALK_ALLOW_KAGGLE_YDB_SECRET=1; Kaggle notebook must load REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON from UserSecretsClient", flush=True)
            return
        raise RuntimeError("Region Talk YDB preflight failed: REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL=1 but REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON is not set")
    try:
        import ydb  # type: ignore
        token = (os.environ.get("REGION_TALK_YDB_IAM_TOKEN") or os.environ.get("YC_IAM_TOKEN") or os.environ.get("YDB_ACCESS_TOKEN") or "").strip()
        key_json = (os.environ.get("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip()
        if key_json:
            import tempfile
            import ydb.iam  # type: ignore
            fd, path = tempfile.mkstemp(prefix="region-talk-ydb-preflight-", suffix=".json")
            os.close(fd)
            try:
                Path(path).write_text(key_json, encoding="utf-8")
                credentials = ydb.iam.ServiceAccountCredentials.from_file(path)
            finally:
                try:
                    os.unlink(path)
                except OSError:
                    pass
        elif token:
            credentials = ydb.AccessTokenCredentials(token)
        else:
            raise RuntimeError("no REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON or REGION_TALK_YDB_IAM_TOKEN/YC_IAM_TOKEN/YDB_ACCESS_TOKEN")
        driver = ydb.Driver(endpoint=endpoint, database=database, credentials=credentials)
        try:
            driver.wait(timeout=int(os.environ.get("REGION_TALK_YDB_PREFLIGHT_TIMEOUT_SECONDS") or "20"), fail_fast=True)
        finally:
            driver.stop(timeout=5)
    except Exception as exc:
        raise RuntimeError(f"Region Talk YDB preflight failed: {type(exc).__name__}: {str(exc)[:240]}") from exc
    credential_kind = "service_account_key" if (os.environ.get("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip() else "access_token"
    print(f"[region-talk-kaggle] YDB preflight OK credential={credential_kind}", flush=True)


def build_input_datasets(client: Any, *, run_id: str, username: str) -> list[str]:
    from cryptography.fernet import Fernet
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
        # This launcher is the live CandidateReport entry point.  Defaulting to
        # a local JSON sandbox can produce a green Kaggle run that reads and
        # writes none of the shared funnel, so JSON must now be an explicit
        # opt-in for offline experiments.
        "REGION_TALK_STATE_BACKEND": os.environ.get("REGION_TALK_STATE_BACKEND", "ydb"),
        "REGION_TALK_EXTERNAL_PUBLICATIONS_ONLY": os.environ.get("REGION_TALK_EXTERNAL_PUBLICATIONS_ONLY", "0"),
        "REGION_TALK_VECTOR_PROBE_ONLY": os.environ.get("REGION_TALK_VECTOR_PROBE_ONLY", "0"),
        "REGION_TALK_VECTOR_PROBE_TEXT_LIMIT": os.environ.get("REGION_TALK_VECTOR_PROBE_TEXT_LIMIT", "6"),
        "REGION_TALK_REQUIRE_YDB_STATE": os.environ.get(
            "REGION_TALK_REQUIRE_YDB_STATE",
            "1" if os.environ.get("REGION_TALK_STATE_BACKEND", "ydb").strip().lower() == "ydb" else "0",
        ),
        "REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL": os.environ.get("REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL", "0"),
        "REGION_TALK_YDB_PREFLIGHT_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_YDB_PREFLIGHT_TIMEOUT_SECONDS", "20"),
        "REGION_TALK_ALLOW_KAGGLE_YDB_SECRET": os.environ.get("REGION_TALK_ALLOW_KAGGLE_YDB_SECRET", "0"),
        "REGION_TALK_KAGGLE_SECRET_NAMES": os.environ.get("REGION_TALK_KAGGLE_SECRET_NAMES", ""),
        "REGION_TALK_YDB_ENDPOINT": os.environ.get("REGION_TALK_YDB_ENDPOINT", ""),
        "REGION_TALK_YDB_DATABASE": os.environ.get("REGION_TALK_YDB_DATABASE", ""),
        "REGION_TALK_YDB_NAMESPACE": os.environ.get("REGION_TALK_YDB_NAMESPACE", "region_talk_compact"),
        "REGION_TALK_YDB_STATE_SNAPSHOT_FILE": os.environ.get("REGION_TALK_YDB_STATE_SNAPSHOT_FILE", ""),
        "REGION_TALK_YDB_MAX_POST_ROWS": os.environ.get("REGION_TALK_YDB_MAX_POST_ROWS", "20000"),
        "REGION_TALK_YDB_MAX_SOURCE_ROWS": os.environ.get("REGION_TALK_YDB_MAX_SOURCE_ROWS", "5000"),
        "REGION_TALK_YDB_SOURCE_QUEUE_FULL_READ_LIMIT": os.environ.get("REGION_TALK_YDB_SOURCE_QUEUE_FULL_READ_LIMIT", "20000"),
        "REGION_TALK_YDB_MAX_CANDIDATE_ROWS": os.environ.get("REGION_TALK_YDB_MAX_CANDIDATE_ROWS", "5000"),
        "REGION_TALK_YDB_PRUNE_LEGACY_QUEUE_PAYLOADS": os.environ.get("REGION_TALK_YDB_PRUNE_LEGACY_QUEUE_PAYLOADS", "1"),
        "REGION_TALK_YDB_PRUNE_MAX_ROWS": os.environ.get("REGION_TALK_YDB_PRUNE_MAX_ROWS", "200"),
        "REGION_TALK_YDB_SKIP_ROW_LEVEL_REWRITE": os.environ.get("REGION_TALK_YDB_SKIP_ROW_LEVEL_REWRITE", "1"),
        "REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS", "8"),
        "REGION_TALK_YDB_HEARTBEAT_REQUEST_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_YDB_HEARTBEAT_REQUEST_TIMEOUT_SECONDS", "5"),
        "REGION_TALK_YDB_HEARTBEAT_MAX_RETRIES": os.environ.get("REGION_TALK_YDB_HEARTBEAT_MAX_RETRIES", "0"),
        "REGION_TALK_YDB_QUEUE_REQUEST_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_YDB_QUEUE_REQUEST_TIMEOUT_SECONDS", "8"),
        "REGION_TALK_YDB_QUEUE_MAX_RETRIES": os.environ.get("REGION_TALK_YDB_QUEUE_MAX_RETRIES", "1"),
        "REGION_TALK_YDB_STATE_LOAD_ATTEMPTS": os.environ.get("REGION_TALK_YDB_STATE_LOAD_ATTEMPTS", "1"),
        "REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS": os.environ.get("REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS", "0"),
        "REGION_TALK_YDB_STATE_LOAD_REQUEST_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_YDB_STATE_LOAD_REQUEST_TIMEOUT_SECONDS", os.environ.get("REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS", "8")),
        "REGION_TALK_YDB_STATE_LOAD_MAX_RETRIES": os.environ.get("REGION_TALK_YDB_STATE_LOAD_MAX_RETRIES", os.environ.get("REGION_TALK_YDB_MAX_RETRIES", "2")),
        "REGION_TALK_YDB_ROW_UPSERT_CHUNK_SIZE": os.environ.get("REGION_TALK_YDB_ROW_UPSERT_CHUNK_SIZE", "100"),
        "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_ROWS": os.environ.get("REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_ROWS", "300"),
        "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES": os.environ.get("REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES", "300"),
        "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_EDGES": os.environ.get("REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_EDGES", "300"),
        "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_COMMENT_LINKS": os.environ.get("REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_COMMENT_LINKS", "100"),
        "REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY": os.environ.get("REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY", "1"),
        "REGION_TALK_YDB_ONLINE_CANDIDATE_WRITE_MAX_ROWS": os.environ.get("REGION_TALK_YDB_ONLINE_CANDIDATE_WRITE_MAX_ROWS", "0"),
        "REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR": os.environ.get("REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR", "1"),
        "REGION_TALK_ENABLE_STACK_WATCHDOG": os.environ.get("REGION_TALK_ENABLE_STACK_WATCHDOG", "1"),
        "REGION_TALK_STACK_WATCHDOG_SECONDS": os.environ.get("REGION_TALK_STACK_WATCHDOG_SECONDS", "300"),
        "REGION_TALK_STACK_WATCHDOG_REPEAT": os.environ.get("REGION_TALK_STACK_WATCHDOG_REPEAT", "0"),
        "REGION_TALK_SOURCE_QUEUE_RECLASSIFY_FULL": os.environ.get("REGION_TALK_SOURCE_QUEUE_RECLASSIFY_FULL", "0"),
        "REGION_TALK_SOURCE_QUEUE_PROGRESS_EVERY_ROWS": os.environ.get("REGION_TALK_SOURCE_QUEUE_PROGRESS_EVERY_ROWS", "500"),
        "REGION_TALK_HF_HUB_DISABLE_PROGRESS_BARS": os.environ.get("REGION_TALK_HF_HUB_DISABLE_PROGRESS_BARS", "1"),
        "REGION_TALK_TQDM_DISABLE": os.environ.get("REGION_TALK_TQDM_DISABLE", "1"),
        "REGION_TALK_TRANSFORMERS_VERBOSITY": os.environ.get("REGION_TALK_TRANSFORMERS_VERBOSITY", "error"),
        "REGION_TALK_LIVE_EVENT_LOG_PATH": os.environ.get("REGION_TALK_LIVE_EVENT_LOG_PATH", "/kaggle/working/region_talk_run_events_live.jsonl"),
        "REGION_TALK_STDOUT_HEARTBEATS": os.environ.get("REGION_TALK_STDOUT_HEARTBEATS", "0"),
        "REGION_TALK_STDOUT_ALL_EVENTS": os.environ.get("REGION_TALK_STDOUT_ALL_EVENTS", "0"),
        "REGION_TALK_KAGGLE_STATUS_STDOUT": os.environ.get("REGION_TALK_KAGGLE_STATUS_STDOUT", "0"),
        "REGION_TALK_DELTA_SCAN_ENABLED": os.environ.get("REGION_TALK_DELTA_SCAN_ENABLED", "1"),
        "REGION_TALK_DELTA_SCAN_WINDOW_DAYS": os.environ.get("REGION_TALK_DELTA_SCAN_WINDOW_DAYS", "14"),
        "REGION_TALK_DELTA_OVERLAP_POSTS": os.environ.get("REGION_TALK_DELTA_OVERLAP_POSTS", "50"),
        "REGION_TALK_PRIORITIZE_TRAVEL_SOURCES": os.environ.get("REGION_TALK_PRIORITIZE_TRAVEL_SOURCES", "1"),
        "REGION_TALK_SOURCE_SELECTION_YDB_QUEUE_ONLY": os.environ.get("REGION_TALK_SOURCE_SELECTION_YDB_QUEUE_ONLY", "0"),
        "REGION_TALK_SOURCE_QUEUE_SELECTION_POOL": os.environ.get("REGION_TALK_SOURCE_QUEUE_SELECTION_POOL", ""),
        "REGION_TALK_SOURCE_QUEUE_SELECTION_POOL_MULTIPLIER": os.environ.get("REGION_TALK_SOURCE_QUEUE_SELECTION_POOL_MULTIPLIER", "10"),
        "REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES": os.environ.get("REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES", "0"),
        "REGION_TALK_MAX_SOURCES": os.environ.get("REGION_TALK_MAX_SOURCES", "220"),
        "REGION_TALK_MAX_POSTS_PER_SOURCE": os.environ.get("REGION_TALK_MAX_POSTS_PER_SOURCE", "50"),
        "REGION_TALK_MAX_POSTS_TO_SCORE_PER_RUN": os.environ.get("REGION_TALK_MAX_POSTS_TO_SCORE_PER_RUN", "180"),
        "REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS": os.environ.get("REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS", "1080"),
        "REGION_TALK_RUNTIME_RESERVE_BEFORE_REPORT_SECONDS": os.environ.get("REGION_TALK_RUNTIME_RESERVE_BEFORE_REPORT_SECONDS", "180"),
        "REGION_TALK_RUNTIME_RESERVE_DURING_SCORING_SECONDS": os.environ.get("REGION_TALK_RUNTIME_RESERVE_DURING_SCORING_SECONDS", "30"),
        "REGION_TALK_RUNTIME_RESERVE_BEFORE_LLM_SECONDS": os.environ.get("REGION_TALK_RUNTIME_RESERVE_BEFORE_LLM_SECONDS", "90"),
        "REGION_TALK_RUNTIME_LOW_BUDGET_MAX_POSTS_TO_SCORE": os.environ.get("REGION_TALK_RUNTIME_LOW_BUDGET_MAX_POSTS_TO_SCORE", "40"),
        "REGION_TALK_VECTOR_HEARTBEAT_EVERY_POSTS": os.environ.get("REGION_TALK_VECTOR_HEARTBEAT_EVERY_POSTS", "25"),
        "REGION_TALK_PRIORITIZE_TEXT_VECTORS": os.environ.get("REGION_TALK_PRIORITIZE_TEXT_VECTORS", "1"),
        "REGION_TALK_TEXT_EMBEDDING_PRIORITY_MIN_MODEL_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_TEXT_EMBEDDING_PRIORITY_MIN_MODEL_TIMEOUT_SECONDS", "420"),
        "REGION_TALK_RUNTIME_RESERVE_DURING_TEXT_EMBEDDING_SECONDS": os.environ.get("REGION_TALK_RUNTIME_RESERVE_DURING_TEXT_EMBEDDING_SECONDS", "120"),
        "REGION_TALK_TEXT_EMBEDDING_SUBPROCESS": os.environ.get("REGION_TALK_TEXT_EMBEDDING_SUBPROCESS", "1"),
        "REGION_TALK_TEXT_EMBEDDING_MODEL_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_TEXT_EMBEDDING_MODEL_TIMEOUT_SECONDS", "420"),
        "REGION_TALK_HF_HUB_DOWNLOAD_TIMEOUT": os.environ.get("REGION_TALK_HF_HUB_DOWNLOAD_TIMEOUT", "60"),
        "REGION_TALK_HF_HUB_ETAG_TIMEOUT": os.environ.get("REGION_TALK_HF_HUB_ETAG_TIMEOUT", "20"),
        "REGION_TALK_HF_HUB_DISABLE_XET": os.environ.get("REGION_TALK_HF_HUB_DISABLE_XET", "1"),
        "REGION_TALK_E5_KAGGLE_MODEL_SOURCE": os.environ.get("REGION_TALK_E5_KAGGLE_MODEL_SOURCE", DEFAULT_E5_KAGGLE_MODEL_SOURCE),
        "REGION_TALK_E5_USE_KAGGLEHUB_FALLBACK": os.environ.get("REGION_TALK_E5_USE_KAGGLEHUB_FALLBACK", "1"),
        "REGION_TALK_FETCH_TELEGRAM": os.environ.get("REGION_TALK_FETCH_TELEGRAM", "1"),
        "REGION_TALK_POST_INPUT_MODE": os.environ.get("REGION_TALK_POST_INPUT_MODE", os.environ.get("REGION_TALK_FETCH_MODE", "")),
        "REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST": os.environ.get("REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST", "1"),
        "REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT": os.environ.get("REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT", "12"),
        "REGION_TALK_TEXT_RESTORE_EXACT_PER_RUN": os.environ.get("REGION_TALK_TEXT_RESTORE_EXACT_PER_RUN", "5"),
        "REGION_TALK_POST_LINK_QUEUE_SCAN_LIMIT": os.environ.get("REGION_TALK_POST_LINK_QUEUE_SCAN_LIMIT", "5000"),
        "REGION_TALK_YDB_CANDIDATE_LINK_LIMIT": os.environ.get("REGION_TALK_YDB_CANDIDATE_LINK_LIMIT", "180"),
        "REGION_TALK_MAX_IMAGES_PER_POST": os.environ.get("REGION_TALK_MAX_IMAGES_PER_POST", "8"),
        "REGION_TALK_MAX_VLM_CALLS": os.environ.get("REGION_TALK_MAX_VLM_CALLS", "0"),
        "REGION_TALK_TG_GOVERNOR_ENABLED": os.environ.get("REGION_TALK_TG_GOVERNOR_ENABLED", "1"),
        "REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN", "2000"),
        "REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN", "30"),
        "REGION_TALK_TG_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN": os.environ.get("REGION_TALK_TG_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN", "1"),
        "REGION_TALK_TG_CACHED_ENTITY_ONLY": os.environ.get("REGION_TALK_TG_CACHED_ENTITY_ONLY", "0"),
        "REGION_TALK_SOURCE_QUEUE_UNCACHED_RESOLVE_LANE_PER_RUN": os.environ.get("REGION_TALK_SOURCE_QUEUE_UNCACHED_RESOLVE_LANE_PER_RUN", "1"),
        "REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN", "150"),
        "REGION_TALK_HISTORY_SOURCES_TARGET": os.environ.get("REGION_TALK_HISTORY_SOURCES_TARGET", "150"),
        "REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE": os.environ.get("REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE", "50"),
        "REGION_TALK_HISTORY_MAX_POST_AGE_DAYS": os.environ.get("REGION_TALK_HISTORY_MAX_POST_AGE_DAYS", "365"),
        "REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD": os.environ.get("REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD", "30"),
        "REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN", "200"),
        "REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN", "250"),
        "REGION_TALK_TG_PUBLIC_WEB_FETCH_FIRST": os.environ.get("REGION_TALK_TG_PUBLIC_WEB_FETCH_FIRST", "0"),
        "REGION_TALK_TG_PUBLIC_WEB_FALLBACK": os.environ.get("REGION_TALK_TG_PUBLIC_WEB_FALLBACK", "0"),
        "REGION_TALK_TG_PUBLIC_WEB_MAX_PAGES_PER_SOURCE": os.environ.get("REGION_TALK_TG_PUBLIC_WEB_MAX_PAGES_PER_SOURCE", "10"),
        "REGION_TALK_TG_PUBLIC_WEB_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_TG_PUBLIC_WEB_TIMEOUT_SECONDS", "20"),
        "REGION_TALK_PUBLICATION_ELIGIBILITY_GATE_VERSION": os.environ.get("REGION_TALK_PUBLICATION_ELIGIBILITY_GATE_VERSION", "region_talk_publication_eligibility_v5"),
        "REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN": os.environ.get("REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN", "200"),
        "REGION_TALK_MAX_NEW_SOURCE_PROBES": os.environ.get("REGION_TALK_MAX_NEW_SOURCE_PROBES", "100"),
        "REGION_TALK_DISCOVERY_MODE": os.environ.get("REGION_TALK_DISCOVERY_MODE", "mixed"),
        "REGION_TALK_HISTORY_SCAN_MODE": os.environ.get("REGION_TALK_HISTORY_SCAN_MODE", "primary_and_delta"),
        "REGION_TALK_TG_FLOODWAIT_MAX_SLEEP_SECONDS": os.environ.get("REGION_TALK_TG_FLOODWAIT_MAX_SLEEP_SECONDS", "60"),
        "REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS": os.environ.get("REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS", "300"),
        "REGION_TALK_TG_FLOODWAIT_COOLDOWN_MARGIN_SECONDS": os.environ.get("REGION_TALK_TG_FLOODWAIT_COOLDOWN_MARGIN_SECONDS", "1800"),
        "REGION_TALK_TG_HUMANLIKE_PACING_ENABLED": os.environ.get("REGION_TALK_TG_HUMANLIKE_PACING_ENABLED", "1"),
        "REGION_TALK_TG_RESOLVE_DELAY_MIN_SECONDS": os.environ.get("REGION_TALK_TG_RESOLVE_DELAY_MIN_SECONDS", "20"),
        "REGION_TALK_TG_RESOLVE_DELAY_MAX_SECONDS": os.environ.get("REGION_TALK_TG_RESOLVE_DELAY_MAX_SECONDS", "45"),
        "REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MIN_SECONDS": os.environ.get("REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MIN_SECONDS", "8"),
        "REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MAX_SECONDS": os.environ.get("REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MAX_SECONDS", "18"),
        "REGION_TALK_TG_SIMILAR_DELAY_MIN_SECONDS": os.environ.get("REGION_TALK_TG_SIMILAR_DELAY_MIN_SECONDS", "20"),
        "REGION_TALK_TG_SIMILAR_DELAY_MAX_SECONDS": os.environ.get("REGION_TALK_TG_SIMILAR_DELAY_MAX_SECONDS", "45"),
        "REGION_TALK_TG_HISTORY_QUERY_DELAY_MIN_SECONDS": os.environ.get("REGION_TALK_TG_HISTORY_QUERY_DELAY_MIN_SECONDS", "2"),
        "REGION_TALK_TG_HISTORY_QUERY_DELAY_MAX_SECONDS": os.environ.get("REGION_TALK_TG_HISTORY_QUERY_DELAY_MAX_SECONDS", "6"),
        "REGION_TALK_TG_MEDIA_DELAY_MIN_SECONDS": os.environ.get("REGION_TALK_TG_MEDIA_DELAY_MIN_SECONDS", "1"),
        "REGION_TALK_TG_MEDIA_DELAY_MAX_SECONDS": os.environ.get("REGION_TALK_TG_MEDIA_DELAY_MAX_SECONDS", "4"),
        "REGION_TALK_TG_SOURCE_PAUSE_MIN_SECONDS": os.environ.get("REGION_TALK_TG_SOURCE_PAUSE_MIN_SECONDS", "4"),
        "REGION_TALK_TG_SOURCE_PAUSE_MAX_SECONDS": os.environ.get("REGION_TALK_TG_SOURCE_PAUSE_MAX_SECONDS", "12"),
        "REGION_TALK_TG_SIMILAR_ENABLED": os.environ.get("REGION_TALK_TG_SIMILAR_ENABLED", os.environ.get("REGION_TALK_DISCOVERY_ENABLE_TELEGRAM_SIMILAR", "1")),
        "REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN": os.environ.get("REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN", os.environ.get("REGION_TALK_DISCOVERY_MAX_SIMILAR_SEED_CHANNELS", "200")),
        "REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED": os.environ.get("REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED", os.environ.get("REGION_TALK_DISCOVERY_MAX_SIMILAR_PER_SEED", "30")),
        "REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN": os.environ.get("REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN", os.environ.get("REGION_TALK_DISCOVERY_MAX_NEW_FRONTIER_PER_RUN", "2000")),
        "REGION_TALK_MAX_NEW_FRONTIER_PER_RUN": os.environ.get("REGION_TALK_MAX_NEW_FRONTIER_PER_RUN", "2000"),
        "REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY": os.environ.get("REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY", "1"),
        "REGION_TALK_TELEGRAM_QUERY_SOURCE": os.environ.get("REGION_TALK_TELEGRAM_QUERY_SOURCE", "manual"),
        "REGION_TALK_TELEGRAM_QUERY_ROTATE": os.environ.get("REGION_TALK_TELEGRAM_QUERY_ROTATE", "0"),
        "REGION_TALK_TELEGRAM_QUERY_ROTATE_OFFSET": os.environ.get("REGION_TALK_TELEGRAM_QUERY_ROTATE_OFFSET", ""),
        "REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES": os.environ.get("REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES", "30"),
        "REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES": os.environ.get("REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES", ""),
        "REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN": os.environ.get("REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN", ""),
        "REGION_TALK_TELEGRAM_KEYWORD_RESULTS_PER_QUERY": os.environ.get("REGION_TALK_TELEGRAM_KEYWORD_RESULTS_PER_QUERY", "10"),
        "REGION_TALK_MAX_KEYWORD_DISCOVERED_SOURCES_PER_RUN": os.environ.get("REGION_TALK_MAX_KEYWORD_DISCOVERED_SOURCES_PER_RUN", "300"),
        "REGION_TALK_FAST_CHECK_KO_ENABLED": os.environ.get("REGION_TALK_FAST_CHECK_KO_ENABLED", "1"),
        "REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN": os.environ.get("REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN", "0"),
        "REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE": os.environ.get("REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE", "2"),
        "REGION_TALK_FAST_CHECK_KO_RESULTS_PER_QUERY": os.environ.get("REGION_TALK_FAST_CHECK_KO_RESULTS_PER_QUERY", "2"),
        "REGION_TALK_FAST_CHECK_QUERY_STRATEGY": os.environ.get("REGION_TALK_FAST_CHECK_QUERY_STRATEGY", "legacy_v1"),
        "REGION_TALK_FAST_CHECK_ADAPTIVE_PREFER_CONTINUATIONS": os.environ.get("REGION_TALK_FAST_CHECK_ADAPTIVE_PREFER_CONTINUATIONS", "0"),
        "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_ENABLED": os.environ.get("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_ENABLED", "1"),
        "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_TABLE": os.environ.get("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_TABLE", "region_talk_external_blogger_evidence"),
        "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_MAX_ROWS": os.environ.get("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_MAX_ROWS", "2000"),
        "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_QUERIES_PER_SOURCE": os.environ.get("REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_QUERIES_PER_SOURCE", "8"),
        "REGION_TALK_FAST_CHECK_STAGE_MAX_SECONDS": os.environ.get("REGION_TALK_FAST_CHECK_STAGE_MAX_SECONDS", "180"),
        "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_RESULTS_PER_QUERY": os.environ.get("REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_RESULTS_PER_QUERY", "20"),
        "REGION_TALK_CONFIRMED_BLOGGER_HISTORY_SLOTS_PER_RUN": os.environ.get("REGION_TALK_CONFIRMED_BLOGGER_HISTORY_SLOTS_PER_RUN", "4"),
        "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_ENABLED": os.environ.get("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_ENABLED", "1"),
        "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_QUERIES_PER_SOURCE": os.environ.get("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_QUERIES_PER_SOURCE", "8"),
        "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_RESULTS_PER_QUERY": os.environ.get("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_RESULTS_PER_QUERY", "20"),
        "REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MIN_SECONDS": os.environ.get("REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MIN_SECONDS", "5"),
        "REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MAX_SECONDS": os.environ.get("REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MAX_SECONDS", "9"),
        "REGION_TALK_RUNTIME_RESERVE_BEFORE_FAST_CHECK_KO_SECONDS": os.environ.get("REGION_TALK_RUNTIME_RESERVE_BEFORE_FAST_CHECK_KO_SECONDS", "300"),
        "REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS": os.environ.get("REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS", "930"),
        "REGION_TALK_RUNTIME_RESERVE_BEFORE_KEYWORD_QUERY_SECONDS": os.environ.get("REGION_TALK_RUNTIME_RESERVE_BEFORE_KEYWORD_QUERY_SECONDS", os.environ.get("REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS", "930")),
        "REGION_TALK_IMAGE_SCORING_MODE": os.environ.get("REGION_TALK_IMAGE_SCORING_MODE", "external_ydb_queue"),
        "REGION_TALK_MEDIA_SCORING_MODE": os.environ.get("REGION_TALK_MEDIA_SCORING_MODE", "retry_queue_first"),
        "REGION_TALK_MEDIA_RETRY_FIRST": os.environ.get("REGION_TALK_MEDIA_RETRY_FIRST", "1"),
        "REGION_TALK_ACTUAL_IMAGE_TARGET": os.environ.get("REGION_TALK_ACTUAL_IMAGE_TARGET", "60"),
        "REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING": os.environ.get("REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING", "0"),
        "REGION_TALK_VK_READ_SERVICE_FIRST": os.environ.get("REGION_TALK_VK_READ_SERVICE_FIRST", "1"),
        "REGION_TALK_FETCH_VKVIDEO_WALL_FALLBACK": os.environ.get("REGION_TALK_FETCH_VKVIDEO_WALL_FALLBACK", "1"),
        "REGION_TALK_AUTH_BUNDLE_ENV": os.environ.get("REGION_TALK_AUTH_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_DISCOVERY1"),
        "REGION_TALK_SEMANTIC_GATE_MODE": os.environ.get("REGION_TALK_SEMANTIC_GATE_MODE", "vector_first_final_llm"),
        "REGION_TALK_LLM_MODEL": os.environ.get("REGION_TALK_LLM_MODEL", "gemini-3.5-flash-lite"),
        "REGION_TALK_LLM_FALLBACK_MODELS": os.environ.get("REGION_TALK_LLM_FALLBACK_MODELS", "gemini-3.1-flash-lite"),
        "REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME": os.environ.get("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME", "GOOGLE_API_KEY3"),
        "REGION_TALK_LLM_CALL_TIMEOUT_SECONDS": os.environ.get("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "60"),
        "GOOGLE_AI_PROVIDER_TIMEOUT_SEC": os.environ.get(
            "GOOGLE_AI_PROVIDER_TIMEOUT_SEC",
            os.environ.get("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "60"),
        ),
        "REGION_TALK_ENABLE_EARLY_LLM": os.environ.get("REGION_TALK_ENABLE_EARLY_LLM", "0"),
        "REGION_TALK_ENABLE_VECTOR_GATES": os.environ.get("REGION_TALK_ENABLE_VECTOR_GATES", "1"),
        "REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS": os.environ.get("REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS", "1"),
        # Production main notebook must keep only E5 in memory. BGE-M3 runs in
        # RegionTalkBgeM3Enrichment and is consumed later from YDB.
        "REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS": os.environ.get("REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS", "0"),
        "REGION_TALK_TEXT_EMBEDDING_MODEL_IDS": os.environ.get("REGION_TALK_TEXT_EMBEDDING_MODEL_IDS", "intfloat/multilingual-e5-base"),
        "REGION_TALK_EXTERNAL_BGE_M3_FUSION_ENABLED": os.environ.get("REGION_TALK_EXTERNAL_BGE_M3_FUSION_ENABLED", "1"),
        "REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE": os.environ.get("REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE", "1"),
        "REGION_TALK_ENABLE_FINAL_LLM_VERIFIER": os.environ.get("REGION_TALK_ENABLE_FINAL_LLM_VERIFIER", "1"),
        "REGION_TALK_TARGET_LLM_CALLS": os.environ.get("REGION_TALK_TARGET_LLM_CALLS", "10"),
        "REGION_TALK_MAX_LLM_FINAL_VERIFY": os.environ.get("REGION_TALK_MAX_LLM_FINAL_VERIFY", "10"),
        "REGION_TALK_MEMORY_VECTOR_RECHECK_MAX_ROWS": os.environ.get("REGION_TALK_MEMORY_VECTOR_RECHECK_MAX_ROWS", "0"),
        "REGION_TALK_MEMORY_VECTOR_RECHECK_BATCH_EMBEDDINGS": os.environ.get("REGION_TALK_MEMORY_VECTOR_RECHECK_BATCH_EMBEDDINGS", "0"),
        "REGION_TALK_SOURCE_PROFILE_BUILD_ENABLED": os.environ.get("REGION_TALK_SOURCE_PROFILE_BUILD_ENABLED", "0"),
        "REGION_TALK_SOURCE_PROFILE_HEARTBEAT_EVERY_ROWS": os.environ.get("REGION_TALK_SOURCE_PROFILE_HEARTBEAT_EVERY_ROWS", "25"),
        "REGION_TALK_LIVE_IMAGE_QUEUE_HANDOFF_EARLY": os.environ.get("REGION_TALK_LIVE_IMAGE_QUEUE_HANDOFF_EARLY", "1"),
        "REGION_TALK_LIGHTWEIGHT_REPORT": os.environ.get("REGION_TALK_LIGHTWEIGHT_REPORT", "0"),
        "REGION_TALK_WRITE_REPORT_ARTIFACTS": os.environ.get("REGION_TALK_WRITE_REPORT_ARTIFACTS", "1"),
        "REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF": os.environ.get("REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF", "0"),
        "REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF": os.environ.get("REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF", "1"),
        "REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS": os.environ.get("REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS", "500"),
        "REGION_TALK_SOURCE_QUEUE_HANDOFF_PERSIST_REORDERED_TAIL": os.environ.get("REGION_TALK_SOURCE_QUEUE_HANDOFF_PERSIST_REORDERED_TAIL", "0"),
        "REGION_TALK_YDB_ONLINE_QUEUE_WRITE_MAX_ROWS": os.environ.get("REGION_TALK_YDB_ONLINE_QUEUE_WRITE_MAX_ROWS", "0"),
        "REGION_TALK_WRITE_SOURCE_STATUS_QUEUE_MIRROR": os.environ.get("REGION_TALK_WRITE_SOURCE_STATUS_QUEUE_MIRROR", "0"),
        "GOOGLE_AI_ALLOW_RESERVE_FALLBACK": "0",
        "GOOGLE_AI_LOCAL_LIMITER_FALLBACK": "0",
        "GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR": "0",
        "REGION_TALK_SEED_FILE": os.environ.get("REGION_TALK_SEED_FILE", "seed-sources-v2.csv"),
        "REGION_TALK_PLACE_LEXICON_FILE": os.environ.get("REGION_TALK_PLACE_LEXICON_FILE", "kaliningrad-place-lexicon-v1.csv"),
        "REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE": os.environ.get("REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE", "public_travel_blogger_channel_links.xlsx"),
        "REGION_TALK_MIN_POST_DATE": os.environ.get("REGION_TALK_MIN_POST_DATE", ""),
        "REGION_TALK_FRESHNESS_HALF_LIFE_DAYS": os.environ.get("REGION_TALK_FRESHNESS_HALF_LIFE_DAYS", "30"),
        "REGION_TALK_OUTPUT_DIR": f"artifacts/region-talk/runs/{run_id}",
    }
    secret_names = region_talk_secret_names(env_config.get("REGION_TALK_AUTH_BUNDLE_ENV"))
    secrets = {name: os.environ.get(name) for name in secret_names if (os.environ.get(name) or "").strip()}
    key = Fernet.generate_key()
    encrypted = Fernet(key).encrypt(json.dumps(secrets, ensure_ascii=False).encode("utf-8"))

    def write_config(folder: Path) -> None:
        feature_dir = PROJECT_ROOT / "docs" / "features" / "region-talk-channel"
        shutil.copy2(feature_dir / "seed-sources-v1.csv", folder / "seed-sources-v1.csv")
        shutil.copy2(feature_dir / "seed-sources-v2.csv", folder / "seed-sources-v2.csv")
        shutil.copy2(feature_dir / "kaliningrad-place-lexicon-v1.csv", folder / "kaliningrad-place-lexicon-v1.csv")
        blogger_link_candidates = []
        if os.environ.get("REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE"):
            blogger_link_candidates.append(Path(str(os.environ["REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE"])))
        blogger_link_candidates.extend([
            PROJECT_ROOT / "artifacts" / "public_travel_blogger_channel_links.xlsx",
            Path("/home/dev/projects/events-bot-new/artifacts/public_travel_blogger_channel_links.xlsx"),
        ])
        for blogger_links in blogger_link_candidates:
            if blogger_links.exists():
                shutil.copy2(blogger_links, folder / "public_travel_blogger_channel_links.xlsx")
                break
        shutil.copytree(PROJECT_ROOT / "google_ai", folder / "google_ai", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        state_candidates = [
            Path(os.environ["REGION_TALK_STATE_FILE"]) if os.environ.get("REGION_TALK_STATE_FILE") else None,
        ]
        state_candidates.extend(sorted((PROJECT_ROOT / "artifacts" / "codex" / "kaggle" / "region-talk-candidate-report").glob("*/artifacts/region-talk/state/region-talk-state.json"), reverse=True))
        state_candidates.append(PROJECT_ROOT / "artifacts" / "region-talk" / "state" / "region-talk-state.json")
        for state_path in [p for p in state_candidates if p]:
            try:
                if state_path.exists():
                    shutil.copy2(state_path, folder / "region-talk-state.json")
                    break
            except Exception:
                pass
        (folder / "region_talk_run_config.json").write_text(json.dumps({"run_id": run_id, "env": env_config, "seed_file": env_config["REGION_TALK_SEED_FILE"], "place_lexicon_file": env_config["REGION_TALK_PLACE_LEXICON_FILE"]}, ensure_ascii=False, indent=2), encoding="utf-8")

    def write_secret_bundle(folder: Path) -> None:
        # Keep encrypted payload and Fernet key in the same Kaggle dataset version.
        # Separate datasets can be cached/mounted out of sync by slug/version and
        # produce InvalidToken before the notebook reaches business logic.
        (folder / "region_talk_secrets.enc").write_bytes(encrypted)
        (folder / "region_talk_fernet.key").write_bytes(key)

    config_ref = create_or_replace_dataset(client, username, f"region-talk-config-{safe_slug}", f"Region Talk config {safe_slug}", write_config)
    secret_ref = create_or_replace_dataset(client, username, f"rt-secret-bundle-{safe_slug}", f"Region Talk sec {safe_slug}", write_secret_bundle)
    wait_dataset_ready(client, config_ref, expected_files=["seed-sources-v1.csv", "seed-sources-v2.csv", "kaliningrad-place-lexicon-v1.csv", "region_talk_run_config.json", "google_ai/__init__.py"])
    wait_dataset_ready(client, secret_ref, expected_files=["region_talk_secrets.enc", "region_talk_fernet.key"])
    return [config_ref, secret_ref]


def cleanup_input_datasets(client: Any, dataset_refs: list[str]) -> None:
    for dataset_ref in dataset_refs:
        if not dataset_ref:
            continue
        try:
            client.delete_dataset(dataset_ref)
            print(f"[region-talk-kaggle] cleaned input dataset {dataset_ref}", flush=True)
        except Exception as exc:
            print(f"[region-talk-kaggle] WARNING failed to clean input dataset {dataset_ref}: {type(exc).__name__}: {exc}", flush=True)


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
    shutil.copytree(PROJECT_ROOT / "google_ai", dst / "google_ai", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    # Kaggle rejects a script source at roughly 1 MB. CandidateReport is a
    # deliberately self-contained notebook worker and can cross that boundary
    # even though the executable compresses well. Keep the uploaded kernel
    # source small without creating another mutable code dataset: the wrapper
    # expands the exact reviewed bytes in memory and executes them with the
    # original filename for tracebacks.
    source_path = dst / "region_talk_candidate_report.py"
    source_bytes = source_path.read_bytes()
    packed = base64.b85encode(zlib.compress(source_bytes, level=9))
    wrapper = (
        "# generated by execute_region_talk_candidate_report.py; exact source is zlib+b85 packed\n"
        "import base64 as _b64, zlib as _zlib\n"
        f"_rt_source = _zlib.decompress(_b64.b85decode({packed!r}))\n"
        "exec(compile(_rt_source, 'region_talk_candidate_report.py', 'exec'), globals(), globals())\n"
    ).encode("utf-8")
    if len(wrapper) >= KAGGLE_KERNEL_SOURCE_SAFETY_BYTES:
        raise RuntimeError(
            "Prepared Region Talk kernel wrapper is too close to Kaggle's source limit: "
            f"wrapper_bytes={len(wrapper)} safety_limit={KAGGLE_KERNEL_SOURCE_SAFETY_BYTES} "
            f"platform_limit={KAGGLE_KERNEL_SOURCE_MAX_BYTES}"
        )
    source_path.write_bytes(wrapper)
    meta_path = dst / "kernel-metadata.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    slug = kernel_slug or str(meta.get("slug") or "region-talk-candidate-report")
    if username:
        meta["id"] = f"{username}/{slug}"
    meta["slug"] = slug
    meta["title"] = "Region Talk Candidate Report"
    meta["enable_gpu"] = False
    meta["enable_internet"] = True
    e5_model_source = str(
        os.getenv("REGION_TALK_E5_KAGGLE_MODEL_SOURCE")
        or DEFAULT_E5_KAGGLE_MODEL_SOURCE
    ).strip()
    model_sources = [str(item).strip() for item in (meta.get("model_sources") or []) if str(item).strip()]
    if e5_model_source and e5_model_source not in model_sources:
        model_sources.append(e5_model_source)
    meta["model_sources"] = model_sources
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst


ACTIVE_KERNEL_STATUSES = {"RUNNING", "PENDING", "QUEUED", "INITIALIZING"}
TERMINAL_KERNEL_STATUSES = {"COMPLETE", "ERROR", "FAILED", "CANCELLED", "CANCEL_ACKNOWLEDGED"}


def _kernel_status_raw(client: Any, kernel_ref: str) -> str:
    status = client.get_kernel_status(kernel_ref)
    return str(status.get("status") or "").upper()


def assert_region_talk_kaggle_slots_free(
    client: Any,
    kernel_refs: list[str],
    *,
    optional_kernel_refs: list[str] | None = None,
    optional_kernel_auth_bundle_envs: dict[str, str] | None = None,
    allow_active: bool = False,
    auth_bundle_env: str = "",
) -> None:
    """Refuse to push over an active own kernel or same Telegram auth bundle.

    Region Talk production uses role-scoped sessions: CandidateReport defaults to
    DISCOVERY1 and ImageDiagnostic defaults to DISCOVERY2. Those two notebooks
    are expected to run in parallel. The guard therefore blocks required/own
    active kernels, and blocks optional sibling kernels only when their declared
    auth bundle equals the bundle of the launch being attempted.
    """
    if allow_active or getenv_bool("REGION_TALK_ALLOW_ACTIVE_KAGGLE_OVERWRITE", False):
        print("[region-talk-kaggle] WARNING active-kernel guard bypassed by explicit override", flush=True)
        return
    own_bundle = auth_bundle_env or os.environ.get("REGION_TALK_AUTH_BUNDLE_ENV") or "TELEGRAM_AUTH_BUNDLE_DISCOVERY1"
    optional_bundles = optional_kernel_auth_bundle_envs or {}
    checked: list[str] = []
    active_required: list[tuple[str, str]] = []
    active_same_bundle_optional: list[tuple[str, str, str]] = []
    active_parallel_optional: list[tuple[str, str, str]] = []
    errors: list[str] = []
    required = list(dict.fromkeys([r for r in kernel_refs if str(r).strip()]))
    optional = [r for r in list(dict.fromkeys(optional_kernel_refs or [])) if str(r).strip() and r not in required]
    for kernel_ref in required + optional:
        ref = str(kernel_ref)
        is_optional = ref in optional
        try:
            raw = _kernel_status_raw(client, ref)
            sibling_bundle = str(optional_bundles.get(ref) or "").strip()
            suffix = "(optional" + (f",bundle={sibling_bundle}" if sibling_bundle else "") + ")" if is_optional else ""
            checked.append(f"{ref}={raw or 'UNKNOWN'}{suffix}")
            if raw in ACTIVE_KERNEL_STATUSES:
                if not is_optional:
                    active_required.append((ref, raw))
                elif sibling_bundle and sibling_bundle == own_bundle:
                    active_same_bundle_optional.append((ref, raw, sibling_bundle))
                else:
                    active_parallel_optional.append((ref, raw, sibling_bundle or "unknown"))
        except Exception as exc:
            msg = f"{ref}:{type(exc).__name__}:{str(exc)[:180]}"
            if is_optional:
                checked.append(f"{ref}=UNVERIFIED_OPTIONAL:{type(exc).__name__}")
            else:
                errors.append(msg)
    if active_required or active_same_bundle_optional:
        refs = ", ".join(f"{ref}({status})" for ref, status in active_required)
        same_bundle = ", ".join(f"{ref}({status},bundle={bundle})" for ref, status, bundle in active_same_bundle_optional)
        joined = ", ".join(x for x in [refs, same_bundle] if x)
        raise RuntimeError(
            "Region Talk Kaggle launch refused: active kernel(s) detected "
            f"{joined}; auth bundle {own_bundle} must not be used concurrently. "
            "Cancel/finish the conflicting run first, or set "
            "REGION_TALK_ALLOW_ACTIVE_KAGGLE_OVERWRITE=1 only after manual resource audit."
        )
    if active_parallel_optional:
        parallel = ", ".join(f"{ref}({status},bundle={bundle})" for ref, status, bundle in active_parallel_optional)
        print(f"[region-talk-kaggle] parallel sibling active but allowed: {parallel}; own_bundle={own_bundle}", flush=True)
    if errors and not getenv_bool("REGION_TALK_ALLOW_UNVERIFIED_KAGGLE_SLOT", False):
        raise RuntimeError(
            "Region Talk Kaggle launch refused: could not verify active kernel slots: "
            + "; ".join(errors)
            + "; set REGION_TALK_ALLOW_UNVERIFIED_KAGGLE_SLOT=1 only after manual Kaggle UI audit."
        )
    print(f"[region-talk-kaggle] active slot check OK: {', '.join(checked) or 'no refs'}", flush=True)


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
        if raw in {"ERROR", "FAILED", "CANCELLED", "CANCEL_ACKNOWLEDGED"}:
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
    ap.add_argument("--max-sources", type=int, default=220)
    ap.add_argument("--keep-input-datasets", action="store_true", help="Do not delete temporary Kaggle config/secret input datasets after a waited run.")
    ap.add_argument("--allow-active-region-talk-kernel", action="store_true", help="Bypass active-kernel/session guard after a manual Kaggle UI audit.")
    ap.add_argument("--vector-probe-only", action="store_true", help="Run only the dual text-embedding probe on YDB texts; skip Telegram discovery and report XLSX.")
    args = ap.parse_args()
    load_env_file(args.env_file)
    run_id = args.run_id or "region-talk-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ.setdefault("REGION_TALK_RUN_ID", run_id)
    os.environ.setdefault("REGION_TALK_DRY_RUN", "1")
    os.environ.setdefault("REGION_TALK_DISABLE_PUBLISH", "1")
    if args.vector_probe_only:
        os.environ["REGION_TALK_VECTOR_PROBE_ONLY"] = "1"
    os.environ.setdefault("REGION_TALK_MAX_SOURCES", str(args.max_sources))
    os.environ.setdefault("REGION_TALK_MAX_POSTS_PER_SOURCE", "50")
    os.environ.setdefault("REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS", "1080")
    os.environ.setdefault("REGION_TALK_RUNTIME_RESERVE_BEFORE_REPORT_SECONDS", "180")
    os.environ.setdefault("REGION_TALK_RUNTIME_RESERVE_BEFORE_LLM_SECONDS", "90")
    os.environ.setdefault("REGION_TALK_RUNTIME_LOW_BUDGET_MAX_POSTS_TO_SCORE", "40")
    os.environ.setdefault("REGION_TALK_PRIORITIZE_TEXT_VECTORS", "1")
    os.environ.setdefault("REGION_TALK_TEXT_EMBEDDING_PRIORITY_MIN_MODEL_TIMEOUT_SECONDS", "420")
    os.environ.setdefault("REGION_TALK_RUNTIME_RESERVE_DURING_TEXT_EMBEDDING_SECONDS", "120")
    os.environ.setdefault("REGION_TALK_TEXT_EMBEDDING_SUBPROCESS", "1")
    os.environ.setdefault("REGION_TALK_TEXT_EMBEDDING_MODEL_TIMEOUT_SECONDS", "420")
    os.environ.setdefault("REGION_TALK_HF_HUB_DOWNLOAD_TIMEOUT", "60")
    os.environ.setdefault("REGION_TALK_HF_HUB_ETAG_TIMEOUT", "20")
    os.environ.setdefault("REGION_TALK_HF_HUB_DISABLE_XET", "1")
    os.environ.setdefault("REGION_TALK_FETCH_TELEGRAM", "1")
    os.environ.setdefault("REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST", "1")
    os.environ.setdefault("REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT", "12")
    os.environ.setdefault("REGION_TALK_MAX_IMAGES_PER_POST", "8")
    os.environ.setdefault("REGION_TALK_MAX_VLM_CALLS", "0")
    os.environ.setdefault("REGION_TALK_TG_GOVERNOR_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN", "2000")
    os.environ.setdefault("REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN", "30")
    os.environ.setdefault("REGION_TALK_TG_CACHED_ENTITY_ONLY", "0")
    os.environ.setdefault("REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN", "150")
    os.environ.setdefault("REGION_TALK_HISTORY_SOURCES_TARGET", "150")
    os.environ.setdefault("REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE", "50")
    os.environ.setdefault("REGION_TALK_HISTORY_MAX_POST_AGE_DAYS", "365")
    os.environ.setdefault("REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD", "30")
    os.environ.setdefault("REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN", "200")
    os.environ.setdefault("REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN", "250")
    os.environ.setdefault("REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN", "200")
    os.environ.setdefault("REGION_TALK_MAX_NEW_SOURCE_PROBES", "100")
    os.environ.setdefault("REGION_TALK_DISCOVERY_MODE", "mixed")
    os.environ.setdefault("REGION_TALK_HISTORY_SCAN_MODE", "primary_and_delta")
    os.environ.setdefault("REGION_TALK_TG_HUMANLIKE_PACING_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_TG_RESOLVE_DELAY_MIN_SECONDS", "20")
    os.environ.setdefault("REGION_TALK_TG_RESOLVE_DELAY_MAX_SECONDS", "45")
    os.environ.setdefault("REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MIN_SECONDS", "8")
    os.environ.setdefault("REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MAX_SECONDS", "18")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_DELAY_MIN_SECONDS", "20")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_DELAY_MAX_SECONDS", "45")
    os.environ.setdefault("REGION_TALK_TG_HISTORY_QUERY_DELAY_MIN_SECONDS", "2")
    os.environ.setdefault("REGION_TALK_TG_HISTORY_QUERY_DELAY_MAX_SECONDS", "6")
    os.environ.setdefault("REGION_TALK_TG_MEDIA_DELAY_MIN_SECONDS", "1")
    os.environ.setdefault("REGION_TALK_TG_MEDIA_DELAY_MAX_SECONDS", "4")
    os.environ.setdefault("REGION_TALK_TG_SOURCE_PAUSE_MIN_SECONDS", "4")
    os.environ.setdefault("REGION_TALK_TG_SOURCE_PAUSE_MAX_SECONDS", "12")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN", "200")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED", "30")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN", "2000")
    os.environ.setdefault("REGION_TALK_MAX_NEW_FRONTIER_PER_RUN", "2000")
    os.environ.setdefault("REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY", "1")
    os.environ.setdefault("REGION_TALK_TELEGRAM_QUERY_SOURCE", "manual")
    os.environ.setdefault("REGION_TALK_TELEGRAM_QUERY_ROTATE", "0")
    os.environ.setdefault("REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES", "30")
    os.environ.setdefault("REGION_TALK_TELEGRAM_KEYWORD_RESULTS_PER_QUERY", "10")
    os.environ.setdefault("REGION_TALK_FAST_CHECK_KO_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN", "0")
    os.environ.setdefault("REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE", "2")
    os.environ.setdefault("REGION_TALK_FAST_CHECK_KO_RESULTS_PER_QUERY", "2")
    os.environ.setdefault("REGION_TALK_FAST_CHECK_QUERY_STRATEGY", "legacy_v1")
    os.environ.setdefault("REGION_TALK_FAST_CHECK_ADAPTIVE_PREFER_CONTINUATIONS", "0")
    os.environ.setdefault("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_TABLE", "region_talk_external_blogger_evidence")
    os.environ.setdefault("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_MAX_ROWS", "2000")
    os.environ.setdefault("REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_QUERIES_PER_SOURCE", "8")
    os.environ.setdefault("REGION_TALK_FAST_CHECK_STAGE_MAX_SECONDS", "180")
    os.environ.setdefault("REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_RESULTS_PER_QUERY", "20")
    os.environ.setdefault("REGION_TALK_CONFIRMED_BLOGGER_HISTORY_SLOTS_PER_RUN", "4")
    os.environ.setdefault("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_QUERIES_PER_SOURCE", "8")
    os.environ.setdefault("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_RESULTS_PER_QUERY", "20")
    os.environ.setdefault("REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MIN_SECONDS", "5")
    os.environ.setdefault("REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MAX_SECONDS", "9")
    os.environ.setdefault("REGION_TALK_RUNTIME_RESERVE_BEFORE_FAST_CHECK_KO_SECONDS", "300")
    os.environ.setdefault("REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS", "930")
    os.environ.setdefault("REGION_TALK_RUNTIME_RESERVE_BEFORE_KEYWORD_QUERY_SECONDS", os.environ.get("REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS", "930"))
    os.environ.setdefault("REGION_TALK_RUNTIME_RESERVE_DURING_SCORING_SECONDS", "30")
    os.environ.setdefault("REGION_TALK_IMAGE_SCORING_MODE", "external_ydb_queue")
    os.environ.setdefault("REGION_TALK_MEDIA_SCORING_MODE", "retry_queue_first")
    os.environ.setdefault("REGION_TALK_MEDIA_RETRY_FIRST", "1")
    os.environ.setdefault("REGION_TALK_ACTUAL_IMAGE_TARGET", "60")
    os.environ.setdefault("REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING", "0")
    os.environ.setdefault("REGION_TALK_VK_READ_SERVICE_FIRST", "1")
    os.environ.setdefault("REGION_TALK_FETCH_VKVIDEO_WALL_FALLBACK", "1")
    os.environ.setdefault("REGION_TALK_STATE_BACKEND", "ydb")
    if (os.environ.get("REGION_TALK_STATE_BACKEND") or "").strip().lower() == "ydb":
        os.environ.setdefault("REGION_TALK_REQUIRE_YDB_STATE", "1")
    else:
        os.environ.setdefault("REGION_TALK_REQUIRE_YDB_STATE", "0")
    os.environ.setdefault("REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL", "0")
    os.environ.setdefault("REGION_TALK_ALLOW_KAGGLE_YDB_SECRET", "0")
    os.environ.setdefault("REGION_TALK_YDB_PRUNE_LEGACY_QUEUE_PAYLOADS", "1")
    os.environ.setdefault("REGION_TALK_YDB_PRUNE_MAX_ROWS", "200")
    os.environ.setdefault("REGION_TALK_YDB_SKIP_ROW_LEVEL_REWRITE", "1")
    os.environ.setdefault("REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS", "8")
    os.environ.setdefault("REGION_TALK_YDB_HEARTBEAT_REQUEST_TIMEOUT_SECONDS", "5")
    os.environ.setdefault("REGION_TALK_YDB_HEARTBEAT_MAX_RETRIES", "0")
    os.environ.setdefault("REGION_TALK_YDB_QUEUE_REQUEST_TIMEOUT_SECONDS", "8")
    os.environ.setdefault("REGION_TALK_YDB_QUEUE_MAX_RETRIES", "1")
    os.environ.setdefault("REGION_TALK_YDB_STATE_LOAD_ATTEMPTS", "1")
    os.environ.setdefault("REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS", "0")
    os.environ.setdefault("REGION_TALK_YDB_STATE_LOAD_REQUEST_TIMEOUT_SECONDS", os.environ.get("REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS", "8"))
    os.environ.setdefault("REGION_TALK_YDB_STATE_LOAD_MAX_RETRIES", os.environ.get("REGION_TALK_YDB_MAX_RETRIES", "2"))
    os.environ.setdefault("REGION_TALK_YDB_ROW_UPSERT_CHUNK_SIZE", "100")
    os.environ.setdefault("REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR", "1")
    os.environ.setdefault("REGION_TALK_ENABLE_STACK_WATCHDOG", "1")
    os.environ.setdefault("REGION_TALK_STACK_WATCHDOG_SECONDS", "300")
    os.environ.setdefault("REGION_TALK_HF_HUB_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("REGION_TALK_TQDM_DISABLE", "1")
    os.environ.setdefault("REGION_TALK_TRANSFORMERS_VERBOSITY", "error")
    os.environ.setdefault("REGION_TALK_LIVE_EVENT_LOG_PATH", "/kaggle/working/region_talk_run_events_live.jsonl")
    os.environ.setdefault("REGION_TALK_STDOUT_HEARTBEATS", "0")
    os.environ.setdefault("REGION_TALK_STDOUT_ALL_EVENTS", "0")
    os.environ.setdefault("REGION_TALK_KAGGLE_STATUS_STDOUT", "0")
    os.environ.setdefault("REGION_TALK_DELTA_SCAN_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_DELTA_SCAN_WINDOW_DAYS", "14")
    os.environ.setdefault("REGION_TALK_DELTA_OVERLAP_POSTS", "50")
    os.environ.setdefault("REGION_TALK_PRIORITIZE_TRAVEL_SOURCES", "1")
    os.environ.setdefault("REGION_TALK_SOURCE_QUEUE_SELECTION_POOL_MULTIPLIER", "10")
    os.environ.setdefault("REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES", "0")
    os.environ.setdefault("REGION_TALK_SEMANTIC_GATE_MODE", "vector_first_final_llm")
    os.environ.setdefault("REGION_TALK_ENABLE_EARLY_LLM", "0")
    os.environ.setdefault("REGION_TALK_ENABLE_VECTOR_GATES", "1")
    os.environ.setdefault("REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS", "1")
    # Keep the main CandidateReport CPU run to E5 only; BGE-M3 is isolated in a
    # separate Kaggle notebook and fused from durable YDB rows on later passes.
    os.environ.setdefault("REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS", "0")
    os.environ.setdefault("REGION_TALK_TEXT_EMBEDDING_MODEL_IDS", "intfloat/multilingual-e5-base")
    os.environ.setdefault("REGION_TALK_EXTERNAL_BGE_M3_FUSION_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE", "1")
    os.environ.setdefault("REGION_TALK_ENABLE_FINAL_LLM_VERIFIER", "1")
    os.environ.setdefault("REGION_TALK_TARGET_LLM_CALLS", "10")
    os.environ.setdefault("REGION_TALK_MAX_LLM_FINAL_VERIFY", "10")
    os.environ.setdefault("REGION_TALK_MEMORY_VECTOR_RECHECK_MAX_ROWS", "0")
    os.environ.setdefault("REGION_TALK_MEMORY_VECTOR_RECHECK_BATCH_EMBEDDINGS", "0")
    os.environ.setdefault("REGION_TALK_SOURCE_PROFILE_BUILD_ENABLED", "0")
    os.environ.setdefault("REGION_TALK_SOURCE_PROFILE_HEARTBEAT_EVERY_ROWS", "25")
    os.environ.setdefault("REGION_TALK_LIVE_IMAGE_QUEUE_HANDOFF_EARLY", "1")
    os.environ.setdefault("REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF", "0")
    os.environ.setdefault("REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF", "1")
    os.environ.setdefault("REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS", "500")
    os.environ.setdefault("REGION_TALK_SOURCE_QUEUE_HANDOFF_PERSIST_REORDERED_TAIL", "0")
    os.environ.setdefault("REGION_TALK_YDB_ONLINE_QUEUE_WRITE_MAX_ROWS", "0")
    os.environ.setdefault("REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY", "1")
    os.environ.setdefault("REGION_TALK_YDB_ONLINE_CANDIDATE_WRITE_MAX_ROWS", "0")
    os.environ.setdefault("REGION_TALK_WRITE_SOURCE_STATUS_QUEUE_MIRROR", "0")
    os.environ.setdefault("REGION_TALK_LLM_MODEL", "gemini-3.5-flash-lite")
    os.environ.setdefault("REGION_TALK_LLM_FALLBACK_MODELS", "gemini-3.1-flash-lite")
    os.environ.setdefault("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME", "GOOGLE_API_KEY3")
    os.environ.setdefault("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "60")
    os.environ.setdefault("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", os.environ.get("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "60"))
    os.environ.setdefault("GOOGLE_AI_ALLOW_RESERVE_FALLBACK", "0")
    os.environ.setdefault("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "0")
    os.environ.setdefault("GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR", "0")
    os.environ.setdefault("REGION_TALK_SEED_FILE", "seed-sources-v2.csv")
    os.environ.setdefault("REGION_TALK_PLACE_LEXICON_FILE", "kaliningrad-place-lexicon-v1.csv")
    os.environ.setdefault("REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE", "public_travel_blogger_channel_links.xlsx")
    os.environ.setdefault("REGION_TALK_TG_PUBLIC_WEB_FALLBACK", "0")
    os.environ.setdefault("REGION_TALK_TG_PUBLIC_WEB_MAX_PAGES_PER_SOURCE", "10")
    os.environ.setdefault("REGION_TALK_TG_PUBLIC_WEB_TIMEOUT_SECONDS", "20")
    os.environ.setdefault("REGION_TALK_MIN_POST_DATE", "")
    os.environ.setdefault("REGION_TALK_FRESHNESS_HALF_LIFE_DAYS", "30")
    preflight_ydb_access()
    kernel_path = prepared_kernel_path(run_id=run_id, kernel_slug=args.kernel_slug)
    client = KaggleClient() if KaggleClient is not None else DirectKaggleClient()
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME is required")
    cleanup_stale_region_talk_input_datasets(client)
    kernel_ref = kernel_ref_from_meta(kernel_path, kernel_slug=args.kernel_slug)
    image_kernel_ref = f"{username}/region-talk-image-diagnostic"
    assert_region_talk_kaggle_slots_free(
        client,
        [kernel_ref],
        optional_kernel_refs=[image_kernel_ref],
        optional_kernel_auth_bundle_envs={image_kernel_ref: "TELEGRAM_AUTH_BUNDLE_DISCOVERY2"},
        allow_active=bool(args.allow_active_region_talk_kernel),
        auth_bundle_env=os.environ.get("REGION_TALK_AUTH_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_DISCOVERY1"),
    )
    dataset_sources = build_input_datasets(client, run_id=run_id, username=username)
    print(f"[region-talk-kaggle] pushing {kernel_ref} run_id={run_id} datasets={len(dataset_sources)}", flush=True)
    client.push_kernel(kernel_path=kernel_path, dataset_sources=dataset_sources)
    if args.no_wait:
        print(f"[region-talk-kaggle] pushed {kernel_ref}; not waiting; temporary input datasets retained until run completion: {dataset_sources}", flush=True)
        return 0
    completed = False
    try:
        poll_kernel(client, kernel_ref, timeout_minutes=args.timeout_minutes, poll_interval_seconds=args.poll_interval_seconds)
        completed = True
        out_dir = ARTIFACT_ROOT / run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        output_file_pattern = os.environ.get(
            "REGION_TALK_KAGGLE_OUTPUT_FILE_PATTERN",
            r"(^|/)(candidates-latest\.xlsx|output\.json|stage_status\.json|run_events\.jsonl|candidate_found\.jsonl|telegram-request-ledger\.jsonl|region-talk-state\.json|[^/]+\.(xlsx|json|jsonl|md|html|log|csv))$",
        )
        files = client.download_kernel_output(kernel_ref, path=out_dir, force=True, file_pattern=output_file_pattern)
        print(f"[region-talk-kaggle] downloaded {len(files)} files to {out_dir}", flush=True)
        latest = out_dir / "candidates-latest.xlsx"
        if latest.exists():
            public_latest = PROJECT_ROOT / "artifacts" / "region-talk" / "candidates-latest.xlsx"
            public_latest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(latest, public_latest)
            print(f"[region-talk-kaggle] latest workbook: {public_latest}", flush=True)
    finally:
        if not args.keep_input_datasets and completed:
            cleanup_input_datasets(client, dataset_sources)
        elif not completed:
            print(f"[region-talk-kaggle] keeping input datasets because run did not complete locally: {dataset_sources}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
