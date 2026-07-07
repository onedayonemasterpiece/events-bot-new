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
            self.api.kernels_push(str(kernel_path))
        except Exception as exc:
            response = getattr(exc, "response", None)
            body = ""
            if response is not None:
                try:
                    body = str(response.text or "")[:1000]
                except Exception:
                    body = ""
            raise RuntimeError("Kaggle kernels_push failed" + (f": {body}" if body else "")) from exc

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


def wait_dataset_ready(client: Any, dataset_ref: str, *, expected_files: list[str], timeout_seconds: int = 240) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_status = ""
    last_files: list[str] = []
    while time.monotonic() < deadline:
        try:
            last_status = str(client.dataset_status(dataset_ref))
            raw_files = client.dataset_list_files(dataset_ref)
            last_files = [str(getattr(x, "name", x)) for x in raw_files]
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
    selected_auth_bundle = (auth_bundle_env or os.environ.get("REGION_TALK_AUTH_BUNDLE_ENV") or "TELEGRAM_AUTH_BUNDLE_DISCOVERY").strip()
    names = [
        "TG_API_ID", "TG_API_HASH", "TELEGRAM_API_ID", "TELEGRAM_API_HASH",
        "SUPABASE_URL", "SUPABASE_SERVICE_KEY", "SUPABASE_KEY", "SUPABASE_SCHEMA",
        "GOOGLE_API_KEY", "GOOGLE_API_KEY3", "GOOGLE_API_KEY_3",
        "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON", "REGION_TALK_YDB_IAM_TOKEN", "YC_IAM_TOKEN", "YDB_ACCESS_TOKEN",
        "VK_ACCESS_TOKEN", "VK_SERVICE_TOKEN", "VK_SERVICE_KEY", "VK_TOKEN",
    ]
    if selected_auth_bundle:
        names.append(selected_auth_bundle)
    return list(dict.fromkeys(names))


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
        "REGION_TALK_STATE_BACKEND": os.environ.get("REGION_TALK_STATE_BACKEND", "json"),
        "REGION_TALK_REQUIRE_YDB_STATE": os.environ.get("REGION_TALK_REQUIRE_YDB_STATE", "0"),
        "REGION_TALK_YDB_ENDPOINT": os.environ.get("REGION_TALK_YDB_ENDPOINT", ""),
        "REGION_TALK_YDB_DATABASE": os.environ.get("REGION_TALK_YDB_DATABASE", ""),
        "REGION_TALK_YDB_NAMESPACE": os.environ.get("REGION_TALK_YDB_NAMESPACE", "region_talk"),
        "REGION_TALK_YDB_STATE_SNAPSHOT_FILE": os.environ.get("REGION_TALK_YDB_STATE_SNAPSHOT_FILE", ""),
        "REGION_TALK_YDB_MAX_POST_ROWS": os.environ.get("REGION_TALK_YDB_MAX_POST_ROWS", "20000"),
        "REGION_TALK_YDB_MAX_SOURCE_ROWS": os.environ.get("REGION_TALK_YDB_MAX_SOURCE_ROWS", "5000"),
        "REGION_TALK_YDB_MAX_CANDIDATE_ROWS": os.environ.get("REGION_TALK_YDB_MAX_CANDIDATE_ROWS", "5000"),
        "REGION_TALK_YDB_PRUNE_LEGACY_QUEUE_PAYLOADS": os.environ.get("REGION_TALK_YDB_PRUNE_LEGACY_QUEUE_PAYLOADS", "1"),
        "REGION_TALK_YDB_PRUNE_MAX_ROWS": os.environ.get("REGION_TALK_YDB_PRUNE_MAX_ROWS", "200"),
        "REGION_TALK_DELTA_SCAN_ENABLED": os.environ.get("REGION_TALK_DELTA_SCAN_ENABLED", "1"),
        "REGION_TALK_DELTA_SCAN_WINDOW_DAYS": os.environ.get("REGION_TALK_DELTA_SCAN_WINDOW_DAYS", "14"),
        "REGION_TALK_DELTA_OVERLAP_POSTS": os.environ.get("REGION_TALK_DELTA_OVERLAP_POSTS", "50"),
        "REGION_TALK_MAX_SOURCES": os.environ.get("REGION_TALK_MAX_SOURCES", "220"),
        "REGION_TALK_MAX_POSTS_PER_SOURCE": os.environ.get("REGION_TALK_MAX_POSTS_PER_SOURCE", "50"),
        "REGION_TALK_MAX_POSTS_TO_SCORE_PER_RUN": os.environ.get("REGION_TALK_MAX_POSTS_TO_SCORE_PER_RUN", "180"),
        "REGION_TALK_VECTOR_HEARTBEAT_EVERY_POSTS": os.environ.get("REGION_TALK_VECTOR_HEARTBEAT_EVERY_POSTS", "25"),
        "REGION_TALK_FETCH_TELEGRAM": os.environ.get("REGION_TALK_FETCH_TELEGRAM", "1"),
        "REGION_TALK_POST_INPUT_MODE": os.environ.get("REGION_TALK_POST_INPUT_MODE", os.environ.get("REGION_TALK_FETCH_MODE", "")),
        "REGION_TALK_YDB_CANDIDATE_LINK_LIMIT": os.environ.get("REGION_TALK_YDB_CANDIDATE_LINK_LIMIT", "180"),
        "REGION_TALK_MAX_IMAGES_PER_POST": os.environ.get("REGION_TALK_MAX_IMAGES_PER_POST", "8"),
        "REGION_TALK_MAX_VLM_CALLS": os.environ.get("REGION_TALK_MAX_VLM_CALLS", "0"),
        "REGION_TALK_TG_GOVERNOR_ENABLED": os.environ.get("REGION_TALK_TG_GOVERNOR_ENABLED", "1"),
        "REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN", "2000"),
        "REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN", "30"),
        "REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN", "150"),
        "REGION_TALK_HISTORY_SOURCES_TARGET": os.environ.get("REGION_TALK_HISTORY_SOURCES_TARGET", "150"),
        "REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE": os.environ.get("REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE", "50"),
        "REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN", "200"),
        "REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN": os.environ.get("REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN", "250"),
        "REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN": os.environ.get("REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN", "200"),
        "REGION_TALK_MAX_NEW_SOURCE_PROBES": os.environ.get("REGION_TALK_MAX_NEW_SOURCE_PROBES", "100"),
        "REGION_TALK_DISCOVERY_MODE": os.environ.get("REGION_TALK_DISCOVERY_MODE", "mixed"),
        "REGION_TALK_HISTORY_SCAN_MODE": os.environ.get("REGION_TALK_HISTORY_SCAN_MODE", "primary_and_delta"),
        "REGION_TALK_TG_FLOODWAIT_MAX_SLEEP_SECONDS": os.environ.get("REGION_TALK_TG_FLOODWAIT_MAX_SLEEP_SECONDS", "60"),
        "REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS": os.environ.get("REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS", "300"),
        "REGION_TALK_TG_FLOODWAIT_COOLDOWN_MARGIN_SECONDS": os.environ.get("REGION_TALK_TG_FLOODWAIT_COOLDOWN_MARGIN_SECONDS", "1800"),
        "REGION_TALK_TG_SIMILAR_ENABLED": os.environ.get("REGION_TALK_TG_SIMILAR_ENABLED", os.environ.get("REGION_TALK_DISCOVERY_ENABLE_TELEGRAM_SIMILAR", "1")),
        "REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN": os.environ.get("REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN", os.environ.get("REGION_TALK_DISCOVERY_MAX_SIMILAR_SEED_CHANNELS", "200")),
        "REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED": os.environ.get("REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED", os.environ.get("REGION_TALK_DISCOVERY_MAX_SIMILAR_PER_SEED", "30")),
        "REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN": os.environ.get("REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN", os.environ.get("REGION_TALK_DISCOVERY_MAX_NEW_FRONTIER_PER_RUN", "2000")),
        "REGION_TALK_MAX_NEW_FRONTIER_PER_RUN": os.environ.get("REGION_TALK_MAX_NEW_FRONTIER_PER_RUN", "2000"),
        "REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY": os.environ.get("REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY", "1"),
        "REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES": os.environ.get("REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES", "80"),
        "REGION_TALK_IMAGE_SCORING_MODE": os.environ.get("REGION_TALK_IMAGE_SCORING_MODE", "external_ydb_queue"),
        "REGION_TALK_MEDIA_SCORING_MODE": os.environ.get("REGION_TALK_MEDIA_SCORING_MODE", "retry_queue_first"),
        "REGION_TALK_MEDIA_RETRY_FIRST": os.environ.get("REGION_TALK_MEDIA_RETRY_FIRST", "1"),
        "REGION_TALK_ACTUAL_IMAGE_TARGET": os.environ.get("REGION_TALK_ACTUAL_IMAGE_TARGET", "60"),
        "REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING": os.environ.get("REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING", "0"),
        "REGION_TALK_VK_READ_SERVICE_FIRST": os.environ.get("REGION_TALK_VK_READ_SERVICE_FIRST", "1"),
        "REGION_TALK_FETCH_VKVIDEO_WALL_FALLBACK": os.environ.get("REGION_TALK_FETCH_VKVIDEO_WALL_FALLBACK", "1"),
        "REGION_TALK_AUTH_BUNDLE_ENV": os.environ.get("REGION_TALK_AUTH_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_DISCOVERY"),
        "REGION_TALK_SEMANTIC_GATE_MODE": os.environ.get("REGION_TALK_SEMANTIC_GATE_MODE", "vector_first_final_llm"),
        "REGION_TALK_LLM_MODEL": os.environ.get("REGION_TALK_LLM_MODEL", "gemini-3.1-flash-lite"),
        "REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME": os.environ.get("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME", "GOOGLE_API_KEY3"),
        "REGION_TALK_ENABLE_EARLY_LLM": os.environ.get("REGION_TALK_ENABLE_EARLY_LLM", "0"),
        "REGION_TALK_ENABLE_VECTOR_GATES": os.environ.get("REGION_TALK_ENABLE_VECTOR_GATES", "1"),
        "REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS": os.environ.get("REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS", "1"),
        "REGION_TALK_ENABLE_FINAL_LLM_VERIFIER": os.environ.get("REGION_TALK_ENABLE_FINAL_LLM_VERIFIER", "1"),
        "REGION_TALK_TARGET_LLM_CALLS": os.environ.get("REGION_TALK_TARGET_LLM_CALLS", "10"),
        "REGION_TALK_MAX_LLM_FINAL_VERIFY": os.environ.get("REGION_TALK_MAX_LLM_FINAL_VERIFY", "10"),
        "GOOGLE_AI_ALLOW_RESERVE_FALLBACK": "0",
        "GOOGLE_AI_LOCAL_LIMITER_FALLBACK": "0",
        "GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR": "0",
        "REGION_TALK_SEED_FILE": os.environ.get("REGION_TALK_SEED_FILE", "seed-sources-v2.csv"),
        "REGION_TALK_PLACE_LEXICON_FILE": os.environ.get("REGION_TALK_PLACE_LEXICON_FILE", "kaliningrad-place-lexicon-v1.csv"),
        "REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE": os.environ.get("REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE", "public_travel_blogger_channel_links.xlsx"),
        "REGION_TALK_MIN_POST_DATE": os.environ.get("REGION_TALK_MIN_POST_DATE", "2026-01-01"),
        "REGION_TALK_FRESHNESS_HALF_LIFE_DAYS": os.environ.get("REGION_TALK_FRESHNESS_HALF_LIFE_DAYS", "30"),
        "REGION_TALK_OUTPUT_DIR": f"artifacts/region-talk/runs/{run_id}",
    }
    secret_names = region_talk_secret_names(env.get("REGION_TALK_AUTH_BUNDLE_ENV"))
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
    ap.add_argument("--max-sources", type=int, default=220)
    ap.add_argument("--keep-input-datasets", action="store_true", help="Do not delete temporary Kaggle config/secret input datasets after a waited run.")
    args = ap.parse_args()
    load_env_file(args.env_file)
    run_id = args.run_id or "region-talk-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ.setdefault("REGION_TALK_RUN_ID", run_id)
    os.environ.setdefault("REGION_TALK_DRY_RUN", "1")
    os.environ.setdefault("REGION_TALK_DISABLE_PUBLISH", "1")
    os.environ.setdefault("REGION_TALK_MAX_SOURCES", str(args.max_sources))
    os.environ.setdefault("REGION_TALK_MAX_POSTS_PER_SOURCE", "50")
    os.environ.setdefault("REGION_TALK_FETCH_TELEGRAM", "1")
    os.environ.setdefault("REGION_TALK_MAX_IMAGES_PER_POST", "8")
    os.environ.setdefault("REGION_TALK_MAX_VLM_CALLS", "0")
    os.environ.setdefault("REGION_TALK_TG_GOVERNOR_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN", "2000")
    os.environ.setdefault("REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN", "30")
    os.environ.setdefault("REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN", "150")
    os.environ.setdefault("REGION_TALK_HISTORY_SOURCES_TARGET", "150")
    os.environ.setdefault("REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE", "50")
    os.environ.setdefault("REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN", "200")
    os.environ.setdefault("REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN", "250")
    os.environ.setdefault("REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN", "200")
    os.environ.setdefault("REGION_TALK_MAX_NEW_SOURCE_PROBES", "100")
    os.environ.setdefault("REGION_TALK_DISCOVERY_MODE", "mixed")
    os.environ.setdefault("REGION_TALK_HISTORY_SCAN_MODE", "primary_and_delta")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN", "200")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED", "30")
    os.environ.setdefault("REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN", "2000")
    os.environ.setdefault("REGION_TALK_MAX_NEW_FRONTIER_PER_RUN", "2000")
    os.environ.setdefault("REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY", "1")
    os.environ.setdefault("REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES", "80")
    os.environ.setdefault("REGION_TALK_IMAGE_SCORING_MODE", "external_ydb_queue")
    os.environ.setdefault("REGION_TALK_MEDIA_SCORING_MODE", "retry_queue_first")
    os.environ.setdefault("REGION_TALK_MEDIA_RETRY_FIRST", "1")
    os.environ.setdefault("REGION_TALK_ACTUAL_IMAGE_TARGET", "60")
    os.environ.setdefault("REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING", "0")
    os.environ.setdefault("REGION_TALK_VK_READ_SERVICE_FIRST", "1")
    os.environ.setdefault("REGION_TALK_FETCH_VKVIDEO_WALL_FALLBACK", "1")
    os.environ.setdefault("REGION_TALK_STATE_BACKEND", "json")
    os.environ.setdefault("REGION_TALK_REQUIRE_YDB_STATE", "0")
    os.environ.setdefault("REGION_TALK_YDB_PRUNE_LEGACY_QUEUE_PAYLOADS", "1")
    os.environ.setdefault("REGION_TALK_YDB_PRUNE_MAX_ROWS", "200")
    os.environ.setdefault("REGION_TALK_DELTA_SCAN_ENABLED", "1")
    os.environ.setdefault("REGION_TALK_DELTA_SCAN_WINDOW_DAYS", "14")
    os.environ.setdefault("REGION_TALK_DELTA_OVERLAP_POSTS", "50")
    os.environ.setdefault("REGION_TALK_SEMANTIC_GATE_MODE", "vector_first_final_llm")
    os.environ.setdefault("REGION_TALK_ENABLE_EARLY_LLM", "0")
    os.environ.setdefault("REGION_TALK_ENABLE_VECTOR_GATES", "1")
    os.environ.setdefault("REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS", "1")
    os.environ.setdefault("REGION_TALK_ENABLE_FINAL_LLM_VERIFIER", "1")
    os.environ.setdefault("REGION_TALK_TARGET_LLM_CALLS", "10")
    os.environ.setdefault("REGION_TALK_MAX_LLM_FINAL_VERIFY", "10")
    os.environ.setdefault("REGION_TALK_LLM_MODEL", "gemini-3.1-flash-lite")
    os.environ.setdefault("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME", "GOOGLE_API_KEY3")
    os.environ.setdefault("GOOGLE_AI_ALLOW_RESERVE_FALLBACK", "0")
    os.environ.setdefault("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "0")
    os.environ.setdefault("GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR", "0")
    os.environ.setdefault("REGION_TALK_SEED_FILE", "seed-sources-v2.csv")
    os.environ.setdefault("REGION_TALK_PLACE_LEXICON_FILE", "kaliningrad-place-lexicon-v1.csv")
    os.environ.setdefault("REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE", "public_travel_blogger_channel_links.xlsx")
    os.environ.setdefault("REGION_TALK_MIN_POST_DATE", "2026-01-01")
    os.environ.setdefault("REGION_TALK_FRESHNESS_HALF_LIFE_DAYS", "30")
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
        print(f"[region-talk-kaggle] pushed {kernel_ref}; not waiting; temporary input datasets retained until run completion: {dataset_sources}", flush=True)
        return 0
    try:
        poll_kernel(client, kernel_ref, timeout_minutes=args.timeout_minutes, poll_interval_seconds=args.poll_interval_seconds)
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
        if not args.keep_input_datasets:
            cleanup_input_datasets(client, dataset_sources)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
