#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import base64
import gzip
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
KERNEL_SRC = PROJECT_ROOT / "kaggle" / "EventCommentFeedback"
PHRASE_BANK = PROJECT_ROOT / "docs" / "features" / "event-comment-feedback" / "phrase-bank-v1.md"
PHRASE_BANK_JSON = PROJECT_ROOT / "docs" / "features" / "event-comment-feedback" / "phrase-bank-v1.json"
ARTIFACT_ROOT = PROJECT_ROOT / "artifacts" / "codex" / "event-comment-feedback"
DEFAULT_MODELS = ["intfloat/multilingual-e5-base", "BAAI/bge-m3"]


def load_env_file(path: Path | None) -> None:
    if not path or not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")




class SimpleKaggleClient:
    """Minimal Kaggle API adapter that does not import project models/sqlmodel."""

    def __init__(self) -> None:
        if not os.getenv("KAGGLE_CONFIG_DIR") and not Path.home().joinpath(".kaggle", "kaggle.json").exists():
            fallback = Path("/home/dev/.kaggle/kaggle.json")
            if fallback.exists():
                os.environ["KAGGLE_CONFIG_DIR"] = str(fallback.parent)
        pipx_site = Path('/opt/pipx/venvs/kaggle/lib/python3.12/site-packages')
        if pipx_site.exists() and str(pipx_site) not in sys.path:
            sys.path.insert(0, str(pipx_site))
        from kaggle.api.kaggle_api_extended import KaggleApi

        self.api = KaggleApi()
        self.api.authenticate()
        self.cli = os.getenv("KAGGLE_CLI", "/usr/local/bin/kaggle")

    def _run_cli(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run([self.cli, *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
        if proc.stdout.strip():
            print(proc.stdout.rstrip(), flush=True)
        if check and proc.returncode != 0:
            raise RuntimeError(f"kaggle {' '.join(args)} failed with code {proc.returncode}")
        return proc

    def create_dataset(self, folder: Path, *, public: bool = False, quiet: bool = True, convert_to_csv: bool = False, dir_mode: str = "zip") -> None:
        args = ["datasets", "create", "-p", str(folder), "-r", dir_mode]
        if public:
            args.append("-u")
        if quiet:
            args.append("-q")
        if not convert_to_csv:
            args.append("-t")
        print(f"[event-comment-feedback] creating Kaggle dataset from {folder}", flush=True)
        self._run_cli(args, check=True)

    def dataset_status(self, dataset_ref: str) -> str:
        proc = self._run_cli(["datasets", "status", dataset_ref], check=False)
        if proc.returncode == 0:
            lines = [line.strip() for line in proc.stdout.splitlines() if line.strip() and not line.startswith("Warning:")]
            return lines[-1] if lines else ""
        # Fall back to Python API for richer exceptions/logs.
        return str(self.api.dataset_status(dataset_ref))

    def dataset_visible_mine(self, dataset_ref: str) -> bool:
        slug = dataset_ref.split("/", 1)[1] if "/" in dataset_ref else dataset_ref
        proc = self._run_cli(["datasets", "list", "--mine", "-s", slug], check=False)
        return dataset_ref in proc.stdout

    def dataset_list_files(self, dataset_ref: str, *, page_size: int = 20) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        token: str | None = None
        while True:
            response = self.api.dataset_list_files(dataset_ref, page_token=token, page_size=page_size)
            files = getattr(response, "files", None)
            if files is None and isinstance(response, list):
                files = response
            for item in files or []:
                name = getattr(item, "name", None) or str(item)
                if name in seen:
                    continue
                seen.add(name)
                out.append({"name": name, "totalBytes": getattr(item, "totalBytes", None)})
            token = getattr(response, "nextPageToken", None) or getattr(response, "next_page_token", None) or None
            if not token or isinstance(response, list):
                break
        return out

    def push_kernel(self, *, kernel_path: Path, dataset_sources: list[str]) -> None:
        meta_path = kernel_path / "kernel-metadata.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        username = (os.getenv("KAGGLE_USERNAME") or "").strip()
        kernel_id = str(meta.get("id") or "").strip()
        if username and kernel_id:
            owner, slug = kernel_id.split("/", 1) if "/" in kernel_id else ("", kernel_id)
            if slug and owner != username:
                meta["id"] = f"{username}/{slug}"
        meta["dataset_sources"] = [str(x).strip() for x in dataset_sources if str(x).strip()]
        meta["enable_internet"] = True
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[event-comment-feedback] pushing Kaggle kernel from {kernel_path} datasets={meta['dataset_sources']}", flush=True)
        deadline = time.monotonic() + 90
        attempt = 0
        last_invalid: list[str] = []
        while True:
            attempt += 1
            response = self.api.kernels_push(str(kernel_path))
            info = response.to_dict() if hasattr(response, "to_dict") else {}
            invalid = list(getattr(response, "invalid_dataset_sources", None) or info.get("invalid_dataset_sources") or [])
            error = getattr(response, "error", None) or info.get("error") or getattr(response, "error_message", None)
            print(
                f"[event-comment-feedback] kernels_push attempt={attempt} ref={info.get('ref')} "
                f"version={info.get('version_number')} invalid={invalid} error={error}",
                flush=True,
            )
            if not invalid and not error:
                return
            last_invalid = invalid
            if error and "Maximum batch CPU session count" in str(error):
                raise RuntimeError(f"Kaggle batch CPU session limit reached: {error}")
            if invalid and time.monotonic() < deadline:
                print("[event-comment-feedback] Kaggle still binding datasets; retry in 10s", flush=True)
                time.sleep(10)
                continue
            raise RuntimeError(f"Kaggle kernels_push failed invalid={last_invalid} error={error}")

    def kernels_pull(self, kernel_ref: str, path: Path, *, metadata: bool = True) -> None:
        self.api.kernels_pull(kernel_ref, path=str(path), metadata=metadata, quiet=True)

    def kernel_has_dataset_sources(self, kernel_ref: str, expected_sources: list[str]) -> tuple[bool, dict[str, Any]]:
        expected = [str(x).strip() for x in expected_sources if str(x).strip()]
        with tempfile.TemporaryDirectory() as tmp:
            pull_dir = Path(tmp)
            self.kernels_pull(kernel_ref, pull_dir, metadata=True)
            meta_path = pull_dir / "kernel-metadata.json"
            if not meta_path.exists():
                raise FileNotFoundError(meta_path)
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        actual = [str(x).strip() for x in (meta.get("dataset_sources") or []) if str(x).strip()]
        if not actual:
            # Some API versions expose pulled metadata as dataset_data_sources.
            actual = [str(x).strip() for x in (meta.get("dataset_data_sources") or []) if str(x).strip()]
        meta["dataset_sources"] = actual
        return all(x in actual for x in expected), meta

    def get_kernel_status(self, kernel_ref: str) -> dict[str, Any]:
        response = self.api.kernels_status(kernel_ref)
        if hasattr(response, "to_dict"):
            result = response.to_dict()
        else:
            try:
                result = json.loads(str(response))
            except Exception:
                result = {}
        if not result.get("status"):
            status = getattr(response, "status", None)
            if status is not None:
                result["status"] = status.name if hasattr(status, "name") else str(status)
        fail = getattr(response, "failure_message", None) or getattr(response, "failureMessage", None)
        if fail and not result.get("failureMessage"):
            result["failureMessage"] = fail
        return result

    def download_kernel_output(self, kernel_ref: str, *, path: Path, force: bool = True) -> list[str]:
        files, _ = self.api.kernels_output(kernel_ref, path=str(path), force=force, quiet=True)
        return list(files or [])

    def delete_dataset(self, dataset_ref: str, *, no_confirm: bool = True) -> None:
        if "/" in dataset_ref:
            owner, slug = dataset_ref.split("/", 1)
        else:
            owner, slug = os.getenv("KAGGLE_USERNAME", ""), dataset_ref
        self.api.dataset_delete(owner, slug, no_confirm=no_confirm)


def write_dataset_metadata(folder: Path, dataset_ref: str, title: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    metadata = json.dumps(
        {"title": title[:100], "id": dataset_ref, "licenses": [{"name": "CC0-1.0"}]},
        ensure_ascii=False,
        indent=2,
    ) + "\n"
    (folder / "dataset-metadata.json").write_text(metadata, encoding="utf-8")


def wait_dataset_ready(client: Any, dataset_ref: str, *, expected_files: list[str], timeout_seconds: int = 300) -> None:
    deadline = time.monotonic() + max(30, int(timeout_seconds))
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            status = str(client.dataset_status(dataset_ref))
            files = client.dataset_list_files(dataset_ref)
            names = {str(f.get("name") or "") for f in files}
            last = {"status": status, "names": sorted(names)}
            if status.strip().lower() == "ready" and all(name in names for name in expected_files):
                print(f"[event-comment-feedback] dataset ready: {dataset_ref} files={sorted(names)}", flush=True)
                return
        except Exception as exc:
            visible = False
            try:
                visible = bool(client.dataset_visible_mine(dataset_ref))
            except Exception:
                visible = False
            last = {"error": f"{type(exc).__name__}: {exc}", "visible_mine": visible}
            if visible:
                print(
                    f"[event-comment-feedback] dataset visible in my list despite status/files error: {dataset_ref}; "
                    "kernel push will validate dataset source binding",
                    flush=True,
                )
                return
        time.sleep(5)
    raise TimeoutError(f"dataset not ready: {dataset_ref}; last={last}")


def wait_kernel_dataset_sources(client: Any, kernel_ref: str, dataset_sources: list[str], timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + max(30, int(timeout_seconds))
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            ok, meta = client.kernel_has_dataset_sources(kernel_ref, dataset_sources)
            last = meta
            if ok:
                print(f"[event-comment-feedback] kernel dataset sources bound: {dataset_sources}", flush=True)
                return
        except Exception as exc:
            last = {"error": f"{type(exc).__name__}: {exc}"}
        time.sleep(10)
    raise TimeoutError(f"kernel dataset sources did not bind: {dataset_sources}; last={last}")


def parse_tg(url: str | None) -> dict[str, Any] | None:
    if not url:
        return None
    u = str(url).strip().rstrip("/")
    m = re.search(r"t\.me/(?:s/)?([A-Za-z0-9_]+)/([0-9]+)(?:\?.*)?$", u)
    if m:
        return {"platform": "telegram", "username": m.group(1), "message_id": int(m.group(2)), "platform_post_key": f"tg:{m.group(1)}:{m.group(2)}"}
    m = re.search(r"t\.me/c/([0-9]+)/([0-9]+)(?:\?.*)?$", u)
    if m:
        return {"platform": "telegram", "private_c": m.group(1), "message_id": int(m.group(2)), "platform_post_key": f"tg:c:{m.group(1)}:{m.group(2)}", "private": True}
    return None


def parse_vk(url: str | None) -> dict[str, Any] | None:
    if not url:
        return None
    u = str(url).strip()
    m = re.search(r"(?:vk\.com/)?wall(-?\d+)_(\d+)", u)
    if not m:
        return None
    owner = int(m.group(1))
    post = int(m.group(2))
    return {"platform": "vk", "owner_id": owner, "post_id": post, "platform_post_key": f"vk:{owner}:{post}"}


def build_manifest_from_sqlite(db_path: Path, *, run_date: str) -> dict[str, Any]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    quick = con.execute("pragma quick_check").fetchone()[0]
    event_cols = {row[1] for row in con.execute('PRAGMA table_info(event)').fetchall()}
    def sel_col(name: str, default: str = 'NULL') -> str:
        return name if name in event_cols else f'{default} AS {name}'
    where = []
    params: list[Any] = []
    if 'lifecycle_status' in event_cols:
        where.append("COALESCE(lifecycle_status,'active')='active'")
    if 'silent' in event_cols:
        where.append('COALESCE(silent,0)=0')
    if 'end_date' in event_cols:
        where.append('(COALESCE(end_date, date) >= ?)'); params.append(run_date)
    else:
        where.append('(date >= ?)'); params.append(run_date)
    where_sql = ' AND '.join(where) if where else '1=1'
    event_rows = con.execute(
        f"""
        SELECT id, title, date, time, {sel_col('end_date')}, location_name, city, {sel_col('event_type')}, {sel_col('ticket_status')},
               {sel_col('lifecycle_status', "'active'")}, {sel_col('telegraph_url')}, {sel_col('source_post_url')}, {sel_col('source_vk_post_url')},
               {sel_col('source_chat_id')}, {sel_col('source_message_id')}, {sel_col('added_at')}
        FROM event
        WHERE {where_sql}
        ORDER BY date ASC, time ASC, id ASC
        """,
        params,
    ).fetchall()
    event_ids = [int(r["id"]) for r in event_rows]
    source_rows: list[sqlite3.Row] = []
    if event_ids:
        q = ",".join("?" for _ in event_ids)
        try:
            source_rows = con.execute(
                f"""
                SELECT id,event_id,source_type,source_url,source_chat_username,source_chat_id,source_message_id,trust_level,imported_at
                FROM event_source WHERE event_id IN ({q}) ORDER BY event_id,id
                """,
                event_ids,
            ).fetchall()
        except sqlite3.Error:
            source_rows = []
    metric_url_comments: defaultdict[str, int] = defaultdict(int)
    vk_comments: dict[tuple[int, int], int] = {}
    try:
        for r in con.execute("""SELECT source_url, MAX(COALESCE(comments,0)) AS comments FROM telegram_post_metric WHERE source_url IS NOT NULL GROUP BY source_url"""):
            if r["source_url"]:
                key = str(r["source_url"]).rstrip("/")
                metric_url_comments[key] = max(metric_url_comments[key], int(r["comments"] or 0))
    except sqlite3.Error:
        pass
    try:
        for r in con.execute("""SELECT group_id, post_id, MAX(COALESCE(comments,0)) AS comments FROM vk_post_metric GROUP BY group_id, post_id"""):
            vk_comments[(int(r["group_id"]), int(r["post_id"]))] = int(r["comments"] or 0)
    except sqlite3.Error:
        pass

    events = {int(r["id"]): dict(r) for r in event_rows}
    sources: list[dict[str, Any]] = []
    for r in source_rows:
        url = (r["source_url"] or "").strip()
        parsed = parse_tg(url) or parse_vk(url)
        if not parsed and r["source_chat_username"] and r["source_message_id"]:
            username = str(r["source_chat_username"]).lstrip("@")
            mid = int(r["source_message_id"])
            parsed = {"platform": "telegram", "username": username, "message_id": mid, "platform_post_key": f"tg:{username}:{mid}"}
            url = f"https://t.me/{username}/{mid}"
        if not parsed:
            continue
        comments = metric_url_comments.get(url.rstrip("/"), 0) if parsed["platform"] == "telegram" else vk_comments.get((parsed.get("owner_id"), parsed.get("post_id")), 0)
        sources.append({"event_id": int(r["event_id"]), "event_source_id": int(r["id"]), "source_url": url, "source_type": r["source_type"], "trust_level": r["trust_level"], "parsed": parsed, "platform": parsed["platform"], "platform_post_key": parsed["platform_post_key"], "metric_comments": int(comments or 0), "source_kind": "event_source"})
    for r in event_rows:
        for field in ["source_post_url", "source_vk_post_url"]:
            url = (r[field] or "").strip()
            parsed = parse_tg(url) or parse_vk(url)
            if not parsed:
                continue
            comments = metric_url_comments.get(url.rstrip("/"), 0) if parsed["platform"] == "telegram" else vk_comments.get((parsed.get("owner_id"), parsed.get("post_id")), 0)
            sources.append({"event_id": int(r["id"]), "event_source_id": None, "source_url": url, "source_type": field, "trust_level": None, "parsed": parsed, "platform": parsed["platform"], "platform_post_key": parsed["platform_post_key"], "metric_comments": int(comments or 0), "source_kind": "event_legacy"})
        if r["source_chat_id"] and r["source_message_id"]:
            parsed = {"platform": "telegram", "chat_id": int(r["source_chat_id"]), "message_id": int(r["source_message_id"]), "platform_post_key": f"tgid:{r['source_chat_id']}:{r['source_message_id']}"}
            sources.append({"event_id": int(r["id"]), "event_source_id": None, "source_url": None, "source_type": "event_source_chat_id", "trust_level": None, "parsed": parsed, "platform": "telegram", "platform_post_key": parsed["platform_post_key"], "metric_comments": 0, "source_kind": "event_legacy_ids"})
    seen: set[tuple[Any, ...]] = set()
    uniq: list[dict[str, Any]] = []
    for src in sources:
        key = (src["event_id"], src.get("event_source_id"), src["platform_post_key"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(src)
    uniq.sort(key=lambda src: (-(src.get("metric_comments") or 0), events.get(src["event_id"], {}).get("date") or "", src["event_id"], src["platform_post_key"]))
    source_posts: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for src in uniq:
        source_posts[src["platform_post_key"]].append({"event_id": src["event_id"], "event_source_id": src.get("event_source_id"), "source_url": src.get("source_url"), "metric_comments": src.get("metric_comments")})
    return {
        "run_date": run_date,
        "quick_check": quick,
        "event_count": len(event_rows),
        "source_link_count": len(uniq),
        "source_post_count": len(source_posts),
        "platform_link_counts": dict(Counter(src["platform"] for src in uniq)),
        "events": {str(k): events[k] for k in events},
        "source_links": uniq,
        "source_posts": dict(source_posts),
        "notes": ["generated for Kaggle event_comment_feedback_discovery; source links uncapped"],
    }


def write_manifest(manifest: dict[str, Any], payload_dir: Path) -> None:
    raw = json.dumps(manifest, ensure_ascii=False, default=str).encode("utf-8")
    (payload_dir / "prod_source_manifest_full.json").write_bytes(raw)
    (payload_dir / "prod_source_manifest_full.json.gz").write_bytes(gzip.compress(raw, compresslevel=9))


def collect_secret_payload(bundle_env: str) -> dict[str, str]:
    names = [
        "TG_API_ID",
        "TG_API_HASH",
        "TELEGRAM_API_ID",
        "TELEGRAM_API_HASH",
        bundle_env,
        "VK_SERVICE_TOKEN",
        "VK_SERVICE_KEY",
        "VK_ACCESS_TOKEN",
    ]
    payload: dict[str, str] = {}
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip():
            payload[name] = str(value)
    if bundle_env not in payload:
        raise RuntimeError(f"{bundle_env} is required")
    if not (payload.get("TG_API_ID") or payload.get("TELEGRAM_API_ID")) or not (payload.get("TG_API_HASH") or payload.get("TELEGRAM_API_HASH")):
        raise RuntimeError("TG_API_ID/TG_API_HASH or TELEGRAM_API_ID/TELEGRAM_API_HASH are required")
    # Normalize preferred names for the kernel.
    if "TG_API_ID" not in payload and payload.get("TELEGRAM_API_ID"):
        payload["TG_API_ID"] = payload["TELEGRAM_API_ID"]
    if "TG_API_HASH" not in payload and payload.get("TELEGRAM_API_HASH"):
        payload["TG_API_HASH"] = payload["TELEGRAM_API_HASH"]
    return payload


def create_input_datasets(
    client: Any,
    *,
    env_user: str,
    run_id: str,
    tmp_root: Path,
    bundle_env: str,
    manifest: dict[str, Any],
    run_config: dict[str, Any],
    source_capability_cache: Path | None = None,
    previous_state_json: Path | None = None,
) -> list[str]:
    from cryptography.fernet import Fernet

    secret_payload = collect_secret_payload(bundle_env)
    key = Fernet.generate_key()
    enc = Fernet(key).encrypt(json.dumps(secret_payload, ensure_ascii=False).encode("utf-8"))
    refs: list[str] = []

    bundle = tmp_root / "bundle"
    bundle.mkdir(parents=True, exist_ok=True)
    payload_content = tmp_root / "payload-content"
    payload_content.mkdir(parents=True, exist_ok=True)
    write_manifest(manifest, payload_content)
    shutil.copy2(PHRASE_BANK, payload_content / "phrase-bank-v1.md")
    if PHRASE_BANK_JSON.exists():
        shutil.copy2(PHRASE_BANK_JSON, payload_content / "phrase-bank-v1.json")
    (payload_content / "run_config.json").write_text(json.dumps(run_config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if source_capability_cache and source_capability_cache.exists():
        shutil.copy2(source_capability_cache, payload_content / "source_capability_cache.json")
    if previous_state_json and previous_state_json.exists():
        shutil.copy2(previous_state_json, payload_content / "event_comment_feedback_state.json")
    archive_path = bundle / "event_comment_feedback_payload.tarball"
    with tarfile.open(archive_path, "w:gz") as tf:
        for item in sorted(payload_content.iterdir()):
            tf.add(item, arcname=item.name)
    (bundle / "secrets.enc").write_bytes(enc)
    bundle_ref = f"{env_user}/ecf-bundle-{run_id.lower()}"
    write_dataset_metadata(bundle, bundle_ref, f"event comment feedback bundle {run_id}")
    client.create_dataset(bundle, public=False, quiet=True, convert_to_csv=False, dir_mode="zip")
    refs.append(bundle_ref)

    key_dir = tmp_root / "key"
    key_dir.mkdir(parents=True, exist_ok=True)
    (key_dir / "fernet.key").write_bytes(key)
    key_ref = f"{env_user}/ecf-key-{run_id.lower()}"
    write_dataset_metadata(key_dir, key_ref, f"event comment feedback key {run_id}")
    client.create_dataset(key_dir, public=False, quiet=True, convert_to_csv=False, dir_mode="zip")
    refs.append(key_ref)

    print(
        "[event-comment-feedback] Kaggle input datasets submitted: "
        f"{refs}; secret_keys={sorted(secret_payload.keys())}; "
        f"events={manifest.get('event_count')} source_posts={manifest.get('source_post_count')} links={manifest.get('source_link_count')}",
        flush=True,
    )
    return refs

def cleanup_datasets(client: Any, refs: list[str]) -> None:
    for ref in refs:
        try:
            client.delete_dataset(ref, no_confirm=True)
            print(f"[event-comment-feedback] deleted temp dataset: {ref}", flush=True)
        except Exception as exc:
            print(f"[event-comment-feedback] failed to delete temp dataset {ref}: {exc}", flush=True)


def create_status_dataset_if_configured(args: argparse.Namespace, client: Any, *, env_user: str, run_id: str, kernel_ref: str, dataset_ref: str) -> str | None:
    status_db = (args.status_db or "").strip()
    callback_url = (args.status_callback_url or os.getenv("KAGGLE_STATUS_CALLBACK_URL") or "").strip()
    if not callback_url:
        webhook = (os.getenv("WEBHOOK_URL") or "").strip()
        if webhook:
            callback_url = webhook.rstrip("/") + "/internal/kaggle/run-event"
    if not status_db or not callback_url:
        print(
            "[event-comment-feedback] status dataset skipped: "
            f"status_db={'yes' if status_db else 'no'} callback_url={'yes' if callback_url else 'no'}",
            flush=True,
        )
        return None
    from db import Database
    from kaggle_status import create_kaggle_run_config, create_kaggle_status_dataset

    db = Database(status_db)
    config = asyncio.run(
        create_kaggle_run_config(
            db,
            run_id=f"event-comment-feedback:{run_id}",
            session_id=None,
            kind="event_comment_feedback_discovery",
            notebook="EventCommentFeedback",
            kernel_ref=kernel_ref,
            dataset_ref=dataset_ref,
            callback_url=callback_url,
            resource_leases=[f"telegram_session:env:{args.telegram_bundle_env}"],
        )
    )
    status_dataset = create_kaggle_status_dataset(
        client,
        username=env_user,
        slug_prefix="status-event-comment-feedback",
        run_id=run_id,
        config=config,
    )
    if status_dataset:
        wait_dataset_ready(client, status_dataset, expected_files=["kaggle_run.json", "kaggle_status_client.py"])
        print(f"[event-comment-feedback] status dataset ready: {status_dataset}", flush=True)
    return status_dataset


def stage_kernel(staging: Path) -> None:
    if not KERNEL_SRC.exists():
        raise FileNotFoundError(KERNEL_SRC)
    shutil.copytree(KERNEL_SRC, staging, dirs_exist_ok=True)
    status_client = PROJECT_ROOT / "kaggle" / "kaggle_status_client.py"
    if status_client.exists():
        shutil.copy2(status_client, staging / "kaggle_status_client.py")


def prepare_manifest_and_config(args: argparse.Namespace, *, run_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if args.manifest:
        manifest_path = Path(args.manifest).resolve()
        if manifest_path.suffix == ".gz":
            raw = gzip.decompress(manifest_path.read_bytes())
            manifest = json.loads(raw.decode("utf-8"))
        else:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        if not args.db:
            raise RuntimeError("Either --db or --manifest is required")
        manifest = build_manifest_from_sqlite(Path(args.db).resolve(), run_date=args.run_date)
    config = {
        "run_id": run_id,
        "schema_version": "event-comment-feedback-kaggle-config-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "models": DEFAULT_MODELS,
        "gate_model": DEFAULT_MODELS[0],
        "max_comments_per_source": args.max_comments_per_source,
        "request_sleep": args.request_sleep,
        "embedding_api_allowed": False,
        "llm_api_allowed_for_final_polish_only": True,
        "read_mode": "api_read_paced_v1",
        "state_mode": args.state_mode,
    }
    return manifest, config

def main() -> int:
    parser = argparse.ArgumentParser(description="Run Event Comment Feedback discovery on Kaggle CPU.")
    parser.add_argument("--env-file", default=str(PROJECT_ROOT / ".env"), help="Optional env file to load before reading credentials")
    parser.add_argument("--db", default="", help="SQLite database snapshot to build source manifest")
    parser.add_argument("--manifest", default="", help="Existing manifest JSON or JSON.GZ")
    parser.add_argument("--run-date", default=os.getenv("EVENT_COMMENT_FEEDBACK_RUN_DATE", datetime.now(timezone.utc).date().isoformat()))
    parser.add_argument("--run-id", default=os.getenv("EVENT_COMMENT_FEEDBACK_RUN_ID", ""))
    parser.add_argument("--telegram-bundle-env", default=os.getenv("EVENT_COMMENT_FEEDBACK_TELEGRAM_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_DISCOVERY"))
    parser.add_argument("--max-comments-per-source", type=int, default=int(os.getenv("EVENT_COMMENT_FEEDBACK_MAX_COMMENTS_PER_SOURCE", "300")))
    parser.add_argument("--request-sleep", type=float, default=float(os.getenv("EVENT_COMMENT_FEEDBACK_REQUEST_SLEEP", "0.45")))
    parser.add_argument("--timeout-minutes", type=int, default=int(os.getenv("EVENT_COMMENT_FEEDBACK_KAGGLE_TIMEOUT_MINUTES", "180")))
    parser.add_argument("--poll-interval", type=int, default=int(os.getenv("EVENT_COMMENT_FEEDBACK_KAGGLE_POLL_INTERVAL", "30")))
    parser.add_argument("--status-db", default=os.getenv("EVENT_COMMENT_FEEDBACK_STATUS_DB", ""))
    parser.add_argument("--status-callback-url", default=os.getenv("KAGGLE_STATUS_CALLBACK_URL", ""))
    parser.add_argument("--source-capability-cache", default=os.getenv("EVENT_COMMENT_FEEDBACK_SOURCE_CAPABILITY_CACHE", ""), help="Optional previous source_capability_cache.json for TTL skips")
    parser.add_argument("--previous-state-json", default=os.getenv("EVENT_COMMENT_FEEDBACK_PREVIOUS_STATE_JSON", ""), help="Optional previous event_comment_feedback_state.json")
    parser.add_argument("--state-mode", choices=["one_off_non_cumulative", "file_incremental"], default=os.getenv("EVENT_COMMENT_FEEDBACK_STATE_MODE", "one_off_non_cumulative"))
    parser.add_argument("--download-output", action="store_true", default=True)
    parser.add_argument("--no-wait", action="store_true")
    parser.add_argument("--keep-temp-datasets", action="store_true", default=(os.getenv("EVENT_COMMENT_FEEDBACK_KEEP_TEMP_DATASETS", "").lower() in {"1", "true", "yes", "on"}))
    parser.add_argument("--keep-staging", action="store_true")
    args = parser.parse_args()

    load_env_file(Path(args.env_file).resolve() if args.env_file else None)
    env_user = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not env_user:
        raise RuntimeError("KAGGLE_USERNAME is required")
    run_id = args.run_id or f"ecf-{utc_stamp()}"
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    client = SimpleKaggleClient()
    temp_dataset_refs: list[str] = []
    with tempfile.TemporaryDirectory(prefix=f"event-comment-feedback-{run_id}-") as tmp:
        tmp_root = Path(tmp)
        staging = tmp_root / "kernel"
        payload_dir = tmp_root / "payload"
        stage_kernel(staging)
        manifest, run_config = prepare_manifest_and_config(args, run_id=run_id)
        dataset_sources = create_input_datasets(
            client,
            env_user=env_user,
            run_id=run_id,
            tmp_root=tmp_root,
            bundle_env=args.telegram_bundle_env,
            manifest=manifest,
            run_config=run_config,
            source_capability_cache=Path(args.source_capability_cache).resolve() if args.source_capability_cache else None,
            previous_state_json=Path(args.previous_state_json).resolve() if args.previous_state_json else None,
        )
        temp_dataset_refs.extend(dataset_sources)
        kernel_ref = f"{env_user}/event-comment-feedback-discovery"
        status_ref = create_status_dataset_if_configured(args, client, env_user=env_user, run_id=run_id, kernel_ref=kernel_ref, dataset_ref=dataset_sources[0])
        if status_ref:
            dataset_sources.append(status_ref)
            temp_dataset_refs.append(status_ref)
        print("[event-comment-feedback] waiting 30s for Kaggle dataset propagation", flush=True)
        time.sleep(30)
        if args.keep_staging:
            keep = ARTIFACT_ROOT / f"staging-{run_id}"
            if keep.exists():
                shutil.rmtree(keep)
            shutil.copytree(tmp_root, keep)
            print(f"[event-comment-feedback] staging kept: {keep}", flush=True)
        try:
            client.push_kernel(kernel_path=staging, dataset_sources=dataset_sources)
            print(f"[event-comment-feedback] pushed {kernel_ref} run_id={run_id} datasets={dataset_sources}", flush=True)
            wait_kernel_dataset_sources(client, kernel_ref, dataset_sources)
            if args.no_wait:
                print(f"[event-comment-feedback] no-wait enabled; kernel_ref={kernel_ref} run_id={run_id}", flush=True)
                return 0
            deadline = time.monotonic() + max(60, args.timeout_minutes * 60)
            last_status: dict[str, Any] | None = None
            while time.monotonic() < deadline:
                time.sleep(max(5, args.poll_interval))
                status = client.get_kernel_status(kernel_ref)
                last_status = status
                raw = str(status.get("status") or "").upper()
                print(f"[event-comment-feedback] Kaggle status={raw} raw={status}", flush=True)
                if raw == "COMPLETE":
                    out_dir = ARTIFACT_ROOT / f"output-{run_id}"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    files = client.download_kernel_output(kernel_ref, path=out_dir, force=True)
                    (ARTIFACT_ROOT / "latest-output-dir.txt").write_text(str(out_dir) + "\n", encoding="utf-8")
                    (ARTIFACT_ROOT / "latest-run-id.txt").write_text(run_id + "\n", encoding="utf-8")
                    print(f"[event-comment-feedback] downloaded {len(files)} files to {out_dir}", flush=True)
                    return 0
                if raw in {"ERROR", "FAILED", "CANCELLED"}:
                    out_dir = ARTIFACT_ROOT / f"output-{run_id}-failed"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    try:
                        client.download_kernel_output(kernel_ref, path=out_dir, force=True)
                        print(f"[event-comment-feedback] downloaded failed output to {out_dir}", flush=True)
                    except Exception as exc:
                        print(f"[event-comment-feedback] failed output download failed: {exc}", flush=True)
                    raise RuntimeError(f"Kaggle failed: {status}")
            raise TimeoutError(f"Kaggle timeout; last_status={last_status}")
        finally:
            if temp_dataset_refs and not args.keep_temp_datasets and not args.no_wait:
                cleanup_datasets(client, temp_dataset_refs)


if __name__ == "__main__":
    raise SystemExit(main())
