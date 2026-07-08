#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import base64
import html
import importlib.util
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
QWEN_MODULE_PATH = ROOT / "kaggle" / "RegionTalkQwen3Embedding06BEnrichment" / "region_talk_qwen3_embedding_06b_enrichment.py"
INPUT_KINDS = ["publication_candidate_item", "image_queue_item", "candidate_memory_item", "processed_post_item", "post_live_item"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#") or "=" not in raw:
            continue
        k, v = raw.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def load_worker_module() -> Any:
    spec = importlib.util.spec_from_file_location("region_talk_qwen3_embedding_06b_enrichment", QWEN_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {QWEN_MODULE_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod




def stable_text_hash(text: str) -> str:
    import hashlib
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def write_fulltext_rows_to_ydb(rows: list[dict[str, Any]], *, dataset_id: str, min_chars: int) -> dict[str, Any]:
    mod = load_worker_module()
    ydb, driver, cfg = mod.ydb_connect()
    table_path = mod.ydb_kv_table_path(cfg)
    pool = ydb.SessionPool(driver)
    now = utc_now_iso()
    ydb_rows: list[tuple[str, str, dict[str, Any]]] = []
    for idx, row in enumerate(rows, start=1):
        text = str(row.get("full_text") or "").strip()
        if len(text) < min_chars:
            continue
        post_id = str(row.get("post_id") or "").strip() or mod.stable_hash(row.get("post_url"), length=16)
        text_hash = stable_text_hash(text)
        payload = {
            "dataset_id": dataset_id,
            "row_index": idx,
            "post_id": post_id,
            "post_url": row.get("post_url") or "",
            "source_id": row.get("source_id") or "",
            "source_title": row.get("source_title") or "",
            "source_url": row.get("source_url") or "",
            "post_date": row.get("post_date") or "",
            "source_kind_original": row.get("source_kind") or "",
            "text": text,
            "full_text": text,
            "text_hash": text_hash,
            "text_len": len(text),
            "fetch_method": row.get("fetch_method") or "",
            "fetched_at": row.get("fetched_at") or now,
            "old_compact_text": row.get("old_compact_text") or "",
            "old_compact_fields": row.get("old_compact_fields") or [],
            "created_at": now,
        }
        pk = f"fulltext_validation_item:{dataset_id}:{post_id}:{text_hash[:12]}"
        ydb_rows.append((pk, "fulltext_validation_item", payload))

    def op(session: Any) -> int:
        mod.ensure_ydb_kv_table(ydb, session, table_path)
        return mod.ydb_upsert_json_many(session, ydb, table_path, ydb_rows, now, chunk_size=25)

    try:
        written = pool.retry_operation_sync(op)
    finally:
        driver.stop(timeout=5)
    return {"kind": "fulltext_validation_item", "dataset_id": dataset_id, "rows_prepared": len(ydb_rows), "rows_written": written, "table_path": table_path}


def decode_local_e2e_bundle() -> dict[str, Any]:
    api_id = int((os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID") or "0").strip() or 0)
    api_hash = (os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH") or "").strip()
    raw = (os.getenv("TELEGRAM_AUTH_BUNDLE_E2E") or "").strip()
    if not api_id or not api_hash or not raw:
        raise RuntimeError("TELEGRAM_AUTH_BUNDLE_E2E plus TG/TELEGRAM API id/hash are required for local full-text refetch")
    bundle = json.loads(base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8"))
    session = str(bundle.get("session") or "").strip()
    if not session:
        raise RuntimeError("TELEGRAM_AUTH_BUNDLE_E2E has no session")
    device = {k: bundle[k] for k in ["device_model", "system_version", "app_version", "lang_code", "system_lang_code"] if bundle.get(k)}
    return {"api_id": api_id, "api_hash": api_hash, "session": session, "device": device}


def parse_tg_url(url: str) -> tuple[str, int] | None:
    m = re.search(r"https?://t\.me/(?:s/)?([^/?#]+)/(\d+)", str(url or ""))
    if not m:
        return None
    handle = m.group(1)
    if handle == "c":
        return None
    return handle, int(m.group(2))


def compact(value: Any, limit: int = 1200) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def row_key(row: dict[str, Any]) -> str:
    url = str(row.get("post_url") or "").split("?", 1)[0].rstrip("/").lower()
    if url:
        return "url:" + url
    return "post:" + str(row.get("post_id") or row.get("candidate_memory_id") or "")


def load_ydb_candidates(limit: int, ydb_limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    mod = load_worker_module()
    ydb, driver, cfg = mod.ydb_connect()
    table_path = mod.ydb_kv_table_path(cfg)
    pool = ydb.SessionPool(driver)

    def op(session: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        existing_fulltext = mod.ydb_select_kind_items(session, ydb, table_path, "fulltext_validation_item", limit=ydb_limit)
        existing_urls = {str(r.get("post_url") or "").split("?", 1)[0].rstrip("/").lower() for r in existing_fulltext.values() if isinstance(r, dict)}
        rows_by_kind: dict[str, list[dict[str, Any]]] = {}
        counts: dict[str, int] = {"existing_fulltext_validation_item": len(existing_fulltext)}
        for kind in INPUT_KINDS:
            items = mod.ydb_select_kind_items(session, ydb, table_path, kind, limit=ydb_limit)
            counts[kind] = len(items)
            rows_by_kind[kind] = []
            for row in items.values():
                if not isinstance(row, dict):
                    continue
                url = str(row.get("post_url") or "").strip()
                if not parse_tg_url(url):
                    continue
                if url.split("?", 1)[0].rstrip("/").lower() in existing_urls:
                    continue
                rr = dict(row)
                rr["_source_kind"] = kind
                rr["_old_compact_text"], rr["_old_compact_fields"] = mod.text_from_row(rr)
                rows_by_kind[kind].append(rr)
        selected: dict[str, dict[str, Any]] = {}
        quotas = {
            "publication_candidate_item": 30,
            "image_queue_item": 60,
            "candidate_memory_item": 120,
            "processed_post_item": 120,
            "post_live_item": 120,
        }
        for kind in INPUT_KINDS:
            bucket = rows_by_kind[kind]
            # Keep deterministic order, but prefer rows with previous vector signals for labeled comparisons.
            bucket.sort(key=lambda r: (0 if r.get("vector_gate_status") else 1, str(r.get("post_date") or ""), row_key(r)))
            for row in bucket:
                if len([1 for x in selected.values() if x.get("_source_kind") == kind]) >= quotas[kind]:
                    break
                selected.setdefault(row_key(row), row)
                if len(selected) >= limit:
                    break
            if len(selected) >= limit:
                break
        if len(selected) < limit:
            for kind in INPUT_KINDS:
                for row in rows_by_kind[kind]:
                    selected.setdefault(row_key(row), row)
                    if len(selected) >= limit:
                        break
                if len(selected) >= limit:
                    break
        return list(selected.values())[:limit], {"table_path": table_path, "loaded_counts": counts}

    try:
        return pool.retry_operation_sync(op)
    finally:
        driver.stop(timeout=5)



async def fetch_texts(args: argparse.Namespace, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from telethon import TelegramClient, errors
    from telethon.sessions import StringSession

    auth = decode_local_e2e_bundle()
    client = TelegramClient(StringSession(auth["session"]), auth["api_id"], auth["api_hash"], flood_sleep_threshold=60, **auth["device"])
    await client.connect()
    out: list[dict[str, Any]] = []
    entity_cache: dict[str, Any] = {}
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("TELEGRAM_AUTH_BUNDLE_E2E is not authorized")
        total = len(candidates)
        for idx, row in enumerate(candidates, start=1):
            url = str(row.get("post_url") or "")
            parsed = parse_tg_url(url)
            full_text = ""
            method = ""
            error = ""
            if parsed:
                handle, mid = parsed
                try:
                    if handle not in entity_cache:
                        await asyncio.sleep(random.uniform(args.resolve_delay_min, args.resolve_delay_max))
                        entity_cache[handle] = await client.get_entity(handle)
                    await asyncio.sleep(random.uniform(args.message_delay_min, args.message_delay_max))
                    msg = await client.get_messages(entity_cache[handle], ids=mid)
                    full_text = str(getattr(msg, "message", None) or getattr(msg, "text", None) or "").strip() if msg else ""
                    method = "telethon_get_messages"
                except errors.FloodWaitError as exc:
                    error = f"FloodWait:{getattr(exc, 'seconds', '')}"
                    if int(getattr(exc, "seconds", 0) or 0) <= args.floodwait_sleep_max:
                        await asyncio.sleep(int(getattr(exc, "seconds", 0) or 0) + 1)
                    else:
                        method = "telethon_floodwait_skipped"
                except Exception as exc:
                    error = f"{type(exc).__name__}:{str(exc)[:180]}"
            rec = {
                "post_url": url,
                "post_id": row.get("post_id") or row.get("candidate_memory_id") or "",
                "source_id": row.get("source_id") or "",
                "source_title": row.get("source_title") or "",
                "source_url": row.get("source_url") or "",
                "post_date": row.get("post_date") or "",
                "source_kind": row.get("_source_kind") or "",
                "old_compact_text": row.get("_old_compact_text") or "",
                "old_compact_fields": row.get("_old_compact_fields") or [],
                "full_text": full_text,
                "full_text_len": len(full_text),
                "fetch_method": method or "failed",
                "fetch_error": error,
                "fetched_at": utc_now_iso(),
            }
            out.append(rec)
            if idx % 25 == 0 or idx == total:
                ok = sum(1 for r in out if r["full_text_len"] >= args.min_full_text_chars)
                print(json.dumps({"progress": idx, "total": total, "ok_min_chars": ok, "last_method": rec["fetch_method"]}, ensure_ascii=False), flush=True)
    finally:
        await client.disconnect()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch full Telegram post texts for existing Region Talk YDB post URLs")
    ap.add_argument("--env-file", type=Path, default=ROOT / ".env")
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--ydb-limit", type=int, default=20000)
    ap.add_argument("--output-dir", type=Path, default=ROOT / "artifacts" / "codex" / "region-talk-fulltext-embedding-validation")
    ap.add_argument("--min-full-text-chars", type=int, default=40)
    ap.add_argument("--resolve-delay-min", type=float, default=2.0)
    ap.add_argument("--resolve-delay-max", type=float, default=5.0)
    ap.add_argument("--message-delay-min", type=float, default=0.8)
    ap.add_argument("--message-delay-max", type=float, default=2.0)
    ap.add_argument("--floodwait-sleep-max", type=int, default=60)
    ap.add_argument("--write-ydb-kind", action="store_true", help="Write fetched full texts to YDB kind fulltext_validation_item")
    args = ap.parse_args()
    load_dotenv(args.env_file)
    candidates, meta = load_ydb_candidates(args.limit, args.ydb_limit)
    run_dir = args.output_dir / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "input_candidates.json").write_text(json.dumps({"meta": meta, "count": len(candidates), "rows": candidates}, ensure_ascii=False, indent=2), encoding="utf-8")
    rows = asyncio.run(fetch_texts(args, candidates))
    good = [r for r in rows if int(r.get("full_text_len") or 0) >= args.min_full_text_chars]
    jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in good) + ("\n" if good else "")
    (run_dir / "posts_fulltext.jsonl").write_text(jsonl, encoding="utf-8")
    (run_dir / "fetch_all_rows.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")
    dataset_id = run_dir.name
    ydb_write = write_fulltext_rows_to_ydb(good, dataset_id=dataset_id, min_chars=args.min_full_text_chars) if args.write_ydb_kind else {"skipped": True}
    summary = {
        "generated_at": utc_now_iso(),
        "dataset_id": dataset_id,
        "candidate_count": len(candidates),
        "fetched_count": len(rows),
        "good_fulltext_count": len(good),
        "min_full_text_chars": args.min_full_text_chars,
        "method_counts": {},
        "output_jsonl": str(run_dir / "posts_fulltext.jsonl"),
        "ydb_write": ydb_write,
    }
    for r in rows:
        summary["method_counts"][r["fetch_method"]] = int(summary["method_counts"].get(r["fetch_method"], 0)) + 1
    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(run_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
