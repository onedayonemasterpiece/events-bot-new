#!/usr/bin/env python3
"""Build and verify the Region Talk publication shortlist from live YDB.

This is the bounded finalization side of CandidateReport: it consumes text/vector
rows and RegionTalkImageDiagnostic `actual_scored` rows from YDB, fetches public
Telegram text when compact YDB rows only contain summaries, ranks a lightweight
publication shortlist, calls Gemini Lite through the existing Supabase
`google_ai_reserve` limiter, writes `publication_candidate_item` rows back to
YDB, and exports a small XLSX for operator review.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests
try:
    from openpyxl import Workbook
except ModuleNotFoundError:  # keep live-YDB finalization usable in slim envs
    Workbook = None  # type: ignore[assignment]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT / "kaggle" / "RegionTalkCandidateReport") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "kaggle" / "RegionTalkCandidateReport"))
import region_talk_candidate_report as rt  # type: ignore  # noqa: E402


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip(); v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def telegram_public_text(url: str, *, timeout: int = 15) -> str:
    match = re.match(r"https?://t\.me/([^/]+)/([0-9]+)", url or "")
    if not match:
        return ""
    handle, post_id = match.group(1), match.group(2)
    try:
        page = requests.get(
            f"https://t.me/s/{handle}/{post_id}",
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        ).text
    except Exception:
        return ""
    marker = f'data-post="{handle}/{post_id}"'
    idx = page.find(marker)
    if idx < 0:
        return ""
    start = page.rfind('<div class="tgme_widget_message_wrap', 0, idx)
    end = page.find('<div class="tgme_widget_message_wrap', idx + 10)
    block = page[start : end if end > 0 else len(page)]
    text_match = re.search(r'<div class="tgme_widget_message_text js-message_text"[^>]*>(.*?)</div>', block, re.S)
    if not text_match:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text_match.group(1))
    text = re.sub(r"<.*?>", "", text)
    return re.sub(r"\n{3,}", "\n\n", html.unescape(text)).strip()


def source_class_guess(title: str) -> str:
    low = (title or "").lower()
    local_markers = [
        "калининград", "kenig", "кёниг", "39", "янтарн", "балтийск",
        "светлогорск", "зеленоградск", "музей", "афиша",
    ]
    return "local_region_source" if any(marker in low for marker in local_markers) else "nonlocal_travel_or_general_source"


def publication_pre_score(row: dict[str, Any]) -> float:
    nonlocal_bonus = 0.35 if row.get("source_class_guess") == "nonlocal_travel_or_general_source" else -0.2
    visual = float(row.get("overall_media_score") or 0)
    postcard = float(row.get("postcardness_score") or 0)
    candidate = float(row.get("candidate_score") or 0)
    vector = 0.12 if row.get("vector_gate_status") == "vector_accept_candidate" else 0.06
    return round(nonlocal_bonus + visual * 0.45 + postcard * 0.20 + candidate * 0.15 + vector, 4)


def read_live_rows(limit_images: int, limit_memory: int, *, reverify_existing: bool = False) -> tuple[Any, Any, Any, str, list[dict[str, Any]]]:
    ydb, driver, cfg = rt.ydb_connect()
    table = rt.ydb_kv_table_path(cfg)
    pool = ydb.SessionPool(driver)

    def op(session: Any) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        images = rt.ydb_select_kind_items(session, ydb, table, "image_queue_item", limit=limit_images)
        memory = rt.ydb_select_kind_items(session, ydb, table, "candidate_memory_item", limit=limit_memory)
        publications = rt.ydb_select_kind_items(session, ydb, table, "publication_candidate_item", limit=limit_images)
        return images, memory, publications

    images_by_pk, memory_by_pk, publications_by_pk = pool.retry_operation_sync(op)
    memory_by_url = {row.get("post_url"): row for row in memory_by_pk.values() if row.get("post_url")}
    publication_by_url: dict[str, dict[str, Any]] = {}
    for row in publications_by_pk.values():
        url = str(row.get("post_url") or "")
        if not url:
            continue
        prev = publication_by_url.get(url)
        if prev is None or str(row.get("updated_at") or "") >= str(prev.get("updated_at") or ""):
            publication_by_url[url] = row
    rows: list[dict[str, Any]] = []
    for image in images_by_pk.values():
        if image.get("image_queue_status") != "actual_scored" or image.get("image_model_input_type") != "actual_image":
            continue
        post_url = image.get("post_url")
        memory = memory_by_url.get(post_url, {})
        publication = publication_by_url.get(str(post_url or ""), {})
        row = {**memory, **image}
        if publication and not reverify_existing:
            for key in [
                "publication_status", "publication_candidate_status", "llm_gate_status", "llm_decision", "llm_reason",
                "llm_model", "content_type", "visit_evidence_type",
            ]:
                if publication.get(key) not in (None, ""):
                    row[key] = publication.get(key)
            row["existing_publication_candidate"] = "true"
        row["source_class_guess"] = source_class_guess(str(row.get("source_title") or ""))
        row["short_summary"] = memory.get("short_summary") or image.get("short_summary") or ""
        row["text"] = memory.get("text") or memory.get("text_excerpt") or ""
        row["publication_pre_score"] = publication_pre_score(row)
        rows.append(row)
    rows.sort(key=lambda r: (-float(r.get("publication_pre_score") or 0), r.get("source_class_guess") != "nonlocal_travel_or_general_source"))
    return ydb, driver, pool, table, rows


def verify_rows(rows: list[dict[str, Any]], *, max_llm: int, model: str, default_env_var_name: str) -> list[dict[str, Any]]:
    candidates = [
        row for row in rows
        if row.get("source_class_guess") == "nonlocal_travel_or_general_source"
        and not str(row.get("publication_status") or "").startswith("gemini_")
    ][:max_llm]
    results: list[dict[str, Any]] = []
    for idx, row in enumerate(candidates, start=1):
        if not row.get("text"):
            row["text"] = telegram_public_text(str(row.get("post_url") or ""))
        if not row.get("text") and row.get("short_summary"):
            row["text"] = "summary: " + str(row.get("short_summary") or "")
        if not row.get("text"):
            row["publication_status"] = "no_text_for_gemini"
            row["publication_candidate_status"] = "filtered_before_llm"
            results.append(row)
            continue
        evidence = {
            "stage": "final_publication_verifier",
            "overall_media_score": row.get("overall_media_score"),
            "postcardness_score": row.get("postcardness_score"),
            "aesthetic_score": row.get("aesthetic_score"),
            "image_model_input_type": row.get("image_model_input_type"),
            "image_queue_status": row.get("image_queue_status"),
            "vector_gate_status": row.get("vector_gate_status"),
            "source_geo_class": row.get("source_class_guess"),
            "source_topic_class": row.get("source_topic_class") or "travel/general",
            "publication_text_story_score": row.get("candidate_score"),
        }
        print(
            f"[region-talk-finalizer] Gemini {idx}/{len(candidates)} {row.get('post_url')} "
            f"source={row.get('source_title')} pre_score={row.get('publication_pre_score')}",
            flush=True,
        )
        verdict = rt.call_region_talk_semantic_llm(
            row,
            evidence,
            model=model,
            default_env_var_name=default_env_var_name,
        )
        row.update(verdict)
        row["publication_rank"] = idx
        if verdict.get("llm_gate_status") == "ok" and verdict.get("llm_decision") == "accept":
            row["publication_status"] = "gemini_accept"
            row["publication_candidate_status"] = "llm_confirmed"
        else:
            row["publication_status"] = "gemini_" + str(verdict.get("llm_decision") or verdict.get("llm_gate_status") or "unknown")
            row["publication_candidate_status"] = (
                "llm_rejected" if verdict.get("llm_decision") == "reject"
                else "llm_needs_review" if verdict.get("llm_gate_status") == "ok"
                else "llm_budget_deferred" if verdict.get("llm_gate_status") == "rate_limited"
                else "llm_error"
            )
        results.append(row)
    return results


def write_publication_rows(pool: Any, ydb: Any, table: str, rows: list[dict[str, Any]], run_id: str) -> int:
    now = rt.utc_now_iso()
    fields = [
        "run_id", "updated_at", "last_seen_run_id", "post_url", "source_title", "source_url", "post_date",
        "publication_rank", "publication_pre_score", "publication_status", "publication_candidate_status", "overall_media_score", "postcardness_score",
        "aesthetic_score", "technical_quality_score", "publication_safety_score", "image_queue_status", "image_model_input_type",
        "vector_gate_status", "candidate_score", "source_class_guess", "short_summary", "text", "llm_gate_status",
        "llm_decision", "llm_reason", "llm_model", "llm_limit_source", "content_type", "visit_evidence_type",
        "has_firsthand_visit_evidence", "emotion_or_impression_evidence", "review_or_opinion_evidence",
        "memorable_detail_evidence", "original_photo_evidence", "whole_post_about_kaliningrad_oblast_score",
        "kaliningrad_mention_role", "llm_usage_input_tokens", "llm_usage_output_tokens", "llm_usage_total_tokens",
    ]
    items = []
    for row in rows:
        payload = rt.compact_record({**row, "run_id": run_id, "updated_at": now, "last_seen_run_id": run_id}, fields, max_len=1800)
        key = payload.get("post_url") or payload.get("image_queue_id") or str(row.get("publication_rank"))
        if key:
            items.append(("publication_candidate_item:" + str(key).replace("publication_candidate_item:", ""), "publication_candidate_item", payload))
    if not items:
        return 0

    def op(session: Any) -> int:
        rt.ensure_ydb_kv_table(ydb, session, table)
        return rt.ydb_upsert_json_many(session, ydb, table, items, now, chunk_size=20, timeout_seconds=8)

    return int(pool.retry_operation_sync(op) or 0)


def write_xlsx(path: Path, verified: list[dict[str, Any]], all_rows: list[dict[str, Any]], include_unverified: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "publication_rank", "publication_status", "llm_decision", "publication_pre_score", "post_url", "source_title",
        "source_class_guess", "overall_media_score", "postcardness_score", "aesthetic_score", "candidate_score",
        "vector_gate_status", "content_type", "visit_evidence_type", "llm_reason", "short_summary", "text",
    ]
    verified_ids = {row.get("post_url") for row in verified}
    export_rows = verified + [row for row in all_rows if row.get("post_url") not in verified_ids][:include_unverified]
    if Workbook is None:
        csv_path = path.with_suffix(".csv")
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            writer.writeheader()
            for row in export_rows:
                writer.writerow({col: row.get(col, "") for col in cols})
        return csv_path
    wb = Workbook()
    ws = wb.active
    ws.title = "publication_shortlist"
    ws.append(cols)
    for row in export_rows:
        ws.append([row.get(col, "") for col in cols])
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = min(70, max(12, max(len(str(cell.value or "")) for cell in col[:30]) + 2))
    wb.save(path)
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--max-llm", type=int, default=10)
    parser.add_argument("--limit-images", type=int, default=5000)
    parser.add_argument("--limit-memory", type=int, default=8000)
    parser.add_argument("--model", default=os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.1-flash-lite")
    parser.add_argument("--default-env-var-name", default=os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "artifacts" / "codex" / "region-talk-finalizer")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reverify-existing", action="store_true", help="Ignore existing publication_candidate_item verifier statuses and call Gemini again.")
    args = parser.parse_args()
    load_env(args.env_file)
    os.environ.setdefault("REGION_TALK_LLM_MODEL", args.model)
    os.environ.setdefault("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME", args.default_env_var_name)
    os.environ.setdefault("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "45")
    os.environ.setdefault("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", os.environ.get("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "45"))
    os.environ.setdefault("REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS", "2200")
    run_id = args.run_id or "region-talk-finalizer-local-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.output_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    ydb, driver, pool, table, rows = read_live_rows(args.limit_images, args.limit_memory, reverify_existing=args.reverify_existing)
    verified = [] if args.dry_run else verify_rows(rows, max_llm=args.max_llm, model=args.model, default_env_var_name=args.default_env_var_name)
    written = 0 if args.dry_run else write_publication_rows(pool, ydb, table, verified, run_id)
    shortlist_artifact = write_xlsx(out_dir / "region-talk-publication-shortlist.xlsx", verified, rows, include_unverified=50)
    payload = {
        "run_id": run_id,
        "actual_scored_rows": len(rows),
        "llm_calls": len([row for row in verified if row.get("llm_gate_status")]),
        "accepted_new": sum(1 for row in verified if row.get("publication_status") == "gemini_accept"),
        "accepted_total": sum(1 for row in rows if row.get("publication_status") == "gemini_accept" or row.get("publication_candidate_status") == "llm_confirmed"),
        "written": written,
        "shortlist_artifact": str(shortlist_artifact),
        "xlsx": str(shortlist_artifact) if shortlist_artifact.suffix == ".xlsx" else "",
        "verified": verified,
        "top_actual": rows[:50],
    }
    (out_dir / "publication_finalizer_results.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: payload[k] for k in ["run_id", "actual_scored_rows", "llm_calls", "accepted_new", "accepted_total", "written", "shortlist_artifact"]}, ensure_ascii=False, indent=2))
    try:
        driver.stop(timeout=5)
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
