#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
QWEN_MODULE_PATH = ROOT / "kaggle" / "RegionTalkQwen3Embedding06BEnrichment" / "region_talk_qwen3_embedding_06b_enrichment.py"

POSITIVE_PUBLICATION_STATUSES = {
    "llm_confirmed",
    "sent_to_chat",
    "gemini_accept",
    "approved",
    "ready_for_manual_review",
}
NEGATIVE_PUBLICATION_STATUSES = {
    "llm_rejected",
    "filtered_before_llm",
    "rejected",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_qwen_module() -> Any:
    spec = importlib.util.spec_from_file_location("region_talk_qwen3_embedding_06b_enrichment", QWEN_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {QWEN_MODULE_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def normalise_url(value: Any) -> str:
    raw = str(value or "").strip()
    raw = raw.split("?", 1)[0].rstrip("/")
    return raw.lower()


def row_join_key(row: dict[str, Any]) -> str:
    url = normalise_url(row.get("post_url"))
    if url:
        return "url:" + url
    post_id = str(row.get("post_id") or "").strip()
    text_hash = str(row.get("text_hash") or row.get("_embedding_text_hash") or "").strip()
    if post_id and text_hash:
        return f"post_text:{post_id}:{text_hash[:16]}"
    if post_id:
        return f"post:{post_id}"
    return "text:" + text_hash[:24]


def is_accept_status(value: Any) -> bool:
    return "accept" in str(value or "").lower() or str(value or "").lower() in {"accepted", "ok"}


def model_metrics(row: dict[str, Any], prefix: str) -> dict[str, Any]:
    pos = float(row.get(f"{prefix}_positive_score") or 0.0)
    neg = float(row.get(f"{prefix}_negative_score") or 0.0)
    margin = float(row.get(f"{prefix}_margin_positive_vs_negative") or (pos - neg))
    geo_margin = float(row.get(f"{prefix}_ko_vs_external_geo_margin") or 0.0)
    return {
        "positive_score": pos,
        "negative_score": neg,
        "positive_margin": margin,
        "geo_margin": geo_margin,
        "quality_axis": round(0.7 * margin + 0.3 * geo_margin, 6),
        "top_class": str(row.get(f"{prefix}_top_class") or ""),
        "ko_geo_top": str(row.get(f"{prefix}_ko_geo_top") or ""),
        "external_geo_top": str(row.get(f"{prefix}_external_geo_top") or ""),
        "accept": is_accept_status(row.get(f"vector_gate_status_{prefix}")),
    }


def status_from_row(row: dict[str, Any]) -> str:
    for field in (
        "publication_candidate_status",
        "publication_status",
        "llm_gate_status",
        "gemini_status",
        "candidate_status",
        "image_queue_status",
        "vector_gate_status",
    ):
        raw = str(row.get(field) or "").strip().lower()
        if raw:
            return raw
    return ""


def build_label_index(items_by_kind: dict[str, dict[str, dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    labels: dict[str, dict[str, Any]] = {}

    def set_label(row: dict[str, Any], label: str, source_kind: str, confidence: int) -> None:
        key = row_join_key(row)
        if not key or key == "text:":
            return
        existing = labels.get(key)
        if existing and int(existing.get("confidence") or 0) > confidence:
            return
        labels[key] = {
            "label": label,
            "label_source_kind": source_kind,
            "label_status": status_from_row(row),
            "confidence": confidence,
        }

    for kind, rows in items_by_kind.items():
        for row in rows.values():
            if not isinstance(row, dict):
                continue
            status = status_from_row(row)
            if kind == "publication_candidate_item":
                if status in POSITIVE_PUBLICATION_STATUSES or row.get("sent_to_chat") is True:
                    set_label(row, "positive_final", kind, 100)
                elif status in NEGATIVE_PUBLICATION_STATUSES:
                    set_label(row, "negative_final", kind, 90)
            elif kind == "image_queue_item":
                if status == "actual_scored" and float(row.get("image_overall_score") or row.get("overall_media_score") or 0.0) >= 0.65:
                    set_label(row, "positive_image_ready", kind, 70)
            elif kind == "candidate_memory_item":
                set_label(row, "positive_text_memory", kind, 50)
            elif kind in {"processed_post_item", "post_live_item"}:
                if "reject" in status or "external" in status or "ad" in status:
                    set_label(row, "negative_processed", kind, 40)
    return labels


def compare_rows(bge_rows: dict[str, dict[str, Any]], qwen_rows: dict[str, dict[str, Any]], labels: dict[str, dict[str, Any]], *, qwen_prefix: str = "qwen3_embedding_0_6b") -> tuple[list[dict[str, Any]], dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for key in sorted(set(bge_rows) & set(qwen_rows)):
        bge = bge_rows[key]
        qwen = qwen_rows[key]
        bge_m = model_metrics(bge, "bge_m3")
        qwen_m = model_metrics(qwen, qwen_prefix)
        label = labels.get(key, {"label": "unlabeled", "label_source_kind": "", "label_status": "", "confidence": 0})
        pairs.append({
            "join_key": key,
            "post_url": bge.get("post_url") or qwen.get("post_url") or "",
            "post_id": bge.get("post_id") or qwen.get("post_id") or "",
            "source_title": bge.get("source_title") or qwen.get("source_title") or "",
            "label": label.get("label"),
            "label_source_kind": label.get("label_source_kind"),
            "label_status": label.get("label_status"),
            "bge_accept": bge_m["accept"],
            "qwen_accept": qwen_m["accept"],
            "gate_agree": bge_m["accept"] == qwen_m["accept"],
            "top_class_agree": bge_m["top_class"] == qwen_m["top_class"],
            "bge_top_class": bge_m["top_class"],
            "qwen_top_class": qwen_m["top_class"],
            "bge_quality_axis": bge_m["quality_axis"],
            "qwen_quality_axis": qwen_m["quality_axis"],
            "quality_axis_delta_qwen_minus_bge": round(float(qwen_m["quality_axis"]) - float(bge_m["quality_axis"]), 6),
            "bge_positive_margin": bge_m["positive_margin"],
            "qwen_positive_margin": qwen_m["positive_margin"],
            "bge_geo_margin": bge_m["geo_margin"],
            "qwen_geo_margin": qwen_m["geo_margin"],
            "bge_ko_geo_top": bge_m["ko_geo_top"],
            "qwen_ko_geo_top": qwen_m["ko_geo_top"],
            "bge_external_geo_top": bge_m["external_geo_top"],
            "qwen_external_geo_top": qwen_m["external_geo_top"],
        })
    labeled = [r for r in pairs if r["label"] != "unlabeled"]
    positives = [r for r in labeled if str(r["label"]).startswith("positive")]
    negatives = [r for r in labeled if str(r["label"]).startswith("negative")]

    def avg(rows: list[dict[str, Any]], field: str) -> float | None:
        if not rows:
            return None
        return round(sum(float(r.get(field) or 0.0) for r in rows) / len(rows), 6)

    summary = {
        "generated_at": utc_now_iso(),
        "pairs": len(pairs),
        "bge_rows": len(bge_rows),
        "qwen_rows": len(qwen_rows),
        "labeled_pairs": len(labeled),
        "positive_pairs": len(positives),
        "negative_pairs": len(negatives),
        "gate_agreement_rate": round(sum(1 for r in pairs if r["gate_agree"]) / len(pairs), 6) if pairs else None,
        "top_class_agreement_rate": round(sum(1 for r in pairs if r["top_class_agree"]) / len(pairs), 6) if pairs else None,
        "avg_delta_qwen_minus_bge_all": avg(pairs, "quality_axis_delta_qwen_minus_bge"),
        "avg_delta_qwen_minus_bge_positive": avg(positives, "quality_axis_delta_qwen_minus_bge"),
        "avg_delta_qwen_minus_bge_negative": avg(negatives, "quality_axis_delta_qwen_minus_bge"),
        "preliminary_interpretation": "positive delta means Qwen scores the row stronger than BGE on the shared prototype axes; this is not production quality proof without enough Gemini/image-confirmed labels.",
    }
    return pairs, summary


def load_ydb_kinds(kinds: list[str], *, limit: int) -> dict[str, dict[str, dict[str, Any]]]:
    mod = load_qwen_module()
    ydb, driver, cfg = mod.ydb_connect()
    table_path = mod.ydb_kv_table_path(cfg)
    pool = ydb.SessionPool(driver)

    def op(session: Any) -> dict[str, dict[str, dict[str, Any]]]:
        mod.ensure_ydb_kv_table(ydb, session, table_path)
        return {kind: mod.ydb_select_kind_items(session, ydb, table_path, kind, limit=limit) for kind in kinds}

    try:
        return pool.retry_operation_sync(op)
    finally:
        driver.stop(timeout=5)


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare Region Talk BGE-M3 and Qwen3-Embedding-0.6B research rows from live YDB")
    ap.add_argument("--env-file", type=Path, default=ROOT / ".env")
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--output-dir", type=Path, default=ROOT / "artifacts" / "codex" / "region-talk-embedding-quality")
    ap.add_argument("--qwen-model-short", default="qwen3_embedding_0_6b")
    ap.add_argument("--qwen-kind", default="")
    args = ap.parse_args()
    load_dotenv(args.env_file)
    qwen_kind = args.qwen_kind or f"{args.qwen_model_short}_enrichment_item"
    kinds = [
        "text_vector_enrichment_item",
        qwen_kind,
        "publication_candidate_item",
        "candidate_memory_item",
        "image_queue_item",
        "processed_post_item",
        "post_live_item",
    ]
    items_by_kind = load_ydb_kinds(kinds, limit=args.limit)
    bge_rows = {
        row_join_key(row): row
        for row in items_by_kind.get("text_vector_enrichment_item", {}).values()
        if isinstance(row, dict) and str(row.get("model_short") or "") == "bge_m3"
    }
    qwen_rows = {
        row_join_key(row): row
        for row in items_by_kind.get(qwen_kind, {}).values()
        if isinstance(row, dict) and str(row.get("model_short") or args.qwen_model_short) == args.qwen_model_short
    }
    labels = build_label_index({kind: rows for kind, rows in items_by_kind.items() if kind not in {"text_vector_enrichment_item", qwen_kind}})
    pairs, summary = compare_rows(bge_rows, qwen_rows, labels, qwen_prefix=args.qwen_model_short)
    summary["qwen_model_short"] = args.qwen_model_short
    summary["qwen_kind"] = qwen_kind
    run_dir = args.output_dir / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "comparison.json").write_text(json.dumps({"summary": summary, "pairs": pairs}, ensure_ascii=False, indent=2), encoding="utf-8")
    with (run_dir / "comparison.csv").open("w", encoding="utf-8", newline="") as fh:
        fieldnames = list(pairs[0].keys()) if pairs else ["join_key", "post_url", "label"]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(pairs)
    print(json.dumps({"ok": True, "output_dir": str(run_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
