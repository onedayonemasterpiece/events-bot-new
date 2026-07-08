#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
QWEN_MODULE_PATH = ROOT / "kaggle" / "RegionTalkQwen3Embedding06BEnrichment" / "region_talk_qwen3_embedding_06b_enrichment.py"

INPUT_KINDS = [
    "publication_candidate_item",
    "candidate_memory_item",
    "image_queue_item",
    "processed_post_item",
    "post_live_item",
]
MODEL_KINDS = {
    "e5_multilingual_base": "e5_multilingual_base_enrichment_item",
    "e5_fulltext_multilingual_base": "e5_fulltext_multilingual_base_enrichment_item",
    "bge_m3": "text_vector_enrichment_item",
    "embeddinggemma_300m": "embeddinggemma_300m_enrichment_item",
    "qwen3_embedding_0_6b": "qwen3_embedding_0_6b_enrichment_item",
}
POSITIVE_CLASSES = {"ko_visit_impression", "ko_route_useful", "ko_visual_place_card"}
ANTI_REGION_CLASSES = {"other_region_travel", "multi_region_roundup"}
EVENT_CLASSES = {"news_report", "event_announcement"}
AD_CLASSES = {"ad_or_promo"}
POSITIVE_PUBLICATION_STATUSES = {"llm_confirmed", "sent_to_chat", "gemini_accept", "approved", "ready_for_manual_review"}
NEGATIVE_PUBLICATION_STATUSES = {"llm_rejected", "filtered_before_llm", "rejected"}


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


def load_worker_module() -> Any:
    spec = importlib.util.spec_from_file_location("region_talk_qwen3_embedding_06b_enrichment", QWEN_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {QWEN_MODULE_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def normalise_url(value: Any) -> str:
    raw = str(value or "").strip().split("?", 1)[0].rstrip("/")
    return raw.lower()


def row_join_key(row: dict[str, Any]) -> str:
    url = normalise_url(row.get("post_url"))
    if url:
        return "url:" + url
    post_id = str(row.get("post_id") or row.get("candidate_memory_id") or row.get("publication_candidate_id") or "").strip()
    text_hash = str(row.get("text_hash") or row.get("_embedding_text_hash") or "").strip()
    if post_id and text_hash:
        return f"post_text:{post_id}:{text_hash[:16]}"
    if post_id:
        return f"post:{post_id}"
    return "text:" + text_hash[:24]


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y", "да"}


def parse_scores(value: Any) -> dict[str, float]:
    if isinstance(value, dict):
        return {str(k): as_float(v) for k, v in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            data = json.loads(value)
            if isinstance(data, dict):
                return {str(k): as_float(v) for k, v in data.items()}
        except Exception:
            return {}
    return {}


def gate_from_accept_review(accept: bool, margin: float) -> str:
    if accept:
        return "accept"
    if margin >= -0.05:
        return "review"
    return "reject"


def legacy_e5_metrics(row: dict[str, Any]) -> dict[str, Any]:
    status = str(row.get("vector_gate_status") or "")
    negative_class = str(row.get("vector_negative_class") or "")
    accept = status == "vector_accept_candidate"
    review = status == "vector_ambiguous_keep_for_ranking"
    positive = as_float(row.get("vector_positive_score"), as_float(row.get("whole_post_about_kaliningrad_oblast_score"), 0.0))
    negative = as_float(row.get("vector_negative_score"), 0.0)
    anti = max(as_float(row.get("vector_other_region_score")), negative if negative_class in ANTI_REGION_CLASSES else 0.0)
    ad = max(as_float(row.get("vector_ad_promo_score")), 0.75 if as_bool(row.get("is_ad_or_promo")) else 0.0, negative if negative_class == "ad_or_promo" else 0.0)
    event = max(as_float(row.get("vector_news_event_score")), negative if negative_class in EVENT_CLASSES else 0.0)
    travel = max(as_float(row.get("vector_visit_impression_score")), 0.55 if str(row.get("vector_content_type") or "") == "visit_impression_candidate" else 0.0)
    useful = max(as_float(row.get("vector_route_useful_score")), as_float(row.get("publication_story_score")), 0.55 if str(row.get("vector_content_type") or "") == "route_useful_candidate" else 0.0)
    visual = max(0.55 if str(row.get("vector_content_type") or "") == "single_location_photo_card" else 0.0, as_float(row.get("postcardness_score")), as_float(row.get("overall_media_score")))
    return {
        "gate": "accept" if accept else "review" if review else "reject" if status.startswith("vector_reject") else "unknown",
        "accept": accept,
        "review": review,
        "ko_region_score": positive,
        "anti_region_score": anti,
        "ad_promo_score": ad,
        "event_news_service_score": event,
        "travel_emotion_review_score": travel,
        "useful_story_score": useful,
        "visual_postcard_text_intent_score": visual,
        "margin": as_float(row.get("vector_margin_positive_vs_negative"), positive - negative),
        "top_external_geo_class": negative_class if negative_class in ANTI_REGION_CLASSES else "",
    }


def enrichment_metrics(row: dict[str, Any] | None, prefix: str) -> dict[str, Any]:
    if not row:
        return {"present": False, "accept": False, "review": False, "gate": "missing"}
    semantic = parse_scores(row.get("semantic_scores_by_class"))
    pos = max([semantic.get(k, 0.0) for k in POSITIVE_CLASSES] + [as_float(row.get(f"{prefix}_positive_score"))])
    neg = max([v for k, v in semantic.items() if k not in POSITIVE_CLASSES] + [as_float(row.get(f"{prefix}_negative_score"))])
    anti = max([semantic.get(k, 0.0) for k in ANTI_REGION_CLASSES] + [as_float(row.get(f"{prefix}_external_geo_score"))])
    ko_geo = as_float(row.get(f"{prefix}_ko_geo_score"))
    external_geo = as_float(row.get(f"{prefix}_external_geo_score"))
    margin = as_float(row.get(f"{prefix}_margin_positive_vs_negative"), pos - neg)
    geo_margin = as_float(row.get(f"{prefix}_ko_vs_external_geo_margin"), ko_geo - external_geo)
    accept = "accept" in str(row.get(f"vector_gate_status_{prefix}") or "").lower()
    gate = gate_from_accept_review(accept, margin)
    return {
        "present": True,
        "gate": gate,
        "accept": accept,
        "review": gate == "review",
        "ko_region_score": max(pos, ko_geo),
        "anti_region_score": anti,
        "ad_promo_score": max([semantic.get(k, 0.0) for k in AD_CLASSES] + [0.0]),
        "event_news_service_score": max([semantic.get(k, 0.0) for k in EVENT_CLASSES] + [0.0]),
        "travel_emotion_review_score": semantic.get("ko_visit_impression", 0.0),
        "useful_story_score": semantic.get("ko_route_useful", 0.0),
        "visual_postcard_text_intent_score": semantic.get("ko_visual_place_card", 0.0),
        "margin": margin,
        "ko_external_margin": geo_margin,
        "top_class": str(row.get(f"{prefix}_top_class") or ""),
        "top_external_geo_class": str(row.get(f"{prefix}_external_geo_top") or ""),
        "text_excerpt": str(row.get("text_excerpt") or "")[:500],
    }


def status_from_row(row: dict[str, Any]) -> str:
    for field in ("publication_candidate_status", "publication_status", "llm_gate_status", "gemini_status", "candidate_status", "image_queue_status", "vector_gate_status"):
        raw = str(row.get(field) or "").strip().lower()
        if raw:
            return raw
    return ""


def label_for_row(row: dict[str, Any]) -> tuple[str, int, str]:
    kind = str(row.get("_ydb_kind") or row.get("_source_kind") or "")
    status = status_from_row(row)
    if kind == "publication_candidate_item":
        if status in POSITIVE_PUBLICATION_STATUSES or as_bool(row.get("sent_to_chat")):
            return "positive_final", 100, status
        if status in NEGATIVE_PUBLICATION_STATUSES:
            return "negative_final", 90, status
    if kind == "image_queue_item" and status == "actual_scored" and as_float(row.get("overall_media_score")) >= 0.65:
        return "positive_image_ready", 70, status
    if kind == "candidate_memory_item":
        if status.startswith("vector_reject"):
            return "negative_text_memory", 45, status
        return "positive_text_memory", 50, status
    if kind in {"processed_post_item", "post_live_item"} and ("reject" in status or "external" in status or "ad" in status):
        return "negative_processed", 40, status
    return "unlabeled", 0, status


def load_ydb_items(kinds: list[str], limit: int) -> dict[str, dict[str, dict[str, Any]]]:
    mod = load_worker_module()
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


def build_base_rows(items_by_kind: dict[str, dict[str, dict[str, Any]]], sample_limit: int) -> dict[str, dict[str, Any]]:
    priority = {kind: idx for idx, kind in enumerate(INPUT_KINDS)}
    mod = load_worker_module()
    records: list[tuple[int, str, str, dict[str, Any]]] = []
    for kind in INPUT_KINDS:
        for row in items_by_kind.get(kind, {}).values():
            if not isinstance(row, dict):
                continue
            rr = dict(row)
            rr["_source_kind"] = kind
            text, used = mod.text_from_row(rr)
            if len(text) < 24:
                continue
            rr["_embedding_text"] = text
            rr["_embedding_text_fields"] = used
            key = row_join_key(rr)
            records.append((priority.get(kind, 9), str(rr.get("post_date") or ""), key, rr))
    records.sort(key=lambda item: (item[0], item[1], item[2]))
    out: dict[str, dict[str, Any]] = {}
    for _prio, _date, key, row in records:
        out.setdefault(key, row)
        if len(out) >= sample_limit:
            break
    return out


def index_model_rows(rows: dict[str, dict[str, Any]], model_short: str | None = None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows.values():
        if not isinstance(row, dict):
            continue
        if model_short and str(row.get("model_short") or "") != model_short:
            continue
        out[row_join_key(row)] = row
    return out


def write_xlsx(path: Path, rows: list[dict[str, Any]]) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Font

    wb = Workbook()
    ws = wb.active
    ws.title = "model_only_candidates"
    if not rows:
        ws.append(["no_model_only_candidates"])
    else:
        fields = list(rows[0].keys())
        ws.append(fields)
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for row in rows:
            ws.append([row.get(f, "") for f in fields])
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def summarize(rows: list[dict[str, Any]], model_names: list[str], runtime: dict[str, Any]) -> dict[str, Any]:
    baseline_accept = {r["join_key"] for r in rows if r["baseline_accept"]}
    summary: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "rows_compared": len(rows),
        "baseline_accepted_count": len(baseline_accept),
        "baseline_review_count": sum(1 for r in rows if r["baseline_gate"] == "review"),
        "runtime_seconds": runtime,
        "models": {},
    }
    for model in model_names:
        accepted = [r for r in rows if r.get(f"{model}_accept")]
        model_only = [r for r in accepted if not r["baseline_accept"]]
        high_conf = [r for r in model_only if as_float(r.get(f"{model}_margin")) >= 0.05 and as_float(r.get(f"{model}_ko_external_margin")) >= 0.0]
        safety = [
            r for r in high_conf
            if as_float(r.get(f"{model}_anti_region_score")) < 0.55
            and as_float(r.get(f"{model}_ad_promo_score")) < 0.55
            and as_float(r.get(f"{model}_event_news_service_score")) < 0.55
        ]
        image_ready = [r for r in safety if r.get("image_ready_or_score")]
        ad_fp = [r for r in accepted if as_float(r.get(f"{model}_ad_promo_score")) >= 0.55]
        anti_fp = [r for r in accepted if as_float(r.get(f"{model}_anti_region_score")) >= 0.55 or as_float(r.get(f"{model}_ko_external_margin")) < -0.02]
        positives = [r for r in model_only if str(r.get("label") or "").startswith("positive")]
        negatives = [r for r in model_only if str(r.get("label") or "").startswith("negative")]
        external_classes = Counter(str(r.get(f"{model}_top_external_geo_class") or "") for r in accepted if r.get(f"{model}_top_external_geo_class"))
        gain_pct = (len(model_only) / len(baseline_accept) * 100.0) if baseline_accept else None
        summary["models"][model] = {
            "present_rows": sum(1 for r in rows if r.get(f"{model}_present")),
            "accepted_count": len(accepted),
            "agreement_with_baseline_rate": round(sum(1 for r in rows if bool(r.get(f"{model}_accept")) == bool(r["baseline_accept"])) / len(rows), 6) if rows else None,
            "model_only_accepted_count": len(model_only),
            "model_only_gain_pct_vs_baseline": round(gain_pct, 3) if gain_pct is not None else None,
            "model_only_high_confidence_count": len(high_conf),
            "model_only_after_safety_filters_count": len(safety),
            "model_only_after_image_ready_or_image_score_count": len(image_ready),
            "baseline_only_count": sum(1 for r in rows if r["baseline_accept"] and not r.get(f"{model}_accept")),
            "anti_region_false_positive_count": len(anti_fp),
            "anti_region_false_positive_rate_of_accepted": round(len(anti_fp) / len(accepted), 6) if accepted else None,
            "ad_promo_high_among_accepted_count": len(ad_fp),
            "ad_promo_high_among_accepted_rate": round(len(ad_fp) / len(accepted), 6) if accepted else None,
            "model_only_proxy_positive_count": len(positives),
            "model_only_proxy_negative_count": len(negatives),
            "model_only_proxy_positive_rate": round(len(positives) / (len(positives) + len(negatives)), 6) if positives or negatives else None,
            "top_external_geo_classes": external_classes.most_common(10),
            "avg_model_only_travel_emotion_review_score": round(sum(as_float(r.get(f"{model}_travel_emotion_review_score")) for r in model_only) / len(model_only), 6) if model_only else None,
            "avg_model_only_useful_story_score": round(sum(as_float(r.get(f"{model}_useful_story_score")) for r in model_only) / len(model_only), 6) if model_only else None,
        }
    return summary


def decision_from_summary(summary: dict[str, Any]) -> str:
    decisions: list[str] = []
    base = max(1, int(summary.get("baseline_accepted_count") or 0))
    for model, metrics in summary.get("models", {}).items():
        runtime = summary.get("runtime_seconds", {}).get(model, {})
        projected_raw = runtime.get("projected_300_seconds")
        projected = as_float(projected_raw, 0.0)
        gain = as_float(metrics.get("model_only_gain_pct_vs_baseline"))
        proxy = metrics.get("model_only_proxy_positive_rate")
        anti_rate = as_float(metrics.get("anti_region_false_positive_rate_of_accepted"))
        ad_rate = as_float(metrics.get("ad_promo_high_among_accepted_rate"))
        high_conf = int(metrics.get("model_only_high_confidence_count") or 0)
        image_ready = int(metrics.get("model_only_after_image_ready_or_image_score_count") or 0)
        model_only = int(metrics.get("model_only_accepted_count") or 0)
        enough_strong_increment = high_conf >= max(10, int(0.3 * max(1, model_only))) or image_ready > 0
        if projected_raw is not None and projected > 3600:
            decisions.append(f"- {model}: CPU-not-practical for 300 rows (projected {projected:.0f}s).")
        elif gain >= 10 and (proxy is None or proxy >= 0.30) and anti_rate <= 0.15 and ad_rate <= 0.15 and enough_strong_increment:
            decisions.append(f"- {model}: candidate for production third lane; passes recall/false-positive/runtime thresholds and has enough strong incremental rows, pending manual review.")
        elif gain >= 10 and (proxy is None or proxy >= 0.30) and anti_rate <= 0.15 and ad_rate <= 0.15:
            decisions.append(f"- {model}: keep as shadow/larger-validation lane; raw recall gain is promising, but strong/image-ready incremental rows are too few for production promotion.")
        elif gain < 5:
            decisions.append(f"- {model}: do not add; recall gain below 5% of baseline ({metrics.get('model_only_gain_pct_vs_baseline')}%).")
        else:
            decisions.append(f"- {model}: keep as shadow/research; mixed thresholds or insufficient labels.")
    if not decisions:
        return "No research model has enough coverage for a decision."
    return "\n".join(decisions) + f"\n\nBaseline accepted rows: {base}."


def main() -> int:
    ap = argparse.ArgumentParser(description="Build Region Talk embedding-model decision artifacts from live YDB rows")
    ap.add_argument("--env-file", type=Path, default=ROOT / ".env")
    ap.add_argument("--sample-limit", type=int, default=300)
    ap.add_argument("--ydb-limit", type=int, default=10000)
    ap.add_argument("--output-dir", type=Path, default=ROOT / "artifacts" / "codex" / "region-talk-embedding-model-decision")
    ap.add_argument("--runtime-json", type=Path, default=None, help="Optional JSON map with per-model elapsed/projected seconds")
    ap.add_argument("--base-kind", default="", help="If set, compare this YDB kind only, e.g. fulltext_validation_item")
    ap.add_argument("--e5-prefix", default="e5_multilingual_base", help="E5 model prefix to use for baseline")
    ap.add_argument("--require-model-source-kind", default="", help="If set, only use enrichment rows with this source_kind")
    args = ap.parse_args()
    load_dotenv(args.env_file)
    base_kinds = [args.base_kind] if args.base_kind else INPUT_KINDS
    kinds = base_kinds + list(MODEL_KINDS.values())
    items_by_kind = load_ydb_items(kinds, args.ydb_limit)
    if args.base_kind:
        # Reuse build_base_rows shape with a single requested kind.
        old_input_kinds = list(INPUT_KINDS)
        INPUT_KINDS[:] = [args.base_kind]
        try:
            base_rows = build_base_rows(items_by_kind, args.sample_limit)
        finally:
            INPUT_KINDS[:] = old_input_kinds
    else:
        base_rows = build_base_rows(items_by_kind, args.sample_limit)
    model_indexes = {}
    for model, kind in MODEL_KINDS.items():
        rows_for_model = items_by_kind.get(kind, {})
        if args.require_model_source_kind:
            rows_for_model = {pk: row for pk, row in rows_for_model.items() if isinstance(row, dict) and str(row.get("source_kind") or "") == args.require_model_source_kind}
        model_indexes[model] = index_model_rows(rows_for_model, model if model != "bge_m3" else "bge_m3")
    runtime = json.loads(args.runtime_json.read_text(encoding="utf-8")) if args.runtime_json and args.runtime_json.exists() else {}
    rows: list[dict[str, Any]] = []
    for key, base in base_rows.items():
        label, confidence, label_status = label_for_row(base)
        legacy_e5 = legacy_e5_metrics(base)
        e5 = enrichment_metrics(model_indexes.get(args.e5_prefix, {}).get(key), args.e5_prefix)
        if not e5.get("present"):
            e5 = {**legacy_e5, "present": False, "gate": "legacy_" + str(legacy_e5.get("gate") or "unknown")}
        bge = enrichment_metrics(model_indexes["bge_m3"].get(key), "bge_m3")
        baseline_accept = bool(e5.get("accept") or bge.get("accept"))
        baseline_review = bool(e5.get("review") or bge.get("review"))
        image_ready = (
            str(base.get("image_queue_status") or "") == "actual_scored"
            or as_float(base.get("overall_media_score")) >= 0.65
            or as_float(base.get("postcardness_score")) >= 0.65
        )
        out: dict[str, Any] = {
            "join_key": key,
            "post_url": base.get("post_url") or "",
            "post_id": base.get("post_id") or base.get("candidate_memory_id") or "",
            "source_title": base.get("source_title") or "",
            "source_kind": base.get("_source_kind") or "",
            "post_date": base.get("post_date") or "",
            "label": label,
            "label_confidence": confidence,
            "label_status": label_status,
            "baseline_accept": baseline_accept,
            "baseline_gate": "accept" if baseline_accept else "review" if baseline_review else "reject",
            f"{args.e5_prefix}_gate": e5.get("gate"),
            "legacy_vector_gate": legacy_e5["gate"],
            "bge_m3_gate": bge.get("gate", "missing"),
            "image_ready_or_score": image_ready,
            "text_excerpt": str(base.get("_embedding_text") or "")[:500],
        }
        for name, metrics in {args.e5_prefix: e5, "legacy_vector": legacy_e5, "bge_m3": bge}.items():
            for field, value in metrics.items():
                if field != "text_excerpt":
                    out[f"{name}_{field}"] = value
        for model in ["embeddinggemma_300m", "qwen3_embedding_0_6b"]:
            metrics = enrichment_metrics(model_indexes[model].get(key), model)
            for field, value in metrics.items():
                out[f"{model}_{field}"] = value
        rows.append(out)
    run_dir = args.output_dir / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    model_names = ["embeddinggemma_300m", "qwen3_embedding_0_6b"]
    summary = summarize(rows, model_names, runtime)
    summary["decision"] = decision_from_summary(summary)
    (run_dir / "comparison.json").write_text(json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    with (run_dir / "comparison.csv").open("w", encoding="utf-8", newline="") as fh:
        fieldnames = list(rows[0].keys()) if rows else ["join_key"]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    model_only = [
        r for r in rows
        if any(r.get(f"{model}_accept") and not r.get("baseline_accept") for model in model_names)
    ]
    write_xlsx(run_dir / "model_only_candidates.xlsx", model_only)
    md = [
        "# Region Talk embedding model decision report",
        "",
        f"Generated: {summary['generated_at']}",
        f"Rows compared: {summary['rows_compared']}",
        f"Baseline E5/BGE accepted: {summary['baseline_accepted_count']}",
        "",
        "## Decision",
        "",
        summary["decision"],
        "",
        "## Model metrics",
        "",
        json.dumps(summary["models"], ensure_ascii=False, indent=2),
        "",
        "Artifacts: `comparison.json`, `comparison.csv`, `model_only_candidates.xlsx`.",
    ]
    (run_dir / "README.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(run_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
