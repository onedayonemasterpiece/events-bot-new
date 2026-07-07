from __future__ import annotations
import asyncio, base64, html, json, os, re, subprocess, sys, time, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, pstdev
from urllib.request import urlretrieve

RUN_STARTED = time.monotonic()
RUN_ID = os.getenv("REGION_TALK_RUN_ID") or os.getenv("RT_IMAGE_DIAG_RUN_ID") or "region-talk-image-diagnostic"
OUT = Path(os.getenv("REGION_TALK_IMAGE_DIAG_OUTPUT_DIR") or f"/kaggle/working/{RUN_ID}")
MEDIA = OUT / "media"
THUMBS = OUT / "contact_sheet_assets"

def refresh_run_paths() -> None:
    global RUN_ID, OUT, MEDIA, THUMBS
    RUN_ID = os.getenv("REGION_TALK_RUN_ID") or os.getenv("RT_IMAGE_DIAG_RUN_ID") or RUN_ID or "region-talk-image-diagnostic"
    OUT = Path(os.getenv("REGION_TALK_IMAGE_DIAG_OUTPUT_DIR") or f"/kaggle/working/{RUN_ID}")
    MEDIA = OUT / "media"
    THUMBS = OUT / "contact_sheet_assets"
    for path in (OUT, MEDIA, THUMBS):
        path.mkdir(parents=True, exist_ok=True)

refresh_run_paths()

def log_event(name: str, **payload):
    payload.setdefault("event_name", name)
    payload.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    print("[region-talk-image-diagnostic] " + json.dumps(payload, ensure_ascii=False)[:1200], flush=True)
    if name in {"kernel_started", "image_queue_poll", "image_batch_started", "image_batch_done", "ydb_source_visual_rollup_written", "report_written", "image_queue_poll_finished_empty"}:
        hb = globals().get("write_region_talk_image_diag_heartbeat")
        if callable(hb):
            try:
                hb({**payload, "run_id": RUN_ID})
            except Exception as exc:
                print(f"[region-talk-image-diagnostic] business_heartbeat_ydb_failed {type(exc).__name__}: {str(exc)[:160]}", flush=True)

def ensure(import_name: str, pip_name: str | None = None) -> bool:
    try:
        __import__(import_name); return True
    except Exception:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pip_name or import_name])
            __import__(import_name); return True
        except Exception as exc:
            log_event("package_unavailable", package=import_name, error=type(exc).__name__ + ": " + str(exc)[:300])
            return False

for imp, pip in [("PIL", "pillow"), ("openpyxl", "openpyxl"), ("requests", "requests"), ("cryptography", "cryptography"), ("telethon", "telethon")]:
    ensure(imp, pip)

import requests
from PIL import Image, ImageStat, ImageFilter
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter
from cryptography.fernet import Fernet


def find_input_file(name: str) -> Path | None:
    for p in Path("/kaggle/input").glob(f"**/{name}"):
        return p
    return None

def load_json_file(name: str) -> dict:
    p = find_input_file(name)
    if not p:
        raise FileNotFoundError(name)
    return json.loads(p.read_text(encoding="utf-8"))

def load_runtime_config() -> dict:
    cfg = {}
    for p in Path("/kaggle/input").glob("**/region_talk_run_config.json"):
        try:
            cfg.update(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            pass
    for k, v in (cfg.get("env") or {}).items():
        if v is not None:
            os.environ[str(k)] = str(v)
    return cfg

def load_kaggle_user_secrets() -> dict:
    names = ["REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON", "REGION_TALK_YDB_IAM_TOKEN", "YDB_ACCESS_TOKEN"]
    extra = (os.getenv("REGION_TALK_KAGGLE_SECRET_NAMES") or "").strip()
    if extra:
        names.extend([x.strip() for x in re.split(r"[,;\\s]+", extra) if x.strip()])
    names = list(dict.fromkeys(names))
    try:
        from kaggle_secrets import UserSecretsClient  # type: ignore
    except Exception as exc:
        return {"ok": False, "source": "kaggle_user_secrets", "error": type(exc).__name__, "loaded": []}
    loaded = []; errors = []; client = UserSecretsClient()
    for name in names:
        if os.getenv(name):
            continue
        try:
            value = client.get_secret(name)
            if value is not None and str(value).strip():
                os.environ.setdefault(name, str(value)); loaded.append(name)
        except Exception as exc:
            errors.append(f"{name}:{type(exc).__name__}")
    return {"ok": bool(loaded), "source": "kaggle_user_secrets", "loaded": loaded, "errors": errors[:5]}

def load_secrets() -> dict:
    status = {"encrypted": {"ok": False}, "kaggle_user_secrets": {"ok": False}}
    pairs = []
    for enc in Path("/kaggle/input").glob("**/region_talk_secrets.enc"):
        key = enc.parent / "region_talk_fernet.key"
        if key.exists(): pairs.append((enc, key))
    for enc, key in pairs:
        try:
            data = json.loads(Fernet(key.read_bytes().strip()).decrypt(enc.read_bytes()).decode("utf-8"))
            for k, v in data.items():
                if v is not None and str(v).strip(): os.environ.setdefault(str(k), str(v))
            status["encrypted"] = {"ok": True, "keys": sorted(data.keys())}
            break
        except Exception as exc:
            last = type(exc).__name__ + ": " + str(exc)[:200]
    else:
        status["encrypted"] = {"ok": False, "error": locals().get("last", "no secrets pair")}
    status["kaggle_user_secrets"] = load_kaggle_user_secrets()
    status["ok"] = bool(status["encrypted"].get("ok") or status["kaggle_user_secrets"].get("ok"))
    return status

runtime_config = load_runtime_config()
refresh_run_paths()
input_payload = load_json_file("image_diag_input.json")
secret_status = load_secrets()
rows = input_payload.get("rows") or []
limit = int(os.getenv("REGION_TALK_IMAGE_DIAG_TOP_N") or input_payload.get("top_n") or 50)

def ydb_table_name(suffix: str = "state_kv") -> str:
    ns = re.sub(r"[^A-Za-z0-9_]+", "_", (os.getenv("REGION_TALK_YDB_NAMESPACE") or "region_talk").strip() or "region_talk").strip("_") or "region_talk"
    return f"{ns}_{suffix}"

def ydb_cfg():
    endpoint=(os.getenv("REGION_TALK_YDB_ENDPOINT") or "").strip(); database=(os.getenv("REGION_TALK_YDB_DATABASE") or "").strip()
    if "?database=" in endpoint:
        endpoint_part, database_part = endpoint.split("?database=", 1)
        endpoint = endpoint_part
        if not database: database = database_part
    endpoint = endpoint.rstrip("/")
    if not endpoint or not database: raise RuntimeError("missing REGION_TALK_YDB_ENDPOINT/REGION_TALK_YDB_DATABASE")
    return endpoint, database, database.rstrip("/") + "/" + ydb_table_name()

def ydb_credentials(ydb):
    token=(os.getenv("REGION_TALK_YDB_IAM_TOKEN") or os.getenv("YC_IAM_TOKEN") or os.getenv("YDB_ACCESS_TOKEN") or "").strip()
    if token: return ydb.AccessTokenCredentials(token)
    key_json=(os.getenv("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip()
    if key_json:
        import tempfile
        import ydb.iam  # type: ignore
        fd, path = tempfile.mkstemp(prefix="region-talk-image-ydb-sa-", suffix=".json")
        os.close(fd)
        Path(path).write_text(key_json, encoding="utf-8")
        return ydb.iam.ServiceAccountCredentials.from_file(path)
    if os.getenv("YDB_USER"): return ydb.StaticCredentials.from_user_password(os.getenv("YDB_USER"), os.getenv("YDB_PASSWORD", ""))
    return None

def ydb_connect():
    ensure("ydb", "ydb[yc]")
    import ydb
    endpoint,database,table_path=ydb_cfg(); creds=ydb_credentials(ydb)
    driver=ydb.Driver(endpoint=endpoint, database=database, credentials=creds) if creds is not None else ydb.Driver(endpoint=endpoint, database=database)
    driver.wait(timeout=int(os.getenv("REGION_TALK_YDB_CONNECT_TIMEOUT_SECONDS") or "20"), fail_fast=True)
    return ydb, driver, table_path

def write_region_talk_image_diag_heartbeat(payload: dict):
    if (os.getenv("REGION_TALK_IMAGE_DIAG_HEARTBEAT_YDB") or "1").lower() in {"0","false","no"}: return
    if (os.getenv("REGION_TALK_STATE_BACKEND") or "").lower() != "ydb" and not os.getenv("REGION_TALK_YDB_ENDPOINT"): return
    ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver); now=datetime.now(timezone.utc).isoformat()
    allowed=["run_id","event_name","created_at","phase","reason","attempt","total","leased","remaining_budget","batch_index","rows","actual_scored","actual_images","failures","xlsx","html","summary","source","max_items_per_run","batch_size","poll_interval_seconds","wait_initial_seconds","wait_after_drain_seconds"]
    clean={k:payload.get(k) for k in allowed if payload.get(k) not in (None,"",[],{})}
    clean.setdefault("run_id", RUN_ID); clean.setdefault("updated_at", now); clean.setdefault("notebook", "RegionTalkImageDiagnostic")
    def op(session):
        query=session.prepare(f"""DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);""")
        tx=session.transaction(ydb.SerializableReadWrite())
        for pk in ["latest_business_heartbeat:image_diagnostic", "business_heartbeat:image_diagnostic:"+RUN_ID]:
            tx.execute(query,{"$pk":pk,"$kind":"business_heartbeat_image_diagnostic","$payload_json":json.dumps(clean,ensure_ascii=False),"$updated_at":now},commit_tx=False)
        tx.commit()
    try: pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)

def ydb_select_kind(kind: str, limit_n: int):
    ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver)
    def op(session):
        max_items=max(1, int(limit_n)); page_size=max(1, min(max_items, int(os.getenv("REGION_TALK_YDB_SELECT_PAGE_SIZE") or "200")))
        out=[]; after=""
        while len(out) < max_items:
            query=session.prepare(f"""DECLARE $kind AS Utf8; DECLARE $after AS Utf8;
SELECT pk, payload_json FROM `{table_path}` WHERE kind = $kind AND pk > $after ORDER BY pk LIMIT {min(page_size, max_items-len(out))};""")
            rs=session.transaction(ydb.StaleReadOnly()).execute(query,{"$kind":kind,"$after":after}, commit_tx=True)
            rows=rs[0].rows if rs else []
            if not rows: break
            for row in rows:
                after=str(row.pk)
                payload=row.payload_json; d=json.loads(payload) if isinstance(payload,str) else dict(payload or {})
                if isinstance(d,dict): d.setdefault("_ydb_pk", str(row.pk)); out.append(d)
            if len(rows) < page_size: break
        return out
    try: return pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)

def ydb_select_image_queue(limit_n: int):
    return ydb_select_kind("image_queue_item", max(1, limit_n*5))

def ydb_select_source_queue(limit_n: int = 10000):
    by_key = {}
    for row in ydb_select_kind("source_queue_item", max(1, limit_n)):
        key = str(row.get("canonical_source_key") or row.get("source_queue_id") or row.get("source_url") or row.get("_ydb_pk") or "")
        if key:
            by_key[key] = {**by_key.get(key, {}), **row}
    # Live CandidateReport writes source/public status aliases immediately while
    # a run is still in progress. ImageDiagnostic must consume those too, or a
    # source selected/skipped/updated only through the live-status path will be
    # invisible to visual rollups until a later final snapshot rewrite.
    for row in ydb_select_kind("source_status_item", max(1, limit_n)):
        key = str(row.get("canonical_source_key") or row.get("source_queue_id") or row.get("source_url") or row.get("_ydb_pk") or "")
        if key:
            by_key[key] = {**by_key.get(key, {}), **row}
    return list(by_key.values())[: max(1, limit_n)]

def text_region_confirmed(r):
    if str(r.get("is_ad_or_promo") or "").lower() in {"true","1","yes"}: return False
    if str(r.get("vector_gate_status") or "").startswith("vector_reject"): return False
    if str(r.get("kaliningrad_oblast_only_scope") or "").lower() not in {"true","1","yes"}: return False
    if str(r.get("kaliningrad_mention_role") or "main_subject") not in {"","main_subject","unclear"}: return False
    if str(r.get("external_geo_mentions") or r.get("mentioned_external_regions") or "").strip(): return False
    return True

def stale_image_lease(r):
    if str(r.get("image_queue_status") or "") != "image_analysis_in_progress":
        return False
    lease_at = str(r.get("lease_at") or "")
    try:
        dt = datetime.fromisoformat(lease_at.replace("Z", "+00:00"))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        return True
    ttl = int(os.getenv("REGION_TALK_IMAGE_DIAG_STALE_LEASE_SECONDS") or "1800")
    return (datetime.now(timezone.utc) - dt).total_seconds() >= max(0, ttl)

def ydb_upsert_image_rows(batch, *, stage: str):
    if not batch or (os.getenv("REGION_TALK_IMAGE_DIAG_WRITE_YDB") or "1").lower() in {"0","false","no"}: return
    ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver); now=datetime.now(timezone.utc).isoformat()
    def op(session):
        query=session.prepare(f"""DECLARE $pk AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at) VALUES ($pk, 'image_queue_item', $payload_json, $updated_at);""")
        tx=session.transaction(ydb.SerializableReadWrite())
        for r in batch:
            key=str(r.get("image_queue_id") or r.get("post_url") or r.get("_ydb_pk") or "")
            if not key: continue
            previous_status=str(r.get("image_queue_status") or "")
            r["last_image_diag_run_id"]=RUN_ID; r["last_image_diag_stage"]=stage; r["last_image_diag_at"]=now
            r["queue_item_updated_at"] = now
            if previous_status:
                r.setdefault("last_status_changed_at", now)
            tx.execute(query,{"$pk":"image_queue_item:"+key.replace("image_queue_item:",""),"$payload_json":json.dumps(r,ensure_ascii=False),"$updated_at":now},commit_tx=False)
        tx.commit()
    try: pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)

def ydb_upsert_source_rows(batch, *, stage: str):
    if not batch or (os.getenv("REGION_TALK_IMAGE_DIAG_WRITE_YDB") or "1").lower() in {"0","false","no"}: return
    ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver); now=datetime.now(timezone.utc).isoformat()
    def op(session):
        query=session.prepare(f"""DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);""")
        tx=session.transaction(ydb.SerializableReadWrite())
        for r in batch:
            key=str(r.get("canonical_source_key") or r.get("source_queue_id") or r.get("source_url") or r.get("_ydb_pk") or "")
            if not key: continue
            r["source_visual_rollup_run_id"]=RUN_ID; r["source_visual_rollup_updated_at"]=now; r["queue_item_updated_at"]=now
            clean_key=key.replace("source_queue_item:","").replace("source_status_item:","").replace("online_source_item:","")
            for kind in ["source_queue_item", "source_status_item"]:
                tx.execute(query,{"$pk":kind+":"+clean_key,"$kind":kind,"$payload_json":json.dumps(r,ensure_ascii=False),"$updated_at":now},commit_tx=False)
        tx.commit()
    try: pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)

def ydb_update_source_visual_rollups():
    try:
        image_rows=ydb_select_kind("image_queue_item", int(os.getenv("REGION_TALK_YDB_MAX_CANDIDATE_ROWS") or "5000"))
        source_rows=ydb_select_source_queue(int(os.getenv("REGION_TALK_YDB_MAX_SOURCE_ROWS") or "5000"))
    except Exception as exc:
        log_event("ydb_source_rollup_read_failed", error=type(exc).__name__ + ": " + str(exc)[:300]); return
    if not source_rows:
        return
    try: min_n=int(os.getenv("REGION_TALK_SOURCE_IMAGE_MIN_ACTUAL_SCORED") or "3")
    except Exception: min_n=3
    try: min_score=float(os.getenv("REGION_TALK_SOURCE_IMAGE_MIN_AVG_SCORE") or "0.55")
    except Exception: min_score=0.55
    updated=[]
    for srow in source_rows:
        urls={str(srow.get("source_url") or "").rstrip('/'), str(srow.get("canonical_url") or "").rstrip('/')} - {""}
        sid=str(srow.get("source_id") or "")
        matches=[]
        for ir in image_rows:
            if not text_region_confirmed(ir): continue
            if sid and str(ir.get("source_id") or "") == sid:
                matches.append(ir); continue
            if str(ir.get("source_url") or "").rstrip('/') in urls:
                matches.append(ir)
        scores=[]
        for ir in matches:
            if str(ir.get("image_queue_status") or "") != "actual_scored" and str(ir.get("image_model_input_type") or "") != "actual_image": continue
            try: scores.append(float(ir.get("overall_media_score") or ir.get("final_visual_score") or 0))
            except Exception: pass
        if not matches and not scores:
            continue
        avg=round(sum(scores)/len(scores),3) if scores else ""
        low=sum(1 for x in scores if x < min_score)
        previous_status=str(srow.get("source_queue_status") or "")
        if len(scores) >= min_n and avg != "" and float(avg) < min_score:
            qstatus="processed_found_ko_low_image_quality"; img_status="exclude_low_image_quality"; reason="kaliningrad_posts_found_but_actual_images_systematically_low_score"
        elif len(scores) > 0:
            qstatus=previous_status or "processed_found_ko_candidate"; img_status="monitor_candidate_image_quality_ok"; reason=""
        else:
            qstatus=previous_status or "processed_found_ko_candidate"; img_status="needs_more_actual_image_evidence"; reason=""
        changed=bool(previous_status and previous_status != qstatus)
        srow.update({
            "source_queue_status": qstatus, "previous_source_queue_status": previous_status if changed else srow.get("previous_source_queue_status", ""),
            "status_changed_this_run": str(changed).lower(), "last_status_changed_at": datetime.now(timezone.utc).isoformat() if changed or not srow.get("last_status_changed_at") else srow.get("last_status_changed_at"),
            "ko_posts_found": max(int(srow.get("ko_posts_found") or 0), len(matches)), "candidate_posts_found": max(int(srow.get("candidate_posts_found") or 0), len(matches)),
            "actual_images_scored_count": len(scores), "avg_actual_image_score": avg, "low_actual_image_count": low,
            "source_image_quality_status": img_status, "source_image_quality_min_actual_scored": min_n, "source_image_quality_min_avg_score": min_score,
            "monitoring_exclusion_reason": reason,
        })
        updated.append(srow)
    if updated:
        ydb_upsert_source_rows(updated, stage="visual_rollup_from_image_diagnostic")
        log_event("ydb_source_visual_rollup_written", sources=len(updated), image_rows=len(image_rows))

def ydb_rows_for_diagnostic(limit_n: int):
    raw=ydb_select_image_queue(limit_n)
    pending=[]
    for r in raw:
        if not text_region_confirmed(r): continue
        status=str(r.get("image_queue_status") or "")
        lease=str(r.get("lease_run_id") or "")
        if status == "actual_scored": continue
        if status == "image_analysis_in_progress" and lease and lease != RUN_ID and not stale_image_lease(r): continue
        if status == "image_analysis_in_progress" and lease and lease != RUN_ID and stale_image_lease(r):
            r["previous_image_queue_status"] = status
            r["stale_lease_reclaimed_from_run_id"] = lease
            r["stale_lease_reclaimed_at"] = datetime.now(timezone.utc).isoformat()
        pending.append(r)
    pending=sorted(pending, key=lambda r: (int(r.get("image_queue_order") or 10**9), str(r.get("post_url") or "")))[:limit_n]
    now=datetime.now(timezone.utc).isoformat()
    for r in pending:
        r["image_queue_status"]="image_analysis_in_progress"; r["lease_run_id"]=RUN_ID; r["lease_at"]=now
    ydb_upsert_image_rows(pending, stage="leased_for_image_analysis")
    return pending, len(raw)

def poll_ydb_image_queue(limit_n: int, *, wait_seconds: int, reason: str):
    deadline = time.monotonic() + max(0, wait_seconds)
    attempt = 0
    while True:
        attempt += 1
        try:
            batch, total = ydb_rows_for_diagnostic(limit_n)
            input_payload["queue_rows_total"] = total
            log_event("image_queue_poll", phase="poll", reason=reason, attempt=attempt, total=total, leased=len(batch), remaining_budget=limit_n)
            if batch:
                return batch, total
        except Exception as exc:
            log_event("ydb_image_queue_read_failed", phase="poll", reason=reason, attempt=attempt, error=type(exc).__name__ + ": " + str(exc)[:300])
            if reason == "initial" and attempt == 1 and wait_seconds <= 0:
                raise
        if time.monotonic() >= deadline:
            return [], int(input_payload.get("queue_rows_total") or 0)
        sleep_for = min(max(1, POLL_INTERVAL_SECONDS), max(1, int(deadline - time.monotonic())))
        time.sleep(sleep_for)

source_mode=(os.getenv("REGION_TALK_IMAGE_DIAG_SOURCE") or input_payload.get("source") or ("ydb" if (os.getenv("REGION_TALK_STATE_BACKEND") or "").lower()=="ydb" else "input")).lower()
MAX_ITEMS_PER_RUN = int(os.getenv("REGION_TALK_IMAGE_DIAG_MAX_ITEMS_PER_RUN") or input_payload.get("max_items_per_run") or limit)
BATCH_SIZE = max(1, min(MAX_ITEMS_PER_RUN, int(os.getenv("REGION_TALK_IMAGE_DIAG_BATCH_SIZE") or input_payload.get("batch_size") or 30)))
POLL_INTERVAL_SECONDS = int(os.getenv("REGION_TALK_IMAGE_DIAG_POLL_INTERVAL_SECONDS") or input_payload.get("poll_interval_seconds") or 60)
WAIT_INITIAL_SECONDS = int(os.getenv("REGION_TALK_IMAGE_DIAG_WAIT_INITIAL_SECONDS") or input_payload.get("wait_initial_seconds") or 600)
WAIT_AFTER_DRAIN_SECONDS = int(os.getenv("REGION_TALK_IMAGE_DIAG_WAIT_AFTER_DRAIN_SECONDS") or input_payload.get("wait_after_drain_seconds") or 600)
if source_mode != "ydb":
    rows = rows[:MAX_ITEMS_PER_RUN]
else:
    rows = []
    input_payload["source"] = "ydb"
log_event("kernel_started", run_id=RUN_ID, source=input_payload.get("source") or source_mode, max_items_per_run=MAX_ITEMS_PER_RUN, batch_size=BATCH_SIZE, poll_interval_seconds=POLL_INTERVAL_SECONDS, wait_initial_seconds=WAIT_INITIAL_SECONDS, wait_after_drain_seconds=WAIT_AFTER_DRAIN_SECONDS, input_rows=len(rows), secret_status={"ok": secret_status.get("ok"), "keys_count": len(secret_status.get("keys") or [])})

model_availability = {
    "cv_local_baseline": {"available": True, "detail": "PIL resolution/sharpness/brightness/contrast baseline"},
    "laion_aesthetic_predictor": {"available": False, "detail": "not loaded yet"},
    "nima_lightweight_quality": {"available": False, "detail": "not loaded yet"},
    "clip_iqa_postcardness_prompt_scorer": {"available": False, "detail": "not loaded yet"},
}
errors = []

def parse_tg(url: str):
    m = re.search(r"t\.me/(?:s/)?([^/?#]+)/([0-9]+)", url or "")
    return (m.group(1), int(m.group(2))) if m else (None, None)

def parse_vk(url: str):
    m = re.search(r"vk\.com/wall(-?\d+)_(\d+)", url or "")
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)

def decode_bundle():
    b64 = (os.getenv(os.getenv("REGION_TALK_AUTH_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_DISCOVERY")) or os.getenv("TELEGRAM_AUTH_BUNDLE_DISCOVERY") or "").strip()
    if not b64: return None
    return json.loads(base64.urlsafe_b64decode(b64.encode("ascii")).decode("utf-8"))

async def fetch_telegram(batch):
    bundle = decode_bundle()
    api_id = os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH")
    if not bundle or not api_id or not api_hash:
        for r in batch:
            r["media_fetch_status"] = "needs_actual_image_fetch"
            r["media_fetch_error"] = "telegram auth bundle/api id/hash unavailable"
        return
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    device = {k: bundle[k] for k in ["device_model", "system_version", "app_version", "lang_code", "system_lang_code"] if bundle.get(k)}
    client = TelegramClient(StringSession(bundle["session"]), int(api_id), api_hash, flood_sleep_threshold=30, **device)
    await client.connect()
    try:
        for idx, r in enumerate(batch, 1):
            log_event("image_fetch_current", phase="telegram_fetch", index=idx, total=len(batch), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), source_title=r.get("source_title"))
            t0 = time.monotonic(); handle, mid = parse_tg(r.get("post_url", ""))
            if not handle:
                r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]="cannot parse telegram url"; continue
            try:
                msg = await client.get_messages(handle, ids=mid)
                if not msg or not getattr(msg, "media", None):
                    r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]="telegram message has no direct media"; continue
                path = await client.download_media(msg, file=str(MEDIA / f"{r['image_queue_id']}_{handle}_{mid}"))
                r["media_download_seconds"] = round(time.monotonic()-t0, 3)
                if path:
                    r["actual_media_path"] = str(path); r["media_fetch_status"] = "downloaded"
                else:
                    r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]="download_media returned empty path"
            except Exception as exc:
                r["media_download_seconds"] = round(time.monotonic()-t0, 3)
                r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"] = type(exc).__name__ + ": " + str(exc)[:300]
            log_event("image_fetch_result", phase="telegram_fetch", index=idx, total=len(batch), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("media_fetch_status"), actual=bool(r.get("actual_media_path")), seconds=r.get("media_download_seconds"), error=r.get("media_fetch_error"))
            if idx % 10 == 0:
                log_event("media_fetch_progress", phase="media_fetch", done=idx, total=len(batch), actual=sum(1 for x in batch if x.get("actual_media_path")))
    finally:
        await client.disconnect()

def fetch_vk(r):
    token = os.getenv("VK_USER_TOKEN") or os.getenv("VK_ACCESS_TOKEN4") or os.getenv("VK_ACCESS_TOKEN5") or os.getenv("VK_ACCESS_TOKEN") or os.getenv("VK_SERVICE_TOKEN") or ""
    owner, pid = parse_vk(r.get("post_url", "")); t0 = time.monotonic()
    if not token or owner is None:
        r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]="VK token unavailable or url parse failed"; return
    try:
        resp = requests.get("https://api.vk.com/method/wall.getById", params={"posts": f"{owner}_{pid}", "access_token": token, "v": "5.199"}, timeout=25)
        data = resp.json(); items = data.get("response")
        if isinstance(items, dict): items = items.get("items") or []
        if not items: raise RuntimeError(str(data.get("error") or "empty VK response")[:300])
        photos=[]
        for a in items[0].get("attachments") or []:
            if a.get("type") == "photo" and a.get("photo"):
                sizes = a["photo"].get("sizes") or []
                if sizes:
                    best = max(sizes, key=lambda s: int(s.get("width") or 0)*int(s.get("height") or 0))
                    if best.get("url"): photos.append(best.get("url"))
        if not photos: raise RuntimeError("no VK photo attachment")
        img = requests.get(photos[0], timeout=35); img.raise_for_status()
        path = MEDIA / f"{r['image_queue_id']}_vk_{owner}_{pid}.jpg"; path.write_bytes(img.content)
        r["actual_media_path"] = str(path); r["media_fetch_status"]="downloaded"; r["media_download_seconds"] = round(time.monotonic()-t0, 3)
    except Exception as exc:
        r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"] = type(exc).__name__ + ": " + str(exc)[:300]; r["media_download_seconds"] = round(time.monotonic()-t0, 3)

def validate_image(r):
    t=time.monotonic()
    p = r.get("actual_media_path")
    if not p: r["actual_image_count"] = 0; return None
    try:
        im = Image.open(p).convert("RGB")
        r["actual_image_count"] = 1; r["image_width"], r["image_height"] = im.size; r["image_file_bytes"] = Path(p).stat().st_size
        th = im.copy(); th.thumbnail((420,300)); tp = THUMBS / (Path(p).stem + ".jpg"); th.save(tp, quality=84); r["thumbnail_path"] = str(tp)
        r["image_decode_seconds"] = round(time.monotonic()-t, 3)
        return im
    except Exception as exc:
        r["image_decode_seconds"] = round(time.monotonic()-t, 3); r["actual_image_count"] = 0; r["media_fetch_status"] = "decode_failed"; r["media_fetch_error"] = type(exc).__name__ + ": " + str(exc)[:300]; return None

def score_cv(im, r):
    t=time.monotonic(); w,h=im.size; gray=im.convert("L"); st=ImageStat.Stat(gray)
    mean_b=st.mean[0]/255.0; std_b=st.stddev[0]/128.0; sharp=ImageStat.Stat(gray.filter(ImageFilter.FIND_EDGES)).mean[0]/64.0
    aspect=w/max(1,h); res=min(1.0,(w*h)/(1280*720)); aspect_score=max(0,1-min(abs(aspect-1.5),1.5)/1.5)
    brightness=max(0,1-abs(mean_b-0.52)/0.52); contrast=max(0,min(1,std_b)); sharp_score=max(0,min(1,sharp))
    technical=.35*res+.25*sharp_score+.2*brightness+.2*contrast; aesthetic=.35*contrast+.25*brightness+.2*aspect_score+.2*sharp_score
    low_noise=.5*sharp_score+.5*contrast; postcard=.45*aesthetic+.35*technical+.2*aspect_score; overall=.35*technical+.3*aesthetic+.25*postcard+.1*low_noise
    r.update({"cv_technical_quality_score":round(technical,3),"cv_aesthetic_score":round(aesthetic,3),"cv_postcardness_score":round(postcard,3),"cv_publication_safety_score":0.98,"cv_low_noise_score":round(low_noise,3),"cv_overall_media_score":round(overall,3),"cv_inference_seconds":round(time.monotonic()-t,3)})

CLIP={"loaded":False,"error":None}
def maybe_clip():
    if CLIP["loaded"]: return True
    if CLIP["error"]: return False
    try:
        import torch
        from transformers import CLIPModel, CLIPProcessor
        t=time.monotonic(); model_id="openai/clip-vit-base-patch32"; device="cuda" if torch.cuda.is_available() else "cpu"
        proc=CLIPProcessor.from_pretrained(model_id); model=CLIPModel.from_pretrained(model_id).to(device); model.eval()
        CLIP.update({"loaded":True,"torch":torch,"processor":proc,"model":model,"device":device})
        model_availability["clip_iqa_postcardness_prompt_scorer"]={"available":True,"detail":f"{model_id} on {device}, load_seconds={round(time.monotonic()-t,2)}"}
        return True
    except Exception as exc:
        CLIP["error"] = type(exc).__name__ + ": " + str(exc)[:500]
        model_availability["clip_iqa_postcardness_prompt_scorer"]={"available":False,"detail":CLIP["error"]}
        return False

def score_clip(im, r):
    if not maybe_clip(): return
    pos=["beautiful postcard travel photo","scenic Baltic sea travel photo","beautiful old European city architecture","Kaliningrad travel postcard photo","atmospheric seaside resort town"]
    neg=["screenshot","meme","advertising banner","news incident photo","low quality blurry photo","document scan","crowded political event","accident scene"]
    prompts=pos+neg; t=time.monotonic()
    try:
        torch=CLIP["torch"]; inputs=CLIP["processor"](text=prompts,images=im,return_tensors="pt",padding=True).to(CLIP["device"])
        with torch.no_grad(): probs=CLIP["model"](**inputs).logits_per_image.softmax(dim=1)[0].detach().cpu().tolist()
        ps=sum(probs[:len(pos)]); ns=sum(probs[len(pos):])
        r.update({"clip_postcardness_score":round(ps/(ps+ns+1e-9),3),"clip_positive_mass":round(ps,4),"clip_negative_mass":round(ns,4),"clip_top_prompt":prompts[max(range(len(prompts)), key=lambda i: probs[i])],"clip_inference_seconds":round(time.monotonic()-t,3)})
    except Exception as exc:
        r["clip_error"] = type(exc).__name__ + ": " + str(exc)[:300]

LAION={"loaded":False,"error":None}
def maybe_laion():
    if LAION["loaded"]: return True
    if LAION["error"]: return False
    try:
        if not maybe_clip():
            raise RuntimeError("CLIP unavailable; LAION aesthetic v1 needs CLIP ViT-B/32 embeddings")
        torch=CLIP["torch"]; t=time.monotonic(); device=CLIP["device"]
        cache=Path.home()/".cache"/"region-talk-image-diagnostic"; cache.mkdir(parents=True, exist_ok=True)
        weights=cache/"sa_0_4_vit_b_32_linear.pth"
        if not weights.exists():
            urlretrieve("https://github.com/LAION-AI/aesthetic-predictor/raw/main/sa_0_4_vit_b_32_linear.pth", weights)
        model=torch.nn.Linear(512,1).to(device)
        try:
            state=torch.load(str(weights), map_location=device, weights_only=True)
        except TypeError:
            state=torch.load(str(weights), map_location=device)
        model.load_state_dict(state); model.eval()
        LAION.update({"loaded":True,"model":model})
        model_availability["laion_aesthetic_predictor"]={"available":True,"detail":f"LAION sa_0_4_vit_b_32_linear on CLIP ViT-B/32, load_seconds={round(time.monotonic()-t,2)}"}
        return True
    except Exception as exc:
        LAION["error"]=type(exc).__name__ + ": " + str(exc)[:500]
        model_availability["laion_aesthetic_predictor"]={"available":False,"detail":LAION["error"]}
        log_event("model_unavailable", model="laion_aesthetic_predictor", error=LAION["error"])
        return False

def score_laion(im, r):
    if not maybe_laion(): return
    t=time.monotonic()
    try:
        torch=CLIP["torch"]
        inputs=CLIP["processor"](images=im, return_tensors="pt").to(CLIP["device"])
        with torch.no_grad():
            emb=CLIP["model"].get_image_features(**inputs)
            if not hasattr(emb, "norm"):
                if hasattr(emb, "pooler_output"):
                    pooled=emb.pooler_output
                    target_in=int(getattr(LAION["model"], "in_features", 512))
                    emb=pooled if int(pooled.shape[-1]) == target_in else CLIP["model"].visual_projection(pooled)
                elif isinstance(emb, (tuple, list)) and emb:
                    emb=emb[0]
            emb=emb / emb.norm(dim=-1, keepdim=True)
            raw=float(LAION["model"](emb).detach().cpu().flatten()[0])
        r.update({"laion_aesthetic_raw_score":round(raw,3),"laion_aesthetic_score":round(max(0.0,min(1.0,raw/10.0)),3),"laion_inference_seconds":round(time.monotonic()-t,3)})
    except Exception as exc:
        r["laion_error"] = type(exc).__name__ + ": " + str(exc)[:300]
        r["laion_inference_seconds"] = round(time.monotonic()-t,3)

NIMA={"loaded":False,"error":None}
def maybe_nima():
    if NIMA["loaded"]: return True
    if NIMA["error"]: return False
    try:
        t=time.monotonic()
        try:
            import pyiqa  # type: ignore
        except Exception:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pyiqa"])
            import pyiqa  # type: ignore
        import torch
        device="cuda" if torch.cuda.is_available() else "cpu"
        last=None
        for metric_name in ("nima", "nima-vgg16-ava"):
            try:
                metric=pyiqa.create_metric(metric_name, device=device)
                NIMA.update({"loaded":True,"metric":metric,"device":device,"metric_name":metric_name})
                model_availability["nima_lightweight_quality"]={"available":True,"detail":f"pyiqa {metric_name} on {device}, load_seconds={round(time.monotonic()-t,2)}"}
                return True
            except Exception as exc:
                last=type(exc).__name__ + ": " + str(exc)[:500]
        raise RuntimeError(last or "pyiqa NIMA metric unavailable")
    except Exception as exc:
        NIMA["error"]=type(exc).__name__ + ": " + str(exc)[:500]
        model_availability["nima_lightweight_quality"]={"available":False,"detail":NIMA["error"]}
        log_event("model_unavailable", model="nima_lightweight_quality", error=NIMA["error"])
        return False

def score_nima(r):
    if not maybe_nima(): return
    t=time.monotonic()
    try:
        score=NIMA["metric"](r.get("actual_media_path"))
        if hasattr(score, "detach"):
            raw=float(score.detach().cpu().flatten()[0])
        else:
            raw=float(score)
        r.update({"nima_quality_raw_score":round(raw,3),"nima_quality_score":round(max(0.0,min(1.0,raw/10.0)),3),"nima_inference_seconds":round(time.monotonic()-t,3)})
    except Exception as exc:
        r["nima_error"] = type(exc).__name__ + ": " + str(exc)[:300]
        r["nima_inference_seconds"] = round(time.monotonic()-t,3)

def finalize(r):
    if r.get("actual_image_count") != 1:
        r["final_visual_status"] = "needs_actual_image_fetch"; return
    vals=[r.get("cv_overall_media_score")]
    if r.get("clip_postcardness_score") not in (None, ""): vals.append(r.get("clip_postcardness_score"))
    if r.get("laion_aesthetic_score") not in (None, ""): vals.append(r.get("laion_aesthetic_score"))
    if r.get("nima_quality_score") not in (None, ""): vals.append(r.get("nima_quality_score"))
    vals=[float(v) for v in vals if v not in (None, "")]
    r["final_visual_score"] = round(sum(vals)/len(vals),3) if vals else ""
    r["final_visual_status"] = "scored_actual_image"
    comps=[r.get("cv_postcardness_score"), r.get("cv_aesthetic_score"), r.get("clip_postcardness_score"), r.get("laion_aesthetic_score"), r.get("nima_quality_score")]
    comps=[float(v) for v in comps if v not in (None, "")]
    r["model_disagreement_score"] = round(pstdev(comps),3) if len(comps)>1 else 0
    r["total_inference_seconds"] = round(sum(float(r.get(k) or 0) for k in ("cv_inference_seconds","clip_inference_seconds","laion_inference_seconds","nima_inference_seconds")), 3)
    r["total_processing_seconds"] = round(sum(float(r.get(k) or 0) for k in ("media_download_seconds","image_decode_seconds","total_inference_seconds")), 3)

def apply_image_queue_status(r):
    previous_status = str(r.get("image_queue_status") or "")
    if r.get("actual_image_count"):
        r["image_queue_status"] = "actual_scored"
        r["image_model_input_type"] = "actual_image"
        r["image_model_type"] = "multi_model_visual_consensus"
        r["overall_media_score"] = r.get("final_visual_score")
        r["postcardness_score"] = r.get("visual_consensus_score") or r.get("clip_postcardness_score") or r.get("cv_postcardness_score")
        r["aesthetic_score"] = r.get("laion_aesthetic_score") or r.get("cv_aesthetic_score")
        r["technical_quality_score"] = r.get("cv_technical_quality_score")
        r["media_acquisition_status"] = "actual_image_downloaded_and_scored"
        r["images_scored_actual_count"] = 1
        r["next_action"] = "human_review_best_image"
    else:
        r["image_queue_status"] = "needs_actual_image_fetch"
        r["media_acquisition_status"] = "needs_actual_image_fetch"
    if previous_status and previous_status != str(r.get("image_queue_status") or ""):
        r["previous_image_queue_status"] = previous_status
        r["status_changed_this_run"] = "true"
        r["last_status_changed_at"] = datetime.now(timezone.utc).isoformat()
    else:
        r.setdefault("status_changed_this_run", "false")
    return r

def process_batch(batch_rows, batch_index: int):
    rows = batch_rows
    log_event("image_batch_started", phase="batch", batch_index=batch_index, rows=len(rows))
    # Fetch media, no source/comment scanning.
    tg=[r for r in rows if "t.me/" in (r.get("post_url") or "")]
    vk=[r for r in rows if "vk.com/wall" in (r.get("post_url") or "")]
    log_event("media_fetch_started", phase="media_fetch", telegram=len(tg), vk=len(vk), total=len(rows))
    try:
        asyncio.run(fetch_telegram(tg))
        for r in tg:
            ydb_upsert_image_rows([r], stage="media_fetch_result")
    except Exception as exc:
        err=type(exc).__name__ + ": " + str(exc)[:500]
        errors.append({"stage":"telegram_fetch_batch","error":err})
        for r in tg:
            if not r.get("media_fetch_status"):
                r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]=err
            ydb_upsert_image_rows([r], stage="media_fetch_result")
    for i, r in enumerate(vk, 1):
        log_event("image_fetch_current", phase="vk_fetch", index=i, total=len(vk), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), source_title=r.get("source_title"))
        fetch_vk(r)
        ydb_upsert_image_rows([r], stage="media_fetch_result")
        log_event("image_fetch_result", phase="vk_fetch", index=i, total=len(vk), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("media_fetch_status"), actual=bool(r.get("actual_media_path")), seconds=r.get("media_download_seconds"), error=r.get("media_fetch_error"))
        if i % 10 == 0: log_event("vk_fetch_progress", phase="media_fetch", done=i, total=len(vk), actual=sum(1 for x in vk if x.get("actual_media_path")))
    log_event("media_fetch_done", phase="media_fetch", actual_downloaded=sum(1 for r in rows if r.get("actual_media_path")), total=len(rows))

    for i, r in enumerate(rows, 1):
        log_event("image_inference_current", phase="inference", index=i, total=len(rows), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), source_title=r.get("source_title"), media_fetch_status=r.get("media_fetch_status"))
        im=validate_image(r)
        if im is None:
            errors.append({"image_queue_id":r.get("image_queue_id"),"post_url":r.get("post_url"),"stage":"media_acquisition_or_decode","error":r.get("media_fetch_error") or "no image"})
            finalize(r)
            apply_image_queue_status(r)
            ydb_upsert_image_rows([r], stage="scored_or_retry")
            log_event("image_inference_result", phase="inference", index=i, total=len(rows), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("final_visual_status"), error=r.get("media_fetch_error"))
            continue
        score_cv(im,r); score_clip(im,r); score_laion(im,r); score_nima(r); finalize(r)
        apply_image_queue_status(r)
        ydb_upsert_image_rows([r], stage="scored_or_retry")
        log_event("image_inference_result", phase="inference", index=i, total=len(rows), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("final_visual_status"), final_visual_score=r.get("final_visual_score"), cv_score=r.get("cv_overall_media_score"), clip_score=r.get("clip_postcardness_score"), laion_score=r.get("laion_aesthetic_score"), nima_score=r.get("nima_quality_score"), download_seconds=r.get("media_download_seconds"), decode_seconds=r.get("image_decode_seconds"), inference_seconds=r.get("total_inference_seconds"), total_processing_seconds=r.get("total_processing_seconds"), width=r.get("image_width"), height=r.get("image_height"))
        if i % 10 == 0: log_event("inference_progress", phase="inference", done=i, total=len(rows), actual_scored=sum(1 for x in rows if x.get("final_visual_status")=="scored_actual_image"))

    for r in rows:
        apply_image_queue_status(r)
    ydb_upsert_image_rows(rows, stage="scored_or_retry")
    ydb_update_source_visual_rollups()
    log_event("image_batch_done", phase="batch", batch_index=batch_index, rows=len(rows), actual_scored=sum(1 for r in rows if r.get("image_queue_status")=="actual_scored"))
    return rows

all_processed_rows=[]
if source_mode == "ydb":
    remaining = MAX_ITEMS_PER_RUN
    batch_index = 0
    wait_seconds = WAIT_INITIAL_SECONDS
    while remaining > 0:
        batch, _total = poll_ydb_image_queue(min(BATCH_SIZE, remaining), wait_seconds=wait_seconds, reason="initial" if batch_index == 0 else "after_drain")
        if not batch:
            log_event("image_queue_poll_finished_empty", phase="poll", reason="initial" if batch_index == 0 else "after_drain", processed=len(all_processed_rows), max_items_per_run=MAX_ITEMS_PER_RUN)
            break
        batch_index += 1
        processed = process_batch(batch, batch_index)
        all_processed_rows.extend(processed)
        remaining -= len(processed)
        wait_seconds = 0
        while remaining > 0:
            batch, _total = poll_ydb_image_queue(min(BATCH_SIZE, remaining), wait_seconds=0, reason="drain_available")
            if not batch:
                wait_seconds = WAIT_AFTER_DRAIN_SECONDS
                break
            batch_index += 1
            processed = process_batch(batch, batch_index)
            all_processed_rows.extend(processed)
            remaining -= len(processed)
    rows = all_processed_rows
else:
    rows = process_batch(rows, 1) if rows else []

actual_rows=[r for r in rows if r.get("final_visual_status")=="scored_actual_image"]
top=sorted(actual_rows,key=lambda r:float(r.get("final_visual_score") or -1),reverse=True)
low=sorted(actual_rows,key=lambda r:float(r.get("final_visual_score") or 999))
disagree=sorted(actual_rows,key=lambda r:float(r.get("model_disagreement_score") or 0),reverse=True)
def timing_stats(field, data):
    vals=[float(r.get(field)) for r in data if r.get(field) not in (None, "")]
    if not vals: return []
    vals_sorted=sorted(vals); p90=vals_sorted[int(0.9*(len(vals_sorted)-1))]
    return [
        {"metric":f"{field}_count","value":len(vals)},
        {"metric":f"{field}_sum","value":round(sum(vals),3)},
        {"metric":f"{field}_mean","value":round(mean(vals),3)},
        {"metric":f"{field}_median","value":round(median(vals),3)},
        {"metric":f"{field}_p90","value":round(p90,3)},
        {"metric":f"{field}_max","value":round(max(vals),3)},
    ]
summary=[
    {"metric":"run_id","value":RUN_ID}, {"metric":"input_rows","value":len(rows)}, {"metric":"actual_images_count","value":len(actual_rows)},
    {"metric":"metadata_only_count","value":len(rows)-len(actual_rows)}, {"metric":"failures_count","value":len(errors)},
    {"metric":"elapsed_seconds","value":round(time.monotonic()-RUN_STARTED,3)}, {"metric":"generated_at","value":datetime.now(timezone.utc).isoformat()},
]
for field in ("media_download_seconds","image_decode_seconds","cv_inference_seconds","clip_inference_seconds","laion_inference_seconds","nima_inference_seconds","total_inference_seconds","total_processing_seconds"):
    summary.extend(timing_stats(field, actual_rows if field!="media_download_seconds" else rows))
if actual_rows:
    proc=[float(r.get("total_processing_seconds") or 0) for r in actual_rows if r.get("total_processing_seconds") not in (None, "")]
    infer=[float(r.get("total_inference_seconds") or 0) for r in actual_rows if r.get("total_inference_seconds") not in (None, "")]
    if proc: summary.append({"metric":"throughput_actual_images_per_min_processing_mean","value":round(60/mean(proc),3)})
    if infer: summary.append({"metric":"throughput_actual_images_per_min_inference_mean","value":round(60/mean(infer),3)})
for k,v in model_availability.items(): summary.append({"metric":f"model_{k}","value":json.dumps(v,ensure_ascii=False)})

def write_sheet(wb,name,data,keys=None):
    ws=wb.create_sheet(name)
    if not data:
        ws.append(["_sheet_note"]); ws.append(["no rows"]); return
    if not keys:
        keys=[]
        for r in data:
            for k in r:
                if k not in keys: keys.append(k)
    ws.append(keys)
    for c in ws[1]: c.font=Font(bold=True); c.fill=PatternFill("solid", fgColor="D9EAF7")
    for r in data: ws.append([json.dumps(r.get(k),ensure_ascii=False) if isinstance(r.get(k),(dict,list)) else r.get(k) for k in keys])
    for i,k in enumerate(keys,1): ws.column_dimensions[get_column_letter(i)].width=min(max(12,len(str(k))+2),45)
wb=Workbook(); wb.remove(wb.active)
write_sheet(wb,"00_summary",summary,["metric","value"]); write_sheet(wb,"01_image_queue_input",rows); write_sheet(wb,"02_scored_images",rows); write_sheet(wb,"03_top_high_score",top[:30]); write_sheet(wb,"04_low_score",low[:30]); write_sheet(wb,"05_model_disagreement",disagree[:30]); write_sheet(wb,"06_errors",errors)
xlsx=OUT/f"{RUN_ID}.xlsx"; wb.save(xlsx)

def rel(path):
    try: return Path(path).relative_to(OUT).as_posix()
    except Exception: return str(path or "")
def card(r,label):
    img=f"<img src='{html.escape(rel(r.get('thumbnail_path')))}'>" if r.get("thumbnail_path") else "<div class='noimg'>no image</div>"
    return f"<a class='card' href='{html.escape(r.get('post_url') or '')}' target='_blank'>{img}<div><b>{label}</b> score {html.escape(str(r.get('final_visual_score','')))} cv {html.escape(str(r.get('cv_overall_media_score','')))} clip {html.escape(str(r.get('clip_postcardness_score','')))} laion {html.escape(str(r.get('laion_aesthetic_score','')))} nima {html.escape(str(r.get('nima_quality_score','')))}<br>time total {html.escape(str(r.get('total_processing_seconds','')))}s infer {html.escape(str(r.get('total_inference_seconds','')))}s<br>{html.escape(r.get('source_title') or '')}<br>{html.escape(r.get('post_url') or '')}</div></a>"
html_doc="""<!doctype html><meta charset='utf-8'><style>body{font-family:Arial,sans-serif;background:#111;color:#eee;margin:24px}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px}.card{display:block;background:#1e1e1e;border:1px solid #444;border-radius:12px;padding:10px;color:#eee;text-decoration:none}img{width:100%;height:210px;object-fit:cover;border-radius:8px}.noimg{height:210px;background:#333;display:flex;align-items:center;justify-content:center}</style>"""
html_doc += f"<h1>Region Talk image diagnostic - {html.escape(RUN_ID)}</h1><p>Actual images: {len(actual_rows)}/{len(rows)}. Models: {html.escape(json.dumps(model_availability,ensure_ascii=False))}</p>"
html_doc += "<h2>Top high score</h2><div class='grid'>" + "".join(card(r,"HIGH") for r in top[:24]) + "</div>"
html_doc += "<h2>Low score</h2><div class='grid'>" + "".join(card(r,"LOW") for r in low[:24]) + "</div>"
html_path=OUT/"contact_sheet.html"; html_path.write_text(html_doc,encoding="utf-8")
summary_md=OUT/"summary.md"
available_names=[k for k,v in model_availability.items() if v.get("available")]
best="blend(" + ", ".join(available_names) + ")" if available_names else "no visual model available"
timing_lines="\n".join(f"- {x['metric']}: {x['value']}" for x in summary if any(s in str(x.get("metric")) for s in ("_mean","_median","_p90","throughput_actual_images_per_min")))
summary_md.write_text(f"""# Region Talk image diagnostic - {RUN_ID}\n\n- Input rows: {len(rows)} from image_candidate_queue.\n- Actual images fetched/decoded/scored: {len(actual_rows)}.\n- Metadata-only/failed rows: {len(rows)-len(actual_rows)}.\n- Elapsed seconds: {round(time.monotonic()-RUN_STARTED,3)}.\n\n## Models that worked\n\n```json\n{json.dumps(model_availability,ensure_ascii=False,indent=2)}\n```\n\n## Timing / throughput\n\n{timing_lines}\n\n## What worked\n\nThis run did not scan sources or comments; it only acquired media for queued image rows and scored actual decoded images.\n\n## What was weak\n\nRows without decoded actual images have no final visual score. Any unavailable model above is recorded with its exact loader error, not substituted with metadata heuristics.\n\n## Visually most convincing scoring\n\nCurrent recommendation: {best}. See `contact_sheet.html`.\n\n## Production recommendations\n\n1. Keep metadata-only rows out of visual ranking.\n2. Package LAION/NIMA weights as stable Kaggle model/input assets if live installs are too slow or flaky.\n3. Use CLIP prompt score as second opinion, not as sole score.\n4. Add screenshot/text/watermark/face/news detectors before publication readiness.\n""",encoding="utf-8")
(OUT/"scored_images.json").write_text(json.dumps({"run_id":RUN_ID,"summary":summary,"model_availability":model_availability,"rows":rows,"errors":errors},ensure_ascii=False,indent=2),encoding="utf-8")
log_event("report_written", phase="report", status="done", run_id=RUN_ID, actual_images=len(actual_rows), rows=len(rows), xlsx=str(xlsx), html=str(html_path), summary=str(summary_md), failures=len(errors), ydb_write="attempted")
