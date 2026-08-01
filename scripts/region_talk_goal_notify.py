#!/usr/bin/env python3
"""Region Talk operator-chat notifier.

Reads Gemini-confirmed ``publication_candidate_item`` rows from YDB and sends
unsent links to the operator Telegram chat.  Delivery may use the bot or one
explicit Region Talk discovery identity.  Generic local E2E/human sessions are
never inputs; the discovery identity is fail-closed while its Kaggle notebook
is active so the same Telegram auth key is not used concurrently.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import html
import io
import json
import os
import random
import re
import fcntl
import subprocess
import sys
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PUBLICATION_ELIGIBILITY_GATE_VERSION = "region_talk_publication_eligibility_v5"
AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION = "region_talk_source_fingerprint_v3"
OPERATOR_REVIEW_PAYLOAD_VERSION = "region_talk_operator_review_payload_v1"
EDITORIAL_WRITER_VERSION = "region_talk_editorial_onboarding_writer_v10_publisher_reader_brief"
EDITORIAL_OUTPUT_CONTRACT = "region_talk_editorial_onboarding_output_v4"
MEDIA_MATERIALIZATION_CONTRACT_VERSION = "region_talk_media_materialization_v1"
PUBLISHER_READER_BRIEF_KIND = "publisher_reader_brief_v1"
PUBLISHER_READER_BRIEF_DIMENSIONS = {
    "outlet_identity", "intended_audience", "distinctive_value",
}

_NOT_TOKEN = r"(?<![A-Za-zА-Яа-яЁё0-9_])[Нн][Ее](?![A-Za-zА-Яа-яЁё0-9_])"
_A_TOKEN = r"[Аа](?![A-Za-zА-Яа-яЁё0-9_])"
_CONTRASTIVE_DELIMITER = r"(?:,|;|:|…|[—–-]|\n(?!\s*\n))\s*(?:[—–-]\s*)?"
_CONTRASTIVE_NOT_A_CLICHE_RE = re.compile(
    rf"(?:{_NOT_TOKEN}(?:(?!\n\s*\n)[^.!?]){{0,900}}?{_CONTRASTIVE_DELIMITER}{_A_TOKEN})"
    # Models sometimes omit the comma entirely; reject that malformed variant
    # inside the same sentence as well.
    rf"|(?:{_NOT_TOKEN}[^.!?\n]{{0,900}}?\s+{_A_TOKEN})"
)
_COMMON_ABBREVIATION_PERIOD_RE = re.compile(
    r"(?<![A-Za-zА-Яа-яЁё0-9_])"
    r"(?:г|ул|д|просп|пер|пос|обл|р-н|им|оз|ст|т\.д|т\.п|т\.е)\."
    r"(?=\s)",
    re.I,
)


def contains_contrastive_not_a_cliche(value: Any) -> bool:
    """Detect the banned Russian adversative template within one sentence.

    The deterministic layer only rejects the style pattern. Rewriting remains
    an LLM-first operation so this guard cannot silently change meaning.
    """

    text = str(value or "")
    # Preserve common abbreviation periods as non-boundaries before the
    # sentence-bounded detector. This catches «не в г. Калининграде, а …»
    # without scanning into a later independent sentence containing «, а».
    text = _COMMON_ABBREVIATION_PERIOD_RE.sub(
        lambda match: match.group(0)[:-1] + "\ue000", text
    )
    return bool(_CONTRASTIVE_NOT_A_CLICHE_RE.search(text))

from scripts.region_talk_review_queue import (  # noqa: E402
    queue_messages,
    queue_snapshot,
    rank_publication_queue,
)
DEFAULT_NOTIFY_CHAT = "https://t.me/+kfaIRh98oHVkYWFi"
DEFAULT_NOTIFY_CHAT_ID = "-5563945596"
DEFAULT_PUBLICATION_SCAN_LIMIT = 5000
TELETHON_TRANSPORT_AUTH_ENVS = {
    "telethon_discovery1": "TELEGRAM_AUTH_BUNDLE_DISCOVERY1",
    "telethon_discovery2": "TELEGRAM_AUTH_BUNDLE_DISCOVERY2",
}
TELETHON_TRANSPORT_KERNELS = {
    "telethon_discovery1": "region-talk-candidate-report",
    "telethon_discovery2": "region-talk-image-diagnostic",
}


@contextmanager
def discovery_session_lease(transport: str):
    """Hold a local single-flight lock for one role-scoped discovery key.

    The remote Kaggle status check prevents local/remote reuse.  This lock
    closes the smaller race between two local agents which both observed the
    notebook as idle and would otherwise connect the same StringSession.
    """

    auth_env = TELETHON_TRANSPORT_AUTH_ENVS.get(str(transport or ""))
    if not auth_env:
        raise RuntimeError(f"unsupported Region Talk Telethon transport: {transport}")
    suffix = auth_env.removeprefix("TELEGRAM_AUTH_BUNDLE_").lower()
    lock_dir = Path(os.getenv("REGION_TALK_TELETHON_LOCK_DIR") or "/tmp")
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"events-bot-region-talk-{suffix}.lock"
    with lock_path.open("w", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"another local Region Talk process owns {auth_env}; refusing concurrent use"
            ) from exc
        assert_telethon_transport_idle(transport)
        yield {"auth_env": auth_env, "lock_path": str(lock_path)}


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


def getenv_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


def ydb_database_name() -> str:
    return (os.getenv("REGION_TALK_YDB_DATABASE_NAME") or "events-bot-acq-discovery").strip() or "events-bot-acq-discovery"


def yc_cli_timeout_seconds() -> int:
    try:
        return max(5, int(os.getenv("REGION_TALK_YC_CLI_TIMEOUT_SECONDS") or "20"))
    except (TypeError, ValueError):
        return 20


def ydb_endpoint_database(*, allow_yc_fallback: bool = True) -> tuple[str, str]:
    endpoint = (os.getenv("REGION_TALK_YDB_ENDPOINT") or "").strip()
    database = (os.getenv("REGION_TALK_YDB_DATABASE") or "").strip()
    if endpoint and database:
        return endpoint.split("?")[0].rstrip("/"), database
    yc = "/home/dev/yandex-cloud/bin/yc"
    if allow_yc_fallback and Path(yc).exists():
        try:
            raw = subprocess.check_output(
                [yc, "ydb", "database", "get", ydb_database_name(), "--format", "json"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=yc_cli_timeout_seconds(),
            )
            data = json.loads(raw)
            import urllib.parse
            url = data["endpoint"]
            return url.split("?")[0].rstrip("/"), urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["database"][0]
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                "Yandex Cloud CLI timed out while resolving Region Talk YDB; "
                "authenticate the existing yc profile or provide direct "
                "REGION_TALK_YDB_ENDPOINT/REGION_TALK_YDB_DATABASE"
            ) from exc
        except Exception:
            pass
    raise RuntimeError("REGION_TALK_YDB_ENDPOINT and REGION_TALK_YDB_DATABASE are required")


def ydb_service_account_key_json() -> str:
    return (os.getenv("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip()


def ydb_access_token() -> str:
    return (os.getenv("REGION_TALK_YDB_IAM_TOKEN") or os.getenv("YC_IAM_TOKEN") or os.getenv("YDB_ACCESS_TOKEN") or "").strip()


def ydb_token(*, allow_yc_fallback: bool = True) -> str:
    token = ydb_access_token()
    if token:
        return token
    yc = "/home/dev/yandex-cloud/bin/yc"
    if allow_yc_fallback and Path(yc).exists():
        try:
            return subprocess.check_output(
                [yc, "iam", "create-token"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=yc_cli_timeout_seconds(),
            ).strip()
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                "Yandex Cloud CLI timed out while minting an IAM token; "
                "interactive browser authentication is required or configure "
                "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON"
            ) from exc
    raise RuntimeError("REGION_TALK_YDB_IAM_TOKEN/YC_IAM_TOKEN/YDB_ACCESS_TOKEN is required")


def ydb_credentials(ydb: Any, *, allow_yc_fallback: bool = True) -> Any:
    token = ydb_access_token()
    if token:
        return ydb.AccessTokenCredentials(token)
    key_json = ydb_service_account_key_json()
    if key_json:
        import tempfile
        import ydb.iam  # type: ignore
        fd, path = tempfile.mkstemp(prefix="region-talk-local-ydb-sa-", suffix=".json")
        os.close(fd)
        try:
            Path(path).write_text(key_json, encoding="utf-8")
            return ydb.iam.ServiceAccountCredentials.from_file(path)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
    if os.getenv("YDB_USER"):
        return ydb.StaticCredentials.from_user_password(os.getenv("YDB_USER"), os.getenv("YDB_PASSWORD", ""))
    if allow_yc_fallback:
        return ydb.AccessTokenCredentials(ydb_token(allow_yc_fallback=True))
    return ydb.credentials_from_env_variables()


def ydb_has_direct_credential() -> bool:
    return bool(ydb_access_token() or ydb_service_account_key_json() or os.getenv("YDB_USER"))


def ensure_ydb_module() -> Any:
    try:
        import ydb  # type: ignore
        return ydb
    except Exception as import_exc:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-q", "ydb[yc]"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            if proc.returncode != 0:
                tail = (proc.stdout or "").strip().splitlines()[-8:]
                raise RuntimeError(
                    "Python package ydb is missing and auto-install failed; "
                    "run the local orchestrator/notifier from a virtualenv with `pip install ydb[yc]`, "
                    "or set REGION_TALK_AUTO_INSTALL=0 after installing dependencies. "
                    "pip_tail=" + " | ".join(tail)[:700]
                ) from import_exc
            import ydb  # type: ignore
            return ydb
        raise


def ydb_table_path(database: str) -> str:
    namespace = re.sub(r"[^A-Za-z0-9_]+", "_", (os.getenv("REGION_TALK_YDB_NAMESPACE") or "region_talk_compact").strip() or "region_talk_compact").strip("_") or "region_talk_compact"
    return database.rstrip("/") + f"/{namespace}_state_kv"


def canonical_source_key_for_row(row: dict[str, Any]) -> str:
    key = str(row.get("canonical_source_key") or "").strip().lower().rstrip("/")
    for prefix in ("source_queue_item:", "source_status_item:", "online_source_item:"):
        if key.startswith(prefix):
            key = key[len(prefix):]
    if key:
        return key
    raw = str(row.get("source_url") or row.get("canonical_url") or row.get("post_url") or "").strip().lower()
    match = re.search(r"(?:https?://)?t\.me/(?:s/)?@?([^/?#]+)", raw)
    return "telegram:" + match.group(1).rstrip("/") if match else raw.rstrip("/")


def authoritative_source_fingerprint(source: dict[str, Any] | None) -> str:
    if not isinstance(source, dict) or not source:
        return ""
    payload = {
        "version": AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
        "canonical_source_key": canonical_source_key_for_row(source),
        "source_queue_status": source.get("source_queue_status") or "",
        "source_scope": source.get("source_scope") or "",
        "source_geo_class": source.get("source_geo_class") or "",
        "source_topic_class": source.get("source_topic_class") or "",
        "source_quick_class": source.get("source_quick_class") or "",
        "monitoring_exclusion_reason": source.get("monitoring_exclusion_reason") or "",
        "source_surface_filter_version": source.get("source_surface_filter_version") or "",
        "source_surface_filter_reason": source.get("source_surface_filter_reason") or "",
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def merge_live_source_rows(source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge canonical source rows with chronology-aware status overlays."""
    sources: dict[str, dict[str, Any]] = {}
    canonical_queue_updated_at: dict[str, str] = {}
    classification_fields = {
        "source_queue_status", "source_scope", "source_geo_class",
        "source_topic_class", "source_quick_class",
        "monitoring_exclusion_reason", "source_surface_filter_version",
        "source_surface_filter_reason", "source_spam_hits",
        "source_hard_spam_hits", "source_commercial_promo_hits",
        "source_spam_hashtags", "next_action",
    }
    for source in source_rows:
        key = canonical_source_key_for_row(source)
        if not key:
            continue
        source_pk = str(source.get("_ydb_pk") or "")
        incoming_is_canonical = source_pk.startswith("source_queue_item:")
        incoming_is_overlay = source_pk.startswith(("source_status_item:", "online_source_item:"))
        incoming_updated_at = max(
            str(source.get("updated_at") or ""),
            str(source.get("queue_item_updated_at") or ""),
            str(source.get("source_status_updated_at") or ""),
            str(source.get("_ydb_updated_at") or ""),
        )
        if incoming_is_canonical:
            canonical_queue_updated_at[key] = incoming_updated_at
        canonical_updated_at = canonical_queue_updated_at.get(key, "")
        current = dict(sources.get(key) or {})
        for field, value in source.items():
            if value in (None, ""):
                continue
            if field in {"posts_scanned", "ko_posts_found", "candidate_posts_found"}:
                try:
                    current[field] = max(int(float(current.get(field) or 0)), int(float(value or 0)))
                except (TypeError, ValueError):
                    current[field] = value
                continue
            if (
                field in classification_fields
                and incoming_is_overlay
                and canonical_updated_at
                and incoming_updated_at
                and incoming_updated_at <= canonical_updated_at
            ):
                # Delivery/confirmed metrics must use the same repaired source
                # chronology as CandidateReport and the orchestrator. A stale
                # status row may add counters, but cannot reapply an obsolete
                # terminal verdict over a newer canonical queue repair.
                continue
            if field == "source_queue_status":
                existing = str(current.get(field) or "")
                incoming = str(value or "")
                if existing.startswith("rejected_") and not incoming.startswith("rejected_"):
                    continue
            if field in {"source_scope", "source_geo_class", "source_quick_class"}:
                local_values = {"local_region", "kaliningrad_local", "local_region_source"}
                if str(current.get(field) or "") in local_values and str(value or "") not in local_values:
                    continue
            current[field] = value
        sources[key] = current
    return list(sources.values())


def attach_live_source_fingerprints(publications: list[dict[str, Any]], source_rows: list[dict[str, Any]]) -> None:
    sources = {
        canonical_source_key_for_row(source): source
        for source in merge_live_source_rows(source_rows)
        if canonical_source_key_for_row(source)
    }
    for row in publications:
        source = sources.get(canonical_source_key_for_row(row))
        row["_live_authoritative_source_fingerprint"] = authoritative_source_fingerprint(source)
        row["_live_authoritative_source_found"] = str(bool(source)).lower()


def publication_scan_limit(send_limit: int) -> int:
    """Scan the ledger before applying the much smaller delivery batch limit.

    Publication rows are ordered by YDB primary key, not by readiness or
    recency. Reading only ``send_limit * 5`` rows can hide a confirmed unsent
    candidate behind older tombstones after the ledger grows.
    """
    configured = int(os.getenv("REGION_TALK_NOTIFY_PUBLICATION_SCAN_LIMIT") or DEFAULT_PUBLICATION_SCAN_LIMIT)
    return max(DEFAULT_PUBLICATION_SCAN_LIMIT, configured, max(1, int(send_limit)) * 5)


def read_publication_rows(limit: int) -> tuple[Any, Any, Any, str, list[dict[str, Any]]]:
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    out = read_kind_rows(pool, ydb, table, "publication_candidate_item", publication_scan_limit(limit))
    source_limit = max(5000, int(os.getenv("REGION_TALK_NOTIFY_SOURCE_SCAN_LIMIT") or "20000"))
    source_rows = read_kind_rows(pool, ydb, table, "source_queue_item", source_limit)
    source_rows += read_kind_rows(pool, ydb, table, "source_status_item", source_limit)
    source_rows += read_kind_rows(pool, ydb, table, "online_source_item", source_limit)
    # Editorial/academic publishers live in their own attestation kind and are
    # intentionally not added to the Telegram/VK source scan queue. They must
    # still participate in the same live fingerprint check before delivery.
    source_rows += read_kind_rows(pool, ydb, table, "external_publication_source_item", source_limit)
    attach_live_source_fingerprints(out, source_rows)
    out.sort(key=lambda r: (int(r.get("publication_rank") or 999999), -float(r.get("publication_score") or 0)))
    return ydb, driver, pool, table, out


def attach_latest_bge_vectors(publications: list[dict[str, Any]], vector_rows: list[dict[str, Any]]) -> None:
    """Attach only the newest durable compatible-looking BGE row per URL.

    Full contract compatibility is checked later by the queue ranker.  Keeping
    that check in one place prevents a missing/mismatched vector from silently
    being called semantic diversity.
    """
    latest: dict[str, dict[str, Any]] = {}
    for row in vector_rows:
        if str(row.get("model_short") or "").lower() != "bge_m3" and "bge-m3" not in str(row.get("model_id") or "").lower():
            continue
        url = canonical_post_url(row)
        if not url or not row.get("embedding_vector_f16_b64"):
            continue
        current = latest.get(url)
        if current is None or str(row.get("created_at") or row.get("updated_at") or "") >= str(current.get("created_at") or current.get("updated_at") or ""):
            latest[url] = row
    fields = (
        "embedding_vector_f16_b64", "embedding_vector_encoding", "embedding_dim",
        "model_id", "encoder_contract", "text_hash", "semantic_bank_version",
    )
    for publication in publications:
        vector = latest.get(canonical_post_url(publication))
        if vector:
            publication.update({field: vector.get(field) for field in fields if vector.get(field) not in (None, "")})


def _json_row_payload(row: Any) -> dict[str, Any]:
    payload = row.payload_json
    return json.loads(payload) if isinstance(payload, str) else dict(payload or {})


def read_kind_rows(pool: Any, ydb: Any, table: str, kind: str, limit: int) -> list[dict[str, Any]]:
    if not re.fullmatch(r"[A-Za-z0-9_:-]+", kind):
        raise ValueError(f"unsafe YDB kind: {kind!r}")
    out = []
    max_items = max(1, int(limit))
    try:
        configured_page = int(os.getenv("REGION_TALK_YDB_SELECT_PAGE_SIZE", "200") or "200")
    except Exception:
        configured_page = 200
    page_size = max(1, min(500, configured_page, max_items))
    prefix = kind + ":"
    prefix_upper = kind + ";"
    after = prefix
    while len(out) < max_items:
        q = (
            f"DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; DECLARE $after AS Utf8; "
            f"SELECT pk, payload_json FROM `{table}` "
            f"WHERE pk >= $prefix AND pk < $prefix_upper AND pk > $after "
            f"ORDER BY pk LIMIT {min(page_size, max_items - len(out))};"
        )
        def op(session: Any):
            query = session.prepare(q)
            return session.transaction(ydb.StaleReadOnly()).execute(
                query,
                {"$prefix": prefix, "$prefix_upper": prefix_upper, "$after": after},
                commit_tx=True,
            )
        rows = pool.retry_operation_sync(op)[0].rows
        if not rows:
            break
        for row in rows:
            after = str(row.pk)
            item = _json_row_payload(row)
            item["_ydb_pk"] = after
            out.append(item)
        if len(rows) < page_size:
            break
    return out


def is_confirmed_publication(row: dict[str, Any]) -> bool:
    """Backward-compatible live-YDB confirmed marker.

    Older finalizer rows used `publication_status=gemini_accept`; newer rows use
    the more explicit `publication_candidate_status=llm_confirmed|sent_to_chat`.
    Treat both as confirmed so notification stats and sending do not depend on
    an operator XLSX/report-tail rewrite.
    """
    if str(row.get("publication_tombstone") or "").lower() == "true" or str(row.get("publication_revoked") or "").lower() == "true":
        return False
    if str(row.get("publication_eligibility_verdict") or "").lower() != "eligible":
        return False
    if str(row.get("publication_eligibility_gate_version") or "") != PUBLICATION_ELIGIBILITY_GATE_VERSION:
        return False
    if str(row.get("authoritative_source_fingerprint_version") or "") != AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION:
        return False
    stored_fingerprint = str(row.get("authoritative_source_fingerprint") or "")
    live_fingerprint = str(row.get("_live_authoritative_source_fingerprint") or "")
    if not stored_fingerprint or not live_fingerprint or stored_fingerprint != live_fingerprint:
        return False
    candidate_status = str(row.get("publication_candidate_status") or "")
    publication_status = str(row.get("publication_status") or "")
    return candidate_status in {"llm_confirmed", "sent_to_chat", "accepted_for_publication"} or publication_status == "gemini_accept"


def is_unsent_confirmed_publication(row: dict[str, Any]) -> bool:
    if not is_confirmed_publication(row):
        return False
    if not is_publication_draft_ready(row):
        return False
    was_sent = (
        str(row.get("publication_candidate_status") or "") == "sent_to_chat"
        or str(row.get("sent_to_chat") or "").lower() == "true"
    )
    if not was_sent:
        return True
    # Before the publication-readiness gate, legacy candidates were marked as
    # delivered even though their operator-ready copy did not exist yet. A bare
    # sent_to_chat flag therefore cannot suppress the first completed draft.
    delivered_fingerprint = str(row.get("sent_operator_review_fingerprint") or "").strip()
    if delivered_fingerprint:
        return delivered_fingerprint != publication_operator_review_fingerprint(row)
    # Compatibility for deliveries made before review identity included media.
    # They remain acknowledged only while no presentation/media manifest exists.
    delivered_draft = str(row.get("sent_publication_draft_fingerprint") or "").strip()
    return bool(operator_review_media_manifest(row)) or delivered_draft != publication_draft_fingerprint(row)


def is_publication_draft_ready(row: dict[str, Any]) -> bool:
    """Require complete operator copy before chat delivery."""

    if str(row.get("publication_draft_status") or "") != "ready_for_operator_review":
        return False
    prompt_version = str(row.get("publication_draft_prompt_version") or "")
    if prompt_version != EDITORIAL_WRITER_VERSION:
        return False
    if str(row.get("publication_draft_contract_version") or "") != EDITORIAL_OUTPUT_CONTRACT:
        return False
    if not all(str(row.get(field) or "").strip() for field in (
        "publication_draft_title",
        "publication_draft_source_attribution",
        "publication_draft_telegram_text",
        "publication_draft_vk_text",
    )):
        return False
    if any(contains_contrastive_not_a_cliche(row.get(field)) for field in (
        "publication_draft_telegram_text",
        "publication_draft_vk_text",
    )):
        return False
    if str(row.get("content_origin_type") or "") in {
        "editorial_publication", "academic_publication",
    }:
        if not (
            str(row.get("source_onboarding_status") or "") == "ready"
            and str(row.get("source_onboarding_paragraph") or "").strip()
            and str(row.get("source_onboarding_publisher_dimensions_status") or "") == "ready"
            and str(row.get("source_onboarding_summary_kind") or "") == PUBLISHER_READER_BRIEF_KIND
        ):
            return False
        try:
            dimensions = json.loads(str(row.get("source_onboarding_publisher_dimensions_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not (
            isinstance(dimensions, dict)
            and set(dimensions) == PUBLISHER_READER_BRIEF_DIMENSIONS
            and all(
                isinstance(dimensions.get(key), dict)
                and str(dimensions[key].get("text") or "").strip()
                and bool(dimensions[key].get("evidence_ids"))
                for key in PUBLISHER_READER_BRIEF_DIMENSIONS
            )
        ):
            return False
    try:
        points = json.loads(str(row.get("publication_draft_fact_points_json") or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not (isinstance(points, list) and points):
        return False
    draft = str(row.get("publication_draft_telegram_text") or "").strip()
    editorial = re.split(r"\n(?:Источник|Оригинал):", draft, maxsplit=1, flags=re.I)[0].strip()
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", editorial) if part.strip()]
    source_name = str(row.get("publication_draft_source_attribution") or row.get("source_title") or "Источник").strip()
    visible_caption = f"{paragraphs[0]}\n\n{paragraphs[1]}\n\nИсточник: {source_name}\nОригинал" if len(paragraphs) == 2 else ""
    if len(paragraphs) != 2 or not (550 <= len(visible_caption) <= 900):
        return False
    media_status = str(row.get("publication_media_materialization_status") or "")
    media_contract = str(row.get("publication_media_materialization_contract_version") or "")
    return bool(
        media_contract == MEDIA_MATERIALIZATION_CONTRACT_VERSION
        and media_status in {"ready", "fallback"}
    )


def publication_draft_fingerprint(row: dict[str, Any]) -> str:
    """Stable identity of the exact operator-ready copy being delivered."""

    payload = {
        "status": str(row.get("publication_draft_status") or "").strip(),
        "title": str(row.get("publication_draft_title") or "").strip(),
        "source_attribution": str(
            row.get("publication_draft_source_attribution") or ""
        ).strip(),
        "telegram_text": str(row.get("publication_draft_telegram_text") or "").strip(),
        "vk_text": str(row.get("publication_draft_vk_text") or "").strip(),
        "fact_points_json": str(
            row.get("publication_draft_fact_points_json") or ""
        ).strip(),
        "prompt_version": str(
            row.get("publication_draft_prompt_version") or ""
        ).strip(),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _canonical_manifest_value(value: Any) -> Any:
    """Return JSON-stable presentation evidence without changing list order."""

    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in {"[", "{"}:
            try:
                return _canonical_manifest_value(json.loads(stripped))
            except (TypeError, ValueError, json.JSONDecodeError):
                return stripped
        return stripped
    if isinstance(value, list):
        return [_canonical_manifest_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _canonical_manifest_value(value[key])
            for key in sorted(value)
            if value[key] not in (None, "")
        }
    return value


def operator_review_media_manifest(row: dict[str, Any]) -> dict[str, Any]:
    """Exact ordered media/presentation surface shown for this review revision.

    Lists deliberately retain order: reordering a carousel is a new operator
    decision even when it contains the same assets.
    """

    manifest_fields = (
        "publication_presentation_manifest_json",
        "publication_media_manifest_json",
        "presentation_manifest_json",
        "selected_media_ids_json",
        "selected_media_materialization_json",
        "media_materialization_items_json",
        "media_manifest_items",
        "publication_media_items",
        "selected_media_ids",
        "selected_media",
        "publication_asset_ids",
    )
    scalar_fields = (
        "input_media_manifest_hash",
        "image_vlm_media_manifest_hash",
        "publication_presentation_mode",
        "publication_visual_strategy",
        "publication_preview_mode",
        "publication_media_layout",
        "publication_primary_image_url",
        "selected_image_url",
        "image_url_or_local_path",
        "media_kind",
        "publication_media_materialization_status",
        "publication_media_materialization_reason",
        "publication_media_materialization_contract_version",
    )
    payload: dict[str, Any] = {}
    for field in (*manifest_fields, *scalar_fields):
        value = row.get(field)
        if value in (None, "", [], {}):
            continue
        payload[field] = _canonical_manifest_value(value)
    return payload


def publication_operator_review_payload(row: dict[str, Any]) -> dict[str, Any]:
    """Binding payload for one exact operator decision."""

    return {
        "version": OPERATOR_REVIEW_PAYLOAD_VERSION,
        "canonical_post_url": canonical_post_url(row),
        "publication_draft_fingerprint": publication_draft_fingerprint(row),
        "media_presentation_manifest": operator_review_media_manifest(row),
    }


def publication_operator_review_fingerprint(row: dict[str, Any]) -> str:
    raw = json.dumps(
        publication_operator_review_payload(row),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_stats_message(limit: int = 20000) -> str:
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    try:
        source_queue = read_kind_rows(pool, ydb, table, "source_queue_item", limit)
        source_status = read_kind_rows(pool, ydb, table, "source_status_item", limit)
        sources = merge_live_source_rows(source_queue + source_status)
        source_candidates = read_kind_rows(pool, ydb, table, "source_candidate_item", limit)
        source_edges = read_kind_rows(pool, ydb, table, "source_edge_item", limit)
        comment_links = read_kind_rows(pool, ydb, table, "comment_link_item", limit)
        posts = read_kind_rows(pool, ydb, table, "processed_post_item", limit)
        candidates = read_kind_rows(pool, ydb, table, "candidate_memory_item", limit)
        images = read_kind_rows(pool, ydb, table, "image_queue_item", limit)
        publications = read_kind_rows(pool, ydb, table, "publication_candidate_item", limit)
        cursors = read_kind_rows(pool, ydb, table, "queue_cursor", 200)
    finally:
        driver.stop()
    rejected_status_prefixes = ("skipped", "error", "reject", "rejected", "debug_self_loop_rejected")
    rejected_sources = [
        r for r in sources
        if str(r.get("fetch_status") or r.get("source_queue_status") or r.get("queue_status") or r.get("frontier_action") or "").startswith(rejected_status_prefixes)
        or bool(str(r.get("monitoring_exclusion_reason") or "").strip())
    ]
    ko_sources = [r for r in sources if int(float(r.get("ko_posts_found") or 0)) > 0]
    attach_live_source_fingerprints(publications, sources)
    actual_images = [r for r in images if str(r.get("image_model_input_type") or "") == "actual_image" or str(r.get("image_queue_status") or "") == "actual_scored"]
    strong_images = [r for r in actual_images if float(r.get("overall_media_score") or r.get("final_visual_score") or 0) >= 0.66]
    confirmed = [r for r in publications if is_confirmed_publication(r)]
    ready_to_send = [r for r in publications if is_unsent_confirmed_publication(r)]
    cursor_by_name: dict[str, dict[str, Any]] = {}
    for row in cursors:
        name = str(row.get("queue_name") or row.get("_ydb_pk") or "").replace("queue_cursor:", "")
        if name and ":" not in name:
            cursor_by_name[name] = row
    cursor_lines = []
    for name in ["source_scan", "unified_source_queue", "source", "image_candidate_queue", "image", "image_diagnostic"]:
        row = cursor_by_name.get(name)
        if not row:
            continue
        pos = row.get("cursor_position") or row.get("done") or 0
        total = row.get("total") or ""
        label = row.get("progress_label") or f"{name}: {pos}" + (f"/{total}" if total else "")
        cursor_lines.append(f"Курсор {name}: {label}")
    return "\n".join([
        "📊 Region Talk live YDB stats",
        f"Каналов/пабликов в базе: {len(sources)}",
        f"Дискавери-кандидатов пабликов: {len(source_candidates)}",
        f"Граф discovery-связей: {len(source_edges)}",
        f"Comment-link discovery rows: {len(comment_links)}",
        f"Каналов отброшено/скрыто/ошибка: {len(rejected_sources)}",
        f"Каналов с постами о Калининградской области: {len(ko_sources)}",
        f"Постов-кандидатов про Калининградскую область: {len(candidates)}",
        f"Постов compact processed: {len(posts)}",
        f"Картинок actual-scored: {len(actual_images)}",
        f"Сильных картинок: {len(strong_images)}",
        f"Gemini-confirmed publication candidates: {len(confirmed)}",
        f"Готово к отправке ссылок: {len(ready_to_send)}",
        *cursor_lines,
        f"updated_at: {datetime.now(timezone.utc).isoformat()}",
    ])


def canonical_post_url(row: dict[str, Any]) -> str:
    raw = str(row.get("post_url") or "").strip().lower().rstrip("/")
    raw = re.sub(r"^https?://(?:www\.)?(?:telegram\.me|t\.me)/s/", "https://t.me/", raw)
    raw = re.sub(r"^https?://(?:www\.)?telegram\.me/", "https://t.me/", raw)
    return raw.split("?", 1)[0].split("#", 1)[0].rstrip("/")


def delivery_random_id(delivery_key: str) -> int:
    value = int.from_bytes(hashlib.sha256(delivery_key.encode("utf-8")).digest()[:8], "big")
    if value >= 2**63:
        value -= 2**64
    return value or 1


def publication_delivery_key(row: dict[str, Any], chat_id: str) -> str:
    identity = "|".join((
        str(chat_id),
        canonical_post_url(row),
        publication_operator_review_fingerprint(row),
    ))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def publication_delivery_review_fields(row: dict[str, Any]) -> dict[str, Any]:
    review_payload = publication_operator_review_payload(row)
    return {
        "operator_review_fingerprint": publication_operator_review_fingerprint(row),
        "operator_review_payload_version": OPERATOR_REVIEW_PAYLOAD_VERSION,
        "operator_review_draft_fingerprint": review_payload["publication_draft_fingerprint"],
        "operator_review_media_manifest_json": json.dumps(
            review_payload["media_presentation_manifest"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "publication_candidate_pk": str(row.get("_ydb_pk") or ""),
        "publication_candidate_id": str(row.get("publication_candidate_id") or ""),
    }


def read_delivery(pool: Any, ydb: Any, table: str, delivery_key: str) -> dict[str, Any]:
    pk = "publication_delivery_item:" + delivery_key
    query_text = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"
    def op(session: Any) -> dict[str, Any]:
        query = session.prepare(query_text)
        result = session.transaction(ydb.StaleReadOnly()).execute(query, {"$pk": pk}, commit_tx=True)
        rows = result[0].rows if result else []
        if not rows:
            return {}
        value = rows[0].payload_json
        return json.loads(value) if isinstance(value, str) else dict(value or {})
    return dict(pool.retry_operation_sync(op) or {})


def upsert_delivery(pool: Any, ydb: Any, table: str, delivery_key: str, payload: dict[str, Any]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    item = {**payload, "delivery_key": delivery_key, "updated_at": now}
    query_text = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);
"""
    def op(session: Any) -> None:
        query = session.prepare(query_text)
        session.transaction(ydb.SerializableReadWrite()).execute(
            query,
            {"$pk": "publication_delivery_item:" + delivery_key, "$kind": "publication_delivery_item", "$payload_json": json.dumps(item, ensure_ascii=False), "$updated_at": now},
            commit_tx=True,
        )
    pool.retry_operation_sync(op)


def upsert_sent(
    pool: Any,
    ydb: Any,
    table: str,
    row: dict[str, Any],
    message_id: int,
    *,
    chat_id: str = "",
    delivery_key: str = "",
    random_id: int = 0,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    item = dict(row)
    pk = str(item.pop("_ydb_pk", "")) or "publication_candidate_item:" + str(item.get("publication_candidate_id") or item.get("post_url"))
    item.update({
        "sent_to_chat": "true",
        "sent_message_id": str(message_id),
        "sent_at": now,
        "sent_chat_id": chat_id,
        "delivery_key": delivery_key,
        "delivery_random_id": str(random_id or ""),
        "sent_publication_draft_fingerprint": publication_draft_fingerprint(item),
        "sent_operator_review_fingerprint": publication_operator_review_fingerprint(item),
        "sent_operator_review_payload_version": OPERATOR_REVIEW_PAYLOAD_VERSION,
        "sent_publication_draft_prompt_version": str(
            item.get("publication_draft_prompt_version") or ""
        ),
        "publication_candidate_status": "sent_to_chat",
    })
    query_text = f"""
DECLARE $pk AS Utf8;
DECLARE $kind AS Utf8;
DECLARE $payload_json AS Json;
DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""
    def op(session: Any) -> None:
        query = session.prepare(query_text)
        session.transaction(ydb.SerializableReadWrite()).execute(
            query,
            {"$pk": pk, "$kind": "publication_candidate_item", "$payload_json": json.dumps(item, ensure_ascii=False), "$updated_at": now},
            commit_tx=True,
        )
    pool.retry_operation_sync(op)


def publication_presentation_manifest(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("publication_presentation_manifest_json")
    try:
        value = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
    except (TypeError, ValueError, json.JSONDecodeError):
        value = {}
    return value if isinstance(value, dict) else {}


def _draft_two_paragraphs(row: dict[str, Any]) -> tuple[str, str]:
    draft = str(row.get("publication_draft_telegram_text") or "").strip()
    editorial = re.split(r"\n(?:Источник|Оригинал):", draft, maxsplit=1, flags=re.I)[0].strip()
    parts = [part.strip() for part in re.split(r"\n\s*\n", editorial) if part.strip()]
    if len(parts) != 2:
        raise RuntimeError("current Region Talk draft is not exactly two editorial paragraphs")
    return parts[0], parts[1]


def public_caption(row: dict[str, Any], *, html_mode: bool = False) -> str:
    """Render the atomic public/review caption with visible attribution."""

    p1, p2 = _draft_two_paragraphs(row)
    if contains_contrastive_not_a_cliche(f"{p1}\n\n{p2}"):
        raise RuntimeError("Region Talk caption contains banned contrastive_not_a_cliche")
    original = str(row.get("post_url") or row.get("canonical_url") or "").strip()
    source_url = str(row.get("source_url") or original).strip()
    source_name = str(row.get("publication_draft_source_attribution") or row.get("source_title") or "Источник").strip()
    if not original or not source_url:
        raise RuntimeError("source/original URL is required for Region Talk caption")
    if html_mode:
        caption = (
            f"{html.escape(p1)}\n\n{html.escape(p2)}\n\n"
            f'<b><a href="{html.escape(source_url, quote=True)}">Источник: {html.escape(source_name)}</a></b>\n'
            f'<b><a href="{html.escape(original, quote=True)}">Оригинал</a></b>'
        )
        visible = f"{p1}\n\n{p2}\n\nИсточник: {source_name}\nОригинал"
    else:
        caption = f"{p1}\n\n{p2}\n\nИсточник: {source_name}\nОригинал: {original}"
        visible = caption
    if not (550 <= len(visible) <= 900):
        raise RuntimeError(f"Region Talk caption must be 550..900 visible chars, got {len(visible)}")
    return caption


def publication_delivery_mode(row: dict[str, Any]) -> str:
    manifest = publication_presentation_manifest(row)
    mode = str(manifest.get("mode") or row.get("publication_presentation_mode") or "")
    if mode not in {"article_hero", "social_hero", "social_album", "social_video", "link_preview_fallback"}:
        raise RuntimeError(f"unsupported Region Talk publication presentation mode: {mode or 'empty'}")
    status = str(manifest.get("status") or row.get("publication_media_materialization_status") or "")
    if mode == "link_preview_fallback":
        if status != "fallback":
            raise RuntimeError("link preview fallback must carry explicit fallback status")
    elif status != "ready" or not list(manifest.get("items") or []):
        raise RuntimeError("media-first review requires an exact ordered materialization manifest")
    return mode


def verify_reviewed_media_digest(data: bytes, item: dict[str, Any]) -> None:
    """Fail closed when refetched bytes differ from the reviewed revision."""

    expected = str(item.get("reviewed_content_sha256") or "").strip().lower()
    if not expected:
        return
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise RuntimeError("media manifest has an invalid reviewed_content_sha256")
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected:
        raise RuntimeError("materialized source media differs from reviewed_content_sha256")


def manifest_item_message_id(item: dict[str, Any]) -> int | None:
    """Return the exact Telegram message id encoded by a manifest media id."""

    # ``hero:1`` is a presentation ordinal, not Telegram message 1.
    match = re.fullmatch(
        r"(?:telegram|tg):([0-9]+)", str(item.get("media_id") or ""), re.I
    )
    return int(match.group(1)) if match else None


async def _telegram_source_media(
    client: Any,
    ref: str,
    media_ids: list[str],
    *,
    max_items: int | None = None,
) -> list[Any]:
    match = re.fullmatch(r"https://t\.me/([^/]+)/([0-9]+)", canonical_post_url({"post_url": ref}), re.I)
    if not match:
        return []
    handle, anchor_id = match.group(1), int(match.group(2))
    exact_ids = []
    for media_id in media_ids:
        found = re.search(r"([0-9]+)$", media_id)
        if found:
            exact_ids.append(int(found.group(1)))
    messages = []
    if exact_ids:
        fetched = await client.get_messages(handle, ids=exact_ids)
        messages = list(fetched if isinstance(fetched, (list, tuple)) else [fetched])
    else:
        anchor = await client.get_messages(handle, ids=anchor_id)
        if anchor is not None and getattr(anchor, "grouped_id", None):
            ids = list(range(max(1, anchor_id - 10), anchor_id + 11))
            nearby = await client.get_messages(handle, ids=ids)
            messages = [item for item in list(nearby or []) if item is not None and getattr(item, "grouped_id", None) == anchor.grouped_id]
            messages.sort(key=lambda item: int(getattr(item, "id", 0) or 0))
        elif anchor is not None:
            messages = [anchor]
    materializable = [message for message in messages if getattr(message, "media", None) is not None]
    # A Telegram source album may contain the platform maximum of ten items,
    # while Region Talk deliberately presents a compact 3..6-frame carousel.
    # Exact reviewed media ids, when present, already encode the visual
    # selection/order.  A source-album locator without such ids uses the first
    # six frames in original source order as a deterministic bounded fallback.
    return materializable[:max_items] if max_items is not None else materializable


async def materialize_telethon_media(client: Any, row: dict[str, Any]) -> list[Any]:
    manifest = publication_presentation_manifest(row)
    items = [dict(item) for item in manifest.get("items") or [] if isinstance(item, dict)]
    if not items:
        return []
    source_refs: dict[str, list[dict[str, Any]]] = {}
    direct_refs: list[dict[str, Any]] = []
    for item in items:
        ref = str(item.get("ref") or "").strip()
        if telegram_post_ref := re.fullmatch(r"https://t\.me/[^/]+/[0-9]+", canonical_post_url({"post_url": ref}), re.I):
            del telegram_post_ref
            source_refs.setdefault(ref, []).append(item)
        else:
            direct_refs.append(item)
    files: list[Any] = []
    for ref, source_items in source_refs.items():
        media_ids = [str(item.get("media_id") or "") for item in source_items]
        messages = await _telegram_source_media(
            client,
            ref,
            media_ids,
            max_items=6 if publication_delivery_mode(row) == "social_album" else None,
        )
        for index, message in enumerate(messages):
            data = await client.download_media(message, file=bytes)
            if not isinstance(data, (bytes, bytearray)) or not data:
                raise RuntimeError("failed to materialize exact Telegram source media bytes")
            message_id = int(getattr(message, "id", 0) or 0)
            item = next(
                (
                    candidate for candidate in source_items
                    if manifest_item_message_id(candidate) == message_id
                ),
                source_items[index] if index < len(source_items) else {},
            )
            verify_reviewed_media_digest(bytes(data), item)
            file = io.BytesIO(bytes(data))
            file.name = "region-talk-source.mp4" if getattr(message, "video", None) else "region-talk-source.jpg"
            files.append(file)
    for item in direct_refs:
        ref = str(item.get("ref") or "").strip()
        kind = str(item.get("kind") or "image")
        if not ref:
            continue
        if Path(ref).is_file():
            data = Path(ref).read_bytes()
        elif re.match(r"https?://", ref, re.I):
            def download() -> bytes:
                with urllib.request.urlopen(ref, timeout=25) as response:
                    payload = response.read(25 * 1024 * 1024 + 1)
                if len(payload) > 25 * 1024 * 1024:
                    raise RuntimeError("Region Talk media exceeds 25 MiB materialization limit")
                return payload
            data = await asyncio.to_thread(download)
        else:
            raise RuntimeError("media manifest item has no materializable URL/path/source post")
        verify_reviewed_media_digest(data, item)
        file = io.BytesIO(data)
        file.name = "region-talk-source.mp4" if kind == "video" else "region-talk-source.jpg"
        files.append(file)
    mode = publication_delivery_mode(row)
    if mode == "social_album" and not (3 <= len(files) <= 6):
        raise RuntimeError(f"social_album must materialize 3..6 ordered items, got {len(files)}")
    if mode in {"article_hero", "social_hero", "social_video"} and len(files) != 1:
        raise RuntimeError(f"{mode} must materialize exactly one item, got {len(files)}")
    return files


def candidate_message(row: dict[str, Any]) -> str:
    if str(row.get("publication_draft_prompt_version") or "") == EDITORIAL_WRITER_VERSION:
        publication_delivery_mode(row)
        # The actual v8 delivery uses linked HTML labels. Validate and render
        # that same visible surface here; counting a long hidden href as plain
        # caption text would reject an otherwise valid operator draft before
        # either Telethon or Bot API can send it.
        return public_caption(row, html_mode=True)
    rank = row.get("publication_rank") or "?"
    publication = row.get("publication") if isinstance(row.get("publication"), dict) else {}
    editorial_pack = row.get("editorial_pack") if isinstance(row.get("editorial_pack"), dict) else {}
    decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
    quality = row.get("quality_assessment") if isinstance(row.get("quality_assessment"), dict) else {}
    url = row.get("post_url") or row.get("canonical_url") or ""
    video_manual = str(row.get("media_review_mode") or "") == "operator_video_review" or str(row.get("media_kind") or "") == "video"
    why = row.get("why_selected") or editorial_pack.get("why_selected") or (
        "текст прошёл строгую E5+BGE и Gemini-проверку; качество видео нужно оценить вручную"
        if video_manual
        else "выбран по тексту, визуальному score и Gemini-проверке"
    )
    summary = row.get("short_summary") or editorial_pack.get("teaser") or ""
    reason = str(row.get("publication_llm_reason") or row.get("llm_reason") or row.get("final_verifier_reason") or decision.get("reason_short") or "")[:280]
    onboarding = (
        str(row.get("source_onboarding_paragraph") or "").strip()
        if str(row.get("source_onboarding_status") or "") == "ready"
        else ""
    )
    editorial = (
        str(row.get("content_origin_type") or "") in {"editorial_publication", "academic_publication"}
        or str(quality.get("track") or "") in {"scholarly", "professional_editorial", "popular_editorial", "reference_or_project_catalog"}
    )
    if editorial and not onboarding:
        onboarding = str(editorial_pack.get("source_overview") or "").strip()
    source_label = "О публикации" if editorial else "О блогере"

    def display_metric(*fields: str) -> Any:
        for field in fields:
            value = row.get(field)
            if value is not None and str(value).strip() != "":
                return value
        return "—"

    publication_score = display_metric("publication_score", "publication_pre_score", "candidate_score")
    if publication_score == "—" and quality.get("normalized_score") is not None:
        publication_score = quality["normalized_score"]
    media_score = display_metric("overall_media_score", "final_visual_score")
    postcard_score = display_metric("postcardness_score", "clip_postcardness_score")
    source_url = str(row.get("source_url") or "").strip()
    if not source_url and publication.get("source_domain"):
        source_url = "https://" + str(publication["source_domain"]).strip().lstrip("/")
    draft = (
        str(row.get("publication_draft_telegram_text") or "").strip()
        if str(row.get("publication_draft_status") or "") == "ready_for_operator_review"
        else ""
    )
    return "\n".join([
        f"✅ Region Talk candidate #{rank}",
        str(url),
        f"Источник: {source_url}" if source_url and source_url != url else "",
        f"Оценка: итог {publication_score} · изображение {media_score} · открыточность {postcard_score}",
        f"Почему: {why}",
        f"{source_label}: {onboarding}" if onboarding else "",
        "🎬 Видео: требуется ручной просмотр" if video_manual else "",
        f"Кратко: {summary}" if summary else "",
        f"Gemini: {reason}" if reason else "",
        f"\n📝 Черновик для Telegram:\n{draft}" if draft else "",
        "\nРеакции редактора: ❤️ или 👍 — одобрить; 👎 — отклонить; ✍️ — нужен новый текст. "
        "Одобрение вместе с ✍️ означает: тема подходит, но текст надо переписать.",
    ]).strip()

async def send_rows(args: argparse.Namespace) -> dict[str, Any]:
    ydb = driver = pool = table = None
    rows: list[dict[str, Any]] = []
    if args.stats:
        messages = [build_stats_message(args.stats_limit)]
    elif args.message:
        messages = [args.message]
    elif args.queue:
        ydb, driver, pool, table, publications = read_publication_rows(max(args.limit, 20))
        vectors = read_kind_rows(pool, ydb, table, "text_vector_enrichment_item", int(args.vector_scan_limit))
        history = read_kind_rows(pool, ydb, table, "publication_semantic_history_item", int(args.history_limit))
        attach_latest_bge_vectors(publications, vectors)
        attach_latest_bge_vectors(history, vectors)
        eligible = [
            row for row in publications
            if is_confirmed_publication(row) and is_publication_draft_ready(row)
        ]
        ranked = rank_publication_queue(
            eligible,
            history=history,
            limit=args.limit,
            diversity_weight=args.diversity_weight,
            adjacency_threshold=args.adjacency_threshold,
        )
        snapshot = queue_snapshot(ranked, requested_by="region_talk_goal_notify")
        messages = queue_messages(snapshot)
        # Queue rendering is deliberately read-only: it must not mutate the
        # candidate delivery ledger or mark any item sent_to_chat.
        rows = []
    else:
        ydb, driver, pool, table, rows = read_publication_rows(args.limit)
        deduped: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not is_unsent_confirmed_publication(row):
                continue
            key = canonical_post_url(row)
            if key and key not in deduped:
                deduped[key] = row
        rows = list(deduped.values())[: args.limit]
        messages = [candidate_message(r) for r in rows]

    # Rendering is read-only and never connects Telegram.
    if args.dry_run:
        if driver is not None:
            driver.stop()
        return {
            "ok": True,
            "sent": [
                {
                    "dry_run": True,
                    "text": text[:120],
                    "post_url": rows[idx].get("post_url") if idx < len(rows) else "",
                }
                for idx, text in enumerate(messages)
            ],
            "sent_count": len(messages),
            "dry_run": True,
            "resolved_chat_id": str(args.expected_chat_id or ""),
            "delivery_account_id": "",
            "transport": str(args.transport),
        }

    try:
        if args.transport in TELETHON_TRANSPORT_AUTH_ENVS:
            return await send_rows_telethon(
                args,
                messages=messages,
                rows=rows,
                ydb=ydb,
                driver=driver,
                pool=pool,
                table=table,
            )
        return await send_rows_bot_api(
            args,
            messages=messages,
            rows=rows,
            ydb=ydb,
            driver=driver,
            pool=pool,
            table=table,
        )
    finally:
        if driver is not None:
            driver.stop()


def _bot_api_call(token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            error = json.loads(exc.read().decode("utf-8"))
        except Exception:
            error = {}
        raise RuntimeError(
            f"telegram_bot_api_{method}_{int(exc.code)}: "
            f"{str(error.get('description') or exc.reason)[:300]}"
        ) from exc
    if not isinstance(data, dict) or not data.get("ok"):
        raise RuntimeError(
            f"telegram_bot_api_{method}_failed: "
            f"{str(data.get('description') if isinstance(data, dict) else 'invalid response')[:300]}"
        )
    result = data.get("result")
    return result if isinstance(result, dict) else {"value": result}


def decode_discovery_bundle(value: str) -> dict[str, Any]:
    """Decode only a role-scoped Region Talk discovery auth bundle."""

    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError("selected Region Talk discovery auth bundle is empty")
    try:
        padded = raw + "=" * ((4 - len(raw) % 4) % 4)
        bundle = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise RuntimeError("selected Region Talk discovery auth bundle is not valid base64 JSON") from exc
    if not isinstance(bundle, dict) or not str(bundle.get("session") or "").strip():
        raise RuntimeError("selected Region Talk discovery auth bundle has no StringSession")
    return bundle


def assert_telethon_transport_idle(transport: str) -> dict[str, str]:
    """Fail closed unless the selected auth bundle's remote kernel is idle."""

    kernel = TELETHON_TRANSPORT_KERNELS.get(str(transport or ""))
    if not kernel:
        raise RuntimeError(f"unsupported Region Talk Telethon transport: {transport}")
    username = str(os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME is required for Telegram auth single-flight verification")
    from scripts.region_talk_orchestrator import (  # imported lazily for CLI startup
        ACTIVE_KERNEL_STATUSES,
        read_kaggle_kernel_statuses,
    )

    statuses = read_kaggle_kernel_statuses(username)
    status = str(statuses.get(kernel) or "").upper()
    if not status or status.startswith("UNVERIFIED"):
        raise RuntimeError(f"cannot verify that {kernel} is idle; refusing shared auth bundle")
    if status in ACTIVE_KERNEL_STATUSES:
        raise RuntimeError(f"{kernel} is {status}; refusing concurrent use of its Telegram auth bundle")
    return statuses


def _telethon_result_message_id(result: Any, random_id: int) -> int:
    """Extract the server message id from Telethon's Updates response."""

    for update in list(getattr(result, "updates", None) or []):
        if int(getattr(update, "random_id", 0) or 0) == int(random_id):
            value = int(getattr(update, "id", 0) or 0)
            if value:
                return value
        message = getattr(update, "message", None)
        value = int(getattr(message, "id", 0) or 0)
        if value:
            return value
    return int(getattr(result, "id", 0) or 0)


async def _telethon_client_and_chat(args: argparse.Namespace) -> tuple[Any, Any, str, str]:
    transport = str(args.transport or "")
    auth_env = TELETHON_TRANSPORT_AUTH_ENVS.get(transport)
    if not auth_env:
        raise RuntimeError(f"unsupported Region Talk Telethon transport: {transport}")
    assert_telethon_transport_idle(transport)
    bundle = decode_discovery_bundle(str(os.getenv(auth_env) or ""))
    api_id = str(os.getenv("TELEGRAM_API_ID") or os.getenv("TG_API_ID") or "").strip()
    api_hash = str(os.getenv("TELEGRAM_API_HASH") or os.getenv("TG_API_HASH") or "").strip()
    if not api_id or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID/API_HASH (or TG_ aliases) are required")
    try:
        from telethon import TelegramClient, functions, utils  # type: ignore
        from telethon.sessions import StringSession  # type: ignore
    except Exception as exc:
        raise RuntimeError("Telethon is required for Region Talk discovery-session delivery") from exc

    client = TelegramClient(
        StringSession(str(bundle["session"])),
        int(api_id),
        api_hash,
        request_retries=0,
        connection_retries=0,
        retry_delay=0,
        auto_reconnect=False,
        flood_sleep_threshold=0,
        raise_last_call_error=True,
        receive_updates=False,
        sequential_updates=True,
        device_model=str(bundle.get("device_model") or "Region Talk delivery"),
        system_version=str(bundle.get("system_version") or "Linux"),
        app_version=str(bundle.get("app_version") or "1.0"),
        lang_code=str(bundle.get("lang_code") or "ru"),
        system_lang_code=str(bundle.get("system_lang_code") or "ru"),
    )
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError(f"{auth_env} is not authorized")
        me = await client.get_me()
        expected_chat_id = str(args.expected_chat_id or "").strip()
        if not re.fullmatch(r"-\d+", expected_chat_id):
            raise RuntimeError("numeric REGION_TALK_NOTIFY_CHAT_ID is required for Telethon delivery")
        try:
            peer = await client.get_input_entity(int(expected_chat_id))
        except Exception as direct_exc:
            match = re.search(r"t\.me/(?:joinchat/|\+)([A-Za-z0-9_-]+)", str(args.chat or ""))
            if not match:
                raise RuntimeError("discovery session cannot resolve the configured operator chat id") from direct_exc
            invite = await client(functions.messages.CheckChatInviteRequest(hash=match.group(1)))
            chat = getattr(invite, "chat", None)
            if chat is None:
                raise RuntimeError("discovery session is not a member of the configured operator chat") from direct_exc
            peer = await client.get_input_entity(chat)
        resolved_chat_id = str(utils.get_peer_id(peer))
        if resolved_chat_id != expected_chat_id:
            raise RuntimeError(
                f"resolved Region Talk Telethon chat id {resolved_chat_id} does not match expected {expected_chat_id}"
            )
        return client, peer, resolved_chat_id, str(getattr(me, "id", "") or "")
    except Exception:
        await client.disconnect()
        raise


async def send_rows_telethon(
    args: argparse.Namespace,
    *,
    messages: list[str],
    rows: list[dict[str, Any]],
    ydb: Any,
    driver: Any,
    pool: Any,
    table: str | None,
) -> dict[str, Any]:
    """Deliver with a role-scoped discovery identity and stable random ids."""

    del driver  # The caller owns the driver lifecycle.
    from telethon import functions  # type: ignore

    with discovery_session_lease(str(args.transport)):
        client, peer, chat_id, account_id = await _telethon_client_and_chat(args)
        sent: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        try:
            for idx, text in enumerate(messages):
                row = rows[idx] if idx < len(rows) else None
                current_editorial = bool(
                    row is not None
                    and str(row.get("publication_draft_prompt_version") or "") == EDITORIAL_WRITER_VERSION
                )
                delivery_key = ""
                existing: dict[str, Any] = {}
                persist_delivery = bool(
                    row is not None and ydb is not None and pool is not None and table is not None
                )
                if persist_delivery and row is not None:
                    delivery_key = publication_delivery_key(row, chat_id)
                    existing = read_delivery(pool, ydb, table, delivery_key)
                    random_id = int(existing.get("random_id") or delivery_random_id(delivery_key))
                    if str(existing.get("status") or "") == "delivered":
                        mid = int(existing.get("message_id") or 0)
                        upsert_sent(
                            pool, ydb, table, row, mid,
                            chat_id=chat_id, delivery_key=delivery_key, random_id=random_id,
                        )
                        sent.append({
                            "message_id": mid,
                            "post_url": row.get("post_url"),
                            "delivery_key": delivery_key,
                            "replayed": True,
                        })
                        continue
                    if (
                        str(existing.get("status") or "") == "sending"
                        and str(row.get("publication_draft_prompt_version") or "") == EDITORIAL_WRITER_VERSION
                    ):
                        # High-level Telethon media upload has no caller-owned
                        # random_id. An ambiguous crash must be reconciled,
                        # never duplicated automatically.
                        raise RuntimeError(
                            f"unconfirmed prior Telethon media delivery requires operator reconciliation: {delivery_key}"
                        )
                else:
                    random_id = delivery_random_id(hashlib.sha256(f"{chat_id}|{text}".encode("utf-8")).hexdigest())

                mode = publication_delivery_mode(row) if current_editorial and row is not None else "link_preview_fallback"
                editorial_caption = public_caption(row, html_mode=True) if current_editorial and row is not None else ""
                # Resolve/download and validate the exact reviewed media before
                # persisting an ambiguous `sending` state.  No Telegram send
                # has started yet, so a materialization failure remains safely
                # retryable after its underlying evidence is repaired.
                files: list[Any] = []
                if current_editorial and row is not None and mode != "link_preview_fallback":
                    try:
                        files = await materialize_telethon_media(client, row)
                    except Exception as exc:
                        # The send has not started, so this failure is neither
                        # ambiguous nor a reason to hide every later review
                        # revision in the same bounded batch. Persist exact
                        # evidence for repair and continue fail-closed.
                        reason = f"{type(exc).__name__}: {str(exc)[:500]}"
                        if persist_delivery:
                            upsert_delivery(pool, ydb, table, delivery_key, {
                                **existing,
                                **publication_delivery_review_fields(row),
                                "status": "materialization_failed",
                                "transport": str(args.transport),
                                "post_url": canonical_post_url(row),
                                "chat_id": chat_id,
                                "random_id": str(random_id),
                                "delivery_stage": "pre_send_media_materialization",
                                "failure_reason": reason,
                                "failed_at": datetime.now(timezone.utc).isoformat(),
                            })
                        failed.append({
                            "post_url": canonical_post_url(row),
                            "delivery_key": delivery_key,
                            "reason": reason,
                        })
                        continue

                if persist_delivery and row is not None:
                    upsert_delivery(pool, ydb, table, delivery_key, {
                        **existing,
                        **publication_delivery_review_fields(row),
                        "status": "sending",
                        "transport": str(args.transport),
                        "post_url": canonical_post_url(row),
                        "chat_id": chat_id,
                        "random_id": str(random_id),
                        "delivery_stage": "telegram_send_started",
                        "sending_started_at": datetime.now(timezone.utc).isoformat(),
                    })

                if current_editorial and row is not None and mode != "link_preview_fallback":
                    result = await client.send_file(
                        peer,
                        files[0] if len(files) == 1 else files,
                        caption=editorial_caption,
                        parse_mode="html",
                        force_document=False,
                    )
                    delivered_messages = list(result if isinstance(result, (list, tuple)) else [result])
                    mid = int(getattr(delivered_messages[0], "id", 0) or 0) if delivered_messages else 0
                elif current_editorial and row is not None:
                    result = await client.send_message(
                        peer,
                        editorial_caption,
                        parse_mode="html",
                        link_preview=not getenv_bool("REGION_TALK_NOTIFY_DISABLE_WEB_PREVIEW", False),
                    )
                    mid = int(getattr(result, "id", 0) or 0)
                else:
                    result = await client(functions.messages.SendMessageRequest(
                        peer=peer,
                        message=text,
                        random_id=random_id,
                        no_webpage=getenv_bool("REGION_TALK_NOTIFY_DISABLE_WEB_PREVIEW", False),
                    ))
                    mid = _telethon_result_message_id(result, random_id)
                if not mid:
                    raise RuntimeError("Telethon delivery returned no verifiable message id")
                if row is not None and ydb is not None and pool is not None and table is not None:
                    upsert_delivery(pool, ydb, table, delivery_key, {
                        **existing,
                        **publication_delivery_review_fields(row),
                        "status": "delivered",
                        "transport": str(args.transport),
                        "post_url": canonical_post_url(row),
                        "chat_id": chat_id,
                        "random_id": str(random_id),
                        "message_id": str(mid),
                        "delivered_at": datetime.now(timezone.utc).isoformat(),
                    })
                    upsert_sent(
                        pool, ydb, table, row, mid,
                        chat_id=chat_id, delivery_key=delivery_key, random_id=random_id,
                    )
                sent.append({"message_id": mid, "post_url": row.get("post_url") if row else ""})
                if idx + 1 < len(messages):
                    await asyncio.sleep(random.uniform(
                        float(os.getenv("REGION_TALK_NOTIFY_DELAY_MIN_SECONDS") or "2"),
                        float(os.getenv("REGION_TALK_NOTIFY_DELAY_MAX_SECONDS") or "5"),
                    ))
        finally:
            await client.disconnect()
        return {
            "ok": not failed,
            "partial": bool(sent and failed),
            "sent": sent,
            "sent_count": len(sent),
            "failed": failed,
            "failed_count": len(failed),
            "dry_run": False,
            "resolved_chat_id": chat_id,
            "delivery_account_id": account_id,
            "transport": str(args.transport),
        }


async def send_rows_bot_api(
    args: argparse.Namespace,
    *,
    messages: list[str],
    rows: list[dict[str, Any]],
    ydb: Any,
    driver: Any,
    pool: Any,
    table: str | None,
) -> dict[str, Any]:
    """Deliver through the production bot without a remote human session."""

    del driver  # The caller owns the driver lifecycle.
    token = str(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required for Region Talk Bot API delivery")
    chat_id = str(args.expected_chat_id or args.chat or "").strip()
    if not chat_id or "t.me/+" in chat_id or "t.me/joinchat/" in chat_id:
        raise RuntimeError("numeric REGION_TALK_NOTIFY_CHAT_ID is required for Bot API delivery")

    me = await asyncio.to_thread(_bot_api_call, token, "getMe", {})
    chat = await asyncio.to_thread(_bot_api_call, token, "getChat", {"chat_id": chat_id})
    resolved_chat_id = str(chat.get("id") or "")
    if resolved_chat_id != chat_id:
        raise RuntimeError(
            f"resolved Region Talk Bot API chat id {resolved_chat_id} does not match expected {chat_id}"
        )
    sent: list[dict[str, Any]] = []
    for idx, text in enumerate(messages):
        row = rows[idx] if idx < len(rows) else None
        delivery_key = ""
        random_id = 0
        existing: dict[str, Any] = {}
        if row is not None and ydb is not None and pool is not None and table is not None:
            delivery_key = publication_delivery_key(row, chat_id)
            existing = read_delivery(pool, ydb, table, delivery_key)
            random_id = int(existing.get("random_id") or delivery_random_id(delivery_key))
            status = str(existing.get("status") or "")
            if status == "delivered":
                mid = int(existing.get("message_id") or 0)
                upsert_sent(
                    pool, ydb, table, row, mid,
                    chat_id=chat_id, delivery_key=delivery_key, random_id=random_id,
                )
                sent.append({
                    "message_id": mid,
                    "post_url": row.get("post_url"),
                    "delivery_key": delivery_key,
                    "replayed": True,
                })
                continue
            if status == "sending":
                # Bot API has no caller-supplied random_id. Do not risk a
                # duplicate after an ambiguous crash; require reconciliation.
                raise RuntimeError(
                    f"unconfirmed prior Bot API delivery requires operator reconciliation: {delivery_key}"
                )
            upsert_delivery(pool, ydb, table, delivery_key, {
                **existing,
                **publication_delivery_review_fields(row),
                "status": "sending",
                "transport": "bot_api",
                "post_url": canonical_post_url(row),
                "chat_id": chat_id,
                "random_id": str(random_id),
                "sending_started_at": datetime.now(timezone.utc).isoformat(),
            })

        if row is None:
            method = "sendMessage"
            payload = {"chat_id": chat_id, "text": text}
        elif str(row.get("publication_draft_prompt_version") or "") != EDITORIAL_WRITER_VERSION:
            method = "sendMessage"
            payload = {"chat_id": chat_id, "text": text}
        else:
            mode = publication_delivery_mode(row)
            manifest = publication_presentation_manifest(row)
            if any(
                str(item.get("reviewed_content_sha256") or "").strip()
                for item in manifest.get("items") or []
                if isinstance(item, dict)
            ):
                raise RuntimeError(
                    "Bot API URL delivery cannot verify reviewed media bytes; use telethon_discovery2"
                )
            refs = [str(item.get("ref") or "") for item in manifest.get("items") or [] if isinstance(item, dict)]
            if mode == "link_preview_fallback":
                method = "sendMessage"
                payload = {"chat_id": chat_id, "text": public_caption(row, html_mode=True), "parse_mode": "HTML"}
            elif not refs or any(not re.match(r"https?://", ref, re.I) or "t.me/" in ref.lower() for ref in refs):
                raise RuntimeError("Bot API media delivery requires exact public media URLs; use telethon_discovery2 for source-post materialization")
            elif mode == "social_album":
                method = "sendMediaGroup"
                caption = public_caption(row, html_mode=True)
                payload = {
                    "chat_id": chat_id,
                    "media": [
                        {"type": "photo", "media": ref, **({"caption": caption, "parse_mode": "HTML"} if index == 0 else {})}
                        for index, ref in enumerate(refs)
                    ],
                }
            else:
                method = "sendVideo" if mode == "social_video" else "sendPhoto"
                media_field = "video" if method == "sendVideo" else "photo"
                payload = {"chat_id": chat_id, media_field: refs[0], "caption": public_caption(row, html_mode=True), "parse_mode": "HTML"}
        result = await asyncio.to_thread(_bot_api_call, token, method, payload)
        if method == "sendMediaGroup":
            group = result.get("value") if isinstance(result.get("value"), list) else []
            result = group[0] if group else {}
        mid = int(result.get("message_id") or 0)
        if row is not None and ydb is not None and pool is not None and table is not None:
            upsert_delivery(pool, ydb, table, delivery_key, {
                **existing,
                **publication_delivery_review_fields(row),
                "status": "delivered",
                "transport": "bot_api",
                "post_url": canonical_post_url(row),
                "chat_id": chat_id,
                "random_id": str(random_id),
                "message_id": str(mid),
                "delivered_at": datetime.now(timezone.utc).isoformat(),
            })
            upsert_sent(
                pool, ydb, table, row, mid,
                chat_id=chat_id, delivery_key=delivery_key, random_id=random_id,
            )
        sent.append({"message_id": mid, "post_url": row.get("post_url") if row else ""})
        if idx + 1 < len(messages):
            await asyncio.sleep(random.uniform(
                float(os.getenv("REGION_TALK_NOTIFY_DELAY_MIN_SECONDS") or "2"),
                float(os.getenv("REGION_TALK_NOTIFY_DELAY_MAX_SECONDS") or "5"),
            ))
    return {
        "ok": True,
        "sent": sent,
        "sent_count": len(sent),
        "dry_run": False,
        "resolved_chat_id": resolved_chat_id,
        "delivery_account_id": str(me.get("id") or ""),
        "transport": "bot_api",
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", type=Path, default=Path(".env"))
    ap.add_argument("--chat", default="")
    ap.add_argument("--expected-chat-id", default="")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--message", default="", help="Send a single status message instead of YDB publication candidates")
    ap.add_argument("--stats", action="store_true", help="Send live Region Talk YDB statistics instead of candidate links")
    ap.add_argument("--queue", action="store_true", help="Send a read-only diversified publication queue snapshot")
    ap.add_argument("--stats-limit", type=int, default=20000)
    ap.add_argument("--vector-scan-limit", type=int, default=20000)
    ap.add_argument("--history-limit", type=int, default=5000)
    ap.add_argument("--diversity-weight", type=float, default=0.28)
    ap.add_argument("--adjacency-threshold", type=float, default=0.86)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--transport",
        choices=("bot_api", *TELETHON_TRANSPORT_AUTH_ENVS),
        default=None,
        help="Bot API or a role-scoped, single-flight Region Talk discovery identity",
    )
    args = ap.parse_args()
    load_env(args.env_file)
    args.transport = args.transport or os.getenv("REGION_TALK_NOTIFY_TRANSPORT") or "telethon_discovery2"
    if args.transport not in {"bot_api", *TELETHON_TRANSPORT_AUTH_ENVS}:
        raise RuntimeError(f"unsupported REGION_TALK_NOTIFY_TRANSPORT: {args.transport}")
    args.chat = args.chat or os.getenv("REGION_TALK_NOTIFY_CHAT") or DEFAULT_NOTIFY_CHAT
    args.expected_chat_id = args.expected_chat_id or os.getenv("REGION_TALK_NOTIFY_CHAT_ID") or DEFAULT_NOTIFY_CHAT_ID
    lock_path = Path(os.getenv("REGION_TALK_NOTIFY_LOCK_FILE") or "/tmp/events-bot-region-talk-notify.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another Region Talk notifier already owns the delivery lock") from exc
        result = asyncio.run(send_rows(args))
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
