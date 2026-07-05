from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AcqConfig
from .surface_filters import is_tg_bot_or_service_surface, is_vk_discovery_surface, tg_handle_from_surface, vk_handle_from_surface

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_DIR = PROJECT_ROOT / "kaggle" / "SubscriberAcquisitionDiscovery"
RUNTIME_SCRIPT = RUNTIME_DIR / "subscriber_acquisition_discovery.py"
OUTPUT_FILENAME = "acq_discovery_result.json"
CONFIG_DATASET_CIPHER = os.getenv("ACQ_DISCOVERY_CONFIG_CIPHER", "subscriber-acquisition-discovery-cipher")
CONFIG_DATASET_KEY = os.getenv("ACQ_DISCOVERY_CONFIG_KEY", "subscriber-acquisition-discovery-key")
TERMINAL_COMPLETE = {"COMPLETE", "SUCCEEDED", "SUCCESS"}
TERMINAL_FAILED = {"ERROR", "FAILED", "CANCELED", "CANCELLED", "CANCEL_ACKNOWLEDGED"}
REMOTE_SESSION_COOLDOWN_SECONDS_DEFAULT = 600
DISCOVERY_AUTH_BUNDLE_ENV = "TELEGRAM_AUTH_BUNDLE_DISCOVERY"
LEGACY_S22_AUTH_BUNDLE_ENV = "TELEGRAM_AUTH_BUNDLE_S22"
REQUIRED_COMMENT_RETRIEVAL_MODELS = ["intfloat/multilingual-e5-base", "BAAI/bge-m3"]

TELEGA_IN_KALININGRAD_TG_SEEDS = [
    ("Kaliningrad_jenskiy", "Женский чат Калининград", "https://telega.in/channels/Kaliningrad_jenskiy/card"),
    ("kpkld", "КП Калининград | Новости региона", "https://telega.in/channels/kpkld/card"),
    ("gokaliningrad_ru", "Едем в Калининград", "https://telega.in/channels/gokaliningrad_ru/card"),
    ("kenig01", "Калининград №1", "https://telega.in/channels/kenig01/card"),
    ("Davai_KLD", "А давай в Калининград?!", "https://telega.in/channels/Davai_KLD/card"),
    ("kaliklove", "в Калининграде любят", "https://telega.in/channels/kaliklove/card"),
    ("jobs39", "Работа в Калининграде", "https://telega.in/channels/jobs39/card"),
    ("anons39", "АНОНС 39 Калининград Афиша", "https://telega.in/channels/anons39/card_max"),
    ("nedvizhimostkalinigrad", "Недвижимость Калининград Live", "https://telega.in/channels/nedvizhimostkalinigrad/card"),
    ("remont3939", "Чат. Стройка и ремонт в Калининграде", "https://telega.in/channels/remont3939/card"),
    ("autoclub_kld", "Автоканал Калининград", "https://telega.in/channels/autoclub_kld/card"),
    ("kaliningrad_now_ru", "Калининград Новостной", "https://telega.in/channels/kaliningrad_now_ru/card_max"),
]

ROUTE_CALIBRATION_TG_SEEDS = [
    ("vKalinigrad_recomendations", "Калининград рекомендации", "golden calibration: route/POI recommendation comments"),
]

SMARTIK_KALININGRAD_VK_SEEDS = [
    ("club42481124", "Подслушано в Калининграде (ПВК)", "https://smartik.ru/kaliningrad/group/42481124"),
    ("club31556867", "Типичный Калининград", "https://smartik.ru/kaliningrad/group/31556867"),
    ("club86855358", "Попутчики | Калининград и область", "https://smartik.ru/kaliningrad/group/86855358"),
    ("club80149142", "ЧС - Калининград и область", "https://smartik.ru/kaliningrad/group/80149142"),
    ("club186019893", "ДТП и ЧП | Калининград | KADAUTO", "https://smartik.ru/kaliningrad/group/186019893"),
]
SMARTIK_KALININGRAD_VK_BY_HANDLE = {handle.casefold(): (title, source_url) for handle, title, source_url in SMARTIK_KALININGRAD_VK_SEEDS}

VK_SOCIAL_SEARCH_VK_SEEDS = [
    ("kuda_go_kld", "Куда сходить Калининград", "vk groups.search: Калининград куда сходить"),
    ("club_topplace", "Куда сходить в Калининграде", "vk groups.search: Калининград куда сходить"),
    ("kuda_dety39", "Куда пойти с ребенком в Калининграде", "vk groups.search: Калининград куда сходить"),
    ("kidsreview_kaliningrad", "Куда сходить с ребенком в Калининграде?", "vk groups.search: Калининград куда сходить"),
    ("visit.kaliningrad", "Путеводитель по Калининградской области", "vk groups.search: Калининград что посмотреть"),
    ("peshiytur", "Пеший туризм в Калининграде", "vk groups.search: Калининград куда съездить"),
    ("tourguilde39", "Калининградская гильдия туризма", "vk groups.search: Калининград туристы"),
    ("blog_batsev", "Калининград: туризм, культура, спорт", "vk groups.search: Калининград туристы"),
    ("otextour", "Калининград отдых и экскурсии", "vk groups.search: Калининград отдых"),
]
VK_SOCIAL_SEARCH_VK_BY_HANDLE = {handle.casefold(): (title, source_url) for handle, title, source_url in VK_SOCIAL_SEARCH_VK_SEEDS}




def live_telegram_scan_enabled() -> bool:
    return (os.getenv("ACQ_ENABLE_LIVE_TG_SCAN") or "").strip().lower() in {"1", "true", "yes", "on"}


def _configured_discovery_auth_bundle_env() -> str:
    configured = (os.getenv("ACQ_TELEGRAM_AUTH_BUNDLE_ENV") or "").strip()
    return configured or DISCOVERY_AUTH_BUNDLE_ENV


def _telegram_auth_bundle_env_candidates() -> list[str]:
    out: list[str] = []
    for name in [_configured_discovery_auth_bundle_env(), DISCOVERY_AUTH_BUNDLE_ENV, LEGACY_S22_AUTH_BUNDLE_ENV]:
        clean = str(name or "").strip()
        if clean and clean not in out:
            out.append(clean)
    return out


def discovery_remote_auth_scope() -> str:
    if not live_telegram_scan_enabled():
        return "none"
    for env_name in _telegram_auth_bundle_env_candidates():
        if (os.getenv(env_name) or "").strip():
            return env_name
    if (os.getenv("TG_SESSION") or os.getenv("TELEGRAM_SESSION") or "").strip():
        return "TG_SESSION"
    return _configured_discovery_auth_bundle_env()


def _discovery_resource_lease_key() -> str | None:
    scope = discovery_remote_auth_scope()
    if scope == "none":
        return None
    if scope == "TG_SESSION":
        return "telegram_session:tg_session"
    return f"telegram_session:env:{scope}"


async def ensure_remote_telegram_session_available_for_discovery() -> None:
    if not live_telegram_scan_enabled():
        return
    from remote_telegram_session import raise_if_remote_telegram_session_busy

    await raise_if_remote_telegram_session_busy(
        current_job_type="subscriber_acquisition_discovery",
        current_auth_scope=discovery_remote_auth_scope(),
    )
    await _raise_if_acq_kernel_ref_or_cooldown_busy()


def _remote_session_marker_path() -> Path:
    configured = (os.getenv("ACQ_REMOTE_SESSION_MARKER_PATH") or "").strip()
    if configured:
        return Path(configured)
    path = PROJECT_ROOT / "artifacts" / "run"
    path.mkdir(parents=True, exist_ok=True)
    return path / "subscriber_acquisition_remote_session.json"


def _remote_session_cooldown_seconds() -> int:
    raw = (os.getenv("ACQ_REMOTE_SESSION_COOLDOWN_SECONDS") or "").strip()
    if not raw:
        return REMOTE_SESSION_COOLDOWN_SECONDS_DEFAULT
    try:
        return max(0, int(float(raw)))
    except Exception:
        return REMOTE_SESSION_COOLDOWN_SECONDS_DEFAULT


def _write_remote_session_marker(*, state: str, run_id: str | None = None, kernel_ref: str | None = None) -> None:
    if not live_telegram_scan_enabled():
        return
    path = _remote_session_marker_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "state": state,
        "run_id": run_id,
        "kernel_ref": kernel_ref,
        "auth_scope": discovery_remote_auth_scope(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _remote_session_cooldown_remaining(now: datetime | None = None) -> tuple[int, dict[str, Any] | None]:
    seconds = _remote_session_cooldown_seconds()
    if seconds <= 0:
        return 0, None
    path = _remote_session_marker_path()
    if not path.exists():
        return 0, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        updated = datetime.fromisoformat(str(payload.get("updated_at") or "").replace("Z", "+00:00"))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
    except Exception:
        return 0, None
    now = now or datetime.now(timezone.utc)
    elapsed = max(0.0, (now - updated.astimezone(timezone.utc)).total_seconds())
    remaining = max(0, int(seconds - elapsed))
    return remaining, payload if remaining > 0 else None


async def _raise_if_acq_kernel_ref_or_cooldown_busy() -> None:
    remaining, marker = _remote_session_cooldown_remaining()
    if remaining > 0:
        raise RuntimeError(
            "remote Telegram session cooldown is active after previous acquisition Kaggle run: "
            f"{remaining}s remaining state={(marker or {}).get('state')} run_id={(marker or {}).get('run_id')}"
        )
    try:
        ref = _kernel_ref_from_meta()
        from video_announce.kaggle_client import KaggleClient

        status_payload = await asyncio.to_thread(KaggleClient()._get_api().kernels_status, ref)
        status = _status_to_text(status_payload)
    except Exception:
        return
    if status and status not in TERMINAL_COMPLETE and status not in TERMINAL_FAILED:
        raise RuntimeError(f"acquisition Kaggle kernel ref is still non-terminal: {ref} status={status}")


@dataclass(frozen=True)
class DiscoveryRuntimeResult:
    payload: dict[str, Any]
    output_path: Path
    runner: str
    kernel_ref: str | None = None
    run_id: str | None = None
    status: str | None = None


def _json_env_value(items: list[Any]) -> str:
    return json.dumps([item for item in items if str(item).strip()], ensure_ascii=False)


def _comment_retrieval_models_env_value() -> str:
    """Always ship both supported semantic models to Kaggle.

    A local env override may append experimental models, but cannot accidentally
    turn the live discovery report into a one-model smoke report.
    """
    configured: list[Any] = []
    raw = (os.getenv("ACQ_COMMENT_RETRIEVAL_MODELS_JSON") or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            configured = parsed if isinstance(parsed, list) else [parsed]
        except Exception:
            configured = [raw]
    models: list[str] = []
    for model in [*configured, *REQUIRED_COMMENT_RETRIEVAL_MODELS]:
        clean = str(model or "").strip()
        if clean and clean not in models:
            models.append(clean)
    return json.dumps(models or list(REQUIRED_COMMENT_RETRIEVAL_MODELS), ensure_ascii=False)


def _vk_seed_item_priority(item: dict[str, Any], index: int) -> tuple[int, int]:
    handle = vk_handle_from_surface(url=item.get("url"), handle=item.get("handle"), external_id=item.get("external_id")).casefold()
    source = str(item.get("source") or "").strip().lower()
    if handle in SMARTIK_KALININGRAD_VK_BY_HANDLE:
        return (0, index)
    if handle in VK_SOCIAL_SEARCH_VK_BY_HANDLE:
        return (1, index)
    if source in {"smartik_kaliningrad_catalog", "vk_social_search"}:
        return (2, index)
    return (3, index)


def _approved_seed_urls_from_payload(payload: dict[str, Any]) -> tuple[list[str], list[str]]:
    tg: list[str] = []
    vk_items: list[tuple[int, str, dict[str, Any]]] = []
    for index, item in enumerate(payload.get("surfaces") or []):
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        platform = str(item.get("platform") or "").strip().lower()
        status = str(item.get("status") or "").strip().lower()
        source = str(item.get("source") or "").strip().lower()
        if status not in {"seed", "candidate", "approved", "needs_comment_resolve"} and source not in {"seed", "vk_source"}:
            continue
        if platform == "tg" and is_tg_bot_or_service_surface(url=url, handle=item.get("handle"), external_id=item.get("external_id")):
            continue
        if platform == "vk" and not is_vk_discovery_surface(url=url, handle=item.get("handle"), external_id=item.get("external_id")):
            continue
        if platform == "vk":
            vk_items.append((index, url, item))
        else:
            tg.append(url)
    vk = [url for index, url, item in sorted(vk_items, key=lambda row: _vk_seed_item_priority(row[2], row[0]))]
    return tg, vk


async def collect_runtime_seed_payload(db) -> dict[str, Any]:
    """Collect reviewed/acquisition and existing VK-monitoring surfaces.

    This is mostly read-only DB access. It may only fail-closed obvious
    non-monitoring Telegram bot/service surfaces as rejected, because those
    should never be sent to Kaggle as crawler frontier.
    """
    try:
        from sqlalchemy import select, text
        from models import AcqSurface
    except Exception:
        return {"surfaces": []}
    surfaces: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    async with db.get_session() as session:
        # Keep a separate all-status seen set before appending static catalog
        # seeds. Otherwise a channel already resolved/rejected in a previous run
        # can be reintroduced from Telega.in as a fresh `needs_comment_resolve`
        # seed, making later Kaggle runs loop over the same publics.
        all_rows = (await session.execute(select(AcqSurface).limit(1000))).scalars().all()
        terminal_tg_handles: set[str] = set()
        for row in all_rows:
            key = (str(row.platform or ""), str(row.external_id or row.url or ""))
            if key[0] and key[1]:
                seen.add(key)
            status = str(row.status or "").strip().lower()
            if str(row.platform or "").strip().lower() == "tg" and (
                status.startswith("rejected") or status == "resolved_has_linked_discussion"
            ):
                handle = tg_handle_from_surface(url=row.url, handle=row.handle, external_id=row.external_id).casefold()
                if handle:
                    terminal_tg_handles.add(handle)
        now = datetime.now(timezone.utc)

        def _is_due_for_runtime_seed(row: AcqSurface) -> bool:
            next_scan = row.next_scan_after
            if next_scan is None:
                return True
            if next_scan.tzinfo is None:
                next_scan = next_scan.replace(tzinfo=timezone.utc)
            return next_scan <= now

        def _is_legacy_auto_approved_vk(row: AcqSurface) -> bool:
            if str(row.platform or "").strip().lower() != "vk":
                return False
            status = str(row.status or "").strip().lower()
            source = str(row.source or "").strip().lower()
            risk = row.risk_json if isinstance(row.risk_json, dict) else {}
            reply_policy = str((risk or {}).get("reply_policy") or "").strip().lower()
            return (
                status == "approved"
                and source in {"", "seed", "allowlist", "vk_source", "smartik_kaliningrad_catalog", "vk_social_search"}
                and reply_policy not in {"confirmed_can_reply_after_human_review", "human_approved"}
            )

        rows = [
            row for row in all_rows
            if str(row.status or "").strip().lower() in {"seed", "candidate", "approved", "needs_comment_resolve"}
            and (_is_due_for_runtime_seed(row) or _is_legacy_auto_approved_vk(row))
        ][:250]

        def _surface_priority(row: AcqSurface) -> tuple[int, int, int, float, int]:
            source = str(row.source or "").strip().lower()
            status = str(row.status or "").strip().lower()
            surface_type = str(row.surface_type or "").strip().lower()
            vk_handle = vk_handle_from_surface(url=row.url, handle=row.handle, external_id=row.external_id).casefold() if str(row.platform or "").lower() == "vk" else ""
            is_smartik = vk_handle in SMARTIK_KALININGRAD_VK_BY_HANDLE
            is_social_search = vk_handle in VK_SOCIAL_SEARCH_VK_BY_HANDLE
            is_curated_social = is_smartik or is_social_search
            is_new_frontier = source in {"discovered", "linked_discussion", "tg_monitoring", "tg_monitoring_canonical", "telega_in", "smartik_kaliningrad_catalog", "vk_social_search"} or is_curated_social
            is_discovered_replyable_tg = source in {"discovered", "linked_discussion"}
            is_replyable_tg = str(row.platform or "").lower() == "tg" and surface_type in {"group", "chat", "megagroup", "linked_discussion"}
            needs_tg_comment_resolve = str(row.platform or "").lower() == "tg" and (
                status == "needs_comment_resolve" or surface_type in {"channel", "unknown_public"}
            )
            reach = row.reach_json if isinstance(row.reach_json, dict) else {}
            basis = str((reach or {}).get("basis") or "").strip().lower()
            is_seed_only = basis in {"", "seed_only", "vk_source_seed", "telega_in_seed", "smartik_catalog_seed"}
            next_scan = row.next_scan_after or datetime.min.replace(tzinfo=timezone.utc)
            if next_scan.tzinfo is None:
                next_scan = next_scan.replace(tzinfo=timezone.utc)
            if is_replyable_tg:
                # Linked/discovered comment surfaces are the main discovery
                # output. Keep them ahead of generic catalog seed groups so the
                # next Kaggle run grows through newly found replyable places
                # instead of spending the first replyable budget on static seeds.
                source_rank = 0 if is_discovered_replyable_tg else 1
            else:
                source_rank = 2 if needs_tg_comment_resolve else (3 if is_curated_social else (4 if is_new_frontier else 5))
            return (
                source_rank,
                0 if is_seed_only else 1,
                0 if status == "candidate" else 1,
                next_scan.timestamp(),
                int(row.id or 0),
            )

        def _quota_env(name: str, default: int) -> int:
            raw = os.getenv(name)
            try:
                value = int(str(raw).strip()) if raw not in {None, ""} else default
            except Exception:
                value = default
            return max(0, value)

        scan_quota_limits = {
            "new": _quota_env("ACQ_NEW_SURFACE_SCAN_QUOTA", 10),
            "rescan": _quota_env("ACQ_RESCAN_SURFACE_QUOTA", 15),
            "approved_rescan": _quota_env("ACQ_APPROVED_SURFACE_RESCAN_QUOTA", 5),
        }
        scan_quota_counts = {"new": 0, "rescan": 0, "approved_rescan": 0, "skipped_due_runtime_limit": 0}

        def _scan_quota_bucket(row: AcqSurface) -> str:
            if str(row.status or "").strip().lower() == "approved" and row.last_scan_at is not None:
                return "approved_rescan"
            if row.last_scan_at is not None:
                return "rescan"
            return "new"

        def _reserve_new_static_seed() -> bool:
            if scan_quota_counts["new"] >= scan_quota_limits["new"]:
                scan_quota_counts["skipped_due_runtime_limit"] += 1
                # Keep catalog/monitoring seeds visible in the backlog payload
                # for operator transparency; runtime scan caps still bound the
                # crawler. DB-backed rows above are the primary quota-selected
                # scan set.
                return True
            scan_quota_counts["new"] += 1
            return True

        pending_existing: list[dict[str, Any]] = []
        for row in sorted(rows, key=_surface_priority):
            quota_bucket = _scan_quota_bucket(row)
            if scan_quota_counts[quota_bucket] >= scan_quota_limits[quota_bucket]:
                scan_quota_counts["skipped_due_runtime_limit"] += 1
                continue
            scan_quota_counts[quota_bucket] += 1
            platform = str(row.platform or "").strip().lower()
            if platform == "tg" and is_tg_bot_or_service_surface(url=row.url, handle=row.handle, external_id=row.external_id):
                row.status = "rejected_bot_or_service"
                row.review_note = "Telegram bot/service links are not monitoring surfaces for acquisition discovery."
                session.add(row)
                continue
            if platform == "vk" and not is_vk_discovery_surface(url=row.url, handle=row.handle, external_id=row.external_id):
                row.status = "rejected_non_community"
                row.review_note = "VK album/app/market/away links are not monitoring surfaces for acquisition discovery; explicit id* profile walls are allowed."
                session.add(row)
                continue
            if platform == "vk":
                status = str(row.status or "").strip().lower()
                source = str(row.source or "").strip().lower()
                reach = row.reach_json if isinstance(row.reach_json, dict) else {}
                risk = row.risk_json if isinstance(row.risk_json, dict) else {}
                basis = str((reach or {}).get("basis") or "").strip().lower()
                reply_policy = str((risk or {}).get("reply_policy") or "").strip().lower()
                if _is_legacy_auto_approved_vk(row):
                    row.status = "candidate"
                    if basis in {"", "seed_only", "vk_wall", "vk_source_seed", "smartik_catalog_seed", "vk_social_search_seed"}:
                        row.last_scan_at = None
                        row.next_scan_after = None
                    row.review_note = "Legacy auto-approved VK discovery seed demoted to candidate; scan result must prove comment availability before review."
                    session.add(row)
                vk_handle = vk_handle_from_surface(url=row.url, handle=row.handle, external_id=row.external_id).casefold()
                if vk_handle in SMARTIK_KALININGRAD_VK_BY_HANDLE or vk_handle in VK_SOCIAL_SEARCH_VK_BY_HANDLE:
                    if vk_handle in SMARTIK_KALININGRAD_VK_BY_HANDLE:
                        title, source_url = SMARTIK_KALININGRAD_VK_BY_HANDLE[vk_handle]
                        row.source = "smartik_kaliningrad_catalog"
                        hint = f"Smartik Kaliningrad public catalog seed: {source_url}"
                        basis = "smartik_catalog_seed"
                    else:
                        title, source_url = VK_SOCIAL_SEARCH_VK_BY_HANDLE[vk_handle]
                        row.source = "vk_social_search"
                        hint = f"VK social/search seed: {source_url}"
                        basis = "vk_social_search_seed"
                    row.title = row.title or title
                    row.topic_hint = row.topic_hint or hint
                    reach = row.reach_json if isinstance(row.reach_json, dict) else {}
                    if str((reach or {}).get("basis") or "").strip().lower() in {"", "seed_only"}:
                        row.reach_json = {"confidence": "low", "basis": basis}
                    session.add(row)
            item = row.model_dump()
            item["scan_quota_bucket"] = quota_bucket
            key = (str(item.get("platform") or ""), str(item.get("external_id") or item.get("url") or ""))
            seen.add(key)
            source = str(item.get("source") or "").strip().lower()
            if source in {"discovered", "linked_discussion", "tg_monitoring", "tg_monitoring_canonical", "telega_in", "smartik_kaliningrad_catalog", "vk_social_search"}:
                surfaces.append(item)
            else:
                pending_existing.append(item)

        def _append_tg_monitoring_seed(
            *,
            handle: str,
            title: str | None,
            source: str,
            topic_hint: str,
            reach: dict[str, Any],
            risk: dict[str, Any],
        ) -> bool:
            clean_handle = str(handle or "").strip().strip("@").strip("/")
            if not clean_handle:
                return False
            if clean_handle.casefold() in terminal_tg_handles:
                return False
            external_id = f"tg:{clean_handle}"
            key = ("tg", external_id)
            if key in seen:
                return False
            if not _reserve_new_static_seed():
                return False
            seen.add(key)
            surfaces.append({
                "platform": "tg",
                "surface_type": "unknown_public",
                "url": f"https://t.me/{clean_handle}",
                "title": title or clean_handle,
                "handle": clean_handle,
                "external_id": external_id,
                "status": "needs_comment_resolve",
                "source": source,
                "topic_hint": topic_hint,
                "reach": reach,
                "risk": risk,
                "scan_quota_bucket": "new",
            })
            return True
        tg_source_table_exists = (await session.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='telegram_source'"))).scalar_one_or_none()
        if tg_source_table_exists:
            info = (await session.execute(text("PRAGMA table_info(telegram_source)"))).all()
            columns = {str(row[1]) for row in info}
            select_cols = ["username"]
            for col in ["title", "enabled", "trust_level", "festival_source", "festival_series", "last_scan_at"]:
                if col in columns:
                    select_cols.append(col)
            order_col = "last_scan_at" if "last_scan_at" in columns else "username"
            result = await session.execute(text(
                f"SELECT {', '.join(select_cols)} FROM telegram_source "
                "WHERE COALESCE(enabled, 1)=1 "
                f"ORDER BY {order_col} DESC, username ASC LIMIT 250"
            ))
            for row in result.mappings().all():
                handle = str(row.get("username") or "").strip().strip("@").strip("/")
                if not handle:
                    continue
                title = str(row.get("title") or "").strip() or None
                festival_series = str(row.get("festival_series") or "").strip()
                trust_level = str(row.get("trust_level") or "").strip()
                reach: dict[str, Any] = {"confidence": "low", "basis": "telegram_monitoring_source"}
                if row.get("last_scan_at"):
                    reach["monitor_last_scan_at"] = str(row.get("last_scan_at"))
                _append_tg_monitoring_seed(
                    handle=handle,
                    title=title,
                    source="tg_monitoring",
                    topic_hint="existing Telegram monitoring source; resolve linked discussion/comments before replyability review",
                    reach=reach,
                    risk={
                        "safety_risk": "low",
                        "spam_risk": "unknown",
                        "trust_level": trust_level or None,
                        "festival_source": bool(row.get("festival_source") or festival_series),
                        "festival_series": festival_series or None,
                    },
                )
        for handle, title, source_url in [*ROUTE_CALIBRATION_TG_SEEDS, *TELEGA_IN_KALININGRAD_TG_SEEDS]:
            external_id = f"tg:{handle}"
            key = ("tg", external_id)
            if key in seen:
                continue
            if not _reserve_new_static_seed():
                continue
            seen.add(key)
            surfaces.append({
                "platform": "tg",
                "surface_type": "group" if handle == "vKalinigrad_recomendations" else "unknown_public",
                "url": f"https://t.me/{handle}",
                "title": title,
                "handle": handle,
                "external_id": external_id,
                "status": "candidate" if handle == "vKalinigrad_recomendations" else "needs_comment_resolve",
                "source": "route_calibration" if handle == "vKalinigrad_recomendations" else "telega_in",
                "topic_hint": f"Telegram route/POI calibration seed: {source_url}" if handle == "vKalinigrad_recomendations" else f"Telega.in Kaliningrad regional catalog seed: {source_url}",
                "reach": {"confidence": "low", "basis": "route_calibration_seed" if handle == "vKalinigrad_recomendations" else "telega_in_seed"},
                "risk": {"safety_risk": "low", "spam_risk": "unknown"},
                "scan_quota_bucket": "new",
            })
        try:
            from telegram_sources import canonical_tg_sources
        except Exception:
            canonical_tg_sources = None  # type: ignore[assignment]
        if canonical_tg_sources is not None:
            for spec in canonical_tg_sources():
                _append_tg_monitoring_seed(
                    handle=spec.username,
                    title=spec.username,
                    source="tg_monitoring_canonical",
                    topic_hint="canonical Telegram Monitoring source from docs/features/telegram-monitoring/sources.yml; resolve linked discussion/comments before replyability review",
                    reach={"confidence": "low", "basis": "telegram_monitoring_canonical_source"},
                    risk={
                        "safety_risk": "low",
                        "spam_risk": "unknown",
                        "trust_level": spec.trust_level,
                        "festival_source": bool(spec.festival_series),
                        "festival_series": spec.festival_series,
                    },
                )
        for handle, title, source_url in SMARTIK_KALININGRAD_VK_SEEDS:
            external_id = f"vk:{handle}"
            key = ("vk", external_id)
            if key in seen:
                continue
            if not _reserve_new_static_seed():
                continue
            seen.add(key)
            surfaces.append({
                "platform": "vk",
                "surface_type": "community",
                "url": f"https://vk.com/{handle}",
                "title": title,
                "handle": handle,
                "external_id": external_id,
                "status": "candidate",
                "source": "smartik_kaliningrad_catalog",
                "topic_hint": f"Smartik Kaliningrad public catalog seed: {source_url}",
                "reach": {"confidence": "low", "basis": "smartik_catalog_seed"},
                "risk": {"safety_risk": "low", "spam_risk": "unknown"},
                "scan_quota_bucket": "new",
            })
        for handle, title, source_url in VK_SOCIAL_SEARCH_VK_SEEDS:
            external_id = f"vk:{handle}"
            key = ("vk", external_id)
            if key in seen:
                continue
            if not _reserve_new_static_seed():
                continue
            seen.add(key)
            surfaces.append({
                "platform": "vk",
                "surface_type": "community",
                "url": f"https://vk.com/{handle}",
                "title": title,
                "handle": handle,
                "external_id": external_id,
                "status": "candidate",
                "source": "vk_social_search",
                "topic_hint": f"VK social/search seed: {source_url}",
                "reach": {"confidence": "low", "basis": "vk_social_search_seed"},
                "risk": {"safety_risk": "low", "spam_risk": "unknown"},
                "scan_quota_bucket": "new",
            })
        surfaces.extend(pending_existing)
        await session.commit()
        opp_table_exists = (await session.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='acq_opportunity'"))).scalar_one_or_none()
        if opp_table_exists:
            opp_rows = (await session.execute(text("SELECT context_url FROM acq_opportunity WHERE context_url IS NOT NULL ORDER BY id DESC LIMIT 2000"))).mappings().all()
            seen_urls = [str(row.get("context_url") or "").strip() for row in opp_rows if str(row.get("context_url") or "").strip()]
            if seen_urls:
                surfaces.append({"_kind": "seen_opportunities", "context_urls": seen_urls})
        table_exists = (await session.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='vk_source'"))).scalar_one_or_none()
        if table_exists:
            info = (await session.execute(text("PRAGMA table_info(vk_source)"))).all()
            columns = {str(row[1]) for row in info}
            select_cols = ["group_id"]
            for col in ["screen_name", "name", "owner_type"]:
                if col in columns:
                    select_cols.append(col)
            result = await session.execute(text(f"SELECT {', '.join(select_cols)} FROM vk_source ORDER BY group_id LIMIT 250"))
            for row in result.mappings().all():
                owner_type = str(row.get("owner_type") or "group").strip().lower()
                if owner_type and owner_type != "group":
                    continue
                screen_name = str(row.get("screen_name") or "").strip()
                group_id = row.get("group_id")
                handle = screen_name or (f"club{int(group_id)}" if group_id is not None else "")
                if not handle:
                    continue
                external_id = f"vk:{handle}"
                key = ("vk", external_id)
                if key in seen:
                    continue
                if not _reserve_new_static_seed():
                    continue
                seen.add(key)
                surfaces.append({
                    "platform": "vk",
                    "surface_type": "community",
                    "url": f"https://vk.com/{handle}",
                    "title": row.get("name"),
                    "handle": handle,
                    "external_id": external_id,
                    "status": "candidate",
                    "source": "vk_source",
                    "topic_hint": "existing VK monitoring source",
                    "reach": {"confidence": "low", "basis": "vk_source_seed"},
                    "risk": {"safety_risk": "low", "spam_risk": "unknown"},
                    "scan_quota_bucket": "new",
                })
    seen_payload = [item for item in surfaces if item.get("_kind") == "seen_opportunities"]
    visible_surfaces = [item for item in surfaces if item.get("_kind") != "seen_opportunities"]
    payload: dict[str, Any] = {"surfaces": visible_surfaces}
    if seen_payload:
        payload["seen_opportunities"] = [{"context_url": url} for url in seen_payload[0].get("context_urls", [])]
    if terminal_tg_handles:
        payload["known_terminal_tg_handles"] = sorted(terminal_tg_handles)
    if "scan_quota_limits" in locals() and "scan_quota_counts" in locals():
        payload["scan_quota_stats"] = {
            **{f"{key}_quota": value for key, value in scan_quota_limits.items()},
            **{f"{key}_selected": value for key, value in scan_quota_counts.items()},
        }
    return payload


def _runtime_env_from_config(config: AcqConfig, seed_payload: dict[str, Any]) -> dict[str, str]:
    tg_seeds, vk_seeds = _approved_seed_urls_from_payload(seed_payload)
    env: dict[str, str] = {
        "ACQ_DEFAULT_LINK_TARGET_URL": config.default_link_target_url,
        "ACQ_MAX_SURFACES_PER_RUN": str(config.max_surfaces_per_run),
        "ACQ_MAX_MESSAGES_PER_SURFACE": str(config.max_messages_per_surface),
        "ACQ_MAX_THREADS_PER_SURFACE": str(config.max_threads_per_surface),
        "ACQ_MAX_OPPORTUNITIES_PER_RUN": str(config.max_opportunities_per_run),
        "ACQ_ENABLE_LIVE_TG_SCAN": os.getenv("ACQ_ENABLE_LIVE_TG_SCAN", "0"),
        "ACQ_TELEGRAM_AUTH_BUNDLE_ENV": discovery_remote_auth_scope() if live_telegram_scan_enabled() and discovery_remote_auth_scope() != "TG_SESSION" else _configured_discovery_auth_bundle_env(),
        "ACQ_ENABLE_LLM_GATE": os.getenv("ACQ_ENABLE_LLM_GATE", "1"),
        "ACQ_LLM_MODEL": os.getenv("ACQ_LLM_MODEL", "models/gemma-4-31b-it"),
        "ACQ_GOOGLE_KEY_ENV": os.getenv("ACQ_GOOGLE_KEY_ENV", "GOOGLE_API_KEY3"),
        "ACQ_ALLOW_GOOGLE_KEY_FALLBACKS": os.getenv("ACQ_ALLOW_GOOGLE_KEY_FALLBACKS", "0"),
        "ACQ_LLM_GATE_MIN_RELEVANCE": os.getenv("ACQ_LLM_GATE_MIN_RELEVANCE", "0.85"),
        "ACQ_MAX_LLM_CALLS_PER_RUN": os.getenv("ACQ_MAX_LLM_CALLS_PER_RUN", "200"),
        "ACQ_RUNTIME_DEADLINE_SECONDS": os.getenv("ACQ_RUNTIME_DEADLINE_SECONDS", "5400"),
        "ACQ_MAX_TG_FRONTIER_PER_RUN": os.getenv("ACQ_MAX_TG_FRONTIER_PER_RUN", ""),
        "ACQ_NEW_SURFACE_SCAN_QUOTA": os.getenv("ACQ_NEW_SURFACE_SCAN_QUOTA", "10"),
        "ACQ_RESCAN_SURFACE_QUOTA": os.getenv("ACQ_RESCAN_SURFACE_QUOTA", "15"),
        "ACQ_APPROVED_SURFACE_RESCAN_QUOTA": os.getenv("ACQ_APPROVED_SURFACE_RESCAN_QUOTA", "5"),
        "ACQ_NEW_SURFACES_SELECTED_FOR_SCAN": str((seed_payload.get("scan_quota_stats") or {}).get("new_selected") or ""),
        "ACQ_NEW_SURFACES_SKIPPED_DUE_RUNTIME_LIMIT": str((seed_payload.get("scan_quota_stats") or {}).get("skipped_due_runtime_limit_selected") or ""),
        "ACQ_MAX_TG_CHANNEL_RESOLVES_PER_RUN": os.getenv("ACQ_MAX_TG_CHANNEL_RESOLVES_PER_RUN", ""),
        "ACQ_MAX_TG_CHANNEL_POSTS_FOR_LINKS": os.getenv("ACQ_MAX_TG_CHANNEL_POSTS_FOR_LINKS", "50"),
        "ACQ_TG_SEARCH_QUERIES_JSON": os.getenv("ACQ_TG_SEARCH_QUERIES_JSON", ""),
        "ACQ_TG_SEARCH_MESSAGES_PER_QUERY": os.getenv("ACQ_TG_SEARCH_MESSAGES_PER_QUERY", "0"),
        "ACQ_MAX_VK_SURFACES_PER_RUN": os.getenv("ACQ_MAX_VK_SURFACES_PER_RUN", ""),
        "ACQ_MAX_VK_POSTS_PER_SURFACE": os.getenv("ACQ_MAX_VK_POSTS_PER_SURFACE", ""),
        "ACQ_MAX_VK_COMMENTS_PER_POST": os.getenv("ACQ_MAX_VK_COMMENTS_PER_POST", ""),
        "ACQ_ENABLE_VK_MEMBER_PROFILE_DISCOVERY": os.getenv("ACQ_ENABLE_VK_MEMBER_PROFILE_DISCOVERY", "1"),
        "ACQ_MAX_VK_MEMBER_PROFILES_DISCOVERED_PER_RUN": os.getenv("ACQ_MAX_VK_MEMBER_PROFILES_DISCOVERED_PER_RUN", "50"),
        "ACQ_MAX_VK_MEMBER_PROFILES_PER_GROUP": os.getenv("ACQ_MAX_VK_MEMBER_PROFILES_PER_GROUP", "30"),
        "ACQ_MAX_VK_AUTHOR_PROFILES_DISCOVERED_PER_RUN": os.getenv("ACQ_MAX_VK_AUTHOR_PROFILES_DISCOVERED_PER_RUN", "80"),
        "ACQ_ENABLE_COMMENT_SEMANTIC_RETRIEVAL": os.getenv("ACQ_ENABLE_COMMENT_SEMANTIC_RETRIEVAL", "0"),
        "ACQ_COMMENT_RETRIEVAL_MODELS_JSON": _comment_retrieval_models_env_value(),
        "ACQ_COMMENT_RETRIEVAL_GATE_MODEL": os.getenv("ACQ_COMMENT_RETRIEVAL_GATE_MODEL", ""),
        "ACQ_COMMENT_RETRIEVAL_MAX_LLM_CANDIDATES": os.getenv("ACQ_COMMENT_RETRIEVAL_MAX_LLM_CANDIDATES", "24"),
        "ACQ_COMMENT_RETRIEVAL_MANUAL_SAMPLE_ROWS": os.getenv("ACQ_COMMENT_RETRIEVAL_MANUAL_SAMPLE_ROWS", ""),
        "ACQ_COMMENT_RETRIEVAL_SCORING_METHOD": os.getenv("ACQ_COMMENT_RETRIEVAL_SCORING_METHOD", ""),
        "ACQ_COMMENT_RETRIEVAL_DEVICE": os.getenv("ACQ_COMMENT_RETRIEVAL_DEVICE", ""),
    }
    seen_context_urls = [
        str(item.get("context_url") or "").strip()
        for item in seed_payload.get("seen_opportunities") or []
        if str(item.get("context_url") or "").strip()
    ]
    if seen_context_urls:
        env["ACQ_SEEN_CONTEXT_URLS_JSON"] = _json_env_value(seen_context_urls[:2000])
    terminal_tg_handles = [
        str(handle).strip().strip("/").lower()
        for handle in seed_payload.get("known_terminal_tg_handles") or []
        if str(handle).strip()
    ]
    if terminal_tg_handles:
        env["ACQ_KNOWN_TERMINAL_TG_HANDLES_JSON"] = _json_env_value(sorted(set(terminal_tg_handles))[:5000])
    if tg_seeds:
        env["ACQ_TG_SEEDS_JSON"] = _json_env_value(tg_seeds)
        tg_seed_meta = []
        for item in seed_payload.get("surfaces") or []:
            if not isinstance(item, dict) or str(item.get("platform") or "").strip().lower() != "tg":
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            risk_json = item.get("risk_json") if isinstance(item.get("risk_json"), dict) else {}
            telegram_access = risk_json.get("_telegram_access") if isinstance(risk_json.get("_telegram_access"), dict) else None
            meta = {
                "url": url,
                "handle": item.get("handle"),
                "external_id": item.get("external_id"),
                "surface_type": item.get("surface_type"),
                "source": item.get("source"),
                "title": item.get("title"),
                "topic_hint": item.get("topic_hint"),
                "scan_quota_bucket": item.get("scan_quota_bucket"),
            }
            reach = item.get("reach") or item.get("reach_json")
            if isinstance(reach, dict):
                meta["reach"] = reach
            if telegram_access:
                meta["telegram_access"] = telegram_access
            tg_seed_meta.append(meta)
        if tg_seed_meta:
            env["ACQ_TG_SEED_SURFACES_JSON"] = _json_env_value(tg_seed_meta[:1000])
    if vk_seeds:
        env["ACQ_VK_SEEDS_JSON"] = _json_env_value(vk_seeds)
        vk_seed_meta = []
        for item in seed_payload.get("surfaces") or []:
            if not isinstance(item, dict) or str(item.get("platform") or "").strip().lower() != "vk":
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            meta = {
                "url": url,
                "handle": item.get("handle"),
                "external_id": item.get("external_id"),
                "surface_type": item.get("surface_type"),
                "source": item.get("source"),
                "title": item.get("title"),
                "topic_hint": item.get("topic_hint"),
                "scan_quota_bucket": item.get("scan_quota_bucket"),
            }
            reach = item.get("reach") or item.get("reach_json")
            if isinstance(reach, dict):
                meta["reach"] = reach
            vk_seed_meta.append(meta)
        if vk_seed_meta:
            env["ACQ_VK_SEED_SURFACES_JSON"] = _json_env_value(vk_seed_meta[:1000])
        # Product request: start VK discovery from all existing monitored VK groups.
        # The runtime still applies per-run budgets and uses read-only methods only.
        env["ACQ_VK_ALLOWLIST_JSON"] = os.getenv("ACQ_VK_ALLOWLIST_JSON") or _json_env_value(vk_seeds)
    return env


def _artifact_dir() -> Path:
    path = PROJECT_ROOT / "artifacts" / "codex" / "subscriber-acquisition-discovery"
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_local_discovery_runtime(*, config: AcqConfig, seed_payload: dict[str, Any] | None = None) -> DiscoveryRuntimeResult:
    """Run the Kaggle script locally as explicit dev/test fallback only."""
    if not RUNTIME_SCRIPT.exists():
        raise FileNotFoundError(f"acquisition runtime script not found: {RUNTIME_SCRIPT}")
    seed_payload = seed_payload or {}
    with tempfile.TemporaryDirectory(prefix="acq-discovery-") as tmp:
        output_dir = Path(tmp)
        env = os.environ.copy()
        env["ACQ_OUTPUT_DIR"] = str(output_dir)
        env.update(_runtime_env_from_config(config, seed_payload))
        completed = subprocess.run(
            [sys.executable, str(RUNTIME_SCRIPT)],
            cwd=str(PROJECT_ROOT),
            env=env,
            text=True,
            capture_output=True,
            timeout=int(os.getenv("ACQ_LOCAL_RUNTIME_TIMEOUT_SECONDS") or "180"),
            check=False,
        )
        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()[-2000:]
            raise RuntimeError(f"acquisition local runtime failed rc={completed.returncode}: {stderr}")
        output_path = output_dir / OUTPUT_FILENAME
        if not output_path.exists():
            raise FileNotFoundError(f"acquisition runtime did not write {OUTPUT_FILENAME}")
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        durable_path = _artifact_dir() / OUTPUT_FILENAME
        durable_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return DiscoveryRuntimeResult(payload=payload, output_path=durable_path, runner="local_shadow_runtime")


def _slugify(value: str, *, max_len: int = 48) -> str:
    import re

    raw = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(value or "").strip().lower()).strip("-")
    if not raw:
        raw = "run"
    return raw[:max_len].rstrip("-")


def _compact_slug_component(value: str, *, max_len: int) -> str:
    raw = _slugify(value, max_len=200)
    if len(raw) <= max_len:
        return raw
    tail_len = min(10, max(4, max_len // 3))
    head_len = max(1, max_len - tail_len - 1)
    return f"{raw[:head_len].rstrip('-')}-{raw[-tail_len:].lstrip('-')}"[:max_len].strip("-") or "run"


def _build_dataset_slug(prefix: str, run_id: str) -> str:
    # Kaggle API currently enforces dataset slugs to be at most 50 chars.
    # Keep the run suffix so frequent E2E/prod runs do not collide while using
    # the same split-dataset pattern as Telegram Monitoring.
    safe_run = _slugify(run_id, max_len=18)
    prefix_budget = max(6, 50 - len(safe_run) - 1)
    return f"{_compact_slug_component(prefix, max_len=prefix_budget)}-{safe_run}"[:50].rstrip("-")


def _require_kaggle_username() -> str:
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME not set")
    return username


def _kernel_ref_from_meta() -> str:
    meta_path = RUNTIME_DIR / "kernel-metadata.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    kernel_id = str(meta.get("id") or meta.get("slug") or "subscriber-acquisition-discovery").strip()
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if username:
        slug = kernel_id.split("/", 1)[-1]
        return f"{username}/{slug}"
    return kernel_id


def _build_secrets_payload() -> str:
    keys = [
        "TG_API_ID",
        "TG_API_HASH",
        "VK_ACCESS_TOKEN",
        "VK_ACCESS_TOKEN4",
        "GOOGLE_API_KEY3",
        "GOOGLE_API_KEY_3",
        "GOOGLE_API_LOCALNAME3",
    ]
    configured_google_key = (os.getenv("ACQ_GOOGLE_KEY_ENV") or "GOOGLE_API_KEY3").strip()
    if configured_google_key and configured_google_key not in keys:
        keys.append(configured_google_key)
    if (os.getenv("ACQ_ALLOW_GOOGLE_KEY_FALLBACKS") or "").strip().lower() in {"1", "true", "yes", "on"}:
        for key in ["GOOGLE_API_KEY", "GOOGLE_API_KEY4", "GOOGLE_API_KEY_4"]:
            if key not in keys:
                keys.append(key)
    if live_telegram_scan_enabled():
        auth_scope = discovery_remote_auth_scope()
        if auth_scope == "TG_SESSION":
            for key in ["TG_SESSION", "TELEGRAM_SESSION"]:
                if key not in keys:
                    keys.append(key)
        elif auth_scope != "none" and auth_scope not in keys:
            keys.append(auth_scope)
    payload = {key: os.getenv(key) for key in keys if (os.getenv(key) or "").strip()}
    if live_telegram_scan_enabled():
        auth_scope = discovery_remote_auth_scope()
        required_auth: list[str]
        if auth_scope == "TG_SESSION":
            required_auth = ["TG_SESSION"] if payload.get("TG_SESSION") else ["TELEGRAM_SESSION"]
        else:
            required_auth = [auth_scope]
        missing = [key for key in [*required_auth, "TG_API_ID", "TG_API_HASH"] if not payload.get(key)]
        if missing:
            raise RuntimeError(f"missing Telegram credentials for acquisition Kaggle run ({auth_scope}): {', '.join(missing)}")
    return json.dumps(payload, ensure_ascii=False)


def _create_dataset(client: Any, username: str, slug_suffix: str, title: str, writer) -> str:
    slug = f"{username}/{slug_suffix}"
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        writer(tmp_path)
        (tmp_path / "dataset-metadata.json").write_text(
            json.dumps({"title": title, "id": slug, "licenses": [{"name": "CC0-1.0"}]}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        try:
            client.create_dataset(tmp_path)
        except Exception:
            try:
                client.create_dataset_version(tmp_path, version_notes=f"refresh {slug_suffix}", quiet=True, convert_to_csv=False, dir_mode="zip")
                return slug
            except Exception:
                try:
                    client.delete_dataset(slug, no_confirm=True)
                except Exception:
                    pass
                client.create_dataset(tmp_path)
    return slug


async def _prepare_kaggle_datasets(db: Any, client: Any, *, config_payload: dict[str, str], secrets_payload: str, run_id: str) -> tuple[str, str]:
    from kaggle_status import create_kaggle_run_config, write_kaggle_status_files
    from source_parsing.telegram.split_secrets import encrypt_secret
    from video_announce.kaggle_client import await_dataset_ready

    encrypted, fernet_key = encrypt_secret(secrets_payload)
    username = _require_kaggle_username()
    slug_suffix = _slugify(run_id, max_len=18)
    lease_key = _discovery_resource_lease_key()
    kaggle_run_config = await create_kaggle_run_config(
        db,
        run_id=f"acq_discovery:{run_id}",
        session_id=None,
        kind="subscriber_acquisition_discovery",
        notebook="SubscriberAcquisitionDiscovery",
        resource_leases=[lease_key] if live_telegram_scan_enabled() and lease_key else [],
    )

    def write_cipher(path: Path) -> None:
        (path / "config.json").write_text(json.dumps(config_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        (path / "secrets.enc").write_bytes(encrypted)
        retrieval_module = RUNTIME_DIR / "comment_semantic_retrieval.py"
        if retrieval_module.exists():
            (path / "comment_semantic_retrieval.py").write_text(retrieval_module.read_text(encoding="utf-8"), encoding="utf-8")
        write_kaggle_status_files(path, kaggle_run_config)

    def write_key(path: Path) -> None:
        (path / "fernet.key").write_bytes(fernet_key)

    slug_cipher = _create_dataset(client, username, _build_dataset_slug(CONFIG_DATASET_CIPHER, run_id), f"Acq Discovery Cipher {slug_suffix}", write_cipher)
    slug_key = _create_dataset(client, username, _build_dataset_slug(CONFIG_DATASET_KEY, run_id), f"Acq Discovery Key {slug_suffix}", write_key)
    expected_cipher_files = ["config.json", "secrets.enc"]
    if (RUNTIME_DIR / "comment_semantic_retrieval.py").exists():
        expected_cipher_files.append("comment_semantic_retrieval.py")
    if kaggle_run_config:
        expected_cipher_files.extend(["kaggle_run.json", "kaggle_status_client.py"])
    await await_dataset_ready(client, slug_cipher, expected_files=expected_cipher_files)
    await await_dataset_ready(client, slug_key, expected_files=["fernet.key"])
    return slug_cipher, slug_key


def _push_kaggle_kernel(client: Any, dataset_sources: list[str]) -> str:
    kernel_ref = _kernel_ref_from_meta()
    client.push_kernel(kernel_path=RUNTIME_DIR, dataset_sources=dataset_sources)
    return kernel_ref


def _status_to_text(status_payload: Any) -> str:
    if isinstance(status_payload, str):
        return status_payload.upper()
    if hasattr(status_payload, "to_dict"):
        try:
            status_payload = status_payload.to_dict()
        except Exception:
            pass
    for attr in ["status", "state"]:
        value = getattr(status_payload, attr, None)
        if value:
            if hasattr(value, "name"):
                return str(value.name).upper()
            if hasattr(value, "value"):
                return str(value.value).upper()
            return str(value).rsplit(".", 1)[-1].upper()
    if isinstance(status_payload, dict):
        value = status_payload.get("status") or status_payload.get("state") or ""
        if hasattr(value, "name"):
            return str(value.name).upper()
        return str(value).rsplit(".", 1)[-1].upper()
    return str(status_payload or "").rsplit(".", 1)[-1].upper()


async def _poll_kaggle_kernel(client: Any, kernel_ref: str, *, timeout_seconds: int, poll_interval_seconds: int) -> tuple[str, Any]:
    api = client._get_api()
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    last_status: Any = None
    while time.monotonic() < deadline:
        if hasattr(client, "get_kernel_status"):
            last_status = await asyncio.to_thread(client.get_kernel_status, kernel_ref)
        else:
            last_status = await asyncio.to_thread(api.kernels_status, kernel_ref)
        status = _status_to_text(last_status)
        if status in TERMINAL_COMPLETE:
            return "complete", last_status
        if status in TERMINAL_FAILED:
            return status.lower(), last_status
        await asyncio.sleep(max(1, int(poll_interval_seconds)))
    return "timeout", last_status


def _download_kaggle_output(client: Any, kernel_ref: str, *, run_id: str) -> Path:
    output_dir = _artifact_dir() / f"kaggle-{_slugify(run_id, max_len=24)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    files = client.download_kernel_output(kernel_ref, path=str(output_dir), force=True)
    output_path = output_dir / OUTPUT_FILENAME
    if not output_path.exists():
        # Kaggle may return nested/relative names; search defensively.
        matches = list(output_dir.rglob(OUTPUT_FILENAME))
        if matches:
            output_path = matches[0]
    if not output_path.exists():
        raise FileNotFoundError(f"{OUTPUT_FILENAME} not found in Kaggle output files={files}")
    return output_path


async def run_kaggle_discovery_runtime(db: Any, *, config: AcqConfig, seed_payload: dict[str, Any] | None = None) -> DiscoveryRuntimeResult:
    await ensure_remote_telegram_session_available_for_discovery()
    from kaggle_registry import register_job, remove_job, update_job_meta
    from video_announce.kaggle_client import KaggleClient

    seed_payload = seed_payload or {}
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    config_payload = _runtime_env_from_config(config, seed_payload)
    config_payload["KAGGLE_RUN_ID"] = run_id
    secrets_payload = _build_secrets_payload()
    client = KaggleClient()
    dataset_cipher = dataset_key = kernel_ref = ""
    registered = False
    try:
        _write_remote_session_marker(state="preparing", run_id=run_id)
        dataset_cipher, dataset_key = await _prepare_kaggle_datasets(db, client, config_payload=config_payload, secrets_payload=secrets_payload, run_id=run_id)
        if int(os.getenv("ACQ_KAGGLE_DATASET_WAIT_SECONDS") or "0") > 0:
            await asyncio.sleep(int(os.getenv("ACQ_KAGGLE_DATASET_WAIT_SECONDS") or "0"))
        kernel_ref = _push_kaggle_kernel(client, [dataset_cipher, dataset_key])
        _write_remote_session_marker(state="running", run_id=run_id, kernel_ref=kernel_ref)
        await register_job(
            "subscriber_acquisition_discovery",
            kernel_ref,
            meta={
                "run_id": run_id,
                "pid": os.getpid(),
                "remote_telegram_auth_scope": discovery_remote_auth_scope() if live_telegram_scan_enabled() else None,
                "dataset_slugs": [dataset_cipher, dataset_key],
                "runner": "kaggle",
            },
        )
        registered = True
        status, status_payload = await _poll_kaggle_kernel(
            client,
            kernel_ref,
            timeout_seconds=int(os.getenv("ACQ_KAGGLE_TIMEOUT_SECONDS") or "9000"),
            poll_interval_seconds=int(os.getenv("ACQ_KAGGLE_POLL_INTERVAL_SECONDS") or "30"),
        )
        await update_job_meta("subscriber_acquisition_discovery", kernel_ref, meta_updates={"last_status": status, "last_status_at": datetime.now(timezone.utc).isoformat()})
        if status != "complete":
            try:
                client._get_api().kernels_delete(kernel_ref, no_confirm=True)
                _write_remote_session_marker(state=f"deleted_after_{status}", run_id=run_id, kernel_ref=kernel_ref)
            except Exception:
                pass
            raise RuntimeError(f"Subscriber Acquisition Kaggle kernel did not complete: {status} {status_payload}")
        output_path = _download_kaggle_output(client, kernel_ref, run_id=run_id)
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        _write_remote_session_marker(state="complete", run_id=run_id, kernel_ref=kernel_ref)
        return DiscoveryRuntimeResult(payload=payload, output_path=output_path, runner="kaggle", kernel_ref=kernel_ref, run_id=run_id, status=status)
    except Exception:
        _write_remote_session_marker(state="error", run_id=run_id, kernel_ref=kernel_ref or None)
        raise
    finally:
        if registered and kernel_ref:
            try:
                await remove_job("subscriber_acquisition_discovery", kernel_ref)
            except Exception:
                pass
