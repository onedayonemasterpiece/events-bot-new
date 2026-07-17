from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import random
import re
import subprocess
import sys
import time
import traceback
import unicodedata
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

INPUT_ROOT = Path("/kaggle/input")
OUTPUT = Path("/kaggle/working/social_metrics_results.json")

MONTHS_RU_GEN = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня",
    7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def _find(name: str) -> Path:
    matches = sorted(INPUT_ROOT.rglob(name))
    if not matches:
        raise RuntimeError(f"required input is missing: {name}")
    return matches[0]


def _ensure_libs() -> None:
    missing = []
    for module, package in (("telethon", "telethon"), ("cryptography", "cryptography")):
        try:
            __import__(module)
        except Exception:
            missing.append(package)
    if missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *missing])


def _load_status_client():
    candidate = _find("kaggle_status_client.py")
    spec = importlib.util.spec_from_file_location("events_bot_kaggle_status_client", candidate)
    if not spec or not spec.loader:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.load_status_client(log=lambda value: print(value, flush=True))


def _decrypt() -> dict[str, str]:
    from cryptography.fernet import Fernet

    payload = Fernet(_find("fernet.key").read_bytes().strip()).decrypt(_find("secrets.enc").read_bytes())
    data = json.loads(payload.decode("utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError("invalid secret payload")
    return {str(key): str(value) for key, value in data.items() if str(value)}


def _metric_count(item: dict[str, Any], key: str) -> int | None:
    value = item.get(key)
    if isinstance(value, dict):
        value = value.get("count")
    return int(value) if isinstance(value, int) and value >= 0 else None


def _normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold().replace("ё", "е")
    return re.sub(r"\s+", " ", re.sub(r"[^a-zа-я0-9]+", " ", text)).strip()


def _contains_sequence(tokens: list[str], needle: list[str]) -> bool:
    if not needle or len(needle) > len(tokens):
        return False
    return any(tokens[index:index + len(needle)] == needle for index in range(len(tokens) - len(needle) + 1))


def _match_post(candidate: dict[str, Any], item: dict[str, Any]) -> tuple[int, float]:
    try:
        target_date = date.fromisoformat(str(candidate.get("date") or "")[:10])
    except ValueError:
        return 0, 0.0
    text = str(item.get("text") or "")
    normalized = _normalize_text(text)
    title_tokens = [token for token in _normalize_text(candidate.get("title")).split() if len(token) >= 3 or token.isdigit()][:6]
    if not title_tokens or not _contains_sequence(_normalize_text(text).split(), title_tokens):
        return 0, 0.0
    if _normalize_text(f"{target_date.day} {MONTHS_RU_GEN.get(target_date.month, '')}") not in normalized:
        return 0, 0.0
    score = 2
    time_match = re.search(r"\d{1,2}:\d{2}", str(candidate.get("time") or ""))
    time_anchor = time_match.group(0) if time_match else ""
    if time_anchor and time_anchor in text:
        score += 1
    location_tokens = [token for token in _normalize_text(candidate.get("location_name")).split() if len(token) >= 4][:3]
    if any(token in normalized for token in location_tokens):
        score += 1
    if (time_anchor or location_tokens) and score < 3:
        return 0, 0.0
    return score, min(1.0, 0.45 + 0.18 * score)


def _resolution_row(
    candidate: dict[str, Any],
    *,
    observed_ts: int,
    status: str,
    item: dict[str, Any] | None = None,
    method: str,
    confidence: float = 0.0,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "candidate_id": candidate["candidate_id"],
        "status": status,
        "observed_ts": observed_ts,
        "match_method": method,
        "match_confidence": confidence,
    }
    if item is not None:
        row.update({
            "live_post_id": int(item.get("id") or 0),
            "post_ts": int(item["date"]) if isinstance(item.get("date"), int) else None,
            # Transient bounded evidence lets Fly rerun the canonical strict
            # matcher. It is validated then discarded, never written to SQLite.
            "evidence_text": str(item.get("text") or "")[:16384],
            "views": _metric_count(item, "views"),
            "likes": _metric_count(item, "likes"),
            "comments": _metric_count(item, "comments"),
            "shares": _metric_count(item, "reposts"),
        })
    return row


async def _resolve_vk(candidates: list[dict[str, Any]], secrets: dict[str, str], wall_limit: int) -> list[dict[str, Any]]:
    if not candidates:
        return []
    token = secrets.get("VK_TOKEN", "")
    if not token:
        raise RuntimeError("VK token is missing")
    by_group: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        by_group.setdefault(str(candidate["publisher_id"]), []).append(candidate)
    out: list[dict[str, Any]] = []
    for group, rows in sorted(by_group.items()):
        direct: dict[int, dict[str, Any]] = {}
        direct_failed: set[str] = set()
        for start in range(0, len(rows), 100):
            chunk = rows[start:start + 100]
            if start:
                await asyncio.sleep(0.35)
            try:
                response = await asyncio.to_thread(
                    _vk_request,
                    "wall.getById",
                    token,
                    posts=",".join(f"-{group}_{row['stored_post_id']}" for row in chunk),
                )
                items = response if isinstance(response, list) else response.get("items", [])
                direct.update({int(item.get("id") or 0): item for item in items if isinstance(item, dict)})
            except Exception:
                direct_failed.update(str(row["candidate_id"]) for row in chunk)

        selected: dict[str, tuple[dict[str, Any], str, float]] = {}
        unresolved: list[dict[str, Any]] = []
        for candidate in rows:
            candidate_id = str(candidate["candidate_id"])
            if candidate_id in direct_failed:
                out.append(_resolution_row(candidate, observed_ts=int(time.time()), status="error", method="direct_error"))
                continue
            item = direct.get(int(candidate["stored_post_id"]))
            score, confidence = _match_post(candidate, item) if item else (0, 0.0)
            if item and score > 0:
                selected[candidate_id] = (item, "direct", confidence)
            else:
                unresolved.append(candidate)

        wall_items: list[dict[str, Any]] = []
        wall_error = False
        if unresolved:
            try:
                for offset in range(0, max(100, min(1000, int(wall_limit))), 100):
                    if offset:
                        await asyncio.sleep(0.35)
                    response = await asyncio.to_thread(
                        _vk_request, "wall.get", token,
                        owner_id=f"-{group}", filter="owner", count=100, offset=offset,
                    )
                    chunk = response.get("items", []) if isinstance(response, dict) else []
                    chunk = [item for item in chunk if isinstance(item, dict)]
                    wall_items.extend(chunk)
                    if len(chunk) < 100:
                        break
            except Exception:
                wall_error = True
        for candidate in unresolved:
            if wall_error:
                out.append(_resolution_row(candidate, observed_ts=int(time.time()), status="error", method="wall_scan_error"))
                continue
            best: tuple[int, int, float, dict[str, Any]] | None = None
            for item in wall_items:
                score, confidence = _match_post(candidate, item)
                rank = (score, int(item.get("date") or 0))
                if score > 0 and (best is None or rank > (best[0], best[1])):
                    best = (score, rank[1], confidence, item)
            if best:
                selected[str(candidate["candidate_id"])] = (best[3], "wall_scan", best[2])
            else:
                out.append(_resolution_row(candidate, observed_ts=int(time.time()), status="missing", method="unmatched"))

        claims: dict[int, list[str]] = {}
        for candidate_id, (item, _method, _confidence) in selected.items():
            claims.setdefault(int(item.get("id") or 0), []).append(candidate_id)
        candidates_by_id = {str(row["candidate_id"]): row for row in rows}
        for candidate_id, (item, method, confidence) in selected.items():
            if len(claims.get(int(item.get("id") or 0), [])) > 1:
                out.append(_resolution_row(candidates_by_id[candidate_id], observed_ts=int(time.time()), status="ambiguous", method="ambiguous"))
            else:
                out.append(_resolution_row(
                    candidates_by_id[candidate_id], observed_ts=int(time.time()), status="published",
                    item=item, method=method, confidence=confidence,
                ))
    return sorted(out, key=lambda row: str(row["candidate_id"]))


def _vk_request(method: str, token: str, **params: Any) -> dict[str, Any]:
    body = urllib.parse.urlencode({**params, "access_token": token, "v": "5.199"}).encode()
    request = urllib.request.Request(f"https://api.vk.com/method/{method}", data=body)
    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.load(response)
    if data.get("error"):
        raise RuntimeError(f"VK API {data['error'].get('error_code')}: {data['error'].get('error_msg')}")
    return data.get("response") or {}


async def _collect_vk(targets: list[dict[str, Any]], secrets: dict[str, str], progress) -> list[dict[str, Any]]:
    token = secrets.get("VK_TOKEN", "")
    if targets and not token:
        raise RuntimeError("VK token is missing")
    out = []
    by_group: dict[str, list[dict[str, Any]]] = {}
    for target in targets:
        by_group.setdefault(str(target["publisher_id"]), []).append(target)
    requests_done = 0
    for group, rows in sorted(by_group.items()):
        for start in range(0, len(rows), 100):
            if requests_done:
                await asyncio.sleep(max(0.35, float(secrets.get("VK_BATCH_PAUSE_SECONDS", "0.35"))))
            chunk = rows[start : start + 100]
            observed_ts = int(time.time())
            try:
                response = await asyncio.to_thread(
                    _vk_request,
                    "wall.getById",
                    token,
                    posts=",".join(f"-{group}_{row['post_id']}" for row in chunk),
                )
                items = response if isinstance(response, list) else response.get("items", [])
                found = {int(item.get("id") or 0): item for item in items if isinstance(item, dict)}
                for row in chunk:
                    item = found.get(int(row["post_id"]))
                    if item is None:
                        out.append({"target_id": row["target_id"], "observed_ts": observed_ts, "status": "not_found", "error_code": "post_not_found"})
                    else:
                        out.append({
                            "target_id": row["target_id"], "observed_ts": observed_ts, "status": "collected",
                            "post_ts": int(item["date"]) if isinstance(item.get("date"), int) else None,
                            "views": _metric_count(item, "views"), "likes": _metric_count(item, "likes"),
                            "comments": _metric_count(item, "comments"), "shares": _metric_count(item, "reposts"),
                            "reactions": None,
                        })
            except Exception as exc:
                code = type(exc).__name__
                out.extend({"target_id": row["target_id"], "observed_ts": observed_ts, "status": "error", "error_code": code} for row in chunk)
            requests_done += 1
            progress(len(out), requests_done, "vk")
    return out


async def _collect_tg(targets: list[dict[str, Any]], secrets: dict[str, str], progress) -> list[dict[str, Any]]:
    if not targets:
        return []
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    raw_bundle = secrets.get("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR", "")
    if not raw_bundle:
        raise RuntimeError("dedicated Telegram bundle is missing")
    import base64
    padded = raw_bundle + "=" * (-len(raw_bundle) % 4)
    bundle = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
    api_id = int(secrets.get("TG_API_ID") or secrets.get("TELEGRAM_API_ID") or 0)
    api_hash = secrets.get("TG_API_HASH") or secrets.get("TELEGRAM_API_HASH") or ""
    if not api_id or not api_hash or not bundle.get("session"):
        raise RuntimeError("Telegram credentials are incomplete")
    await asyncio.sleep(random.SystemRandom().uniform(4.0, 12.0))
    kwargs = {key: bundle[key] for key in ("device_model", "system_version", "app_version", "lang_code", "system_lang_code") if bundle.get(key)}
    client = TelegramClient(StringSession(str(bundle["session"])), api_id, api_hash, flood_sleep_threshold=60, **kwargs)
    out = []
    requests_done = 0
    made_request = False
    by_channel: dict[str, list[dict[str, Any]]] = {}
    for target in targets:
        by_channel.setdefault(str(target["publisher_id"]), []).append(target)
    async with client:
        for channel_index, (channel, rows) in enumerate(sorted(by_channel.items())):
            if channel_index:
                await asyncio.sleep(random.SystemRandom().uniform(5.0, 15.0))
            entity = await client.get_input_entity(channel)
            for start in range(0, len(rows), 50):
                if made_request:
                    await asyncio.sleep(random.SystemRandom().uniform(2.0, 5.0))
                chunk = rows[start : start + 50]
                observed_ts = int(time.time())
                try:
                    messages = await client.get_messages(entity, ids=[int(row["post_id"]) for row in chunk])
                    by_id = {int(message.id): message for message in (messages or []) if message is not None and getattr(message, "id", None)}
                    for row in chunk:
                        message = by_id.get(int(row["post_id"]))
                        if message is None:
                            out.append({"target_id": row["target_id"], "observed_ts": observed_ts, "status": "not_found", "error_code": "post_not_found"})
                            continue
                        reactions_container = getattr(message, "reactions", None)
                        reactions: dict[str, int] = {}
                        for reaction in getattr(reactions_container, "results", None) or []:
                            count = getattr(reaction, "count", None)
                            label = getattr(getattr(reaction, "reaction", None), "emoticon", None) or str(getattr(reaction, "reaction", "reaction"))
                            if isinstance(count, int) and count >= 0:
                                reactions[str(label)] = reactions.get(str(label), 0) + count
                        message_date = getattr(message, "date", None)
                        replies = getattr(getattr(message, "replies", None), "replies", None)
                        out.append({
                            "target_id": row["target_id"], "observed_ts": observed_ts, "status": "collected",
                            "post_ts": int(message_date.timestamp()) if isinstance(message_date, datetime) else None,
                            "views": int(message.views) if isinstance(getattr(message, "views", None), int) else None,
                            "likes": sum(reactions.values()) if reactions_container is not None else None,
                            "comments": int(replies) if isinstance(replies, int) else None,
                            "shares": int(message.forwards) if isinstance(getattr(message, "forwards", None), int) else None,
                            "reactions": reactions if reactions_container is not None else None,
                        })
                except Exception as exc:
                    code = type(exc).__name__
                    out.extend({"target_id": row["target_id"], "observed_ts": observed_ts, "status": "error", "error_code": code} for row in chunk)
                made_request = True
                requests_done += 1
                progress(len(out), requests_done, f"telegram:{channel}")
    return out


async def main() -> None:
    _ensure_libs()
    status = _load_status_client()
    acquired: list[str] = []
    manifest: dict[str, Any] = {}
    state = {"phase": "preflight", "targets_done": 0, "targets_total": 0, "requests_done": 0, "progress_label": "подготовка"}

    def emit(event: str, phase: str, run_status: str, message: str | None = None) -> None:
        if status:
            status.event(event, phase=phase, status=run_status, progress=dict(state), message=message)

    try:
        if not status or not status.enabled:
            raise RuntimeError("Kaggle status callback is required")
        emit("kernel_started", "preflight", "running")
        for resource in status.config.get("resource_leases") or []:
            if not status.acquire_resource(str(resource), ttl_seconds=3 * 60 * 60):
                raise RuntimeError(f"required Kaggle resource is busy: {resource}")
            acquired.append(str(resource))
        manifest = json.loads(_find("social_metrics_manifest.json").read_text())
        secrets = _decrypt()
        targets = list(manifest.get("targets") or [])
        candidates = list(manifest.get("vk_resolve_candidates") or [])
        total_work = len(targets) + len(candidates)
        state.update({"phase": "run", "targets_total": total_work, "progress_label": f"цели 0/{total_work}"})
        emit("preflight_ok", "preflight", "running")
        if status:
            status.start_alive(interval_seconds=60, progress_provider=lambda: dict(state))

        def progress(done: int, requests_done: int, label: str) -> None:
            state.update({
                "phase": "run", "targets_done": done, "requests_done": requests_done,
                "progress_percent": min(95, int(done * 100 / max(1, len(targets)))),
                "progress_label": f"{label} · цели {done}/{len(targets)}",
            })
            emit("alive", "run", "running")

        vk = [row for row in targets if row.get("platform") == "vk"]
        tg = [row for row in targets if row.get("platform") == "telegram"]
        observations = await _collect_vk(vk, secrets, progress)
        tg_observations = await _collect_tg(tg, secrets, lambda done, req, label: progress(len(observations) + done, req, label))
        observations.extend(tg_observations)
        resolutions = await _resolve_vk(
            candidates,
            secrets,
            int(manifest.get("vk_wall_scan_limit") or 1000),
        )
        result = {
            "schema_version": 2, "run_id": manifest["run_id"],
            "manifest_sha256": manifest["manifest_sha256"], "observations": observations,
            "vk_resolutions": resolutions,
            "diagnostics": {
                "targets": len(targets), "observations": len(observations),
                "resolve_candidates": len(candidates), "resolutions": len(resolutions),
            },
        }
        OUTPUT.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        state.update({"phase": "report", "targets_done": len(observations) + len(resolutions), "progress_percent": 100, "progress_label": f"цели {len(observations) + len(resolutions)}/{total_work}"})
        emit("report_written", "report", "done")
    except Exception as exc:
        state["phase"] = "failed"
        emit("report_written", "failed", "failed", "".join(traceback.format_exception_only(type(exc), exc)).strip())
        raise
    finally:
        if status:
            status.stop_alive()
            for resource in reversed(acquired):
                try:
                    status.release_resource(resource)
                except Exception as exc:
                    print(f"resource release failed: {exc}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
