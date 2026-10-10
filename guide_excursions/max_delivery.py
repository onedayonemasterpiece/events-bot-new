"""Durable per-edition delivery. Unknown external outcomes are never re-sent.

The existing scheduler drives this ledger; it is not a second queue/scheduler.
Only the process that atomically claims a prepared intent may submit it. A
restart observes the original request identity instead of publishing again.
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)


async def ensure_delivery_schema(conn):
    await conn.execute("""CREATE TABLE IF NOT EXISTS guide_visual_delivery (
        issue_id INTEGER NOT NULL, provider TEXT NOT NULL, target_id TEXT NOT NULL,
        request_key TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
        payload_json TEXT NOT NULL, operation_id TEXT, receipt_json TEXT,
        error_code TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(issue_id, provider, target_id)
    )""")


def request_key(issue_id: int, provider: str, target_id: str) -> str:
    identity = f"guide-visual:v1:{int(issue_id)}:{provider}:{target_id}"
    return "guide-visual-" + hashlib.sha256(identity.encode()).hexdigest()


async def prepare_delivery(db, *, issue_id: int, provider: str, target_id: str,
                           payload: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze the first payload. Config/content drift cannot change a replay."""
    key = request_key(issue_id, provider, target_id)
    async with db.raw_conn() as conn:
        await ensure_delivery_schema(conn)
        await conn.execute(
            "INSERT OR IGNORE INTO guide_visual_delivery "
            "(issue_id, provider, target_id, request_key, state, payload_json) "
            "VALUES (?, ?, ?, ?, 'prepared', ?)",
            (issue_id, provider, target_id, key,
             json.dumps(dict(payload), ensure_ascii=False, sort_keys=True)),
        )
        await conn.commit()
    return await get_delivery(db, key)


async def get_delivery(db, key: str) -> dict[str, Any]:
    async with db.raw_conn() as conn:
        await ensure_delivery_schema(conn)
        cur = await conn.execute("SELECT * FROM guide_visual_delivery WHERE request_key=?", (key,))
        row = await cur.fetchone()
        if row is None:
            raise LookupError("delivery_not_found")
        result = dict(zip([col[0] for col in cur.description], row))
    result["payload"] = json.loads(result.pop("payload_json"))
    result["receipt"] = json.loads(result.pop("receipt_json") or "{}")
    return result


async def _record(db, row, *, state: str, receipt=None, operation_id=None, error_code=None):
    # Store/log bounded machine codes, never provider exception messages or URLs.
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE guide_visual_delivery SET state=?, operation_id=COALESCE(?, operation_id), "
            "receipt_json=COALESCE(?, receipt_json), error_code=?, updated_at=CURRENT_TIMESTAMP "
            "WHERE request_key=? AND state NOT IN ('published','owner_confirmed_present')",
            (state, operation_id, json.dumps(receipt) if receipt is not None else None,
             error_code, row["request_key"]),
        )
        await conn.commit()
    persisted = await get_delivery(db, row["request_key"])
    logger.info("guide_visual_delivery issue_id=%s provider=%s target_id=%s request_key=%s "
                "operation_id=%s state=%s error_code=%s", row["issue_id"], row["provider"],
                row["target_id"], row["request_key"], operation_id or row.get("operation_id"),
                persisted["state"], persisted["error_code"])
    return persisted


async def drive_delivery(db, row: Mapping[str, Any], transport) -> dict[str, Any]:
    """transport.submit(payload,key), observe(key,operation_id) are bounded calls.

    Observation preserves the original identity after restart or reply loss.
    A missing remote operation is not proof of non-delivery. A transport may
    explicitly recover the same frozen operation only through a server-proven
    zero-dispatch, single-admission recovery key. Unknown effects never resend.
    """
    row = await get_delivery(db, row["request_key"])
    if row["state"] in {"published", "owner_confirmed_present"}:
        return row
    claimed = False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "UPDATE guide_visual_delivery SET state='submitting', updated_at=CURRENT_TIMESTAMP "
            "WHERE request_key=? AND state='prepared'", (row["request_key"],))
        claimed = cur.rowcount == 1
        await conn.commit()
    row = await get_delivery(db, row["request_key"])
    if hasattr(transport, "restore_recovery"):
        transport.restore_recovery(row["receipt"].get("recovery"))
    try:
        result = (await transport.submit(row["payload"], row["request_key"]) if claimed
                  else await transport.observe(row["request_key"], row.get("operation_id")))
    except Exception as exc:
        code = str(exc) if type(exc).__name__ == "SnapshotUnavailable" else "transport_unavailable"
        if len(code) > 80 or not code.replace("_", "").isalnum():
            code = "transport_unavailable"
        return await _record(db, row, state="outcome_unknown", error_code=code)
    if not isinstance(result, Mapping):
        return await _record(db, row, state="outcome_unknown", error_code="invalid_receipt")
    state = str(result.get("state") or "outcome_unknown")
    operation_id = result.get("operation_id") or row.get("operation_id")
    receipt_raw = result.get("receipt") or {}
    if not isinstance(receipt_raw, Mapping):
        return await _record(db, row, state="outcome_unknown", error_code="invalid_receipt")
    receipt = dict(receipt_raw)
    if state == "published" and not receipt.get("post_urls") and not receipt.get("message_ids") and not receipt.get("item_ref"):
        state = "outcome_unknown"
    if state == "owner_confirmed_present":
        adjudication = receipt.get("adjudication")
        if (not isinstance(adjudication, Mapping)
                or adjudication.get("disposition") != "owner_confirmed_present"
                or adjudication.get("quarantine_release") != "done"
                or adjudication.get("original_outcome") != "outcome_unknown"
                or adjudication.get("replay_allowed") is not False):
            state = "outcome_unknown"
    if state not in {"published", "owner_confirmed_present", "accepted", "scheduled", "failed", "outcome_unknown"}:
        state = "outcome_unknown"
    error_code = result.get("error_code")
    if error_code and (not str(error_code).replace("_", "").isalnum() or len(str(error_code)) > 80):
        error_code = "provider_failure"
    return await _record(db, row, state=state, receipt=receipt,
                         operation_id=operation_id, error_code=error_code)


async def run_legacy_delivery(db, *, issue_id, provider, target_id, send):
    """Guard existing TG/VK sends; uncertain outcomes need provider inspection.

    These legacy transports have no durable admission API. We therefore never
    retry a claimed send automatically after a crash. Successful sibling sends
    are saved separately and remain available to the next scheduler tick.
    """
    class Transport:
        async def submit(self, payload, key):
            result = await send()
            if not result.get("published"):
                return {"state": "outcome_unknown", "error_code": "legacy_receipt_missing"}
            receipt = dict(result)
            if result.get("url"):
                receipt["post_urls"] = [result["url"]]
            return {"state": "published", "receipt": receipt}

        async def observe(self, key, operation_id):
            return {"state": "outcome_unknown", "error_code": "legacy_provider_observation_required"}

    row = await prepare_delivery(db, issue_id=issue_id, provider=provider, target_id=str(target_id),
                                 payload={"issue_id": int(issue_id), "provider": provider, "target_id": str(target_id)})
    row = await drive_delivery(db, row, Transport())
    return dict(row["receipt"], published=row["state"] == "published", state=row["state"],
                reason=row.get("error_code"), request_key=row["request_key"])
