#!/usr/bin/env python3
"""Sanitized, bounded inventory and replay for the Postbox feedback DLQ.

Inventory never deletes messages. Replay deletes an exact YMQ receipt only after
the production consumer returns success for the complete queue-message envelope.
No raw queue body, provider receipt, recipient, token, subject or body is logged
or written to the manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from serverless.email_postbox_events import index as consumer  # noqa: E402


SCHEMA = "kenigevents.postbox_dlq_inventory.v1"
YMQ_ENDPOINT = "https://message-queue.api.cloud.yandex.net"


class RecoveryError(RuntimeError):
    pass


def _required(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise RecoveryError(f"env_missing:{name.lower()}")
    return value


def _sha(value: str | bytes) -> str:
    raw = value if isinstance(value, bytes) else value.encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _records(body: str) -> tuple[str, list[Mapping[str, Any]]]:
    try:
        value = json.loads(body)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise RecoveryError("ymq_body_json_invalid") from exc
    if not isinstance(value, Mapping):
        raise RecoveryError("ymq_envelope_invalid")
    if {"eventId", "eventType", "mail"}.issubset(value):
        return "raw_postbox_event", [value]
    messages = value.get("messages")
    if isinstance(messages, list) and messages and all(
        isinstance(item, Mapping) for item in messages
    ):
        return "consumer_messages", list(messages)
    raise RecoveryError("ymq_envelope_unsupported")


def _sqs_client() -> Any:
    import boto3

    return boto3.client(
        "sqs",
        endpoint_url=str(os.environ.get("POSTBOX_DLQ_ENDPOINT") or YMQ_ENDPOINT),
        region_name="ru-central1",
        aws_access_key_id=_required("POSTBOX_DLQ_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_required("POSTBOX_DLQ_AWS_SECRET_ACCESS_KEY"),
    )


def _attributes(client: Any, queue_url: str) -> dict[str, int]:
    result = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    ).get("Attributes") or {}
    return {
        "visible": int(result.get("ApproximateNumberOfMessages") or 0),
        "inflight": int(result.get("ApproximateNumberOfMessagesNotVisible") or 0),
    }


def _classify(message_ids: Iterable[str]) -> dict[str, dict[str, str]]:
    ids = list(dict.fromkeys(message_ids))
    if not ids:
        return {}
    if len(ids) > 500:
        result: dict[str, dict[str, str]] = {}
        for start in range(0, len(ids), 500):
            result.update(_classify(ids[start:start + 500]))
        return result
    base = _required("PERSONALIZATION_SUPABASE_URL").rstrip("/")
    key = _required("PERSONALIZATION_SUPABASE_SECRET_KEY")
    headers = {"Content-Type": "application/json", "apikey": key}
    if not key.startswith("sb_secret_"):
        headers["Authorization"] = f"Bearer {key}"
    request = urllib.request.Request(
        f"{base}/rest/v1/rpc/email_classify_postbox_receipts_v1",
        data=_canonical({"p_provider_message_ids": ids}),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            raw = response.read(1_000_000)
    except urllib.error.HTTPError as exc:
        exc.read(16_384)
        raise RecoveryError(f"classification_http_{exc.code}") from exc
    try:
        rows = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise RecoveryError("classification_response_invalid") from exc
    if not isinstance(rows, list) or len(rows) != len(ids):
        raise RecoveryError("classification_response_invalid")
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise RecoveryError("classification_response_invalid")
        digest = str(row.get("message_sha256") or "")
        if len(digest) != 64:
            raise RecoveryError("classification_response_invalid")
        result[digest] = {
            "source_classification": str(row.get("source_classification") or "unknown"),
            "correlation_status": str(row.get("correlation_status") or "unknown"),
        }
    return result


def inventory(client: Any, queue_url: str, *, max_messages: int, visibility: int) -> dict[str, Any]:
    before = _attributes(client, queue_url)
    if before["inflight"]:
        raise RecoveryError("inventory_requires_zero_inflight")
    handles: list[str] = []
    queue_rows: dict[str, dict[str, Any]] = {}
    raw_message_ids: list[str] = []
    empty_receives = 0
    try:
        while len(queue_rows) < max_messages and empty_receives < 10:
            response = client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=min(10, max_messages - len(queue_rows)),
                WaitTimeSeconds=2,
                VisibilityTimeout=visibility,
                AttributeNames=["SentTimestamp"],
            )
            messages = response.get("Messages") or []
            if not messages:
                empty_receives += 1
                continue
            empty_receives = 0
            for message in messages:
                queue_id = str(message.get("MessageId") or "")
                receipt = str(message.get("ReceiptHandle") or "")
                body = str(message.get("Body") or "")
                if not queue_id or not receipt:
                    raise RecoveryError("ymq_message_identity_invalid")
                handles.append(receipt)
                queue_hash = _sha(queue_id)
                if queue_hash in queue_rows:
                    continue
                row: dict[str, Any] = {
                    "queue_message_sha256": queue_hash,
                    "body_sha256": _sha(body),
                    "sent_at_ms": str((message.get("Attributes") or {}).get("SentTimestamp") or ""),
                    "envelope_schema": "unknown",
                    "events": [],
                    "stable_error_code": None,
                }
                try:
                    schema, records = _records(body)
                    row["envelope_schema"] = schema
                    for record in records:
                        event_id = str(record.get("eventId") or "")
                        event_type = str(record.get("eventType") or "")
                        mail = record.get("mail")
                        provider_message_id = str(
                            mail.get("messageId") if isinstance(mail, Mapping) else ""
                        )
                        event_time = ""
                        for key in ("delivery", "bounce", "deliveryDelay", "subscription", "complaint", "open", "click", "mail"):
                            section = record.get(key)
                            if isinstance(section, Mapping) and section.get("timestamp"):
                                event_time = str(section["timestamp"])
                                break
                        if provider_message_id:
                            raw_message_ids.append(provider_message_id)
                        row["events"].append({
                            "event_id_sha256": _sha(event_id) if event_id else None,
                            "message_id_sha256": _sha(provider_message_id) if provider_message_id else None,
                            "event_type": event_type or "unknown",
                            "event_time_utc": event_time or None,
                            "evidence_sha256": _sha(_canonical(record)),
                            "source_classification": "pending",
                            "correlation_status": "pending",
                        })
                except RecoveryError as exc:
                    row["stable_error_code"] = str(exc)
                queue_rows[queue_hash] = row
        time.sleep(2)
        hidden_snapshot = _attributes(client, queue_url)
        if len(queue_rows) >= max_messages and (
            before["visible"] > max_messages
            or hidden_snapshot["visible"] > 0
        ):
            raise RecoveryError("inventory_bound_too_small")
        if hidden_snapshot["visible"] != 0 or hidden_snapshot["inflight"] != len(queue_rows):
            raise RecoveryError("inventory_snapshot_did_not_reconcile")
        classification = _classify(raw_message_ids)
        for row in queue_rows.values():
            for event in row["events"]:
                details = classification.get(str(event["message_id_sha256"]))
                if details:
                    event.update(details)
                else:
                    event["source_classification"] = "unproven"
                    event["correlation_status"] = "correlation_pending"
        event_rows = [event for row in queue_rows.values() for event in row["events"]]
        classifications = Counter(
            f"{event['source_classification']}:{event['correlation_status']}"
            for event in event_rows
        )
        manifest = {
            "schema": SCHEMA,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "queue_url_sha256": _sha(queue_url),
            "before": before,
            "hidden_snapshot": hidden_snapshot,
            "inventory": {
                "queue_messages": len(queue_rows),
                "unique_event_ids": len({event["event_id_sha256"] for event in event_rows if event["event_id_sha256"]}),
                "unique_message_ids": len({event["message_id_sha256"] for event in event_rows if event["message_id_sha256"]}),
                "event_types": dict(sorted(Counter(event["event_type"] for event in event_rows).items())),
                "classifications": dict(sorted(classifications.items())),
                "malformed_or_unsupported": sum(bool(row["stable_error_code"]) for row in queue_rows.values()),
            },
            "messages": sorted(queue_rows.values(), key=lambda item: item["queue_message_sha256"]),
        }
        manifest["manifest_sha256"] = _sha(_canonical(manifest))
        return manifest
    finally:
        restore_failed = False
        for start in range(0, len(handles), 10):
            entries = [
                {"Id": str(index), "ReceiptHandle": handle, "VisibilityTimeout": 0}
                for index, handle in enumerate(handles[start:start + 10])
            ]
            result = client.change_message_visibility_batch(
                QueueUrl=queue_url, Entries=entries
            )
            if result.get("Failed"):
                restore_failed = True
        if restore_failed:
            raise RecoveryError("visibility_restore_failed")


def load_reviewed_inventory(path: Path, expected_sha256: str) -> dict[str, str]:
    raw = path.read_bytes()
    if _sha(raw) != expected_sha256.lower().strip():
        raise RecoveryError("inventory_file_sha256_mismatch")
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise RecoveryError("inventory_file_invalid") from exc
    if not isinstance(value, Mapping) or value.get("schema") != SCHEMA:
        raise RecoveryError("inventory_file_invalid")
    messages = value.get("messages")
    if not isinstance(messages, list) or not messages:
        raise RecoveryError("inventory_file_invalid")
    approved: dict[str, str] = {}
    for row in messages:
        if not isinstance(row, Mapping):
            raise RecoveryError("inventory_file_invalid")
        queue_hash = str(row.get("queue_message_sha256") or "")
        body_hash = str(row.get("body_sha256") or "")
        if len(queue_hash) != 64 or len(body_hash) != 64 or queue_hash in approved:
            raise RecoveryError("inventory_file_invalid")
        approved[queue_hash] = body_hash
    return approved


def replay(
    client: Any,
    queue_url: str,
    *,
    batch_size: int,
    approved: Mapping[str, str],
    inventory_sha256: str,
) -> dict[str, Any]:
    if batch_size not in range(1, 11):
        raise RecoveryError("initial_replay_batch_must_be_1_to_10")
    response = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=batch_size,
        WaitTimeSeconds=2,
        VisibilityTimeout=120,
    )
    results = Counter()
    rows = []
    messages = response.get("Messages") or []
    for position, message in enumerate(messages):
        queue_hash = _sha(str(message.get("MessageId") or ""))
        receipt = str(message.get("ReceiptHandle") or "")
        try:
            body = str(message.get("Body") or "")
            if approved.get(queue_hash) != _sha(body):
                raise RecoveryError("queue_message_not_in_reviewed_inventory")
            schema, records = _records(body)
            outcome = consumer.process_event({"messages": records}, env=os.environ)
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            status = "deleted_after_verified_consumer_success"
            results["deleted"] += 1
            results["applied"] += int(outcome.get("applied") or 0)
            results["duplicate"] += int(outcome.get("duplicates") or 0)
            rows.append({"queue_message_sha256": queue_hash, "envelope_schema": schema, "status": status})
        except Exception as exc:
            client.change_message_visibility(
                QueueUrl=queue_url, ReceiptHandle=receipt, VisibilityTimeout=0
            )
            for unprocessed in messages[position + 1:]:
                client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=str(unprocessed.get("ReceiptHandle") or ""),
                    VisibilityTimeout=0,
                )
            if isinstance(exc, RecoveryError):
                code = str(exc)
            elif isinstance(exc, (consumer.BatchError, consumer.EventError)):
                code = getattr(exc, "code", None) or str(exc)
            else:
                code = "replay_transport_or_validation_failed"
            rows.append({
                "queue_message_sha256": queue_hash,
                "status": "retained",
                "stable_error_code": str(code)[:120],
            })
            results["retained"] += 1
            break
    return {
        "schema": "kenigevents.postbox_dlq_replay.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "queue_url_sha256": _sha(queue_url),
        "inventory_file_sha256": inventory_sha256,
        "totals": dict(results),
        "messages": rows,
        "after": _attributes(client, queue_url),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("inventory", "replay"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-messages", type=int, default=500)
    parser.add_argument("--visibility-seconds", type=int, default=300)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--inventory", type=Path)
    parser.add_argument("--inventory-sha256")
    args = parser.parse_args()
    if args.max_messages not in range(1, 5001):
        raise RecoveryError("max_messages_out_of_range")
    if args.visibility_seconds not in range(60, 901):
        raise RecoveryError("visibility_seconds_out_of_range")
    queue_url = _required("POSTBOX_DLQ_QUEUE_URL")
    client = _sqs_client()
    if args.mode == "inventory":
        result = inventory(
            client, queue_url,
            max_messages=args.max_messages,
            visibility=args.visibility_seconds,
        )
    else:
        if os.environ.get("POSTBOX_DLQ_REPLAY_CONFIRM") != "INC-2026-08-04":
            raise RecoveryError("replay_confirmation_missing")
        if args.inventory is None or not args.inventory_sha256:
            raise RecoveryError("reviewed_inventory_required")
        approved = load_reviewed_inventory(args.inventory, args.inventory_sha256)
        result = replay(
            client,
            queue_url,
            batch_size=args.batch_size,
            approved=approved,
            inventory_sha256=args.inventory_sha256.lower().strip(),
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(_canonical(result) + b"\n")
    os.chmod(args.output, 0o600)
    print(json.dumps({
        "mode": args.mode,
        "output": str(args.output),
        "sha256": _sha(args.output.read_bytes()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
