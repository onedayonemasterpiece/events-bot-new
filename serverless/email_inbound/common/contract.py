from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import UTC, datetime, timedelta
from email.utils import parseaddr
from typing import Any, Mapping, Sequence


ENVELOPE_SCHEMA = "kenigevents.email_inbound.envelope.v1"
POINTER_SCHEMA = "kenigevents.email_inbound.pointer.v1"
ADAPTER_SCHEMA = "kenigevents.email_inbound.adapter.v1"

HEADER_ALLOWLIST = frozenset(
    {
        "authentication-results",
        "cc",
        "content-transfer-encoding",
        "content-type",
        "date",
        "delivered-to",
        "from",
        "in-reply-to",
        "message-id",
        "references",
        "reply-to",
        "subject",
        "to",
        "x-original-to",
        "x-yandex-fwd",
    }
)

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_MAILBOX_RE = re.compile(r"^[^\s@]+@[^\s@]+$")
_GO_RECEIVED_AT_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})[ T](?P<time>\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+\+0000\s+UTC"
)


class ContractError(ValueError):
    """A fail-closed contract validation error with a stable non-PII code."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def hmac_hex(secret: str, namespace: str, value: bytes) -> str:
    if len(secret.encode("utf-8")) < 32:
        raise ContractError("secret_too_short")
    material = namespace.encode("ascii") + b"\x00" + value
    return hmac.new(secret.encode("utf-8"), material, hashlib.sha256).hexdigest()


def normalize_received_at(value: Any) -> str:
    raw = str(value or "").strip()
    match = _GO_RECEIVED_AT_RE.match(raw)
    if match:
        raw = f"{match.group('date')}T{match.group('time')}+00:00"
    elif raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ContractError("received_at_invalid") from exc
    if parsed.tzinfo is None:
        raise ContractError("received_at_timezone_missing")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _clean_header_value(value: Any) -> str:
    clean = str(value or "").replace("\x00", "").strip()
    if len(clean.encode("utf-8")) > 8_192:
        raise ContractError("header_value_too_large")
    return clean


def normalize_headers(raw_headers: Any) -> dict[str, list[str]]:
    if not isinstance(raw_headers, list):
        raise ContractError("headers_not_list")
    normalized: dict[str, list[str]] = {}
    total_bytes = 0
    for item in raw_headers:
        if not isinstance(item, Mapping):
            raise ContractError("header_not_object")
        name = str(item.get("name") or "").strip().lower()
        if name not in HEADER_ALLOWLIST:
            continue
        values = item.get("values")
        if not isinstance(values, list):
            raise ContractError("header_values_not_list")
        output = normalized.setdefault(name, [])
        for raw_value in values[:20]:
            clean = _clean_header_value(raw_value)
            total_bytes += len(name.encode("ascii")) + len(clean.encode("utf-8"))
            if total_bytes > 32_768:
                raise ContractError("headers_too_large")
            output.append(clean)
    return normalized


def first_header(headers: Mapping[str, Sequence[str]], name: str) -> str:
    values = headers.get(name.lower()) or []
    return str(values[0]) if values else ""


def _normalized_message_id(headers: Mapping[str, Sequence[str]]) -> str:
    return first_header(headers, "message-id").strip().lower()[:998]


def _normalized_sender(headers: Mapping[str, Sequence[str]]) -> str:
    _, address = parseaddr(first_header(headers, "from"))
    return address.strip().lower()[:320]


def _content_media_type(headers: Mapping[str, Sequence[str]]) -> str:
    value = first_header(headers, "content-type").split(";", 1)[0].strip().lower()
    return value if value else "text/plain"


def _normalize_attachments(value: Any) -> list[dict[str, str]]:
    if value in (None, {}):
        return []
    if not isinstance(value, Mapping):
        raise ContractError("attachments_not_object")
    bucket = str(value.get("bucket_id") or "").strip()
    keys = value.get("keys") or []
    if not isinstance(keys, list):
        raise ContractError("attachment_keys_not_list")
    if len(keys) > 100:
        raise ContractError("too_many_attachments")
    result = []
    for key in keys:
        clean_key = str(key or "").strip()
        if not bucket or not clean_key or len(clean_key.encode("utf-8")) >= 1_024:
            raise ContractError("attachment_reference_invalid")
        result.append({"bucket": bucket, "key": clean_key})
    return sorted(result, key=lambda item: (item["bucket"], item["key"]))


def _validate_mailbox(mailbox: str) -> str:
    value = mailbox.strip().lower()
    if len(value) > 320 or not _MAILBOX_RE.fullmatch(value):
        raise ContractError("mailbox_invalid")
    return value


def build_envelope_and_pointer(
    message: Mapping[str, Any],
    *,
    mailbox: str,
    bucket: str,
    idempotency_secret: str,
    max_body_bytes: int = 220_000,
    retention_days: int = 30,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    if not isinstance(message, Mapping):
        raise ContractError("mail_message_not_object")
    mailbox = _validate_mailbox(mailbox)
    bucket = bucket.strip()
    if not bucket or len(bucket) > 63:
        raise ContractError("bucket_invalid")
    if retention_days < 1 or retention_days > 365:
        raise ContractError("retention_days_invalid")

    received_at = normalize_received_at(message.get("received_at"))
    headers = normalize_headers(message.get("headers"))
    body = str(message.get("message") or "")
    body_bytes = body.encode("utf-8")
    if len(body_bytes) > max_body_bytes:
        raise ContractError("body_too_large")
    body_sha = sha256_hex(body_bytes)
    attachments = _normalize_attachments(message.get("attachments"))

    identity = {
        "mailbox": mailbox,
        "message_id": _normalized_message_id(headers),
        "headers": headers,
        "body_sha256": body_sha,
        "attachments": attachments,
    }
    inbound_id = hmac_hex(idempotency_secret, "inbound-id-v1", canonical_json(identity))
    received_dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
    expires_at = (received_dt + timedelta(days=retention_days)).isoformat().replace(
        "+00:00", "Z"
    )
    object_key = (
        f"messages/{received_dt:%Y/%m/%d}/{inbound_id}/envelope.json"
    )

    envelope = {
        "schema": ENVELOPE_SCHEMA,
        "inbound_id": inbound_id,
        "mailbox": mailbox,
        "received_at": received_at,
        "headers": headers,
        "trigger_body": {
            "value": body,
            "sha256": body_sha,
            "bytes": len(body_bytes),
        },
        "attachments": attachments,
    }
    envelope_bytes = canonical_json(envelope)
    envelope_sha = sha256_hex(envelope_bytes)
    message_id = _normalized_message_id(headers)
    sender = _normalized_sender(headers)
    pointer = {
        "schema": POINTER_SCHEMA,
        "inbound_id": inbound_id,
        "mailbox": mailbox,
        "received_at": received_at,
        "object": {
            "bucket": bucket,
            "key": object_key,
            "sha256": envelope_sha,
            "expires_at": expires_at,
        },
        "message_id_hmac": hmac_hex(
            idempotency_secret, "message-id-v1", message_id.encode("utf-8")
        )
        if message_id
        else None,
        "sender_hmac": hmac_hex(
            idempotency_secret, "sender-v1", sender.encode("utf-8")
        )
        if sender
        else None,
        "body": {
            "bytes": len(body_bytes),
            "sha256": body_sha,
            "media_type": _content_media_type(headers),
        },
        "attachments": {"count": len(attachments)},
    }
    validate_pointer(pointer)
    return envelope, pointer, envelope_bytes


def validate_pointer(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema") != POINTER_SCHEMA:
        raise ContractError("pointer_schema_invalid")
    inbound_id = str(value.get("inbound_id") or "")
    if not _HEX64_RE.fullmatch(inbound_id):
        raise ContractError("inbound_id_invalid")
    _validate_mailbox(str(value.get("mailbox") or ""))
    normalize_received_at(value.get("received_at"))
    object_ref = value.get("object")
    if not isinstance(object_ref, dict):
        raise ContractError("object_reference_invalid")
    for field in ("bucket", "key"):
        if not str(object_ref.get(field) or "").strip():
            raise ContractError("object_reference_invalid")
    if not _HEX64_RE.fullmatch(str(object_ref.get("sha256") or "")):
        raise ContractError("object_sha256_invalid")
    normalize_received_at(object_ref.get("expires_at"))
    body = value.get("body")
    if not isinstance(body, dict):
        raise ContractError("body_metadata_invalid")
    body_bytes = body.get("bytes")
    if not isinstance(body_bytes, int) or body_bytes < 0 or body_bytes > 3_500_000:
        raise ContractError("body_bytes_invalid")
    if not _HEX64_RE.fullmatch(str(body.get("sha256") or "")):
        raise ContractError("body_sha256_invalid")
    attachments = value.get("attachments")
    if not isinstance(attachments, dict):
        raise ContractError("attachment_metadata_invalid")
    count = attachments.get("count")
    if not isinstance(count, int) or count < 0 or count > 100:
        raise ContractError("attachment_count_invalid")
    for optional_hmac in ("message_id_hmac", "sender_hmac"):
        candidate = value.get(optional_hmac)
        if candidate is not None and not _HEX64_RE.fullmatch(str(candidate)):
            raise ContractError(f"{optional_hmac}_invalid")
    return value


def build_adapter_payload(pointer: Mapping[str, Any]) -> dict[str, Any]:
    validated = validate_pointer(dict(pointer))
    return {
        "schema": ADAPTER_SCHEMA,
        "inbound_id": validated["inbound_id"],
        "mailbox": validated["mailbox"],
        "received_at": validated["received_at"],
        "object": validated["object"],
        "message_id_hmac": validated.get("message_id_hmac"),
        "sender_hmac": validated.get("sender_hmac"),
        "body": validated["body"],
        "attachments": validated["attachments"],
    }
