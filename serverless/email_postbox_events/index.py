from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from email.utils import getaddresses
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit


MAX_RECORD_BYTES = 65_536
MAX_BATCH_BYTES = 131_072
MAX_BATCH_RECORDS = 100
_SAFE_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,300}$")
_SAFE_MESSAGE_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,512}$")

EVENT_TYPES = {
    "Send": ("accepted", ("mail", "timestamp")),
    "Rendering Failure": ("rendering_failure", ("mail", "timestamp")),
    "Delivery": ("delivered", ("delivery", "timestamp")),
    "Bounce": ("hard_bounce", ("bounce", "timestamp")),
    "DeliveryDelay": ("delivery_delay", ("deliveryDelay", "timestamp")),
    "Subscription": ("unsubscribe", ("subscription", "timestamp")),
    "Complaint": ("complaint", ("complaint", "timestamp")),
    "Open": ("open", ("open", "timestamp")),
    "Click": ("click", ("click", "timestamp")),
}

Transport = Callable[[str, bytes, Mapping[str, str], float], tuple[int, bytes]]


class EventError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class BatchError(RuntimeError):
    pass


def _log(level: str, message: str, **fields: str) -> None:
    # Cloud Functions captures stdout and recognizes this single-line JSON shape
    # as a structured log. Callers must pass only bounded non-PII fields.
    row = {"level": level, "message": message, "stream_name": "postbox-events", **fields}
    print(json.dumps(row, ensure_ascii=True, sort_keys=True, separators=(",", ":")), flush=True)


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name) or "").strip()
    if not value:
        raise EventError(f"env_missing:{name.lower()}")
    return value


def _enabled(env: Mapping[str, str]) -> bool:
    return str(env.get("POSTBOX_EVENT_CONSUMER_ENABLED") or "").strip().lower() in {
        "1", "true", "yes", "on"
    }


def _canonical(value: Mapping[str, Any]) -> bytes:
    try:
        result = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise EventError("event_json_invalid") from exc
    if len(result) > MAX_RECORD_BYTES:
        raise EventError("event_too_large")
    return result


def _mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EventError(code)
    return value


def _string(value: Any, code: str, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str):
        raise EventError(code)
    result = value.strip()
    if not result or (pattern and not pattern.fullmatch(result)):
        raise EventError(code)
    return result


def _timestamp(record: Mapping[str, Any], path: tuple[str, str]) -> str:
    parent = _mapping(record.get(path[0]), "event_timestamp_object_invalid")
    raw = _string(parent.get(path[1]), "event_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EventError("event_timestamp_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EventError("event_timestamp_invalid")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _addresses(values: Any, code: str) -> set[str]:
    if not isinstance(values, list) or not values or not all(isinstance(v, str) for v in values):
        raise EventError(code)
    parsed = [address.strip().lower() for _name, address in getaddresses(values) if address.strip()]
    if len(parsed) != 1 or len(set(parsed)) != 1:
        raise EventError(code)
    address = parsed[0]
    if len(address) not in range(3, 321) or "@" not in address or any(c in address for c in "\r\n\x00"):
        raise EventError(code)
    return {address}


def _event_recipients(record: Mapping[str, Any], event_type: str, common_to: set[str]) -> set[str]:
    if event_type == "Delivery":
        values = _mapping(record.get("delivery"), "delivery_invalid").get("recipients")
        recipients = _addresses(values, "delivery_recipients_invalid")
    elif event_type == "Bounce":
        bounce = _mapping(record.get("bounce"), "bounce_invalid")
        bounce_type = _string(bounce.get("bounceType"), "bounce_type_invalid").lower()
        if bounce_type not in {"permanent", "permenent"}:
            raise EventError("bounce_type_unsupported")
        rows = bounce.get("bouncedRecipients")
        if not isinstance(rows, list) or not rows:
            raise EventError("bounce_recipients_invalid")
        recipients = _addresses(
            [_mapping(row, "bounce_recipient_invalid").get("emailAddress") for row in rows],
            "bounce_recipients_invalid",
        )
    elif event_type == "DeliveryDelay":
        rows = _mapping(record.get("deliveryDelay"), "delivery_delay_invalid").get("delayedRecipients")
        if not isinstance(rows, list) or not rows:
            raise EventError("delivery_delay_recipients_invalid")
        recipients = _addresses(
            [_mapping(row, "delivery_delay_recipient_invalid").get("emailAddress") for row in rows],
            "delivery_delay_recipients_invalid",
        )
    elif event_type == "Complaint":
        rows = _mapping(record.get("complaint"), "complaint_invalid").get("complainedRecipients")
        if not isinstance(rows, list) or not rows:
            raise EventError("complaint_recipients_invalid")
        recipients = _addresses(
            [_mapping(row, "complaint_recipient_invalid").get("emailAddress") for row in rows],
            "complaint_recipients_invalid",
        )
    else:
        recipients = common_to
    if recipients != common_to:
        raise EventError("recipient_mismatch")
    return recipients


def _recipient_hmac(address: str, key: str) -> str:
    digest = hmac.new(key.encode("utf-8"), address.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def parse_record(record: Any, *, env: Mapping[str, str]) -> dict[str, Any]:
    obj = _mapping(record, "event_invalid")
    canonical = _canonical(obj)
    event_id = _string(obj.get("eventId"), "event_id_invalid", _SAFE_ID_RE)
    provider_type = _string(obj.get("eventType"), "event_type_invalid")
    mapped = EVENT_TYPES.get(provider_type)
    if not mapped:
        raise EventError("event_type_unsupported")
    internal_type, time_path = mapped
    mail = _mapping(obj.get("mail"), "mail_invalid")
    message_id = _string(mail.get("messageId"), "message_id_invalid", _SAFE_MESSAGE_ID_RE)
    if _string(mail.get("identityId"), "identity_id_invalid") != _required(env, "POSTBOX_EXPECTED_IDENTITY_ID"):
        raise EventError("identity_mismatch")
    tags = _mapping(mail.get("tags"), "mail_tags_invalid")
    config_values = tags.get("ses:configuration-set")
    if not isinstance(config_values, list) or _required(env, "POSTBOX_EXPECTED_CONFIGURATION_TAG") not in config_values:
        raise EventError("configuration_mismatch")
    from_values = tags.get("ses:from-domain")
    if not isinstance(from_values, list) or _required(env, "POSTBOX_EXPECTED_FROM_DOMAIN") not in from_values:
        raise EventError("from_domain_mismatch")
    common = _mapping(mail.get("commonHeaders"), "common_headers_invalid")
    common_to = _addresses(common.get("to"), "common_recipient_invalid")
    recipients = _event_recipients(obj, provider_type, common_to)
    address = next(iter(recipients))
    try:
        key_version = int(_required(env, "EMAIL_ADDRESS_HMAC_KEY_VERSION"))
    except ValueError as exc:
        raise EventError("hmac_key_version_invalid") from exc
    if key_version < 1:
        raise EventError("hmac_key_version_invalid")
    recipient_hmac = _recipient_hmac(address, _required(env, "EMAIL_ADDRESS_HMAC_KEY"))
    return {
        "p_provider_event_key": event_id,
        "p_provider_message_id": message_id,
        "p_event_type": internal_type,
        "p_event_at": _timestamp(obj, time_path),
        "p_recipient_hmac": recipient_hmac,
        "p_hmac_key_version": key_version,
        "p_payload_sha256": hashlib.sha256(canonical).hexdigest(),
    }


def urllib_transport(url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read(16_384)
    except urllib.error.HTTPError as exc:
        exc.read(16_384)
        return int(exc.code), b""


def _store(payload: Mapping[str, Any], *, env: Mapping[str, str], transport: Transport) -> str:
    base = _required(env, "PERSONALIZATION_SUPABASE_URL").rstrip("/")
    parsed = urlsplit(base)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path or parsed.query or parsed.fragment:
        raise EventError("supabase_url_invalid")
    key = _required(env, "PERSONALIZATION_SUPABASE_SECRET_KEY")
    status, response = transport(
        f"{base}/rest/v1/rpc/email_record_postbox_event_v2",
        json.dumps(dict(payload), separators=(",", ":")).encode("utf-8"),
        {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "apikey": key,
            "User-Agent": "KenigEventsPostboxEvents/1.0",
        },
        10.0,
    )
    if status in {408, 409, 425, 429} or status >= 500:
        raise EventError("supabase_retryable")
    if status < 200 or status >= 300:
        raise EventError("supabase_rejected")
    try:
        result = json.loads(response)
    except json.JSONDecodeError as exc:
        raise EventError("supabase_response_invalid") from exc
    if result == "correlation_pending":
        raise EventError("correlation_pending")
    if result not in {"applied", "duplicate"}:
        raise EventError("supabase_response_invalid")
    return result


def process_event(event: Any, *, env: Mapping[str, str], transport: Transport = urllib_transport) -> dict[str, Any]:
    if not _enabled(env):
        raise BatchError("consumer_disabled")
    top = _mapping(event, "batch_invalid")
    messages = top.get("messages")
    if not isinstance(messages, list) or not messages or len(messages) > MAX_BATCH_RECORDS:
        raise BatchError("batch_invalid")
    try:
        total = len(json.dumps(messages, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    except (TypeError, ValueError, UnicodeError) as exc:
        raise BatchError("batch_invalid") from exc
    if total > MAX_BATCH_BYTES:
        raise BatchError("batch_too_large")

    applied = duplicates = failures = 0
    for record in messages:
        event_hash = "unknown"
        try:
            payload = parse_record(record, env=env)
            event_hash = hashlib.sha256(payload["p_provider_event_key"].encode()).hexdigest()[:16]
            result = _store(payload, env=env, transport=transport)
            applied += result == "applied"
            duplicates += result == "duplicate"
            _log("INFO", "postbox_event_ok", event_hash=event_hash, result=result)
        except EventError as exc:
            failures += 1
            _log("ERROR", "postbox_event_failed", event_hash=event_hash, error_code=exc.code)
        except Exception:
            failures += 1
            _log("ERROR", "postbox_event_failed", event_hash=event_hash, error_code="transport_failed")
    if failures:
        raise BatchError("batch_failed")
    return {"ok": True, "applied": applied, "duplicates": duplicates, "records": len(messages)}


def handler(event: Any, context: Any) -> dict[str, Any]:
    del context
    return process_event(event, env=os.environ)
