from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

try:
    from ..common.contract import ADAPTER_SCHEMA, POINTER_SCHEMA, validate_pointer
    from ..common.crypto import SIGNATURE_VERSION, adapter_signature
except ImportError:
    from common.contract import ADAPTER_SCHEMA, POINTER_SCHEMA, validate_pointer
    from common.crypto import SIGNATURE_VERSION, adapter_signature


MAX_BODY_BYTES = 32_768
Transport = Callable[[str, bytes, Mapping[str, str], float], tuple[int, bytes]]


class AdapterError(RuntimeError):
    def __init__(self, code: str, status: int = 400):
        super().__init__(code)
        self.code = code
        self.status = status


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name) or "").strip()
    if not value:
        raise AdapterError(f"env_missing:{name.lower()}", 500)
    return value


def _headers(event: Mapping[str, Any]) -> dict[str, str]:
    raw = event.get("headers") or {}
    if not isinstance(raw, Mapping):
        raise AdapterError("headers_invalid")
    return {str(key).lower(): str(value).strip() for key, value in raw.items()}


def _request_body(event: Mapping[str, Any]) -> bytes:
    value = event.get("body")
    if not isinstance(value, str):
        raise AdapterError("body_invalid")
    try:
        body = base64.b64decode(value, validate=True) if event.get("isBase64Encoded") else value.encode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise AdapterError("body_invalid") from exc
    if not body or len(body) > MAX_BODY_BYTES:
        raise AdapterError("body_size_invalid", 413)
    return body


def _validate_signature(
    *, body: bytes, headers: Mapping[str, str], path: str, env: Mapping[str, str], now: Callable[[], float]
) -> None:
    key_id = headers.get("x-kenig-key-id", "")
    allowed_ids = {_required(env, "EMAIL_INBOUND_ADAPTER_KEY_ID")}
    previous_id = str(env.get("EMAIL_INBOUND_ADAPTER_PREVIOUS_KEY_ID") or "").strip()
    if previous_id:
        allowed_ids.add(previous_id)
    if key_id not in allowed_ids:
        raise AdapterError("signature_key_unknown", 401)
    try:
        timestamp = int(headers.get("x-kenig-timestamp", ""))
    except ValueError as exc:
        raise AdapterError("signature_timestamp_invalid", 401) from exc
    if abs(int(now()) - timestamp) > 300:
        raise AdapterError("signature_timestamp_expired", 401)
    digest = hashlib.sha256(body).hexdigest()
    if not hmac.compare_digest(digest, headers.get("x-kenig-content-sha256", "")):
        raise AdapterError("signature_digest_invalid", 401)
    secret_name = (
        "EMAIL_INBOUND_ADAPTER_SECRET"
        if key_id == env.get("EMAIL_INBOUND_ADAPTER_KEY_ID")
        else "EMAIL_INBOUND_ADAPTER_PREVIOUS_SECRET"
    )
    _digest, signature = adapter_signature(
        secret=_required(env, secret_name), path=path, timestamp=timestamp, body=body
    )
    expected = f"{SIGNATURE_VERSION}.{signature}"
    if not hmac.compare_digest(expected, headers.get("x-kenig-signature", "")):
        raise AdapterError("signature_invalid", 401)


def _validated_payload(body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdapterError("body_json_invalid") from exc
    if not isinstance(payload, dict) or payload.get("schema") != ADAPTER_SCHEMA:
        raise AdapterError("payload_schema_invalid")
    pointer = dict(payload)
    pointer["schema"] = POINTER_SCHEMA
    try:
        validate_pointer(pointer)
    except ValueError as exc:
        raise AdapterError("payload_contract_invalid") from exc
    if pointer["mailbox"] != "info@kenigevents.ru":
        raise AdapterError("payload_mailbox_invalid")
    return payload


def _rpc_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    obj = payload["object"]
    body = payload["body"]
    return {
        "p_inbound_id": payload["inbound_id"],
        "p_contract_schema": payload["schema"],
        "p_mailbox": payload["mailbox"],
        "p_received_at": payload["received_at"],
        "p_object_bucket": obj["bucket"],
        "p_object_key": obj["key"],
        "p_object_sha256": obj["sha256"],
        "p_object_expires_at": obj["expires_at"],
        "p_message_id_hmac": payload.get("message_id_hmac"),
        "p_sender_hmac": payload.get("sender_hmac"),
        "p_body_bytes": body["bytes"],
        "p_body_sha256": body["sha256"],
        "p_body_media_type": body["media_type"],
        "p_attachment_count": payload["attachments"]["count"],
    }


def urllib_transport(url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read(16_384)
    except urllib.error.HTTPError as exc:
        exc.read(16_384)
        return int(exc.code), b""


def process_http_event(
    event: Any,
    *,
    env: Mapping[str, str],
    transport: Transport = urllib_transport,
    now: Callable[[], float] = time.time,
) -> tuple[int, dict[str, Any]]:
    if not isinstance(event, Mapping):
        raise AdapterError("event_invalid")
    if str(event.get("httpMethod") or "").upper() != "POST":
        raise AdapterError("method_not_allowed", 405)
    headers = _headers(event)
    if headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise AdapterError("content_type_invalid", 415)
    body = _request_body(event)
    path = _required(env, "EMAIL_INBOUND_ADAPTER_PATH")
    if not path.startswith("/") or "?" in path or "#" in path:
        raise AdapterError("env_invalid:email_inbound_adapter_path", 500)
    _validate_signature(body=body, headers=headers, path=path, env=env, now=now)
    payload = _validated_payload(body)
    base_url = _required(env, "PERSONALIZATION_SUPABASE_URL").rstrip("/")
    parsed = urlsplit(base_url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path:
        raise AdapterError("env_invalid:personalization_supabase_url", 500)
    key = _required(env, "PERSONALIZATION_SUPABASE_SECRET_KEY")
    rpc_body = json.dumps(_rpc_payload(payload), separators=(",", ":")).encode("utf-8")
    status, response_body = transport(
        f"{base_url}/rest/v1/rpc/email_record_inbound_receipt_v1",
        rpc_body,
        {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "apikey": key,
            "User-Agent": "KenigEventsEmailAdapter/1.0",
        },
        10.0,
    )
    if status < 200 or status >= 300:
        raise AdapterError("receipt_store_failed", 503)
    try:
        result = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise AdapterError("receipt_response_invalid", 503) from exc
    if result not in {"accepted", "duplicate"}:
        raise AdapterError("receipt_response_invalid", 503)
    return 200, {"ok": True, "status": result, "inbound_id": payload["inbound_id"]}


def _response(status: int, body: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store"},
        "body": json.dumps(dict(body), separators=(",", ":")),
    }


def handler(event: Any, context: Any) -> dict[str, Any]:
    del context
    try:
        status, body = process_http_event(event, env=os.environ)
        return _response(status, body)
    except AdapterError as exc:
        return _response(exc.status, {"ok": False, "error": exc.code})
    except Exception:
        return _response(500, {"ok": False, "error": "internal_error"})
