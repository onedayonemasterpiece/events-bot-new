from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

try:  # Repository import.
    from ..common.contract import (
        ContractError,
        build_adapter_payload,
        canonical_json,
        validate_pointer,
    )
    from ..common.crypto import SIGNATURE_VERSION, adapter_signature
    from ..common.safe_logging import safe_log
except ImportError:  # Cloud Functions ZIP import.
    from common.contract import (
        ContractError,
        build_adapter_payload,
        canonical_json,
        validate_pointer,
    )
    from common.crypto import SIGNATURE_VERSION, adapter_signature
    from common.safe_logging import safe_log


LOGGER = logging.getLogger(__name__)
_KEY_ID_RE = re.compile(r"^[a-z0-9_-]{1,40}$")


class DeliveryError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


Transport = Callable[[str, bytes, Mapping[str, str], float], tuple[int, bytes]]


def _required_env(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name) or "").strip()
    if not value:
        raise DeliveryError(f"env_missing:{name.lower()}")
    return value


def _adapter_endpoint(env: Mapping[str, str]) -> tuple[str, str]:
    endpoint = _required_env(env, "EMAIL_INBOUND_ADAPTER_URL")
    parsed = urlsplit(endpoint)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise DeliveryError("adapter_url_invalid")
    if parsed.query or parsed.fragment:
        raise DeliveryError("adapter_url_invalid")
    return endpoint, parsed.path or "/"


def _queue_messages(event: Any) -> list[Mapping[str, Any]]:
    if not isinstance(event, Mapping):
        raise DeliveryError("event_not_object")
    messages = event.get("messages")
    if not isinstance(messages, list) or not messages:
        raise DeliveryError("event_messages_invalid")
    if len(messages) > 10:
        raise DeliveryError("event_batch_too_large")
    if not all(isinstance(message, Mapping) for message in messages):
        raise DeliveryError("event_message_invalid")
    return messages


def _pointer_from_queue_message(message: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    details = message.get("details")
    if not isinstance(details, Mapping):
        raise DeliveryError("queue_details_invalid")
    queue_message = details.get("message")
    if not isinstance(queue_message, Mapping):
        raise DeliveryError("queue_message_invalid")
    body = queue_message.get("body")
    if not isinstance(body, str) or len(body.encode("utf-8")) > 32_768:
        raise DeliveryError("queue_body_invalid")
    try:
        pointer = json.loads(body)
    except json.JSONDecodeError as exc:
        raise DeliveryError("queue_body_json_invalid") from exc
    try:
        validated = validate_pointer(pointer)
    except ContractError as exc:
        raise DeliveryError(exc.code) from exc
    return validated, str(queue_message.get("message_id") or "")


def urllib_transport(
    url: str, body: bytes, headers: Mapping[str, str], timeout: float
) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        data=body,
        headers=dict(headers),
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read(16_384)
    except urllib.error.HTTPError as exc:
        exc.read(16_384)
        return int(exc.code), b""


def process_queue_event(
    event: Any,
    *,
    env: Mapping[str, str],
    transport: Transport = urllib_transport,
    now: Callable[[], float] = time.time,
    logger: logging.Logger = LOGGER,
) -> dict[str, Any]:
    endpoint, path = _adapter_endpoint(env)
    key_id = _required_env(env, "EMAIL_INBOUND_ADAPTER_KEY_ID")
    if not _KEY_ID_RE.fullmatch(key_id):
        raise DeliveryError("adapter_key_id_invalid")
    secret = _required_env(env, "EMAIL_INBOUND_ADAPTER_SECRET")
    try:
        timeout = float(str(env.get("EMAIL_INBOUND_ADAPTER_TIMEOUT_SECONDS") or "10"))
    except ValueError as exc:
        raise DeliveryError("adapter_timeout_invalid") from exc
    if timeout <= 0 or timeout > 30:
        raise DeliveryError("adapter_timeout_invalid")

    delivered: list[str] = []
    for message in _queue_messages(event):
        inbound_id = ""
        queue_message_id = ""
        try:
            pointer, queue_message_id = _pointer_from_queue_message(message)
            inbound_id = pointer["inbound_id"]
            body = canonical_json(build_adapter_payload(pointer))
            if len(body) > 32_768:
                raise DeliveryError("adapter_payload_too_large")
            timestamp = int(now())
            digest, signature = adapter_signature(
                secret=secret,
                path=path,
                timestamp=timestamp,
                body=body,
            )
            status, response_body = transport(
                endpoint,
                body,
                {
                    "Content-Type": "application/json",
                    "X-Kenig-Key-Id": key_id,
                    "X-Kenig-Timestamp": str(timestamp),
                    "X-Kenig-Content-SHA256": digest,
                    "X-Kenig-Signature": f"{SIGNATURE_VERSION}.{signature}",
                },
                timeout,
            )
            if status < 200 or status >= 300:
                raise DeliveryError(f"adapter_http_status:{status}")
            try:
                acknowledgement = json.loads(response_body or b"{}")
            except json.JSONDecodeError as exc:
                raise DeliveryError("adapter_response_json_invalid") from exc
            if (
                not isinstance(acknowledgement, Mapping)
                or acknowledgement.get("ok") is not True
                or acknowledgement.get("inbound_id") != inbound_id
                or acknowledgement.get("status") not in {"accepted", "duplicate"}
            ):
                raise DeliveryError("adapter_response_invalid")
            safe_log(
                logger,
                logging.INFO,
                stage="adapter_delivered",
                inbound_id=inbound_id,
                queue_message_id=queue_message_id,
            )
            delivered.append(inbound_id)
        except (ContractError, DeliveryError) as exc:
            code = exc.code if hasattr(exc, "code") else "delivery_contract_failed"
            safe_log(
                logger,
                logging.ERROR,
                stage="adapter_failed",
                inbound_id=inbound_id,
                queue_message_id=queue_message_id,
                error_code=code,
            )
            if isinstance(exc, DeliveryError):
                raise
            raise DeliveryError(code) from exc
        except Exception:
            safe_log(
                logger,
                logging.ERROR,
                stage="adapter_failed",
                inbound_id=inbound_id,
                queue_message_id=queue_message_id,
                error_code="adapter_transport_failed",
            )
            # Do not chain transport exceptions: HTTP/TLS errors can contain the
            # endpoint or response details and platform logs include tracebacks.
            raise DeliveryError("adapter_transport_failed") from None
    return {"ok": True, "delivered": len(delivered), "inbound_ids": delivered}


def handler(event: Any, context: Any) -> dict[str, Any]:
    del context
    return process_queue_event(event, env=os.environ)
