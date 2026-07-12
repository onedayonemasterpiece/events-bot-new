from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from serverless.email_inbound.common.crypto import adapter_signature
from serverless.email_inbound.delivery.index import DeliveryError, process_queue_event


FIXTURE = (
    Path(__file__).parent / "fixtures" / "email_inbound" / "ymq_pointer.json"
)


def load_event() -> dict:
    return json.loads(FIXTURE.read_text())


def env() -> dict[str, str]:
    return {
        "EMAIL_INBOUND_ADAPTER_URL": "https://example.test/functions/v1/email-inbound",
        "EMAIL_INBOUND_ADAPTER_KEY_ID": "current",
        "EMAIL_INBOUND_ADAPTER_SECRET": "s" * 32,
    }


def test_delivery_signs_minimized_payload() -> None:
    captured = {}

    def transport(url, body, headers, timeout):
        captured.update(url=url, body=body, headers=headers, timeout=timeout)
        inbound_id = json.loads(body)["inbound_id"]
        return 200, json.dumps(
            {"ok": True, "status": "accepted", "inbound_id": inbound_id}
        ).encode()

    result = process_queue_event(
        load_event(), env=env(), transport=transport, now=lambda: 1_720_000_000
    )
    assert result["delivered"] == 1
    payload = captured["body"].decode()
    assert "sender@example" not in payload
    assert "subject" not in payload
    digest, signature = adapter_signature(
        secret="s" * 32,
        path="/functions/v1/email-inbound",
        timestamp=1_720_000_000,
        body=captured["body"],
    )
    assert captured["headers"]["X-Kenig-Content-SHA256"] == digest
    assert captured["headers"]["X-Kenig-Signature"] == f"v1.{signature}"


@pytest.mark.parametrize("status", [400, 401, 409, 429, 500])
def test_non_success_adapter_status_retries_via_ymq(status: int) -> None:
    def transport(*_args):
        return status, b"must not enter the error"

    with pytest.raises(DeliveryError, match=f"adapter_http_status:{status}"):
        process_queue_event(load_event(), env=env(), transport=transport)


def test_duplicate_ack_is_success() -> None:
    def transport(_url, body, _headers, _timeout):
        inbound_id = json.loads(body)["inbound_id"]
        return 200, json.dumps(
            {"ok": True, "status": "duplicate", "inbound_id": inbound_id}
        ).encode()

    assert process_queue_event(load_event(), env=env(), transport=transport)[
        "delivered"
    ] == 1


def test_wrong_ack_id_fails_closed() -> None:
    def transport(*_args):
        return 200, b'{"ok":true,"status":"accepted","inbound_id":"wrong"}'

    with pytest.raises(DeliveryError, match="adapter_response_invalid"):
        process_queue_event(load_event(), env=env(), transport=transport)


def test_transport_exception_is_redacted(caplog) -> None:
    caplog.set_level(logging.INFO)

    def transport(*_args):
        raise RuntimeError("body for sender@example.test")

    with pytest.raises(DeliveryError, match="adapter_transport_failed") as captured:
        process_queue_event(load_event(), env=env(), transport=transport)
    assert captured.value.__cause__ is None
    assert "sender@example.test" not in caplog.text


def test_adapter_requires_https() -> None:
    bad_env = env()
    bad_env["EMAIL_INBOUND_ADAPTER_URL"] = "http://example.test/adapter"
    with pytest.raises(DeliveryError, match="adapter_url_invalid"):
        process_queue_event(load_event(), env=bad_env, transport=lambda *_: (200, b"{}"))
