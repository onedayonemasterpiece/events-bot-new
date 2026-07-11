from __future__ import annotations

import json
from pathlib import Path

import pytest

from serverless.email_inbound.adapter.index import AdapterError, process_http_event
from serverless.email_inbound.common.contract import ADAPTER_SCHEMA, canonical_json
from serverless.email_inbound.common.crypto import adapter_signature


FIXTURES = Path(__file__).parent / "fixtures" / "email_inbound"
PATH = "/d4e-test-adapter"
NOW = 1_783_800_000


def _env() -> dict[str, str]:
    return {
        "EMAIL_INBOUND_ADAPTER_KEY_ID": "current-2026-07",
        "EMAIL_INBOUND_ADAPTER_SECRET": "adapter-secret-that-is-definitely-long-enough",
        "EMAIL_INBOUND_ADAPTER_PATH": PATH,
        "PERSONALIZATION_SUPABASE_URL": "https://example.supabase.co",
        "PERSONALIZATION_SUPABASE_SECRET_KEY": "sb_secret_fixture_not_a_real_key",
    }


def _event() -> dict[str, object]:
    fixture = json.loads((FIXTURES / "ymq_pointer.json").read_text())
    payload = json.loads(fixture["messages"][0]["details"]["message"]["body"])
    payload["schema"] = ADAPTER_SCHEMA
    body = canonical_json(payload)
    digest, signature = adapter_signature(
        secret=_env()["EMAIL_INBOUND_ADAPTER_SECRET"],
        path=PATH,
        timestamp=NOW,
        body=body,
    )
    return {
        "httpMethod": "POST",
        "headers": {
            "content-type": "application/json",
            "x-kenig-key-id": "current-2026-07",
            "x-kenig-timestamp": str(NOW),
            "x-kenig-content-sha256": digest,
            "x-kenig-signature": f"v1.{signature}",
        },
        "body": body.decode(),
        "isBase64Encoded": False,
    }


@pytest.mark.parametrize("result", ["accepted", "duplicate"])
def test_signed_receipt_uses_secret_apikey_and_returns_ack(result: str) -> None:
    calls: list[tuple[str, dict[str, object], dict[str, str]]] = []

    def transport(url: str, body: bytes, headers: dict[str, str], timeout: float):
        calls.append((url, json.loads(body), headers))
        assert timeout == 10.0
        return 200, json.dumps(result).encode()

    status, response = process_http_event(
        _event(), env=_env(), transport=transport, now=lambda: NOW
    )

    assert status == 200
    assert response["status"] == result
    assert response["inbound_id"]
    assert calls[0][0].endswith("/rest/v1/rpc/email_record_inbound_receipt_v1")
    assert calls[0][2]["apikey"].startswith("sb_secret_")
    assert "Authorization" not in calls[0][2]
    assert calls[0][1]["p_contract_schema"] == ADAPTER_SCHEMA
    assert calls[0][1]["p_attachment_count"] == 0


def test_invalid_signature_never_calls_supabase() -> None:
    event = _event()
    event["headers"]["x-kenig-signature"] = "v1.invalid"

    with pytest.raises(AdapterError, match="signature_invalid"):
        process_http_event(
            event,
            env=_env(),
            transport=lambda *_args: pytest.fail("transport must not run"),
            now=lambda: NOW,
        )


def test_expired_signature_fails_closed() -> None:
    with pytest.raises(AdapterError, match="signature_timestamp_expired"):
        process_http_event(
            _event(),
            env=_env(),
            transport=lambda *_args: pytest.fail("transport must not run"),
            now=lambda: NOW + 301,
        )


def test_provider_error_is_stable_and_does_not_echo_response() -> None:
    with pytest.raises(AdapterError, match="receipt_store_failed"):
        process_http_event(
            _event(),
            env=_env(),
            transport=lambda *_args: (500, b"sensitive upstream response"),
            now=lambda: NOW,
        )
