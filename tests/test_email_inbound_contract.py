from __future__ import annotations

import json
from pathlib import Path

import pytest

from serverless.email_inbound.common.contract import (
    ContractError,
    build_adapter_payload,
    build_envelope_and_pointer,
    canonical_json,
    normalize_headers,
    validate_pointer,
)


FIXTURES = Path(__file__).parent / "fixtures" / "email_inbound"
SECRET = "i" * 32


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_build_envelope_is_deterministic_and_filters_headers() -> None:
    message = load_fixture("mail_trigger_minimal.json")["messages"][0]
    first = build_envelope_and_pointer(
        message,
        mailbox="info@kenigevents.ru",
        bucket="kenigevents-email-inbound-test",
        idempotency_secret=SECRET,
    )
    second = build_envelope_and_pointer(
        message,
        mailbox="info@kenigevents.ru",
        bucket="kenigevents-email-inbound-test",
        idempotency_secret=SECRET,
    )
    envelope, pointer, envelope_bytes = first
    assert first == second
    assert "received" not in envelope["headers"]
    assert envelope["received_at"] == "2026-07-11T20:30:00.123000Z"
    assert pointer["object"]["key"].endswith(
        f"/{pointer['inbound_id']}/envelope.json"
    )
    assert canonical_json(envelope) == envelope_bytes
    assert len(canonical_json(pointer)) < 2_000


def test_attachment_order_does_not_change_idempotency_key() -> None:
    message = load_fixture("mail_trigger_attachment.json")["messages"][0]
    first = build_envelope_and_pointer(
        message,
        mailbox="info@kenigevents.ru",
        bucket="kenigevents-email-inbound-test",
        idempotency_secret=SECRET,
    )[1]
    message["attachments"]["keys"].reverse()
    second = build_envelope_and_pointer(
        message,
        mailbox="info@kenigevents.ru",
        bucket="kenigevents-email-inbound-test",
        idempotency_secret=SECRET,
    )[1]
    assert first["inbound_id"] == second["inbound_id"]


def test_changed_attachment_reference_changes_idempotency_key() -> None:
    message = load_fixture("mail_trigger_attachment.json")["messages"][0]
    first = build_envelope_and_pointer(
        message,
        mailbox="info@kenigevents.ru",
        bucket="kenigevents-email-inbound-test",
        idempotency_secret=SECRET,
    )[1]
    message["attachments"]["keys"][0] = "opaque/different"
    second = build_envelope_and_pointer(
        message,
        mailbox="info@kenigevents.ru",
        bucket="kenigevents-email-inbound-test",
        idempotency_secret=SECRET,
    )[1]
    assert first["inbound_id"] != second["inbound_id"]


def test_oversize_body_fails_closed() -> None:
    message = load_fixture("mail_trigger_minimal.json")["messages"][0]
    message["message"] = "x" * 11
    with pytest.raises(ContractError, match="body_too_large"):
        build_envelope_and_pointer(
            message,
            mailbox="info@kenigevents.ru",
            bucket="kenigevents-email-inbound-test",
            idempotency_secret=SECRET,
            max_body_bytes=10,
        )


def test_header_budget_and_unknown_headers() -> None:
    assert normalize_headers([{"name": "X-Unknown", "values": ["ignored"]}]) == {}
    with pytest.raises(ContractError, match="header_value_too_large"):
        normalize_headers([{"name": "Subject", "values": ["x" * 8_193]}])


def test_pointer_fixture_and_adapter_projection() -> None:
    queue_event = load_fixture("ymq_pointer.json")
    pointer = json.loads(queue_event["messages"][0]["details"]["message"]["body"])
    assert validate_pointer(pointer) is pointer
    adapter = build_adapter_payload(pointer)
    assert adapter["schema"] == "kenigevents.email_inbound.adapter.v1"
    serialized = canonical_json(adapter).decode("utf-8")
    assert "sender@example" not in serialized
    assert "subject" not in serialized
