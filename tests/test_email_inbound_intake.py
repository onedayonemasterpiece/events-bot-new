from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from serverless.email_inbound.intake.index import IntakeError, process_mail_event


FIXTURES = Path(__file__).parent / "fixtures" / "email_inbound"


class FakeS3:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls: list[dict] = []

    def put_object(self, **kwargs):
        if self.fail:
            raise RuntimeError("provider error with sender@example.test")
        self.calls.append(kwargs)
        return {"ETag": "fixture"}


class FakeSQS:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls: list[dict] = []

    def send_message(self, **kwargs):
        if self.fail:
            raise RuntimeError("provider error with sender@example.test")
        self.calls.append(kwargs)
        return {"MessageId": "fixture-message"}


def load_event() -> dict:
    return json.loads((FIXTURES / "mail_trigger_minimal.json").read_text())


def env() -> dict[str, str]:
    return {
        "EMAIL_INBOUND_BUCKET": "kenigevents-email-inbound-test",
        "EMAIL_INBOUND_QUEUE_URL": "https://queue.invalid/test",
        "EMAIL_INBOUND_MAILBOX": "info@kenigevents.ru",
        "EMAIL_INBOUND_IDEMPOTENCY_SECRET": "i" * 32,
    }


def test_intake_uploads_before_enqueuing_and_is_retry_stable() -> None:
    s3 = FakeS3()
    sqs = FakeSQS()
    first = process_mail_event(load_event(), s3_client=s3, sqs_client=sqs, env=env())
    second = process_mail_event(load_event(), s3_client=s3, sqs_client=sqs, env=env())
    assert first == second
    assert len(s3.calls) == len(sqs.calls) == 2
    assert s3.calls[0]["Key"] == s3.calls[1]["Key"]
    assert s3.calls[0]["Body"] == s3.calls[1]["Body"]
    pointer = json.loads(sqs.calls[0]["MessageBody"])
    assert pointer["inbound_id"] == first["inbound_ids"][0]


def test_upload_failure_does_not_enqueue() -> None:
    sqs = FakeSQS()
    with pytest.raises(IntakeError, match="provider_operation_failed"):
        process_mail_event(
            load_event(), s3_client=FakeS3(fail=True), sqs_client=sqs, env=env()
        )
    assert sqs.calls == []


def test_logs_do_not_echo_provider_exception_or_pii(caplog) -> None:
    caplog.set_level(logging.INFO)
    with pytest.raises(IntakeError) as captured:
        process_mail_event(
            load_event(), s3_client=FakeS3(fail=True), sqs_client=FakeSQS(), env=env()
        )
    assert captured.value.__cause__ is None
    assert "sender@example.test" not in caplog.text
    assert "KE-MAIL-E2E" not in caplog.text
    assert "provider_operation_failed" in caplog.text


def test_intake_rejects_large_batch() -> None:
    event = load_event()
    event["messages"] *= 11
    with pytest.raises(IntakeError, match="event_batch_too_large"):
        process_mail_event(event, s3_client=FakeS3(), sqs_client=FakeSQS(), env=env())
