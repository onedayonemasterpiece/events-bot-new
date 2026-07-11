from __future__ import annotations

import io
import json

from serverless.email_inbound.collector.index import STATE_SCHEMA, collect, normalized_body


RAW = b"""From: Person <sender@example.test>\r
To: info@kenigevents.ru\r
Subject: Collector fixture\r
Message-ID: <collector-2@example.test>\r
Content-Type: multipart/mixed; boundary=x\r
\r
--x\r
Content-Type: text/plain; charset=utf-8\r
\r
Hello from IMAP.\r
--x\r
Content-Type: application/octet-stream\r
Content-Disposition: attachment; filename=test.bin\r
Content-Transfer-Encoding: base64\r
\r
U0VDUkVU\r
--x--\r
"""


class MissingKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3:
    def __init__(self, state: dict | None = None):
        self.state = state
        self.puts: list[dict] = []

    def get_object(self, **_kwargs):
        if self.state is None:
            raise MissingKey()
        return {"Body": io.BytesIO(json.dumps(self.state).encode())}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        if kwargs["Key"].startswith("state/"):
            self.state = json.loads(kwargs["Body"])
        return {}


class FakeSQS:
    def __init__(self):
        self.messages: list[str] = []

    def send_message(self, *, MessageBody: str, **_kwargs):
        self.messages.append(MessageBody)
        return {"MessageId": "queue-fixture"}


class FakeImap:
    def __init__(self, *_args, **_kwargs):
        self.fetches: list[int] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def login(self, *_args):
        return "OK", []

    def select(self, *_args, **_kwargs):
        return "OK", [b"2"]

    def status(self, *_args):
        return "OK", [b'"INBOX" (UIDVALIDITY 77)']

    def uid(self, action, _charset, criterion):
        assert action == "search"
        return "OK", [b"1 2" if criterion == "ALL" else b"2"]


class FetchingImap(FakeImap):
    def uid(self, action, *args):
        if action == "fetch":
            uid = int(args[0])
            self.fetches.append(uid)
            return "OK", [(b"RFC822", RAW)]
        return super().uid(action, *args)


def _env() -> dict[str, str]:
    return {
        "EMAIL_INBOUND_BUCKET": "kenigevents-email-inbound-test",
        "EMAIL_INBOUND_QUEUE_URL": "https://queue.invalid/processing",
        "EMAIL_INBOUND_MAILBOX": "info@kenigevents.ru",
        "EMAIL_INBOUND_IDEMPOTENCY_SECRET": "idempotency-secret-that-is-at-least-thirty-two-bytes",
        "EMAIL_INBOUND_IMAP_HOST": "imap.spaceweb.test",
        "EMAIL_INBOUND_IMAP_LOGIN": "info@kenigevents.ru",
        "EMAIL_INBOUND_IMAP_PASSWORD": "not-a-real-password",
    }


def test_normalized_body_skips_attachment_payload() -> None:
    import email

    value = normalized_body(email.message_from_bytes(RAW))
    assert "Hello from IMAP." in value
    assert "U0VDUkVU" not in value


def test_first_run_bootstraps_without_replaying_existing_mail() -> None:
    s3 = FakeS3()
    sqs = FakeSQS()

    result = collect(env=_env(), s3_client=s3, sqs_client=sqs, imap_factory=FakeImap)

    assert result == {"ok": True, "bootstrapped": True, "collected": 0, "last_uid": 2}
    assert s3.state["schema"] == STATE_SCHEMA
    assert s3.state["last_uid"] == 2
    assert sqs.messages == []


def test_new_uid_is_copied_without_changing_imap_seen_state() -> None:
    s3 = FakeS3({"schema": STATE_SCHEMA, "uidvalidity": "77", "last_uid": 1})
    sqs = FakeSQS()

    result = collect(env=_env(), s3_client=s3, sqs_client=sqs, imap_factory=FetchingImap)

    assert result["collected"] == 1
    assert result["last_uid"] == 2
    assert s3.state["last_uid"] == 2
    assert len(sqs.messages) == 1
    pointer = json.loads(sqs.messages[0])
    assert pointer["mailbox"] == "info@kenigevents.ru"
    assert pointer["attachments"]["count"] == 0
