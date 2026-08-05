from __future__ import annotations

import json

from scripts.ops import postbox_dlq_recover as recover


def record(event_id="event-1", message_id="message-1", event_type="Send"):
    return {
        "eventId": event_id,
        "eventType": event_type,
        "mail": {"messageId": message_id, "timestamp": "2026-08-04T10:00:00Z"},
    }


class FakeSqs:
    def __init__(self, bodies):
        self.messages = [
            {
                "MessageId": f"queue-{index}",
                "ReceiptHandle": f"receipt-{index}",
                "Body": json.dumps(body),
                "Attributes": {"SentTimestamp": "1785837600000"},
            }
            for index, body in enumerate(bodies)
        ]
        self.cursor = 0
        self.deleted = []
        self.restored = []

    def get_queue_attributes(self, **_kwargs):
        visible = len(self.messages) - self.cursor
        return {"Attributes": {
            "ApproximateNumberOfMessages": str(visible),
            "ApproximateNumberOfMessagesNotVisible": str(self.cursor),
        }}

    def receive_message(self, **kwargs):
        if self.cursor >= len(self.messages):
            return {}
        size = kwargs["MaxNumberOfMessages"]
        result = self.messages[self.cursor:self.cursor + size]
        self.cursor += len(result)
        return {"Messages": result}

    def change_message_visibility_batch(self, **kwargs):
        self.restored.extend(item["ReceiptHandle"] for item in kwargs["Entries"])
        return {"Successful": kwargs["Entries"]}

    def change_message_visibility(self, **kwargs):
        self.restored.append(kwargs["ReceiptHandle"])

    def delete_message(self, **kwargs):
        self.deleted.append(kwargs["ReceiptHandle"])


def test_inventory_is_exact_sanitized_and_restores_visibility(monkeypatch):
    sqs = FakeSqs([
        record(),
        {"messages": [record("event-2", "message-2", "Delivery")]},
    ])
    monkeypatch.setattr(recover, "_classify", lambda ids: {
        recover._sha(value): {
            "source_classification": "focus_auth",
            "correlation_status": "bound",
        }
        for value in ids
    })
    monkeypatch.setattr(recover.time, "sleep", lambda _seconds: None)
    result = recover.inventory(sqs, "https://queue.invalid/private", max_messages=10, visibility=300)

    assert result["inventory"] == {
        "queue_messages": 2,
        "unique_event_ids": 2,
        "unique_message_ids": 2,
        "event_types": {"Delivery": 1, "Send": 1},
        "classifications": {"focus_auth:bound": 2},
        "malformed_or_unsupported": 0,
    }
    serialized = json.dumps(result)
    assert "message-1" not in serialized
    assert "event-1" not in serialized
    assert "queue.invalid" not in serialized
    assert sorted(sqs.restored) == ["receipt-0", "receipt-1"]


def test_inventory_retains_malformed_body_as_stable_error(monkeypatch):
    sqs = FakeSqs([{"unexpected": "recipient@example.test"}])
    monkeypatch.setattr(recover, "_classify", lambda _ids: {})
    monkeypatch.setattr(recover.time, "sleep", lambda _seconds: None)
    result = recover.inventory(sqs, "queue", max_messages=10, visibility=300)
    assert result["inventory"]["malformed_or_unsupported"] == 1
    assert result["messages"][0]["stable_error_code"] == "ymq_envelope_unsupported"
    assert "recipient@example.test" not in json.dumps(result)


def test_replay_deletes_only_success_and_releases_failure_tail(monkeypatch):
    sqs = FakeSqs([record("ok", "message-ok"), record("bad", "message-bad")])
    outcomes = iter((
        {"ok": True, "records": 1, "applied": 1, "duplicates": 0},
        recover.consumer.BatchError("batch_failed"),
    ))

    def process(*_args, **_kwargs):
        value = next(outcomes)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(recover.consumer, "process_event", process)
    approved = {
        recover._sha(message["MessageId"]): recover._sha(message["Body"])
        for message in sqs.messages
    }
    result = recover.replay(
        sqs,
        "queue",
        batch_size=2,
        approved=approved,
        inventory_sha256="a" * 64,
    )
    assert sqs.deleted == ["receipt-0"]
    assert sqs.restored == ["receipt-1"]
    assert result["totals"] == {"deleted": 1, "applied": 1, "duplicate": 0, "retained": 1}


def test_replay_refuses_message_not_in_reviewed_inventory(monkeypatch):
    sqs = FakeSqs([record("new", "message-new")])
    monkeypatch.setattr(
        recover.consumer,
        "process_event",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )
    result = recover.replay(
        sqs,
        "queue",
        batch_size=1,
        approved={"0" * 64: "1" * 64},
        inventory_sha256="a" * 64,
    )
    assert sqs.deleted == []
    assert sqs.restored == ["receipt-0"]
    assert result["messages"][0]["stable_error_code"] == (
        "queue_message_not_in_reviewed_inventory"
    )


def test_reviewed_inventory_binds_file_integrity_and_target_queue(tmp_path):
    unsigned = {
        "schema": recover.SCHEMA,
        "queue_url_sha256": recover._sha("queue-a"),
        "messages": [{
            "queue_message_sha256": "a" * 64,
            "body_sha256": "b" * 64,
        }],
    }
    value = {**unsigned, "manifest_sha256": recover._sha(recover._canonical(unsigned))}
    path = tmp_path / "inventory.json"
    path.write_bytes(recover._canonical(value))
    file_sha = recover._sha(path.read_bytes())
    assert recover.load_reviewed_inventory(path, file_sha, "queue-a") == {
        "a" * 64: "b" * 64
    }
    try:
        recover.load_reviewed_inventory(path, file_sha, "queue-b")
    except recover.RecoveryError as exc:
        assert str(exc) == "inventory_queue_mismatch"
    else:
        raise AssertionError("queue mismatch was accepted")
