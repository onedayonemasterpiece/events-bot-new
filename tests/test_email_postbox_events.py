from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from serverless.email_postbox_events import index


ENV = {
    "POSTBOX_EVENT_CONSUMER_ENABLED": "true",
    "POSTBOX_EXPECTED_IDENTITY_ID": "identity-1",
    "POSTBOX_EXPECTED_CONFIGURATION_TAG": "config-1",
    "POSTBOX_EXPECTED_FROM_DOMAIN": "kenigevents.ru",
    "EMAIL_ADDRESS_HMAC_KEY": "fixture-hmac-key",
    "EMAIL_ADDRESS_HMAC_KEY_VERSION": "1",
    "PERSONALIZATION_SUPABASE_URL": "https://example.supabase.co",
    "PERSONALIZATION_SUPABASE_SECRET_KEY": "sb_secret_fixture_not_real",
}


def event(kind: str = "Delivery", *, recipient: str = "User@Example.test") -> dict:
    row = {
        "eventId": f"event-{kind}",
        "eventType": kind,
        "mail": {
            "timestamp": "2026-07-12T07:00:00+00:00",
            "messageId": "provider-message-1",
            "identityId": "identity-1",
            "commonHeaders": {"to": [f"Recipient <{recipient}>"]},
            "tags": {
                "ses:configuration-set": ["config-1"],
                "ses:from-domain": ["kenigevents.ru"],
            },
        },
    }
    if kind == "Delivery":
        row["delivery"] = {"timestamp": "2026-07-12T07:01:00Z", "recipients": [recipient]}
    elif kind == "Bounce":
        row["bounce"] = {
            "timestamp": "2026-07-12T07:02:00Z",
            "bounceType": "Permanent",
            "bouncedRecipients": [{"emailAddress": recipient}],
        }
    elif kind == "DeliveryDelay":
        row["deliveryDelay"] = {
            "timestamp": "2026-07-12T07:03:00Z",
            "delayedRecipients": [{"emailAddress": recipient}],
        }
    elif kind == "Complaint":
        row["complaint"] = {
            "timestamp": "2026-07-12T07:04:00Z",
            "complainedRecipients": [{"emailAddress": recipient}],
        }
    elif kind == "Subscription":
        row["subscription"] = {"timestamp": "2026-07-12T07:05:00Z", "source": "UnsubscribeHeader"}
    elif kind == "Open":
        row["open"] = {"timestamp": "2026-07-12T07:06:00Z"}
    elif kind == "Click":
        row["click"] = {"timestamp": "2026-07-12T07:07:00Z"}
    elif kind == "Rendering Failure":
        row["failure"] = {"errorMessage": "hidden"}
    elif kind == "Send":
        row["send"] = {}
    return row


def transport_result(result: str = "applied", status: int = 200):
    calls = []

    def transport(url, body, headers, timeout):
        calls.append((url, json.loads(body), headers, timeout))
        return status, json.dumps(result).encode()

    return calls, transport


@pytest.mark.parametrize(
    ("provider_type", "internal", "timestamp"),
    [
        ("Send", "accepted", "2026-07-12T07:00:00Z"),
        ("Rendering Failure", "rendering_failure", "2026-07-12T07:00:00Z"),
        ("Delivery", "delivered", "2026-07-12T07:01:00Z"),
        ("Bounce", "hard_bounce", "2026-07-12T07:02:00Z"),
        ("DeliveryDelay", "delivery_delay", "2026-07-12T07:03:00Z"),
        ("Subscription", "unsubscribe", "2026-07-12T07:05:00Z"),
        ("Complaint", "complaint", "2026-07-12T07:04:00Z"),
        ("Open", "open", "2026-07-12T07:06:00Z"),
        ("Click", "click", "2026-07-12T07:07:00Z"),
    ],
)
def test_all_provider_event_mappings(provider_type, internal, timestamp):
    payload = index.parse_record(event(provider_type), env=ENV)
    assert payload["p_event_type"] == internal
    assert payload["p_event_at"] == timestamp
    assert payload["p_provider_message_id"] == "provider-message-1"
    assert len(payload["p_payload_sha256"]) == 64


def test_recipient_hmac_is_normalized_base64url_without_padding():
    payload = index.parse_record(event(recipient="  USER@Example.Test "), env=ENV)
    expected = base64.urlsafe_b64encode(
        hmac.new(b"fixture-hmac-key", b"user@example.test", hashlib.sha256).digest()
    ).decode().rstrip("=")
    assert payload["p_recipient_hmac"] == expected
    assert len(expected) == 43 and "=" not in expected


def test_documented_bounce_typo_is_accepted():
    row = event("Bounce")
    row["bounce"]["bounceType"] = "Permenent"
    assert index.parse_record(row, env=ENV)["p_event_type"] == "hard_bounce"


@pytest.mark.parametrize(
    "mutator,error",
    [
        (lambda row: row.pop("eventId"), "event_id_invalid"),
        (lambda row: row["mail"].update(identityId="other"), "identity_mismatch"),
        (lambda row: row["mail"]["tags"].update({"ses:configuration-set": ["other"]}), "configuration_mismatch"),
        (lambda row: row["mail"]["tags"].update({"ses:from-domain": ["other.test"]}), "from_domain_mismatch"),
        (lambda row: row["delivery"].update(recipients=["other@example.test"]), "recipient_mismatch"),
        (lambda row: row["mail"]["commonHeaders"].update(to=["a@example.test", "b@example.test"]), "common_recipient_invalid"),
    ],
)
def test_schema_and_identity_mismatches_fail_closed(mutator, error):
    row = event()
    mutator(row)
    with pytest.raises(index.EventError, match=error):
        index.parse_record(row, env=ENV)


def test_unknown_bounce_type_never_becomes_hard_suppression():
    row = event("Bounce")
    row["bounce"]["bounceType"] = "Transient"
    with pytest.raises(index.EventError, match="bounce_type_unsupported"):
        index.parse_record(row, env=ENV)


def test_payload_hash_is_stable_under_object_key_order():
    first = event()
    second = dict(reversed(list(first.items())))
    assert index.parse_record(first, env=ENV)["p_payload_sha256"] == index.parse_record(second, env=ENV)["p_payload_sha256"]


def test_process_event_calls_service_only_rpc_and_accepts_duplicate(capsys):
    calls, transport = transport_result("duplicate")
    result = index.process_event({"messages": [event()]}, env=ENV, transport=transport)
    logs = capsys.readouterr().out
    assert result == {"ok": True, "applied": 0, "duplicates": 1, "records": 1}
    assert calls[0][0].endswith("/rest/v1/rpc/email_record_postbox_event_v2")
    assert calls[0][2]["apikey"].startswith("sb_secret_")
    assert '"message":"postbox_event_ok"' in logs
    assert "user@example.test" not in logs.lower()
    assert "sb_secret" not in logs


def test_correlation_pending_retries_whole_invocation():
    _calls, transport = transport_result("correlation_pending")
    with pytest.raises(index.BatchError, match="batch_failed"):
        index.process_event({"messages": [event()]}, env=ENV, transport=transport)


def test_all_records_are_attempted_before_generic_batch_failure():
    calls = []

    def transport(_url, body, _headers, _timeout):
        calls.append(json.loads(body))
        return (503, b"") if len(calls) == 1 else (200, b'"applied"')

    with pytest.raises(index.BatchError, match="batch_failed"):
        index.process_event({"messages": [event(), {**event("Send"), "eventId": "event-2"}]}, env=ENV, transport=transport)
    assert len(calls) == 2


def test_kill_switch_prevents_transport():
    calls, transport = transport_result()
    env = {**ENV, "POSTBOX_EVENT_CONSUMER_ENABLED": "false"}
    with pytest.raises(index.BatchError, match="consumer_disabled"):
        index.process_event({"messages": [event()]}, env=env, transport=transport)
    assert calls == []


def test_external_error_body_and_recipient_are_not_logged(capsys):
    def transport(*_args):
        return 500, b"user@example.test secret diagnostic"

    with pytest.raises(index.BatchError):
        index.process_event({"messages": [event()]}, env=ENV, transport=transport)
    logs = capsys.readouterr().out
    assert '"error_code":"supabase_retryable"' in logs
    assert "user@example.test" not in logs
    assert "diagnostic" not in logs


def test_subscription_without_event_id_is_rejected():
    row = event("Subscription")
    row.pop("eventId")
    with pytest.raises(index.EventError, match="event_id_invalid"):
        index.parse_record(row, env=ENV)
