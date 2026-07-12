from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from email_control.models import ProviderResult
from email_control.providers.base import AmbiguousDelivery, ProviderRejected
from email_control.scheduler import EmailMonitorConfig, PostboxHealthMonitor
from email_control.supabase_rpc import EmailControlRpcClient, EmailControlRpcTemporaryError
from email_control.worker import EmailWorkerConfig, PostboxOutboxWorker
from email_control.yandex_iam import YandexIamError, YandexIamTokenProvider


def claim(*, dry_run: bool = False, attempt: int = 1, payload: dict | None = None) -> dict:
    return {
        "outbox_id": "00000000-0000-0000-0000-000000000101",
        "lease_token": "00000000-0000-0000-0000-000000000102",
        "stream": "transactional",
        "provider": "postbox",
        "kind": "account_auth",
        "recipient_email": "user@example.test",
        "payload_json": payload or {"subject": "Test", "text": "Body"},
        "template_version": "transactional-plain-v1",
        "dry_run": dry_run,
        "attempt_number": attempt,
    }


def config(**changes) -> EmailWorkerConfig:
    values = {
        "enabled": True,
        "worker_id": "postbox:test",
        "claim_limit": 5,
        "lease_seconds": 180,
        "max_attempts": 5,
        "retry_base_seconds": 300,
        "retry_max_seconds": 3600,
        "supabase_url": "https://example.supabase.co",
        "supabase_secret_key": "sb_secret_fixture",
        "postbox_enabled": True,
        "postbox_endpoint": "https://postbox.cloud.yandex.net/v2/email/outbound-emails",
        "postbox_from": "notify@kenigevents.ru",
        "postbox_from_name": "Kenig Events",
        "postbox_reply_to": "info@kenigevents.ru",
        "postbox_configuration_set": "kenigevents-transactional",
        "postbox_sa_key_json": "unused-by-fake",
    }
    values.update(changes)
    return EmailWorkerConfig(**values)


class FakeRpc:
    def __init__(self, claims: list[dict]):
        self.claims = claims
        self.calls: list[tuple[str, dict]] = []
        self.fail_mark = False
        self.fail_accepted_finish = False

    def call(self, name, payload=None):
        row = dict(payload or {})
        self.calls.append((name, row))
        if name == "email_recover_expired_postbox_claims_v2":
            return [{"retryable_count": 1, "unknown_count": 2}]
        if name == "email_claim_postbox_outbox_v2":
            return self.claims
        if name == "email_mark_network_started_v1" and self.fail_mark:
            raise EmailControlRpcTemporaryError("supabase_retryable")
        if name == "email_mark_network_started_v1":
            return "00000000-0000-0000-0000-000000000103"
        if name == "email_finish_attempt_v1":
            if row["p_outcome"] == "accepted" and self.fail_accepted_finish:
                raise EmailControlRpcTemporaryError("supabase_retryable")
            return row["p_outcome"]
        if name == "email_fail_postbox_claim_before_network_v1":
            return "retryable" if row["p_retryable"] else "terminal_failed"
        raise AssertionError(name)


class FakeToken:
    def __init__(self):
        self.calls = 0

    def get_token(self):
        self.calls += 1
        return "iam-token"


class FakeAdapter:
    outcome: object = ProviderResult("postbox", "provider-message-1", True, False, 200)
    prepared: list[bytes] = []

    def __init__(self, adapter_config):
        self.config = adapter_config

    def prepare(self, message):
        assert message.to_email == "user@example.test"
        body = b'{"prepared":true}'
        self.prepared.append(body)
        return body

    def send_prepared(self, body):
        assert body == b'{"prepared":true}'
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def finish_outcomes(rpc: FakeRpc) -> list[str]:
    return [body["p_outcome"] for name, body in rpc.calls if name == "email_finish_attempt_v1"]


def test_worker_accepts_and_persists_real_provider_message_id():
    rpc = FakeRpc([claim()])
    token = FakeToken()
    FakeAdapter.outcome = ProviderResult("postbox", "provider-message-1", True, False, 200)
    stats = PostboxOutboxWorker(
        config(), rpc=rpc, token_provider=token, adapter_factory=FakeAdapter
    ).run_once()
    assert stats.public() == {
        "claimed": 1,
        "accepted": 1,
        "dry_run": 0,
        "retryable": 0,
        "unknown": 0,
        "failed": 0,
        "recovered_retryable": 1,
        "recovered_unknown": 2,
        "errors": 0,
    }
    assert token.calls == 1
    assert finish_outcomes(rpc) == ["accepted"]
    finish = next(body for name, body in rpc.calls if name == "email_finish_attempt_v1")
    assert finish["p_provider_message_id"] == "provider-message-1"
    assert any(name == "email_mark_network_started_v1" for name, _body in rpc.calls)


def test_dry_run_never_mints_token_or_marks_network():
    rpc = FakeRpc([claim(dry_run=True)])
    token = FakeToken()
    stats = PostboxOutboxWorker(
        config(postbox_enabled=False), rpc=rpc, token_provider=token, adapter_factory=FakeAdapter
    ).run_once()
    assert stats.dry_run == 1 and token.calls == 0
    assert finish_outcomes(rpc) == ["dry_run"]
    assert not any(name == "email_mark_network_started_v1" for name, _body in rpc.calls)


def test_invalid_template_fails_before_network_without_send_attempt():
    row = claim()
    row["template_version"] = "caller-html-v999"
    rpc = FakeRpc([row])
    stats = PostboxOutboxWorker(config(), rpc=rpc, token_provider=FakeToken()).run_once()
    assert stats.failed == 1
    preflight = next(body for name, body in rpc.calls if name == "email_fail_postbox_claim_before_network_v1")
    assert preflight["p_error_class"] == "template_version_invalid"
    assert preflight["p_retryable"] is False
    assert not any(name == "email_mark_network_started_v1" for name, _body in rpc.calls)


@pytest.mark.parametrize(
    ("outcome", "attempt", "expected"),
    [
        (AmbiguousDelivery("hidden"), 1, "unknown"),
        (ProviderRejected("hidden", status=429, retryable=True), 1, "retryable"),
        (ProviderRejected("hidden", status=429, retryable=True), 5, "failed"),
    ],
)
def test_provider_failure_classification(outcome, attempt, expected):
    rpc = FakeRpc([claim(attempt=attempt)])
    FakeAdapter.outcome = outcome
    stats = PostboxOutboxWorker(
        config(), rpc=rpc, token_provider=FakeToken(), adapter_factory=FakeAdapter
    ).run_once()
    assert finish_outcomes(rpc) == [expected]
    assert getattr(stats, expected) == 1


def test_ambiguous_mark_network_rpc_never_calls_provider_or_releases_claim():
    rpc = FakeRpc([claim()])
    rpc.fail_mark = True
    FakeAdapter.outcome = ProviderResult("postbox", "must-not-send", True, False, 200)
    stats = PostboxOutboxWorker(
        config(), rpc=rpc, token_provider=FakeToken(), adapter_factory=FakeAdapter
    ).run_once()
    assert stats.errors == 1
    assert finish_outcomes(rpc) == []
    assert not any(name == "email_fail_postbox_claim_before_network_v1" for name, _body in rpc.calls)


def test_accepted_provider_response_with_ambiguous_db_finish_is_quarantined_without_resend():
    rpc = FakeRpc([claim()])
    rpc.fail_accepted_finish = True
    FakeAdapter.outcome = ProviderResult("postbox", "provider-message-1", True, False, 200)
    stats = PostboxOutboxWorker(
        config(), rpc=rpc, token_provider=FakeToken(), adapter_factory=FakeAdapter
    ).run_once()
    assert finish_outcomes(rpc) == ["accepted", "unknown"]
    assert stats.unknown == 1
    assert stats.accepted == 0


def _private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


def test_iam_provider_mints_short_lived_token_once_and_caches_it():
    calls = []
    now = 1_800_000_000
    key = json.dumps(
        {
            "id": "abcdefghij1234567890",
            "service_account_id": "serviceacct1234567890",
            "private_key": _private_key(),
        }
    )

    def transport(url, body, headers, timeout):
        calls.append((url, json.loads(body), headers, timeout))
        expires = datetime.fromtimestamp(now + 3600, timezone.utc).isoformat()
        return 200, json.dumps({"iamToken": "t1.fixture", "expiresAt": expires}).encode()

    provider = YandexIamTokenProvider(key, transport=transport, now=lambda: now)
    assert provider.get_token() == "t1.fixture"
    assert provider.get_token() == "t1.fixture"
    assert len(calls) == 1
    assert set(calls[0][1]) == {"jwt"}
    assert "private_key" not in json.dumps(calls)


def test_iam_provider_accepts_only_matching_yandex_cli_key_preamble():
    private_key = _private_key()
    key_id = "abcdefghij1234567890"
    base = {
        "id": key_id,
        "service_account_id": "serviceacct1234567890",
        "private_key": (
            f"PLEASE DO NOT REMOVE THIS LINE! Yandex.Cloud SA Key ID <{key_id}>\n"
            + private_key
        ),
    }
    provider = YandexIamTokenProvider(
        json.dumps(base),
        transport=lambda *_args: (
            200,
            json.dumps(
                {
                    "iamToken": "t1.fixture",
                    "expiresAt": datetime.fromtimestamp(
                        1_800_003_600, timezone.utc
                    ).isoformat(),
                }
            ).encode(),
        ),
        now=lambda: 1_800_000_000,
    )
    assert provider.get_token() == "t1.fixture"

    base["private_key"] = base["private_key"].replace(key_id, "wrongkeyid1234567890", 1)
    with pytest.raises(YandexIamError, match="authorized_key_invalid"):
        YandexIamTokenProvider(json.dumps(base))


def test_supabase_rpc_uses_apikey_only_and_classifies_retryable_status():
    calls = []

    def ok(url, body, headers, timeout):
        calls.append((url, json.loads(body), headers, timeout))
        return 200, b'{"ready_count":0}'

    client = EmailControlRpcClient("https://example.supabase.co", "sb_secret_fixture", transport=ok)
    assert client.call("email_postbox_health_v1") == {"ready_count": 0}
    assert calls[0][2]["apikey"] == "sb_secret_fixture"
    assert "Authorization" not in calls[0][2]

    client = EmailControlRpcClient(
        "https://example.supabase.co",
        "sb_secret_fixture",
        transport=lambda *_args: (503, b"secret diagnostic"),
    )
    with pytest.raises(EmailControlRpcTemporaryError, match="supabase_retryable"):
        client.call("email_postbox_health_v1")


def monitor_config() -> EmailMonitorConfig:
    return EmailMonitorConfig(
        enabled=True,
        interval_seconds=300,
        alert_cooldown_seconds=900,
        submitted_warning_seconds=900,
        submitted_alarm_seconds=3600,
        retryable_due_warning=5,
        supabase_url="https://example.supabase.co",
        supabase_secret_key="sb_secret_fixture",
        dlq_queue_url="https://queue.example.test/dlq",
        dlq_access_key_id="key-id",
        dlq_secret_access_key="secret",
        dlq_endpoint="https://message-queue.api.cloud.yandex.net",
    )


def test_monitor_combines_pii_free_health_and_dlq_alarm():
    class Rpc:
        def call(self, name, payload=None):
            assert name == "email_postbox_health_v1"
            return {
                "unknown_delivery_count": 1,
                "expired_claim_count": 0,
                "oldest_submitted_seconds": 4000,
                "terminal_failed_24h_count": 0,
                "retryable_due_count": 0,
            }

    class Sqs:
        def get_queue_attributes(self, **kwargs):
            assert kwargs["AttributeNames"] == ["All"]
            return {
                "Attributes": {
                    "ApproximateNumberOfMessages": "1",
                    "ApproximateNumberOfMessagesNotVisible": "0",
                }
            }

    monitor = PostboxHealthMonitor(monitor_config(), rpc=Rpc(), sqs_client=Sqs())
    health = monitor.inspect()
    assert health["dlq_visible_count"] == 1
    assert monitor.alarms(health) == [
        ("alarm", "postbox_dlq_nonempty"),
        ("alarm", "postbox_unknown_delivery"),
        ("alarm", "postbox_delivery_event_lag"),
    ]
