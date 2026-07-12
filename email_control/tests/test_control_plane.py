from __future__ import annotations

import base64
import json
from concurrent.futures import ThreadPoolExecutor
from email import message_from_bytes
from pathlib import Path

import pytest

from email_control import EmailMessage, RecommendationAdmissionGate, RecommendationIssue, SendEligibility, Stream
from email_control.providers import (
    NotiSendAdapter,
    NotiSendConfig,
    NotiSendWebhookParser,
    PostboxAdapter,
    PostboxConfig,
    ProviderRejected,
    ProviderRouter,
    verified_status_signal,
)
from email_control.providers.base import HttpResponse


class RecordingTransport:
    def __init__(self, responses: list[HttpResponse]):
        self.responses = list(responses)
        self.requests: list[tuple[str, str, dict[str, str], bytes | None]] = []

    def request(self, method: str, url: str, *, headers, body):
        self.requests.append((method, url, dict(headers), body))
        if not self.responses:
            raise AssertionError("unexpected provider call")
        return self.responses.pop(0)


def message(stream: Stream) -> EmailMessage:
    return EmailMessage(
        outbox_id="00000000-0000-0000-0000-000000000001",
        idempotency_key=f"email-contract:{stream.value}:1",
        stream=stream,
        to_email="seed@example.test",
        subject="Тест KenigEvents",
        text="Три события уже ждут.",
        html="<p>Три события уже ждут.</p>",
        reply_to="info@kenigevents.ru",
    )


def test_recommendation_capacity_is_atomic_and_never_admits_201st_user() -> None:
    gate = RecommendationAdmissionGate()
    user_ids = [f"user-{index:03d}" for index in range(201)]
    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(gate.activate, user_ids))
    assert sum(results) == 200
    assert len(gate.active_users()) == 200
    assert results.count(False) == 1


def test_recommendation_capacity_activation_is_idempotent_and_slot_can_be_reused() -> None:
    gate = RecommendationAdmissionGate()
    assert gate.activate("same-user") is True
    assert gate.activate("same-user") is True
    for index in range(199):
        assert gate.activate(f"user-{index}") is True
    assert gate.activate("overflow") is False
    gate.revoke("same-user")
    assert gate.activate("overflow") is True
    assert len(gate.active_users()) == 200


@pytest.mark.parametrize("event_ids", [(1, 2), (1, 2, 3, 4), (1, 1, 2)])
def test_exact_three_distinct_events_gate(event_ids: tuple[int, ...]) -> None:
    issue = RecommendationIssue(event_ids, None, page_published=True, page_validated=True)
    with pytest.raises(ValueError, match="exactly three distinct"):
        issue.validate_sendable()


def test_hero_is_one_of_exact_three_and_page_must_be_published_and_validated() -> None:
    with pytest.raises(ValueError, match="hero"):
        RecommendationIssue((1, 2, 3), 4, True, True).validate_sendable()
    with pytest.raises(ValueError, match="published and validated"):
        RecommendationIssue((1, 2, 3), 1, True, False).validate_sendable()
    RecommendationIssue((1, 2, 3), 1, True, True).validate_sendable()


def test_send_eligibility_fails_closed_above_cap_or_when_suppressed() -> None:
    issue = RecommendationIssue((1, 2, 3), 1, True, True)
    valid = SendEligibility(True, True, True, False, 200)
    assert valid.allows_recommendation(issue) is True
    assert SendEligibility(True, True, True, False, 201).allows_recommendation(issue) is False
    assert SendEligibility(True, True, True, True, 100).allows_recommendation(issue) is False


def test_postbox_api_uses_raw_mime_reply_to_and_real_message_id() -> None:
    transport = RecordingTransport([HttpResponse(200, b'{"MessageId":"pb-real-123"}', {})])
    adapter = PostboxAdapter(
        PostboxConfig(enabled=True, dry_run=False, iam_token="test-token", configuration_set="transactional"),
        transport,
    )
    result = adapter.send(message(Stream.TRANSACTIONAL))
    assert result.provider_message_id == "pb-real-123"
    assert result.accepted is True and result.dry_run is False
    request_body = json.loads(transport.requests[0][3])
    raw = base64.b64decode(request_body["Content"]["Raw"]["Data"])
    mime = message_from_bytes(raw)
    assert mime["Reply-To"] == "info@kenigevents.ru"
    assert mime["X-KenigEvents-Outbox-ID"] == "00000000-0000-0000-0000-000000000001"
    assert request_body["ConfigurationSetName"] == "transactional"


def test_postbox_dry_run_never_calls_transport_and_has_no_fake_provider_id() -> None:
    transport = RecordingTransport([])
    result = PostboxAdapter(PostboxConfig(enabled=False, dry_run=True), transport).send(message(Stream.TRANSACTIONAL))
    assert result.dry_run is True
    assert result.provider_message_id is None
    assert transport.requests == []


def test_notisend_individual_message_uses_subscriber_payment_and_real_id() -> None:
    transport = RecordingTransport([HttpResponse(201, b'{"id":417,"status":"queued"}', {})])
    adapter = NotiSendAdapter(NotiSendConfig(enabled=True, dry_run=False, api_token="test-token"), transport)
    result = adapter.send(message(Stream.RECOMMENDATION))
    assert result.provider_message_id == "417"
    payload = json.loads(transport.requests[0][3])
    assert transport.requests[0][1].endswith("/email/messages")
    assert payload["payment"] == "subscriber"
    assert payload["smtp_headers"]["Reply-To"] == "info@kenigevents.ru"
    assert payload["smtp_headers"]["X-KenigEvents-Outbox-ID"]


def test_notisend_acceptance_without_real_id_is_rejected() -> None:
    transport = RecordingTransport([HttpResponse(200, b'{"status":"queued"}', {})])
    adapter = NotiSendAdapter(NotiSendConfig(enabled=True, dry_run=False, api_token="test-token"), transport)
    with pytest.raises(ProviderRejected, match="no message id"):
        adapter.send(message(Stream.RECOMMENDATION))


def test_router_has_no_cross_provider_fallback() -> None:
    postbox_transport = RecordingTransport([HttpResponse(429, b'{"Code":"TooManyRequestsException"}', {})])
    notisend_transport = RecordingTransport([])
    router = ProviderRouter(
        PostboxAdapter(
            PostboxConfig(enabled=True, dry_run=False, iam_token="token", configuration_set="transactional"),
            postbox_transport,
        ),
        NotiSendAdapter(NotiSendConfig(enabled=True, dry_run=False, api_token="token"), notisend_transport),
    )
    with pytest.raises(ProviderRejected) as exc:
        router.send(message(Stream.TRANSACTIONAL))
    assert exc.value.retryable is True
    assert len(postbox_transport.requests) == 1
    assert notisend_transport.requests == []


def test_notisend_failure_never_falls_back_to_postbox() -> None:
    notisend_transport = RecordingTransport([HttpResponse(402, b'{"errors":[{"code":402}]}', {})])
    postbox_transport = RecordingTransport([])
    router = ProviderRouter(
        PostboxAdapter(
            PostboxConfig(enabled=True, dry_run=False, iam_token="token", configuration_set="transactional"),
            postbox_transport,
        ),
        NotiSendAdapter(NotiSendConfig(enabled=True, dry_run=False, api_token="token"), notisend_transport),
    )
    with pytest.raises(ProviderRejected) as exc:
        router.send(message(Stream.RECOMMENDATION))
    assert exc.value.status == 402
    assert len(notisend_transport.requests) == 1
    assert postbox_transport.requests == []


def test_notisend_webhook_is_always_untrusted_until_authenticated_api_poll() -> None:
    raw = json.dumps(
        {
            "meta": {"type": "api"},
            "events": [
                {"id": 417, "name": "unsubscribed", "email": "seed@example.test", "timestamp": 1783800000}
            ],
        }
    ).encode()
    [signal] = NotiSendWebhookParser().parse(raw)
    assert signal.event_type == "unsubscribe"
    assert signal.authenticated is False
    assert signal.verified is False

    verified = verified_status_signal(
        "417",
        {"id": 417, "to": "seed@example.test", "status": "delivered", "events": {"unsubscribe": 1}},
    )
    assert verified.provider_message_id == "417"
    assert verified.event_type == "unsubscribe"
    assert verified.authenticated is True and verified.verified is True


def test_migration_seeds_disabled_switches_and_enforces_provider_routing() -> None:
    migration = next(Path("supabase/migrations").glob("*_email_control_plane_v1.sql")).read_text()
    assert "('global', false, true)" in migration
    assert "('transactional', false, true)" in migration
    assert "('recommendation', false, true)" in migration
    assert "stream = 'transactional' and provider = 'postbox'" in migration
    assert "stream = 'recommendation' and provider = 'notisend'" in migration
    assert "recommendation_capacity_full" in migration
    assert "not p_authenticated or not p_verified" in migration
    assert "revoke all on all tables in schema email_control" in migration


def test_environment_config_defaults_are_production_disabled(monkeypatch) -> None:
    from email_control.config import notisend_config_from_env, postbox_config_from_env

    for name in (
        "POSTBOX_EMAIL_ENABLED",
        "POSTBOX_EMAIL_DRY_RUN",
        "NOTISEND_EMAIL_ENABLED",
        "NOTISEND_EMAIL_DRY_RUN",
        "POSTBOX_EMAIL_IAM_TOKEN",
        "NOTISEND_EMAIL_API_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    postbox = postbox_config_from_env()
    notisend = notisend_config_from_env()
    assert postbox.enabled is False and postbox.dry_run is True
    assert notisend.enabled is False and notisend.dry_run is True
    assert postbox.iam_token == ""
    assert notisend.api_token == ""
