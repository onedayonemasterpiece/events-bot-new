from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "serverless" / "focus_auth_email_hook"))
import index as hook  # noqa: E402

SECRET_KEY = b"test-standard-webhook-secret"
SECRET = "v1,whsec_" + base64.b64encode(SECRET_KEY).decode()
ATTEMPT = uuid.UUID("2e44e6f2-baa2-4ee1-9a21-26bd297f0740")
USER = uuid.UUID("e57ea060-989b-4c40-a594-9e42b42f3b4c")


def env(**overrides):
    values = {
        "SEND_EMAIL_HOOK_SECRET": SECRET,
        "PERSONALIZATION_SUPABASE_URL": "https://example.supabase.co",
        "PERSONALIZATION_SUPABASE_SECRET_KEY": "secret-test-key",
        "POSTBOX_CONFIGURATION_SET": "auth",
        "NOTISEND_API_TOKEN": "notisend-test-token",
        "AUTH_NOTISEND_FROM_EMAIL": "events@news.kenigevents.ru",
    }
    values.update(overrides)
    return values


def payload(*, created_at=None, attempt=ATTEMPT):
    return {
        "user": {
            "id": str(USER),
            "email": "focus-e2e@kenigevents.ru",
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        },
        "email_data": {
            "token": "123456",
            "token_hash": "abc123hash",
            "redirect_to": f"https://kenigevents.ru/fokus-gruppa/priglashenie/?focus_auth_attempt={attempt}",
            "email_action_type": "signup",
            "site_url": "https://kenigevents.ru",
        },
    }


def signed(value, *, webhook_id="msg_test", now=None):
    raw = json.dumps(value, separators=(",", ":")).encode()
    timestamp = str(int(time.time() if now is None else now))
    digest = base64.b64encode(
        hmac.new(SECRET_KEY, webhook_id.encode() + b"." + timestamp.encode() + b"." + raw, hashlib.sha256).digest()
    ).decode()
    return raw, {
        "webhook-id": webhook_id,
        "webhook-timestamp": timestamp,
        "webhook-signature": f"v1,{digest}",
    }


class FakeTransport:
    def __init__(
        self,
        *,
        ordinal=1,
        existing=None,
        provider_status=200,
        provider_body=None,
        complete=True,
        notisend_admitted=True,
    ):
        self.ordinal = ordinal
        self.existing = existing
        self.provider_status = provider_status
        self.provider_body = provider_body
        self.complete = complete
        self.notisend_admitted = notisend_admitted
        self.calls = []

    def __call__(self, method, url, headers, body, timeout):
        self.calls.append((method, url, headers, body, timeout))
        if url.endswith("/rpc/focus_auth_begin_delivery_v1"):
            request = json.loads(body)
            wants_notisend = (
                request["p_prefer_notisend"] is True
                or self.ordinal > 1
                or request["p_action_type"] != "signup"
            )
            if self.existing:
                row = {
                    "send_ordinal": self.ordinal,
                    "is_new": False,
                    "notisend_admitted": self.existing.get("previous_provider") == "notisend",
                    **self.existing,
                }
            else:
                row = {
                    "send_ordinal": self.ordinal,
                    "is_new": True,
                    "previous_provider": None,
                    "previous_outcome": "started",
                    "previous_message_id": None,
                    "notisend_admitted": wants_notisend and self.notisend_admitted,
                }
            return 200, json.dumps([row]).encode()
        if url.endswith("/rpc/focus_auth_complete_delivery_v1"):
            return 200, json.dumps(self.complete).encode()
        if "postbox.cloud.yandex.net" in url:
            body_value = self.provider_body if self.provider_body is not None else {"MessageId": "postbox-receipt"}
            return self.provider_status, json.dumps(body_value).encode()
        if url.endswith("/email/messages"):
            body_value = self.provider_body if self.provider_body is not None else {"id": "notisend-receipt"}
            return self.provider_status, json.dumps(body_value).encode()
        raise AssertionError(url)


def test_standard_webhook_verification_and_expiry():
    raw, headers = signed(payload(), now=1_700_000_000)
    assert hook.verify_standard_webhook(raw, headers, SECRET, now=1_700_000_000) == "msg_test"
    with pytest.raises(hook.HookError, match="webhook_timestamp_expired"):
        hook.verify_standard_webhook(raw, headers, SECRET, now=1_700_000_301)


def test_new_first_send_uses_postbox_and_persists_receipt():
    raw, headers = signed(payload())
    transport = FakeTransport()
    result = hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=transport)
    assert result == {"attempt_id": str(ATTEMPT), "provider": "postbox", "duplicate": False}
    assert any("postbox.cloud.yandex.net" in call[1] for call in transport.calls)
    completion = json.loads([call[3] for call in transport.calls if call[1].endswith("focus_auth_complete_delivery_v1")][0])
    assert completion["p_provider"] == "postbox"
    assert completion["p_outcome"] == "accepted"
    assert completion["p_provider_message_id"] == "postbox-receipt"


def test_returning_user_uses_notisend_subscriber_payment():
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    raw, headers = signed(payload(created_at=old))
    transport = FakeTransport()
    result = hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=transport)
    assert result["provider"] == "notisend"
    send = next(call for call in transport.calls if call[1].endswith("/email/messages"))
    request = json.loads(send[3])
    assert request["payment"] == "subscriber"
    assert request["to"] == "focus-e2e@kenigevents.ru"
    assert request["subject"].startswith("Код 123456")


def test_recent_existing_magic_link_uses_notisend_without_age_heuristic():
    value = payload()
    value["email_data"]["email_action_type"] = "magiclink"
    raw, headers = signed(value)
    result = hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=FakeTransport())
    assert result["provider"] == "notisend"


def test_second_send_uses_notisend_even_for_recent_user():
    raw, headers = signed(payload())
    result = hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=FakeTransport(ordinal=2))
    assert result["provider"] == "notisend"


def test_fixed_user_allowlist_uses_notisend():
    raw, headers = signed(payload())
    result = hook.process(
        raw,
        headers,
        context=SimpleNamespace(token="iam-token"),
        env=env(FOCUS_AUTH_NOTISEND_USER_IDS=str(USER)),
        transport=FakeTransport(),
    )
    assert result["provider"] == "notisend"


def test_fixed_test_mailbox_uses_notisend_without_creating_new_identity():
    raw, headers = signed(payload())
    result = hook.process(
        raw,
        headers,
        context=SimpleNamespace(token="iam-token"),
        env=env(FOCUS_AUTH_NOTISEND_EMAILS="focus-e2e@kenigevents.ru"),
        transport=FakeTransport(),
    )
    assert result["provider"] == "notisend"


def test_notisend_capacity_full_routes_before_dispatch_to_postbox():
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    raw, headers = signed(payload(created_at=old))
    transport = FakeTransport(notisend_admitted=False)
    result = hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=transport)
    assert result["provider"] == "postbox"
    begin = next(call for call in transport.calls if call[1].endswith("focus_auth_begin_delivery_v1"))
    assert json.loads(begin[3])["p_prefer_notisend"] is True
    assert any("postbox.cloud.yandex.net" in call[1] for call in transport.calls)
    assert not any(call[1].endswith("/email/messages") for call in transport.calls)


def test_accepted_duplicate_returns_success_without_provider_send():
    raw, headers = signed(payload())
    transport = FakeTransport(existing={
        "previous_provider": "notisend",
        "previous_outcome": "accepted",
        "previous_message_id": "receipt-existing",
    })
    result = hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=transport)
    assert result == {"attempt_id": str(ATTEMPT), "provider": "notisend", "duplicate": True}
    assert len(transport.calls) == 1


def test_ambiguous_duplicate_never_sends_via_other_provider():
    raw, headers = signed(payload())
    transport = FakeTransport(existing={
        "previous_provider": "postbox",
        "previous_outcome": "ambiguous",
        "previous_message_id": None,
    })
    with pytest.raises(hook.HookError, match="delivery_attempt_already_finalized"):
        hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=transport)
    assert len(transport.calls) == 1


def test_provider_reject_is_recorded_and_no_cross_provider_retry():
    raw, headers = signed(payload())
    transport = FakeTransport(provider_status=429, provider_body={"error": "rate"})
    with pytest.raises(hook.HookError, match="provider_rejected"):
        hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=transport)
    provider_calls = [call for call in transport.calls if "postbox.cloud.yandex.net" in call[1] or call[1].endswith("/email/messages")]
    assert len(provider_calls) == 1
    completion = json.loads([call[3] for call in transport.calls if call[1].endswith("focus_auth_complete_delivery_v1")][0])
    assert completion["p_outcome"] == "definitive_reject"


def test_notisend_2xx_without_receipt_is_ambiguous_and_never_retried():
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    raw, headers = signed(payload(created_at=old))
    transport = FakeTransport(provider_status=200, provider_body={"accepted": True})
    with pytest.raises(hook.HookError, match="provider_receipt_invalid"):
        hook.process(raw, headers, context=SimpleNamespace(token="iam-token"), env=env(), transport=transport)
    provider_calls = [
        call for call in transport.calls
        if "postbox.cloud.yandex.net" in call[1] or call[1].endswith("/email/messages")
    ]
    assert len(provider_calls) == 1
    completion = json.loads([
        call[3] for call in transport.calls if call[1].endswith("focus_auth_complete_delivery_v1")
    ][0])
    assert completion["p_provider"] == "notisend"
    assert completion["p_outcome"] == "ambiguous"


def test_rendered_email_has_one_link_and_six_digit_code():
    subject, text, html_body = hook._render_message("654321", "https://example.test/verify")
    assert subject.startswith("Код 654321")
    assert "654321" in text and "654321" in html_body
    assert html_body.count("href=") == 1
    assert "Войти по ссылке" in html_body


def test_handler_rejects_invalid_signature_without_leaking_payload(capsys):
    event = {"body": "{}", "headers": {"webhook-id": "x", "webhook-timestamp": "1", "webhook-signature": "v1,bad"}}
    response = hook.handler(event, SimpleNamespace(token="iam"))
    assert response["statusCode"] in {401, 500}
    output = capsys.readouterr().out
    assert "focus-e2e@" not in output
    assert "123456" not in output
