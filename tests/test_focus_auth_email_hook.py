from __future__ import annotations

import base64
import email
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
        "EMAIL_ADDRESS_HMAC_KEY": "fixture-auth-email-hmac-key",
        "EMAIL_ADDRESS_HMAC_KEY_VERSION": "1",
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
        suppressed_hmac=None,
    ):
        self.ordinal = ordinal
        self.existing = existing
        self.provider_status = provider_status
        self.provider_body = provider_body
        self.complete = complete
        self.notisend_admitted = notisend_admitted
        self.suppressed_hmac = suppressed_hmac
        self.calls = []
        self.provider_calls = 0

    def __call__(self, method, url, headers, body, timeout):
        self.calls.append((method, url, headers, body, timeout))
        if url.endswith("/rpc/focus_auth_begin_delivery_batch_v1"):
            request = json.loads(body)
            deliveries = request["p_deliveries"]
            if any(item["email_hmac"] == self.suppressed_hmac for item in deliveries):
                return 200, json.dumps({
                    "admitted": False,
                    "admission_status": "recipient_suppressed",
                    "results": [],
                }).encode()
            rows = []
            for index, delivery in enumerate(deliveries):
                wants_notisend = (
                    request["p_action_type"] != "email_change"
                    and (
                        delivery["prefer_notisend"] is True
                        or self.ordinal + index > 1
                        or request["p_action_type"] != "signup"
                    )
                )
                if self.existing:
                    row = {
                        "attempt_id": delivery["attempt_id"],
                        "send_ordinal": self.ordinal + index,
                        "is_new": False,
                        "notisend_admitted": self.existing.get("previous_provider") == "notisend",
                        **self.existing,
                    }
                else:
                    row = {
                        "attempt_id": delivery["attempt_id"],
                        "send_ordinal": self.ordinal + index,
                        "is_new": True,
                        "previous_provider": None,
                        "previous_outcome": "started",
                        "previous_message_id": None,
                        "notisend_admitted": wants_notisend and self.notisend_admitted,
                    }
                rows.append(row)
            return 200, json.dumps({
                "admitted": True,
                "admission_status": "admitted",
                "results": rows,
            }).encode()
        if url.endswith("/rpc/focus_auth_complete_delivery_batch_v1"):
            return 200, json.dumps(self.complete).encode()
        if "postbox.cloud.yandex.net" in url:
            self.provider_calls += 1
            body_value = self.provider_body if self.provider_body is not None else {
                "MessageId": "postbox-receipt" if self.provider_calls == 1 else f"postbox-receipt-{self.provider_calls}"
            }
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
    completion = json.loads([call[3] for call in transport.calls if call[1].endswith("focus_auth_complete_delivery_batch_v1")][0])
    assert completion["p_results"][0]["provider"] == "postbox"
    assert completion["p_results"][0]["outcome"] == "accepted"
    assert completion["p_results"][0]["provider_message_id"] == "postbox-receipt"
    admission = json.loads(
        next(call[3] for call in transport.calls if call[1].endswith("focus_auth_begin_delivery_batch_v1"))
    )
    assert admission["p_deliveries"][0]["email_hmac"] == hook._recipient_hmac(
        "focus-e2e@kenigevents.ru", "fixture-auth-email-hmac-key"
    )
    assert admission["p_deliveries"][0]["hmac_key_version"] == 1
    assert "focus-e2e@kenigevents.ru" not in json.dumps(admission)


def test_postbox_uses_access_token_from_yandex_function_context():
    raw, headers = signed(payload())
    transport = FakeTransport()

    hook.process(
        raw,
        headers,
        context=SimpleNamespace(token={
            "access_token": "iam-token-from-context",
            "expires_in": 43_200,
            "token_type": "Bearer",
        }),
        env=env(),
        transport=transport,
    )

    postbox = next(call for call in transport.calls if "postbox.cloud.yandex.net" in call[1])
    assert postbox[2]["X-YaCloud-SubjectToken"] == "iam-token-from-context"


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
    begin = next(call for call in transport.calls if call[1].endswith("focus_auth_begin_delivery_batch_v1"))
    assert json.loads(begin[3])["p_deliveries"][0]["prefer_notisend"] is True
    assert any("postbox.cloud.yandex.net" in call[1] for call in transport.calls)
    assert not any(call[1].endswith("/email/messages") for call in transport.calls)


def test_exact_suppressed_email_is_rejected_before_any_provider_network_call():
    raw, headers = signed(payload())
    suppressed = hook._recipient_hmac(
        "focus-e2e@kenigevents.ru", "fixture-auth-email-hmac-key"
    )
    transport = FakeTransport(suppressed_hmac=suppressed)

    with pytest.raises(hook.HookError, match="recipient_suppressed"):
        hook.process(
            raw,
            headers,
            context=SimpleNamespace(token="iam-token"),
            env=env(),
            transport=transport,
        )

    assert len(transport.calls) == 1
    assert transport.calls[0][1].endswith("/rpc/focus_auth_begin_delivery_batch_v1")
    assert not any("postbox.cloud.yandex.net" in call[1] for call in transport.calls)
    assert not any(call[1].endswith("/email/messages") for call in transport.calls)


def test_legitimate_email_change_is_not_blocked_by_same_user_history():
    old_hmac = hook._recipient_hmac(
        "old-address@kenigevents.ru", "fixture-auth-email-hmac-key"
    )
    changed = payload(attempt=uuid.uuid4())
    changed["user"]["email"] = "current-address@kenigevents.ru"
    changed["user"]["new_email"] = "new-address@kenigevents.ru"
    changed["email_data"].update({
        "email_action_type": "email_change",
        "token_new": "654321",
        "token_hash_new": "hash-for-current-address",
    })
    raw, headers = signed(changed, webhook_id="msg_email_change")
    transport = FakeTransport(suppressed_hmac=old_hmac)

    result = hook.process(
        raw,
        headers,
        context=SimpleNamespace(token="iam-token"),
        env=env(),
        transport=transport,
    )

    assert result["providers"] == ["postbox", "postbox"]
    assert result["deliveries"] == 2
    admission = json.loads(transport.calls[0][3])
    assert len(admission["p_deliveries"]) == 2
    assert all(item["email_hmac"] != old_hmac for item in admission["p_deliveries"])
    assert transport.provider_calls == 2
    assert not any("old-address@" in str(call[3]) for call in transport.calls)

    postbox_requests = [
        json.loads(call[3]) for call in transport.calls
        if "postbox.cloud.yandex.net" in call[1]
    ]
    messages = {}
    for item in postbox_requests:
        parsed = email.message_from_bytes(
            base64.b64decode(item["Content"]["Raw"]["Data"])
        )
        messages[item["Destination"]["ToAddresses"][0]] = "\n".join(
            part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8")
            for part in parsed.walk()
            if part.get_content_maintype() == "text"
        )
    current = messages["current-address@kenigevents.ru"]
    new = messages["new-address@kenigevents.ru"]
    assert "token=hash-for-current-address" in current
    assert "654321" not in current
    assert "token=abc123hash" in new


def test_secure_email_change_is_atomic_when_either_exact_recipient_is_suppressed():
    changed = payload(attempt=uuid.uuid4())
    changed["user"].update({
        "email": "current-address@kenigevents.ru",
        "new_email": "new-address@kenigevents.ru",
    })
    changed["email_data"].update({
        "email_action_type": "email_change",
        "token_new": "654321",
        "token_hash_new": "hash-for-current-address",
    })
    raw, headers = signed(changed, webhook_id="msg_email_change_suppressed")
    suppressed = hook._recipient_hmac(
        "new-address@kenigevents.ru", "fixture-auth-email-hmac-key"
    )
    transport = FakeTransport(suppressed_hmac=suppressed)

    with pytest.raises(hook.HookError, match="recipient_suppressed"):
        hook.process(
            raw,
            headers,
            context=SimpleNamespace(token="iam-token"),
            env=env(),
            transport=transport,
        )
    assert len(transport.calls) == 1
    assert transport.provider_calls == 0


def test_insecure_email_change_sends_only_to_new_address():
    changed = payload(attempt=uuid.uuid4())
    changed["user"].update({
        "email": "current-address@kenigevents.ru",
        "new_email": "new-address@kenigevents.ru",
    })
    changed["email_data"]["email_action_type"] = "email_change"
    raw, headers = signed(changed, webhook_id="msg_insecure_email_change")
    transport = FakeTransport()

    result = hook.process(
        raw,
        headers,
        context=SimpleNamespace(token="iam-token"),
        env=env(),
        transport=transport,
    )
    assert result["provider"] == "postbox"
    assert transport.provider_calls == 1
    postbox = next(call for call in transport.calls if "postbox.cloud.yandex.net" in call[1])
    request = json.loads(postbox[3])
    assert request["Destination"]["ToAddresses"] == ["new-address@kenigevents.ru"]


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
    completion = json.loads([call[3] for call in transport.calls if call[1].endswith("focus_auth_complete_delivery_batch_v1")][0])
    assert completion["p_results"][0]["outcome"] == "definitive_reject"


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
        call[3] for call in transport.calls if call[1].endswith("focus_auth_complete_delivery_batch_v1")
    ][0])
    assert completion["p_results"][0]["provider"] == "notisend"
    assert completion["p_results"][0]["outcome"] == "ambiguous"


@pytest.mark.parametrize("receipt", ["x" * 513, "bad\nreceipt"])
def test_postbox_2xx_with_invalid_receipt_is_ambiguous(receipt):
    raw, headers = signed(payload())
    transport = FakeTransport(provider_body={"MessageId": receipt})
    with pytest.raises(hook.HookError, match="provider_receipt_invalid") as error:
        hook.process(
            raw,
            headers,
            context=SimpleNamespace(token="iam-token"),
            env=env(),
            transport=transport,
        )
    assert error.value.provider_outcome == "ambiguous"
    completion = json.loads(next(
        call[3] for call in transport.calls
        if call[1].endswith("focus_auth_complete_delivery_batch_v1")
    ))
    assert completion["p_results"][0]["outcome"] == "ambiguous"


def test_modern_supabase_secret_key_is_not_sent_as_bearer():
    raw, headers = signed(payload())
    transport = FakeTransport()
    hook.process(
        raw,
        headers,
        context=SimpleNamespace(token="iam-token"),
        env=env(PERSONALIZATION_SUPABASE_SECRET_KEY="sb_secret_fixture"),
        transport=transport,
    )
    rpc_calls = [call for call in transport.calls if "/rest/v1/rpc/" in call[1]]
    assert rpc_calls
    assert all(call[2]["apikey"] == "sb_secret_fixture" for call in rpc_calls)
    assert all("Authorization" not in call[2] for call in rpc_calls)


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


def test_handler_acknowledges_suppression_without_membership_leak(monkeypatch, capsys):
    def suppressed(*_args, **_kwargs):
        raise hook.HookError("recipient_suppressed", status=403)

    monkeypatch.setattr(hook, "process", suppressed)
    event = {"body": "{}", "headers": {}}
    response = hook.handler(event, SimpleNamespace(token="iam"))
    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {}
    output = capsys.readouterr().out
    assert "recipient_suppressed" not in output
