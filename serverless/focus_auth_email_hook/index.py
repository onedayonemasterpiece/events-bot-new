from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import html
import json
import os
import re
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from email.message import EmailMessage
from email.policy import SMTP
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, quote, urlsplit

MAX_BODY_BYTES = 65_536
MAX_RESPONSE_BYTES = 32_768
WEBHOOK_TOLERANCE_SECONDS = 300
ATTEMPT_NAMESPACE = uuid.UUID("d14ed2c6-3e91-4ced-8bd0-7d3a81ba2a32")
ALLOWED_ACTIONS = {"signup", "magiclink", "email", "recovery", "invite", "reauthentication"}
TOKEN_RE = re.compile(r"^\d{6}$")

Transport = Callable[[str, str, Mapping[str, str], bytes | None, float], tuple[int, bytes]]


class HookError(RuntimeError):
    def __init__(self, code: str, *, status: int = 500, provider_outcome: str | None = None):
        super().__init__(code)
        self.code = code
        self.status = status
        self.provider_outcome = provider_outcome


def _log(level: str, event: str, **fields: Any) -> None:
    # Never pass email, OTP, token/hash, redirect URL, IP or User-Agent here.
    safe = {"level": level, "event": event, "component": "focus-auth-email-hook"}
    safe.update({key: value for key, value in fields.items() if value is not None})
    print(json.dumps(safe, ensure_ascii=True, sort_keys=True, separators=(",", ":")), flush=True)


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name) or "").strip()
    if not value:
        raise HookError(f"env_missing:{name.lower()}", status=500, provider_outcome="configuration_error")
    return value


def _header(headers: Mapping[str, Any], name: str) -> str:
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            return str(value or "").strip()
    return ""


def verify_standard_webhook(raw_body: bytes, headers: Mapping[str, Any], secret: str, *, now: int | None = None) -> str:
    webhook_id = _header(headers, "webhook-id")
    timestamp_raw = _header(headers, "webhook-timestamp")
    signatures = _header(headers, "webhook-signature").split()
    if not webhook_id or not timestamp_raw or not signatures:
        raise HookError("webhook_headers_missing", status=401)
    try:
        timestamp = int(timestamp_raw)
    except ValueError as exc:
        raise HookError("webhook_timestamp_invalid", status=401) from exc
    current = int(time.time() if now is None else now)
    if abs(current - timestamp) > WEBHOOK_TOLERANCE_SECONDS:
        raise HookError("webhook_timestamp_expired", status=401)

    encoded_secret = secret.strip()
    if encoded_secret.startswith("v1,whsec_"):
        encoded_secret = encoded_secret[len("v1,whsec_"):]
    elif encoded_secret.startswith("whsec_"):
        encoded_secret = encoded_secret[len("whsec_"):]
    try:
        key = base64.b64decode(encoded_secret, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HookError("hook_secret_invalid", status=500, provider_outcome="configuration_error") from exc
    signed = webhook_id.encode() + b"." + timestamp_raw.encode() + b"." + raw_body
    expected = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode("ascii")
    for item in signatures:
        version, separator, candidate = item.partition(",")
        if separator and version == "v1" and hmac.compare_digest(candidate, expected):
            return webhook_id
    raise HookError("webhook_signature_invalid", status=401)


def urllib_transport(method: str, url: str, headers: Mapping[str, str], body: bytes | None, timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read(MAX_RESPONSE_BYTES)
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read(MAX_RESPONSE_BYTES)
    except (TimeoutError, urllib.error.URLError, OSError) as exc:
        # A network error after write may still mean provider acceptance.
        raise HookError("network_outcome_unknown", status=503, provider_outcome="ambiguous") from exc


def _json_response(status: int, body: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
        "isBase64Encoded": False,
        "body": json.dumps(body, ensure_ascii=False, separators=(",", ":")),
    }


def _parse_event(event: Mapping[str, Any]) -> tuple[bytes, Mapping[str, Any]]:
    raw = event.get("body", "")
    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(str(raw), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise HookError("request_body_invalid", status=400) from exc
    else:
        body = str(raw).encode("utf-8")
    if not body or len(body) > MAX_BODY_BYTES:
        raise HookError("request_body_invalid", status=400)
    headers = event.get("headers")
    if not isinstance(headers, Mapping):
        raise HookError("request_headers_invalid", status=400)
    return body, headers


def _payload(raw_body: bytes) -> Mapping[str, Any]:
    try:
        value = json.loads(raw_body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HookError("request_json_invalid", status=400) from exc
    if not isinstance(value, Mapping):
        raise HookError("request_json_invalid", status=400)
    return value


def _attempt_id(redirect_to: str, webhook_id: str) -> uuid.UUID:
    try:
        candidate = parse_qs(urlsplit(redirect_to).query).get("focus_auth_attempt", [""])[0]
        if candidate:
            return uuid.UUID(candidate)
    except (ValueError, TypeError):
        pass
    return uuid.uuid5(ATTEMPT_NAMESPACE, webhook_id)


def _rpc(
    name: str,
    payload: Mapping[str, Any],
    *,
    env: Mapping[str, str],
    transport: Transport,
    timeout: float = 1.1,
) -> Any:
    base = _required(env, "PERSONALIZATION_SUPABASE_URL").rstrip("/")
    parsed = urlsplit(base)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path or parsed.query or parsed.fragment:
        raise HookError("supabase_url_invalid", status=500, provider_outcome="configuration_error")
    key = _required(env, "PERSONALIZATION_SUPABASE_SECRET_KEY")
    status, raw = transport(
        "POST",
        f"{base}/rest/v1/rpc/{name}",
        {"Content-Type": "application/json", "Accept": "application/json", "apikey": key, "Authorization": f"Bearer {key}"},
        json.dumps(dict(payload), separators=(",", ":")).encode(),
        timeout,
    )
    if status < 200 or status >= 300:
        raise HookError("delivery_ledger_rejected", status=503)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HookError("delivery_ledger_response_invalid", status=503) from exc


def _begin_delivery(
    attempt_id: uuid.UUID,
    user_id: uuid.UUID,
    action: str,
    prefer_notisend: bool,
    *,
    env: Mapping[str, str],
    transport: Transport,
) -> Mapping[str, Any]:
    result = _rpc(
        "focus_auth_begin_delivery_v1",
        {
            "p_attempt_id": str(attempt_id),
            "p_user_id": str(user_id),
            "p_action_type": action,
            "p_prefer_notisend": prefer_notisend,
        },
        env=env,
        transport=transport,
    )
    if not isinstance(result, list) or len(result) != 1 or not isinstance(result[0], Mapping):
        raise HookError("delivery_ledger_response_invalid", status=503)
    return result[0]


def _complete_delivery(
    attempt_id: uuid.UUID,
    provider: str,
    outcome: str,
    message_id: str | None,
    *,
    env: Mapping[str, str],
    transport: Transport,
) -> None:
    result = _rpc(
        "focus_auth_complete_delivery_v1",
        {"p_attempt_id": str(attempt_id), "p_provider": provider, "p_outcome": outcome, "p_provider_message_id": message_id},
        env=env,
        transport=transport,
    )
    if result is not True:
        raise HookError("delivery_ledger_completion_conflict", status=503)


def _is_returning_user(created_at: str, *, now: datetime | None = None) -> bool:
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except (ValueError, AttributeError) as exc:
        raise HookError("user_created_at_invalid", status=400) from exc
    if created.tzinfo is None:
        raise HookError("user_created_at_invalid", status=400)
    current = now or datetime.now(timezone.utc)
    return (current - created.astimezone(timezone.utc)).total_seconds() > 600


def choose_provider(
    *,
    action: str,
    send_ordinal: int,
    returning_user: bool,
    user_id: uuid.UUID,
    to_email: str,
    env: Mapping[str, str],
) -> str:
    configured = {item.strip() for item in str(env.get("FOCUS_AUTH_NOTISEND_USER_IDS") or "").split(",") if item.strip()}
    configured_emails = {item.strip().lower() for item in str(env.get("FOCUS_AUTH_NOTISEND_EMAILS") or "").split(",") if item.strip()}
    # Supabase emits `signup` only for a newly created email identity. Every
    # other supported action belongs to an existing identity, even when its
    # created_at is only seconds old. The age check protects imported/legacy
    # accounts whose first post-migration action is unexpectedly labelled.
    existing_identity = action != "signup" or returning_user
    if str(user_id) in configured or to_email in configured_emails or existing_identity or send_ordinal > 1:
        return "notisend"
    return "postbox"


def _confirmation_url(email_data: Mapping[str, Any], *, env: Mapping[str, str]) -> str:
    token_hash = str(email_data.get("token_hash") or "").strip()
    action = str(email_data.get("email_action_type") or "").strip()
    redirect = str(email_data.get("redirect_to") or "").strip()
    if not token_hash or action not in ALLOWED_ACTIONS or not redirect:
        raise HookError("email_link_data_invalid", status=400)
    base = _required(env, "PERSONALIZATION_SUPABASE_URL").rstrip("/")
    return f"{base}/auth/v1/verify?token={quote(token_hash)}&type={quote(action)}&redirect_to={quote(redirect, safe='')}"


def _render_message(token: str, confirmation_url: str) -> tuple[str, str, str]:
    if not TOKEN_RE.fullmatch(token):
        raise HookError("email_token_invalid", status=400)
    subject = f"Код {token} — вход в Анонсы"
    safe_url = html.escape(confirmation_url, quote=True)
    text = (
        f"Код для входа: {token}\n\n"
        f"Войти по ссылке: {confirmation_url}\n\n"
        "Код и ссылка действуют один раз. Если вы не запрашивали вход, просто проигнорируйте письмо."
    )
    html_body = f"""<!doctype html><html lang=\"ru\"><body style=\"margin:0;background:#f7f0e6;color:#241a15;font-family:Arial,sans-serif\"><table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\"><tr><td align=\"center\" style=\"padding:24px 12px\"><table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"max-width:520px;background:#fffaf3;border:1px solid #e5d3c1;border-radius:24px\"><tr><td style=\"padding:32px\"><div style=\"font-size:14px;font-weight:700;color:#a54222;letter-spacing:.08em;text-transform:uppercase\">Анонсы</div><h1 style=\"margin:14px 0 8px;font-size:28px;line-height:1.15\">Вход по почте</h1><p style=\"margin:0 0 24px;font-size:16px;line-height:1.5;color:#6d5c52\">Введите этот код на странице входа:</p><div style=\"font-size:36px;line-height:1;font-weight:800;letter-spacing:.18em;text-align:center;background:#f4e7d8;border-radius:18px;padding:22px 12px\">{token}</div><p style=\"margin:24px 0 12px;text-align:center\"><a href=\"{safe_url}\" style=\"display:inline-block;background:#b94d25;color:#fff;text-decoration:none;font-weight:700;border-radius:999px;padding:15px 24px\">Войти по ссылке</a></p><p style=\"margin:20px 0 0;font-size:13px;line-height:1.5;color:#89766b\">Код и ссылка действуют один раз. Если вы не запрашивали вход, просто проигнорируйте письмо.</p></td></tr></table></td></tr></table></body></html>"""
    return subject, text, html_body


def _postbox_send(to_email: str, subject: str, text: str, html_body: str, attempt_id: uuid.UUID, *, iam_token: str, env: Mapping[str, str], transport: Transport) -> str:
    from_email = str(env.get("AUTH_POSTBOX_FROM_EMAIL") or "notify@kenigevents.ru")
    from_name = str(env.get("AUTH_FROM_NAME") or "Полюбить Калининград · Анонсы")
    reply_to = str(env.get("AUTH_REPLY_TO") or "info@kenigevents.ru")
    configuration_set = _required(env, "POSTBOX_CONFIGURATION_SET")
    message = EmailMessage(policy=SMTP)
    message["From"] = f"{from_name} <{from_email}>"
    message["To"] = to_email
    message["Reply-To"] = reply_to
    message["Subject"] = subject
    message["X-KenigEvents-Auth-Attempt"] = str(attempt_id)
    message.set_content(text)
    message.add_alternative(html_body, subtype="html")
    payload = {
        "ConfigurationSetName": configuration_set,
        "FromEmailAddress": from_email,
        "Destination": {"ToAddresses": [to_email]},
        "Content": {"Raw": {"Data": base64.b64encode(message.as_bytes()).decode("ascii")}},
    }
    status, raw = transport(
        "POST",
        str(env.get("POSTBOX_ENDPOINT") or "https://postbox.cloud.yandex.net/v2/email/outbound-emails"),
        {"Content-Type": "application/json", "X-YaCloud-SubjectToken": iam_token},
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(),
        2.0,
    )
    if status != 200:
        raise HookError("provider_rejected", status=503, provider_outcome="definitive_reject")
    try:
        message_id = str(json.loads(raw)["MessageId"]).strip()
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HookError("provider_receipt_invalid", status=503, provider_outcome="definitive_reject") from exc
    if not message_id:
        raise HookError("provider_receipt_invalid", status=503, provider_outcome="definitive_reject")
    return message_id


def _notisend_send(to_email: str, subject: str, text: str, html_body: str, attempt_id: uuid.UUID, *, env: Mapping[str, str], transport: Transport) -> str:
    token = _required(env, "NOTISEND_API_TOKEN")
    payload = {
        "from_email": _required(env, "AUTH_NOTISEND_FROM_EMAIL"),
        "from_name": str(env.get("AUTH_FROM_NAME") or "Полюбить Калининград · Анонсы"),
        "to": to_email,
        "subject": subject,
        "text": text,
        "html": html_body,
        "payment": "subscriber",
        "smtp_headers": {
            "Reply-To": str(env.get("AUTH_REPLY_TO") or "info@kenigevents.ru"),
            "X-KenigEvents-Auth-Attempt": str(attempt_id),
        },
    }
    endpoint = str(env.get("NOTISEND_API_ENDPOINT") or "https://api.notisend.ru/v1").rstrip("/")
    status, raw = transport(
        "POST",
        f"{endpoint}/email/messages",
        {"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(),
        2.0,
    )
    if status < 200 or status >= 300:
        raise HookError("provider_rejected", status=503, provider_outcome="definitive_reject")
    try:
        message_id = str(json.loads(raw)["id"]).strip()
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        # A 2xx without a usable receipt may already have consumed the provider
        # request and recipient slot. Keep it ambiguous: never retry another
        # provider and never release the capacity reservation automatically.
        raise HookError("provider_receipt_invalid", status=503, provider_outcome="ambiguous") from exc
    if not message_id:
        raise HookError("provider_receipt_invalid", status=503, provider_outcome="ambiguous")
    return message_id


def process(raw_body: bytes, headers: Mapping[str, Any], *, context: Any, env: Mapping[str, str], transport: Transport = urllib_transport, now: int | None = None) -> dict[str, Any]:
    webhook_id = verify_standard_webhook(raw_body, headers, _required(env, "SEND_EMAIL_HOOK_SECRET"), now=now)
    data = _payload(raw_body)
    user = data.get("user")
    email_data = data.get("email_data")
    if not isinstance(user, Mapping) or not isinstance(email_data, Mapping):
        raise HookError("hook_payload_invalid", status=400)
    try:
        user_id = uuid.UUID(str(user.get("id") or ""))
    except ValueError as exc:
        raise HookError("hook_user_invalid", status=400) from exc
    to_email = str(user.get("email") or "").strip().lower()
    if len(to_email) not in range(3, 321) or "@" not in to_email or any(char in to_email for char in "\r\n\x00"):
        raise HookError("hook_user_invalid", status=400)
    action = str(email_data.get("email_action_type") or "").strip()
    if action not in ALLOWED_ACTIONS:
        raise HookError("hook_action_unsupported", status=400)
    attempt_id = _attempt_id(str(email_data.get("redirect_to") or ""), webhook_id)
    prefer_notisend = choose_provider(
        action=action,
        send_ordinal=1,
        returning_user=_is_returning_user(str(user.get("created_at") or "")),
        user_id=user_id,
        to_email=to_email,
        env=env,
    ) == "notisend"
    row = _begin_delivery(
        attempt_id,
        user_id,
        action,
        prefer_notisend,
        env=env,
        transport=transport,
    )
    is_new = row.get("is_new") is True
    previous_outcome = str(row.get("previous_outcome") or "")
    previous_provider = str(row.get("previous_provider") or "") or None
    previous_message_id = str(row.get("previous_message_id") or "") or None
    if not is_new:
        if previous_outcome == "accepted" and previous_provider and previous_message_id:
            _log("INFO", "delivery_duplicate_acknowledged", attempt_id=str(attempt_id), provider=previous_provider)
            return {"attempt_id": str(attempt_id), "provider": previous_provider, "duplicate": True}
        raise HookError("delivery_attempt_already_finalized", status=503)

    try:
        send_ordinal = int(row.get("send_ordinal"))
    except (TypeError, ValueError) as exc:
        raise HookError("delivery_ledger_response_invalid", status=503) from exc
    notisend_admitted = row.get("notisend_admitted")
    if not isinstance(notisend_admitted, bool):
        raise HookError("notisend_capacity_response_invalid", status=503)
    # Capacity routing happened atomically with attempt reservation, before any
    # provider network dispatch. It is not a retry/fallback and cannot duplicate
    # one Auth email.
    provider = "notisend" if notisend_admitted else "postbox"
    subject, text, html_body = _render_message(str(email_data.get("token") or ""), _confirmation_url(email_data, env=env))
    try:
        if provider == "notisend":
            message_id = _notisend_send(to_email, subject, text, html_body, attempt_id, env=env, transport=transport)
        else:
            iam_token = str(getattr(context, "token", "") or "").strip()
            if not iam_token:
                raise HookError("postbox_iam_token_missing", status=500, provider_outcome="configuration_error")
            message_id = _postbox_send(to_email, subject, text, html_body, attempt_id, iam_token=iam_token, env=env, transport=transport)
    except HookError as exc:
        outcome = exc.provider_outcome or "ambiguous"
        try:
            _complete_delivery(attempt_id, provider, outcome, None, env=env, transport=transport)
        except HookError:
            _log("ERROR", "delivery_completion_failed", attempt_id=str(attempt_id), provider=provider, outcome=outcome)
        raise
    _complete_delivery(attempt_id, provider, "accepted", message_id, env=env, transport=transport)
    _log("INFO", "delivery_accepted", attempt_id=str(attempt_id), provider=provider, send_ordinal=send_ordinal)
    return {"attempt_id": str(attempt_id), "provider": provider, "duplicate": False}


def handler(event: Mapping[str, Any], context: Any) -> dict[str, Any]:
    try:
        body, headers = _parse_event(event)
        process(body, headers, context=context, env=os.environ)
        return _json_response(200, {})
    except HookError as exc:
        _log("ERROR", "hook_failed", error_code=exc.code)
        return _json_response(exc.status, {"error": {"http_code": exc.status, "message": exc.code}})
    except Exception:
        _log("ERROR", "hook_failed", error_code="internal_error")
        return _json_response(500, {"error": {"http_code": 500, "message": "internal_error"}})
