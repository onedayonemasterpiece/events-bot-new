from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from email_control.models import EmailMessage, ProviderResult, ProviderSignal, Stream

from .base import HttpTransport, ProviderConfigurationError, ProviderRejected, UrllibTransport


@dataclass(frozen=True)
class NotiSendConfig:
    enabled: bool = False
    dry_run: bool = True
    endpoint: str = "https://api.notisend.ru/v1"
    api_token: str = ""
    from_email: str = "events@news.kenigevents.ru"
    from_name: str = "Kenig Events"
    reply_to: str = "info@kenigevents.ru"


class NotiSendAdapter:
    provider = "notisend"

    def __init__(self, config: NotiSendConfig, transport: HttpTransport | None = None):
        self.config = config
        self.transport = transport or UrllibTransport()

    @property
    def auth_headers(self) -> dict[str, str]:
        if not self.config.api_token:
            raise ProviderConfigurationError("NotiSend API token is required")
        return {
            "Authorization": f"Bearer {self.config.api_token}",
            "Content-Type": "application/json",
        }

    def send(self, message: EmailMessage) -> ProviderResult:
        if message.stream is not Stream.RECOMMENDATION:
            raise ValueError("NotiSend is reserved for recommendations; no provider fallback is allowed")
        if not self.config.enabled or self.config.dry_run:
            return ProviderResult(self.provider, None, accepted=False, dry_run=True)

        smtp_headers = {
            "Reply-To": message.reply_to or self.config.reply_to,
            "X-KenigEvents-Outbox-ID": message.outbox_id,
            **dict(message.headers),
        }
        payload = {
            "from_email": self.config.from_email,
            "from_name": self.config.from_name,
            "to": message.to_email,
            "subject": message.subject,
            "text": message.text,
            "html": message.html,
            # Never spill into purchased credits or another provider when the 200-user
            # subscriber allowance is unavailable.
            "payment": "subscriber",
            "smtp_headers": smtp_headers,
        }
        response = self.transport.request(
            "POST",
            f"{self.config.endpoint.rstrip('/')}/email/messages",
            headers=self.auth_headers,
            body=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        )
        if response.status < 200 or response.status >= 300:
            raise ProviderRejected(
                f"NotiSend rejected send with HTTP {response.status}",
                status=response.status,
                retryable=response.status == 429 or response.status >= 500,
            )
        try:
            provider_id = str(response.json()["id"]).strip()  # type: ignore[index]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ProviderRejected("NotiSend acceptance response has no message id", status=response.status) from exc
        if not provider_id:
            raise ProviderRejected("NotiSend acceptance response has empty message id", status=response.status)
        return ProviderResult(self.provider, provider_id, accepted=True, dry_run=False, response_code=response.status)

    def get_message(self, provider_message_id: str) -> dict[str, Any]:
        response = self.transport.request(
            "GET",
            f"{self.config.endpoint.rstrip('/')}/email/messages/{provider_message_id}",
            headers=self.auth_headers,
            body=None,
        )
        if response.status != 200:
            raise ProviderRejected(
                f"NotiSend status lookup failed with HTTP {response.status}",
                status=response.status,
                retryable=response.status == 429 or response.status >= 500,
            )
        data = response.json()
        if not isinstance(data, dict) or str(data.get("id", "")) != str(provider_message_id):
            raise ProviderRejected("NotiSend status response does not match message id", status=response.status)
        return data


_EVENT_MAP = {
    "delivered": "delivered",
    "skipped": "skipped",
    "soft_bounced": "soft_bounce",
    "hard_bounced": "hard_bounce",
    "opened": "open",
    "clicked": "click",
    "unsubscribed": "unsubscribe",
    "complained": "complaint",
}


class NotiSendWebhookParser:
    """Parse NotiSend batches as untrusted signals.

    Public NotiSend documentation describes no webhook signature. These rows must not
    alter delivery or suppression until an authenticated GET /messages/:id verifies them.
    """

    def parse(self, raw_body: bytes) -> list[ProviderSignal]:
        if len(raw_body) > 512_000:
            raise ValueError("NotiSend webhook body is too large")
        payload = json.loads(raw_body.decode("utf-8"))
        if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
            raise ValueError("invalid NotiSend webhook payload")
        events = payload["events"]
        if len(events) > 500:
            raise ValueError("NotiSend webhook batch exceeds documented limit")
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        body_hash = hashlib.sha256(raw_body).hexdigest()
        signals: list[ProviderSignal] = []
        for item in events:
            if not isinstance(item, dict):
                raise ValueError("invalid NotiSend webhook event")
            name = str(item.get("name", ""))
            event_type = _EVENT_MAP.get(name)
            if not event_type:
                continue
            event_id = str(item.get("id", "")).strip()
            timestamp = int(item.get("timestamp", 0))
            if not event_id or timestamp <= 0:
                raise ValueError("NotiSend webhook event id and timestamp are required")
            meta_key = f"{meta.get('type', 'unknown')}:{meta.get('id', 'api')}"
            event_key = f"{meta_key}:{event_id}:{name}:{timestamp}"
            signals.append(
                ProviderSignal(
                    provider="notisend",
                    provider_event_key=event_key,
                    # Public webhook event IDs are not trusted as message correlation.
                    # The authenticated GET response must use the stored POST response id.
                    provider_message_id=None,
                    event_type=event_type,
                    event_at=timestamp,
                    email=str(item.get("email")) if item.get("email") else None,
                    authenticated=False,
                    verified=False,
                    payload_sha256=body_hash,
                )
            )
        return signals


def verified_status_signal(provider_message_id: str, status_payload: dict[str, Any]) -> ProviderSignal:
    """Build an authoritative signal only from a bearer-authenticated API response."""

    status = str(status_payload.get("status", ""))
    event_type = {
        "queued": "accepted",
        "sent": "accepted",
        "delivered": "delivered",
        "skipped": "skipped",
        "soft_bounced": "soft_bounce",
        "hard_bounced": "hard_bounce",
    }.get(status)
    events = status_payload.get("events") if isinstance(status_payload.get("events"), dict) else {}
    if int(events.get("spam", 0) or 0) > 0:
        event_type = "complaint"
    elif int(events.get("unsubscribe", 0) or 0) > 0:
        event_type = "unsubscribe"
    if event_type is None:
        raise ValueError(f"unsupported NotiSend status: {status}")
    canonical = json.dumps(status_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return ProviderSignal(
        provider="notisend",
        provider_event_key=f"api-status:{provider_message_id}:{event_type}:{hashlib.sha256(canonical).hexdigest()}",
        provider_message_id=provider_message_id,
        event_type=event_type,
        event_at=int(datetime.now(timezone.utc).timestamp()),
        email=str(status_payload.get("to")) if status_payload.get("to") else None,
        authenticated=True,
        verified=True,
        payload_sha256=hashlib.sha256(canonical).hexdigest(),
    )
