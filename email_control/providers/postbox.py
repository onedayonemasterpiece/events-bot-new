from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from email.message import EmailMessage as MimeMessage
from email.policy import SMTP

from email_control.models import EmailMessage, ProviderResult, Stream

from .base import HttpTransport, ProviderConfigurationError, ProviderRejected, UrllibTransport


@dataclass(frozen=True)
class PostboxConfig:
    enabled: bool = False
    dry_run: bool = True
    endpoint: str = "https://postbox.cloud.yandex.net/v2/email/outbound-emails"
    iam_token: str = ""
    from_email: str = "notify@kenigevents.ru"
    from_name: str = "Kenig Events"
    reply_to: str = "info@kenigevents.ru"
    configuration_set: str = ""


class PostboxAdapter:
    provider = "postbox"

    def __init__(self, config: PostboxConfig, transport: HttpTransport | None = None):
        self.config = config
        self.transport = transport or UrllibTransport()

    def send(self, message: EmailMessage) -> ProviderResult:
        if message.stream is not Stream.TRANSACTIONAL:
            raise ValueError("Postbox is reserved for transactional mail; no provider fallback is allowed")
        if not self.config.enabled or self.config.dry_run:
            return ProviderResult(self.provider, None, accepted=False, dry_run=True)
        if not self.config.iam_token or not self.config.configuration_set:
            raise ProviderConfigurationError("Postbox IAM token and configuration set are required")

        mime = MimeMessage(policy=SMTP)
        mime["From"] = f"{self.config.from_name} <{self.config.from_email}>"
        mime["To"] = message.to_email
        mime["Reply-To"] = message.reply_to or self.config.reply_to
        mime["Subject"] = message.subject
        mime["X-KenigEvents-Outbox-ID"] = message.outbox_id
        for name, value in message.headers.items():
            if name.lower() in {"from", "to", "subject", "message-id", "return-path"}:
                raise ValueError(f"caller may not override {name}")
            mime[name] = value
        mime.set_content(message.text or "")
        if message.html:
            mime.add_alternative(message.html, subtype="html")

        payload = {
            "ConfigurationSetName": self.config.configuration_set,
            "FromEmailAddress": self.config.from_email,
            "Destination": {"ToAddresses": [message.to_email]},
            "Content": {"Raw": {"Data": base64.b64encode(mime.as_bytes()).decode("ascii")}},
        }
        response = self.transport.request(
            "POST",
            self.config.endpoint,
            headers={
                "Content-Type": "application/json",
                "X-YaCloud-SubjectToken": self.config.iam_token,
            },
            body=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        )
        if response.status != 200:
            raise ProviderRejected(
                f"Postbox rejected send with HTTP {response.status}",
                status=response.status,
                retryable=response.status == 429 or response.status >= 500,
            )
        try:
            provider_id = str(response.json()["MessageId"]).strip()  # type: ignore[index]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ProviderRejected("Postbox acceptance response has no MessageId", status=response.status) from exc
        if not provider_id:
            raise ProviderRejected("Postbox acceptance response has empty MessageId", status=response.status)
        return ProviderResult(self.provider, provider_id, accepted=True, dry_run=False, response_code=response.status)
