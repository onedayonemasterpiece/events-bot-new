from __future__ import annotations

import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formataddr, make_msgid


@dataclass(frozen=True)
class PostboxConfig:
    enabled: bool = False
    dry_run: bool = True
    smtp_host: str = "postbox.cloud.yandex.net"
    smtp_port: int = 587
    access_key_id: str = ""
    secret_access_key: str = ""
    from_email: str = "info@kenigevents.ru"
    from_name: str = "Полюбить Калининград | Анонсы"
    configuration_set: str = ""

    @classmethod
    def from_env(cls) -> "PostboxConfig":
        return cls(
            enabled=(os.getenv("POSTBOX_ENABLED") or "0").strip().lower() in {"1", "true", "yes"},
            dry_run=(os.getenv("POSTBOX_DRY_RUN") or "1").strip().lower() not in {"0", "false", "no"},
            smtp_host=os.getenv("POSTBOX_SMTP_HOST") or "postbox.cloud.yandex.net",
            smtp_port=int(os.getenv("POSTBOX_SMTP_PORT") or "587"),
            access_key_id=os.getenv("POSTBOX_ACCESS_KEY_ID") or "",
            secret_access_key=os.getenv("POSTBOX_SECRET_ACCESS_KEY") or os.getenv("POSTBOX_SECRET_KEY") or "",
            from_email=os.getenv("POSTBOX_FROM_EMAIL") or "info@kenigevents.ru",
            from_name=os.getenv("POSTBOX_FROM_NAME") or "Полюбить Калининград | Анонсы",
            configuration_set=os.getenv("POSTBOX_CONFIGURATION_SET") or "",
        )


@dataclass(frozen=True)
class PostboxSendResult:
    provider_message_id: str
    dry_run: bool


class PostboxSmtpSender:
    """Yandex Cloud Postbox SMTP-compatible sender; defaults to dry-run."""

    def __init__(self, config: PostboxConfig | None = None) -> None:
        self.config = config or PostboxConfig.from_env()

    def build_message(self, *, to_email: str, subject: str, text: str, html: str | None = None) -> EmailMessage:
        msg = EmailMessage()
        msg["From"] = formataddr((self.config.from_name, self.config.from_email))
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Message-ID"] = make_msgid(domain=self.config.from_email.split("@")[-1])
        if self.config.configuration_set:
            msg["X-SES-CONFIGURATION-SET"] = self.config.configuration_set
        msg.set_content(text)
        if html:
            msg.add_alternative(html, subtype="html")
        return msg

    def send(self, *, to_email: str, subject: str, text: str, html: str | None = None) -> PostboxSendResult:
        msg = self.build_message(to_email=to_email, subject=subject, text=text, html=html)
        message_id = str(msg["Message-ID"])
        if self.config.dry_run or not self.config.enabled:
            return PostboxSendResult(provider_message_id=f"dry-run:{message_id}", dry_run=True)
        if not self.config.access_key_id or not self.config.secret_access_key:
            raise RuntimeError("Postbox credentials are missing")
        smtp_cls = smtplib.SMTP_SSL if self.config.smtp_port == 465 else smtplib.SMTP
        with smtp_cls(self.config.smtp_host, self.config.smtp_port, timeout=30) as smtp:
            if self.config.smtp_port != 465:
                smtp.starttls()
            smtp.login(self.config.access_key_id, self.config.secret_access_key)
            smtp.send_message(msg)
        return PostboxSendResult(provider_message_id=message_id, dry_run=False)
