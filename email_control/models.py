from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


class Stream(str, Enum):
    TRANSACTIONAL = "transactional"
    RECOMMENDATION = "recommendation"


PROVIDER_BY_STREAM = {
    Stream.TRANSACTIONAL: "postbox",
    Stream.RECOMMENDATION: "notisend",
}


@dataclass(frozen=True)
class EmailMessage:
    outbox_id: str
    idempotency_key: str
    stream: Stream
    to_email: str
    subject: str
    text: str
    html: str
    reply_to: str
    headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in {
            "outbox_id": self.outbox_id,
            "idempotency_key": self.idempotency_key,
            "to_email": self.to_email,
            "subject": self.subject,
            "reply_to": self.reply_to,
        }.items():
            if not str(value).strip():
                raise ValueError(f"{name} is required")
            if "\r" in str(value) or "\n" in str(value):
                raise ValueError(f"{name} contains a header newline")
        if not self.text and not self.html:
            raise ValueError("text or html content is required")

    @property
    def provider(self) -> str:
        return PROVIDER_BY_STREAM[self.stream]


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    provider_message_id: str | None
    accepted: bool
    dry_run: bool
    response_code: int | None = None


@dataclass(frozen=True)
class ProviderSignal:
    provider: str
    provider_event_key: str
    provider_message_id: str | None
    event_type: str
    event_at: int
    email: str | None
    authenticated: bool
    verified: bool
    payload_sha256: str
