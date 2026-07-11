from __future__ import annotations

from email_control.models import EmailMessage, ProviderResult, Stream

from .notisend import NotiSendAdapter
from .postbox import PostboxAdapter


class ProviderRouter:
    """Fixed stream routing. Deliberately has no fallback branch."""

    def __init__(self, postbox: PostboxAdapter, notisend: NotiSendAdapter):
        self.postbox = postbox
        self.notisend = notisend

    def send(self, message: EmailMessage) -> ProviderResult:
        if message.stream is Stream.TRANSACTIONAL:
            return self.postbox.send(message)
        if message.stream is Stream.RECOMMENDATION:
            return self.notisend.send(message)
        raise ValueError(f"unsupported email stream: {message.stream}")
