from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Mapping, Protocol


class ProviderError(RuntimeError):
    pass


class ProviderConfigurationError(ProviderError):
    pass


class ProviderRejected(ProviderError):
    def __init__(self, message: str, *, status: int | None = None, retryable: bool = False):
        super().__init__(message)
        self.status = status
        self.retryable = retryable


class AmbiguousDelivery(ProviderError):
    """The request may have been accepted; automatic retry would risk a duplicate."""


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]

    def json(self) -> object:
        return json.loads(self.body.decode("utf-8"))


class HttpTransport(Protocol):
    def request(self, method: str, url: str, *, headers: Mapping[str, str], body: bytes | None) -> HttpResponse: ...


class UrllibTransport:
    def __init__(self, timeout_seconds: float = 20.0):
        self.timeout_seconds = timeout_seconds

    def request(self, method: str, url: str, *, headers: Mapping[str, str], body: bytes | None) -> HttpResponse:
        request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return HttpResponse(response.status, response.read(), dict(response.headers.items()))
        except urllib.error.HTTPError as exc:
            return HttpResponse(exc.code, exc.read(), dict(exc.headers.items()))
        except (TimeoutError, socket.timeout, urllib.error.URLError, OSError) as exc:
            raise AmbiguousDelivery(f"provider request outcome is unknown: {type(exc).__name__}") from exc
