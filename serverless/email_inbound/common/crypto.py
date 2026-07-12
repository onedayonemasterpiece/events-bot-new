from __future__ import annotations

import base64
import hashlib
import hmac

from .contract import ContractError


SIGNATURE_VERSION = "v1"


def content_sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def adapter_signature(
    *, secret: str, path: str, timestamp: int, body: bytes
) -> tuple[str, str]:
    if len(secret.encode("utf-8")) < 32:
        raise ContractError("adapter_secret_too_short")
    if not path.startswith("/") or "\n" in path:
        raise ContractError("adapter_path_invalid")
    digest = content_sha256(body)
    canonical = (
        f"{SIGNATURE_VERSION}\nPOST\n{path}\n{timestamp}\n{digest}"
    ).encode("utf-8")
    raw = hmac.new(secret.encode("utf-8"), canonical, hashlib.sha256).digest()
    signature = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    return digest, signature
