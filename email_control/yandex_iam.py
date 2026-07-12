from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Mapping

import jwt


IAM_TOKEN_URL = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
_SAFE_ID = re.compile(r"^[a-z0-9]{10,64}$")
_KEY_PREAMBLE = re.compile(
    r"^PLEASE DO NOT REMOVE THIS LINE! Yandex\.Cloud SA Key ID <([a-z0-9]{10,64})>$"
)
TokenTransport = Callable[[str, bytes, Mapping[str, str], float], tuple[int, bytes]]


class YandexIamError(RuntimeError):
    pass


def _transport(url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return int(response.status), response.read(16_384)
    except urllib.error.HTTPError as exc:
        exc.read(16_384)
        return int(exc.code), b""
    except (TimeoutError, OSError, urllib.error.URLError) as exc:
        raise YandexIamError("iam_token_transport_failed") from exc


class YandexIamTokenProvider:
    """Mint and cache short-lived IAM tokens from one authorized SA key."""

    def __init__(
        self,
        key_json: str,
        *,
        transport: TokenTransport = _transport,
        now: Callable[[], float] = time.time,
        timeout_seconds: float = 10.0,
    ) -> None:
        if not isinstance(key_json, str) or len(key_json) not in range(50, 32_001):
            raise YandexIamError("authorized_key_invalid")
        try:
            raw = json.loads(key_json)
        except json.JSONDecodeError as exc:
            raise YandexIamError("authorized_key_invalid") from exc
        if not isinstance(raw, dict):
            raise YandexIamError("authorized_key_invalid")
        self.key_id = str(raw.get("id") or "").strip()
        self.service_account_id = str(raw.get("service_account_id") or "").strip()
        private_key = str(raw.get("private_key") or "").strip()
        pem_marker = "-----BEGIN PRIVATE KEY-----"
        pem_offset = private_key.find(pem_marker)
        preamble = private_key[:pem_offset].strip() if pem_offset >= 0 else ""
        preamble_match = _KEY_PREAMBLE.fullmatch(preamble) if preamble else None
        if preamble and (preamble_match is None or preamble_match.group(1) != self.key_id):
            raise YandexIamError("authorized_key_invalid")
        self.private_key = private_key[pem_offset:].strip() if pem_offset >= 0 else private_key
        if (
            not _SAFE_ID.fullmatch(self.key_id)
            or not _SAFE_ID.fullmatch(self.service_account_id)
            or not self.private_key.startswith("-----BEGIN PRIVATE KEY-----")
            or not self.private_key.endswith("-----END PRIVATE KEY-----")
        ):
            raise YandexIamError("authorized_key_invalid")
        self.transport = transport
        self.now = now
        self.timeout_seconds = min(max(float(timeout_seconds), 1.0), 30.0)
        self._token = ""
        self._expires_at = 0.0
        self._lock = threading.Lock()

    def get_token(self) -> str:
        now = float(self.now())
        if self._token and self._expires_at - now > 300:
            return self._token
        with self._lock:
            now = float(self.now())
            if self._token and self._expires_at - now > 300:
                return self._token
            claims = {
                "iss": self.service_account_id,
                "aud": IAM_TOKEN_URL,
                "iat": int(now),
                "exp": int(now) + 3600,
            }
            try:
                encoded = jwt.encode(
                    claims,
                    self.private_key,
                    algorithm="PS256",
                    headers={"kid": self.key_id},
                )
            except Exception as exc:
                raise YandexIamError("jwt_sign_failed") from exc
            status, response = self.transport(
                IAM_TOKEN_URL,
                json.dumps({"jwt": encoded}, separators=(",", ":")).encode(),
                {"Content-Type": "application/json", "Accept": "application/json"},
                self.timeout_seconds,
            )
            if status != 200:
                raise YandexIamError("iam_token_rejected")
            try:
                result = json.loads(response)
                token = str(result["iamToken"]).strip()
                expires = datetime.fromisoformat(str(result["expiresAt"]).replace("Z", "+00:00"))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise YandexIamError("iam_token_response_invalid") from exc
            if not token or expires.tzinfo is None:
                raise YandexIamError("iam_token_response_invalid")
            expires_at = expires.astimezone(timezone.utc).timestamp()
            if expires_at - now < 600:
                raise YandexIamError("iam_token_lifetime_invalid")
            self._token = token
            self._expires_at = expires_at
            return token
