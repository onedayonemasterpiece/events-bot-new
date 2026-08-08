from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any, Mapping


class TokenValidationError(ValueError):
    pass


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(value: str) -> bytes:
    if not isinstance(value, str):
        raise TokenValidationError("invalid_base64")
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode((value + padding).encode("ascii"))
    except Exception as exc:
        raise TokenValidationError("invalid_base64") from exc


def random_token(bytes_count: int = 32) -> str:
    return secrets.token_urlsafe(bytes_count)


def secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def constant_time_equal(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


def pkce_s256(verifier: str) -> str:
    return b64url_encode(hashlib.sha256(verifier.encode("ascii")).digest())


def verify_pkce(verifier: str, challenge: str) -> bool:
    if not verifier or not challenge:
        return False
    try:
        calculated = pkce_s256(verifier)
    except (UnicodeEncodeError, ValueError):
        return False
    return hmac.compare_digest(calculated, challenge)


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sign_compact_token(payload: Mapping[str, Any], signing_key: str, *, token_type: str) -> str:
    header = {"alg": "HS256", "typ": token_type}
    header_part = b64url_encode(_canonical_json(header))
    payload_part = b64url_encode(_canonical_json(dict(payload)))
    signing_input = f"{header_part}.{payload_part}".encode("ascii")
    signature = hmac.new(signing_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_part}.{payload_part}.{b64url_encode(signature)}"


def verify_compact_token(
    token: str,
    signing_key: str,
    *,
    expected_type: str,
    now: int | None = None,
) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise TokenValidationError("malformed_token")
    header_part, payload_part, signature_part = parts
    signing_input = f"{header_part}.{payload_part}".encode("ascii")
    expected_signature = hmac.new(
        signing_key.encode("utf-8"), signing_input, hashlib.sha256
    ).digest()
    actual_signature = b64url_decode(signature_part)
    if not hmac.compare_digest(actual_signature, expected_signature):
        raise TokenValidationError("bad_signature")
    try:
        header = json.loads(b64url_decode(header_part))
        payload = json.loads(b64url_decode(payload_part))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise TokenValidationError("malformed_token") from exc
    if not isinstance(header, dict) or not isinstance(payload, dict):
        raise TokenValidationError("malformed_token")
    if header.get("alg") != "HS256" or header.get("typ") != expected_type:
        raise TokenValidationError("unexpected_token_type")
    current = int(time.time()) if now is None else int(now)
    exp = payload.get("exp")
    nbf = payload.get("nbf")
    if not isinstance(exp, int) or exp <= current:
        raise TokenValidationError("token_expired")
    if isinstance(nbf, int) and nbf > current + 30:
        raise TokenValidationError("token_not_yet_valid")
    return payload


@dataclass(frozen=True, slots=True)
class AccessIdentity:
    subject: str
    client_id: str
    scopes: frozenset[str]
    audience: str
    token_id: str
    expires_at: int


def mint_access_token(
    *,
    signing_key: str,
    issuer: str,
    audience: str,
    subject: str,
    client_id: str,
    scopes: set[str] | frozenset[str],
    lifetime_seconds: int,
    now: int | None = None,
) -> tuple[str, AccessIdentity]:
    issued = int(time.time()) if now is None else int(now)
    token_id = secrets.token_hex(16)
    expires = issued + int(lifetime_seconds)
    payload = {
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "client_id": client_id,
        "scope": " ".join(sorted(scopes)),
        "iat": issued,
        "nbf": issued - 5,
        "exp": expires,
        "jti": token_id,
    }
    token = sign_compact_token(payload, signing_key, token_type="at+jwt")
    identity = AccessIdentity(
        subject=subject,
        client_id=client_id,
        scopes=frozenset(scopes),
        audience=audience,
        token_id=token_id,
        expires_at=expires,
    )
    return token, identity


def validate_access_token(
    token: str,
    *,
    signing_key: str,
    issuer: str,
    audience: str,
    now: int | None = None,
) -> AccessIdentity:
    payload = verify_compact_token(
        token,
        signing_key,
        expected_type="at+jwt",
        now=now,
    )
    if payload.get("iss") != issuer:
        raise TokenValidationError("wrong_issuer")
    token_audience = payload.get("aud")
    if token_audience != audience:
        raise TokenValidationError("wrong_audience")
    subject = payload.get("sub")
    client_id = payload.get("client_id")
    token_id = payload.get("jti")
    scope_raw = payload.get("scope", "")
    exp = payload.get("exp")
    if not all(isinstance(v, str) and v for v in (subject, client_id, token_id)):
        raise TokenValidationError("missing_claim")
    if not isinstance(scope_raw, str) or not isinstance(exp, int):
        raise TokenValidationError("missing_claim")
    return AccessIdentity(
        subject=subject,
        client_id=client_id,
        scopes=frozenset(part for part in scope_raw.split() if part),
        audience=audience,
        token_id=token_id,
        expires_at=exp,
    )
