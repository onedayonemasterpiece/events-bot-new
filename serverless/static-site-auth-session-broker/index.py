"""Constrained GitHub OIDC -> one-shot Supabase test-session credential broker.

The broker never invokes ``/auth/v1/otp`` and never sends mail.  It admits one
admin ``generate_link`` credential only after a durable, atomic per-run/persona
ledger claim.  All audit identifiers are keyed hashes; credentials and PII are
never logged.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlsplit

GITHUB_ISSUER = "https://token.actions.githubusercontent.com"
GITHUB_JWKS_URL = f"{GITHUB_ISSUER}/.well-known/jwks"
MAX_BODY_BYTES = 16_384
MAX_RESPONSE_BYTES = 32_768
SECRET_CANDIDATE = "{secret-candidate}"
_RUN_RE = re.compile(r"^[1-9][0-9]{0,19}$")
_PERSONA_RE = re.compile(r"^[a-z][a-z0-9-]{1,63}$")
_OTP_RE = re.compile(r"^[0-9]{6,10}$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

Transport = Callable[[str, str, Mapping[str, str], bytes | None, float], tuple[int, bytes]]
Verifier = Callable[[str, Mapping[str, str]], Mapping[str, Any]]
AuditSink = Callable[[Mapping[str, Any]], None]


class BrokerError(RuntimeError):
    def __init__(self, code: str, *, status: int = 403):
        super().__init__(code)
        self.code = code
        self.status = status


@dataclass(frozen=True)
class Policy:
    audience: str
    repositories: frozenset[str]
    refs: frozenset[str]
    workflow_refs: frozenset[str]
    environments: frozenset[str]
    events: frozenset[str]
    runs: frozenset[str]
    personas: Mapping[str, str]
    redirects: tuple[str, ...]
    per_run_persona_limit: int
    audit_key: str
    supabase_url: str
    supabase_service_role_key: str


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name) or "").strip()
    if not value:
        raise BrokerError(f"env_missing:{name.lower()}", status=500)
    return value


def _allowlist(env: Mapping[str, str], name: str) -> frozenset[str]:
    raw = _required(env, name)
    values = frozenset(item.strip() for item in re.split(r"[,\n]", raw) if item.strip())
    if not values or "*" in values:
        raise BrokerError(f"{name.lower()}_invalid", status=500)
    return values


def _base_url(value: str) -> str:
    parsed = urlsplit(value.rstrip("/"))
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password \
            or parsed.path or parsed.query or parsed.fragment:
        raise BrokerError("supabase_url_invalid", status=500)
    return value.rstrip("/")


def _service_role_key(env: Mapping[str, str]) -> str:
    """Return a broker-only legacy service-role JWT, failing closed on key type.

    Supabase's modern ``sb_secret_*`` keys are deliberately not accepted here:
    the Auth Admin endpoint used by this broker requires the legacy
    service-role JWT in both ``apikey`` and ``Authorization`` headers for this
    project.  Payload inspection is only a local configuration guard; upstream
    still authenticates the signed JWT.
    """
    key = _required(env, "AUTH_SESSION_BROKER_SUPABASE_SERVICE_ROLE_KEY")
    if key.startswith("sb_secret_"):
        raise BrokerError("supabase_service_role_key_invalid", status=500)
    try:
        parts = key.split(".")
        if len(parts) != 3:
            raise ValueError("jwt_parts")
        payload_raw = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_raw.encode()))
    except (ValueError, binascii.Error, json.JSONDecodeError, UnicodeError) as exc:
        raise BrokerError("supabase_service_role_key_invalid", status=500) from exc
    if not isinstance(payload, Mapping) or payload.get("role") != "service_role":
        raise BrokerError("supabase_service_role_key_invalid", status=500)
    return key


def _redirect_shape(value: str, *, template: bool) -> tuple[str, str, int | None]:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password \
            or parsed.query or parsed.fragment:
        raise BrokerError(
            "redirect_allowlist_invalid" if template else "redirect_not_allowed",
            status=500 if template else 403,
        )
    placeholder_count = parsed.path.count(SECRET_CANDIDATE)
    if template:
        if placeholder_count not in {0, 1}:
            raise BrokerError("redirect_allowlist_invalid", status=500)
        if placeholder_count == 1 and parsed.path != f"/_review/{SECRET_CANDIDATE}/poisk/":
            raise BrokerError("redirect_allowlist_invalid", status=500)
    elif placeholder_count:
        raise BrokerError("redirect_not_allowed")
    return f"{parsed.scheme}://{parsed.netloc}", parsed.path, placeholder_count or None


def policy_from_env(env: Mapping[str, str]) -> Policy:
    try:
        personas_raw = json.loads(_required(env, "AUTH_SESSION_BROKER_PERSONAS_JSON"))
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise BrokerError("personas_invalid", status=500) from exc
    if not isinstance(personas_raw, dict) or not personas_raw:
        raise BrokerError("personas_invalid", status=500)
    personas: dict[str, str] = {}
    for persona, email in personas_raw.items():
        persona = str(persona)
        email = str(email).strip().lower()
        if not _PERSONA_RE.fullmatch(persona) or len(email) > 320 or "@" not in email \
                or any(char in email for char in "\r\n\x00"):
            raise BrokerError("personas_invalid", status=500)
        personas[persona] = email

    redirects = tuple(item.strip() for item in re.split(
        r"[,\n]", _required(env, "AUTH_SESSION_BROKER_ALLOWED_REDIRECTS")
    ) if item.strip())
    if not redirects or "*" in redirects:
        raise BrokerError("redirect_allowlist_invalid", status=500)
    for redirect in redirects:
        _redirect_shape(redirect, template=True)

    runs = _allowlist(env, "AUTH_SESSION_BROKER_ALLOWED_RUNS")
    if any(item != "github-claim-bound" and not _RUN_RE.fullmatch(item) for item in runs):
        raise BrokerError("allowed_runs_invalid", status=500)
    try:
        limit = int(_required(env, "AUTH_SESSION_BROKER_PER_RUN_PERSONA_LIMIT"))
    except ValueError as exc:
        raise BrokerError("per_run_persona_limit_invalid", status=500) from exc
    # This broker deliberately implements one credential per verified run and
    # persona. A higher value would weaken the replay boundary.
    if limit != 1:
        raise BrokerError("per_run_persona_limit_invalid", status=500)

    audit_key = _required(env, "AUTH_SESSION_BROKER_AUDIT_HMAC_KEY")
    if len(audit_key.encode()) < 32:
        raise BrokerError("audit_hmac_key_invalid", status=500)
    return Policy(
        audience=_required(env, "AUTH_SESSION_BROKER_OIDC_AUDIENCE"),
        repositories=_allowlist(env, "AUTH_SESSION_BROKER_ALLOWED_REPOSITORIES"),
        refs=_allowlist(env, "AUTH_SESSION_BROKER_ALLOWED_REFS"),
        workflow_refs=_allowlist(env, "AUTH_SESSION_BROKER_ALLOWED_WORKFLOW_REFS"),
        environments=_allowlist(env, "AUTH_SESSION_BROKER_ALLOWED_ENVIRONMENTS"),
        events=_allowlist(env, "AUTH_SESSION_BROKER_ALLOWED_EVENTS"),
        runs=runs,
        personas=personas,
        redirects=redirects,
        per_run_persona_limit=limit,
        audit_key=audit_key,
        supabase_url=_base_url(_required(env, "PERSONALIZATION_SUPABASE_URL")),
        supabase_service_role_key=_service_role_key(env),
    )


def verify_github_oidc(token: str, env: Mapping[str, str]) -> Mapping[str, Any]:
    """Verify a GitHub Actions JWT against GitHub's published JWKS.

    PyJWT is imported lazily so unit tests can inject a verifier without loading
    network/crypto dependencies.  Signature, issuer, audience, lifetime and the
    security-relevant claims are all mandatory.
    """
    try:
        import jwt
    except ImportError as exc:  # pragma: no cover - deployment configuration
        raise BrokerError("oidc_verifier_unavailable", status=500) from exc
    try:
        signing_key = jwt.PyJWKClient(GITHUB_JWKS_URL, cache_keys=True).get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token, signing_key.key, algorithms=["RS256"],
            issuer=GITHUB_ISSUER, audience=_required(env, "AUTH_SESSION_BROKER_OIDC_AUDIENCE"),
            options={"require": [
                "exp", "iat", "nbf", "iss", "aud", "sub", "repository", "ref", "workflow_ref",
                "environment", "event_name", "run_id", "run_attempt", "sha", "jti",
            ]},
        )
    except Exception as exc:
        raise BrokerError("oidc_token_invalid", status=401) from exc
    if not isinstance(claims, Mapping):
        raise BrokerError("oidc_token_invalid", status=401)
    return claims


def _claim(claims: Mapping[str, Any], name: str) -> str:
    value = str(claims.get(name) or "").strip()
    if not value:
        raise BrokerError(f"claim_missing:{name}")
    return value


def _redirect_allowed(value: str, allowlist: tuple[str, ...]) -> bool:
    origin, path, _placeholder = _redirect_shape(value, template=False)
    for rule in allowlist:
        rule_origin, rule_path, placeholder = _redirect_shape(rule, template=True)
        if origin != rule_origin:
            continue
        if not placeholder and path == rule_path:
            return True
        if placeholder:
            pattern = "^" + re.escape(rule_path).replace(re.escape(SECRET_CANDIDATE), r"[A-Za-z0-9_-]{43}") + "$"
            if re.fullmatch(pattern, path):
                return True
    return False


def authorize_request(request: Mapping[str, Any], claims: Mapping[str, Any], policy: Policy) -> tuple[str, str, str]:
    if _claim(claims, "iss") != GITHUB_ISSUER or _claim(claims, "aud") != policy.audience:
        raise BrokerError("oidc_identity_invalid")
    repository = _claim(claims, "repository")
    ref = _claim(claims, "ref")
    workflow_ref = _claim(claims, "workflow_ref")
    environment = _claim(claims, "environment")
    event_name = _claim(claims, "event_name")
    run_id = _claim(claims, "run_id")
    if repository not in policy.repositories:
        raise BrokerError("repository_not_allowed")
    if ref not in policy.refs:
        raise BrokerError("ref_not_allowed")
    if workflow_ref not in policy.workflow_refs:
        raise BrokerError("workflow_not_allowed")
    if environment not in policy.environments:
        raise BrokerError("environment_not_allowed")
    if event_name not in policy.events:
        raise BrokerError("event_not_allowed")
    if not _RUN_RE.fullmatch(run_id):
        raise BrokerError("run_invalid")
    if "github-claim-bound" not in policy.runs and run_id not in policy.runs:
        raise BrokerError("run_not_allowed")
    expected_sub = f"repo:{repository}:environment:{environment}"
    if _claim(claims, "sub") != expected_sub:
        raise BrokerError("subject_not_allowed")
    if not _SHA_RE.fullmatch(_claim(claims, "sha")):
        raise BrokerError("sha_invalid")

    requested_run = str(request.get("run_id") or "").strip()
    if requested_run != run_id:
        raise BrokerError("run_claim_mismatch")
    persona = str(request.get("persona_id") or "").strip()
    if persona not in policy.personas:
        raise BrokerError("persona_not_allowed")
    redirect = str(request.get("redirect_to") or "").strip()
    if not _redirect_allowed(redirect, policy.redirects):
        raise BrokerError("redirect_not_allowed")
    return run_id, persona, redirect


def urllib_transport(method: str, url: str, headers: Mapping[str, str], body: bytes | None, timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read(MAX_RESPONSE_BYTES)
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read(MAX_RESPONSE_BYTES)
    except (TimeoutError, urllib.error.URLError, OSError) as exc:
        raise BrokerError("upstream_unavailable", status=503) from exc


def _headers(policy: Policy) -> dict[str, str]:
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "apikey": policy.supabase_service_role_key,
        "authorization": f"Bearer {policy.supabase_service_role_key}",
    }


def _json_call(path: str, payload: Mapping[str, Any], *, policy: Policy, transport: Transport) -> Any:
    status, raw = transport(
        "POST", f"{policy.supabase_url}{path}", _headers(policy),
        json.dumps(dict(payload), separators=(",", ":")).encode(), 5.0,
    )
    if status < 200 or status >= 300:
        raise BrokerError("supabase_request_rejected", status=503)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise BrokerError("supabase_response_invalid", status=503) from exc


def _audit_hash(value: str, key: str) -> str:
    return hmac.new(key.encode(), value.encode(), hashlib.sha256).hexdigest()[:24]


def _audit(sink: AuditSink, policy: Policy, *, outcome: str, claims: Mapping[str, Any], persona: str, redirect: str) -> None:
    # Never include claim values, run ids, redirect paths, email, OTP or JWT.
    sink({
        "schema": "static_site_auth_session_broker_audit.v1",
        "component": "static-site-auth-session-broker", "outcome": outcome,
        "repository_hash": _audit_hash(_claim(claims, "repository"), policy.audit_key),
        "ref_hash": _audit_hash(_claim(claims, "ref"), policy.audit_key),
        "workflow_hash": _audit_hash(_claim(claims, "workflow_ref"), policy.audit_key),
        "environment_hash": _audit_hash(_claim(claims, "environment"), policy.audit_key),
        "run_hash": _audit_hash(_claim(claims, "run_id"), policy.audit_key),
        "persona_hash": _audit_hash(persona, policy.audit_key),
        "redirect_hash": _audit_hash(redirect, policy.audit_key),
    })


def _stdout_audit(row: Mapping[str, Any]) -> None:
    print(json.dumps(dict(row), ensure_ascii=True, sort_keys=True, separators=(",", ":")), flush=True)


def process(
    request: Mapping[str, Any], *, token: str, env: Mapping[str, str] = os.environ,
    transport: Transport = urllib_transport, verifier: Verifier = verify_github_oidc,
    audit_sink: AuditSink = _stdout_audit,
) -> dict[str, Any]:
    if not isinstance(request, Mapping):
        raise BrokerError("request_invalid", status=400)
    policy = policy_from_env(env)
    claims = verifier(token, env)
    run_id, persona, redirect = authorize_request(request, claims, policy)
    run_attempt = _claim(claims, "run_attempt")
    if not _RUN_RE.fullmatch(run_attempt):
        raise BrokerError("run_attempt_invalid")

    admission = _json_call("/rest/v1/rpc/claim_static_site_auth_session_issue_v1", {
        "p_run_id": run_id,
        "p_run_attempt": int(run_attempt),
        "p_persona_id": persona,
        "p_repository": _claim(claims, "repository"),
        "p_workflow_ref": _claim(claims, "workflow_ref"),
        "p_limit": policy.per_run_persona_limit,
    }, policy=policy, transport=transport)
    admitted = admission is True or (isinstance(admission, Mapping) and admission.get("admitted") is True)
    if not admitted:
        _audit(audit_sink, policy, outcome="limit_rejected", claims=claims, persona=persona, redirect=redirect)
        raise BrokerError("issuance_limit_reached", status=409)

    # This calls the raw GoTrue REST endpoint rather than supabase-js.  Its
    # request/response contract is snake_case and flat; the SDK alone wraps
    # these fields below ``data.properties``.
    issued = _json_call("/auth/v1/admin/generate_link", {
        "type": "magiclink", "email": policy.personas[persona], "redirect_to": redirect,
    }, policy=policy, transport=transport)
    properties = issued.get("properties") if isinstance(issued, Mapping) else None
    if not isinstance(properties, Mapping) and isinstance(issued, Mapping) and isinstance(issued.get("data"), Mapping):
        properties = issued["data"].get("properties")
    if not isinstance(properties, Mapping) and isinstance(issued, Mapping):
        properties = issued
    email_otp = str(properties.get("email_otp") if isinstance(properties, Mapping) else "")
    if not _OTP_RE.fullmatch(email_otp):
        raise BrokerError("issuer_response_invalid", status=503)
    action_link = str(properties.get("action_link") if isinstance(properties, Mapping) else "")
    action = urlsplit(action_link)
    action_query = parse_qs(action.query, keep_blank_values=True)
    issued_redirect = str(properties.get("redirect_to") if isinstance(properties, Mapping) else "")
    expected_auth_origin = urlsplit(policy.supabase_url)
    if action.scheme != "https" or action.netloc != expected_auth_origin.netloc \
            or action.path != "/auth/v1/verify" or action.fragment \
            or issued_redirect != redirect or action_query.get("redirect_to") != [redirect]:
        raise BrokerError("issuer_response_invalid", status=503)
    _audit(audit_sink, policy, outcome="issued", claims=claims, persona=persona, redirect=redirect)
    return {
        "email_otp": email_otp,
        # This is returned only to the authenticated caller and is never
        # included in audit. Mobile adapters may open it in the platform
        # browser so Supabase completes the callback in that browser's storage.
        "action_link": action_link,
        "counters": {
            "admin_credential_count": 1, "product_otp_issue_count": 0,
            "external_mail_send_count": 0, "external_mail_receipt_count": 0,
        },
    }


def _header(headers: Mapping[str, Any], name: str) -> str:
    for key, value in headers.items():
        if str(key).lower() == name.lower():
            return str(value or "").strip()
    return ""


def _response(status: int, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {"statusCode": status, "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
            "isBase64Encoded": False, "body": json.dumps(dict(payload), separators=(",", ":"))}


def handler(event: Mapping[str, Any], _context: Any) -> dict[str, Any]:
    headers = event.get("headers") if isinstance(event, Mapping) else None
    authorization = _header(headers if isinstance(headers, Mapping) else {}, "authorization")
    scheme, separator, token = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        return _response(401, {"error": "unauthorized"})
    try:
        raw_value = event.get("body", "")
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(str(raw_value), validate=True)
        else:
            raw = str(raw_value).encode()
        if not raw or len(raw) > MAX_BODY_BYTES:
            raise BrokerError("request_invalid", status=400)
        request = json.loads(raw)
        result = process(request, token=token.strip())
        return _response(200, result)
    except (binascii.Error, json.JSONDecodeError, UnicodeError):
        return _response(400, {"error": "request_invalid"})
    except BrokerError as exc:
        public = "unauthorized" if exc.status in {401, 403} else exc.code
        return _response(exc.status, {"error": public})
