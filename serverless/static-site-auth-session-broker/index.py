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
import threading
import time
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
PLATFORMS = ("browser", "android", "ios")
PURPOSES = ("production_health", "release_qualification", "legacy_debug")
PURPOSE_PLATFORM_PERSONA_IDS = {
    "production_health": {
        "browser": "search-cached-browser",
        "android": "search-cached-android",
        "ios": "search-cached-ios",
    },
    "release_qualification": {
        "browser": "search-cold-browser",
    },
    # The legacy workflow shares a concurrency group with production health,
    # so these compatibility paths cannot overlap their cached personas.
    "legacy_debug": {
        "browser": "search-cached-browser",
        "android": "search-cached-android",
        "ios": "search-cached-ios",
    },
}

Transport = Callable[[str, str, Mapping[str, str], bytes | None, float], tuple[int, bytes]]
Verifier = Callable[[str, Mapping[str, str]], Mapping[str, Any]]
AuditSink = Callable[[Mapping[str, Any]], None]


class BrokerError(RuntimeError):
    def __init__(self, code: str, *, status: int = 403, claim: str | None = None):
        super().__init__(code)
        self.code = code
        self.status = status
        self.claim = claim

    def public_payload(self) -> dict[str, str]:
        code = "unauthorized" if self.status in {401, 403} else self.code
        payload = {"error": code}
        if self.claim:
            payload.update({
                "claim": self.claim,
                "product_health": "UNKNOWN",
                "execution_status": "BLOCKED",
                "failure_class": "UNKNOWN",
            })
        return payload


@dataclass(frozen=True)
class Persona:
    persona_id: str
    email: str


@dataclass(frozen=True)
class Policy:
    audience: str
    repositories: frozenset[str]
    refs: frozenset[str]
    workflow_refs: frozenset[str]
    environments: frozenset[str]
    events: frozenset[str]
    runs: frozenset[str]
    purpose_platform_personas: Mapping[tuple[str, str], Persona]
    redirects: tuple[str, ...]
    per_run_persona_limit: int
    audit_key: str
    supabase_url: str
    supabase_service_role_key: str


@dataclass(frozen=True)
class AuthorizedIssue:
    repository: str
    workflow_ref: str
    run_id: str
    run_attempt: int
    purpose: str
    platform: str
    persona: Persona
    redirect: str

    @property
    def identity(self) -> tuple[str, str, str, int, str, str]:
        # Repository/workflow/run fields come from verified GitHub OIDC. The
        # platform/persona pair comes from the server's closed purpose mapping;
        # no caller-supplied persona or mirrored OIDC identity is trusted.
        return (
            self.repository,
            self.workflow_ref,
            self.run_id,
            self.run_attempt,
            self.platform,
            self.persona.persona_id,
        )


@dataclass
class _IssueFlight:
    ready: threading.Event
    result: dict[str, Any] | None = None
    error: BrokerError | None = None


@dataclass
class _IssueReplay:
    result: dict[str, Any]
    expires_at: float
    remaining_replays: int = 1


_ISSUE_FLIGHTS: dict[tuple[str, str, str, int, str, str], _IssueFlight] = {}
_ISSUE_FLIGHTS_LOCK = threading.Lock()
_ISSUE_REPLAYS: dict[tuple[str, str, str, int, str, str], _IssueReplay] = {}
_ISSUE_REPLAY_TTL_SECONDS = 30.0
_ISSUE_REPLAY_MAX_ENTRIES = 12
_DURABLE_REPLAY_POLL_ATTEMPTS = 20
_DURABLE_REPLAY_POLL_SECONDS = 0.25


def reset_transient_issue_state_for_tests() -> None:
    with _ISSUE_FLIGHTS_LOCK:
        _ISSUE_FLIGHTS.clear()
        _ISSUE_REPLAYS.clear()



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
    required_personas = {
        persona_id
        for platform_map in PURPOSE_PLATFORM_PERSONA_IDS.values()
        for persona_id in platform_map.values()
    }
    if not required_personas.issubset(personas):
        raise BrokerError("platform_personas_invalid", status=500)
    purpose_platform_personas = {
        (purpose, platform): Persona(persona_id=persona_id, email=personas[persona_id])
        for purpose, platform_map in PURPOSE_PLATFORM_PERSONA_IDS.items()
        for platform, persona_id in platform_map.items()
    }
    platform_emails = [personas[persona_id] for persona_id in sorted(required_personas)]
    if len(set(platform_emails)) != len(platform_emails):
        raise BrokerError("platform_personas_not_unique", status=500)

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
        purpose_platform_personas=purpose_platform_personas,
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


def authorize_request(
    request: Mapping[str, Any], claims: Mapping[str, Any], policy: Policy,
) -> AuthorizedIssue:
    if _claim(claims, "iss") != GITHUB_ISSUER or _claim(claims, "aud") != policy.audience:
        raise BrokerError("oidc_identity_invalid")
    repository = _claim(claims, "repository")
    ref = _claim(claims, "ref")
    workflow_ref = _claim(claims, "workflow_ref")
    environment = _claim(claims, "environment")
    event_name = _claim(claims, "event_name")
    run_id = _claim(claims, "run_id")
    run_attempt = _claim(claims, "run_attempt")
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
    if not _RUN_RE.fullmatch(run_attempt):
        raise BrokerError("run_attempt_invalid")

    # Identity fields are forbidden in the body. They must come exclusively
    # from the verified OIDC claims above; accepting mirrors would create a
    # second, spoofable source of truth.
    if set(request) != {"purpose", "platform", "redirect_to"}:
        raise BrokerError("request_identity_spoofed")
    purpose = str(request.get("purpose") or "").strip()
    if purpose not in PURPOSES:
        raise BrokerError("purpose_not_allowed")
    platform = str(request.get("platform") or "").strip()
    if platform not in PLATFORMS:
        raise BrokerError("platform_not_allowed")
    persona = policy.purpose_platform_personas.get((purpose, platform))
    if persona is None:
        raise BrokerError("purpose_platform_not_allowed")
    redirect = str(request.get("redirect_to") or "").strip()
    if not _redirect_allowed(redirect, policy.redirects):
        raise BrokerError("redirect_not_allowed")
    return AuthorizedIssue(
        repository=repository,
        workflow_ref=workflow_ref,
        run_id=run_id,
        run_attempt=int(run_attempt),
        purpose=purpose,
        platform=platform,
        persona=persona,
        redirect=redirect,
    )


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


def _credential_fernet(policy: Policy) -> Any:
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:  # pragma: no cover - deployment configuration
        raise BrokerError("credential_cipher_unavailable", status=500) from exc
    key = hashlib.sha256(
        b"kenigevents-search-broker-credential-v1\0" + policy.audit_key.encode()
    ).digest()
    return Fernet(base64.urlsafe_b64encode(key))


def _seal_issued_result(result: Mapping[str, Any], policy: Policy) -> str:
    raw = json.dumps(dict(result), sort_keys=True, separators=(",", ":")).encode()
    return _credential_fernet(policy).encrypt(raw).decode()


def _unseal_issued_result(ciphertext: str, policy: Policy) -> dict[str, Any]:
    try:
        raw = _credential_fernet(policy).decrypt(ciphertext.encode(), ttl=180)
        result = json.loads(raw)
    except Exception as exc:
        raise BrokerError("credential_replay_invalid", status=503) from exc
    if not isinstance(result, Mapping):
        raise BrokerError("credential_replay_invalid", status=503)
    email_otp = str(result.get("email_otp") or "")
    action_link = str(result.get("action_link") or "")
    counters = result.get("counters")
    if result.get("claim") != "new" or not _OTP_RE.fullmatch(email_otp) \
            or not action_link or not isinstance(counters, Mapping):
        raise BrokerError("credential_replay_invalid", status=503)
    return dict(result)


def _audit_hash(value: str, key: str) -> str:
    return hmac.new(key.encode(), value.encode(), hashlib.sha256).hexdigest()[:24]


def _audit(
    sink: AuditSink, policy: Policy, *, outcome: str, claims: Mapping[str, Any],
    persona: str, purpose: str, platform: str, redirect: str,
) -> None:
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
        "purpose": purpose,
        "platform": platform,
        "redirect_hash": _audit_hash(redirect, policy.audit_key),
    })


def _stdout_audit(row: Mapping[str, Any]) -> None:
    print(json.dumps(dict(row), ensure_ascii=True, sort_keys=True, separators=(",", ":")), flush=True)


def _issue_authorized(
    authorized: AuthorizedIssue, *, claims: Mapping[str, Any], policy: Policy,
    transport: Transport, audit_sink: AuditSink,
) -> dict[str, Any]:
    claim_payload = {
        "p_run_id": authorized.run_id,
        "p_run_attempt": authorized.run_attempt,
        "p_platform": authorized.platform,
        "p_persona_id": authorized.persona.persona_id,
        "p_repository": authorized.repository,
        "p_workflow_ref": authorized.workflow_ref,
        "p_limit": policy.per_run_persona_limit,
    }
    admission = ""
    replay_ciphertext = ""
    for poll in range(_DURABLE_REPLAY_POLL_ATTEMPTS):
        raw_admission = _json_call(
            "/rest/v1/rpc/claim_static_site_auth_session_issue_v2",
            claim_payload, policy=policy, transport=transport,
        )
        if isinstance(raw_admission, Mapping):
            admission = str(raw_admission.get("claim") or raw_admission.get("outcome") or "").strip().lower()
            replay_ciphertext = str(raw_admission.get("credential_ciphertext") or "")
        else:
            admission = str(raw_admission or "").strip().lower()
        if admission != "duplicate_inflight" or poll + 1 >= _DURABLE_REPLAY_POLL_ATTEMPTS:
            break
        time.sleep(_DURABLE_REPLAY_POLL_SECONDS)

    if admission == "replay":
        result = _unseal_issued_result(replay_ciphertext, policy)
        if result.get("platform") != authorized.platform:
            raise BrokerError("credential_replay_invalid", status=503)
        _audit(
            audit_sink, policy, outcome="replayed_durable", claims=claims,
            persona=authorized.persona.persona_id, purpose=authorized.purpose,
            platform=authorized.platform,
            redirect=authorized.redirect,
        )
        return result
    if admission not in {"new", "duplicate_inflight", "duplicate_consumed", "persona_busy"}:
        raise BrokerError("admission_response_invalid", status=503)
    if admission != "new":
        _audit(
            audit_sink, policy, outcome=admission, claims=claims,
            persona=authorized.persona.persona_id, purpose=authorized.purpose,
            platform=authorized.platform,
            redirect=authorized.redirect,
        )
        status = 423 if admission == "persona_busy" else 409
        raise BrokerError(admission, status=status, claim=admission)

    # This calls the raw GoTrue REST endpoint rather than supabase-js. Its
    # request/response contract is snake_case and flat.
    issued = _json_call("/auth/v1/admin/generate_link", {
        "type": "magiclink", "email": authorized.persona.email,
        "redirect_to": authorized.redirect,
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
            or issued_redirect != authorized.redirect \
            or action_query.get("redirect_to") != [authorized.redirect]:
        raise BrokerError("issuer_response_invalid", status=503)
    result = {
        "claim": "new",
        "platform": authorized.platform,
        "email_otp": email_otp,
        "action_link": action_link,
        "counters": {
            "admin_credential_count": 1, "product_otp_issue_count": 0,
            "external_mail_send_count": 0, "external_mail_receipt_count": 0,
        },
    }
    completed = _json_call(
        "/rest/v1/rpc/complete_static_site_auth_session_issue_v2",
        {**{key: value for key, value in claim_payload.items() if key != "p_limit"},
         "p_credential_ciphertext": _seal_issued_result(result, policy)},
        policy=policy, transport=transport,
    )
    if completed is not True:
        raise BrokerError("credential_replay_completion_rejected", status=503)
    _audit(
        audit_sink, policy, outcome="issued", claims=claims,
        persona=authorized.persona.persona_id, purpose=authorized.purpose,
        platform=authorized.platform,
        redirect=authorized.redirect,
    )
    return result


def _coalesced_issue(
    authorized: AuthorizedIssue, *, claims: Mapping[str, Any], policy: Policy,
    transport: Transport, audit_sink: AuditSink,
) -> dict[str, Any]:
    """Coalesce overlap and allow one bounded lost-response replay.

    One replay exists in broker process memory for at most 30 seconds. The SQL
    claim ledger also holds a short-lived encrypted credential so a retry after
    process loss can return the same issue instead of generating another one.
    Plaintext credentials are never written to a file, log, or job artifact.
    """
    now = time.monotonic()
    replayed: dict[str, Any] | None = None
    with _ISSUE_FLIGHTS_LOCK:
        for identity, replay in list(_ISSUE_REPLAYS.items()):
            if replay.expires_at <= now or replay.remaining_replays < 1:
                del _ISSUE_REPLAYS[identity]
        replay = _ISSUE_REPLAYS.get(authorized.identity)
        if replay is not None:
            replayed = dict(replay.result)
            replay.remaining_replays -= 1
            if replay.remaining_replays < 1:
                del _ISSUE_REPLAYS[authorized.identity]
        flight = _ISSUE_FLIGHTS.get(authorized.identity)
        owner = flight is None
        if replayed is None and owner:
            flight = _IssueFlight(ready=threading.Event())
            _ISSUE_FLIGHTS[authorized.identity] = flight
    if replayed is not None:
        _audit(
            audit_sink, policy, outcome="replayed", claims=claims,
            persona=authorized.persona.persona_id, purpose=authorized.purpose,
            platform=authorized.platform,
            redirect=authorized.redirect,
        )
        return replayed
    assert flight is not None
    if not owner:
        flight.ready.wait(timeout=15.0)
        if not flight.ready.is_set():
            raise BrokerError("coalesced_issue_timeout", status=503, claim="duplicate_inflight")
        if flight.error:
            raise BrokerError(flight.error.code, status=flight.error.status, claim=flight.error.claim)
        if not flight.result:
            raise BrokerError("coalesced_issue_invalid", status=503)
        return dict(flight.result)

    try:
        flight.result = _issue_authorized(
            authorized, claims=claims, policy=policy,
            transport=transport, audit_sink=audit_sink,
        )
        with _ISSUE_FLIGHTS_LOCK:
            if len(_ISSUE_REPLAYS) >= _ISSUE_REPLAY_MAX_ENTRIES:
                oldest = min(_ISSUE_REPLAYS, key=lambda key: _ISSUE_REPLAYS[key].expires_at)
                del _ISSUE_REPLAYS[oldest]
            _ISSUE_REPLAYS[authorized.identity] = _IssueReplay(
                result=dict(flight.result), expires_at=time.monotonic() + _ISSUE_REPLAY_TTL_SECONDS,
            )
        return dict(flight.result)
    except BrokerError as exc:
        flight.error = exc
        raise
    finally:
        flight.ready.set()
        with _ISSUE_FLIGHTS_LOCK:
            if _ISSUE_FLIGHTS.get(authorized.identity) is flight:
                del _ISSUE_FLIGHTS[authorized.identity]


def process(
    request: Mapping[str, Any], *, token: str, env: Mapping[str, str] = os.environ,
    transport: Transport = urllib_transport, verifier: Verifier = verify_github_oidc,
    audit_sink: AuditSink = _stdout_audit,
) -> dict[str, Any]:
    if not isinstance(request, Mapping):
        raise BrokerError("request_invalid", status=400)
    policy = policy_from_env(env)
    claims = verifier(token, env)
    authorized = authorize_request(request, claims, policy)
    return _coalesced_issue(
        authorized, claims=claims, policy=policy,
        transport=transport, audit_sink=audit_sink,
    )


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
        return _response(exc.status, exc.public_payload())
