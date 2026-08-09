from __future__ import annotations

import base64
import importlib.util
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

import pytest

ROOT = Path(__file__).resolve().parents[1]
BROKER_PATH = ROOT / "serverless" / "static-site-auth-session-broker" / "index.py"
SPEC = importlib.util.spec_from_file_location("static_site_auth_session_broker_index", BROKER_PATH)
assert SPEC and SPEC.loader
broker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = broker
SPEC.loader.exec_module(broker)


def _service_role_jwt(role: str = "service_role") -> str:
    encode = lambda value: base64.urlsafe_b64encode(
        json.dumps(value, separators=(",", ":")).encode()
    ).decode().rstrip("=")
    return f"{encode({'alg': 'HS256', 'typ': 'JWT'})}.{encode({'role': role})}.signature"


def environment(**overrides: str) -> dict[str, str]:
    values = {
        "AUTH_SESSION_BROKER_OIDC_AUDIENCE": "https://auth-broker.kenigevents.invalid",
        "AUTH_SESSION_BROKER_ALLOWED_REPOSITORIES": "onedayonemasterpiece/events-bot-new",
        "AUTH_SESSION_BROKER_ALLOWED_REFS": "refs/heads/main",
        "AUTH_SESSION_BROKER_ALLOWED_WORKFLOW_REFS": (
            "onedayonemasterpiece/events-bot-new/.github/workflows/static-site-search-canary.yml@refs/heads/main"
        ),
        "AUTH_SESSION_BROKER_ALLOWED_ENVIRONMENTS": "search-e2e",
        "AUTH_SESSION_BROKER_ALLOWED_EVENTS": "schedule,workflow_dispatch,repository_dispatch",
        "AUTH_SESSION_BROKER_ALLOWED_RUNS": "github-claim-bound",
        "AUTH_SESSION_BROKER_PERSONAS_JSON": json.dumps({
            "search-cached-browser": "search-cached@example.invalid",
            "search-cold-browser": "search-cold@example.invalid",
            "search-degraded-browser": "search-degraded@example.invalid",
            "search-cached-android": "search-android@example.invalid",
            "search-cached-ios": "search-ios@example.invalid",
        }),
        "AUTH_SESSION_BROKER_ALLOWED_REDIRECTS": (
            "https://kenigevents.ru/poisk/\n"
            "https://kenigevents.ru/_review/{secret-candidate}/poisk/"
        ),
        "AUTH_SESSION_BROKER_PER_RUN_PERSONA_LIMIT": "1",
        "AUTH_SESSION_BROKER_AUDIT_HMAC_KEY": "unit-test-audit-key-with-enough-entropy",
        "PERSONALIZATION_SUPABASE_URL": "https://project.supabase.co",
        "AUTH_SESSION_BROKER_SUPABASE_SERVICE_ROLE_KEY": _service_role_jwt(),
    }
    values.update(overrides)
    return values


def claims(**overrides: str) -> dict[str, str]:
    values = {
        "iss": "https://token.actions.githubusercontent.com",
        "aud": "https://auth-broker.kenigevents.invalid",
        "sub": "repo:onedayonemasterpiece/events-bot-new:environment:search-e2e",
        "repository": "onedayonemasterpiece/events-bot-new",
        "ref": "refs/heads/main",
        "workflow_ref": (
            "onedayonemasterpiece/events-bot-new/.github/workflows/static-site-search-canary.yml@refs/heads/main"
        ),
        "environment": "search-e2e",
        "event_name": "schedule",
        "run_id": "123456789",
        "run_attempt": "1",
        "repository_id": "1234",
        "repository_owner_id": "5678",
        "sha": "a" * 40,
        "jti": "opaque-jti",
    }
    values.update(overrides)
    return values


class Transport:
    def __init__(self, *, claim: str = "new"):
        self.claim = claim
        self.calls: list[tuple[str, str, dict[str, str], bytes | None, float]] = []

    def __call__(self, method, url, headers, body, timeout):
        self.calls.append((method, url, dict(headers), body, timeout))
        if url.endswith("/rpc/claim_static_site_auth_session_issue_v2"):
            return 200, json.dumps(self.claim).encode()
        if url.endswith("/auth/v1/admin/generate_link"):
            redirect = json.loads(body)["redirect_to"]
            return 200, json.dumps({
                "email_otp": "456789",
                "action_link": (
                    "https://project.supabase.co/auth/v1/verify?token=secret&"
                    f"redirect_to={quote(redirect, safe='')}"
                ),
                "redirect_to": redirect,
            }).encode()
        raise AssertionError(url)


def test_authorized_github_run_claims_once_and_returns_no_mail_issuer_contract():
    transport = Transport()
    logs: list[dict] = []
    result = broker.process(
        {"platform": "browser", "redirect_to": "https://kenigevents.ru/poisk/"},
        token="opaque-signed-jwt", env=environment(), transport=transport,
        verifier=lambda _token, _env: claims(), audit_sink=logs.append,
    )
    assert result == {
        "claim": "new",
        "platform": "browser",
        "email_otp": "456789",
        "action_link": (
            "https://project.supabase.co/auth/v1/verify?token=secret&"
            "redirect_to=https%3A%2F%2Fkenigevents.ru%2Fpoisk%2F"
        ),
        "counters": {
            "admin_credential_count": 1,
            "product_otp_issue_count": 0,
            "external_mail_send_count": 0,
            "external_mail_receipt_count": 0,
        },
    }
    assert [call[1].rsplit("/", 1)[-1] for call in transport.calls] == [
        "claim_static_site_auth_session_issue_v2", "generate_link"
    ]
    expected_key = environment()["AUTH_SESSION_BROKER_SUPABASE_SERVICE_ROLE_KEY"]
    assert transport.calls[0][2]["apikey"] == expected_key
    assert transport.calls[0][2]["authorization"] == f"Bearer {expected_key}"
    ledger = json.loads(transport.calls[0][3])
    assert ledger["p_run_id"] == "123456789"
    assert ledger["p_run_attempt"] == 1
    assert ledger["p_repository"] == "onedayonemasterpiece/events-bot-new"
    assert ledger["p_workflow_ref"] == claims()["workflow_ref"]
    assert ledger["p_platform"] == "browser"
    assert ledger["p_persona_id"] == "search-cached-browser"
    assert ledger["p_limit"] == 1
    raw_generate_link = json.loads(transport.calls[1][3])
    assert raw_generate_link == {
        "type": "magiclink",
        "email": "search-cached@example.invalid",
        "redirect_to": "https://kenigevents.ru/poisk/",
    }
    assert logs[-1]["outcome"] == "issued"
    serialized = json.dumps(logs)
    assert "example.invalid" not in serialized
    assert "opaque-signed-jwt" not in serialized
    assert "456789" not in serialized
    assert "123456789" not in serialized
    assert "token=secret" not in serialized
    assert expected_key not in serialized


@pytest.mark.parametrize(("platform", "persona_id", "email"), [
    ("browser", "search-cached-browser", "search-cached@example.invalid"),
    ("android", "search-cached-android", "search-android@example.invalid"),
    ("ios", "search-cached-ios", "search-ios@example.invalid"),
])
def test_each_platform_uses_only_its_dedicated_server_side_persona(platform, persona_id, email):
    transport = Transport()
    result = broker.process(
        {"platform": platform, "redirect_to": "https://kenigevents.ru/poisk/"},
        token="jwt", env=environment(), transport=transport,
        verifier=lambda _token, _env: claims(), audit_sink=lambda _row: None,
    )
    assert result["platform"] == platform
    assert json.loads(transport.calls[0][3])["p_persona_id"] == persona_id
    assert json.loads(transport.calls[1][3])["email"] == email


def test_broker_accepts_sdk_wrapped_generate_link_shape_for_issuer_compatibility():
    class WrappedTransport(Transport):
        def __call__(self, method, url, headers, body, timeout):
            if url.endswith("/auth/v1/admin/generate_link"):
                self.calls.append((method, url, dict(headers), body, timeout))
                return 200, json.dumps({
                    "data": {
                        "properties": {
                            "email_otp": "456789",
                            "action_link": (
                                "https://project.supabase.co/auth/v1/verify?token=secret&"
                                "redirect_to=https%3A%2F%2Fkenigevents.ru%2Fpoisk%2F"
                            ),
                            "redirect_to": "https://kenigevents.ru/poisk/",
                        }
                    }
                }).encode()
            return super().__call__(method, url, headers, body, timeout)

    result = broker.process(
        {"platform": "browser", "redirect_to": "https://kenigevents.ru/poisk/"},
        token="jwt", env=environment(), transport=WrappedTransport(),
        verifier=lambda _token, _env: claims(), audit_sink=lambda _row: None,
    )
    assert result["email_otp"] == "456789"


def test_broker_rejects_generate_link_redirect_drift():
    class DriftedRedirectTransport(Transport):
        def __call__(self, method, url, headers, body, timeout):
            if url.endswith("/auth/v1/admin/generate_link"):
                self.calls.append((method, url, dict(headers), body, timeout))
                return 200, json.dumps({
                    "email_otp": "456789",
                    "action_link": (
                        "https://project.supabase.co/auth/v1/verify?token=secret&"
                        "redirect_to=https%3A%2F%2Fkenigevents.ru%2F"
                    ),
                    "redirect_to": "https://kenigevents.ru/",
                }).encode()
            return super().__call__(method, url, headers, body, timeout)

    with pytest.raises(broker.BrokerError, match="issuer_response_invalid"):
        broker.process(
            {"platform": "browser", "redirect_to": "https://kenigevents.ru/poisk/"},
            token="jwt", env=environment(), transport=DriftedRedirectTransport(),
            verifier=lambda _token, _env: claims(), audit_sink=lambda _row: None,
        )


@pytest.mark.parametrize(("claim_name", "claim_value", "code"), [
    ("repository", "attacker/repo", "repository_not_allowed"),
    ("ref", "refs/heads/feature", "ref_not_allowed"),
    ("workflow_ref", "attacker/repo/.github/workflows/pwn.yml@refs/heads/main", "workflow_not_allowed"),
    ("environment", "unprotected", "environment_not_allowed"),
    ("event_name", "pull_request", "event_not_allowed"),
    ("run_id", "not-allowlisted", "run_invalid"),
])
def test_every_github_identity_dimension_is_fail_closed(claim_name, claim_value, code):
    transport = Transport()
    with pytest.raises(broker.BrokerError, match=code):
        broker.process(
            {"platform": "browser", "redirect_to": "https://kenigevents.ru/poisk/"},
            token="jwt", env=environment(), transport=transport,
            verifier=lambda _token, _env: claims(**{claim_name: claim_value}), audit_sink=lambda _row: None,
        )
    assert transport.calls == []


def test_platform_is_closed_and_spoofable_identity_fields_are_rejected():
    for request, code in [
        ({"platform": "desktop", "redirect_to": "https://kenigevents.ru/poisk/"},
         "platform_not_allowed"),
        ({"platform": "browser", "redirect_to": "https://attacker.invalid/poisk/"},
         "redirect_not_allowed"),
        ({"platform": "browser", "persona_id": "search-cached-android",
          "redirect_to": "https://kenigevents.ru/poisk/"}, "request_identity_spoofed"),
        ({"platform": "browser", "run_id": "987654321",
          "redirect_to": "https://kenigevents.ru/poisk/"}, "request_identity_spoofed"),
        ({"platform": "browser", "repository": "attacker/repo",
          "redirect_to": "https://kenigevents.ru/poisk/"}, "request_identity_spoofed"),
    ]:
        with pytest.raises(broker.BrokerError, match=code):
            broker.process(request, token="jwt", env=environment(), transport=Transport(),
                           verifier=lambda _token, _env: claims(), audit_sink=lambda _row: None)

    token = "Z" * 43
    result = broker.process(
        {"platform": "browser", "redirect_to": f"https://kenigevents.ru/_review/{token}/poisk/"},
        token="jwt", env=environment(), transport=Transport(), verifier=lambda _token, _env: claims(),
        audit_sink=lambda _row: None,
    )
    assert result["email_otp"] == "456789"


@pytest.mark.parametrize(("claim", "status"), [
    ("duplicate_inflight", 409),
    ("persona_busy", 423),
])
def test_typed_ledger_rejection_never_generates_another_credential(claim, status):
    transport = Transport(claim=claim)
    with pytest.raises(broker.BrokerError, match=claim) as caught:
        broker.process(
            {"platform": "browser", "redirect_to": "https://kenigevents.ru/poisk/"},
            token="jwt", env=environment(), transport=transport, verifier=lambda _token, _env: claims(),
            audit_sink=lambda _row: None,
        )
    assert caught.value.status == status
    assert caught.value.public_payload() == {
        "error": claim,
        "claim": claim,
        "product_health": "UNKNOWN",
        "execution_status": "BLOCKED",
        "failure_class": "UNKNOWN",
    }
    assert len(transport.calls) == 1
    assert transport.calls[0][1].endswith("/rpc/claim_static_site_auth_session_issue_v2")


def test_exact_run_allowlist_is_supported_but_wildcards_are_forbidden():
    broker.process(
        {"platform": "browser", "redirect_to": "https://kenigevents.ru/poisk/"},
        token="jwt", env=environment(AUTH_SESSION_BROKER_ALLOWED_RUNS="123456789"), transport=Transport(),
        verifier=lambda _token, _env: claims(), audit_sink=lambda _row: None,
    )
    with pytest.raises(broker.BrokerError, match="allowed_runs_invalid"):
        broker.policy_from_env(environment(AUTH_SESSION_BROKER_ALLOWED_RUNS="*"))


@pytest.mark.parametrize("key", [
    "sb_secret_unit_test",
    "not-a-jwt",
    _service_role_jwt("anon"),
])
def test_broker_requires_dedicated_legacy_service_role_jwt(key):
    with pytest.raises(broker.BrokerError, match="supabase_service_role_key_invalid"):
        broker.policy_from_env(environment(AUTH_SESSION_BROKER_SUPABASE_SERVICE_ROLE_KEY=key))


def test_platform_personas_are_server_derived_and_emails_must_be_unique():
    policy = broker.policy_from_env(environment())
    assert {
        platform: persona.persona_id
        for platform, persona in policy.platform_personas.items()
    } == {
        "browser": "search-cached-browser",
        "android": "search-cached-android",
        "ios": "search-cached-ios",
    }
    configured = json.loads(environment()["AUTH_SESSION_BROKER_PERSONAS_JSON"])
    configured["search-cached-ios"] = configured["search-cached-android"]
    with pytest.raises(broker.BrokerError, match="platform_personas_not_unique"):
        broker.policy_from_env(environment(
            AUTH_SESSION_BROKER_PERSONAS_JSON=json.dumps(configured),
        ))
    del configured["search-cached-ios"]
    with pytest.raises(broker.BrokerError, match="platform_personas_invalid"):
        broker.policy_from_env(environment(
            AUTH_SESSION_BROKER_PERSONAS_JSON=json.dumps(configured),
        ))


def test_identical_concurrent_issue_is_coalesced_to_one_claim_and_generate_link():
    transport = Transport()
    verifier_count = 0
    verifier_lock = threading.Lock()
    both_verified = threading.Event()

    def verifier(_token, _env):
        nonlocal verifier_count
        with verifier_lock:
            verifier_count += 1
            if verifier_count == 2:
                both_verified.set()
        return claims()

    original_transport = transport.__call__

    def delayed_transport(method, url, headers, body, timeout):
        if url.endswith("/rpc/claim_static_site_auth_session_issue_v2"):
            assert both_verified.wait(timeout=2)
        return original_transport(method, url, headers, body, timeout)

    request = {"platform": "browser", "redirect_to": "https://kenigevents.ru/poisk/"}
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            broker.process, request, token="jwt-a", env=environment(),
            transport=delayed_transport, verifier=verifier, audit_sink=lambda _row: None,
        )
        second = pool.submit(
            broker.process, request, token="jwt-b", env=environment(),
            transport=delayed_transport, verifier=verifier, audit_sink=lambda _row: None,
        )
        assert first.result(timeout=3) == second.result(timeout=3)
    assert sum(call[1].endswith("/rpc/claim_static_site_auth_session_issue_v2") for call in transport.calls) == 1
    assert sum(call[1].endswith("/auth/v1/admin/generate_link") for call in transport.calls) == 1


def test_http_handler_requires_bearer_token_and_never_echoes_internal_errors():
    response = broker.handler({"headers": {}, "body": "{}", "isBase64Encoded": False}, None)
    assert response["statusCode"] == 401
    assert json.loads(response["body"]) == {"error": "unauthorized"}
