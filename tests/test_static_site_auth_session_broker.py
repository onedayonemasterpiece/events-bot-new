from __future__ import annotations

import json
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BROKER_PATH = ROOT / "serverless" / "static-site-auth-session-broker" / "index.py"
SPEC = importlib.util.spec_from_file_location("static_site_auth_session_broker_index", BROKER_PATH)
assert SPEC and SPEC.loader
broker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = broker
SPEC.loader.exec_module(broker)


def environment(**overrides: str) -> dict[str, str]:
    values = {
        "AUTH_SESSION_BROKER_OIDC_AUDIENCE": "https://auth-broker.kenigevents.invalid",
        "AUTH_SESSION_BROKER_ALLOWED_REPOSITORIES": "onedayonemasterpiece/events-bot-new",
        "AUTH_SESSION_BROKER_ALLOWED_REFS": "refs/heads/main",
        "AUTH_SESSION_BROKER_ALLOWED_WORKFLOW_REFS": (
            "onedayonemasterpiece/events-bot-new/.github/workflows/static-site-search.yml@refs/heads/main"
        ),
        "AUTH_SESSION_BROKER_ALLOWED_ENVIRONMENTS": "static-site-search",
        "AUTH_SESSION_BROKER_ALLOWED_EVENTS": "schedule,workflow_dispatch",
        "AUTH_SESSION_BROKER_ALLOWED_RUNS": "github-claim-bound",
        "AUTH_SESSION_BROKER_PERSONAS_JSON": json.dumps({
            "search-cached": "search-cached@example.invalid",
            "search-cold": "search-cold@example.invalid",
        }),
        "AUTH_SESSION_BROKER_ALLOWED_REDIRECTS": (
            "https://kenigevents.ru/poisk/\n"
            "https://kenigevents.ru/_review/{secret-candidate}/poisk/"
        ),
        "AUTH_SESSION_BROKER_PER_RUN_PERSONA_LIMIT": "1",
        "AUTH_SESSION_BROKER_AUDIT_HMAC_KEY": "unit-test-audit-key-with-enough-entropy",
        "PERSONALIZATION_SUPABASE_URL": "https://project.supabase.co",
        "PERSONALIZATION_SUPABASE_SECRET_KEY": "sb_secret_unit_test",
    }
    values.update(overrides)
    return values


def claims(**overrides: str) -> dict[str, str]:
    values = {
        "iss": "https://token.actions.githubusercontent.com",
        "aud": "https://auth-broker.kenigevents.invalid",
        "sub": "repo:onedayonemasterpiece/events-bot-new:environment:static-site-search",
        "repository": "onedayonemasterpiece/events-bot-new",
        "ref": "refs/heads/main",
        "workflow_ref": (
            "onedayonemasterpiece/events-bot-new/.github/workflows/static-site-search.yml@refs/heads/main"
        ),
        "environment": "static-site-search",
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
    def __init__(self, *, admitted: bool = True):
        self.admitted = admitted
        self.calls: list[tuple[str, str, dict[str, str], bytes | None, float]] = []

    def __call__(self, method, url, headers, body, timeout):
        self.calls.append((method, url, dict(headers), body, timeout))
        if url.endswith("/rpc/claim_static_site_auth_session_issue_v1"):
            return 200, json.dumps({"admitted": self.admitted}).encode()
        if url.endswith("/auth/v1/admin/generate_link"):
            return 200, json.dumps({
                "properties": {
                    "email_otp": "456789",
                    "action_link": "https://project.supabase.co/auth/v1/verify?token=secret",
                }
            }).encode()
        raise AssertionError(url)


def test_authorized_github_run_claims_once_and_returns_no_mail_issuer_contract():
    transport = Transport()
    logs: list[dict] = []
    result = broker.process(
        {"persona_id": "search-cached", "redirect_to": "https://kenigevents.ru/poisk/", "run_id": "123456789"},
        token="opaque-signed-jwt", env=environment(), transport=transport,
        verifier=lambda _token, _env: claims(), audit_sink=logs.append,
    )
    assert result == {
        "email_otp": "456789",
        "action_link": "https://project.supabase.co/auth/v1/verify?token=secret",
        "counters": {
            "admin_credential_count": 1,
            "product_otp_issue_count": 0,
            "external_mail_send_count": 0,
            "external_mail_receipt_count": 0,
        },
    }
    assert [call[1].rsplit("/", 1)[-1] for call in transport.calls] == [
        "claim_static_site_auth_session_issue_v1", "generate_link"
    ]
    ledger = json.loads(transport.calls[0][3])
    assert ledger["p_run_id"] == "123456789"
    assert ledger["p_persona_id"] == "search-cached"
    assert ledger["p_limit"] == 1
    assert logs[-1]["outcome"] == "issued"
    serialized = json.dumps(logs)
    assert "example.invalid" not in serialized
    assert "opaque-signed-jwt" not in serialized
    assert "456789" not in serialized
    assert "123456789" not in serialized


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
            {"persona_id": "search-cached", "redirect_to": "https://kenigevents.ru/poisk/",
             "run_id": "not-allowlisted" if claim_name == "run_id" else "123456789"},
            token="jwt", env=environment(), transport=transport,
            verifier=lambda _token, _env: claims(**{claim_name: claim_value}), audit_sink=lambda _row: None,
        )
    assert transport.calls == []


def test_run_is_bound_to_verified_claim_and_persona_and_redirect_are_allowlisted():
    for request, code in [
        ({"persona_id": "unknown", "redirect_to": "https://kenigevents.ru/poisk/", "run_id": "123456789"},
         "persona_not_allowed"),
        ({"persona_id": "search-cached", "redirect_to": "https://attacker.invalid/poisk/", "run_id": "123456789"},
         "redirect_not_allowed"),
        ({"persona_id": "search-cached", "redirect_to": "https://kenigevents.ru/poisk/", "run_id": "987654321"},
         "run_claim_mismatch"),
    ]:
        with pytest.raises(broker.BrokerError, match=code):
            broker.process(request, token="jwt", env=environment(), transport=Transport(),
                           verifier=lambda _token, _env: claims(), audit_sink=lambda _row: None)

    token = "Z" * 43
    result = broker.process(
        {"persona_id": "search-cold", "redirect_to": f"https://kenigevents.ru/_review/{token}/poisk/",
         "run_id": "123456789"},
        token="jwt", env=environment(), transport=Transport(), verifier=lambda _token, _env: claims(),
        audit_sink=lambda _row: None,
    )
    assert result["email_otp"] == "456789"


def test_per_run_persona_ledger_rejection_never_generates_another_credential():
    transport = Transport(admitted=False)
    with pytest.raises(broker.BrokerError, match="issuance_limit_reached"):
        broker.process(
            {"persona_id": "search-cached", "redirect_to": "https://kenigevents.ru/poisk/", "run_id": "123456789"},
            token="jwt", env=environment(), transport=transport, verifier=lambda _token, _env: claims(),
            audit_sink=lambda _row: None,
        )
    assert len(transport.calls) == 1
    assert transport.calls[0][1].endswith("/rpc/claim_static_site_auth_session_issue_v1")


def test_exact_run_allowlist_is_supported_but_wildcards_are_forbidden():
    broker.process(
        {"persona_id": "search-cached", "redirect_to": "https://kenigevents.ru/poisk/", "run_id": "123456789"},
        token="jwt", env=environment(AUTH_SESSION_BROKER_ALLOWED_RUNS="123456789"), transport=Transport(),
        verifier=lambda _token, _env: claims(), audit_sink=lambda _row: None,
    )
    with pytest.raises(broker.BrokerError, match="allowed_runs_invalid"):
        broker.policy_from_env(environment(AUTH_SESSION_BROKER_ALLOWED_RUNS="*"))


def test_http_handler_requires_bearer_token_and_never_echoes_internal_errors():
    response = broker.handler({"headers": {}, "body": "{}", "isBase64Encoded": False}, None)
    assert response["statusCode"] == 401
    assert json.loads(response["body"]) == {"error": "unauthorized"}
