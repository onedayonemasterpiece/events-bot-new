import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra" / "yandex" / "supabase-relay"


def _spec() -> str:
    return (INFRA / "openapi.yaml").read_text(encoding="utf-8")


def _declared_paths(spec: str) -> set[str]:
    return set(re.findall(r"^  (/[^:]+):$", spec, flags=re.MULTILINE))


def _methods_for_path(spec: str, path: str) -> set[str]:
    block = spec.split(f"  {path}:\n", 1)[1]
    next_path = re.search(r"^  /[^:]+:$", block, flags=re.MULTILINE)
    if next_path:
        block = block[: next_path.start()]
    return set(re.findall(r"^    (get|head|post|put|patch|delete):$", block, flags=re.MULTILINE))


def test_relay_is_stateless_fixed_upstream_and_exact_origin() -> None:
    desired = json.loads((INFRA / "desired-state.json").read_text(encoding="utf-8"))
    spec = _spec()

    assert desired["schema"] == "kenigevents.supabase_relay.yc_desired_state.v2"
    assert desired["api_gateway"]["cors_origin"] == "https://kenigevents.ru"
    assert desired["api_gateway"]["logging"] is False
    assert desired["api_gateway"]["stateful"] is False
    assert desired["api_gateway"]["service_account_id"] is None
    assert desired["security"]["service_role_key"] is False
    assert desired["security"]["forwards_cookie"] is False
    assert desired["security"]["forwards_client_host"] is False
    assert desired["security"]["forwards_client_ip_headers"] is False
    assert desired["security"]["forwards_browser_origin"] is False
    assert "origin: https://kenigevents.ru" in spec
    assert "origin: '*'" not in spec
    assert spec.count("type: http") == 31
    assert "type: cloud_functions" not in spec
    assert "type: cloud_ydb" not in spec
    assert "serviceAccount" not in spec
    assert "service_account" not in spec


def test_relay_has_only_explicit_product_routes_and_methods() -> None:
    spec = _spec()
    declared = _declared_paths(spec)
    desired = json.loads((INFRA / "desired-state.json").read_text(encoding="utf-8"))
    expected_routes = {
        route.split(" ", 1)[1]
        for group in desired["api_gateway"]["allowed_upstream_routes"].values()
        for route in group
    }
    assert declared == expected_routes

    # The only greedy suffix is deliberately scoped to the one private upload
    # bucket. Product APIs themselves must never regain catch-all routes.
    assert spec.count("{path+}") == 1
    assert "/auth/v1/{path+}" not in spec
    assert "/rest/v1/{path+}" not in spec
    assert "/functions/v1/{path+}" not in spec
    assert "/storage/v1/object/focus-feedback/{path+}" in spec
    assert "/storage/v1/object/focus-feedback/{path}" in spec
    assert "/realtime/v1/" not in spec
    assert "/auth/v1/admin" not in spec
    assert "/functions/v1/{" not in spec
    assert "/rest/v1/rpc/search_events_by_embedding_v1" not in spec
    assert "/rest/v1/rpc/reserve_event_search_quota_v3" not in spec

    assert _methods_for_path(spec, "/auth/v1/health") == {"get", "head"}
    assert _methods_for_path(spec, "/auth/v1/otp") == {"post"}
    assert _methods_for_path(spec, "/auth/v1/verify") == {"get", "post"}
    assert _methods_for_path(spec, "/auth/v1/callback") == {"get"}
    assert _methods_for_path(spec, "/auth/v1/token") == {"post"}
    assert _methods_for_path(spec, "/auth/v1/logout") == {"post"}
    assert _methods_for_path(spec, "/auth/v1/user/identities/authorize") == {"get"}
    assert _methods_for_path(spec, "/rest/v1/my_saved_events_v1") == {"get", "head"}
    assert _methods_for_path(spec, "/rest/v1/rpc/set_saved_event_state_v1") == {"post"}
    assert _methods_for_path(spec, "/rest/v1/rpc/register_focus_group_participant_v1") == {"post"}
    assert _methods_for_path(spec, "/rest/v1/rpc/submit_focus_group_feedback_v2") == {"post"}
    assert _methods_for_path(spec, "/rest/v1/rpc/transport_probe_v1") == {"post"}
    assert _methods_for_path(spec, "/functions/v1/event-search") == {"post"}
    assert _methods_for_path(spec, "/functions/v1/transport-probe") == {"post"}
    assert _methods_for_path(
        spec, "/storage/v1/object/focus-feedback/{path+}"
    ) == {"post", "delete"}
    assert _methods_for_path(
        spec, "/storage/v1/object/focus-feedback"
    ) == {"delete"}
    assert not re.search(r"^    (put|patch):$", spec, flags=re.MULTILINE)
    assert spec.count("    delete:") == 2


def test_relay_strips_cookie_host_and_spoofable_forwarding_headers() -> None:
    spec = _spec()
    integration_count = spec.count("type: http")
    for header in (
        "Host: ''",
        "Cookie: ''",
        "Forwarded: ''",
        "X-Forwarded-For: ''",
        "X-Forwarded-Host: ''",
        "X-Forwarded-Proto: ''",
        "X-Real-IP: ''",
    ):
        assert sum(line.strip() == header for line in spec.splitlines()) == integration_count


def test_relay_never_reflects_an_untrusted_browser_origin() -> None:
    spec = _spec()
    integration_count = spec.count("type: http")

    # Supabase reflects the Origin it receives. Every fixed HTTP integration
    # must therefore override the caller-provided value with the one public
    # KenigEvents origin instead of forwarding it via the wildcard mapping.
    assert sum(
        line.strip() == "Origin: https://kenigevents.ru"
        for line in spec.splitlines()
    ) == integration_count


def test_relay_does_not_use_deprecated_global_rate_limit_without_sws_profile() -> None:
    desired = json.loads((INFRA / "desired-state.json").read_text(encoding="utf-8"))
    spec = _spec()
    sws = desired["smart_web_security"]

    assert sws["security_profile_id"] is None
    assert sws["deployment_status"] == "not_attached"
    assert "x-yc-apigateway-rate-limit" not in spec
    assert "rateLimit:" not in spec
    assert "smartWebSecurity:" not in spec
