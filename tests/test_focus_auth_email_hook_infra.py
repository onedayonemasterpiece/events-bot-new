import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra" / "yandex" / "focus-auth-email-hook"
SOURCE = ROOT / "serverless" / "focus_auth_email_hook"
MIGRATION = ROOT / "supabase" / "migrations" / "20260801222242_focus_auth_delivery_attempt_v1.sql"


def test_function_boundary_is_narrow_and_secret_safe() -> None:
    desired = json.loads((INFRA / "desired-state.json").read_text(encoding="utf-8"))
    spec = (INFRA / "openapi.yaml").read_text(encoding="utf-8")

    assert desired["schema"] == "kenigevents.focus_auth_email_hook.yc_desired_state.v1"
    assert desired["function"]["runtime"] == "python312"
    assert desired["function"]["timeout_seconds"] == 5
    assert desired["function"]["stage_budget_ms"]["total_network_ceiling"] == 4_200
    assert desired["function"]["stage_budget_ms"]["total_network_ceiling"] < 5_000
    assert desired["function"]["required_roles"] == ["postbox.sender"]
    assert desired["api_gateway"]["public_path"] == "/v1/send-email"
    assert desired["api_gateway"]["method"] == "POST"
    assert desired["api_gateway"]["max_body_bytes"] == 65_536
    assert set(desired["lockbox"]["required_environment_variables"]) == {
        "SEND_EMAIL_HOOK_SECRET",
        "PERSONALIZATION_SUPABASE_SECRET_KEY",
        "NOTISEND_API_TOKEN",
        "FOCUS_AUTH_NOTISEND_EMAILS",
    }
    assert spec.count("type: cloud_functions") == 1
    assert spec.count("    post:") == 1
    assert "    get:" not in spec
    assert "{path" not in spec
    assert "cors" not in spec.lower()
    assert "${FOCUS_AUTH_EMAIL_HOOK_FUNCTION_ID}" in spec
    assert "${FOCUS_AUTH_EMAIL_HOOK_INVOKER_SERVICE_ACCOUNT_ID}" in spec


def test_provider_policy_has_one_dispatch_and_shared_unique_recipient_cap() -> None:
    desired = json.loads((INFRA / "desired-state.json").read_text(encoding="utf-8"))
    source = (SOURCE / "index.py").read_text(encoding="utf-8")
    migration = MIGRATION.read_text(encoding="utf-8")

    assert desired["provider_policy"]["new_first_send"] == "postbox"
    assert desired["provider_policy"]["returning_or_repeat_send"] == "notisend"
    assert desired["provider_policy"]["notisend_payment"] == "subscriber"
    assert desired["provider_policy"]["cross_provider_retry_after_ambiguous"] is False
    assert desired["provider_policy"]["unique_recipient_capacity"] == 200
    assert '"p_prefer_notisend": prefer_notisend' in source
    assert "focus_auth_reserve_notisend_recipient_v1" in migration
    assert 'provider = "notisend" if notisend_admitted else "postbox"' in source
    assert source.count("_notisend_send(") == 2  # definition + one dispatch site
    assert source.count("_postbox_send(") == 2  # definition + one dispatch site
    assert "email_control.notisend_recipient_admission" in migration
    assert "external_reserved_count" in migration
    assert "notisend_capacity_full" in migration
    assert "first_attempt_id" in migration
    assert "normalized_email" not in migration


def test_build_is_deterministic_and_contains_only_runtime_sources() -> None:
    script = (INFRA / "build-function.sh").read_text(encoding="utf-8")
    requirements = (SOURCE / "requirements.txt").read_text(encoding="utf-8")

    assert 'for name in ("index.py", "requirements.txt")' in script
    assert "ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))" in script
    assert "sha256sum" in script
    assert "Standard library only" in requirements
    assert "pip install" not in script
