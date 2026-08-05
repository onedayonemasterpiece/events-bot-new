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
    assert desired["function"]["stage_budget_ms"]["recipient_admission_and_capacity_rpc"] == 1_100
    assert desired["function"]["stage_budget_ms"]["total_network_ceiling"] < 5_000
    assert desired["function"]["required_roles"] == ["postbox.sender"]
    assert desired["function"]["secret_access"] == {
        "hook_secret": "lockbox.payloadViewer",
        "email_address_hmac_secret": "lockbox.payloadViewer",
        "notisend_secret": "lockbox.payloadViewer",
        "notisend_secret_kms_key": "kms.keys.encrypterDecrypter",
    }
    assert desired["api_gateway"]["function_access_role"] == "functions.functionInvoker"
    assert desired["api_gateway"]["public_path"] == "/v1/send-email"
    assert desired["api_gateway"]["method"] == "POST"
    assert desired["api_gateway"]["max_body_bytes"] == 65_536
    assert set(desired["lockbox"]["required_environment_variables"]) == {
        "SEND_EMAIL_HOOK_SECRET",
        "EMAIL_ADDRESS_HMAC_KEY",
        "EMAIL_ADDRESS_HMAC_KEY_VERSION",
        "PERSONALIZATION_SUPABASE_SECRET_KEY",
        "NOTISEND_API_TOKEN",
        "FOCUS_AUTH_NOTISEND_EMAILS",
    }
    assert "EMAIL_ADDRESS_HMAC_KEY_VERSION" not in desired["lockbox"]["non_secret_environment_variables"]
    assert desired["lockbox"]["email_address_hmac_secret_id"] == "e6qeqbto7ticn9fsklgq"
    assert desired["lockbox"]["email_address_hmac_secret_version_id"] == "e6qi77mdnpmetpljf5qa"
    assert desired["lockbox"]["email_address_hmac_key"] == "hmac_key"
    assert desired["lockbox"]["email_address_hmac_version_key"] == "hmac_key_version"
    assert spec.count("type: cloud_functions") == 1
    assert spec.count("    post:") == 1
    assert "    get:" not in spec
    assert "{path" not in spec
    assert "cors" not in spec.lower()
    assert "${FOCUS_AUTH_EMAIL_HOOK_FUNCTION_ID}" in spec
    assert "${FOCUS_AUTH_EMAIL_HOOK_INVOKER_SERVICE_ACCOUNT_ID}" in spec


def test_unreconciled_notisend_capacity_is_unknown_not_zero() -> None:
    migration = (
        ROOT
        / "supabase/migrations/20260802013600_notisend_unreconciled_capacity_unknown.sql"
    ).read_text(encoding="utf-8")

    assert "alter column provider_used_count drop not null" in migration
    assert "alter column provider_used_count drop default" in migration
    assert "set provider_used_count = null" in migration
    assert "where provider_reconciled_at is null" in migration
    assert "provider_reconciliation_value_chk" in migration

    projection = (
        ROOT
        / "supabase/migrations/20260802014500_notisend_unreconciled_operator_projection.sql"
    ).read_text(encoding="utf-8")
    assert "focus_auth_operator_summary_base_v1" in projection
    assert "{notisend_capacity,provider_reported}" in projection
    assert "{notisend_capacity,admitted_after_reconcile}" in projection
    assert "{notisend_capacity,occupied}" in projection
    assert "{notisend_capacity,available}" in projection
    assert projection.count("'null'::jsonb") == 4
    assert "to service_role" in projection


def test_provider_policy_has_one_dispatch_and_shared_unique_recipient_cap() -> None:
    desired = json.loads((INFRA / "desired-state.json").read_text(encoding="utf-8"))
    source = (SOURCE / "index.py").read_text(encoding="utf-8")
    migration = MIGRATION.read_text(encoding="utf-8")

    assert desired["provider_policy"]["new_first_send"] == "postbox"
    assert desired["provider_policy"]["returning_or_repeat_send"] == "notisend"
    assert desired["provider_policy"]["notisend_payment"] == "subscriber"
    assert desired["provider_policy"]["cross_provider_retry_after_ambiguous"] is False
    assert desired["provider_policy"]["unique_recipient_capacity"] == 200
    assert desired["provider_policy"]["unique_recipient_capacity_scope"] == "provider_billing_period"
    assert desired["provider_policy"]["requires_provider_counter_reconciliation"] is True
    assert desired["provider_policy"]["auth_route_when_unreconciled_expired_or_full"] == "postbox"
    assert desired["provider_policy"]["email_change"] == "postbox"
    assert desired["provider_policy"]["secure_email_change_deliveries"] == 2
    assert desired["provider_policy"]["hmac_rotation_with_active_backlog"] == (
        "forbidden_without_overlap_migration"
    )
    assert '"p_deliveries"' in source
    assert '"email_hmac": item["email_hmac"]' in source
    assert '"hmac_key_version": item["hmac_key_version"]' in source
    assert "focus_auth_begin_delivery_batch_v1" in source
    assert "focus_auth_complete_delivery_batch_v1" in source
    assert "focus_auth_reserve_notisend_recipient_v1" in migration
    assert 'provider = "notisend" if notisend_admitted else "postbox"' in source
    assert source.count("_notisend_send(") == 2  # definition + one dispatch site
    assert source.count("_postbox_send(") == 2  # definition + one dispatch site
    assert "email_control.notisend_recipient_admission" in migration
    assert "provider_period_key" in migration
    assert "provider_period_ends_at" in migration
    assert "provider_used_count" in migration
    assert "provider_reconciled_at" in migration
    assert "focus_auth_reconcile_notisend_capacity_v1" in migration
    assert "included_in_provider_snapshot" in migration
    assert "and not a.included_in_provider_snapshot" in migration
    assert "does not\n  -- release a recipient" in migration
    assert "references auth.users (id) on delete cascade" not in migration
    assert "current NotiSend billing period is still active" in migration
    assert "NotiSend provider count is behind local admissions" in migration
    assert "NotiSend billing period key was already used" in migration
    assert "v_provider_used + v_incremental_count >= v_capacity" in migration
    assert "p_outcome in ('definitive_reject', 'configuration_error')" in migration
    assert "delete from email_control.notisend_recipient_admission" in migration
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
