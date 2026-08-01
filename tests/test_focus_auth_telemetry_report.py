import importlib.util
import json
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "report_focus_auth_telemetry.py"
MIGRATION = ROOT / "supabase" / "migrations" / "20260801222242_focus_auth_delivery_attempt_v1.sql"


def _module():
    spec = importlib.util.spec_from_file_location("focus_auth_report", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


def test_report_contract_is_aggregate_only() -> None:
    script = SCRIPT.read_text(encoding="utf-8")
    migration = MIGRATION.read_text(encoding="utf-8")
    assert "focus_auth_operator_summary_v1" in script
    assert "focus_auth_operator_summary_v1" in migration
    assert "verification_auth_result" in migration
    assert "p_verified boolean" in migration
    assert "verified outcome requires matching authenticated user" in migration
    assert "login_method_outcomes" in migration
    assert "otp_issue_transport" in migration
    assert "otp_verify_transport" in migration
    assert "notisend_capacity" in migration
    for forbidden in ("normalized_email", "provider_message_id", "token_hash", "user_agent", "ip_address"):
        assert forbidden not in script


def test_report_writes_simple_chatgpt_bundle(tmp_path: Path) -> None:
    module = _module()
    payload = {
        "schema": "kenigevents.focus_auth_operator_summary.v1",
        "totals": {"delivery_attempts": 1, "unique_users": 1, "verified_methods": 1},
        "delivery_by_provider": [],
        "login_method_outcomes": [],
        "otp_issue_transport": [],
        "otp_verify_transport": [],
        "notisend_capacity": {"occupied": 1, "capacity": 200, "available": 199},
    }
    output = tmp_path / "report"
    with patch.dict(module.os.environ, {
        "PERSONALIZATION_SUPABASE_URL": "https://example.supabase.co",
        "PERSONALIZATION_SUPABASE_SECRET_KEY": "secret",
    }), patch.object(module, "_request", return_value=payload), patch.object(
        module.sys, "argv", ["report", "--output", str(output)]
    ):
        assert module.main() == 0
    assert json.loads((output / "summary.json").read_text()) == payload
    prompt = (output / "CHATGPT_PROMPT.txt").read_text()
    assert "email и Яндекс" in prompt
    assert "PASS/WARN/FAIL" in prompt
    assert not any(value in prompt for value in ("secret", "example.supabase.co"))
