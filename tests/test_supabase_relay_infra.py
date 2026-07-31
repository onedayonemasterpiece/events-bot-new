import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra" / "yandex" / "supabase-relay"


def test_relay_is_stateless_fixed_upstream_and_exact_origin() -> None:
    desired = json.loads((INFRA / "desired-state.json").read_text(encoding="utf-8"))
    spec = (INFRA / "openapi.yaml").read_text(encoding="utf-8")

    assert desired["schema"] == "kenigevents.supabase_relay.yc_desired_state.v1"
    assert desired["api_gateway"]["cors_origin"] == "https://kenigevents.ru"
    assert desired["api_gateway"]["logging"] is False
    assert desired["api_gateway"]["stateful"] is False
    assert desired["api_gateway"]["service_account_id"] is None
    assert desired["security"]["service_role_key"] is False
    assert "origin: https://kenigevents.ru" in spec
    assert "origin: '*'" not in spec
    assert spec.count("type: http") == 3
    assert "type: cloud_functions" not in spec
    assert "type: cloud_ydb" not in spec
    assert "serviceAccount" not in spec
    assert "service_account" not in spec


def test_relay_forwards_only_bounded_public_api_prefixes_without_cookies() -> None:
    spec = (INFRA / "openapi.yaml").read_text(encoding="utf-8")

    for prefix in ("/auth/v1/{path+}", "/rest/v1/{path+}", "/functions/v1/{path+}"):
        assert prefix in spec
    assert "Cookie: ''" in spec
    assert "Host: ''" in spec
    assert "X-Forwarded-Host: ''" in spec
    assert "'*': '*'" in spec
    assert "/storage/v1/" not in spec
    assert "/realtime/v1/" not in spec
