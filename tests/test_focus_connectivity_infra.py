import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra" / "yandex" / "focus-connectivity"


def test_focus_connectivity_desired_state_is_read_only_and_dedicated() -> None:
    desired = json.loads((INFRA / "desired-state.json").read_text(encoding="utf-8"))

    assert desired["schema"] == "kenigevents.focus_connectivity.yc_desired_state.v1"
    assert desired["ydb"]["table_name"] == "focus_connectivity_probe"
    assert desired["ydb"]["key"] == {"probe_id": "primary"}
    assert desired["ydb"]["expected"] == {"status": "ready", "schema_version": 1}
    assert desired["service_account"]["name"] == "focus-connectivity-probe"
    assert desired["service_account"]["role"] == "ydb.viewer"
    assert desired["service_account"]["static_keys"] == 0
    assert desired["api_gateway"]["method"] == "GET"
    assert desired["api_gateway"]["cors_origin"] == "https://kenigevents.ru"


def test_focus_connectivity_gateway_has_no_write_integration() -> None:
    spec = (INFRA / "openapi.yaml").read_text(encoding="utf-8")

    assert "action: GetItem" in spec
    assert "table_name: focus_connectivity_probe" in spec
    assert "origin: https://kenigevents.ru" in spec
    assert "action: PutItem" not in spec
    assert "action: UpdateItem" not in spec
    assert "action: DeleteItem" not in spec
    assert "\n    post:" not in spec
