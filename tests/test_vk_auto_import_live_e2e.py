from __future__ import annotations

import importlib.util
from pathlib import Path


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "e2e" / "run_vk_auto_import_live.py"
    spec = importlib.util.spec_from_file_location("run_vk_auto_import_live", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_vk_live_e2e_terminal_response_classification() -> None:
    module = _module()
    assert module._terminal_response_kind("🏁 VK auto import завершён") == "success"
    assert module._terminal_response_kind("Not authorized") == "authorization_denied"
    assert module._terminal_response_kind("Результат: ошибка provider") == "operator_error"
    assert module._terminal_response_kind("Запускаю авторазбор") is None
