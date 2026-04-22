from __future__ import annotations

from pathlib import Path


def test_tg_monitor_script_uses_google_ai_key3_and_gemma4() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "GoogleAIClient" in source
    assert "GOOGLE_API_KEY3" in source
    assert "GOOGLE_API_LOCALNAME3" in source
    assert "models/gemma-4-31b-it" in source
    assert "response_schema" in source
    assert "SupabaseLimiter" not in source
    assert "import google.generativeai as genai" not in source
    assert "genai.configure(" not in source


def test_tg_monitor_service_stages_script_built_notebook_and_google_ai_bundle() -> None:
    source = Path("source_parsing/telegram/service.py").read_text(encoding="utf-8")

    assert "_build_notebook_payload_from_script" in source
    assert "_sync_notebook_entrypoint" in source
    assert "_stage_google_ai_bundle(prepared)" in source
    assert "_sync_notebook_entrypoint(prepared)" in source
    assert "key.startswith(\"GOOGLE_API_LOCALNAME\")" in source
