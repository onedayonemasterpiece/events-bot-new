from __future__ import annotations

from pathlib import Path


def test_tg_monitor_script_uses_google_ai_key3_and_gemma4() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "GoogleAIClient" in source
    assert "GOOGLE_API_KEY3" in source
    assert "GOOGLE_API_LOCALNAME3" in source
    assert "or GOOGLE_KEY_ENV" in source
    assert "or GOOGLE_ACCOUNT_ENV" in source
    assert "models/gemma-4-31b-it" in source
    assert "response_schema" in source
    assert "SupabaseLimiter" not in source
    assert "import google.generativeai as genai" not in source
    assert "genai.configure(" not in source
    assert "action=local_primary_limiter" in source
    assert "resolved = primary_ids" in source
    assert "primary_ids or fallback_ids" not in source
    assert "return list(_CANDIDATE_KEY_IDS)" in source
    assert "GOOGLE_AI_PROVIDER_TIMEOUT_SEC" in source
    assert "TG_MONITORING_LLM_TIMEOUT_SECONDS" in source
    assert "or '45'" in source


def test_tg_monitor_script_blocks_social_links_as_source_websites() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "_SOURCE_WEBSITE_BLOCK_RE" in source
    assert "instagram\\.com" in source
    assert "linktr\\.ee" in source
    assert "_is_disallowed_source_website_url" in source


def test_tg_monitor_extract_prompt_hardens_gemma4_ocr_merge_rules() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "Never return whitespace-only strings." in source
    assert "Use evidence from both message text and OCR." in source
    assert "Prefer filling location_name and location_address" in source
    assert "Do not invent end_date for single-date events." in source


def test_tg_monitor_runner_bootstraps_google_ai_bundle_for_kaggle_notebook() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "bootstrap_google_ai_bundle" in source
    assert "importlib.util.find_spec('google_ai')" in source
    assert "Path('/kaggle/input')" in source
    assert "tg_monitor.google_ai bootstrap" in source


def test_tg_monitor_service_stages_script_built_notebook_and_google_ai_bundle() -> None:
    source = Path("source_parsing/telegram/service.py").read_text(encoding="utf-8")

    assert "_embedded_google_ai_sources" in source
    assert "_build_notebook_payload_from_script" in source
    assert "_sync_notebook_entrypoint" in source
    assert "_stage_google_ai_bundle(prepared)" in source
    assert "_sync_notebook_entrypoint(prepared)" in source
    assert "_TG_EMBEDDED_GOOGLE_AI" in source
    assert "_TG_EMBEDDED_ROOT" in source
    assert "__file__ = str((_TG_NOTEBOOK_ROOT / 'telegram_monitor.py').resolve())" in source
    assert "_tg_run_main_sync" in source
    assert "nest_asyncio.apply(loop)" in source
    assert "loop.run_until_complete(main())" in source
    assert "key.startswith(\"GOOGLE_API_LOCALNAME\")" in source
    assert "Do not ship unrelated GOOGLE_API_KEY* values" in source
    assert "\"TG_MONITORING_GOOGLE_KEY_ENV\": google_key_env" in source
