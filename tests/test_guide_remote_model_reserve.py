import ast
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from google_ai.exceptions import ProviderError
from guide_excursions import kaggle_service


@pytest.mark.asyncio
@pytest.mark.parametrize("all_unavailable", [False, True])
async def test_remote_guide_uses_configured_reserve_with_bounded_sends(monkeypatch, all_unavailable):
    for key in ("TG_API_ID", "TG_API_HASH", "TG_SESSION", "GOOGLE_API_KEY2"):
        monkeypatch.setenv(key, "1")
    for key in ("TELEGRAM_AUTH_BUNDLE_S22", "GUIDE_MONITORING_AUTH_BUNDLE_ENV"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("GOOGLE_AI_FALLBACK_MODELS", "gemini-3.5-flash-lite,gemini-3.1-flash-lite")
    payload = json.loads(kaggle_service._build_secrets_payload())
    # The child must see the transported settings, never the parent's environment.
    monkeypatch.delenv("GOOGLE_AI_FALLBACK_MODELS")
    for key, value in payload.items():
        monkeypatch.setenv(key, value)

    async def no_wait(*_args, **_kwargs):
        pass

    monkeypatch.setattr(asyncio, "sleep", no_wait)
    namespace = dict(
        asyncio=asyncio, os=__import__("os"), Any=object,
        _GEMMA_CLIENTS={}, _bootstrap_repo_bundle=lambda: None,
        _log_llm_gateway_once=lambda: None, _get_supabase_client=lambda: None,
        _GuideSecretsProviderAdapter=lambda value: value, _guide_account_name=lambda: None,
        GOOGLE_KEY_ENV="GOOGLE_API_KEY2", GOOGLE_FALLBACK_KEY_ENV="GOOGLE_API_KEY",
        LLM_TIMEOUT_SECONDS=5, LLM_TIMEOUT_RETRY_ATTEMPTS=0,
        LLM_PROVIDER_5XX_RETRY_ATTEMPTS=0, collapse_ws=lambda value: str(value or "").strip(),
        _extract_json=json.loads, _retry_after_seconds=lambda _: None,
        _provider_5xx_status=lambda _: 503,
    )
    source = ast.parse(Path("kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py").read_text())
    functions = [node for node in source.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and node.name in {"_get_gemma_client", "ask_gemma"}]
    exec(compile(ast.Module(body=functions, type_ignores=[]), "guide-actual-functions", "exec"), namespace)
    client = namespace["_get_gemma_client"]("guide_reserve_control")
    sends = []

    async def provider(*, ctx, **_kwargs):
        sends.append(ctx.model)
        if all_unavailable or ctx.model.startswith("gemma"):
            raise ProviderError(error_type="server", error_message="503 UNAVAILABLE", status_code=503, retryable=True)
        return '{"ok": true}', SimpleNamespace(total_tokens=1)

    monkeypatch.setattr(client, "_attempt_generate", provider)
    monkeypatch.setattr(client, "_notify_incident", no_wait)
    call = namespace["ask_gemma"]("gemma-4-31b", "control", consumer="guide_reserve_control")
    if all_unavailable:
        with pytest.raises(ProviderError):
            await call
        assert len(sends) <= 3
        assert "gemini-3.5-flash-lite" in sends
    else:
        assert await call == {"ok": True}
        assert sends == ["gemma-4-31b", "gemini-3.5-flash-lite"]
