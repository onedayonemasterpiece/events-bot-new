from __future__ import annotations

import pytest

from private_events_mcp.config import PrivateEventsMCPConfig


def _enabled_env(monkeypatch) -> None:
    values = {
        "PRIVATE_EVENTS_MCP_ENABLED": "1",
        "PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL": "https://events.example",
        "PRIVATE_EVENTS_MCP_PATH_SECRET": "p" * 32,
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID": "chatgpt-client",
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET": "s" * 32,
        "PRIVATE_EVENTS_MCP_OPERATOR_TOKEN": "o" * 32,
        "PRIVATE_EVENTS_MCP_SIGNING_KEY": "k" * 43,
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_enabled_config_requires_distinct_static_codex_client(monkeypatch) -> None:
    _enabled_env(monkeypatch)
    with pytest.raises(ValueError, match="CODEX_OAUTH_CLIENT_ID"):
        PrivateEventsMCPConfig.from_env()

    monkeypatch.setenv("PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID", "chatgpt-client")
    with pytest.raises(ValueError, match="must be distinct"):
        PrivateEventsMCPConfig.from_env()

    monkeypatch.setenv("PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID", "codex-public-client")
    config = PrivateEventsMCPConfig.from_env()
    assert config.oauth_client_ids == frozenset({"chatgpt-client", "codex-public-client"})


def test_disabled_config_does_not_parse_mcp_only_client_values(monkeypatch) -> None:
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_ENABLED", "0")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID", "invalid id")
    assert PrivateEventsMCPConfig.from_env().enabled is False


def test_universal_social_flags_are_strictly_parented_and_provider_bound(
    monkeypatch,
) -> None:
    _enabled_env(monkeypatch)
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID", "codex-public-client")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_DM_ENABLED", "1")
    with pytest.raises(ValueError, match="UNIVERSAL_SOCIAL_ENABLED"):
        PrivateEventsMCPConfig.from_env()

    monkeypatch.setenv("PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_ENABLED", "1")
    with pytest.raises(ValueError, match="at least one provider"):
        PrivateEventsMCPConfig.from_env()

    monkeypatch.setenv("PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_TELEGRAM_ENABLED", "1")
    config = PrivateEventsMCPConfig.from_env()
    assert config.universal_social_enabled is True
    assert config.universal_social_telegram_enabled is True
    assert config.universal_social_dm_enabled is True
