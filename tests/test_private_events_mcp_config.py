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


def _media_enabled_env(monkeypatch) -> None:
    _enabled_env(monkeypatch)
    values = {
        "PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID": "codex-public-client",
        "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_ENABLED": "1",
        "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_TELEGRAM_ENABLED": "1",
        "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_MEDIA_STORY_ENABLED": "1",
        "PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN": "approval_" + "a" * 48,
        "PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS": "files.example",
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


def test_media_limits_use_the_canonical_documented_environment_names(
    monkeypatch,
) -> None:
    _media_enabled_env(monkeypatch)
    documented = {
        "PRIVATE_EVENTS_MCP_MEDIA_MAX_ASSET_BYTES": "1048576",
        "PRIVATE_EVENTS_MCP_MEDIA_MAX_STORE_BYTES": "2097152",
        "PRIVATE_EVENTS_MCP_MEDIA_ASSET_TTL_SECONDS": "600",
        "PRIVATE_EVENTS_MCP_MEDIA_DOWNLOAD_TIMEOUT_SECONDS": "7",
        "PRIVATE_EVENTS_MCP_MEDIA_MAX_WIDTH": "1024",
        "PRIVATE_EVENTS_MCP_MEDIA_MAX_HEIGHT": "2048",
        "PRIVATE_EVENTS_MCP_MEDIA_MAX_PIXELS": "1500000",
    }
    for key, value in documented.items():
        monkeypatch.setenv(key, value)

    # Historical, undocumented names must not shadow the canonical contract.
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_MAX_ASSET_BYTES", "17")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_MAX_STORE_BYTES", "19")
    config = PrivateEventsMCPConfig.from_env()

    assert config.max_asset_bytes == 1_048_576
    assert config.max_store_bytes == 2_097_152
    assert config.asset_ttl_seconds == 600
    assert config.download_timeout_seconds == 7
    assert config.max_width == 1024
    assert config.max_height == 2048
    assert config.max_pixels == 1_500_000


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("PRIVATE_EVENTS_MCP_MEDIA_MAX_ASSET_BYTES", "0"),
        ("PRIVATE_EVENTS_MCP_MEDIA_MAX_STORE_BYTES", "0"),
        ("PRIVATE_EVENTS_MCP_MEDIA_ASSET_TTL_SECONDS", "59"),
        ("PRIVATE_EVENTS_MCP_MEDIA_DOWNLOAD_TIMEOUT_SECONDS", "0"),
        ("PRIVATE_EVENTS_MCP_MEDIA_MAX_WIDTH", "8193"),
        ("PRIVATE_EVENTS_MCP_MEDIA_MAX_HEIGHT", "0"),
        ("PRIVATE_EVENTS_MCP_MEDIA_MAX_PIXELS", "40000001"),
        ("PRIVATE_EVENTS_MCP_MEDIA_MAX_PIXELS", "forty-million"),
    ],
)
def test_media_limit_bounds_report_the_canonical_environment_name(
    monkeypatch, name: str, value: str
) -> None:
    _media_enabled_env(monkeypatch)
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        PrivateEventsMCPConfig.from_env()


def test_media_store_budget_must_cover_the_asset_budget(monkeypatch) -> None:
    _media_enabled_env(monkeypatch)
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_MEDIA_MAX_ASSET_BYTES", "2097152")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_MEDIA_MAX_STORE_BYTES", "1048576")

    with pytest.raises(ValueError, match="MEDIA_MAX_STORE_BYTES"):
        PrivateEventsMCPConfig.from_env()


@pytest.mark.parametrize(
    "unsafe_origin",
    [
        "https://user:secret@events.example",
        "https://events.example?token=secret",
        "https://events.example#secret",
        "https://events.example:444",
        "https://events.example\nsecret.example",
        "https://events.example\tsecret.example",
        "https://example..com",
        "https://-bad.example",
        "https://bad-.example",
        "https://127.1",
        "https://127.000.000.001",
        "https://2130706433",
        "https://0x7f000001",
        "https://[fe80::1%25eth0]",
    ],
)
def test_enabled_config_rejects_noncanonical_or_secret_bearing_origin(
    monkeypatch, unsafe_origin: str
) -> None:
    _enabled_env(monkeypatch)
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID", "codex-public-client")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL", unsafe_origin)
    with pytest.raises(ValueError, match="PUBLIC_BASE_URL"):
        PrivateEventsMCPConfig.from_env()


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
    monkeypatch.setenv(
        "PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN", "approval_" + "a" * 48
    )
    config = PrivateEventsMCPConfig.from_env()
    assert config.universal_social_enabled is True
    assert config.universal_social_telegram_enabled is True
    assert config.universal_social_dm_enabled is True
