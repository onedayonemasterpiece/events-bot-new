from __future__ import annotations

import pytest

from prod_ops_mcp.config import OpsMCPConfig


def test_disabled_config_does_not_require_secrets(monkeypatch):
    monkeypatch.delenv("ENABLE_PROD_OPS_MCP", raising=False)
    monkeypatch.delenv("PROD_OPS_MCP_PATH_SECRET", raising=False)
    monkeypatch.delenv("PROD_OPS_MCP_BEARER_TOKEN", raising=False)
    config = OpsMCPConfig.from_env()
    assert config.enabled is False


def test_enabled_config_requires_independent_long_secrets(monkeypatch):
    monkeypatch.setenv("ENABLE_PROD_OPS_MCP", "1")
    monkeypatch.setenv("PROD_OPS_MCP_PATH_SECRET", "x" * 40)
    monkeypatch.setenv("PROD_OPS_MCP_BEARER_TOKEN", "x" * 40)
    with pytest.raises(ValueError, match="must be different"):
        OpsMCPConfig.from_env()


def test_path_only_mode_is_clamped(monkeypatch):
    monkeypatch.setenv("ENABLE_PROD_OPS_MCP", "1")
    monkeypatch.setenv("PROD_OPS_MCP_PATH_SECRET", "p" * 40)
    monkeypatch.delenv("PROD_OPS_MCP_BEARER_TOKEN", raising=False)
    monkeypatch.setenv("PROD_OPS_MCP_ALLOW_PATH_ONLY_AUTH", "1")
    monkeypatch.setenv("PROD_OPS_MCP_PATH_ONLY_REQUESTS_PER_MINUTE", "12")
    monkeypatch.setenv("PROD_OPS_MCP_PATH_ONLY_EGRESS_BYTES_PER_HOUR", "1048576")
    config = OpsMCPConfig.from_env()
    assert config.path_only_requests_per_minute == 4
    assert config.path_only_egress_bytes_per_hour == 256 * 1024
