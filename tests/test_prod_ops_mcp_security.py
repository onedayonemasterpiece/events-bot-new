from __future__ import annotations

import pytest

from prod_ops_mcp.config import OpsMCPConfig
from prod_ops_mcp.security import AdmissionController, AdmissionError, AuthError, authenticate, redact


def config(**overrides):
    values = dict(
        enabled=True,
        bind_host="127.0.0.1",
        port=8091,
        database_path="/tmp/db.sqlite",
        path_secret="p" * 40,
        bearer_token="b" * 40,
        allow_path_only_auth=False,
        allowed_origins=(),
        max_request_bytes=32768,
        max_response_bytes=196608,
        max_concurrency=1,
        ingress_requests_per_minute=30,
        ingress_burst=5,
        requests_per_minute=12,
        burst=3,
        egress_bytes_per_hour=1048576,
        path_only_requests_per_minute=4,
        path_only_egress_bytes_per_hour=262144,
        db_timeout_ms=300,
        cache_ttl_seconds=10,
    )
    values.update(overrides)
    return OpsMCPConfig(**values)


def test_bearer_is_constant_boundary_and_bad_token_does_not_fall_back():
    cfg = config(allow_path_only_auth=True)
    with pytest.raises(AuthError, match="invalid bearer"):
        authenticate({"Authorization": "Bearer wrong"}, cfg)
    auth = authenticate({}, cfg)
    assert auth.mode == "path_only"
    assert auth.permits("events_find")
    assert not auth.permits("event_explain")


def test_origin_is_rejected_by_default():
    with pytest.raises(AuthError, match="Origin"):
        authenticate(
            {"Authorization": "Bearer " + "b" * 40, "Origin": "https://evil.example"},
            config(),
        )


def test_redaction_removes_tokens_and_sensitive_url_params():
    value = redact(
        {
            "access_token": "secret",
            "url": "https://example.test/a?token=abc&item=ok#fragment",
        }
    )
    assert value["access_token"] == "<redacted>"
    assert "abc" not in value["url"]
    assert "fragment" not in value["url"]
    assert "item=ok" in value["url"]


def test_redaction_parses_json_strings_and_hides_secret_preview_paths():
    value = redact({
        "metrics_json": '{"access_token":"secret","url":"https://x.test/_review/abcdefghijklmnopqrstuvwxyz0123456789/page"}',
        "last_error": "Authorization: Bearer top-secret",
    })
    assert value["metrics_json"]["access_token"] == "<redacted>"
    assert "abcdefghijklmnopqrstuvwxyz" not in value["metrics_json"]["url"]
    assert "top-secret" not in value["last_error"]


@pytest.mark.asyncio
async def test_global_ingress_limit_applies_before_auth():
    cfg = config(ingress_requests_per_minute=1, ingress_burst=1)
    admission = AdmissionController(cfg)
    await admission.admit_ingress()
    with pytest.raises(AdmissionError, match="global ingress"):
        await admission.admit_ingress()
