from __future__ import annotations

import json

from source_parsing.telegram.service import _build_secrets_payload


def test_build_secrets_payload_includes_yandex_storage_env(monkeypatch):
    monkeypatch.setenv("TG_API_ID", "123")
    monkeypatch.setenv("TG_API_HASH", "hash")
    monkeypatch.setenv("GOOGLE_API_KEY", "google")
    monkeypatch.setenv("TG_SESSION", "session")
    monkeypatch.setenv("YC_SA_BOT_STORAGE", "access")
    monkeypatch.setenv("YC_SA_BOT_STORAGE_KEY", "secret")
    monkeypatch.setenv("YC_STORAGE_BUCKET", "kenigevents")
    monkeypatch.setenv("YC_STORAGE_ENDPOINT", "https://storage.yandexcloud.net")

    payload = json.loads(_build_secrets_payload())

    assert payload["YC_SA_BOT_STORAGE"] == "access"
    assert payload["YC_SA_BOT_STORAGE_KEY"] == "secret"
    assert payload["YC_STORAGE_BUCKET"] == "kenigevents"
    assert payload["YC_STORAGE_ENDPOINT"] == "https://storage.yandexcloud.net"
