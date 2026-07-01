from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


def load_runtime():
    path = Path("kaggle/SubscriberAcquisitionDiscovery/subscriber_acquisition_discovery.py")
    spec = importlib.util.spec_from_file_location("acq_discovery_runtime", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_extract_candidate_surfaces_from_public_links():
    runtime = load_runtime()
    surfaces = runtime.extract_candidate_surfaces("Вот t.me/some_kgd_chat и https://vk.com/club12345")
    by_external = {s["external_id"]: s for s in surfaces}
    assert by_external["tg:some_kgd_chat"]["source"] == "discovered"
    assert by_external["vk:club12345"]["platform"] == "vk"


def test_build_opportunity_from_message_is_review_only_with_sticker_observation():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://t.me/example", platform="tg")
    message = SimpleNamespace(id=12, message="Где послушать концерт на выходных? 😂", date=datetime(2026, 7, 1, tzinfo=timezone.utc))
    opp = runtime.build_opportunity_from_message(surface, message, default_target_url="https://t.me/kenigevents")
    assert opp is not None
    assert opp["context_url"] == "https://t.me/example/12"
    assert opp["link_target"]["kind"] == "pka_channel"
    assert opp["scores"]["source"] == "deterministic_shadow_prefilter"
    assert opp["sticker_observation"]["fit"] == "possible"


def test_shadow_payload_keeps_vk_seed_candidate_without_allowlist(monkeypatch):
    runtime = load_runtime()
    monkeypatch.setenv("ACQ_TG_SEEDS_JSON", '["https://t.me/a_public"]')
    monkeypatch.setenv("ACQ_VK_SEEDS_JSON", '["https://vk.com/test_public"]')
    monkeypatch.delenv("ACQ_VK_ALLOWLIST_JSON", raising=False)
    payload = runtime.build_shadow_payload()
    by_external = {s["external_id"]: s for s in payload["surfaces"]}
    assert by_external["tg:a_public"]["status"] == "candidate"
    assert by_external["vk:test_public"]["status"] == "candidate"
    assert payload["stats"]["external_sends"] == 0
    assert payload["stats"]["comments_posted"] == 0
    assert payload["stats"]["stickers_sent"] == 0


def test_build_vk_opportunity_is_read_only_review_payload():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://vk.com/test_public", platform="vk")
    comment = {"id": 7, "date": 1782900000, "text": "Где найти афишу на выходные?", "attachments": [{"type": "sticker"}]}
    opp = runtime.build_vk_opportunity(surface, owner_id=-123, post_id=55, comment=comment, default_target_url="https://t.me/kenigevents")
    assert opp is not None
    assert opp["platform"] == "vk"
    assert opp["context_url"] == "https://vk.com/wall-123_55?reply=7"
    assert opp["sticker_observation"]["fit"] == "possible"
    assert opp["scores"]["source"] == "deterministic_shadow_prefilter"


def test_vk_api_guard_rejects_write_method():
    runtime = load_runtime()
    try:
        runtime._vk_api("wall.createComment", token="x", params={})
    except RuntimeError as exc:
        assert "forbidden VK method" in str(exc)
    else:
        raise AssertionError("VK write method was not rejected")


def test_vk_allowlist_without_token_is_safe_seed_only(monkeypatch):
    runtime = load_runtime()
    monkeypatch.delenv("VK_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("VK_ACCESS_TOKEN4", raising=False)
    surfaces, opportunities, diagnostics = runtime.scan_vk_shadow_surfaces(["https://vk.com/test_public"], ["https://vk.com/test_public"])
    assert surfaces == []
    assert opportunities == []
    assert diagnostics and "token is not configured" in diagnostics[0]


def test_kaggle_runtime_static_no_external_write_calls():
    source = Path("kaggle/SubscriberAcquisitionDiscovery/subscriber_acquisition_discovery.py").read_text(encoding="utf-8")
    forbidden_snippets = [
        ".send_message(",
        ".send_file(",
        ".send_reaction(",
        "JoinChannelRequest",
        "ImportChatInviteRequest",
        "messages.send",
        "wall.post",
        "wall.createComment",
        "stories.",
    ]
    for snippet in forbidden_snippets:
        assert snippet not in source


def test_review_module_only_sends_to_configured_review_chat_static():
    source = Path("subscriber_acquisition/review.py").read_text(encoding="utf-8")
    assert "bot.send_message" in source
    assert "cfg.review_chat_id" in source
    assert "ensure_review_chat(sent_chat_id" in source


def test_kaggle_runtime_env_allowlists_vk_monitoring_seeds_for_discovery():
    from subscriber_acquisition.config import AcqConfig
    from subscriber_acquisition.kaggle_runner import _runtime_env_from_config

    payload = {"surfaces": [
        {"platform": "vk", "url": "https://vk.com/club1", "external_id": "vk:club1", "status": "candidate", "source": "vk_source"},
        {"platform": "vk", "url": "https://vk.com/club2", "external_id": "vk:club2", "status": "candidate", "source": "vk_source"},
    ]}
    env = _runtime_env_from_config(AcqConfig(), payload)
    assert env["ACQ_VK_SEEDS_JSON"] == '["https://vk.com/club1", "https://vk.com/club2"]'
    assert env["ACQ_VK_ALLOWLIST_JSON"] == env["ACQ_VK_SEEDS_JSON"]
