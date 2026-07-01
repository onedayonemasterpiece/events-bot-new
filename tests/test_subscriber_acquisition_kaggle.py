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
