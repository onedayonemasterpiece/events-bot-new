from __future__ import annotations

import importlib.util
import json
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
    surfaces = runtime.extract_candidate_surfaces("Вот t.me/some_kgd_chat и https://vk.com/wall-12345_9")
    by_external = {s["external_id"]: s for s in surfaces}
    assert by_external["tg:some_kgd_chat"]["source"] == "discovered"
    assert by_external["vk:club12345"]["platform"] == "vk"
    assert by_external["vk:club12345"]["url"] == "https://vk.com/club12345"




def test_out_of_region_telegram_surface_is_rejected_not_queued():
    runtime = load_runtime()

    surfaces = runtime.extract_candidate_surfaces("Смотрите ещё https://t.me/visitNavahrudak")

    assert len(surfaces) == 1
    surface = surfaces[0]
    assert surface["external_id"] == "tg:visitNavahrudak"
    assert surface["status"] == "rejected_out_of_region"
    assert surface["topic_cluster"] == "out_of_region"
    assert runtime._is_surface_scan_candidate(surface) is False


def test_acquisition_intent_covers_site_filters_and_partnership(monkeypatch):
    runtime = load_runtime()
    monkeypatch.setenv("ACQ_STATIC_SITE_BASE_URL", "https://kenigevents.ru")
    surface = runtime._seed_surface("https://t.me/example", platform="tg")

    partner = runtime.build_opportunity_from_message(
        surface,
        SimpleNamespace(id=21, message="Куда прислать афишу и как добавить мероприятие?"),
        default_target_url="https://t.me/kenigevents",
    )
    badge = runtime.build_opportunity_from_message(
        surface,
        SimpleNamespace(id=22, message="Есть поиск мероприятий по Пушкинской карте и для детей?"),
        default_target_url="https://t.me/kenigevents",
    )
    exhibitions = runtime.build_opportunity_from_message(
        surface,
        SimpleNamespace(id=23, message="Где посмотреть все выставки в Калининграде?"),
        default_target_url="https://t.me/kenigevents",
    )

    assert partner["topic_cluster"] == "organizer_partnership"
    assert partner["link_target"]["url"] == "https://kenigevents.ru/partnerstvo/"
    assert badge["topic_cluster"] == "event_badges_filters"
    assert badge["matched_intent"] == "event_badge_or_filter_request"
    assert exhibitions["topic_cluster"] == "event_site_search"
    assert exhibitions["link_target"]["url"] == "https://kenigevents.ru/vystavki/"


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






def test_badge_word_in_advice_is_not_filter_request():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://vk.com/example", platform="vk")

    opp = runtime.build_vk_opportunity(
        surface,
        owner_id=-1,
        post_id=2,
        comment={"id": 3, "text": "Вот вам совет: добавьте игры с картинками растений. Детям нравятся такие занятия."},
        default_target_url="https://t.me/kenigevents",
    )

    assert opp is None


def test_organizer_thanks_is_not_partnership_opportunity():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://vk.com/example", platform="vk")

    opp = runtime.build_vk_opportunity(
        surface,
        owner_id=-1,
        post_id=2,
        comment={"id": 3, "text": "Спасибо организаторам за прекрасно проведенное время❤"},
        default_target_url="https://t.me/kenigevents",
    )

    assert opp is None


def test_existing_event_logistics_comment_is_not_acquisition_opportunity():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://vk.com/example", platform="vk")

    assert runtime.build_vk_opportunity(
        surface,
        owner_id=-1,
        post_id=2,
        comment={"id": 3, "text": "Подскажите, до скольки мероприятие?"},
        default_target_url="https://t.me/kenigevents",
    ) is None


def test_generic_where_comment_is_not_event_opportunity():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://t.me/example", platform="tg")
    message = SimpleNamespace(id=12, message="А где 34 автобус?!", date=datetime(2026, 7, 1, tzinfo=timezone.utc))

    assert runtime.build_opportunity_from_message(surface, message, default_target_url="https://t.me/kenigevents") is None


def test_telegram_opportunity_filter_requires_comments_not_channel_posts():
    runtime = load_runtime()

    channel_post = SimpleNamespace(post=True, fwd_from=None, reply_to=None)
    copied_discussion_post = SimpleNamespace(post=False, fwd_from=object(), reply_to=object())
    linked_comment = SimpleNamespace(post=False, fwd_from=None, reply_to=object())
    group_message = SimpleNamespace(post=False, fwd_from=None, reply_to=None)

    assert runtime.is_comment_opportunity_message(channel_post, surface_type="channel", relation=None) is False
    assert runtime.is_comment_opportunity_message(copied_discussion_post, surface_type="linked_discussion", relation="linked_discussion") is False
    assert runtime.is_comment_opportunity_message(linked_comment, surface_type="linked_discussion", relation="linked_discussion") is True
    assert runtime.is_comment_opportunity_message(group_message, surface_type="group", relation=None) is True


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



def test_vk_token_lanes_prefer_monitoring_token_without_leaking_values(monkeypatch):
    runtime = load_runtime()
    monkeypatch.setenv("VK_ACCESS_TOKEN", "generic-token")
    monkeypatch.setenv("VK_ACCESS_TOKEN4", "monitoring-token")

    lanes = runtime._vk_token_lanes()

    assert [name for name, _token in lanes] == ["VK_ACCESS_TOKEN4", "VK_ACCESS_TOKEN"]
    assert [token for _name, token in lanes] == ["monitoring-token", "generic-token"]


def test_tg_frontier_queue_dedupes_and_respects_limit():
    runtime = load_runtime()
    queue = []
    queued = set()

    assert runtime._enqueue_tg_url(queue, queued, "https://t.me/first", limit=2) is True
    assert runtime._enqueue_tg_url(queue, queued, "https://t.me/first/", limit=2) is False
    assert runtime._enqueue_tg_url(queue, queued, "https://t.me/second", limit=2) is True
    assert runtime._enqueue_tg_url(queue, queued, "https://t.me/third", limit=2) is False

    assert queue == ["https://t.me/first", "https://t.me/second"]


def test_kaggle_config_overrides_stale_acq_env(monkeypatch, tmp_path):
    runtime = load_runtime()
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"ACQ_ENABLE_LIVE_TG_SCAN": "1"}), encoding="utf-8")

    monkeypatch.setenv("ACQ_ENABLE_LIVE_TG_SCAN", "0")
    monkeypatch.setattr(runtime, "_find_input_file", lambda name: config_path if name == "config.json" else None)

    loaded = runtime._load_kaggle_env()
    assert loaded["config_loaded"] is True
    assert runtime.os.environ["ACQ_ENABLE_LIVE_TG_SCAN"] == "1"


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


def test_kaggle_dataset_slug_stays_within_current_api_limit():
    from subscriber_acquisition.kaggle_runner import (
        CONFIG_DATASET_CIPHER,
        CONFIG_DATASET_KEY,
        _build_dataset_slug,
    )

    run_id = "20260701T010203Z-abcdef1234567890"
    for prefix in [CONFIG_DATASET_CIPHER, CONFIG_DATASET_KEY, "subscriber-acquisition-discovery-extra-long-prefix"]:
        slug = _build_dataset_slug(prefix, run_id)
        assert 6 <= len(slug) <= 50
        assert "20260701t010203z" in slug

    assert _build_dataset_slug(CONFIG_DATASET_CIPHER, run_id) != _build_dataset_slug(CONFIG_DATASET_KEY, run_id)


def test_kaggle_status_parser_handles_sdk_enum_values():
    from subscriber_acquisition.kaggle_runner import _status_to_text

    class StatusEnum:
        name = "COMPLETE"

    class Response:
        status = StatusEnum()

    assert _status_to_text(Response()) == "COMPLETE"
    assert _status_to_text({"status": "KernelWorkerStatus.COMPLETE"}) == "COMPLETE"
