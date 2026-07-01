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


def test_extract_candidate_surfaces_skips_telegram_bots_and_service_links():
    runtime = load_runtime()

    surfaces = runtime.extract_candidate_surfaces(
        "https://t.me/somehelperbot https://t.me/addstickers/Foo "
        "https://t.me/share/url?url=x https://t.me/real_kgd_chat"
    )

    assert [surface["external_id"] for surface in surfaces] == ["tg:real_kgd_chat"]
    assert runtime._is_tg_discovery_bot_or_service_handle("weatherbot") is True
    assert runtime._is_tg_discovery_bot_or_service_handle("real_kgd_chat") is False


def test_vk_surface_extractor_skips_non_community_links():
    runtime = load_runtime()

    surfaces = runtime.extract_candidate_surfaces(
        "https://vk.com/album-123_456 https://vk.com/app7070938_-1 https://vk.com/market-1 "
        "https://vk.com/away.php https://vk.com/id6786438 https://vk.com/club123"
    )

    assert [surface["external_id"] for surface in surfaces] == ["vk:club123"]
    assert runtime._is_vk_scan_domain_candidate("album-123_456") is False
    assert runtime._is_vk_scan_domain_candidate("club123") is True




def test_out_of_region_telegram_surface_is_rejected_not_queued():
    runtime = load_runtime()

    surfaces = runtime.extract_candidate_surfaces("Смотрите ещё https://t.me/visitNavahrudak")

    assert len(surfaces) == 1
    surface = surfaces[0]
    assert surface["external_id"] == "tg:visitNavahrudak"
    assert surface["status"] == "rejected_out_of_region"
    assert surface["topic_cluster"] == "out_of_region"
    assert runtime._is_surface_scan_candidate(surface) is False


def test_rejected_no_comments_surface_is_not_scan_candidate():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://t.me/no_comments_channel", platform="tg")
    surface["status"] = "rejected_no_comments"

    assert runtime._is_surface_scan_candidate(surface) is False




def test_trip_recommendation_requirement_is_acquisition_topic():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://t.me/example", platform="tg")

    opp = runtime.build_opportunity_from_message(
        surface,
        SimpleNamespace(id=31, message="Куда съездить на один день из Калининграда на электричке в выходные?"),
        default_target_url="https://t.me/kenigevents",
    )

    assert opp is not None
    assert opp["matched_intent"] == "trip_route_recommendation_context"
    assert opp["topic_cluster"] == "trip_route_recommendation"
    assert opp["link_target"]["kind"] == "other"
    assert opp["link_target"]["url"] is None
    assert "Конкретный маршрут" in opp["link_target"]["label"]
    assert opp["fallback_link_target"]["url"] is None


def test_trip_recommendation_prefilter_covers_looser_route_questions():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://t.me/example", platform="tg")

    examples = [
        "Что посмотреть за день в области с детьми?",
        "Куда поехать на выходных из Калининграда?",
        "Посоветуйте маршрут по области: замки и побережье",
    ]

    for index, text in enumerate(examples, start=40):
        opp = runtime.build_opportunity_from_message(
            surface,
            SimpleNamespace(id=index, message=text),
            default_target_url="https://t.me/kenigevents",
        )
        assert opp is not None, text
        assert opp["topic_cluster"] == "trip_route_recommendation"


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


def test_venue_policy_question_is_not_badge_filter_opportunity():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://vk.com/yunost_park", platform="vk")

    assert runtime.build_vk_opportunity(
        surface,
        owner_id=-1,
        post_id=2,
        comment={"id": 3, "text": "Здравствуйте, есть ли у вас какие-то льготы для детей-инвалидов????"},
        default_target_url="https://t.me/kenigevents",
    ) is None


def test_citywide_accessible_event_search_is_still_badge_filter_opportunity():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://vk.com/example", platform="vk")

    opp = runtime.build_vk_opportunity(
        surface,
        owner_id=-1,
        post_id=2,
        comment={"id": 4, "text": "Где найти мероприятия с льготами и доступностью для детей-инвалидов в Калининграде?"},
        default_target_url="https://t.me/kenigevents",
    )

    assert opp is not None
    assert opp["matched_intent"] == "event_badge_or_filter_request"
    assert opp["topic_cluster"] == "event_badges_filters"


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


def test_shadow_payload_preserves_scanned_vk_surface_metadata(monkeypatch):
    runtime = load_runtime()
    monkeypatch.setenv("ACQ_VK_SEEDS_JSON", '["https://vk.com/vagonka39"]')
    monkeypatch.setenv("ACQ_VK_ALLOWLIST_JSON", '["https://vk.com/vagonka39"]')

    scanned = runtime._seed_surface("https://vk.com/vagonka39", platform="vk")
    scanned.update({"source": "allowlist", "status": "approved", "reach": {"basis": "vk_wall", "confidence": "low"}})
    payload = runtime.build_shadow_payload(scanned_surfaces=[scanned], scanned_opportunities=[])

    by_external = {s["external_id"]: s for s in payload["surfaces"]}
    assert by_external["vk:vagonka39"]["source"] == "allowlist"
    assert by_external["vk:vagonka39"]["reach"]["basis"] == "vk_wall"


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


def test_vk_social_wall_post_can_be_reply_opportunity_but_official_post_cannot():
    runtime = load_runtime()
    social_surface = runtime._seed_surface("https://vk.com/club42481124", platform="vk")
    official_surface = runtime._seed_surface("https://vk.com/shaman_kaliningrad", platform="vk")
    post = {
        "owner_id": -42481124,
        "id": 101,
        "date": 1782900000,
        "text": "Подскажите, куда съездить на один день из Калининграда с детьми?",
        "comments": {"count": 3},
        "views": {"count": 1200},
    }

    opp = runtime.build_vk_wall_post_opportunity(social_surface, post=post, default_target_url="https://t.me/kenigevents")

    assert opp is not None
    assert opp["context_url"] == "https://vk.com/wall-42481124_101"
    assert opp["topic_cluster"] == "trip_route_recommendation"
    assert opp["evidence"]["relation"] == "vk_social_wall_post"
    assert runtime.build_vk_wall_post_opportunity(official_surface, post=post, default_target_url="https://t.me/kenigevents") is None


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


def test_runtime_deadline_helpers_stop_after_budget(monkeypatch):
    runtime = load_runtime()
    monkeypatch.setenv("ACQ_RUNTIME_DEADLINE_SECONDS", "1")

    deadline = runtime._deadline_after_seconds()

    assert isinstance(deadline, float)
    assert runtime._deadline_reached(deadline - 10) is True
    assert runtime._deadline_reached(None) is False


def test_seen_context_is_skipped_before_spending_gemma(monkeypatch):
    runtime = load_runtime()
    runtime.LLM_GATE_STATS["skipped_seen_context"] = 0
    opp = {"platform": "tg", "context_url": "https://t.me/example/1", "context_text_snippet": "Куда сходить?"}
    keys = set()
    diagnostics = []

    assert runtime._should_skip_opportunity_before_llm(
        opp,
        seen_contexts={"https://t.me/example/1"},
        opportunity_keys=keys,
        diagnostics=diagnostics,
    ) is True

    assert runtime.LLM_GATE_STATS["skipped_seen_context"] == 1
    assert not keys
    assert "before Gemma" in diagnostics[0]


def test_llm_gate_has_visible_per_run_budget(monkeypatch):
    runtime = load_runtime()
    runtime.LLM_GATE_STATS["calls"] = 0
    runtime.LLM_GATE_STATS["reserved"] = 0
    runtime.LLM_GATE_STATS["blocked_rate_limit"] = 0
    runtime.LLM_GATE_STATS["estimated_input_tokens"] = 0
    monkeypatch.setenv("ACQ_MAX_LLM_CALLS_PER_RUN", "1")

    diagnostics = []
    assert runtime._reserve_llm_gate_call("Куда сходить?", diagnostics) is True
    runtime.LLM_GATE_STATS["calls"] = 1
    assert runtime._reserve_llm_gate_call("Куда сходить?", diagnostics) is False

    snapshot = runtime._llm_limit_snapshot()
    assert snapshot["max_calls_per_run"] == 1
    assert snapshot["calls_used_this_run"] == 1
    assert snapshot["blocked_rate_limit"] == 1
    assert snapshot["estimated_input_tokens_this_run"] > 0


def test_kaggle_config_overrides_stale_acq_env(monkeypatch, tmp_path):
    runtime = load_runtime()
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"ACQ_ENABLE_LIVE_TG_SCAN": "1"}), encoding="utf-8")

    monkeypatch.setenv("ACQ_ENABLE_LIVE_TG_SCAN", "0")
    monkeypatch.setattr(runtime, "_find_input_file", lambda name: config_path if name == "config.json" else None)

    loaded = runtime._load_kaggle_env()
    assert loaded["config_loaded"] is True
    assert runtime.os.environ["ACQ_ENABLE_LIVE_TG_SCAN"] == "1"


def test_telega_in_kaliningrad_seeds_are_tagged():
    runtime = load_runtime()
    assert "https://t.me/anons39" in runtime.DEFAULT_TG_SEEDS
    surface = runtime._seed_surface("https://t.me/anons39", platform="tg")
    assert surface["source"] == "telega_in"
    assert "Telega.in" in surface["topic_hint"]


def test_llm_gate_rejects_low_confidence_acceptance(monkeypatch):
    runtime = load_runtime()
    surface = runtime._seed_surface("https://t.me/example", platform="tg")
    opp = {
        "platform": "tg",
        "surface_external_id": surface["external_id"],
        "context_url": "https://t.me/example/44",
        "context_text_snippet": "Куда сходить?",
        "matched_intent": "event_recommendation_question",
        "topic_cluster": "local_events",
        "link_target": {"kind": "pka_channel", "label": "ПКА", "url": "https://t.me/kenigevents"},
        "scores": {"relevance": 0.5, "spam_risk": "low", "safety_risk": "low"},
    }
    monkeypatch.setattr(runtime, "_google_api_key", lambda: ("test-key", "GOOGLE_API_KEY3"))
    monkeypatch.setattr(runtime, "_call_acq_llm_gate_sync", lambda _opp, _surface: {
        "is_candidate": True,
        "matched_intent": "event_recommendation_question",
        "topic_cluster": "local_events",
        "best_reply_strategy": "unclear",
        "target_kind": "pka_channel",
        "target_label": "ПКА",
        "target_url": "https://t.me/kenigevents",
        "reason": "weak",
        "relevance": 0.4,
        "spam_risk": "low",
        "safety_risk": "low",
        "checklist": [{"id": "need", "question": "Есть потребность?", "answer": True, "note": "да"}],
    })

    diagnostics = []
    assert runtime._llm_review_opportunity_sync(opp, surface, diagnostics) is None
    assert runtime.LLM_GATE_STATS["rejected_low_confidence"] == 1
    assert "relevance" in diagnostics[0]


def test_llm_gate_prompt_rejects_event_local_schedule_logistics():
    runtime = load_runtime()
    surface = runtime._seed_surface("https://vk.com/vagonka39", platform="vk")
    prompt = runtime._llm_gate_prompt({"platform": "vk", "context_text_snippet": "А где афиша 1 дня ?", "matched_intent": "event_recommendation_question", "topic_cluster": "local_events", "link_target": {}}, surface)
    assert "афиша/программа/расписание 1 дня" in prompt
    assert "локальная логистика текущего события" in prompt
    assert "общий канал" in prompt
    assert "льготы/скидки/билеты/условия/доступность" in prompt
    assert "venue policy" in prompt


def test_llm_gate_rejects_post_event_praise(monkeypatch):
    runtime = load_runtime()
    surface = runtime._seed_surface("https://vk.com/example", platform="vk")
    opp = {
        "platform": "vk",
        "surface_external_id": surface["external_id"],
        "context_url": "https://vk.com/wall-1_2?reply=3",
        "context_text_snippet": "Погода была отличная, компания веселая. Спасибо организаторам за прекрасно проведенное время❤",
        "matched_intent": "organizer_partnership",
        "topic_cluster": "organizer_partnership",
        "link_target": {"kind": "topic_landing", "label": "Добавить событие", "url": "https://kenigevents.ru/partnerstvo/"},
        "scores": {"relevance": 0.6, "spam_risk": "low", "safety_risk": "low"},
    }
    monkeypatch.setattr(runtime, "_google_api_key", lambda: ("test-key", "GOOGLE_API_KEY3"))
    monkeypatch.setattr(runtime, "_call_acq_llm_gate_sync", lambda _opp, _surface: {
        "is_candidate": False,
        "matched_intent": "none",
        "topic_cluster": "none",
        "best_reply_strategy": "no_reply",
        "target_kind": "none",
        "target_label": "",
        "target_url": "",
        "reason": "Постфактум-отзыв и благодарность, нет вопроса или полезного acquisition-ответа.",
        "relevance": 0.0,
        "spam_risk": "low",
        "safety_risk": "low",
        "checklist": [{"id": "need", "question": "Есть потребность?", "answer": False, "note": "только благодарность"}],
    })

    diagnostics = []
    assert runtime._llm_review_opportunity_sync(opp, surface, diagnostics) is None
    assert runtime.LLM_GATE_STATS["rejected"] == 1
    assert "rejected" in diagnostics[0]


def test_llm_gate_accepts_and_persists_checklist(monkeypatch):
    runtime = load_runtime()
    surface = runtime._seed_surface("https://t.me/example", platform="tg")
    opp = runtime.build_opportunity_from_message(
        surface,
        SimpleNamespace(id=44, message="Куда сходить с ребёнком на выходных в Калининграде?", date=datetime(2026, 7, 1, tzinfo=timezone.utc)),
        default_target_url="https://t.me/kenigevents",
    )
    assert opp is not None
    monkeypatch.setattr(runtime, "_google_api_key", lambda: ("test-key", "GOOGLE_API_KEY3"))
    monkeypatch.setattr(runtime, "_call_acq_llm_gate_sync", lambda _opp, _surface: {
        "is_candidate": True,
        "matched_intent": "event_recommendation_request",
        "topic_cluster": "kids_weekend_events",
        "best_reply_strategy": "short_native_reply_with_one_relevant_link",
        "target_kind": "pka_channel",
        "target_label": "Полюбить Калининград Анонсы",
        "target_url": "https://t.me/kenigevents",
        "reason": "Есть явный запрос куда сходить с ребёнком на выходных.",
        "relevance": 0.92,
        "spam_risk": "low",
        "safety_risk": "low",
        "checklist": [
            {"id": "question", "question": "Есть вопрос?", "answer": True, "note": "куда сходить"},
            {"id": "future_need", "question": "Будущая потребность?", "answer": True, "note": "на выходных"},
        ],
    })

    accepted = runtime._llm_review_opportunity_sync(opp, surface, [])

    assert accepted is not None
    assert accepted["scores"]["source"] == "gemma4_acquisition_gate"
    assert accepted["scores"]["relevance"] == 0.92
    assert accepted["evidence"]["llm_gate"]["model"].startswith("models/gemma-4")
    assert accepted["evidence"]["llm_gate"]["checklist"][0]["answer"] is True


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
    assert env["ACQ_ENABLE_LLM_GATE"] == "1"
    assert env["ACQ_GOOGLE_KEY_ENV"] == "GOOGLE_API_KEY3"
    assert env["ACQ_LLM_MODEL"].startswith("models/gemma-4")
    assert env["ACQ_LLM_GATE_MIN_RELEVANCE"]
    assert env["ACQ_RUNTIME_DEADLINE_SECONDS"]
    assert env["ACQ_MAX_LLM_CALLS_PER_RUN"]
    assert "ACQ_TG_SEARCH_MESSAGES_PER_QUERY" in env
    assert "ACQ_MAX_VK_SURFACES_PER_RUN" in env
    assert "ACQ_MAX_VK_POSTS_PER_SURFACE" in env
    assert "ACQ_MAX_VK_COMMENTS_PER_POST" in env


def test_smartik_vk_seeds_are_configured():
    from subscriber_acquisition.kaggle_runner import SMARTIK_KALININGRAD_VK_SEEDS

    handles = {handle for handle, _title, _url in SMARTIK_KALININGRAD_VK_SEEDS}

    assert "club42481124" in handles
    assert "club31556867" in handles
    assert all(url.startswith("https://smartik.ru/kaliningrad/group/") for _handle, _title, url in SMARTIK_KALININGRAD_VK_SEEDS)


def test_vk_scanner_prefers_live_comment_threads_static():
    source = Path("kaggle/SubscriberAcquisitionDiscovery/subscriber_acquisition_discovery.py").read_text(encoding="utf-8")
    assert '"filter": "all"' in source
    assert '"sort": "desc"' in source
    assert "posts_without_comments_skipped" in source
    assert "rate_limit_backoffs" in source


def test_kaggle_secrets_use_isolated_gemma_key_lane(monkeypatch):
    from subscriber_acquisition.kaggle_runner import _build_secrets_payload

    monkeypatch.setenv("GOOGLE_API_KEY3", "key3")
    monkeypatch.setenv("GOOGLE_API_KEY", "generic-key")
    monkeypatch.delenv("ACQ_ALLOW_GOOGLE_KEY_FALLBACKS", raising=False)

    payload = json.loads(_build_secrets_payload())

    assert payload["GOOGLE_API_KEY3"] == "key3"
    assert "GOOGLE_API_KEY" not in payload


def test_runtime_env_passes_seen_context_urls():
    from subscriber_acquisition.config import AcqConfig
    from subscriber_acquisition.kaggle_runner import _runtime_env_from_config

    env = _runtime_env_from_config(AcqConfig(), {"surfaces": [], "seen_opportunities": [{"context_url": "https://t.me/example/1"}]})

    assert env["ACQ_SEEN_CONTEXT_URLS_JSON"] == '["https://t.me/example/1"]'


def test_remote_session_marker_cooldown_blocks_fast_reuse(monkeypatch, tmp_path):
    from subscriber_acquisition import kaggle_runner

    marker = tmp_path / "remote-session.json"
    monkeypatch.setenv("ACQ_ENABLE_LIVE_TG_SCAN", "1")
    monkeypatch.setenv("ACQ_REMOTE_SESSION_MARKER_PATH", str(marker))
    monkeypatch.setenv("ACQ_REMOTE_SESSION_COOLDOWN_SECONDS", "600")

    kaggle_runner._write_remote_session_marker(state="complete", run_id="run-1", kernel_ref="owner/kernel")
    remaining, payload = kaggle_runner._remote_session_cooldown_remaining()

    assert remaining > 0
    assert payload["run_id"] == "run-1"
    assert payload["state"] == "complete"


def test_discovery_auth_scope_none_when_tg_scan_disabled(monkeypatch):
    from subscriber_acquisition import kaggle_runner

    monkeypatch.setenv("ACQ_ENABLE_LIVE_TG_SCAN", "0")

    assert kaggle_runner.discovery_remote_auth_scope() == "none"


def test_seen_context_urls_env_parser(monkeypatch):
    runtime = load_runtime()
    monkeypatch.setenv("ACQ_SEEN_CONTEXT_URLS_JSON", '["https://t.me/example/1"]')

    assert runtime._seen_context_urls() == {"https://t.me/example/1"}


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
