from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "kaggle" / "EventCommentFeedback" / "event_comment_feedback_discovery.py"


def load_module(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("ecf_probe", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "WORK", tmp_path)
    monkeypatch.setattr(mod, "STATUS_PATH", tmp_path / "status.jsonl")
    return mod


def test_phrase_bank_json_has_new_practical_classes(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    phrases = mod.parse_phrase_bank(ROOT / "docs/features/event-comment-feedback/phrase-bank-v1.json")
    by_id = {p["id"]: p for p in phrases}
    assert by_id["ticket_purchase_technical_problem"]["singular_safe"] is True
    assert by_id["source_copy_or_official_reply"]["publishable"] is False
    assert by_id["performance_praised"]["card_title"] == "Хвалят постановку"


def test_adaptive_source_comment_cap(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    assert mod.source_comment_cap({"metric_comments": 0}, 300) == 60
    assert mod.source_comment_cap({"metric_comments": 20}, 300) == 30
    assert mod.source_comment_cap({"metric_comments": 80}, 300) == 100
    assert mod.source_comment_cap({"metric_comments": 600}, 300) == 300
    assert mod.source_comment_cap({"metric_comments": 600}, 120) == 120


def test_guards_ticket_and_accessibility_false_positive(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    assert "sold_out_without_ticket_topic" in mod.guard("как же жалко расставаться", "sold_out_disappointment")
    assert "accessibility_confused_with_ticket_purchase" in mod.guard("не работает ссылка на покупку билета", "accessibility_concern")
    assert "ticket_resale_or_private_ticket_request" in mod.guard("приму в дар 2 билета", "ticket_availability_question")


def test_fetch_error_summary_buckets(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    summary = mod.write_fetch_error_summary(
        [
            {"platform": "vk", "platform_post_key": "vk:-1:2", "status": "not_accessible_or_deleted", "code": 15, "message": "Access denied", "token": "secret"},
            {"platform": "telegram", "platform_post_key": "tg:x:1", "status": "error", "error_type": "MsgIdInvalidError", "message": "bad"},
        ],
        [{"platform": "telegram", "platform_post_key": "tgid:1:2", "skip_reason": "telegram_missing_username_or_chat_id"}],
        [{"platform": "vk", "platform_post_key": "vk:-1:2", "cap": 100, "fetched": 0}],
    )
    assert summary["errors_total"] == 2
    assert summary["skipped_total"] == 1
    assert (tmp_path / "fetch_error_summary.json").exists()
    text = (tmp_path / "fetch_error_summary.json").read_text(encoding="utf-8")
    assert "secret" not in text
    buckets = json.loads(text)["buckets"]
    assert any(b["code"] == 15 for b in buckets)


def test_p1_prefilter_false_positive_examples(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    cases = {
        "Есть подработка на лето,оплата от 8000 в день": "job_spam_or_earnings_ad",
        "Такой же вопрос": "contextless_short_reply",
        "Администрация, а что помешало убрать грунт с дороги и тротуара?": "municipal_road_complaint_offtopic",
        "блин как круто выглядит саша петров на афише": "poster_or_announcement_reaction",
        "Питухи крутые бойцовские птицы": "offtopic_meme_or_noise",
        "Благодарю за подсказку 🙏🙏🙏": "contextless_short_reply",
        "Добрый день! В последнем посте с благодарностями есть ссылки на них.": "official_reply",
    }
    for text, expected in cases.items():
        assert mod.classify_comment_type(text)[0] == expected


def test_p1_phrase_specific_anchors(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    assert "phrase_lexical_guard" in mod.guard("Там есть купить какие-то снеки, еда, чтобы на трибунах перекусить?", "ticket_availability_question")
    assert "phrase_lexical_guard" in mod.guard("Здравствуйте! Можно взять с собой планшет?", "barcode_or_eticket_question")
    assert "phrase_lexical_guard" in mod.guard("Вместо бесплатного концерта можно нам дороги отремонтировать?", "parking_questions")
    assert mod.guard("А бутылку воды можно взять?", "food_drinks_question") == []
    assert "question_phrase_without_direct_question_or_problem" in mod.guard("Здравствуйте. Есть штрих-код билетов и номера билетов", "barcode_or_eticket_question")
    assert mod.guard("Есть ли штрих-код билетов и номера билетов?", "barcode_or_eticket_question") == []


def test_p1_public_candidate_gate_is_strict(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    phrase = {"tone": "neutral", "singular_safe": False}
    assert mod.public_candidate_gate(phrase, {"model_agreement": False, "e5_rank": 1, "bge_rank": 1, "sparse_score": 0.9, "positive_margin": 0.2}) is False
    assert mod.public_candidate_gate(phrase, {"model_agreement": True, "e5_rank": 3, "bge_rank": 1, "sparse_score": 0.9, "positive_margin": 0.2}) is False
    assert mod.public_candidate_gate(phrase, {"model_agreement": True, "e5_rank": 1, "bge_rank": 1, "sparse_score": 0.05, "positive_margin": 0.2}) is False
    assert mod.public_candidate_gate(phrase, {"model_agreement": True, "e5_rank": 1, "bge_rank": 2, "sparse_score": 0.12, "positive_margin": 0.08}) is True
    assert mod.public_candidate_gate(phrase, {"model_agreement": True, "e5_rank": 1, "bge_rank": 2, "sparse_score": 0.12, "positive_margin": 0.01}) is False


def test_p15_official_reply_prefix_and_question_gate(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    assert mod.strip_vk_mention_prefix("[id123|Мария], здравствуйте! Да, вход с водой запрещен") == "здравствуйте! Да, вход с водой запрещен"
    assert mod.classify_comment_type("[id123|Мария], здравствуйте! Да, на территории стадиона предусмотрены точки питания")[0] == "official_reply"
    assert "question_phrase_without_direct_question_or_problem" in mod.guard("Да, вход с любыми напитками запрещен", "admission_rules_question")
    assert mod.guard("Можно ли взять воду?", "food_drinks_question") == []
    assert mod.guard("Не открывается регистрация", "registration_interest") == []


def test_p15_site_flags_and_capability_cache(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    import datetime as dt
    now = dt.datetime(2026, 7, 6, tzinfo=dt.timezone.utc)
    assert mod.event_site_flags({"date": "2026-07-04"}, now)["eligible_for_site_export"] is False
    assert mod.event_site_flags({"date": "2026-07-06"}, now)["eligible_for_site_export"] is True
    payload = mod.write_source_capability_cache(
        [{"platform":"vk","platform_post_key":"vk:-1:2","status":"not_accessible_or_deleted","code":15}],
        [{"platform":"telegram","platform_post_key":"tgid:1:2","skip_reason":"telegram_missing_username_or_chat_id"}],
        [{"platform":"vk","platform_post_key":"vk:-1:3","fetched":2,"scanned":2}],
    )
    caps = {r["platform_post_key"]: r["comments_capability"] for r in payload["sources"]}
    assert caps["vk:-1:2"] == "forbidden_or_deleted"
    assert caps["tgid:1:2"] == "entity_resolution_failed"
    assert caps["vk:-1:3"] == "available"


def test_p16_official_question_false_positive_is_user(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    assert mod.classify_comment_type("Добрый день, как можно связаться с музыкантами?")[0] == "user_feedback"
    assert mod.classify_comment_type("Здравствуйте! Сейчас нет информации, когда будет следующий тираж. Следите за новостями")[0] == "official_reply"


def test_p16_non_question_phrases_do_not_get_question_guard(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    assert "question_phrase_without_direct_question_or_problem" not in mod.guard("Блин. Надо идтить...", "intent_to_attend")
    assert "question_phrase_without_direct_question_or_problem" in mod.guard("Да, вход с любыми напитками запрещен", "admission_rules_question")


def test_p16_capability_enum_mapping(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    cases = [
        ({"skip_reason": "capability_no_comments"}, "no_comments"),
        ({"skip_reason": "capability_no_discussion_or_deleted"}, "no_discussion_or_deleted"),
        ({"status": "error", "error_type": "ValueError", "message": "could not find entity"}, "entity_resolution_failed"),
        ({"status": "not_accessible_or_deleted", "code": 15}, "forbidden_or_deleted"),
    ]
    for item, expected in cases:
        assert mod.derive_comments_capability(item)[0] == expected


def test_p16_state_payload_counts(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    comments = [
        {"comment_key": "vk:1", "text": "Отлично", "platform_post_key": "vk:p", "links": [{"event_id": 1}]},
        {"comment_key": "vk:2", "text": "Вопрос?", "platform_post_key": "vk:p", "links": [{"event_id": 2}]},
    ]
    prev = {"comments": {"vk:1": {"first_seen_at": "2026-07-01T00:00:00+00:00"}}, "source_capabilities": {"old": {}}}
    cache = {"sources": [{"platform_post_key": "vk:p", "comments_capability": "available", "last_checked_at": "now", "next_check_after": None}]}
    payload, stats = mod.build_state_payload(comments, cache, prev, "file_incremental")
    assert stats["state_mode"] == "file_incremental"
    assert stats["comments_known_before"] == 1
    assert stats["comments_known_after"] == 2
    assert stats["new_comments_this_run"] == 1
    assert stats["comments_reused_from_cache"] == 1
    assert payload["source_capabilities"]["vk:p"]["comments_capability"] == "available"


def test_p16_site_status_for_past_gate_pass(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    site_status, public_status = mod.site_export_status_for("semantic_public_gate_pass", {"eligible_for_site_export": False, "is_past_event": True})
    assert site_status == "site_ineligible_past_event"
    assert not public_status.startswith("public_ready")
    site_status, public_status = mod.site_export_status_for("semantic_public_gate_pass", {"eligible_for_site_export": True, "is_past_event": False})
    assert site_status == "site_public_ready_social_proof"
    assert public_status == "site_public_ready_social_proof"


def test_p17_phrase_bank_signal_layers_and_atomic_classes(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    phrases = mod.parse_phrase_bank(ROOT / "docs/features/event-comment-feedback/phrase-bank-v1.json")
    by_id = {p["id"]: p for p in phrases}
    expected = {
        "ticket_sales_start_question",
        "registration_timing_question",
        "registration_friction_or_closed",
        "ticket_sector_availability_question",
        "additional_dates_question",
        "time_conflict_question",
        "transfer_question",
        "lineup_announcement_question",
        "single_intent_to_attend",
        "single_anticipation_signal",
        "past_visit_repeat_intent",
    }
    assert expected <= set(by_id)
    assert all(p.get("signal_layer") for p in phrases)
    assert by_id["ticket_sales_start_question"]["signal_layer"] == "practical_question"
    assert by_id["registration_friction_or_closed"]["signal_layer"] == "friction_problem"
    assert by_id["single_intent_to_attend"]["signal_layer"] == "single_social_signal"


def test_p17_exact_practical_gate_and_site_statuses(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    phrases = {p["id"]: p for p in mod.parse_phrase_bank(ROOT / "docs/features/event-comment-feedback/phrase-bank-v1.json")}
    assert mod.guard_phrase("А когда начнется продажа билетов?", phrases["ticket_sales_start_question"]) == []
    assert "exact_anchor_missing" in mod.guard_phrase("А можно ли узнать подробности?", phrases["ticket_sales_start_question"])
    assert "question_phrase_without_direct_question_or_problem" in mod.guard_phrase("Продажа билетов стартовала", phrases["ticket_sales_start_question"])
    flags = {"eligible_for_site_export": True, "is_past_event": False}
    assert mod.site_export_status_for("semantic_public_gate_pass", flags, "practical_question", "practical_single")[0] == "site_public_ready_practical_single"
    assert mod.site_export_status_for("semantic_public_gate_pass", flags, "friction_problem", "friction_single")[0] == "site_public_ready_friction_single"


def test_p17_missed_signal_audit_suggestions(tmp_path, monkeypatch):
    mod = load_module(tmp_path, monkeypatch)
    assert mod.audit_suggested_signal("А повтор будет? Другие даты планируются?")[:2] == ("additional_dates_question", "practical_question")
    assert mod.audit_suggested_signal("Не успела на регистрацию, мест нет")[:2] == ("registration_friction_or_closed", "friction_problem")
