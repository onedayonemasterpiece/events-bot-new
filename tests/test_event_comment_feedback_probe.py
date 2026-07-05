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
