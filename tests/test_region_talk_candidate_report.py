from __future__ import annotations

import importlib.util
import os
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "kaggle" / "RegionTalkCandidateReport" / "region_talk_candidate_report.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_candidate_report", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkCandidateReportTests(unittest.TestCase):
    def test_seed_csv_and_report_workbook(self) -> None:
        mod = load_module()
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        self.assertEqual(len(seeds), 30)
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            payload = mod.build_report(seeds, [], [], "unit-run", tmp_path)
            xlsx = tmp_path / "region-talk-candidates-unit-run.xlsx"
            self.assertTrue(payload["ok"])
            self.assertTrue(xlsx.exists())
            with zipfile.ZipFile(xlsx) as zf:
                names = set(zf.namelist())
                workbook = zf.read("xl/workbook.xml").decode("utf-8")
            self.assertIn("xl/styles.xml", names)
            self.assertIn("docProps/core.xml", names)
            self.assertIn("docProps/app.xml", names)
            self.assertIn("04_review_queue", workbook)
            self.assertIn("09_image_quality", workbook)
            self.assertIn("15_manual_decisions", workbook)
            self.assertIn("17_source_graph_edges", workbook)
            self.assertIn("18_place_lexicon_matches", workbook)
            summary = json.loads((tmp_path / "region-talk-candidates-unit-run.json").read_text(encoding="utf-8"))["summary"]
            self.assertEqual(summary["source_count_seeded"], 30)
            self.assertEqual(summary["posts_fetched"], 0)

    def test_text_and_media_scoring_strong_region_media(self) -> None:
        mod = load_module()
        text_score = mod.score_text("Калининград и Куршская коса: красивый маршрут, море, дюны и архитектура")
        media = mod.media_scores(True, text_score)
        self.assertGreater(text_score["region_relevance_score"], 0)
        self.assertGreaterEqual(media["postcardness_score"], 0.72)
        self.assertTrue(media["is_selected_for_publication"])

    def test_seed_v2_and_place_lexicon_contract(self) -> None:
        mod = load_module()
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v2.csv")
        self.assertGreaterEqual(len(seeds), 300)
        lexicon = mod.load_place_lexicon(ROOT / "docs" / "features" / "region-talk-channel" / "kaliningrad-place-lexicon-v1.csv")
        names = {r["canonical_name"] for r in lexicon}
        for required in ["Краснолесье", "Виштынецкое озеро", "Роминтенская пуща", "Куршская коса", "Балтийская коса"]:
            self.assertIn(required, names)

    def test_kaliningrad_only_scope_rejects_multi_region_lists(self) -> None:
        mod = load_module()
        lexicon = mod.load_place_lexicon(ROOT / "docs" / "features" / "region-talk-channel" / "kaliningrad-place-lexicon-v1.csv")
        good = mod.kaliningrad_oblast_only_scope_gate("Маршрут: Калининград, Зеленоградск, Светлогорск и Куршская коса. Очень атмосферная поездка.", lexicon)
        self.assertTrue(good["kaliningrad_oblast_only_scope"])
        bad = mod.kaliningrad_oblast_only_scope_gate("Куда поехать летом: Байкал, Дагестан, Калининград и Сочи — 10 мест России", lexicon)
        self.assertFalse(bad["kaliningrad_oblast_only_scope"])
        self.assertIn("байкал", bad["external_geo_mentions"])

    def test_semantic_meaning_requires_llm_not_regex_rejection(self) -> None:
        mod = load_module()
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [
            {"post_id":"post_ad", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/1", "platform_post_key":"tg:src:1", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград и Куршская коса: зарегистрируйтесь на географический диктант, билеты и программа мероприятия", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"},
            {"post_id":"post_old", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/2", "platform_post_key":"tg:src:2", "post_date":"2025-12-31T12:00:00+00:00", "text":"Калининград, Зеленоградск и Куршская коса — красивый маршрут, впечатления и полезные детали поездки", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"},
        ]
        with tempfile.TemporaryDirectory() as td:
            payload = mod.build_report(seeds, [], posts, "gate-run", Path(td))
        dropped = payload["sheets"]["08_dropped_posts"]
        review = payload["sheets"]["04_review_queue"]
        dropped_reasons = {r["post_id"]: r["rejection_reason"] for r in dropped}
        self.assertEqual(dropped_reasons["post_old"], "reject_stale_or_missing_date")
        by_review_id = {r["post_id"]: r for r in review}
        self.assertEqual(by_review_id["post_ad"]["rejection_reason"], "semantic_gate_not_run")
        self.assertEqual(by_review_id["post_ad"]["current_stage"], "pre_candidate_needs_llm")
        self.assertIn("deterministic_ad_promo_evidence", by_review_id["post_ad"]["semantic_evidence_flags"])
        self.assertEqual(payload["summary"]["image_model_calls"], 0)
        self.assertEqual(payload["summary"]["pre_candidates_created"], 1)
        self.assertTrue(all(r["image_scoring_skipped"] == "true" for r in dropped + review))

    def test_ambiguous_place_requires_context(self) -> None:
        mod = load_module()
        lexicon = mod.load_place_lexicon(ROOT / "docs" / "features" / "region-talk-channel" / "kaliningrad-place-lexicon-v1.csv")
        ant = mod.kaliningrad_oblast_only_scope_gate("Рыжий лесной муравей строит огромные муравейники в тайге", lexicon)
        self.assertFalse(ant["kaliningrad_oblast_only_scope"])
        self.assertIn("Лесной", ant["ambiguous_place_names"])
        kosa = mod.kaliningrad_oblast_only_scope_gate("Лесной, Куршская коса, Калининградская область — спокойная остановка маршрута", lexicon)
        self.assertTrue(kosa["kaliningrad_oblast_only_scope"])

    def test_llm_accept_allows_image_scoring(self) -> None:
        mod = load_module()
        old_max = os.environ.get("REGION_TALK_MAX_LLM_CALLS")
        os.environ["REGION_TALK_MAX_LLM_CALLS"] = "1"
        try:
            mod.call_region_talk_semantic_llm = lambda post, evidence: {"llm_gate_status":"ok", "llm_model":"fake", "llm_decision":"accept", "whole_post_about_kaliningrad_oblast_score":0.9, "kaliningrad_mention_role":"main_subject", "is_digest_or_roundup":"false", "is_multi_topic_digest":"false", "llm_is_ad_or_promo":"false", "llm_is_news_or_trash":"false", "llm_content_type":"visit_impression_candidate", "llm_reason":"ok"}
            seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
            posts = [{"post_id":"post_ok", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/3", "platform_post_key":"tg:src:3", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград и Куршская коса: красивый маршрут, личные впечатления, море, дюны и что особенно запомнилось", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}]
            with tempfile.TemporaryDirectory() as td:
                payload = mod.build_report(seeds, [], posts, "llm-accept-run", Path(td))
            self.assertEqual(payload["summary"]["llm_calls"], 1)
            self.assertEqual(payload["summary"]["image_model_calls"], 1)
            self.assertTrue(payload["sheets"]["09_image_quality"])
        finally:
            if old_max is None:
                os.environ.pop("REGION_TALK_MAX_LLM_CALLS", None)
            else:
                os.environ["REGION_TALK_MAX_LLM_CALLS"] = old_max


if __name__ == "__main__":
    unittest.main()
