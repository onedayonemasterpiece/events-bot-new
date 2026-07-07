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
            self.assertIn("09a_image_candidate_queue", workbook)
            self.assertIn("09b_image_fetch_retry_queue", workbook)
            self.assertIn("09c_image_debug_fallback", workbook)
            self.assertIn("09d_image_driven_top", workbook)
            self.assertIn("15_manual_decisions", workbook)
            self.assertIn("17_source_graph_edges", workbook)
            self.assertIn("18_place_lexicon_matches", workbook)
            self.assertIn("12a_source_frontier_unique", workbook)
            self.assertIn("12_source_queue", workbook)
            self.assertIn("12a_active_tg_vk_frontier", workbook)
            self.assertIn("12b_telegram_similar_channels", workbook)
            self.assertIn("12d_similar_seed_queue", workbook)
            self.assertIn("20_telegram_rate_observability", workbook)
            self.assertIn("04a_current_run_shortlist", workbook)
            self.assertIn("04k_keyword_hit_candidates", workbook)
            self.assertIn("06a_candidate_memory", workbook)
            self.assertIn("06b_candidate_memory_top", workbook)
            self.assertIn("07b_prev_candidates_not_refetch", workbook)
            self.assertIn("12c_source_frontier_queue_next", workbook)
            self.assertIn("13b_source_delta_scan", workbook)
            self.assertIn("12e_telegram_keyword_discovery", workbook)
            self.assertIn("12e_keyword_posts", workbook)
            self.assertIn("12f_source_classification", workbook)
            self.assertIn("12g_external_links_quarantine", workbook)
            self.assertIn("21_manual_review_queue", workbook)
            self.assertIn("22_candidate_deltas", workbook)
            self.assertIn("14d_llm_usage_by_stage", workbook)
            self.assertIn("23_vk_wall_setup", workbook)
            self.assertIn("24_source_yield_metrics", workbook)
            summary = json.loads((tmp_path / "region-talk-candidates-unit-run.json").read_text(encoding="utf-8"))["summary"]
            self.assertEqual(summary["source_count_seeded"], 30)
            self.assertEqual(summary["posts_fetched"], 0)
        self.assertIn("telegram_similar_channels_status", summary)
        self.assertIn("history_sources_target", summary)
        self.assertIn("similar_seed_queue_total", summary)
        self.assertIn("previous_state_loaded", summary)
        self.assertIn("sources_primary_scanned_total_all_time", summary)
        self.assertIn("keyword_search_queries_processed", summary)
        self.assertIn("source_queue_total", summary)
        self.assertIn("image_queue_total", summary)
        self.assertEqual(summary["favorites_candidates_consistency_status"], "ok")

    def test_text_and_media_scoring_strong_region_media(self) -> None:
        mod = load_module()
        text_score = mod.score_text("Калининград и Куршская коса: красивый маршрут, море, дюны и архитектура")
        media = mod.media_scores(True, text_score)
        self.assertGreater(text_score["region_relevance_score"], 0)
        self.assertGreaterEqual(media["postcardness_score"], 0.72)
        self.assertFalse(media["is_selected_for_publication"])
        self.assertEqual(media["image_publication_ready"], "false")
        self.assertEqual(media["image_reviewable"], "false")
        self.assertEqual(media["failure_reason"], "needs_actual_image_fetch")

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

    def test_visit_classifier_populates_story_fields(self) -> None:
        mod = load_module()
        fields = mod.infer_visit_semantic_fields(
            "Мы приехали в Калининград на выходные, прогулялись по Амалиенау, нам понравилось море и вот наш маршрут.",
            {"content_type":"visit_impression_candidate"},
            {"visit_impression_score":0.4, "emotion_observation_score":0.3, "useful_route_score":0.3},
            {"has_media": True},
        )
        self.assertEqual(fields["has_firsthand_visit_evidence"], "true")
        self.assertIn("мы приехали", fields["first_person_markers"])
        self.assertEqual(fields["useful_route_evidence"], "true")
        self.assertGreater(float(fields["publication_story_score"]), 0.5)

    def test_similar_seed_queue_usage_is_merged(self) -> None:
        mod = load_module()
        mod._REGION_TALK_TELEGRAM_RUNTIME["similar_seed_updates"] = {
            "similar_seed_" + mod.stable_hash("https://t.me/source_a"): {
                "similar_last_used_at": "2026-07-06T00:00:00+00:00",
                "similar_last_scanned_at": "2026-07-06T00:00:00+00:00",
                "similar_use_count_increment": 1,
                "similar_last_result_count": 50,
                "similar_last_unique_count": 12,
                "similar_next_allowed_at": "2026-07-13T00:00:00+00:00",
            }
        }
        rows = mod.build_similar_seed_queue({}, [{
            "platform": "telegram", "fetch_status": "ok", "source_id": "src_a",
            "canonical_url": "https://t.me/source_a", "source_title": "A",
            "monitor_priority_score": 0.5, "source_kind": "travel",
        }], [], "run-usage", "2026-07-06T00:01:00+00:00")
        self.assertEqual(rows[0]["similar_seed_use_count"], 1)
        self.assertEqual(rows[0]["similar_seed_last_result_count"], 50)
        self.assertEqual(rows[0]["similar_seed_last_unique_count"], 12)

    def test_hard_region_gate_blocks_non_ko_rows_before_memory_and_shortlist(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        calls = {"n": 0}
        def fake_llm(*args, **kwargs):
            calls["n"] += 1
            return {"llm_gate_status":"ok", "llm_decision":"accept", "llm_reason":"should not be called for hard region rejects"}
        mod.call_region_talk_semantic_llm = fake_llm
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        base = {"source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_date":"2026-06-01T12:00:00+00:00", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}
        posts = [
            {**base, "post_id":"ko_ok", "post_url":"https://t.me/src/ko", "platform_post_key":"tg:src:ko", "text":"Калининград, Зеленоградск и Куршская коса: личные впечатления от поездки, море, дюны, маршрут и что особенно запомнилось."},
        ]
        for idx, place in enumerate(["Санкт-Петербург", "Москва", "Самара", "Челябинск", "Якутия", "Владимир"], start=1):
            posts.append({**base, "post_id":f"not_ko_{idx}", "post_url":f"https://t.me/src/nonko{idx}", "platform_post_key":f"tg:src:nonko{idx}", "text":f"{place} и Калининград: подборка поездок по России, впечатления, красивые места, маршруты и что посмотреть летом."})
        with tempfile.TemporaryDirectory() as td:
            payload = mod.build_report(seeds, [], posts, "hard-region-run", Path(td))
        false_urls = {f"https://t.me/src/nonko{i}" for i in range(1, 7)}
        for sheet_name in ("06a_candidate_memory", "04a_final_shortlist", "21_manual_review_queue"):
            urls = {r.get("post_url") for r in payload["sheets"][sheet_name] if isinstance(r, dict)}
            self.assertFalse(false_urls & urls, sheet_name)
        dropped = {r.get("post_url"): r for r in payload["sheets"]["08_dropped_posts"]}
        self.assertEqual({dropped[u]["rejection_reason"] for u in false_urls}, {"vector_reject_multi_region_roundup"})
        self.assertEqual(payload["summary"]["wide_funnel_llm_calls"], 0)
        self.assertEqual(calls["n"], 0)
        memory_urls = {r.get("post_url") for r in payload["sheets"]["06a_candidate_memory"] if isinstance(r, dict)}
        self.assertIn("https://t.me/src/ko", memory_urls)

    def test_semantic_meaning_requires_llm_not_regex_rejection(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
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
        self.assertIn(dropped_reasons["post_ad"], {"reject_ad_or_promo", "vector_reject_ad_promo", "vector_reject_news_event"})
        by_dropped_id = {r["post_id"]: r for r in dropped}
        self.assertIn("deterministic_ad_promo_evidence", by_dropped_id["post_ad"]["semantic_evidence_flags"])
        self.assertEqual(payload["summary"]["image_model_calls"], 0)
        self.assertEqual(payload["summary"]["pre_candidates_created"], 0)
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
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        mod.call_region_talk_semantic_llm = lambda post, evidence, **kwargs: {"llm_gate_status":"ok", "llm_provider":"google_gemini", "llm_model":"fake", "llm_default_env_var_name":"GOOGLE_API_KEY", "llm_limit_source":"supabase_google_ai_reserve", "llm_decision":"accept", "whole_post_about_kaliningrad_oblast_score":0.9, "kaliningrad_mention_role":"main_subject", "is_digest_or_roundup":"false", "is_multi_topic_digest":"false", "llm_is_ad_or_promo":"false", "llm_is_news_or_trash":"false", "llm_content_type":"visit_impression_candidate", "llm_reason":"ok"}
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{"post_id":"post_ok", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/3", "platform_post_key":"tg:src:3", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград и Куршская коса: красивый маршрут, личные впечатления, море, дюны и что особенно запомнилось", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}]
        with tempfile.TemporaryDirectory() as td:
            payload = mod.build_report(seeds, [], posts, "llm-accept-run", Path(td))
        self.assertEqual(payload["summary"]["wide_funnel_llm_calls"], 0)
        self.assertEqual(payload["summary"]["llm_calls"], 0)
        self.assertEqual(payload["summary"]["llm_limit_source"], "supabase_google_ai")
        self.assertEqual(payload["summary"]["image_model_calls"], 1)
        self.assertEqual(payload["summary"]["text_vector_rows_scored"], 1)
        self.assertTrue(payload["sheets"]["09_image_quality"])

    def test_llm_error_has_retry_sheet(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        mod.call_region_talk_semantic_llm = lambda post, evidence, **kwargs: {"llm_gate_status":"rate_limited", "llm_provider":"google_gemini", "llm_model":"fake", "llm_default_env_var_name":"GOOGLE_API_KEY", "llm_limit_source":"supabase_google_ai_reserve", "llm_reason":"RateLimitError: rpd"}
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{"post_id":"post_retry", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/4", "platform_post_key":"tg:src:4", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград и Куршская коса: красивый маршрут, море, дюны, Светлогорск и полезные впечатления от поездки", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}]
        with tempfile.TemporaryDirectory() as td:
            payload = mod.build_report(seeds, [], posts, "llm-retry-run", Path(td))
        self.assertEqual(payload["summary"]["wide_funnel_llm_calls"], 0)
        self.assertEqual(payload["summary"]["llm_retry_rows"], 0)
        self.assertEqual(payload["sheets"]["14d_llm_usage_by_stage"][0]["llm_calls"], 0)

    def test_obvious_news_event_rejected_before_llm(self) -> None:
        mod = load_module()
        calls = {"n": 0}
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        def fake_llm(*args, **kwargs):
            calls["n"] += 1
            return {"llm_gate_status":"ok", "llm_decision":"accept"}
        mod.call_region_talk_semantic_llm = fake_llm
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{"post_id":"post_news_event", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/41", "platform_post_key":"tg:src:41", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград: официальный анонс мероприятия, регистрация, билеты, расписание и программа конкурса", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}]
        with tempfile.TemporaryDirectory() as td:
            payload = mod.build_report(seeds, [], posts, "news-vector-run", Path(td))
        row = payload["sheets"]["08_dropped_posts"][0]
        self.assertEqual(calls["n"], 0)
        self.assertIn(row["rejection_reason"], {"reject_ad_or_promo", "vector_reject_news_event", "vector_reject_ad_promo"})
        self.assertIn(row["llm_status"], {"not_called_vector_reject", "not_called_until_final_verifier"})
        self.assertEqual(payload["summary"]["wide_funnel_llm_calls"], 0)

    def test_news_and_external_homonyms_are_vector_rejected_before_memory(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        base = {"source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_date":"2026-06-29T12:00:00+00:00", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}
        posts = [
            {**base, "post_id":"news_kalina", "post_url":"https://t.me/src/news1", "platform_post_key":"tg:src:news1", "text":"Уголовное дело в отношении директора нацпарка Куршская коса возбуждено из-за незаконной вырубки деревьев. Следствие сообщает подробности."},
            {**base, "post_id":"moscow_sokolniki", "post_url":"https://t.me/src/moscow1", "platform_post_key":"tg:src:moscow1", "text":"Отправляемся гулять в обновленный парк Сокольники. Здесь можно насладиться природой. #Москва_в_объективе"},
            {**base, "post_id":"ko_visit", "post_url":"https://t.me/src/ko1", "platform_post_key":"tg:src:ko1", "text":"Вчера ездили на Куршскую косу из Зеленоградска: дюны, море, маршрут, личные впечатления и что особенно запомнилось."},
        ]
        with tempfile.TemporaryDirectory() as td:
            payload = mod.build_report(seeds, [], posts, "vector-news-run", Path(td))
        memory_urls = {r.get("post_url") for r in payload["sheets"]["06a_candidate_memory"] if isinstance(r, dict)}
        self.assertNotIn("https://t.me/src/news1", memory_urls)
        self.assertNotIn("https://t.me/src/moscow1", memory_urls)
        self.assertIn("https://t.me/src/ko1", memory_urls)
        dropped = {r.get("post_url"): r for r in payload["sheets"]["08_dropped_posts"]}
        self.assertEqual(dropped["https://t.me/src/news1"]["rejection_reason"], "vector_reject_news_event")
        self.assertEqual(dropped["https://t.me/src/moscow1"]["rejection_reason"], "vector_reject_not_kaliningrad_oblast")
        self.assertEqual(payload["summary"]["wide_funnel_llm_calls"], 0)

    def test_llm_sync_wrapper_works_inside_active_event_loop(self) -> None:
        mod = load_module()
        class FakeClient:
            async def generate_content_async(self, **kwargs):
                class Usage:
                    input_tokens = 1
                    output_tokens = 1
                    total_tokens = 2
                return '{"decision":"accept","reason":"ok"}', Usage()
        mod.get_region_talk_llm_gateway = lambda default_env_var_name: FakeClient()
        async def inner():
            return mod.call_region_talk_semantic_llm({"text":"Калининград"}, {}, model="fake", default_env_var_name="GOOGLE_API_KEY3")
        result = __import__('asyncio').run(inner())
        self.assertEqual(result["llm_gate_status"], "ok")
        self.assertEqual(result["llm_decision"], "accept")

    def test_ad_promo_rubrika_is_not_ruble_price(self) -> None:
        mod = load_module()
        gate = mod.ad_promo_gate("В рубрике про Зеленоградск — прогулка, море и красивые детали маршрута")
        self.assertFalse(gate["is_ad_or_promo"])
        self.assertEqual(gate["ad_promo_hits"], "")
        hard = mod.ad_promo_gate("Экскурсия по Калининграду: цена 1500 руб., регистрация обязательна")
        self.assertTrue(hard["is_ad_or_promo"])
        self.assertIn("price_rub", hard["ad_promo_hits"])

    def test_source_coverage_reports_vk_and_more_than_telegram_enabled(self) -> None:
        mod = load_module()
        seeds = [
            mod.Seed("tg1", "telegram", "TG1", "@viewrussia", "https://t.me/viewrussia", "travel", "", 1, "", "", "", "", "", "", True, "unknown", ""),
            mod.Seed("tg2", "telegram", "TG2", "@moya_planeta", "https://t.me/moya_planeta", "travel", "", 2, "", "", "", "", "", "", False, "unknown", ""),
            mod.Seed("vk1", "vk", "VK1", "@places", "https://vk.com/places", "travel", "", 1, "", "", "", "", "", "", True, "unknown", ""),
            mod.Seed("vv1", "vkvideo", "VV1", "@rgoclub", "https://vk.com/rgoclub", "travel", "", 1, "", "", "", "", "", "", True, "unknown", ""),
        ]
        os.environ["REGION_TALK_MAX_SOURCES"] = "4"
        os.environ["REGION_TALK_FETCH_TELEGRAM"] = "0"
        try:
            rows, posts = __import__("asyncio").run(mod.fetch_telegram_posts(seeds, mod.Status(), Path(tempfile.mkdtemp())))
        finally:
            os.environ.pop("REGION_TALK_MAX_SOURCES", None)
            os.environ.pop("REGION_TALK_FETCH_TELEGRAM", None)
        self.assertFalse(posts)
        statuses = {r["source_seed_id"]: r["fetch_status"] for r in rows}
        self.assertEqual(statuses["tg1"], "skipped_fetch_disabled")
        self.assertEqual(statuses["tg2"], "skipped_fetch_disabled")
        self.assertIn(statuses["vk1"], {"skipped_vk_wall_not_configured", "skipped_vk_wall_not_implemented", "skipped_fetch_disabled"})
        self.assertEqual(statuses["vv1"], "skipped_vkvideo_auxiliary_not_implemented")

    def test_weak_image_not_marked_reviewable_in_final_shortlist(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        mod.call_region_talk_semantic_llm = lambda post, evidence, **kwargs: {"llm_gate_status":"ok", "llm_provider":"google_gemini", "llm_model":"fake", "llm_default_env_var_name":"GOOGLE_API_KEY", "llm_limit_source":"supabase_google_ai_reserve", "llm_decision":"accept", "whole_post_about_kaliningrad_oblast_score":0.9, "kaliningrad_mention_role":"main_subject", "is_digest_or_roundup":"false", "is_multi_topic_digest":"false", "llm_is_ad_or_promo":"false", "llm_is_news_or_trash":"false", "llm_content_type":"encyclopedic_card_candidate", "content_type":"encyclopedic_card_candidate", "llm_reason":"ok"}
        mod.media_scores = lambda has_media, text_score, post=None: {"technical_quality_score":0.4,"aesthetic_score":0.4,"postcardness_score":0.4,"region_visual_relevance_score":0.5,"publication_safety_score":0.9,"low_noise_score":0.8,"overall_media_score":0.55,"is_selected_for_publication":False,"image_publication_ready":"false","image_reviewable":"false","image_quality_bucket":"weak_image","recognized_visual_elements":"","model_short_explanation":"weak","failure_reason":"below_reviewable_image_threshold","model_id":"fake","model_version":"test","image_model_type":"clip","image_model_runtime":"kaggle_local","image_model_input_type":"actual_image","image_scoring_mode":"cv_aesthetic_clip","image_model_device":"cpu","image_download_status":"downloaded_actual_image"}
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{"post_id":"post_weak_media", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/5", "platform_post_key":"tg:src:5", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград и Куршская коса: красивый маршрут, море, дюны и что особенно запомнилось в поездке", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}]
        with tempfile.TemporaryDirectory() as td:
            payload = mod.build_report(seeds, [], posts, "weak-image-run", Path(td))
        self.assertTrue(payload["sheets"]["04a_final_shortlist"])
        self.assertFalse(payload["sheets"]["04a_current_run_shortlist"])
        self.assertEqual(payload["sheets"]["04a_final_shortlist"][0]["decision_bucket"], "good_text_weak_actual_image")
        self.assertEqual(len(payload["sheets"]["10_good_text_weak_media"]), 1)
        self.assertEqual(payload["sheets"]["10_good_text_weak_media"][0]["current_stage"], "good_text_weak_media")

    def test_increment_state_second_run_is_not_baseline(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        mod.call_region_talk_semantic_llm = lambda post, evidence, **kwargs: {"llm_gate_status":"ok", "llm_provider":"google_gemini", "llm_model":"fake", "llm_default_env_var_name":"GOOGLE_API_KEY", "llm_limit_source":"supabase_google_ai_reserve", "llm_decision":"reject", "llm_reason":"not enough"}
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{"post_id":"post_seen", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/6", "platform_post_key":"tg:src:6", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград и Куршская коса: маршрут, море, дюны и впечатления от поездки", "text_excerpt":"", "has_media":False, "media_count":0, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}]
        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / "runs" / "r1"
            second = Path(td) / "runs" / "r2"
            first = mod.build_report(seeds, [], posts, "r1", base)
            again = mod.build_report(seeds, [], posts, "r2", second)
        self.assertEqual(first["summary"]["increment_state_loaded"], "false")
        self.assertEqual(again["summary"]["increment_state_loaded"], "true")
        inc = again["sheets"]["02_increment"][0]
        self.assertEqual(inc["new_this_run"], "no")
        self.assertEqual(inc["seen_run_count"], 2)
        self.assertEqual(inc["previous_run_id"], "r1")


    def test_candidate_memory_persists_when_not_refetched_and_metadata_pending(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        mod.call_region_talk_semantic_llm = lambda post, evidence, **kwargs: {"llm_gate_status":"ok", "llm_provider":"google_gemini", "llm_model":"fake", "llm_default_env_var_name":"GOOGLE_API_KEY", "llm_limit_source":"supabase_google_ai_reserve", "llm_decision":"accept", "whole_post_about_kaliningrad_oblast_score":0.9, "kaliningrad_mention_role":"main_subject", "is_digest_or_roundup":"false", "is_multi_region_roundup":"false", "is_multi_topic_digest":"false", "is_single_location_card":"true", "llm_is_ad_or_promo":"false", "llm_is_news_or_trash":"false", "llm_content_type":"single_location_photo_card", "content_type":"single_location_photo_card", "visit_evidence_type":"single_location_photo_card", "has_firsthand_visit_evidence":"false", "emotion_or_impression_evidence":"false", "review_or_opinion_evidence":"false", "original_photo_evidence":"true", "llm_reason":"single place card"}
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{"post_id":"post_memory", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/77", "platform_post_key":"tg:src:77", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград, Светлогорск и Куршская коса: красивая прогулка, личные впечатления, маршрут, море, дюны, полезные детали и что особенно запомнилось", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}]
        with tempfile.TemporaryDirectory() as td:
            first = mod.build_report(seeds, [], posts, "mem-r1", Path(td) / "runs" / "mem-r1")
            second = mod.build_report(seeds, [], [], "mem-r2", Path(td) / "runs" / "mem-r2")
        row = first["sheets"]["06a_candidate_memory"][0]
        self.assertEqual(row["current_stage"], "image_fetch_retry_needed")
        self.assertEqual(row["image_status"], "needs_actual_image_fetch")
        self.assertEqual(row["visual_decision"], "pending")
        self.assertEqual(row["image_publication_ready"], "false")
        self.assertEqual(first["sheets"]["09b_image_fetch_retry_queue"][0]["post_url"], "https://t.me/src/77")
        self.assertEqual(first["sheets"]["04a_final_shortlist"][0]["post_url"], "https://t.me/src/77")
        self.assertEqual(first["summary"]["candidate_memory_total"], 1)
        self.assertEqual(second["summary"]["candidate_memory_not_refetched_this_run"], 1)
        self.assertEqual(second["sheets"]["07b_prev_candidates_not_refetch"][0]["post_url"], "https://t.me/src/77")
        self.assertEqual(second["sheets"]["22_candidate_deltas"][0]["delta_bucket"], "not_refetched_this_run")

    def test_single_location_guardrail_not_roundup_reject(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        mod.call_region_talk_semantic_llm = lambda post, evidence, **kwargs: {"llm_gate_status":"ok", "llm_provider":"google_gemini", "llm_model":"fake", "llm_default_env_var_name":"GOOGLE_API_KEY", "llm_limit_source":"supabase_google_ai_reserve", "llm_decision":"reject", "whole_post_about_kaliningrad_oblast_score":0.9, "kaliningrad_mention_role":"main_subject", "is_digest_or_roundup":"true", "is_multi_region_roundup":"true", "is_multi_topic_digest":"false", "llm_is_ad_or_promo":"false", "llm_is_news_or_trash":"false", "llm_content_type":"reject", "content_type":"reject", "llm_reason":"Это дайджест красивых мест России"}
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{"post_id":"post_tihoe", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"Красивые места России", "platform":"telegram", "handle":"@viewrussia", "post_url":"https://t.me/viewrussia/30742", "platform_post_key":"tg:viewrussia:30742", "post_date":"2026-06-01T12:00:00+00:00", "text":"Калининград, Светлогорск, Озеро Тихое и Куршская коса. Тихая вода, прогулка вокруг озера, красивые виды, маршрут, личные впечатления и что особенно запомнилось.", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/viewrussia"}]
        with tempfile.TemporaryDirectory() as td:
            payload = mod.build_report(seeds, [], posts, "single-card-run", Path(td))
        self.assertFalse([r for r in payload["sheets"]["08_dropped_posts"] if r.get("post_id") == "post_tihoe" and r.get("rejection_reason") == "llm_reject"])
        self.assertTrue(payload["sheets"]["06a_candidate_memory"])
        self.assertIn(payload["sheets"]["06a_candidate_memory"][0]["current_stage"], {"image_fetch_retry_needed", "needs_image_review", "good_text_weak_media"})

    def test_public_blogger_links_imports_frontier_only_and_dedupes(self) -> None:
        mod = load_module()
        from openpyxl import Workbook
        with tempfile.TemporaryDirectory() as td:
            x = Path(td) / "bloggers.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "Links"
            ws.append(["Platform", "Handle", "URL", "Type", "Category", "Source", "Source page", "Collected on", "Notes"])
            ws.append(["Telegram", "@travel_one", "https://t.me/travel_one", "channel", "Путешествия", "test", "https://example.test", "2026-07-06", ""])
            ws.append(["Telegram", "@travel_one", "https://t.me/travel_one", "channel", "Путешествия", "test", "https://example.test", "2026-07-06", "duplicate"])
            ws.append(["VK", "@vk_travel", "https://vk.com/vk_travel", "community", "Путешествия", "test", "https://example.test", "2026-07-06", ""])
            wb.save(x)
            old = os.environ.get("REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE")
            os.environ["REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE"] = str(x)
            try:
                rows = mod.load_public_blogger_links({})
            finally:
                if old is None:
                    os.environ.pop("REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE", None)
                else:
                    os.environ["REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE"] = old
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r["candidate_source_status"] == "source_frontier" for r in rows))
        self.assertEqual({r["edge_type"] for r in rows}, {"public_travel_blogger_catalog"})

    def test_source_frontier_unique_dedupes_and_keeps_private_fields_out(self) -> None:
        mod = load_module()
        rows = [
            {"source_candidate_id":"src_cand_x", "normalized_url":"https://t.me/foo", "platform_guess":"telegram", "edge_type":"post_text_link", "discovery_type":"post_text", "confidence":0.55, "discovered_from_source":"A"},
            {"source_candidate_id":"src_cand_x", "normalized_url":"https://t.me/foo", "platform_guess":"telegram", "edge_type":"telegram_similar_channel", "discovery_type":"telegram_similar_channels", "confidence":0.85, "recommended_title":"Foo Travel", "recommended_username":"foo", "discovered_from_source":"B", "private_state_key":"telegram:username:foo"},
        ]
        frontier = mod.build_source_frontier_unique(rows, {}, "run1")
        self.assertEqual(len(frontier), 1)
        row = frontier[0]
        self.assertIn("telegram_similar_channel", row["edge_types_all"])
        self.assertGreaterEqual(row["source_candidate_score"], 0.75)
        self.assertNotIn("access_hash", json.dumps(row).lower())
        self.assertNotIn("channel_id_private", json.dumps(row).lower())


    def test_ydb_required_state_fails_fast_without_config(self) -> None:
        mod = load_module()
        old_backend = os.environ.get("REGION_TALK_STATE_BACKEND")
        old_require = os.environ.get("REGION_TALK_REQUIRE_YDB_STATE")
        for key in ["REGION_TALK_YDB_ENDPOINT", "REGION_TALK_YDB_DATABASE", "REGION_TALK_YDB_STATE_SNAPSHOT_FILE"]:
            os.environ.pop(key, None)
        os.environ["REGION_TALK_STATE_BACKEND"] = "ydb"
        os.environ["REGION_TALK_REQUIRE_YDB_STATE"] = "1"
        try:
            with tempfile.TemporaryDirectory() as td:
                with self.assertRaises(RuntimeError):
                    mod.load_region_talk_state(Path(td))
        finally:
            if old_backend is None: os.environ.pop("REGION_TALK_STATE_BACKEND", None)
            else: os.environ["REGION_TALK_STATE_BACKEND"] = old_backend
            if old_require is None: os.environ.pop("REGION_TALK_REQUIRE_YDB_STATE", None)
            else: os.environ["REGION_TALK_REQUIRE_YDB_STATE"] = old_require

    def test_ydb_config_parses_endpoint_database_and_compacts_state(self) -> None:
        mod = load_module()
        old_endpoint = os.environ.get("REGION_TALK_YDB_ENDPOINT")
        old_database = os.environ.get("REGION_TALK_YDB_DATABASE")
        old_namespace = os.environ.get("REGION_TALK_YDB_NAMESPACE")
        try:
            os.environ["REGION_TALK_YDB_ENDPOINT"] = "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/cloud/db"
            os.environ.pop("REGION_TALK_YDB_DATABASE", None)
            os.environ["REGION_TALK_YDB_NAMESPACE"] = "region-talk/test"
            cfg = mod.ydb_config_status()
            self.assertEqual(cfg["endpoint"], "grpcs://ydb.serverless.yandexcloud.net:2135")
            self.assertEqual(cfg["database"], "/ru-central1/cloud/db")
            self.assertEqual(mod.ydb_table_name("state_kv"), "region_talk_test_state_kv")
            compact = mod.compact_region_talk_state_for_ydb({
                "run_id": "r1",
                "state_schema_version": "full",
                "updated_at": "now",
                "posts": {"p1": {"post_id": "p1", "post_url": "https://t.me/x/1", "text": "RAW TEXT MUST NOT BE STORED", "candidate_score": 0.7, "overall_media_score": 0.8, "image_quality_bucket": "reviewable"}},
                "region_talk_sources": {"s1": {"source_id": "s1", "canonical_url": "https://t.me/x", "source_title": "X", "description": "RAW SOURCE DESC", "frontier_status": "legacy_noise"}},
                "candidate_memory": {"c1": {"candidate_memory_id": "c1", "post_url": "https://t.me/x/1", "short_summary": "ok", "raw_payload_json": "NO"}},
                "source_cursors": {"s1": {"last_seen_post_key": "1"}},
                "unified_source_queue": {"telegram:x": {"source_queue_id": "srcq1", "queue_order": 1, "canonical_source_key": "telegram:x", "platform": "telegram", "source_url": "https://t.me/x", "source_queue_status": "pending_scan", "status_color_hint": "white_pending", "row_fill_color": "white_pending"}},
                "image_candidate_queue": {"imgq1": {"image_queue_id": "imgq1", "image_queue_order": 1, "post_url": "https://t.me/x/1", "image_queue_status": "needs_actual_image_fetch", "status_color_hint": "yellow_retry"}},
                "source_frontier_queue_next": {"legacy": {"canonical_url": "https://t.me/legacy"}},
                "similar_seed_queue": {"legacy": {"canonical_url": "https://t.me/similar"}},
                "all_time_metrics": {"posts": 1},
            })
        finally:
            if old_endpoint is None: os.environ.pop("REGION_TALK_YDB_ENDPOINT", None)
            else: os.environ["REGION_TALK_YDB_ENDPOINT"] = old_endpoint
            if old_database is None: os.environ.pop("REGION_TALK_YDB_DATABASE", None)
            else: os.environ["REGION_TALK_YDB_DATABASE"] = old_database
            if old_namespace is None: os.environ.pop("REGION_TALK_YDB_NAMESPACE", None)
            else: os.environ["REGION_TALK_YDB_NAMESPACE"] = old_namespace
        blob = json.dumps(compact, ensure_ascii=False)
        self.assertIn("processed_posts", compact)
        self.assertEqual(compact["state_schema_version"], "region-talk-ydb-compact-v2")
        self.assertIn("unified_source_queue", compact)
        self.assertIn("image_candidate_queue", compact)
        self.assertNotIn("source_frontier_queue_next", compact)
        self.assertNotIn("similar_seed_queue", compact)
        self.assertIn("https://t.me/x/1", blob)
        self.assertNotIn("RAW TEXT MUST NOT BE STORED", blob)
        self.assertNotIn("RAW SOURCE DESC", blob)
        self.assertNotIn("raw_payload_json", blob)
        self.assertNotIn("frontier_status", blob)
        self.assertNotIn("status_color_hint", blob)
        self.assertNotIn("row_fill_color", blob)

    def test_vk_wall_uses_service_key_first_and_skips_catalog_paths(self) -> None:
        mod = load_module()
        old = {k: os.environ.get(k) for k in ["VK_SERVICE_TOKEN", "VK_SERVICE_KEY", "VK_ACCESS_TOKEN", "REGION_TALK_VK_READ_SERVICE_FIRST"]}
        try:
            os.environ.pop("VK_SERVICE_TOKEN", None)
            os.environ["VK_SERVICE_KEY"] = "service"
            os.environ["VK_ACCESS_TOKEN"] = "user"
            os.environ["REGION_TALK_VK_READ_SERVICE_FIRST"] = "1"
            self.assertEqual(mod.vk_wall_token(), "service")
            self.assertEqual(mod.vk_wall_token_kind(), "VK_SERVICE_KEY")
            seed = mod.Seed("s", "vk", "Search", "", "https://vk.com/search?c[q]=x", "", "", 1, "", "", "", "", "", "", True, "", "")
            self.assertEqual(mod.vk_domain_from_seed(seed), "")
            wall = mod.Seed("s2", "vkvideo", "Wall", "@intravel39", "https://vk.com/intravel39", "", "", 1, "", "", "", "", "", "", True, "", "")
            self.assertEqual(mod.vk_domain_from_seed(wall), "intravel39")
        finally:
            for k, v in old.items():
                if v is None: os.environ.pop(k, None)
                else: os.environ[k] = v

    def test_source_frontier_dedupes_by_canonical_key_across_discovery_types(self) -> None:
        mod = load_module()
        rows = [
            {"source_candidate_id":"src_cand_catalog", "canonical_source_key":"telegram:foo", "normalized_url":"https://t.me/foo", "platform_guess":"telegram", "edge_type":"public_travel_blogger_catalog", "discovery_type":"public_travel_blogger_catalog", "confidence":0.45, "discovered_from_source":"catalog"},
            {"source_candidate_id":"src_cand_similar", "normalized_url":"https://t.me/foo", "recommended_username":"foo", "platform_guess":"telegram_channel", "edge_type":"telegram_similar_channel", "discovery_type":"telegram_similar_channels", "confidence":0.85, "recommended_title":"Foo Travel", "discovered_from_source":"similar"},
        ]
        frontier = mod.build_source_frontier_unique(rows, {}, "canon-run")
        self.assertEqual(len(frontier), 1)
        self.assertEqual(frontier[0]["canonical_source_key"], "telegram:foo")
        self.assertIn("public_travel_blogger_catalog", frontier[0]["discovery_types"])
        self.assertIn("telegram_similar_channels", frontier[0]["discovery_types"])

    def test_unified_source_queue_only_tg_vk_dedupes_and_inserts_keyword_after_cursor(self) -> None:
        mod = load_module()
        seeds = [
            mod.Seed("seed1", "telegram", "Seed TG", "@seedtg", "https://t.me/seedtg", "", "", 1, "", "", "", "", "", "", True, "", ""),
            mod.Seed("seed2", "youtube", "YT", "", "https://youtube.com/x", "", "", 1, "", "", "", "", "", "", True, "", ""),
        ]
        previous = {
            "unified_source_queue_cursor_position": 1,
            "unified_source_queue": {
                "telegram:old": {"canonical_source_key": "telegram:old", "platform": "telegram", "source_url": "https://t.me/old", "queue_order": 1, "source_queue_status": "processed_no_ko"},
                "telegram:tail": {"canonical_source_key": "telegram:tail", "platform": "telegram", "source_url": "https://t.me/tail", "queue_order": 2, "source_queue_status": "pending_scan"},
                "telegram:https://tgstat.ru/search?q=x": {"canonical_source_key": "telegram:https://tgstat.ru/search?q=x", "platform": "telegram", "source_url": "https://tgstat.ru/search?q=x", "queue_order": 3, "source_queue_status": "pending_scan"},
            },
        }
        rows, metrics = mod.build_unified_source_queue(
            previous,
            seeds,
            [{"source_id": "src_old", "platform": "telegram", "canonical_url": "https://t.me/old", "canonical_source_key": "telegram:old", "fetch_status": "ok", "posts_scanned": 2}],
            [
                {"platform": "dzen", "canonical_url": "https://dzen.ru/nope", "canonical_source_key": "dzen:nope"},
                {"platform": "telegram", "canonical_url": "https://tgstat.ru/search?query=travel", "canonical_source_key": "telegram:https://tgstat.ru/search?query=travel"},
                {"platform": "telegram", "canonical_url": "https://t.me/channel/123", "canonical_source_key": "telegram:channel", "source_candidate_score": 0.8},
                {"platform": "vk", "canonical_url": "https://vk.com/video", "canonical_source_key": "vk:video", "source_candidate_score": 0.5},
                {"platform": "vk", "canonical_url": "https://vk.com/video-123_456", "canonical_source_key": "vk:video-123_456", "source_candidate_score": 0.5},
                {"platform": "vk", "canonical_url": "https://vk.com/wall-123_456", "canonical_source_key": "vk:wall-123_456", "source_candidate_score": 0.5},
                {"platform": "vk", "canonical_url": "https://vk.com/vktravel", "canonical_source_key": "vk:vktravel", "source_candidate_score": 0.5},
                {"platform": "telegram", "canonical_url": "https://t.me/similar_tail", "recommended_username": "similar_tail", "canonical_source_key": "telegram:similar_tail", "edge_type": "telegram_similar_channel", "source_candidate_score": 0.8},
            ],
            [{"platform": "telegram", "canonical_url": "https://t.me/seedtg", "canonical_source_key": "telegram:seedtg", "discovery_type": "public_travel_blogger_catalog"}],
            [{"platform": "telegram", "recommended_canonical_url": "https://t.me/keynew", "recommended_username": "keynew", "canonical_source_key": "telegram:keynew"}],
            [{"keyword_hit_source_url": "https://t.me/keypost", "platform": "telegram", "canonical_source_key": "telegram:keypost"}],
            {"src_old": [{"kaliningrad_oblast_only_scope": False, "current_stage": "dropped_text_gate"}]},
            "run-q",
            "2026-07-07T00:00:00+00:00",
        )
        self.assertTrue(rows)
        self.assertEqual(metrics["source_queue_only_telegram_vk"], "true")
        self.assertEqual(metrics["source_queue_only_target_source_urls"], "true")
        urls = {r["source_url"] for r in rows}
        self.assertNotIn("https://youtube.com/x", urls)
        self.assertNotIn("https://dzen.ru/nope", urls)
        self.assertFalse(any("tgstat.ru/search" in u for u in urls))
        self.assertFalse(any("vk.com/video" in u for u in urls))
        self.assertFalse(any("vk.com/wall-123_456" in u for u in urls))
        self.assertFalse(any(u.rstrip("/").endswith("/123") for u in urls))
        orders = {r["canonical_source_key"]: r["queue_order"] for r in rows}
        self.assertEqual(orders["telegram:keynew"], 2)
        self.assertEqual(orders["telegram:keypost"], 3)
        self.assertGreater(orders["telegram:tail"], orders["telegram:keypost"])
        self.assertGreater(orders["telegram:similar_tail"], orders["telegram:tail"])
        self.assertEqual(len([r for r in rows if r["canonical_source_key"] == "telegram:seedtg"]), 1)
        self.assertGreaterEqual(metrics["source_queue_non_target_skipped_this_run"], 5)

    def test_source_queue_marks_low_image_quality_sources_for_monitoring_exclusion(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 0,
            "unified_source_queue": {
                "telegram:weakpics": {"canonical_source_key": "telegram:weakpics", "platform": "telegram", "source_url": "https://t.me/weakpics", "queue_order": 1, "source_queue_status": "pending_scan"},
            },
        }
        posts = [
            {"kaliningrad_oblast_only_scope": True, "is_ad_or_promo": False, "current_stage": "semantic_candidate", "image_model_input_type": "actual_image", "overall_media_score": score}
            for score in (0.22, 0.31, 0.40)
        ]
        rows, metrics = mod.build_unified_source_queue(
            previous, [],
            [{"source_id": "src_weak", "platform": "telegram", "canonical_url": "https://t.me/weakpics", "canonical_source_key": "telegram:weakpics", "fetch_status": "ok", "posts_scanned": 3}],
            [], [], [], [],
            {"src_weak": posts},
            "run-q", "2026-07-07T00:00:00+00:00",
        )
        row = next(r for r in rows if r["canonical_source_key"] == "telegram:weakpics")
        self.assertEqual(row["source_image_quality_status"], "exclude_low_image_quality")
        self.assertEqual(row["source_queue_status"], "processed_found_ko_low_image_quality")
        self.assertEqual(row["monitoring_exclusion_reason"], "kaliningrad_posts_found_but_actual_images_systematically_low_score")
        self.assertEqual(metrics["source_queue_low_image_quality_excluded_total"], 1)

    def test_image_candidate_queue_limits_next_batch_and_sorts_actual_top(self) -> None:
        mod = load_module()
        posts = [
            {"post_id": f"p{i}", "post_url": f"https://t.me/src/{i}", "platform_post_key": f"tg:src:{i}", "source_id": "src", "source_title": "S", "source_url": "https://t.me/src", "post_date": "2026-07-01T00:00:00+00:00", "has_media": True, "is_ad_or_promo": False, "kaliningrad_oblast_only_scope": True, "kaliningrad_mention_role": "main_subject", "current_stage": "semantic_candidate", "candidate_score": 0.5}
            for i in range(35)
        ]
        media_rows = [
            {"post_url": "https://t.me/src/3", "image_model_input_type": "actual_image", "image_model_type": "clip", "overall_media_score": 0.8, "postcardness_score": 0.9, "aesthetic_score": 0.7, "image_url_or_local_path": "/tmp/3.jpg"},
            {"post_url": "https://t.me/src/4", "image_model_input_type": "metadata_only", "failure_reason": "needs_actual_image_fetch", "overall_media_score": 0.1},
        ]
        old_target = os.environ.get("REGION_TALK_IMAGE_QUEUE_TARGET_PER_RUN")
        os.environ["REGION_TALK_IMAGE_QUEUE_TARGET_PER_RUN"] = "30"
        try:
            queue, top, metrics = mod.build_image_candidate_queue({}, posts, [], media_rows, "run-img", "2026-07-07T00:00:00+00:00")
        finally:
            if old_target is None:
                os.environ.pop("REGION_TALK_IMAGE_QUEUE_TARGET_PER_RUN", None)
            else:
                os.environ["REGION_TALK_IMAGE_QUEUE_TARGET_PER_RUN"] = old_target
        self.assertEqual(metrics["image_queue_target_this_run"], 30)
        self.assertLessEqual(metrics["image_queue_selected_next_batch"], 30)
        self.assertTrue(top)
        self.assertEqual(top[0]["post_url"], "https://t.me/src/3")
        self.assertEqual(top[0]["image_queue_status"], "actual_scored")

    def test_image_candidate_queue_prunes_non_region_and_media_only_rows(self) -> None:
        mod = load_module()
        previous = {"image_candidate_queue": {
            "bad_prev": {"image_queue_id": "bad_prev", "image_queue_order": 1, "post_url": "https://t.me/bad/1", "source_title": "МЧС Краснодарского края", "kaliningrad_oblast_only_scope": False, "image_queue_status": "needs_actual_image_fetch"},
            "good_prev": {"image_queue_id": "good_prev", "image_queue_order": 2, "post_url": "https://t.me/good/1", "source_title": "KO", "kaliningrad_oblast_only_scope": True, "kaliningrad_mention_role": "main_subject", "current_stage": "semantic_candidate", "image_queue_status": "needs_actual_image_fetch"},
        }}
        posts = [
            {"post_id": "bad_current", "post_url": "https://t.me/buryatia/1", "source_title": "Минтуризм Бурятии", "has_media": True, "is_ad_or_promo": False, "kaliningrad_oblast_only_scope": False, "current_stage": "dropped_text_gate"},
            {"post_id": "good_current", "post_url": "https://t.me/ko/1", "source_title": "KO", "has_media": True, "is_ad_or_promo": False, "kaliningrad_oblast_only_scope": True, "kaliningrad_mention_role": "main_subject", "current_stage": "semantic_candidate"},
            {"post_id": "external", "post_url": "https://t.me/roundup/1", "source_title": "Roundup", "has_media": True, "is_ad_or_promo": False, "kaliningrad_oblast_only_scope": True, "kaliningrad_mention_role": "one_item", "external_geo_mentions": "Бурятия", "current_stage": "semantic_candidate"},
        ]
        media_rows = [
            {"post_url": "https://t.me/mediaonly/1", "image_model_input_type": "actual_image", "overall_media_score": 0.9},
            {"post_url": "https://t.me/ko/1", "image_model_input_type": "actual_image", "overall_media_score": 0.8},
        ]
        queue, top, metrics = mod.build_image_candidate_queue(previous, posts, [], media_rows, "run-img", "2026-07-07T00:00:00+00:00")
        urls = {r.get("post_url") for r in queue}
        self.assertIn("https://t.me/good/1", urls)
        self.assertIn("https://t.me/ko/1", urls)
        self.assertNotIn("https://t.me/bad/1", urls)
        self.assertNotIn("https://t.me/buryatia/1", urls)
        self.assertNotIn("https://t.me/roundup/1", urls)
        self.assertNotIn("https://t.me/mediaonly/1", urls)
        self.assertEqual(metrics["image_queue_pruned_non_region_previous"], 1)
        self.assertGreaterEqual(metrics["image_queue_rejected_non_region_inputs"], 3)
        self.assertEqual(metrics["image_queue_text_region_confirmed_total"], len(queue))

    def test_candidate_found_jsonl_uses_stage_events_schema(self) -> None:
        mod = load_module()
        mod.load_llm_limit_snapshot = lambda model, default_env: {"llm_limit_source":"supabase_google_ai", "supabase_limiter_model_found":"true", "supabase_scoped_key_found":"true"}
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{"post_id":"event_post", "source_id":seeds[0].source_id, "source_seed_id":seeds[0].source_seed_id, "source_title":"src", "platform":"telegram", "handle":"@src", "post_url":"https://t.me/src/8", "platform_post_key":"tg:src:8", "post_date":"2026-06-01T12:00:00+00:00", "text":"Мы приехали в Калининград, гуляли по Амалиенау, понравилось море, маршрут и что особенно запомнилось на Куршской косе", "text_excerpt":"", "has_media":True, "media_count":1, "rights_policy":"unknown", "source_kind":"travel_media", "source_type":"travel_media", "source_url":"https://t.me/src"}]
        with tempfile.TemporaryDirectory() as td:
            mod.build_report(seeds, [], posts, "event-run", Path(td))
            rows = [json.loads(line) for line in (Path(td) / "candidate_found.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertTrue(rows)
        self.assertTrue({"run_id", "event_at", "source_id", "post_url", "stage", "next_action", "short_summary"}.issubset(rows[0]))
        self.assertIn("fresh_ko_post_found", {r["event_type"] for r in rows})


if __name__ == "__main__":
    unittest.main()
