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
            self.assertIn("09b_image_fetch_retry_queue", workbook)
            self.assertIn("15_manual_decisions", workbook)
            self.assertIn("17_source_graph_edges", workbook)
            self.assertIn("18_place_lexicon_matches", workbook)
            self.assertIn("12a_source_frontier_unique", workbook)
            self.assertIn("12b_telegram_similar_channels", workbook)
            self.assertIn("12d_similar_seed_queue", workbook)
            self.assertIn("20_telegram_rate_observability", workbook)
            self.assertIn("04a_current_run_shortlist", workbook)
            self.assertIn("06a_candidate_memory", workbook)
            self.assertIn("06b_candidate_memory_top", workbook)
            self.assertIn("07b_prev_candidates_not_refetch", workbook)
            self.assertIn("12c_source_frontier_queue_next", workbook)
            self.assertIn("13b_source_delta_scan", workbook)
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
        self.assertEqual({dropped[u]["rejection_reason"] for u in false_urls}, {"reject_not_kaliningrad_oblast_only"})
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
        self.assertEqual(dropped_reasons["post_ad"], "reject_ad_or_promo")
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
        self.assertIn(row["rejection_reason"], {"reject_ad_or_promo", "vector_reject_news_event"})
        self.assertIn(row["llm_status"], {"not_called_vector_reject", "not_called_until_final_verifier"})
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
        self.assertIn(statuses["vk1"], {"skipped_vk_wall_not_configured", "skipped_vk_wall_not_implemented"})
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


if __name__ == "__main__":
    unittest.main()
