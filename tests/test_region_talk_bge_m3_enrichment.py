from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
BGE_MODULE_PATH = ROOT / "kaggle" / "RegionTalkBgeM3Enrichment" / "region_talk_bge_m3_enrichment.py"
BGE_RUNNER_PATH = ROOT / "kaggle" / "execute_region_talk_bge_m3_enrichment.py"
CANDIDATE_MODULE_PATH = ROOT / "kaggle" / "RegionTalkCandidateReport" / "region_talk_candidate_report.py"


def load_bge_module():
    spec = importlib.util.spec_from_file_location("region_talk_bge_m3_enrichment", BGE_MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_bge_runner_module():
    spec = importlib.util.spec_from_file_location("execute_region_talk_bge_m3_enrichment", BGE_RUNNER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_candidate_module():
    spec = importlib.util.spec_from_file_location("region_talk_candidate_report_for_bge_test", CANDIDATE_MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkBgeM3EnrichmentTests(unittest.TestCase):
    def test_semantic_bank_matches_candidate_report_contract(self) -> None:
        bge = load_bge_module()
        candidate = load_candidate_module()
        self.assertEqual(bge.semantic_bank_v1(), candidate.semantic_bank_v1())

    def test_bge_model_reference_finds_nested_kaggle_mount(self) -> None:
        mod = load_bge_module()
        keys = [
            "REGION_TALK_BGE_MODEL_LOCAL_PATH",
            "REGION_TALK_KAGGLE_INPUT_ROOT",
            "REGION_TALK_BGE_USE_KAGGLEHUB_FALLBACK",
        ]
        old = {key: os.environ.get(key) for key in keys}
        try:
            with TemporaryDirectory() as td:
                model_dir = Path(td) / "models" / "owner" / "baai-bge-m3" / "transformers" / "default" / "1"
                model_dir.mkdir(parents=True)
                for name in ("config.json", "pytorch_model.bin", "tokenizer.json"):
                    (model_dir / name).write_text("{}", encoding="utf-8")
                os.environ.pop("REGION_TALK_BGE_MODEL_LOCAL_PATH", None)
                os.environ["REGION_TALK_KAGGLE_INPUT_ROOT"] = td
                os.environ["REGION_TALK_BGE_USE_KAGGLEHUB_FALLBACK"] = "0"
                reference, origin = mod.bge_model_reference()
                self.assertEqual(reference, str(model_dir))
                self.assertEqual(origin, "local_model_path")
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_bge_launcher_attaches_pinned_model_source(self) -> None:
        mod = load_bge_runner_module()
        kernel_path = mod.prepared_kernel_path(run_id="unit-bge-model", kernel_slug="unit-bge-model")
        import json
        metadata = json.loads((kernel_path / "kernel-metadata.json").read_text(encoding="utf-8"))
        self.assertIn(mod.DEFAULT_BGE_KAGGLE_MODEL_SOURCE, metadata["model_sources"])

    def test_secret_names_do_not_package_telegram_sessions(self) -> None:
        mod = load_bge_runner_module()
        names = mod.bge_secret_names()
        self.assertIn("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY1", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY2", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_E2E", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_S22", names)
        self.assertNotIn("TELEGRAM_SESSION", names)

    def test_collect_text_rows_prefers_publication_and_dedupes_when_legacy_inputs_enabled(self) -> None:
        mod = load_bge_module()
        old = os.environ.get("REGION_TALK_BGE_E5_ONLY")
        os.environ["REGION_TALK_BGE_E5_ONLY"] = "0"
        items = {
            "candidate_memory_item": {
                "candidate_memory_item:a": {"post_id": "p1", "post_url": "https://t.me/a/1", "short_summary": "Калининград и Куршская коса: личные впечатления от поездки."},
            },
            "publication_candidate_item": {
                "publication_candidate_item:b": {"post_id": "p2", "post_url": "https://t.me/b/2", "short_summary": "Зеленоградск, море и прогулка: что запомнилось автору."},
            },
            "image_queue_item": {
                "image_queue_item:dup": {"post_id": "p1", "post_url": "https://t.me/a/1", "text_excerpt": "Дубликат того же поста."},
            },
        }
        try:
            rows = mod.collect_text_rows(items, existing_pks=set(), limit=10)
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_BGE_E5_ONLY", None)
            else:
                os.environ["REGION_TALK_BGE_E5_ONLY"] = old
        self.assertEqual([row["post_id"] for row in rows], ["p2", "p1"])
        self.assertTrue(all(row.get("_enrichment_pk", "").startswith("text_vector_enrichment_item:") for row in rows))
        self.assertEqual(len(rows), 2)

    def test_collect_text_rows_uses_e5_text_vector_items_as_bge_input(self) -> None:
        mod = load_bge_module()
        text = "Личный отзыв о поездке в Калининградскую область: море, дюны и Куршская коса."
        sha = mod.text_hash(text)
        e5_row = {
            "post_id": "p-e5",
            "post_url": "https://t.me/e5/1",
            "model_id": "intfloat/multilingual-e5-base",
            "model_short": "e5",
            "text_hash": sha,
            "text_excerpt": text,
        }
        rows = mod.collect_text_rows({"text_vector_enrichment_item": {"text_vector_enrichment_item:p-e5:e5": e5_row}}, existing_pks=set(), limit=5)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["_source_kind"], "text_vector_enrichment_item")
        self.assertEqual(rows[0]["_embedding_text_hash"], sha)
        self.assertEqual(rows[0]["_paired_e5_text_hash"], sha)
        self.assertIn(":bge_m3:", rows[0]["_enrichment_pk"])

    def test_collect_text_rows_rescores_existing_pk_when_semantic_bank_is_stale(self) -> None:
        mod = load_bge_module()
        text = "Личный отзыв о поездке в Калининградскую область и на Куршскую косу."
        sha = mod.text_hash(text)
        e5_row = {
            "post_id": "p-stale-bank",
            "post_url": "https://t.me/e5/2",
            "model_id": "intfloat/multilingual-e5-base",
            "model_short": "e5",
            "text_hash": sha,
            "text_excerpt": text,
        }
        bge_pk = mod.enrichment_pk(e5_row["post_id"], e5_row["post_url"], sha)
        stale_bge = {
            "post_id": e5_row["post_id"],
            "post_url": e5_row["post_url"],
            "model_id": mod.MODEL_ID,
            "model_short": mod.MODEL_SHORT,
            "encoder_contract": mod.ENCODER_CONTRACT,
            "text_hash": sha,
            "semantic_bank_version": "semantic_bank_v1",
            "semantic_bank_hash": "stale-bank-hash",
            "semantic_scores_by_class": {"ko_visit_impression": 0.7},
        }
        items = {"text_vector_enrichment_item": {"e5": e5_row, bge_pk: stale_bge}}
        rows = mod.collect_text_rows(items, existing_pks=items["text_vector_enrichment_item"], limit=5)
        self.assertEqual([row["post_id"] for row in rows], ["p-stale-bank"])
        self.assertEqual(mod.LAST_COLLECT_STATS["existing_stale_rescore"], 1)
        self.assertEqual(rows[0]["_enrichment_pk"], bge_pk)

        version, bank_hash = mod.bank_version_and_hash(mod.semantic_bank_v1(), version="semantic_bank_v1")
        current_bge = dict(stale_bge, semantic_bank_version=version, semantic_bank_hash=bank_hash[:16])
        current_items = {"text_vector_enrichment_item": {"e5": e5_row, bge_pk: current_bge}}
        current_rows = mod.collect_text_rows(current_items, existing_pks=current_items["text_vector_enrichment_item"], limit=5)
        self.assertEqual(current_rows, [])
        self.assertEqual(mod.LAST_COLLECT_STATS["existing_skipped"], 1)

    def test_collect_text_rows_selects_missing_pairs_before_stale_bank_refresh(self) -> None:
        mod = load_bge_module()
        items = {"text_vector_enrichment_item": {}}
        existing = {}

        # The stale population is product-priority and much larger than the
        # batch. It must not starve a newly discovered ordinary E5 row.
        for index in range(6):
            text = f"Старый точный пост {index} о поездке в Калининградскую область."
            sha = mod.text_hash(text)
            e5 = {
                "post_id": f"stale-{index}",
                "post_url": f"https://t.me/stale/{index}",
                "model_id": "intfloat/multilingual-e5-base",
                "model_short": "e5",
                "text_hash": sha,
                "text_excerpt": text,
                "post_date": f"2026-07-{index + 1:02d}T00:00:00+00:00",
                "post_link_priority": 0,
                "priority_reason": "global_keyword_search_exact_post",
            }
            items["text_vector_enrichment_item"][f"e5:stale:{index}"] = e5
            bge_pk = mod.enrichment_pk(e5["post_id"], e5["post_url"], sha)
            existing[bge_pk] = {
                "post_id": e5["post_id"],
                "post_url": e5["post_url"],
                "model_id": mod.MODEL_ID,
                "model_short": mod.MODEL_SHORT,
                "encoder_contract": mod.ENCODER_CONTRACT,
                "text_hash": sha,
                "semantic_bank_version": "semantic_bank_v1",
                "semantic_bank_hash": "stale-bank-hash",
                "semantic_scores_by_class": {"ko_visit_impression": 0.7},
            }

        missing_text = "Новый обычный пост о личной поездке на Куршскую косу."
        missing = {
            "post_id": "missing-live-pair",
            "post_url": "https://t.me/new/1",
            "model_id": "intfloat/multilingual-e5-base",
            "model_short": "e5",
            "text_hash": mod.text_hash(missing_text),
            "text_excerpt": missing_text,
            "post_date": "2026-07-14T00:00:00+00:00",
        }
        items["text_vector_enrichment_item"]["e5:missing"] = missing
        items["text_vector_enrichment_item"].update(existing)

        rows = mod.collect_text_rows(items, existing_pks=items["text_vector_enrichment_item"], limit=5)

        self.assertEqual(rows[0]["post_id"], "missing-live-pair")
        self.assertEqual(mod.LAST_COLLECT_STATS["missing_current_bge"], 1)
        self.assertEqual(mod.LAST_COLLECT_STATS["existing_stale_rescore"], 6)
        self.assertEqual(mod.LAST_COLLECT_STATS["selected_missing_current_bge"], 1)
        self.assertEqual(mod.LAST_COLLECT_STATS["selected_stale_rescore"], 4)

    def test_external_publication_e5_row_uses_product_priority_lane(self) -> None:
        mod = load_bge_module()
        row = {
            "source_queue_status": "confirmed_external_publication_research",
            "source_topic_class": "editorial_publication",
        }
        self.assertTrue(mod._is_product_priority_row(row))

    def test_collect_text_rows_skips_bge_vector_rows_by_default(self) -> None:
        mod = load_bge_module()
        text = "BGE result should not be re-embedded as BGE input."
        sha = mod.text_hash(text)
        bge_row = {
            "post_id": "p-bge",
            "post_url": "https://t.me/bge/1",
            "model_id": "BAAI/bge-m3",
            "model_short": "bge_m3",
            "text_hash": sha,
            "text_excerpt": text,
        }
        rows = mod.collect_text_rows({"text_vector_enrichment_item": {"text_vector_enrichment_item:p-bge:bge": bge_row}}, existing_pks=set(), limit=5)
        self.assertEqual(rows, [])

    def test_collect_text_rows_skips_terminal_local_and_spam_e5_rows(self) -> None:
        mod = load_bge_module()
        rows_by_pk = {}
        for index, status in enumerate(("rejected_local_region_source", "rejected_spam_source", ""), start=1):
            text = f"Личный рассказ {index} о поездке в Калининградскую область и впечатлениях."
            rows_by_pk[f"e5:{index}"] = {
                "post_id": f"p{index}",
                "post_url": f"https://t.me/source/{index}",
                "model_id": "intfloat/multilingual-e5-base",
                "model_short": "e5",
                "text_hash": mod.text_hash(text),
                "text_excerpt": text,
                "source_queue_status": status,
            }
        rows = mod.collect_text_rows(
            {"text_vector_enrichment_item": rows_by_pk},
            existing_pks=set(),
            limit=5,
        )
        self.assertEqual([row["post_id"] for row in rows], ["p3"])
        self.assertEqual(mod.LAST_COLLECT_STATS["source_terminal_skipped"], 2)

    def test_collect_text_rows_reserves_priority_and_fifo_capacity(self) -> None:
        mod = load_bge_module()
        items = {"text_vector_enrichment_item": {}}
        for index in range(10):
            text = f"Пост {index} о поездке в Калининградскую область и личных впечатлениях."
            row = {
                "post_id": f"p{index}",
                "post_url": f"https://t.me/src/{index}",
                "model_id": "intfloat/multilingual-e5-base",
                "model_short": "e5",
                "text_hash": mod.text_hash(text),
                "text_excerpt": text,
                "post_date": f"2026-07-{index + 1:02d}T00:00:00+00:00",
            }
            if index < 8:
                row.update({
                    "discovery_method": "exact_post_link_queue",
                    "post_link_priority": 0,
                    "priority_reason": "global_keyword_search_exact_post",
                })
            items["text_vector_enrichment_item"][f"e5:{index}"] = row
        with mock.patch.dict(os.environ, {"REGION_TALK_BGE_PRIORITY_SHARE_PERCENT": "80"}):
            rows = mod.collect_text_rows(items, existing_pks=set(), limit=5)
        self.assertEqual(len(rows), 5)
        # Four product-priority rows (fresh-first) plus one oldest FIFO row.
        self.assertEqual([row["post_id"] for row in rows[:4]], ["p7", "p6", "p5", "p4"])
        self.assertEqual(rows[4]["post_id"], "p8")

    def test_short_exact_post_is_not_left_in_permanent_dual_pending(self) -> None:
        mod = load_bge_module()
        short = "Калининград"
        priority_row = {
            "post_id": "p-short-exact",
            "post_url": "https://t.me/source/10",
            "model_id": "intfloat/multilingual-e5-base",
            "model_short": "e5",
            "text_hash": mod.text_hash(short),
            "text_excerpt": short,
            "discovery_method": "exact_post_link_queue",
            "post_link_priority": 0,
            "priority_reason": "global_keyword_search_exact_post",
        }
        generic_row = {
            "post_id": "p-short-generic",
            "post_url": "https://t.me/source/11",
            "model_id": "intfloat/multilingual-e5-base",
            "model_short": "e5",
            "text_hash": mod.text_hash("Зеленоградск"),
            "text_excerpt": "Зеленоградск",
        }

        rows = mod.collect_text_rows(
            {"text_vector_enrichment_item": {"e5:priority": priority_row, "e5:generic": generic_row}},
            existing_pks=set(),
            limit=5,
        )

        self.assertEqual([row["post_id"] for row in rows], ["p-short-exact"])
        self.assertEqual(mod.LAST_COLLECT_STATS["short_text_skipped"], 1)

    def test_ydb_load_retries_with_fresh_driver_and_does_not_scan_vector_kind_twice(self) -> None:
        mod = load_bge_module()
        drivers = []
        events = []
        select_calls = []

        class FakeDriver:
            def __init__(self, attempt: int) -> None:
                self.attempt = attempt
                self.stopped = False

            def stop(self, timeout=5) -> None:
                self.stopped = True

        class FakePool:
            def __init__(self, driver: FakeDriver) -> None:
                self.driver = driver

            def retry_operation_sync(self, operation):
                if self.driver.attempt < 3:
                    raise RuntimeError("Deadline exceeded on request")
                return operation(object())

        class FakeYdb:
            SessionPool = FakePool

        def connect():
            driver = FakeDriver(len(drivers) + 1)
            drivers.append(driver)
            return FakeYdb, driver, {"database": "/local/test"}

        text = "Личный отзыв о поездке в Калининградскую область, море и Куршская коса."
        vector_rows = {
            "text_vector_enrichment_item:e5": {
                "post_id": "p-retry",
                "post_url": "https://t.me/retry/1",
                "model_id": "intfloat/multilingual-e5-base",
                "model_short": "e5",
                "text_hash": mod.text_hash(text),
                "text_excerpt": text,
            }
        }

        def select(_session, _ydb, _table, kind, *, limit):
            select_calls.append(kind)
            return vector_rows if kind == "text_vector_enrichment_item" else {}

        with mock.patch.dict(os.environ, {
            "REGION_TALK_BGE_INPUT_KINDS": "text_vector_enrichment_item",
            "REGION_TALK_BGE_YDB_LOAD_ATTEMPTS": "3",
            "REGION_TALK_BGE_YDB_LOAD_RETRY_BASE_SECONDS": "0",
        }), mock.patch.object(mod, "ydb_connect", side_effect=connect), mock.patch.object(
            mod, "ensure_ydb_kv_table"
        ), mock.patch.object(mod, "ydb_select_kind_items", side_effect=select), mock.patch.object(
            mod, "emit_event", side_effect=lambda name, **payload: events.append((name, payload))
        ):
            rows, meta = mod.load_ydb_rows(4)

        self.assertEqual([row["post_id"] for row in rows], ["p-retry"])
        self.assertEqual(meta["ydb_load_attempt"], 3)
        self.assertEqual(len(drivers), 3)
        self.assertTrue(all(driver.stopped for driver in drivers))
        self.assertEqual(select_calls, ["text_vector_enrichment_item"])
        self.assertEqual([name for name, _payload in events], ["bge_ydb_load_retry", "bge_ydb_load_retry"])

    def test_no_rows_emits_terminal_done_heartbeat(self) -> None:
        mod = load_bge_module()
        events: list[tuple[str, dict]] = []
        with TemporaryDirectory() as tmpdir, mock.patch.object(
            mod, "load_ydb_rows", return_value=([], {"collect_stats": {}})
        ), mock.patch.object(
            mod, "emit_event", side_effect=lambda name, **payload: events.append((name, payload))
        ), mock.patch.object(
            mod.Path, "cwd", return_value=Path(tmpdir)
        ):
            result = mod.run_bge_enrichment("unit-no-rows", Path(tmpdir) / "output")
        self.assertEqual(result["status"], "no_rows")
        self.assertEqual(events[-1][0], "bge_enrichment_done")
        self.assertEqual(events[-1][1]["status"], "no_rows")
        self.assertEqual(events[-1][1]["bge_rows_written"], 0)

    def test_build_enrichment_payload_contains_geo_and_antivector_fields(self) -> None:
        mod = load_bge_module()
        row = {
            "post_id": "p1",
            "post_url": "https://t.me/a/1",
            "source_title": "source",
            "_embedding_text": "Калининград, Куршская коса и море — личный отзыв.",
            "_embedding_text_hash": mod.text_hash("Калининград, Куршская коса и море — личный отзыв."),
            "_embedding_text_fields": ["short_summary"],
        }
        payload = mod.build_enrichment_payload(
            row,
            {"ko_visit_impression": 0.72, "ad_or_promo": 0.21, "other_region_travel": 0.18},
            {"ko_geo:Куршская коса": 0.81, "external_ru_geo:Сочи": 0.17},
            [0.1, 0.2, 0.3],
            run_id="unit",
            semantic_bank_version="semantic_bank_v1",
            semantic_bank_hash="abcdef0123456789",
            geo_bank_version="geo_discriminator_bank_v1",
            geo_bank_hash="1234567890abcdef",
            embedding_dim=3,
            row_index=1,
        )
        self.assertEqual(payload["model_id"], "BAAI/bge-m3")
        self.assertEqual(payload["encoder_contract"], "bge_m3_flagembedding_dense_v1")
        self.assertEqual(payload["bge_m3_positive_class"], "ko_visit_impression")
        self.assertEqual(payload["bge_m3_ko_geo_top"], "Куршская коса")
        self.assertEqual(payload["bge_m3_external_geo_top"], "Сочи")
        self.assertNotIn("text_excerpt", payload)
        self.assertNotIn("embedding_vector", payload)
        self.assertEqual(payload["embedding_vector_encoding"], "f16_le_base64")
        decoded = mod.decode_dense_vector_f16(payload["embedding_vector_f16_b64"], 3)
        self.assertEqual(len(decoded), 3)
        self.assertAlmostEqual(decoded[0], 0.1, places=3)

    def test_compact_paired_e5_payload_drops_consumed_text_only(self) -> None:
        mod = load_bge_module()
        compact = mod.compact_paired_e5_payload({
            "_ydb_pk": "text_vector_enrichment_item:e5",
            "model_id": "intfloat/multilingual-e5-base",
            "text_excerpt": "x" * 3000,
            "text_hash": "hash",
            "semantic_scores_by_class": {"ko_visit_impression": 0.7},
        }, pruned_at="2026-07-11T00:00:00+00:00")
        self.assertNotIn("text_excerpt", compact)
        self.assertNotIn("_ydb_pk", compact)
        self.assertEqual(compact["text_hash"], "hash")
        self.assertEqual(compact["semantic_scores_by_class"]["ko_visit_impression"], 0.7)
        self.assertTrue(compact["text_payload_pruned_after_bge"])


if __name__ == "__main__":
    unittest.main()
