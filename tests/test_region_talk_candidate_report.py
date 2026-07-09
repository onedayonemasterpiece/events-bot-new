from __future__ import annotations

import asyncio
import importlib.util
import os
import json
import sys
import tempfile
import types
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "kaggle" / "RegionTalkCandidateReport" / "region_talk_candidate_report.py"
RUNNER_MODULE_PATH = ROOT / "kaggle" / "execute_region_talk_candidate_report.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_candidate_report", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_runner_module():
    spec = importlib.util.spec_from_file_location("execute_region_talk_candidate_report", RUNNER_MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkCandidateReportTests(unittest.TestCase):
    def _seed(self, mod, handle: str, *, priority: int = 0, seed_id: str | None = None):
        return mod.Seed(
            source_seed_id=seed_id or handle.strip("@"),
            platform="telegram",
            source_title=handle.strip("@"),
            handle=handle,
            url="https://t.me/" + handle.strip("@"),
            source_kind="unit",
            source_scope_guess="unit",
            priority=priority,
            discovered_from="unit",
            discovered_from_url="",
            why_seeded="unit",
            expected_value="unit",
            known_risks="",
            initial_status="pending_scan",
            monitoring_enabled=True,
            rights_policy="unknown",
            notes="",
        )

    def test_source_selection_prefers_cursor_queue_over_static_seed_rescans(self) -> None:
        mod = load_module()
        static_seed = self._seed(mod, "@staticold", priority=0, seed_id="seed_1")
        queue_seed = self._seed(mod, "@queuefresh", priority=10, seed_id="seed_2")
        key = mod.canonical_source_key("telegram", queue_seed.handle, queue_seed.canonical_url)
        previous_state = {
            "unified_source_queue_cursor_position": 100,
            "unified_source_queue": {
                key: {
                    "canonical_source_key": key,
                    "platform": "telegram",
                    "handle": queue_seed.handle,
                    "source_url": queue_seed.canonical_url,
                    "canonical_url": queue_seed.canonical_url,
                    "source_title": queue_seed.source_title,
                    "source_queue_status": "pending_scan",
                    "queue_order": 101,
                }
            },
        }

        selected = mod.selected_sources_for_run([static_seed, queue_seed], 1, previous_state=previous_state)

        self.assertEqual([s.handle for s in selected], [queue_seed.handle])

    def test_source_cursor_never_regresses_when_pending_gap_is_before_previous_cursor(self) -> None:
        mod = load_module()
        seed = self._seed(mod, "@freshcursor", seed_id="seed_cursor")
        key = mod.canonical_source_key("telegram", seed.handle, seed.canonical_url)
        previous_state = {
            "unified_source_queue_cursor_position": 100,
            "unified_source_queue": {
                "old-gap": {
                    "canonical_source_key": "old-gap",
                    "platform": "telegram",
                    "handle": "@oldgap",
                    "source_url": "https://t.me/oldgap",
                    "canonical_url": "https://t.me/oldgap",
                    "source_title": "oldgap",
                    "source_queue_status": "pending_scan",
                    "queue_order": 90,
                },
                key: {
                    "canonical_source_key": key,
                    "platform": "telegram",
                    "handle": seed.handle,
                    "source_url": seed.canonical_url,
                    "canonical_url": seed.canonical_url,
                    "source_title": seed.source_title,
                    "source_queue_status": "pending_scan",
                    "queue_order": 101,
                },
            },
        }
        source_rows = [{
            "source_id": seed.source_id,
            "platform": "telegram",
            "handle": seed.handle,
            "canonical_url": seed.canonical_url,
            "fetch_status": "ok",
            "fetch_attempted": "true",
            "posts_scanned": 3,
            "source_title": seed.source_title,
        }]

        _rows, metrics = mod.build_unified_source_queue(
            previous_state,
            [seed],
            source_rows,
            [],
            [],
            [],
            [],
            {},
            "unit-run",
            "2026-07-09T00:00:00+00:00",
        )

        self.assertGreaterEqual(metrics["source_queue_cursor_position"], 100)

    def test_runner_secret_names_only_include_selected_auth_bundle(self) -> None:
        mod = load_runner_module()
        names = mod.region_talk_secret_names("TELEGRAM_AUTH_BUNDLE_DISCOVERY1")
        self.assertIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY1", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_S22", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_E2E", names)

        names = mod.region_talk_secret_names("TELEGRAM_AUTH_BUNDLE_S22")
        self.assertIn("TELEGRAM_AUTH_BUNDLE_S22", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY1", names)

    def test_runner_config_serializes_orchestrator_telethon_limits(self) -> None:
        mod = load_runner_module()
        old_env = {k: os.environ.get(k) for k in [
            "REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT",
            "REGION_TALK_TG_PUBLIC_WEB_FALLBACK",
            "REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MIN_SECONDS",
            "REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN",
            "REGION_TALK_TELEGRAM_QUERY_SOURCE",
            "REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR",
            "REGION_TALK_TG_CACHED_ENTITY_ONLY",
            "REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN",
            "REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS",
            "REGION_TALK_YDB_ONLINE_QUEUE_WRITE_MAX_ROWS",
            "REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY",
            "REGION_TALK_YDB_ONLINE_CANDIDATE_WRITE_MAX_ROWS",
            "REGION_TALK_YDB_STATE_LOAD_ATTEMPTS",
            "REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS",
        ]}
        old_create = mod.create_or_replace_dataset
        old_wait = mod.wait_dataset_ready
        captured: dict[str, dict] = {}

        def fake_create(_client, _username, slug, _title, writer):
            with tempfile.TemporaryDirectory() as td:
                folder = Path(td)
                writer(folder)
                cfg = folder / "region_talk_run_config.json"
                if cfg.exists():
                    captured[slug] = json.loads(cfg.read_text(encoding="utf-8"))
            return "unit/" + slug

        try:
            os.environ["REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT"] = "3"
            os.environ["REGION_TALK_TG_PUBLIC_WEB_FALLBACK"] = "0"
            os.environ["REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MIN_SECONDS"] = "8"
            os.environ["REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN"] = "5"
            os.environ["REGION_TALK_TELEGRAM_QUERY_SOURCE"] = "place_lexicon"
            os.environ["REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR"] = "1"
            os.environ["REGION_TALK_TG_CACHED_ENTITY_ONLY"] = "1"
            os.environ["REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN"] = "0"
            os.environ["REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS"] = "80"
            os.environ["REGION_TALK_YDB_ONLINE_QUEUE_WRITE_MAX_ROWS"] = "80"
            os.environ["REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY"] = "1"
            os.environ["REGION_TALK_YDB_ONLINE_CANDIDATE_WRITE_MAX_ROWS"] = "80"
            os.environ["REGION_TALK_YDB_STATE_LOAD_ATTEMPTS"] = "4"
            os.environ["REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS"] = "20"
            mod.create_or_replace_dataset = fake_create
            mod.wait_dataset_ready = lambda *args, **kwargs: None
            mod.build_input_datasets(object(), run_id="unit-run", username="unit")
            env = next(iter(captured.values()))["env"]
            self.assertEqual(env["REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT"], "3")
            self.assertEqual(env["REGION_TALK_TG_PUBLIC_WEB_FALLBACK"], "0")
            self.assertEqual(env["REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MIN_SECONDS"], "8")
            self.assertEqual(env["REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN"], "5")
            self.assertEqual(env["REGION_TALK_TELEGRAM_QUERY_SOURCE"], "place_lexicon")
            self.assertEqual(env["REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR"], "1")
            self.assertEqual(env["REGION_TALK_TG_CACHED_ENTITY_ONLY"], "1")
            self.assertEqual(env["REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN"], "0")
            self.assertEqual(env["REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS"], "80")
            self.assertEqual(env["REGION_TALK_YDB_ONLINE_QUEUE_WRITE_MAX_ROWS"], "80")
            self.assertEqual(env["REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY"], "1")
            self.assertEqual(env["REGION_TALK_YDB_ONLINE_CANDIDATE_WRITE_MAX_ROWS"], "80")
            self.assertEqual(env["REGION_TALK_YDB_STATE_LOAD_ATTEMPTS"], "4")
            self.assertEqual(env["REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS"], "20")
        finally:
            mod.create_or_replace_dataset = old_create
            mod.wait_dataset_ready = old_wait
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_candidate_memory_ydb_write_changed_only_skips_not_refetched_rows(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY")
        try:
            os.environ["REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY"] = "1"
            rows = [
                {"candidate_memory_id": "old", "not_refetched_this_run": "true", "last_refetched_run_id": "previous"},
                {"candidate_memory_id": "fresh", "not_refetched_this_run": "false", "last_refetched_run_id": "run-1"},
                {"candidate_memory_id": "new", "not_refetched_this_run": "true", "first_candidate_run_id": "run-1"},
            ]
            selected = mod.candidate_memory_rows_for_ydb_write(rows, run_id="run-1", bge_memory_fusion_stats={"promoted": 0, "rejected": 0})
            self.assertEqual([r["candidate_memory_id"] for r in selected], ["fresh", "new"])
            selected_after_bge_change = mod.candidate_memory_rows_for_ydb_write(rows, run_id="run-1", bge_memory_fusion_stats={"promoted": 1, "rejected": 0})
            self.assertEqual([r["candidate_memory_id"] for r in selected_after_bge_change], ["fresh", "new"])
            rows[0]["external_bge_m3_fusion_changed_this_run"] = "true"
            selected_after_row_bge_change = mod.candidate_memory_rows_for_ydb_write(rows, run_id="run-1", bge_memory_fusion_stats={"promoted": 1, "rejected": 0})
            self.assertEqual([r["candidate_memory_id"] for r in selected_after_row_bge_change], ["old", "fresh", "new"])
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY", None)
            else:
                os.environ["REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY"] = old


    def test_telegram_governor_humanlike_pacing_is_logged_and_observable(self) -> None:
        mod = load_module()
        old_env = {
            key: os.environ.get(key)
            for key in [
                "REGION_TALK_TG_HUMANLIKE_PACING_ENABLED",
                "UNIT_TG_DELAY_MIN",
                "UNIT_TG_DELAY_MAX",
            ]
        }
        old_sleep = mod.asyncio.sleep
        old_uniform = mod.random.uniform
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        try:
            os.environ["REGION_TALK_TG_HUMANLIKE_PACING_ENABLED"] = "1"
            os.environ["UNIT_TG_DELAY_MIN"] = "0.1"
            os.environ["UNIT_TG_DELAY_MAX"] = "0.1"
            mod.asyncio.sleep = fake_sleep
            mod.random.uniform = lambda a, b: 0.1
            with tempfile.TemporaryDirectory() as td:
                gov = mod.TelegramRequestGovernor("unit-run", Path(td) / "out" / "run", {})
                ok = asyncio.run(gov.humanlike_pause(
                    "ResolveUsernameRequest",
                    "src",
                    "https://t.me/example",
                    min_env="UNIT_TG_DELAY_MIN",
                    max_env="UNIT_TG_DELAY_MAX",
                    default_min=20.0,
                    default_max=45.0,
                    reserve_seconds=0,
                ))
                self.assertTrue(ok)
                self.assertEqual(sleeps, [0.1])
                self.assertEqual(gov.humanlike_sleep_count, 1)
                self.assertAlmostEqual(gov.humanlike_sleep_total_seconds, 0.1)
                self.assertEqual(gov.ledger[-1]["method_class"], "humanlike_pacing")
                self.assertEqual(gov.ledger[-1]["decision"], "sleep")
                obs = gov.observability_row("start", "finish")
                self.assertEqual(obs["telegram_humanlike_pacing_enabled"], "true")
                self.assertEqual(obs["telegram_humanlike_sleep_count"], 1)
        finally:
            mod.asyncio.sleep = old_sleep
            mod.random.uniform = old_uniform
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_telegram_governor_floodwait_blocks_followup_calls(self) -> None:
        mod = load_module()
        with tempfile.TemporaryDirectory() as td:
            gov = mod.TelegramRequestGovernor("unit-run", Path(td) / "out" / "run", {})
            cooldown_until = gov.mark_floodwait("get_messages.exact_post_link", "post_link_queue", "https://t.me/example", 123)
            self.assertTrue(gov.degraded)
            self.assertEqual(gov.telegram_phase_status, "degraded_floodwait")
            self.assertEqual(gov.max_floodwait_seconds, 123)
            self.assertEqual(gov.floodwait_cooldown_until, cooldown_until)
            self.assertFalse(gov.has_total_request_budget("messages.searchGlobal", "keyword_discovery", "Калининград"))
            self.assertEqual(gov.ledger[-1]["decision"], "skipped_telegram_cooldown")

    def test_telegram_governor_remember_entity_builds_cached_peer(self) -> None:
        mod = load_module()
        class Entity:
            id = 12345
            access_hash = 67890
            title = "Cached Travel"

        old_modules = {name: sys.modules.get(name) for name in ["telethon", "telethon.tl", "telethon.tl.types"]}
        fake_telethon = types.ModuleType("telethon")
        fake_tl = types.ModuleType("telethon.tl")
        fake_types = types.ModuleType("telethon.tl.types")
        class InputPeerChannel:
            def __init__(self, channel_id, access_hash):
                self.channel_id = channel_id
                self.access_hash = access_hash
        fake_types.InputPeerChannel = InputPeerChannel
        try:
            sys.modules["telethon"] = fake_telethon
            sys.modules["telethon.tl"] = fake_tl
            sys.modules["telethon.tl.types"] = fake_types
            with tempfile.TemporaryDirectory() as td:
                gov = mod.TelegramRequestGovernor("unit-run", Path(td) / "out" / "run", {})
                self.assertTrue(gov.remember_entity("cachedtravel", Entity(), source="unit"))
                peer = gov.cached_input_peer("cachedtravel")
                self.assertIsNotNone(peer)
                self.assertEqual(peer.channel_id, 12345)
                self.assertEqual(peer.access_hash, 67890)
                self.assertEqual(gov.entity_cache["telegram:username:cachedtravel"]["channel_id_private"], "12345")
                self.assertEqual(gov.entity_cache["telegram:username:cachedtravel"]["access_hash_private"], "67890")
        finally:
            for name, value in old_modules.items():
                if value is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = value

    def test_telegram_governor_cached_entity_bypasses_resolve_cooldown(self) -> None:
        mod = load_module()
        class Entity:
            id = 12345
            access_hash = 67890
            title = "Cached Travel"

        old_modules = {name: sys.modules.get(name) for name in ["telethon", "telethon.tl", "telethon.tl.types"]}
        fake_telethon = types.ModuleType("telethon")
        fake_tl = types.ModuleType("telethon.tl")
        fake_types = types.ModuleType("telethon.tl.types")
        class InputPeerChannel:
            def __init__(self, channel_id, access_hash):
                self.channel_id = channel_id
                self.access_hash = access_hash
        fake_types.InputPeerChannel = InputPeerChannel
        try:
            sys.modules["telethon"] = fake_telethon
            sys.modules["telethon.tl"] = fake_tl
            sys.modules["telethon.tl.types"] = fake_types
            with tempfile.TemporaryDirectory() as td:
                gov = mod.TelegramRequestGovernor("unit-run", Path(td) / "out" / "run", {})
                self.assertTrue(gov.remember_entity("cachedtravel", Entity(), source="unit"))
                gov.mark_floodwait("get_entity.exact_post_link", "post_link_queue", "https://t.me/other", 3600)
                seed = mod.Seed(
                    source_seed_id="seed_cached",
                    platform="telegram",
                    source_title="Cached Travel",
                    handle="cachedtravel",
                    url="https://t.me/cachedtravel",
                    source_kind="channel",
                    source_scope_guess="travel",
                    priority=1,
                    discovered_from="unit",
                    discovered_from_url="",
                    why_seeded="unit",
                    expected_value="unit",
                    known_risks="",
                    initial_status="pending_scan",
                    monitoring_enabled=True,
                    rights_policy="manual_review",
                    notes="",
                )
                entity, meta = asyncio.run(gov.resolve_entity(object(), seed))
                self.assertIsNotNone(entity)
                self.assertEqual(entity.channel_id, 12345)
                self.assertEqual(meta["telegram_resolve_status"], "resolved_from_private_cache")
                self.assertEqual(gov.ledger[-1]["decision"], "skipped_cache_hit")
        finally:
            for name, value in old_modules.items():
                if value is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = value

    def test_telegram_governor_exact_resolve_floodwait_blocks_network_resolve(self) -> None:
        mod = load_module()
        with tempfile.TemporaryDirectory() as td:
            gov = mod.TelegramRequestGovernor("unit-run", Path(td) / "out" / "run", {})
            cooldown_until = gov.mark_floodwait("get_entity.exact_post_link", "post_link_queue", "https://t.me/example", 123)
            self.assertIn("method:ResolveUsernameRequest", gov.cooldowns)
            self.assertIn("method:get_entity.exact_post_link", gov.cooldowns)
            self.assertEqual(gov.cooldowns["method:ResolveUsernameRequest"]["cooldown_until"], cooldown_until)

    def test_telegram_governor_cached_entity_only_blocks_network_resolve(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_TG_CACHED_ENTITY_ONLY")
        try:
            os.environ["REGION_TALK_TG_CACHED_ENTITY_ONLY"] = "1"
            with tempfile.TemporaryDirectory() as td:
                gov = mod.TelegramRequestGovernor("unit-run", Path(td) / "out" / "run", {})
                seed = mod.Seed(
                    source_seed_id="seed_uncached",
                    platform="telegram",
                    source_title="Uncached",
                    handle="uncached",
                    url="https://t.me/uncached",
                    source_kind="channel",
                    source_scope_guess="travel",
                    priority=1,
                    discovered_from="unit",
                    discovered_from_url="",
                    why_seeded="unit",
                    expected_value="unit",
                    known_risks="",
                    initial_status="pending_scan",
                    monitoring_enabled=True,
                    rights_policy="manual_review",
                    notes="",
                )
                entity, meta = asyncio.run(gov.resolve_entity(object(), seed))
                self.assertIsNone(entity)
                self.assertEqual(meta["telegram_resolve_status"], "skipped_cached_entity_only_no_private_entity")
                self.assertEqual(gov.resolve_network_attempts, 0)
                self.assertEqual(gov.ledger[-1]["decision"], "skipped_cached_entity_only_no_private_entity")
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_TG_CACHED_ENTITY_ONLY", None)
            else:
                os.environ["REGION_TALK_TG_CACHED_ENTITY_ONLY"] = old

    def test_source_fast_check_backlog_prefers_cached_entities(self) -> None:
        mod = load_module()
        state = {
            "unified_source_queue_cursor_position": 0,
            "telegram_entity_cache": {
                "telegram:username:cached": {
                    "channel_id_private": "12345",
                    "access_hash_private": "67890",
                }
            },
            "unified_source_queue": {
                "telegram:username:uncached": {
                    "canonical_source_key": "telegram:username:uncached",
                    "platform": "telegram",
                    "source_url": "https://t.me/uncached",
                    "canonical_url": "https://t.me/uncached",
                    "handle": "uncached",
                    "queue_order": 1,
                    "source_queue_status": "pending_scan",
                },
                "telegram:username:cached": {
                    "canonical_source_key": "telegram:username:cached",
                    "platform": "telegram",
                    "source_url": "https://t.me/cached",
                    "canonical_url": "https://t.me/cached",
                    "handle": "cached",
                    "queue_order": 2,
                    "source_queue_status": "pending_scan",
                },
            },
        }
        seeds = mod.source_fast_check_backlog_seeds(state, 2)
        self.assertEqual([s.handle for s in seeds], ["@cached", "@uncached"])

    def test_source_surface_filter_catches_latin_kaliningrad_and_39_suffix(self) -> None:
        mod = load_module()
        latin = mod.source_surface_terminal_fields({
            "source_title": "Я люблю Калининград",
            "handle": "https://t.me/i_love_kaliningrad",
        })
        self.assertEqual(latin["source_queue_status"], mod.LOCAL_REGION_SOURCE_STATUS)
        self.assertEqual(latin["source_scope"], "local_region")
        self.assertIn("kaliningrad", latin["source_local_hits"])

        suffix = mod.source_surface_terminal_fields({
            "source_title": "молод39",
            "handle": "https://t.me/molod39",
        })
        self.assertEqual(suffix["source_queue_status"], mod.LOCAL_REGION_SOURCE_STATUS)
        self.assertEqual(suffix["source_scope"], "local_region")

    def test_source_queue_local_signal_overrides_processed_candidate_status(self) -> None:
        mod = load_module()
        now = "2026-07-09T00:00:00+00:00"
        rows, metrics = mod.build_unified_source_queue(
            {
                "unified_source_queue": {
                    "telegram:username:local": {
                        "canonical_source_key": "telegram:username:local",
                        "source_queue_id": "srcq_local",
                        "queue_order": 1,
                        "platform": "telegram",
                        "source_url": "https://t.me/i_love_kaliningrad",
                        "canonical_url": "https://t.me/i_love_kaliningrad",
                        "handle": "@i_love_kaliningrad",
                        "source_title": "https://t.me/i_love_kaliningrad",
                        "source_queue_status": "processed_found_ko_candidate",
                        "source_geo_class": "kaliningrad_local",
                        "source_quick_class": "local_region_source",
                        "posts_scanned": 43,
                        "ko_posts_found": 20,
                        "candidate_posts_found": 31,
                    }
                },
                "unified_source_queue_cursor_position": 0,
            },
            [],
            [],
            [],
            [],
            [],
            [],
            {},
            "unit-run",
            now,
        )
        self.assertEqual(rows[0]["source_queue_status"], mod.LOCAL_REGION_SOURCE_STATUS)
        self.assertEqual(rows[0]["source_scope"], "local_region")
        self.assertEqual(metrics["source_queue_rejected_local_region_source_total"], 1)

    def test_ydb_source_status_merge_preserves_terminal_local_status(self) -> None:
        mod = load_module()
        state = {
            "unified_source_queue": {
                "telegram:username:local": {
                    "canonical_source_key": "telegram:username:local",
                    "source_queue_status": mod.LOCAL_REGION_SOURCE_STATUS,
                    "source_scope": "local_region",
                    "source_geo_class": "kaliningrad_local",
                    "source_quick_class": "local_region_source",
                }
            }
        }
        mod.merge_ydb_source_queue_status_items(
            state,
            {},
            {
                "source_status_item:telegram:username:local": {
                    "canonical_source_key": "telegram:username:local",
                    "source_queue_status": "processed_found_ko_candidate",
                    "source_scope": "unknown",
                    "source_geo_class": "unknown",
                    "source_quick_class": "candidate_keep",
                    "posts_scanned": 50,
                }
            },
        )
        row = state["unified_source_queue"]["telegram:username:local"]
        self.assertEqual(row["source_queue_status"], mod.LOCAL_REGION_SOURCE_STATUS)
        self.assertEqual(row["source_scope"], "local_region")
        self.assertEqual(row["source_geo_class"], "kaliningrad_local")
        self.assertEqual(row["source_quick_class"], "local_region_source")
        self.assertEqual(row["posts_scanned"], 50)

    def test_ydb_online_write_circuit_breaker_disables_retries_after_auth_error(self) -> None:
        mod = load_module()
        old_env = os.environ.get("REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR")
        cache = dict(mod._YDB_BUSINESS_HEARTBEAT_CACHE)
        try:
            os.environ["REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR"] = "1"
            mod._YDB_BUSINESS_HEARTBEAT_CACHE.update({"online_write_disabled": False, "online_write_disabled_reason": "", "online_write_failures": 0})
            mod._record_ydb_online_write_failure(Exception("Unauthenticated: token expired"), label="unit")
            self.assertFalse(mod._ydb_online_write_allowed())
            self.assertIn("Unauthenticated", mod._YDB_BUSINESS_HEARTBEAT_CACHE["online_write_disabled_reason"])
        finally:
            mod._YDB_BUSINESS_HEARTBEAT_CACHE.clear()
            mod._YDB_BUSINESS_HEARTBEAT_CACHE.update(cache)
            if old_env is None:
                os.environ.pop("REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR", None)
            else:
                os.environ["REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR"] = old_env

    def test_runner_ydb_backend_refuses_missing_config_without_json_fallback(self) -> None:
        mod = load_runner_module()
        old = {k: os.environ.get(k) for k in [
            "REGION_TALK_STATE_BACKEND",
            "REGION_TALK_REQUIRE_YDB_STATE",
            "REGION_TALK_YDB_ENDPOINT",
            "REGION_TALK_YDB_DATABASE",
        ]}
        try:
            os.environ["REGION_TALK_STATE_BACKEND"] = "ydb"
            os.environ.pop("REGION_TALK_REQUIRE_YDB_STATE", None)
            os.environ.pop("REGION_TALK_YDB_ENDPOINT", None)
            os.environ.pop("REGION_TALK_YDB_DATABASE", None)
            with self.assertRaisesRegex(RuntimeError, "refusing json_fallback live run"):
                mod.preflight_ydb_access()
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

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
            self.assertIn("04p_publication_queue", workbook)
            self.assertIn("04q_publication_confirmed", workbook)
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
        self.assertEqual(media["postcardness_score"], 0.0)
        self.assertEqual(media["image_model_type"], "external_ydb_queue")
        self.assertEqual(media["image_model_runtime"], "not_run_in_candidate_report")
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

    def test_place_lexicon_builds_broad_discovery_and_preflight_terms(self) -> None:
        mod = load_module()
        lexicon = mod.load_place_lexicon(ROOT / "docs" / "features" / "region-talk-channel" / "kaliningrad-place-lexicon-v1.csv")
        terms = mod.build_region_talk_place_query_terms(lexicon)
        self.assertGreaterEqual(len(terms["source_preflight_terms"]), 90)
        self.assertGreaterEqual(len(terms["hashtag_terms"]), 70)
        for required in ["Калининград", "Балтийск", "Черняховск", "Куршская коса", "Рыбная деревня", "Виштынецкое озеро"]:
            self.assertIn(required, terms["source_preflight_terms"])
        for required in ["#Калининград", "#Балтийск", "#Куршскаякоса", "#Рыбнаядеревня"]:
            self.assertIn(required, terms["hashtag_terms"])
        self.assertIn("путешествие Калининград", terms["global_keyword_terms"])

    def test_post_link_queue_item_from_keyword_hit_keeps_exact_post_work(self) -> None:
        mod = load_module()
        row = {
            "keyword_hit_post_url": "https://t.me/travel_foo/123",
            "keyword_hit_source_url": "https://t.me/travel_foo",
            "canonical_source_key": "telegram:travel_foo",
            "source_title": "Travel Foo",
            "username_or_handle": "travel_foo",
            "matched_query": "Калининград",
            "keyword_hit_text_excerpt": "Ездили в Калининград, очень понравилась Куршская коса",
            "discovery_type": "telegram_keyword_search",
        }
        item = mod.post_link_queue_item_from_keyword_hit(row, run_id="unit-run")
        self.assertEqual(item["post_link_status"], "pending_fetch")
        self.assertEqual(item["post_url"], "https://t.me/travel_foo/123")
        self.assertEqual(item["source_key"], "telegram:travel_foo")
        self.assertEqual(item["matched_query"], "Калининград")
        self.assertEqual(item["priority_reason"], "global_keyword_search_exact_post")
        self.assertIn("postlink_", item["post_link_queue_id"])
        self.assertTrue(item["evidence_excerpt_hash"])

    def test_post_link_queue_item_from_keyword_hit_terminally_rejects_local_source(self) -> None:
        mod = load_module()
        row = {
            "keyword_hit_post_url": "https://t.me/kldevents/2158",
            "keyword_hit_source_url": "https://t.me/kldevents",
            "canonical_source_key": "telegram:kldevents",
            "source_title": "Калининград Афиша",
            "username_or_handle": "kldevents",
            "matched_query": "Калининград",
            "keyword_hit_text_excerpt": "Калининград сегодня",
            "discovery_type": "telegram_keyword_search",
        }
        item = mod.post_link_queue_item_from_keyword_hit(row, run_id="unit-run")
        self.assertEqual(item["post_link_status"], "terminal_source_rejected")
        self.assertEqual(item["source_scope"], "local_region")
        self.assertEqual(item["source_quick_class"], "local_region_source")
        self.assertEqual(item["next_action"], "do_not_fetch_exact_post_from_rejected_source")

    def test_fast_check_queries_use_broad_anchor_plus_poi_under_two_query_budget(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE")
        try:
            os.environ["REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE"] = "2"
            queries = mod.source_fast_check_queries("unit-run", 0)
            self.assertEqual(queries[0], "Калининград")
            self.assertEqual(len(queries), 2)
            self.assertIn(queries[1], {"Зеленоградск", "Куршская коса", "Светлогорск", "Балтийск", "Янтарный", "Черняховск", "Балтийская коса", "Виштынец", "Роминтенская пуща"})
            self.assertNotEqual(queries[1], "Калининградская область")
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE", None)
            else:
                os.environ["REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE"] = old

    def test_fast_check_no_hit_is_not_rechecked_or_prioritized_as_keyword(self) -> None:
        mod = load_module()
        previous_state = {
            "unified_source_queue_cursor_position": 10,
            "unified_source_queue": {
                "telegram:no_hit": {
                    "canonical_source_key": "telegram:no_hit",
                    "platform": "telegram",
                    "source_url": "https://t.me/no_hit",
                    "source_title": "Already preflighted",
                    "source_queue_status": "pending_scan",
                    "fast_check_status": "no_hit",
                    "discovery_type": "source_local_keyword_fast_check",
                    "queue_order": 11,
                },
                "telegram:fresh": {
                    "canonical_source_key": "telegram:fresh",
                    "platform": "telegram",
                    "source_url": "https://t.me/fresh",
                    "source_title": "Fresh source",
                    "source_queue_status": "pending_scan",
                    "queue_order": 12,
                },
            },
        }
        self.assertEqual(mod.source_queue_priority_bucket(previous_state["unified_source_queue"]["telegram:no_hit"]), 2)
        seeds = mod.source_fast_check_backlog_seeds(previous_state, 5)
        self.assertEqual([s.canonical_url for s in seeds], ["https://t.me/fresh"])

    def test_fast_check_ko_hit_source_is_primary_due_even_after_partial_scan(self) -> None:
        mod = load_module()
        hit = mod.Seed(
            source_seed_id="unified_queue_hit", platform="telegram", source_title="KO hit source",
            handle="@ko_hit", url="https://t.me/ko_hit", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="fast-check", expected_value="", known_risks="", initial_status="needs_rescan_or_retry",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        fresh = mod.Seed(
            source_seed_id="unified_queue_fresh", platform="telegram", source_title="Fresh",
            handle="@fresh_after_hit", url="https://t.me/fresh_after_hit", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="fresh", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {"unified_source_queue": {
            "telegram:ko_hit": {
                "canonical_source_key": "telegram:ko_hit",
                "source_url": "https://t.me/ko_hit",
                "source_queue_status": "needs_rescan_or_retry",
                "posts_scanned": 12,
                "last_scan_run_id": "old-run",
                "fast_check_status": "ko_hit",
                "fast_check_hit_post_url": "https://t.me/ko_hit/10",
                "queue_order": 50,
            },
            "telegram:fresh_after_hit": {
                "canonical_source_key": "telegram:fresh_after_hit",
                "source_url": "https://t.me/fresh_after_hit",
                "source_queue_status": "pending_scan",
                "queue_order": 51,
            },
        }}
        due = mod._seed_scan_due_state(hit, previous_state)
        self.assertTrue(due["due"])
        self.assertFalse(due["is_rescan"])
        self.assertEqual(due["reason"], "fast_check_ko_hit_priority")
        selected = mod.selected_sources_for_run([fresh, hit], 2, previous_state=previous_state)
        self.assertEqual([s.canonical_url for s in selected], ["https://t.me/ko_hit", "https://t.me/fresh_after_hit"])

    def test_fast_check_priority_source_queue_row_moves_after_cursor_and_persists_evidence(self) -> None:
        mod = load_module()
        seed = mod.Seed(
            source_seed_id="unified_queue_hit", platform="telegram", source_title="KO hit source",
            handle="@ko_hit", url="https://t.me/ko_hit", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="fast-check", expected_value="", known_risks="", initial_status="needs_rescan_or_retry",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {
            "unified_source_queue_cursor_position": 100,
            "unified_source_queue": {
                "telegram:ko_hit": {
                    "canonical_source_key": "telegram:ko_hit",
                    "source_url": "https://t.me/ko_hit",
                    "canonical_url": "https://t.me/ko_hit",
                    "source_queue_status": "needs_rescan_or_retry",
                    "queue_order": 431,
                    "posts_scanned": 4,
                }
            },
        }
        row = mod.fast_check_priority_source_queue_row(
            seed,
            {
                "fast_check_status": "ko_hit",
                "keyword_hit_post_url": "https://t.me/ko_hit/10",
                "matched_query": "Калининград",
                "source_title": "KO hit source",
            },
            previous_state,
            run_id="run-1",
            priority_offset=2,
        )
        self.assertEqual(row["queue_order"], 102)
        self.assertEqual(row["previous_queue_order"], 431)
        self.assertEqual(row["queue_order_changed_reason"], "fast_check_ko_hit_reinsert_after_cursor")
        self.assertEqual(row["fast_check_status"], "ko_hit")
        self.assertEqual(row["fast_check_hit_post_url"], "https://t.me/ko_hit/10")
        self.assertEqual(row["fast_check_matched_query"], "Калининград")

    def test_ydb_candidate_link_rows_respects_zero_limit_before_connect(self) -> None:
        mod = load_module()
        self.assertEqual(mod.ydb_candidate_link_rows_from_row_kv(0, kinds=("post_link_queue_item",)), [])

    def test_online_discovery_items_are_capped_before_ydb_write(self) -> None:
        mod = load_module()
        old_env = os.environ.get("REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES")
        old_writer = mod.write_region_talk_online_queue_items
        calls = []
        try:
            os.environ["REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES"] = "2"
            def fake_writer(rows, **kwargs):
                calls.append((kwargs.get("kind"), len(rows)))
                return len(rows)
            mod.write_region_talk_online_queue_items = fake_writer
            rows = [{"source_candidate_id": f"s{i}", "canonical_url": f"https://t.me/s{i}"} for i in range(5)]
            out = mod.write_region_talk_online_discovery_items(rows, [], run_id="unit", stage="unit")
            self.assertEqual(out["source_candidate_item"], 2)
            self.assertIn(("source_candidate_item", 2), calls)
        finally:
            mod.write_region_talk_online_queue_items = old_writer
            if old_env is None:
                os.environ.pop("REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES", None)
            else:
                os.environ["REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES"] = old_env

    def test_telegram_keyword_query_plan_uses_lexicon_hashtag_quota_and_rotation(self) -> None:
        mod = load_module()
        old_env = {k: os.environ.get(k) for k in [
            "REGION_TALK_TELEGRAM_QUERY_SOURCE",
            "REGION_TALK_TELEGRAM_KEYWORD_QUERIES",
            "REGION_TALK_TELEGRAM_HASHTAG_QUERIES",
            "REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES",
            "REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES",
            "REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN",
            "REGION_TALK_TELEGRAM_QUERY_ROTATE",
            "REGION_TALK_TELEGRAM_QUERY_ROTATE_OFFSET",
        ]}
        try:
            os.environ.pop("REGION_TALK_TELEGRAM_KEYWORD_QUERIES", None)
            os.environ.pop("REGION_TALK_TELEGRAM_HASHTAG_QUERIES", None)
            os.environ["REGION_TALK_TELEGRAM_QUERY_SOURCE"] = "place_lexicon"
            os.environ["REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES"] = "7"
            os.environ["REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES"] = "3"
            os.environ["REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN"] = "4"
            os.environ["REGION_TALK_TELEGRAM_QUERY_ROTATE"] = "0"
            plan = mod.build_telegram_keyword_query_plan("unit-run")
            self.assertEqual(plan["keyword_query_source"], "place_lexicon")
            self.assertEqual(plan["hashtag_query_source"], "place_lexicon")
            self.assertEqual(len(plan["selected_keyword_queries"]), 3)
            self.assertEqual(len(plan["selected_hashtag_queries"]), 4)
            self.assertEqual(len(plan["selected_queries"]), 7)
            self.assertGreaterEqual(plan["hashtag_terms_planned_total"], 70)
            self.assertGreaterEqual(plan["source_preflight_terms_planned_total"], 90)
            self.assertIn("#Калининград", plan["selected_hashtag_queries"])
        finally:
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_kaliningrad_only_scope_rejects_multi_region_lists(self) -> None:
        mod = load_module()
        lexicon = mod.load_place_lexicon(ROOT / "docs" / "features" / "region-talk-channel" / "kaliningrad-place-lexicon-v1.csv")
        good = mod.kaliningrad_oblast_only_scope_gate("Маршрут: Калининград, Зеленоградск, Светлогорск и Куршская коса. Очень атмосферная поездка.", lexicon)
        self.assertTrue(good["kaliningrad_oblast_only_scope"])
        bad = mod.kaliningrad_oblast_only_scope_gate("Куда поехать летом: Байкал, Дагестан, Калининград и Сочи — 10 мест России", lexicon)
        self.assertFalse(bad["kaliningrad_oblast_only_scope"])
        self.assertIn("байкал", bad["external_geo_mentions"])


    def test_source_selection_skips_recently_processed_publics_until_due(self) -> None:
        mod = load_module()
        processed = mod.Seed(
            source_seed_id="unified_queue_processed", platform="telegram", source_title="Путешествуем.РФ",
            handle="@puteshestvuem_rf", url="https://t.me/puteshestvuem_rf", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="processed", expected_value="", known_risks="", initial_status="processed_found_ko",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        pending = mod.Seed(
            source_seed_id="unified_queue_pending", platform="telegram", source_title="Новый тревел",
            handle="@new_travel", url="https://t.me/new_travel", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="pending", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {
            "unified_source_queue": {
                "telegram:puteshestvuem_rf": {
                    "canonical_source_key": "telegram:puteshestvuem_rf",
                    "source_url": "https://t.me/puteshestvuem_rf",
                    "source_queue_status": "processed_found_ko",
                    "_ydb_updated_at": mod.utc_now_iso(),
                    "queue_order": 1,
                },
                "telegram:new_travel": {
                    "canonical_source_key": "telegram:new_travel",
                    "source_url": "https://t.me/new_travel",
                    "source_queue_status": "pending_scan",
                    "queue_order": 2,
                },
            }
        }
        selected = mod.selected_sources_for_run([processed, pending], 2, previous_state=previous_state)
        self.assertEqual([s.canonical_url for s in selected], ["https://t.me/new_travel"])
        self.assertEqual(mod._REGION_TALK_TELEGRAM_RUNTIME["source_selection_not_due_total"], 1)

    def test_pending_row_with_scan_evidence_waits_until_primary_queue_empty(self) -> None:
        mod = load_module()
        partially_scanned = mod.Seed(
            source_seed_id="unified_queue_partial", platform="telegram", source_title="Частично просмотрен",
            handle="@partial_travel", url="https://t.me/partial_travel", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="partial", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        never_scanned = mod.Seed(
            source_seed_id="unified_queue_fresh", platform="telegram", source_title="Новый паблик",
            handle="@fresh_travel", url="https://t.me/fresh_travel", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="fresh", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {"unified_source_queue": {
            "telegram:partial_travel": {
                "canonical_source_key": "telegram:partial_travel",
                "source_url": "https://t.me/partial_travel",
                "source_queue_status": "pending_scan",
                "posts_scanned": 12,
                "last_scan_run_id": "run-old",
                "queue_order": 1,
            },
            "telegram:fresh_travel": {
                "canonical_source_key": "telegram:fresh_travel",
                "source_url": "https://t.me/fresh_travel",
                "source_queue_status": "pending_scan",
                "queue_order": 2,
            },
        }}
        selected = mod.selected_sources_for_run([partially_scanned, never_scanned], 2, previous_state=previous_state)
        self.assertEqual([s.canonical_url for s in selected], ["https://t.me/fresh_travel"])
        partial_due = mod._seed_scan_due_state(partially_scanned, previous_state)
        self.assertTrue(partial_due["is_rescan"])
        self.assertNotEqual(partial_due["reason"], "no_previous_scan_cursor")

    def test_source_selection_rescans_processed_public_when_due(self) -> None:
        mod = load_module()
        seed = mod.Seed(
            source_seed_id="unified_queue_processed_due", platform="telegram", source_title="Путешествуем.РФ",
            handle="@puteshestvuem_rf", url="https://t.me/puteshestvuem_rf", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="processed", expected_value="", known_risks="", initial_status="processed_found_ko",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {"source_cursors": {seed.source_id: {"next_history_scan_at": "2020-01-01T00:00:00+00:00", "primary_scan_completed_at": "2020-01-01T00:00:00+00:00"}}}
        selected = mod.selected_sources_for_run([seed], 1, previous_state=previous_state)
        self.assertEqual([s.canonical_url for s in selected], ["https://t.me/puteshestvuem_rf"])



    def test_discovery_tail_reserve_honors_explicit_orchestrator_budget(self) -> None:
        mod = load_module()
        old = {
            key: os.environ.get(key)
            for key in [
                "REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS",
                "REGION_TALK_PRIORITIZE_TEXT_VECTORS",
                "REGION_TALK_TEXT_EMBEDDING_PRIORITY_MIN_MODEL_TIMEOUT_SECONDS",
                "REGION_TALK_RUNTIME_RESERVE_DURING_TEXT_EMBEDDING_SECONDS",
            ]
        }
        try:
            os.environ["REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS"] = "240"
            os.environ["REGION_TALK_PRIORITIZE_TEXT_VECTORS"] = "1"
            os.environ["REGION_TALK_TEXT_EMBEDDING_PRIORITY_MIN_MODEL_TIMEOUT_SECONDS"] = "420"
            os.environ["REGION_TALK_RUNTIME_RESERVE_DURING_TEXT_EMBEDDING_SECONDS"] = "120"
            self.assertEqual(mod.discovery_tail_reserve_seconds(), 240)
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_post_age_days_summary_measures_history_depth(self) -> None:
        mod = load_module()
        now = mod.datetime(2026, 7, 8, 12, 0, tzinfo=mod.timezone.utc)
        rows = [
            {"post_date": "2026-07-08T00:00:00+00:00"},
            {"post_date": "2026-07-06T12:00:00+00:00"},
        ]
        summary = mod.post_age_days_summary(rows, now=now)
        self.assertEqual(summary["history_posts_with_dates"], 2)
        self.assertEqual(summary["history_avg_post_age_days"], 1.25)
        self.assertEqual(summary["history_newest_post_age_days"], 0.5)
        self.assertEqual(summary["history_oldest_post_age_days"], 2.0)
        self.assertEqual(summary["history_newest_post_date"], "2026-07-08T00:00:00+00:00")
        self.assertEqual(summary["history_oldest_post_date"], "2026-07-06T12:00:00+00:00")

    def test_history_age_cutoff_defaults_to_one_year(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_HISTORY_MAX_POST_AGE_DAYS")
        try:
            os.environ.pop("REGION_TALK_HISTORY_MAX_POST_AGE_DAYS", None)
            cutoff = mod.history_min_post_datetime(mod.datetime(2026, 7, 8, tzinfo=mod.timezone.utc))
            self.assertEqual(cutoff.date().isoformat(), "2025-07-08")
            self.assertTrue(mod.is_history_post_older_than_cutoff("2025-07-07T23:59:59+00:00", cutoff))
            self.assertFalse(mod.is_history_post_older_than_cutoff("2025-07-08T00:00:01+00:00", cutoff))
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_HISTORY_MAX_POST_AGE_DAYS", None)
            else:
                os.environ["REGION_TALK_HISTORY_MAX_POST_AGE_DAYS"] = old

    def test_text_post_volume_rejects_at_thirty_per_day(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD")
        try:
            os.environ["REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD"] = "30"
            counts = {}
            for _ in range(29):
                decision = mod.record_text_post_volume(counts, "2026-07-08T10:00:00+00:00")
                self.assertFalse(decision["rejected"])
            decision = mod.record_text_post_volume(counts, "2026-07-08T23:59:00+00:00")
            self.assertTrue(decision["rejected"])
            self.assertEqual(decision["day"], "2026-07-08")
            self.assertEqual(decision["count"], 30)
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD", None)
            else:
                os.environ["REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD"] = old

    def test_high_volume_source_queue_status_is_terminal_and_not_due(self) -> None:
        mod = load_module()
        seed = mod.Seed(
            source_seed_id="unified_queue_news", platform="telegram", source_title="Новости 24",
            handle="@news24", url="https://t.me/news24", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="news", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {"unified_source_queue": {"telegram:news24": {
            "canonical_source_key": "telegram:news24", "source_url": "https://t.me/news24",
            "source_queue_status": mod.HIGH_VOLUME_TEXT_POSTS_STATUS, "queue_order": 1,
            "_ydb_updated_at": "2020-01-01T00:00:00+00:00",
        }}}
        due = mod._seed_scan_due_state(seed, previous_state)
        self.assertFalse(due["due"])
        self.assertEqual(due["reason"], "terminal_rejected_source")
        selected = mod.selected_sources_for_run([seed], 1, previous_state=previous_state)
        self.assertEqual(selected, [])

    def test_source_queue_posts_scanned_never_decreases_on_rescan(self) -> None:
        mod = load_module()
        previous = {"unified_source_queue": {"telegram:travel": {
            "canonical_source_key": "telegram:travel", "platform": "telegram",
            "source_url": "https://t.me/travel", "queue_order": 1,
            "source_queue_status": "processed_no_ko", "posts_scanned": 31,
        }}}
        source_row = {
            "source_id": "src_travel", "platform": "telegram", "canonical_url": "https://t.me/travel",
            "canonical_source_key": "telegram:travel", "fetch_status": "ok", "posts_scanned": 17,
        }
        rows, _ = mod.build_unified_source_queue(previous, [], [source_row], [], [], [], [], {}, "run-q", "2026-07-08T00:00:00+00:00")
        row = next(r for r in rows if r["canonical_source_key"] == "telegram:travel")
        self.assertEqual(row["posts_scanned"], 31)

    def test_source_queue_preserves_high_volume_rejection(self) -> None:
        mod = load_module()
        previous = {"unified_source_queue": {"telegram:news24": {
            "canonical_source_key": "telegram:news24", "platform": "telegram",
            "source_url": "https://t.me/news24", "queue_order": 1, "source_queue_status": "pending_scan",
        }}}
        source_row = {
            "source_id": "src_news", "platform": "telegram", "canonical_url": "https://t.me/news24",
            "canonical_source_key": "telegram:news24", "fetch_status": mod.HIGH_VOLUME_TEXT_POSTS_STATUS,
            "monitoring_exclusion_reason": mod.HIGH_VOLUME_TEXT_POSTS_REASON,
            "high_volume_text_posts_date": "2026-07-08", "high_volume_text_posts_count": 30,
            "high_volume_text_posts_threshold": 30, "posts_scanned": 0,
        }
        rows, metrics = mod.build_unified_source_queue(previous, [], [source_row], [], [], [], [], {}, "run-q", "2026-07-08T00:00:00+00:00")
        row = next(r for r in rows if r["canonical_source_key"] == "telegram:news24")
        self.assertEqual(row["source_queue_status"], mod.HIGH_VOLUME_TEXT_POSTS_STATUS)
        self.assertEqual(row["monitoring_exclusion_reason"], mod.HIGH_VOLUME_TEXT_POSTS_REASON)
        self.assertEqual(row["next_action"], "do_not_rescan_rejected_source")
        self.assertEqual(row["high_volume_text_posts_count"], 30)
        self.assertEqual(metrics["source_queue_rejected_high_volume_total"], 1)

    def test_final_verifier_waits_for_actual_image_scoring(self) -> None:
        mod = load_module()
        old_mode = os.environ.get("REGION_TALK_TEXT_VECTOR_MODE")
        old_enable = os.environ.get("REGION_TALK_ENABLE_FINAL_LLM_VERIFIER")
        old_timeout = os.environ.get("REGION_TALK_ABORT_REPORT_TAIL_ON_EMBEDDING_DEFER")
        os.environ["REGION_TALK_TEXT_VECTOR_MODE"] = "prototype"
        os.environ["REGION_TALK_ENABLE_FINAL_LLM_VERIFIER"] = "1"
        os.environ["REGION_TALK_ABORT_REPORT_TAIL_ON_EMBEDDING_DEFER"] = "0"
        calls = []
        old_call = mod.call_region_talk_semantic_llm
        old_gate = mod.text_vector_gate
        old_blogger = mod.load_public_blogger_links
        old_xlsx = mod.write_xlsx
        try:
            mod.load_public_blogger_links = lambda previous_state: []
            mod.write_xlsx = lambda path, sheets: Path(path).write_text("xlsx skipped in unit", encoding="utf-8")
            mod.call_region_talk_semantic_llm = lambda *args, **kwargs: calls.append(args) or {"llm_gate_status": "ok", "llm_decision": "accept", "llm_reason": "unit", "llm_model": "unit"}
            def fake_gate(text, ts, scope, ad_gate, substance, embedding_scores=None, allow_embedding_fallback=True):
                return {
                    "vector_gate_status": "vector_accept_candidate",
                    "vector_content_type": "visit_impression_candidate",
                    "vector_positive_score": 0.9,
                    "vector_news_event_score": 0.1,
                    "vector_ad_promo_score": 0.1,
                    "vector_roundup_score": 0.1,
                    "vector_low_substance_score": 0.1,
                    "vector_visit_impression_score": 0.9,
                    "vector_route_useful_score": 0.2,
                    "vector_emotion_observation_score": 0.8,
                    "vector_margin_positive_vs_negative": 0.5,
                    "vector_rejection_reason": "",
                    "vector_gate_confidence": "high",
                    "needs_llm_final_verify": "true",
                    "llm_status": "not_called_until_final_verifier",
                    "llm_not_called_reason": "",
                    "text_embedding_model_id": "unit",
                    "text_embedding_runtime": "unit",
                }
            mod.text_vector_gate = fake_gate
            seed = mod.Seed("s", "telegram", "Travel", "@travel", "https://t.me/travel", "travel_media", "", 1, "unit", "", "", "", "", "pending_scan", True, "unknown", "")
            post = {
                "post_id": "p1", "source_id": seed.source_id, "source_seed_id": seed.source_seed_id,
                "source_title": "Travel", "platform": "telegram", "handle": "@travel",
                "post_url": "https://t.me/travel/1", "platform_post_key": "tg:travel:1",
                "post_date": "2026-06-01T12:00:00+00:00",
                "text": "Личный отзыв о поездке в Калининградскую область: Куршская коса, море, эмоции и советы.",
                "has_media": True, "media_count": 1, "rights_policy": "unknown",
                "source_kind": "travel_media", "source_type": "travel_media", "source_url": "https://t.me/travel",
            }
            with tempfile.TemporaryDirectory() as td:
                report = mod.build_report([seed], [], [post], "final-image-wait-unit", Path(td))
            self.assertEqual(calls, [])
            events = report.get("summary", {})
            self.assertEqual(events.get("final_verifier_llm_calls"), 0)
        finally:
            mod.call_region_talk_semantic_llm = old_call
            mod.text_vector_gate = old_gate
            mod.load_public_blogger_links = old_blogger
            mod.write_xlsx = old_xlsx
            for key, val in [("REGION_TALK_TEXT_VECTOR_MODE", old_mode), ("REGION_TALK_ENABLE_FINAL_LLM_VERIFIER", old_enable), ("REGION_TALK_ABORT_REPORT_TAIL_ON_EMBEDDING_DEFER", old_timeout)]:
                if val is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = val

    def test_vector_probe_loads_state_with_output_dir_and_writes_result(self) -> None:
        mod = load_module()
        old_load = mod.load_region_talk_state
        old_scores = mod.dual_model_semantic_scores_batch
        old_ydb = mod._write_vector_probe_result_to_ydb
        calls = []
        try:
            def fake_load(output_dir):
                calls.append(Path(output_dir))
                return ({"candidate_memory": {"c1": {"candidate_memory_id": "c1", "post_url": "https://t.me/a/1", "short_summary": "Личный отзыв о Калининграде"}}}, {"ydb_read_status": "ok"})
            mod.load_region_talk_state = fake_load
            mod.dual_model_semantic_scores_batch = lambda texts, report_event=None: [{"text_embedding_model_id": "+".join(mod.TEXT_EMBEDDING_MODELS), "vector_top_class": "ko_visit_impression", "vector_top_score": 0.9}]
            ydb_writes = []
            mod._write_vector_probe_result_to_ydb = lambda result: ydb_writes.append(result["summary"].get("ydb_write_status")) or True
            with tempfile.TemporaryDirectory() as td:
                out = mod.run_vector_probe("probe-unit", Path(td), status=None)
                self.assertTrue(calls and calls[0] == Path(td))
                self.assertTrue(out["ok"])
                self.assertEqual(out["summary"]["ydb_write_status"], "ok")
                self.assertEqual(ydb_writes, ["pending", "ok"])
                self.assertTrue((Path(td) / "vector_probe_result.json").exists())
        finally:
            mod.load_region_talk_state = old_load
            mod.dual_model_semantic_scores_batch = old_scores
            mod._write_vector_probe_result_to_ydb = old_ydb
            Path("output.json").unlink(missing_ok=True)

    def test_dual_embeddings_run_sequential_model_passes_and_release_each_model(self) -> None:
        mod = load_module()
        old_mode = os.environ.get("REGION_TALK_TEXT_VECTOR_MODE")
        old_require = os.environ.get("REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS")
        os.environ["REGION_TALK_TEXT_VECTOR_MODE"] = "dual_embeddings"
        os.environ["REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS"] = "1"
        calls = []
        releases = []
        events = []
        old_runner = mod._run_embedding_model_pass_bounded
        old_release = mod.release_text_embedding_model
        def fake_runner(model_id, texts, flat_labels, flat_texts, semantic_bank_version, bank_hash, report_event=None):
            calls.append((model_id, list(texts)))
            labels = sorted(set(flat_labels))
            scores = {label: (0.9 if label == "ko_visit_impression" else 0.1) for label in labels}
            return {"ok": True, "scores": [dict(scores) for _ in texts]}
        try:
            mod._run_embedding_model_pass_bounded = fake_runner
            mod.release_text_embedding_model = lambda model_id: releases.append(model_id)
            out = mod.dual_model_semantic_scores_batch(["текст 1", "текст 2"], report_event=lambda name, **payload: events.append((name, payload)))
            self.assertEqual([c[0] for c in calls], mod.TEXT_EMBEDDING_MODELS)
            self.assertEqual([c[1] for c in calls], [["текст 1", "текст 2"], ["текст 1", "текст 2"]])
            self.assertEqual(releases, mod.TEXT_EMBEDDING_MODELS)
            started = [payload for name, payload in events if name == "text_embedding_model_pass_started"]
            self.assertTrue(started)
            self.assertTrue(all(p.get("text_embedding_models_loaded") == 0 for p in started))
            self.assertTrue(all(p.get("text_embedding_execution_mode") == "sequential_one_model_in_memory" for p in started))
            self.assertEqual(len(out), 2)
            self.assertEqual(out[0].get("vector_fusion_reason"), "both_models")
        finally:
            mod._run_embedding_model_pass_bounded = old_runner
            mod.release_text_embedding_model = old_release
            if old_mode is None:
                os.environ.pop("REGION_TALK_TEXT_VECTOR_MODE", None)
            else:
                os.environ["REGION_TALK_TEXT_VECTOR_MODE"] = old_mode
            if old_require is None:
                os.environ.pop("REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS", None)
            else:
                os.environ["REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS"] = old_require

    def test_text_vector_priority_protects_embedding_timeout_and_discovery_reserve(self) -> None:
        mod = load_module()
        old = {k: os.environ.get(k) for k in [
            "REGION_TALK_PRIORITIZE_TEXT_VECTORS",
            "REGION_TALK_TEXT_EMBEDDING_MODEL_TIMEOUT_SECONDS",
            "REGION_TALK_TEXT_EMBEDDING_PRIORITY_MIN_MODEL_TIMEOUT_SECONDS",
            "REGION_TALK_RUNTIME_RESERVE_DURING_TEXT_EMBEDDING_SECONDS",
            "REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS",
        ]}
        try:
            os.environ["REGION_TALK_PRIORITIZE_TEXT_VECTORS"] = "1"
            os.environ["REGION_TALK_TEXT_EMBEDDING_MODEL_TIMEOUT_SECONDS"] = "30"
            os.environ["REGION_TALK_TEXT_EMBEDDING_PRIORITY_MIN_MODEL_TIMEOUT_SECONDS"] = "420"
            os.environ["REGION_TALK_RUNTIME_RESERVE_DURING_TEXT_EMBEDDING_SECONDS"] = "120"
            os.environ.pop("REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS", None)
            self.assertEqual(mod.text_embedding_model_timeout_seconds(), 420)
            self.assertGreaterEqual(mod.discovery_tail_reserve_seconds(), 420 * 2 + 120)
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_source_selection_finishes_pending_queue_before_processed_rescans(self) -> None:
        mod = load_module()
        processed_due = mod.Seed(
            source_seed_id="processed_due", platform="telegram", source_title="Старый обработанный",
            handle="@old_done", url="https://t.me/old_done", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="processed", expected_value="", known_risks="", initial_status="processed_found_ko",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        pending = mod.Seed(
            source_seed_id="pending_new", platform="telegram", source_title="Непройденный",
            handle="@pending_new", url="https://t.me/pending_new", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=10, discovered_from="unit", discovered_from_url="",
            why_seeded="pending", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {
            "source_cursors": {processed_due.source_id: {"next_history_scan_at": "2020-01-01T00:00:00+00:00", "primary_scan_completed_at": "2020-01-01T00:00:00+00:00"}},
            "unified_source_queue": {
                "telegram:old_done": {"canonical_source_key": "telegram:old_done", "source_url": "https://t.me/old_done", "source_queue_status": "processed_found_ko", "queue_order": 1},
                "telegram:pending_new": {"canonical_source_key": "telegram:pending_new", "source_url": "https://t.me/pending_new", "source_queue_status": "pending_scan", "queue_order": 2},
            },
        }
        selected = mod.selected_sources_for_run([processed_due, pending], 2, previous_state=previous_state)
        self.assertEqual([s.canonical_url for s in selected], ["https://t.me/pending_new"])

    def test_keyword_sources_inserted_after_cursor_win_over_product_priority(self) -> None:
        mod = load_module()
        keyword = mod.Seed(
            source_seed_id="pending_keyword", platform="telegram", source_title="Случайный канал",
            handle="@keyword_hit", url="https://t.me/keyword_hit", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="telegram_keyword_search", discovered_from_url="",
            why_seeded="keyword", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        travel = mod.Seed(
            source_seed_id="pending_travel", platform="telegram", source_title="Авторские путешествия по России",
            handle="@author_travel", url="https://t.me/author_travel", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="pending", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {"unified_source_queue": {
            "telegram:keyword_hit": {
                "canonical_source_key": "telegram:keyword_hit", "source_url": "https://t.me/keyword_hit",
                "source_title": "Случайный канал", "source_queue_status": "pending_scan",
                "queue_order": 10, "added_from": "telegram_keyword_search", "insertion_policy": "insert_after_cursor",
            },
            "telegram:author_travel": {
                "canonical_source_key": "telegram:author_travel", "source_url": "https://t.me/author_travel",
                "source_title": "Авторские путешествия по России", "source_queue_status": "pending_scan",
                "queue_order": 11, "added_from": "public_travel_blogger_catalog",
            },
        }}
        selected = mod.selected_sources_for_run([travel, keyword], 2, previous_state=previous_state)
        self.assertEqual([s.canonical_url for s in selected], ["https://t.me/keyword_hit", "https://t.me/author_travel"])

    def test_keyword_existing_pending_is_physically_reinserted_after_cursor(self) -> None:
        mod = load_module()
        previous = {"unified_source_queue_cursor_position": 5, "unified_source_queue": {
            "telegram:ordinary": {
                "canonical_source_key": "telegram:ordinary", "platform": "telegram",
                "source_url": "https://t.me/ordinary", "source_title": "Обычный паблик",
                "source_queue_status": "pending_scan", "queue_order": 6,
            },
            "telegram:keyword_hit": {
                "canonical_source_key": "telegram:keyword_hit", "platform": "telegram",
                "source_url": "https://t.me/keyword_hit", "source_title": "External Travel Notes",
                "source_queue_status": "pending_scan", "queue_order": 100,
                "added_from": "telegram_keyword_search", "insertion_policy": "legacy_tail_keyword",
            },
        }}
        rows, metrics = mod.build_unified_source_queue(previous, [], [], [], [], [], [], {}, "run-keyword", "2026-07-09T00:00:00+00:00")
        by_key = {r["canonical_source_key"]: r for r in rows}
        self.assertEqual(by_key["telegram:keyword_hit"]["queue_order"], 6)
        self.assertEqual(by_key["telegram:ordinary"]["queue_order"], 7)
        self.assertEqual(by_key["telegram:keyword_hit"]["queue_order_changed_this_run"], "true")
        self.assertEqual(metrics["source_queue_keyword_existing_promoted_this_run"], 1)
        old_max = os.environ.get("REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS")
        try:
            os.environ["REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS"] = "1"
            handoff = mod._source_queue_handoff_rows(rows, cursor_position=5, run_id="run-keyword")
        finally:
            if old_max is None:
                os.environ.pop("REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS", None)
            else:
                os.environ["REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS"] = old_max
        self.assertEqual(len([r for r in handoff if not r.get("_sheet_note")]), 1)
        self.assertEqual(handoff[0]["canonical_source_key"], "telegram:keyword_hit")

    def test_hashtag_keyword_surface_filter_routes_local_and_spam_sources_out_of_cursor(self) -> None:
        mod = load_module()
        previous = {"unified_source_queue_cursor_position": 10, "unified_source_queue": {
            "telegram:ordinary": {
                "canonical_source_key": "telegram:ordinary", "platform": "telegram",
                "source_url": "https://t.me/ordinary", "source_title": "Обычный паблик",
                "source_queue_status": "pending_scan", "queue_order": 11,
            },
        }}
        keyword_rows = [
            {
                "canonical_source_key": "telegram:lovekenig", "recommended_canonical_url": "https://t.me/lovekenig",
                "recommended_username": "lovekenig", "recommended_title": "Полюбить Калининград",
                "discovery_type": "telegram_hashtag_search", "edge_type": "telegram_hashtag_search",
                "matched_hashtag": "#Калининград", "keyword_hit_text_excerpt": "#Калининград красивые места",
            },
            {
                "canonical_source_key": "telegram:spammy", "recommended_canonical_url": "https://t.me/spammy",
                "recommended_username": "spammy", "recommended_title": "ТЫ НЕ СМОЖЕШЬ УЙТИ 😈",
                "discovery_type": "telegram_hashtag_search", "edge_type": "telegram_hashtag_search",
                "matched_hashtag": "#Калининград", "keyword_hit_text_excerpt": "#Калининград #bonus #blackjack",
            },
            {
                "canonical_source_key": "telegram:travelhit", "recommended_canonical_url": "https://t.me/travelhit",
                "recommended_username": "travelhit", "recommended_title": "Маршруты по России",
                "discovery_type": "telegram_hashtag_search", "edge_type": "telegram_hashtag_search",
                "matched_hashtag": "#Калининград", "keyword_hit_text_excerpt": "Ездили в Калининград и на Куршскую косу, делимся маршрутом",
            },
        ]
        rows, metrics = mod.build_unified_source_queue(previous, [], [], [], [], keyword_rows, [], {}, "run-hash", "2026-07-09T00:00:00+00:00")
        by_key = {r["canonical_source_key"]: r for r in rows}
        self.assertEqual(by_key["telegram:travelhit"]["queue_order"], 11)
        self.assertEqual(by_key["telegram:lovekenig"]["source_queue_status"], mod.LOCAL_REGION_SOURCE_STATUS)
        self.assertEqual(by_key["telegram:spammy"]["source_queue_status"], mod.SPAM_SOURCE_STATUS)
        self.assertGreater(by_key["telegram:lovekenig"]["queue_order"], by_key["telegram:travelhit"]["queue_order"])
        self.assertGreater(by_key["telegram:spammy"]["queue_order"], by_key["telegram:travelhit"]["queue_order"])
        self.assertEqual(metrics["source_queue_keyword_surface_filtered_this_run"], 2)
        self.assertEqual(metrics["source_queue_rejected_local_region_source_total"], 1)
        self.assertEqual(metrics["source_queue_rejected_spam_source_total"], 1)

    def test_source_spam_surface_filter_does_not_treat_vystavka_as_betting(self) -> None:
        mod = load_module()
        decision = mod.source_discovery_surface_filter({
            "source_title": "Маршруты по России",
            "handle": "@travel_notes",
            "keyword_hit_text_excerpt": "Выставка и прогулка по Калининграду, маршрут и личные впечатления",
        })
        self.assertNotEqual(decision["source_quick_class"], "spam_source_reject")
        self.assertNotIn("ставк", decision.get("source_spam_hits", ""))

    def test_source_selection_prioritizes_pending_travel_sources(self) -> None:
        mod = load_module()
        travel = mod.Seed(
            source_seed_id="pending_travel", platform="telegram", source_title="Авторские путешествия по России",
            handle="@author_travel", url="https://t.me/author_travel", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="pending", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        general = mod.Seed(
            source_seed_id="pending_general", platform="telegram", source_title="Корпорация МСП",
            handle="@corp_msp", url="https://t.me/corp_msp", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="pending", expected_value="", known_risks="", initial_status="pending_scan",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        previous_state = {
            "unified_source_queue": {
                "telegram:corp_msp": {
                    "canonical_source_key": "telegram:corp_msp",
                    "source_url": "https://t.me/corp_msp",
                    "source_title": "Корпорация МСП",
                    "source_queue_status": "pending_scan",
                    "queue_order": 1,
                },
                "telegram:author_travel": {
                    "canonical_source_key": "telegram:author_travel",
                    "source_url": "https://t.me/author_travel",
                    "source_title": "Авторские путешествия по России",
                    "source_queue_status": "pending_scan",
                    "queue_order": 999,
                    "added_from": "public_travel_blogger_catalog",
                },
            }
        }
        selected = mod.selected_sources_for_run([general, travel], 2, previous_state=previous_state)
        self.assertEqual([s.canonical_url for s in selected], ["https://t.me/author_travel", "https://t.me/corp_msp"])
        self.assertTrue(mod._REGION_TALK_TELEGRAM_RUNTIME["source_selection_travel_priority_enabled"])

    def test_publication_goal_can_rescan_known_ko_sources_before_pending(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES")
        os.environ["REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES"] = "1"
        try:
            known = mod.Seed(
                source_seed_id="known_ko", platform="telegram", source_title="Уютная Россия | Путешествия",
                handle="@travel_yutturizm", url="https://t.me/travel_yutturizm", source_kind="unified_source_queue",
                source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
                why_seeded="processed", expected_value="", known_risks="", initial_status="processed_found_ko_candidate",
                monitoring_enabled=True, rights_policy="unknown", notes="",
            )
            pending = mod.Seed(
                source_seed_id="pending_new", platform="telegram", source_title="Pending source",
                handle="@pending_new", url="https://t.me/pending_new", source_kind="unified_source_queue",
                source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
                why_seeded="pending", expected_value="", known_risks="", initial_status="pending_scan",
                monitoring_enabled=True, rights_policy="unknown", notes="",
            )
            previous_state = {"unified_source_queue": {
                "telegram:travel_yutturizm": {"canonical_source_key": "telegram:travel_yutturizm", "source_url": "https://t.me/travel_yutturizm", "source_queue_status": "processed_found_ko_candidate", "queue_order": 1, "ko_posts_found": 4, "candidate_posts_found": 4},
                "telegram:pending_new": {"canonical_source_key": "telegram:pending_new", "source_url": "https://t.me/pending_new", "source_queue_status": "pending_scan", "queue_order": 2},
            }}
            selected = mod.selected_sources_for_run([pending, known], 2, previous_state=previous_state)
            self.assertEqual([s.canonical_url for s in selected], ["https://t.me/travel_yutturizm", "https://t.me/pending_new"])
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES", None)
            else:
                os.environ["REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES"] = old

    def test_publication_goal_orders_known_ko_by_product_yield_before_seed_priority(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES")
        os.environ["REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES"] = "1"
        try:
            high_yield = mod.Seed(
                source_seed_id="known_high", platform="telegram", source_title="Путешествуем.РФ",
                handle="@puteshestvuem_rf", url="https://t.me/puteshestvuem_rf", source_kind="unified_source_queue",
                source_scope_guess="canonical_queue", priority=99, discovered_from="unit", discovered_from_url="",
                why_seeded="processed", expected_value="", known_risks="", initial_status="processed_found_ko_candidate",
                monitoring_enabled=True, rights_policy="unknown", notes="",
            )
            low_yield = mod.Seed(
                source_seed_id="known_low", platform="telegram", source_title="Яндекс Путешествия",
                handle="@yandex_travel", url="https://t.me/yandex_travel", source_kind="unified_source_queue",
                source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
                why_seeded="processed", expected_value="", known_risks="", initial_status="processed_found_ko_candidate",
                monitoring_enabled=True, rights_policy="unknown", notes="",
            )
            previous_state = {"unified_source_queue": {
                "telegram:yandex_travel": {"canonical_source_key": "telegram:yandex_travel", "source_url": "https://t.me/yandex_travel", "source_title": "Яндекс Путешествия", "source_queue_status": "processed_found_ko_candidate", "queue_order": 1, "ko_posts_found": 1, "candidate_posts_found": 1},
                "telegram:puteshestvuem_rf": {"canonical_source_key": "telegram:puteshestvuem_rf", "source_url": "https://t.me/puteshestvuem_rf", "source_title": "Путешествуем.РФ", "source_queue_status": "processed_found_ko_candidate", "queue_order": 2, "ko_posts_found": 3, "candidate_posts_found": 3, "added_from": "public_travel_blogger_catalog"},
            }}
            selected = mod.selected_sources_for_run([low_yield, high_yield], 2, previous_state=previous_state)
            self.assertEqual([s.canonical_url for s in selected], ["https://t.me/puteshestvuem_rf", "https://t.me/yandex_travel"])
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES", None)
            else:
                os.environ["REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES"] = old

    def test_source_selection_default_processed_rescan_interval_is_two_weeks(self) -> None:
        from datetime import datetime, timedelta, timezone
        mod = load_module()
        seed = mod.Seed(
            source_seed_id="processed_recent", platform="telegram", source_title="Недавно обработанный",
            handle="@recent_done", url="https://t.me/recent_done", source_kind="unified_source_queue",
            source_scope_guess="canonical_queue", priority=0, discovered_from="unit", discovered_from_url="",
            why_seeded="processed", expected_value="", known_risks="", initial_status="processed_found_ko",
            monitoring_enabled=True, rights_policy="unknown", notes="",
        )
        recent = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        previous_state = {"unified_source_queue": {"telegram:recent_done": {"canonical_source_key": "telegram:recent_done", "source_url": "https://t.me/recent_done", "source_queue_status": "processed_found_ko", "_ydb_updated_at": recent, "queue_order": 1}}}
        selected = mod.selected_sources_for_run([seed], 1, previous_state=previous_state)
        self.assertEqual(selected, [])

    def test_vector_gate_rejects_ivan_kupala_multi_region_event_roundup(self) -> None:
        mod = load_module()
        lexicon = mod.load_place_lexicon(ROOT / "docs" / "features" / "region-talk-channel" / "kaliningrad-place-lexicon-v1.csv")
        text = (
            "Иван Купала — старинный народный праздник. "
            "А еще мы собрали места, где можно отпраздновать Ивана Купалу в эти выходные: "
            "Мира Парк — 4–5 июля, Новосибирская область. "
            "Поселение викингов «Кауп» — 4 июля, Калининградская область. "
            "Сплетете венок, поучаствуете в старинных играх и попробуете блюда средневековой кухни."
        )
        scope = mod.kaliningrad_oblast_only_scope_gate(text, lexicon)
        self.assertFalse(scope["kaliningrad_oblast_only_scope"])
        self.assertIn("новосибирская область", scope["external_geo_mentions"])
        gate = mod.text_vector_gate(
            text,
            mod.score_text(text),
            scope,
            mod.ad_promo_gate(text),
            mod.score_substance(text),
            embedding_scores={
                "vector_positive_semantic_score": 0.71,
                "vector_negative_score": 0.10,
                "vector_negative_class": "low_substance",
                "vector_positive_semantic_class": "ko_route_useful",
                "text_embedding_runtime": "unit_dual_embedding_stub",
                "text_embedding_model_id": "unit/e5+unit/bge",
            },
        )
        self.assertEqual(gate["vector_gate_status"], "vector_reject_multi_region_roundup")


    def test_vector_gate_can_disable_per_row_embedding_fallback(self) -> None:
        mod = load_module()
        lexicon = mod.load_place_lexicon(ROOT / "docs" / "features" / "region-talk-channel" / "kaliningrad-place-lexicon-v1.csv")
        text = "Калининград и Светлогорск: прогулка, море, личные впечатления и полезный маршрут."
        old_dual = mod.dual_model_semantic_scores
        def boom(_text: str):
            raise AssertionError("per-row embedding fallback must not be called")
        try:
            mod.dual_model_semantic_scores = boom
            gate = mod.text_vector_gate(
                text,
                mod.score_text(text),
                mod.kaliningrad_oblast_only_scope_gate(text, lexicon),
                mod.ad_promo_gate(text),
                mod.score_substance(text),
                allow_embedding_fallback=False,
            )
        finally:
            mod.dual_model_semantic_scores = old_dual
        self.assertIn(gate["vector_gate_status"], {"vector_accept_candidate", "vector_ambiguous_keep_for_ranking"})
        self.assertEqual(gate["text_embedding_runtime"], "kaggle_local_prototype_vector_gate")

    def test_external_bge_m3_row_fuses_with_local_e5_without_loading_bge(self) -> None:
        mod = load_module()
        e5_scores = {
            "text_embedding_model_id": mod.E5_TEXT_MODEL_ID,
            "text_embedding_scores_by_model": {
                mod.E5_TEXT_MODEL_ID: {
                    "ko_visit_impression": 0.78,
                    "ko_route_useful": 0.55,
                    "ko_visual_place_card": 0.50,
                    "other_region_travel": 0.11,
                    "multi_region_roundup": 0.10,
                    "news_report": 0.10,
                    "event_announcement": 0.10,
                    "ad_or_promo": 0.10,
                    "low_substance": 0.10,
                }
            },
        }
        bge_row = {
            "text_vector_enrichment_id": "p1:bge_m3:hash",
            "model_id": mod.BGE_M3_TEXT_MODEL_ID,
            "encoder_contract": mod.BGE_M3_ENCODER_CONTRACT,
            "text_hash": "sha",
            "semantic_scores_by_class": {
                "ko_visit_impression": 0.82,
                "ko_route_useful": 0.57,
                "ko_visual_place_card": 0.61,
                "other_region_travel": 0.12,
                "multi_region_roundup": 0.11,
                "news_report": 0.10,
                "event_announcement": 0.10,
                "ad_or_promo": 0.09,
                "low_substance": 0.08,
            },
        }
        fused = mod.fuse_e5_with_external_bge_m3(e5_scores, bge_row, text_hash="sha", match_mode="post_url_text_hash")
        self.assertEqual(fused["text_vector_fusion_status"], "fused_e5_bge_m3")
        self.assertEqual(fused["text_embedding_runtime"], "kaggle_local_e5_plus_external_bge_m3_ydb")
        self.assertEqual(fused["bge_m3_text_hash_match"], "true")
        self.assertGreater(float(fused["vector_positive_semantic_score"]), float(fused["vector_negative_score"]))

    def test_image_queue_requires_fused_e5_bge_when_enabled(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE")
        os.environ["REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE"] = "1"
        try:
            base = {
                "post_id": "p1",
                "post_url": "https://t.me/src/1",
                "has_media": True,
                "is_ad_or_promo": False,
                "kaliningrad_oblast_only_scope": True,
                "kaliningrad_mention_role": "main_subject",
                "current_stage": "semantic_candidate",
                "vector_gate_status": "vector_accept_candidate",
            }
            missing = dict(base)
            fused = {**base, "post_id": "p2", "post_url": "https://t.me/src/2", "text_vector_fusion_status": "fused_e5_bge_m3"}
            queue, _top, metrics = mod.build_image_candidate_queue({}, [missing, fused], [], [], "run-img", "2026-07-07T00:00:00+00:00")
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE", None)
            else:
                os.environ["REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE"] = old
        self.assertEqual([r["post_url"] for r in queue], ["https://t.me/src/2"])
        self.assertEqual(metrics["image_queue_rejected_non_region_inputs"], 1)

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

    def test_llm_call_timeout_returns_structured_error(self) -> None:
        mod = load_module()
        old = {k: os.environ.get(k) for k in ["REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "GOOGLE_AI_PROVIDER_TIMEOUT_SEC", "REGION_TALK_AUTO_INSTALL"]}
        class FakeClient:
            provider_timeout_seconds = 0.0
            async def generate_content_async(self, **kwargs):
                await __import__("asyncio").sleep(1)
                return '{"decision":"accept","reason":"late"}', object()
        client = FakeClient()
        mod.get_region_talk_llm_gateway = lambda default_env_var_name: client
        try:
            os.environ["REGION_TALK_LLM_CALL_TIMEOUT_SECONDS"] = "0.05"
            os.environ["REGION_TALK_AUTO_INSTALL"] = "0"
            os.environ.pop("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", None)
            started = __import__("time").monotonic()
            result = mod.call_region_talk_semantic_llm({"text":"Калининград"}, {}, model="fake", default_env_var_name="GOOGLE_API_KEY3")
            elapsed = __import__("time").monotonic() - started
            self.assertLess(elapsed, 3.0)
            self.assertEqual(result["llm_gate_status"], "error")
            self.assertIn("TimeoutError", result["llm_reason"])
            self.assertEqual(os.environ.get("GOOGLE_AI_PROVIDER_TIMEOUT_SEC"), "0.05")
            self.assertEqual(client.provider_timeout_seconds, 0.05)
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_llm_prompt_is_compact_and_uses_text_fallbacks(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS")
        try:
            os.environ["REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS"] = "900"
            prompt = mod.llm_text_gate_prompt(
                {
                    "source_title": "Уютная Россия",
                    "post_url": "https://t.me/example/1",
                    "post_date": "2026-07-07",
                    "text": "",
                    "text_excerpt": "Калининград и Зеленоградск: " + ("очень красивый маршрут " * 200),
                    "short_summary": "fallback summary",
                    "debug_blob": "x" * 20000,
                },
                {
                    "stage": "publication_queue_final_verifier",
                    "overall_media_score": 0.88,
                    "postcardness_score": 0.77,
                    "large_unused_debug": "x" * 20000,
                },
            )
            data = json.loads(prompt)
            self.assertLess(len(prompt.encode("utf-8")), 3500)
            self.assertIn("text_excerpt:", data["post"]["text"])
            self.assertLessEqual(len(data["post"]["text"]), 900)
            self.assertNotIn("large_unused_debug", prompt)
            self.assertNotIn("debug_blob", prompt)
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS", None)
            else:
                os.environ["REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS"] = old

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
            os.environ["REGION_TALK_YDB_DATABASE"] = "/ru-central1/cloud/db-from-env"
            cfg2 = mod.ydb_config_status()
            self.assertEqual(cfg2["endpoint"], "grpcs://ydb.serverless.yandexcloud.net:2135")
            self.assertEqual(cfg2["database"], "/ru-central1/cloud/db-from-env")
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
                "post_link_queue": {"pl1": {"post_link_queue_id": "pl1", "post_url": "https://t.me/x/2", "keyword_hit_text_excerpt": "short", "raw_text": "NO"}},
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
        self.assertEqual(compact["state_schema_version"], "region-talk-ydb-compact-v3")
        self.assertEqual(compact["queue_contract_version"], "ydb_row_level_unified_source_queue_v2_and_image_candidate_queue_v2")
        self.assertIn("queue_cursors", compact)
        self.assertIn("unified_source_queue", compact)
        self.assertIn("image_candidate_queue", compact)
        self.assertIn("post_link_queue", compact)
        self.assertIn("pl1", compact["post_link_queue"])
        self.assertNotIn("raw_text", json.dumps(compact["post_link_queue"], ensure_ascii=False))
        self.assertNotIn("source_frontier_queue_next", compact)
        self.assertNotIn("similar_seed_queue", compact)
        self.assertIn("https://t.me/x/1", blob)
        self.assertNotIn("RAW TEXT MUST NOT BE STORED", blob)
        self.assertNotIn("RAW SOURCE DESC", blob)
        self.assertNotIn("raw_payload_json", blob)
        self.assertNotIn("frontier_status", blob)
        self.assertIn("queue_cursors", compact)

    def test_ydb_source_status_rows_overlay_source_queue_state(self) -> None:
        mod = load_module()
        state = {
            "unified_source_queue": {
                "telegram:old": {
                    "canonical_source_key": "telegram:old",
                    "source_queue_status": "pending_scan",
                    "source_title": "Old",
                }
            }
        }
        merged = mod.merge_ydb_source_queue_status_items(
            state,
            {
                "source_queue_item:telegram:old": {
                    "canonical_source_key": "telegram:old",
                    "source_queue_status": "processed_ok",
                    "ko_posts_found": 1,
                }
            },
            {
                "source_status_item:telegram:new": {
                    "canonical_source_key": "telegram:new",
                    "source_queue_status": "selected_or_observed",
                    "fetch_status": "selected_for_run",
                    "source_url": "https://t.me/new",
                }
            },
            {
                "online_source_item:telegram:live": {
                    "canonical_source_key": "telegram:live",
                    "queue_status": "skipped_or_rejected",
                    "fetch_status": "skipped_fetch_disabled",
                    "source_url": "https://t.me/live",
                }
            },
        )
        self.assertEqual(merged, 3)
        queue = state["unified_source_queue"]
        self.assertEqual(queue["telegram:old"]["source_queue_status"], "processed_ok")
        self.assertEqual(queue["telegram:old"]["ko_posts_found"], 1)
        self.assertEqual(queue["telegram:new"]["fetch_status"], "selected_for_run")
        self.assertEqual(queue["telegram:live"]["fetch_status"], "skipped_fetch_disabled")

    def test_ydb_source_status_overlay_does_not_clobber_queue_fields_with_empty_live_rows(self) -> None:
        mod = load_module()
        state = {
            "unified_source_queue": {
                "telegram:keyword": {
                    "canonical_source_key": "telegram:keyword",
                    "source_queue_status": "pending_scan",
                    "queue_order": 77,
                    "posts_scanned": 3,
                    "source_url": "https://t.me/keyword",
                }
            }
        }
        mod.merge_ydb_source_queue_status_items(
            state,
            {},
            {
                "source_status_item:telegram:keyword": {
                    "canonical_source_key": "telegram:keyword",
                    "source_queue_status": "",
                    "queue_order": "",
                    "posts_scanned": 0,
                    "fetch_status": "selected_for_run",
                }
            },
            {
                "online_source_item:telegram:keyword": {
                    "canonical_source_key": "telegram:keyword",
                    "source_queue_status": "",
                    "queue_order": "",
                    "posts_scanned": 0,
                    "online_update_stage": "source_selected_for_run",
                }
            },
        )
        row = state["unified_source_queue"]["telegram:keyword"]
        self.assertEqual(row["source_queue_status"], "pending_scan")
        self.assertEqual(row["queue_order"], 77)
        self.assertEqual(row["posts_scanned"], 3)
        self.assertEqual(row["fetch_status"], "selected_for_run")

    def test_online_source_selected_for_run_does_not_write_durable_source_queue(self) -> None:
        mod = load_module()
        payload = mod._online_source_payload(
            {"platform": "telegram", "handle": "keyword", "canonical_url": "https://t.me/keyword"},
            run_id="run-q",
            stage="source_selected_for_run",
            status="selected_for_run",
        )
        self.assertFalse(mod._online_source_should_write_source_queue_item(payload, "source_selected_for_run"))
        self.assertTrue(mod._online_source_should_write_source_queue_item({**payload, "queue_order": 77}, "source_selected_for_run"))

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
                {"platform": "telegram", "canonical_url": "https://t.me/+InviteHash", "canonical_source_key": "telegram:+invitehash", "source_candidate_score": 0.8},
                {"platform": "telegram", "canonical_url": "https://t.me/c/123456", "canonical_source_key": "telegram:c/123456", "source_candidate_score": 0.8},
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
        self.assertFalse(any("t.me/+" in u for u in urls))
        self.assertFalse(any("t.me/c/" in u for u in urls))
        orders = {r["canonical_source_key"]: r["queue_order"] for r in rows}
        self.assertEqual(orders["telegram:keynew"], 2)
        self.assertEqual(orders["telegram:keypost"], 3)
        self.assertGreater(orders["telegram:tail"], orders["telegram:keypost"])
        self.assertGreater(orders["telegram:similar_tail"], orders["telegram:tail"])
        self.assertEqual(len([r for r in rows if r["canonical_source_key"] == "telegram:seedtg"]), 1)
        self.assertGreaterEqual(metrics["source_queue_non_target_skipped_this_run"], 5)

    def test_unified_source_queue_reprioritizes_existing_keyword_hit_after_cursor(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 10,
            "unified_source_queue": {
                "telegram:hittravel": {
                    "canonical_source_key": "telegram:hittravel",
                    "platform": "telegram",
                    "source_url": "https://t.me/hittravel",
                    "queue_order": 50,
                    "source_queue_status": "pending_scan",
                    "added_from": "frontier",
                },
                "telegram:tail": {
                    "canonical_source_key": "telegram:tail",
                    "platform": "telegram",
                    "source_url": "https://t.me/tail",
                    "queue_order": 51,
                    "source_queue_status": "pending_scan",
                },
            },
        }
        rows, metrics = mod.build_unified_source_queue(
            previous, [], [], [], [],
            [],
            [{"keyword_hit_source_url": "https://t.me/hittravel", "platform": "telegram", "canonical_source_key": "telegram:hittravel"}],
            {},
            "run-q",
            "2026-07-07T00:00:00+00:00",
        )
        by_key = {r["canonical_source_key"]: r for r in rows}
        self.assertEqual(by_key["telegram:hittravel"]["queue_order"], 11)
        self.assertEqual(by_key["telegram:hittravel"]["insertion_policy"], "keyword_reinsert_after_cursor")
        self.assertEqual(by_key["telegram:hittravel"]["keyword_discovery_status"], "keyword_evidence")
        self.assertGreater(by_key["telegram:tail"]["queue_order"], by_key["telegram:hittravel"]["queue_order"])
        self.assertEqual(metrics["source_queue_keyword_inserted_this_run"], 0)
        self.assertEqual(metrics["source_queue_keyword_existing_promoted_this_run"], 1)
        self.assertEqual(metrics["source_queue_keyword_prioritized_this_run"], 1)

    def test_source_queue_cursor_stops_before_pending_gap(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 1,
            "unified_source_queue": {
                "telegram:done_late": {
                    "canonical_source_key": "telegram:done_late",
                    "platform": "telegram",
                    "source_url": "https://t.me/done_late",
                    "queue_order": 50,
                    "source_queue_status": "processed_no_ko",
                    "posts_scanned": 3,
                    "last_history_fetch_at": "2026-07-01T00:00:00+00:00",
                },
                "telegram:pending_gap": {
                    "canonical_source_key": "telegram:pending_gap",
                    "platform": "telegram",
                    "source_url": "https://t.me/pending_gap",
                    "queue_order": 10,
                    "source_queue_status": "pending_scan",
                    "added_from": "telegram_keyword_search",
                },
            },
        }
        rows, metrics = mod.build_unified_source_queue(previous, [], [], [], [], [], [], {}, "run-q", "2026-07-07T00:00:00+00:00")
        by_key = {r["canonical_source_key"]: r for r in rows}
        self.assertEqual(metrics["source_queue_cursor_position"], 1)
        self.assertEqual(metrics["source_queue_pending_before_max_processed_gap_total"], 1)
        self.assertEqual(by_key["telegram:pending_gap"]["is_after_cursor"], "true")

    def test_unified_queue_dynamic_seeds_prioritizes_keyword_gap_before_cursor(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 100,
            "unified_source_queue": {
                "telegram:normal_after": {
                    "canonical_source_key": "telegram:normal_after",
                    "platform": "telegram",
                    "source_url": "https://t.me/normal_after",
                    "queue_order": 101,
                    "source_queue_status": "pending_scan",
                },
                "telegram:keyword_before": {
                    "canonical_source_key": "telegram:keyword_before",
                    "platform": "telegram",
                    "source_url": "https://t.me/keyword_before",
                    "queue_order": 20,
                    "source_queue_status": "processed_no_ko",
                    "posts_scanned": 0,
                    "added_from": "telegram_keyword_search",
                },
            },
        }
        selected = mod.unified_queue_dynamic_seeds(previous, 2)
        self.assertEqual([s.canonical_url for s in selected], ["https://t.me/keyword_before", "https://t.me/normal_after"])

    def test_processed_status_without_scan_evidence_returns_to_pending(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 10,
            "unified_source_queue": {
                "telegram:fakeprocessed": {
                    "canonical_source_key": "telegram:fakeprocessed",
                    "platform": "telegram",
                    "source_url": "https://t.me/fakeprocessed",
                    "queue_order": 5,
                    "source_queue_status": "processed_no_ko",
                    "last_scan_run_id": "legacy-imageq-run",
                    "posts_scanned": 0,
                },
            },
        }
        seed = mod.Seed("fakeprocessed", "telegram", "Fake Processed", "@fakeprocessed", "https://t.me/fakeprocessed", "", "", 1, "", "", "", "", "", "", True, "", "")
        due = mod._seed_scan_due_state(seed, previous)
        self.assertTrue(due["due"])
        self.assertFalse(due["is_rescan"])
        self.assertEqual(due["reason"], "no_previous_scan_cursor")
        rows, metrics = mod.build_unified_source_queue(previous, [], [], [], [], [], [], {}, "run-q", "2026-07-07T00:00:00+00:00")
        row = next(r for r in rows if r["canonical_source_key"] == "telegram:fakeprocessed")
        self.assertEqual(row["source_queue_status"], "pending_scan")
        self.assertEqual(row["fake_processed_without_scan_evidence"], "true")
        self.assertEqual(metrics["source_queue_fake_processed_without_scan_evidence_total"], 1)

    def test_fake_processed_keyword_hit_is_reinserted_after_cursor(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 10,
            "unified_source_queue": {
                "telegram:fakekeyword": {
                    "canonical_source_key": "telegram:fakekeyword",
                    "platform": "telegram",
                    "source_url": "https://t.me/fakekeyword",
                    "queue_order": 5,
                    "source_queue_status": "processed_no_ko",
                    "last_scan_run_id": "legacy-imageq-run",
                    "posts_scanned": 0,
                },
                "telegram:tail": {
                    "canonical_source_key": "telegram:tail",
                    "platform": "telegram",
                    "source_url": "https://t.me/tail",
                    "queue_order": 11,
                    "source_queue_status": "pending_scan",
                },
            },
        }
        rows, metrics = mod.build_unified_source_queue(
            previous, [], [], [], [],
            [],
            [{"keyword_hit_source_url": "https://t.me/fakekeyword", "platform": "telegram", "canonical_source_key": "telegram:fakekeyword"}],
            {},
            "run-q",
            "2026-07-07T00:00:00+00:00",
        )
        by_key = {r["canonical_source_key"]: r for r in rows}
        self.assertEqual(by_key["telegram:fakekeyword"]["queue_order"], 11)
        self.assertEqual(by_key["telegram:fakekeyword"]["source_queue_status"], "pending_scan")
        self.assertEqual(by_key["telegram:fakekeyword"]["insertion_policy"], "keyword_reinsert_after_cursor")
        self.assertEqual(by_key["telegram:fakekeyword"]["fake_processed_without_scan_evidence"], "true")
        self.assertGreater(by_key["telegram:tail"]["queue_order"], by_key["telegram:fakekeyword"]["queue_order"])
        self.assertEqual(metrics["source_queue_keyword_existing_promoted_this_run"], 1)
        self.assertEqual(metrics["source_queue_keyword_prioritized_this_run"], 1)

    def test_historical_keyword_edge_reinserts_fake_processed_after_cursor(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 10,
            "unified_source_queue": {
                "telegram:edgehit": {
                    "canonical_source_key": "telegram:edgehit",
                    "platform": "telegram",
                    "source_url": "https://t.me/edgehit",
                    "queue_order": 5,
                    "source_queue_status": "processed_no_ko",
                    "last_scan_run_id": "legacy-imageq-run",
                    "posts_scanned": 0,
                },
                "telegram:tail": {
                    "canonical_source_key": "telegram:tail",
                    "platform": "telegram",
                    "source_url": "https://t.me/tail",
                    "queue_order": 11,
                    "source_queue_status": "pending_scan",
                },
            },
            "source_candidates": {
                "src_edgehit": {
                    "source_candidate_id": "src_edgehit",
                    "canonical_source_key": "telegram:edgehit",
                    "handle": "edgehit",
                    "platform_guess": "telegram",
                }
            },
            "source_edges": {
                "edge_keyword": {
                    "edge_id": "edge_keyword",
                    "to_source_candidate_id": "src_edgehit",
                    "edge_type": "telegram_keyword_search",
                    "from_source_id": "keyword:калининград",
                }
            },
        }
        rows, metrics = mod.build_unified_source_queue(
            previous, [], [], [], [], [], [], {}, "run-q", "2026-07-07T00:00:00+00:00"
        )
        by_key = {r["canonical_source_key"]: r for r in rows}
        self.assertEqual(by_key["telegram:edgehit"]["queue_order"], 11)
        self.assertEqual(by_key["telegram:edgehit"]["source_queue_status"], "pending_scan")
        self.assertEqual(by_key["telegram:edgehit"]["insertion_policy"], "keyword_reinsert_after_cursor")
        self.assertEqual(by_key["telegram:edgehit"]["fake_processed_without_scan_evidence"], "true")
        self.assertGreater(by_key["telegram:tail"]["queue_order"], by_key["telegram:edgehit"]["queue_order"])
        self.assertEqual(metrics["source_queue_historical_keyword_sources_total"], 1)
        self.assertEqual(metrics["source_queue_keyword_existing_promoted_this_run"], 1)

    def test_keyword_context_row_without_url_uses_canonical_key_handle(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 7,
            "source_candidates": {
                "src_kenigevents": {
                    "source_candidate_id": "src_kenigevents",
                    "canonical_source_key": "telegram:kenigevents",
                    "handle": "kenigevents",
                    "online_update_stage": "keyword_hit_source_context",
                    "queue_status": "candidate",
                }
            },
        }
        rows, metrics = mod.build_unified_source_queue(
            previous, [], [], [], [], [], [], {}, "run-q", "2026-07-07T00:00:00+00:00"
        )
        by_key = {r["canonical_source_key"]: r for r in rows}
        self.assertIn("telegram:kenigevents", by_key)
        self.assertEqual(by_key["telegram:kenigevents"]["source_url"], "https://t.me/kenigevents")
        self.assertEqual(by_key["telegram:kenigevents"]["queue_order"], 1)
        self.assertEqual(by_key["telegram:kenigevents"]["source_queue_status"], mod.LOCAL_REGION_SOURCE_STATUS)
        self.assertEqual(metrics["source_queue_historical_keyword_sources_total"], 1)
        self.assertEqual(metrics["source_queue_keyword_inserted_this_run"], 0)
        self.assertEqual(metrics["source_queue_keyword_surface_filtered_this_run"], 1)

    def test_source_queue_handoff_rows_limits_full_rewrite_but_keeps_keyword_and_cursor_neighbourhood(self) -> None:
        mod = load_module()
        old = os.environ.get("REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS")
        try:
            os.environ["REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS"] = "5"
            rows = [
                {"canonical_source_key": f"telegram:s{i}", "queue_order": i, "source_queue_status": "processed_no_ko"}
                for i in range(1, 30)
            ]
            rows.append({"canonical_source_key": "telegram:keyword", "queue_order": 2, "added_from": "telegram_keyword_search", "source_queue_status": "pending_scan"})
            rows.append({"canonical_source_key": "telegram:changed", "queue_order": 28, "status_changed_this_run": "true"})
            selected = mod._source_queue_handoff_rows(rows, 10, "run-q")
            keys = {r["canonical_source_key"] for r in selected}
            self.assertLessEqual(len(selected), 5)
            self.assertIn("telegram:keyword", keys)
            self.assertIn("telegram:changed", keys)
            self.assertTrue(any(8 <= int(r.get("queue_order") or 0) <= 12 for r in selected))
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS", None)
            else:
                os.environ["REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS"] = old

    def test_image_queue_preserves_terminal_unsupported_media_status(self) -> None:
        mod = load_module()
        previous = {
            "image_candidate_queue": {
                "https://t.me/travel/1": {
                    "image_queue_id": "imgq_video",
                    "image_queue_order": 1,
                    "post_url": "https://t.me/travel/1",
                    "source_url": "https://t.me/travel",
                    "image_queue_status": "not_reviewable_unsupported_media",
                    "image_model_input_type": "unsupported_media",
                    "kaliningrad_oblast_only_scope": True,
                    "kaliningrad_mention_role": "main_subject",
                    "current_stage": "semantic_candidate",
                    "vector_gate_status": "vector_accept_candidate",
                    "text_vector_fusion_status": "fused_e5_bge_m3",
                }
            }
        }
        rows, _, _ = mod.build_image_candidate_queue(
            previous,
            [{
                "post_url": "https://t.me/travel/1",
                "source_url": "https://t.me/travel",
                "has_media": True,
                "kaliningrad_oblast_only_scope": True,
                "kaliningrad_mention_role": "main_subject",
                "current_stage": "semantic_candidate",
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "image_model_input_type": "metadata_only",
            }],
            [], [], "run-q", "2026-07-07T00:00:00+00:00",
        )
        row = next(r for r in rows if r["post_url"] == "https://t.me/travel/1")
        self.assertEqual(row["image_queue_status"], "not_reviewable_unsupported_media")
        self.assertEqual(row["next_action"], "skip_unsupported_media")

    def test_source_queue_uses_ydb_image_queue_scores_for_source_rollup(self) -> None:
        mod = load_module()
        previous = {
            "unified_source_queue_cursor_position": 1,
            "unified_source_queue": {
                "telegram:visual": {
                    "canonical_source_key": "telegram:visual", "platform": "telegram",
                    "source_url": "https://t.me/visual", "canonical_url": "https://t.me/visual",
                    "queue_order": 1, "source_queue_status": "processed_found_ko_candidate",
                    "ko_posts_found": 1, "candidate_posts_found": 1,
                },
            },
            "image_candidate_queue": {
                f"img{i}": {
                    "image_queue_id": f"img{i}", "image_queue_order": i,
                    "post_url": f"https://t.me/visual/{i}", "source_url": "https://t.me/visual",
                    "kaliningrad_oblast_only_scope": True, "kaliningrad_mention_role": "main_subject",
                    "current_stage": "semantic_candidate", "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image", "overall_media_score": score,
                }
                for i, score in enumerate([0.2, 0.3, 0.4], start=1)
            },
        }
        rows, metrics = mod.build_unified_source_queue(previous, [], [], [], [], [], [], {}, "run-q", "2026-07-07T00:00:00+00:00")
        row = next(r for r in rows if r["canonical_source_key"] == "telegram:visual")
        self.assertEqual(row["actual_images_scored_count"], 3)
        self.assertEqual(row["source_image_quality_status"], "exclude_low_image_quality")
        self.assertEqual(row["source_queue_status"], "processed_found_ko_low_image_quality")
        self.assertEqual(row["status_changed_this_run"], "true")
        self.assertEqual(metrics["source_queue_low_image_quality_excluded_total"], 1)

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

    def test_status_event_writes_business_heartbeat_hook(self) -> None:
        mod = load_module()
        calls = []
        mod.write_region_talk_business_heartbeat = lambda payload: calls.append(dict(payload))
        old_run = os.environ.get("REGION_TALK_RUN_ID")
        os.environ["REGION_TALK_RUN_ID"] = "heartbeat-unit"
        try:
            status = mod.Status()
            status.event("alive", phase="fetch", progress_label="источники 1/2 · Source", current_source_title="Source")
        finally:
            if old_run is None:
                os.environ.pop("REGION_TALK_RUN_ID", None)
            else:
                os.environ["REGION_TALK_RUN_ID"] = old_run
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["event_name"], "alive")
        self.assertEqual(calls[0]["run_id"], "heartbeat-unit")
        self.assertEqual(calls[0]["phase"], "fetch")
        self.assertIn("event_seq", calls[0])

    def test_parse_public_telegram_post_url_for_ydb_candidate_links(self) -> None:
        mod = load_module()
        self.assertEqual(mod.parse_public_telegram_post_url("https://t.me/puteshestvuem_rf/14373"), ("puteshestvuem_rf", 14373))
        self.assertEqual(mod.parse_public_telegram_post_url("http://t.me/travel_yutturizm/36480?single"), ("travel_yutturizm", 36480))
        self.assertIsNone(mod.parse_public_telegram_post_url("https://t.me/c/123/456"))
        self.assertIsNone(mod.parse_public_telegram_post_url("https://vk.com/wall-1_2"))

    def test_dual_embedding_batch_releases_each_model(self) -> None:
        mod = load_module()

        class FakeMatrix:
            def __init__(self, rows):
                self.rows = rows
            @property
            def T(self):
                return FakeMatrix([list(col) for col in zip(*self.rows)])
            def __matmul__(self, other):
                return FakeMatrix([[sum(a * b for a, b in zip(row, col)) for col in other.rows] for row in self.rows])
            def tolist(self):
                return self.rows

        old_models = mod.TEXT_EMBEDDING_MODELS
        old_embed = mod.embed_texts_for_model
        old_release = mod.release_text_embedding_model
        old_mode = os.environ.get("REGION_TALK_TEXT_VECTOR_MODE")
        old_require = os.environ.get("REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS")
        old_ydb_cache = os.environ.get("REGION_TALK_YDB_SEMANTIC_BANK_CACHE")
        old_subprocess = os.environ.get("REGION_TALK_TEXT_EMBEDDING_SUBPROCESS")
        calls = []
        def fake_embed(model_id, texts, *, query=False):
            calls.append(("embed", model_id, query, len(texts)))
            base = 1.0 if model_id == "model_a" else 0.5
            return FakeMatrix([[base, 1.0 - base] for _ in texts])
        def fake_release(model_id):
            calls.append(("release", model_id))
        try:
            mod.TEXT_EMBEDDING_MODELS = ["model_a", "model_b"]
            mod.embed_texts_for_model = fake_embed
            mod.release_text_embedding_model = fake_release
            os.environ["REGION_TALK_TEXT_VECTOR_MODE"] = "dual"
            os.environ["REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS"] = "1"
            os.environ["REGION_TALK_YDB_SEMANTIC_BANK_CACHE"] = "0"
            os.environ["REGION_TALK_TEXT_EMBEDDING_SUBPROCESS"] = "0"
            rows = mod.dual_model_semantic_scores_batch(["текст 1", "текст 2"])
        finally:
            mod.TEXT_EMBEDDING_MODELS = old_models
            mod.embed_texts_for_model = old_embed
            mod.release_text_embedding_model = old_release
            if old_mode is None: os.environ.pop("REGION_TALK_TEXT_VECTOR_MODE", None)
            else: os.environ["REGION_TALK_TEXT_VECTOR_MODE"] = old_mode
            if old_require is None: os.environ.pop("REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS", None)
            else: os.environ["REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS"] = old_require
            if old_ydb_cache is None: os.environ.pop("REGION_TALK_YDB_SEMANTIC_BANK_CACHE", None)
            else: os.environ["REGION_TALK_YDB_SEMANTIC_BANK_CACHE"] = old_ydb_cache
            if old_subprocess is None: os.environ.pop("REGION_TALK_TEXT_EMBEDDING_SUBPROCESS", None)
            else: os.environ["REGION_TALK_TEXT_EMBEDDING_SUBPROCESS"] = old_subprocess
        self.assertEqual(len(rows), 2)
        self.assertIn(("release", "model_a"), calls)
        self.assertIn(("release", "model_b"), calls)
        model_b_first_embed = next(i for i, call in enumerate(calls) if call[0] == "embed" and call[1] == "model_b" and call[2] is False)
        self.assertLess(calls.index(("release", "model_a")), model_b_first_embed)
        self.assertEqual(rows[0]["text_embedding_runtime"], "kaggle_local_dual_text_embeddings_sequential")

    def test_compact_business_payload_keeps_vector_counters(self) -> None:
        mod = load_module()
        compact = mod._compact_business_payload({
            "run_id": "heartbeat-unit",
            "event_name": "vector_scoring_alive",
            "phase": "vector_scoring",
            "status": "running",
            "progress_label": "posts 25/180",
            "posts_fetched": 653,
            "posts_to_score": 180,
            "posts_scored": 25,
            "posts_deferred": 473,
            "raw_post_text": "must not be persisted",
        })
        self.assertEqual(compact["posts_to_score"], 180)
        self.assertEqual(compact["posts_scored"], 25)
        self.assertEqual(compact["posts_deferred"], 473)
        self.assertNotIn("raw_post_text", compact)

    def test_ydb_retention_defaults_bound_snapshots_and_semantic_cache(self) -> None:
        mod = load_module()
        old_env = {k: os.environ.get(k) for k in [
            "REGION_TALK_YDB_RETENTION_PRUNE",
            "REGION_TALK_YDB_RUN_SNAPSHOT_KEEP_LAST",
            "REGION_TALK_YDB_SEMANTIC_BANK_KEEP_LAST",
        ]}
        calls: list[tuple[str, int, set[str]]] = []
        original = mod.ydb_prune_kind_keep_latest

        def fake_prune(session, ydb, table_path, kind, keep_last, *, protected_pks=None):
            calls.append((kind, int(keep_last), set(protected_pks or set())))
            return 0

        try:
            for key in old_env:
                os.environ.pop(key, None)
            mod.ydb_prune_kind_keep_latest = fake_prune
            mod.ydb_prune_compact_retention(None, None, "/db/region_talk_state_kv")
        finally:
            mod.ydb_prune_kind_keep_latest = original
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        plan = {kind: keep for kind, keep, _ in calls}
        self.assertEqual(plan["run_state_snapshot"], 1)
        self.assertEqual(plan["semantic_bank_embedding"], 4)
        protected = {kind: pks for kind, _, pks in calls}
        self.assertIn("queue_cursor:source_scan", protected["queue_cursor"])
        self.assertIn("queue_cursor:image_diagnostic", protected["queue_cursor"])

    def test_candidate_report_delegates_image_scoring_to_ydb_worker_by_default(self) -> None:
        mod = load_module()
        old_mode = os.environ.get("REGION_TALK_IMAGE_SCORING_MODE")
        old_allow = os.environ.get("REGION_TALK_CANDIDATE_REPORT_ALLOW_IMAGE_MODEL_SCORING")
        try:
            os.environ.pop("REGION_TALK_IMAGE_SCORING_MODE", None)
            os.environ.pop("REGION_TALK_CANDIDATE_REPORT_ALLOW_IMAGE_MODEL_SCORING", None)
            ms = mod.media_scores(True, {"anchor_hits": ["Калининград"], "positive_hits": ["море"]}, {"primary_media_path": "/tmp/nonexistent.jpg"})
        finally:
            if old_mode is None: os.environ.pop("REGION_TALK_IMAGE_SCORING_MODE", None)
            else: os.environ["REGION_TALK_IMAGE_SCORING_MODE"] = old_mode
            if old_allow is None: os.environ.pop("REGION_TALK_CANDIDATE_REPORT_ALLOW_IMAGE_MODEL_SCORING", None)
            else: os.environ["REGION_TALK_CANDIDATE_REPORT_ALLOW_IMAGE_MODEL_SCORING"] = old_allow
        self.assertEqual(ms["image_model_type"], "external_ydb_queue")
        self.assertEqual(ms["image_model_runtime"], "not_run_in_candidate_report")
        self.assertEqual(ms["failure_reason"], "needs_actual_image_fetch")
        self.assertEqual(mod.current_image_scoring_mode(), "external_ydb_queue")

    def test_candidate_report_uses_actual_image_scores_from_ydb_queue(self) -> None:
        mod = load_module()
        ms = mod._image_scores_from_ydb_queue({
            "image_queue_status": "actual_scored",
            "image_model_input_type": "actual_image",
            "overall_media_score": 0.81,
            "clip_postcardness_score": 0.83,
            "laion_aesthetic_score": 0.79,
            "cv_technical_quality_score": 0.76,
            "image_model_type": "multi_model_visual_consensus",
        })
        self.assertIsNotNone(ms)
        self.assertEqual(ms["image_model_runtime"], "external_region_talk_image_diagnostic")
        self.assertEqual(ms["image_model_input_type"], "actual_image")
        self.assertEqual(ms["image_publication_ready"], "true")

    def test_publication_queue_requires_nonlocal_actual_image_and_llm_accept(self) -> None:
        mod = load_module()
        rows, goal = mod.build_publication_candidate_queue(
            [
                {
                    "candidate_memory_id": "cm1", "post_id": "p1", "post_url": "https://t.me/travel/1",
                    "source_id": "s1", "source_title": "Travel", "source_geo_class": "nonlocal_russia",
                    "source_topic_class": "travel_blogger", "post_date": "2026-07-01T00:00:00+00:00",
                    "kaliningrad_oblast_only_scope": True, "kaliningrad_mention_role": "main_subject",
                    "is_ad_or_promo": False, "vector_gate_status": "vector_accept_candidate",
                    "vector_content_type": "visit_impression_candidate", "final_verifier_status": "ok",
                    "final_verifier_decision": "accept", "final_verifier_reason": "подходит",
                    "has_firsthand_visit_evidence": "true", "emotion_or_impression_evidence": "true",
                    "review_or_opinion_evidence": "true", "short_summary": "Калининград впечатлил морем",
                    "matched_place_names": "Калининград", "nonlocal_value_score": 0.8,
                },
                {
                    "candidate_memory_id": "cm2", "post_id": "p2", "post_url": "https://t.me/local/2",
                    "source_id": "s2", "source_title": "Local", "source_geo_class": "kaliningrad_local",
                    "source_topic_class": "travel_blogger", "kaliningrad_oblast_only_scope": True,
                    "vector_gate_status": "vector_accept_candidate", "final_verifier_status": "ok",
                    "final_verifier_decision": "accept",
                },
            ],
            [
                {"post_url": "https://t.me/travel/1", "image_queue_status": "actual_scored", "image_model_input_type": "actual_image", "overall_media_score": 0.86, "postcardness_score": 0.77, "aesthetic_score": 0.74},
                {"post_url": "https://t.me/local/2", "image_queue_status": "actual_scored", "image_model_input_type": "actual_image", "overall_media_score": 0.90, "postcardness_score": 0.88},
            ],
            [],
            {"publication_goal_id": "unit-goal", "target_confirmed": 20, "llm_budget_max": 100},
            run_id="pub-run",
            run_now="2026-07-07T00:00:00+00:00",
            llm_model="gemini-3.1-flash-lite",
            llm_default_env_var_name="GOOGLE_API_KEY3",
        )
        by_url = {r["post_url"]: r for r in rows}
        self.assertEqual(by_url["https://t.me/travel/1"]["publication_candidate_status"], "llm_confirmed")
        self.assertEqual(by_url["https://t.me/travel/1"]["visual_confirmation_source"], "RegionTalkImageDiagnostic actual-image scoring")
        self.assertEqual(by_url["https://t.me/local/2"]["publication_candidate_status"], "filtered_before_llm")
        self.assertEqual(by_url["https://t.me/local/2"]["why_not_selected"], "local_kaliningrad_source_for_separate_monitoring")
        self.assertEqual(goal["confirmed_count"], 1)
        self.assertEqual(goal["llm_calls_used_total"], 0)

    def test_publication_queue_counts_sent_rows_as_confirmed_and_budgeted(self) -> None:
        mod = load_module()
        rows, goal = mod.build_publication_candidate_queue(
            [
                {
                    "candidate_memory_id": "cm1", "post_id": "p1", "post_url": "https://t.me/travel/1",
                    "source_id": "s1", "source_title": "Travel", "source_geo_class": "nonlocal_russia",
                    "source_topic_class": "travel_blogger", "post_date": "2026-07-01T00:00:00+00:00",
                    "kaliningrad_oblast_only_scope": True, "kaliningrad_mention_role": "main_subject",
                    "is_ad_or_promo": False, "vector_gate_status": "vector_accept_candidate",
                    "final_verifier_status": "ok", "final_verifier_decision": "accept",
                    "has_firsthand_visit_evidence": "true", "emotion_or_impression_evidence": "true",
                    "nonlocal_value_score": 0.8,
                }
            ],
            [
                {"post_url": "https://t.me/travel/1", "image_queue_status": "actual_scored", "image_model_input_type": "actual_image", "overall_media_score": 0.86, "postcardness_score": 0.77}
            ],
            [
                {
                    "publication_candidate_id": "prev1", "publication_goal_id": "unit-goal",
                    "post_url": "https://t.me/travel/1", "publication_candidate_status": "sent_to_chat",
                    "sent_to_chat": "true", "sent_message_id": "123",
                }
            ],
            {"publication_goal_id": "unit-goal", "target_confirmed": 1, "llm_budget_max": 100, "llm_calls_used_total": 7},
            run_id="pub-run-2",
            run_now="2026-07-07T00:00:00+00:00",
            llm_model="gemini-3.1-flash-lite",
            llm_default_env_var_name="GOOGLE_API_KEY3",
            current_run_preverified_llm_calls=2,
        )
        self.assertEqual(rows[0]["publication_candidate_status"], "sent_to_chat")
        self.assertEqual(rows[0]["goal_stop_candidate"], "true")
        self.assertEqual(goal["confirmed_count"], 1)
        self.assertEqual(goal["sent_count"], 1)
        self.assertEqual(goal["goal_status"], "complete")
        self.assertEqual(goal["llm_calls_used_total"], 9)
        self.assertEqual(goal["llm_calls_used_preverified_this_run"], 2)

    def test_status_event_uses_kaggle_progress_argument(self) -> None:
        mod = load_module()
        class Client:
            def __init__(self) -> None:
                self.kw = None
            def event(self, event, **kwargs):
                self.kw = kwargs
        st = mod.Status()
        st.client = Client()
        st.event("unit_event", phase="unit", status="running", value=1)
        self.assertIsInstance(st.client.kw.get("progress"), dict)
        self.assertNotIn("progress_json", st.client.kw)

    def test_online_post_payload_excludes_raw_text_and_keeps_hashes(self) -> None:
        mod = load_module()
        payload = mod._online_post_payload(
            {
                "post_id": "p1",
                "source_id": "s1",
                "platform": "telegram",
                "post_url": "https://t.me/src/1",
                "platform_post_key": "tg:src:1",
                "post_date": "2026-07-01T00:00:00+00:00",
                "text": "сырой текст поста не должен попадать в YDB row",
                "text_excerpt": "сырой текст",
                "has_media": True,
            },
            run_id="online-run",
            stage="unit",
        )
        self.assertNotIn("text", payload)
        self.assertEqual(payload["post_id"], "p1")
        self.assertEqual(payload["run_id"], "online-run")
        self.assertTrue(payload.get("text_hash"))
        self.assertTrue(payload.get("text_excerpt_hash"))

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

    def test_compact_ydb_state_keeps_discovery_graph_rows(self) -> None:
        mod = load_module()
        compact = mod.compact_region_talk_state_for_ydb({
            "run_id": "disc-run",
            "updated_at": "2026-07-07T00:00:00+00:00",
            "source_candidates": {
                "sc1": {
                    "source_candidate_id": "sc1",
                    "canonical_source_key": "telegram:@travel",
                    "source_title": "Travel",
                    "canonical_url": "https://t.me/travel",
                    "candidate_source_status": "source_frontier",
                    "raw_private_field": "drop",
                }
            },
            "source_edges": {
                "e1": {
                    "edge_id": "e1",
                    "from_source_id": "src1",
                    "to_source_candidate_id": "sc1",
                    "edge_type": "post_text_link",
                    "evidence_url": "https://t.me/travel",
                    "evidence_context_short": "link evidence",
                }
            },
            "comment_discovery": {
                "c1": {
                    "comment_link_id": "c1",
                    "post_url": "https://t.me/src/1",
                    "from_comment_id_hash": "h",
                    "comment_text_redacted": "redacted",
                    "normalized_url": "https://t.me/travel",
                }
            },
        })
        self.assertIn("source_candidates", compact)
        self.assertIn("source_edges", compact)
        self.assertIn("comment_discovery", compact)
        self.assertEqual(compact["source_candidates"]["sc1"]["source_candidate_id"], "sc1")
        self.assertNotIn("raw_private_field", compact["source_candidates"]["sc1"])
        self.assertEqual(compact["source_edges"]["e1"]["edge_id"], "e1")
        self.assertEqual(compact["comment_discovery"]["c1"]["comment_link_id"], "c1")

    def test_build_report_defers_when_dual_embeddings_fail(self) -> None:
        mod = load_module()
        old_mode = os.environ.get("REGION_TALK_TEXT_VECTOR_MODE")
        old_require = os.environ.get("REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS")
        os.environ["REGION_TALK_TEXT_VECTOR_MODE"] = "dual_embeddings"
        os.environ["REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS"] = "1"
        mod.dual_model_semantic_scores_batch = lambda texts, report_event=None: (_ for _ in ()).throw(TimeoutError("unit embedding timeout"))
        seeds = mod.load_seeds(ROOT / "docs" / "features" / "region-talk-channel" / "seed-sources-v1.csv")
        posts = [{
            "post_id": "embed_timeout_post", "source_id": seeds[0].source_id,
            "source_seed_id": seeds[0].source_seed_id, "source_title": "src",
            "platform": "telegram", "handle": "@src", "post_url": "https://t.me/src/99",
            "platform_post_key": "tg:src:99", "post_date": "2026-06-01T12:00:00+00:00",
            "text": "Мы приехали в Калининград, гуляли по Куршской косе, понравилось море",
            "has_media": True, "media_count": 1, "rights_policy": "unknown",
            "source_kind": "travel_media", "source_type": "travel_media", "source_url": "https://t.me/src",
        }]
        try:
            with tempfile.TemporaryDirectory() as td:
                report = mod.build_report(seeds, [], posts, "embed-timeout-run", Path(td))
                self.assertEqual(report.get("status"), "partial")
                self.assertEqual(report["summary"]["partial_reason"], "text_embedding_batch_deferred")
                self.assertEqual(report["summary"]["posts_scored"], 0)
                self.assertEqual(report["summary"]["posts_deferred_by_runtime_budget"], 1)
        finally:
            if old_mode is None:
                os.environ.pop("REGION_TALK_TEXT_VECTOR_MODE", None)
            else:
                os.environ["REGION_TALK_TEXT_VECTOR_MODE"] = old_mode
            if old_require is None:
                os.environ.pop("REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS", None)
            else:
                os.environ["REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS"] = old_require


class RegionTalkKaggleLauncherTests(unittest.TestCase):
    def test_launcher_config_propagates_llm_timeouts(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("rt_candidate_launcher", ROOT / "kaggle" / "execute_region_talk_candidate_report.py")
        self.assertIsNotNone(spec)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)
        old = {k: os.environ.get(k) for k in ["REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "GOOGLE_AI_PROVIDER_TIMEOUT_SEC", "REGION_TALK_TG_RESOLVE_DELAY_MIN_SECONDS"]}
        captured: dict[str, dict] = {}
        def fake_create(_client, _username, slug, _title, writer):
            with tempfile.TemporaryDirectory() as td:
                folder = Path(td)
                writer(folder)
                cfg = folder / "region_talk_run_config.json"
                if cfg.exists():
                    captured["config"] = json.loads(cfg.read_text(encoding="utf-8"))
            return "zigomaro/" + slug
        try:
            os.environ["REGION_TALK_LLM_CALL_TIMEOUT_SECONDS"] = "47"
            os.environ["REGION_TALK_TG_RESOLVE_DELAY_MIN_SECONDS"] = "11"
            os.environ.pop("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", None)
            mod.create_or_replace_dataset = fake_create
            mod.wait_dataset_ready = lambda *args, **kwargs: None
            refs = mod.build_input_datasets(object(), run_id="unit-timeout", username="zigomaro")
            self.assertEqual(len(refs), 2)
            env = captured["config"]["env"]
            self.assertEqual(env["REGION_TALK_LLM_CALL_TIMEOUT_SECONDS"], "47")
            self.assertEqual(env["GOOGLE_AI_PROVIDER_TIMEOUT_SEC"], "47")
            self.assertEqual(env["REGION_TALK_TG_HUMANLIKE_PACING_ENABLED"], "1")
            self.assertEqual(env["REGION_TALK_TG_RESOLVE_DELAY_MIN_SECONDS"], "11")
            self.assertEqual(env["REGION_TALK_TG_SIMILAR_DELAY_MAX_SECONDS"], "45")
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_active_slot_guard_refuses_running_required_kernel(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("rt_candidate_launcher", ROOT / "kaggle" / "execute_region_talk_candidate_report.py")
        self.assertIsNotNone(spec)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)

        class FakeClient:
            def get_kernel_status(self, ref: str) -> dict[str, str]:
                return {"status": "RUNNING" if ref.endswith("candidate") else "COMPLETE"}

        with self.assertRaisesRegex(RuntimeError, "active kernel"):
            mod.assert_region_talk_kaggle_slots_free(FakeClient(), ["u/candidate"], auth_bundle_env="TELEGRAM_AUTH_BUNDLE_DISCOVERY1")

    def test_active_slot_guard_allows_running_optional_sibling_with_different_bundle(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("rt_candidate_launcher", ROOT / "kaggle" / "execute_region_talk_candidate_report.py")
        self.assertIsNotNone(spec)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)

        class FakeClient:
            def get_kernel_status(self, ref: str) -> dict[str, str]:
                return {"status": "RUNNING" if ref.endswith("image") else "COMPLETE"}

        mod.assert_region_talk_kaggle_slots_free(
            FakeClient(),
            ["u/candidate"],
            optional_kernel_refs=["u/image"],
            optional_kernel_auth_bundle_envs={"u/image": "TELEGRAM_AUTH_BUNDLE_DISCOVERY2"},
            auth_bundle_env="TELEGRAM_AUTH_BUNDLE_DISCOVERY1",
        )

    def test_active_slot_guard_refuses_optional_sibling_with_same_bundle(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("rt_candidate_launcher", ROOT / "kaggle" / "execute_region_talk_candidate_report.py")
        self.assertIsNotNone(spec)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)

        class FakeClient:
            def get_kernel_status(self, ref: str) -> dict[str, str]:
                return {"status": "RUNNING" if ref.endswith("image") else "COMPLETE"}

        with self.assertRaisesRegex(RuntimeError, "auth bundle TELEGRAM_AUTH_BUNDLE_DISCOVERY1"):
            mod.assert_region_talk_kaggle_slots_free(
                FakeClient(),
                ["u/candidate"],
                optional_kernel_refs=["u/image"],
                optional_kernel_auth_bundle_envs={"u/image": "TELEGRAM_AUTH_BUNDLE_DISCOVERY1"},
                auth_bundle_env="TELEGRAM_AUTH_BUNDLE_DISCOVERY1",
            )


    def test_active_slot_guard_ignores_unverified_optional_sibling(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("rt_candidate_launcher", ROOT / "kaggle" / "execute_region_talk_candidate_report.py")
        self.assertIsNotNone(spec)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)

        class FakeClient:
            def get_kernel_status(self, ref: str) -> dict[str, str]:
                if ref.endswith("image"):
                    raise ValueError("permission denied")
                return {"status": "COMPLETE"}

        mod.assert_region_talk_kaggle_slots_free(FakeClient(), ["u/candidate"], optional_kernel_refs=["u/image"], auth_bundle_env="TELEGRAM_AUTH_BUNDLE_DISCOVERY1")

    def test_active_slot_guard_allows_terminal_kernels(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("rt_candidate_launcher", ROOT / "kaggle" / "execute_region_talk_candidate_report.py")
        self.assertIsNotNone(spec)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)

        class FakeClient:
            def get_kernel_status(self, ref: str) -> dict[str, str]:
                return {"status": "COMPLETE"}

        mod.assert_region_talk_kaggle_slots_free(FakeClient(), ["u/candidate", "u/image"], auth_bundle_env="TELEGRAM_AUTH_BUNDLE_DISCOVERY1")


if __name__ == "__main__":
    unittest.main()
