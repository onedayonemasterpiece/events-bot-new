from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "region_talk_low_frequency_research.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_low_frequency_research", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeAdapter:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []

    def search(self, request):
        self.calls.append(dict(request))
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, BaseException):
                raise response
            return response
        return {"messages": []}


class RegionTalkLowFrequencyResearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mod = load_module()
        self.t0 = datetime(2026, 7, 13, 12, tzinfo=timezone.utc)
        self.sources = [
            {"source_id": "two", "username": "Two", "peer_id": 102, "access_hash": 202},
            {"source_id": "one", "username": "One", "peer_id": 101, "access_hash": 201},
        ]

    def manifest(self, **overrides):
        values = {
            "code_sha": "2de9194753318f515eab7d1bfa327325a3484ecf",
            "t0": self.t0,
            "sources": self.sources,
            "current_queries": ["Калининград"],
            "expanded_queries": ["Советск", "Тапиау"],
            "continuation_queries": ["Советск", "Тапиау"],
            "k0_urls": ["https://t.me/one/1"],
            "anchors": [{"source_id": "one", "url": "https://t.me/one/1", "published_at": "2026-07-01T00:00:00Z"}],
        }
        values.update(overrides)
        return self.mod.build_manifest(**values)

    def test_manifest_and_query_hashes_are_deterministic(self) -> None:
        first = self.manifest()
        second = self.manifest()
        self.assertEqual(first, second)
        self.assertEqual(first["window_days"], 365)
        self.assertEqual(first["window_start"], "2025-07-13T12:00:00Z")
        self.assertEqual(first["source_identities"][0]["source_id"], "one")
        self.assertEqual(
            first["query_hashes"][self.mod.STRUCTURE_A][0]["sha256"],
            self.mod.stable_hash("Калининград"),
        )
        changed = self.manifest(expanded_queries=["Советск", "Черняховск"])
        self.assertNotEqual(first["manifest_hash"], changed["manifest_hash"])

    def test_default_and_ungated_capture_are_zero_network_and_zero_write(self) -> None:
        adapter = FakeAdapter()
        with tempfile.TemporaryDirectory() as temporary:
            capture_path = Path(temporary) / "raw.jsonl"
            self.assertEqual(self.mod.main([]), 2)
            with self.assertRaises(self.mod.SafetyGateError):
                self.mod.run_capture(
                    self.manifest(), capture_path, adapter,
                    auth_role="DISCOVERY1", environ={"TELEGRAM_AUTH_BUNDLE_DISCOVERY1": "x"},
                )
            self.assertEqual(adapter.calls, [])
            self.assertFalse(capture_path.exists())

    def test_url_taxonomy_has_exact_categories_and_expected_membership(self) -> None:
        taxonomy = self.mod.classify_url_taxonomy(
            [
                {"url": "https://t.me/one/1", "is_KO": True},
                {"url": "https://t.me/new/2", "is_KO": True},
                {"url": "https://t.me/new/2", "is_KO": True},
                {"url": "https://t.me/new/3"},
            ],
            ["https://telegram.me/one/1?x=1"],
            downstream_rows=[{"post_url": "https://t.me/new/2", "downstream_status": "accepted"}],
        )
        self.assertEqual(tuple(taxonomy["buckets"]), self.mod.URL_TAXONOMY)
        self.assertEqual(taxonomy["buckets"]["baseline_replay"], ["https://t.me/one/1"])
        self.assertEqual(taxonomy["buckets"]["experiment_repeat"], ["https://t.me/new/2"])
        self.assertEqual(
            taxonomy["buckets"]["dedup_eligible"],
            ["https://t.me/new/2", "https://t.me/new/3", "https://t.me/one/1"],
        )
        self.assertEqual(taxonomy["buckets"]["new"], ["https://t.me/new/2", "https://t.me/new/3"])
        self.assertEqual(taxonomy["buckets"]["new_KO"], ["https://t.me/new/2"])
        self.assertEqual(taxonomy["buckets"]["downstream_accepted"], ["https://t.me/new/2"])

    def test_continuation_scheduler_is_round_robin_fair(self) -> None:
        scheduled = self.mod.schedule_continuations(
            self.manifest()["source_identities"],
            ["q0", "q1", "q2"],
            cursors={"one": 1, "two": 0},
        )
        self.assertEqual(
            [(row["source_id"], row["query"]) for row in scheduled],
            [("one", "q1"), ("two", "q0"), ("one", "q2"), ("two", "q1"), ("two", "q2")],
        )
        for start in range(0, len(scheduled), 2):
            self.assertEqual(len({row["source_id"] for row in scheduled[start : start + 2]}), min(2, len(scheduled[start : start + 2])))

    def test_request_budget_never_allows_one_request_over_ceiling(self) -> None:
        budget = self.mod.RequestBudget({"total_requests": 2, "per_source_requests": 2})
        request = {"source_id": "one", "structure": "A_current", "query": "q"}
        self.assertEqual(budget.reserve(request), 1)
        with self.assertRaises(self.mod.RequestCeilingReached):
            budget.reserve(request)  # per source/query is one, with no adapter-side retry

        manifest = self.manifest(
            current_queries=["a", "b"],
            expanded_queries=["c"],
            continuation_queries=["d"],
            request_ceilings={"total_requests": 2, "per_source_requests": 2},
        )
        adapter = FakeAdapter()
        with tempfile.TemporaryDirectory() as temporary:
            summary = self.mod.run_capture(
                manifest,
                Path(temporary) / "raw.jsonl",
                adapter,
                allow_telegram_read=True,
                auth_role="DISCOVERY1",
                environ={"TELEGRAM_AUTH_BUNDLE_DISCOVERY1": "secret"},
            )
        self.assertEqual(len(adapter.calls), 2)
        self.assertEqual(summary["requests_attempted"], 2)
        self.assertEqual(summary["stop_reason"], "total_request_ceiling")
        self.assertTrue(all(call["cached_peer_only"] and not call["allow_entity_resolve"] for call in adapter.calls))

    def test_flood_wait_aborts_immediately_without_retry_and_appends_error(self) -> None:
        class FloodWaitError(Exception):
            pass

        adapter = FakeAdapter([FloodWaitError("wait 900 seconds"), {"messages": []}])
        with tempfile.TemporaryDirectory() as temporary:
            capture_path = Path(temporary) / "raw.jsonl"
            capture_path.write_text('{"preexisting":true}\n', encoding="utf-8")
            with self.assertRaisesRegex(self.mod.CaptureAborted, "flood_wait"):
                self.mod.run_capture(
                    self.manifest(),
                    capture_path,
                    adapter,
                    allow_telegram_read=True,
                    auth_role="DISCOVERY2",
                    environ={
                        "TELEGRAM_AUTH_BUNDLE_DISCOVERY2": "discovery-secret",
                        "TELEGRAM_AUTH_BUNDLE_E2E": "must-not-be-used",
                        "TELEGRAM_AUTH_BUNDLE_S22": "must-not-be-used",
                    },
                )
            rows = [json.loads(line) for line in capture_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(rows[0], {"preexisting": True})
        self.assertEqual(rows[1]["error"]["type"], "FloodWaitError")
        self.assertNotIn("access_hash", rows[1]["request"])
        self.assertNotIn("auth_bundle", rows[1]["request"])

    def test_second_timeout_aborts_and_never_retries(self) -> None:
        adapter = FakeAdapter([TimeoutError("first"), TimeoutError("second")])
        manifest = self.manifest(request_ceilings={"error_rate_min_requests": 20})
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(self.mod.CaptureAborted, "second_timeout"):
                self.mod.run_capture(
                    manifest,
                    Path(temporary) / "raw.jsonl",
                    adapter,
                    allow_telegram_read=True,
                    auth_role="DISCOVERY1",
                    environ={"TELEGRAM_AUTH_BUNDLE_DISCOVERY1": "secret"},
                )
        self.assertEqual(len(adapter.calls), 2)

    def test_final_error_rate_above_five_percent_aborts_short_capture(self) -> None:
        adapter = FakeAdapter([RuntimeError("read failed"), {"messages": []}])
        manifest = self.manifest(
            current_queries=["only"],
            expanded_queries=["unused"],
            continuation_queries=["unused-too"],
            request_ceilings={"total_requests": 2},
        )
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(self.mod.CaptureAborted, "error_rate_above_5_percent"):
                self.mod.run_capture(
                    manifest,
                    Path(temporary) / "raw.jsonl",
                    adapter,
                    allow_telegram_read=True,
                    auth_role="DISCOVERY1",
                    environ={"TELEGRAM_AUTH_BUNDLE_DISCOVERY1": "secret"},
                )
        self.assertEqual(len(adapter.calls), 2)

    def test_entity_resolve_attempt_aborts_on_first_response(self) -> None:
        adapter = FakeAdapter([{"messages": [], "event": "entity_resolve_attempt"}])
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(self.mod.CaptureAborted, "entity_resolve_attempt"):
                self.mod.run_capture(
                    self.manifest(),
                    Path(temporary) / "raw.jsonl",
                    adapter,
                    allow_telegram_read=True,
                    auth_role="DISCOVERY1",
                    environ={"TELEGRAM_AUTH_BUNDLE_DISCOVERY1": "secret"},
                )
        self.assertEqual(len(adapter.calls), 1)

    def test_replay_truncates_to_inclusive_365_day_window_and_marks_anchor(self) -> None:
        manifest = self.manifest()
        request = {
            "request_index": 1,
            "source_id": "one",
            "peer_id": 101,
            "structure": "A_current",
            "query": "Калининград",
            "query_hash": self.mod.stable_hash("Калининград"),
            "result_limit": 20,
        }
        raw = [
            {
                "schema": self.mod.RAW_SCHEMA_VERSION,
                "manifest_hash": manifest["manifest_hash"],
                "request": request,
                "response": {
                    "messages": [
                        {"url": "https://t.me/one/1", "post_date": "2026-07-02T00:00:00Z"},
                        {"url": "https://t.me/one/2", "post_date": "2025-07-13T12:00:00Z"},
                        {"url": "https://t.me/one/3", "post_date": "2025-07-13T11:59:59Z"},
                        {"url": "https://t.me/one/4", "post_date": "2026-07-13T12:00:01Z"},
                        {"url": "https://t.me/one/5"},
                    ]
                },
            }
        ]
        replay = self.mod.replay_capture(manifest, raw)
        self.assertEqual([row["url"] for row in replay["rows"]], ["https://t.me/one/1", "https://t.me/one/2"])
        self.assertTrue(replay["rows"][0]["anchor_window_replay"])
        self.assertFalse(replay["rows"][1]["anchor_window_replay"])
        self.assertEqual(replay["messages_truncated"], 3)
        self.assertEqual(replay["request_metrics"]["A_current"]["attempted"], 1)

    def test_visible_text_guard_accepts_place_cases_but_rejects_telegram_stemming_noise(self) -> None:
        self.assertFalse(self.mod.source_query_matches_visible_text("Советск", "Советский район Москвы"))
        self.assertTrue(self.mod.source_query_matches_visible_text("Советск", "Выходные в Советске"))
        self.assertTrue(self.mod.source_query_matches_visible_text("Калининградская область", "маршрут по Калининградской области"))
        self.assertTrue(self.mod.source_query_matches_visible_text("Калининград", "Вернулись из Калининграда"))
        self.assertTrue(self.mod.source_query_matches_visible_text("Бальга", "Поехали к замку Бальга"))
        self.assertTrue(self.mod.source_query_matches_visible_text("Виштынец", "Отдыхали на Виштынце"))
        self.assertTrue(self.mod.source_query_matches_visible_text("Тапиау", "Увидели замок Тапиау"))

    def test_report_exposes_request_normalised_metrics_by_structure(self) -> None:
        manifest = self.manifest()
        raw = [
            {
                "schema": self.mod.RAW_SCHEMA_VERSION,
                "manifest_hash": manifest["manifest_hash"],
                "request": {
                    "request_index": 1,
                    "source_id": "one",
                    "structure": "A_current",
                    "query": "Калининград",
                    "query_hash": self.mod.stable_hash("Калининград"),
                },
                "response": {
                    "messages": [{
                        "url": "https://t.me/one/9",
                        "post_date": "2026-07-01T00:00:00Z",
                        "text": "Поездка в Калининград",
                        "is_KO": True,
                    }]
                },
            },
            {
                "schema": self.mod.RAW_SCHEMA_VERSION,
                "manifest_hash": manifest["manifest_hash"],
                "request": {
                    "request_index": 2,
                    "source_id": "one",
                    "structure": "B_expanded",
                    "query": "Тапиау",
                    "query_hash": self.mod.stable_hash("Тапиау"),
                },
                "response": {"messages": []},
            },
        ]
        replay = self.mod.replay_capture(manifest, raw)
        report = self.mod.build_report(manifest, replay)
        self.assertEqual(report["by_structure"]["A_current"]["requests"]["attempted"], 1)
        self.assertEqual(report["by_structure"]["A_current"]["counts"]["new_KO"], 1)
        self.assertEqual(report["by_structure"]["A_current"]["new_KO_urls_per_100_requests"], 100.0)
        self.assertEqual(report["by_structure"]["B_expanded"]["requests"]["attempted"], 1)
        self.assertIsNone(report["by_structure"]["D_anchor_window_replay"]["new_urls_per_100_requests"])

    def test_only_explicit_discovery_roles_are_accepted(self) -> None:
        for role in ("E2E", "S22", "", "discovery3"):
            with self.assertRaises(self.mod.SafetyGateError):
                self.mod.select_auth_bundle(role, os.environ)

    def test_live_adapter_requires_pacing_and_records_delays_without_real_sleep(self) -> None:
        class LiveFake(FakeAdapter):
            requires_human_pacing = True

        adapter = LiveFake()
        manifest = self.manifest(
            current_queries=["a"],
            expanded_queries=["b"],
            continuation_queries=["c"],
            request_ceilings={"total_requests": 2, "per_source_requests": 2},
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "raw.jsonl"
            with self.assertRaisesRegex(self.mod.SafetyGateError, "human-like pacing"):
                self.mod.run_capture(
                    manifest,
                    path,
                    adapter,
                    allow_telegram_read=True,
                    auth_role="DISCOVERY1",
                    environ={"TELEGRAM_AUTH_BUNDLE_DISCOVERY1": "secret"},
                )
            sleeps = []
            summary = self.mod.run_capture(
                manifest,
                path,
                adapter,
                allow_telegram_read=True,
                auth_role="DISCOVERY1",
                environ={"TELEGRAM_AUTH_BUNDLE_DISCOVERY1": "secret"},
                pace_requests=True,
                sleep_fn=sleeps.append,
                uniform_fn=lambda low, high: low,
            )
            rows = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual(summary["requests_attempted"], 2)
        self.assertTrue(summary["human_pacing_enabled"])
        self.assertEqual(sleeps, [5.0, 10.0])
        self.assertEqual([row["request"]["paced_delay_seconds"] for row in rows], sleeps)


if __name__ == "__main__":
    unittest.main()
