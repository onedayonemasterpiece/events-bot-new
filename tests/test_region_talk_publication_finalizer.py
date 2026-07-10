from __future__ import annotations

import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_publication_finalizer.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_publication_finalizer", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def external_source(handle: str = "travelcase") -> dict[str, str]:
    return {
        "canonical_source_key": f"telegram:{handle}",
        "platform": "telegram",
        "handle": handle,
        "source_url": f"https://t.me/{handle}",
        "source_title": "Travel notes",
        "source_scope": "external",
        "source_geo_class": "nonlocal_russia",
        "source_topic_class": "travel_blogger",
    }


def candidate_row(url: str = "https://t.me/travelcase/10", **overrides):
    row = {
        "post_url": url,
        "source_title": "Travel notes",
        "source_url": "https://t.me/travelcase",
        "canonical_source_key": "telegram:travelcase",
        "source_class_guess": "nonlocal_travel_or_general_source",
        "_authoritative_source": external_source(),
        "_previous_publication": {},
        "finalization_trigger": "never_finalized",
        "text": "Личный рассказ о поездке в Калининградскую область.",
        "attempt_count": 0,
    }
    row.update(overrides)
    return row


def eligibility(verdict: str = "eligible") -> dict[str, object]:
    return {
        "verdict": verdict,
        "evidence": {"source_policy": verdict, "authoritative": True},
        "gate_version": "publication-source-gate-v3",
    }


class RegionTalkPublicationFinalizerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_module()

    def test_source_class_guess_uses_region_talk_local_source_filter(self) -> None:
        self.assertEqual(
            self.mod.source_class_guess("Дом китобоя", "https://t.me/domkitoboya", {}),
            "local_region_source",
        )

    def test_ineligible_tombstone_is_current_only_with_current_source_fingerprint(self) -> None:
        row = {
            "publication_eligibility_verdict": "review",
            "publication_eligibility_gate_version": "gate-v1",
            "publication_eligibility_evidence": "same-evidence",
            "authoritative_source_fingerprint": "source-v2-value",
            "authoritative_source_fingerprint_version": self.mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            "_previous_publication": {
                "publication_status": "eligibility_review_tombstone",
                "publication_candidate_status": "tombstoned_review",
                "publication_eligibility_verdict": "review",
                "publication_eligibility_gate_version": "gate-v1",
                "publication_eligibility_evidence": "same-evidence",
                "authoritative_source_fingerprint": "source-v2-value",
                "authoritative_source_fingerprint_version": self.mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            },
        }
        self.assertTrue(self.mod._ineligible_state_is_current(row, "review"))
        row["_previous_publication"]["authoritative_source_fingerprint_version"] = "region_talk_source_fingerprint_v1"
        self.assertFalse(self.mod._ineligible_state_is_current(row, "review"))
        self.assertEqual(
            self.mod.source_class_guess("Travel notes", "https://t.me/example_travel", {}),
            "nonlocal_travel_or_general_source",
        )

    def test_normalize_post_url_collapses_public_telegram_variants(self) -> None:
        variants = [
            "http://T.ME/TravelCase/10/",
            "https://telegram.me/travelcase/10?single=1#x",
            "https://t.me/s/TravelCase/10",
            "t.me/travelcase/10/",
        ]
        self.assertEqual(
            {self.mod.normalize_post_url(url) for url in variants},
            {"https://t.me/travelcase/10"},
        )

    def test_authoritative_source_index_and_live_rows_join_by_canonical_key(self) -> None:
        mod = self.mod
        source = external_source("travelcase")
        kinds = {
            "image_queue_item": {
                "image:10": {
                    "post_url": "https://telegram.me/TravelCase/10?single=1",
                    "source_url": "https://t.me/TravelCase/",
                    "source_title": "Travel notes",
                    "platform": "telegram",
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "overall_media_score": 0.8,
                    "updated_at": "2026-07-10T08:00:00+00:00",
                },
                "image:11": {
                    "post_url": "https://t.me/travelcase/11",
                    "source_url": "https://t.me/travelcase",
                    "source_title": "Travel notes",
                    "platform": "telegram",
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "overall_media_score": 0.7,
                    "updated_at": "2026-07-10T08:00:00+00:00",
                },
            },
            "candidate_memory_item": {
                "memory:10": {
                    "post_url": "https://t.me/s/travelcase/10/",
                    "text_excerpt": "Exact YDB text",
                }
            },
            "publication_candidate_item": {
                "publication:10": {
                    "post_url": "http://t.me/TRAVELCASE/10",
                    "publication_status": "gemini_accept",
                    "publication_candidate_status": "llm_confirmed",
                    "updated_at": "2026-07-10T07:00:00+00:00",
                }
            },
            "source_queue_item": {"source:travelcase": source},
            "source_status_item": {},
            "online_source_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        pool = Pool()

        class Ydb:
            def SessionPool(self, _driver):
                return pool

        ydb = Ydb()
        with (
            mock.patch.object(mod.rt, "ydb_connect", return_value=(ydb, object(), object())),
            mock.patch.object(mod.rt, "ydb_kv_table_path", return_value="table"),
            mock.patch.object(mod.rt, "ydb_select_kind_items", side_effect=lambda _s, _y, _t, kind, limit: kinds[kind]),
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-10T09:00:00+00:00"),
        ):
            _ydb, _driver, _pool, _table, rows = mod.read_live_rows(100, 100)

        by_url = {row["post_url"]: row for row in rows}
        finalized = by_url["https://t.me/travelcase/10"]
        never_finalized = by_url["https://t.me/travelcase/11"]
        self.assertEqual(finalized["_authoritative_source"], source)
        self.assertEqual(finalized["canonical_source_key"], "telegram:travelcase")
        self.assertEqual(finalized["text"], "Exact YDB text")
        self.assertEqual(finalized["finalization_trigger"], "")
        self.assertEqual(never_finalized["finalization_trigger"], "never_finalized")

    def test_eligibility_helper_receives_authoritative_source_and_fields_persist(self) -> None:
        mod = self.mod
        row = candidate_row()
        authoritative = row["_authoritative_source"]

        def gate(actual_row, actual_source):
            self.assertIs(actual_row, row)
            self.assertIs(actual_source, authoritative)
            return eligibility("eligible")

        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, side_effect=gate) as gate_mock,
            mock.patch.object(
                mod.rt,
                "call_region_talk_semantic_llm",
                return_value={"llm_gate_status": "ok", "llm_decision": "accept", "llm_reason": "good"},
            ),
        ):
            result = mod.verify_rows(
                [row],
                max_llm=1,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-10T10:00:00+00:00",
            )

        self.assertEqual(gate_mock.call_count, 1)
        self.assertEqual(result[0]["publication_eligibility_verdict"], "eligible")
        self.assertEqual(result[0]["publication_eligibility_gate_version"], "publication-source-gate-v3")
        self.assertEqual(json.loads(result[0]["publication_eligibility_evidence"])["source_policy"], "eligible")

        captured = {}

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        def capture(_session, _ydb, _table, items, _now, **_kwargs):
            captured["items"] = items
            return len(items)

        with (
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-10T10:01:00+00:00"),
            mock.patch.object(mod.rt, "ensure_ydb_kv_table"),
            mock.patch.object(mod.rt, "ydb_upsert_json_many", side_effect=capture),
        ):
            self.assertEqual(mod.write_publication_rows(Pool(), object(), "table", result, "run-1"), 1)
        payload = captured["items"][0][2]
        self.assertEqual(payload["publication_eligibility_verdict"], "eligible")
        self.assertEqual(payload["publication_eligibility_gate_version"], "publication-source-gate-v3")
        self.assertEqual(payload["attempt_count"], 1)
        self.assertEqual(payload["finalizer_state_version"], mod.PUBLICATION_FINALIZER_STATE_VERSION)

    def test_unknown_local_and_spam_fail_closed_without_gemini_and_revoke_prior_accept(self) -> None:
        mod = self.mod
        unknown = candidate_row(
            "https://t.me/unknown/1",
            canonical_source_key="telegram:unknown",
            _authoritative_source=None,
        )
        local_source = {
            "canonical_source_key": "telegram:domkitoboya",
            "platform": "telegram",
            "handle": "domkitoboya",
            "source_url": "https://t.me/domkitoboya",
            "source_title": "Дом китобоя",
        }
        local = candidate_row(
            "https://t.me/domkitoboya/2",
            canonical_source_key="telegram:domkitoboya",
            _authoritative_source=local_source,
            _previous_publication={
                "publication_status": "gemini_accept",
                "publication_candidate_status": "llm_confirmed",
            },
        )
        spam = candidate_row("https://t.me/travelcase/3")

        gate_results = [eligibility("eligible"), eligibility("eligible"), eligibility("spam")]
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, side_effect=gate_results) as gate_mock,
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [unknown, local, spam],
                max_llm=10,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-10T10:00:00+00:00",
            )

        self.assertEqual(gate_mock.call_count, 3)
        llm_mock.assert_not_called()
        self.assertEqual([row["publication_eligibility_verdict"] for row in result], ["review", "reject", "reject"])
        self.assertEqual(unknown["publication_candidate_status"], "tombstoned_review")
        self.assertEqual(local["publication_status"], "eligibility_revoked")
        self.assertEqual(local["publication_candidate_status"], "revoked")
        self.assertEqual(local["publication_revoked"], "true")
        self.assertEqual(spam["publication_candidate_status"], "tombstoned_reject")

        prior_revoke = dict(local)
        repeated = candidate_row(
            "https://t.me/domkitoboya/2",
            canonical_source_key="telegram:domkitoboya",
            _authoritative_source=local_source,
            _previous_publication=prior_revoke,
        )
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility("eligible")),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_again,
        ):
            self.assertEqual(
                mod.verify_rows(
                    [repeated],
                    max_llm=10,
                    model="gemini-test",
                    default_env_var_name="KEY",
                    now_iso="2026-07-11T10:00:00+00:00",
                ),
                [],
            )
        llm_again.assert_not_called()

    def test_terminal_and_retryable_gemini_statuses_track_attempts_and_retry_due(self) -> None:
        mod = self.mod
        retry = candidate_row("https://t.me/travelcase/20", attempt_count=2)
        accepted = candidate_row("https://t.me/travelcase/21", attempt_count=0)
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, side_effect=[eligibility(), eligibility()]),
            mock.patch.object(
                mod.rt,
                "call_region_talk_semantic_llm",
                side_effect=[
                    {"llm_gate_status": "rate_limited", "llm_reason": "429"},
                    {"llm_gate_status": "ok", "llm_decision": "accept", "llm_reason": "good"},
                ],
            ),
            mock.patch.dict(os.environ, {"REGION_TALK_FINALIZER_RATE_LIMIT_RETRY_SECONDS": "60"}),
        ):
            result = mod.verify_rows(
                [retry, accepted],
                max_llm=2,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-10T10:00:00+00:00",
            )

        self.assertEqual(len(result), 2)
        self.assertEqual(retry["publication_status"], "gemini_rate_limited")
        self.assertEqual(retry["finalization_status"], "retryable")
        self.assertEqual(retry["attempt_count"], 3)
        self.assertEqual(retry["next_attempt_after"], "2026-07-10T10:01:00+00:00")
        self.assertEqual(
            mod.finalization_trigger(retry, now_iso="2026-07-10T10:00:30+00:00"),
            "",
        )
        self.assertEqual(
            mod.finalization_trigger(retry, now_iso="2026-07-10T10:01:01+00:00"),
            "retry_due",
        )
        self.assertEqual(accepted["publication_status"], "gemini_accept")
        self.assertEqual(accepted["finalization_status"], "terminal")
        self.assertEqual(accepted["attempt_count"], 1)
        self.assertEqual(mod.finalization_trigger(accepted, now_iso="2026-07-11T10:00:00+00:00"), "")

    def test_no_text_is_terminal_and_does_not_repeatedly_consume_llm_budget(self) -> None:
        mod = self.mod
        first = candidate_row(text="", short_summary="")
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility()),
            mock.patch.object(mod, "telegram_public_text", return_value="") as fallback_mock,
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [first],
                max_llm=1,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-10T10:00:00+00:00",
            )
        self.assertEqual(result[0]["publication_status"], "no_text_for_gemini")
        self.assertEqual(result[0]["attempt_count"], 0)
        fallback_mock.assert_called_once()
        llm_mock.assert_not_called()

        second = candidate_row(
            text="",
            short_summary="",
            finalization_trigger=mod.finalization_trigger(result[0], now_iso="2026-07-11T10:00:00+00:00"),
        )
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility()),
            mock.patch.object(mod, "telegram_public_text") as fallback_again,
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_again,
        ):
            self.assertEqual(
                mod.verify_rows(
                    [second],
                    max_llm=1,
                    model="gemini-test",
                    default_env_var_name="KEY",
                    now_iso="2026-07-11T10:00:00+00:00",
                ),
                [],
            )
        fallback_again.assert_not_called()
        llm_again.assert_not_called()

    def test_public_tme_fallback_is_default_off_and_requires_explicit_opt_in(self) -> None:
        mod = self.mod
        response = mock.Mock()
        response.text = (
            '<div class="tgme_widget_message_wrap">'
            '<div data-post="travelcase/10"></div>'
            '<div class="tgme_widget_message_text js-message_text">Exact<br>public text</div>'
        )
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(mod.PUBLIC_TME_FALLBACK_ENV, None)
            with mock.patch.object(mod.requests, "get") as get_mock:
                self.assertEqual(mod.telegram_public_text("https://t.me/travelcase/10"), "")
                get_mock.assert_not_called()

        with (
            mock.patch.dict(os.environ, {mod.PUBLIC_TME_FALLBACK_ENV: "true"}),
            mock.patch.object(mod.requests, "get", return_value=response) as get_mock,
        ):
            self.assertEqual(
                mod.telegram_public_text("https://t.me/s/TravelCase/10?single=1"),
                "Exact\npublic text",
            )
            self.assertEqual(get_mock.call_args.args[0], "https://t.me/s/travelcase/10")

    def test_existing_confirmed_row_gets_current_eligibility_attestation_without_llm(self) -> None:
        mod = self.mod
        previous = {
            "publication_status": "gemini_accept",
            "publication_candidate_status": "llm_confirmed",
        }
        row = candidate_row(finalization_trigger="", _previous_publication=previous, **previous)
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility("eligible")),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [row], max_llm=3, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-07-11T10:00:00+00:00",
            )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["publication_status"], "gemini_accept")
        self.assertEqual(result[0]["publication_eligibility_verdict"], "eligible")
        self.assertEqual(result[0]["llm_attempted_this_run"], "false")
        llm_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
