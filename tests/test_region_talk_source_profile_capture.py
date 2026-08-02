from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
CAPTURE_PATH = ROOT / "scripts" / "region_talk_source_profile_capture.py"
CANDIDATE_REPORT_PATH = (
    ROOT / "kaggle" / "RegionTalkCandidateReport" / "region_talk_candidate_report.py"
)


def load_path(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def authored_posts(count: int = 40) -> list[dict[str, object]]:
    topics = [
        "Маршрут по Куршской косе и дорога к дюнам",
        "История старого вокзала и архивные детали",
        "Архитектура модернизма: фасад и планировка",
        "Личный дневник поездки к Балтийскому морю",
        "Практический совет: когда ехать и где парковаться",
        "Музейная выставка и культурная программа города",
        "Природная тропа вокруг озера и наблюдение за птицами",
        "Городская прогулка по тихим кварталам",
    ]
    now = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
    return [
        {
            "platform_post_key": f"tg:travel_notes:{index + 1}",
            "post_url": f"https://t.me/travel_notes/{index + 1}",
            "post_date": (now - timedelta(days=index)).isoformat(),
            "text": f"{topics[index % len(topics)]}. Авторское наблюдение номер {index + 1}.",
        }
        for index in range(count)
    ]


class SourceProfileCaptureTests(unittest.TestCase):
    def test_one_current_post_is_not_a_ready_reusable_profile(self) -> None:
        mod = load_path("rt_capture_p0_one", CAPTURE_PATH)
        capture = mod.build_source_profile_capture(
            {
                "platform": "telegram",
                "handle": "@travel_notes",
                "source_url": "https://t.me/travel_notes",
                "source_title": "Travel Notes",
            },
            authored_posts(1),
            description="Авторский канал о поездках.",
            archive_exhausted=True,
        )
        self.assertEqual(capture["capture_status"], "insufficient_authored_posts")
        self.assertFalse(capture["profile_llm_eligible"])
        self.assertEqual(capture["authored_count"], 1)

    def test_fifty_post_capture_is_stable_diverse_and_retains_metadata(self) -> None:
        mod = load_path("rt_capture_p0_stable", CAPTURE_PATH)
        posts = authored_posts(40)
        posts += [
            {
                "platform_post_key": f"tg:travel_notes:repost:{index}",
                "post_url": f"https://t.me/travel_notes/{100 + index}",
                "post_date": f"2026-06-{20-index:02d}T12:00:00+00:00",
                "text": f"Пересланная запись {index}",
                "is_forwarded_or_repost": True,
            }
            for index in range(4)
        ]
        posts += [
            {
                "platform_post_key": f"tg:travel_notes:service:{index}",
                "post_url": f"https://t.me/travel_notes/{110 + index}",
                "post_date": f"2026-06-{15-index:02d}T12:00:00+00:00",
                "text": "",
                "service_action": "MessageActionChatEditTitle",
            }
            for index in range(2)
        ]
        posts += [
            {
                "platform_post_key": f"tg:travel_notes:ad:{index}",
                "post_url": f"https://t.me/travel_notes/{120 + index}",
                "post_date": f"2026-06-{10-index:02d}T12:00:00+00:00",
                "text": f"Реклама. Купите тур со скидкой и промокодом TEST{index}.",
            }
            for index in range(4)
        ]
        source = {
            "platform": "telegram",
            "handle": "@Travel_Notes",
            "source_url": "https://t.me/Travel_Notes/",
            "source_title": " Travel   Notes ",
            "platform_source_id": "777",
        }
        pinned = {
            "platform_post_key": "tg:travel_notes:7",
            "post_url": "https://t.me/travel_notes/7",
            "post_date": "2026-07-27T12:00:00+00:00",
            "text": "Навигация по главным авторским маршрутам канала.",
        }

        first = mod.build_source_profile_capture(
            source,
            posts,
            description="  Авторский канал  о поездках и архитектуре. ",
            pinned_post=pinned,
        )
        second = mod.build_source_profile_capture(
            {**source, "source_title": "Travel Notes"},
            list(reversed(posts)),
            description="Авторский канал о поездках и архитектуре.",
            pinned_post=dict(reversed(list(pinned.items()))),
        )

        self.assertEqual(first["scanned_count"], 50)
        self.assertEqual(first["authored_count"], 40)
        self.assertEqual(first["classification_counts"], {
            "authored": 40,
            "repost": 4,
            "service": 2,
            "ad_like": 4,
        })
        self.assertEqual(first["capture_status"], "ready")
        self.assertEqual(first["description_evidence"]["text"], "Авторский канал о поездках и архитектуре.")
        self.assertEqual(first["pinned_evidence"]["platform_post_key"], "tg:travel_notes:7")
        self.assertGreaterEqual(len(first["representative_excerpts"]), 8)
        self.assertLessEqual(len(first["representative_excerpts"]), 16)
        self.assertGreaterEqual(len({row["topic"] for row in first["representative_excerpts"]}), 3)
        self.assertTrue(all(row["classification"] == "authored" for row in first["representative_excerpts"]))
        self.assertEqual(first["capture_fingerprint"], second["capture_fingerprint"])
        self.assertEqual(first["canonical_source_key"], "telegram:travel_notes")
        self.assertNotIn("posts", first)

        unchanged = mod.capture_change_decision(first, second)
        self.assertEqual(unchanged["capture_change_status"], "unchanged")
        self.assertFalse(unchanged["profile_llm_call_required"])
        self.assertEqual(unchanged["profile_llm_calls_requested"], 0)

    def test_scan_defaults_and_bounds_are_fail_closed(self) -> None:
        mod = load_path("rt_capture_p0_bounds", CAPTURE_PATH)
        self.assertEqual(mod.capture_settings({})["scan_posts"], 50)
        self.assertEqual(
            mod.capture_settings({"REGION_TALK_SOURCE_PROFILE_SCAN_POSTS": "5"})["scan_posts"],
            30,
        )
        self.assertEqual(
            mod.capture_settings({"REGION_TALK_SOURCE_PROFILE_SCAN_POSTS": "500"})["scan_posts"],
            80,
        )
        self.assertEqual(mod.capture_settings({})["min_authored_posts"], 20)

    def test_storage_key_is_stable_and_does_not_grant_publication(self) -> None:
        mod = load_path("rt_capture_p0_storage", CAPTURE_PATH)
        capture = mod.build_source_profile_capture(
            {"platform": "vk", "source_url": "https://vk.com/travel.public"},
            authored_posts(30),
            description="Путевые заметки.",
        )
        self.assertEqual(
            mod.source_profile_capture_storage_pk(capture),
            "source_profile_capture_item:vk:travel.public",
        )
        self.assertFalse(capture["autopublish_allowed"])
        self.assertEqual(capture["publication_effect"], "none")
        self.assertNotIn("publication_permission", capture)


class CandidateReportCaptureAdapterTests(unittest.TestCase):
    def test_vk_wall_adapter_retains_description_pinned_and_copy_history(self) -> None:
        mod = load_path("rt_candidate_capture_vk", CANDIDATE_REPORT_PATH)
        seed = SimpleNamespace(
            platform="vk",
            handle="travel_public",
            canonical_url="https://vk.com/travel_public",
            source_title="Travel Public",
            source_id="src_vk",
        )
        now = int(datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc).timestamp())
        items = [
            {
                "id": index + 1,
                "owner_id": -777,
                "from_id": -777,
                "date": now - index * 86400,
                "text": f"Авторский маршрут и наблюдение {index + 1}",
                "is_pinned": 1 if index == 0 else 0,
                "copy_history": [{"owner_id": -999, "id": 4}] if index == 2 else [],
            }
            for index in range(30)
        ]
        response = {
            "groups": [{"id": 777, "name": "Travel Public", "description": "Авторские поездки по России."}],
        }
        capture = mod.build_vk_source_profile_capture(
            seed,
            items,
            response=response,
            scan_posts=30,
            archive_exhausted=True,
        )
        self.assertEqual(capture["canonical_source_key"], "vk:travel_public")
        self.assertEqual(capture["description_evidence"]["text"], "Авторские поездки по России.")
        self.assertEqual(capture["pinned_evidence"]["platform_post_key"], "vk:-777:1")
        self.assertEqual(capture["classification_counts"]["repost"], 1)
        self.assertEqual(capture["scanned_count"], 30)

    def test_candidate_report_uses_same_telegram_client_and_governor_without_media(self) -> None:
        mod = load_path("rt_candidate_capture_adapter", CANDIDATE_REPORT_PATH)
        now = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
        messages = [
            SimpleNamespace(
                id=index + 1,
                message=f"Маршрут и авторское наблюдение {index + 1}",
                date=now - timedelta(days=index),
                action=None,
                fwd_from=None,
                forward=None,
            )
            for index in range(30)
        ]
        messages[2].fwd_from = SimpleNamespace(from_name="Другой канал")

        class Client:
            download_calls = 0

            def iter_messages(self, entity, **kwargs):
                self.iter_kwargs = kwargs

                async def rows():
                    for item in messages[: kwargs["limit"]]:
                        yield item

                return rows()

            async def download_media(self, *_args, **_kwargs):
                self.download_calls += 1

        class Governor:
            total_attempted = 0
            total_ok = 0
            requests_by_method: dict[str, int] = {}

            def has_total_request_budget(self, *_args, **_kwargs):
                return True

            async def humanlike_pause(self, *_args, **_kwargs):
                return True

            def log(self, *_args, **_kwargs):
                return None

        seed = SimpleNamespace(
            platform="telegram",
            handle="@travel_notes",
            canonical_url="https://t.me/travel_notes",
            source_title="Travel Notes",
            source_id="src_1",
        )
        entity = SimpleNamespace(id=777, title="Travel Notes", about="Авторские путешествия", pinned_msg_id=1)
        client = Client()
        governor = Governor()
        with mock.patch.object(
            mod,
            "telegram_source_profile_metadata",
            mock.AsyncMock(return_value={
                "description": "Авторские путешествия",
                "platform_source_id": "777",
                "pinned_message_id": 1,
            }),
        ):
            capture = asyncio.run(mod.acquire_telegram_source_profile_capture(
                client,
                entity,
                seed,
                governor,
                scan_posts=30,
                archive_exhausted=True,
            ))

        self.assertEqual(client.iter_kwargs["limit"], 30)
        self.assertNotIn("wait_time", client.iter_kwargs)
        self.assertEqual(client.download_calls, 0)
        self.assertEqual(capture["scanned_count"], 30)
        self.assertEqual(capture["classification_counts"]["repost"], 1)
        self.assertEqual(capture["description_evidence"]["text"], "Авторские путешествия")
        self.assertEqual(governor.requests_by_method["source_profile.iter_messages"], 1)

    def test_current_capture_read_precedes_changed_write_and_unchanged_is_noop(self) -> None:
        mod = load_path("rt_candidate_capture_ydb", CANDIDATE_REPORT_PATH)
        capture = mod.build_source_profile_capture(
            {"platform": "telegram", "handle": "travel_notes"},
            authored_posts(30),
            description="Авторский канал.",
        )
        pool = mock.Mock()
        pool.retry_operation_sync.side_effect = [capture]
        with mock.patch.object(mod, "_get_business_heartbeat_pool", return_value=(mock.Mock(), mock.Mock(), pool, "/db/kv")):
            result = mod.write_region_talk_online_source_profile_capture(
                capture,
                run_id="run-test",
                stage="unit",
            )
        self.assertEqual(result["capture_change_status"], "unchanged")
        self.assertEqual(result["written_ydb_rows"], 0)
        self.assertFalse(result["profile_llm_call_required"])
        self.assertEqual(pool.retry_operation_sync.call_count, 1)


if __name__ == "__main__":
    unittest.main()
