from __future__ import annotations

import importlib.util
import base64
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import social_metrics_batch as smb
from db import Database
from models import Event


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)


def _event(event_id: int, *, vk_post_id: int | None = None, tg_post_id: int | None = None) -> Event:
    return Event(
        id=event_id,
        title=f"Событие {event_id}",
        description="Описание",
        date="2026-07-20",
        time="19:00",
        location_name="Площадка",
        source_text="Источник",
        source_vk_post_url=(
            f"https://vk.com/wall-231920894_{vk_post_id}" if vk_post_id else None
        ),
        tg_event_post_id=tg_post_id,
        tg_event_post_url=(f"https://t.me/kldevents/{tg_post_id}" if tg_post_id else None),
        lifecycle_status="active",
        identity_status="canonical",
        silent=False,
    )


@pytest.mark.asyncio
async def test_batch_collector_writes_four_bucket_snapshot_and_legacy_metrics(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    post_ts = int(NOW.timestamp()) - 90 * 60
    async with db.get_session() as session:
        session.add(_event(1, vk_post_id=101))
        await session.commit()
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO event_publication(
                event_id, platform, target, stored_url, live_url, stored_post_id,
                live_post_id, status
            ) VALUES(1,'vk','klgdevents',?,?,?,?, 'published')
            """,
            (
                "https://vk.com/wall-231920894_101",
                "https://vk.com/wall-231920894_201",
                101,
                201,
            ),
        )
        await conn.commit()
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO vk_post_metric(
                group_id, post_id, age_day, source_url, post_ts, collected_ts,
                views, likes, comments, reposts
            ) VALUES(231920894,201,0,?,?,?,?,?,?,?)
            """,
            (
                "https://vk.com/wall-231920894_201",
                post_ts,
                post_ts + 60,
                10,
                1,
                None,
                0,
            ),
        )
        await conn.commit()

    calls: list[tuple[int, list[int]]] = []

    async def fetcher(group_id: int, post_ids):
        calls.append((group_id, list(post_ids)))
        return {
            201: smb.MetricPayload(
                post_id=201,
                post_ts=post_ts,
                views=500,
                likes=20,
                comments=None,
                shares=0,
                reactions={"👍": 20},
            )
        }

    monkeypatch.setenv("ENABLE_SOCIAL_METRICS_BATCH", "1")
    monkeypatch.setenv("SOCIAL_METRICS_VK_ENABLED", "1")
    monkeypatch.setenv("SOCIAL_METRICS_TG_ENABLED", "0")
    result = await smb.run_social_metrics_batch(
        db,
        now_utc=NOW,
        vk_fetcher=fetcher,
        resolve_vk=False,
    )

    assert calls == [(231920894, [201])]
    assert result["collected"] == 1
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                """
                SELECT age_bucket, views, likes, comments, shares, reactions_json, status
                FROM social_metric_snapshot
                WHERE platform='vk' AND publisher_id='231920894' AND post_id=201
                """
            )
        ).fetchone()
        legacy = await (
            await conn.execute(
            "SELECT views, likes, comments, reposts FROM vk_post_metric WHERE group_id=231920894 AND post_id=201"
            )
        ).fetchone()
    assert tuple(row[:5]) == ("1h", 500, 20, None, 0)
    assert json.loads(row[5]) == {"👍": 20}
    assert row[6] == "collected"
    assert tuple(legacy) == (500, 20, None, 0)
    await db.close()


def test_due_plan_skips_missed_points_instead_of_copying_one_value():
    bucket, skipped = smb._due_plan(
        post_ts=int(NOW.timestamp()) - 8 * 60 * 60,
        now_ts=int(NOW.timestamp()),
        terminal_buckets=set(),
    )
    assert bucket == "6h"
    assert skipped == ("1h",)


@pytest.mark.asyncio
async def test_vk_resolver_rejects_unrelated_stored_id_and_batches_wall_recovery(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(_event(1, vk_post_id=1001))
        await session.commit()

    direct_calls: list[list[int]] = []

    async def direct(_group_id, post_ids):
        direct_calls.append(list(post_ids))
        return {
            1001: {
                "id": 1001,
                "date": int(NOW.timestamp()),
                "text": "Совсем другая публикация\n20 июля 19:00\nДругое место",
            }
        }

    async def wall(_group_id, *, limit):
        assert limit == 1000
        return [
            {
                "id": 2001,
                "date": int(NOW.timestamp()),
                "text": "Событие 1\n20 июля 19:00\nПлощадка",
            }
        ]

    monkeypatch.setattr(smb, "_vk_raw_posts_rate_limited", direct)
    monkeypatch.setattr(smb, "_vk_wall_scan_rate_limited", wall)
    result = await smb.resolve_owned_vk_publications_batch(db, now_utc=NOW)

    assert direct_calls == [[1001]]
    assert result["published"] == 1
    assert result["changed_id"] == 1
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT live_post_id, match_method, status FROM event_publication WHERE event_id=1"
            )
        ).fetchone()
    assert tuple(row) == (2001, "wall_scan", "published")
    await db.close()


@pytest.mark.asyncio
async def test_telegram_requires_only_dedicated_bundle_and_never_borrows_other_sessions(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(_event(1, tg_post_id=777))
        await session.commit()

    called = False

    async def forbidden_fetcher(_targets):
        nonlocal called
        called = True
        return {}

    monkeypatch.setenv("ENABLE_SOCIAL_METRICS_BATCH", "1")
    monkeypatch.setenv("SOCIAL_METRICS_VK_ENABLED", "0")
    monkeypatch.setenv("SOCIAL_METRICS_TG_ENABLED", "1")
    monkeypatch.delenv("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR", raising=False)
    monkeypatch.setenv("SOCIAL_METRICS_TG_AUTH_BUNDLE", "old-generic-must-not-be-used")
    monkeypatch.setenv("TELEGRAM_AUTH_BUNDLE_E2E", "must-not-be-used")
    monkeypatch.setenv("TELEGRAM_AUTH_BUNDLE_S22", "must-not-be-used")

    result = await smb.run_social_metrics_batch(
        db,
        now_utc=NOW,
        tg_fetcher=forbidden_fetcher,
        resolve_vk=False,
    )

    assert called is False
    assert result["telegram"] == {
        "enabled": False,
        "reason": "missing_dedicated_bundle",
        "targets_ready": 1,
    }
    await db.close()


@pytest.mark.asyncio
async def test_telegram_batch_preserves_null_zero_forwards_and_reactions(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(_event(1, tg_post_id=778))
        await session.commit()

    post_ts = int(NOW.timestamp()) - 90 * 60

    async def fetcher(targets):
        assert [(target.publisher_id, target.post_id) for target in targets] == [("kldevents", 778)]
        return {
            ("kldevents", 778): smb.MetricPayload(
                post_id=778,
                post_ts=post_ts,
                views=120,
                likes=0,
                comments=None,
                shares=4,
                reactions={},
            )
        }

    bundle = base64.urlsafe_b64encode(json.dumps({"session": "placeholder"}).encode()).decode()
    monkeypatch.setenv("ENABLE_SOCIAL_METRICS_BATCH", "1")
    monkeypatch.setenv("SOCIAL_METRICS_VK_ENABLED", "0")
    monkeypatch.setenv("SOCIAL_METRICS_TG_ENABLED", "1")
    monkeypatch.setenv("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR", bundle)
    result = await smb.run_social_metrics_batch(
        db,
        now_utc=NOW,
        tg_fetcher=fetcher,
        resolve_vk=False,
    )

    assert result["collected"] == 1
    async with db.raw_conn() as conn:
        snapshot = await (
            await conn.execute(
                """
                SELECT likes, comments, shares, reactions_json
                FROM social_metric_snapshot
                WHERE platform='telegram' AND post_id=778
                """
            )
        ).fetchone()
        legacy = await (
            await conn.execute(
                """
                SELECT likes, comments, forwards, reactions_json
                FROM telegram_post_metric WHERE message_id=778
                """
            )
        ).fetchone()
    assert tuple(snapshot[:3]) == (0, None, 4)
    assert json.loads(snapshot[3]) == {}
    assert tuple(legacy[:3]) == (0, None, 4)
    assert json.loads(legacy[3]) == {}

    called_again = False

    async def too_early_fetcher(_targets):
        nonlocal called_again
        called_again = True
        return {}

    repeat = await smb.run_social_metrics_batch(
        db,
        now_utc=NOW + timedelta(minutes=30),
        tg_fetcher=too_early_fetcher,
        resolve_vk=False,
    )
    assert called_again is False
    assert repeat["telegram"] == {"enabled": True, "targets": 0}
    await db.close()


@pytest.mark.asyncio
async def test_owned_telegram_targets_use_exact_event_forward_ledgers_only(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    message_ts = int(NOW.timestamp()) - 90 * 60
    event = _event(1, tg_post_id=778)
    event.tg_event_post_url = "https://t.me/c/3954607218/778"
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
    async with db.raw_conn() as conn:
        await conn.execute("INSERT INTO telegram_source(username, title, enabled) VALUES('kldevents','Events',1)")
        source_id = int((await (await conn.execute("SELECT id FROM telegram_source WHERE username='kldevents'")).fetchone())[0])
        await conn.execute(
            """
            INSERT INTO telegram_post_metric(
                source_id,message_id,age_day,source_url,message_ts,collected_ts,views,likes
            ) VALUES(?,778,0,'https://t.me/kldevents/778',?,?,10,1)
            """,
            (source_id, message_ts, message_ts + 60),
        )
        await conn.execute("INSERT INTO promo_campaign(id,title,status) VALUES(1,'test','active')")
        await conn.execute(
            "INSERT INTO promo_activity(id,campaign_id,surface,enabled) VALUES(1,1,'tg_repost',1)"
        )
        await conn.execute(
            """
            INSERT INTO promo_exposure(
                campaign_id,activity_id,event_id,surface,placement_kind,publish_status,
                published_at,details_json
            ) VALUES(1,1,1,'tg_repost','rolling_window_repost','TG_FORWARDED',?,?)
            """,
            (
                (NOW - timedelta(minutes=80)).isoformat(),
                json.dumps(
                    {
                        "source_url": "https://t.me/c/3954607218/778",
                        "target_url": "https://t.me/kenigevents/900",
                    }
                ),
            ),
        )
        await conn.execute(
            """
            INSERT INTO promo_exposure(
                campaign_id,activity_id,event_id,surface,placement_kind,publish_status,
                published_at,details_json
            ) VALUES(1,1,1,'tg_repost','rolling_window_repost','TG_FORWARDED',?,?)
            """,
            (
                NOW.isoformat(),
                json.dumps(
                    {
                        "source_url": "https://t.me/c/3954607218/779",
                        "target_url": "https://t.me/kenigevents/998",
                    }
                ),
            ),
        )
        # A channel message with no exact kldevents -> kenigevents event-forward
        # contract must not enter the metric target set.
        await conn.execute(
            """
            INSERT INTO promo_exposure(
                campaign_id,activity_id,event_id,surface,placement_kind,publish_status,
                published_at,details_json
            ) VALUES(1,1,1,'tg_repost','rolling_window_repost','TG_FORWARDED',?,?)
            """,
            (
                NOW.isoformat(),
                json.dumps(
                    {
                        "source_url": "https://t.me/another_channel/5",
                        "target_url": "https://t.me/kenigevents/999",
                    }
                ),
            ),
        )
        await conn.execute(
            """
            INSERT INTO poll_repost_run(
                profile_key,run_key,status,target_event_date,poll_chat_id,poll_message_id,
                chosen_event_id,reply_message_id,forwarded_message_id,updated_at
            ) VALUES('popular','run-1','forwarded','2026-07-20','@kenigevents',800,1,850,901,?)
            """,
            ((NOW - timedelta(minutes=70)).isoformat(),),
        )
        await conn.commit()

    targets = await smb.load_social_post_targets(db, now_utc=NOW)
    own_tg = [target for target in targets if target.platform == "telegram" and target.owned]
    assert [(target.publisher_id, target.post_id, target.publication_kind) for target in own_tg] == [
        ("kenigevents", 900, "event_forward"),
        ("kenigevents", 901, "event_forward"),
        ("kldevents", 778, "event_announcement"),
    ]
    assert next(target for target in own_tg if target.post_id == 778).post_ts == message_ts
    assert {target.post_id for target in own_tg}.isdisjoint({800, 850, 998, 999})
    await db.close()


@pytest.mark.asyncio
async def test_official_vk_targets_use_only_exact_single_event_ledgers(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event = _event(1)
    event.vk_repost_url = "https://vk.com/wall-231828790_501"
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
    async with db.raw_conn() as conn:
        await conn.execute("INSERT INTO promo_campaign(id,title,status) VALUES(1,'test','active')")
        await conn.execute("INSERT INTO promo_activity(id,campaign_id,surface,enabled) VALUES(1,1,'vk_repost',1)")
        exposures = [
            ("vk_repost", "PUBLISHED_MAIN", {"target_group_id": 231828790, "target_url": "https://vk.com/wall-231828790_502"}),
            ("vk_story", "PUBLISHED_MAIN", {"target_group_id": 231828790, "target_url": "https://vk.com/wall-231828790_503"}),
            ("vk_repost", "FAILED", {"target_group_id": 231828790, "target_url": "https://vk.com/wall-231828790_504"}),
            ("vk_repost", "PUBLISHED_MAIN", {"target_group_id": 231920894, "target_url": "https://vk.com/wall-231920894_505"}),
        ]
        for surface, status, details in exposures:
            await conn.execute(
                """
                INSERT INTO promo_exposure(
                    campaign_id,activity_id,event_id,surface,placement_kind,publish_status,
                    published_at,details_json,public_targets_json
                ) VALUES(1,1,1,?,'rolling_window_repost',?,?,?,'[]')
                """,
                (surface, status, NOW.isoformat(), json.dumps(details)),
            )
        await conn.commit()

    monkeypatch.setenv("SOCIAL_METRICS_VK_OFFICIAL_GROUP_ID", "231828790")
    targets = await smb.load_social_post_targets(db, now_utc=NOW)
    official = [target for target in targets if target.platform == "vk" and target.publisher_id == "231828790"]
    assert [(target.post_id, target.publication_kind, target.owned) for target in official] == [
        (501, "event_forward", True),
        (502, "event_forward", True),
    ]
    await db.close()


@pytest.mark.asyncio
async def test_human_pause_uses_bounded_config_without_fake_actions(monkeypatch):
    delays: list[float] = []

    async def fake_sleep(seconds: float):
        delays.append(seconds)

    monkeypatch.setattr(smb.asyncio, "sleep", fake_sleep)
    monkeypatch.setenv("SOCIAL_METRICS_TG_BETWEEN_REQUESTS_SECONDS", "1.25,1.25")
    actual = await smb._human_pause("SOCIAL_METRICS_TG_BETWEEN_REQUESTS_SECONDS", (2.0, 5.0))
    assert actual == 1.25
    assert delays == [1.25]


@pytest.mark.asyncio
async def test_vk_requests_are_chunked_by_one_hundred(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    post_ts = int(NOW.timestamp()) - 90 * 60
    events = [_event(index, vk_post_id=1000 + index) for index in range(1, 102)]
    async with db.get_session() as session:
        session.add_all(events)
        await session.commit()
    async with db.raw_conn() as conn:
        await conn.executemany(
            """
            INSERT INTO event_publication(
                event_id, platform, target, stored_url, live_url, stored_post_id,
                live_post_id, status
            ) VALUES(?,'vk','klgdevents',?,?,?,?, 'published')
            """,
            [
                (
                    index,
                    f"https://vk.com/wall-231920894_{1000 + index}",
                    f"https://vk.com/wall-231920894_{2000 + index}",
                    1000 + index,
                    2000 + index,
                )
                for index in range(1, 102)
            ],
        )
        await conn.executemany(
            """
            INSERT INTO vk_post_metric(
                group_id, post_id, age_day, source_url, post_ts, collected_ts, views, likes
            ) VALUES(231920894,?,0,?,?,?,1,0)
            """,
            [
                (post_id, f"https://vk.com/wall-231920894_{post_id}", post_ts, post_ts + 1)
                for post_id in range(2001, 2102)
            ],
        )
        await conn.commit()

    chunk_sizes: list[int] = []

    async def fetcher(_group_id: int, post_ids):
        chunk_sizes.append(len(post_ids))
        return {
            int(post_id): smb.MetricPayload(
                post_id=int(post_id),
                post_ts=post_ts,
                views=100,
                likes=2,
                comments=0,
                shares=0,
            )
            for post_id in post_ids
        }

    monkeypatch.setenv("ENABLE_SOCIAL_METRICS_BATCH", "1")
    monkeypatch.setenv("SOCIAL_METRICS_VK_ENABLED", "1")
    monkeypatch.setenv("SOCIAL_METRICS_TG_ENABLED", "0")
    monkeypatch.setenv("SOCIAL_METRICS_VK_BATCH_SIZE", "100")
    monkeypatch.setenv("SOCIAL_METRICS_VK_BATCH_PAUSE_MS", "0")

    result = await smb.run_social_metrics_batch(
        db,
        now_utc=NOW,
        vk_fetcher=fetcher,
        resolve_vk=False,
    )

    assert chunk_sizes == [100, 1]
    assert result["chunks"] == 2
    assert result["collected"] == 101
    await db.close()


def _load_exporter():
    root = Path(os.getenv("EVENTS_BOT_ROOT") or Path(__file__).resolve().parents[1])
    path = root / "site" / "scripts" / "export-production-preview-data.py"
    spec = importlib.util.spec_from_file_location("popular_exporter", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exporter_reads_telegram_forwards_and_builds_explainable_signals(tmp_path):
    exporter = _load_exporter()
    con = sqlite3.connect(tmp_path / "export.sqlite")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        create table telegram_post_metric(
            source_url text, likes integer, views integer, forwards integer, collected_ts integer
        );
        create table vk_post_metric(
            source_url text, likes integer, views integer, reposts integer, collected_ts integer
        );
        create table social_metric_snapshot(
            platform text, publisher_id text, post_id integer, age_bucket text,
            source_url text, views integer, likes integer, comments integer,
            shares integer, collected_ts integer, status text
        );
        insert into telegram_post_metric values('https://t.me/kldevents/7', 12, 1000, 8, 10);
        insert into social_metric_snapshot values('telegram','kldevents',7,'1h','https://t.me/kldevents/7',100,2,0,1,19,'collected');
        insert into social_metric_snapshot values('telegram','kldevents',7,'6h','https://t.me/kldevents/7',500,12,4,8,20,'collected');
        insert into social_metric_snapshot values('vk','999',8,'6h','https://vk.com/wall-999_8',200,4,0,1,20,'collected');
        """
    )
    urls = ["https://t.me/kldevents/7", "https://vk.com/wall-999_8"]

    assert exporter.source_metrics(con, urls) == (16, 700, 9, 2)
    reasons, score = exporter.popularity_signals(con, urls)
    assert reasons == ["fast_growth", "frequently_shared", "discussed", "multi_source"]
    assert score == 8.5
    con.close()


def test_exporter_maps_only_exact_managed_event_forwards_and_collapses_owned_family(tmp_path):
    exporter = _load_exporter()
    con = sqlite3.connect(tmp_path / "forward-export.sqlite")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        create table event_source(event_id integer, source_url text);
        create table event_publication(
            event_id integer, stored_url text, live_url text, status text
        );
        create table promo_exposure(
            event_id integer, surface text, publish_status text,
            details_json text, public_targets_json text
        );
        create table poll_repost_run(
            chosen_event_id integer, status text, poll_chat_id text,
            forwarded_message_id integer
        );
        create table social_metric_snapshot(
            platform text, publisher_id text, post_id integer, age_bucket text,
            source_url text, views integer, likes integer, comments integer,
            shares integer, collected_ts integer, status text
        );
        insert into promo_exposure values(
            1,'tg_repost','TG_FORWARDED',
            '{"source_url":"https://t.me/kldevents/7","target_url":"https://t.me/kenigevents/70"}',
            '[]'
        );
        insert into promo_exposure values(
            1,'tg_repost','TG_FORWARDED',
            '{"source_url":"https://t.me/other/8","target_url":"https://t.me/other/80"}',
            '[]'
        );
        insert into promo_exposure values(
            1,'vk_repost','PUBLISHED_MAIN',
            '{"target_group_id":231828790,"target_url":"https://vk.com/wall-231828790_170"}',
            '[]'
        );
        insert into promo_exposure values(
            1,'vk_story','PUBLISHED_MAIN',
            '{"target_group_id":231828790,"target_url":"https://vk.com/wall-231828790_171"}',
            '[]'
        );
        insert into poll_repost_run values(1,'forwarded','@kenigevents',71);
        insert into poll_repost_run values(1,'forwarded','@other',72);
        insert into social_metric_snapshot values(
            'telegram','kldevents',7,'6h','https://t.me/kldevents/7',500,12,4,8,20,'collected'
        );
        insert into social_metric_snapshot values(
            'telegram','kenigevents',70,'6h','https://t.me/kenigevents/70',450,10,3,7,20,'collected'
        );
        insert into social_metric_snapshot values(
            'vk','231828790',170,'6h','https://vk.com/wall-231828790_170',700,15,5,9,20,'collected'
        );
        """
    )
    row = {
        "source_post_url": None,
        "source_vk_post_url": None,
        "tg_event_post_url": "https://t.me/kldevents/7",
        "vk_repost_url": "https://vk.com/wall-231828790_170",
    }
    urls = exporter.collect_source_urls(con, 1, row)
    assert urls == [
        "https://t.me/kldevents/7",
        "https://vk.com/wall-231828790_170",
        "https://t.me/kenigevents/70",
        "https://t.me/kenigevents/71",
    ]
    assert exporter.source_metrics(con, urls) == (15, 700, 9, 1)
    reasons, _score = exporter.popularity_signals(con, urls)
    assert "multi_source" not in reasons
    con.close()


def test_scheduler_registers_one_batch_job_not_one_job_per_publication(monkeypatch):
    import scheduling

    class DummyExecutor:
        pass

    class DummyJob:
        def __init__(self, job_id: str) -> None:
            self.id = job_id
            self.next_run_time = None

    class DummyScheduler:
        def __init__(self, **_kwargs):
            self.jobs = []

        def configure(self, **_kwargs):
            return None

        def add_job(self, _func, _trigger, *, id, **_kwargs):
            self.jobs.append(DummyJob(id))
            return self.jobs[-1]

        def get_job(self, job_id):
            return next((job for job in self.jobs if job.id == job_id), None)

        def add_listener(self, *_args, **_kwargs):
            return None

        def start(self):
            return None

        def shutdown(self, **_kwargs):
            return None

    monkeypatch.setattr(scheduling, "AsyncIOExecutor", lambda: DummyExecutor())
    monkeypatch.setattr(scheduling, "AsyncIOScheduler", DummyScheduler)
    monkeypatch.setattr(scheduling, "_scheduler", None)
    monkeypatch.setenv("ENABLE_CORE_SCHEDULERS", "0")
    monkeypatch.setenv("ENABLE_SOCIAL_METRICS_KAGGLE", "1")
    monkeypatch.delenv("ENABLE_NIGHTLY_PAGE_SYNC", raising=False)
    monkeypatch.setitem(
        sys.modules,
        "handlers.pinned_button",
        SimpleNamespace(pinned_button_scheduler=lambda *_args, **_kwargs: None),
    )

    scheduler = scheduling.startup(
        db=SimpleNamespace(engine=object()),
        bot=None,
        vk_scheduler=lambda *_args, **_kwargs: None,
        vk_poll_scheduler=lambda *_args, **_kwargs: None,
        vk_crawl_cron=lambda *_args, **_kwargs: None,
        cleanup_scheduler=lambda *_args, **_kwargs: None,
        partner_notification_scheduler=lambda *_args, **_kwargs: None,
        nightly_page_sync=lambda *_args, **_kwargs: None,
        rebuild_fest_nav_if_changed=lambda *_args, **_kwargs: None,
    )
    try:
        assert [job.id for job in scheduler.jobs].count("social_metrics_batch") == 1
    finally:
        scheduling.cleanup()
