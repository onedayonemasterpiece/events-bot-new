import asyncio
import json
import sqlite3
from types import SimpleNamespace

import pytest

from db import Database
from guide_excursions import service


@pytest.mark.asyncio
@pytest.mark.parametrize("path,stage,fail", [
    ("download", "occurrence", False), ("download", "profile", False),
    ("download", "occurrence", True), ("download", "profile", True),
    ("local", "scan", False), ("local", "occurrence", False),
    ("local", "profile", False), ("local", "occurrence", True),
    ("local", "profile", True),
])
async def test_guide_provider_wait_releases_writer_and_preserves_reimport(
    tmp_path, monkeypatch, path, stage, fail,
):
    db_path = tmp_path / "guide.sqlite"
    db = Database(str(db_path))
    await db.init()
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE concurrent_write_probe(value TEXT)")
    occurrence = {
        "canonical_title": "Прогулка по Литовскому валу", "date": "2099-11-20",
        "time": "10:00", "city": "Калининград", "status": "available",
        "meeting_point": "Литовский вал", "digest_eligible": True,
    }
    post = {
        "message_id": 12345, "source_url": "https://t.me/ruin_keepers/12345",
        "text": "20 ноября 2099 в 10:00 прогулка по Литовскому валу.",
        "post_kind": "announce_single", "prefilter_passed": True,
        "llm_status": "ok", "occurrences": [occurrence],
    }
    payload = {"run_id": "transaction-control", "sources": [{
        "platform": "telegram", "username": "ruin_keepers",
        "source_status": "ok", "posts": [post],
    }]}
    results = tmp_path / "results.json"
    results.write_text(json.dumps(payload))
    writer_errors = []
    checked = []

    def write_from_another_connection():
        try:
            with sqlite3.connect(db_path, timeout=0.05) as conn:
                conn.execute("INSERT INTO concurrent_write_probe VALUES (?)", (stage,))
        except sqlite3.OperationalError as exc:
            writer_errors.append(str(exc))

    async def boundary(name):
        if name == stage:
            checked.append(name)
            await asyncio.to_thread(write_from_another_connection)
            if fail:
                raise RuntimeError("fixture provider failure")

    async def enrich(rows):
        await boundary("occurrence")
        return {row["occurrence_id"]: {"main_hook": "Архитектура и история вала"} for row in rows}

    async def profiles(rows):
        await boundary("profile")
        return {row["profile_id"]: {"summary_short": "Исторические прогулки"} for row in rows}

    async def scan(*args, **kwargs):
        await boundary("scan")
        return SimpleNamespace(source_title="Хранители руин", about_text="", about_links=[]), [
            service._guide_post_from_payload(post, source_username="ruin_keepers", source_title="Хранители руин")
        ]

    monkeypatch.setattr(service, "apply_occurrence_enrichment", enrich)
    monkeypatch.setattr(service, "apply_profile_enrichment", profiles)
    monkeypatch.setattr(service, "scan_source_posts", scan)
    monkeypatch.setattr(service, "_should_prefilter", lambda *args: True)
    try:
        for _ in range(2):
            if path == "download":
                metrics, errors, report = await service._import_results_file_inner(db, results_path=str(results))
                source_report = report["sources"][0]
            else:
                async with db.raw_conn() as conn:
                    await service._enable_row_factory(conn)
                    await service.seed_guide_sources(conn)
                    await conn.commit()
                    rows = await service._get_enabled_sources(conn)
                    row = next(row for row in rows if row["username"] == "ruin_keepers")
                    parsed = service._coerce_occurrence_from_payload(
                        occurrence, post=service._guide_post_from_payload(post, source_username="ruin_keepers", source_title="Хранители руин"), source_row=row,
                    )
                    monkeypatch.setattr(service, "parse_post_occurrences", lambda **kwargs: [parsed])
                    # Model the pending writes from the previous source in the same local batch.
                    await conn.execute("INSERT INTO concurrent_write_probe VALUES ('previous-source')")
                    metrics, source_report = await service._scan_and_import_source(
                        conn, None, source_row=row, limit=1, days_back=1,
                    )
                    await conn.commit()
            assert source_report["source_status"] == ("partial" if fail else "ok")
            assert bool(source_report["errors"]) is fail
        assert checked == [stage, stage]
        assert writer_errors == []
        with sqlite3.connect(db_path) as conn:
            assert conn.execute("SELECT COUNT(*) FROM guide_occurrence").fetchone()[0] == 1
            assert conn.execute("SELECT COUNT(*) FROM guide_occurrence_source").fetchone()[0] == 1
            facts = json.loads(conn.execute("SELECT fact_pack_json FROM guide_occurrence").fetchone()[0])
            if not (fail and stage == "occurrence"):
                assert facts["main_hook"] == "Архитектура и история вала"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_authoritative_empty_roles_clear_old_roles_on_normal_reimport(tmp_path, monkeypatch):
    db = Database(str(tmp_path / 'roles.sqlite'))
    await db.init()
    async def no_enrichment(rows): return {}
    monkeypatch.setattr(service, 'apply_occurrence_enrichment', no_enrichment)
    monkeypatch.setattr(service, 'apply_profile_enrichment', no_enrichment)
    occurrence = {'canonical_title': 'Архитектура Литовского вала', 'date': '2099-11-20',
        'status': 'available', 'digest_eligible': True,
        'guide_names': ['Гид'], 'organizer_names': ['Старый организатор']}
    payload = {'run_id': 'role-update', 'sources': [{'platform': 'telegram',
        'username': 'ruin_keepers', 'source_status': 'ok', 'posts': [{
            'message_id': 12345, 'source_url': 'https://t.me/ruin_keepers/12345',
            'text': '20 ноября 2099. Прогулка.', 'post_kind': 'announce_single',
            'prefilter_passed': True, 'llm_status': 'ok', 'occurrences': [occurrence]}]}]}
    results = tmp_path / 'results.json'
    try:
        for guides, organizers in ((['Гид'], ['Старый организатор']), ([], [])):
            occurrence['guide_names'] = guides
            occurrence['organizer_names'] = organizers
            results.write_text(json.dumps(payload))
            await service._import_results_file_inner(db, results_path=str(results))
        async with db.raw_conn() as conn:
            row = await (await conn.execute('SELECT guide_names_json,organizer_names_json FROM guide_occurrence')).fetchone()
        assert json.loads(row[0]) == []
        assert json.loads(row[1]) == []
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('eligible,reason,expected', [
    (False, 'Предложение для организованной группы, а не индивидуальных участников.', 0),
    (False, None, 0),
    (True, None, 1),
    (True, 'sold_out', 0),
])
async def test_normal_import_preserves_model_exclusion_despite_rich_facts(
    tmp_path, monkeypatch, eligible, reason, expected,
):
    # OSQ post 982 supplied an explicit negative model decision alongside
    # a date, city, summary and booking phone. Import must not promote it.
    async def no_enrichment(_rows):
        return {}

    monkeypatch.setattr(service, 'apply_occurrence_enrichment', no_enrichment)
    monkeypatch.setattr(service, 'apply_profile_enrichment', no_enrichment)
    occurrence = {
        'canonical_title': 'Завод OSQ Group', 'date': '2099-11-19',
        'city': 'Калининград', 'status': 'available',
        'summary_one_liner': 'Посещение производства для организованной группы старшеклассников.',
        'booking_text': '+7 962 255-54-91 Татьяна',
        'availability_mode': 'scheduled_public',
        'digest_eligible': eligible, 'digest_eligibility_reason': reason,
    }
    payload = {'run_id': 'eligibility-import-fixture', 'sources': [{
        'platform': 'telegram', 'username': 'excursions_profitour',
        'source_status': 'ok', 'posts': [{
            'message_id': 982, 'source_url': 'https://t.me/excursions_profitour/982',
            'text': '19 ноября для организованной группы старшеклассников: завод OSQ Group.',
            'prefilter_passed': True, 'post_kind': 'announce_single',
            'llm_status': 'ok', 'occurrences': [occurrence],
        }],
    }]}
    result = tmp_path / 'results.json'
    result.write_text(json.dumps(payload, ensure_ascii=False))
    db = Database(str(tmp_path / 'guide.sqlite'))
    await db.init()
    try:
        _, errors, _ = await service._import_results_file_inner(db, results_path=str(result))
        assert not errors
        async with db.raw_conn() as conn:
            row = await (await conn.execute(
                'SELECT digest_eligible, digest_eligibility_reason FROM guide_occurrence'
            )).fetchone()
        assert row[0] == expected
        if reason:
            assert row[1] == reason
    finally:
        await db.close()
