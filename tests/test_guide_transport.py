import json

import pytest

from db import Database
from guide_excursions import service
from guide_excursions.visual_digest import _fetch_rows_by_ids, _modes_for


@pytest.mark.asyncio
async def test_grounded_modes_survive_sparse_imports_without_crossing_routes(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "guide.sqlite"))
    await db.init()

    async def no_enrichment(rows):
        return {}

    monkeypatch.setattr(service, "apply_profile_enrichment", no_enrichment)
    monkeypatch.setattr(service, "apply_occurrence_enrichment", no_enrichment)
    results = tmp_path / "guide.json"

    async def ingest(message_id, day, *, title="Рассвет над Балтикой", modes=(), scope="occurrence", program=None, legacy_claim=None):
        url = f"https://t.me/ruin_keepers/{message_id}"
        quotes = {("walk",): "пешеходный маршрут", ("bus",): "на автобусе",
                  ("bus", "walk"): "автобусно-пешеходная экскурсия"}
        quote = quotes.get(modes, "")
        occurrence = {"canonical_title": title, "date": day, "time": "14:00",
                      "status": "available", "city": "Калининград", "digest_eligible": True,
                      "fact_claims": [{"fact_key": "city", "fact_value": "Калининград"}]}
        if legacy_claim:
            occurrence["fact_claims"].append({"fact_key": "group_format", "fact_value": legacy_claim})
        occurrence["fact_pack"] = {"program": program} if program else {}
        if modes:
            occurrence["group_format"] = "small_group"
            occurrence["fact_pack"]["transport"] = {"modes": list(modes), "scope": scope,
                                                      "quote": quote, "source_block_id": "B1"}
        post = {"message_id": message_id, "post_date": f"2099-01-{message_id:02d}T10:00:00Z",
                "source_url": url, "text": f"{title}. {quote}", "llm_status": "ok",
                "prefilter_passed": True, "occurrences": [occurrence]}
        results.write_text(json.dumps({"sources": [{"platform": "telegram", "username": "ruin_keepers",
                                                   "posts": [post]}]}))
        _, errors, _ = await service._import_results_file_inner(db, results_path=str(results))
        assert not errors
        async with db.raw_conn() as conn:
            await service._enable_row_factory(conn)
            cur = await conn.execute("SELECT id FROM guide_occurrence WHERE canonical_title=? AND date IS ?", (title, day))
            return (await _fetch_rows_by_ids(conn, [(await cur.fetchone())["id"]]))[0]

    try:
        first = await ingest(1, "2099-11-20", modes=("walk",), scope="route")
        assert _modes_for(first) == ["walk"]
        assert _modes_for({"canonical_title": "Ферма, улица Школьная", "group_format": "open"}) == []
        assert _modes_for({"fact_pack": {"group_format": "walking_tour"}}) == ["walk"]
        same = await ingest(2, "2099-11-20")
        assert _modes_for(same) == ["walk"] and same["group_format"] == "small_group"
        later = await ingest(3, "2099-11-28")
        assert _modes_for(later) == ["walk"]
        assert later["transport"]["source_url"] == first["source_post_url"]
        assert later["transport"]["source_block_id"] == "B1"
        assert _modes_for(await ingest(4, "2099-11-28", title="Зелёные истории")) == []
        assert _modes_for(await ingest(5, "2099-11-28", modes=("bus",))) == ["bus"]
        assert _modes_for(await ingest(6, "2099-11-28", modes=("bus", "walk"))) == ["bus", "walk"]
        assert _modes_for(await ingest(7, "2099-11-29", modes=("bus",), scope="route")) == ["bus"]
        assert _modes_for(await ingest(8, "2099-11-30")) == []
        program = "club237389053:2099-11"
        announcement = await ingest(9, None, title="Ноябрьская программа", modes=("walk",), scope="program", program=program)
        member = await ingest(10, "2099-11-22", title="Новый маршрут", program=program)
        assert _modes_for(member) == ["walk"]
        assert member["transport"]["source_url"] == announcement["source_post_url"]
        assert member["transport"]["program_source_url"] == member["source_post_url"]
        assert _modes_for(await ingest(11, "2099-12-01", title="Новый маршрут", program="club237389053:2099-12")) == []
        assert _modes_for(await ingest(12, "2099-11-22", title="Другой проект", program="club7:2099-11")) == []
        legacy = await ingest(13, "2099-11-27", title="Старая запись", legacy_claim="walking_tour")
        assert _modes_for(legacy) == ["walk"]
    finally:
        await db.close()
