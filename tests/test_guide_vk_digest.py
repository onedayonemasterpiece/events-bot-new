import pytest

from db import Database
from guide_excursions.service import build_guide_vk_digest_text
from guide_excursions.service import publish_latest_guide_digest_to_vk


@pytest.mark.asyncio
async def test_build_guide_vk_digest_text_starts_with_count_and_months_and_shortens_tg_links():
    calls: list[tuple[str, str]] = []

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        calls.append((method, params["url"]))
        return {"response": {"short_url": f"https://vk.cc/{len(calls)}", "key": str(len(calls))}}

    text = await build_guide_vk_digest_text(
        [
            {
                "canonical_title": "Прогулка по Амалиенау",
                "date": "2026-05-30",
                "time": "12:00",
                "city": "Калининград",
                "guide_names": ["Анна Иванова"],
                "digest_blurb": "Маршрут про виллы, городские легенды и довоенную планировку.",
                "booking_text": "записаться в Telegram",
                "booking_url": "https://t.me/example/10",
                "source_post_url": "https://t.me/source/100",
                "source_title": "Guide channel",
                "source_platform": "telegram",
            },
            {
                "canonical_title": "Поездка к кирхам",
                "date": "2026-06-02",
                "city": "Гвардейск",
                "source_platform": "vk",
                "source_username": "ivsguide",
                "source_title": "Игорь Селин",
                "source_flags": {"vk_personal_page": True},
                "source_post_url": "https://vk.com/wall123_456",
            },
        ],
        vk_api_fn=fake_vk_api,
    )

    assert text.splitlines()[0] == "Новые экскурсии: 2 выхода на май и июнь"
    assert "Запись: записаться в Telegram · vk.cc/1" in text
    assert "Анонс: vk.cc/2" in text
    assert "Автор: Игорь Селин · https://vk.com/ivsguide" in text
    assert ("utils.getShortLink", "https://t.me/example/10") in calls
    assert ("utils.getShortLink", "https://t.me/source/100") in calls


@pytest.mark.asyncio
async def test_publish_latest_guide_digest_to_vk_uses_issue_items_and_stores_vk_target(tmp_path):
    db = Database(str(tmp_path / "guide.sqlite"))
    async with db.raw_conn() as conn:
        await conn.executescript(
            """
            CREATE TABLE guide_profile(
                id INTEGER PRIMARY KEY,
                display_name TEXT,
                marketing_name TEXT,
                summary_short TEXT,
                facts_rollup_json TEXT
            );
            CREATE TABLE guide_source(
                id INTEGER PRIMARY KEY,
                username TEXT,
                platform TEXT,
                title TEXT,
                source_kind TEXT,
                flags_json TEXT,
                about_text TEXT,
                about_links_json TEXT,
                priority_weight REAL,
                primary_profile_id INTEGER
            );
            CREATE TABLE guide_occurrence(
                id INTEGER PRIMARY KEY,
                primary_source_id INTEGER,
                guide_names_json TEXT,
                organizer_names_json TEXT,
                audience_fit_json TEXT,
                fact_pack_json TEXT,
                canonical_title TEXT,
                date TEXT,
                time TEXT,
                city TEXT,
                digest_blurb TEXT,
                booking_text TEXT,
                booking_url TEXT,
                channel_url TEXT
            );
            CREATE TABLE guide_monitor_post(
                id INTEGER PRIMARY KEY,
                text TEXT,
                source_url TEXT,
                post_date TEXT
            );
            CREATE TABLE guide_occurrence_source(
                occurrence_id INTEGER,
                post_id INTEGER,
                role TEXT
            );
            CREATE TABLE guide_digest_issue(
                id INTEGER PRIMARY KEY,
                family TEXT,
                status TEXT,
                items_json TEXT,
                published_targets_json TEXT
            );
            """
        )
        await conn.execute(
            "INSERT INTO guide_profile(id, display_name, marketing_name, summary_short, facts_rollup_json) VALUES(1,'Guide','Guide','','{}')"
        )
        await conn.execute(
            """
            INSERT INTO guide_source(
                id, username, platform, title, source_kind, flags_json,
                about_text, about_links_json, priority_weight, primary_profile_id
            ) VALUES(1,'guide','telegram','Guide channel','guide_personal','{}','','[]',1.0,1)
            """
        )
        await conn.execute(
            """
            INSERT INTO guide_occurrence(
                id, primary_source_id, guide_names_json, organizer_names_json,
                audience_fit_json, fact_pack_json, canonical_title, date, time,
                city, digest_blurb, booking_text, booking_url, channel_url
            ) VALUES(10,1,'["Guide Name"]','[]','[]','{}','Маршрут','2026-05-30','12:00','Калининград','Описание','Запись','https://t.me/book/1','')
            """
        )
        await conn.execute(
            "INSERT INTO guide_monitor_post(id, text, source_url, post_date) VALUES(1,'text','https://t.me/source/10','2026-05-19')"
        )
        await conn.execute(
            "INSERT INTO guide_occurrence_source(occurrence_id, post_id, role) VALUES(10,1,'primary')"
        )
        await conn.execute(
            "INSERT INTO guide_digest_issue(id, family, status, items_json, published_targets_json) VALUES(5,'new_occurrences','published','[10]','{}')"
        )
        await conn.commit()

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        return {"response": {"short_url": "https://vk.cc/demo", "key": "demo"}}

    posted: dict[str, str] = {}

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None):
        posted["group_id"] = group_id
        posted["message"] = message
        return "https://vk.com/wall-123_99"

    result = await publish_latest_guide_digest_to_vk(
        db,
        family="new_occurrences",
        group_id="123",
        vk_api_fn=fake_vk_api,
        post_to_vk_fn=fake_post_to_vk,
    )

    assert result["published"] is True
    assert posted["group_id"] == "123"
    assert posted["message"].splitlines()[0] == "Новые экскурсии: 1 выход на май"
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT published_targets_json FROM guide_digest_issue WHERE id=5")
        row = await cur.fetchone()
    assert "vk:uhtykaliningrad" in row[0]
    assert "wall-123_99" in row[0]
    await db.close()
