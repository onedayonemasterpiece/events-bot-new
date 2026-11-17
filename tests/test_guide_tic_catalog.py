from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import aiosqlite
import pytest

from guide_excursions import tic_catalog as tic

URL = tic.ORIGIN + "/excursions/route/"
INDEX = '<a class="excursions-item__link" href="/excursions/route/"><h3>Программа: По аллеям</h3></a>'
DETAIL = """
<div id="excursion-info"><h2 class="excursion-info__title">Программа: По аллеям</h2>
<div class="content-poster__table-row"><span class="vi-table__title">Дополнительно</span>
<span class="vi-table__description">Каштановая аллея ведёт от мельницы к старой плотине.
Стоимость прогулки — пятьсот рублей.<br>
Экскурсовод: <a href="/pro/gidy-i-ekskursovody/guide/">Петрова Анна</a><br>
График экскурсий: 12 сентября, 11:00. Предыдущая специальная программа включала посещение замка.
</span></div></div><div class="price">Бесплатно</div>"""
ROW = {"id": 1, "canonical_title": "По аллеям", "guide_names_json": '["Анна Петрова"]',
       "organizer_names_json": '["Программа"]'}


class TestDB:
    __test__ = False

    def __init__(self, path):
        self.path = path

    @asynccontextmanager
    async def raw_conn(self):
        async with aiosqlite.connect(self.path) as conn:
            yield conn


@pytest.mark.asyncio
async def test_only_confirmed_route_author_and_stable_excerpt_reach_offline_consumer(tmp_path):
    db = TestDB(tmp_path / "routes.sqlite")
    async with db.raw_conn() as conn:
        await conn.execute("CREATE TABLE guide_occurrence(canonical_title, guide_names_json, organizer_names_json, fact_pack_json, date, digest_eligible, status)")
        await conn.execute("INSERT INTO guide_occurrence VALUES(?,?,?,NULL,?,1,'scheduled')",
                           (ROW["canonical_title"], ROW["guide_names_json"], ROW["organizer_names_json"],
                            (date.today() + timedelta(days=2)).isoformat()))
        await conn.commit()
        assert await tic.load_tic_background(conn, [ROW]) == {}

    geo = tic.ORIGIN + "/excursions/search/other-city/"
    sitemap = f"<urlset><url><loc>{geo}</loc></url><url><loc>https://other.example/test</loc></url></urlset>"
    pages = {**dict.fromkeys(tic.INDEXES, INDEX.replace("По аллеям", "Иная тема").replace("/route/", "/other/")),
             tic.SITEMAP: sitemap, geo: INDEX, URL: DETAIL}

    async def get(url):
        return pages[url]

    started = datetime.now(timezone.utc)
    stats = await tic.refresh_tic_catalog(db, fetch=get, now=started)
    assert (stats["listed"], stats["title_candidates"], stats["confirmed"]) == (2, 1, 1)
    assert stats["indexes_read"] == stats["indexes_discovered"] == 3
    await tic.refresh_tic_catalog(db, fetch=get)
    async with db.raw_conn() as conn:
        result = await tic.load_tic_background(conn, [
            ROW, dict(ROW, id=2, guide_names_json='["Вера Петрова"]'),
            dict(ROW, id=3, organizer_names_json='["Другой проект"]'),
            dict(ROW, id=4, fact_pack={"guide_names": []}),
            dict(ROW, id=5, fact_pack={"organizer_names": []}),
        ])
        assert set(result) == {1}
        assert result[1]["url"] == URL
        assert result[1]["text"] == "Каштановая аллея ведёт от мельницы к старой плотине."
        assert result[1]["retrieved_at"]
        assert (await conn.execute_fetchall("SELECT COUNT(*) FROM guide_web_route_evidence"))[0][0] == 1

    async def failed(url):
        if url == URL:
            raise TimeoutError()
        return pages[url]

    assert (await tic.refresh_tic_catalog(db, fetch=failed, wanted_occurrences=[ROW], now=started+timedelta(days=1)))["errors"] == 1
    async with db.raw_conn() as conn:
        assert 1 in await tic.load_tic_background(conn, [ROW])
        await conn.execute("UPDATE guide_web_route_evidence SET retrieved_at='2000-01-01'")
        await conn.commit()
        assert await tic.load_tic_background(conn, [ROW]) == {}

    await tic.refresh_tic_catalog(db, fetch=get, wanted_occurrences=[ROW], now=started+timedelta(days=2))
    pages[URL] = DETAIL.replace("Петрова Анна", "Петрова Вера")
    assert (await tic.refresh_tic_catalog(db, fetch=get, wanted_occurrences=[ROW], now=started+timedelta(days=3)))["confirmed"] == 0
    async with db.raw_conn() as conn:
        assert await tic.load_tic_background(conn, [ROW]) == {}


def test_discovery_uses_local_cards_and_requires_attributed_guide():
    assert list(tic._listing(INDEX + INDEX.replace('/excursions/route/', 'https://elsewhere.example/excursions/route/'))) == [URL]
    assert tic._detail(DETAIL.replace("Экскурсовод:", "Среди гостей:")) is None
    assert tic._detail(DETAIL.replace('/pro/gidy-i-ekskursovody/guide/', 'https://elsewhere.example/pro/gidy-i-ekskursovody/guide/')) is None
    assert tic._detail(DETAIL.replace('Петрова Анна', 'Петрова')) is None


def test_reviewed_marketing_name_is_scoped_to_tic_profile():
    html = DETAIL.replace("/pro/gidy-i-ekskursovody/guide/", next(iter(tic._PROFILE_ALIASES))).replace("Петрова Анна", "Усманова Шахноза")
    assert set(tic._detail(html)[1]) == {"усманова шахноза", "ноза усманова"}
    assert "ноза усманова" not in tic._detail(DETAIL.replace("Петрова Анна", "Усманова Шахноза"))[1]


@pytest.mark.asyncio
async def test_sparse_site_lookup_respects_durable_retry_after(tmp_path):
    import aiohttp
    db = TestDB(tmp_path / "limits.sqlite")
    calls = []
    async def limited(url):
        calls.append(url)
        raise aiohttp.ClientResponseError(None, (), status=429, headers={"Retry-After": "172800"})

    assert (await tic.refresh_tic_catalog(db, fetch=limited, wanted_occurrences=[]))["indexes_read"] == 0
    assert not calls
    assert (await tic.refresh_tic_catalog(db, fetch=limited, wanted_occurrences=[ROW]))["limited"] == 1
    assert calls == [tic.SITEMAP]
    later = datetime.now(timezone.utc) + timedelta(hours=26)
    assert (await tic.refresh_tic_catalog(db, fetch=limited, wanted_occurrences=[ROW], now=later))["cooldown"] == 1
    assert calls == [tic.SITEMAP]
