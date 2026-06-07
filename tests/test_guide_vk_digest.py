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

    assert text.splitlines()[0] == "Новые экскурсии: 2 выхода, 30 мая и 2 июня"
    assert text.splitlines()[1] == "Совместно с Полюбить Калининград Афиша: https://vk.com/klgdevents"
    assert "Запись: записаться в Telegram · vk.cc/1" in text
    assert "Анонс: vk.cc/2" in text
    assert "Автор: Игорь Селин · https://vk.com/ivsguide" in text
    assert ("utils.getShortLink", "https://t.me/example/10") in calls
    assert ("utils.getShortLink", "https://t.me/source/100") in calls


@pytest.mark.asyncio
async def test_publish_latest_guide_digest_to_vk_uses_issue_items_and_stores_vk_target(tmp_path):
    db = Database(str(tmp_path / "guide.sqlite"))
    media_path = tmp_path / "guide.jpg"
    media_path.write_bytes(b"fake-jpeg")
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
                media_items_json TEXT,
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
            """
            INSERT INTO guide_digest_issue(
                id, family, status, items_json, media_items_json, published_targets_json
            ) VALUES(5,'new_occurrences','published','[10]',?,'{}')
            """,
            (
                (
                    '[{"occurrence_id":10,"media_asset":'
                    f'{{"kind":"photo","path":"{media_path}"}}'
                    "}]"
                ),
            ),
        )
        await conn.commit()

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        return {"response": {"short_url": "https://vk.cc/demo", "key": "demo"}}

    uploaded: list[tuple[str, bytes, str]] = []

    async def fake_upload_vk_photo_bytes(group_id, image_bytes, db=None, bot=None, filename="image.jpg", **kwargs):
        uploaded.append((group_id, image_bytes, filename))
        return "photo-123_1"

    posted: dict[str, object] = {}

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None, carousel=False):
        posted["group_id"] = group_id
        posted["message"] = message
        posted["attachments"] = attachments
        posted["carousel"] = carousel
        return "https://vk.com/wall-123_99"

    result = await publish_latest_guide_digest_to_vk(
        db,
        family="new_occurrences",
        group_id="123",
        vk_api_fn=fake_vk_api,
        post_to_vk_fn=fake_post_to_vk,
        upload_vk_photo_bytes_fn=fake_upload_vk_photo_bytes,
    )

    assert result["published"] is True
    assert posted["group_id"] == "123"
    assert posted["message"].splitlines()[0] == "Новые экскурсии: 1 выход, 30 мая"
    assert posted["message"].splitlines()[1] == "Совместно с Полюбить Калининград Афиша: https://vk.com/klgdevents"
    assert uploaded == [("123", b"fake-jpeg", "guide.jpg")]
    assert posted["attachments"] == ["photo-123_1"]
    assert result["attachments_count"] == 1
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT published_targets_json FROM guide_digest_issue WHERE id=5")
        row = await cur.fetchone()
    assert "vk:uhtykaliningrad" in row[0]
    assert "wall-123_99" in row[0]
    assert "attachments_count" in row[0]
    await db.close()


@pytest.mark.asyncio
async def test_publish_latest_guide_digest_to_vk_uses_hook_carousel_without_media_assets(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "guide.sqlite"))
    from guide_excursions import service

    monkeypatch.setattr(service, "GUIDE_MEDIA_STORE_ROOT", tmp_path / "media")
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
                media_items_json TEXT,
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
            ) VALUES(20,1,'["Guide Name"]','[]','[]','{}','Маршрут','2026-05-30','12:00','Калининград','Описание','Запись','https://t.me/book/1','')
            """
        )
        await conn.execute(
            "INSERT INTO guide_monitor_post(id, text, source_url, post_date) VALUES(1,'text','https://t.me/source/20','2026-05-19')"
        )
        await conn.execute(
            "INSERT INTO guide_occurrence_source(occurrence_id, post_id, role) VALUES(20,1,'primary')"
        )
        await conn.execute(
            """
            INSERT INTO guide_digest_issue(
                id, family, status, items_json, media_items_json, published_targets_json
            ) VALUES(6,'new_occurrences','published','[20]','[]','{}')
            """
        )
        await conn.commit()

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        return {"response": {"short_url": "https://vk.cc/demo", "key": "demo"}}

    async def fake_build_carousel_slides(rows, media_items, seed=0):
        assert len(rows) == 1
        assert media_items == []
        assert seed == 6
        return [b"slide-one", b"slide-two"]

    import guide_excursions.hook_carousel as hook_carousel

    monkeypatch.setattr(hook_carousel, "build_carousel_slides", fake_build_carousel_slides)

    uploaded: list[tuple[str, bytes, str]] = []

    async def fake_upload_vk_photo_bytes(group_id, image_bytes, db=None, bot=None, filename="image.jpg", **kwargs):
        uploaded.append((group_id, image_bytes, filename))
        return f"photo-123_{len(uploaded)}"

    posted: dict[str, object] = {}

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None, carousel=False):
        posted["group_id"] = group_id
        posted["message"] = message
        posted["attachments"] = attachments
        posted["carousel"] = carousel
        return "https://vk.com/wall-123_100"

    result = await publish_latest_guide_digest_to_vk(
        db,
        family="new_occurrences",
        group_id="123",
        vk_api_fn=fake_vk_api,
        post_to_vk_fn=fake_post_to_vk,
        upload_vk_photo_bytes_fn=fake_upload_vk_photo_bytes,
    )

    assert result["published"] is True
    assert posted["carousel"] is True
    assert posted["message"].splitlines()[1] == "Совместно с Полюбить Калининград Афиша: https://vk.com/klgdevents"
    assert posted["attachments"] == ["photo-123_1", "photo-123_2"]
    assert result["attachments_count"] == 2
    assert uploaded == [
        ("123", b"slide-one", "slide_0.jpg"),
        ("123", b"slide-two", "slide_1.jpg"),
    ]
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT published_targets_json FROM guide_digest_issue WHERE id=6")
        row = await cur.fetchone()
    assert "vk:uhtykaliningrad" in row[0]
    assert "wall-123_100" in row[0]
    await db.close()


@pytest.mark.asyncio
async def test_publish_latest_guide_digest_to_vk_fails_closed_when_carousel_upload_incomplete(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "guide.sqlite"))
    from guide_excursions import service

    monkeypatch.setattr(service, "GUIDE_MEDIA_STORE_ROOT", tmp_path / "media")
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
                summary_one_liner TEXT,
                date TEXT,
                time TEXT,
                city TEXT,
                digest_blurb TEXT,
                booking_text TEXT,
                booking_url TEXT,
                channel_url TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                duration_text TEXT,
                meeting_point TEXT,
                route_summary TEXT,
                price_text TEXT,
                status TEXT,
                seats_text TEXT,
                availability_mode TEXT,
                post_kind TEXT,
                group_format TEXT,
                aggregator_only INTEGER DEFAULT 0,
                views INTEGER,
                likes INTEGER
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
                media_items_json TEXT,
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
                audience_fit_json, fact_pack_json, canonical_title, summary_one_liner,
                date, time, city, digest_blurb, booking_text, booking_url, channel_url
            )
            VALUES(30,1,'["Guide Name"]','[]','[]','{}','Экскурсия','Короткий хук','2026-06-30','12:00','Калининград','Описание','Запись','https://t.me/book/1','')
            """
        )
        await conn.execute(
            "INSERT INTO guide_monitor_post(id, text, source_url, post_date) VALUES(1,'text','https://t.me/source/30','2026-06-19')"
        )
        await conn.execute(
            "INSERT INTO guide_occurrence_source(occurrence_id, post_id, role) VALUES(30,1,'primary')"
        )
        await conn.execute(
            "INSERT INTO guide_digest_issue(id, family, status, items_json, media_items_json, published_targets_json) VALUES(6,'new_occurrences','published','[30]','[]','{}')"
        )
        await conn.commit()

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        return {"response": {"short_url": "https://vk.cc/demo", "key": "demo"}}

    async def fake_build_carousel_slides(rows, media_items, seed=0):
        return [b"slide-one", b"slide-two"]

    import guide_excursions.hook_carousel as hook_carousel

    monkeypatch.setattr(hook_carousel, "build_carousel_slides", fake_build_carousel_slides)

    async def fake_upload_vk_photo_bytes(group_id, image_bytes, db=None, bot=None, filename="image.jpg", **kwargs):
        return "photo-123_1" if filename == "slide_0.jpg" else None

    posted = False

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None, carousel=False):
        nonlocal posted
        posted = True
        return "https://vk.com/wall-123_101"

    with pytest.raises(RuntimeError, match="carousel upload failed"):
        await publish_latest_guide_digest_to_vk(
            db,
            family="new_occurrences",
            group_id="123",
            vk_api_fn=fake_vk_api,
            post_to_vk_fn=fake_post_to_vk,
            upload_vk_photo_bytes_fn=fake_upload_vk_photo_bytes,
        )

    assert posted is False
    await db.close()


@pytest.mark.asyncio
async def test_publish_latest_guide_digest_to_vk_repairs_existing_with_recovered_vk_media(
    tmp_path, monkeypatch
):
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
                source_id INTEGER,
                message_id INTEGER,
                text TEXT,
                source_url TEXT,
                post_date TEXT,
                media_refs_json TEXT,
                media_assets_json TEXT,
                last_scanned_at TEXT
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
                media_items_json TEXT,
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
            ) VALUES(1,'balticsyndicate','vk','Baltic','guide_project','{}','','[]',1.0,1)
            """
        )
        await conn.execute(
            """
            INSERT INTO guide_occurrence(
                id, primary_source_id, guide_names_json, organizer_names_json,
                audience_fit_json, fact_pack_json, canonical_title, date, time,
                city, digest_blurb, booking_text, booking_url, channel_url
            ) VALUES(30,1,'["Baltic"]','[]','[]','{}','Река города К','2026-06-06','18:00','Калининград','Описание','Запись','https://vk.com/wall-99453147_1475','')
            """
        )
        await conn.execute(
            """
            INSERT INTO guide_monitor_post(
                id, source_id, message_id, text, source_url, post_date,
                media_refs_json, media_assets_json
            ) VALUES(
                1, 1, 1475, 'text', 'https://vk.com/wall-99453147_1475',
                '2026-06-04 20:01:31', '[]', '[]'
            )
            """
        )
        await conn.execute(
            "INSERT INTO guide_occurrence_source(occurrence_id, post_id, role) VALUES(30,1,'primary')"
        )
        await conn.execute(
            """
            INSERT INTO guide_digest_issue(
                id, family, status, items_json, media_items_json, published_targets_json
            ) VALUES(7,'new_occurrences','published','[30]','[]',?)
            """,
            (
                (
                    '{"vk:uhtykaliningrad":{"message_ids":[33],"text_message_ids":[33],'
                    '"media_message_ids":[33],"post_urls":["https://vk.com/wall-123_33"],'
                    '"group_id":123,"transport":"vk_wall","attachments_count":2}}'
                ),
            ),
        )
        await conn.commit()

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        if method == "utils.resolveScreenName":
            return {"response": {"type": "group", "object_id": 123}}
        if method == "wall.getById":
            assert params["posts"] == "-99453147_1475"
            return {
                "response": [
                    {
                        "id": 1475,
                        "attachments": [
                            {
                                "type": "photo",
                                "photo": {
                                    "owner_id": -99453147,
                                    "id": 555,
                                    "sizes": [
                                        {"width": 100, "height": 100, "url": "https://example.com/s.jpg"},
                                        {"width": 1200, "height": 900, "url": "https://example.com/l.jpg"},
                                    ],
                                },
                            }
                        ],
                    }
                ]
            }
        return {"response": {"short_url": "https://vk.cc/demo", "key": "demo"}}

    import guide_excursions.service as service

    monkeypatch.setattr(service, "_download_guide_vk_media_bytes", lambda url: b"vk-photo")
    monkeypatch.setattr(service, "GUIDE_MEDIA_STORE_ROOT", tmp_path / "media")

    async def fake_build_carousel_slides(rows, media_items, seed=0):
        assert media_items[0]["media_asset"]["path"]
        assert media_items[0]["media_ref"]["url"] == "https://example.com/l.jpg"
        return [b"photo-hook-slide", b"cta-slide"]

    import guide_excursions.hook_carousel as hook_carousel

    monkeypatch.setattr(hook_carousel, "build_carousel_slides", fake_build_carousel_slides)

    uploaded: list[tuple[str, bytes, str]] = []

    async def fake_upload_vk_photo_bytes(group_id, image_bytes, db=None, bot=None, filename="image.jpg", **kwargs):
        uploaded.append((group_id, image_bytes, filename))
        return f"photo-123_{len(uploaded)}"

    edited: dict[str, object] = {}

    async def fake_edit_vk_post(post_url, message, db=None, bot=None, attachments=None, carousel=False):
        edited["post_url"] = post_url
        edited["attachments"] = attachments
        edited["carousel"] = carousel
        return True

    result = await publish_latest_guide_digest_to_vk(
        db,
        family="new_occurrences",
        issue_id=7,
        group_id="123",
        vk_api_fn=fake_vk_api,
        upload_vk_photo_bytes_fn=fake_upload_vk_photo_bytes,
        edit_vk_post_fn=fake_edit_vk_post,
        repair_existing=True,
    )

    assert result["repaired"] is True
    assert result["attachments_count"] == 2
    assert edited["post_url"] == "https://vk.com/wall-123_33"
    assert edited["attachments"] == ["photo-123_1", "photo-123_2"]
    assert edited["carousel"] is True
    assert uploaded == [
        ("123", b"photo-hook-slide", "slide_0.jpg"),
        ("123", b"cta-slide", "slide_1.jpg"),
    ]
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT media_refs_json, media_assets_json FROM guide_monitor_post WHERE id=1"
        )
        post_row = await cur.fetchone()
        cur = await conn.execute("SELECT media_items_json FROM guide_digest_issue WHERE id=7")
        issue_row = await cur.fetchone()
    assert "https://example.com/l.jpg" in post_row[0]
    assert "vk-photo" not in post_row[1]
    assert "balticsyndicate_1475_0_0.jpg" in post_row[1]
    assert "balticsyndicate_1475_0_0.jpg" in issue_row[0]
    await db.close()
