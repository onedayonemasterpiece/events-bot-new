import asyncio
import json

import aiosqlite

from guide_excursions.seed import seed_guide_sources
from guide_excursions.sources import GuideSourceSpec, canonical_guide_sources


def test_canonical_guide_sources_include_murnikova_channel() -> None:
    sources = canonical_guide_sources()
    by_username = {source.username: source for source in sources}

    assert "murnikovat" in by_username
    source = by_username["murnikovat"]
    assert source.profile_slug == "tatyana-murnikova"
    assert source.display_name == "Татьяна Мурникова"
    assert source.source_kind == "guide_personal"


def test_canonical_guide_sources_include_kaliningradlibrary_channel() -> None:
    sources = canonical_guide_sources()
    by_source = {(source.platform, source.username): source for source in sources}

    assert ("telegram", "kaliningradlibrary") in by_source
    source = by_source[("telegram", "kaliningradlibrary")]
    assert source.profile_slug == "kaliningrad-library"
    assert source.display_name == "Калининградская областная научная библиотека"
    assert source.source_kind == "organization_with_tours"
    assert source.flags == {"organization": True, "mixed_topic": True, "library": True}


def test_canonical_guide_sources_include_progulki_s_katey_channel() -> None:
    sources = canonical_guide_sources()
    by_source = {(source.platform, source.username): source for source in sources}

    assert ("telegram", "progulki_s_katey") in by_source
    source = by_source[("telegram", "progulki_s_katey")]
    assert source.profile_slug == "progulki-s-katey"
    assert source.display_name == "ПРОгулки с Катей"
    assert source.source_kind == "guide_personal"
    assert source.trust_level == "medium"
    assert source.flags == {"mixed_topic": True}


def test_canonical_guide_sources_include_vk_publics() -> None:
    sources = canonical_guide_sources()
    by_source = {(source.platform, source.username): source for source in sources}

    assert ("vk", "balticsyndicate") in by_source
    baltic = by_source[("vk", "balticsyndicate")]
    assert baltic.source_url == "https://vk.ru/balticsyndicate"
    assert baltic.source_kind == "guide_project"
    assert baltic.flags == {"vk_public": True, "mixed_topic": True}

    assert ("vk", "konb39") in by_source
    konb = by_source[("vk", "konb39")]
    assert konb.profile_slug == "kaliningrad-library"
    assert konb.source_url == "https://vk.com/konb39"
    assert konb.flags == {"organization": True, "mixed_topic": True, "library": True, "vk_public": True}

    assert ("vk", "ruin.keepers") in by_source
    ruin = by_source[("vk", "ruin.keepers")]
    assert ruin.profile_slug == "ruin-keepers"
    assert ruin.source_url == "https://vk.com/ruin.keepers"
    assert ruin.source_kind == "organization_with_tours"
    assert ruin.flags == {"organization": True, "vk_public": True}

    assert ("vk", "narodexcursovod") in by_source
    narod = by_source[("vk", "narodexcursovod")]
    assert narod.profile_slug == "narodny-excursovod"
    assert narod.source_url == "https://vk.com/narodexcursovod"
    assert narod.source_kind == "organization_with_tours"
    assert narod.flags == {"organization": True, "vk_public": True, "mixed_topic": True}


def test_canonical_guide_sources_are_normalized_unique_and_sorted() -> None:
    sources = canonical_guide_sources()
    keys = [(source.platform, source.username) for source in sources]

    assert keys == sorted(keys)
    assert len(keys) == len(set(keys))
    assert all(source.username == source.username.lower() and "@" not in source.username for source in sources)
    assert {source.platform for source in sources} == {"telegram", "vk"}


def test_seed_guide_sources_keeps_platforms_and_merges_profile_links(tmp_path) -> None:
    async def _run() -> None:
        async with aiosqlite.connect(tmp_path / "guide.sqlite") as conn:
            await conn.execute(
                """
                CREATE TABLE guide_profile(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT UNIQUE,
                    profile_kind TEXT,
                    display_name TEXT,
                    marketing_name TEXT,
                    source_links_json TEXT,
                    base_region TEXT,
                    first_seen_at TEXT,
                    last_seen_at TEXT
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE guide_source(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT,
                    username TEXT,
                    primary_profile_id INTEGER,
                    source_kind TEXT,
                    trust_level TEXT,
                    priority_weight REAL,
                    enabled INTEGER,
                    flags_json TEXT,
                    base_region TEXT,
                    added_via TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )
            await seed_guide_sources(
                conn,
                sources=(
                    GuideSourceSpec(
                        username="librarytg",
                        profile_slug="library",
                        profile_kind="organization",
                        display_name="Library",
                        marketing_name="Library",
                        source_kind="organization_with_tours",
                        trust_level="medium",
                    ),
                    GuideSourceSpec(
                        username="libraryvk",
                        profile_slug="library",
                        profile_kind="organization",
                        display_name="Library",
                        marketing_name="Library",
                        source_kind="organization_with_tours",
                        trust_level="medium",
                        platform="vk",
                        source_url="https://vk.com/libraryvk",
                    ),
                ),
            )
            await conn.commit()
            cur = await conn.execute("SELECT platform, username FROM guide_source ORDER BY platform, username")
            assert await cur.fetchall() == [("telegram", "librarytg"), ("vk", "libraryvk")]
            cur = await conn.execute("SELECT source_links_json FROM guide_profile WHERE slug='library'")
            row = await cur.fetchone()

        links = json.loads(row[0])
        assert links == ["https://t.me/librarytg", "https://vk.com/libraryvk"]

    asyncio.run(_run())
