from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

import pytest

from db import Database
from video_announce.cherryflash_excursions import choose_guide_excursion_promo, seats_count


def test_seats_count_requires_positive_explicit_free_places() -> None:
    assert seats_count("4 места") == 4
    assert seats_count("1") == 1
    assert seats_count("запись в резерв") == 0
    assert seats_count("количество мест ограничено") is None


@pytest.mark.asyncio
async def test_choose_guide_excursion_promo_filters_future_personal_avatar_and_repeats(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    con = sqlite3.connect(db.path)
    con.execute(
        "INSERT INTO guide_profile(id, slug, profile_kind, display_name, marketing_name) VALUES(900,'amber_test','person','Amber Fringilla','Amber')"
    )
    con.execute(
        "INSERT INTO guide_source(id, platform, username, title, primary_profile_id, source_kind) VALUES(900,'telegram','amber_test','Путешествия по пРуссии',900,'guide_personal')"
    )
    con.execute(
        "INSERT INTO guide_source(id, platform, username, title, primary_profile_id, source_kind) VALUES(901,'telegram','ruin_test','Хранители руин',900,'organization_with_tours')"
    )
    con.execute(
        """
        INSERT INTO guide_occurrence(id, primary_source_id, source_fingerprint, canonical_title, title_normalized, date, time, status, seats_text, booking_text, booking_url, channel_url, guide_names_json, organizer_names_json, participant_profiles_json)
        VALUES(10,900,'fp10','Поход в Заворотье Кальтхоф','поход', '2026-07-08','11:00','available','3 места','связаться с организатором','https://t.me/amber_fringilla/100','https://t.me/amber_fringilla/100','["Юлия Гришанова"]','[]','["Юлия Гришанова"]')
        """
    )
    con.execute(
        """
        INSERT INTO guide_occurrence(id, primary_source_id, source_fingerprint, canonical_title, title_normalized, date, time, status, seats_text, booking_text, booking_url, channel_url, guide_names_json, organizer_names_json, participant_profiles_json)
        VALUES(11,900,'fp11','Запись в резерв','резерв', '2026-07-08','12:00','available','резерв','Запись в резерв','https://t.me/amber_fringilla','https://t.me/amber_fringilla','["Юлия Гришанова"]','[]','["Юлия Гришанова"]')
        """
    )
    con.execute(
        """
        INSERT INTO guide_occurrence(id, primary_source_id, source_fingerprint, canonical_title, title_normalized, date, time, status, seats_text, booking_text, booking_url, channel_url, guide_names_json, organizer_names_json, participant_profiles_json)
        VALUES(12,901,'fp12','Организационная экскурсия','орг', '2026-07-08','13:00','available','5 мест','',NULL,'https://t.me/ruin_keepers','["Юлия Гришанова"]','[]','["Юлия Гришанова"]')
        """
    )
    con.commit()
    con.close()

    promo = await choose_guide_excursion_promo(
        db,
        now=datetime(2026, 7, 5, 8, 0, tzinfo=timezone.utc),
    )

    assert promo is not None
    assert promo["occurrence_id"] == 10
    assert promo["contact"] == "@amber_fringilla"
    assert 2 <= promo["insert_position"] <= 6

    con = sqlite3.connect(db.path)
    con.execute(
        "INSERT INTO videoannounce_session(id, status, profile_key, selection_params, created_at) VALUES(100,'DONE','popular_review',?, '2026-07-06 00:10:00')",
        (json.dumps({"guide_excursion_promo": {"occurrence_id": 10}}),),
    )
    con.commit(); con.close()
    repeated = await choose_guide_excursion_promo(
        db,
        now=datetime(2026, 7, 6, 8, 0, tzinfo=timezone.utc),
    )
    assert repeated is None
