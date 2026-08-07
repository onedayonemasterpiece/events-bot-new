from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("/data/db.sqlite")
PROFILE_SLUG = "festival-80-stories"
SOURCE_PLATFORM = "web"
SOURCE_USERNAME = "kgd80-amber-combine-jewelry-excursion"
SOURCE_TITLE = "Фестиваль «80 историй о главном»"
FINGERPRINT = "manual:kgd80:amber-combine-jewelry-excursion:2026-08-11T11:00"
TITLE = "Экскурсия на ювелирное производство Калининградского янтарного комбината"
TITLE_NORMALIZED = "экскурсия на ювелирное производство калининградского янтарного комбината"
EVENT_DATE = "2026-08-11"
EVENT_TIME = "11:00"
URL = "https://kgd80.ru/special/amber-combine-jewelry-excursion/"
ORGANIZERS = [SOURCE_TITLE, "Калининградский янтарный комбинат"]
AUDIENCE = ["активным участникам фестиваля «80 историй о главном»"]
BLURB = (
    "Редкая возможность попасть не в музей при комбинате, а на действующее "
    "ювелирное производство и увидеть, как янтарь проходит путь до готового изделия."
)
BOOKING_TEXT = "Подать заявку до 9 августа, 11:00"
SEATS_TEXT = (
    "Участие по результатам розыгрыша; подать заявку могут только граждане Российской Федерации"
)
MEETING_POINT = "Калининградский янтарный комбинат, посёлок Янтарный"
NOW = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def table_columns(connection, table):
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info('{table}')")}


def filtered(connection, table, values):
    columns = table_columns(connection, table)
    return {key: value for key, value in values.items() if key in columns}


def insert_row(connection, table, values):
    data = filtered(connection, table, values)
    if not data:
        raise RuntimeError(f"no compatible columns for {table}")
    names = list(data)
    placeholders = ",".join("?" for _ in names)
    cursor = connection.execute(
        f"INSERT INTO {table} ({','.join(names)}) VALUES ({placeholders})",
        tuple(data[name] for name in names),
    )
    return int(cursor.lastrowid)


def update_row(connection, table, row_id, values):
    data = filtered(connection, table, values)
    data.pop("id", None)
    if not data:
        return
    names = list(data)
    connection.execute(
        f"UPDATE {table} SET " + ",".join(f"{name}=?" for name in names) + " WHERE id=?",
        (*[data[name] for name in names], int(row_id)),
    )


def upsert_occurrence():
    if not DB_PATH.is_file():
        raise RuntimeError(f"production database not found: {DB_PATH}")

    connection = sqlite3.connect(str(DB_PATH), timeout=45)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=45000")
    connection.execute("PRAGMA foreign_keys=ON")

    try:
        required_tables = {"guide_profile", "guide_source", "guide_occurrence"}
        found_tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('guide_profile','guide_source','guide_occurrence')"
            )
        }
        if found_tables != required_tables:
            raise RuntimeError(f"guide digest tables missing: {sorted(required_tables - found_tables)}")

        connection.execute("BEGIN IMMEDIATE")

        profile_values = {
            "slug": PROFILE_SLUG,
            "profile_kind": "organization",
            "display_name": SOURCE_TITLE,
            "marketing_name": SOURCE_TITLE,
            "source_links_json": dumps([URL]),
            "base_region": "Калининградская область",
            "first_seen_at": NOW,
            "last_seen_at": NOW,
            "created_at": NOW,
            "updated_at": NOW,
        }
        profile = connection.execute(
            "SELECT id FROM guide_profile WHERE slug=?",
            (PROFILE_SLUG,),
        ).fetchone()
        if profile:
            profile_id = int(profile["id"])
            update_row(connection, "guide_profile", profile_id, profile_values)
        else:
            profile_id = insert_row(connection, "guide_profile", profile_values)

        source_values = {
            "platform": SOURCE_PLATFORM,
            "username": SOURCE_USERNAME,
            "title": SOURCE_TITLE,
            "primary_profile_id": profile_id,
            "source_kind": "organization_with_tours",
            "trust_level": "official",
            "priority_weight": 100.0,
            "enabled": 0,
            "flags_json": dumps({"manual_digest_source": True, "official_event_page": True}),
            "base_region": "Калининградская область",
            "added_via": "manual_editorial_add",
            "created_at": NOW,
            "updated_at": NOW,
        }
        source = connection.execute(
            "SELECT id FROM guide_source WHERE platform=? AND username=?",
            (SOURCE_PLATFORM, SOURCE_USERNAME),
        ).fetchone()
        if source:
            source_id = int(source["id"])
            update_row(connection, "guide_source", source_id, source_values)
        else:
            source_id = insert_row(connection, "guide_source", source_values)

        fact_pack = {
            "source_post_url": URL,
            "availability_mode": "limited",
            "post_kind": "announce_single",
            "group_format": "экскурсия на действующее ювелирное производство",
            "route_summary": "действующее ювелирное производство Калининградского янтарного комбината",
            "organizer_names": ORGANIZERS,
            "audience_fit": AUDIENCE,
            "booking_text": BOOKING_TEXT,
            "booking_url": URL,
        }
        occurrence_values = {
            "primary_source_id": source_id,
            "source_fingerprint": FINGERPRINT,
            "canonical_title": TITLE,
            "title_normalized": TITLE_NORMALIZED,
            "participant_profiles_json": dumps([]),
            "guide_names_json": dumps([]),
            "organizer_names_json": dumps(ORGANIZERS),
            "digest_eligible": 1,
            "digest_eligibility_reason": "manual_editorial_add",
            "is_last_call": 0,
            "aggregator_only": 0,
            "date": EVENT_DATE,
            "time": EVENT_TIME,
            "city": "посёлок Янтарный",
            "meeting_point": MEETING_POINT,
            "audience_fit_json": dumps(AUDIENCE),
            "price_text": "Бесплатно",
            "booking_text": BOOKING_TEXT,
            "booking_url": URL,
            "channel_url": URL,
            "status": "scheduled",
            "seats_text": SEATS_TEXT,
            "summary_one_liner": BLURB,
            "digest_blurb": BLURB,
            "fact_pack_json": dumps(fact_pack),
            "published_new_digest_issue_id": None,
            "published_last_call_digest_issue_id": None,
            "published_visual_digest_issue_id": None,
            "created_at": NOW,
            "updated_at": NOW,
        }
        matches = connection.execute(
            "SELECT id FROM guide_occurrence WHERE source_fingerprint=? OR booking_url=? ORDER BY id",
            (FINGERPRINT, URL),
        ).fetchall()
        unique_ids = sorted({int(row["id"]) for row in matches})
        if len(unique_ids) > 1:
            raise RuntimeError(f"multiple matching occurrence rows found: {unique_ids}")
        if unique_ids:
            occurrence_id = unique_ids[0]
            update_row(connection, "guide_occurrence", occurrence_id, occurrence_values)
            action = "updated"
        else:
            occurrence_id = insert_row(connection, "guide_occurrence", occurrence_values)
            action = "inserted"

        connection.commit()
        return occurrence_id, action
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


async def verify(occurrence_id, action):
    from db import Database
    from guide_excursions.service import build_guide_digest_preview

    db = Database(str(DB_PATH))
    try:
        preview = await build_guide_digest_preview(
            db,
            family="new_occurrences",
            limit=24,
        )
    finally:
        await db.close()

    items = list(preview.get("items") or [])
    texts = [str(value) for value in (preview.get("texts") or [])]
    rendered = "\n\n".join(texts)
    matching_positions = []
    for index, item in enumerate(items, start=1):
        item_url = str(item.get("booking_url") or item.get("source_post_url") or "").strip()
        item_title = str(item.get("canonical_title") or "").strip()
        if item_url == URL or item_title == TITLE:
            matching_positions.append(index)

    if not matching_positions:
        raise RuntimeError(
            f"saved occurrence {occurrence_id} is absent from the exact 24-item next digest preview"
        )
    required_fragments = [TITLE, URL, "11 августа", "ювелирное производство"]
    missing = [fragment for fragment in required_fragments if fragment not in rendered]
    if missing:
        raise RuntimeError(f"digest preview lost required fragments: {missing}")
    if "не в музей" not in rendered:
        raise RuntimeError("digest preview lost the production-not-museum distinction")

    print(f"AMBER_DIGEST_ACTION={action}")
    print(f"AMBER_DIGEST_OCCURRENCE_ID={occurrence_id}")
    print(f"AMBER_DIGEST_PREVIEW_POSITION={matching_positions[0]}")
    print(f"AMBER_DIGEST_PREVIEW_ITEMS={len(items)}")
    print("AMBER_DIGEST_ELIGIBLE=true")
    print(f"AMBER_DIGEST_URL={URL}")
    print("AMBER_DIGEST_CARD_START")
    print(rendered)
    print("AMBER_DIGEST_CARD_END")


if __name__ == "__main__":
    occurrence_id, action = upsert_occurrence()
    asyncio.run(verify(occurrence_id, action))
