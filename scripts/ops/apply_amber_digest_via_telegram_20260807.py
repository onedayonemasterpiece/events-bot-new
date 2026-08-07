from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Iterable

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

BOT_USERNAME = "events_love39_bot"
TITLE = "Экскурсия на ювелирное производство Калининградского янтарного комбината"
EVENT_URL = "https://kgd80.ru/special/amber-combine-jewelry-excursion/"
SEED_SCRIPT = Path("scripts/ops/add_amber_excursion_digest_20260807.py")


def _decode_bundle(value: str) -> dict:
    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError("TELEGRAM_AUTH_BUNDLE_GH_ACTIONS is empty")
    if raw.startswith("{"):
        data = json.loads(raw)
    else:
        padded = raw + ("=" * (-len(raw) % 4))
        try:
            decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        except Exception:
            decoded = base64.b64decode(padded.encode("ascii")).decode("utf-8")
        data = json.loads(decoded)
    if not isinstance(data, dict) or not data.get("session"):
        raise RuntimeError("Telegram auth bundle has no session")
    return data


def _message_urls(message) -> list[str]:
    text = str(message.raw_text or "")
    urls: list[str] = []
    for entity in message.entities or []:
        if isinstance(entity, MessageEntityTextUrl):
            value = str(entity.url or "").strip()
        elif isinstance(entity, MessageEntityUrl):
            value = text[entity.offset : entity.offset + entity.length].strip()
        else:
            continue
        if value and value not in urls:
            urls.append(value)
    return urls


async def _new_bot_messages(client: TelegramClient, target, after_id: int, *, limit: int = 100):
    messages = await client.get_messages(target, limit=limit)
    return sorted(
        [message for message in messages if int(message.id) > int(after_id) and not message.out],
        key=lambda message: int(message.id),
    )


async def _wait_for_dump(client: TelegramClient, target, after_id: int, destination: Path, timeout: int = 120):
    deadline = time.monotonic() + timeout
    seen: set[int] = set()
    while time.monotonic() < deadline:
        for message in await _new_bot_messages(client, target, after_id):
            if int(message.id) in seen:
                continue
            seen.add(int(message.id))
            text = str(message.raw_text or "").strip()
            if text == "Not authorized":
                raise RuntimeError("Telegram E2E account is not a production superadmin")
            filename = str(getattr(message.file, "name", "") or "")
            if message.document and filename == "dump.sql":
                await client.download_media(message, file=str(destination))
                if not destination.is_file() or destination.stat().st_size == 0:
                    raise RuntimeError("downloaded dump.sql is empty")
                return message
        await asyncio.sleep(1)
    raise TimeoutError("production bot did not return dump.sql")


def _restore_sql_dump(sql_path: Path, db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    sql_text = sql_path.read_text(encoding="utf-8")
    connection = sqlite3.connect(str(db_path))
    try:
        connection.executescript(sql_text)
        row = connection.execute("PRAGMA quick_check").fetchone()
        if not row or str(row[0]).lower() != "ok":
            raise RuntimeError(f"SQLite quick_check failed: {row!r}")
    finally:
        connection.close()


def _load_seed_module(db_path: Path):
    if not SEED_SCRIPT.is_file():
        raise RuntimeError(f"seed script missing: {SEED_SCRIPT}")
    os.environ["AMBER_DB_PATH"] = str(db_path)
    spec = importlib.util.spec_from_file_location("amber_digest_seed_20260807", SEED_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load amber digest seed script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_sql_dump(db_path: Path, destination: Path) -> None:
    connection = sqlite3.connect(str(db_path))
    try:
        row = connection.execute("PRAGMA quick_check").fetchone()
        if not row or str(row[0]).lower() != "ok":
            raise RuntimeError(f"modified database quick_check failed: {row!r}")
        with destination.open("w", encoding="utf-8", newline="\n") as handle:
            for statement in connection.iterdump():
                handle.write(statement)
                handle.write("\n")
    finally:
        connection.close()
    if not destination.is_file() or destination.stat().st_size == 0:
        raise RuntimeError("modified SQL dump is empty")


def _verify_occurrence_in_db(db_path: Path, expected_id: int | None = None) -> dict:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT id, canonical_title, date, time, booking_url, digest_eligible,
                   status, published_new_digest_issue_id, digest_blurb, seats_text
            FROM guide_occurrence
            WHERE booking_url=?
            ORDER BY id
            """,
            (EVENT_URL,),
        ).fetchall()
    finally:
        connection.close()
    if len(rows) != 1:
        raise RuntimeError(f"expected exactly one amber occurrence, found {len(rows)}")
    row = dict(rows[0])
    if expected_id is not None and int(row["id"]) != int(expected_id):
        raise RuntimeError(f"occurrence id changed: expected {expected_id}, got {row['id']}")
    checks = {
        "canonical_title": TITLE,
        "date": "2026-08-11",
        "time": "11:00",
        "booking_url": EVENT_URL,
        "digest_eligible": 1,
        "status": "scheduled",
    }
    for key, expected in checks.items():
        if row.get(key) != expected:
            raise RuntimeError(f"stored occurrence mismatch {key}: {row.get(key)!r}")
    if row.get("published_new_digest_issue_id") is not None:
        raise RuntimeError("occurrence is already marked as published")
    blurb = str(row.get("digest_blurb") or "")
    if "не в музей" not in blurb or "ювелирное производство" not in blurb:
        raise RuntimeError("stored digest blurb lost production-not-museum distinction")
    if "6" in str(row.get("seats_text") or "") or "победител" in str(row.get("seats_text") or "").lower():
        raise RuntimeError("public seats text exposes winner count")
    return row


async def _wait_for_text(client: TelegramClient, target, after_id: int, expected: str, timeout: int = 120):
    deadline = time.monotonic() + timeout
    seen: set[int] = set()
    while time.monotonic() < deadline:
        for message in await _new_bot_messages(client, target, after_id):
            if int(message.id) in seen:
                continue
            seen.add(int(message.id))
            text = str(message.raw_text or "").strip()
            if text == "Not authorized":
                raise RuntimeError("production bot rejected superadmin operation")
            if expected in text:
                return message
        await asyncio.sleep(1)
    raise TimeoutError(f"production bot did not answer with {expected!r}")


async def _collect_guide_preview(client: TelegramClient, target, after_id: int, timeout: int = 180):
    deadline = time.monotonic() + timeout
    collected: dict[int, object] = {}
    last_change = time.monotonic()
    found_card = False
    while time.monotonic() < deadline:
        changed = False
        for message in await _new_bot_messages(client, target, after_id, limit=200):
            if int(message.id) not in collected:
                collected[int(message.id)] = message
                last_change = time.monotonic()
                changed = True
        messages = [collected[key] for key in sorted(collected)]
        for message in messages:
            text = str(message.raw_text or "")
            urls = _message_urls(message)
            if TITLE in text and EVENT_URL in urls and "11 августа" in text and "не в музей" in text:
                found_card = True
                break
        if found_card and time.monotonic() - last_change >= 4:
            return messages
        if not changed:
            await asyncio.sleep(1)
    raise TimeoutError("exact production /guide_recent preview did not contain the amber card")


def _extract_preview_evidence(messages: Iterable[object], occurrence_id: int) -> tuple[int, str, list[str]]:
    position = 0
    card_text = ""
    card_urls: list[str] = []
    for message in messages:
        text = str(message.raw_text or "")
        if text.startswith("🧾 Recent guide findings"):
            item_lines = [line for line in text.splitlines() if line.startswith("- #")]
            for index, line in enumerate(item_lines, start=1):
                if f"#{occurrence_id} " in line or TITLE in line:
                    position = index
                    break
        urls = _message_urls(message)
        if TITLE in text and EVENT_URL in urls:
            card_text = text.strip()
            card_urls = urls
    if position < 1:
        raise RuntimeError("production preview summary did not expose occurrence position")
    if not card_text:
        raise RuntimeError("production preview did not expose exact rendered card text")
    for fragment in (TITLE, "11 августа", "ювелирное производство", "не в музей"):
        if fragment not in card_text:
            raise RuntimeError(f"production card lost required fragment: {fragment}")
    if EVENT_URL not in card_urls:
        raise RuntimeError("production card lost application URL entity")
    if "6" in card_text or "победител" in card_text.lower():
        raise RuntimeError("production digest card exposes winner count")
    return position, card_text, card_urls


async def main() -> None:
    bundle = _decode_bundle(os.environ.get("TELEGRAM_AUTH_BUNDLE_GH_ACTIONS", ""))
    api_id = int(os.environ.get("TELEGRAM_GH_ACTIONS_API_ID", "0") or 0)
    api_hash = str(os.environ.get("TELEGRAM_GH_ACTIONS_API_HASH", "")).strip()
    if api_id <= 0 or not api_hash:
        raise RuntimeError("Telegram GH Actions API credentials are missing")

    client = TelegramClient(
        StringSession(str(bundle["session"])),
        api_id,
        api_hash,
        device_model=str(bundle.get("device_model") or "GitHub Actions"),
        system_version=str(bundle.get("system_version") or "Linux"),
        app_version=str(bundle.get("app_version") or "1.0"),
        lang_code=str(bundle.get("lang_code") or "ru"),
        system_lang_code=str(bundle.get("system_lang_code") or "ru"),
    )

    with tempfile.TemporaryDirectory(prefix="amber-digest-") as temporary:
        root = Path(temporary)
        original_sql = root / "before.sql"
        modified_db = root / "modified.sqlite"
        modified_sql = root / "modified.sql"
        validation_db = root / "validation.sqlite"
        after_sql = root / "after.sql"
        after_db = root / "after.sqlite"

        await client.connect()
        try:
            if not await client.is_user_authorized():
                raise RuntimeError("Telegram GH Actions session is not authorized")
            me = await client.get_me()
            print(f"TELEGRAM_E2E_USER_ID={int(me.id)}")
            print(f"TELEGRAM_E2E_USERNAME={str(getattr(me, 'username', '') or '')}")
            target = await client.get_entity(BOT_USERNAME)

            dump_command = await client.send_message(target, "/dumpdb")
            await _wait_for_dump(client, target, int(dump_command.id), original_sql)
            print(f"PRODUCTION_DUMP_BYTES={original_sql.stat().st_size}")

            _restore_sql_dump(original_sql, modified_db)
            seed = _load_seed_module(modified_db)
            occurrence_id, action = seed.upsert_occurrence()
            stored = _verify_occurrence_in_db(modified_db, occurrence_id)
            _write_sql_dump(modified_db, modified_sql)
            _restore_sql_dump(modified_sql, validation_db)
            _verify_occurrence_in_db(validation_db, occurrence_id)
            print(f"AMBER_LOCAL_ACTION={action}")
            print(f"AMBER_LOCAL_OCCURRENCE_ID={occurrence_id}")
            print(f"MODIFIED_DUMP_BYTES={modified_sql.stat().st_size}")

            upload = await client.send_file(
                target,
                str(modified_sql),
                caption="Operator-authorized amber digest database maintenance",
                force_document=True,
            )
            restore_command = await client.send_message(target, "/restore", reply_to=int(upload.id))
            await _wait_for_text(client, target, int(restore_command.id), "Database restored")
            print("PRODUCTION_DATABASE_RESTORED=true")

            preview_command = await client.send_message(target, "/guide_recent")
            preview_messages = await _collect_guide_preview(client, target, int(preview_command.id))
            position, card_text, card_urls = _extract_preview_evidence(preview_messages, occurrence_id)

            final_dump_command = await client.send_message(target, "/dumpdb")
            await _wait_for_dump(client, target, int(final_dump_command.id), after_sql)
            _restore_sql_dump(after_sql, after_db)
            final_row = _verify_occurrence_in_db(after_db, occurrence_id)

            print(f"AMBER_DIGEST_ACTION={action}")
            print(f"AMBER_DIGEST_OCCURRENCE_ID={occurrence_id}")
            print(f"AMBER_DIGEST_PREVIEW_POSITION={position}")
            print("AMBER_DIGEST_ELIGIBLE=true")
            print(f"AMBER_DIGEST_URL={EVENT_URL}")
            print(f"AMBER_DIGEST_FINAL_STATUS={final_row['status']}")
            print("AMBER_DIGEST_WINNER_COUNT_HIDDEN=true")
            print("AMBER_DIGEST_CARD_START")
            print(card_text)
            print("AMBER_DIGEST_CARD_URLS=" + " | ".join(card_urls))
            print("AMBER_DIGEST_CARD_END")
        finally:
            await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
