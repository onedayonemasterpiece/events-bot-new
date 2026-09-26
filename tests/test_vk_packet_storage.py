from __future__ import annotations

import json
import sqlite3

import pytest

from vk_packet_storage import decode_packet_json, encode_packet_json
from scripts.ops.compress_vk_packet_storage import compact


def test_vk_packet_json_storage_roundtrip_and_legacy_read() -> None:
    raw = json.dumps({"text": "Афиша Калининграда " * 5000}, ensure_ascii=False)
    encoded = encode_packet_json(raw)
    assert encoded.startswith("zlib64:")
    assert len(encoded) < len(raw) // 5
    assert decode_packet_json(encoded) == raw
    assert decode_packet_json(raw) == raw


def test_vk_packet_json_storage_rejects_corrupt_payload() -> None:
    with pytest.raises(ValueError, match="invalid compressed VK packet JSON"):
        decode_packet_json("zlib64:not-base64")


def test_vk_packet_compaction_preserves_content_and_is_resumable(tmp_path) -> None:
    db_path = tmp_path / "packets.sqlite"
    raw = json.dumps({"text": "Калининград " * 5000}, ensure_ascii=False)
    attachments = json.dumps({"photos": ["https://example.test/image"] * 1000})
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE vk_source_packet "
            "(id INTEGER PRIMARY KEY, raw_payload_json TEXT, attachment_metadata_json TEXT, payload_hash TEXT)"
        )
        conn.execute(
            "INSERT INTO vk_source_packet VALUES (1, ?, ?, 'original-hash')",
            (raw, attachments),
        )
        conn.execute(
            "INSERT INTO vk_source_packet VALUES (2, ?, ?, 'second-hash')",
            (raw, attachments),
        )
    assert compact(db_path, batch_size=1, max_batches=1)["updated"] == 1
    assert compact(db_path, batch_size=1)["updated"] == 1
    assert compact(db_path, batch_size=1)["updated"] == 0
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT raw_payload_json,attachment_metadata_json,payload_hash "
            "FROM vk_source_packet ORDER BY id"
        ).fetchall()
    assert [row[2] for row in rows] == ["original-hash", "second-hash"]
    assert all(decode_packet_json(row[0]) == raw for row in rows)
    assert all(decode_packet_json(row[1]) == attachments for row in rows)


@pytest.mark.asyncio
async def test_compressed_packet_replays_through_vk_import_boundary(tmp_path) -> None:
    from db import Database
    from vk_auto_queue import _load_vk_durable_packet_evidence
    from vk_intake import _persist_vk_source_packet
    from vk_source_envelope import build_vk_source_envelope

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        envelope = build_vk_source_envelope(
            {"id": 99, "date": 1790438400, "text": "Афиша " * 5000, "attachments": []},
            owner_id=1,
            media_limit=None,
        )
        packet_id, is_new = await _persist_vk_source_packet(
            db, group_id=1, owner_type="group", post=envelope,
            source_url="https://vk.com/wall-1_99", keyword_hints=(),
            date_hints=(), event_ts_hint=None,
        )
        assert is_new
        async with db.raw_conn() as conn:
            row = await (await conn.execute(
                "SELECT raw_payload_json FROM vk_source_packet WHERE id=?", (packet_id,)
            )).fetchone()
        assert row[0].startswith("zlib64:")
        from scripts.ops.smart_update_loss_census import _vk_packet_replayability

        assert _vk_packet_replayability({"raw_payload_json": row[0]}) == "replayable_lossless"
        evidence = await _load_vk_durable_packet_evidence(
            db, group_id=1, post_id=99, source_packet_id=packet_id,
            media_limit=0,
        )
        assert evidence["replayability"] == "replayable_lossless"
        assert evidence["text"] == envelope["text"]
    finally:
        await db.close()
