#!/usr/bin/env python3
"""Resume-safe, lossless compaction of legacy VK packet JSON on a live SQLite DB.

Run only after the application version that reads both plain and compressed
packet JSON is deployed. The script never deletes packets or changes hashes.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from vk_packet_storage import decode_packet_json, encode_packet_json


def compact(db_path: Path, *, batch_size: int = 100, max_batches: int = 0) -> dict[str, int]:
    count = raw_bytes = stored_bytes = 0
    last_id = 0
    batches = 0
    with sqlite3.connect(str(db_path), timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        while True:
            rows = conn.execute(
                """
                SELECT id, raw_payload_json, attachment_metadata_json
                FROM vk_source_packet WHERE id > ? ORDER BY id LIMIT ?
                """,
                (last_id, batch_size),
            ).fetchall()
            if not rows:
                break
            updates = []
            for packet_id, raw, attachments in rows:
                last_id = int(packet_id)
                if not isinstance(raw, str) or not isinstance(attachments, str):
                    raise ValueError(f"packet {packet_id}: invalid stored JSON type")
                # Verify every existing compressed value while migrating.
                decode_packet_json(raw)
                decode_packet_json(attachments)
                compressed_raw = raw if raw.startswith("zlib64:") else encode_packet_json(raw)
                compressed_attachments = (
                    attachments if attachments.startswith("zlib64:")
                    else encode_packet_json(attachments)
                )
                if compressed_raw != raw or compressed_attachments != attachments:
                    updates.append((compressed_raw, compressed_attachments, packet_id))
                    raw_bytes += len(raw.encode()) + len(attachments.encode())
                    stored_bytes += len(compressed_raw.encode()) + len(compressed_attachments.encode())
            if updates:
                conn.executemany(
                    """
                    UPDATE vk_source_packet
                    SET raw_payload_json=?, attachment_metadata_json=?
                    WHERE id=?
                    """,
                    updates,
                )
                conn.commit()
                count += len(updates)
                print(
                    f"batch={batches + 1} last_id={last_id} updated={len(updates)} "
                    f"total_updated={count} saved_bytes={raw_bytes - stored_bytes}",
                    flush=True,
                )
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            batches += 1
            if max_batches and batches >= max_batches:
                break
    return {"updated": count, "saved_bytes": raw_bytes - stored_bytes, "last_id": last_id}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-batches", type=int, default=0)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 500:
        parser.error("batch size must be between 1 and 500")
    print(compact(args.db, batch_size=args.batch_size, max_batches=args.max_batches))


if __name__ == "__main__":
    main()
