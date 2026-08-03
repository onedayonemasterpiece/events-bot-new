#!/usr/bin/env python3
"""Reproduce and verify the source binding of the provisional PR-A review seed.

The semantic row choices remain editorial input.  This tool verifies their raw
EventSource bindings against the ignored production evidence export, verifies
the index/snapshot binding, and writes the seed with a deterministic JSON
serialization.  It does not score events or create owner gold.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


SOURCE_REF_HASH_FIELDS = (
    "event_id",
    "source_id",
    "source_type",
    "source_url",
    "trust_level",
    "source_chat_username",
    "source_message_id",
    "source_text_sha256",
    "source_text_char_count",
)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: top-level JSON value must be an object")
    return value


def source_ref_hash(ref: Mapping[str, Any]) -> str:
    return digest(canonical_bytes({field: ref.get(field) for field in SOURCE_REF_HASH_FIELDS}))


def iter_seed_evidence(seed: Mapping[str, Any]) -> Iterable[tuple[int, Mapping[str, Any], Mapping[str, Any], str]]:
    for payload in seed.get("labels", {}).values():
        for side in ("positives", "hard_negatives"):
            for row in payload.get(side, []):
                refs = row.get("source_refs") or []
                if refs:
                    yield row["event_id"], refs[0], row, "source_"


def iter_receipt_evidence(index: Mapping[str, Any], index_path: Path) -> Iterable[tuple[int, Mapping[str, Any], Mapping[str, Any], str]]:
    for entry in index.get("receipts", []):
        receipt = load(index_path.parent / entry["path"])
        for evidence in receipt.get("source_evidence", []):
            yield evidence["event_id"], evidence["source_ref"], evidence, "raw_source_"


def verify_quote(
    event_id: int,
    ref: Mapping[str, Any],
    evidence: Mapping[str, Any],
    prefix: str,
    source: Mapping[str, Any],
) -> None:
    source_text = source.get("source_text")
    quote = evidence.get(f"{prefix}quote")
    if not isinstance(source_text, str) or not isinstance(quote, str):
        raise ValueError(f"event {event_id}: source/quote text is missing")
    start = evidence.get(f"{prefix}quote_start_char")
    end = evidence.get(f"{prefix}quote_end_char")
    if not isinstance(start, int) or not isinstance(end, int) or source_text[start:end] != quote:
        raise ValueError(f"event {event_id}: quote offsets do not select the frozen source text")
    if ref.get("source_text_sha256") != digest(source_text.encode("utf-8")):
        raise ValueError(f"event {event_id}: source_text_sha256 mismatch")
    if ref.get("source_text_char_count") != len(source_text):
        raise ValueError(f"event {event_id}: source_text_char_count mismatch")
    if ref.get("source_ref_sha256") != source_ref_hash(ref):
        raise ValueError(f"event {event_id}: source_ref_sha256 mismatch")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--source-review-index", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    snapshot_bytes = args.snapshot.read_bytes()
    snapshot = json.loads(snapshot_bytes)
    if snapshot_bytes != canonical_bytes(snapshot):
        raise ValueError("snapshot is not canonical-json-v1 (or has a trailing newline)")
    if snapshot.get("schema_version") != "static-collections-evidence-snapshot-v1":
        raise ValueError("unexpected snapshot schema_version")
    if snapshot.get("serialization_contract") != "canonical-json-v1":
        raise ValueError("unexpected snapshot serialization_contract")
    if snapshot.get("query_contract") != "event-review-source-v1":
        raise ValueError("unexpected snapshot query_contract")
    events = snapshot.get("events") or []
    if [event.get("id") for event in events] != sorted(event.get("id") for event in events):
        raise ValueError("snapshot events are not ordered by id")
    sources: dict[tuple[int, int], Mapping[str, Any]] = {}
    for event in events:
        event_sources = event.get("event_sources") or []
        if [source.get("id") for source in event_sources] != sorted(
            source.get("id") for source in event_sources
        ):
            raise ValueError(f"event {event.get('id')}: EventSource rows are not ordered by id")
        for source in event_sources:
            sources[(event["id"], source["id"])] = source

    seed = load(args.seed)
    index = load(args.source_review_index)
    snapshot_sha256 = digest(snapshot_bytes)
    if seed.get("evidence_snapshot_sha256") != snapshot_sha256:
        raise ValueError("seed evidence snapshot hash mismatch")
    if index.get("source_snapshot_sha256") != snapshot_sha256:
        raise ValueError("source-review index snapshot hash mismatch")

    for event_id, ref, evidence, prefix in (
        *iter_seed_evidence(seed),
        *iter_receipt_evidence(index, args.source_review_index),
    ):
        source_id = ref.get("source_id")
        source = sources.get((event_id, source_id))
        if source is None:
            raise ValueError(f"event {event_id}: EventSource {source_id} absent from snapshot")
        verify_quote(event_id, ref, evidence, prefix, source)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(seed, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
