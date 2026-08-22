from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import static_site_search_snapshot as search_snapshot
from static_site_release import (
    StaticSiteRetryableError,
    delete_immutable_snapshot,
    make_request_payload,
    validate_vector_barrier,
)


def _receipt(*, catalog_revision: str) -> dict:
    return {
        "schema_version": "event_vector_sync_receipt_v2",
        "status": "complete",
        "complete": True,
        "catalog_revision": catalog_revision,
        "corpus_revision": "b" * 64,
        "search_document_revision": "b" * 64,
        "search_v3_hash": "b" * 64,
        "related_v1_hash": "c" * 64,
        "coverage": {"status": "complete"},
        "event_revisions": {"42": "revision-42"},
        "run_id": "projection-test",
    }


def _payload() -> dict:
    return make_request_payload(
        reason="smart_update",
        event_ids=[42],
        event_revisions={42: "revision-42"},
        require_vector_barrier=True,
    )


def test_snapshot_catalog_drift_fails_before_kaggle_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "event-vector-receipt.json"
    source.write_text(json.dumps(_receipt(catalog_revision="a" * 64)), encoding="utf-8")
    snapshot = tmp_path / "snapshot.sqlite"
    snapshot.write_bytes(b"snapshot")
    monkeypatch.setattr(
        search_snapshot,
        "snapshot_search_catalog_revision",
        lambda *_args, **_kwargs: "d" * 64,
    )

    with pytest.raises(StaticSiteRetryableError, match="catalog_revision_pending"):
        search_snapshot.bind_snapshot_search_receipt(
            request_payload=_payload(),
            source_receipt_path=source,
            snapshot_path=snapshot,
            current_date="2026-08-08",
            current_datetime="2026-08-08T12:00:00+02:00",
        )
    assert not snapshot.with_suffix(".search-receipt.json").exists()


def test_receipt_is_one_frozen_post_snapshot_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "event-vector-receipt.json"
    source.write_text(json.dumps(_receipt(catalog_revision="a" * 64)), encoding="utf-8")
    snapshot = tmp_path / "snapshot.sqlite"
    snapshot.write_bytes(b"snapshot")
    payload = _payload()
    assert validate_vector_barrier(payload, source)["catalog_revision"] == "a" * 64

    source.write_text(json.dumps(_receipt(catalog_revision="b" * 64)), encoding="utf-8")
    monkeypatch.setattr(
        search_snapshot,
        "snapshot_search_catalog_revision",
        lambda *_args, **_kwargs: "b" * 64,
    )
    frozen, evidence = search_snapshot.bind_snapshot_search_receipt(
        request_payload=payload,
        source_receipt_path=source,
        snapshot_path=snapshot,
        current_date="2026-08-08",
        current_datetime="2026-08-08T12:00:00+02:00",
    )
    source.write_text(json.dumps(_receipt(catalog_revision="c" * 64)), encoding="utf-8")

    assert evidence["catalog_revision"] == "b" * 64
    assert json.loads(frozen.read_text(encoding="utf-8"))["catalog_revision"] == "b" * 64
    assert frozen.stat().st_mode & 0o777 == 0o600
    removed = delete_immutable_snapshot(snapshot, tmp_path / "snapshot.manifest.json")
    assert removed > 0
    assert not frozen.exists()


def test_snapshot_catalog_revision_uses_vector_probe_free_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = tmp_path / "snapshot.sqlite"
    sqlite3.connect(snapshot).close()

    class FakeExporter:
        SKIP_IMAGE_PROBES = False

        @staticmethod
        def split_current_datetime(_value: str, current_date: str) -> tuple[str, str]:
            return current_date, "12:00"

        @staticmethod
        def fetch_rows(*_args, **_kwargs) -> list[dict[str, int]]:
            return [{"id": 42}]

        @staticmethod
        def event_participants_for_events(*_args, **_kwargs) -> dict[int, list]:
            return {42: []}

        @staticmethod
        def event_video_assets_for_events(*_args, **_kwargs) -> dict[int, list]:
            return {42: []}

        def build_event(self, *_args, **_kwargs) -> dict[str, int]:
            assert self.SKIP_IMAGE_PROBES is True
            return {"id": 42}

        @staticmethod
        def normalize_linked_occurrences(_events: list[dict[str, int]]) -> None:
            return None

        @staticmethod
        def exported_search_catalog_revision(_events: list[dict[str, int]]) -> str:
            return "a" * 64

    exporter = FakeExporter()
    monkeypatch.setattr(search_snapshot, "_load_exporter", lambda: exporter)

    revision = search_snapshot.snapshot_search_catalog_revision(
        snapshot,
        current_date="2026-08-22",
        current_datetime="2026-08-22T12:00:00+02:00",
    )

    assert revision == "a" * 64
    assert exporter.SKIP_IMAGE_PROBES is False
