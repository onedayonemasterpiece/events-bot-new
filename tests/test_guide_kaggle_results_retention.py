from __future__ import annotations

import os
import time
from pathlib import Path

from guide_excursions import kaggle_service


def _write_bundle(root: Path, name: str, *, size: int = 1024, age_days: int = 0) -> Path:
    bundle = root / name
    bundle.mkdir(parents=True)
    (bundle / "guide_excursions_results.json").write_text(
        '{"run_id":"%s"}' % name.replace("guide-excursions-", ""),
        encoding="utf-8",
    )
    (bundle / "payload.bin").write_bytes(b"x" * size)
    mtime = time.time() - age_days * 86400
    os.utime(bundle, (mtime, mtime))
    for child in bundle.iterdir():
        os.utime(child, (mtime, mtime))
    return bundle


def test_persist_downloaded_guide_results_prunes_old_bundles(tmp_path, monkeypatch):
    store = tmp_path / "store"
    source = tmp_path / "download" / "guide-excursions-new"
    source.mkdir(parents=True)
    (source / "guide_excursions_results.json").write_text('{"run_id":"new"}', encoding="utf-8")

    _write_bundle(store, "guide-excursions-old", age_days=5)
    fresh = _write_bundle(store, "guide-excursions-fresh", age_days=0)

    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_ROOT", store)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_RETENTION_DAYS", 2)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MAX_RUNS", 6)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MAX_MB", 0)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MIN_FREE_MB", 0)

    persisted = kaggle_service.persist_downloaded_guide_results(
        source / "guide_excursions_results.json",
        "new",
    )

    assert persisted == store / "guide-excursions-new" / "guide_excursions_results.json"
    assert persisted.is_file()
    assert not (store / "guide-excursions-old").exists()
    assert fresh.exists()


def test_persist_downloaded_guide_results_keeps_only_recent_run_count(tmp_path, monkeypatch):
    store = tmp_path / "store"
    source = tmp_path / "download" / "guide-excursions-new"
    source.mkdir(parents=True)
    (source / "guide_excursions_results.json").write_text('{"run_id":"new"}', encoding="utf-8")

    for idx in range(4):
        _write_bundle(store, f"guide-excursions-{idx}", age_days=0)
        mtime = time.time() - (10 - idx)
        os.utime(store / f"guide-excursions-{idx}", (mtime, mtime))

    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_ROOT", store)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_RETENTION_DAYS", 0)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MAX_RUNS", 2)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MAX_MB", 0)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MIN_FREE_MB", 0)

    kaggle_service.persist_downloaded_guide_results(
        source / "guide_excursions_results.json",
        "new",
    )

    assert not (store / "guide-excursions-0").exists()
    assert not (store / "guide-excursions-1").exists()
    assert not (store / "guide-excursions-2").exists()
    assert (store / "guide-excursions-3").exists()
    assert (store / "guide-excursions-new").exists()


def test_new_and_unacknowledged_bundles_count_toward_byte_budget(tmp_path, monkeypatch):
    store = tmp_path / "results"
    old = _write_bundle(store, "guide-excursions-old", size=600_000, age_days=0)
    pending = _write_bundle(store, "guide-excursions-push-pending", size=300_000, age_days=0)
    (pending / ".delivery_pending").touch()
    source = _write_bundle(tmp_path / "downloads", "guide-excursions-new", size=400_000, age_days=0)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_ROOT", store)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MAX_MB", 1)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MAX_RUNS", 10)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MIN_FREE_MB", 0)
    result = kaggle_service.persist_downloaded_guide_results(source / "guide_excursions_results.json", "new")
    assert result.is_file() and (pending / ".delivery_pending").exists()
    assert not old.exists()
    assert kaggle_service._path_size_bytes(store) <= 1024 * 1024


def test_acknowledged_push_import_triggers_retention_and_preserves_pending(tmp_path, monkeypatch):
    from guide_excursions.result_delivery import finish_result_import

    store = tmp_path / "results"
    old = _write_bundle(store, "guide-excursions-recovered", size=400_000, age_days=1)
    pending = _write_bundle(store, "guide-excursions-push-active", size=400_000, age_days=0)
    accepted = _write_bundle(store, "guide-excursions-push-done", size=400_000, age_days=0)
    for bundle in (pending, accepted):
        (bundle / ".delivery_pending").touch()
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_ROOT", store)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MAX_MB", 1)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MAX_RUNS", 2)
    monkeypatch.setattr(kaggle_service, "RESULTS_STORE_MIN_FREE_MB", 0)
    finish_result_import("done")
    assert accepted.exists() and not (accepted / ".delivery_pending").exists()
    assert (pending / ".delivery_pending").exists()
    assert not old.exists()
    finish_result_import("done")
    assert accepted.exists() and pending.exists()
