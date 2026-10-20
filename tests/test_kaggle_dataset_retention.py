from datetime import datetime, timezone
import json
import sqlite3

import pytest

from kaggle_dataset_retention import (
    KaggleRetentionStore, cleanup_inputs, plan_cleanup, protected_local_inputs,
)

NOW = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)


def row(slug="static-site-builder-input-old", **values):
    return {"ref": "owner/" + slug, "isPrivate": True, "currentVersionNumber": 1,
            "lastUpdated": "2026-09-01T00:00:00Z", "totalBytes": 123, **values}


def test_plan_is_scoped_to_private_immutable_stale_owned_unpinned_inputs():
    rows = [row(), row("cherryflash-session-old"), row("runcoveer-production-backup"),
            row(ref="other/static-site-builder-input-old"), row(isPrivate=False),
            row(currentVersionNumber=2), row(lastUpdated="2026-10-20T06:00:00Z"),
            row(lastUpdated="bad"), row("static-site-builder-input-pinned"),
            row("static-site-builder-input-version/1")]
    plan = plan_cleanup(rows, owner="owner", now=NOW,
                        protected={"owner/static-site-builder-input-pinned"})
    assert {x["ref"] for x in plan["candidates"]} == {
        "owner/static-site-builder-input-old", "owner/cherryflash-session-old"}
    assert {x["reason"] for x in plan["kept"]} == {
        "versioned_or_unknown_identity", "within_retention", "unknown_age",
        "current_notebook_or_recoverable_run"}
    with pytest.raises(ValueError):
        plan_cleanup(rows, owner="owner", protected=set(), min_age_hours=0)


class Store:
    def __init__(self):
        self.rows = [row(), row("cherryflash-session-failure"), row("cherryflash-session-unknown")]
        self.effects = []

    def protected_notebook_inputs(self):
        return set()

    def list_inputs(self):
        return list(self.rows)

    def delete(self, ref):
        self.effects.append(ref)
        if ref.endswith("failure"):
            raise OSError("provider failed before deletion")
        self.rows = [r for r in self.rows if r["ref"] != ref]
        if ref.endswith("unknown"):
            raise TimeoutError("reply lost after the effect")


def test_dry_run_never_calls_delete():
    store = Store()
    result = cleanup_inputs(store, owner="owner")
    assert len(result["candidates"]) == 3
    assert store.effects == []


def test_readback_reconciles_lost_reply_and_failed_deletions_are_retried():
    store = Store()
    events = []
    result = cleanup_inputs(store, owner="owner", apply=True, record=events.append)
    assert set(result["verified_absent"]) == {
        "owner/static-site-builder-input-old", "owner/cherryflash-session-unknown"}
    assert result["remaining"] == ["owner/cherryflash-session-failure"]
    assert events[0]["event"] == "plan"
    for index, event in enumerate(events):
        if event["event"] == "delete_acknowledged":
            assert events[index - 1] == {"event": "delete_requested", "ref": event["ref"]}
    store.effects.clear()
    cleanup_inputs(store, owner="owner", apply=True)
    assert store.effects == ["owner/cherryflash-session-failure"]


def test_incomplete_notebook_metadata_aborts_before_any_mutation():
    store = Store()
    def unavailable():
        raise OSError("notebook inventory unavailable")
    store.protected_notebook_inputs = unavailable
    with pytest.raises(OSError):
        cleanup_inputs(store, owner="owner", apply=True)
    assert not store.effects


def test_opaque_notebooks_cannot_be_used_without_project_ownership():
    store = Store()
    store.redacted_notebook_rows = 5
    with pytest.raises(RuntimeError, match="canonical project ownership"):
        cleanup_inputs(store, owner="owner", apply=True)
    assert not store.effects


def test_new_local_handoff_is_rechecked_before_each_delete(monkeypatch):
    import kaggle_dataset_retention as module
    store = Store()
    store.rows = [row()]
    checks = iter([set(), {row()["ref"]}])
    monkeypatch.setattr(module, "protected_local_inputs", lambda _: next(checks))
    result = cleanup_inputs(store, owner="owner", apply=True)
    assert not store.effects
    assert result["kept"][-1]["reason"] == "new_recoverable_run"


def ledger(path):
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE kaggle_run_ledger(run_id TEXT, dataset_ref TEXT, status TEXT, terminal_at TEXT);
        CREATE TABLE kaggle_resource_lease(run_id TEXT, status TEXT);
        CREATE TABLE static_site_build_state(active_run_id TEXT, active_job_id INTEGER);
        CREATE TABLE joboutbox(id INTEGER, payload TEXT);
        CREATE TABLE videoannounce_session(kaggle_dataset TEXT, status TEXT);
    """)
    return conn


def test_local_ledger_protects_unknown_active_leases_and_restart_handoffs(tmp_path):
    path = tmp_path / "state.sqlite"
    with ledger(path) as conn:
        conn.executemany("INSERT INTO kaggle_run_ledger VALUES (?,?,?,?)", [
            ("unknown", "owner/cherryflash-session-unknown", "unknown", None),
            ("leased", "owner/cherryflash-session-leased", "done", "2026-09-01"),
            ("finished", "owner/static-site-builder-input-finished", "done", "2026-09-01")])
        conn.execute("INSERT INTO kaggle_resource_lease VALUES ('leased','active')")
        conn.execute("INSERT INTO static_site_build_state VALUES ('missing-ledger',1)")
        conn.execute("INSERT INTO joboutbox VALUES (1,?)", (json.dumps({"remote_handoff": {
            "dataset_ref": "owner/static-site-builder-input-recover/1"}}),))
        conn.execute("INSERT INTO videoannounce_session VALUES (?, 'PUBLISH_BLOCKED')",
                     ("owner/cherryflash-session-blocked",))
    assert protected_local_inputs(path) == {
        "owner/cherryflash-session-unknown", "owner/cherryflash-session-leased",
        "owner/static-site-builder-input-recover", "owner/cherryflash-session-blocked"}


def test_unknown_active_static_identity_and_missing_db_fail_closed(tmp_path):
    path = tmp_path / "state.sqlite"
    with ledger(path) as conn:
        conn.execute("INSERT INTO kaggle_run_ledger VALUES ('other',?,'running',NULL)",
                     ("owner/static-site-builder-input-other",))
        conn.execute("INSERT INTO static_site_build_state VALUES ('missing-ledger',1)")
        conn.execute("INSERT INTO joboutbox VALUES (1,'{}')")
    with pytest.raises(RuntimeError, match="identity is unknown"):
        protected_local_inputs(path)
    with pytest.raises(sqlite3.OperationalError):
        protected_local_inputs(tmp_path / "not-created.sqlite")
    assert not (tmp_path / "not-created.sqlite").exists()


def test_sdk_false_delete_response_is_not_success(monkeypatch):
    from types import SimpleNamespace
    store = KaggleRetentionStore(SimpleNamespace(dataset_delete=lambda *a, **kw: False))
    monkeypatch.setattr("kaggle_dataset_retention.time.sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="did not acknowledge"):
        store.delete("owner/static-site-builder-input-old")


def test_repeated_provider_page_is_not_a_complete_inventory(monkeypatch):
    from types import SimpleNamespace
    store = KaggleRetentionStore(SimpleNamespace(dataset_list=lambda **kw: [row()]))
    monkeypatch.setattr("kaggle_dataset_retention.time.sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="pagination did not advance"):
        store.list_inputs()


def test_reviewed_allowlist_cannot_expand_to_other_inputs():
    store = Store()
    result = cleanup_inputs(store, owner="owner", apply=True,
                            approved_refs={"owner/static-site-builder-input-old"})
    assert store.effects == ["owner/static-site-builder-input-old"]
    assert result["verified_absent"] == store.effects


def test_transport_timeout_does_not_retry_unknown_delete(monkeypatch):
    from types import SimpleNamespace
    calls = []
    def delete(*args, **kwargs):
        calls.append(args)
        raise TimeoutError("response lost")
    store = KaggleRetentionStore(SimpleNamespace(dataset_delete=delete))
    monkeypatch.setattr("kaggle_dataset_retention.time.sleep", lambda _: None)
    with pytest.raises(TimeoutError):
        store.delete("owner/static-site-builder-input-old")
    assert len(calls) == 1


def test_sdk_placeholder_is_recorded_and_addressable_sources_are_protected(monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace
    def kernels_list(**kwargs):
        if kwargs["page"] != 1:
            return []
        return [{"ref": "owner/kernel"}, {"title": "[Private Notebook]",
                    "lastRunTime": "2010-04-01T00:00:00.000Z"}]
    response = SimpleNamespace(metadata={"ref": "owner/kernel",
        "datasetDataSources": ["owner/static-site-builder-input-pinned/3"]})
    client = SimpleNamespace(kernels=SimpleNamespace(kernels_api_client=SimpleNamespace(
        get_kernel=lambda request: response)))
    store = KaggleRetentionStore(SimpleNamespace(kernels_list=kernels_list,
        build_kaggle_client=lambda: nullcontext(client)))
    monkeypatch.setattr("kaggle_dataset_retention.time.sleep", lambda _: None)
    assert store.protected_notebook_inputs() == {"owner/static-site-builder-input-pinned"}
    assert store.redacted_notebook_rows == 1
