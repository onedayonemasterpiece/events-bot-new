from datetime import date

from festival_web_research.selection import (
    build_input_fingerprint, group_current_url_rows, select_current_url_rows,
)


def test_selection_uses_pending_url_and_explicit_period_only() -> None:
    rows = [
        {"id": 1, "status": "pending", "source_kind": "url", "url": "https://example.org/f", "start_date": "2026-08-01", "end_date": "2026-08-03"},
        {"id": 2, "status": "pending", "source_kind": "url", "url": "https://example.org/old", "event_date": "2026-07-01"},
        {"id": 3, "status": "done", "source_kind": "url", "url": "https://example.org/x", "event_date": "2026-08-01"},
        {"id": 4, "status": "pending", "source_kind": "vk", "url": "https://example.org/x", "event_date": "2026-08-01"},
        {"id": 5, "status": "pending", "source_kind": "url", "url": "https://example.org/no-date", "title": "Festival tomorrow"},
    ]
    selected, rejected = select_current_url_rows(rows, cutoff=date(2026, 7, 31))
    assert [row.queue_item_id for row in selected] == [1]
    assert rejected == {2: "stale_explicit_period", 3: "not_pending_url", 4: "not_pending_url", 5: "invalid_or_ambiguous:no explicit event period"}


def test_ambiguous_periods_fail_closed() -> None:
    selected, rejected = select_current_url_rows([{
        "id": 7, "status": "pending", "source_kind": "url", "url": "https://example.org/",
        "explicit_date_signals": [{"date": "2026-08-01"}, {"date": "2026-09-01"}],
    }], cutoff=date(2026, 7, 31))
    assert selected == []
    assert "ambiguous explicit event periods" in rejected[7]


def test_grouping_uses_only_supplied_identity_hints() -> None:
    common = {"status": "pending", "source_kind": "url", "event_date": "2026-08-01"}
    rows = [
        {**common, "id": 1, "url": "https://example.org/a", "series_identity_hint": " My Fest ", "edition_identity_hint": "2026"},
        {**common, "id": 2, "url": "https://example.org/b", "series_identity_hint": "my fest", "edition_identity_hint": "2026"},
        {**common, "id": 3, "url": "https://example.org/my-fest-2026"},
        {**common, "id": 4, "url": "https://example.org/my-fest-2026"},
    ]
    selected, _ = select_current_url_rows(rows, cutoff=date(2026, 7, 31))
    groups = group_current_url_rows(
        selected, contract_version="v1", taxonomy_sha256="a" * 64,
        prompt_sha256="b" * 64, normalizer_version="n1",
    )
    assert [group.queue_item_ids for group in groups] == [[1, 2], [3], [4]]
    assert groups[0].target_key == "hint:my fest:2026"
    assert groups[1].target_key == "unresolved:queue:3"


def test_fingerprint_is_stable_but_binds_snapshot_and_versions() -> None:
    selected, _ = select_current_url_rows([{
        "id": 1, "status": "pending", "source_kind": "url", "url": "https://example.org/?b=2&a=1",
        "event_date": "2026-08-01", "snapshot_sha256": "a" * 64,
    }], cutoff=date(2026, 7, 31))
    kwargs = dict(target_key="t", rows=selected, contract_version="v1", taxonomy_sha256="b"*64, prompt_sha256="c"*64, normalizer_version="n1")
    first = build_input_fingerprint(**kwargs)
    assert first == build_input_fingerprint(**kwargs)
    changed = build_input_fingerprint(**{**kwargs, "contract_version": "v2"})
    assert changed != first
    assert selected[0].canonical_url == "https://example.org/?a=1&b=2"
