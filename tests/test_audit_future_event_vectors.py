from types import SimpleNamespace

from scripts.inspect.audit_future_event_vectors import reuse_matching_vectors


def test_reuse_matching_vectors_rejects_stale_or_unversioned_rows():
    current = {1: SimpleNamespace(sha256="current-1"), 2: SimpleNamespace(sha256="current-2")}
    stored = {
        1: ([1.0, 0.0], "current-1"),
        2: ([0.0, 1.0], "stale-2"),
        3: ([0.5, 0.5], "current-3"),
        4: ([0.5, 0.5], ""),
    }

    assert reuse_matching_vectors(stored, current) == {1: [1.0, 0.0]}
