from scripts.inspect.audit_media_dedup import (
    PosterRow,
    _local_anomaly_count,
    _scan_poster_rows,
)


def _row(*, row_id: int, path: str, raw_sha256: str | None, phash: str | None):
    return PosterRow(
        id=row_id,
        event_id=100 + row_id,
        poster_hash=f"source-{row_id}",
        phash=phash,
        raw_sha256=raw_sha256,
        supabase_path=path,
        supabase_url=f"https://static.kenigevents.ru/{path}",
        updated_at=None,
    )


def test_poster_audit_distinguishes_exact_v2_from_legacy_and_checks_digest() -> None:
    exact = "a" * 64
    wrong = "b" * 64
    legacy = "c" * 64
    stats = _scan_poster_rows(
        [
            _row(
                row_id=1,
                path=f"p/image/v2/aa/{exact}.webp",
                raw_sha256=exact,
                phash="d" * 64,
            ),
            _row(
                row_id=2,
                path=f"p/image/v2/bb/{wrong}.webp",
                raw_sha256="e" * 64,
                phash="f" * 64,
            ),
            _row(
                row_id=3,
                path=f"p/dh16/cc/{legacy}.webp",
                raw_sha256=None,
                phash=legacy,
            ),
        ],
        near_threshold=-1,
        near_max_pairs=0,
    )

    assert stats["exact_v2_paths"] == 2
    assert stats["legacy_dh16_paths"] == 1
    assert stats["noncanonical_paths"] == 0
    assert len(stats["encoded_sha_mismatch"]) == 1
    assert stats["encoded_sha_missing"] == []
    assert stats["phash_mismatch"] == []
    assert _local_anomaly_count(
        stats, {"sha_mismatches": [], "sha256_multi_paths": []}
    ) >= 2  # one exact digest mismatch plus one recent legacy path
