from __future__ import annotations

from guide_excursions.service import _display_rows_excluding_published_reference_clusters


def test_digest_preview_suppresses_candidates_clustered_with_published_references() -> None:
    display, suppressed = _display_rows_excluding_published_reference_clusters(
        [
            {"id": 140, "canonical_title": "Огонь Брюстерорта"},
            {"id": 141, "canonical_title": "Прогулка по Зеленоградску"},
        ],
        coverage_by_display_id={
            140: [140, 120],
            141: [141],
        },
        published_reference_ids={120},
        limit=24,
    )

    assert [row["id"] for row in display] == [141]
    assert suppressed == [140]


def test_digest_candidate_query_requires_iso_dates() -> None:
    import inspect

    from guide_excursions import service

    source = inspect.getsource(service._fetch_digest_candidates)

    assert "go.date GLOB '????-??-??'" in source

