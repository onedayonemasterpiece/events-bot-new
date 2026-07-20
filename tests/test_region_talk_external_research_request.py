from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_external_research_request.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_external_research_request", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_seen_snapshot_merges_new_ledger_and_preledger_intake() -> None:
    mod = load_module()
    rows = mod.build_seen_publications(
        seen_rows=[{
            "canonical_url": "https://example.org/excluded?utm_source=x",
            "title": "Excluded",
            "seen_disposition": "excluded",
        }],
        intake_rows=[{
            "canonical_url": "https://example.org/article#top",
            "doi": "doi:10.1234/ABC",
            "publication": {"title": "Article", "source_name": "Journal"},
            "decision": {"import_status": "ready_for_region_talk_scoring"},
        }],
    )

    assert len(rows) == 2
    assert any(row["doi"] == "10.1234/abc" and row["disposition"] == "candidate" for row in rows)
    assert any(row["canonical_url"] == "https://example.org/excluded" for row in rows)


def test_request_snapshot_is_deterministic_and_schema_valid() -> None:
    mod = load_module()
    kwargs = dict(
        request_id="region-talk-2026-07-20-02",
        as_of_date="2026-07-20",
        window_start="2025-01-01",
        window_end="2026-07-20",
        research_languages=["ru", "en"],
        product_language_policy="ru_or_mostly_ru",
        maximum_candidates=30,
        maximum_candidates_per_contour=5,
        blocked_domains=[],
        seen_publications=[{
            "canonical_url": "https://archi.ru/russia/101203/vsya-mudrost-okeana",
            "doi": None,
            "title": "Вся мудрость океана",
            "source_name": "Архи.ру",
            "disposition": "candidate",
        }],
        generated_at="2026-07-20T12:00:00+00:00",
    )
    first = mod.build_request(**kwargs)
    second = mod.build_request(**kwargs)

    assert first == second
    assert first["duplicate_guard"]["seen_publication_count"] == 1
    assert first["duplicate_guard"]["snapshot_id"].startswith("rtseen_")
    assert json.loads(json.dumps(first, ensure_ascii=False)) == first
