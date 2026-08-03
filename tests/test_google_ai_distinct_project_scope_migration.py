from __future__ import annotations

import re
from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "supabase"
    / "migrations"
    / "20260803084716_google_ai_distinct_project_scopes.sql"
)


def test_distinct_project_scope_migration_maps_all_six_keys_one_to_one() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    pairs = re.findall(
        r"\('?(GOOGLE_API_KEY\d*)'?,\s*'([^']+operator-project-key\d+-20260803)'\)",
        sql,
    )

    assert {env for env, _scope in pairs} == {
        "GOOGLE_API_KEY",
        "GOOGLE_API_KEY2",
        "GOOGLE_API_KEY3",
        "GOOGLE_API_KEY4",
        "GOOGLE_API_KEY5",
        "GOOGLE_API_KEY6",
    }
    assert len({scope for _env, scope in pairs}) == 6
    assert "COUNT(DISTINCT quota_scope)" in sql
    assert "v_rows <> 6 OR v_scopes <> 6" in sql
    assert "google:unmapped-shared" not in sql


def test_scope_split_preserves_rolling_window_and_429_attribution() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    assert "LOCK TABLE google_ai_api_keys IN ACCESS EXCLUSIVE MODE" in sql
    assert "UPDATE google_ai_provider_cooldowns" in sql
    assert "source_api_key_id = k.id" in sql
    assert "UPDATE google_ai_request_attempts" in sql
    assert "UPDATE google_ai_requests" in sql
    assert sql.count("api_key_id = k.id") >= 3


def test_scope_split_contains_no_google_api_key_values() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    assert "AIza" not in sql
    assert "secret and project identifier remain outside the ledger" in sql
