from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_antigravity_limit_migration_uses_safe_caps() -> None:
    migration = (
        ROOT / "migrations" / "006_google_ai_antigravity_limits.sql"
    ).read_text(encoding="utf-8")

    assert "antigravity-preview-05-2026" in migration
    assert "60 RPM, 100000 TPM, 100 RPD" in migration
    assert "('antigravity-preview-05-2026', 54, 96000, 90, 1000)" in migration


def test_antigravity_docs_distinguish_tpm_from_per_request_budget() -> None:
    docs = (
        ROOT / "docs" / "features" / "llm-gateway" / "README.md"
    ).read_text(encoding="utf-8")

    assert "`100000 TPM` — минутная квота" in docs
    assert "`agent_config.max_total_tokens`" in docs
    assert "Structured output у Antigravity preview не поддерживается" in docs


def test_shared_reservation_is_serialized_per_key_and_model() -> None:
    migration = (ROOT / "migrations" / "008_google_ai_atomic_reserve.sql").read_text()
    assert "pg_advisory_xact_lock" in migration
    assert "v_key.id::text || ':' || p_model" in migration
    assert migration.count("WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no") >= 2
