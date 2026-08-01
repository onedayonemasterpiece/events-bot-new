from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_both_stable_flash_lite_models_have_safe_limiter_rows() -> None:
    migrations = [
        ROOT / "migrations" / "010_google_ai_gemini_35_flash_lite_limits.sql",
        ROOT / "supabase" / "migrations" / "20260801191005_google_ai_gemini_35_flash_lite_limits.sql",
    ]
    expected_rows = [
        "('gemini-3.5-flash-lite', 13, 240000, 450, 1000)",
        "('gemini-3.1-flash-lite', 13, 240000, 450, 1000)",
    ]
    for migration in migrations:
        source = migration.read_text(encoding="utf-8")
        for row in expected_rows:
            assert row in source
        assert "ON CONFLICT (model) DO UPDATE" in source
