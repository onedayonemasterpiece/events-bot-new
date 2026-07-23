from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
BUILDER_PATH = ROOT / "kaggle/StaticSiteBuilder/static_site_builder.py"


def load_builder(name: str):
    spec = importlib.util.spec_from_file_location(name, BUILDER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_duration_enrichment_is_between_export_and_every_astro_build() -> None:
    source = BUILDER_PATH.read_text(encoding="utf-8")
    export_position = source.index("export_preview_data_if_configured(config)")
    duration_position = source.index("duration_enrichment = enrich_event_durations(config)")
    node_position = source.index("env = ensure_node22(env)")
    production_position = source.index("run(['npm', 'run', 'build:production']")
    candidate_position = source.index("run(['npm', 'run', 'build:secret-candidate']")
    preview_position = source.index("run(['npm', 'run', 'build:preview']")

    assert export_position < duration_position < node_position
    assert duration_position < min(production_position, candidate_position, preview_position)


def test_builder_duration_command_is_bounded_and_never_contains_secret_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = load_builder("static_site_builder_duration_command_test")
    site = tmp_path / "site"
    (site / "scripts").mkdir(parents=True)
    (site / "src/data").mkdir(parents=True)
    (site / "scripts/enrich-event-duration-estimates.py").write_text("# fixture\n", encoding="utf-8")
    output_path = site / "src/data/event-duration-estimates.json"
    commands: list[tuple[list[str], dict[str, str]]] = []

    def fake_run(command, *, env, **_kwargs):
        commands.append((command, env))
        output_path.write_text(json.dumps({
            "version": 2,
            "scope": "build_time",
            "estimates": [{"event_id": 6529}],
            "failures": [],
        }), encoding="utf-8")

    monkeypatch.setattr(builder, "SITE_DIR", site)
    monkeypatch.setattr(builder, "ensure_python_deps_for_gemma", lambda _config: None)
    monkeypatch.setattr(builder, "load_encrypted_secrets_to_env", lambda: None)
    monkeypatch.setattr(builder, "load_kaggle_secret_to_env", lambda _name: None)
    monkeypatch.setattr(builder, "run", fake_run)
    monkeypatch.setenv("GOOGLE_API_KEY4", "must-not-appear-in-command")

    summary = builder.enrich_event_durations({
        "duration_enrichment": True,
        "duration_model": "gemini-3.1-flash-lite",
        "duration_key_envs": "GOOGLE_API_KEY4",
        "duration_max_events": 7,
    })

    assert summary == {
        "enabled": True,
        "status": "ok",
        "model": "gemini-3.1-flash-lite",
        "estimate_count": 1,
        "failure_count": 0,
        "candidate_limit": 7,
    }
    command, env = commands[0]
    assert command[command.index("--model") + 1] == "gemini-3.1-flash-lite"
    assert command[command.index("--key-envs") + 1] == "GOOGLE_API_KEY4"
    assert command[command.index("--max-events") + 1] == "7"
    assert "must-not-appear-in-command" not in " ".join(command)
    assert env["PYTHONPATH"].split(":")[0] == str(site)


def test_kaggle_builder_cache_hit_needs_no_provider_call_or_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = load_builder("static_site_builder_duration_cache_test")
    site = tmp_path / "site"
    (site / "scripts").mkdir(parents=True)
    (site / "src/data").mkdir(parents=True)
    for relative in [
        "scripts/enrich-event-duration-estimates.py",
        "src/data/preview-events.json",
        "src/data/transportSchedules.json",
        "src/data/event-duration-estimates.json",
    ]:
        shutil.copy2(ROOT / "site" / relative, site / relative)
    events_path = site / "src/data/preview-events.json"
    events_payload = json.loads(events_path.read_text(encoding="utf-8"))
    events_payload["events"] = [
        event for event in events_payload["events"] if int(event["id"]) == 6529
    ]
    events_path.write_text(
        json.dumps(events_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    output_path = site / "src/data/event-duration-estimates.json"
    before = hashlib.sha256(output_path.read_bytes()).hexdigest()

    monkeypatch.setattr(builder, "SITE_DIR", site)
    monkeypatch.setattr(builder, "ensure_python_deps_for_gemma", lambda _config: None)
    monkeypatch.setattr(builder, "load_encrypted_secrets_to_env", lambda: None)
    monkeypatch.setattr(builder, "load_kaggle_secret_to_env", lambda _name: None)
    for name in ["GOOGLE_API_KEY", "GOOGLE_API_KEY2", "GOOGLE_API_KEY3", "GOOGLE_API_KEY4", "GOOGLE_API_KEY5"]:
        monkeypatch.delenv(name, raising=False)

    summary = builder.enrich_event_durations({
        "duration_enrichment": True,
        "duration_model": "gemini-3.1-flash-lite",
        "duration_key_envs": "GOOGLE_API_KEY4",
        "duration_max_events": 1,
        "duration_require_complete": True,
    })

    assert summary["status"] == "ok"
    assert summary["estimate_count"] == 1
    assert hashlib.sha256(output_path.read_bytes()).hexdigest() == before


def test_duration_config_rejects_unbounded_or_unsafe_values() -> None:
    builder = load_builder("static_site_builder_duration_contract_test")
    with pytest.raises(ValueError, match="max events"):
        builder.duration_enrichment_contract({"duration_max_events": 51})
    with pytest.raises(ValueError, match="key env"):
        builder.duration_enrichment_contract({"duration_key_envs": "GOOGLE_API_KEY4,$SECRET"})
    with pytest.raises(ValueError, match="model id"):
        builder.duration_enrichment_contract({"duration_model": "../../bad model"})


def test_runner_encrypted_payload_includes_duration_key_and_limiter_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.run_static_site_builder_kaggle as runner

    monkeypatch.setenv("GOOGLE_API_KEY4", "duration-key")
    monkeypatch.setenv("SUPABASE_URL", "https://limiter.invalid")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "limiter-key")
    args = SimpleNamespace(
        duration_enrichment=True,
        duration_key_envs="GOOGLE_API_KEY4",
        gemma_related_verify=False,
        gemma_related_key_env="GOOGLE_API_KEY4",
        related_mode="sparse",
        sync_pgvector_vectors=False,
        pgvector_embedding_key_env="GOOGLE_API_KEY4",
    )

    payload = runner.build_runtime_secret_payload(args)

    assert payload["GOOGLE_API_KEY4"] == "duration-key"
    assert payload["SUPABASE_URL"] == "https://limiter.invalid"
    assert payload["SUPABASE_SERVICE_KEY"] == "limiter-key"
