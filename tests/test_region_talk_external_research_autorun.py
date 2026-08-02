from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import region_talk_external_research_autorun as autorun


def empty_result() -> dict:
    return {
        "schema_version": "region_talk_external_research.v1",
        "run": {
            "request_id": "region-talk-external-2026-07-31-170000",
            "executed_at": "2026-07-31T17:00:00Z",
            "window_start": "2025-01-01",
            "window_end": "2026-07-31",
            "research_languages": ["ru", "en"],
            "product_language_policy": "ru_or_mostly_ru",
            "scope_note": "bounded qualitative web research, not exhaustive",
        },
        "coverage": [],
        "candidates": [],
        "excluded": [],
        "unresolved": [],
        "run_uncertainties": [],
    }


def test_build_prompt_preserves_external_source_rule_and_incremental_caps(tmp_path: Path) -> None:
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("Source must be external/nonregional.", encoding="utf-8")

    prompt = autorun.build_prompt(
        prompt_path=prompt_path,
        maximum_candidates=5,
        maximum_total_rows=15,
    )

    assert "external/nonregional" in prompt
    assert "at most 5 clean candidates" in prompt
    assert "at most 15 total" in prompt
    assert "do not lower" in prompt


@pytest.mark.asyncio
async def test_run_uses_grounded_tools_and_validates_contract(tmp_path: Path) -> None:
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("Open the live registry.", encoding="utf-8")

    class Client:
        provider_timeout_seconds = 0

        def __init__(self) -> None:
            self.call = None

        async def generate_content_async(self, **kwargs):
            self.call = kwargs
            return json.dumps(empty_result(), ensure_ascii=False), SimpleNamespace(
                input_tokens=100,
                output_tokens=200,
                total_tokens=300,
            )

    client = Client()
    result = await autorun.run_autoresearch(
        execute=False,
        force=True,
        input_path=None,
        prompt_path=prompt_path,
        output_dir=tmp_path / "out",
        marker_path=tmp_path / "marker.json",
        model="gemini-3.1-flash-lite",
        default_key_env="GOOGLE_API_KEY3",
        maximum_candidates=5,
        maximum_total_rows=15,
        cooldown_hours=6,
        client=client,
    )

    assert result["status"] == "validated"
    assert result["candidate_rows_received"] == 0
    assert result["written_ydb_rows"] == 0
    assert result["usage"]["total_tokens"] == 300
    assert Path(result["raw_path"]).stat().st_mode & 0o777 == 0o600
    assert client.call["generation_config"]["tools"] == [
        {"google_search": {}},
        {"url_context": {}},
    ]
    assert client.call["generation_config"]["response_mime_type"] == "application/json"


@pytest.mark.asyncio
async def test_fresh_marker_skips_without_calling_provider(tmp_path: Path) -> None:
    marker = tmp_path / "marker.json"
    marker.write_text(
        json.dumps({"completed_at": datetime.now(timezone.utc).isoformat()}),
        encoding="utf-8",
    )

    class Client:
        async def generate_content_async(self, **kwargs):  # pragma: no cover - must not run
            raise AssertionError("provider should not be called during cooldown")

    result = await autorun.run_autoresearch(
        execute=True,
        force=False,
        input_path=None,
        prompt_path=tmp_path / "missing-prompt.txt",
        output_dir=tmp_path / "out",
        marker_path=marker,
        model="gemini-3.1-flash-lite",
        default_key_env="GOOGLE_API_KEY3",
        maximum_candidates=5,
        maximum_total_rows=15,
        cooldown_hours=6,
        client=Client(),
    )

    assert result["status"] == "skipped_cooldown"


@pytest.mark.asyncio
async def test_execute_receipt_exposes_sorted_new_replay_conflict_fields_and_exact_bytes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    raw_bytes = (json.dumps(empty_result(), ensure_ascii=False, indent=2) + "\r\n").encode("utf-8")
    input_path = tmp_path / "input.json"
    input_path.write_bytes(raw_bytes)
    monkeypatch.setattr(autorun, "read_seen_from_ydb", lambda _limit: [])
    monkeypatch.setattr(autorun, "publish_current_registry", lambda *, seen_limit: {
        "seen_publication_count": 0,
        "seen_limit": seen_limit,
    })
    monkeypatch.setattr(autorun, "execute_import", lambda _prepared: {
        "status": "committed",
        "written_ydb_rows": 1,
        "new_intake_count": 2,
        "new_intake_ids": ["extpub_b", "extpub_a"],
        "replay_count": 1,
        "replay_ids": ["extpub_old"],
        "conflict_count": 0,
    })

    result = await autorun.run_autoresearch(
        execute=True,
        force=True,
        input_path=input_path,
        prompt_path=tmp_path / "unused.txt",
        output_dir=tmp_path / "out",
        marker_path=tmp_path / "marker.json",
        model="unused",
        default_key_env="UNUSED",
        maximum_candidates=5,
        maximum_total_rows=15,
        cooldown_hours=6,
    )

    import hashlib

    assert result["input_json_sha256"] == hashlib.sha256(raw_bytes).hexdigest()
    assert result["new_intake_count"] == 2
    assert result["new_intake_ids"] == ["extpub_a", "extpub_b"]
    assert result["replay_ids"] == ["extpub_old"]
    assert result["conflict_count"] == 0
    assert Path(result["raw_path"]).read_bytes() == raw_bytes


@pytest.mark.asyncio
async def test_rejected_execute_batch_is_not_success_and_writes_nothing(tmp_path: Path, monkeypatch) -> None:
    payload = empty_result()
    payload["candidates"] = [{}]
    input_path = tmp_path / "invalid-row.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(autorun, "read_seen_from_ydb", lambda _limit: [])
    monkeypatch.setattr(autorun, "execute_import", lambda _prepared: pytest.fail("writer must not run"))
    monkeypatch.setattr(
        autorun,
        "publish_current_registry",
        lambda **_kwargs: pytest.fail("registry must not publish"),
    )

    result = await autorun.run_autoresearch(
        execute=True,
        force=True,
        input_path=input_path,
        prompt_path=tmp_path / "unused.txt",
        output_dir=tmp_path / "out",
        marker_path=tmp_path / "marker.json",
        model="unused",
        default_key_env="UNUSED",
        maximum_candidates=5,
        maximum_total_rows=15,
        cooldown_hours=6,
    )

    assert result["ok"] is False
    assert result["status"] == "rejected_no_write"
    assert result["written_ydb_rows"] == 0
    assert result["new_intake_count"] == 0
    assert not (tmp_path / "marker.json").exists()


@pytest.mark.asyncio
async def test_execute_identity_conflict_is_visible_and_not_marked_success(tmp_path: Path, monkeypatch) -> None:
    input_path = tmp_path / "empty.json"
    input_path.write_text(json.dumps(empty_result()), encoding="utf-8")
    monkeypatch.setattr(autorun, "read_seen_from_ydb", lambda _limit: [])
    monkeypatch.setattr(
        autorun,
        "execute_import",
        lambda _prepared: (_ for _ in ()).throw(autorun.ContractError("identity reservation conflict")),
    )
    monkeypatch.setattr(
        autorun,
        "publish_current_registry",
        lambda **_kwargs: pytest.fail("registry must not publish"),
    )

    result = await autorun.run_autoresearch(
        execute=True,
        force=True,
        input_path=input_path,
        prompt_path=tmp_path / "unused.txt",
        output_dir=tmp_path / "out",
        marker_path=tmp_path / "marker.json",
        model="unused",
        default_key_env="UNUSED",
        maximum_candidates=5,
        maximum_total_rows=15,
        cooldown_hours=6,
    )

    assert result["ok"] is False
    assert result["status"] == "conflict_no_write"
    assert result["conflict_count"] == 1
    assert "identity reservation conflict" in result["execution_error"]
    assert result["written_ydb_rows"] == 0
