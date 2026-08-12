from __future__ import annotations

import json

import pytest

import kaggle_registry


@pytest.fixture(autouse=True)
def _isolated_registry(tmp_path, monkeypatch):
    monkeypatch.setattr(kaggle_registry, "_REGISTRY_PATH", tmp_path / "jobs.json")


@pytest.mark.asyncio
async def test_launch_intent_is_durable_and_visible_before_remote_push():
    await kaggle_registry.register_launch_intent(
        "tg_monitoring", "run-1", meta={"kernel_ref_hint": "owner/kernel"}
    )

    intents = await kaggle_registry.list_launch_intents("tg_monitoring")

    assert [item["run_id"] for item in intents] == ["run-1"]
    assert intents[0]["state"] == "prepared"
    await kaggle_registry.remove_launch_intent("tg_monitoring", "run-1")
    assert await kaggle_registry.list_launch_intents("tg_monitoring") == []


@pytest.mark.asyncio
async def test_corrupt_registry_fails_closed_instead_of_becoming_empty():
    kaggle_registry._REGISTRY_PATH.write_text("{broken", encoding="utf-8")

    with pytest.raises(kaggle_registry.KaggleRegistryError):
        await kaggle_registry.list_jobs()


@pytest.mark.asyncio
async def test_job_registration_preserves_pending_launch_intent():
    await kaggle_registry.register_launch_intent("guide_monitoring", "run-1")
    await kaggle_registry.register_job(
        "guide_monitoring", "owner/kernel", meta={"run_id": "run-1"}
    )

    raw = json.loads(kaggle_registry._REGISTRY_PATH.read_text(encoding="utf-8"))

    assert len(raw["jobs"]) == 1
    assert len(raw["launch_intents"]) == 1
