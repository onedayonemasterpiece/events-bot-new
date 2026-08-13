from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

import kaggle_registry


@pytest.fixture(autouse=True)
def _isolated_registry(tmp_path, monkeypatch):
    monkeypatch.setattr(kaggle_registry, "_REGISTRY_PATH", tmp_path / "jobs.json")


def _intent_meta() -> dict:
    return {
        "kernel_ref_hint": "owner/kernel",
        "dataset_slugs": ["owner/config-run-1", "owner/key-run-1"],
        "remote_revision_before_push": 4,
    }


@pytest.mark.asyncio
async def test_launch_intent_is_durable_and_visible_before_remote_push():
    await kaggle_registry.register_launch_intent(
        "tg_monitoring", "run-1", meta=_intent_meta()
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
@pytest.mark.parametrize(
    "dataset_slugs",
    [
        [],
        ["owner/one"],
        ["owner/one", "owner/one"],
        ["owner/one", ""],
        ["owner/one", "owner/two", "owner/three"],
    ],
)
async def test_launch_intent_requires_two_nonempty_unique_datasets(dataset_slugs):
    meta = _intent_meta()
    meta["dataset_slugs"] = dataset_slugs
    with pytest.raises(kaggle_registry.KaggleRegistryError, match="two unique"):
        await kaggle_registry.register_launch_intent(
            "tg_monitoring", "run-1", meta=meta
        )


@pytest.mark.asyncio
async def test_launch_intent_requires_positive_exact_revision_baseline():
    meta = _intent_meta()
    meta["remote_revision_before_push"] = 0
    with pytest.raises(kaggle_registry.KaggleRegistryError, match="revision baseline"):
        await kaggle_registry.register_launch_intent(
            "tg_monitoring", "run-1", meta=meta
        )


@pytest.mark.asyncio
async def test_job_registration_preserves_pending_launch_intent():
    await kaggle_registry.register_launch_intent(
        "guide_monitoring", "run-1", meta=_intent_meta()
    )
    await kaggle_registry.register_job(
        "guide_monitoring", "owner/kernel", meta={"run_id": "run-1"}
    )

    raw = json.loads(kaggle_registry._REGISTRY_PATH.read_text(encoding="utf-8"))

    assert len(raw["jobs"]) == 1
    assert len(raw["launch_intents"]) == 1


@pytest.mark.asyncio
async def test_atomic_promotion_has_no_intent_job_gap():
    await kaggle_registry.register_launch_intent(
        "guide_monitoring", "run-1", meta=_intent_meta()
    )

    await kaggle_registry.promote_launch_intent(
        "guide_monitoring", "run-1", "owner/kernel", meta={"mode": "full"}
    )

    raw = json.loads(kaggle_registry._REGISTRY_PATH.read_text(encoding="utf-8"))
    assert raw["launch_intents"] == []
    assert raw["jobs"][0]["meta"]["run_id"] == "run-1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"jobs": ["corrupt"], "launch_intents": []},
        {"jobs": [], "launch_intents": ["corrupt"]},
        {
            "jobs": [{"id": "j", "type": "t", "kernel_ref": "k", "meta": {}}],
            "launch_intents": [],
        },
    ],
)
async def test_parsed_but_schema_corrupt_registry_fails_closed(payload):
    kaggle_registry._REGISTRY_PATH.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(kaggle_registry.KaggleRegistryError):
        await kaggle_registry.list_jobs()


@pytest.mark.asyncio
async def test_indeterminate_intent_promotes_only_on_exact_remote_datasets():
    await kaggle_registry.register_launch_intent(
        "tg_monitoring",
        "run-1",
        meta={
            **_intent_meta(),
        },
    )
    await kaggle_registry.mark_launch_intent_indeterminate(
        "tg_monitoring", "run-1", error=TimeoutError("ambiguous")
    )

    class Client:
        def kernel_has_dataset_sources(self, kernel_ref, expected):
            assert kernel_ref == "owner/kernel"
            return True, {"dataset_sources": list(expected)}

    outcomes = await kaggle_registry.reconcile_launch_intents(
        "tg_monitoring", client=Client()
    )

    assert outcomes == [{"run_id": "run-1", "status": "promoted"}]
    assert await kaggle_registry.list_launch_intents("tg_monitoring") == []
    assert (await kaggle_registry.list_jobs("tg_monitoring"))[0]["meta"]["run_id"] == "run-1"


@pytest.mark.asyncio
async def test_pre_submit_crash_clears_only_after_remote_no_advance_evidence():
    await kaggle_registry.register_launch_intent(
        "guide_monitoring",
        "run-1",
        meta={
            **_intent_meta(),
        },
    )
    created_at = datetime.fromisoformat(
        (await kaggle_registry.list_launch_intents())[0]["created_at"]
    )
    deleted = []

    class Client:
        def kernel_has_dataset_sources(self, _kernel_ref, _expected):
            return False, {"dataset_sources": ["owner/previous"]}

        def get_kernel_revision(self, _kernel_ref):
            return 4

        def delete_dataset(self, slug):
            deleted.append(slug)

    outcomes = await kaggle_registry.reconcile_launch_intents(
        "guide_monitoring",
        client=Client(),
        now=created_at + timedelta(hours=1),
    )

    assert outcomes == [{"run_id": "run-1", "status": "not_submitted"}]
    assert deleted == ["owner/config-run-1", "owner/key-run-1"]
    assert await kaggle_registry.list_launch_intents() == []


@pytest.mark.asyncio
async def test_remote_advance_with_different_identity_stays_indeterminate():
    await kaggle_registry.register_launch_intent(
        "guide_monitoring",
        "run-1",
        meta={
            **_intent_meta(),
        },
    )
    created_at = datetime.fromisoformat(
        (await kaggle_registry.list_launch_intents())[0]["created_at"]
    )

    class Client:
        def kernel_has_dataset_sources(self, _kernel_ref, _expected):
            return False, {"dataset_sources": ["owner/operator-run"]}

        def get_kernel_revision(self, _kernel_ref):
            return 5

    outcomes = await kaggle_registry.reconcile_launch_intents(
        "guide_monitoring",
        client=Client(),
        now=created_at + timedelta(hours=1),
    )

    assert outcomes == [
        {
            "run_id": "run-1",
            "status": "indeterminate_remote_advanced",
            "baseline": 4,
            "remote_revision": 5,
        }
    ]
    assert len(await kaggle_registry.list_launch_intents()) == 1


@pytest.mark.asyncio
async def test_missing_remote_ref_never_clears_ambiguous_intent():
    await kaggle_registry.register_launch_intent(
        "guide_monitoring", "run-1", meta=_intent_meta()
    )
    created_at = datetime.fromisoformat(
        (await kaggle_registry.list_launch_intents())[0]["created_at"]
    )

    class MissingResponse:
        status_code = 404

    class NotFound(RuntimeError):
        response = MissingResponse()

    class Client:
        def kernel_has_dataset_sources(self, _kernel_ref, _expected):
            raise NotFound("404")

    outcomes = await kaggle_registry.reconcile_launch_intents(
        "guide_monitoring",
        client=Client(),
        now=created_at + timedelta(hours=1),
    )

    assert outcomes == [
        {"run_id": "run-1", "status": "indeterminate", "error": "NotFound"}
    ]
    assert len(await kaggle_registry.list_launch_intents()) == 1
