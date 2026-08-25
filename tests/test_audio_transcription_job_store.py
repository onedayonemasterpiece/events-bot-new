import asyncio
from types import SimpleNamespace

import pytest

from audio_transcription.asset_store import AudioAssetStore
from audio_transcription.contracts import JobState
from audio_transcription.job_store import AudioJobStore, JobOwnershipError
from audio_transcription.kaggle_backend import DispatchReceipt, ReconcileResult
from audio_transcription.service import AudioTranscriptionService


def test_job_store_is_idempotent_and_owner_bound(tmp_path):
    store = AudioJobStore(tmp_path / "jobs.sqlite3")
    owner = "a" * 64
    first, created = store.create(
        owner_binding=owner,
        idempotency_key="lecture-2026-08-12",
        asset_ref="aud_example",
        request={"precision": "phrase"},
    )
    assert created is True
    second, created_again = store.create(
        owner_binding=owner,
        idempotency_key="lecture-2026-08-12",
        asset_ref="aud_other",
        request={"precision": "segment"},
    )
    assert created_again is False
    assert second.job_ref == first.job_ref

    updated = store.update(
        first.job_ref,
        state=JobState.RUNNING,
        kernel_ref="user/kernel",
        progress={"phase": "run", "progress_percent": 50},
    )
    assert updated.state is JobState.RUNNING
    assert updated.kernel_ref == "user/kernel"
    assert store.active_jobs() == (updated,)

    with pytest.raises(JobOwnershipError):
        store.get(first.job_ref, owner_binding="b" * 64)


@pytest.mark.asyncio
async def test_provider_ingress_checks_durable_job_before_loading_bytes(tmp_path):
    root = tmp_path / "audio-runtime"
    assets = AudioAssetStore(
        root / "assets",
        allowed_hosts=("files.example.test",),
        max_asset_bytes=1024,
        max_store_bytes=4096,
        ttl_seconds=60,
        timeout_seconds=5,
    )
    jobs = AudioJobStore(root / "jobs.sqlite3")
    service = AudioTranscriptionService(
        SimpleNamespace(root=root, result_root=root / "results"),
        asset_store=assets,
        job_store=jobs,
        backend=object(),
    )
    # Keep this unit test at the ingress boundary; dispatch is covered by the
    # existing backend/runtime suites.
    service._schedule_dispatch = lambda _job: None  # type: ignore[method-assign]
    loads = 0

    async def load():
        nonlocal loads
        loads += 1
        await asyncio.sleep(0)
        return b"OggS" + b"\0" * 32

    values = {
        "owner_binding": "a" * 64,
        "idempotency_key": "tg:" + "c" * 64,
        "provider_fingerprint": "b" * 64,
        "content_loader": load,
        "mime_type": "audio/ogg",
    }
    first, second = await asyncio.gather(
        service.start_provider_transcription(**values),
        service.start_provider_transcription(**values),
    )

    assert first["job_ref"] == second["job_ref"]
    assert {first["created"], second["created"]} == {True, False}
    assert loads == 1
    stored = jobs.find_by_idempotency(
        owner_binding=values["owner_binding"],
        idempotency_key=values["idempotency_key"],
    )
    assert stored is not None
    assert stored.request["source_sha256"]
    assert stored.request["source_binding"] != values["provider_fingerprint"]


@pytest.mark.asyncio
async def test_provider_ingress_first_status_observes_instant_ready_backend(tmp_path):
    root = tmp_path / "audio-runtime"
    result_root = root / "results"

    class InstantBackend:
        async def dispatch(self, job):
            return DispatchReceipt("test/kernel", "input/ref", "key/ref")

        async def reconcile(self, job):
            result_dir = result_root / job.job_ref
            result_dir.mkdir(parents=True)
            (result_dir / "transcript.json").write_text(
                '{"segments": [], "source": {"kind": "test"}}',
                encoding="utf-8",
            )
            (result_dir / "manifest.json").write_text("{}", encoding="utf-8")
            (result_dir / "transcript.txt").write_text("ready now", encoding="utf-8")
            return ReconcileResult(
                state=JobState.COMPLETE,
                progress={"phase": "complete", "progress_percent": 100},
                result_dir=str(result_dir),
            )

    assets = AudioAssetStore(
        root / "assets",
        allowed_hosts=("files.example.test",),
        max_asset_bytes=1024,
        max_store_bytes=4096,
        ttl_seconds=60,
        timeout_seconds=5,
    )
    service = AudioTranscriptionService(
        SimpleNamespace(root=root, result_root=result_root),
        asset_store=assets,
        job_store=AudioJobStore(root / "jobs.sqlite3"),
        backend=InstantBackend(),
    )
    owner = "a" * 64
    started = await service.start_provider_transcription(
        owner_binding=owner,
        idempotency_key="tg:" + "b" * 64,
        provider_fingerprint="c" * 64,
        content_loader=lambda: b"OggS" + b"\0" * 32,
        mime_type="audio/ogg",
    )

    status = await service.status(job_ref=started["job_ref"], owner_binding=owner)
    result = await service.get_result(
        job_ref=started["job_ref"],
        owner_binding=owner,
        view="plain",
        offset=0,
        limit=100,
    )

    assert status["state"] == "complete"
    assert status["result_available"] is True
    assert result["ready"] is True
    assert result["text"] == "ready now"
    await service.close()
