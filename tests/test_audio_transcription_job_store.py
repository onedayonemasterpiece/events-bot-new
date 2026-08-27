import asyncio
import time
from types import SimpleNamespace

import pytest

from audio_transcription.asset_store import AudioAssetStore
from audio_transcription.contracts import JobState
from audio_transcription.job_store import AudioJobStore, JobOwnershipError
from audio_transcription.kaggle_backend import (
    DispatchReceipt,
    KaggleAudioBackend,
    ReconcileResult,
)
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
async def test_batch_snapshot_wait_and_get_fail_closed_for_mixed_owners(tmp_path):
    root = tmp_path / "audio-runtime"
    jobs = AudioJobStore(root / "jobs.sqlite3")
    owner_a = "a" * 64
    owner_b = "b" * 64
    job_a, _ = jobs.create(
        owner_binding=owner_a,
        idempotency_key="tg:" + "1" * 64,
        asset_ref="aud_owner_a",
        request={"source_sha256": "1" * 64},
    )
    job_b, _ = jobs.create(
        owner_binding=owner_b,
        idempotency_key="tg:" + "2" * 64,
        asset_ref="aud_owner_b",
        request={"source_sha256": "2" * 64},
    )
    service = AudioTranscriptionService(
        SimpleNamespace(
            root=root,
            result_root=root / "results",
            poll_interval_seconds=20,
        ),
        asset_store=object(),
        job_store=jobs,
        backend=object(),
    )

    own = await service.wait_many(
        owner_binding=owner_a,
        job_refs=[job_a.job_ref],
        wait_seconds=0,
    )
    assert [job.job_ref for job in own.jobs] == [job_a.job_ref]
    assert own.wait_expired is False
    assert own.next_poll_after_seconds == 20

    with pytest.raises(JobOwnershipError):
        service.snapshot_many(
            owner_binding=owner_a,
            job_refs=[job_a.job_ref, job_b.job_ref],
        )
    with pytest.raises(JobOwnershipError):
        await service.wait_many(
            owner_binding=owner_a,
            job_refs=[job_a.job_ref, job_b.job_ref],
            wait_seconds=0,
        )
    with pytest.raises(JobOwnershipError):
        await service.get_many(
            owner_binding=owner_a,
            job_refs=[job_a.job_ref, job_b.job_ref],
            limit=100,
        )


@pytest.mark.asyncio
async def test_batch_get_many_returns_reproducible_plain_continuation(tmp_path):
    root = tmp_path / "audio-runtime"
    result_root = root / "results"
    jobs = AudioJobStore(root / "jobs.sqlite3")
    owner = "a" * 64
    job, _ = jobs.create(
        owner_binding=owner,
        idempotency_key="tg:" + "3" * 64,
        asset_ref="aud_long_result",
        request={"source_sha256": "3" * 64},
    )
    result_dir = result_root / job.job_ref
    result_dir.mkdir(parents=True)
    full_text = "длинная расшифровка " * 100
    (result_dir / "transcript.txt").write_text(full_text, encoding="utf-8")
    jobs.update(job.job_ref, state=JobState.COMPLETE, result_dir=str(result_dir))
    service = AudioTranscriptionService(
        SimpleNamespace(
            root=root,
            result_root=result_root,
            poll_interval_seconds=20,
        ),
        asset_store=object(),
        job_store=jobs,
        backend=object(),
    )

    pages = await service.get_many(
        owner_binding=owner,
        job_refs=[job.job_ref],
        limit=37,
    )
    first = pages[job.job_ref]
    assert first["text"] == full_text[:37]
    assert first["next_offset"] == 37
    continuation = await service.get_result(
        job_ref=job.job_ref,
        owner_binding=owner,
        view="plain",
        offset=first["next_offset"],
        limit=60_000,
    )
    assert first["text"] + continuation["text"] == full_text


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


@pytest.mark.asyncio
async def test_dispatch_scheduler_keeps_multiple_durable_jobs_serial(tmp_path):
    root = tmp_path / "audio-runtime"
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingBackend:
        def __init__(self):
            self.calls = []

        async def dispatch(self, job):
            self.calls.append(job.job_ref)
            entered.set()
            await release.wait()
            return DispatchReceipt("test/kernel", "input/ref", "key/ref")

    backend = BlockingBackend()
    jobs = AudioJobStore(root / "jobs.sqlite3")
    owner = "a" * 64
    first, _ = jobs.create(
        owner_binding=owner,
        idempotency_key="tg:" + "1" * 64,
        asset_ref="aud_first",
        request={"source_sha256": "1" * 64},
    )
    second, _ = jobs.create(
        owner_binding=owner,
        idempotency_key="tg:" + "2" * 64,
        asset_ref="aud_second",
        request={"source_sha256": "2" * 64},
    )
    service = AudioTranscriptionService(
        SimpleNamespace(root=root, result_root=root / "results"),
        asset_store=object(),
        job_store=jobs,
        backend=backend,
    )

    service._schedule_dispatch(first)
    service._schedule_dispatch(second)
    await asyncio.wait_for(entered.wait(), timeout=1)
    await asyncio.sleep(0)

    assert backend.calls == [first.job_ref]
    assert jobs.get(second.job_ref, owner_binding=owner).state is JobState.QUEUED

    release.set()
    await asyncio.gather(*tuple(service._tasks.values()))
    assert jobs.get(first.job_ref, owner_binding=owner).state is JobState.RUNNING
    await service.close()


@pytest.mark.asyncio
async def test_shared_session_busy_applies_global_dispatch_backoff(tmp_path):
    root = tmp_path / "audio-runtime"

    class RemoteTelegramSessionBusyError(Exception):
        pass

    class BusyBackend:
        def __init__(self):
            self.calls = 0

        async def dispatch(self, _job):
            self.calls += 1
            raise RemoteTelegramSessionBusyError("synthetic shared-session hold")

    backend = BusyBackend()
    jobs = AudioJobStore(root / "jobs.sqlite3")
    owner = "a" * 64
    first, _ = jobs.create(
        owner_binding=owner,
        idempotency_key="tg:" + "3" * 64,
        asset_ref="aud_first",
        request={"source_sha256": "3" * 64},
    )
    second, _ = jobs.create(
        owner_binding=owner,
        idempotency_key="tg:" + "4" * 64,
        asset_ref="aud_second",
        request={"source_sha256": "4" * 64},
    )
    service = AudioTranscriptionService(
        SimpleNamespace(
            root=root,
            result_root=root / "results",
            poll_interval_seconds=20,
        ),
        asset_store=object(),
        job_store=jobs,
        backend=backend,
    )

    service._schedule_dispatch(first)
    await asyncio.gather(*tuple(service._tasks.values()))
    service._schedule_dispatch(second)

    assert backend.calls == 1
    assert service._dispatch_retry_not_before > asyncio.get_running_loop().time()
    assert jobs.get(first.job_ref, owner_binding=owner).state is JobState.QUEUED
    assert jobs.get(second.job_ref, owner_binding=owner).state is JobState.QUEUED
    await service.close()


@pytest.mark.asyncio
async def test_kaggle_reconcile_persists_bounded_retry_after(tmp_path):
    class StatusError(Exception):
        def __init__(self):
            self.response = SimpleNamespace(headers={"retry-after": "4600"})

    class RateLimitedClient:
        def get_kernel_status(self, _kernel_ref):
            raise StatusError()

    jobs = AudioJobStore(tmp_path / "jobs.sqlite3")
    owner = "a" * 64
    job, _ = jobs.create(
        owner_binding=owner,
        idempotency_key="tg:" + "5" * 64,
        asset_ref="aud_rate_limited",
        request={"source_sha256": "5" * 64},
    )
    job = jobs.update(job.job_ref, state=JobState.RUNNING, kernel_ref="test/kernel")
    before = int(time.time())

    result = await KaggleAudioBackend(
        SimpleNamespace(), object(), client=RateLimitedClient()
    ).reconcile(job)

    assert result.state is JobState.RUNNING
    assert result.progress["phase"] == "status_unavailable"
    assert result.progress["retry_after_seconds"] == 4600
    assert before + 4600 <= result.progress["retry_not_before"] <= int(time.time()) + 4600


@pytest.mark.asyncio
async def test_monitor_honors_persisted_reconcile_retry_window(tmp_path):
    root = tmp_path / "audio-runtime"

    class CountingBackend:
        def __init__(self):
            self.reconciles = 0

        async def reconcile(self, _job):
            self.reconciles += 1
            return ReconcileResult(
                state=JobState.RUNNING,
                progress={"phase": "running", "progress_percent": 50},
            )

    backend = CountingBackend()
    jobs = AudioJobStore(root / "jobs.sqlite3")
    owner = "a" * 64
    job, _ = jobs.create(
        owner_binding=owner,
        idempotency_key="tg:" + "6" * 64,
        asset_ref="aud_retry_hold",
        request={"source_sha256": "6" * 64},
    )
    jobs.update(
        job.job_ref,
        state=JobState.RUNNING,
        kernel_ref="test/kernel",
        progress={
            "phase": "status_unavailable",
            "retry_not_before": int(time.time()) + 60,
        },
    )
    service = AudioTranscriptionService(
        SimpleNamespace(
            root=root,
            result_root=root / "results",
            poll_interval_seconds=0.01,
        ),
        asset_store=object(),
        job_store=jobs,
        backend=backend,
    )
    service._last_retention_cleanup = time.monotonic()
    service._monitor_task = asyncio.create_task(service._monitor_loop())
    await asyncio.sleep(0.05)

    assert backend.reconciles == 0
    await service.close()
