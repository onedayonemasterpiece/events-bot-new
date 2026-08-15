import pytest

from audio_transcription.contracts import JobState
from audio_transcription.job_store import AudioJobStore, JobOwnershipError


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
