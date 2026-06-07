import pytest
import requests

import remote_telegram_session as guard


@pytest.mark.asyncio
async def test_cancel_acknowledged_remote_job_is_not_busy(monkeypatch):
    async def fake_list_jobs(job_type=None):
        return [
            {
                "type": "tg_monitoring",
                "kernel_ref": "zigomaro/telegram-monitor-bot",
                "created_at": "2026-04-27T21:41:25+00:00",
                "meta": {"run_id": "cancelled-run"},
            }
        ]

    class FakeKaggleClient:
        def get_kernel_status(self, kernel_ref):
            return {"status": "CANCEL_ACKNOWLEDGED", "failureMessage": ""}

    monkeypatch.setattr(guard, "list_jobs", fake_list_jobs)
    monkeypatch.setattr(guard, "KaggleClient", FakeKaggleClient)

    conflicts = await guard.find_remote_telegram_session_conflicts(
        current_job_type="tg_monitoring",
    )

    assert conflicts == []


@pytest.mark.asyncio
async def test_running_remote_job_is_busy(monkeypatch):
    async def fake_list_jobs(job_type=None):
        return [
            {
                "type": "guide_monitoring",
                "kernel_ref": "zigomaro/guide-excursions-monitor",
                "created_at": "2026-04-27T21:41:25+00:00",
                "meta": {"run_id": "running-run"},
            }
        ]

    class FakeKaggleClient:
        def get_kernel_status(self, kernel_ref):
            return {"status": "RUNNING", "failureMessage": ""}

    monkeypatch.setattr(guard, "list_jobs", fake_list_jobs)
    monkeypatch.setattr(guard, "KaggleClient", FakeKaggleClient)

    conflicts = await guard.find_remote_telegram_session_conflicts(
        current_job_type="tg_monitoring",
    )

    assert len(conflicts) == 1
    assert conflicts[0].job_type == "guide_monitoring"
    assert conflicts[0].status == "RUNNING"


def _http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.HTTPError(f"{status_code} error", response=response)


@pytest.mark.asyncio
async def test_fresh_unknown_status_lookup_failure_is_busy(monkeypatch):
    async def fake_list_jobs(job_type=None):
        return [
            {
                "type": "guide_monitoring",
                "kernel_ref": "zigomaro/guide-excursions-monitor",
                "created_at": "2026-06-07T19:55:00+00:00",
                "meta": {"run_id": "fresh-unknown"},
            }
        ]

    class FakeKaggleClient:
        def get_kernel_status(self, kernel_ref):
            raise _http_error(500)

    monkeypatch.setattr(guard, "list_jobs", fake_list_jobs)
    monkeypatch.setattr(guard, "KaggleClient", FakeKaggleClient)
    monkeypatch.setattr(guard, "_job_age_minutes", lambda _job: 15.0)

    conflicts = await guard.find_remote_telegram_session_conflicts(
        current_job_type="tg_monitoring",
    )

    assert len(conflicts) == 1
    assert conflicts[0].status == "UNKNOWN"
    assert conflicts[0].run_id == "fresh-unknown"
    assert "Kaggle status lookup failed" in (conflicts[0].failure_message or "")


@pytest.mark.asyncio
async def test_stale_transient_unknown_status_lookup_failure_is_not_busy(monkeypatch):
    marked: list[tuple[str, str, dict]] = []

    async def fake_list_jobs(job_type=None):
        return [
            {
                "type": "guide_monitoring",
                "kernel_ref": "zigomaro/guide-excursions-monitor",
                "created_at": "2026-06-06T07:00:23+00:00",
                "meta": {"run_id": "stale-unknown"},
            }
        ]

    async def fake_update_job_meta(job_type, kernel_ref, *, meta_updates=None, delete_keys=None):
        marked.append((job_type, kernel_ref, dict(meta_updates or {})))
        return {"type": job_type, "kernel_ref": kernel_ref, "meta": dict(meta_updates or {})}

    class FakeKaggleClient:
        def get_kernel_status(self, kernel_ref):
            raise _http_error(500)

    monkeypatch.setattr(guard, "list_jobs", fake_list_jobs)
    monkeypatch.setattr(guard, "KaggleClient", FakeKaggleClient)
    monkeypatch.setattr(guard, "update_job_meta", fake_update_job_meta)
    monkeypatch.setattr(guard, "_job_age_minutes", lambda _job: 24 * 60.0)
    monkeypatch.setenv("REMOTE_TELEGRAM_SESSION_UNKNOWN_STALE_MINUTES", "480")

    conflicts = await guard.find_remote_telegram_session_conflicts(
        current_job_type="tg_monitoring",
    )

    assert conflicts == []
    assert marked
    assert marked[0][0:2] == ("guide_monitoring", "zigomaro/guide-excursions-monitor")
    assert marked[0][2]["remote_session_guard_ignore_reason"] == "stale_transient_status_lookup_failure"


@pytest.mark.asyncio
async def test_stale_non_transient_unknown_status_lookup_failure_stays_busy(monkeypatch):
    async def fake_list_jobs(job_type=None):
        return [
            {
                "type": "guide_monitoring",
                "kernel_ref": "zigomaro/guide-excursions-monitor",
                "created_at": "2026-06-06T07:00:23+00:00",
                "meta": {"run_id": "auth-error"},
            }
        ]

    class FakeKaggleClient:
        def get_kernel_status(self, kernel_ref):
            raise _http_error(401)

    monkeypatch.setattr(guard, "list_jobs", fake_list_jobs)
    monkeypatch.setattr(guard, "KaggleClient", FakeKaggleClient)
    monkeypatch.setattr(guard, "_job_age_minutes", lambda _job: 24 * 60.0)

    conflicts = await guard.find_remote_telegram_session_conflicts(
        current_job_type="tg_monitoring",
    )

    assert len(conflicts) == 1
    assert conflicts[0].status == "UNKNOWN"
