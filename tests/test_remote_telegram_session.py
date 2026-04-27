import pytest

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
