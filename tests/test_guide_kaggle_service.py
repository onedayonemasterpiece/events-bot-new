from __future__ import annotations

from pathlib import Path

import pytest
import requests

from guide_excursions import service as guide_service
from guide_excursions.kaggle_service import _is_transient_kaggle_status_error


def _http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.HTTPError(f"{status_code} error", response=response)


def test_kaggle_status_http_500_is_transient() -> None:
    assert _is_transient_kaggle_status_error(_http_error(500))
    assert _is_transient_kaggle_status_error(_http_error(503))


def test_kaggle_status_http_400_is_not_transient() -> None:
    assert not _is_transient_kaggle_status_error(_http_error(400))


@pytest.mark.asyncio
async def test_resume_guide_monitor_jobs_uses_output_when_status_lookup_fails(
    monkeypatch, tmp_path
) -> None:
    jobs = [
        {
            "kernel_ref": "zigomaro/guide-excursions-monitor",
            "meta": {
                "run_id": "f18774f300c7",
                "mode": "full",
                "pid": 999999,
                "auto_publish_after_import": False,
            },
        }
    ]
    removed: list[tuple[str, str]] = []
    imported: list[dict] = []
    results_path = tmp_path / "guide_excursions_results.json"
    results_path.write_text('{"run_id":"f18774f300c7"}', encoding="utf-8")

    class DummyKaggleClient:
        def get_kernel_status(self, _kernel_ref: str) -> dict:
            raise _http_error(500)

    async def fake_list_jobs(job_type: str | None = None) -> list[dict]:
        assert job_type == "guide_monitoring"
        return jobs

    async def fake_download_guide_results(_client, kernel_ref: str, run_id: str) -> Path:
        assert kernel_ref == "zigomaro/guide-excursions-monitor"
        assert run_id == "f18774f300c7"
        return results_path

    async def fake_run_guide_import_from_results(_db, **kwargs):
        imported.append(kwargs)
        assert kwargs["results_path"] == str(results_path)
        assert kwargs["run_id"] == "f18774f300c7"
        assert kwargs["kaggle_meta"]["status"] == "complete"
        assert kwargs["kaggle_meta"]["status_data"]["status"] == "UNKNOWN"
        return guide_service.GuideMonitorResult(
            run_id="f18774f300c7",
            ops_run_id=123,
            trigger="recovery_import",
            mode="full",
            metrics={},
            errors=[],
            import_completed=True,
        )

    async def fake_remove_job(job_type: str, kernel_ref: str) -> None:
        removed.append((job_type, kernel_ref))

    monkeypatch.setattr(guide_service, "KaggleClient", DummyKaggleClient)
    monkeypatch.setattr(guide_service, "list_jobs", fake_list_jobs)
    monkeypatch.setattr(guide_service, "download_guide_results", fake_download_guide_results)
    monkeypatch.setattr(
        guide_service,
        "run_guide_import_from_results",
        fake_run_guide_import_from_results,
    )
    monkeypatch.setattr(guide_service, "remove_job", fake_remove_job)

    recovered = await guide_service.resume_guide_monitor_jobs(object(), None, chat_id=123)

    assert recovered == 1
    assert len(imported) == 1
    assert removed == [("guide_monitoring", "zigomaro/guide-excursions-monitor")]
