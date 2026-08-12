from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import requests

from guide_excursions import service as guide_service
from guide_excursions import kaggle_service
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


def test_google_ai_bundle_contains_complete_deterministic_source_tree(tmp_path) -> None:
    expected = sorted(
        path.relative_to(kaggle_service.GOOGLE_AI_PACKAGE_PATH).as_posix()
        for path in kaggle_service.GOOGLE_AI_PACKAGE_PATH.rglob("*.py")
        if "__pycache__" not in path.parts
    )

    embedded = kaggle_service._embedded_google_ai_sources()

    assert list(embedded) == expected
    assert "__init__.py" in embedded
    assert "limiter_supabase.py" in embedded
    assert "interactions.py" in embedded

    staged_root = tmp_path / "staged"
    kaggle_service.stage_repo_bundle(staged_root)
    staged = sorted(
        path.relative_to(staged_root / "google_ai").as_posix()
        for path in (staged_root / "google_ai").rglob("*")
        if path.is_file()
    )
    assert staged == expected
    assert not list(staged_root.rglob("*.pyc"))


def test_generated_guide_notebook_imports_complete_google_ai_package(tmp_path) -> None:
    runner = tmp_path / "guide_excursions_monitor.py"
    runner.write_text(
        """from __future__ import annotations

from google_ai import AntigravityInteractionsClient, GoogleAIClient
from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client

assert GoogleAIClient is not None
assert AntigravityInteractionsClient is not None
assert build_google_ai_limiter_supabase_client is not None
print("guide-google-ai-import-closure-ok")
""",
        encoding="utf-8",
    )
    notebook = kaggle_service._build_notebook_payload_from_script(runner)
    generated_runner = tmp_path / "generated_guide_notebook.py"
    generated_runner.write_text(
        "".join(notebook["cells"][1]["source"]),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "-I", str(generated_runner)],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert "guide-google-ai-import-closure-ok" in completed.stdout


def test_guide_flat_bundle_bootstrap_copies_all_python_sources() -> None:
    source = Path(
        "kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py"
    ).read_text(encoding="utf-8")

    assert 'sorted(flat_repo_root.glob("*.py"))' in source
    assert "shutil.copy2(flat_repo_root / name" not in source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kernel_status", "expects_reconciliation"),
    [("failed", True), ("timeout", False)],
)
async def test_only_terminal_failed_kernel_reconciles_status_lease_before_raise(
    monkeypatch, kernel_status: str, expects_reconciliation: bool
) -> None:
    reconciled: list[dict] = []

    class DummyKaggleClient:
        pass

    async def fake_build_config(*_args, **_kwargs):
        return {"sources": [{"username": "source"}]}

    async def fake_prepare(**_kwargs):
        return "cipher", "key"

    async def fake_push(*_args, **_kwargs):
        return "zigomaro/guide-excursions-monitor", {"dataset_sources": ["cipher", "key"]}

    async def fake_shape(*_args, **_kwargs):
        return {"dataset_sources": ["cipher", "key"]}

    async def fake_register(*_args, **_kwargs):
        return None

    async def fake_launch_intent(*_args, **_kwargs):
        return None

    async def fake_poll(*_args, **_kwargs):
        return kernel_status, {"status": kernel_status.upper()}, 12.0

    async def fake_reconcile(_db, **kwargs):
        reconciled.append(kwargs)
        return {"status": "failed_reconciled", "released_resource_count": 1}

    async def fake_cleanup(_slugs):
        return None

    monkeypatch.setattr(kaggle_service, "KaggleClient", DummyKaggleClient)
    monkeypatch.setattr(kaggle_service, "_build_config_payload", fake_build_config)
    monkeypatch.setattr(kaggle_service, "_build_secrets_payload", lambda: "{}")
    monkeypatch.setattr(kaggle_service, "_prepare_kaggle_datasets", fake_prepare)
    monkeypatch.setattr(kaggle_service, "_push_kernel", fake_push)
    monkeypatch.setattr(kaggle_service, "_wait_for_remote_kernel_shape", fake_shape)
    monkeypatch.setattr(kaggle_service, "register_job", fake_register)
    monkeypatch.setattr(kaggle_service, "register_launch_intent", fake_launch_intent)
    monkeypatch.setattr(kaggle_service, "remove_launch_intent", fake_launch_intent)
    monkeypatch.setattr(kaggle_service, "_poll_kaggle_kernel", fake_poll)
    monkeypatch.setattr(kaggle_service, "reconcile_kaggle_run_failure_from_host", fake_reconcile)
    monkeypatch.setattr(kaggle_service, "_cleanup_datasets", fake_cleanup)
    monkeypatch.setattr(kaggle_service, "DATASET_PROPAGATION_WAIT_SECONDS", 0)
    monkeypatch.setattr(kaggle_service, "KAGGLE_STARTUP_WAIT_SECONDS", 0)

    with pytest.raises(RuntimeError, match=f"Guide Kaggle kernel failed \\({kernel_status}\\)"):
        await kaggle_service.run_guide_monitor_kaggle(
            object(),
            run_id="failed-holder",
            mode="full",
            limit=60,
            days_back=5,
        )

    expected = []
    if expects_reconciliation:
        expected = [
            {
                "run_id": "guide_monitor:failed-holder",
                "message": "Guide Kaggle kernel failed (failed)",
            }
        ]
    assert reconciled == expected


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
