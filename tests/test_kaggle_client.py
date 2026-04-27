import importlib
import sys
import types
from pathlib import Path

import pytest


class DummyDataset:
    def __init__(self, title=None):
        self.title = title


def _install_dummy_kaggle(monkeypatch):
    dummy_kaggle = types.ModuleType("kaggle")
    dummy_kaggle.__path__ = []

    dummy_api_pkg = types.ModuleType("kaggle.api")
    dummy_api_pkg.__path__ = []

    dummy_api_extended = types.ModuleType("kaggle.api.kaggle_api_extended")

    class DummyKaggleApi:
        def authenticate(self):
            return None

    dummy_api_extended.KaggleApi = DummyKaggleApi
    dummy_api_pkg.kaggle_api_extended = dummy_api_extended
    dummy_kaggle.api = dummy_api_pkg

    monkeypatch.setitem(sys.modules, "kaggle", dummy_kaggle)
    monkeypatch.setitem(sys.modules, "kaggle.api", dummy_api_pkg)
    monkeypatch.setitem(
        sys.modules, "kaggle.api.kaggle_api_extended", dummy_api_extended
    )


def test_kaggle_test_skips_max_size(monkeypatch):
    _install_dummy_kaggle(monkeypatch)
    KaggleClient = importlib.import_module("video_announce.kaggle_client").KaggleClient

    called_kwargs = {}

    class StubApi:
        def dataset_list(self, **kwargs):
            nonlocal called_kwargs
            called_kwargs = kwargs
            return [DummyDataset(title="Sample Title")]

    client = KaggleClient()
    monkeypatch.setattr(client, "_get_api", lambda: StubApi())

    result = client.kaggle_test()

    assert result == "Sample Title"
    assert called_kwargs == {"page": 1}


def test_kaggle_test_handles_missing_titles(monkeypatch):
    _install_dummy_kaggle(monkeypatch)
    KaggleClient = importlib.import_module("video_announce.kaggle_client").KaggleClient

    class StubApi:
        def dataset_list(self, **kwargs):
            assert "max_size" not in kwargs
            return [DummyDataset(), DummyDataset(title=None)]

    client = KaggleClient()
    monkeypatch.setattr(client, "_get_api", lambda: StubApi())

    result = client.kaggle_test()

    assert result == "ok (datasets=2)"


def test_dataset_list_files_paginates_until_expected_file_can_be_seen(monkeypatch):
    _install_dummy_kaggle(monkeypatch)
    KaggleClient = importlib.import_module("video_announce.kaggle_client").KaggleClient

    class File:
        def __init__(self, name: str) -> None:
            self.name = name

    class Response:
        def __init__(self, files, next_page_token=None) -> None:
            self.files = files
            self.nextPageToken = next_page_token

    calls: list[tuple[str | None, int]] = []

    class StubApi:
        def dataset_list_files(self, dataset, page_token=None, page_size=20):
            assert dataset == "zigomaro/cherryflash-session-162"
            calls.append((page_token, page_size))
            if page_token is None:
                return Response([File("Final.png"), File("assets/Akrobat-Bold.otf")], "page-2")
            assert page_token == "page-2"
            return Response([File("payload.json"), File("assets/cherryflash_selection.json")])

    client = KaggleClient()
    monkeypatch.setattr(client, "_get_api", lambda: StubApi())

    files = client.dataset_list_files("zigomaro/cherryflash-session-162", page_size=20)

    assert [item["name"] for item in files] == [
        "Final.png",
        "assets/Akrobat-Bold.otf",
        "payload.json",
        "assets/cherryflash_selection.json",
    ]
    assert calls == [(None, 20), ("page-2", 20)]


@pytest.mark.asyncio
async def test_await_dataset_ready_waits_until_status_ready_and_payload_visible(monkeypatch):
    _install_dummy_kaggle(monkeypatch)
    module = importlib.import_module("video_announce.kaggle_client")

    class StubClient:
        def __init__(self) -> None:
            self.status_calls = 0
            self.files_calls = 0

        def dataset_status(self, dataset_ref: str) -> str:
            assert dataset_ref == "zigomaro/cherryflash-session-161"
            self.status_calls += 1
            if self.status_calls == 1:
                return "running"
            return "ready"

        def dataset_list_files(self, dataset_ref: str, *, page_size: int = 20) -> list[dict[str, object]]:
            assert dataset_ref == "zigomaro/cherryflash-session-161"
            assert page_size >= 20
            self.files_calls += 1
            if self.files_calls == 1:
                return [{"name": "bootstrap.txt"}]
            return [{"name": "payload.json"}, {"name": "assets/cherryflash_selection.json"}]

    meta = await module.await_dataset_ready(
        StubClient(),
        "zigomaro/cherryflash-session-161",
        timeout_seconds=2,
        poll_interval_seconds=1,
        expected_files=["payload.json"],
    )

    assert meta["status"] == "ready"
    assert "payload.json" in meta["files"]


def test_deploy_kernel_update_retries_without_gpu_on_cherryflash_quota(monkeypatch, tmp_path):
    _install_dummy_kaggle(monkeypatch)
    module = importlib.import_module("video_announce.kaggle_client")
    KaggleClient = module.KaggleClient

    kernel_dir = tmp_path / "CherryFlash"
    kernel_dir.mkdir()
    (kernel_dir / "kernel-metadata.json").write_text(
        """
{
  "id": "zigomaro/cherryflash",
  "title": "CherryFlash",
  "code_file": "cherryflash.ipynb",
  "language": "python",
  "kernel_type": "notebook",
  "is_private": true,
  "enable_gpu": true,
  "enable_internet": true,
  "dataset_sources": []
}
""".strip(),
        encoding="utf-8",
    )
    (kernel_dir / "cherryflash.ipynb").write_text(
        '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}',
        encoding="utf-8",
    )

    class Response:
        def __init__(self, *, ref="", version_number=0, error="", invalid_dataset_sources=None):
            self.ref = ref
            self.versionNumber = version_number
            self.error = error
            self.invalidDatasetSources = list(invalid_dataset_sources or [])

    push_gpu_flags: list[bool] = []

    class StubApi:
        def kernels_push(self, folder, timeout=None):
            del timeout
            meta = module.json.loads((Path(folder) / "kernel-metadata.json").read_text(encoding="utf-8"))
            push_gpu_flags.append(bool(meta.get("enable_gpu")))
            if len(push_gpu_flags) == 1:
                return Response(
                    error="Maximum weekly GPU quota of 30.00 hours reached.",
                    invalid_dataset_sources=["zigomaro/cherryflash-session-200"],
                )
            return Response(ref="/code/zigomaro/cherryflash", version_number=53)

    client = KaggleClient()
    monkeypatch.setattr(client, "_get_api", lambda: StubApi())
    monkeypatch.setattr(
        module,
        "find_local_kernel",
        lambda kernel_ref: {"path": str(kernel_dir), "slug": "cherryflash", "id": kernel_ref},
    )
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    result = client.deploy_kernel_update(
        "zigomaro/cherryflash",
        ["zigomaro/cherryflash-session-200"],
    )

    assert result == "zigomaro/cherryflash"
    assert push_gpu_flags == [True, False]


def test_deploy_kernel_update_retries_invalid_dataset_sources_until_valid(monkeypatch, tmp_path):
    _install_dummy_kaggle(monkeypatch)
    module = importlib.import_module("video_announce.kaggle_client")
    KaggleClient = module.KaggleClient

    kernel_dir = tmp_path / "CherryFlash"
    kernel_dir.mkdir()
    (kernel_dir / "kernel-metadata.json").write_text(
        """
{
  "id": "zigomaro/cherryflash",
  "title": "CherryFlash",
  "code_file": "cherryflash.ipynb",
  "language": "python",
  "kernel_type": "notebook",
  "is_private": true,
  "enable_gpu": false,
  "enable_internet": true,
  "dataset_sources": []
}
""".strip(),
        encoding="utf-8",
    )
    (kernel_dir / "cherryflash.ipynb").write_text(
        '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}',
        encoding="utf-8",
    )

    class Response:
        def __init__(self, *, ref="", version_number=0, error="", invalid_dataset_sources=None):
            self.ref = ref
            self.versionNumber = version_number
            self.error = error
            self.invalidDatasetSources = list(invalid_dataset_sources or [])

    calls = {"count": 0}

    class StubApi:
        def kernels_push(self, folder, timeout=None):
            del folder, timeout
            calls["count"] += 1
            if calls["count"] == 1:
                return Response(
                    ref="/code/zigomaro/cherryflash",
                    version_number=53,
                    invalid_dataset_sources=["zigomaro/cherryflash-session-201"],
                )
            return Response(ref="/code/zigomaro/cherryflash", version_number=54)

    client = KaggleClient()
    monkeypatch.setattr(client, "_get_api", lambda: StubApi())
    monkeypatch.setattr(
        module,
        "find_local_kernel",
        lambda kernel_ref: {"path": str(kernel_dir), "slug": "cherryflash", "id": kernel_ref},
    )
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    result = client.deploy_kernel_update(
        "zigomaro/cherryflash",
        ["zigomaro/cherryflash-session-201"],
    )

    assert result == "zigomaro/cherryflash"
    assert calls["count"] == 2


def test_deploy_kernel_update_prunes_stale_cherryflash_session_sources(monkeypatch, tmp_path):
    _install_dummy_kaggle(monkeypatch)
    module = importlib.import_module("video_announce.kaggle_client")
    KaggleClient = module.KaggleClient

    kernel_dir = tmp_path / "CherryFlash"
    kernel_dir.mkdir()
    (kernel_dir / "kernel-metadata.json").write_text(
        """
{
  "id": "zigomaro/cherryflash",
  "title": "CherryFlash",
  "code_file": "cherryflash.ipynb",
  "language": "python",
  "kernel_type": "notebook",
  "is_private": true,
  "enable_gpu": true,
  "enable_internet": true,
  "dataset_sources": [
    "zigomaro/cherryflash-session-192-1777189467",
    "zigomaro/crumple-video-story-secrets-cipher"
  ]
}
""".strip(),
        encoding="utf-8",
    )
    (kernel_dir / "cherryflash.ipynb").write_text(
        '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}',
        encoding="utf-8",
    )
    pushed_sources: list[str] = []

    class Response:
        ref = "/code/zigomaro/cherryflash"
        versionNumber = 77
        error = ""
        invalidDatasetSources: list[str] = []

    class StubApi:
        def kernels_push(self, folder, timeout=None):
            del timeout
            meta = module.json.loads((Path(folder) / "kernel-metadata.json").read_text(encoding="utf-8"))
            pushed_sources.extend(meta["dataset_sources"])
            return Response()

    client = KaggleClient()
    monkeypatch.setattr(client, "_get_api", lambda: StubApi())
    monkeypatch.setattr(
        module,
        "find_local_kernel",
        lambda kernel_ref: {"path": str(kernel_dir), "slug": "cherryflash", "id": kernel_ref},
    )
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    result = client.deploy_kernel_update(
        "zigomaro/cherryflash",
        ["zigomaro/cherryflash-session-219-1777298000"],
    )

    assert result == "zigomaro/cherryflash"
    assert pushed_sources == [
        "zigomaro/crumple-video-story-secrets-cipher",
        "zigomaro/cherryflash-session-219-1777298000",
    ]
