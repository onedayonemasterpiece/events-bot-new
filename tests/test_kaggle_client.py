import importlib
import sys
import types

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
