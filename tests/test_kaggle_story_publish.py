from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
HELPER_PATH = ROOT / "kaggle" / "CrumpleVideo" / "story_publish.py"
SPEC = importlib.util.spec_from_file_location("test_story_publish_helper", HELPER_PATH)
assert SPEC and SPEC.loader
sys.modules.setdefault("cv2", types.SimpleNamespace(VideoCapture=lambda *_args, **_kwargs: None))
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)


def test_story_safe_video_transcodes_even_if_canvas_is_already_story_size(
    monkeypatch,
    tmp_path: Path,
) -> None:
    src = tmp_path / "input.mp4"
    src.write_bytes(b"input")
    output_dir = tmp_path / "out"
    commands: list[list[str]] = []

    def fake_dimensions(path: Path) -> tuple[int, int]:
        if path.name == helper.STORY_SAFE_VIDEO_FILENAME:
            return (helper.STORY_VIDEO_WIDTH, helper.STORY_VIDEO_HEIGHT)
        return (helper.STORY_VIDEO_WIDTH, helper.STORY_VIDEO_HEIGHT)

    def fake_run(cmd, capture_output=None, text=None, check=None):  # noqa: ANN001,ARG001
        commands.append(cmd)
        story_path = output_dir / helper.STORY_SAFE_VIDEO_FILENAME
        story_path.parent.mkdir(parents=True, exist_ok=True)
        story_path.write_bytes(b"x" * 1024)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(helper, "_ffmpeg_available", lambda: True)
    monkeypatch.setattr(helper, "_video_dimensions", fake_dimensions)
    monkeypatch.setattr(helper, "_video_probe", lambda path: {"path": str(path), "size_bytes": 1024})
    monkeypatch.setattr(helper.subprocess, "run", fake_run)

    story_path = helper._ensure_story_safe_video(
        src,
        output_dir=output_dir,
        log=lambda *_args, **_kwargs: None,
    )

    assert story_path == output_dir / helper.STORY_SAFE_VIDEO_FILENAME
    assert story_path != src
    assert commands
    ffmpeg_cmd = commands[0]
    assert ffmpeg_cmd[:4] == ["ffmpeg", "-y", "-i", str(src)]
    assert ffmpeg_cmd[ffmpeg_cmd.index("-vf") + 1] == "setsar=1"
    assert ffmpeg_cmd[ffmpeg_cmd.index("-c:v") + 1] == "libx264"
    assert ffmpeg_cmd[ffmpeg_cmd.index("-preset") + 1] == helper.STORY_VIDEO_PRESET
    assert ffmpeg_cmd[ffmpeg_cmd.index("-b:v") + 1] == helper.STORY_VIDEO_BITRATE
    assert ffmpeg_cmd[ffmpeg_cmd.index("-maxrate") + 1] == helper.STORY_VIDEO_MAXRATE
    assert ffmpeg_cmd[ffmpeg_cmd.index("-bufsize") + 1] == helper.STORY_VIDEO_BUFSIZE
    assert ffmpeg_cmd[ffmpeg_cmd.index("-tag:v") + 1] == "avc1"
    assert ffmpeg_cmd[ffmpeg_cmd.index("-c:a") + 1] == "aac"
    assert ffmpeg_cmd[ffmpeg_cmd.index("-b:a") + 1] == "128k"
    assert "-shortest" in ffmpeg_cmd


def test_story_safe_video_scales_and_pads_non_story_canvas(
    monkeypatch,
    tmp_path: Path,
) -> None:
    src = tmp_path / "input.mov"
    src.write_bytes(b"input")
    output_dir = tmp_path / "out"
    commands: list[list[str]] = []

    def fake_dimensions(path: Path) -> tuple[int, int]:
        if path.name == helper.STORY_SAFE_VIDEO_FILENAME:
            return (helper.STORY_VIDEO_WIDTH, helper.STORY_VIDEO_HEIGHT)
        return (1080, 1920)

    def fake_run(cmd, capture_output=None, text=None, check=None):  # noqa: ANN001,ARG001
        commands.append(cmd)
        story_path = output_dir / helper.STORY_SAFE_VIDEO_FILENAME
        story_path.parent.mkdir(parents=True, exist_ok=True)
        story_path.write_bytes(b"x" * 1024)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(helper, "_ffmpeg_available", lambda: True)
    monkeypatch.setattr(helper, "_video_dimensions", fake_dimensions)
    monkeypatch.setattr(helper, "_video_probe", lambda path: {"path": str(path), "size_bytes": 1024})
    monkeypatch.setattr(helper.subprocess, "run", fake_run)

    helper._ensure_story_safe_video(
        src,
        output_dir=output_dir,
        log=lambda *_args, **_kwargs: None,
    )

    ffmpeg_cmd = commands[0]
    filter_graph = ffmpeg_cmd[ffmpeg_cmd.index("-vf") + 1]
    assert "scale=720:1280:force_original_aspect_ratio=decrease:flags=lanczos" in filter_graph
    assert "pad=720:1280:(ow-iw)/2:(oh-ih)/2:color=black" in filter_graph


def test_story_report_includes_media_probe(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "story.mp4"
    src.write_bytes(b"video")
    monkeypatch.setattr(helper, "_video_probe", lambda path: {"path": str(path), "width": 720})

    report = helper._build_story_report(
        {"mode": "video", "pinned": True},
        phase="publish",
        account={"id": 1, "username": "story"},
        media_path=src,
    )

    assert report["media_path"] == str(src)
    assert report["media"] == {"path": str(src), "width": 720}


def test_validate_native_story_video_accepts_hevc_render(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "final.mp4"
    src.write_bytes(b"video")
    monkeypatch.setattr(
        helper,
        "_video_probe",
        lambda path: {
            "path": str(path),
            "size_bytes": 9 * 1024 * 1024,
            "width": 720,
            "height": 1280,
            "fps": 30.0,
            "duration_seconds": 53.0,
            "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
            "video_codec": "hevc",
            "video_tag": "hvc1",
            "pix_fmt": "yuv420p",
            "audio_codec": "aac",
            "audio_sample_rate": 48000,
            "audio_channels": 2,
            "approx_bitrate_kbps": 1428.0,
        },
    )

    validated = helper._validate_native_story_video(src, log=lambda *_args, **_kwargs: None)

    assert validated == src


def test_validate_native_story_video_rejects_non_native_profile(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "final.mp4"
    src.write_bytes(b"video")
    monkeypatch.setattr(
        helper,
        "_video_probe",
        lambda path: {
            "path": str(path),
            "size_bytes": 4 * 1024 * 1024,
            "width": 720,
            "height": 1280,
            "fps": 30.0,
            "duration_seconds": 53.0,
            "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
            "video_codec": "h264",
            "video_tag": "avc1",
            "pix_fmt": "yuv420p",
            "audio_codec": "aac",
            "audio_sample_rate": 44100,
            "audio_channels": 2,
        },
    )

    with pytest.raises(RuntimeError, match="native Telegram-ready final mp4"):
        helper._validate_native_story_video(src, log=lambda *_args, **_kwargs: None)


class _FakeCanSendStoryRequest:
    def __init__(self, *, peer):  # noqa: ANN001
        self.peer = peer


class _FakeSendStoryRequest:
    def __init__(self, **kwargs):  # noqa: ANN003
        self.kwargs = kwargs


class _FakeCanSendResult:
    def __init__(self, peer: str):
        self.peer = peer

    def to_dict(self) -> dict[str, str]:
        return {"peer": self.peer}


class _FakeStoryClient:
    def __init__(self, *, boost_fail_peers: set[str], story_ids: dict[str, int]):
        self.boost_fail_peers = boost_fail_peers
        self.story_ids = story_ids
        self.sent_requests: list[dict[str, object]] = []

    async def get_me(self) -> SimpleNamespace:
        return SimpleNamespace(id=1, username="story", premium=True)

    async def get_input_entity(self, peer_ref: str) -> str:
        return f"peer:{peer_ref}"

    async def __call__(self, request):  # noqa: ANN001
        if isinstance(request, _FakeCanSendStoryRequest):
            if request.peer in self.boost_fail_peers:
                raise RuntimeError("BOOSTS_REQUIRED")
            return _FakeCanSendResult(request.peer)
        if isinstance(request, _FakeSendStoryRequest):
            peer = str(request.kwargs["peer"])
            if peer in self.boost_fail_peers:
                raise RuntimeError("BOOSTS_REQUIRED")
            self.sent_requests.append(request.kwargs)
            return SimpleNamespace(
                story_id=self.story_ids[peer],
                to_dict=lambda: {"story_id": self.story_ids[peer]},
            )
        raise AssertionError(f"Unexpected request: {request!r}")


def _patch_story_request_types(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(helper.functions.stories, "CanSendStoryRequest", _FakeCanSendStoryRequest)
    monkeypatch.setattr(helper.functions.stories, "SendStoryRequest", _FakeSendStoryRequest)
    monkeypatch.setattr(
        helper.types,
        "InputPrivacyValueAllowAll",
        lambda: "allow_all",  # noqa: ARG005
    )
    monkeypatch.setattr(
        helper.types,
        "InputMediaStory",
        lambda *, peer, id: {"peer": peer, "id": id},
    )
    monkeypatch.setattr(helper, "_video_probe", lambda path: {"path": str(path)})


@pytest.mark.asyncio
async def test_story_preflight_only_requires_primary_target(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_story_request_types(monkeypatch)
    client = _FakeStoryClient(
        boost_fail_peers={"peer:@lovekenig"},
        story_ids={},
    )

    report = await helper._story_targets_report(
        client,
        auth={},
        config={
            "targets": [
                {"peer": "@kenigevents", "mode": "upload"},
                {"peer": "@lovekenig", "mode": "repost_previous"},
                {"peer": "@loving_guide39", "mode": "repost_previous"},
            ]
        },
        log=lambda *_args, **_kwargs: None,
        phase="preflight",
        media_path=None,
        honor_delays=False,
    )

    assert report["ok"] is True
    assert report["blocking_ok"] is True
    assert report["fanout_ok"] is False
    assert report["partial_ok"] is True
    assert [item["ok"] for item in report["targets"]] == [True, False, True]
    assert [item["blocking"] for item in report["targets"]] == [True, False, False]


@pytest.mark.asyncio
async def test_story_publish_continues_after_non_blocking_fanout_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_story_request_types(monkeypatch)

    async def _fake_input_media_for_path(*_args, **_kwargs):  # noqa: ANN002,ANN003
        return "uploaded-media"

    monkeypatch.setattr(helper, "_input_media_for_path", _fake_input_media_for_path)
    monkeypatch.setattr(helper, "_extract_story_id", lambda result: result.story_id)

    client = _FakeStoryClient(
        boost_fail_peers={"peer:@lovekenig"},
        story_ids={
            "peer:@kenigevents": 101,
            "peer:@loving_guide39": 202,
        },
    )
    media_path = tmp_path / "story.mp4"
    media_path.write_bytes(b"video")

    report = await helper._story_targets_report(
        client,
        auth={},
        config={
            "targets": [
                {"peer": "@kenigevents", "mode": "upload"},
                {"peer": "@lovekenig", "mode": "repost_previous"},
                {"peer": "@loving_guide39", "mode": "repost_previous"},
            ]
        },
        log=lambda *_args, **_kwargs: None,
        phase="publish",
        media_path=media_path,
        honor_delays=False,
    )

    assert report["ok"] is True
    assert report["blocking_ok"] is True
    assert report["fanout_ok"] is False
    assert report["partial_ok"] is True
    assert [item.get("story_id") for item in report["targets"]] == [101, None, 202]
    assert len(client.sent_requests) == 2
    assert client.sent_requests[1]["fwd_from_story"] == 101
    assert client.sent_requests[1]["fwd_from_id"] == "peer:@kenigevents"


@pytest.mark.asyncio
async def test_story_publish_marks_required_fanout_failure_after_render(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_story_request_types(monkeypatch)

    async def _fake_input_media_for_path(*_args, **_kwargs):  # noqa: ANN002,ANN003
        return "uploaded-media"

    monkeypatch.setattr(helper, "_input_media_for_path", _fake_input_media_for_path)
    monkeypatch.setattr(helper, "_extract_story_id", lambda result: result.story_id)

    media_path = tmp_path / "story.mp4"
    media_path.write_bytes(b"video")

    preflight = await helper._story_targets_report(
        _FakeStoryClient(
            boost_fail_peers={"peer:@kenigevents"},
            story_ids={},
        ),
        auth={},
        config={
            "targets": [
                {"peer": "me", "mode": "upload"},
                {"peer": "@kenigevents", "mode": "repost_previous", "required": True},
                {"peer": "@lovekenig", "mode": "repost_previous", "required": True},
            ]
        },
        log=lambda *_args, **_kwargs: None,
        phase="preflight",
        media_path=None,
        honor_delays=False,
    )

    assert preflight["ok"] is True
    assert preflight["blocking_ok"] is True
    assert preflight["required_ok"] is False

    publish = await helper._story_targets_report(
        _FakeStoryClient(
            boost_fail_peers={"peer:@kenigevents"},
            story_ids={
                "peer:me": 100,
                "peer:@lovekenig": 202,
            },
        ),
        auth={},
        config={
            "targets": [
                {"peer": "me", "mode": "upload"},
                {"peer": "@kenigevents", "mode": "repost_previous", "required": True},
                {"peer": "@lovekenig", "mode": "repost_previous", "required": True},
            ]
        },
        log=lambda *_args, **_kwargs: None,
        phase="publish",
        media_path=media_path,
        honor_delays=False,
    )

    assert publish["ok"] is False
    assert publish["blocking_ok"] is True
    assert publish["required_ok"] is False
    assert publish["fanout_ok"] is False
    assert [item["required"] for item in publish["targets"]] == [True, True, True]
    assert [item["ok"] for item in publish["targets"]] == [True, False, True]
    assert [item.get("story_id") for item in publish["targets"]] == [100, None, 202]


@pytest.mark.asyncio
async def test_story_publish_posts_business_target_via_bot_api(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_story_request_types(monkeypatch)
    client = _FakeStoryClient(boost_fail_peers=set(), story_ids={})
    media_path = tmp_path / "story.mp4"
    media_path.write_bytes(b"video")
    calls: list[dict[str, object]] = []

    class _Response:
        status_code = 200
        text = ""

        def json(self) -> dict[str, object]:
            return {"ok": True, "result": {"id": 777}}

    def fake_post(url, data=None, files=None, timeout=None):  # noqa: ANN001
        calls.append(
            {
                "url": url,
                "data": dict(data or {}),
                "file_name": files["story"][0],
                "timeout": timeout,
            }
        )
        return _Response()

    monkeypatch.setattr(helper.requests, "post", fake_post)

    report = await helper._story_targets_report(
        client,
        auth={
            "business_bot_token": "123:test",
            "business_connections": [
                {
                    "connection_hash": "hash-1",
                    "connection_id": "biz-secret",
                    "is_enabled": True,
                    "can_manage_stories": True,
                }
            ],
        },
        config={
            "period_seconds": 43200,
            "targets": [
                {
                    "peer": "business:hash-1",
                    "label": "business:hash-1",
                    "transport": "telegram_business",
                    "business_connection_hash": "hash-1",
                    "delay_seconds": 600,
                },
            ],
        },
        log=lambda *_args, **_kwargs: None,
        phase="publish",
        media_path=media_path,
        honor_delays=False,
    )

    assert report["ok"] is True
    assert report["targets"][0]["transport"] == "telegram_business"
    assert report["targets"][0]["story_id"] == 777
    assert calls
    assert calls[0]["url"] == "https://api.telegram.org/bot123:test/postStory"
    data = calls[0]["data"]
    assert data["business_connection_id"] == "biz-secret"
    assert data["active_period"] == "43200"
    assert "biz-secret" not in str(report)


@pytest.mark.asyncio
async def test_business_target_missing_secret_blocks_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_story_request_types(monkeypatch)
    client = _FakeStoryClient(boost_fail_peers=set(), story_ids={})

    report = await helper._story_targets_report(
        client,
        auth={"business_bot_token": "123:test", "business_connections": []},
        config={
            "period_seconds": 43200,
            "targets": [
                {
                    "peer": "business:hash-1",
                    "label": "business:hash-1",
                    "transport": "telegram_business",
                    "business_connection_hash": "hash-1",
                    "blocking": True,
                    "required": True,
                },
            ],
        },
        log=lambda *_args, **_kwargs: None,
        phase="preflight",
        media_path=None,
        honor_delays=False,
    )

    assert report["ok"] is False
    assert report["blocking_ok"] is False
    assert report["required_ok"] is False
    assert report["targets"][0]["ok"] is False
    assert "business connection secret is missing" in report["targets"][0]["error"]
