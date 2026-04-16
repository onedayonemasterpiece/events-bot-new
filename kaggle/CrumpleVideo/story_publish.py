from __future__ import annotations

import json
import logging
import mimetypes
from pathlib import Path
import shutil
import subprocess
from typing import Any

import cv2
from cryptography.fernet import Fernet
from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession


logger = logging.getLogger(__name__)

CONFIG_FILENAME = "story_publish.json"
CIPHER_FILENAME = "story_publish.enc"
KEY_FILENAME = "story_publish.key"
MAX_VIDEO_BYTES = 30 * 1024 * 1024
PREMIUM_REQUIRED_ERROR_NAME = "PremiumAccountRequiredError"
CRUMPLE_VIDEO_STORY_PREVIEW_TS = 0
STORY_VIDEO_WIDTH = 1080
STORY_VIDEO_HEIGHT = 1920
STORY_SAFE_VIDEO_FILENAME = "crumple_video_story_1080x1920.mp4"


def _find_input_file(filename: str, *, search_roots: list[Path]) -> Path | None:
    seen: set[Path] = set()
    for root in search_roots:
        candidate = root / filename
        if candidate.exists():
            return candidate
        if root in seen or not root.exists():
            continue
        seen.add(root)
        try:
            matches = sorted(root.rglob(filename))
        except Exception:
            matches = []
        if matches:
            return matches[0]
    return None


def load_story_publish_runtime(*, search_roots: list[Path], log) -> dict[str, Any] | None:
    config_path = _find_input_file(CONFIG_FILENAME, search_roots=search_roots)
    if not config_path:
        log("[SKIP] story_publish.json not found; story publish disabled for this run.")
        return None

    cipher_path = _find_input_file(CIPHER_FILENAME, search_roots=search_roots)
    key_path = _find_input_file(KEY_FILENAME, search_roots=search_roots)
    if not cipher_path or not key_path:
        raise RuntimeError("Story publish config exists but encrypted secrets are missing")

    config = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise RuntimeError("story_publish.json must be an object")
    if not isinstance(config.get("targets"), list) or not config["targets"]:
        raise RuntimeError("story_publish.json must contain non-empty targets")

    fernet = Fernet(key_path.read_bytes().strip())
    auth_raw = fernet.decrypt(cipher_path.read_bytes())
    auth = json.loads(auth_raw.decode("utf-8"))
    if not isinstance(auth, dict):
        raise RuntimeError("Decrypted story auth payload must be an object")
    return {"config": config, "auth": auth}


def _story_media_path(
    config: dict[str, Any],
    *,
    final_video_path: Path | None,
    intro_path: Path | None,
    posters: list[Path] | None,
) -> Path:
    mode = str(config.get("mode") or "video").strip().lower()
    if mode == "image":
        for candidate in list(posters or [])[1:]:
            if candidate and candidate.exists():
                return candidate
        if intro_path and intro_path.exists():
            return intro_path
        for candidate in posters or []:
            if candidate and candidate.exists():
                return candidate
        raise RuntimeError("Image smoke mode requested but no image candidate was found")

    if not final_video_path or not final_video_path.exists():
        raise RuntimeError("Story video publish requested but final video is missing")
    return final_video_path


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _video_dimensions(path: Path) -> tuple[int, int] | None:
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        return None
    try:
        width = max(1, int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or STORY_VIDEO_WIDTH))
        height = max(1, int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or STORY_VIDEO_HEIGHT))
        return width, height
    finally:
        cap.release()


def _ensure_story_safe_video(
    path: Path,
    *,
    output_dir: Path,
    log,
) -> Path:
    dimensions = _video_dimensions(path)
    if not _ffmpeg_available():
        raise RuntimeError("ffmpeg is required to prepare story-safe video upload")

    output_dir.mkdir(parents=True, exist_ok=True)
    story_path = output_dir / STORY_SAFE_VIDEO_FILENAME
    if dimensions == (STORY_VIDEO_WIDTH, STORY_VIDEO_HEIGHT):
        filter_graph = "setsar=1"
    else:
        filter_graph = (
            f"scale={STORY_VIDEO_WIDTH}:{STORY_VIDEO_HEIGHT}:"
            "force_original_aspect_ratio=decrease:flags=lanczos,"
            f"pad={STORY_VIDEO_WIDTH}:{STORY_VIDEO_HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=black,"
            "setsar=1"
        )
    result = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(path),
            "-vf",
            filter_graph,
            "-r",
            "30",
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-c:v",
            "libx264",
            "-profile:v",
            "high",
            "-level:v",
            "4.1",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "30",
            "-keyint_min",
            "30",
            "-sc_threshold",
            "0",
            "-bf",
            "0",
            "-tag:v",
            "avc1",
            "-movflags",
            "+faststart",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            str(story_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(
            "Failed to prepare story-safe video canvas"
            + (f": {stderr}" if stderr else "")
        )
    output_dimensions = _video_dimensions(story_path)
    if output_dimensions != (STORY_VIDEO_WIDTH, STORY_VIDEO_HEIGHT):
        raise RuntimeError(
            "Story-safe video has unexpected canvas "
            f"{output_dimensions!r}, expected {(STORY_VIDEO_WIDTH, STORY_VIDEO_HEIGHT)!r}"
        )
    if story_path.stat().st_size > MAX_VIDEO_BYTES:
        raise RuntimeError(
            "Story-safe video exceeds Telegram story limit "
            f"({story_path.stat().st_size} bytes > {MAX_VIDEO_BYTES})"
        )
    original_label = (
        f"{dimensions[0]}x{dimensions[1]}"
        if isinstance(dimensions, tuple) and len(dimensions) == 2
        else "unknown"
    )
    log(
        "✅ Story-safe video prepared: "
        f"{story_path.name} ({original_label} -> {STORY_VIDEO_WIDTH}x{STORY_VIDEO_HEIGHT}, h264/aac)"
    )
    return story_path


async def _create_client(auth: dict[str, Any]) -> TelegramClient:
    client = TelegramClient(
        StringSession(str(auth.get("session") or "").strip()),
        int(auth["api_id"]),
        str(auth["api_hash"]),
        device_model=str(auth.get("device_model") or "iPhone 14 Pro"),
        system_version=str(auth.get("system_version") or "iOS 17.2"),
        app_version=str(auth.get("app_version") or "10.5.1"),
        lang_code=str(auth.get("lang_code") or "ru"),
        system_lang_code=str(auth.get("system_lang_code") or auth.get("lang_code") or "ru"),
    )
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("Story publish Telethon client is not authorized")
    return client


def _account_info(me: Any) -> dict[str, Any]:
    return {
        "id": getattr(me, "id", None),
        "username": getattr(me, "username", None),
        "premium": bool(getattr(me, "premium", False)),
    }


def _account_label(account: dict[str, Any] | None) -> str:
    if not isinstance(account, dict):
        return "current account"
    username = str(account.get("username") or "").strip()
    if username:
        return f"@{username}"
    account_id = account.get("id")
    if account_id:
        return f"id={account_id}"
    return "current account"


def _format_story_error(exc: Exception, *, account: dict[str, Any] | None) -> str:
    base = f"{type(exc).__name__}: {exc}"
    if type(exc).__name__ == PREMIUM_REQUIRED_ERROR_NAME and account and not account.get("premium"):
        return (
            f"{base}. {_account_label(account)} is not Telegram Premium, "
            "so this session cannot publish stories."
        )
    return base


def _video_attributes(path: Path) -> list[types.TypeDocumentAttribute]:
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        return [
            types.DocumentAttributeVideo(
                duration=1.0,
                w=1080,
                h=1920,
                supports_streaming=True,
            )
        ]
    try:
        width = max(1, int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 1080))
        height = max(1, int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 1920))
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 24.0)
        frame_count = float(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0)
        duration = frame_count / fps if fps > 0 and frame_count > 0 else 1.0
    finally:
        cap.release()
    return [
        types.DocumentAttributeVideo(
            duration=max(1.0, float(duration)),
            w=width,
            h=height,
            supports_streaming=True,
        )
    ]


async def _input_media_for_path(client: TelegramClient, path: Path):
    suffix = path.suffix.lower()
    uploaded = await client.upload_file(str(path))
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        return types.InputMediaUploadedPhoto(file=uploaded)
    if suffix in {".mp4", ".mov", ".mkv", ".webm"}:
        if path.stat().st_size > MAX_VIDEO_BYTES:
            raise RuntimeError(
                f"Story video exceeds Telegram story limit ({path.stat().st_size} bytes > {MAX_VIDEO_BYTES})"
            )
        mime_type = mimetypes.guess_type(str(path))[0] or "video/mp4"
        return types.InputMediaUploadedDocument(
            file=uploaded,
            mime_type=mime_type,
            attributes=_video_attributes(path),
            video_timestamp=CRUMPLE_VIDEO_STORY_PREVIEW_TS,
        )
    raise RuntimeError(f"Unsupported story media type: {path.name}")


def _extract_story_id(result: Any) -> int | None:
    updates = getattr(result, "updates", None) or []
    for update in updates:
        if isinstance(update, types.UpdateStoryID):
            return int(update.id)
        if isinstance(update, types.UpdateStory):
            story = getattr(update, "story", None)
            story_id = getattr(story, "id", None)
            if story_id is not None:
                return int(story_id)
    return None


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass
    return str(value)


def write_story_publish_report(report: dict[str, Any], *, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "story_publish_report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    return report_path


def _build_story_report(
    config: dict[str, Any],
    *,
    phase: str,
    account: dict[str, Any],
    media_path: Path | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "ok": False,
        "phase": phase,
        "mode": config.get("mode") or "video",
        "period_seconds": int(config.get("period_seconds") or 24 * 60 * 60),
        "pinned": bool(config.get("pinned")),
        "account": account,
        "targets": [],
    }
    if media_path is not None:
        report["media_path"] = str(media_path)
    return report


async def _story_targets_report(
    client: TelegramClient,
    *,
    config: dict[str, Any],
    log,
    phase: str,
    media_path: Path | None = None,
    honor_delays: bool,
) -> dict[str, Any]:
    me = await client.get_me()
    account = _account_info(me)
    log(
        f"Story {phase} account: {_account_label(account)} premium={account.get('premium')}"
    )
    report = _build_story_report(
        config,
        phase=phase,
        account=account,
        media_path=media_path,
    )
    previous_story: dict[str, Any] | None = None
    for idx, target_cfg in enumerate(config.get("targets") or []):
        peer_ref = str(target_cfg.get("peer") or "").strip()
        label = str(target_cfg.get("label") or peer_ref or f"target-{idx + 1}")
        delay_seconds = max(0, int(target_cfg.get("delay_seconds") or 0))
        publish_mode = str(target_cfg.get("mode") or "upload").strip().lower()
        if publish_mode not in {"upload", "repost_previous"}:
            publish_mode = "upload"
        if honor_delays and delay_seconds:
            log(f"⏳ Story target {label}: sleeping {delay_seconds}s before publish")
            import asyncio

            await asyncio.sleep(delay_seconds)
        target_report = {
            "peer": peer_ref,
            "label": label,
            "delay_seconds": delay_seconds,
            "mode": publish_mode,
            "period_seconds": int(config.get("period_seconds") or 24 * 60 * 60),
            "pinned": bool(config.get("pinned")),
            "ok": False,
        }
        try:
            peer = await client.get_input_entity(peer_ref)
            can_send = await client(functions.stories.CanSendStoryRequest(peer=peer))
            target_report["can_send"] = (
                can_send.to_dict() if hasattr(can_send, "to_dict") else str(can_send)
            )
            if media_path is not None:
                send_kwargs: dict[str, Any]
                if publish_mode == "repost_previous":
                    if not previous_story:
                        raise RuntimeError(
                            "repost_previous target requires a successful prior story target"
                        )
                    source_peer = previous_story["peer"]
                    source_story_id = int(previous_story["story_id"])
                    media = types.InputMediaStory(peer=source_peer, id=source_story_id)
                    target_report["source_story_id"] = source_story_id
                    target_report["source_peer"] = previous_story.get("peer_ref")
                    target_report["source_label"] = previous_story.get("label")
                    send_kwargs = {
                        "peer": peer,
                        "media": media,
                        "privacy_rules": [types.InputPrivacyValueAllowAll()],
                        "pinned": bool(config.get("pinned")),
                        "period": int(config.get("period_seconds") or 24 * 60 * 60),
                        "fwd_from_id": source_peer,
                        "fwd_from_story": source_story_id,
                    }
                else:
                    media = await _input_media_for_path(client, media_path)
                    send_kwargs = {
                        "peer": peer,
                        "media": media,
                        "privacy_rules": [types.InputPrivacyValueAllowAll()],
                        "pinned": bool(config.get("pinned")),
                        "caption": str(config.get("caption") or "").strip() or None,
                        "period": int(config.get("period_seconds") or 24 * 60 * 60),
                    }
                result = await client(
                    functions.stories.SendStoryRequest(**send_kwargs)
                )
                target_report["story_id"] = _extract_story_id(result)
                target_report["result"] = (
                    result.to_dict() if hasattr(result, "to_dict") else str(result)
                )
                log(
                    f"✅ Story published to {label}"
                    + (
                        f" (story_id={target_report['story_id']})"
                        if target_report.get("story_id") is not None
                        else ""
                    )
                )
                if target_report.get("story_id") is not None:
                    previous_story = {
                        "peer": peer,
                        "peer_ref": peer_ref,
                        "label": label,
                        "story_id": int(target_report["story_id"]),
                    }
            else:
                log(f"✅ Story preflight passed for {label}")
            target_report["ok"] = True
        except Exception as exc:
            target_report["error"] = _format_story_error(exc, account=account)
            log(
                f"❌ Story {phase} failed for {label}: {target_report['error']}"
            )
        report["targets"].append(target_report)
    report["ok"] = bool(report["targets"]) and all(
        bool(item.get("ok")) for item in report["targets"]
    )
    return report


async def preflight_story_publish_from_kaggle(
    *,
    search_roots: list[Path],
    output_dir: Path,
    log,
) -> dict[str, Any] | None:
    runtime = load_story_publish_runtime(search_roots=search_roots, log=log)
    if not runtime:
        return None

    config = runtime["config"]
    auth = runtime["auth"]
    client = await _create_client(auth)
    try:
        report = await _story_targets_report(
            client,
            config=config,
            log=log,
            phase="preflight",
            media_path=None,
            honor_delays=False,
        )
        if not report.get("ok"):
            write_story_publish_report(report, output_dir=output_dir)
        return report
    finally:
        try:
            await client.disconnect()
        except Exception:
            logger.warning("story_publish: failed to disconnect client", exc_info=True)


async def publish_story_from_kaggle(
    *,
    final_video_path: Path | None,
    intro_path: Path | None,
    posters: list[Path] | None,
    search_roots: list[Path],
    output_dir: Path,
    log,
) -> dict[str, Any] | None:
    runtime = load_story_publish_runtime(search_roots=search_roots, log=log)
    if not runtime:
        return None

    config = runtime["config"]
    auth = runtime["auth"]
    media_path = _story_media_path(
        config,
        final_video_path=final_video_path,
        intro_path=intro_path,
        posters=posters,
    )
    if str(config.get("mode") or "video").strip().lower() == "video":
        media_path = _ensure_story_safe_video(
            media_path,
            output_dir=output_dir,
            log=log,
        )
    client = await _create_client(auth)
    try:
        report = await _story_targets_report(
            client,
            config=config,
            log=log,
            phase="publish",
            media_path=media_path,
            honor_delays=True,
        )
        write_story_publish_report(report, output_dir=output_dir)
        return report
    finally:
        try:
            await client.disconnect()
        except Exception:
            logger.warning("story_publish: failed to disconnect client", exc_info=True)
