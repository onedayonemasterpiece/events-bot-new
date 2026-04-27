from __future__ import annotations

import json
import logging
import mimetypes
from fractions import Fraction
from pathlib import Path
import shutil
import subprocess
from typing import Any

import cv2
import requests
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
STORY_VIDEO_WIDTH = 720
STORY_VIDEO_HEIGHT = 1280
STORY_SAFE_VIDEO_FILENAME = "crumple_video_story_720x1280.mp4"
STORY_VIDEO_PRESET = "fast"
STORY_VIDEO_BITRATE = "900k"
STORY_VIDEO_MAXRATE = "1200k"
STORY_VIDEO_BUFSIZE = "2400k"
STORY_UPLOAD_PROFILE_LEGACY_H264 = "legacy_h264_transcode"
STORY_UPLOAD_PROFILE_NATIVE_HEVC = "telegram_story_native_hevc_720p_v1"
CHERRYFLASH_NATIVE_TARGET_BYTES = 15 * 1024 * 1024
CHERRYFLASH_NATIVE_VIDEO_TAG = "hvc1"
CHERRYFLASH_NATIVE_VIDEO_CODEC = "hevc"
CHERRYFLASH_NATIVE_AUDIO_CODEC = "aac"
CHERRYFLASH_NATIVE_AUDIO_SAMPLE_RATE = 48000
CHERRYFLASH_NATIVE_AUDIO_CHANNELS = 2
CHERRYFLASH_NATIVE_MAX_DURATION_SECONDS = 60.0


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
    target_labels = [
        str(item.get("label") or item.get("peer") or "?")
        for item in config.get("targets") or []
        if isinstance(item, dict)
    ]
    config_business_hashes = [
        str(item.get("business_connection_hash") or "").strip()
        for item in config.get("targets") or []
        if isinstance(item, dict)
        and str(item.get("transport") or "").strip().lower() == "telegram_business"
    ]
    auth_business_hashes = [
        str(item.get("connection_hash") or "").strip()
        for item in auth.get("business_connections") or []
        if isinstance(item, dict)
    ]
    missing_business_hashes = [
        item for item in config_business_hashes if item and item not in set(auth_business_hashes)
    ]
    log(
        "Story runtime loaded: "
        f"config={config_path} cipher={cipher_path} key={key_path} "
        f"targets={target_labels} business_targets={len(config_business_hashes)} "
        f"business_secrets={len(auth_business_hashes)} missing_business={missing_business_hashes}"
    )
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


def _ffprobe_available() -> bool:
    return shutil.which("ffprobe") is not None


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


def _ffprobe_json(path: Path) -> dict[str, Any] | None:
    if not _ffprobe_available():
        return None
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_format",
            "-show_streams",
            "-print_format",
            "json",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout or "{}")
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _fps_from_ratio(value: str | None) -> float | None:
    raw = str(value or "").strip()
    if not raw or raw == "0/0":
        return None
    try:
        return float(Fraction(raw))
    except Exception:
        return None


def _video_probe(path: Path) -> dict[str, Any]:
    info: dict[str, Any] = {"path": str(path)}
    try:
        info["size_bytes"] = int(path.stat().st_size)
    except Exception:
        pass
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        info["readable"] = False
        return info
    try:
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        frame_count = float(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0)
        duration = frame_count / fps if fps > 0 and frame_count > 0 else 0.0
        info.update(
            {
                "readable": True,
                "width": width,
                "height": height,
                "fps": round(fps, 3),
                "frame_count": int(frame_count) if frame_count > 0 else 0,
                "duration_seconds": round(duration, 3),
            }
        )
        size_bytes = info.get("size_bytes")
        if duration > 0 and isinstance(size_bytes, int):
            info["approx_bitrate_kbps"] = round(size_bytes * 8 / duration / 1000, 1)
    finally:
        cap.release()
    ffprobe_data = _ffprobe_json(path)
    if isinstance(ffprobe_data, dict):
        format_info = ffprobe_data.get("format")
        if isinstance(format_info, dict):
            format_name = format_info.get("format_name")
            if format_name:
                info["format_name"] = str(format_name)
            format_bit_rate = format_info.get("bit_rate")
            if format_bit_rate:
                try:
                    info["format_bit_rate"] = int(format_bit_rate)
                except Exception:
                    pass
        streams = ffprobe_data.get("streams")
        if isinstance(streams, list):
            video_stream = next(
                (stream for stream in streams if isinstance(stream, dict) and stream.get("codec_type") == "video"),
                None,
            )
            audio_stream = next(
                (stream for stream in streams if isinstance(stream, dict) and stream.get("codec_type") == "audio"),
                None,
            )
            if isinstance(video_stream, dict):
                codec_name = video_stream.get("codec_name")
                if codec_name:
                    info["video_codec"] = str(codec_name)
                pix_fmt = video_stream.get("pix_fmt")
                if pix_fmt:
                    info["pix_fmt"] = str(pix_fmt)
                codec_tag = video_stream.get("codec_tag_string")
                if codec_tag:
                    info["video_tag"] = str(codec_tag)
                ffprobe_fps = _fps_from_ratio(
                    str(video_stream.get("avg_frame_rate") or video_stream.get("r_frame_rate") or "")
                )
                if ffprobe_fps is not None:
                    info["fps"] = round(ffprobe_fps, 3)
            if isinstance(audio_stream, dict):
                audio_codec = audio_stream.get("codec_name")
                if audio_codec:
                    info["audio_codec"] = str(audio_codec)
                sample_rate = audio_stream.get("sample_rate")
                if sample_rate:
                    try:
                        info["audio_sample_rate"] = int(sample_rate)
                    except Exception:
                        pass
                channels = audio_stream.get("channels")
                if channels:
                    try:
                        info["audio_channels"] = int(channels)
                    except Exception:
                        pass
    return info


def _story_upload_profile(config: dict[str, Any]) -> str:
    raw = str(config.get("upload_profile") or "").strip().lower()
    if raw == STORY_UPLOAD_PROFILE_NATIVE_HEVC:
        return STORY_UPLOAD_PROFILE_NATIVE_HEVC
    return STORY_UPLOAD_PROFILE_LEGACY_H264


def _validate_native_story_video(path: Path, *, log) -> Path:
    if not path.exists():
        raise RuntimeError("Story video publish requested but final video is missing")
    probe = _video_probe(path)
    problems: list[str] = []
    if int(probe.get("size_bytes") or 0) > CHERRYFLASH_NATIVE_TARGET_BYTES:
        problems.append(
            f"size={probe.get('size_bytes')} exceeds {CHERRYFLASH_NATIVE_TARGET_BYTES} bytes"
        )
    if int(probe.get("width") or 0) != STORY_VIDEO_WIDTH or int(probe.get("height") or 0) != STORY_VIDEO_HEIGHT:
        problems.append(
            f"canvas={probe.get('width')}x{probe.get('height')} expected {STORY_VIDEO_WIDTH}x{STORY_VIDEO_HEIGHT}"
        )
    fps = probe.get("fps")
    if not isinstance(fps, (int, float)) or abs(float(fps) - 30.0) > 0.2:
        problems.append(f"fps={fps!r} expected ~30")
    duration_seconds = probe.get("duration_seconds")
    if isinstance(duration_seconds, (int, float)) and float(duration_seconds) > CHERRYFLASH_NATIVE_MAX_DURATION_SECONDS:
        problems.append(
            f"duration_seconds={duration_seconds} exceeds {CHERRYFLASH_NATIVE_MAX_DURATION_SECONDS}"
        )
    if str(probe.get("format_name") or "").find("mp4") < 0:
        problems.append(f"format={probe.get('format_name')!r} is not mp4")
    if str(probe.get("video_codec") or "").strip().lower() != CHERRYFLASH_NATIVE_VIDEO_CODEC:
        problems.append(f"video_codec={probe.get('video_codec')!r} expected {CHERRYFLASH_NATIVE_VIDEO_CODEC}")
    if str(probe.get("video_tag") or "").strip().lower() != CHERRYFLASH_NATIVE_VIDEO_TAG:
        problems.append(f"video_tag={probe.get('video_tag')!r} expected {CHERRYFLASH_NATIVE_VIDEO_TAG}")
    if str(probe.get("pix_fmt") or "").strip().lower() != "yuv420p":
        problems.append(f"pix_fmt={probe.get('pix_fmt')!r} expected 'yuv420p'")
    if str(probe.get("audio_codec") or "").strip().lower() != CHERRYFLASH_NATIVE_AUDIO_CODEC:
        problems.append(f"audio_codec={probe.get('audio_codec')!r} expected {CHERRYFLASH_NATIVE_AUDIO_CODEC}")
    if int(probe.get("audio_sample_rate") or 0) != CHERRYFLASH_NATIVE_AUDIO_SAMPLE_RATE:
        problems.append(
            f"audio_sample_rate={probe.get('audio_sample_rate')!r} expected {CHERRYFLASH_NATIVE_AUDIO_SAMPLE_RATE}"
        )
    if int(probe.get("audio_channels") or 0) != CHERRYFLASH_NATIVE_AUDIO_CHANNELS:
        problems.append(
            f"audio_channels={probe.get('audio_channels')!r} expected {CHERRYFLASH_NATIVE_AUDIO_CHANNELS}"
        )
    if problems:
        raise RuntimeError(
            "CherryFlash story upload requires a native Telegram-ready final mp4; "
            + "; ".join(problems)
        )
    story_kb = int(probe.get("size_bytes") or path.stat().st_size) / 1024
    bitrate_label = ""
    if isinstance(probe.get("approx_bitrate_kbps"), (int, float)):
        bitrate_label = f", ~{probe['approx_bitrate_kbps']} kbps"
    log(
        "✅ Story-native video validated: "
        f"{path.name} ({probe.get('width')}x{probe.get('height')}, "
        f"{probe.get('video_codec')}/{probe.get('audio_codec')}, "
        f"{story_kb:.1f} KiB{bitrate_label})"
    )
    return path


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
            STORY_VIDEO_PRESET,
            "-b:v",
            STORY_VIDEO_BITRATE,
            "-maxrate",
            STORY_VIDEO_MAXRATE,
            "-bufsize",
            STORY_VIDEO_BUFSIZE,
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
            "-ac",
            "2",
            "-ar",
            "44100",
            "-shortest",
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
    story_info = _video_probe(story_path)
    story_size = int(story_info.get("size_bytes") or story_path.stat().st_size)
    story_kb = story_size / 1024
    story_bitrate = story_info.get("approx_bitrate_kbps")
    bitrate_label = (
        f", ~{story_bitrate} kbps" if isinstance(story_bitrate, (int, float)) else ""
    )
    log(
        "✅ Story-safe video prepared: "
        f"{story_path.name} ({original_label} -> {STORY_VIDEO_WIDTH}x{STORY_VIDEO_HEIGHT}, "
        f"h264/aac, bitrate={STORY_VIDEO_BITRATE}, maxrate={STORY_VIDEO_MAXRATE}, "
        f"{story_kb:.1f} KiB{bitrate_label})"
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
        "blocking_ok": False,
        "required_ok": False,
        "fanout_ok": False,
        "partial_ok": False,
    }
    if media_path is not None:
        report["media_path"] = str(media_path)
        report["media"] = _video_probe(media_path)
    return report


def _story_target_is_blocking(target_cfg: dict[str, Any], *, index: int) -> bool:
    blocking = target_cfg.get("blocking")
    if isinstance(blocking, bool):
        return blocking
    return index == 0


def _story_target_is_required(target_cfg: dict[str, Any], *, blocking: bool) -> bool:
    required = target_cfg.get("required")
    if isinstance(required, bool):
        return required
    return blocking


def _business_connection_for_target(
    auth: dict[str, Any],
    target_cfg: dict[str, Any],
) -> dict[str, Any] | None:
    target_hash = str(target_cfg.get("business_connection_hash") or "").strip()
    for item in auth.get("business_connections") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("connection_hash") or "").strip() == target_hash:
            return item
    return None


def _business_story_result_id(result: dict[str, Any]) -> int | None:
    payload = result.get("result")
    if not isinstance(payload, dict):
        return None
    story_id = payload.get("id")
    try:
        return int(story_id)
    except (TypeError, ValueError):
        return None


def _post_business_story(
    *,
    auth: dict[str, Any],
    target_cfg: dict[str, Any],
    media_path: Path,
    config: dict[str, Any],
) -> dict[str, Any]:
    connection = _business_connection_for_target(auth, target_cfg)
    if not connection:
        raise RuntimeError("business connection secret is missing")
    if not bool(connection.get("is_enabled")):
        raise RuntimeError("business connection is disabled")
    if not bool(connection.get("can_manage_stories")):
        raise RuntimeError("business connection lacks can_manage_stories")
    bot_token = str(auth.get("business_bot_token") or "").strip()
    if not bot_token:
        raise RuntimeError("business Bot API token is missing")
    connection_id = str(connection.get("connection_id") or "").strip()
    if not connection_id:
        raise RuntimeError("business connection id is missing")

    suffix = media_path.suffix.lower()
    attach_name = "story"
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        content = {"type": "photo", "photo": f"attach://{attach_name}"}
    elif suffix in {".mp4", ".mov"}:
        content = {
            "type": "video",
            "video": f"attach://{attach_name}",
            "cover_frame_timestamp": 0.0,
        }
    else:
        raise RuntimeError(f"Unsupported business story media type: {media_path.name}")

    data: dict[str, Any] = {
        "business_connection_id": connection_id,
        "content": json.dumps(content, ensure_ascii=False),
        "active_period": str(int(config.get("period_seconds") or 24 * 60 * 60)),
    }
    caption = str(config.get("caption") or "").strip()
    if caption:
        data["caption"] = caption
    files = {
        attach_name: (
            media_path.name,
            media_path.open("rb"),
            mimetypes.guess_type(str(media_path))[0] or "application/octet-stream",
        )
    }
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{bot_token}/postStory",
            data=data,
            files=files,
            timeout=180,
        )
    finally:
        files[attach_name][1].close()
    try:
        result = response.json()
    except Exception:
        result = {"ok": False, "description": response.text}
    if response.status_code >= 400 or not bool(result.get("ok")):
        description = str(result.get("description") or response.text or response.status_code)
        raise RuntimeError(f"Bot API postStory failed: {description}")
    return result


async def _story_targets_report(
    client: TelegramClient,
    *,
    auth: dict[str, Any],
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
        transport = str(target_cfg.get("transport") or "telethon").strip().lower()
        blocking = _story_target_is_blocking(target_cfg, index=idx)
        required = _story_target_is_required(target_cfg, blocking=blocking)
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
            "transport": transport,
            "blocking": blocking,
            "required": required,
            "period_seconds": int(config.get("period_seconds") or 24 * 60 * 60),
            "pinned": bool(config.get("pinned")),
            "ok": False,
        }
        try:
            if transport == "telegram_business":
                connection = _business_connection_for_target(auth, target_cfg)
                if not connection:
                    raise RuntimeError("business connection secret is missing")
                if not bool(connection.get("is_enabled")):
                    raise RuntimeError("business connection is disabled")
                if not bool(connection.get("can_manage_stories")):
                    raise RuntimeError("business connection lacks can_manage_stories")
                target_report["connection_hash"] = str(
                    target_cfg.get("business_connection_hash") or ""
                ).strip()
                if media_path is not None:
                    result = _post_business_story(
                        auth=auth,
                        target_cfg=target_cfg,
                        media_path=media_path,
                        config=config,
                    )
                    target_report["story_id"] = _business_story_result_id(result)
                    target_report["result"] = result.get("result")
                    log(
                        f"✅ Business story published to {label}"
                        + (
                            f" (story_id={target_report['story_id']})"
                            if target_report.get("story_id") is not None
                            else ""
                        )
                    )
                else:
                    log(f"✅ Business story preflight passed for {label}")
                target_report["ok"] = True
                report["targets"].append(target_report)
                continue
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
    targets = report["targets"]
    blocking_targets = [item for item in targets if item.get("blocking")]
    required_targets = [
        item for item in targets if item.get("blocking") or item.get("required")
    ]
    report["fanout_ok"] = bool(targets) and all(bool(item.get("ok")) for item in targets)
    report["blocking_ok"] = bool(blocking_targets) and all(
        bool(item.get("ok")) for item in blocking_targets
    )
    report["required_ok"] = bool(required_targets) and all(
        bool(item.get("ok")) for item in required_targets
    )
    report["partial_ok"] = bool(report["blocking_ok"]) and not bool(report["fanout_ok"])
    if phase == "preflight":
        report["ok"] = bool(report["blocking_ok"])
    else:
        report["ok"] = bool(report["required_ok"])
    if report["partial_ok"]:
        failed_labels = [
            str(item.get("label") or item.get("peer") or "?")
            for item in targets
            if not item.get("ok")
        ]
        phase_label = phase.capitalize()
        failed_required_labels = [
            str(item.get("label") or item.get("peer") or "?")
            for item in targets
            if not item.get("ok") and item.get("required")
        ]
        if failed_required_labels and phase == "preflight":
            log(
                f"⚠️ {phase_label} render gate passed; "
                f"required fanout currently unavailable: {', '.join(failed_required_labels)}"
            )
        elif failed_required_labels:
            log(
                f"❌ {phase_label} required fanout failed: "
                f"{', '.join(failed_required_labels)}"
            )
        else:
            log(
                f"⚠️ {phase_label} primary target passed; "
                f"continuing despite best-effort fanout failures: {', '.join(failed_labels)}"
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
            auth=auth,
            config=config,
            log=log,
            phase="preflight",
            media_path=None,
            honor_delays=False,
        )
        if not report.get("fanout_ok", report.get("ok")):
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
        upload_profile = _story_upload_profile(config)
        if upload_profile == STORY_UPLOAD_PROFILE_NATIVE_HEVC:
            media_path = _validate_native_story_video(media_path, log=log)
        else:
            media_path = _ensure_story_safe_video(
                media_path,
                output_dir=output_dir,
                log=log,
            )
    client = await _create_client(auth)
    try:
        report = await _story_targets_report(
            client,
            auth=auth,
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
