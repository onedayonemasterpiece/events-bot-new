from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

from .contracts import AudioChunk, AudioProbe


class AudioProcessingError(RuntimeError):
    def __init__(self, code: str, message: str, *, retry_safe: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retry_safe = retry_safe


def require_ffmpeg() -> tuple[str, str]:
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if not ffmpeg or not ffprobe:
        raise AudioProcessingError(
            "FFMPEG_NOT_AVAILABLE",
            "ffmpeg and ffprobe are required",
            retry_safe=False,
        )
    return ffmpeg, ffprobe


def _run(command: list[str], *, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise AudioProcessingError(
            "FFMPEG_TIMEOUT", "audio processing timed out", retry_safe=True
        ) from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()[-1000:]
        raise AudioProcessingError(
            "AUDIO_PROCESSING_FAILED",
            f"ffmpeg/ffprobe failed: {detail}",
            retry_safe=False,
        ) from exc


def probe_audio(path: str | Path, *, timeout_seconds: int = 60) -> AudioProbe:
    _, ffprobe = require_ffmpeg()
    source = Path(path)
    completed = _run(
        [
            ffprobe,
            "-v",
            "error",
            "-show_format",
            "-show_streams",
            "-of",
            "json",
            str(source),
        ],
        timeout_seconds=timeout_seconds,
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise AudioProcessingError(
            "FFPROBE_INVALID_OUTPUT", "ffprobe returned invalid JSON"
        ) from exc
    streams = payload.get("streams") if isinstance(payload, dict) else None
    if not isinstance(streams, list):
        streams = []
    audio_stream = next(
        (
            stream
            for stream in streams
            if isinstance(stream, dict) and stream.get("codec_type") == "audio"
        ),
        None,
    )
    if audio_stream is None:
        raise AudioProcessingError("AUDIO_STREAM_MISSING", "file contains no audio stream")
    format_data = payload.get("format") if isinstance(payload.get("format"), dict) else {}
    duration_raw = format_data.get("duration") or audio_stream.get("duration")
    try:
        duration_ms = int(round(float(duration_raw) * 1000))
    except (TypeError, ValueError) as exc:
        raise AudioProcessingError(
            "AUDIO_DURATION_MISSING", "audio duration is unavailable"
        ) from exc
    tags: dict[str, str] = {}
    for key, value in (format_data.get("tags") or {}).items():
        name = str(key)
        text = str(value)
        tags[name] = text
        tags[f"format.{name}"] = text
    for key, value in (audio_stream.get("tags") or {}).items():
        name = str(key)
        text = str(value)
        tags.setdefault(name, text)
        tags[f"stream.{name}"] = text
    sample_rate = None
    try:
        sample_rate = int(audio_stream.get("sample_rate"))
    except (TypeError, ValueError):
        pass
    channels = None
    try:
        channels = int(audio_stream.get("channels"))
    except (TypeError, ValueError):
        pass
    return AudioProbe(
        duration_ms=duration_ms,
        format_name=str(format_data.get("format_name") or "unknown"),
        codec_name=(
            str(audio_stream.get("codec_name"))
            if audio_stream.get("codec_name")
            else None
        ),
        sample_rate=sample_rate,
        channels=channels,
        tags=tags,
    )


_SILENCE_END_RE = re.compile(r"silence_end:\s*(?P<seconds>\d+(?:\.\d+)?)")


def detect_silence_end_points(
    path: str | Path,
    *,
    noise_db: float = -35.0,
    min_silence_seconds: float = 0.45,
    timeout_seconds: int = 1800,
) -> tuple[int, ...]:
    ffmpeg, _ = require_ffmpeg()
    command = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-i",
        str(path),
        "-af",
        f"silencedetect=noise={noise_db:g}dB:d={min_silence_seconds:g}",
        "-f",
        "null",
        "-",
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise AudioProcessingError(
            "SILENCE_DETECTION_TIMEOUT", "silence detection timed out", retry_safe=True
        ) from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[-1000:]
        raise AudioProcessingError(
            "SILENCE_DETECTION_FAILED", f"silence detection failed: {detail}"
        )
    return tuple(
        sorted(
            {
                int(round(float(match.group("seconds")) * 1000))
                for match in _SILENCE_END_RE.finditer(completed.stderr or "")
            }
        )
    )


def transcode_chunk_to_voice(
    source: str | Path,
    chunk: AudioChunk,
    target: str | Path,
    *,
    timeout_seconds: int = 300,
) -> Path:
    ffmpeg, _ = require_ffmpeg()
    destination = Path(target)
    destination.parent.mkdir(parents=True, exist_ok=True)
    completed = _run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-ss",
            f"{chunk.start_ms / 1000:.3f}",
            "-t",
            f"{chunk.duration_ms / 1000:.3f}",
            "-i",
            str(source),
            "-map_metadata",
            "-1",
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-c:a",
            "libopus",
            "-b:a",
            "32k",
            "-vbr",
            "on",
            "-compression_level",
            "10",
            "-application",
            "voip",
            "-y",
            str(destination),
        ],
        timeout_seconds=timeout_seconds,
    )
    del completed
    if not destination.is_file() or destination.stat().st_size <= 0:
        raise AudioProcessingError("VOICE_CHUNK_EMPTY", "transcoded voice chunk is empty")
    return destination
