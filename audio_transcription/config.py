from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def _strict_bool(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().casefold()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise ValueError(f"{name} must be an explicit boolean")


def _strict_int(name: str, default: int, *, low: int, high: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not low <= value <= high:
        raise ValueError(f"{name} must be between {low} and {high}")
    return value


def _hosts(value: str) -> tuple[str, ...]:
    result: list[str] = []
    for raw in value.split(","):
        host = raw.strip()
        if not host:
            continue
        if "://" in host or "/" in host or "@" in host:
            raise ValueError("audio allowed hosts must be hostnames")
        if host not in result:
            result.append(host)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class AudioTranscriptionConfig:
    enabled: bool
    root: Path
    asset_root: Path
    job_db_path: Path
    result_root: Path
    allowed_hosts: tuple[str, ...]
    max_asset_bytes: int
    max_store_bytes: int
    asset_ttl_seconds: int
    download_timeout_seconds: int
    result_retention_days: int
    poll_interval_seconds: int
    max_run_hours: int
    kernel_ref: str
    kernel_source: str
    auth_bundle_env: str
    telegram_peer: str
    cleanup_messages: bool
    keep_kaggle_datasets: bool

    @classmethod
    def from_env(cls, *, mcp_enabled: bool) -> "AudioTranscriptionConfig":
        enabled = (
            _strict_bool("PRIVATE_EVENTS_MCP_AUDIO_TRANSCRIPTION_ENABLED")
            if mcp_enabled
            else False
        )
        root = Path(
            (os.getenv("AUDIO_TRANSCRIPTION_ROOT") or "/data/audio-transcription").strip()
        )
        host_value = (
            os.getenv("AUDIO_TRANSCRIPTION_ALLOWED_HOSTS")
            or os.getenv("PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS")
            or ""
        )
        username = (os.getenv("KAGGLE_USERNAME") or "").strip()
        default_kernel_ref = f"{username}/events-bot-audio-transcription" if username else ""
        config = cls(
            enabled=enabled,
            root=root,
            asset_root=root / "assets",
            job_db_path=root / "jobs.sqlite3",
            result_root=root / "results",
            allowed_hosts=_hosts(host_value) if enabled else (),
            max_asset_bytes=_strict_int(
                "AUDIO_TRANSCRIPTION_MAX_ASSET_BYTES",
                512 * 1024 * 1024,
                low=1 * 1024 * 1024,
                high=2 * 1024 * 1024 * 1024,
            ),
            max_store_bytes=_strict_int(
                "AUDIO_TRANSCRIPTION_MAX_STORE_BYTES",
                2 * 1024 * 1024 * 1024,
                low=16 * 1024 * 1024,
                high=8 * 1024 * 1024 * 1024,
            ),
            asset_ttl_seconds=_strict_int(
                "AUDIO_TRANSCRIPTION_ASSET_TTL_SECONDS",
                24 * 3600,
                low=3600,
                high=7 * 24 * 3600,
            ),
            download_timeout_seconds=_strict_int(
                "AUDIO_TRANSCRIPTION_DOWNLOAD_TIMEOUT_SECONDS",
                120,
                low=5,
                high=600,
            ),
            result_retention_days=_strict_int(
                "AUDIO_TRANSCRIPTION_RESULT_RETENTION_DAYS", 7, low=1, high=30
            ),
            poll_interval_seconds=_strict_int(
                "AUDIO_TRANSCRIPTION_POLL_INTERVAL_SECONDS", 20, low=5, high=120
            ),
            max_run_hours=_strict_int(
                "AUDIO_TRANSCRIPTION_MAX_RUN_HOURS", 8, low=1, high=12
            ),
            kernel_ref=(
                os.getenv("AUDIO_TRANSCRIPTION_KAGGLE_KERNEL_REF") or default_kernel_ref
            ).strip(),
            kernel_source=(
                os.getenv("AUDIO_TRANSCRIPTION_KAGGLE_KERNEL_SOURCE")
                or "local:AudioTranscription"
            ).strip(),
            auth_bundle_env=(
                os.getenv("AUDIO_TRANSCRIPTION_AUTH_BUNDLE_ENV")
                or "TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION"
            ).strip(),
            telegram_peer=(
                os.getenv("AUDIO_TRANSCRIPTION_TELEGRAM_PEER") or "me"
            ).strip(),
            cleanup_messages=_strict_bool(
                "AUDIO_TRANSCRIPTION_CLEANUP_MESSAGES", default=True
            ),
            keep_kaggle_datasets=_strict_bool(
                "AUDIO_TRANSCRIPTION_KEEP_KAGGLE_DATASETS", default=False
            ),
        )
        if enabled:
            config.validate()
        return config

    def validate(self) -> None:
        for path in (self.root, self.asset_root, self.job_db_path, self.result_root):
            if not path.is_absolute():
                raise ValueError("audio transcription paths must be absolute")
        if not self.allowed_hosts:
            raise ValueError(
                "AUDIO_TRANSCRIPTION_ALLOWED_HOSTS or "
                "PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS is required"
            )
        if self.max_store_bytes < self.max_asset_bytes:
            raise ValueError("audio store must cover at least one maximum-sized asset")
        if not self.kernel_ref or "/" not in self.kernel_ref:
            raise ValueError("AUDIO_TRANSCRIPTION_KAGGLE_KERNEL_REF must be owner/slug")
        if not self.kernel_source:
            raise ValueError("AUDIO_TRANSCRIPTION_KAGGLE_KERNEL_SOURCE is required")
        if self.auth_bundle_env in {
            "TELEGRAM_AUTH_BUNDLE_E2E",
            "TELEGRAM_SESSION",
            "TG_SESSION",
        }:
            raise ValueError("audio transcription cannot borrow the local E2E Telegram session")
        if self.auth_bundle_env == "TELEGRAM_AUTH_BUNDLE_S22" and not _strict_bool(
            "AUDIO_TRANSCRIPTION_ALLOW_SHARED_REMOTE_AUTH", default=False
        ):
            raise ValueError(
                "audio transcription requires its own TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION; "
                "set AUDIO_TRANSCRIPTION_ALLOW_SHARED_REMOTE_AUTH=1 only for an explicit migration"
            )
        required = (
            "KAGGLE_USERNAME",
            "KAGGLE_KEY",
            "TG_API_ID",
            "TG_API_HASH",
            self.auth_bundle_env,
        )
        missing = [name for name in required if not (os.getenv(name) or "").strip()]
        if missing:
            raise ValueError(
                "missing audio transcription runtime secrets: " + ", ".join(missing)
            )
