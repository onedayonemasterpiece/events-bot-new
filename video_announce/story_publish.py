from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from models import Channel
from source_parsing.telegram.split_secrets import encrypt_secret
from .kaggle_client import KaggleClient

logger = logging.getLogger(__name__)

STORY_PUBLISH_CONFIG_FILENAME = "story_publish.json"
STORY_PUBLISH_CIPHER_FILENAME = "story_publish.enc"
STORY_PUBLISH_KEY_FILENAME = "story_publish.key"

DEFAULT_CIPHER_DATASET_SLUG = "crumple-video-story-secrets-cipher"
DEFAULT_KEY_DATASET_SLUG = "crumple-video-story-secrets-key"
DEFAULT_SINGLE_DAY_STORY_PERIOD_SECONDS = 12 * 60 * 60
DEFAULT_MULTI_DAY_STORY_PERIOD_SECONDS = 24 * 60 * 60


@dataclass(frozen=True, slots=True)
class StoryTarget:
    peer: str
    label: str
    delay_seconds: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "peer": self.peer,
            "label": self.label,
            "delay_seconds": self.delay_seconds,
        }


def _read_env_file_value(key: str) -> str | None:
    path = Path(".env")
    if not path.exists():
        return None
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line or line.lstrip().startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() == key:
                return value.strip()
    except Exception:
        return None
    return None


def _get_env_value(key: str) -> str:
    value = (os.getenv(key) or "").strip()
    if value:
        return value
    return (_read_env_file_value(key) or "").strip()


def _env_enabled(key: str, *, default: bool = False) -> bool:
    raw = _get_env_value(key)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _require_env_any(*keys: str) -> str:
    for key in keys:
        value = _get_env_value(key)
        if value:
            return value
    raise RuntimeError(f"Missing required env: {' or '.join(keys)}")


def _read_positive_int_env(key: str, default: int) -> int:
    raw = _get_env_value(key)
    if not raw:
        return default
    try:
        value = int(raw)
        if value <= 0:
            raise ValueError
        return value
    except ValueError as exc:
        raise RuntimeError(f"Invalid {key}={raw!r}: expected positive int") from exc


def _read_optional_positive_int_env(key: str) -> int | None:
    raw = _get_env_value(key)
    if not raw:
        return None
    try:
        value = int(raw)
        if value <= 0:
            raise ValueError
        return value
    except ValueError as exc:
        raise RuntimeError(f"Invalid {key}={raw!r}: expected positive int") from exc


def story_publish_enabled() -> bool:
    return _env_enabled("VIDEO_ANNOUNCE_STORY_ENABLED", default=False)


def story_publish_required() -> bool:
    return _env_enabled("VIDEO_ANNOUNCE_STORY_REQUIRED", default=False)


def story_publish_health_status() -> str:
    required = story_publish_required()
    enabled = story_publish_enabled()
    if not enabled:
        return "disabled" if required else "disabled_optional"

    bundle_env_key = (_get_env_value("VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV") or "").strip()
    session_env_key = (_get_env_value("VIDEO_ANNOUNCE_STORY_SESSION_ENV") or "").strip()
    if bundle_env_key:
        if not _get_env_value(bundle_env_key):
            return f"auth_bundle_missing:{bundle_env_key}"
    elif session_env_key:
        if not _get_env_value(session_env_key):
            return f"session_missing:{session_env_key}"
    else:
        return "auth_source_missing"

    try:
        explicit_targets = _parse_story_targets_json()
        extra_targets = [] if explicit_targets else _parse_extra_targets_json()
    except RuntimeError as exc:
        return f"config_error:{exc}"

    use_main_target = _env_enabled("VIDEO_ANNOUNCE_STORY_USE_MAIN_CHANNEL", default=True)
    if not explicit_targets and not use_main_target and not extra_targets:
        return "targets_missing"
    return "ok"


def _normalize_peer(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    raw = re.sub(r"^https?://t\.me/", "@", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^t\.me/", "@", raw, flags=re.IGNORECASE)
    if raw.startswith("@"):
        username = raw[1:].split("/", 1)[0].split("?", 1)[0].strip()
        return f"@{username}" if username else ""
    return raw


def _parse_auth_bundle(bundle_b64: str, *, env_key: str) -> dict[str, Any]:
    try:
        raw = base64.urlsafe_b64decode(bundle_b64.encode("ascii")).decode("utf-8")
        payload = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"Invalid {env_key}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid {env_key}: expected JSON object")
    if not str(payload.get("session") or "").strip():
        raise RuntimeError(f"Invalid {env_key}: missing session")
    return payload


def _story_session_payload() -> dict[str, Any]:
    api_id = _require_env_any("TG_API_ID", "TELEGRAM_API_ID")
    api_hash = _require_env_any("TG_API_HASH", "TELEGRAM_API_HASH")
    bundle_env_key = (_get_env_value("VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV") or "").strip()
    session_env_key = (_get_env_value("VIDEO_ANNOUNCE_STORY_SESSION_ENV") or "").strip()
    source_channel_id_raw = (_get_env_value("SOURCE_CHANNEL_ID") or "").strip()

    auth: dict[str, Any] = {
        "api_id": int(api_id),
        "api_hash": str(api_hash),
    }
    if source_channel_id_raw:
        try:
            auth["source_channel_id"] = int(source_channel_id_raw)
        except ValueError as exc:
            raise RuntimeError("SOURCE_CHANNEL_ID must be int") from exc
    if bundle_env_key:
        bundle_raw = _get_env_value(bundle_env_key)
        if not bundle_raw:
            raise RuntimeError(
                f"{bundle_env_key} is empty but VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV requests it"
            )
        bundle = _parse_auth_bundle(bundle_raw, env_key=bundle_env_key)
        auth.update(
            {
                "session": str(bundle["session"]).strip(),
                "device_model": bundle.get("device_model"),
                "system_version": bundle.get("system_version"),
                "app_version": bundle.get("app_version"),
                "lang_code": bundle.get("lang_code"),
                "system_lang_code": bundle.get("system_lang_code"),
                "auth_source": bundle_env_key,
            }
        )
        return auth

    if session_env_key:
        session = _get_env_value(session_env_key)
        if not session:
            raise RuntimeError(
                f"{session_env_key} is empty but VIDEO_ANNOUNCE_STORY_SESSION_ENV requests it"
            )
        auth.update(
            {
                "session": session,
                "auth_source": session_env_key,
            }
        )
        return auth

    raise RuntimeError(
        "VIDEO_ANNOUNCE_STORY_ENABLED=1 requires either "
        "VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV or VIDEO_ANNOUNCE_STORY_SESSION_ENV"
    )


def build_story_secrets_payload() -> str:
    payload = _story_session_payload()
    logger.info(
        "video_announce.story secrets auth_source=%s has_session=%s",
        payload.get("auth_source") or "-",
        bool(payload.get("session")),
    )
    return json.dumps(payload, ensure_ascii=False)


def _normalize_dataset_slug(config_key: str, default_slug: str) -> str:
    raw = (_get_env_value(config_key) or "").strip()
    if raw and "/" in raw:
        return raw
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME not set")
    slug = raw or default_slug
    slug = re.sub(r"[^a-z0-9-]+", "-", slug.strip().lower()).strip("-")
    slug = slug[:60].rstrip("-") or default_slug
    return f"{username}/{slug}"


def _create_or_update_dataset(
    client: KaggleClient,
    dataset_slug: str,
    *,
    title: str,
    filename: str,
    data: bytes,
) -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        (tmp_path / filename).write_bytes(data)
        (tmp_path / "dataset-metadata.json").write_text(
            json.dumps(
                {
                    "title": title,
                    "id": dataset_slug,
                    "licenses": [{"name": "CC0-1.0"}],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        try:
            client.create_dataset(tmp_path)
        except Exception:
            logger.exception(
                "video_announce.story dataset create failed, trying version update dataset=%s",
                dataset_slug,
            )
            client.create_dataset_version(
                tmp_path,
                version_notes="refresh story secrets",
                quiet=True,
                convert_to_csv=False,
                dir_mode="zip",
            )
    return dataset_slug


async def ensure_story_secret_datasets(client: KaggleClient) -> list[str]:
    payload = build_story_secrets_payload()
    encrypted, key = encrypt_secret(payload)
    if not encrypted or not key:
        raise RuntimeError("Failed to encrypt story secrets payload")

    cipher_slug = _normalize_dataset_slug(
        "VIDEO_ANNOUNCE_STORY_CIPHER_DATASET",
        DEFAULT_CIPHER_DATASET_SLUG,
    )
    key_slug = _normalize_dataset_slug(
        "VIDEO_ANNOUNCE_STORY_KEY_DATASET",
        DEFAULT_KEY_DATASET_SLUG,
    )
    await asyncio.to_thread(
        _create_or_update_dataset,
        client,
        cipher_slug,
        title="CrumpleVideo story secrets (cipher)",
        filename=STORY_PUBLISH_CIPHER_FILENAME,
        data=encrypted,
    )
    await asyncio.to_thread(
        _create_or_update_dataset,
        client,
        key_slug,
        title="CrumpleVideo story secrets (key)",
        filename=STORY_PUBLISH_KEY_FILENAME,
        data=key,
    )
    return [cipher_slug, key_slug]


def _dedupe_targets(targets: list[StoryTarget]) -> list[StoryTarget]:
    seen: set[str] = set()
    deduped: list[StoryTarget] = []
    for target in targets:
        key = target.peer.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(target)
    return deduped


def _parse_targets_json_env(env_key: str) -> list[StoryTarget]:
    raw = (_get_env_value(env_key) or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{env_key} must be valid JSON") from exc
    if not isinstance(payload, list):
        raise RuntimeError(f"{env_key} must be a JSON list")

    targets: list[StoryTarget] = []
    for idx, item in enumerate(payload):
        if isinstance(item, str):
            peer = _normalize_peer(item)
            label = peer or f"extra-{idx + 1}"
            delay_seconds = 0
        elif isinstance(item, dict):
            peer = _normalize_peer(str(item.get("peer") or item.get("target") or ""))
            label = str(item.get("label") or peer or f"extra-{idx + 1}")
            try:
                delay_seconds = max(0, int(item.get("delay_seconds") or 0))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"{env_key}[{idx}].delay_seconds must be int"
                ) from exc
        else:
            raise RuntimeError(f"{env_key} items must be strings or objects")
        if not peer:
            raise RuntimeError(f"{env_key}[{idx}] is missing peer/target")
        targets.append(
            StoryTarget(peer=peer, label=label.strip() or peer, delay_seconds=delay_seconds)
        )
    return targets


def _parse_story_targets_json() -> list[StoryTarget]:
    return _parse_targets_json_env("VIDEO_ANNOUNCE_STORY_TARGETS_JSON")


def _parse_extra_targets_json() -> list[StoryTarget]:
    return _parse_targets_json_env("VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON")


async def _resolve_main_target(
    db,
    main_chat_id: int | None,
) -> StoryTarget | None:
    if not main_chat_id or not _env_enabled("VIDEO_ANNOUNCE_STORY_USE_MAIN_CHANNEL", default=True):
        return None
    async with db.get_session() as session:
        channel = await session.get(Channel, main_chat_id)
    if channel and channel.username:
        peer = f"@{channel.username.lstrip('@')}"
        label = peer
    else:
        peer = str(main_chat_id)
        label = channel.title if channel and channel.title else peer
    return StoryTarget(peer=peer, label=label)


def _normalize_story_event_dates(selected_event_dates: list[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in selected_event_dates or []:
        value = str(raw or "").strip()
        if not value:
            continue
        base = value.split("..", 1)[0].strip()
        try:
            parsed = date.fromisoformat(base).isoformat()
        except ValueError:
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return normalized


def _story_period_seconds(
    *,
    selected_event_dates: list[str] | None,
) -> int:
    override = _read_optional_positive_int_env("VIDEO_ANNOUNCE_STORY_PERIOD_SECONDS")
    if override is not None:
        return override
    unique_dates = _normalize_story_event_dates(selected_event_dates)
    if len(unique_dates) == 1:
        return DEFAULT_SINGLE_DAY_STORY_PERIOD_SECONDS
    return DEFAULT_MULTI_DAY_STORY_PERIOD_SECONDS


def _story_should_be_pinned(
    *,
    mode: str,
    smoke_only: bool,
) -> bool:
    if smoke_only or mode != "video":
        return False
    return _env_enabled("VIDEO_ANNOUNCE_STORY_PINNED", default=True)


async def build_story_publish_config(
    db,
    *,
    main_chat_id: int | None,
    selection_params: dict[str, Any] | None = None,
    selected_event_dates: list[str] | None = None,
) -> dict[str, Any] | None:
    if not story_publish_enabled():
        return None
    selection_params = selection_params or {}
    targets = _parse_story_targets_json()
    if not targets:
        main_target = await _resolve_main_target(db, main_chat_id)
        if main_target:
            targets.append(main_target)
        targets.extend(_parse_extra_targets_json())
    targets = _dedupe_targets(targets)
    if not targets:
        logger.info("video_announce.story: enabled but no targets configured")
        return None

    raw_mode = str(selection_params.get("story_publish_mode") or "video").strip().lower()
    mode = raw_mode if raw_mode in {"video", "image"} else "video"
    smoke_only = bool(
        selection_params.get("story_smoke_only")
        or selection_params.get("story_publish_smoke_only")
    )
    if smoke_only:
        mode = "image"

    caption = selection_params.get("story_caption")
    caption_text = str(caption).strip() if isinstance(caption, str) else ""

    config = {
        "version": 1,
        "mode": mode,
        "smoke_only": smoke_only,
        "period_seconds": _story_period_seconds(selected_event_dates=selected_event_dates),
        "pinned": _story_should_be_pinned(mode=mode, smoke_only=smoke_only),
        "caption": caption_text or None,
        "targets": [target.as_dict() for target in targets],
    }
    logger.info(
        "video_announce.story config mode=%s smoke_only=%s period_seconds=%s pinned=%s dates=%s targets=%s",
        mode,
        smoke_only,
        config["period_seconds"],
        config["pinned"],
        _normalize_story_event_dates(selected_event_dates),
        [target.peer for target in targets],
    )
    return config
