"""Production-only provider bindings for the private Social Workspace.

The core MCP package stays provider-neutral.  This module is imported lazily by
``main_part2.create_app`` only when the universal social master switch is on.
It exposes fixed high-level adapters, never raw Telethon or VK methods.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from private_events_mcp.config import PrivateEventsMCPConfig
from private_events_mcp.repository import redact_and_clip_untrusted
from private_events_mcp.social_workspace import (
    MediaRole,
    SocialAction,
    SocialTargetKind,
)
from private_events_mcp_provider_adapters import build_role_scoped_telegram_client
from private_events_mcp_vk_adapter import (
    VK_API_VERSION,
    VK_FIXED_METHOD_ALLOWLIST,
    VKActor,
    VKWorkspaceAdapter,
)


class ProviderBindingError(RuntimeError):
    """Sanitized production binding error."""


def _bounded_ref(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(24)}"


class _BoundedMap:
    def __init__(self, maximum: int = 20_000) -> None:
        self.maximum = maximum
        self.values: dict[str, Any] = {}
        self.lock = threading.RLock()

    def put(self, key: str, value: Any) -> None:
        with self.lock:
            if key not in self.values and len(self.values) >= self.maximum:
                raise ProviderBindingError("provider reference capacity exhausted")
            self.values[key] = value

    def get(self, key: str) -> Any:
        with self.lock:
            if key not in self.values:
                raise ProviderBindingError("provider reference is unknown")
            return self.values[key]


class InMemoryVKOpaqueRefStore:
    """Process-local inner refs; outer OAuth refs remain durable and encrypted."""

    def __init__(self) -> None:
        self._maps = {kind: _BoundedMap() for kind in ("target", "item", "asset")}

    def mint(self, kind: str, native_value: Mapping[str, Any]) -> str:
        if kind not in self._maps or not isinstance(native_value, Mapping):
            raise ProviderBindingError("invalid VK provider reference")
        ref = _bounded_ref({"target": "tgt", "item": "itm", "asset": "ast"}[kind])
        self._maps[kind].put(ref, copy.deepcopy(dict(native_value)))
        return ref

    def resolve(self, kind: str, opaque_ref: str) -> Mapping[str, Any]:
        if kind not in self._maps:
            raise ProviderBindingError("invalid VK provider reference")
        return copy.deepcopy(self._maps[kind].get(opaque_ref))


class SQLiteProviderCoordinator:
    """Small cross-process lease/rate/cooldown state in the isolated auth DB."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.RLock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS social_provider_vk_state (
                    actor TEXT PRIMARY KEY,
                    next_call_ms INTEGER NOT NULL DEFAULT 0,
                    captcha_until_ms INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS social_provider_tg_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    fence_seq INTEGER NOT NULL DEFAULT 0,
                    lease_hash TEXT,
                    lease_until_ms INTEGER NOT NULL DEFAULT 0,
                    flood_until_ms INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS social_provider_tg_operation (
                    operation_ref TEXT PRIMARY KEY,
                    action_digest TEXT NOT NULL,
                    result_json TEXT,
                    claimed_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO social_provider_tg_state(singleton) VALUES(1);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=3.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=3000")
        return conn

    @staticmethod
    def now_ms() -> int:
        return int(time.time() * 1000)


class DurableVKGovernor:
    def __init__(self, state: SQLiteProviderCoordinator, *, interval_ms: int = 400) -> None:
        self.state = state
        self.interval_ms = max(100, min(interval_ms, 2000))

    async def before_call(self, actor: VKActor, _capability: str) -> None:
        now = self.state.now_ms()
        with self.state._lock, self.state._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT next_call_ms FROM social_provider_vk_state WHERE actor=?",
                (actor.value,),
            ).fetchone()
            reserved = max(now, int(row["next_call_ms"]) if row else 0)
            conn.execute(
                """INSERT INTO social_provider_vk_state(actor,next_call_ms,updated_at_ms)
                   VALUES(?,?,?) ON CONFLICT(actor) DO UPDATE SET
                   next_call_ms=excluded.next_call_ms,updated_at_ms=excluded.updated_at_ms""",
                (actor.value, reserved + self.interval_ms, now),
            )
            conn.execute("COMMIT")
        delay = (reserved - now) / 1000
        if delay > 3:
            raise ProviderBindingError("VK provider rate budget unavailable")
        if delay > 0:
            await asyncio.sleep(delay)

    async def after_call(self, _actor: VKActor, _capability: str, _outcome: str) -> None:
        return None


class DurableVKCooldown:
    def __init__(self, state: SQLiteProviderCoordinator, *, captcha_seconds: int = 600) -> None:
        self.state = state
        self.captcha_ms = max(60, min(captcha_seconds, 3600)) * 1000

    async def ensure_available(self, actor: VKActor) -> None:
        with self.state._lock, self.state._connect() as conn:
            row = conn.execute(
                "SELECT captcha_until_ms FROM social_provider_vk_state WHERE actor=?",
                (actor.value,),
            ).fetchone()
        if row and int(row["captcha_until_ms"]) > self.state.now_ms():
            raise ProviderBindingError("VK provider cooldown active")

    async def record_captcha(self, actor: VKActor) -> None:
        now = self.state.now_ms()
        with self.state._lock, self.state._connect() as conn:
            conn.execute(
                """INSERT INTO social_provider_vk_state(actor,captcha_until_ms,updated_at_ms)
                   VALUES(?,?,?) ON CONFLICT(actor) DO UPDATE SET
                   captcha_until_ms=excluded.captcha_until_ms,
                   updated_at_ms=excluded.updated_at_ms""",
                (actor.value, now + self.captcha_ms, now),
            )

    async def record_success(self, _actor: VKActor) -> None:
        return None


_VK_TOKEN_ENVS: Mapping[VKActor, str] = {
    VKActor.PUBLIC_READER: "PRIVATE_EVENTS_MCP_VK_PUBLIC_READER_TOKEN",
    VKActor.DIALOG_READER: "PRIVATE_EVENTS_MCP_VK_DIALOG_READER_TOKEN",
    VKActor.USER_MESSENGER: "PRIVATE_EVENTS_MCP_VK_USER_MESSENGER_TOKEN",
    VKActor.COMMUNITY_EDITOR: "PRIVATE_EVENTS_MCP_VK_COMMUNITY_EDITOR_TOKEN",
    VKActor.ANALYTICS_READER: "PRIVATE_EVENTS_MCP_VK_ANALYTICS_READER_TOKEN",
    VKActor.STORY_READER: "PRIVATE_EVENTS_MCP_VK_STORY_READER_TOKEN",
    VKActor.STORY_EDITOR: "PRIVATE_EVENTS_MCP_VK_STORY_EDITOR_TOKEN",
}


class DedicatedVKActorTransport:
    """Fixed-host VK transport with explicit actor credentials and no fallback."""

    def __init__(
        self,
        *,
        allowed: Mapping[VKActor, frozenset[str]],
        environ: Mapping[str, str] | None = None,
        response_cap_bytes: int = 2 * 1024 * 1024,
    ) -> None:
        self.allowed = dict(allowed)
        self.environ = os.environ if environ is None else environ
        self.response_cap_bytes = response_cap_bytes

    def _token(self, actor: VKActor) -> str:
        return str(self.environ.get(_VK_TOKEN_ENVS[actor]) or "").strip()

    def permits(self, actor: VKActor, capability: str) -> bool:
        return capability in self.allowed.get(actor, frozenset()) and bool(self._token(actor))

    @staticmethod
    def _form_value(value: Any) -> str:
        if isinstance(value, Mapping):
            return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"))
        if isinstance(value, (list, tuple)):
            return ",".join(str(item) for item in value)
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)

    async def invoke(
        self,
        *,
        actor: VKActor,
        method: str,
        params: Mapping[str, Any],
        version: str,
        timeout_seconds: float,
    ) -> Mapping[str, Any] | list[Any]:
        if method not in VK_FIXED_METHOD_ALLOWLIST or version != VK_API_VERSION:
            raise ProviderBindingError("VK method contract rejected")
        token = self._token(actor)
        if not token:
            raise ProviderBindingError("VK actor is unavailable")
        from aiohttp import ClientSession, ClientTimeout

        form = {key: self._form_value(value) for key, value in params.items()}
        form.update({"access_token": token, "v": VK_API_VERSION})
        timeout = ClientTimeout(total=max(1.0, min(float(timeout_seconds), 30.0)))
        async with ClientSession(timeout=timeout) as session, session.post(
                f"https://api.vk.com/method/{method}",
                data=form,
                allow_redirects=False,
        ) as response:
            if response.status != 200:
                raise ProviderBindingError("VK provider unavailable")
            if response.content_length and response.content_length > self.response_cap_bytes:
                raise ProviderBindingError("VK provider response too large")
            body = await response.content.read(self.response_cap_bytes + 1)
        if len(body) > self.response_cap_bytes:
            raise ProviderBindingError("VK provider response too large")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ProviderBindingError("VK provider response invalid") from None
        if not isinstance(payload, Mapping):
            raise ProviderBindingError("VK provider response invalid")
        if "error" in payload:
            error = payload.get("error")
            code = error.get("error_code") if isinstance(error, Mapping) else None
            return {"error": {"error_code": code if type(code) is int else 0}}
        result = payload.get("response")
        if not isinstance(result, (Mapping, list)):
            raise ProviderBindingError("VK provider response invalid")
        return result


def _vk_allowed(config: PrivateEventsMCPConfig) -> dict[VKActor, frozenset[str]]:
    allowed: dict[VKActor, set[str]] = {
        VKActor.PUBLIC_READER: {"discover", "read_public", "search_public", "audience"},
        VKActor.DIALOG_READER: set(),
        VKActor.USER_MESSENGER: set(),
        VKActor.COMMUNITY_EDITOR: set(),
        VKActor.ANALYTICS_READER: {"analytics"},
        VKActor.STORY_READER: set(),
        VKActor.STORY_EDITOR: set(),
    }
    if config.universal_social_private_read_enabled:
        allowed[VKActor.DIALOG_READER].update({"dialogs", "search_newsfeed"})
    if config.universal_social_dm_enabled:
        allowed[VKActor.USER_MESSENGER].add("dm_send")
    if config.universal_social_post_enabled:
        allowed[VKActor.USER_MESSENGER].add("forward")
        allowed[VKActor.COMMUNITY_EDITOR].update(
            {"post_publish", "comment", "reaction", "forward"}
        )
    if config.universal_social_edit_delete_enabled:
        allowed[VKActor.USER_MESSENGER].update({"edit", "delete"})
        allowed[VKActor.COMMUNITY_EDITOR].update({"edit", "delete"})
    if config.universal_social_media_story_enabled:
        allowed[VKActor.STORY_READER].add("story_read")
        allowed[VKActor.STORY_EDITOR].add("story_write")
    return {actor: frozenset(capabilities) for actor, capabilities in allowed.items()}


def _sanitize_provider_text(value: str) -> str:
    sanitized = redact_and_clip_untrusted(value, limit=8192)
    return sanitized if isinstance(sanitized, str) else "[redacted]"


def build_vk_workspace_adapter(config: PrivateEventsMCPConfig) -> VKWorkspaceAdapter:
    state = SQLiteProviderCoordinator(config.auth_database_path)
    return VKWorkspaceAdapter(
        transport=DedicatedVKActorTransport(allowed=_vk_allowed(config)),
        refs=InMemoryVKOpaqueRefStore(),
        governor=DurableVKGovernor(state),
        cooldown=DurableVKCooldown(state),
        sanitize_text=_sanitize_provider_text,
        timeout_seconds=config.social_provider_timeout_seconds,
    )


def build_private_events_mcp_workspace_adapters(
    config: PrivateEventsMCPConfig,
) -> dict[str, Any]:
    """Build only explicitly enabled provider adapters without making calls."""

    if not config.universal_social_enabled:
        return {}
    adapters: dict[str, Any] = {}
    if config.universal_social_telegram_enabled:
        # Telegram implementation and durable ref/governor wiring are added by
        # the same integration before this builder is enabled in production.
        adapters["telegram"] = build_telegram_workspace_adapter(config)
    if config.universal_social_vk_enabled:
        adapters["vk"] = build_vk_workspace_adapter(config)
    return adapters


def build_telegram_workspace_adapter(config: PrivateEventsMCPConfig) -> Any:
    from private_events_mcp_telegram_adapter import TelegramWorkspaceAdapter

    return TelegramWorkspaceAdapter(
        client_factory=build_role_scoped_telegram_client,
        refs=InMemoryTelegramOpaqueRefStore(config),
        governor=DurableTelegramGovernor(
            SQLiteProviderCoordinator(config.auth_database_path)
        ),
        operation_timeout_seconds=config.social_provider_timeout_seconds,
    )


class InMemoryTelegramOpaqueRefStore:
    """Bounded, process-local Telethon entity store behind durable outer refs."""

    def __init__(self, config: PrivateEventsMCPConfig) -> None:
        from private_events_mcp_telegram_adapter import (
            TelegramAssetBinding,
            TelegramItemBinding,
            TelegramOperationClaim,
            TelegramTargetBinding,
        )

        self._Target = TelegramTargetBinding
        self._Item = TelegramItemBinding
        self._Asset = TelegramAssetBinding
        self._Claim = TelegramOperationClaim
        self._targets = _BoundedMap()
        self._items = _BoundedMap()
        self._assets = _BoundedMap()
        self._cursors = _BoundedMap()
        self._operation_lock = threading.RLock()
        self._state = SQLiteProviderCoordinator(config.auth_database_path)
        actions: set[SocialAction] = set()
        if config.universal_social_dm_enabled:
            actions.add(SocialAction.SEND_MESSAGE)
        if config.universal_social_post_enabled:
            actions.update(
                {
                    SocialAction.PUBLISH,
                    SocialAction.COMMENT,
                    SocialAction.REACTION,
                    SocialAction.FORWARD,
                    SocialAction.SCHEDULE,
                }
            )
        if config.universal_social_edit_delete_enabled:
            actions.update({SocialAction.EDIT, SocialAction.DELETE})
        if config.universal_social_media_story_enabled:
            actions.add(SocialAction.STORY)
        self._allowed_actions = frozenset(actions)

    @staticmethod
    def _copy(value: Any) -> Any:
        try:
            result = copy.deepcopy(value)
        except Exception:  # noqa: BLE001 - Telethon entities vary but failures are sanitized
            raise ProviderBindingError("Telegram binding cannot be snapshotted") from None
        if result is value:
            raise ProviderBindingError("Telegram binding cannot be detached")
        return result

    def resolve_target(self, target_ref: str) -> Any:
        return self._copy(self._targets.get(target_ref))

    def resolve_item(self, item_ref: str) -> Any:
        return copy.deepcopy(self._items.get(item_ref))

    def resolve_asset(self, asset_ref: str) -> Any:
        return copy.deepcopy(self._assets.get(asset_ref))

    def mint_target(
        self,
        *,
        entity: Any,
        kind: SocialTargetKind,
        title: str,
        canonical_handle: str | None,
        profile_link: str | None,
        is_self: bool,
    ) -> Any:
        ref = _bounded_ref("tgt")
        entity_copy = self._copy(entity)
        native = f"{kind.value}:{getattr(entity_copy, 'id', 'self')}:{canonical_handle or ''}"
        binding = self._Target(
            ref,
            kind,
            entity_copy,
            title,
            canonical_handle,
            profile_link,
            is_self=is_self,
            allowed_actions=self._allowed_actions,
            binding_version=hashlib.sha256(native.encode()).hexdigest(),
        )
        self._targets.put(ref, binding)
        return self._copy(binding)

    def mint_item(
        self,
        *,
        target_ref: str,
        message_id: int,
        allowed_actions: frozenset[SocialAction] | None = None,
    ) -> Any:
        self._targets.get(target_ref)
        ref = _bounded_ref("itm")
        binding = self._Item(
            ref,
            target_ref,
            message_id,
            allowed_actions=(allowed_actions or self._allowed_actions) & self._allowed_actions,
        )
        self._items.put(ref, binding)
        return copy.deepcopy(binding)

    def mint_read_asset(self, *, target_ref: str, media: Any, role: MediaRole) -> str:
        self._targets.get(target_ref)
        ref = _bounded_ref("ast")
        self._assets.put(ref, self._Asset(ref, role, self._copy(media)))
        return ref

    def mint_cursor(self, *, family: str, state: Mapping[str, Any]) -> str:
        ref = _bounded_ref("cur")
        self._cursors.put(ref, (family, copy.deepcopy(dict(state))))
        return ref

    def resolve_cursor(self, *, family: str, cursor: str) -> Mapping[str, Any]:
        stored_family, state = self._cursors.get(cursor)
        if not hmac.compare_digest(stored_family, family):
            raise ProviderBindingError("Telegram cursor context mismatch")
        return copy.deepcopy(state)

    def claim_operation(self, *, operation_ref: str, action_digest: str) -> Any:
        now = self._state.now_ms()
        with self._operation_lock, self._state._lock, self._state._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM social_provider_tg_operation WHERE operation_ref=?",
                (operation_ref,),
            ).fetchone()
            if row is None:
                conn.execute(
                    """INSERT INTO social_provider_tg_operation(
                       operation_ref,action_digest,claimed_at_ms,updated_at_ms)
                       VALUES(?,?,?,?)""",
                    (operation_ref, action_digest, now, now),
                )
                conn.execute("COMMIT")
                return self._Claim(operation_ref, action_digest, True, None)
            conn.execute("COMMIT")
        if row["action_digest"] != action_digest:
            raise ProviderBindingError("Telegram operation reference conflict")
        result = json.loads(row["result_json"]) if row["result_json"] else None
        return self._Claim(operation_ref, action_digest, False, result)

    def release_operation(self, *, operation_ref: str, action_digest: str) -> bool:
        with self._operation_lock, self._state._lock, self._state._connect() as conn:
            changed = conn.execute(
                """DELETE FROM social_provider_tg_operation
                   WHERE operation_ref=? AND action_digest=? AND result_json IS NULL""",
                (operation_ref, action_digest),
            ).rowcount
        return changed == 1

    def complete_operation(
        self,
        *,
        operation_ref: str,
        action_digest: str,
        result: Mapping[str, Any],
    ) -> Any:
        encoded = json.dumps(dict(result), ensure_ascii=False, sort_keys=True)
        with self._operation_lock, self._state._lock, self._state._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT action_digest,result_json FROM social_provider_tg_operation WHERE operation_ref=?",
                (operation_ref,),
            ).fetchone()
            if row is None or row["action_digest"] != action_digest:
                conn.execute("ROLLBACK")
                raise ProviderBindingError("Telegram operation reference conflict")
            if row["result_json"] is not None and row["result_json"] != encoded:
                conn.execute("ROLLBACK")
                raise ProviderBindingError("Telegram operation result conflict")
            conn.execute(
                """UPDATE social_provider_tg_operation SET result_json=?,updated_at_ms=?
                   WHERE operation_ref=? AND action_digest=?""",
                (encoded, self._state.now_ms(), operation_ref, action_digest),
            )
            conn.execute("COMMIT")
        return self._Claim(
            operation_ref, action_digest, False, copy.deepcopy(dict(result))
        )

    def resolve_operation(self, operation_ref: str) -> Any:
        with self._state._lock, self._state._connect() as conn:
            row = conn.execute(
                "SELECT action_digest,result_json FROM social_provider_tg_operation WHERE operation_ref=?",
                (operation_ref,),
            ).fetchone()
        if row is None:
            raise ProviderBindingError("Telegram operation is unknown")
        return self._Claim(
            operation_ref,
            row["action_digest"],
            False,
            json.loads(row["result_json"]) if row["result_json"] else None,
        )


class DurableTelegramGovernor:
    def __init__(self, state: SQLiteProviderCoordinator, *, lease_seconds: int = 150) -> None:
        self.state = state
        self.lease_ms = max(30, min(lease_seconds, 300)) * 1000

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    def cooldown_remaining(self) -> int:
        with self.state._lock, self.state._connect() as conn:
            row = conn.execute(
                "SELECT flood_until_ms FROM social_provider_tg_state WHERE singleton=1"
            ).fetchone()
        return max(0, (int(row["flood_until_ms"]) - self.state.now_ms() + 999) // 1000)

    def note_flood_wait(self, seconds: int) -> None:
        now = self.state.now_ms()
        wait_ms = max(1, min(int(seconds), 7 * 86400)) * 1000
        with self.state._lock, self.state._connect() as conn:
            conn.execute(
                """UPDATE social_provider_tg_state SET flood_until_ms=MAX(flood_until_ms,?),
                   updated_at_ms=? WHERE singleton=1""",
                (now + wait_ms, now),
            )

    def acquire(self, _operation: str) -> Any:
        from private_events_mcp_telegram_adapter import TelegramLease

        now = self.state.now_ms()
        fence = secrets.token_urlsafe(32)
        with self.state._lock, self.state._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT lease_until_ms,flood_until_ms FROM social_provider_tg_state WHERE singleton=1"
            ).fetchone()
            if int(row["flood_until_ms"]) > now:
                conn.execute("ROLLBACK")
                raise ProviderBindingError("Telegram flood cooldown active")
            if int(row["lease_until_ms"]) > now:
                conn.execute("ROLLBACK")
                raise ProviderBindingError("Telegram session lease is busy")
            conn.execute(
                """UPDATE social_provider_tg_state SET fence_seq=fence_seq+1,
                   lease_hash=?,lease_until_ms=?,updated_at_ms=? WHERE singleton=1""",
                (self._hash(fence), now + self.lease_ms, now),
            )
            conn.execute("COMMIT")
        return TelegramLease(fence)

    def assert_current(self, lease: Any) -> bool:
        fence = getattr(lease, "fence", "")
        if not isinstance(fence, str) or not fence:
            return False
        with self.state._lock, self.state._connect() as conn:
            row = conn.execute(
                "SELECT lease_hash,lease_until_ms FROM social_provider_tg_state WHERE singleton=1"
            ).fetchone()
        return hmac.compare_digest(str(row["lease_hash"] or ""), self._hash(fence)) and int(
            row["lease_until_ms"]
        ) > self.state.now_ms()

    def release(self, lease: Any) -> None:
        fence = getattr(lease, "fence", "")
        if not isinstance(fence, str) or not fence:
            return
        now = self.state.now_ms()
        with self.state._lock, self.state._connect() as conn:
            conn.execute(
                """UPDATE social_provider_tg_state SET lease_hash=NULL,lease_until_ms=0,
                   updated_at_ms=? WHERE singleton=1 AND lease_hash=?""",
                (now, self._hash(fence)),
            )


__all__ = [
    "DedicatedVKActorTransport",
    "DurableTelegramGovernor",
    "DurableVKCooldown",
    "DurableVKGovernor",
    "InMemoryTelegramOpaqueRefStore",
    "InMemoryVKOpaqueRefStore",
    "ProviderBindingError",
    "SQLiteProviderCoordinator",
    "build_private_events_mcp_workspace_adapters",
    "build_telegram_workspace_adapter",
    "build_vk_workspace_adapter",
]
