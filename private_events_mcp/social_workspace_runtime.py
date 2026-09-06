"""Durable, provider-neutral runtime for the private Social Workspace tools.

The runtime owns every security boundary around a deliberately tiny adapter
protocol.  Adapters never receive OAuth identity data and their native ids are
encrypted in the OAuth SQLite database before an opaque reference is returned.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import hmac
import inspect
import io
import json
import logging
import re
import secrets
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import urlencode

from .access_policy import social_scopes_authorized
from .auth_store import OAuthStateStore
from .media_contract import AssetIngestor, VerifiedAsset
from .social_workspace import (
    DIRECT_USER_AUTHORIZED_ACTIONS,
    SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_DIALOG_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_RESOLVE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_SEARCH_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_NOTIFICATIONS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_SCHEDULED_ITEMS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
    AssetStageRequest,
    MediaRole,
    SocialAction,
    SocialActionIntent,
    SocialActionStatus,
    SocialPlatform,
    SocialReadOperation,
    SocialReadRequest,
    SocialScheduledItemsRequest,
    SocialWorkspaceValidationError,
    compute_action_digest,
    validate_action_status_response,
    validate_capabilities,
    validate_document_attachment_policy,
    validate_editorial_sample_response,
    validate_resolved_target_preview,
)
from .tool_catalog import ToolCallContext, ToolExecutionResult

logger = logging.getLogger(__name__)

# One read response can contain 25 logical items with up to 10 attachments
# each.  Registration is deliberately small-bounded, but every attachment must
# finish one individually bounded materialization attempt before the shared
# durable-store wait begins.
TRANSCRIPTION_REGISTRATION_CONCURRENCY = 3
MAX_TRANSCRIPTION_ATTACHMENTS_PER_READ = 250


class SocialWorkspaceRuntimeError(SocialWorkspaceValidationError):
    """A fail-closed runtime policy or durable-state check failed."""

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        retry_safe: bool = False,
        audit_reason_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retry_safe = bool(retry_safe)
        self.audit_reason_code = audit_reason_code or (
            error_code.lower() if error_code else type(self).__name__.lower()
        )


class SocialWorkspaceAdapter(Protocol):
    """Stable provider adapter surface; native provider methods stay private."""

    async def capabilities(self, target_ref: str | None) -> Mapping[str, Any]: ...
    async def resolve(self, request: SocialReadRequest) -> Mapping[str, Any]: ...
    async def read(self, request: SocialReadRequest) -> Mapping[str, Any]: ...
    async def execute(
        self, intent: SocialActionIntent, *, operation_ref: str
    ) -> Mapping[str, Any]: ...
    async def reconcile(self, operation_ref: str) -> Mapping[str, Any]: ...
    async def scheduled_items(
        self,
        *,
        target_ref: str,
        scheduled_from: str | None = None,
        scheduled_to: str | None = None,
        text_sha256: str | None = None,
        media_count: int | None = None,
        limit: int = 10,
    ) -> Mapping[str, Any]: ...
    async def retry(
        self,
        intent: SocialActionIntent,
        *,
        operation_ref: str,
        attempt_number: int,
    ) -> Mapping[str, Any]: ...
    async def stage_asset(
        self, asset: VerifiedAsset, *, role: MediaRole
    ) -> str: ...
    async def read_asset(
        self, asset_ref: str, *, owner_binding: str, max_bytes: int
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class RuntimePrincipal:
    client_id: str
    subject: str
    resource: str
    scopes: frozenset[str]

    @classmethod
    def from_context(cls, context: ToolCallContext) -> RuntimePrincipal:
        if not isinstance(context, ToolCallContext):
            raise SocialWorkspaceRuntimeError("authenticated tool context is required")
        identity = context.identity
        if not identity.client_id or not identity.subject or not context.resource:
            raise SocialWorkspaceRuntimeError("authenticated principal is incomplete")
        return cls(identity.client_id, identity.subject, context.resource, identity.scopes)


@dataclass(frozen=True, slots=True)
class SocialBudgetLimits:
    attempts: int = 1000
    rate: int = 1000
    egress: int = 16 * 1024 * 1024
    media: int = 1000

    def for_dimension(self, metric: str, dimension: str) -> int:
        """Return independently configurable conservative layer defaults.

        A caller may still pass explicit per-dimension overrides to the runtime;
        these defaults avoid pretending that global, principal, target and
        action limits are the same policy boundary.
        """

        base = int(getattr(self, metric))
        if dimension == "global":
            return max(1, base * 10)
        if dimension == "principal":
            return max(1, base)
        if dimension == "action":
            return max(1, base // 2)
        if dimension == "target":
            return max(1, base // 4)
        raise ValueError("unknown budget dimension")


_REF_RE = re.compile(r"^(tgt|itm|ast)_[A-Za-z0-9_-]{16,160}$")
_INGESTED_REF_RE = re.compile(r"^ing_[A-Za-z0-9_-]{24,160}$")
_CONTENT_DIGEST_RE = re.compile(r"^sha256:[a-f0-9]{64}$")
_MIME_TYPE_RE = re.compile(
    r"^(?:image|video|audio|application|text)/[A-Za-z0-9.+-]{1,64}$"
)
_SECRET_KEY = re.compile(
    r"(?:^id$|(?:provider|peer|owner|chat|user)_id$|provider|native|identifier|access_hash|token|secret|password|authorization|cookie|session|raw|method|path|url)",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(r"(?i)(?:bearer\s+[a-z0-9._~-]{8,}|(?:token|secret|password)=\S+)")


def _now_rfc3339(now: int) -> str:
    return datetime.fromtimestamp(now, timezone.utc).isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class SocialWorkspaceRuntime:
    """ChatGPT social orchestration backed only by the OAuth/auth SQLite file."""

    def __init__(
        self,
        *,
        store: OAuthStateStore,
        adapters: Mapping[str, SocialWorkspaceAdapter],
        encryption_key: str,
        policy_version: str = "social-workspace-v1",
        provider_timeout_seconds: float = 15.0,
        reference_ttl_seconds: int = 30 * 86400,
        preparation_ttl_seconds: int = 600,
        approval_ttl_seconds: int = 300,
        sample_ttl_seconds: int = 3600,
        response_cap_bytes: int = 128 * 1024,
        budget_limits: SocialBudgetLimits | Mapping[str, int] | None = None,
        budget_dimension_limits: Mapping[str, Mapping[str, int]] | None = None,
        circuit_failure_threshold: int = 3,
        circuit_cooldown_seconds: int = 60,
        approval_url_base: str | None = None,
        asset_ingestor: AssetIngestor | None = None,
        asset_max_bytes: int = 30 * 1024 * 1024,
        document_max_bytes: int = 48 * 1024 * 1024,
        asset_ttl_seconds: int = 3600,
        asset_ingest_timeout_seconds: float = 20.0,
        asset_max_width: int = 8192,
        asset_max_height: int = 8192,
        asset_max_pixels: int = 40_000_000,
        media_story_enabled: bool = True,
        file_send_enabled: bool = False,
        clock: Any = time.time,
    ) -> None:
        if not isinstance(store, OAuthStateStore):
            raise TypeError("store must be OAuthStateStore")
        if not isinstance(encryption_key, str) or len(encryption_key) < 16:
            raise ValueError("encryption_key must contain at least 16 characters")
        self.store = store
        self.adapters = dict(adapters)
        if set(self.adapters) - {"telegram", "vk"}:
            raise ValueError("unsupported social adapter")
        self._key = hashlib.sha256(encryption_key.encode("utf-8")).digest()
        self.policy_version = policy_version
        self.provider_timeout_seconds = float(provider_timeout_seconds)
        self.reference_ttl_seconds = int(reference_ttl_seconds)
        self.preparation_ttl_seconds = int(preparation_ttl_seconds)
        self.approval_ttl_seconds = int(approval_ttl_seconds)
        self.sample_ttl_seconds = int(sample_ttl_seconds)
        self.response_cap_bytes = int(response_cap_bytes)
        if budget_limits is None:
            self.budgets = SocialBudgetLimits()
        elif isinstance(budget_limits, SocialBudgetLimits):
            self.budgets = budget_limits
        else:
            self.budgets = SocialBudgetLimits(**dict(budget_limits))
        if any(getattr(self.budgets, field) < 1 for field in ("attempts", "rate", "egress", "media")):
            raise ValueError("budget limits must be positive")
        self._budget_dimension_limits: dict[str, dict[str, int]] = {}
        for metric in ("attempts", "rate", "egress", "media"):
            configured = dict((budget_dimension_limits or {}).get(metric, {}))
            values: dict[str, int] = {}
            for dimension in ("global", "principal", "target", "action"):
                value = configured.get(
                    dimension, self.budgets.for_dimension(metric, dimension)
                )
                if type(value) is not int or value < 1:
                    raise ValueError("budget dimension limits must be positive integers")
                values[dimension] = value
            self._budget_dimension_limits[metric] = values
        self.circuit_failure_threshold = max(1, int(circuit_failure_threshold))
        self.circuit_cooldown_seconds = max(1, int(circuit_cooldown_seconds))
        if approval_url_base is not None and not approval_url_base.startswith(
            ("https://", "http://127.0.0.1", "http://localhost")
        ):
            raise ValueError("approval_url_base must be HTTPS or local HTTP")
        self.approval_url_base = approval_url_base
        self.asset_ingestor = asset_ingestor
        self.asset_max_bytes = int(asset_max_bytes)
        self.document_max_bytes = int(document_max_bytes)
        self.asset_ttl_seconds = int(asset_ttl_seconds)
        self.asset_ingest_timeout_seconds = float(asset_ingest_timeout_seconds)
        self.asset_max_width = int(asset_max_width)
        self.asset_max_height = int(asset_max_height)
        self.asset_max_pixels = int(asset_max_pixels)
        self.media_story_enabled = bool(media_story_enabled)
        self.file_send_enabled = bool(file_send_enabled)
        self.audio_transcription_service: Any | None = None
        if self.asset_max_bytes < 1 or self.asset_max_bytes > 64 * 1024 * 1024:
            raise ValueError("asset_max_bytes is outside the media budget")
        if self.document_max_bytes < 1 or self.document_max_bytes > 64 * 1024 * 1024:
            raise ValueError("document_max_bytes is outside the document budget")
        if self.asset_ttl_seconds < 60 or self.asset_ttl_seconds > 86400:
            raise ValueError("asset_ttl_seconds is outside the supported TTL")
        if not 1 <= self.asset_ingest_timeout_seconds <= 120:
            raise ValueError("asset ingest timeout is outside the supported range")
        if not 1 <= self.asset_max_width <= 8192 or not 1 <= self.asset_max_height <= 8192:
            raise ValueError("asset dimensions are outside the supported range")
        if not 1 <= self.asset_max_pixels <= 40_000_000:
            raise ValueError("asset pixel budget is outside the supported range")
        self._clock = clock

    def enable_audio_transcription(self, service: Any) -> None:
        """Attach the existing audio service to trusted Telegram read ingress."""

        required = ("start_provider_transcription", "wait_many", "get_many")
        if any(not callable(getattr(service, name, None)) for name in required):
            raise TypeError("audio transcription service contract is incomplete")
        self.audio_transcription_service = service

    def _now(self) -> int:
        return int(self._clock())

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _binding(self, principal: RuntimePrincipal) -> tuple[str, str, str]:
        return tuple(self._hash(value) for value in (
            principal.client_id, principal.subject, principal.resource
        ))  # type: ignore[return-value]

    def _principal_hash(self, principal: RuntimePrincipal) -> str:
        return self._hash(
            f"{principal.client_id}\0{principal.subject}\0{principal.resource}"
        )

    def _encrypt(self, plaintext: str) -> str:
        nonce = secrets.token_bytes(16)
        raw = plaintext.encode("utf-8")
        stream = bytearray()
        counter = 0
        while len(stream) < len(raw):
            stream.extend(hmac.new(self._key, nonce + counter.to_bytes(4, "big"), hashlib.sha256).digest())
            counter += 1
        encrypted = bytes(a ^ b for a, b in zip(raw, stream))
        tag = hmac.new(self._key, nonce + encrypted, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(nonce + encrypted + tag).decode("ascii")

    def _decrypt(self, envelope: str) -> str:
        try:
            packed = base64.urlsafe_b64decode(envelope.encode("ascii"))
            nonce, encrypted, tag = packed[:16], packed[16:-32], packed[-32:]
        except Exception as exc:
            raise SocialWorkspaceRuntimeError("encrypted reference is invalid") from exc
        expected = hmac.new(self._key, nonce + encrypted, hashlib.sha256).digest()
        if len(nonce) != 16 or not hmac.compare_digest(tag, expected):
            raise SocialWorkspaceRuntimeError("encrypted reference integrity failed")
        stream = bytearray()
        counter = 0
        while len(stream) < len(encrypted):
            stream.extend(hmac.new(self._key, nonce + counter.to_bytes(4, "big"), hashlib.sha256).digest())
            counter += 1
        return bytes(a ^ b for a, b in zip(encrypted, stream)).decode("utf-8")

    def _adapter(self, platform: SocialPlatform | str) -> SocialWorkspaceAdapter:
        name = platform.value if isinstance(platform, SocialPlatform) else platform
        adapter = self.adapters.get(name)
        if adapter is None:
            raise SocialWorkspaceRuntimeError("social provider is disabled")
        return adapter

    def _mint_ref(
        self,
        kind: str,
        provider_ref: Any,
        platform: str,
        principal: RuntimePrincipal,
        *,
        expires_at: int | None = None,
    ) -> str:
        if not isinstance(provider_ref, (str, int)) or not str(provider_ref):
            raise SocialWorkspaceRuntimeError("provider returned an invalid reference")
        prefix = {"target": "tgt", "item": "itm", "asset": "ast"}[kind]
        public_ref = f"{prefix}_{secrets.token_urlsafe(24)}"
        now = self._now()
        expiry = now + self.reference_ttl_seconds
        if expires_at is not None:
            if type(expires_at) is not int or expires_at <= now:
                raise SocialWorkspaceRuntimeError("reference expiry is invalid")
            expiry = min(expiry, expires_at)
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:
            conn.execute(
                """INSERT INTO social_workspace_ref(
                    ref_hash,ref_kind,client_hash,subject_hash,resource_hash,platform,
                    policy_version,provider_ref_ciphertext,expires_at,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (self._hash(public_ref), kind, client, subject, resource, platform,
                 self.policy_version, self._encrypt(str(provider_ref)),
                 expiry, now),
            )
        return public_ref

    def _resolve_ref(
        self, public_ref: str, kind: str, platform: str, principal: RuntimePrincipal
    ) -> str:
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:
            row = conn.execute(
                """SELECT * FROM social_workspace_ref WHERE ref_hash=? AND ref_kind=?
                   AND client_hash=? AND subject_hash=? AND resource_hash=? AND platform=?
                   AND policy_version=? AND expires_at>?""",
                (self._hash(public_ref), kind, client, subject, resource, platform,
                 self.policy_version, self._now()),
            ).fetchone()
        if row is None:
            raise SocialWorkspaceRuntimeError("opaque reference is expired or not bound")
        return self._decrypt(row["provider_ref_ciphertext"])

    def _ref_platform(self, public_ref: str, kind: str, principal: RuntimePrincipal) -> str:
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:
            row = conn.execute(
                """SELECT platform FROM social_workspace_ref WHERE ref_hash=? AND ref_kind=?
                   AND client_hash=? AND subject_hash=? AND resource_hash=?
                   AND policy_version=? AND expires_at>?""",
                (self._hash(public_ref), kind, client, subject, resource,
                 self.policy_version, self._now()),
            ).fetchone()
        if row is None:
            raise SocialWorkspaceRuntimeError("opaque reference is expired or not bound")
        return str(row["platform"])

    def _store_target_preview(self, public_ref: str, value: Mapping[str, Any]) -> None:
        allowed = {
            key: value[key]
            for key in (
                "platform",
                "target_ref",
                "kind",
                "display_name",
                "canonical_handle",
                "profile_link",
            )
            if key in value
        }
        if "display_name" not in allowed and isinstance(value.get("title"), str):
            allowed["display_name"] = value["title"]
        with self.store._lock, self.store._connect() as conn:
            conn.execute(
                """INSERT INTO social_workspace_ref_preview(ref_hash,preview_json,created_at)
                   VALUES(?,?,?) ON CONFLICT(ref_hash) DO UPDATE SET
                   preview_json=excluded.preview_json,created_at=excluded.created_at""",
                (self._hash(public_ref), self._encrypt(_json(allowed)), self._now()),
            )

    def _store_item_preview(self, public_ref: str, value: Mapping[str, Any]) -> None:
        allowed = {
            key: value[key]
            for key in (
                "item_ref",
                "target_ref",
                "kind",
                "published_at",
                "scheduled_at",
                "queue",
                "text",
                "caption",
                "text_sha256",
                "media_count",
                "media_roles",
            )
            if key in value
        }
        with self.store._lock, self.store._connect() as conn:
            conn.execute(
                """INSERT INTO social_workspace_ref_preview(ref_hash,preview_json,created_at)
                   VALUES(?,?,?) ON CONFLICT(ref_hash) DO UPDATE SET
                   preview_json=excluded.preview_json,created_at=excluded.created_at""",
                (self._hash(public_ref), self._encrypt(_json(allowed)), self._now()),
            )

    def _store_scheduled_item_previews(
        self,
        value: Mapping[str, Any],
        *,
        platform: str,
        target_ref: str,
    ) -> None:
        """Keep enough closed scheduled identity for exact delete approval."""

        items = value.get("items")
        if not isinstance(items, list):
            return
        for item in items:
            if not isinstance(item, Mapping):
                continue
            item_ref = item.get("item_ref")
            if not isinstance(item_ref, str) or _REF_RE.fullmatch(item_ref) is None:
                continue
            self._store_item_preview(
                item_ref,
                {
                    **item,
                    "target_ref": target_ref,
                    "kind": (
                        "scheduled_message"
                        if platform == "telegram"
                        else "scheduled_post"
                    ),
                },
            )

    def _scheduled_item_direct_authorized_on_conn(
        self, conn: sqlite3.Connection, intent: SocialActionIntent,
        principal: RuntimePrincipal,
    ) -> bool:
        """Allow explicit edits/deletes only for a bound scheduled queue item."""
        if intent.action not in {SocialAction.EDIT, SocialAction.DELETE} or not intent.item_ref:
            return False
        client, subject, resource = self._binding(principal)
        row = conn.execute(
            """SELECT p.preview_json FROM social_workspace_ref AS r
               JOIN social_workspace_ref_preview AS p ON p.ref_hash=r.ref_hash
               WHERE r.ref_hash=? AND r.ref_kind='item' AND r.client_hash=?
                 AND r.subject_hash=? AND r.resource_hash=? AND r.platform=?
                 AND r.policy_version=? AND r.expires_at>?""",
            (self._hash(intent.item_ref), client, subject, resource,
             intent.platform.value, self.policy_version, self._now()),
        ).fetchone()
        if row is None:
            return False
        try:
            preview = json.loads(self._decrypt(str(row["preview_json"])))
        except Exception:
            return False
        return (
            isinstance(preview, dict)
            and preview.get("item_ref") == intent.item_ref
            and preview.get("queue") == "scheduled"
            and preview.get("kind") in {"scheduled_post", "scheduled_message"}
        )

    @staticmethod
    def _asset_refs(intent: SocialActionIntent) -> tuple[str, ...]:
        if intent.content is None:
            return ()
        values = [attachment.asset_ref for attachment in intent.content.media]
        values.extend(
            entity.custom_emoji_asset_ref
            for entity in intent.content.entities
            if entity.custom_emoji_asset_ref is not None
        )
        return tuple(dict.fromkeys(values))

    def _validate_verified_asset(
        self,
        asset: Any,
        *,
        owner_binding: str,
        requested_expires_at: int,
        role: MediaRole,
    ) -> VerifiedAsset:
        if not isinstance(asset, VerifiedAsset):
            raise SocialWorkspaceRuntimeError("asset ingestor returned an invalid result")
        if (
            not isinstance(asset.owner_binding, str)
            or not re.fullmatch(r"[a-f0-9]{64}", asset.owner_binding)
            or not hmac.compare_digest(asset.owner_binding, owner_binding)
        ):
            raise SocialWorkspaceRuntimeError("verified asset owner binding mismatch")
        if not _INGESTED_REF_RE.fullmatch(asset.storage_ref):
            raise SocialWorkspaceRuntimeError("verified asset storage reference is invalid")
        if not _CONTENT_DIGEST_RE.fullmatch(asset.content_digest):
            raise SocialWorkspaceRuntimeError("verified asset digest is invalid")
        if not _MIME_TYPE_RE.fullmatch(asset.mime_type):
            raise SocialWorkspaceRuntimeError("verified asset MIME type is invalid")
        asset_role = getattr(asset, "role", None)
        normalized_role = (
            asset_role.value if isinstance(asset_role, MediaRole) else asset_role
        )
        if normalized_role is None and role is MediaRole.IMAGE:
            # Compatibility for image manifests created before roles were
            # explicit. Documents never receive this downgrade.
            normalized_role = MediaRole.IMAGE.value
        if normalized_role != role.value:
            raise SocialWorkspaceRuntimeError("verified asset role binding mismatch")
        expected_family = {
            MediaRole.IMAGE: "image/",
            MediaRole.VIDEO: "video/",
            MediaRole.AUDIO: "audio/",
        }.get(role)
        if expected_family is not None and not asset.mime_type.startswith(expected_family):
            raise SocialWorkspaceRuntimeError("verified asset MIME type does not match role")
        byte_limit = (
            self.document_max_bytes
            if role is MediaRole.DOCUMENT
            else self.asset_max_bytes
        )
        if type(asset.byte_length) is not int or not 1 <= asset.byte_length <= byte_limit:
            raise SocialWorkspaceRuntimeError("verified asset size is outside the media budget")
        now = self._now()
        if (
            type(asset.expires_at) is not int
            or asset.expires_at <= now
            or asset.expires_at > requested_expires_at
        ):
            raise SocialWorkspaceRuntimeError("verified asset expiry is invalid")
        dimensions = (asset.width, asset.height)
        if (asset.width is None) != (asset.height is None) or any(
            value is not None and type(value) is not int
            for value in dimensions
        ):
            raise SocialWorkspaceRuntimeError("verified asset dimensions are invalid")
        if asset.width is not None and (
            not 1 <= asset.width <= self.asset_max_width
            or not 1 <= asset.height <= self.asset_max_height
            or asset.width * asset.height > self.asset_max_pixels
        ):
            raise SocialWorkspaceRuntimeError("verified asset dimensions exceed the media budget")
        if role in {MediaRole.IMAGE, MediaRole.VIDEO, MediaRole.ANIMATION} and asset.width is None:
            raise SocialWorkspaceRuntimeError("verified visual asset dimensions are required")
        if role is MediaRole.DOCUMENT:
            if asset.width is not None or asset.height is not None:
                raise SocialWorkspaceRuntimeError(
                    "verified document dimensions must be absent"
                )
            display_name = getattr(asset, "display_name", None)
            classification = getattr(asset, "classification", None)
            if (
                not isinstance(display_name, str)
                or not 1 <= len(display_name) <= 255
                or any(character in display_name for character in ("/", "\\", "\x00"))
                or not isinstance(classification, str)
                or not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", classification)
            ):
                raise SocialWorkspaceRuntimeError(
                    "verified document metadata is invalid"
                )
        return asset

    def _mint_verified_asset_ref(
        self,
        provider_ref: str,
        platform: str,
        principal: RuntimePrincipal,
        asset: VerifiedAsset,
    ) -> str:
        if not isinstance(provider_ref, str) or not provider_ref:
            raise SocialWorkspaceRuntimeError("provider returned an invalid asset reference")
        public_ref = f"ast_{secrets.token_urlsafe(24)}"
        value: dict[str, Any] = {
            "asset_ref": public_ref,
            "storage_ref": asset.storage_ref,
            "role": (
                asset.role.value
                if isinstance(getattr(asset, "role", None), MediaRole)
                else getattr(asset, "role", MediaRole.IMAGE.value)
            ),
            "content_digest": asset.content_digest,
            "mime_type": asset.mime_type,
            "byte_length": asset.byte_length,
            "expires_at": asset.expires_at,
        }
        if asset.width is not None:
            value["width"] = asset.width
            value["height"] = asset.height
        if getattr(asset, "display_name", None) is not None:
            value["display_name"] = asset.display_name
        if getattr(asset, "classification", None) is not None:
            value["classification"] = asset.classification
        now = self._now()
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """INSERT INTO social_workspace_ref(
                       ref_hash,ref_kind,client_hash,subject_hash,resource_hash,platform,
                       policy_version,provider_ref_ciphertext,expires_at,created_at
                       ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (
                        self._hash(public_ref),
                        "asset",
                        client,
                        subject,
                        resource,
                        platform,
                        self.policy_version,
                        self._encrypt(provider_ref),
                        asset.expires_at,
                        now,
                    ),
                )
                conn.execute(
                    """INSERT INTO social_workspace_ref_preview(
                       ref_hash,preview_json,created_at) VALUES(?,?,?)""",
                    (
                        self._hash(public_ref),
                        self._encrypt(_json(value)),
                        now,
                    ),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return public_ref

    def _asset_metadata_on_conn(
        self,
        conn: sqlite3.Connection,
        asset_ref: str,
        platform: str,
        binding: tuple[str, str, str],
        *,
        allow_expired: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        client, subject, resource = binding
        row = conn.execute(
            """SELECT r.expires_at,p.preview_json FROM social_workspace_ref AS r
               JOIN social_workspace_ref_preview AS p ON p.ref_hash=r.ref_hash
               WHERE r.ref_hash=? AND r.ref_kind='asset' AND r.client_hash=?
               AND r.subject_hash=? AND r.resource_hash=? AND r.platform=?
               AND r.policy_version=?""",
            (
                self._hash(asset_ref),
                client,
                subject,
                resource,
                platform,
                self.policy_version,
            ),
        ).fetchone()
        if row is None:
            raise SocialWorkspaceRuntimeError(
                "asset reference is unknown or not bound",
                error_code="ASSET_UNKNOWN",
            )
        expired = int(row["expires_at"]) <= self._now()
        if expired and not allow_expired:
            raise SocialWorkspaceRuntimeError(
                "asset reference is expired or not bound",
                error_code="ASSET_EXPIRED",
            )
        try:
            metadata = json.loads(self._decrypt(str(row["preview_json"])))
        except Exception:  # noqa: BLE001 - encrypted local state is untrusted
            raise SocialWorkspaceRuntimeError("verified asset metadata is invalid") from None
        allowed = {
            "asset_ref",
            "storage_ref",
            "role",
            "content_digest",
            "mime_type",
            "byte_length",
            "expires_at",
            "width",
            "height",
            "display_name",
            "classification",
        }
        if not isinstance(metadata, dict) or set(metadata) - allowed:
            raise SocialWorkspaceRuntimeError("verified asset metadata is invalid")
        if (
            metadata.get("asset_ref") != asset_ref
            or (
                metadata.get("storage_ref") is not None
                and (
                    not isinstance(metadata["storage_ref"], str)
                    or not _INGESTED_REF_RE.fullmatch(metadata["storage_ref"])
                )
            )
            or metadata.get("role", "image") not in {"image", "document"}
            or not isinstance(metadata.get("content_digest"), str)
            or not _CONTENT_DIGEST_RE.fullmatch(metadata["content_digest"])
            or not isinstance(metadata.get("mime_type"), str)
            or not _MIME_TYPE_RE.fullmatch(metadata["mime_type"])
            or type(metadata.get("byte_length")) is not int
            or not 1 <= metadata["byte_length"] <= (
                self.document_max_bytes
                if metadata.get("role", "image") == "document"
                else self.asset_max_bytes
            )
            or type(metadata.get("expires_at")) is not int
            or metadata["expires_at"] != int(row["expires_at"])
        ):
            raise SocialWorkspaceRuntimeError("verified asset metadata is invalid")
        width, height = metadata.get("width"), metadata.get("height")
        if (width is None) != (height is None) or any(
            value is not None and type(value) is not int
            for value in (width, height)
        ):
            raise SocialWorkspaceRuntimeError("verified asset metadata is invalid")
        if width is not None and (
            not 1 <= width <= self.asset_max_width
            or not 1 <= height <= self.asset_max_height
            or width * height > self.asset_max_pixels
        ):
            raise SocialWorkspaceRuntimeError("verified asset metadata is invalid")
        if metadata["mime_type"].startswith(("image/", "video/")) and width is None:
            raise SocialWorkspaceRuntimeError("verified asset metadata is invalid")
        metadata.setdefault("role", "image")
        if metadata["role"] == "document" and (
                width is not None
                or not isinstance(metadata.get("storage_ref"), str)
                or not isinstance(metadata.get("display_name"), str)
                or not 1 <= len(metadata["display_name"]) <= 255
                or any(
                    character in metadata["display_name"]
                    for character in ("/", "\\", "\x00")
                )
                or not isinstance(metadata.get("classification"), str)
                or not re.fullmatch(
                    r"[a-z0-9][a-z0-9_.-]{0,63}", metadata["classification"]
                )
            ):
                raise SocialWorkspaceRuntimeError("verified asset metadata is invalid")
        return metadata, expired

    def _asset_metadata_for_intent(
        self,
        intent: SocialActionIntent,
        principal: RuntimePrincipal,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> list[dict[str, Any]]:
        refs = self._asset_refs(intent)
        if not refs:
            return []
        if conn is None:
            with self.store._lock, self.store._connect() as owned_conn:
                metadata = [
                    self._asset_metadata_on_conn(
                        owned_conn,
                        ref,
                        intent.platform.value,
                        self._binding(principal),
                    )[0]
                    for ref in refs
                ]
        else:
            metadata = [
                self._asset_metadata_on_conn(
                    conn,
                    ref,
                    intent.platform.value,
                    self._binding(principal),
                )[0]
                for ref in refs
            ]
        by_ref = {str(item["asset_ref"]): item for item in metadata}
        if intent.content is not None:
            for attachment in intent.content.media:
                if by_ref[attachment.asset_ref]["role"] != attachment.role.value:
                    raise SocialWorkspaceRuntimeError(
                        "verified asset role binding mismatch"
                    )
        return metadata

    @staticmethod
    def _digest_asset_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
        """Return only externally safe immutable fields bound into a digest."""

        allowed = (
            "asset_ref",
            "content_digest",
            "mime_type",
            "byte_length",
            "expires_at",
            "width",
            "height",
            "display_name",
            "classification",
        )
        result = {key: metadata[key] for key in allowed if key in metadata}
        if metadata.get("role") == MediaRole.DOCUMENT.value:
            result["role"] = MediaRole.DOCUMENT.value
        return result

    def _digest_assets(
        self, metadata: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return [self._digest_asset_metadata(item) for item in metadata]

    def _store_target_previews_from_output(self, value: Any) -> None:
        """Persist every closed-schema target preview minted by a read.

        Search/list results can be used as mutation destinations without a
        second resolve call.  Their human-visible identity therefore has to be
        retained for the later independent approval page, not just the opaque
        reference.
        """

        if isinstance(value, Mapping):
            target_ref = value.get("target_ref")
            target_kind = value.get("kind")
            title = value.get("display_name", value.get("title"))
            if (
                isinstance(target_ref, str)
                and _REF_RE.fullmatch(target_ref)
                and isinstance(target_kind, str)
                and isinstance(title, str)
                and title.strip()
            ):
                self._store_target_preview(target_ref, value)
            item_ref = value.get("item_ref")
            item_kind = value.get("kind")
            if (
                isinstance(item_ref, str)
                and _REF_RE.fullmatch(item_ref)
                and isinstance(item_kind, str)
                and any(isinstance(value.get(key), str) for key in ("text", "caption"))
            ):
                self._store_item_preview(item_ref, value)
            for child in value.values():
                self._store_target_previews_from_output(child)
        elif isinstance(value, list):
            for child in value:
                self._store_target_previews_from_output(child)

    def _audit(
        self, principal: RuntimePrincipal, *, platform: str | None, operation: str,
        outcome: str, reason: str, target_ref: str | None = None,
        action_digest: str | None = None, response_bytes: int = 0, media_items: int = 0,
    ) -> None:
        platform = platform if platform in {"telegram", "vk"} else None
        allowed_operations = {
            *(item.value for item in SocialReadOperation),
            "capabilities", "prepare", "commit", "commit_ingress", "reconcile",
            "scheduled_items_list", "retry",
            "asset_stage", "asset_status", "asset_preview", "social_tool", "invalid",
        }
        operation = operation if operation in allowed_operations else "invalid"
        outcome = outcome if outcome in {
            "succeeded", "succeeded_response_withheld", "outcome_unknown",
            "failed", "denied", "received", "validated", "resolved",
            "started", "reserved", "replayed",
        } else "failed"
        reason = re.sub(r"[^a-z0-9_]", "_", str(reason).lower())[:64] or "unknown"
        if target_ref is not None and not _REF_RE.fullmatch(target_ref):
            target_ref = None
        principal_hash = self._principal_hash(principal)
        with self.store._lock, self.store._connect() as conn:
            conn.execute(
                """INSERT INTO social_workspace_audit(
                   principal_hash,platform,operation,target_ref_hash,action_digest,
                   outcome,reason_code,response_bytes,media_items,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (principal_hash, platform, operation,
                 self._hash(target_ref) if target_ref else None, action_digest,
                 outcome, reason,
                 max(0, response_bytes), max(0, media_items), self._now()),
            )

    def audit_denial(
        self, context: ToolCallContext, *, platform: str | None, operation: str,
        reason: str, target_ref: str | None = None,
    ) -> None:
        self._audit(
            RuntimePrincipal.from_context(context),
            platform=platform if platform in {"telegram", "vk"} else None,
            operation=operation if operation in {
                *(item.value for item in SocialReadOperation),
                "capabilities", "prepare", "commit", "reconcile",
                "scheduled_items_list", "retry",
                "asset_stage", "asset_status", "asset_preview", "social_tool",
            } else "invalid",
            outcome="denied",
            reason=reason,
            target_ref=target_ref if isinstance(target_ref, str) and _REF_RE.fullmatch(target_ref) else None,
        )

    def audit_commit_ingress(
        self,
        context: ToolCallContext,
        *,
        outcome: str,
        reason: str,
    ) -> None:
        """Persist a safe MCP commit boundary before state-dependent scope checks."""

        self._audit(
            RuntimePrincipal.from_context(context),
            platform=None,
            operation="commit_ingress",
            outcome=outcome,
            reason=reason,
        )

    def _stable_target_budget_key(
        self, principal: RuntimePrincipal, platform: str, target_ref: str | None,
        *, conn: sqlite3.Connection | None = None,
    ) -> str:
        if not target_ref:
            return f"{platform}\0-"
        if not _REF_RE.fullmatch(target_ref) or not target_ref.startswith("tgt_"):
            raise SocialWorkspaceRuntimeError("target budget reference is invalid")
        if conn is None:
            native = self._resolve_ref(target_ref, "target", platform, principal)
        else:
            client, subject, resource = self._binding(principal)
            row = conn.execute(
                """SELECT provider_ref_ciphertext FROM social_workspace_ref
                   WHERE ref_hash=? AND ref_kind='target' AND client_hash=?
                   AND subject_hash=? AND resource_hash=? AND platform=?
                   AND policy_version=? AND expires_at>?""",
                (
                    self._hash(target_ref), client, subject, resource, platform,
                    self.policy_version, self._now(),
                ),
            ).fetchone()
            if row is None:
                raise SocialWorkspaceRuntimeError(
                    "opaque reference is expired or not bound"
                )
            native = self._decrypt(row["provider_ref_ciphertext"])
        return f"{platform}\0{self._hash(native)}"

    def _budget_keys(
        self, principal: RuntimePrincipal, platform: str, target_ref: str | None,
        action: str, *, conn: sqlite3.Connection | None = None,
    ) -> tuple[tuple[str, str], ...]:
        principal_key = f"{principal.client_id}\0{principal.subject}\0{principal.resource}"
        return (
            ("global", "global"),
            ("principal", principal_key),
            (
                "target",
                self._stable_target_budget_key(
                    principal, platform, target_ref, conn=conn
                ),
            ),
            ("action", f"{platform}\0{action}"),
        )

    def _action_budget_target_ref(
        self,
        intent: SocialActionIntent,
        conn: sqlite3.Connection,
    ) -> str | None:
        """Return the actual mutation destination for per-target budgets.

        Forwarding is charged to its destination.  Item-only mutations are
        charged to the source target captured in the encrypted human preview.
        Collapsing these families into a shared ``platform/-`` bucket would
        make unrelated destinations deny each other and would not represent a
        real per-target policy.
        """

        direct = intent.destination_target_ref or intent.target_ref
        if direct is not None:
            return direct
        if intent.item_ref is None:
            return None
        row = conn.execute(
            "SELECT preview_json FROM social_workspace_ref_preview WHERE ref_hash=?",
            (self._hash(intent.item_ref),),
        ).fetchone()
        if row is None:
            raise SocialWorkspaceRuntimeError("item target budget binding is unavailable")
        try:
            preview = json.loads(self._decrypt(row["preview_json"]))
            target_ref = preview.get("target_ref")
        except Exception:  # noqa: BLE001 - encrypted preview is untrusted state
            raise SocialWorkspaceRuntimeError(
                "item target budget binding is invalid"
            ) from None
        if not isinstance(target_ref, str) or not _REF_RE.fullmatch(target_ref):
            raise SocialWorkspaceRuntimeError("item target budget binding is invalid")
        return target_ref

    def _consume_budget(
        self, principal: RuntimePrincipal, platform: str, target_ref: str | None,
        action: str, metric: str, amount: int,
    ) -> None:
        if amount <= 0:
            return
        now = self._now()
        period_format = "%Y-%m-%d" if metric == "attempts" else "%Y-%m-%dT%H"
        period = datetime.fromtimestamp(now, timezone.utc).strftime(period_format)
        budget_keys = self._budget_keys(principal, platform, target_ref, action)
        with self.store._lock, self.store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for dimension, raw_key in budget_keys:
                    limit = self._budget_dimension_limits[metric][dimension]
                    key = self._hash(raw_key)
                    row = conn.execute(
                        "SELECT amount FROM social_workspace_budget WHERE period=? AND dimension=? AND bucket_hash=? AND metric=?",
                        (period, dimension, key, metric),
                    ).fetchone()
                    used = int(row["amount"]) if row else 0
                    if used + amount > limit:
                        raise SocialWorkspaceRuntimeError(
                            f"{metric} budget exceeded",
                            error_code=f"{metric.upper()}_BUDGET_EXCEEDED",
                        )
                    conn.execute(
                        """INSERT INTO social_workspace_budget(period,dimension,bucket_hash,metric,amount,updated_at)
                           VALUES(?,?,?,?,?,?) ON CONFLICT(period,dimension,bucket_hash,metric)
                           DO UPDATE SET amount=amount+excluded.amount,updated_at=excluded.updated_at""",
                        (period, dimension, key, metric, amount, now),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _check_circuit(self, principal: RuntimePrincipal, platform: str, target_ref: str | None) -> None:
        if not target_ref:
            return
        client, subject, resource = self._binding(principal)
        target_hash = self._hash(
            self._stable_target_budget_key(principal, platform, target_ref)
        )
        with self.store._lock, self.store._connect() as conn:
            row = conn.execute(
                """SELECT flood_until,circuit_open_until FROM social_workspace_circuit
                   WHERE client_hash=? AND subject_hash=? AND resource_hash=? AND platform=? AND target_ref_hash=?""",
                (client, subject, resource, platform, target_hash),
            ).fetchone()
        now = self._now()
        if row and ((row["flood_until"] or 0) > now or (row["circuit_open_until"] or 0) > now):
            raise SocialWorkspaceRuntimeError("provider flood/circuit gate is open")

    def _record_provider_result(
        self, principal: RuntimePrincipal, platform: str, target_ref: str | None,
        *, success: bool, flood_seconds: int = 0,
    ) -> None:
        if not target_ref:
            return
        client, subject, resource = self._binding(principal)
        now = self._now()
        target_hash = self._hash(
            self._stable_target_budget_key(principal, platform, target_ref)
        )
        with self.store._lock, self.store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT consecutive_failures FROM social_workspace_circuit
                   WHERE client_hash=? AND subject_hash=? AND resource_hash=? AND platform=? AND target_ref_hash=?""",
                (client, subject, resource, platform, target_hash),
            ).fetchone()
            failures = 0 if success else (int(row["consecutive_failures"]) if row else 0) + 1
            circuit_until = now + self.circuit_cooldown_seconds if failures >= self.circuit_failure_threshold else None
            conn.execute(
                """INSERT INTO social_workspace_circuit(client_hash,subject_hash,resource_hash,platform,
                   target_ref_hash,consecutive_failures,flood_until,circuit_open_until,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(client_hash,subject_hash,resource_hash,platform,target_ref_hash)
                   DO UPDATE SET consecutive_failures=excluded.consecutive_failures,
                   flood_until=excluded.flood_until,circuit_open_until=excluded.circuit_open_until,
                   updated_at=excluded.updated_at""",
                (client, subject, resource, platform, target_hash, failures,
                 now + flood_seconds if flood_seconds else None, circuit_until, now),
            )
            conn.execute("COMMIT")

    def _native_read(self, request: SocialReadRequest, principal: RuntimePrincipal) -> SocialReadRequest:
        platform = request.platform.value
        target = self._resolve_ref(request.target_ref, "target", platform, principal) if request.target_ref else None
        item = self._resolve_ref(request.item_ref, "item", platform, principal) if request.item_ref else None
        return replace(request, target_ref=target, item_ref=item)

    def _native_intent(self, intent: SocialActionIntent, principal: RuntimePrincipal) -> SocialActionIntent:
        platform = intent.platform.value
        target = self._resolve_ref(intent.target_ref, "target", platform, principal) if intent.target_ref else None
        item = self._resolve_ref(intent.item_ref, "item", platform, principal) if intent.item_ref else None
        destination = self._resolve_ref(intent.destination_target_ref, "target", platform, principal) if intent.destination_target_ref else None
        media = intent.content
        if media is not None:
            media = replace(
                media,
                entities=tuple(
                    replace(
                        entity,
                        custom_emoji_asset_ref=self._resolve_ref(
                            entity.custom_emoji_asset_ref,
                            "asset",
                            platform,
                            principal,
                        ),
                    )
                    if entity.custom_emoji_asset_ref is not None
                    else entity
                    for entity in media.entities
                ),
                media=tuple(
                    replace(
                        attachment,
                        asset_ref=self._resolve_ref(
                            attachment.asset_ref, "asset", platform, principal
                        ),
                    )
                    for attachment in media.media
                ),
            )
        return replace(intent, target_ref=target, item_ref=item,
                       destination_target_ref=destination, content=media)

    def _resolve_ref_for_reconciliation(
        self,
        public_ref: str,
        kind: str,
        platform: str,
        principal: RuntimePrincipal,
    ) -> str:
        """Resolve an expired ref only for an already-bound operation readback.

        Scheduled reconciliation may happen after short-lived staged assets have
        expired.  It never reopens or uploads those bytes; the provider adapter
        needs only the durable encrypted binding to reconstruct the original
        text/media fingerprint.  Principal/resource/policy binding remains exact.
        """

        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:
            row = conn.execute(
                """SELECT provider_ref_ciphertext FROM social_workspace_ref
                   WHERE ref_hash=? AND ref_kind=? AND client_hash=?
                     AND subject_hash=? AND resource_hash=? AND platform=?
                     AND policy_version=?""",
                (
                    self._hash(public_ref),
                    kind,
                    client,
                    subject,
                    resource,
                    platform,
                    self.policy_version,
                ),
            ).fetchone()
        if row is None:
            raise SocialWorkspaceRuntimeError(
                "historical opaque reference is unknown or not bound"
            )
        return self._decrypt(str(row["provider_ref_ciphertext"]))

    def _native_reconciliation_intent(
        self, intent: SocialActionIntent, principal: RuntimePrincipal
    ) -> SocialActionIntent:
        """Recover provider refs without re-authorizing an expired mutation.

        This path is deliberately reachable only from ``reconcile`` after the
        operation/preparation has been matched to the current principal.  It is
        evidence recovery, not a retry or provider mutation authorization.
        """

        platform = intent.platform.value
        resolve = lambda value, kind: (  # noqa: E731 - local closed resolver
            self._resolve_ref_for_reconciliation(
                value, kind, platform, principal
            )
            if value
            else None
        )
        content = intent.content
        if content is not None:
            content = replace(
                content,
                entities=tuple(
                    replace(
                        entity,
                        custom_emoji_asset_ref=resolve(
                            entity.custom_emoji_asset_ref, "asset"
                        ),
                    )
                    if entity.custom_emoji_asset_ref is not None
                    else entity
                    for entity in content.entities
                ),
                media=tuple(
                    replace(
                        attachment,
                        asset_ref=str(resolve(attachment.asset_ref, "asset")),
                    )
                    for attachment in content.media
                ),
            )
        return replace(
            intent,
            target_ref=resolve(intent.target_ref, "target"),
            item_ref=resolve(intent.item_ref, "item"),
            destination_target_ref=resolve(
                intent.destination_target_ref, "target"
            ),
            content=content,
        )

    def _sanitize_provider_output(
        self, value: Any, platform: str, principal: RuntimePrincipal,
        *, known_refs: Mapping[tuple[str, str], str] | None = None,
    ) -> Any:
        known = dict(known_refs or {})

        def walk(node: Any, key: str | None = None) -> Any:
            if isinstance(node, Mapping):
                clean: dict[str, Any] = {}
                for raw_key, child in node.items():
                    name = str(raw_key)
                    if _SECRET_KEY.search(name):
                        continue
                    clean[name] = walk(child, name)
                return clean
            if isinstance(node, (list, tuple)):
                return [walk(child, key) for child in node]
            if key in {"target_ref", "actor_target_ref", "destination_target_ref"} and node is not None:
                raw = str(node)
                if ("target", raw) not in known:
                    known[("target", raw)] = self._mint_ref("target", raw, platform, principal)
                return known[("target", raw)]
            if key in {
                "item_ref",
                "item_refs",
                "observed_item_ref",
                "root_item_ref",
            } and node is not None:
                raw = str(node)
                if ("item", raw) not in known:
                    known[("item", raw)] = self._mint_ref("item", raw, platform, principal)
                return known[("item", raw)]
            # Provider read contracts expose media as an array of bare opaque
            # asset strings, while action contracts expose ``asset_ref`` in an
            # attachment object.  Both shapes must cross the same outer
            # principal/provider binding boundary.  Returning the adapter's
            # inner ``ast_*`` from a read makes the immediately following
            # ``social_asset_preview`` lookup fail because that token has no
            # row in ``social_workspace_ref``.
            if key in {"asset_ref", "media"} and node is not None:
                raw = str(node)
                if ("asset", raw) not in known:
                    known[("asset", raw)] = self._mint_ref(
                        "asset", raw, platform, principal
                    )
                return known[("asset", raw)]
            if isinstance(node, str):
                maximum = 60_000 if key == "text" else 8192
                return _SECRET_VALUE.sub("[REDACTED]", node)[:maximum]
            if node is None or isinstance(node, (bool, int, float)):
                return node
            return str(node)[:1024]

        redacted = walk(value)
        encoded = _json(redacted).encode("utf-8")
        if len(encoded) > self.response_cap_bytes:
            raise SocialWorkspaceRuntimeError("response cap exceeded")
        return redacted

    def _project_contract_value(
        self,
        value: Any,
        schema: Mapping[str, Any],
        *,
        root: Mapping[str, Any] | None = None,
        field: str = "response",
    ) -> Any:
        """Project provider data onto one closed output schema and validate it.

        Provider adapters are untrusted boundaries.  Merely removing known
        secret-looking keys is insufficient because a new native identifier can
        appear under an innocuous key.  Projection makes the output allowlist
        authoritative and drops everything the public contract does not name.
        """

        root = root or schema
        reference = schema.get("$ref")
        if isinstance(reference, str):
            prefix = "#/$defs/"
            if not reference.startswith(prefix):
                raise SocialWorkspaceRuntimeError("provider response schema is invalid")
            definition = root.get("$defs", {}).get(reference[len(prefix):])
            if not isinstance(definition, Mapping):
                raise SocialWorkspaceRuntimeError("provider response schema is invalid")
            return self._project_contract_value(
                value, definition, root=root, field=field
            )

        expected_type = schema.get("type")
        if isinstance(expected_type, list):
            for candidate in expected_type:
                try:
                    return self._project_contract_value(
                        value,
                        {**schema, "type": candidate},
                        root=root,
                        field=field,
                    )
                except SocialWorkspaceRuntimeError:
                    continue
            raise SocialWorkspaceRuntimeError("provider response is invalid")
        if expected_type == "object":
            if not isinstance(value, Mapping):
                raise SocialWorkspaceRuntimeError("provider response is invalid")
            properties = schema.get("properties", {})
            if not isinstance(properties, Mapping):
                raise SocialWorkspaceRuntimeError("provider response schema is invalid")
            required = schema.get("required", [])
            if not isinstance(required, list) or any(name not in value for name in required):
                raise SocialWorkspaceRuntimeError("provider response is incomplete")
            projected = {
                name: self._project_contract_value(
                    value[name], child, root=root, field=f"{field}.{name}"
                )
                for name, child in properties.items()
                if name in value and isinstance(child, Mapping)
            }
            minimum = schema.get("minProperties")
            if type(minimum) is int and len(projected) < minimum:
                raise SocialWorkspaceRuntimeError("provider response is incomplete")
            one_of = schema.get("oneOf")
            if isinstance(one_of, list):
                matches = 0
                for option in one_of:
                    if not isinstance(option, Mapping):
                        continue
                    option_required = option.get("required", [])
                    if isinstance(option_required, list) and all(
                        name in projected for name in option_required
                    ):
                        matches += 1
                if matches != 1:
                    raise SocialWorkspaceRuntimeError("provider response binding is invalid")
            return projected

        if expected_type == "array":
            if not isinstance(value, list):
                raise SocialWorkspaceRuntimeError("provider response is invalid")
            maximum = schema.get("maxItems")
            minimum = schema.get("minItems")
            if type(maximum) is int and len(value) > maximum:
                raise SocialWorkspaceRuntimeError("provider response page is too large")
            if type(minimum) is int and len(value) < minimum:
                raise SocialWorkspaceRuntimeError("provider response is incomplete")
            child = schema.get("items", {})
            if not isinstance(child, Mapping):
                raise SocialWorkspaceRuntimeError("provider response schema is invalid")
            projected = [
                self._project_contract_value(item, child, root=root, field=field)
                for item in value
            ]
            if schema.get("uniqueItems") and len({_json(item) for item in projected}) != len(projected):
                raise SocialWorkspaceRuntimeError("provider response contains duplicates")
            return projected

        if expected_type == "string":
            if not isinstance(value, str):
                raise SocialWorkspaceRuntimeError("provider response is invalid")
            minimum = schema.get("minLength")
            maximum = schema.get("maxLength")
            if type(minimum) is int and len(value) < minimum:
                raise SocialWorkspaceRuntimeError("provider response is incomplete")
            if type(maximum) is int and len(value) > maximum:
                raise SocialWorkspaceRuntimeError("provider response field is too large")
            pattern = schema.get("pattern")
            if isinstance(pattern, str) and re.fullmatch(pattern, value) is None:
                raise SocialWorkspaceRuntimeError("provider response reference is invalid")
            if schema.get("format") == "date-time":
                try:
                    datetime.fromisoformat(
                        value[:-1] + "+00:00" if value.endswith("Z") else value
                    )
                except ValueError as exc:
                    raise SocialWorkspaceRuntimeError(
                        "provider response timestamp is invalid"
                    ) from exc
        elif expected_type == "integer":
            if type(value) is not int:
                raise SocialWorkspaceRuntimeError("provider response is invalid")
            if type(schema.get("minimum")) is int and value < schema["minimum"]:
                raise SocialWorkspaceRuntimeError("provider response is invalid")
            if type(schema.get("maximum")) is int and value > schema["maximum"]:
                raise SocialWorkspaceRuntimeError("provider response is invalid")
        elif expected_type == "number":
            if type(value) not in {int, float} or value < schema.get("minimum", value):
                raise SocialWorkspaceRuntimeError("provider response is invalid")
        elif expected_type == "boolean":
            if type(value) is not bool:
                raise SocialWorkspaceRuntimeError("provider response is invalid")
        elif expected_type == "null":
            if value is not None:
                raise SocialWorkspaceRuntimeError("provider response is invalid")
        elif expected_type is not None:
            raise SocialWorkspaceRuntimeError("provider response schema is invalid")

        if "const" in schema and value != schema["const"]:
            raise SocialWorkspaceRuntimeError("provider response discriminator is invalid")
        choices = schema.get("enum")
        if isinstance(choices, list) and value not in choices:
            raise SocialWorkspaceRuntimeError("provider response discriminator is invalid")
        return value

    def _project_read_output(
        self, request: SocialReadRequest, safe: Mapping[str, Any]
    ) -> dict[str, Any]:
        schemas: dict[SocialReadOperation, Mapping[str, Any]] = {
            SocialReadOperation.RESOLVE_ITEM: SOCIAL_WORKSPACE_ITEM_RESOLVE_OUTPUT_SCHEMA,
            SocialReadOperation.SEARCH_TARGETS: SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA,
            SocialReadOperation.LIST_DIALOGS: SOCIAL_WORKSPACE_DIALOG_LIST_OUTPUT_SCHEMA,
            SocialReadOperation.SEARCH_ITEMS: SOCIAL_WORKSPACE_ITEM_SEARCH_OUTPUT_SCHEMA,
            SocialReadOperation.LIST_ITEMS: SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA,
            SocialReadOperation.GET_ITEM: SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA,
            SocialReadOperation.LIST_COMMENTS: SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
            SocialReadOperation.LIST_REACTIONS: SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA,
            SocialReadOperation.LIST_STORIES: SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA,
            SocialReadOperation.GET_STATISTICS: SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA,
            SocialReadOperation.GET_AUDIENCE: SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA,
            SocialReadOperation.LIST_NOTIFICATIONS: SOCIAL_WORKSPACE_NOTIFICATIONS_OUTPUT_SCHEMA,
        }
        schema = schemas.get(request.operation)
        if schema is None:
            raise SocialWorkspaceRuntimeError("provider read operation is unsupported")
        projected = self._project_contract_value(safe, schema)
        if not isinstance(projected, dict):
            raise SocialWorkspaceRuntimeError("provider response must be an object")
        if (
            request.operation is SocialReadOperation.RESOLVE_ITEM
            and request.expected_target_kinds
        ):
            source_target = projected.get("source_target")
            resolved_kind = (
                source_target.get("kind")
                if isinstance(source_target, Mapping)
                else None
            )
            expected_kind = request.expected_target_kinds[0]
            if resolved_kind != expected_kind.value:
                raise SocialWorkspaceRuntimeError(
                    "resolved item source target kind mismatch"
                )
        encoded = _json(projected).encode("utf-8")
        if len(encoded) > self.response_cap_bytes:
            raise SocialWorkspaceRuntimeError("response cap exceeded")
        return projected

    async def _enrich_telegram_audio(
        self,
        raw: Mapping[str, Any],
        *,
        principal: RuntimePrincipal,
        wait_seconds: int,
    ) -> tuple[Mapping[str, Any], dict[str, Any] | None]:
        service = self.audio_transcription_service
        if service is None:
            return raw, None
        adapter = self._adapter(SocialPlatform.TELEGRAM.value)
        if not callable(getattr(adapter, "read_asset", None)):
            return raw, None
        result = copy.deepcopy(dict(raw))
        owner_binding = self._principal_hash(principal)
        configured_limit = getattr(getattr(service, "config", None), "max_asset_bytes", None)
        adapter_limit = getattr(adapter, "max_read_asset_bytes", None)
        valid_limits = [
            int(value)
            for value in (configured_limit, adapter_limit)
            if type(value) is int and 0 < value <= 2 * 1024 * 1024 * 1024
        ]
        max_bytes = min(valid_limits) if valid_limits else 64 * 1024 * 1024

        def base_transcription(
            *,
            status: str,
            created: bool,
            cache_hit: bool,
            next_poll_after_seconds: int,
            job_ref: str | None = None,
            error_code: str | None = None,
        ) -> dict[str, Any]:
            payload: dict[str, Any] = {
                "status": status,
                "cache_hit": cache_hit,
                "created": created,
                "text_included": False,
                "truncated": False,
                "next_offset": None,
                "next_poll_after_seconds": next_poll_after_seconds,
                "trust": "untrusted_external_data",
            }
            if job_ref is not None:
                payload["transcription_ref"] = job_ref
            if error_code is not None:
                payload["error_code"] = error_code
            return payload

        groups: dict[str, dict[str, Any]] = {}
        voice_total = 0
        materialization_failures = 0
        normal_poll_after = max(
            1,
            min(
                86_400,
                int(getattr(getattr(service, "config", None), "poll_interval_seconds", 20)),
            ),
        )

        def collect(node: Any) -> None:
            nonlocal voice_total
            if isinstance(node, dict):
                attachments = node.get("attachments")
                if isinstance(attachments, list):
                    for attachment in attachments[:10]:
                        if (
                            not isinstance(attachment, dict)
                            or attachment.get("kind") not in {"voice", "audio"}
                        ):
                            continue
                        voice_total += 1
                        if voice_total > MAX_TRANSCRIPTION_ATTACHMENTS_PER_READ:
                            raise SocialWorkspaceRuntimeError(
                                "provider voice batch exceeds response cap"
                            )
                        asset_ref = attachment.get("asset_ref")
                        fingerprint = attachment.pop("binding_fingerprint", None)
                        if (
                            not isinstance(asset_ref, str)
                            or not isinstance(fingerprint, str)
                            or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
                        ):
                            attachment["transcription"] = base_transcription(
                                status="failed",
                                created=False,
                                cache_hit=False,
                                next_poll_after_seconds=normal_poll_after,
                                error_code="TRANSCRIPTION_BINDING_INVALID",
                            )
                            continue
                        identity = hmac.new(
                            self._key,
                            (
                                "telegram-read-transcription\0"
                                + owner_binding
                                + "\0"
                                + fingerprint
                            ).encode("ascii"),
                            hashlib.sha256,
                        ).hexdigest()
                        group = groups.setdefault(
                            "tg:" + identity,
                            {
                                "fingerprint": fingerprint,
                                "asset_ref": asset_ref,
                                "mime_type": (
                                    str(attachment["mime_type"])
                                    if isinstance(attachment.get("mime_type"), str)
                                    else None
                                ),
                                "attachments": [],
                            },
                        )
                        group["attachments"].append(attachment)
                for child in list(node.values()):
                    collect(child)
            elif isinstance(node, list):
                for child in node:
                    collect(child)

        collect(result)

        registration_limit = asyncio.Semaphore(
            TRANSCRIPTION_REGISTRATION_CONCURRENCY
        )

        def mark_registration_failed(group: dict[str, Any]) -> None:
            nonlocal materialization_failures
            if group.get("registration_failed"):
                return
            materialization_failures += len(group["attachments"])
            group["registration_failed"] = True
            for attachment in group["attachments"]:
                attachment["transcription"] = base_transcription(
                    status="failed",
                    created=False,
                    cache_hit=False,
                    next_poll_after_seconds=normal_poll_after,
                    error_code="TRANSCRIPTION_MATERIALIZATION_FAILED",
                )

        async def register(idempotency_key: str, group: dict[str, Any]) -> None:
            try:
                async with registration_limit:
                    started = await asyncio.wait_for(
                        service.start_provider_transcription(
                            owner_binding=owner_binding,
                            idempotency_key=idempotency_key,
                            provider_fingerprint=group["fingerprint"],
                            content_loader=lambda ref=group["asset_ref"]: adapter.read_asset(
                                ref,
                                owner_binding=owner_binding,
                                max_bytes=max_bytes,
                            ),
                            mime_type=group["mime_type"],
                        ),
                        timeout=self.provider_timeout_seconds,
                    )
                job_ref = str(started.get("job_ref") or "")
                if re.fullmatch(r"atr_[A-Za-z0-9_-]{24,160}", job_ref) is None:
                    raise ValueError("transcription service returned an invalid job")
                group["job_ref"] = job_ref
                group["created"] = bool(started.get("created", False))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one media must not fail the social read
                mark_registration_failed(group)

        registration_tasks = [
            asyncio.create_task(register(key, group))
            for key, group in groups.items()
        ]
        if registration_tasks:
            try:
                await asyncio.gather(*registration_tasks)
            except asyncio.CancelledError:
                for task in registration_tasks:
                    task.cancel()
                await asyncio.gather(*registration_tasks, return_exceptions=True)
                raise

        registered = [
            group for group in groups.values() if isinstance(group.get("job_ref"), str)
        ]
        job_refs = [str(group["job_ref"]) for group in registered]
        snapshot = None
        jobs_by_ref: dict[str, Any] = {}
        wait_expired = False
        next_poll_after = normal_poll_after if registered else 0
        wait_actual_seconds = 0.0
        if job_refs:
            snapshot = await service.wait_many(
                owner_binding=owner_binding,
                job_refs=job_refs,
                wait_seconds=wait_seconds,
            )
            jobs_by_ref = {job.job_ref: job for job in snapshot.jobs}
            wait_expired = bool(snapshot.wait_expired)
            next_poll_after = int(snapshot.next_poll_after_seconds)
            wait_actual_seconds = float(snapshot.wait_actual_seconds)

        ready_groups: list[dict[str, Any]] = []
        for group in registered:
            job_ref = str(group["job_ref"])
            job = jobs_by_ref.get(job_ref)
            created = bool(group.get("created", False))
            if job is None:
                status = "failed"
                error_code = "TRANSCRIPTION_STATE_UNAVAILABLE"
                per_item_poll = normal_poll_after
            elif job.state.value == "complete":
                status = "ready"
                error_code = None
                per_item_poll = 0
                ready_groups.append(group)
            elif job.state.value in {"failed", "cancelled"}:
                status = "failed"
                code = job.error_code
                error_code = (
                    str(code)
                    if isinstance(code, str) and re.fullmatch(r"[A-Z0-9_]{3,64}", code)
                    else "TRANSCRIPTION_FAILED"
                )
                per_item_poll = 0
            elif job.state.value in {"queued", "dispatching"}:
                status = "queued"
                error_code = None
                per_item_poll = next_poll_after
            else:
                status = "running"
                error_code = None
                per_item_poll = next_poll_after
            for attachment in group["attachments"]:
                attachment["transcription"] = base_transcription(
                    status=status,
                    created=created,
                    cache_hit=not created,
                    next_poll_after_seconds=per_item_poll,
                    job_ref=job_ref,
                    error_code=error_code,
                )

        # Reserve space for outer opaque refs and projection before selecting a
        # single reproducible plain-text prefix per ready job.
        ready_attachments = sum(len(group["attachments"]) for group in ready_groups)
        base_size = len(_json(result).encode("utf-8"))
        inline_budget = max(0, self.response_cap_bytes - base_size - 16 * 1024)
        inline_limit = (
            min(60_000, inline_budget // max(1, ready_attachments * 4))
            if ready_attachments
            else 0
        )
        completed: dict[str, Mapping[str, Any]] = {}
        if inline_limit > 0 and ready_groups:
            completed = await service.get_many(
                owner_binding=owner_binding,
                job_refs=[str(group["job_ref"]) for group in ready_groups],
                limit=max(1, inline_limit),
            )
        for group in ready_groups:
            job_ref = str(group["job_ref"])
            page = completed.get(job_ref)
            for attachment in group["attachments"]:
                transcription = attachment["transcription"]
                if page is None or not page.get("ready"):
                    if inline_limit <= 0:
                        transcription["truncated"] = True
                        transcription["next_offset"] = 0
                    else:
                        transcription["status"] = "failed"
                        transcription["error_code"] = "TRANSCRIPTION_RESULT_UNAVAILABLE"
                    continue
                text = page.get("text")
                if not isinstance(text, str):
                    transcription["status"] = "failed"
                    transcription["error_code"] = "TRANSCRIPTION_RESULT_UNAVAILABLE"
                    continue
                transcription["text"] = text
                transcription["text_included"] = True
                next_offset = page.get("next_offset")
                transcription["truncated"] = next_offset is not None
                transcription["next_offset"] = (
                    int(next_offset) if type(next_offset) is int else None
                )

        status_counts = {"ready": 0, "queued": 0, "running": 0, "failed": 0}
        for group in groups.values():
            for attachment in group["attachments"]:
                transcription = attachment.get("transcription")
                status = (
                    transcription.get("status")
                    if isinstance(transcription, Mapping)
                    else None
                )
                if status in status_counts:
                    status_counts[str(status)] += 1
        status_counts["failed"] += voice_total - sum(status_counts.values())
        summary = {
            "total": voice_total,
            **status_counts,
            "cache_hits": sum(
                1 for group in registered if not bool(group.get("created", False))
            ),
            "created": sum(
                1 for group in registered if bool(group.get("created", False))
            ),
            "wait_expired": wait_expired,
            "next_poll_after_seconds": (
                next_poll_after
                if status_counts["queued"] or status_counts["running"]
                else normal_poll_after
                if materialization_failures
                else 0
            ),
        }
        result["transcription_summary"] = summary
        return result, {
            "voice_total": voice_total,
            "jobs_created": summary["created"],
            "cache_hits": summary["cache_hits"],
            "ready": summary["ready"],
            "queued": summary["queued"],
            "running": summary["running"],
            "failed": summary["failed"],
            "wait_requested_ms": wait_seconds * 1000,
            "wait_actual_ms": round(wait_actual_seconds * 1000),
            "wait_expired": wait_expired,
        }

    @staticmethod
    def _safe_provider_error() -> SocialWorkspaceRuntimeError:
        return SocialWorkspaceRuntimeError("social provider operation failed")

    async def capabilities(
        self, target_ref: str | None, context: ToolCallContext, *, platform: str
    ) -> dict[str, Any]:
        principal = RuntimePrincipal.from_context(context)
        native = self._resolve_ref(target_ref, "target", platform, principal) if target_ref else None
        try:
            self._consume_budget(principal, platform, target_ref, "capabilities", "rate", 1)
            try:
                raw = await asyncio.wait_for(
                    self._adapter(platform).capabilities(native),
                    self.provider_timeout_seconds,
                )
            except asyncio.TimeoutError:
                raise SocialWorkspaceRuntimeError(
                    "social provider operation timed out"
                ) from None
            except Exception:  # noqa: BLE001 - provider exception text is untrusted
                raise self._safe_provider_error() from None
            safe = self._sanitize_provider_output(raw, platform, principal,
                known_refs={("target", native): target_ref} if native and target_ref else None)
            if not isinstance(safe, dict):
                raise SocialWorkspaceRuntimeError("provider response must be an object")
            safe["platform"] = platform
            if target_ref:
                safe["target_ref"] = target_ref
            features = safe.get("content_features")
            if isinstance(features, list):
                media_features = {"image", "video", "document", "audio", "animation"}
                allowed_media: set[str] = set()
                actions = safe.get("actions")
                if self.asset_ingestor is not None and self.media_story_enabled:
                    allowed_media.add("image")
                if (
                    self.asset_ingestor is not None
                    and self.file_send_enabled
                    and platform == SocialPlatform.TELEGRAM.value
                    and isinstance(actions, list)
                    and SocialAction.SEND_MESSAGE.value in actions
                ):
                    allowed_media.add("document")
                safe["content_features"] = [
                    value
                    for value in features
                    if value not in media_features or value in allowed_media
                ]
            validated = validate_capabilities(safe)
            result = asdict(validated)
            result = {key: (sorted(str(v) for v in value) if isinstance(value, (set, frozenset)) else str(value) if hasattr(value, "value") else value) for key, value in result.items()}
            result = {key: value for key, value in result.items() if value is not None}
            size = len(_json(result).encode())
            if size > self.response_cap_bytes:
                raise SocialWorkspaceRuntimeError("response cap exceeded")
            self._consume_budget(principal, platform, target_ref, "capabilities", "egress", size)
            self._audit(principal, platform=platform, operation="capabilities", outcome="succeeded", reason="ok", target_ref=target_ref, response_bytes=size)
            return result
        except Exception as exc:
            self._audit(principal, platform=platform, operation="capabilities", outcome="denied", reason=type(exc).__name__, target_ref=target_ref)
            raise

    async def resolve(self, request: SocialReadRequest, context: ToolCallContext) -> dict[str, Any]:
        if request.operation is not SocialReadOperation.RESOLVE_TARGET:
            raise SocialWorkspaceRuntimeError("resolve requires resolve_target")
        principal = RuntimePrincipal.from_context(context)
        platform = request.platform.value
        try:
            self._consume_budget(principal, platform, None, request.operation.value, "rate", 1)
            try:
                raw = await asyncio.wait_for(
                    self._adapter(platform).resolve(request),
                    self.provider_timeout_seconds,
                )
            except asyncio.TimeoutError:
                raise SocialWorkspaceRuntimeError(
                    "social provider operation timed out"
                ) from None
            except Exception:  # noqa: BLE001 - provider exception text is untrusted
                raise self._safe_provider_error() from None
            safe = self._sanitize_provider_output(raw, platform, principal)
            if not isinstance(safe, dict):
                raise SocialWorkspaceRuntimeError("provider response must be an object")
            safe["platform"] = platform
            safe["trust"] = "untrusted_external_data"
            safe["is_exact_match"] = True
            validate_resolved_target_preview(request, safe)
            self._store_target_preview(str(safe["target_ref"]), safe)
            size = len(_json(safe).encode())
            if size > self.response_cap_bytes:
                raise SocialWorkspaceRuntimeError("response cap exceeded")
            self._consume_budget(principal, platform, safe.get("target_ref"), request.operation.value, "egress", size)
            self._audit(principal, platform=platform, operation=request.operation.value, outcome="succeeded", reason="ok", target_ref=safe.get("target_ref"), response_bytes=size)
            return safe
        except Exception as exc:
            self._audit(principal, platform=platform, operation=request.operation.value, outcome="denied", reason=type(exc).__name__)
            raise

    def _sample_state(self, request: SocialReadRequest, principal: RuntimePrincipal) -> tuple[str, int, str | None]:
        now = self._now()
        client, subject, resource = self._binding(principal)
        binding = _json({
            "target_ref": request.target_ref,
            "expected_target_kinds": sorted(v.value for v in request.expected_target_kinds),
            "purpose": request.purpose.value if request.purpose else None,
            "date_from": request.date_from,
            "date_to": request.date_to, "total_limit": request.total_limit,
            "authorization_basis": (
                request.authorization_basis.value
                if request.authorization_basis
                else None
            ),
        })
        with self.store._lock, self.store._connect() as conn:
            if request.sample_ref is None:
                sample_ref = "smp_" + secrets.token_urlsafe(24)
                conn.execute(
                    """INSERT INTO social_workspace_sample(sample_hash,client_hash,subject_hash,
                       resource_hash,platform,target_ref_hash,binding_json,cumulative_count,total_limit,
                       expires_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (self._hash(sample_ref), client, subject, resource, request.platform.value,
                     self._hash(request.target_ref or ""), binding, 0, request.total_limit,
                     now + self.sample_ttl_seconds, now, now),
                )
                return sample_ref, 0, None
            row = conn.execute(
                """SELECT * FROM social_workspace_sample WHERE sample_hash=? AND client_hash=?
                   AND subject_hash=? AND resource_hash=? AND platform=? AND expires_at>?""",
                (self._hash(request.sample_ref), client, subject, resource,
                 request.platform.value, now),
            ).fetchone()
            if row is None or row["binding_json"] != binding:
                raise SocialWorkspaceRuntimeError("sample continuation binding mismatch")
            if not row["continuation_cursor_hash"] or not hmac.compare_digest(row["continuation_cursor_hash"], self._hash(request.cursor or "")):
                raise SocialWorkspaceRuntimeError("sample cursor is not server-minted")
            if not row["continuation_cursor_ciphertext"]:
                raise SocialWorkspaceRuntimeError("sample provider cursor is unavailable")
            return (request.sample_ref, int(row["cumulative_count"]),
                    self._decrypt(row["continuation_cursor_ciphertext"]))

    async def read(self, request: SocialReadRequest, context: ToolCallContext) -> dict[str, Any]:
        if request.operation is SocialReadOperation.RESOLVE_TARGET:
            return await self.resolve(request, context)
        read_started = asyncio.get_running_loop().time()
        batch_telemetry: dict[str, Any] | None = None
        principal = RuntimePrincipal.from_context(context)
        platform = request.platform.value
        target_ref = request.target_ref
        provider_attempted = False
        provider_call_succeeded = False
        flood_seconds = 0
        try:
            self._check_circuit(principal, platform, target_ref)
            self._consume_budget(principal, platform, target_ref, request.operation.value, "rate", 1)
            sample: tuple[str, int, str | None] | None = None
            if request.operation is SocialReadOperation.EDITORIAL_SAMPLE:
                sample = self._sample_state(request, principal)
                if sample[1] + request.page_size > request.total_limit:
                    raise SocialWorkspaceRuntimeError("editorial sample cumulative limit exceeded")
            native = self._native_read(request, principal)
            if sample is not None:
                native = replace(
                    native, sample_ref=sample[0], cursor=sample[2]
                )
            known: dict[tuple[str, str], str] = {}
            if target_ref and native.target_ref:
                known[("target", native.target_ref)] = target_ref
            if request.item_ref and native.item_ref:
                known[("item", native.item_ref)] = request.item_ref
            provider_attempted = True
            try:
                raw = await asyncio.wait_for(
                    self._adapter(platform).read(native), self.provider_timeout_seconds
                )
            except asyncio.TimeoutError:
                raise SocialWorkspaceRuntimeError(
                    "social provider operation timed out"
                ) from None
            except Exception as exc:  # noqa: BLE001 - provider exception text is untrusted
                flood_seconds = int(getattr(exc, "retry_after", 0) or 0)
                raise self._safe_provider_error() from None
            provider_call_succeeded = True
            if (
                platform == SocialPlatform.TELEGRAM.value
                and request.transcribe_audio
                and request.operation
                in {
                    SocialReadOperation.RESOLVE_ITEM,
                    SocialReadOperation.LIST_ITEMS,
                    SocialReadOperation.GET_ITEM,
                    SocialReadOperation.LIST_COMMENTS,
                }
                and isinstance(raw, Mapping)
            ):
                raw, batch_telemetry = await self._enrich_telegram_audio(
                    raw,
                    principal=principal,
                    wait_seconds=request.transcription_wait_seconds,
                )
            safe = self._sanitize_provider_output(raw, platform, principal, known_refs=known)
            if not isinstance(safe, dict):
                raise SocialWorkspaceRuntimeError("provider response must be an object")
            safe["trust"] = "untrusted_external_data"
            if sample:
                sample_ref, cumulative, _ = sample
                items = safe.get("items", [])
                if not isinstance(items, list) or len(items) > request.page_size:
                    raise SocialWorkspaceRuntimeError("editorial page exceeds requested size")
                count = len(items)
                new_total = cumulative + count
                provider_next_cursor = safe.get("next_cursor")
                next_cursor = provider_next_cursor
                if new_total >= request.total_limit:
                    next_cursor = None
                    safe.pop("next_cursor", None)
                elif next_cursor is not None:
                    next_cursor = secrets.token_urlsafe(24)
                    safe["next_cursor"] = next_cursor
                safe.update({"sample_ref": sample_ref, "sampled_count": count,
                             "cumulative_count": new_total, "total_limit": request.total_limit,
                             "storage_disposition": "ephemeral_no_index"})
                # Contract validation provides an additional exact shape/binding gate.
                from .social_workspace import EditorialSampleState, SocialReadPurpose
                state = EditorialSampleState(sample_ref, target_ref or "",
                    frozenset(request.expected_target_kinds), request.purpose or SocialReadPurpose.EDITORIAL_ANALYSIS,
                    request.date_from, request.date_to, request.total_limit, cumulative,
                    True, request.cursor, request.cursor is not None, True, False)
                validate_editorial_sample_response(request, state, safe)
                with self.store._lock, self.store._connect() as conn:
                    changed = conn.execute(
                        """UPDATE social_workspace_sample SET cumulative_count=?,
                           continuation_cursor_hash=?,continuation_cursor_ciphertext=?,
                           updated_at=? WHERE sample_hash=? AND cumulative_count=?""",
                        (new_total, self._hash(next_cursor) if next_cursor else None,
                         self._encrypt(str(provider_next_cursor)) if next_cursor else None,
                         self._now(), self._hash(sample_ref), cumulative),
                    ).rowcount
                if changed != 1:
                    raise SocialWorkspaceRuntimeError("sample continuation was concurrently consumed")
            else:
                safe = self._project_read_output(request, safe)
            self._store_target_previews_from_output(safe)
            size = len(_json(safe).encode("utf-8"))
            if size > self.response_cap_bytes:
                raise SocialWorkspaceRuntimeError("response cap exceeded")
            media_count = self._count_media(safe)
            self._consume_budget(principal, platform, target_ref, request.operation.value, "egress", size)
            self._consume_budget(principal, platform, target_ref, request.operation.value, "media", media_count)
            self._record_provider_result(principal, platform, target_ref, success=True)
            audit_reason = (
                f"editorial_{request.authorization_basis.value}"
                if request.operation is SocialReadOperation.EDITORIAL_SAMPLE
                and request.authorization_basis is not None
                else "ok"
            )
            self._audit(principal, platform=platform, operation=request.operation.value,
                        outcome="succeeded", reason=audit_reason, target_ref=target_ref,
                        response_bytes=size, media_items=media_count)
            if batch_telemetry is not None:
                logger.info(
                    "social_voice_transcription_batch voice_total=%s jobs_created=%s "
                    "cache_hits=%s ready=%s queued=%s running=%s failed=%s "
                    "wait_requested_ms=%s wait_actual_ms=%s response_duration_ms=%s "
                    "wait_expired=%s",
                    batch_telemetry["voice_total"],
                    batch_telemetry["jobs_created"],
                    batch_telemetry["cache_hits"],
                    batch_telemetry["ready"],
                    batch_telemetry["queued"],
                    batch_telemetry["running"],
                    batch_telemetry["failed"],
                    batch_telemetry["wait_requested_ms"],
                    batch_telemetry["wait_actual_ms"],
                    round((asyncio.get_running_loop().time() - read_started) * 1000),
                    batch_telemetry["wait_expired"],
                )
            return safe
        except Exception as exc:
            if provider_attempted:
                self._record_provider_result(
                    principal,
                    platform,
                    target_ref,
                    success=provider_call_succeeded,
                    flood_seconds=(0 if provider_call_succeeded else flood_seconds),
                )
            self._audit(principal, platform=platform, operation=request.operation.value,
                        outcome="denied", reason=type(exc).__name__, target_ref=target_ref)
            raise

    async def scheduled_items(
        self,
        request: SocialScheduledItemsRequest,
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = RuntimePrincipal.from_context(context)
        platform = request.platform.value
        if not social_scopes_authorized(request.required_scopes, principal.scopes):
            self._audit(
                principal,
                platform=platform,
                operation="scheduled_items_list",
                outcome="denied",
                reason="missing_scope",
                target_ref=request.target_ref,
            )
            raise SocialWorkspaceRuntimeError(
                "required social schedule scope is missing"
            )
        native_target = self._resolve_ref(
            request.target_ref, "target", platform, principal
        )
        adapter = self._adapter(platform)
        list_scheduled = getattr(adapter, "scheduled_items", None)
        if not callable(list_scheduled):
            raise SocialWorkspaceRuntimeError(
                "scheduled queue read is unavailable"
            )
        try:
            raw = await asyncio.wait_for(
                list_scheduled(
                    target_ref=native_target,
                    scheduled_from=request.scheduled_from,
                    scheduled_to=request.scheduled_to,
                    text_sha256=request.text_sha256,
                    media_count=request.media_count,
                    limit=request.limit,
                ),
                self.provider_timeout_seconds,
            )
            safe = self._sanitize_provider_output(
                raw,
                platform,
                principal,
                known_refs={("target", native_target): request.target_ref},
            )
            if not isinstance(safe, dict):
                raise SocialWorkspaceRuntimeError(
                    "scheduled queue response must be an object"
                )
            safe.update(
                {
                    "platform": platform,
                    "target_ref": request.target_ref,
                    "queue": "scheduled",
                    "trust": "untrusted_external_data",
                }
            )
            projected = self._project_contract_value(
                safe, SOCIAL_WORKSPACE_SCHEDULED_ITEMS_OUTPUT_SCHEMA
            )
            if not isinstance(projected, dict):
                raise SocialWorkspaceRuntimeError(
                    "scheduled queue response must be an object"
                )
            self._store_scheduled_item_previews(
                projected,
                platform=platform,
                target_ref=request.target_ref,
            )
            size = len(_json(projected).encode("utf-8"))
            self._consume_budget(
                principal,
                platform,
                request.target_ref,
                SocialAction.SCHEDULE.value,
                "egress",
                size,
            )
            self._audit(
                principal,
                platform=platform,
                operation="scheduled_items_list",
                outcome="succeeded",
                reason="scheduled_queue_read",
                target_ref=request.target_ref,
                response_bytes=size,
                media_items=self._count_media(projected),
            )
            return projected
        except asyncio.TimeoutError:
            self._audit(
                principal,
                platform=platform,
                operation="scheduled_items_list",
                outcome="failed",
                reason="provider_timeout",
                target_ref=request.target_ref,
            )
            raise SocialWorkspaceRuntimeError(
                "social provider operation timed out"
            ) from None
        except Exception as exc:
            self._audit(
                principal,
                platform=platform,
                operation="scheduled_items_list",
                outcome="failed",
                reason=type(exc).__name__,
                target_ref=request.target_ref,
            )
            raise

    @staticmethod
    def _count_media(value: Any) -> int:
        if isinstance(value, Mapping):
            return sum((len(child) if key == "media" and isinstance(child, list) else SocialWorkspaceRuntime._count_media(child)) for key, child in value.items())
        if isinstance(value, list):
            return sum(SocialWorkspaceRuntime._count_media(child) for child in value)
        return 0

    @staticmethod
    def _has_document(intent: SocialActionIntent) -> bool:
        return bool(
            intent.content
            and any(
                attachment.role is MediaRole.DOCUMENT
                for attachment in intent.content.media
            )
        )

    def _enforce_document_runtime_policy(self, intent: SocialActionIntent) -> None:
        validate_document_attachment_policy(intent)
        if not self._has_document(intent):
            return
        if not self.file_send_enabled or self.asset_ingestor is None:
            raise SocialWorkspaceRuntimeError("social document sending is disabled")
        if not callable(getattr(self.asset_ingestor, "reverify", None)):
            raise SocialWorkspaceRuntimeError(
                "document asset reverification is unavailable"
            )

    async def _authorize_document_target(
        self, intent: SocialActionIntent, principal: RuntimePrincipal
    ) -> None:
        if not self._has_document(intent):
            return
        native_target = self._resolve_ref(
            intent.target_ref,
            "target",
            SocialPlatform.TELEGRAM.value,
            principal,
        )
        try:
            raw = await asyncio.wait_for(
                self._adapter(SocialPlatform.TELEGRAM.value).capabilities(
                    native_target
                ),
                self.provider_timeout_seconds,
            )
        except asyncio.TimeoutError:
            raise SocialWorkspaceRuntimeError(
                "social provider capability check timed out"
            ) from None
        except Exception:  # noqa: BLE001 - provider exception text is untrusted
            raise self._safe_provider_error() from None
        if not isinstance(raw, Mapping):
            raise SocialWorkspaceRuntimeError("provider capabilities are invalid")
        actions = raw.get("actions")
        features = raw.get("content_features")
        if (
            not isinstance(actions, list)
            or SocialAction.SEND_MESSAGE.value not in actions
            or not isinstance(features, list)
            or MediaRole.DOCUMENT.value not in features
        ):
            raise SocialWorkspaceRuntimeError(
                "target does not permit Telegram document send_message"
            )

    async def _reverify_document_assets(
        self, intent: SocialActionIntent, principal: RuntimePrincipal,
        metadata: list[dict[str, Any]],
    ) -> None:
        if not metadata:
            return
        self._enforce_document_runtime_policy(intent)
        if self.asset_ingestor is None or not callable(getattr(self.asset_ingestor, "reverify", None)):
            raise SocialWorkspaceRuntimeError("asset reverification is unavailable")
        reverify = self.asset_ingestor.reverify
        owner_binding = self._principal_hash(principal)
        timeout = max(self.provider_timeout_seconds, self.asset_ingest_timeout_seconds)
        for stored in metadata:
            role = MediaRole(str(stored["role"]))
            maximum = self.document_max_bytes if role is MediaRole.DOCUMENT else self.asset_max_bytes
            try:
                refreshed = await asyncio.wait_for(asyncio.to_thread(
                    reverify, stored["storage_ref"], owner_binding=owner_binding,
                    max_bytes=maximum, role=role.value), timeout=timeout)
            except asyncio.TimeoutError:
                raise SocialWorkspaceRuntimeError("asset reverification timed out") from None
            if inspect.isawaitable(refreshed):
                raise SocialWorkspaceRuntimeError("asset reverification must be synchronous")
            verified = self._validate_verified_asset(refreshed, owner_binding=owner_binding,
                requested_expires_at=int(stored["expires_at"]), role=role)
            keys = ["storage_ref", "role", "content_digest", "mime_type",
                    "byte_length", "expires_at", "width", "height"]
            if role is MediaRole.DOCUMENT:
                keys.extend(["display_name", "classification"] )
            fresh = {
                "storage_ref": verified.storage_ref,
                "role": verified.role.value if isinstance(verified.role, MediaRole) else verified.role,
                "content_digest": verified.content_digest, "mime_type": verified.mime_type,
                "byte_length": verified.byte_length, "expires_at": verified.expires_at,
                "width": verified.width, "height": verified.height,
                "display_name": verified.display_name, "classification": verified.classification,
            }
            if {key: fresh.get(key) for key in keys} != {key: stored.get(key) for key in keys}:
                raise SocialWorkspaceRuntimeError("verified asset bytes or metadata changed")


    async def prepare(self, intent: SocialActionIntent, context: ToolCallContext) -> dict[str, Any]:
        principal = RuntimePrincipal.from_context(context)
        if not social_scopes_authorized(intent.required_scopes, principal.scopes):
            self._audit(principal, platform=intent.platform.value, operation="prepare",
                        outcome="denied", reason="missing_scope", target_ref=intent.target_ref)
            raise SocialWorkspaceRuntimeError("required social action scope is missing")
        platform = intent.platform.value
        digest: str | None = None
        # Resolve every reference now, so cross-principal/resource/target mutation
        # fails before an approval can ever be issued.
        try:
            self._native_intent(intent, principal)
            self._enforce_document_runtime_policy(intent)
            await self._authorize_document_target(intent, principal)
            preflight_assets = self._asset_metadata_for_intent(intent, principal)
            await self._reverify_document_assets(
                intent, principal, preflight_assets
            )
        except Exception as exc:
            self._audit(principal, platform=platform, operation="prepare",
                        outcome="denied", reason=type(exc).__name__,
                        target_ref=intent.target_ref, action_digest=digest)
            raise
        now = self._now()
        client, subject, resource = self._binding(principal)
        idem = self._hash(intent.idempotency_key)
        direct_scheduled_item = False
        with self.store._lock, self.store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                verified_assets = self._asset_metadata_for_intent(
                    intent, principal, conn=conn
                )
                direct_scheduled_item = self._scheduled_item_direct_authorized_on_conn(
                    conn, intent, principal
                )
                digest = compute_action_digest(
                    intent, verified_assets=self._digest_assets(verified_assets) or None
                )
                existing = conn.execute(
                    """SELECT * FROM social_workspace_preparation WHERE client_hash=? AND
                       subject_hash=? AND resource_hash=? AND platform=? AND action=? AND idempotency_hash=?""",
                    (client, subject, resource, platform, intent.action.value, idem),
                ).fetchone()
                if existing:
                    if existing["action_digest"] != digest:
                        raise SocialWorkspaceRuntimeError("idempotency key payload mutation denied")
                    if not existing["operation_ref"]:
                        operation = "op_" + secrets.token_urlsafe(24)
                        conn.execute(
                            """UPDATE social_workspace_preparation SET operation_ref=?
                               WHERE preparation_hash=? AND operation_ref IS NULL""",
                            (operation, existing["preparation_hash"]),
                        )
                    logical_action = existing["logical_action_ref"]
                    if not logical_action:
                        logical_action = "act_" + secrets.token_urlsafe(24)
                        conn.execute(
                            """UPDATE social_workspace_preparation
                               SET logical_action_ref=?
                               WHERE preparation_hash=? AND logical_action_ref IS NULL""",
                            (logical_action, existing["preparation_hash"]),
                        )
                    conn.execute("COMMIT")
                    reserved_operation = str(existing["operation_ref"] or operation)
                    return self._preparation_result(existing["preparation_ref"], intent,
                        digest, int(existing["expires_at"]), str(existing["status"]),
                        logical_action_ref=str(logical_action),
                        reserved_operation_ref=reserved_operation)
                prep = "prep_" + secrets.token_urlsafe(24)
                operation = "op_" + secrets.token_urlsafe(24)
                logical_action = "act_" + secrets.token_urlsafe(24)
                expires = now + self.preparation_ttl_seconds
                if verified_assets:
                    expires = min(
                        expires,
                        *(int(asset["expires_at"]) for asset in verified_assets),
                    )
                persisted_intent = {
                    key: value
                    for key, value in asdict(intent).items()
                    if value is not None
                }
                status = (
                    SocialActionStatus.APPROVED.value
                    if intent.action in DIRECT_USER_AUTHORIZED_ACTIONS or direct_scheduled_item
                    else SocialActionStatus.AWAITING_HUMAN_APPROVAL.value
                )
                conn.execute(
                    """INSERT INTO social_workspace_preparation(preparation_hash,preparation_ref,
                       client_hash,subject_hash,resource_hash,platform,action,target_ref_hash,
                       action_digest,idempotency_hash,intent_ciphertext,operation_ref,
                       logical_action_ref,status,expires_at,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (self._hash(prep), prep, client, subject, resource, platform,
                     intent.action.value, self._hash(intent.target_ref) if intent.target_ref else None,
                     digest, idem, self._encrypt(_json(persisted_intent)), operation,
                     logical_action,
                     status, expires, now),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                conn.execute(
                    """INSERT INTO social_workspace_audit(principal_hash,platform,operation,
                       target_ref_hash,action_digest,outcome,reason_code,response_bytes,
                       media_items,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (self._principal_hash(principal), platform, "prepare",
                     self._hash(intent.target_ref) if intent.target_ref else None,
                     digest, "denied", "durable_preparation_denied", 0,
                     len(intent.content.media) if intent.content else 0, self._now()),
                )
                raise
        assert digest is not None
        status = (
            SocialActionStatus.APPROVED.value
            if intent.action in DIRECT_USER_AUTHORIZED_ACTIONS or direct_scheduled_item
            else SocialActionStatus.AWAITING_HUMAN_APPROVAL.value
        )
        self._audit(principal, platform=platform, operation="prepare", outcome="succeeded",
                    reason=("direct_user_authorized" if status == SocialActionStatus.APPROVED.value
                            else "awaiting_approval"),
                    target_ref=intent.target_ref, action_digest=digest)
        return self._preparation_result(
            prep,
            intent,
            digest,
            expires,
            status,
            logical_action_ref=logical_action,
            reserved_operation_ref=operation,
        )

    def _preparation_result(
        self, prep: str, intent: SocialActionIntent, digest: str, expires: int,
        status: str, *, logical_action_ref: str | None = None,
        reserved_operation_ref: str,
    ) -> dict[str, Any]:
        result = {"preparation_ref": prep, "action": intent.action.value,
                  "status": status,
                  "action_digest": digest,
                  "reserved_operation_ref": reserved_operation_ref,
                  "commit_required": True,
                  "next_action": (
                      "social_action_commit"
                      if status == SocialActionStatus.APPROVED.value
                      else "complete_external_approval"
                  ),
                  "operation_state": SocialActionStatus.NOT_STARTED.value,
                  "provider_attempted": False,
                  "summary": f"{intent.action.value} on approved opaque reference",
                  "expires_at": _now_rfc3339(expires),
                  "required_scopes": sorted(intent.required_scopes)}
        if logical_action_ref is not None:
            result["logical_action_ref"] = logical_action_ref
        if intent.target_ref is not None:
            result["target_ref"] = intent.target_ref
        if intent.item_ref is not None:
            result["item_ref"] = intent.item_ref
        if (
            status == SocialActionStatus.AWAITING_HUMAN_APPROVAL.value
            and self.approval_url_base
        ):
            result["approval_url"] = self.approval_url_base + "?" + urlencode(
                {"preparation_ref": prep, "action_digest": digest}
            )
        return result

    def approval_preview(
        self, *, preparation_ref: str, action_digest: str
    ) -> dict[str, Any]:
        """Return the exact human preview after the HTTP layer authenticates operator."""

        now = self._now()
        with self.store._lock, self.store._connect() as conn:
            row = conn.execute(
                """SELECT * FROM social_workspace_preparation
                   WHERE preparation_hash=? AND action_digest=? AND expires_at>?""",
                (self._hash(preparation_ref), action_digest, now),
            ).fetchone()
            if row is None:
                raise SocialWorkspaceRuntimeError("preparation is expired or unknown")
            intent = self._intent_from_row(row)
            verified_assets = [
                self._asset_metadata_on_conn(
                    conn,
                    ref,
                    intent.platform.value,
                    (
                        str(row["client_hash"]),
                        str(row["subject_hash"]),
                        str(row["resource_hash"]),
                    ),
                )[0]
                for ref in self._asset_refs(intent)
            ]
            if compute_action_digest(
                intent, verified_assets=self._digest_assets(verified_assets) or None
            ) != action_digest:
                raise SocialWorkspaceRuntimeError("verified asset action digest mismatch")

            def ref_preview(ref: str | None) -> dict[str, Any] | None:
                if ref is None:
                    return None
                preview = conn.execute(
                    "SELECT preview_json FROM social_workspace_ref_preview WHERE ref_hash=?",
                    (self._hash(ref),),
                ).fetchone()
                return (
                    json.loads(self._decrypt(preview["preview_json"]))
                    if preview
                    else None
                )

            target = ref_preview(intent.target_ref)
            destination = ref_preview(intent.destination_target_ref)
            item = ref_preview(intent.item_ref)
            source_target = (
                ref_preview(str(item.get("target_ref")))
                if item and isinstance(item.get("target_ref"), str)
                else None
            )
        if intent.target_ref is not None and target is None:
            raise SocialWorkspaceRuntimeError("human target preview is unavailable")
        if intent.destination_target_ref is not None and destination is None:
            raise SocialWorkspaceRuntimeError(
                "human destination preview is unavailable"
            )
        if intent.item_ref is not None and item is None:
            raise SocialWorkspaceRuntimeError("human item preview is unavailable")
        if intent.item_ref is not None and source_target is None:
            raise SocialWorkspaceRuntimeError(
                "human source target preview is unavailable"
            )
        content = None
        if intent.content is not None:
            asset_by_ref = {
                str(asset["asset_ref"]): asset for asset in verified_assets
            }
            content = {
                "text": intent.content.text,
                "entities": [asdict(item) for item in intent.content.entities],
                "media": [
                    {
                        "role": item.role.value,
                        "asset_fingerprint": self._hash(item.asset_ref)[:12],
                        "content_digest": asset_by_ref[item.asset_ref]["content_digest"],
                        "mime_type": asset_by_ref[item.asset_ref]["mime_type"],
                        "byte_length": asset_by_ref[item.asset_ref]["byte_length"],
                        "width": asset_by_ref[item.asset_ref].get("width"),
                        "height": asset_by_ref[item.asset_ref].get("height"),
                        "display_name": asset_by_ref[item.asset_ref].get(
                            "display_name"
                        ),
                        "classification": asset_by_ref[item.asset_ref].get(
                            "classification"
                        ),
                        "expires_at": _now_rfc3339(
                            int(asset_by_ref[item.asset_ref]["expires_at"])
                        ),
                        "alt_text": item.alt_text,
                        "spoiler": item.spoiler,
                    }
                    for item in intent.content.media
                ],
                "verified_assets": [
                    {
                        **self._digest_asset_metadata(asset),
                        "asset_fingerprint": self._hash(str(asset["asset_ref"]))[:12],
                        "expires_at": _now_rfc3339(int(asset["expires_at"])),
                    }
                    for asset in verified_assets
                ],
            }
        return {
            "platform": intent.platform.value,
            "action": intent.action.value,
            "action_digest": action_digest,
            "target": target,
            "destination_target": destination,
            "item": item,
            "source_target": source_target,
            "content": content,
            "reaction": intent.reaction,
            "reaction_preset": (
                intent.reaction_preset.value
                if intent.reaction_preset is not None
                else None
            ),
            "schedule_at": intent.schedule_at,
            "expires_at": _now_rfc3339(int(row["expires_at"])),
        }

    def approve_preparation(
        self, *, preparation_ref: str, operator_principal: str, operator_nonce: str,
        ttl_seconds: int | None = None,
    ) -> dict[str, str]:
        """Server approval-page helper; deliberately never registered as an MCP tool.

        The production caller must authenticate the operator and supply a fresh,
        single-use CSRF/approval nonce.  Model-authored tool arguments can never
        create this state.
        """
        if not operator_principal.strip() or len(operator_nonce) < 16:
            raise SocialWorkspaceRuntimeError("authenticated operator and nonce are required")
        now = self._now()
        with self.store._lock, self.store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                prep = conn.execute(
                    "SELECT * FROM social_workspace_preparation WHERE preparation_hash=?",
                    (self._hash(preparation_ref),),
                ).fetchone()
                if prep is None or int(prep["expires_at"]) <= now:
                    raise SocialWorkspaceRuntimeError("preparation is expired or unknown")
                intent = self._intent_from_row(prep)
                verified_assets = [
                    self._asset_metadata_on_conn(
                        conn,
                        ref,
                        intent.platform.value,
                        (
                            str(prep["client_hash"]),
                            str(prep["subject_hash"]),
                            str(prep["resource_hash"]),
                        ),
                    )[0]
                    for ref in self._asset_refs(intent)
                ]
                if compute_action_digest(
                    intent, verified_assets=self._digest_assets(verified_assets) or None
                ) != str(prep["action_digest"]):
                    raise SocialWorkspaceRuntimeError(
                        "verified asset action digest mismatch"
                    )
                nonce_hash = self._hash(operator_nonce)
                if conn.execute("SELECT 1 FROM social_workspace_approval WHERE operator_nonce_hash=?", (nonce_hash,)).fetchone():
                    raise SocialWorkspaceRuntimeError("operator approval nonce was already used")
                approval = "apr_" + secrets.token_urlsafe(24)
                receipt = "arc_" + secrets.token_urlsafe(24)
                expires = min(int(prep["expires_at"]), now + int(ttl_seconds or self.approval_ttl_seconds))
                conn.execute(
                    """INSERT INTO social_workspace_approval(approval_hash,approval_ref,receipt_ref,
                       receipt_hash,preparation_hash,client_hash,subject_hash,resource_hash,target_ref_hash,
                       action_digest,operator_hash,operator_nonce_hash,expires_at,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (self._hash(approval), self._hash(approval),
                     self._hash(receipt), self._hash(receipt),
                     self._hash(preparation_ref), prep["client_hash"], prep["subject_hash"],
                     prep["resource_hash"], prep["target_ref_hash"], prep["action_digest"],
                     self._hash(operator_principal), nonce_hash, expires, now),
                )
                conn.execute(
                    "UPDATE social_workspace_preparation SET status=? WHERE preparation_hash=?",
                    (SocialActionStatus.APPROVED.value, self._hash(preparation_ref)),
                )
                conn.execute("COMMIT")
                return {"approval_ref": approval, "approval_receipt": receipt}
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _intent_from_row(self, row: sqlite3.Row) -> SocialActionIntent:
        from .social_workspace import validate_prepare_request
        return validate_prepare_request(json.loads(self._decrypt(row["intent_ciphertext"])))

    async def commit(
        self, payload: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        principal = RuntimePrincipal.from_context(context)
        browser_approved = set(payload) == {"preparation_ref", "action_digest"}
        credential_approved = set(payload) == {
            "preparation_ref",
            "approval_ref",
            "approval_receipt",
            "action_digest",
        }
        if not browser_approved and not credential_approved:
            self._audit(principal, platform=None, operation="commit", outcome="denied", reason="invalid_commit_shape")
            raise SocialWorkspaceRuntimeError(
                "commit requires exact browser-approved fields",
                error_code="INVALID_COMMIT_SHAPE",
            )
        prep_ref = str(payload["preparation_ref"])
        digest = str(payload["action_digest"])
        now = self._now()
        client, subject, resource = self._binding(principal)
        # Re-authorize the current kill switch, OAuth scope, target rights and
        # immutable bytes before consuming the one-shot preparation.
        with self.store._lock, self.store._connect() as conn:
            preflight = conn.execute(
                "SELECT * FROM social_workspace_preparation WHERE preparation_hash=?",
                (self._hash(prep_ref),),
            ).fetchone()
        if preflight is None:
            raise SocialWorkspaceRuntimeError(
                "approval or preparation is invalid",
                error_code="PREPARATION_UNKNOWN",
            )
        self._audit(
            principal,
            platform=str(preflight["platform"]),
            operation="commit_ingress",
            outcome="resolved",
            reason="preparation_resolved",
            action_digest=digest,
        )
        if (
            preflight["client_hash"],
            preflight["subject_hash"],
            preflight["resource_hash"],
        ) != (client, subject, resource):
            raise SocialWorkspaceRuntimeError(
                "preparation binding mismatch",
                error_code="PREPARATION_BINDING_MISMATCH",
            )
        if preflight["action_digest"] != digest:
            raise SocialWorkspaceRuntimeError(
                "preparation action digest mismatch",
                error_code="ACTION_DIGEST_MISMATCH",
            )
        with self.store._lock, self.store._connect() as conn:
            existing = conn.execute(
                """SELECT operation_ref FROM social_workspace_operation
                   WHERE preparation_hash=? AND client_hash=? AND subject_hash=?
                     AND resource_hash=?""",
                (self._hash(prep_ref), client, subject, resource),
            ).fetchone()
        if existing is not None:
            operation = str(existing["operation_ref"])
            self._audit(
                principal,
                platform=str(preflight["platform"]),
                operation="commit_ingress",
                outcome="replayed",
                reason="operation_already_committed",
                action_digest=digest,
            )
            return await self.status("operation", operation, context)
        if int(preflight["expires_at"]) <= now:
            raise SocialWorkspaceRuntimeError(
                "approval or preparation expired",
                error_code="PREPARATION_EXPIRED",
            )
        self._audit(
            principal,
            platform=str(preflight["platform"]),
            operation="commit_ingress",
            outcome="started",
            reason="preflight_started",
            action_digest=digest,
        )
        preflight_intent = self._intent_from_row(preflight)
        self._enforce_document_runtime_policy(preflight_intent)
        if not social_scopes_authorized(
            preflight_intent.required_scopes, principal.scopes
        ):
            raise SocialWorkspaceRuntimeError("required social action scope is missing")
        await self._authorize_document_target(preflight_intent, principal)
        preflight_assets = self._asset_metadata_for_intent(
            preflight_intent, principal
        )
        if compute_action_digest(
            preflight_intent,
            verified_assets=self._digest_assets(preflight_assets) or None,
        ) != digest:
            raise SocialWorkspaceRuntimeError("verified asset action digest mismatch")
        await self._reverify_document_assets(
            preflight_intent, principal, preflight_assets
        )
        # Resolve every opaque reference before reserving an attempt. A local
        # expired/foreign binding must never consume provider-attempt budget.
        preflight_native_intent = self._native_intent(preflight_intent, principal)
        replay_existing = False
        with self.store._lock, self.store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                prep = conn.execute("SELECT * FROM social_workspace_preparation WHERE preparation_hash=?", (self._hash(prep_ref),)).fetchone()
                if browser_approved:
                    approval = conn.execute(
                        """SELECT * FROM social_workspace_approval
                           WHERE preparation_hash=? AND action_digest=? AND consumed_at IS NULL
                           ORDER BY created_at DESC LIMIT 1""",
                        (self._hash(prep_ref), digest),
                    ).fetchone()
                else:
                    approval = conn.execute(
                        """SELECT * FROM social_workspace_approval
                           WHERE approval_hash=? AND receipt_hash=?""",
                        (
                            self._hash(str(payload["approval_ref"])),
                            self._hash(str(payload["approval_receipt"])),
                        ),
                    ).fetchone()
                if prep is None:
                    raise SocialWorkspaceRuntimeError("approval or preparation is invalid")
                logical_action = prep["logical_action_ref"]
                if not logical_action:
                    logical_action = "act_" + secrets.token_urlsafe(24)
                    conn.execute(
                        """UPDATE social_workspace_preparation
                           SET logical_action_ref=?
                           WHERE preparation_hash=? AND logical_action_ref IS NULL""",
                        (logical_action, self._hash(prep_ref)),
                    )
                prep_binding = (
                    prep["client_hash"],
                    prep["subject_hash"],
                    prep["resource_hash"],
                    prep["action_digest"],
                    prep["target_ref_hash"],
                )
                expected_prep_binding = (
                    client,
                    subject,
                    resource,
                    digest,
                    prep["target_ref_hash"],
                )
                if prep_binding != expected_prep_binding:
                    raise SocialWorkspaceRuntimeError("preparation binding mismatch")
                if int(prep["expires_at"]) <= now:
                    raise SocialWorkspaceRuntimeError("approval or preparation expired")
                intent = self._intent_from_row(prep)
                self._enforce_document_runtime_policy(intent)
                existing_operation = conn.execute(
                    """SELECT * FROM social_workspace_operation
                       WHERE preparation_hash=? AND client_hash=? AND subject_hash=?
                         AND resource_hash=?""",
                    (self._hash(prep_ref), client, subject, resource),
                ).fetchone()
                direct_user_authorized = False
                if existing_operation is None:
                    direct_user_authorized = (
                        browser_approved
                        and prep["status"] == SocialActionStatus.APPROVED.value
                        and (
                            intent.action in DIRECT_USER_AUTHORIZED_ACTIONS
                            or self._scheduled_item_direct_authorized_on_conn(
                                conn, intent, principal
                            )
                        )
                    )
                    if not direct_user_authorized:
                        if approval is None:
                            raise SocialWorkspaceRuntimeError(
                                "approval or preparation is invalid"
                            )
                        expected_binding = (
                            client,
                            subject,
                            resource,
                            digest,
                            self._hash(prep_ref),
                            prep["target_ref_hash"],
                        )
                        actual_binding = (
                            approval["client_hash"],
                            approval["subject_hash"],
                            approval["resource_hash"],
                            approval["action_digest"],
                            approval["preparation_hash"],
                            approval["target_ref_hash"],
                        )
                        if expected_binding != actual_binding:
                            raise SocialWorkspaceRuntimeError("approval binding mismatch")
                        if int(approval["expires_at"]) <= now:
                            raise SocialWorkspaceRuntimeError(
                                "approval or preparation expired"
                            )
                        if approval["consumed_at"] is not None:
                            raise SocialWorkspaceRuntimeError(
                                "approval receipt was already consumed"
                            )
                verified_assets = self._asset_metadata_for_intent(
                    intent, principal, conn=conn
                )
                if compute_action_digest(
                    intent, verified_assets=self._digest_assets(verified_assets) or None
                ) != digest:
                    raise SocialWorkspaceRuntimeError(
                        "verified asset action digest mismatch"
                    )
                if not social_scopes_authorized(
                    intent.required_scopes, principal.scopes
                ):
                    raise SocialWorkspaceRuntimeError("required social action scope is missing")
                target_ref = self._action_budget_target_ref(intent, conn)
                if existing_operation is not None:
                    operation = str(existing_operation["operation_ref"])
                    replay_existing = True
                else:
                    self._consume_budget_on_conn(
                        conn, principal, intent.platform.value, target_ref,
                        intent.action.value, "attempts", 1, now,
                    )
                    self._consume_budget_on_conn(
                        conn, principal, intent.platform.value, target_ref,
                        intent.action.value, "media",
                        len(intent.content.media) if intent.content else 0, now,
                    )
                    if not direct_user_authorized:
                        changed = conn.execute(
                            """UPDATE social_workspace_approval SET consumed_at=?
                               WHERE approval_hash=? AND consumed_at IS NULL""",
                            (now, approval["approval_hash"]),
                        ).rowcount
                        if changed != 1:
                            raise SocialWorkspaceRuntimeError(
                                "approval receipt was already consumed"
                            )
                    operation = str(
                        prep["operation_ref"] or ("op_" + secrets.token_urlsafe(24))
                    )
                    if not prep["operation_ref"]:
                        conn.execute(
                            "UPDATE social_workspace_preparation SET operation_ref=? WHERE preparation_hash=?",
                            (operation, self._hash(prep_ref)),
                        )
                    conn.execute(
                        """INSERT INTO social_workspace_operation(operation_hash,operation_ref,
                           preparation_hash,client_hash,subject_hash,resource_hash,platform,action,
                           target_ref_hash,status,retry_safe,provider_attempted_at,created_at,updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (self._hash(operation), operation, self._hash(prep_ref), client, subject,
                         resource, intent.platform.value, intent.action.value,
                         self._hash(target_ref) if target_ref else None,
                         SocialActionStatus.PROVIDER_ATTEMPTED.value, 0, now, now, now),
                    )
                    conn.execute(
                        "UPDATE social_workspace_preparation SET status=? WHERE preparation_hash=?",
                        (SocialActionStatus.COMMITTED.value, self._hash(prep_ref)),
                    )
                conn.execute("COMMIT")
            except Exception as exc:
                conn.execute("ROLLBACK")
                conn.execute(
                    """INSERT INTO social_workspace_audit(principal_hash,platform,operation,
                       target_ref_hash,action_digest,outcome,reason_code,response_bytes,
                       media_items,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (self._principal_hash(principal), str(preflight["platform"]),
                     "commit", preflight["target_ref_hash"], digest, "denied",
                     str(getattr(exc, "audit_reason_code", type(exc).__name__)).lower()[:64],
                     0, 0, self._now()),
                )
                raise
        platform = intent.platform.value
        media_count = len(intent.content.media) if intent.content else 0
        if replay_existing:
            self._audit(
                principal,
                platform=platform,
                operation="commit_ingress",
                outcome="replayed",
                reason="operation_already_committed",
                target_ref=target_ref,
                action_digest=digest,
            )
            return await self.status("operation", operation, context)
        self._audit(
            principal,
            platform=platform,
            operation="commit_ingress",
            outcome="reserved",
            reason="operation_reserved",
            target_ref=target_ref,
            action_digest=digest,
        )
        self._audit(
            principal,
            platform=platform,
            operation="commit_ingress",
            outcome="started",
            reason="provider_attempt_started",
            target_ref=target_ref,
            action_digest=digest,
        )
        provider_returned = False
        try:
            native = preflight_native_intent
            known: dict[tuple[str, str], str] = {}
            if native.target_ref and intent.target_ref:
                known[("target", native.target_ref)] = intent.target_ref
            if native.item_ref and intent.item_ref:
                known[("item", native.item_ref)] = intent.item_ref
            # Mutating adapters own their transport deadline and durable
            # provider-ledger finalization.  An equal runtime wait_for used to
            # cancel Telegram after its claim but before completion/release.
            raw = await self._adapter(platform).execute(
                native, operation_ref=operation
            )
            provider_returned = True
            safe = self._sanitize_provider_output(raw, platform, principal, known_refs=known)
            if not isinstance(safe, dict):
                raise SocialWorkspaceRuntimeError("provider action result must be an object")
            safe.update({"platform": platform, "operation_ref": operation,
                         "action": intent.action.value,
                         "preparation_ref": prep_ref,
                         "logical_action_ref": str(logical_action),
                         "preparation_status": SocialActionStatus.COMMITTED.value,
                         "provider_attempted": True,
                         "attempt_number": 1})
            safe.setdefault("status", SocialActionStatus.SUCCEEDED.value)
            safe["operation_status"] = safe["status"]
            safe.setdefault("retry_safe", False)
            validate_action_status_response(safe)
            size = len(_json(safe).encode())
            if size > self.response_cap_bytes:
                raise SocialWorkspaceRuntimeError("response cap exceeded")
            self._consume_budget(principal, platform, target_ref, intent.action.value, "egress", size)
            self._finish_operation(operation, safe, safe.get("error_code"))
            succeeded = safe["status"] == SocialActionStatus.SUCCEEDED.value
            self._record_provider_result(
                principal, platform, target_ref, success=succeeded
            )
            audit_outcome = str(safe["status"])
            audit_reason = (
                "provider_succeeded"
                if succeeded
                else str(safe.get("error_code") or "provider_returned_non_success")
            )
            self._audit(principal, platform=platform, operation="commit", outcome=audit_outcome,
                        reason=audit_reason, target_ref=target_ref,
                        action_digest=digest, response_bytes=size, media_items=media_count)
            return safe
        except asyncio.CancelledError:
            # The adapter has already observed cancellation at its durable
            # provider-operation boundary.  Persist conservative outer state
            # and let protocol cancellation propagate; status can reconcile.
            unknown = {
                "platform": platform,
                "operation_ref": operation,
                "preparation_ref": prep_ref,
                "logical_action_ref": str(logical_action),
                "action": intent.action.value,
                "status": SocialActionStatus.OUTCOME_UNKNOWN.value,
                "preparation_status": SocialActionStatus.COMMITTED.value,
                "operation_status": SocialActionStatus.OUTCOME_UNKNOWN.value,
                "provider_attempted": True,
                "retry_safe": False,
                "attempt_number": 1,
                "mutation_boundary_reached": True,
                "error_code": "provider_cancelled",
            }
            self._finish_operation(operation, unknown, "provider_cancelled")
            self._audit(
                principal,
                platform=platform,
                operation="commit",
                outcome="outcome_unknown",
                reason="provider_cancelled",
                target_ref=target_ref,
                action_digest=digest,
            )
            raise
        except asyncio.TimeoutError:
            unknown = {"platform": platform, "operation_ref": operation,
                       "preparation_ref": prep_ref,
                       "logical_action_ref": str(logical_action),
                       "action": intent.action.value, "status": "outcome_unknown",
                       "preparation_status": SocialActionStatus.COMMITTED.value,
                       "operation_status": SocialActionStatus.OUTCOME_UNKNOWN.value,
                       "provider_attempted": True,
                       "retry_safe": False, "attempt_number": 1,
                       "mutation_boundary_reached": True,
                       "error_code": "provider_timeout"}
            self._finish_operation(operation, unknown, "provider_timeout")
            self._record_provider_result(principal, platform, target_ref, success=False)
            self._audit(principal, platform=platform, operation="commit", outcome="outcome_unknown",
                        reason="provider_timeout", target_ref=target_ref, action_digest=digest)
            return unknown
        except Exception as exc:  # noqa: BLE001 - classify post-attempt failures safely
            if provider_returned:
                withheld = {
                    "platform": platform,
                    "operation_ref": operation,
                    "action": intent.action.value,
                    "status": SocialActionStatus.OUTCOME_UNKNOWN.value,
                    "preparation_status": SocialActionStatus.COMMITTED.value,
                    "operation_status": SocialActionStatus.OUTCOME_UNKNOWN.value,
                    "provider_attempted": True,
                    "retry_safe": False,
                    "error_code": "response_withheld",
                }
                self._finish_operation(operation, withheld, "response_withheld")
                self._record_provider_result(
                    principal, platform, target_ref, success=True
                )
                self._audit(
                    principal, platform=platform, operation="commit",
                    outcome="succeeded_response_withheld",
                    reason="response_withheld", target_ref=target_ref,
                    action_digest=digest, media_items=media_count,
                )
                return withheld
            failed = {"platform": platform, "operation_ref": operation,
                      "preparation_ref": prep_ref,
                      "logical_action_ref": str(logical_action),
                      "action": intent.action.value, "status": "failed",
                      "preparation_status": SocialActionStatus.COMMITTED.value,
                      "operation_status": SocialActionStatus.FAILED.value,
                      "provider_attempted": True,
                      "retry_safe": False, "attempt_number": 1,
                      "error_code": "provider_failure"}
            self._finish_operation(operation, failed, "provider_failure")
            self._record_provider_result(principal, platform, target_ref, success=False,
                                         flood_seconds=int(getattr(exc, "retry_after", 0) or 0))
            self._audit(principal, platform=platform, operation="commit", outcome="failed",
                        reason="provider_failure", target_ref=target_ref, action_digest=digest)
            raise self._safe_provider_error() from None

    def _consume_budget_on_conn(self, conn: sqlite3.Connection, principal: RuntimePrincipal,
        platform: str, target_ref: str | None, action: str, metric: str, amount: int, now: int) -> None:
        period_format = "%Y-%m-%d" if metric == "attempts" else "%Y-%m-%dT%H"
        period = datetime.fromtimestamp(now, timezone.utc).strftime(period_format)
        for dimension, raw_key in self._budget_keys(
            principal, platform, target_ref, action, conn=conn
        ):
            limit = self._budget_dimension_limits[metric][dimension]
            key = self._hash(raw_key)
            row = conn.execute("SELECT amount FROM social_workspace_budget WHERE period=? AND dimension=? AND bucket_hash=? AND metric=?", (period, dimension, key, metric)).fetchone()
            if (int(row["amount"]) if row else 0) + amount > limit:
                raise SocialWorkspaceRuntimeError(
                    f"{metric} budget exceeded",
                    error_code=f"{metric.upper()}_BUDGET_EXCEEDED",
                )
            conn.execute("""INSERT INTO social_workspace_budget(period,dimension,bucket_hash,metric,amount,updated_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(period,dimension,bucket_hash,metric)
                DO UPDATE SET amount=amount+excluded.amount,updated_at=excluded.updated_at""",
                (period, dimension, key, metric, amount, now))

    def _finish_operation(self, operation: str, result: Mapping[str, Any], error: str | None) -> None:
        with self.store._lock, self.store._connect() as conn:
            conn.execute("""UPDATE social_workspace_operation SET status=?,retry_safe=?,
                result_json=?,error_code=?,retry_in_progress=0,updated_at=?
                WHERE operation_hash=?""",
                (result["status"], int(bool(result.get("retry_safe"))), _json(result), error,
                 self._now(), self._hash(operation)))

    async def status(self, reference_kind: str, reference: str, context: ToolCallContext) -> dict[str, Any]:
        principal = RuntimePrincipal.from_context(context)
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:
            if reference_kind == "operation":
                row = conn.execute("SELECT * FROM social_workspace_operation WHERE operation_hash=? AND client_hash=? AND subject_hash=? AND resource_hash=?", (self._hash(reference), client, subject, resource)).fetchone()
                if row is None:
                    reserved = conn.execute(
                        """SELECT * FROM social_workspace_preparation
                           WHERE operation_ref=? AND client_hash=? AND subject_hash=?
                             AND resource_hash=?""",
                        (reference, client, subject, resource),
                    ).fetchone()
                    if reserved is None:
                        raise SocialWorkspaceRuntimeError(
                            "operation is unknown or not bound",
                            error_code="OPERATION_UNKNOWN",
                        )
                    return {
                        "platform": reserved["platform"],
                        "operation_ref": reference,
                        "preparation_ref": str(reserved["preparation_ref"]),
                        "logical_action_ref": str(reserved["logical_action_ref"]),
                        "action": reserved["action"],
                        "status": SocialActionStatus.NOT_STARTED.value,
                        "preparation_status": reserved["status"],
                        "operation_status": SocialActionStatus.NOT_STARTED.value,
                        "provider_attempted": False,
                        "mutation_boundary_reached": False,
                        "retry_safe": False,
                        "error_code": "operation_not_started",
                    }
                if row["result_json"]:
                    return json.loads(row["result_json"])
                return {"platform": row["platform"], "operation_ref": reference,
                        "action": row["action"], "status": row["status"],
                        "operation_status": row["status"],
                        "provider_attempted": row["provider_attempted_at"] is not None,
                        "retry_safe": bool(row["retry_safe"]),
                        "attempt_number": int(row["attempt_number"] or 1)}
            prep = conn.execute("SELECT * FROM social_workspace_preparation WHERE preparation_hash=? AND client_hash=? AND subject_hash=? AND resource_hash=?", (self._hash(reference), client, subject, resource)).fetchone()
            if prep is None:
                raise SocialWorkspaceRuntimeError("preparation is unknown or not bound")
            if not prep["logical_action_ref"]:
                logical_action_ref = "act_" + secrets.token_urlsafe(24)
                conn.execute(
                    """UPDATE social_workspace_preparation
                       SET logical_action_ref=?
                       WHERE preparation_hash=? AND logical_action_ref IS NULL""",
                    (logical_action_ref, self._hash(reference)),
                )
                prep = conn.execute(
                    """SELECT * FROM social_workspace_preparation
                       WHERE preparation_hash=?""",
                    (self._hash(reference),),
                ).fetchone()
                assert prep is not None
            linked = conn.execute(
                """SELECT * FROM social_workspace_operation
                   WHERE preparation_hash=? AND client_hash=? AND subject_hash=? AND resource_hash=?""",
                (self._hash(reference), client, subject, resource),
            ).fetchone()
            if linked is not None:
                if linked["result_json"]:
                    return json.loads(linked["result_json"])
                return {"platform": linked["platform"], "operation_ref": linked["operation_ref"],
                        "action": linked["action"], "status": linked["status"],
                        "preparation_status": prep["status"],
                        "operation_status": linked["status"],
                        "provider_attempted": linked["provider_attempted_at"] is not None,
                        "retry_safe": bool(linked["retry_safe"]),
                        "preparation_ref": str(prep["preparation_ref"]),
                        "logical_action_ref": str(prep["logical_action_ref"]),
                        "attempt_number": int(linked["attempt_number"] or 1)}
            operation_ref = prep["operation_ref"]
            if not operation_ref:
                operation_ref = "op_" + secrets.token_urlsafe(24)
                conn.execute(
                    """UPDATE social_workspace_preparation SET operation_ref=?
                       WHERE preparation_hash=? AND operation_ref IS NULL""",
                    (operation_ref, self._hash(reference)),
                )
            return {"platform": prep["platform"], "operation_ref": operation_ref,
                    "preparation_ref": str(prep["preparation_ref"]),
                    "logical_action_ref": str(prep["logical_action_ref"]),
                    "action": prep["action"], "status": prep["status"],
                    "preparation_status": prep["status"],
                    "operation_status": SocialActionStatus.NOT_STARTED.value,
                    "provider_attempted": False,
                    "mutation_boundary_reached": False,
                    "retry_safe": False}

    async def retry(
        self, operation_ref: str, context: ToolCallContext
    ) -> dict[str, Any]:
        """Run one bounded CAS-guarded retry for a proven safe terminal failure."""

        principal = RuntimePrincipal.from_context(context)
        client, subject, resource = self._binding(principal)
        now = self._now()
        with self.store._lock, self.store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    """SELECT o.*,p.preparation_ref,p.logical_action_ref,
                              p.intent_ciphertext,p.action_digest
                       FROM social_workspace_operation o
                       JOIN social_workspace_preparation p
                         ON p.preparation_hash=o.preparation_hash
                       WHERE o.operation_hash=? AND o.client_hash=?
                         AND o.subject_hash=? AND o.resource_hash=?""",
                    (self._hash(operation_ref), client, subject, resource),
                ).fetchone()
                if row is None:
                    raise SocialWorkspaceRuntimeError(
                        "operation is unknown or not bound"
                    )
                if (
                    row["status"] != SocialActionStatus.FAILED.value
                    or not bool(row["retry_safe"])
                    or not row["result_json"]
                ):
                    raise SocialWorkspaceRuntimeError(
                        "operation is not a terminal retry-safe failure"
                    )
                attempt_number = int(row["attempt_number"] or 1) + 1
                if attempt_number > 3:
                    raise SocialWorkspaceRuntimeError("retry attempt limit reached")
                intent = self._intent_from_row(row)
                adapter_retry = getattr(
                    self._adapter(intent.platform.value), "retry", None
                )
                if not callable(adapter_retry):
                    raise SocialWorkspaceRuntimeError(
                        "provider retry is unavailable"
                    )
                if not social_scopes_authorized(
                    intent.required_scopes, principal.scopes
                ):
                    raise SocialWorkspaceRuntimeError(
                        "required social action scope is missing"
                    )
                target_ref = self._action_budget_target_ref(intent, conn)
                self._consume_budget_on_conn(
                    conn,
                    principal,
                    intent.platform.value,
                    target_ref,
                    intent.action.value,
                    "attempts",
                    1,
                    now,
                )
                changed = conn.execute(
                    """UPDATE social_workspace_operation
                       SET status=?,retry_safe=0,result_json=NULL,error_code=NULL,
                           retry_in_progress=1,retry_started_at=?,attempt_number=?,
                           provider_attempted_at=?,updated_at=?
                       WHERE operation_hash=? AND status='failed'
                         AND retry_safe=1 AND retry_in_progress=0
                         AND attempt_number=?""",
                    (
                        SocialActionStatus.PROVIDER_ATTEMPTED.value,
                        now,
                        attempt_number,
                        now,
                        now,
                        self._hash(operation_ref),
                        attempt_number - 1,
                    ),
                ).rowcount
                if changed != 1:
                    raise SocialWorkspaceRuntimeError(
                        "retry is already in progress"
                    )
                logical_action_ref = row["logical_action_ref"]
                if not logical_action_ref:
                    logical_action_ref = "act_" + secrets.token_urlsafe(24)
                    conn.execute(
                        """UPDATE social_workspace_preparation
                           SET logical_action_ref=?
                           WHERE preparation_hash=? AND logical_action_ref IS NULL""",
                        (logical_action_ref, row["preparation_hash"]),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        platform = intent.platform.value
        native = self._native_intent(intent, principal)
        known: dict[tuple[str, str], str] = {}
        if native.target_ref and intent.target_ref:
            known[("target", native.target_ref)] = intent.target_ref
        if native.item_ref and intent.item_ref:
            known[("item", native.item_ref)] = intent.item_ref
        provider_returned = False
        try:
            parameters = inspect.signature(adapter_retry).parameters
            accepts_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            )
            kwargs: dict[str, Any] = {"operation_ref": operation_ref}
            if "attempt_number" in parameters or accepts_kwargs:
                kwargs["attempt_number"] = attempt_number
            raw = await adapter_retry(native, **kwargs)
            provider_returned = True
            safe = self._sanitize_provider_output(
                raw, platform, principal, known_refs=known
            )
            if not isinstance(safe, dict):
                raise SocialWorkspaceRuntimeError(
                    "provider retry result must be an object"
                )
            safe.update(
                {
                    "platform": platform,
                    "operation_ref": operation_ref,
                    "preparation_ref": str(row["preparation_ref"]),
                    "logical_action_ref": str(logical_action_ref),
                    "action": intent.action.value,
                    "attempt_number": attempt_number,
                }
            )
            safe.setdefault("retry_safe", False)
            validate_action_status_response(safe)
            self._finish_operation(
                operation_ref, safe, safe.get("error_code")
            )
            succeeded = safe["status"] == SocialActionStatus.SUCCEEDED.value
            self._record_provider_result(
                principal, platform, target_ref, success=succeeded
            )
            self._audit(
                principal,
                platform=platform,
                operation="retry",
                outcome=str(safe["status"]),
                reason=(
                    "provider_succeeded"
                    if succeeded
                    else str(safe.get("error_code") or "provider_returned_non_success")
                ),
                target_ref=target_ref,
                action_digest=str(row["action_digest"]),
            )
            return safe
        except asyncio.CancelledError:
            unknown = {
                "platform": platform,
                "operation_ref": operation_ref,
                "preparation_ref": str(row["preparation_ref"]),
                "logical_action_ref": str(logical_action_ref),
                "action": intent.action.value,
                "status": SocialActionStatus.OUTCOME_UNKNOWN.value,
                "retry_safe": False,
                "attempt_number": attempt_number,
                "mutation_boundary_reached": True,
                "error_code": "provider_cancelled",
            }
            self._finish_operation(operation_ref, unknown, "provider_cancelled")
            raise
        except Exception:
            result = {
                "platform": platform,
                "operation_ref": operation_ref,
                "preparation_ref": str(row["preparation_ref"]),
                "logical_action_ref": str(logical_action_ref),
                "action": intent.action.value,
                "status": SocialActionStatus.OUTCOME_UNKNOWN.value,
                "retry_safe": False,
                "attempt_number": attempt_number,
                "error_code": (
                    "response_withheld" if provider_returned else "provider_failure"
                ),
            }
            self._finish_operation(operation_ref, result, result["error_code"])
            self._record_provider_result(
                principal, platform, target_ref, success=provider_returned
            )
            return result

    async def reconcile(self, operation_ref: str, context: ToolCallContext) -> dict[str, Any]:
        principal = RuntimePrincipal.from_context(context)
        current = await self.status("operation", operation_ref, context)
        reconcilable = {
            SocialActionStatus.OUTCOME_UNKNOWN.value,
            SocialActionStatus.PROVIDER_ATTEMPTED.value,
        }
        if current["status"] not in reconcilable:
            return current
        unknown = {
            "platform": current["platform"],
            "operation_ref": operation_ref,
            "action": current["action"],
            "status": SocialActionStatus.OUTCOME_UNKNOWN.value,
            "retry_safe": False,
            "error_code": "reconciliation_pending",
        }
        adapter = self._adapter(current["platform"])
        reconcile = getattr(adapter, "reconcile", None)
        if not callable(reconcile):
            self._finish_operation(operation_ref, unknown, unknown["error_code"])
            return unknown
        known: dict[tuple[str, str], str] = {}
        native_intent: SocialActionIntent | None = None
        prep: sqlite3.Row | None = None
        operation_row: sqlite3.Row | None = None
        with self.store._lock, self.store._connect() as conn:
            operation_row = conn.execute(
                """SELECT o.* FROM social_workspace_operation o
                   WHERE o.operation_hash=? AND o.client_hash=?
                     AND o.subject_hash=? AND o.resource_hash=?""",
                (self._hash(operation_ref), *self._binding(principal)),
            ).fetchone()
            prep = conn.execute(
                """SELECT p.* FROM social_workspace_preparation p
                   JOIN social_workspace_operation o
                     ON o.preparation_hash=p.preparation_hash
                   WHERE o.operation_hash=? AND o.client_hash=?
                     AND o.subject_hash=? AND o.resource_hash=?""",
                (self._hash(operation_ref), *self._binding(principal)),
            ).fetchone()
        if prep is not None:
            unknown["preparation_ref"] = str(prep["preparation_ref"])
            if prep["logical_action_ref"]:
                unknown["logical_action_ref"] = str(prep["logical_action_ref"])
        if operation_row is not None:
            unknown["attempt_number"] = int(
                operation_row["attempt_number"] or 1
            )
        if prep is not None:
            original_intent = self._intent_from_row(prep)
            native_intent = self._native_reconciliation_intent(
                original_intent, principal
            )
            if native_intent.target_ref and original_intent.target_ref:
                known[("target", native_intent.target_ref)] = original_intent.target_ref
            if native_intent.item_ref and original_intent.item_ref:
                known[("item", native_intent.item_ref)] = original_intent.item_ref
        try:
            parameters = inspect.signature(reconcile).parameters
            accepts_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            )
            kwargs: dict[str, Any] = {}
            if native_intent is not None and (
                "intent" in parameters or accepts_kwargs
            ):
                kwargs["intent"] = native_intent
            if operation_row is not None and (
                "evidence" in parameters or accepts_kwargs
            ):
                kwargs["evidence"] = {
                    "provider_attempted_at": operation_row["provider_attempted_at"],
                    "attempt_number": int(operation_row["attempt_number"] or 1),
                }
            # Reconciliation is also provider-deadline-owned.  It may need
            # durable readback/finalization beyond the former equal timeout.
            raw = await reconcile(operation_ref, **kwargs)
            safe = self._sanitize_provider_output(
                raw, current["platform"], principal, known_refs=known
            )
            if not isinstance(safe, dict):
                raise SocialWorkspaceRuntimeError("provider status must be an object")
            safe.update({"platform": current["platform"], "operation_ref": operation_ref,
                         "action": current["action"]})
            if prep is not None:
                safe.setdefault("preparation_ref", str(prep["preparation_ref"]))
                if prep["logical_action_ref"]:
                    safe.setdefault(
                        "logical_action_ref", str(prep["logical_action_ref"])
                    )
            if operation_row is not None:
                safe.setdefault(
                    "attempt_number", int(operation_row["attempt_number"] or 1)
                )
            safe.setdefault("retry_safe", False)
            validate_action_status_response(safe)
            self._finish_operation(operation_ref, safe, safe.get("error_code"))
            reconciled_status = str(safe["status"])
            self._audit(principal, platform=current["platform"], operation="reconcile",
                        outcome=reconciled_status,
                        reason=(
                            "status_reconciled"
                            if reconciled_status == SocialActionStatus.SUCCEEDED.value
                            else str(safe.get("error_code") or "reconciliation_pending")
                        ))
            return safe
        except asyncio.CancelledError:
            self._audit(
                principal,
                platform=current["platform"],
                operation="reconcile",
                outcome="outcome_unknown",
                reason="reconciliation_cancelled",
            )
            raise
        except Exception:  # noqa: BLE001 - provider exception text is untrusted
            self._finish_operation(operation_ref, unknown, unknown["error_code"])
            self._audit(
                principal, platform=current["platform"], operation="reconcile",
                outcome="outcome_unknown", reason="reconciliation_pending",
            )
            return unknown

    @staticmethod
    def _asset_scope_authorized(
        platform: str, role: MediaRole, scopes: frozenset[str]
    ) -> bool:
        suffixes = (
            ("dm:send",)
            if role is MediaRole.DOCUMENT
            else ("post:publish", "story:write")
        )
        return any(
            social_scopes_authorized(frozenset({f"{platform}:{suffix}"}), scopes)
            for suffix in suffixes
        )

    async def stage_asset(
        self, request: AssetStageRequest, context: ToolCallContext
    ) -> dict[str, Any]:
        """Ingest one ChatGPT file and bind the provider-staged asset.

        Core never downloads ``download_url``.  It authorizes and reserves
        budgets before delegating that outbound/storage work to the injected
        ingestor, and the provider adapter receives only ``VerifiedAsset``.
        """
        principal = RuntimePrincipal.from_context(context)
        platform = request.platform.value
        try:
            if not self._asset_scope_authorized(
                platform, request.role, principal.scopes
            ):
                raise SocialWorkspaceRuntimeError("required social asset scope is missing")
            if request.role is MediaRole.IMAGE and not self.media_story_enabled:
                raise SocialWorkspaceRuntimeError("image asset staging is disabled")
            if request.role is MediaRole.DOCUMENT and (
                not self.file_send_enabled or platform != SocialPlatform.TELEGRAM.value
            ):
                raise SocialWorkspaceRuntimeError("document asset staging is disabled")
            if request.role not in {MediaRole.IMAGE, MediaRole.DOCUMENT}:
                raise SocialWorkspaceRuntimeError(
                    "only image or document asset staging is enabled"
                )
            if self.asset_ingestor is None:
                raise SocialWorkspaceRuntimeError("social asset ingestor is unavailable")
            adapter = self._adapter(platform)
            stage = getattr(adapter, "stage_asset", None)
            if not callable(stage):
                raise SocialWorkspaceRuntimeError("social provider asset staging is unavailable")
            self._consume_budget(principal, platform, None, "asset_stage", "rate", 1)
            self._consume_budget(principal, platform, None, "asset_stage", "media", 1)
            owner_binding = self._principal_hash(principal)
            requested_expires_at = self._now() + min(
                self.asset_ttl_seconds, self.reference_ttl_seconds
            )
            byte_limit = (
                self.document_max_bytes
                if request.role is MediaRole.DOCUMENT
                else self.asset_max_bytes
            )
            try:
                if request.source_asset_ref is not None:
                    if request.role is not MediaRole.IMAGE:
                        raise SocialWorkspaceRuntimeError(
                            "provider-read promotion supports images only"
                        )
                    promote = getattr(self.asset_ingestor, "ingest_provider_bytes", None)
                    if not callable(promote):
                        raise SocialWorkspaceRuntimeError(
                            "provider-read asset promotion is unavailable"
                        )
                    client, subject, resource = self._binding(principal)
                    with self.store._lock, self.store._connect() as conn:
                        source = conn.execute(
                            """SELECT platform,provider_ref_ciphertext,expires_at
                               FROM social_workspace_ref WHERE ref_hash=? AND ref_kind='asset'
                               AND client_hash=? AND subject_hash=? AND resource_hash=?
                               AND policy_version=? AND expires_at>?""",
                            (self._hash(request.source_asset_ref), client, subject, resource,
                             self.policy_version, self._now()),
                        ).fetchone()
                    if source is None:
                        raise SocialWorkspaceRuntimeError(
                            "source asset is expired or not bound", error_code="ASSET_EXPIRED"
                        )
                    source_platform = str(source["platform"] )
                    source_ref = self._decrypt(str(source["provider_ref_ciphertext"]))
                    reader = getattr(self._adapter(source_platform), "read_asset", None)
                    if not callable(reader):
                        raise SocialWorkspaceRuntimeError(
                            "provider-read asset materialization is unavailable"
                        )
                    materialized = await asyncio.wait_for(
                        reader(source_ref, owner_binding=owner_binding, max_bytes=byte_limit),
                        timeout=self.provider_timeout_seconds,
                    )
                    raw = materialized if type(materialized) is bytes else getattr(materialized, "content", None)
                    if type(raw) is not bytes or not 1 <= len(raw) <= byte_limit:
                        raise SocialWorkspaceRuntimeError("provider returned an invalid image asset")
                    claimed_length = getattr(materialized, "byte_length", None)
                    claimed_digest = getattr(materialized, "content_digest", None)
                    claimed_mime = getattr(materialized, "mime_type", None)
                    measured_digest = "sha256:" + hashlib.sha256(raw).hexdigest()
                    if claimed_length is not None and claimed_length != len(raw):
                        raise SocialWorkspaceRuntimeError("provider image integrity check failed")
                    if claimed_digest is not None and (
                        not isinstance(claimed_digest, str)
                        or not hmac.compare_digest(claimed_digest, measured_digest)
                    ):
                        raise SocialWorkspaceRuntimeError("provider image integrity check failed")
                    if claimed_mime is not None and (
                        not isinstance(claimed_mime, str) or not claimed_mime.startswith("image/")
                    ):
                        raise SocialWorkspaceRuntimeError("provider image MIME check failed")
                    requested_expires_at = min(requested_expires_at, int(source["expires_at"]))
                    ingested = await asyncio.wait_for(
                        promote(raw, owner_binding=owner_binding, max_bytes=byte_limit,
                            expires_at=requested_expires_at, content_digest=measured_digest,
                            mime_type=claimed_mime),
                        timeout=self.asset_ingest_timeout_seconds,
                    )
                else:
                    assert request.file is not None
                    ingest_parameters = inspect.signature(self.asset_ingestor.ingest).parameters
                    ingest_kwargs: dict[str, Any] = {
                        "owner_binding": owner_binding, "max_bytes": byte_limit,
                        "expires_at": requested_expires_at,
                    }
                    if "role" in ingest_parameters:
                        ingest_kwargs["role"] = (request.role.value
                            if request.role is MediaRole.DOCUMENT else "story_media")
                    elif request.role is MediaRole.DOCUMENT:
                        raise SocialWorkspaceRuntimeError(
                            "document-aware asset ingestor is unavailable"
                        )
                    ingested = await asyncio.wait_for(
                        self.asset_ingestor.ingest(request.file, **ingest_kwargs),
                        timeout=self.asset_ingest_timeout_seconds,
                    )
            except asyncio.TimeoutError:
                raise SocialWorkspaceRuntimeError("social asset ingestion timed out") from None
            verified = self._validate_verified_asset(
                ingested,
                owner_binding=owner_binding,
                requested_expires_at=requested_expires_at,
                role=request.role,
            )
            try:
                provider_ref = await asyncio.wait_for(
                    stage(verified, role=request.role),
                    timeout=self.provider_timeout_seconds,
                )
            except asyncio.TimeoutError:
                raise SocialWorkspaceRuntimeError(
                    "social provider asset staging timed out"
                ) from None
            asset_ref = self._mint_verified_asset_ref(
                provider_ref, platform, principal, verified
            )
            result = {"asset_ref": asset_ref, "status": "ready"}
            self._audit(principal, platform=platform, operation="asset_stage",
                        outcome="succeeded", reason="verified_asset_bound", media_items=1)
            return result
        except Exception as exc:
            audit_reason = getattr(
                exc, "audit_reason_code", type(exc).__name__
            )
            self._audit(principal, platform=platform, operation="asset_stage",
                        outcome="denied", reason=str(audit_reason))
            raise

    async def asset_status(
        self, asset_ref: str, context: ToolCallContext, *, platform: str | None = None
    ) -> dict[str, Any]:
        principal = RuntimePrincipal.from_context(context)
        if platform is None:
            client, subject, resource = self._binding(principal)
            with self.store._lock, self.store._connect() as conn:
                row = conn.execute(
                    """SELECT platform FROM social_workspace_ref WHERE ref_hash=?
                       AND ref_kind='asset' AND client_hash=? AND subject_hash=?
                       AND resource_hash=? AND policy_version=?""",
                    (
                        self._hash(asset_ref),
                        client,
                        subject,
                        resource,
                        self.policy_version,
                    ),
                ).fetchone()
            if row is None:
                raise SocialWorkspaceRuntimeError("asset reference is unknown or not bound")
            platform = str(row["platform"])
        with self.store._lock, self.store._connect() as conn:
            metadata, expired = self._asset_metadata_on_conn(
                conn,
                asset_ref,
                platform,
                self._binding(principal),
                allow_expired=True,
            )
        role = MediaRole(str(metadata["role"]))
        if not self._asset_scope_authorized(platform, role, principal.scopes):
            raise SocialWorkspaceRuntimeError("required social asset scope is missing")
        result = {
            "asset_ref": asset_ref,
            "status": "expired" if expired else "ready",
            "mime_type": metadata["mime_type"],
            "byte_length": metadata["byte_length"],
            "content_digest": metadata["content_digest"],
            "expires_at": _now_rfc3339(int(metadata["expires_at"])),
            "trust": "untrusted_external_data",
        }
        if metadata.get("width") is not None:
            result["width"] = metadata["width"]
            result["height"] = metadata["height"]
        if metadata.get("display_name") is not None:
            result["display_name"] = metadata["display_name"]
            result["classification"] = metadata["classification"]
        self._audit(principal, platform=platform, operation="asset_status",
                    outcome="succeeded", reason=str(result["status"]))
        return result

    @staticmethod
    def _bounded_image_preview(raw: bytes) -> tuple[bytes, int, int]:
        """Decode and re-encode one provider image as a metadata-free thumbnail."""

        try:
            from PIL import Image, ImageOps

            with Image.open(io.BytesIO(raw)) as source:
                if source.format not in {"JPEG", "PNG", "WEBP"}:
                    raise SocialWorkspaceRuntimeError("story asset is not a supported image")
                width, height = source.size
                if not 1 <= width <= 8192 or not 1 <= height <= 8192:
                    raise SocialWorkspaceRuntimeError("story image dimensions are invalid")
                if width * height > 40_000_000:
                    raise SocialWorkspaceRuntimeError("story image pixel budget exceeded")
                source.load()
                image = ImageOps.exif_transpose(source).convert("RGB")
                image.thumbnail((768, 768), Image.Resampling.LANCZOS)
                width, height = image.size
                for quality in (82, 74, 66, 58, 50, 42):
                    output = io.BytesIO()
                    image.save(
                        output,
                        format="JPEG",
                        quality=quality,
                        optimize=True,
                        progressive=True,
                    )
                    preview = output.getvalue()
                    if 0 < len(preview) <= 65536:
                        return preview, width, height
        except SocialWorkspaceRuntimeError:
            raise
        except Exception:  # noqa: BLE001 - provider media is untrusted
            raise SocialWorkspaceRuntimeError("story image preview is invalid") from None
        raise SocialWorkspaceRuntimeError("story image preview exceeds response budget")

    async def asset_preview(
        self,
        platform: str,
        asset_ref: str,
        context: ToolCallContext,
    ) -> ToolExecutionResult:
        """Return a bounded MCP image block for a principal-bound story asset."""

        principal = RuntimePrincipal.from_context(context)
        try:
            if platform not in self.adapters:
                raise SocialWorkspaceRuntimeError("social provider is disabled")
            if not social_scopes_authorized(
                frozenset({f"{platform}:story:read"}), principal.scopes
            ):
                raise SocialWorkspaceRuntimeError("required story read scope is missing")
            if self._ref_platform(asset_ref, "asset", principal) != platform:
                raise SocialWorkspaceRuntimeError("asset provider binding mismatch")
            provider_ref = self._resolve_ref(asset_ref, "asset", platform, principal)
            reader = getattr(self._adapter(platform), "read_asset", None)
            if not callable(reader):
                raise SocialWorkspaceRuntimeError("social asset preview is unavailable")
            self._consume_budget(principal, platform, None, "asset_preview", "rate", 1)
            materialized = await asyncio.wait_for(
                reader(
                    provider_ref,
                    owner_binding=self._principal_hash(principal),
                    max_bytes=self.asset_max_bytes,
                ),
                timeout=self.provider_timeout_seconds,
            )
            if isinstance(materialized, bytes):
                raw = materialized
            else:
                raw = getattr(materialized, "content", None)
            if type(raw) is not bytes or not 1 <= len(raw) <= self.asset_max_bytes:
                raise SocialWorkspaceRuntimeError("provider returned an invalid story asset")
            preview, width, height = await asyncio.to_thread(
                self._bounded_image_preview, raw
            )
            structured = {
                "platform": platform,
                "asset_ref": asset_ref,
                "mime_type": "image/jpeg",
                "byte_length": len(preview),
                "width": width,
                "height": height,
                "trust": "untrusted_external_data",
            }
            encoded = base64.b64encode(preview).decode("ascii")
            self._consume_budget(
                principal, platform, None, "asset_preview", "egress", len(encoded)
            )
            self._audit(
                principal,
                platform=platform,
                operation="asset_preview",
                outcome="succeeded",
                reason="bounded_image_returned",
                response_bytes=len(encoded),
                media_items=1,
            )
            return ToolExecutionResult(
                structured=structured,
                content=(
                    {"type": "image", "data": encoded, "mimeType": "image/jpeg"},
                ),
            )
        except Exception as exc:
            self._audit(
                principal,
                platform=platform,
                operation="asset_preview",
                outcome="denied",
                reason=type(exc).__name__,
            )
            raise


__all__ = [
    "AssetIngestor", "RuntimePrincipal", "SocialBudgetLimits", "SocialWorkspaceAdapter",
    "SocialWorkspaceRuntime", "SocialWorkspaceRuntimeError",
]
