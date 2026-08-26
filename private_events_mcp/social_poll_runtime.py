"""Durable orchestration for native Telegram and VK polls.

The runtime reuses the private Social Workspace OAuth database and opaque target
references. Provider identifiers are encrypted at rest and never cross the MCP
boundary. Every mutation is prepared, digest-bound, idempotent and reconciled.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import secrets
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from .access_policy import social_scopes_authorized
from .social_poll_contract import (
    CompatibilityPolicy,
    PollAction,
    PollActionIntent,
    PollErrorCode,
    PollLifecycle,
    PollValidationError,
    poll_action_digest,
    poll_intent_payload,
    poll_intent_request,
    poll_revision,
    validate_poll_option_ref,
    validate_poll_prepare_request,
    validate_poll_ref,
)
from .social_workspace_runtime import RuntimePrincipal, SocialWorkspaceRuntime
from .tool_catalog import ToolCallContext


_PREP_RE = re.compile(r"^prep_[A-Za-z0-9_-]{24,160}$")
_OP_RE = re.compile(r"^op_[A-Za-z0-9_-]{24,160}$")
_CURSOR_RE = re.compile(r"^[A-Za-z0-9_-]{1,512}$")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now_iso(now: int | None = None) -> str:
    return datetime.fromtimestamp(now or int(time.time()), timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class PollProviderContext:
    base_runtime: SocialWorkspaceRuntime
    principal: RuntimePrincipal
    platform: str
    target_provider_ref: str
    poll_ref: str
    option_refs: Mapping[str, str]
    operation_ref: str

    def resolve_asset(self, public_ref: str) -> str:
        return self.base_runtime._resolve_ref(  # noqa: SLF001 - internal extension boundary
            public_ref,
            "asset",
            self.platform,
            self.principal,
        )


class PollProvider(Protocol):
    platform: str
    transport: str
    principal_type: str

    async def capabilities(
        self, *, target_provider_ref: str | None
    ) -> Mapping[str, Any]: ...

    async def validate_and_preview(
        self,
        intent: PollActionIntent,
        *,
        target_provider_ref: str,
        existing: Mapping[str, Any] | None,
    ) -> Mapping[str, Any]: ...

    async def execute(
        self,
        intent: PollActionIntent,
        *,
        context: PollProviderContext,
        existing: Mapping[str, Any] | None,
        step: Any,
    ) -> Mapping[str, Any]: ...

    async def reconcile(
        self,
        *,
        context: PollProviderContext,
        existing: Mapping[str, Any],
        step: Any,
    ) -> Mapping[str, Any]: ...

    async def get(
        self,
        *,
        context: PollProviderContext,
        existing: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    async def results(
        self,
        *,
        context: PollProviderContext,
        existing: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    async def voters(
        self,
        *,
        context: PollProviderContext,
        existing: Mapping[str, Any],
        option_binding: Any | None,
        cursor: str | None,
        limit: int,
    ) -> Mapping[str, Any]: ...


class PollWorkspaceRuntime:
    """Durable poll subdomain attached to one SocialWorkspaceRuntime."""

    SCHEMA_VERSION = 1

    def __init__(self, base_runtime: SocialWorkspaceRuntime) -> None:
        if not isinstance(base_runtime, SocialWorkspaceRuntime):
            raise TypeError("base_runtime must be SocialWorkspaceRuntime")
        self.base = base_runtime
        self.store = base_runtime.store
        self._providers: dict[str, PollProvider] = {}
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        statements = (
            """CREATE TABLE IF NOT EXISTS social_poll_schema_meta(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                version INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS social_poll(
                poll_hash TEXT PRIMARY KEY,
                poll_ref TEXT NOT NULL UNIQUE,
                client_hash TEXT NOT NULL,
                subject_hash TEXT NOT NULL,
                resource_hash TEXT NOT NULL,
                platform TEXT NOT NULL,
                target_ref_hash TEXT NOT NULL,
                target_ref_ciphertext TEXT NOT NULL,
                item_ref_ciphertext TEXT,
                schedule_ref TEXT,
                specification_ciphertext TEXT NOT NULL,
                provider_binding_ciphertext TEXT,
                provider_state_ciphertext TEXT,
                prepared_payload_digest TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                schedule_mode TEXT,
                revision TEXT NOT NULL,
                scheduled_at TEXT,
                timezone TEXT,
                original_offset TEXT,
                provider_schedule_at TEXT,
                published_at TEXT,
                closed_at TEXT,
                canceled_at TEXT,
                actual_publish_at TEXT,
                publish_drift_seconds INTEGER,
                last_synced_at TEXT,
                result_complete INTEGER,
                result_source TEXT,
                reconciliation_required INTEGER NOT NULL DEFAULT 0,
                orphaned INTEGER NOT NULL DEFAULT 0,
                compensation_pending INTEGER NOT NULL DEFAULT 0,
                error_code TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS ix_social_poll_principal
               ON social_poll(client_hash,subject_hash,resource_hash,platform,updated_at)""",
            """CREATE INDEX IF NOT EXISTS ix_social_poll_lifecycle
               ON social_poll(lifecycle_state,scheduled_at,updated_at)""",
            """CREATE TABLE IF NOT EXISTS social_poll_option(
                poll_hash TEXT NOT NULL,
                option_hash TEXT NOT NULL,
                option_ref TEXT NOT NULL UNIQUE,
                client_key TEXT NOT NULL,
                position INTEGER NOT NULL,
                text_ciphertext TEXT NOT NULL,
                provider_binding_ciphertext TEXT,
                votes INTEGER,
                rate REAL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(poll_hash,option_hash),
                UNIQUE(poll_hash,client_key),
                FOREIGN KEY(poll_hash) REFERENCES social_poll(poll_hash) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS social_poll_preparation(
                preparation_hash TEXT PRIMARY KEY,
                preparation_ref TEXT NOT NULL UNIQUE,
                client_hash TEXT NOT NULL,
                subject_hash TEXT NOT NULL,
                resource_hash TEXT NOT NULL,
                platform TEXT NOT NULL,
                action TEXT NOT NULL,
                poll_hash TEXT NOT NULL,
                idempotency_hash TEXT NOT NULL,
                action_digest TEXT NOT NULL,
                intent_ciphertext TEXT NOT NULL,
                preview_ciphertext TEXT NOT NULL,
                status TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(client_hash,subject_hash,resource_hash,platform,idempotency_hash)
            )""",
            """CREATE INDEX IF NOT EXISTS ix_social_poll_prep_expiry
               ON social_poll_preparation(status,expires_at)""",
            """CREATE TABLE IF NOT EXISTS social_poll_operation(
                operation_hash TEXT PRIMARY KEY,
                operation_ref TEXT NOT NULL UNIQUE,
                preparation_hash TEXT NOT NULL,
                client_hash TEXT NOT NULL,
                subject_hash TEXT NOT NULL,
                resource_hash TEXT NOT NULL,
                platform TEXT NOT NULL,
                action TEXT NOT NULL,
                poll_hash TEXT NOT NULL,
                action_digest TEXT NOT NULL,
                status TEXT NOT NULL,
                provider_attempted INTEGER NOT NULL DEFAULT 0,
                retry_safe INTEGER NOT NULL DEFAULT 0,
                result_ciphertext TEXT,
                error_code TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(preparation_hash)
            )""",
            """CREATE TABLE IF NOT EXISTS social_poll_provider_step(
                operation_hash TEXT NOT NULL,
                step_key TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                state TEXT NOT NULL,
                attempted INTEGER NOT NULL DEFAULT 0,
                binding_ciphertext TEXT,
                error_code TEXT,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(operation_hash,step_key)
            )""",
            """CREATE TABLE IF NOT EXISTS social_poll_result_snapshot(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_hash TEXT NOT NULL,
                snapshot_ciphertext TEXT NOT NULL,
                complete INTEGER NOT NULL,
                source TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS ix_social_poll_snapshot_latest
               ON social_poll_result_snapshot(poll_hash,id DESC)""",
            """CREATE TABLE IF NOT EXISTS social_poll_voter_observation(
                poll_hash TEXT NOT NULL,
                voter_hash TEXT NOT NULL,
                voter_ciphertext TEXT NOT NULL,
                option_refs_ciphertext TEXT NOT NULL,
                active INTEGER NOT NULL,
                source TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(poll_hash,voter_hash)
            )""",
            """CREATE TABLE IF NOT EXISTS social_poll_background(
                background_hash TEXT PRIMARY KEY,
                background_ref TEXT NOT NULL UNIQUE,
                client_hash TEXT NOT NULL,
                subject_hash TEXT NOT NULL,
                resource_hash TEXT NOT NULL,
                platform TEXT NOT NULL,
                provider_binding_ciphertext TEXT NOT NULL,
                preview_ciphertext TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS social_poll_audit(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                principal_hash TEXT NOT NULL,
                platform TEXT,
                operation TEXT NOT NULL,
                poll_hash TEXT,
                option_hash TEXT,
                outcome TEXT NOT NULL,
                reason_code TEXT,
                result_count INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            )""",
        )
        now = int(time.time())
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute("PRAGMA foreign_keys=ON")
            for statement in statements:
                conn.execute(statement)
            row = conn.execute(
                "SELECT version FROM social_poll_schema_meta WHERE singleton=1"
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO social_poll_schema_meta(singleton,version,updated_at) VALUES(1,?,?)",
                    (self.SCHEMA_VERSION, now),
                )
            elif int(row["version"]) != self.SCHEMA_VERSION:
                raise RuntimeError("unsupported social poll schema version")

    def provider(self, platform: str) -> PollProvider:
        cached = self._providers.get(platform)
        if cached is not None:
            return cached
        adapter = self.base.adapters.get(platform)
        if adapter is None:
            raise PollValidationError(
                PollErrorCode.POLL_UNSUPPORTED,
                "poll provider is disabled",
                platform=platform,
                capability_requirement="provider adapter enabled",
            )
        if platform == "telegram":
            from private_events_mcp_telegram_polls import TelegramPollProvider

            provider: PollProvider = TelegramPollProvider(adapter)
        elif platform == "vk":
            from private_events_mcp_vk_polls import VKPollProvider

            provider = VKPollProvider(adapter)
        else:
            raise PollValidationError(PollErrorCode.POLL_UNSUPPORTED, "unsupported poll platform")
        self._providers[platform] = provider
        return provider

    def _principal(self, context: ToolCallContext) -> RuntimePrincipal:
        return RuntimePrincipal.from_context(context)

    def _binding(self, principal: RuntimePrincipal) -> tuple[str, str, str]:
        return self.base._binding(principal)  # noqa: SLF001

    def _encrypt_json(self, value: Any) -> str:
        return self.base._encrypt(_json(value))  # noqa: SLF001

    def _decrypt_json(self, value: str | None) -> Any:
        if not value:
            return None
        return json.loads(self.base._decrypt(value))  # noqa: SLF001

    def _audit(
        self,
        principal: RuntimePrincipal,
        *,
        platform: str | None,
        operation: str,
        poll_hash: str | None,
        outcome: str,
        reason: str | None = None,
        option_hash: str | None = None,
        result_count: int = 0,
    ) -> None:
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute(
                """INSERT INTO social_poll_audit(
                    principal_hash,platform,operation,poll_hash,option_hash,
                    outcome,reason_code,result_count,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    self.base._principal_hash(principal),  # noqa: SLF001
                    platform,
                    operation,
                    poll_hash,
                    option_hash,
                    outcome,
                    (reason or "")[:64] or None,
                    max(0, int(result_count)),
                    int(time.time()),
                ),
            )

    def _authorize(self, principal: RuntimePrincipal, scopes: frozenset[str]) -> None:
        if not social_scopes_authorized(scopes, principal.scopes):
            raise PollValidationError(
                PollErrorCode.POLL_AUTHORIZATION_MISSING,
                "required poll scope is missing",
                capability_requirement=next(iter(scopes), None),
            )

    def _row_for_poll(
        self, poll_ref: str, principal: RuntimePrincipal
    ) -> sqlite3.Row:
        validate_poll_ref(poll_ref)
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            row = conn.execute(
                """SELECT * FROM social_poll WHERE poll_hash=? AND client_hash=?
                   AND subject_hash=? AND resource_hash=?""",
                (self.base._hash(poll_ref), client, subject, resource),  # noqa: SLF001
            ).fetchone()
        if row is None:
            raise PollValidationError(
                PollErrorCode.POLL_REFERENCE_INVALID,
                "poll_ref is unknown or not bound",
                field_path="poll_ref",
            )
        return row

    def _options(self, poll_hash: str) -> list[dict[str, Any]]:
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            rows = conn.execute(
                "SELECT * FROM social_poll_option WHERE poll_hash=? ORDER BY position",
                (poll_hash,),
            ).fetchall()
        return [
            {
                "poll_option_ref": row["option_ref"],
                "client_key": row["client_key"],
                "position": int(row["position"]),
                "text": self.base._decrypt(row["text_ciphertext"]),  # noqa: SLF001
                "provider_binding": self._decrypt_json(row["provider_binding_ciphertext"]),
                "votes": row["votes"],
                "rate": row["rate"],
            }
            for row in rows
        ]

    def _existing(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "poll_ref": row["poll_ref"],
            "platform": row["platform"],
            "target_ref": self.base._decrypt(row["target_ref_ciphertext"]),  # noqa: SLF001
            "item_ref": (
                self.base._decrypt(row["item_ref_ciphertext"])  # noqa: SLF001
                if row["item_ref_ciphertext"]
                else None
            ),
            "schedule_ref": row["schedule_ref"],
            "specification": self._decrypt_json(row["specification_ciphertext"]),
            "provider_binding": self._decrypt_json(row["provider_binding_ciphertext"]),
            "provider_state": self._decrypt_json(row["provider_state_ciphertext"]),
            "lifecycle_state": row["lifecycle_state"],
            "schedule_mode": row["schedule_mode"],
            "revision": row["revision"],
            "scheduled_at": row["scheduled_at"],
            "timezone": row["timezone"],
            "original_offset": row["original_offset"],
            "provider_schedule_at": row["provider_schedule_at"],
            "published_at": row["published_at"],
            "closed_at": row["closed_at"],
            "canceled_at": row["canceled_at"],
            "actual_publish_at": row["actual_publish_at"],
            "publish_drift_seconds": row["publish_drift_seconds"],
            "last_synced_at": row["last_synced_at"],
            "result_complete": (
                bool(row["result_complete"]) if row["result_complete"] is not None else None
            ),
            "result_source": row["result_source"],
            "reconciliation_required": bool(row["reconciliation_required"]),
            "orphaned": bool(row["orphaned"]),
            "compensation_pending": bool(row["compensation_pending"]),
            "error_code": row["error_code"],
            "options": self._options(row["poll_hash"]),
        }

    def _provider_context(
        self,
        row: sqlite3.Row,
        principal: RuntimePrincipal,
        operation_ref: str,
        *,
        option_refs: Mapping[str, str] | None = None,
    ) -> PollProviderContext:
        target_ref = self.base._decrypt(row["target_ref_ciphertext"])  # noqa: SLF001
        provider_target = self.base._resolve_ref(  # noqa: SLF001
            target_ref, "target", row["platform"], principal
        )
        resolved_option_refs = dict(option_refs or {
            item["client_key"]: item["poll_option_ref"]
            for item in self._options(row["poll_hash"])
        })
        return PollProviderContext(
            self.base,
            principal,
            row["platform"],
            provider_target,
            row["poll_ref"],
            resolved_option_refs,
            operation_ref,
        )

    async def capabilities(
        self,
        *,
        platform: str,
        target_ref: str | None,
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = self._principal(context)
        target_provider_ref = None
        target_kind = None
        if target_ref is not None:
            target_provider_ref = self.base._resolve_ref(  # noqa: SLF001
                target_ref, "target", platform, principal
            )
        provider = self.provider(platform)
        try:
            raw = dict(
                await asyncio.wait_for(
                    provider.capabilities(target_provider_ref=target_provider_ref),
                    self.base.provider_timeout_seconds,
                )
            )
        except PollValidationError:
            raise
        except Exception:
            raw = {
                "support": "conditional",
                "authorization": {
                    "status": "unknown",
                    "missing_scopes": [],
                    "missing_permissions": ["provider capability probe failed"],
                },
            }
        create_supported = bool(raw.get("create", {}).get("supported", False))
        result = {
            "support": raw.get("support", "supported" if create_supported else "conditional"),
            "platform": platform,
            "transport": getattr(provider, "transport", "unknown"),
            "principal_type": getattr(provider, "principal_type", "unknown"),
            "provider_api_version": raw.get("provider_api_version"),
            "authorization": raw.get(
                "authorization",
                {"status": "ready", "missing_scopes": [], "missing_permissions": []},
            ),
            "target": {
                "target_ref": target_ref,
                "kind": target_kind,
                "verified": target_ref is not None,
            },
            "create": raw.get("create", {"supported": False, "kinds": []}),
            "publish": raw.get("publish", {"immediate": False}),
            "schedule": raw.get(
                "schedule",
                {"supported": False, "mode": "unsupported", "editable": False, "cancelable": False},
            ),
            "lifecycle": raw.get(
                "lifecycle",
                {"close": False, "delete_container": False, "edit_published": False},
            ),
            "reads": raw.get(
                "reads",
                {
                    "state": False,
                    "results": False,
                    "voters": {"support": "unsupported", "complete_history": False},
                },
            ),
            "fields": raw.get("fields", {}),
            "limits": raw.get("limits", {}),
            "implementation": raw.get(
                "implementation",
                {"adapter": "implemented", "tested": "unit", "live_verified": False},
            ),
        }
        return result

    async def prepare(
        self,
        arguments: Mapping[str, Any],
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = self._principal(context)
        intent = validate_poll_prepare_request(arguments)
        self._authorize(principal, intent.required_scopes)
        platform = intent.platform.value
        provider = self.provider(platform)
        now = int(time.time())
        client, subject, resource = self._binding(principal)
        action_digest = poll_action_digest(intent)
        idem_hash = _hash(intent.idempotency_key)

        existing_row: sqlite3.Row | None = None
        existing: dict[str, Any] | None = None
        if intent.poll_ref:
            existing_row = self._row_for_poll(intent.poll_ref, principal)
            if existing_row["platform"] != platform:
                raise PollValidationError(
                    PollErrorCode.POLL_FIELD_CONFLICT,
                    "poll platform mismatch",
                    field_path="platform",
                    platform=platform,
                )
            existing = self._existing(existing_row)
            if intent.expected_revision and intent.expected_revision != existing["revision"]:
                raise PollValidationError(
                    PollErrorCode.POLL_REVISION_CONFLICT,
                    "poll revision changed",
                    field_path="expected_revision",
                    platform=platform,
                )
            target_ref = existing["target_ref"]
            poll_ref = intent.poll_ref
            poll_hash = existing_row["poll_hash"]
            option_refs = {
                item["client_key"]: item["poll_option_ref"] for item in existing["options"]
            }
            if intent.poll is not None:
                for option in intent.poll.options:
                    option_refs.setdefault(
                        option.client_key, "popt_" + secrets.token_urlsafe(24)
                    )
        else:
            assert intent.target_ref is not None and intent.poll is not None
            target_ref = intent.target_ref
            poll_ref = "pol_" + secrets.token_urlsafe(24)
            poll_hash = self.base._hash(poll_ref)  # noqa: SLF001
            option_refs = {
                option.client_key: "popt_" + secrets.token_urlsafe(24)
                for option in intent.poll.options
            }
        provider_target = self.base._resolve_ref(  # noqa: SLF001
            target_ref, "target", platform, principal
        )
        try:
            plan = dict(
                await asyncio.wait_for(
                    provider.validate_and_preview(
                        intent,
                        target_provider_ref=provider_target,
                        existing=existing,
                    ),
                    self.base.provider_timeout_seconds,
                )
            )
        except PollValidationError:
            raise
        except Exception:
            raise PollValidationError(
                PollErrorCode.POLL_UNSUPPORTED,
                "provider poll capability validation failed",
                platform=platform,
                transport=getattr(provider, "transport", None),
                retryable=True,
                safe_to_retry=True,
            ) from None
        transformations = plan.get("compatibility_transformations", [])
        if transformations and (
            intent.poll is None
            or intent.poll.compatibility_policy is not CompatibilityPolicy.EXPLICIT_BEST_EFFORT
        ):
            raise PollValidationError(
                PollErrorCode.POLL_FIELD_UNSUPPORTED,
                "provider requires explicit_best_effort for listed transformations",
                platform=platform,
                capability_requirement="compatibility_policy=explicit_best_effort",
            )
        schedule_ref = (
            existing.get("schedule_ref")
            if existing and existing.get("schedule_ref")
            else ("sch_" + secrets.token_urlsafe(24) if intent.action is PollAction.SCHEDULE else None)
        )
        preview = {
            "platform": platform,
            "transport": getattr(provider, "transport", "unknown"),
            "principal_type": getattr(provider, "principal_type", "unknown"),
            "target_ref": target_ref,
            "poll_ref": poll_ref,
            "schedule_ref": schedule_ref,
            "action": intent.action.value,
            "question": (
                intent.poll.question.text
                if intent.poll is not None
                else existing.get("specification", {}).get("poll", {}).get("question", {}).get("text")
            ),
            "options": [
                {
                    "poll_option_ref": option_refs[option.client_key],
                    "client_key": option.client_key,
                    "text": option.text.text,
                }
                for option in (intent.poll.options if intent.poll is not None else ())
            ]
            or [
                {
                    "poll_option_ref": item["poll_option_ref"],
                    "client_key": item["client_key"],
                    "text": item["text"],
                }
                for item in (existing or {}).get("options", [])
            ],
            "kind": intent.poll.kind.value if intent.poll is not None else None,
            "anonymous": intent.poll.anonymous if intent.poll is not None else None,
            "multiple_answers": intent.poll.multiple_answers if intent.poll is not None else None,
            "schedule_at": intent.schedule_at,
            "schedule_at_utc": intent.schedule_at_utc,
            "timezone": intent.timezone,
            "schedule_mode": plan.get("schedule_mode"),
            "close": poll_intent_payload(intent).get("poll", {}).get("close") if intent.poll else None,
            "available_after_commit": plan.get("available_after_commit", []),
            "provider_plan": plan.get("safe_preview", {}),
        }
        prep_ref = "prep_" + secrets.token_urlsafe(24)
        expires_at = now + self.base.preparation_ttl_seconds
        intent_payload = poll_intent_request(intent)
        persisted_content = intent_payload.get("content") or {}
        specification = {
            "content": {
                key: persisted_content.get(key)
                for key in ("text", "entities", "media")
                if key in persisted_content
            },
            "poll": persisted_content.get("poll"),
        }
        revision = poll_revision(specification if intent.poll is not None else (existing or {}).get("specification", {}))

        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute("BEGIN IMMEDIATE")
            try:
                duplicate = conn.execute(
                    """SELECT * FROM social_poll_preparation WHERE client_hash=?
                       AND subject_hash=? AND resource_hash=? AND platform=?
                       AND idempotency_hash=?""",
                    (client, subject, resource, platform, idem_hash),
                ).fetchone()
                if duplicate is not None:
                    if duplicate["action_digest"] != action_digest:
                        raise PollValidationError(
                            PollErrorCode.POLL_FIELD_CONFLICT,
                            "idempotency_key is already bound to another poll action",
                            field_path="idempotency_key",
                            platform=platform,
                        )
                    conn.execute("COMMIT")
                    return self._decrypt_json(duplicate["preview_ciphertext"])
                if existing_row is None:
                    conn.execute(
                        """INSERT INTO social_poll(
                            poll_hash,poll_ref,client_hash,subject_hash,resource_hash,
                            platform,target_ref_hash,target_ref_ciphertext,schedule_ref,
                            specification_ciphertext,prepared_payload_digest,lifecycle_state,
                            schedule_mode,revision,scheduled_at,timezone,original_offset,
                            provider_schedule_at,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            poll_hash,
                            poll_ref,
                            client,
                            subject,
                            resource,
                            platform,
                            self.base._hash(target_ref),  # noqa: SLF001
                            self.base._encrypt(target_ref),  # noqa: SLF001
                            schedule_ref,
                            self._encrypt_json(specification),
                            action_digest,
                            PollLifecycle.PREPARED.value,
                            plan.get("schedule_mode"),
                            revision,
                            intent.schedule_at_utc,
                            intent.timezone,
                            intent.original_offset,
                            plan.get("provider_schedule_at"),
                            now,
                            now,
                        ),
                    )
                    for position, option in enumerate(intent.poll.options):
                        option_ref = option_refs[option.client_key]
                        conn.execute(
                            """INSERT INTO social_poll_option(
                                poll_hash,option_hash,option_ref,client_key,position,
                                text_ciphertext,updated_at
                            ) VALUES(?,?,?,?,?,?,?)""",
                            (
                                poll_hash,
                                self.base._hash(option_ref),  # noqa: SLF001
                                option_ref,
                                option.client_key,
                                position,
                                self.base._encrypt(option.text.text),  # noqa: SLF001
                                now,
                            ),
                        )
                response = {
                    "preparation_ref": prep_ref,
                    "action": intent.action.value,
                    "status": "prepared",
                    "action_digest": action_digest,
                    "poll_ref": poll_ref,
                    **({"schedule_ref": schedule_ref} if schedule_ref else {}),
                    "summary": plan.get(
                        "summary",
                        f"{intent.action.value} native {platform} poll",
                    ),
                    "expires_at": _now_iso(expires_at),
                    "required_scopes": sorted(intent.required_scopes),
                    "preview": preview,
                    "compatibility_transformations": transformations,
                }
                conn.execute(
                    """INSERT INTO social_poll_preparation(
                        preparation_hash,preparation_ref,client_hash,subject_hash,
                        resource_hash,platform,action,poll_hash,idempotency_hash,
                        action_digest,intent_ciphertext,preview_ciphertext,status,
                        expires_at,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        self.base._hash(prep_ref),  # noqa: SLF001
                        prep_ref,
                        client,
                        subject,
                        resource,
                        platform,
                        intent.action.value,
                        poll_hash,
                        idem_hash,
                        action_digest,
                        self._encrypt_json(intent_payload),
                        self._encrypt_json(response),
                        "prepared",
                        expires_at,
                        now,
                        now,
                    ),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        self._audit(
            principal,
            platform=platform,
            operation="prepare",
            poll_hash=poll_hash,
            outcome="prepared",
            reason=intent.action.value,
        )
        return response

    def _preparation(
        self, preparation_ref: str, principal: RuntimePrincipal
    ) -> sqlite3.Row:
        if not _PREP_RE.fullmatch(preparation_ref):
            raise PollValidationError(
                PollErrorCode.POLL_REFERENCE_INVALID,
                "preparation_ref is invalid",
                field_path="preparation_ref",
            )
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            row = conn.execute(
                """SELECT * FROM social_poll_preparation WHERE preparation_hash=?
                   AND client_hash=? AND subject_hash=? AND resource_hash=?""",
                (self.base._hash(preparation_ref), client, subject, resource),  # noqa: SLF001
            ).fetchone()
        if row is None:
            raise PollValidationError(
                PollErrorCode.POLL_REFERENCE_INVALID,
                "preparation_ref is unknown or not bound",
                field_path="preparation_ref",
            )
        return row

    def is_poll_preparation(self, preparation_ref: Any) -> bool:
        if not isinstance(preparation_ref, str) or not _PREP_RE.fullmatch(preparation_ref):
            return False
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            return (
                conn.execute(
                    "SELECT 1 FROM social_poll_preparation WHERE preparation_hash=?",
                    (self.base._hash(preparation_ref),),  # noqa: SLF001
                ).fetchone()
                is not None
            )

    def is_poll_operation(self, operation_ref: Any) -> bool:
        if not isinstance(operation_ref, str) or not _OP_RE.fullmatch(operation_ref):
            return False
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            return (
                conn.execute(
                    "SELECT 1 FROM social_poll_operation WHERE operation_hash=?",
                    (self.base._hash(operation_ref),),  # noqa: SLF001
                ).fetchone()
                is not None
            )

    def _step_hook(self, operation_hash: str):
        def step(
            key: str,
            *,
            ordinal: int,
            state: str,
            attempted: bool = False,
            binding: Any = None,
            error_code: str | None = None,
        ) -> None:
            if not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", key):
                raise ValueError("invalid provider step key")
            now = int(time.time())
            with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
                conn.execute(
                    """INSERT INTO social_poll_provider_step(
                        operation_hash,step_key,ordinal,state,attempted,
                        binding_ciphertext,error_code,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(operation_hash,step_key) DO UPDATE SET
                        ordinal=excluded.ordinal,state=excluded.state,
                        attempted=MAX(social_poll_provider_step.attempted,excluded.attempted),
                        binding_ciphertext=COALESCE(excluded.binding_ciphertext,social_poll_provider_step.binding_ciphertext),
                        error_code=excluded.error_code,updated_at=excluded.updated_at""",
                    (
                        operation_hash,
                        key,
                        ordinal,
                        state,
                        int(bool(attempted)),
                        self._encrypt_json(binding) if binding is not None else None,
                        error_code,
                        now,
                    ),
                )
        return step

    async def commit(
        self,
        arguments: Mapping[str, Any],
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = self._principal(context)
        allowed = {"preparation_ref", "action_digest", "approval_ref", "approval_receipt"}
        if set(arguments) - allowed:
            raise PollValidationError(
                PollErrorCode.POLL_FIELD_UNSUPPORTED,
                "unsupported commit field",
            )
        prep_ref = arguments.get("preparation_ref")
        digest = arguments.get("action_digest")
        if not isinstance(prep_ref, str) or not isinstance(digest, str):
            raise PollValidationError(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "preparation_ref and action_digest are required",
            )
        prep = self._preparation(prep_ref, principal)
        if prep["action_digest"] != digest:
            raise PollValidationError(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "action_digest mismatch",
                field_path="action_digest",
            )
        if int(prep["expires_at"]) <= int(time.time()):
            raise PollValidationError(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "preparation expired",
                field_path="preparation_ref",
            )
        intent = validate_poll_prepare_request(self._decrypt_json(prep["intent_ciphertext"]))
        self._authorize(principal, intent.required_scopes)
        operation_ref = "op_" + secrets.token_urlsafe(24)
        operation_hash = self.base._hash(operation_ref)  # noqa: SLF001
        poll_hash = prep["poll_hash"]
        now = int(time.time())
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute("BEGIN IMMEDIATE")
            try:
                replay = conn.execute(
                    "SELECT * FROM social_poll_operation WHERE preparation_hash=?",
                    (prep["preparation_hash"],),
                ).fetchone()
                if replay is not None:
                    conn.execute("COMMIT")
                    if replay["result_ciphertext"]:
                        return self._decrypt_json(replay["result_ciphertext"])
                    return {
                        "platform": replay["platform"],
                        "operation_ref": replay["operation_ref"],
                        "preparation_ref": prep_ref,
                        "action": replay["action"],
                        "status": PollLifecycle.UNKNOWN.value,
                        "poll_ref": self._poll_ref_by_hash(poll_hash),
                        "lifecycle_state": PollLifecycle.UNKNOWN.value,
                        "retry_safe": False,
                        "error_code": PollErrorCode.PROVIDER_RECONCILIATION_REQUIRED.value,
                    }
                conn.execute(
                    """INSERT INTO social_poll_operation(
                        operation_hash,operation_ref,preparation_hash,client_hash,
                        subject_hash,resource_hash,platform,action,poll_hash,
                        action_digest,status,provider_attempted,retry_safe,
                        created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        operation_hash,
                        operation_ref,
                        prep["preparation_hash"],
                        client,
                        subject,
                        resource,
                        prep["platform"],
                        prep["action"],
                        poll_hash,
                        digest,
                        PollLifecycle.COMMITTED.value,
                        0,
                        0,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    "UPDATE social_poll_preparation SET status='committed',updated_at=? WHERE preparation_hash=?",
                    (now, prep["preparation_hash"]),
                )
                conn.execute(
                    "UPDATE social_poll SET lifecycle_state=?,updated_at=? WHERE poll_hash=?",
                    (PollLifecycle.COMMITTED.value, now, poll_hash),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        row = self._row_by_hash(poll_hash, principal)
        existing = self._existing(row)
        provider = self.provider(row["platform"])
        prepared_response = self._decrypt_json(prep["preview_ciphertext"])
        prepared_options = prepared_response.get("preview", {}).get("options", [])
        prepared_option_refs = {
            str(item["client_key"]): str(item["poll_option_ref"])
            for item in prepared_options
            if isinstance(item, Mapping)
            and isinstance(item.get("client_key"), str)
            and isinstance(item.get("poll_option_ref"), str)
        }
        pctx = self._provider_context(
            row,
            principal,
            operation_ref,
            option_refs=prepared_option_refs or None,
        )
        step = self._step_hook(operation_hash)
        attempted = False
        try:
            with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
                conn.execute(
                    "UPDATE social_poll_operation SET provider_attempted=1,status=?,updated_at=? WHERE operation_hash=?",
                    (PollLifecycle.DISPATCHING.value, int(time.time()), operation_hash),
                )
            attempted = True
            raw = dict(
                await asyncio.wait_for(
                    provider.execute(
                        intent,
                        context=pctx,
                        existing=existing,
                        step=step,
                    ),
                    self.base.provider_timeout_seconds,
                )
            )
            result = self._apply_provider_result(
                row,
                principal,
                operation_ref,
                prep_ref,
                intent,
                raw,
                option_refs=pctx.option_refs,
            )
            self._finish_operation(operation_hash, result)
            self._audit(
                principal,
                platform=row["platform"],
                operation="commit",
                poll_hash=poll_hash,
                outcome=result["status"],
                reason=intent.action.value,
            )
            return result
        except asyncio.TimeoutError:
            result = self._mark_unknown(
                row,
                operation_ref,
                prep_ref,
                intent,
                PollErrorCode.PROVIDER_OUTCOME_UNKNOWN.value,
            )
            self._finish_operation(operation_hash, result)
            return result
        except PollValidationError as exc:
            if attempted and not exc.safe_to_retry:
                result = self._mark_unknown(
                    row,
                    operation_ref,
                    prep_ref,
                    intent,
                    exc.error_code,
                )
                self._finish_operation(operation_hash, result)
                return result
            result = self._mark_failed(row, operation_ref, prep_ref, intent, exc.error_code)
            self._finish_operation(operation_hash, result)
            raise
        except Exception:
            if attempted:
                result = self._mark_unknown(
                    row,
                    operation_ref,
                    prep_ref,
                    intent,
                    PollErrorCode.PROVIDER_RECONCILIATION_REQUIRED.value,
                )
                self._finish_operation(operation_hash, result)
                return result
            result = self._mark_failed(
                row,
                operation_ref,
                prep_ref,
                intent,
                PollErrorCode.POLL_UNSUPPORTED.value,
            )
            self._finish_operation(operation_hash, result)
            raise PollValidationError(
                PollErrorCode.POLL_UNSUPPORTED,
                "poll provider execution failed before mutation",
                platform=row["platform"],
                retryable=True,
                safe_to_retry=True,
            ) from None

    def _poll_ref_by_hash(self, poll_hash: str) -> str:
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            row = conn.execute(
                "SELECT poll_ref FROM social_poll WHERE poll_hash=?", (poll_hash,)
            ).fetchone()
        if row is None:
            raise PollValidationError(PollErrorCode.POLL_REFERENCE_INVALID, "poll is missing")
        return row["poll_ref"]

    def _row_by_hash(self, poll_hash: str, principal: RuntimePrincipal) -> sqlite3.Row:
        return self._row_for_poll(self._poll_ref_by_hash(poll_hash), principal)

    def _apply_provider_result(
        self,
        row: sqlite3.Row,
        principal: RuntimePrincipal,
        operation_ref: str,
        prep_ref: str,
        intent: PollActionIntent,
        raw: Mapping[str, Any],
        *,
        option_refs: Mapping[str, str],
    ) -> dict[str, Any]:
        lifecycle = str(raw.get("lifecycle_state") or raw.get("status") or PollLifecycle.UNKNOWN.value)
        if lifecycle not in {state.value for state in PollLifecycle}:
            lifecycle = PollLifecycle.UNKNOWN.value
        provider_item_ref = raw.get("provider_item_ref")
        item_ref = self._existing(row).get("item_ref")
        if provider_item_ref is not None:
            item_ref = self.base._mint_ref(  # noqa: SLF001
                "item", provider_item_ref, row["platform"], principal
            )
        provider_binding = raw.get("provider_binding")
        provider_state = raw.get("provider_state")
        option_bindings = raw.get("provider_option_bindings")
        proposed_specification: Mapping[str, Any] | None = None
        if intent.action is PollAction.EDIT and intent.poll is not None:
            persisted = poll_intent_request(intent).get("content") or {}
            proposed_specification = {
                "content": {
                    key: persisted.get(key)
                    for key in ("text", "entities", "media")
                    if key in persisted
                },
                "poll": persisted.get("poll"),
            }
        revision = str(
            raw.get("revision")
            or (
                poll_revision(proposed_specification)
                if proposed_specification is not None
                else row["revision"]
            )
        )
        now = int(time.time())
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """UPDATE social_poll SET
                        item_ref_ciphertext=COALESCE(?,item_ref_ciphertext),
                        specification_ciphertext=COALESCE(?,specification_ciphertext),
                        prepared_payload_digest=?,
                        provider_binding_ciphertext=COALESCE(?,provider_binding_ciphertext),
                        provider_state_ciphertext=COALESCE(?,provider_state_ciphertext),
                        lifecycle_state=?,schedule_mode=COALESCE(?,schedule_mode),
                        revision=?,scheduled_at=COALESCE(?,scheduled_at),
                        timezone=COALESCE(?,timezone),original_offset=COALESCE(?,original_offset),
                        provider_schedule_at=COALESCE(?,provider_schedule_at),
                        published_at=COALESCE(?,published_at),closed_at=COALESCE(?,closed_at),
                        canceled_at=COALESCE(?,canceled_at),actual_publish_at=COALESCE(?,actual_publish_at),
                        publish_drift_seconds=COALESCE(?,publish_drift_seconds),
                        last_synced_at=?,reconciliation_required=?,orphaned=?,
                        compensation_pending=?,error_code=?,updated_at=?
                       WHERE poll_hash=?""",
                    (
                        self.base._encrypt(item_ref) if item_ref else None,  # noqa: SLF001
                        (
                            self._encrypt_json(proposed_specification)
                            if proposed_specification is not None
                            else None
                        ),
                        poll_action_digest(intent),
                        self._encrypt_json(provider_binding) if provider_binding is not None else None,
                        self._encrypt_json(provider_state) if provider_state is not None else None,
                        lifecycle,
                        raw.get("schedule_mode"),
                        revision,
                        intent.schedule_at_utc,
                        intent.timezone,
                        intent.original_offset,
                        raw.get("provider_schedule_at"),
                        raw.get("published_at"),
                        raw.get("closed_at"),
                        raw.get("canceled_at"),
                        raw.get("actual_publish_at"),
                        raw.get("publish_drift_seconds"),
                        raw.get("last_synced_at") or _now_iso(now),
                        int(bool(raw.get("reconciliation_required", lifecycle == PollLifecycle.UNKNOWN.value))),
                        int(bool(raw.get("orphaned", lifecycle == PollLifecycle.ORPHANED.value))),
                        int(bool(raw.get("compensation_pending", lifecycle == PollLifecycle.COMPENSATION_PENDING.value))),
                        raw.get("error_code"),
                        now,
                        row["poll_hash"],
                    ),
                )
                if proposed_specification is not None and intent.poll is not None:
                    desired_keys = {option.client_key for option in intent.poll.options}
                    for position, option in enumerate(intent.poll.options):
                        option_ref = option_refs.get(option.client_key)
                        if option_ref is None:
                            raise PollValidationError(
                                PollErrorCode.POLL_OPTION_NOT_FOUND,
                                "prepared option reference is missing",
                                field_path=f"content.poll.options[{position}].client_key",
                                platform=row["platform"],
                            )
                        conn.execute(
                            """INSERT INTO social_poll_option(
                                poll_hash,option_hash,option_ref,client_key,position,
                                text_ciphertext,updated_at
                            ) VALUES(?,?,?,?,?,?,?)
                            ON CONFLICT(poll_hash,client_key) DO UPDATE SET
                                position=excluded.position,
                                text_ciphertext=excluded.text_ciphertext,
                                updated_at=excluded.updated_at""",
                            (
                                row["poll_hash"],
                                self.base._hash(option_ref),  # noqa: SLF001
                                option_ref,
                                option.client_key,
                                position,
                                self.base._encrypt(option.text.text),  # noqa: SLF001
                                now,
                            ),
                        )
                    existing_keys = {
                        str(item[0])
                        for item in conn.execute(
                            "SELECT client_key FROM social_poll_option WHERE poll_hash=?",
                            (row["poll_hash"],),
                        ).fetchall()
                    }
                    for removed_key in existing_keys - desired_keys:
                        conn.execute(
                            "DELETE FROM social_poll_option WHERE poll_hash=? AND client_key=?",
                            (row["poll_hash"], removed_key),
                        )
                if isinstance(option_bindings, Mapping):
                    for client_key, binding in option_bindings.items():
                        conn.execute(
                            """UPDATE social_poll_option SET provider_binding_ciphertext=?,updated_at=?
                               WHERE poll_hash=? AND client_key=?""",
                            (self._encrypt_json(binding), now, row["poll_hash"], str(client_key)),
                        )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        result = {
            "platform": row["platform"],
            "operation_ref": operation_ref,
            "preparation_ref": prep_ref,
            "action": intent.action.value,
            "status": lifecycle,
            "poll_ref": row["poll_ref"],
            "lifecycle_state": lifecycle,
            "retry_safe": False,
            "revision": revision,
            **({"item_ref": item_ref} if item_ref else {}),
            **({"schedule_ref": row["schedule_ref"]} if row["schedule_ref"] else {}),
            **({"error_code": raw.get("error_code")} if raw.get("error_code") else {}),
            "reconciliation_required": bool(
                raw.get("reconciliation_required", lifecycle == PollLifecycle.UNKNOWN.value)
            ),
        }
        return result

    def _mark_unknown(
        self,
        row: sqlite3.Row,
        operation_ref: str,
        prep_ref: str,
        intent: PollActionIntent,
        error_code: str,
    ) -> dict[str, Any]:
        now = int(time.time())
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute(
                """UPDATE social_poll SET lifecycle_state=?,reconciliation_required=1,
                   error_code=?,updated_at=? WHERE poll_hash=?""",
                (PollLifecycle.UNKNOWN.value, error_code, now, row["poll_hash"]),
            )
        return {
            "platform": row["platform"],
            "operation_ref": operation_ref,
            "preparation_ref": prep_ref,
            "action": intent.action.value,
            "status": PollLifecycle.UNKNOWN.value,
            "poll_ref": row["poll_ref"],
            "lifecycle_state": PollLifecycle.UNKNOWN.value,
            "retry_safe": False,
            "error_code": error_code,
            "reconciliation_required": True,
        }

    def _mark_failed(
        self,
        row: sqlite3.Row,
        operation_ref: str,
        prep_ref: str,
        intent: PollActionIntent,
        error_code: str,
    ) -> dict[str, Any]:
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute(
                "UPDATE social_poll SET lifecycle_state=?,error_code=?,updated_at=? WHERE poll_hash=?",
                (PollLifecycle.FAILED.value, error_code, int(time.time()), row["poll_hash"]),
            )
        return {
            "platform": row["platform"],
            "operation_ref": operation_ref,
            "preparation_ref": prep_ref,
            "action": intent.action.value,
            "status": PollLifecycle.FAILED.value,
            "poll_ref": row["poll_ref"],
            "lifecycle_state": PollLifecycle.FAILED.value,
            "retry_safe": False,
            "error_code": error_code,
            "reconciliation_required": False,
        }

    def _finish_operation(self, operation_hash: str, result: Mapping[str, Any]) -> None:
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute(
                """UPDATE social_poll_operation SET status=?,retry_safe=?,
                   result_ciphertext=?,error_code=?,updated_at=? WHERE operation_hash=?""",
                (
                    result["status"],
                    int(bool(result.get("retry_safe"))),
                    self._encrypt_json(dict(result)),
                    result.get("error_code"),
                    int(time.time()),
                    operation_hash,
                ),
            )

    async def status(
        self,
        arguments: Mapping[str, Any],
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = self._principal(context)
        prep_ref = arguments.get("preparation_ref")
        operation_ref = arguments.get("operation_ref")
        if (prep_ref is None) == (operation_ref is None):
            raise PollValidationError(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "exactly one status reference is required",
            )
        client, subject, resource = self._binding(principal)
        if prep_ref is not None:
            prep = self._preparation(str(prep_ref), principal)
            response = self._decrypt_json(prep["preview_ciphertext"])
            response["status"] = prep["status"]
            return response
        if not isinstance(operation_ref, str) or not _OP_RE.fullmatch(operation_ref):
            raise PollValidationError(
                PollErrorCode.POLL_REFERENCE_INVALID,
                "operation_ref is invalid",
                field_path="operation_ref",
            )
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            operation = conn.execute(
                """SELECT * FROM social_poll_operation WHERE operation_hash=?
                   AND client_hash=? AND subject_hash=? AND resource_hash=?""",
                (self.base._hash(operation_ref), client, subject, resource),  # noqa: SLF001
            ).fetchone()
        if operation is None:
            raise PollValidationError(
                PollErrorCode.POLL_REFERENCE_INVALID,
                "operation is unknown or not bound",
            )
        if operation["result_ciphertext"]:
            result = self._decrypt_json(operation["result_ciphertext"])
            if result.get("reconciliation_required"):
                return await self.reconcile(operation_ref, context)
            return result
        return await self.reconcile(operation_ref, context)

    async def reconcile(
        self,
        operation_ref: str,
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = self._principal(context)
        client, subject, resource = self._binding(principal)
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            operation = conn.execute(
                """SELECT * FROM social_poll_operation WHERE operation_hash=?
                   AND client_hash=? AND subject_hash=? AND resource_hash=?""",
                (self.base._hash(operation_ref), client, subject, resource),  # noqa: SLF001
            ).fetchone()
            prep = (
                conn.execute(
                    "SELECT * FROM social_poll_preparation WHERE preparation_hash=?",
                    (operation["preparation_hash"],),
                ).fetchone()
                if operation is not None
                else None
            )
        if operation is None or prep is None:
            raise PollValidationError(PollErrorCode.POLL_REFERENCE_INVALID, "operation is unknown")
        intent = validate_poll_prepare_request(self._decrypt_json(prep["intent_ciphertext"]))
        self._authorize(principal, intent.required_scopes)
        row = self._row_by_hash(operation["poll_hash"], principal)
        existing = self._existing(row)
        provider = self.provider(row["platform"])
        prepared_response = self._decrypt_json(prep["preview_ciphertext"])
        prepared_options = prepared_response.get("preview", {}).get("options", [])
        prepared_option_refs = {
            str(item["client_key"]): str(item["poll_option_ref"])
            for item in prepared_options
            if isinstance(item, Mapping)
            and isinstance(item.get("client_key"), str)
            and isinstance(item.get("poll_option_ref"), str)
        }
        pctx = self._provider_context(
            row,
            principal,
            operation_ref,
            option_refs=prepared_option_refs or None,
        )
        step = self._step_hook(operation["operation_hash"])
        try:
            raw = dict(
                await asyncio.wait_for(
                    provider.reconcile(context=pctx, existing=existing, step=step),
                    self.base.provider_timeout_seconds,
                )
            )
        except Exception:
            result = self._mark_unknown(
                row,
                operation_ref,
                prep["preparation_ref"],
                intent,
                PollErrorCode.PROVIDER_RECONCILIATION_REQUIRED.value,
            )
            self._finish_operation(operation["operation_hash"], result)
            return result
        result = self._apply_provider_result(
            row,
            principal,
            operation_ref,
            prep["preparation_ref"],
            intent,
            raw,
            option_refs=pctx.option_refs,
        )
        self._finish_operation(operation["operation_hash"], result)
        return result

    def _public_poll(
        self,
        row: sqlite3.Row,
        *,
        provider_state: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        existing = self._existing(row)
        specification = existing["specification"] or {}
        poll = specification.get("poll") or {}
        result = {
            "poll_ref": row["poll_ref"],
            "item_ref": existing["item_ref"],
            "target_ref": existing["target_ref"],
            "platform": row["platform"],
            "transport": getattr(self.provider(row["platform"]), "transport", "unknown"),
            "question": poll.get("question"),
            "options": [
                {
                    "poll_option_ref": item["poll_option_ref"],
                    "client_key": item["client_key"],
                    "text": item["text"],
                }
                for item in existing["options"]
            ],
            "kind": poll.get("kind"),
            "anonymous": poll.get("anonymous"),
            "multiple_answers": poll.get("multiple_answers"),
            "schedule_ref": existing["schedule_ref"],
            "schedule_mode": existing["schedule_mode"],
            "scheduled_at": existing["scheduled_at"],
            "provider_schedule_at": existing["provider_schedule_at"],
            "published_at": existing["published_at"],
            "actual_publish_at": existing["actual_publish_at"],
            "publish_drift_seconds": existing["publish_drift_seconds"],
            "closed_at": existing["closed_at"],
            "lifecycle_state": existing["lifecycle_state"],
            "available_actions": self._available_actions(existing),
            "revision": existing["revision"],
            "last_synchronized_at": existing["last_synced_at"],
            "staleness": "fresh" if existing["last_synced_at"] else "unknown",
            "completeness": {
                "complete": existing["result_complete"],
                "source": existing["result_source"],
            },
            "provider_metadata": dict(provider_state or existing.get("provider_state") or {}),
            "reconciliation_required": existing["reconciliation_required"],
            "orphaned": existing["orphaned"],
        }
        return result

    @staticmethod
    def _available_actions(existing: Mapping[str, Any]) -> list[str]:
        state = existing["lifecycle_state"]
        actions: list[str] = []
        if state in {
            PollLifecycle.PREPARED.value,
            PollLifecycle.QUEUED.value,
            PollLifecycle.PROVIDER_SCHEDULED.value,
        }:
            actions.extend(
                [
                    PollAction.EDIT.value,
                    PollAction.RESCHEDULE.value,
                    PollAction.CANCEL.value,
                ]
            )
        if state in {PollLifecycle.PUBLISHED.value, PollLifecycle.OPEN.value}:
            actions.extend([PollAction.CLOSE.value, PollAction.DELETE_CONTAINER.value])
        if state == PollLifecycle.CLOSED.value:
            actions.append(PollAction.DELETE_CONTAINER.value)
        return actions

    async def get(
        self,
        arguments: Mapping[str, Any],
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = self._principal(context)
        poll_ref = validate_poll_ref(arguments.get("poll_ref"))
        row = self._row_for_poll(poll_ref, principal)
        self._authorize(principal, frozenset({f"{row['platform']}:read:public"}))
        refresh = arguments.get("refresh", True)
        if type(refresh) is not bool:
            raise PollValidationError(PollErrorCode.POLL_FIELD_CONFLICT, "refresh must be boolean")
        provider_state: Mapping[str, Any] | None = None
        if refresh and row["provider_binding_ciphertext"]:
            provider = self.provider(row["platform"])
            pctx = self._provider_context(row, principal, "op_read_" + secrets.token_urlsafe(18))
            try:
                provider_state = dict(
                    await asyncio.wait_for(
                        provider.get(context=pctx, existing=self._existing(row)),
                        self.base.provider_timeout_seconds,
                    )
                )
                self._update_read_state(row, provider_state)
                row = self._row_for_poll(poll_ref, principal)
            except Exception:
                provider_state = {"refresh_failed": True, "unavailable_reason": "provider_read_failed"}
        result = self._public_poll(row, provider_state=provider_state)
        self._audit(
            principal,
            platform=row["platform"],
            operation="poll_get",
            poll_hash=row["poll_hash"],
            outcome="succeeded",
        )
        return result

    def _update_read_state(self, row: sqlite3.Row, state: Mapping[str, Any]) -> None:
        lifecycle = state.get("lifecycle_state")
        if lifecycle not in {value.value for value in PollLifecycle}:
            lifecycle = row["lifecycle_state"]
        now_iso = state.get("observed_at") or _now_iso()
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute(
                """UPDATE social_poll SET provider_state_ciphertext=?,lifecycle_state=?,
                   last_synced_at=?,published_at=COALESCE(?,published_at),
                   actual_publish_at=COALESCE(?,actual_publish_at),
                   closed_at=COALESCE(?,closed_at),updated_at=? WHERE poll_hash=?""",
                (
                    self._encrypt_json(dict(state)),
                    lifecycle,
                    now_iso,
                    state.get("published_at"),
                    state.get("actual_publish_at"),
                    state.get("closed_at"),
                    int(time.time()),
                    row["poll_hash"],
                ),
            )

    async def results(
        self,
        arguments: Mapping[str, Any],
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = self._principal(context)
        poll_ref = validate_poll_ref(arguments.get("poll_ref"))
        row = self._row_for_poll(poll_ref, principal)
        self._authorize(principal, frozenset({f"{row['platform']}:analytics"}))
        provider = self.provider(row["platform"])
        pctx = self._provider_context(row, principal, "op_results_" + secrets.token_urlsafe(18))
        try:
            raw = dict(
                await asyncio.wait_for(
                    provider.results(context=pctx, existing=self._existing(row)),
                    self.base.provider_timeout_seconds,
                )
            )
        except Exception:
            snapshot = self._latest_snapshot(row["poll_hash"])
            if snapshot is None:
                return {
                    "poll_ref": poll_ref,
                    "state": row["lifecycle_state"],
                    "total_voters": None,
                    "options": [],
                    "complete": False,
                    "source": "unavailable",
                    "observed_at": None,
                    "unavailable_reason": "provider_results_unavailable",
                }
            raw = snapshot
            raw["complete"] = False
            raw["source"] = "stale_snapshot"
            raw["unavailable_reason"] = "provider_results_unavailable"
        normalized = self._normalize_results(row, raw)
        self._save_snapshot(row, normalized)
        self._audit(
            principal,
            platform=row["platform"],
            operation="poll_results",
            poll_hash=row["poll_hash"],
            outcome="succeeded",
            result_count=len(normalized["options"]),
        )
        return normalized

    def _normalize_results(
        self, row: sqlite3.Row, raw: Mapping[str, Any]
    ) -> dict[str, Any]:
        options_by_key = {item["client_key"]: item for item in self._options(row["poll_hash"])}
        normalized: list[dict[str, Any]] = []
        raw_options = raw.get("options")
        if isinstance(raw_options, list):
            for item in raw_options:
                if not isinstance(item, Mapping):
                    continue
                client_key = item.get("client_key")
                mapped = options_by_key.get(str(client_key)) if client_key is not None else None
                if mapped is None:
                    continue
                votes = item.get("votes") if type(item.get("votes")) is int else None
                rate = item.get("rate") if isinstance(item.get("rate"), (int, float)) else None
                normalized.append(
                    {
                        "poll_option_ref": mapped["poll_option_ref"],
                        "client_key": mapped["client_key"],
                        "text": mapped["text"],
                        "votes": votes,
                        "rate": float(rate) if rate is not None else None,
                    }
                )
        return {
            "poll_ref": row["poll_ref"],
            "state": raw.get("state") or raw.get("lifecycle_state") or row["lifecycle_state"],
            "total_voters": raw.get("total_voters") if type(raw.get("total_voters")) is int else None,
            "options": normalized,
            "complete": bool(raw.get("complete", False)),
            "source": str(raw.get("source") or "provider"),
            "observed_at": raw.get("observed_at") or _now_iso(),
            **(
                {"unavailable_reason": raw.get("unavailable_reason")}
                if raw.get("unavailable_reason")
                else {}
            ),
        }

    def _save_snapshot(self, row: sqlite3.Row, normalized: Mapping[str, Any]) -> None:
        now = int(time.time())
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            conn.execute(
                """INSERT INTO social_poll_result_snapshot(
                    poll_hash,snapshot_ciphertext,complete,source,observed_at,created_at
                ) VALUES(?,?,?,?,?,?)""",
                (
                    row["poll_hash"],
                    self._encrypt_json(dict(normalized)),
                    int(bool(normalized.get("complete"))),
                    normalized.get("source") or "provider",
                    normalized.get("observed_at") or _now_iso(now),
                    now,
                ),
            )
            conn.execute(
                """UPDATE social_poll SET result_complete=?,result_source=?,
                   last_synced_at=?,updated_at=? WHERE poll_hash=?""",
                (
                    int(bool(normalized.get("complete"))),
                    normalized.get("source") or "provider",
                    normalized.get("observed_at") or _now_iso(now),
                    now,
                    row["poll_hash"],
                ),
            )

    def _latest_snapshot(self, poll_hash: str) -> dict[str, Any] | None:
        with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
            row = conn.execute(
                """SELECT snapshot_ciphertext FROM social_poll_result_snapshot
                   WHERE poll_hash=? ORDER BY id DESC LIMIT 1""",
                (poll_hash,),
            ).fetchone()
        return self._decrypt_json(row["snapshot_ciphertext"]) if row else None

    async def voters(
        self,
        arguments: Mapping[str, Any],
        context: ToolCallContext,
    ) -> dict[str, Any]:
        principal = self._principal(context)
        poll_ref = validate_poll_ref(arguments.get("poll_ref"))
        row = self._row_for_poll(poll_ref, principal)
        self._authorize(principal, frozenset({f"{row['platform']}:audience"}))
        existing = self._existing(row)
        poll_spec = (existing.get("specification") or {}).get("poll") or {}
        if bool(poll_spec.get("anonymous", True)):
            self._audit(
                principal,
                platform=row["platform"],
                operation="poll_voters",
                poll_hash=row["poll_hash"],
                outcome="denied",
                reason=PollErrorCode.POLL_RESULTS_PRIVATE.value,
            )
            raise PollValidationError(
                PollErrorCode.POLL_RESULTS_PRIVATE,
                "voter identities are unavailable for anonymous polls",
                platform=row["platform"],
            )
        limit = arguments.get("limit", 50)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise PollValidationError(
                PollErrorCode.POLL_LIMIT_EXCEEDED,
                "voter limit must be between 1 and 100",
                field_path="limit",
            )
        cursor = arguments.get("cursor")
        if cursor is not None and (
            not isinstance(cursor, str) or not _CURSOR_RE.fullmatch(cursor)
        ):
            raise PollValidationError(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "voter cursor is invalid",
                field_path="cursor",
            )
        option_binding = None
        option_hash = None
        option_ref = arguments.get("poll_option_ref")
        if option_ref is not None:
            validate_poll_option_ref(option_ref)
            option_hash = self.base._hash(option_ref)  # noqa: SLF001
            with self.store._lock, self.store._connect() as conn:  # noqa: SLF001
                option_row = conn.execute(
                    """SELECT * FROM social_poll_option WHERE poll_hash=? AND option_hash=?""",
                    (row["poll_hash"], option_hash),
                ).fetchone()
            if option_row is None:
                raise PollValidationError(
                    PollErrorCode.POLL_OPTION_NOT_FOUND,
                    "poll_option_ref does not belong to poll_ref",
                    field_path="poll_option_ref",
                )
            option_binding = self._decrypt_json(option_row["provider_binding_ciphertext"])
        provider = self.provider(row["platform"])
        pctx = self._provider_context(row, principal, "op_voters_" + secrets.token_urlsafe(18))
        try:
            raw = dict(
                await asyncio.wait_for(
                    provider.voters(
                        context=pctx,
                        existing=existing,
                        option_binding=option_binding,
                        cursor=cursor,
                        limit=limit,
                    ),
                    self.base.provider_timeout_seconds,
                )
            )
        except PollValidationError:
            raise
        except Exception:
            raise PollValidationError(
                PollErrorCode.POLL_VOTERS_UNAVAILABLE,
                "provider voter list is unavailable",
                platform=row["platform"],
                retryable=True,
                safe_to_retry=True,
            ) from None
        voters = raw.get("voters")
        safe_voters = []
        if isinstance(voters, list):
            for voter in voters[:limit]:
                if not isinstance(voter, Mapping):
                    continue
                public = {
                    key: voter.get(key)
                    for key in ("voter_ref", "display_name", "profile_link", "option_refs", "voted_at")
                    if voter.get(key) is not None
                }
                safe_voters.append(public)
        result = {
            "poll_ref": poll_ref,
            **({"poll_option_ref": option_ref} if option_ref else {}),
            "voters": safe_voters,
            "complete": bool(raw.get("complete", False)),
            "source": str(raw.get("source") or "provider"),
            "privacy": {
                "anonymous": False,
                "identity_access": "provider_authorized",
                "bounded": True,
                "server_page_limit": 100,
            },
            "observed_at": raw.get("observed_at") or _now_iso(),
            **({"next_cursor": raw["next_cursor"]} if raw.get("next_cursor") else {}),
            **(
                {"unavailable_reason": raw.get("unavailable_reason")}
                if raw.get("unavailable_reason")
                else {}
            ),
        }
        self._audit(
            principal,
            platform=row["platform"],
            operation="poll_voters",
            poll_hash=row["poll_hash"],
            option_hash=option_hash,
            outcome="succeeded",
            result_count=len(safe_voters),
        )
        return result


__all__ = ["PollProvider", "PollProviderContext", "PollWorkspaceRuntime"]
