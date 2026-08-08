from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .crypto import secret_hash, verify_pkce


class OAuthStoreError(ValueError):
    pass


class SocialTicketError(ValueError):
    pass


class SocialPublishBudgetError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class AuthorizationGrant:
    subject: str
    client_id: str
    redirect_uri: str
    resource: str
    scopes: frozenset[str]


@dataclass(frozen=True, slots=True)
class RefreshGrant:
    subject: str
    client_id: str
    resource: str
    scopes: frozenset[str]


@dataclass(frozen=True, slots=True)
class PreparedPublication:
    client_id: str
    subject: str
    resource: str
    platform: str
    target_alias: str
    text_hash: str
    idempotency_hash: str


class OAuthStateStore:
    """Small durable OAuth state store, isolated from the production event DB."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()
        os.chmod(self.path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=1.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=1000")
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS oauth_authorization_code (
                    code_hash TEXT PRIMARY KEY,
                    subject TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    redirect_uri TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    scopes TEXT NOT NULL,
                    code_challenge TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    used_at INTEGER,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_oauth_code_expiry
                    ON oauth_authorization_code(expires_at);

                CREATE TABLE IF NOT EXISTS oauth_refresh_token (
                    token_hash TEXT PRIMARY KEY,
                    subject TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    scopes TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    revoked_at INTEGER,
                    rotated_to_hash TEXT,
                    created_at INTEGER NOT NULL,
                    last_used_at INTEGER
                );
                CREATE INDEX IF NOT EXISTS ix_oauth_refresh_expiry
                    ON oauth_refresh_token(expires_at);

                CREATE TABLE IF NOT EXISTS oauth_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    client_fingerprint TEXT,
                    subject TEXT,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_oauth_audit_created_at
                    ON oauth_audit(created_at);

                CREATE TABLE IF NOT EXISTS social_preparation_ticket (
                    ticket_hash TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    target_alias TEXT NOT NULL,
                    text_hash TEXT NOT NULL,
                    idempotency_hash TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    consumed_at INTEGER,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_social_ticket_expiry
                    ON social_preparation_ticket(expires_at);
                CREATE UNIQUE INDEX IF NOT EXISTS ux_social_ticket_idempotency
                    ON social_preparation_ticket(
                        client_id, subject, resource, platform, target_alias,
                        idempotency_hash
                    );

                CREATE TABLE IF NOT EXISTS social_publish_daily_budget (
                    budget_day TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    target_alias TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(
                        budget_day, client_id, subject, resource, platform,
                        target_alias
                    )
                );

                CREATE TABLE IF NOT EXISTS social_action_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    client_fingerprint TEXT NOT NULL,
                    subject_fingerprint TEXT NOT NULL,
                    resource_fingerprint TEXT NOT NULL,
                    platform TEXT,
                    target_alias TEXT,
                    text_hash TEXT,
                    ticket_fingerprint TEXT,
                    idempotency_fingerprint TEXT,
                    receipt_fingerprint TEXT,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_social_action_audit_created_at
                    ON social_action_audit(created_at);
                CREATE TRIGGER IF NOT EXISTS social_action_audit_no_update
                BEFORE UPDATE ON social_action_audit
                BEGIN
                    SELECT RAISE(ABORT, 'social_action_audit is append-only');
                END;
                DROP TRIGGER IF EXISTS social_action_audit_no_delete;
                CREATE TRIGGER social_action_audit_no_delete
                BEFORE DELETE ON social_action_audit
                WHEN OLD.created_at >= CAST(strftime('%s', 'now') AS INTEGER) - 7776000
                BEGIN
                    SELECT RAISE(ABORT, 'social_action_audit is immutable during retention');
                END;

                -- Social Workspace state is intentionally colocated with OAuth
                -- state, never with the canonical events database.  Provider
                -- identifiers and action bodies are stored only as encrypted
                -- envelopes; public references are random tokens whose hashes
                -- form the lookup keys below.
                CREATE TABLE IF NOT EXISTS social_workspace_ref (
                    ref_hash TEXT PRIMARY KEY,
                    ref_kind TEXT NOT NULL CHECK(ref_kind IN ('target','item','asset')),
                    client_hash TEXT NOT NULL,
                    subject_hash TEXT NOT NULL,
                    resource_hash TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    policy_version TEXT NOT NULL,
                    provider_ref_ciphertext TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_social_workspace_ref_expiry
                    ON social_workspace_ref(expires_at);
                CREATE TABLE IF NOT EXISTS social_workspace_ref_preview (
                    ref_hash TEXT PRIMARY KEY,
                    preview_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS social_workspace_sample (
                    sample_hash TEXT PRIMARY KEY,
                    client_hash TEXT NOT NULL,
                    subject_hash TEXT NOT NULL,
                    resource_hash TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    target_ref_hash TEXT NOT NULL,
                    binding_json TEXT NOT NULL,
                    continuation_cursor_hash TEXT,
                    continuation_cursor_ciphertext TEXT,
                    cumulative_count INTEGER NOT NULL DEFAULT 0,
                    total_limit INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS social_workspace_preparation (
                    preparation_hash TEXT PRIMARY KEY,
                    preparation_ref TEXT NOT NULL UNIQUE,
                    client_hash TEXT NOT NULL,
                    subject_hash TEXT NOT NULL,
                    resource_hash TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_ref_hash TEXT,
                    action_digest TEXT NOT NULL,
                    idempotency_hash TEXT NOT NULL,
                    intent_ciphertext TEXT NOT NULL,
                    status TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(client_hash, subject_hash, resource_hash, platform,
                           action, idempotency_hash)
                );
                CREATE TABLE IF NOT EXISTS social_workspace_approval (
                    approval_hash TEXT PRIMARY KEY,
                    approval_ref TEXT NOT NULL UNIQUE,
                    receipt_ref TEXT NOT NULL,
                    receipt_hash TEXT NOT NULL UNIQUE,
                    preparation_hash TEXT NOT NULL,
                    client_hash TEXT NOT NULL,
                    subject_hash TEXT NOT NULL,
                    resource_hash TEXT NOT NULL,
                    target_ref_hash TEXT,
                    action_digest TEXT NOT NULL,
                    operator_hash TEXT NOT NULL,
                    operator_nonce_hash TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    consumed_at INTEGER,
                    created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS social_workspace_operation (
                    operation_hash TEXT PRIMARY KEY,
                    operation_ref TEXT NOT NULL UNIQUE,
                    preparation_hash TEXT NOT NULL UNIQUE,
                    client_hash TEXT NOT NULL,
                    subject_hash TEXT NOT NULL,
                    resource_hash TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_ref_hash TEXT,
                    status TEXT NOT NULL,
                    retry_safe INTEGER NOT NULL,
                    result_json TEXT,
                    error_code TEXT,
                    provider_attempted_at INTEGER,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS social_workspace_budget (
                    period TEXT NOT NULL,
                    dimension TEXT NOT NULL,
                    bucket_hash TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(period, dimension, bucket_hash, metric)
                );
                CREATE TABLE IF NOT EXISTS social_workspace_circuit (
                    client_hash TEXT NOT NULL,
                    subject_hash TEXT NOT NULL,
                    resource_hash TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    target_ref_hash TEXT NOT NULL,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    flood_until INTEGER,
                    circuit_open_until INTEGER,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(client_hash, subject_hash, resource_hash, platform,
                                target_ref_hash)
                );
                CREATE TABLE IF NOT EXISTS social_workspace_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    principal_hash TEXT NOT NULL,
                    platform TEXT,
                    operation TEXT NOT NULL,
                    target_ref_hash TEXT,
                    action_digest TEXT,
                    outcome TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    response_bytes INTEGER NOT NULL DEFAULT 0,
                    media_items INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_social_workspace_audit_created
                    ON social_workspace_audit(created_at);
                CREATE TRIGGER IF NOT EXISTS social_workspace_audit_no_update
                BEFORE UPDATE ON social_workspace_audit
                BEGIN
                    SELECT RAISE(ABORT, 'social_workspace_audit is append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS social_workspace_audit_no_delete
                BEFORE DELETE ON social_workspace_audit
                BEGIN
                    SELECT RAISE(ABORT, 'social_workspace_audit is append-only');
                END;
                """
            )

    @staticmethod
    def _scopes_to_text(scopes: set[str] | frozenset[str]) -> str:
        return " ".join(sorted(scopes))

    @staticmethod
    def _text_to_scopes(value: str) -> frozenset[str]:
        return frozenset(item for item in (value or "").split() if item)

    def _cleanup(self, conn: sqlite3.Connection, now: int) -> None:
        conn.execute(
            "DELETE FROM oauth_authorization_code WHERE expires_at < ? OR used_at IS NOT NULL",
            (now - 300,),
        )
        conn.execute(
            "DELETE FROM oauth_refresh_token WHERE expires_at < ? OR revoked_at < ?",
            (now - 86400, now - 30 * 86400),
        )
        conn.execute("DELETE FROM oauth_audit WHERE created_at < ?", (now - 90 * 86400,))
        conn.execute(
            "DELETE FROM social_action_audit WHERE created_at < ?",
            (now - 90 * 86400,),
        )
        # Preparation rows double as a bounded idempotency ledger. The daily
        # reservation cap bounds growth; a 90-day replay window is far longer
        # than the five-minute ticket lifetime while keeping the auth DB finite.
        conn.execute(
            "DELETE FROM social_preparation_ticket WHERE created_at < ?",
            (now - 90 * 86400,),
        )
        old_day = datetime.fromtimestamp(now - 90 * 86400, tz=timezone.utc).date().isoformat()
        conn.execute(
            "DELETE FROM social_publish_daily_budget WHERE budget_day < ?",
            (old_day,),
        )
        conn.execute("DELETE FROM social_workspace_ref WHERE expires_at < ?", (now,))
        conn.execute(
            """DELETE FROM social_workspace_ref_preview
               WHERE ref_hash NOT IN (SELECT ref_hash FROM social_workspace_ref)"""
        )
        conn.execute("DELETE FROM social_workspace_sample WHERE expires_at < ?", (now,))
        conn.execute(
            "DELETE FROM social_workspace_approval WHERE expires_at < ?",
            (now - 86400,),
        )
        conn.execute(
            "DELETE FROM social_workspace_preparation WHERE created_at < ?",
            (now - 90 * 86400,),
        )
        conn.execute(
            "DELETE FROM social_workspace_operation WHERE created_at < ?",
            (now - 90 * 86400,),
        )

    def create_authorization_code(
        self,
        *,
        code: str,
        subject: str,
        client_id: str,
        redirect_uri: str,
        resource: str,
        scopes: set[str] | frozenset[str],
        code_challenge: str,
        expires_at: int,
        now: int | None = None,
    ) -> None:
        current = int(time.time()) if now is None else int(now)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._cleanup(conn, current)
                conn.execute(
                    """
                    INSERT INTO oauth_authorization_code(
                        code_hash, subject, client_id, redirect_uri, resource,
                        scopes, code_challenge, expires_at, created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        secret_hash(code),
                        subject,
                        client_id,
                        redirect_uri,
                        resource,
                        self._scopes_to_text(scopes),
                        code_challenge,
                        int(expires_at),
                        current,
                    ),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def consume_authorization_code(
        self,
        *,
        code: str,
        client_id: str,
        redirect_uri: str,
        resource: str,
        code_verifier: str,
        allowed_scopes: frozenset[str] | None = None,
        now: int | None = None,
    ) -> AuthorizationGrant:
        current = int(time.time()) if now is None else int(now)
        digest = secret_hash(code)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM oauth_authorization_code WHERE code_hash=?",
                    (digest,),
                ).fetchone()
                if row is None:
                    raise OAuthStoreError("invalid_grant")
                if row["used_at"] is not None or int(row["expires_at"]) <= current:
                    raise OAuthStoreError("invalid_grant")
                if (
                    row["client_id"] != client_id
                    or row["redirect_uri"] != redirect_uri
                    or row["resource"] != resource
                ):
                    raise OAuthStoreError("invalid_grant")
                stored_scopes = self._text_to_scopes(row["scopes"])
                if allowed_scopes is not None and not stored_scopes.issubset(allowed_scopes):
                    # Stale/corrupt grants must be rejected before the one-use
                    # row is consumed, so repairing the client policy cannot
                    # accidentally bless a previously over-broad code.
                    raise OAuthStoreError("invalid_scope")
                if not verify_pkce(code_verifier, row["code_challenge"]):
                    raise OAuthStoreError("invalid_grant")
                changed = conn.execute(
                    """
                    UPDATE oauth_authorization_code
                    SET used_at=?
                    WHERE code_hash=? AND used_at IS NULL
                    """,
                    (current, digest),
                ).rowcount
                if changed != 1:
                    raise OAuthStoreError("invalid_grant")
                conn.execute("COMMIT")
                return AuthorizationGrant(
                    subject=row["subject"],
                    client_id=row["client_id"],
                    redirect_uri=row["redirect_uri"],
                    resource=row["resource"],
                    scopes=stored_scopes,
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def create_refresh_token(
        self,
        *,
        token: str,
        subject: str,
        client_id: str,
        resource: str,
        scopes: set[str] | frozenset[str],
        expires_at: int,
        now: int | None = None,
    ) -> None:
        current = int(time.time()) if now is None else int(now)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO oauth_refresh_token(
                    token_hash, subject, client_id, resource, scopes,
                    expires_at, created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    secret_hash(token),
                    subject,
                    client_id,
                    resource,
                    self._scopes_to_text(scopes),
                    int(expires_at),
                    current,
                ),
            )

    def rotate_refresh_token(
        self,
        *,
        old_token: str,
        new_token: str,
        client_id: str,
        resource: str,
        new_expires_at: int,
        requested_scopes: set[str] | frozenset[str] | None = None,
        allowed_scopes: frozenset[str] | None = None,
        now: int | None = None,
    ) -> RefreshGrant:
        current = int(time.time()) if now is None else int(now)
        old_hash = secret_hash(old_token)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM oauth_refresh_token WHERE token_hash=?",
                    (old_hash,),
                ).fetchone()
                if row is None:
                    raise OAuthStoreError("invalid_grant")
                if row["revoked_at"] is not None or int(row["expires_at"]) <= current:
                    raise OAuthStoreError("invalid_grant")
                if row["client_id"] != client_id or row["resource"] != resource:
                    raise OAuthStoreError("invalid_grant")
                original_scopes = self._text_to_scopes(row["scopes"])
                if allowed_scopes is not None and not original_scopes.issubset(allowed_scopes):
                    # Check precedes revocation/rotation to keep a rejected
                    # stale grant unchanged and independently auditable.
                    raise OAuthStoreError("invalid_scope")
                scopes = original_scopes
                if requested_scopes is not None:
                    requested = frozenset(requested_scopes)
                    if not requested.issubset(original_scopes):
                        raise OAuthStoreError("invalid_scope")
                    scopes = requested
                # A refresh token is meaningful only while offline_access is
                # retained. Narrowing it away revokes the old token without
                # persisting an unreachable replacement.
                new_hash = secret_hash(new_token) if "offline_access" in scopes else None
                updated = conn.execute(
                    """
                    UPDATE oauth_refresh_token
                    SET revoked_at=?, rotated_to_hash=?, last_used_at=?
                    WHERE token_hash=? AND revoked_at IS NULL
                    """,
                    (current, new_hash, current, old_hash),
                ).rowcount
                if updated != 1:
                    raise OAuthStoreError("invalid_grant")
                if new_hash is not None:
                    conn.execute(
                        """
                        INSERT INTO oauth_refresh_token(
                            token_hash, subject, client_id, resource, scopes,
                            expires_at, created_at
                        ) VALUES(?,?,?,?,?,?,?)
                        """,
                        (
                            new_hash,
                            row["subject"],
                            row["client_id"],
                            row["resource"],
                            self._scopes_to_text(scopes),
                            int(new_expires_at),
                            current,
                        ),
                    )
                conn.execute("COMMIT")
                return RefreshGrant(
                    subject=row["subject"],
                    client_id=row["client_id"],
                    resource=row["resource"],
                    scopes=scopes,
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def audit(
        self,
        *,
        action: str,
        outcome: str,
        client_fingerprint: str | None = None,
        subject: str | None = None,
        details: dict[str, Any] | None = None,
        now: int | None = None,
    ) -> None:
        current = int(time.time()) if now is None else int(now)
        payload = json.dumps(details or {}, ensure_ascii=False, sort_keys=True)[:4000]
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO oauth_audit(
                    action, outcome, client_fingerprint, subject,
                    details_json, created_at
                ) VALUES(?,?,?,?,?,?)
                """,
                (action, outcome, client_fingerprint, subject, payload, current),
            )

    def create_preparation_ticket(
        self,
        *,
        ticket: str,
        client_id: str,
        subject: str,
        resource: str,
        platform: str,
        target_alias: str,
        text_hash: str,
        idempotency_key: str,
        expires_at: int,
        daily_limit: int = 10,
        now: int | None = None,
    ) -> None:
        current = int(time.time()) if now is None else int(now)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._cleanup(conn, current)
                self._reserve_publish_attempt_on_conn(
                    conn,
                    client_id=client_id,
                    subject=subject,
                    resource=resource,
                    platform=platform,
                    target_alias=target_alias,
                    daily_limit=daily_limit,
                    now=current,
                )
                conn.execute(
                    """
                    INSERT INTO social_preparation_ticket(
                        ticket_hash, client_id, subject, resource, platform,
                        target_alias, text_hash, idempotency_hash, expires_at,
                        created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        secret_hash(ticket),
                        client_id,
                        subject,
                        resource,
                        platform,
                        target_alias,
                        text_hash,
                        secret_hash(idempotency_key),
                        int(expires_at),
                        current,
                    ),
                )
                conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                conn.execute("ROLLBACK")
                raise SocialTicketError("idempotency_key_already_used") from exc
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def consume_preparation_ticket(
        self,
        *,
        ticket: str,
        client_id: str,
        subject: str,
        resource: str,
        platform: str,
        target_alias: str,
        text_hash: str,
        idempotency_key: str,
        now: int | None = None,
    ) -> PreparedPublication:
        current = int(time.time()) if now is None else int(now)
        digest = secret_hash(ticket)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM social_preparation_ticket WHERE ticket_hash=?",
                    (digest,),
                ).fetchone()
                if row is None or row["consumed_at"] is not None:
                    raise SocialTicketError("invalid_preparation_ticket")
                if int(row["expires_at"]) <= current:
                    raise SocialTicketError("expired_preparation_ticket")
                expected = (
                    client_id,
                    subject,
                    resource,
                    platform,
                    target_alias,
                    text_hash,
                    secret_hash(idempotency_key),
                )
                actual = (
                    row["client_id"],
                    row["subject"],
                    row["resource"],
                    row["platform"],
                    row["target_alias"],
                    row["text_hash"],
                    row["idempotency_hash"],
                )
                if actual != expected:
                    raise SocialTicketError("preparation_ticket_binding_mismatch")
                changed = conn.execute(
                    """
                    UPDATE social_preparation_ticket SET consumed_at=?
                    WHERE ticket_hash=? AND consumed_at IS NULL
                    """,
                    (current, digest),
                ).rowcount
                if changed != 1:
                    raise SocialTicketError("invalid_preparation_ticket")
                conn.execute("COMMIT")
                return PreparedPublication(
                    client_id=row["client_id"],
                    subject=row["subject"],
                    resource=row["resource"],
                    platform=row["platform"],
                    target_alias=row["target_alias"],
                    text_hash=row["text_hash"],
                    idempotency_hash=row["idempotency_hash"],
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def audit_social_action(
        self,
        *,
        action: str,
        outcome: str,
        client_id: str,
        subject: str,
        resource: str,
        platform: str | None = None,
        target_alias: str | None = None,
        text_hash: str | None = None,
        ticket: str | None = None,
        idempotency_key: str | None = None,
        receipt_reference: str | None = None,
        now: int | None = None,
    ) -> None:
        """Append a deliberately fixed-shape, fingerprint-only social audit row."""

        current = int(time.time()) if now is None else int(now)
        fingerprint = lambda value: secret_hash(value)[:16] if value else None
        with self._lock, self._connect() as conn:
            self._cleanup(conn, current)
            conn.execute(
                """
                INSERT INTO social_action_audit(
                    action, outcome, client_fingerprint, subject_fingerprint,
                    resource_fingerprint, platform, target_alias, text_hash,
                    ticket_fingerprint, idempotency_fingerprint,
                    receipt_fingerprint, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    action[:80],
                    outcome[:40],
                    fingerprint(client_id),
                    fingerprint(subject),
                    fingerprint(resource),
                    platform,
                    target_alias,
                    text_hash,
                    fingerprint(ticket),
                    fingerprint(idempotency_key),
                    fingerprint(receipt_reference),
                    current,
                ),
            )

    @staticmethod
    def _reserve_publish_attempt_on_conn(
        conn: sqlite3.Connection,
        *,
        client_id: str,
        subject: str,
        resource: str,
        platform: str,
        target_alias: str,
        daily_limit: int,
        now: int,
    ) -> int:
        current = int(now)
        budget_day = datetime.fromtimestamp(current, tz=timezone.utc).date().isoformat()
        limit = max(1, int(daily_limit))
        row = conn.execute(
            """
            SELECT attempts FROM social_publish_daily_budget
            WHERE budget_day=? AND client_id=? AND subject=? AND resource=?
              AND platform=? AND target_alias=?
            """,
            (budget_day, client_id, subject, resource, platform, target_alias),
        ).fetchone()
        attempts = int(row["attempts"]) if row is not None else 0
        if attempts >= limit:
            raise SocialPublishBudgetError("daily_publish_attempt_limit_reached")
        next_attempts = attempts + 1
        conn.execute(
            """
            INSERT INTO social_publish_daily_budget(
                budget_day, client_id, subject, resource, platform,
                target_alias, attempts, updated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(
                budget_day, client_id, subject, resource, platform,
                target_alias
            ) DO UPDATE SET attempts=excluded.attempts, updated_at=excluded.updated_at
            """,
            (
                budget_day,
                client_id,
                subject,
                resource,
                platform,
                target_alias,
                next_attempts,
                current,
            ),
        )
        return next_attempts
