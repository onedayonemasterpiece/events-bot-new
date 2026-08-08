from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .crypto import secret_hash, verify_pkce


class OAuthStoreError(ValueError):
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


class OAuthStateStore:
    """Small durable OAuth state store, isolated from the production event DB."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

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
                    scopes=self._text_to_scopes(row["scopes"]),
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
        now: int | None = None,
    ) -> RefreshGrant:
        current = int(time.time()) if now is None else int(now)
        old_hash = secret_hash(old_token)
        new_hash = secret_hash(new_token)
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
                scopes = original_scopes
                if requested_scopes is not None:
                    requested = frozenset(requested_scopes)
                    if not requested.issubset(original_scopes):
                        raise OAuthStoreError("invalid_scope")
                    scopes = requested
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
