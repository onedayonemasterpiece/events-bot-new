"""Canonical-DB partner grants, independent of Telegram and OAuth token storage.

Tokens carry a principal and credential epoch, never organization/portfolio
claims supplied by a client. Every operation resolves the current grant again.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from .crypto import AccessIdentity
from .tool_catalog import ToolExecutionError

PARTNER_SCOPES = frozenset({
    'offline_access',
    'partner:events:read', 'partner:events:propose', 'partner:promo:read',
    'partner:promo:request', 'partner:publications:read',
})
PARTNER_ACTIONS = frozenset({
    'event_create', 'event_edit', 'event_reschedule', 'event_postpone',
    'event_cancel', 'promo_create', 'promo_activity_add', 'promo_pause',
    'promo_resume', 'promo_archive', 'promo_update',
})
REVIEW_ALWAYS = frozenset({'event_reschedule', 'event_postpone', 'event_cancel'})
_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$')
_SUBJECT = re.compile(r'^partner:([a-f0-9]{32}):([1-9][0-9]*)$')

SCHEMA = """
CREATE TABLE IF NOT EXISTS mcp_partner (
    principal_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    policy_revision INTEGER NOT NULL DEFAULT 1,
    scopes_json TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    auto_approve_json TEXT NOT NULL,
    limits_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mcp_partner_credential (
    client_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL UNIQUE,
    credential_epoch INTEGER NOT NULL DEFAULT 1,
    secret_hash TEXT NOT NULL,
    redirect_uris_json TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mcp_partner_event (
    principal_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    event_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (principal_id, event_id)
);
CREATE INDEX IF NOT EXISTS ix_mcp_partner_event_tenant
    ON mcp_partner_event(tenant_id, event_id);
"""


def _error(code: str, message: str = 'Partner policy denies this operation') -> ToolExecutionError:
    return ToolExecutionError(code, message, retry_safe=True)


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise _error('INVALID_ARGUMENTS', f'Invalid {field}')
    return value


def _integer(value: Any, low: int, high: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise _error('INVALID_ARGUMENTS', f'Invalid {field}')
    return value


def _set(value: Any, allowed: frozenset[str], field: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(x, str) or x not in allowed for x in value):
        raise _error('INVALID_ARGUMENTS', f'Invalid {field}')
    return sorted(set(value))


def validate_policy(value: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {'scopes', 'actions', 'auto_approve', 'limits'}
    if set(value) - allowed:
        raise _error('INVALID_ARGUMENTS', 'Unknown policy field')
    scopes = _set(value.get('scopes', []), PARTNER_SCOPES, 'scopes')
    actions = _set(value.get('actions', []), PARTNER_ACTIONS, 'actions')
    automatic = _set(value.get('auto_approve', []), PARTNER_ACTIONS, 'auto_approve')
    if set(automatic) - set(actions) or set(automatic) & REVIEW_ALWAYS:
        raise _error('INVALID_ARGUMENTS', 'Lifecycle changes always require owner review')
    raw = value.get('limits', {})
    if not isinstance(raw, dict) or set(raw) - {'active_campaigns', 'campaign_exposures', 'daily_exposures', 'campaign_days', 'activities'}:
        raise _error('INVALID_ARGUMENTS', 'Invalid limits')
    limits = {}
    for name, default, ceiling in (
        ('active_campaigns', 2, 100), ('campaign_exposures', 3, 100),
        ('daily_exposures', 1, 10), ('campaign_days', 30, 365), ('activities', 3, 10),
    ):
        limits[name] = _integer(raw.get(name, default), 1, ceiling, name)
    return {'scopes': scopes, 'actions': actions, 'auto_approve': automatic, 'limits': limits}


def validate_redirects(value: Any) -> list[str]:
    if not isinstance(value, list) or not 1 <= len(value) <= 8:
        raise _error('INVALID_ARGUMENTS', 'One to eight exact OAuth redirect URIs are required')
    result = []
    for uri in value:
        if not isinstance(uri, str) or len(uri) > 1000:
            raise _error('INVALID_ARGUMENTS', 'Invalid redirect URI')
        try:
            p = urlsplit(uri)
            port = p.port
        except ValueError:
            raise _error('INVALID_ARGUMENTS', 'Invalid redirect URI') from None
        if p.username or p.password or p.query or p.fragment or not p.path or '%' in p.netloc or '\\' in uri:
            raise _error('INVALID_ARGUMENTS', 'Redirect URI must be an exact callback without query or fragment')
        https = p.scheme == 'https' and bool(p.hostname) and port in (None, 443)
        loopback = p.scheme == 'http' and p.hostname == '127.0.0.1' and port is not None and 1024 <= port <= 65535 and p.netloc == f'127.0.0.1:{port}'
        native = p.scheme == 'ladeno' and p.netloc == 'oauth' and p.path == '/callback'
        if not (https or loopback or native):
            raise _error('INVALID_ARGUMENTS', 'Only HTTPS, explicit native loopback or LADENO callbacks are supported')
        result.append(uri)
    return sorted(set(result))


@dataclass(frozen=True)
class PartnerGrant:
    principal_id: str
    tenant_id: str
    organization_id: str
    display_name: str
    status: str
    policy_revision: int
    scopes: frozenset[str]
    actions: frozenset[str]
    auto_approve: frozenset[str]
    limits: Mapping[str, int]
    client_id: str
    credential_epoch: int
    expires_at: int
    redirect_uris: tuple[str, ...]

    @property
    def subject(self) -> str:
        return f'partner:{self.principal_id}:{self.credential_epoch}'

    def public(self) -> dict[str, Any]:
        return {
            'principal_id': self.principal_id, 'tenant_id': self.tenant_id,
            'organization_id': self.organization_id, 'display_name': self.display_name,
            'status': self.status, 'policy_revision': self.policy_revision,
            'scopes': sorted(self.scopes), 'actions': sorted(self.actions),
            'auto_approve': sorted(self.auto_approve), 'limits': dict(self.limits),
            'client_id': self.client_id, 'credential_epoch': self.credential_epoch,
            'expires_at': self.expires_at, 'redirect_uris': list(self.redirect_uris),
        }


class _ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


class PartnerAccessStore:
    def __init__(self, database_path: str | Path, *, resource: str, signing_key: str):
        self.path = str(database_path)
        self.resource = resource
        self.signing_key = signing_key

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f'{Path(self.path).resolve().as_uri()}?mode=rw', uri=True, timeout=1.5, factory=_ClosingConnection)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA busy_timeout=1500')
        return conn

    def _hash(self, value: str) -> str:
        return hmac.new(self.signing_key.encode(), ('partner-login:' + value).encode(), hashlib.sha256).hexdigest()

    @staticmethod
    def _grant(row: sqlite3.Row) -> PartnerGrant:
        return PartnerGrant(
            principal_id=row['principal_id'], tenant_id=row['tenant_id'],
            organization_id=row['organization_id'], display_name=row['display_name'],
            status=row['status'], policy_revision=int(row['policy_revision']),
            scopes=frozenset(json.loads(row['scopes_json'])), actions=frozenset(json.loads(row['actions_json'])),
            auto_approve=frozenset(json.loads(row['auto_approve_json'])), limits=json.loads(row['limits_json']),
            client_id=row['client_id'], credential_epoch=int(row['credential_epoch']),
            expires_at=int(row['expires_at']), redirect_uris=tuple(json.loads(row['redirect_uris_json'])),
        )

    def get(self, principal_id: str | None = None, *, client_id: str | None = None, conn=None) -> PartnerGrant:
        own = conn is None
        conn = conn or self._connect()
        try:
            field, value = ('p.principal_id', principal_id) if principal_id is not None else ('c.client_id', client_id)
            row = conn.execute(f'SELECT p.*,c.client_id,c.credential_epoch,c.expires_at,c.redirect_uris_json FROM mcp_partner p JOIN mcp_partner_credential c USING(principal_id) WHERE {field}=?', (value,)).fetchone()
            if row is None:
                raise _error('NOT_FOUND', 'Partner not found')
            return self._grant(row)
        finally:
            if own:
                conn.close()

    def list(self, *, limit: int = 20, before: str | None = None) -> list[dict[str, Any]]:
        _integer(limit, 1, 50, 'limit')
        with self._connect() as conn:
            rows = conn.execute('SELECT principal_id FROM mcp_partner WHERE (? IS NULL OR principal_id<?) ORDER BY principal_id DESC LIMIT ?', (before, before, limit)).fetchall()
            return [self.get(row[0], conn=conn).public() for row in rows]

    def resolve(self, identity: AccessIdentity, *, scope: str | None = None, action: str | None = None, event_id: int | None = None, conn=None) -> PartnerGrant:
        match = _SUBJECT.fullmatch(identity.subject)
        if identity.audience != self.resource or match is None:
            raise _error('ACCESS_DENIED')
        grant = self.get(match[1], conn=conn)
        if grant.status != 'active' or grant.expires_at <= int(time.time()) or int(match[2]) != grant.credential_epoch or identity.client_id != grant.client_id:
            raise _error('ACCESS_REVOKED')
        if scope and (scope not in identity.scopes or scope not in grant.scopes):
            raise _error('SCOPE_DENIED')
        if action and action not in grant.actions:
            raise _error('ACTION_DENIED')
        if event_id is not None and not self.owns(grant, event_id, conn=conn):
            raise _error('NOT_FOUND', 'Object not found')
        return grant

    def authenticate(self, client_id: str, secret: str) -> PartnerGrant:
        with self._connect() as conn:
            grant = self.get(client_id=client_id, conn=conn)
            row = conn.execute('SELECT secret_hash FROM mcp_partner_credential WHERE client_id=?', (client_id,)).fetchone()
            valid = isinstance(secret, str) and len(secret) <= 200 and hmac.compare_digest(self._hash(secret), row[0])
            if not valid or grant.status != 'active' or grant.expires_at <= int(time.time()):
                raise _error('ACCESS_DENIED')
            return grant

    def owns(self, grant: PartnerGrant, event_id: int, *, conn=None) -> bool:
        own = conn is None
        conn = conn or self._connect()
        try:
            return conn.execute('SELECT 1 FROM mcp_partner_event WHERE principal_id=? AND tenant_id=? AND organization_id=? AND event_id=?', (grant.principal_id, grant.tenant_id, grant.organization_id, int(event_id))).fetchone() is not None
        finally:
            if own:
                conn.close()

    def create(self, *, tenant_id: str, organization_id: str, display_name: str, policy: Mapping[str, Any], redirect_uris: list[str], expires_at: int, event_ids: list[int] | None = None) -> dict[str, Any]:
        tenant_id = _identifier(tenant_id, 'tenant_id')
        organization_id = _identifier(organization_id, 'organization_id')
        if not isinstance(display_name, str) or not 1 <= len(display_name.strip()) <= 160:
            raise _error('INVALID_ARGUMENTS', 'Invalid display name')
        policy = validate_policy(policy)
        redirects = validate_redirects(redirect_uris)
        now = int(time.time())
        _integer(expires_at, now + 60, now + 366 * 86400, 'expires_at')
        principal = secrets.token_hex(16)
        client_id = 'partner-' + secrets.token_hex(16)
        secret = secrets.token_urlsafe(32)
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('INSERT INTO mcp_partner VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', (principal, tenant_id, organization_id, display_name.strip(), 'active', 1, _canonical(policy['scopes']), _canonical(policy['actions']), _canonical(policy['auto_approve']), _canonical(policy['limits']), now, now))
            conn.execute('INSERT INTO mcp_partner_credential VALUES (?,?,?,?,?,?,?)', (client_id, principal, 1, self._hash(secret), _canonical(redirects), expires_at, now))
            self._set_portfolio(conn, self.get(principal, conn=conn), event_ids or [])
            result = self.get(principal, conn=conn).public()
        return {**result, 'login_secret': secret, 'secret_display': 'once', 'resource': self.resource, 'telegram_required': False}

    def _set_portfolio(self, conn, grant: PartnerGrant, event_ids: list[int]) -> None:
        if not isinstance(event_ids, list) or len(event_ids) > 500:
            raise _error('INVALID_ARGUMENTS', 'Invalid portfolio')
        for event_id in event_ids:
            _integer(event_id, 1, 2**63 - 1, 'event_id')
            if conn.execute('SELECT 1 FROM event WHERE id=?', (event_id,)).fetchone() is None:
                raise _error('NOT_FOUND', 'Portfolio event not found')
        conn.execute('DELETE FROM mcp_partner_event WHERE principal_id=?', (grant.principal_id,))
        conn.executemany('INSERT INTO mcp_partner_event VALUES (?,?,?,?,?)', [(grant.principal_id, grant.tenant_id, grant.organization_id, event_id, int(time.time())) for event_id in sorted(set(event_ids))])

    def change(self, principal_id: str, *, action: str, expected_revision: int, policy=None, event_ids=None, expires_at=None) -> dict[str, Any]:
        if action not in {'suspend', 'resume', 'revoke', 'rotate', 'policy', 'portfolio'}:
            raise _error('INVALID_ARGUMENTS', 'Unknown partner action')
        _integer(expected_revision, 1, 2**63 - 1, 'expected_revision')
        secret = None
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            grant = self.get(principal_id, conn=conn)
            if grant.policy_revision != expected_revision:
                raise _error('STALE_PARTNER_REVISION')
            if grant.status == 'revoked':
                raise _error('ACCESS_REVOKED', 'A revoked principal cannot be revived')
            if action == 'policy':
                p = validate_policy(policy or {})
                conn.execute('UPDATE mcp_partner SET scopes_json=?,actions_json=?,auto_approve_json=?,limits_json=? WHERE principal_id=?', (_canonical(p['scopes']), _canonical(p['actions']), _canonical(p['auto_approve']), _canonical(p['limits']), principal_id))
            elif action == 'portfolio':
                self._set_portfolio(conn, grant, event_ids)
            elif action in {'suspend', 'revoke', 'resume'}:
                status = {'suspend': 'suspended', 'revoke': 'revoked', 'resume': 'active'}[action]
                conn.execute('UPDATE mcp_partner SET status=? WHERE principal_id=?', (status, principal_id))
            if action in {'suspend', 'revoke', 'rotate'}:
                secret = secrets.token_urlsafe(32) if action == 'rotate' else None
                new_expiry = grant.expires_at if expires_at is None else _integer(expires_at, int(time.time()) + 60, int(time.time()) + 366 * 86400, 'expires_at')
                conn.execute('UPDATE mcp_partner_credential SET credential_epoch=credential_epoch+1,secret_hash=COALESCE(?,secret_hash),expires_at=?,updated_at=? WHERE principal_id=?', (self._hash(secret) if secret else None, new_expiry, int(time.time()), principal_id))
            conn.execute('UPDATE mcp_partner SET policy_revision=policy_revision+1,updated_at=? WHERE principal_id=?', (int(time.time()), principal_id))
            result = self.get(principal_id, conn=conn).public()
        if secret:
            result.update(login_secret=secret, secret_display='once')
        return result
