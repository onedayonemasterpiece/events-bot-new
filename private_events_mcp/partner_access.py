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
import unicodedata
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
PARTNER_AUTHORITY_KIND_TO_SUBJECT = {
    'venue_operator': 'venue',
    'organizer': 'organization',
    'festival_operator': 'festival',
    'represented_person': 'person',
    'represented_collective': 'collective',
    'series_operator': 'series',
    'programme_operator': 'programme',
}
PARTNER_AUTHORITY_KINDS = frozenset(PARTNER_AUTHORITY_KIND_TO_SUBJECT)
PARTNER_AUTHORITY_SUBJECT_TYPES = frozenset(PARTNER_AUTHORITY_KIND_TO_SUBJECT.values())
PARTNER_PEOPLE_AUTHORITY_KINDS = frozenset({'represented_person', 'represented_collective'})
_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$')
_SUBJECT = re.compile(r'^partner:([a-f0-9]{32}):([1-9][0-9]*)$')

SCHEMA = """
CREATE TABLE IF NOT EXISTS mcp_partner_campaign (
 campaign_id INTEGER PRIMARY KEY, principal_id TEXT NOT NULL, tenant_id TEXT NOT NULL, organization_id TEXT NOT NULL
);
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
CREATE TABLE IF NOT EXISTS mcp_partner_authority (
    authority_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    authority_kind TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    aliases_json TEXT NOT NULL,
    constraints_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(principal_id, authority_kind, subject_type, subject_key)
);
CREATE INDEX IF NOT EXISTS ix_mcp_partner_authority_owner
    ON mcp_partner_authority(principal_id, tenant_id, organization_id);
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


def _authority_text(value: Any, field: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str):
        raise _error('INVALID_ARGUMENTS', f'Invalid {field}')
    clean = value.strip()
    if not 1 <= len(clean) <= maximum or any(ord(ch) < 0x20 for ch in clean):
        raise _error('INVALID_ARGUMENTS', f'Invalid {field}')
    return clean


def normalize_authority_name(value: Any) -> str:
    """Conservative exact-name normalization; never fuzzy/substring authority."""
    if not isinstance(value, str):
        return ''
    clean = unicodedata.normalize('NFKC', value).casefold().replace('ё', 'е')
    clean = re.sub(r'[\W_]+', ' ', clean, flags=re.UNICODE)
    return re.sub(r'\s+', ' ', clean).strip()


def validate_authorities(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > 100:
        raise _error('INVALID_ARGUMENTS', 'Invalid authorities')
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for raw in value:
        if not isinstance(raw, Mapping):
            raise _error('INVALID_ARGUMENTS', 'Invalid authority')
        allowed = {
            'authority_kind', 'subject_type', 'subject_key', 'display_name',
            'aliases', 'cities',
        }
        if set(raw) - allowed:
            raise _error('INVALID_ARGUMENTS', 'Unknown authority field')
        kind = raw.get('authority_kind')
        subject_type = raw.get('subject_type')
        if kind not in PARTNER_AUTHORITY_KINDS:
            raise _error('INVALID_ARGUMENTS', 'Invalid authority_kind')
        if subject_type != PARTNER_AUTHORITY_KIND_TO_SUBJECT[kind]:
            raise _error('INVALID_ARGUMENTS', 'authority subject_type does not match authority_kind')
        subject_key = _identifier(raw.get('subject_key'), 'subject_key')
        display_name = _authority_text(raw.get('display_name'), 'display_name')
        aliases_raw = raw.get('aliases', [])
        if not isinstance(aliases_raw, list) or len(aliases_raw) > 32:
            raise _error('INVALID_ARGUMENTS', 'Invalid authority aliases')
        aliases: list[str] = []
        normalized_aliases: set[str] = set()
        for item in [display_name, *aliases_raw]:
            alias = _authority_text(item, 'authority alias')
            normalized = normalize_authority_name(alias)
            if not normalized:
                raise _error('INVALID_ARGUMENTS', 'Invalid authority alias')
            if normalized not in normalized_aliases:
                normalized_aliases.add(normalized)
                aliases.append(alias)
        cities_raw = raw.get('cities', [])
        if not isinstance(cities_raw, list) or len(cities_raw) > 20:
            raise _error('INVALID_ARGUMENTS', 'Invalid authority cities')
        cities: list[str] = []
        normalized_cities: set[str] = set()
        for item in cities_raw:
            city = _authority_text(item, 'authority city', maximum=120)
            normalized = normalize_authority_name(city)
            if normalized and normalized not in normalized_cities:
                normalized_cities.add(normalized)
                cities.append(city)
        key = (kind, subject_type, subject_key)
        if key in seen:
            raise _error('INVALID_ARGUMENTS', 'Duplicate authority binding')
        seen.add(key)
        result.append({
            'authority_kind': kind,
            'subject_type': subject_type,
            'subject_key': subject_key,
            'display_name': display_name,
            'aliases': aliases,
            'cities': cities,
        })
    return result


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
        if scope and scope not in identity.scopes:
            raise _error('SCOPE_DENIED')
        return self._resolve_actor(identity.subject, identity.client_id, identity.audience,
                                   scope=scope, action=action, event_id=event_id, conn=conn)

    def resolve_durable(self, *, actor_subject: str, actor_client_id: str, actor_audience: str,
                        scope: str, action: str, event_id: int | None = None, conn=None) -> PartnerGrant:
        """Current policy for an already authorized stored intent, not a new login.

        The ledger owns immutable actor provenance. This internal boundary never
        creates an OAuth identity, borrows a token scope or authenticates a caller.
        Rotation/suspension invalidates the stored subject's credential epoch.
        """
        if scope not in PARTNER_SCOPES or action not in PARTNER_ACTIONS:
            raise _error('ACCESS_DENIED')
        return self._resolve_actor(actor_subject, actor_client_id, actor_audience,
                                   scope=scope, action=action, event_id=event_id, conn=conn)

    def _resolve_actor(self, subject, client_id, audience, *, scope, action, event_id, conn):
        match = _SUBJECT.fullmatch(subject) if isinstance(subject, str) else None
        if audience != self.resource or match is None:
            raise _error('ACCESS_DENIED')
        grant = self.get(match[1], conn=conn)
        if grant.status != 'active' or grant.expires_at <= int(time.time()) or int(match[2]) != grant.credential_epoch or client_id != grant.client_id:
            raise _error('ACCESS_REVOKED')
        if scope and scope not in grant.scopes:
            raise _error('SCOPE_DENIED')
        if action and action not in grant.actions:
            raise _error('ACTION_DENIED')
        if event_id is not None:
            event_id = _integer(event_id, 1, 2**63-1, 'event_id')
            if not self.owns(grant, event_id, conn=conn):
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

    def authorities(self, grant: PartnerGrant, *, conn=None) -> list[dict[str, Any]]:
        own = conn is None
        conn = conn or self._connect()
        try:
            rows = conn.execute(
                'SELECT authority_id,authority_kind,subject_type,subject_key,display_name,aliases_json,constraints_json '
                'FROM mcp_partner_authority WHERE principal_id=? AND tenant_id=? AND organization_id=? '
                'ORDER BY authority_kind,subject_type,subject_key',
                (grant.principal_id, grant.tenant_id, grant.organization_id),
            ).fetchall()
            result = []
            for row in rows:
                aliases = json.loads(row['aliases_json']) if isinstance(row['aliases_json'], str) else []
                constraints = json.loads(row['constraints_json']) if isinstance(row['constraints_json'], str) else {}
                result.append({
                    'authority_id': row['authority_id'],
                    'authority_kind': row['authority_kind'],
                    'subject_type': row['subject_type'],
                    'subject_key': row['subject_key'],
                    'display_name': row['display_name'],
                    'aliases': aliases if isinstance(aliases, list) else [],
                    'cities': constraints.get('cities', []) if isinstance(constraints, dict) else [],
                })
            return result
        finally:
            if own:
                conn.close()

    def authority_kinds(self, grant: PartnerGrant, *, conn=None) -> tuple[str, ...]:
        return tuple(sorted({item['authority_kind'] for item in self.authorities(grant, conn=conn)}))

    def _set_authorities(self, conn, grant: PartnerGrant, authorities: Any) -> None:
        items = validate_authorities(authorities)
        conn.execute('DELETE FROM mcp_partner_authority WHERE principal_id=?', (grant.principal_id,))
        now = int(time.time())
        for item in items:
            raw_id = f"{grant.principal_id}:{item['authority_kind']}:{item['subject_type']}:{item['subject_key']}"
            authority_id = 'auth_' + hashlib.sha256(raw_id.encode('utf-8')).hexdigest()[:24]
            conn.execute(
                'INSERT INTO mcp_partner_authority(authority_id,principal_id,tenant_id,organization_id,authority_kind,subject_type,subject_key,display_name,aliases_json,constraints_json,created_at) '
                'VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (
                    authority_id, grant.principal_id, grant.tenant_id, grant.organization_id,
                    item['authority_kind'], item['subject_type'], item['subject_key'],
                    item['display_name'], _canonical(item['aliases']),
                    _canonical({'cities': item['cities']}), now,
                ),
            )

    def evaluate_create_candidate(self, grant: PartnerGrant, candidate: Any, *, conn=None) -> dict[str, Any]:
        authorities = self.authorities(grant, conn=conn)
        if not authorities:
            return {'status': 'review_required', 'reason': 'partner_authority_not_configured', 'matched_authority': None}

        venue_name = normalize_authority_name(getattr(candidate, 'location_name', None))
        city = normalize_authority_name(getattr(candidate, 'city', None))
        organizers = {
            normalize_authority_name(value)
            for value in (getattr(candidate, 'organizer_names', None) or [])
            if normalize_authority_name(value)
        }
        festivals = {
            normalize_authority_name(value)
            for value in (
                getattr(candidate, 'festival', None),
                getattr(candidate, 'festival_full', None),
            )
            if normalize_authority_name(value)
        }
        series = {
            normalize_authority_name(value)
            for value in (
                getattr(candidate, 'festival_series', None),
                *(getattr(candidate, 'authority_series_names', None) or []),
            )
            if normalize_authority_name(value)
        }
        people: set[str] = set()
        decisions = getattr(candidate, 'collection_semantic_decisions', None)
        if isinstance(decisions, Mapping):
            for item in decisions.get('people_appearances') or []:
                if not isinstance(item, Mapping) or item.get('appearance') != 'confirmed':
                    continue
                if item.get('role') not in {'performer', 'speaker', 'author', 'host'}:
                    continue
                name = normalize_authority_name(item.get('name'))
                if name:
                    people.add(name)

        reasons: list[str] = []
        for authority in authorities:
            aliases = {
                normalize_authority_name(value)
                for value in authority.get('aliases', [])
                if normalize_authority_name(value)
            }
            kind = authority['authority_kind']
            matched = False
            if kind == 'venue_operator':
                allowed_cities = {
                    normalize_authority_name(value)
                    for value in authority.get('cities', [])
                    if normalize_authority_name(value)
                }
                matched = bool(
                    venue_name and venue_name in aliases
                    and (not allowed_cities or city in allowed_cities)
                )
            elif kind == 'organizer':
                matched = bool(aliases & organizers)
            elif kind == 'festival_operator':
                matched = bool(aliases & festivals)
            elif kind in PARTNER_PEOPLE_AUTHORITY_KINDS:
                matched = bool(aliases & people)
            elif kind == 'series_operator':
                matched = bool(aliases & series)
            elif kind == 'programme_operator':
                reasons.append('programme_scope_requires_structured_evidence')
            if matched:
                return {
                    'status': 'matched',
                    'reason': 'partner_authority_matched',
                    'matched_authority': {
                        'authority_id': authority['authority_id'],
                        'authority_kind': authority['authority_kind'],
                        'subject_type': authority['subject_type'],
                        'subject_key': authority['subject_key'],
                        'display_name': authority['display_name'],
                    },
                }

        if any(a['authority_kind'] == 'organizer' for a in authorities) and not organizers:
            reasons.append('organizer_evidence_missing')
        if not people and any(a['authority_kind'] in PARTNER_PEOPLE_AUTHORITY_KINDS for a in authorities):
            reasons.append('people_evidence_missing')
        if not series and any(a['authority_kind'] == 'series_operator' for a in authorities):
            reasons.append('series_evidence_missing')
        return {
            'status': 'review_required',
            'reason': sorted(set(reasons))[0] if reasons else 'partner_authority_no_match',
            'matched_authority': None,
        }

    def create(self, *, tenant_id: str, organization_id: str, display_name: str, policy: Mapping[str, Any], redirect_uris: list[str], expires_at: int, event_ids: list[int] | None = None, authorities: list[Mapping[str, Any]] | None = None) -> dict[str, Any]:
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
            grant = self.get(principal, conn=conn)
            self._set_portfolio(conn, grant, event_ids or [])
            self._set_authorities(conn, grant, authorities or [])
            result = grant.public()
            result['authorities'] = self.authorities(grant, conn=conn)
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

    def change(self, principal_id: str, *, action: str, expected_revision: int, policy=None, event_ids=None, authorities=None, expires_at=None) -> dict[str, Any]:
        if action not in {'suspend', 'resume', 'revoke', 'rotate', 'policy', 'portfolio', 'authorities'}:
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
            elif action == 'authorities':
                if authorities is None:
                    raise _error('INVALID_ARGUMENTS', 'authorities are required')
                self._set_authorities(conn, grant, authorities)
            elif action in {'suspend', 'revoke', 'resume'}:
                status = {'suspend': 'suspended', 'revoke': 'revoked', 'resume': 'active'}[action]
                conn.execute('UPDATE mcp_partner SET status=? WHERE principal_id=?', (status, principal_id))
            if action in {'suspend', 'revoke', 'rotate'}:
                secret = secrets.token_urlsafe(32) if action == 'rotate' else None
                new_expiry = grant.expires_at if expires_at is None else _integer(expires_at, int(time.time()) + 60, int(time.time()) + 366 * 86400, 'expires_at')
                conn.execute('UPDATE mcp_partner_credential SET credential_epoch=credential_epoch+1,secret_hash=COALESCE(?,secret_hash),expires_at=?,updated_at=? WHERE principal_id=?', (self._hash(secret) if secret else None, new_expiry, int(time.time()), principal_id))
            conn.execute('UPDATE mcp_partner SET policy_revision=policy_revision+1,updated_at=? WHERE principal_id=?', (int(time.time()), principal_id))
            current = self.get(principal_id, conn=conn)
            result = current.public()
            result['authorities'] = self.authorities(current, conn=conn)
        if secret:
            result.update(login_secret=secret, secret_display='once')
        return result
