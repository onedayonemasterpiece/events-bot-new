from __future__ import annotations

import asyncio
import json
import logging
import time
from types import MethodType

from aiohttp import web

from .auth_store import OAuthStateStore, OAuthStoreError, RefreshGrant
from .crypto import (
    TokenValidationError,
    random_token,
    secret_hash,
    validate_access_token,
)
from .oauth import OAuthHTTPError, PrivateOAuthServer

logger = logging.getLogger(__name__)


def _refresh_scopes(store: OAuthStateStore, token: str) -> frozenset[str] | None:
    """Return stored scopes for one live token without exposing its value."""

    digest = secret_hash(token)
    current = int(time.time())
    with store._lock, store._connect() as conn:
        row = conn.execute(
            """
            SELECT scopes, expires_at, revoked_at
            FROM oauth_refresh_token
            WHERE token_hash=?
            """,
            (digest,),
        ).fetchone()
    if row is None or row["revoked_at"] is not None:
        return None
    if int(row["expires_at"]) <= current:
        return None
    return store._text_to_scopes(row["scopes"])


def _use_confidential_refresh_token(
    store: OAuthStateStore,
    *,
    token: str,
    client_id: str,
    resource: str,
    requested_scopes: frozenset[str] | None,
    allowed_scopes: frozenset[str],
    now: int | None = None,
) -> RefreshGrant:
    """Use one bounded, client-bound confidential refresh token in place."""

    current = int(time.time()) if now is None else int(now)
    digest = secret_hash(token)
    with store._lock, store._connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            store._cleanup(conn, current)
            row = conn.execute(
                "SELECT * FROM oauth_refresh_token WHERE token_hash=?",
                (digest,),
            ).fetchone()
            if row is None:
                raise OAuthStoreError("invalid_grant")
            if row["revoked_at"] is not None or int(row["expires_at"]) <= current:
                raise OAuthStoreError("invalid_grant")
            if row["client_id"] != client_id or row["resource"] != resource:
                raise OAuthStoreError("invalid_grant")

            original_scopes = store._text_to_scopes(row["scopes"])
            if not original_scopes.issubset(allowed_scopes):
                raise OAuthStoreError("invalid_scope")
            scopes = original_scopes
            if requested_scopes is not None:
                if not requested_scopes.issubset(original_scopes):
                    raise OAuthStoreError("invalid_scope")
                scopes = requested_scopes

            changed = conn.execute(
                """
                UPDATE oauth_refresh_token
                SET scopes=?, last_used_at=?
                WHERE token_hash=? AND revoked_at IS NULL
                """,
                (store._scopes_to_text(scopes), current, digest),
            ).rowcount
            if changed != 1:
                raise OAuthStoreError("invalid_grant")
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


def _error_response(
    server: PrivateOAuthServer, error: OAuthHTTPError
) -> web.Response:
    response = server._json_response(
        {"error": error.error, "error_description": error.description},
        status=error.status,
    )
    if error.status == 401:
        response.headers["WWW-Authenticate"] = 'Basic realm="private-events-mcp"'
    return response


def install_chatgpt_refresh_policy(server: PrivateOAuthServer) -> None:
    """Issue durable refresh credentials for the confidential ChatGPT client.

    The original flow remains authoritative. This wrapper only fills the gap
    where ChatGPT receives an access grant without the OIDC-style
    ``offline_access`` scope. Such grants receive a bounded refresh token that
    remains bound to the exact confidential client, resource and scopes.

    Explicit ``offline_access`` grants keep the existing rotating,
    replay-resistant implementation. Public Codex/OpenCode clients are
    untouched and always retain their original rotation policy.
    """

    original = server.handle_token

    async def handle_token(
        self: PrivateOAuthServer, request: web.Request
    ) -> web.Response:
        try:
            form = await request.post()
            client = self._authenticate_client(request, form)
        except OAuthHTTPError:
            return await original(request)

        if client.client_id != self.config.oauth_client_id:
            return await original(request)

        grant_type = str(form.get("grant_type") or "")

        if grant_type == "authorization_code":
            response = await original(request)
            if response.status != 200 or not response.body:
                return response
            try:
                payload = json.loads(response.body)
            except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
                return response
            if payload.get("refresh_token"):
                return response

            access_token = payload.get("access_token")
            if not isinstance(access_token, str) or not access_token:
                return response
            try:
                identity = validate_access_token(
                    access_token,
                    signing_key=self.config.signing_key,
                    issuer=self.config.issuer,
                    audience=self.config.resource,
                )
            except TokenValidationError:
                logger.exception(
                    "chatgpt refresh issuance could not validate fresh access token"
                )
                return response
            if identity.client_id != self.config.oauth_client_id:
                return response

            refresh_token = random_token(48)
            try:
                await asyncio.to_thread(
                    self.store.create_refresh_token,
                    token=refresh_token,
                    subject=identity.subject,
                    client_id=identity.client_id,
                    resource=identity.audience,
                    scopes=identity.scopes,
                    expires_at=int(time.time()) + self.config.refresh_ttl_seconds,
                )
                await asyncio.to_thread(
                    self.store.audit,
                    action="token",
                    outcome="refresh_issued",
                    client_fingerprint=secret_hash(identity.client_id)[:12],
                    subject=identity.subject,
                    details={
                        "grant_type": grant_type,
                        "refresh_mode": "confidential_stable",
                        "scopes": sorted(identity.scopes),
                    },
                )
            except Exception:
                logger.exception("chatgpt confidential refresh issuance failed")
                return response
            payload["refresh_token"] = refresh_token
            return self._json_response(payload)

        if grant_type != "refresh_token":
            return await original(request)

        old_token = str(form.get("refresh_token") or "")
        scopes = _refresh_scopes(self.store, old_token) if old_token else None
        if scopes is None or "offline_access" in scopes:
            return await original(request)

        try:
            resource = self._validate_resource(
                str(form.get("resource") or ""), client
            )
            requested_raw = str(form.get("scope") or "").strip()
            requested = (
                self._parse_scopes(requested_raw, client)
                if requested_raw
                else None
            )
            try:
                grant = await asyncio.to_thread(
                    _use_confidential_refresh_token,
                    self.store,
                    token=old_token,
                    client_id=client.client_id,
                    resource=resource,
                    requested_scopes=requested,
                    allowed_scopes=client.allowed_scopes,
                )
            except OAuthStoreError as exc:
                raise OAuthHTTPError(
                    str(exc), "Refresh token is invalid"
                ) from exc

            payload = self._token_payload(
                subject=grant.subject,
                client_id=grant.client_id,
                resource=grant.resource,
                scopes=grant.scopes,
            )
            payload["refresh_token"] = old_token
            await asyncio.to_thread(
                self.store.audit,
                action="token",
                outcome="refreshed",
                client_fingerprint=secret_hash(client.client_id)[:12],
                subject=grant.subject,
                details={
                    "grant_type": grant_type,
                    "refresh_mode": "confidential_stable",
                    "scopes": sorted(grant.scopes),
                },
            )
            return self._json_response(payload)
        except OAuthHTTPError as exc:
            return _error_response(self, exc)

    server.handle_token = MethodType(handle_token, server)
