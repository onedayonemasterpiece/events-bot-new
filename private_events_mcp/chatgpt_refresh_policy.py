from __future__ import annotations

import asyncio
import time
from types import MethodType

from aiohttp import web

from .auth_store import OAuthStateStore, OAuthStoreError, RefreshGrant
from .crypto import random_token, secret_hash
from .oauth import (
    SUBJECT,
    OAuthHTTPError,
    PrivateOAuthServer,
    _PKCE_VERIFIER_RE,
)


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
    """Use a client-bound confidential refresh token without rotating it.

    Rotation remains mandatory for the public Codex/OpenCode clients. ChatGPT
    is a predefined confidential client authenticated on every token request;
    retaining one bounded token avoids concurrent refresh races while keeping
    the token bound to the exact client, resource and granted scopes.
    """

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


def install_chatgpt_refresh_policy(server: PrivateOAuthServer) -> None:
    """Make refresh durable for the predefined confidential ChatGPT client.

    Existing grants that explicitly contain ``offline_access`` retain the
    original rotating, replay-resistant implementation. Grants without that
    OIDC-flavoured scope receive a bounded confidential-client refresh token
    and can reuse it until expiry. Public clients are untouched.
    """

    original = server.handle_token

    async def handle_token(self: PrivateOAuthServer, request: web.Request) -> web.Response:
        try:
            form = await request.post()
            client = self._authenticate_client(request, form)
        except OAuthHTTPError:
            return await original(request)

        if client.client_id != self.config.oauth_client_id:
            return await original(request)

        grant_type = str(form.get("grant_type") or "")
        if grant_type == "refresh_token":
            old_token = str(form.get("refresh_token") or "")
            scopes = _refresh_scopes(self.store, old_token) if old_token else None
            if scopes is None or "offline_access" in scopes:
                return await original(request)

        try:
            resource = self._validate_resource(
                str(form.get("resource") or ""), client
            )
            now = int(time.time())

            if grant_type == "authorization_code":
                code = str(form.get("code") or "")
                redirect_uri = self._validate_redirect_uri(
                    str(form.get("redirect_uri") or ""), client
                )
                verifier = str(form.get("code_verifier") or "")
                if not code or not _PKCE_VERIFIER_RE.fullmatch(verifier):
                    raise OAuthHTTPError(
                        "invalid_request", "Code and PKCE verifier are required"
                    )
                try:
                    grant = await asyncio.to_thread(
                        self.store.consume_authorization_code,
                        code=code,
                        client_id=client.client_id,
                        redirect_uri=redirect_uri,
                        resource=resource,
                        code_verifier=verifier,
                        allowed_scopes=client.allowed_scopes,
                    )
                except OAuthStoreError as exc:
                    raise OAuthHTTPError(
                        str(exc), "Authorization code is invalid"
                    ) from exc

                payload = self._token_payload(
                    subject=grant.subject,
                    client_id=grant.client_id,
                    resource=grant.resource,
                    scopes=grant.scopes,
                )
                refresh_token = random_token(48)
                await asyncio.to_thread(
                    self.store.create_refresh_token,
                    token=refresh_token,
                    subject=grant.subject,
                    client_id=grant.client_id,
                    resource=grant.resource,
                    scopes=grant.scopes,
                    expires_at=now + self.config.refresh_ttl_seconds,
                )
                payload["refresh_token"] = refresh_token
                await asyncio.to_thread(
                    self.store.audit,
                    action="token",
                    outcome="issued",
                    client_fingerprint=secret_hash(client.client_id)[:12],
                    subject=grant.subject,
                    details={
                        "grant_type": grant_type,
                        "refresh_mode": (
                            "rotating"
                            if "offline_access" in grant.scopes
                            else "confidential_stable"
                        ),
                        "scopes": sorted(grant.scopes),
                    },
                )
                return self._json_response(payload)

            if grant_type == "refresh_token":
                old_token = str(form.get("refresh_token") or "")
                if not old_token:
                    raise OAuthHTTPError(
                        "invalid_request", "Refresh token is required"
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

            return await original(request)
        except OAuthHTTPError as exc:
            await asyncio.to_thread(
                self.store.audit,
                action="token",
                outcome="denied",
                client_fingerprint=secret_hash(client.client_id)[:12],
                details={
                    "grant_type": grant_type[:64],
                    "error": exc.error,
                },
            )
            response = self._json_response(
                {"error": exc.error, "error_description": exc.description},
                status=exc.status,
            )
            if exc.status == 401:
                response.headers["WWW-Authenticate"] = (
                    'Basic realm="private-events-mcp"'
                )
            return response

    server.handle_token = MethodType(handle_token, server)
