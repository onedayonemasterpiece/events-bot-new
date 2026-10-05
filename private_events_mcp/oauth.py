from __future__ import annotations

import asyncio
import base64
import html
import logging
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from aiohttp import web

from .access_policy import (
    APPROVAL_REQUIRED_SOCIAL_SCOPES,
    CHATGPT_DEFAULT_SCOPES,
    CHATGPT_MAX_SCOPES,
    CODEX_DEFAULT_SCOPES,
    CODEX_MAX_SCOPES,
    GRANULAR_SOCIAL_SCOPES,
    LEGACY_PUBLISH_SCOPES,
    LEGACY_SOCIAL_SCOPES,
    OPENCODE_DEFAULT_SCOPES,
    OPENCODE_MAX_SCOPES,
    SOCIAL_SCOPES,
)
from .auth_store import OAuthStateStore, OAuthStoreError
from .config import PrivateEventsMCPConfig
from .crypto import (
    AccessIdentity,
    TokenValidationError,
    constant_time_equal,
    mint_access_token,
    random_token,
    secret_hash,
    sign_compact_token,
    validate_access_token,
    verify_compact_token,
)
from .limits import SlidingWindowLimiter
from .partner_access import PARTNER_SCOPES, PartnerAccessStore
from .tool_catalog import ToolExecutionError

logger = logging.getLogger(__name__)

ALL_SCOPES = CHATGPT_MAX_SCOPES | CODEX_MAX_SCOPES
SUBJECT = "events-bot-owner"
_PKCE_CHALLENGE_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_PKCE_VERIFIER_RE = re.compile(r"^[A-Za-z0-9._~-]{43,128}$")
_CODEX_CALLBACK_PATH_RE = re.compile(r"^/callback/(?!\.{1,2}$)[A-Za-z0-9._~-]{1,160}$")


class OAuthHTTPError(ValueError):
    def __init__(self, error: str, description: str, status: int = 400) -> None:
        super().__init__(description)
        self.error = error
        self.description = description
        self.status = status


@dataclass(frozen=True, slots=True)
class AuthorizationRequest:
    response_type: str
    client_id: str
    redirect_uri: str
    state: str
    resource: str
    scopes: frozenset[str]
    code_challenge: str
    code_challenge_method: str


@dataclass(frozen=True, slots=True)
class OAuthClient:
    client_id: str
    token_endpoint_auth_method: str
    allowed_resources: frozenset[str]
    allowed_scopes: frozenset[str]
    default_scopes: frozenset[str]


class PrivateOAuthServer:
    """Minimal single-operator OAuth 2.1 authorization server.

    This is deliberately a predefined-client implementation: ChatGPT is a
    static confidential client, while Codex and OpenCode are distinct static
    public clients. Every authorization-code exchange additionally requires
    PKCE S256. It is not a general identity provider and does not expose dynamic
    client registration.
    """

    def __init__(self, config: PrivateEventsMCPConfig) -> None:
        self.config = config
        config.ensure_auth_directory()
        self.store = OAuthStateStore(config.auth_database_path)
        self.failure_limiter = SlidingWindowLimiter()
        self.partners = PartnerAccessStore(
            config.database_path, resource=config.partner_resource, signing_key=config.signing_key,
        ) if config.partner_enabled else None

    @staticmethod
    def _json_response(
        payload: Mapping[str, Any], *, status: int = 200
    ) -> web.Response:
        response = web.json_response(dict(payload), status=status)
        response.headers.update(
            {
                "Cache-Control": "no-store",
                "Pragma": "no-cache",
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "no-referrer",
            }
        )
        return response

    def protected_resource_metadata(self) -> dict[str, Any]:
        return self.protected_resource_metadata_for(self.config.resource)

    def protected_resource_metadata_for(self, resource: str) -> dict[str, Any]:
        if self.partners is not None and resource == self.config.partner_resource:
            return {
                "resource": resource, "authorization_servers": [self.config.issuer],
                "scopes_supported": sorted(PARTNER_SCOPES),
                "resource_documentation": self.config.documentation_url,
                "bearer_methods_supported": ["header"],
            }
        clients = tuple(
            client
            for client_id in self.config.oauth_client_ids
            if resource in (client := self._client(client_id)).allowed_resources
        )
        if not clients:
            raise ValueError("unregistered_oauth_resource")
        return {
            "resource": resource,
            "authorization_servers": [self.config.issuer],
            "scopes_supported": sorted(
                frozenset().union(*(client.allowed_scopes for client in clients))
            ),
            "resource_documentation": self.config.documentation_url,
            "bearer_methods_supported": ["header"],
        }

    def authorization_server_metadata(self) -> dict[str, Any]:
        return {
            "issuer": self.config.issuer,
            "authorization_endpoint": self.config.authorization_endpoint,
            "token_endpoint": self.config.token_endpoint,
            "response_types_supported": ["code"],
            "response_modes_supported": ["query"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": [
                "client_secret_basic",
                "client_secret_post",
                "none",
            ],
            "scopes_supported": sorted(ALL_SCOPES | (PARTNER_SCOPES if self.partners else frozenset())),
            "service_documentation": self.config.documentation_url,
        }

    async def handle_protected_resource_metadata(
        self, _request: web.Request
    ) -> web.Response:
        return self._json_response(self.protected_resource_metadata())

    async def handle_authorization_server_metadata(
        self, _request: web.Request
    ) -> web.Response:
        return self._json_response(self.authorization_server_metadata())

    @staticmethod
    def _parse_scopes(raw: str | None, client: OAuthClient) -> frozenset[str]:
        if not raw:
            return client.default_scopes
        scopes = frozenset(item for item in raw.split() if item)
        if not scopes or not scopes.issubset(client.allowed_scopes):
            raise OAuthHTTPError("invalid_scope", "Requested scope is not available")
        return scopes

    def _client(self, client_id: str) -> OAuthClient:
        if constant_time_equal(client_id, self.config.oauth_client_id):
            return OAuthClient(
                client_id,
                "client_secret_basic",
                frozenset({self.config.resource}),
                CHATGPT_MAX_SCOPES,
                CHATGPT_DEFAULT_SCOPES,
            )
        if constant_time_equal(client_id, self.config.codex_oauth_client_id):
            return OAuthClient(
                client_id,
                "none",
                frozenset({self.config.codex_resource}),
                CODEX_MAX_SCOPES,
                CODEX_DEFAULT_SCOPES,
            )
        if self.config.opencode_oauth_client_id and constant_time_equal(
            client_id, self.config.opencode_oauth_client_id
        ):
            return OAuthClient(
                client_id,
                "none",
                frozenset({self.config.resource}),
                OPENCODE_MAX_SCOPES,
                OPENCODE_DEFAULT_SCOPES,
            )
        if self.partners is not None:
            try:
                grant = self.partners.get(client_id=client_id)
                if grant.status != "active" or grant.expires_at <= int(time.time()):
                    raise ToolExecutionError("PARTNER_ACCESS_REVOKED", "Partner access revoked")
                return OAuthClient(client_id, "none", frozenset({self.config.partner_resource}),
                                   grant.scopes, grant.scopes - {"offline_access"})
            except ToolExecutionError:
                pass
        raise OAuthHTTPError("unauthorized_client", "Unknown OAuth client")

    @staticmethod
    def _validate_resource(value: str, client: OAuthClient) -> str:
        if value not in client.allowed_resources:
            raise OAuthHTTPError("invalid_target", "The resource parameter is invalid")
        return value

    @staticmethod
    def _validate_chatgpt_redirect_uri(value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "chatgpt.com"
            or parsed.netloc != "chatgpt.com"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
            or parsed.query
        ):
            raise OAuthHTTPError("invalid_request", "Redirect URI is not allowed")
        path = parsed.path or ""
        modern = path.startswith("/connector/oauth/") and len(path.split("/")) >= 4
        legacy = path == "/connector_platform_oauth_redirect"
        if not (modern or legacy) or parsed.fragment:
            raise OAuthHTTPError("invalid_request", "Redirect URI is not allowed")
        return value

    @staticmethod
    def _validate_codex_redirect_uri(value: str) -> str:
        """Accept only Codex's explicit IPv4 loopback callback contract.

        The raw authority comparison intentionally rejects DNS aliases, IPv6,
        userinfo, non-canonical/implicit ports and encoded authority tricks.
        The callback nonce is a single URL-safe path segment, never a query.
        """

        if not value.startswith("http://127.0.0.1:") or "?" in value or "#" in value:
            raise OAuthHTTPError("invalid_request", "Redirect URI is not allowed")
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError as exc:
            raise OAuthHTTPError(
                "invalid_request", "Redirect URI is not allowed"
            ) from exc
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.username is not None
            or parsed.password is not None
            or port is None
            or not (1 <= port <= 65535)
            or parsed.netloc != f"127.0.0.1:{port}"
            or parsed.query
            or parsed.fragment
            or not _CODEX_CALLBACK_PATH_RE.fullmatch(parsed.path)
        ):
            raise OAuthHTTPError("invalid_request", "Redirect URI is not allowed")
        return value

    @staticmethod
    def _validate_opencode_redirect_uri(value: str) -> str:
        """Accept OpenCode's exact IPv4 loopback path on a dynamic local port.

        RFC 8252 allows native public clients to select an available loopback
        port.  The authorization code remains bound to the exact redirect URI
        and mandatory S256 verifier, while DNS aliases, userinfo, queries,
        fragments, implicit/privileged/zero-padded ports and alternate paths
        remain rejected.
        """

        if not value.startswith("http://127.0.0.1:") or "?" in value or "#" in value:
            raise OAuthHTTPError("invalid_request", "Redirect URI is not allowed")
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError as exc:
            raise OAuthHTTPError(
                "invalid_request", "Redirect URI is not allowed"
            ) from exc
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.username is not None
            or parsed.password is not None
            or port is None
            or not (1024 <= port <= 65535)
            or parsed.netloc != f"127.0.0.1:{port}"
            or parsed.path != "/mcp/oauth/callback"
            or parsed.query
            or parsed.fragment
        ):
            raise OAuthHTTPError("invalid_request", "Redirect URI is not allowed")
        return value

    def _validate_redirect_uri(self, value: str, client: OAuthClient) -> str:
        if self.partners is not None and self.config.partner_resource in client.allowed_resources:
            grant = self.partners.get(client_id=client.client_id)
            if value not in grant.redirect_uris:
                raise OAuthHTTPError("invalid_request", "Redirect URI is not allowed")
            return value
        if client.client_id == self.config.oauth_client_id:
            return self._validate_chatgpt_redirect_uri(value)
        if client.client_id == self.config.codex_oauth_client_id:
            return self._validate_codex_redirect_uri(value)
        if (
            self.config.opencode_oauth_client_id
            and client.client_id == self.config.opencode_oauth_client_id
        ):
            return self._validate_opencode_redirect_uri(value)
        raise OAuthHTTPError("unauthorized_client", "Unknown OAuth client")

    def _parse_authorization_request(
        self, params: Mapping[str, str]
    ) -> AuthorizationRequest:
        response_type = params.get("response_type", "")
        client_id = params.get("client_id", "")
        client = self._client(client_id)
        redirect_uri = self._validate_redirect_uri(
            params.get("redirect_uri", ""), client
        )
        state = params.get("state", "")
        resource = self._validate_resource(params.get("resource", ""), client)
        code_challenge = params.get("code_challenge", "")
        method = params.get("code_challenge_method", "")
        if response_type != "code":
            raise OAuthHTTPError(
                "unsupported_response_type", "Only authorization code is supported"
            )
        if not state or len(state) > 2048:
            raise OAuthHTTPError("invalid_request", "State is required")
        if method != "S256" or not _PKCE_CHALLENGE_RE.fullmatch(code_challenge):
            raise OAuthHTTPError("invalid_request", "PKCE S256 is required")
        scopes = self._parse_scopes(params.get("scope"), client)
        return AuthorizationRequest(
            response_type=response_type,
            client_id=client_id,
            redirect_uri=redirect_uri,
            state=state,
            resource=resource,
            scopes=scopes,
            code_challenge=code_challenge,
            code_challenge_method=method,
        )

    def _seal_authorization_request(self, request: AuthorizationRequest) -> str:
        now = int(time.time())
        return sign_compact_token(
            {
                "response_type": request.response_type,
                "client_id": request.client_id,
                "redirect_uri": request.redirect_uri,
                "state": request.state,
                "resource": request.resource,
                "scope": " ".join(sorted(request.scopes)),
                "code_challenge": request.code_challenge,
                "code_challenge_method": request.code_challenge_method,
                "iat": now,
                "nbf": now - 5,
                "exp": now + self.config.authorization_code_ttl_seconds,
                "nonce": random_token(16),
            },
            self.config.signing_key,
            token_type="oauth-request+jwt",
        )

    def _unseal_authorization_request(self, token: str) -> AuthorizationRequest:
        try:
            payload = verify_compact_token(
                token,
                self.config.signing_key,
                expected_type="oauth-request+jwt",
            )
        except TokenValidationError as exc:
            raise OAuthHTTPError(
                "invalid_request", "Authorization request expired"
            ) from exc
        return self._parse_authorization_request(
            {
                "response_type": str(payload.get("response_type") or ""),
                "client_id": str(payload.get("client_id") or ""),
                "redirect_uri": str(payload.get("redirect_uri") or ""),
                "state": str(payload.get("state") or ""),
                "resource": str(payload.get("resource") or ""),
                "scope": str(payload.get("scope") or ""),
                "code_challenge": str(payload.get("code_challenge") or ""),
                "code_challenge_method": str(
                    payload.get("code_challenge_method") or ""
                ),
            }
        )

    @staticmethod
    def _error_page(error: OAuthHTTPError) -> web.Response:
        body = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Доступ не предоставлен</title></head>
<body><main><h1>Доступ не предоставлен</h1><p>{html.escape(error.description)}</p></main></body></html>"""
        return web.Response(
            text=body,
            status=error.status,
            content_type="text/html",
            headers={
                "Cache-Control": "no-store",
                "Referrer-Policy": "no-referrer",
                "X-Content-Type-Options": "nosniff",
                "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
            },
        )

    async def handle_authorize_get(self, request: web.Request) -> web.Response:
        try:
            auth_request = self._parse_authorization_request(request.query)
        except OAuthHTTPError as exc:
            return self._error_page(exc)
        if self.partners is not None and auth_request.resource == self.config.partner_resource:
            return self._partner_authorize_page(auth_request)
        sealed = self._seal_authorization_request(auth_request)
        callback = urlsplit(auth_request.redirect_uri)
        callback_origin = (
            "https://chatgpt.com"
            if auth_request.client_id == self.config.oauth_client_id
            else f"http://127.0.0.1:{callback.port}"
        )
        scopes = ", ".join(sorted(auth_request.scopes))
        if auth_request.client_id == self.config.oauth_client_id:
            client_name = "ChatGPT"
        elif auth_request.client_id == self.config.codex_oauth_client_id:
            client_name = "Codex"
        else:
            client_name = "OpenCode"
        social = auth_request.scopes & SOCIAL_SCOPES
        capability_text = (
            "Будет предоставлен доступ к событиям, incident reports и операционным "
            "квитанциям."
        )
        social_warning = ""
        if social:
            granular = social & GRANULAR_SOCIAL_SCOPES
            legacy = social & LEGACY_SOCIAL_SCOPES
            if granular:
                capability_text += (
                    " Дополнительно запрошены перечисленные ниже granular social "
                    "capabilities для Telegram/VK."
                )
            if legacy:
                capability_text += (
                    " Также запрошены стабильные provider-level social scopes для "
                    "совместимости существующих ChatGPT/OpenCode клиентов."
                )
            warnings: list[str] = []
            if social & APPROVAL_REQUIRED_SOCIAL_SCOPES:
                warnings.append(
                    "Исходящие действия по явному поручению пользователя выполняются через "
                    "одноразовые prepare/commit без второго подтверждения; edit/delete требуют "
                    "отдельного внешнего подтверждения оператора."
                )
            if social & LEGACY_PUBLISH_SCOPES:
                warnings.append(
                    "Стабильные publish-scopes сохраняют старые allowlist-инструменты "
                    "с одноразовым prepare/commit ticket; новые типизированные исходящие "
                    "действия не требуют второго подтверждения, а edit/delete требуют."
                )
            if warnings:
                social_warning = (
                    "<p><strong>Внимание:</strong> " + " ".join(warnings) + "</p>"
                )
        body = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Подключить Events Bot</title>
<style>body{{font:16px system-ui;max-width:680px;margin:48px auto;padding:0 20px;color:#151515}}input{{width:100%;box-sizing:border-box;padding:12px;font-size:16px}}button{{margin-top:16px;padding:12px 18px;font-size:16px}}code{{word-break:break-word}}.muted{{color:#666}}</style></head>
<body><main><h1>Подключить Events Bot к {html.escape(client_name)}</h1>
<p>{html.escape(capability_text)}</p>
{social_warning}
<p class="muted">Scopes: <code>{html.escape(scopes)}</code></p>
<form method="post" autocomplete="off">
<input type="hidden" name="authorization_request" value="{html.escape(sealed, quote=True)}">
<label>Операторский bootstrap-токен<br><input type="password" name="operator_token" required autofocus autocomplete="off"></label>
<button type="submit">Предоставить доступ</button>
</form></main></body></html>"""
        return web.Response(
            text=body,
            content_type="text/html",
            headers={
                "Cache-Control": "no-store",
                "Pragma": "no-cache",
                "Referrer-Policy": "no-referrer",
                "X-Content-Type-Options": "nosniff",
                "Content-Security-Policy": (
                    "default-src 'none'; style-src 'unsafe-inline'; "
                    f"form-action 'self' {callback_origin}; "
                    "base-uri 'none'; frame-ancestors 'none'"
                ),
            },
        )

    async def handle_authorize_post(self, request: web.Request) -> web.Response:
        remote = request.remote or "unknown"
        if not self.failure_limiter.allow(
            f"oauth:{remote}",
            limit=self.config.oauth_failures_per_10_minutes,
            window_seconds=600,
        ):
            return self._error_page(
                OAuthHTTPError("temporarily_unavailable", "Слишком много попыток", 429)
            )
        try:
            form = await request.post()
            sealed = str(form.get("authorization_request") or "")
            supplied = str(form.get("operator_token") or "")
            auth_request = self._unseal_authorization_request(sealed)
            subject = SUBJECT
            partner_request = self.partners is not None and auth_request.resource == self.config.partner_resource
            if partner_request:
                try:
                    grant = self.partners.authenticate(auth_request.client_id, str(form.get("partner_login") or ""))
                    subject = grant.subject
                except ToolExecutionError:
                    await asyncio.sleep(0.2)
                    raise OAuthHTTPError("access_denied", "Неверные данные партнёрского входа", 403) from None
            elif not constant_time_equal(supplied, self.config.operator_token):
                await asyncio.sleep(0.2)
                await asyncio.to_thread(
                    self.store.audit,
                    action="authorize",
                    outcome="denied",
                    client_fingerprint=secret_hash(auth_request.client_id)[:12],
                    details={"remote": remote},
                )
                raise OAuthHTTPError(
                    "access_denied", "Неверный операторский токен", 403
                )
            code = random_token(32)
            now = int(time.time())
            await asyncio.to_thread(
                self.store.create_authorization_code,
                code=code,
                subject=subject,
                client_id=auth_request.client_id,
                redirect_uri=auth_request.redirect_uri,
                resource=auth_request.resource,
                scopes=auth_request.scopes,
                code_challenge=auth_request.code_challenge,
                expires_at=now + self.config.authorization_code_ttl_seconds,
            )
            await asyncio.to_thread(
                self.store.audit,
                action="authorize",
                outcome="granted",
                client_fingerprint=secret_hash(auth_request.client_id)[:12],
                subject=subject,
                details={"scopes": sorted(auth_request.scopes)},
            )
            target = self._append_query(
                auth_request.redirect_uri,
                {"code": code, "state": auth_request.state},
            )
            raise web.HTTPFound(target, headers={"Cache-Control": "no-store"})
        except web.HTTPException:
            raise
        except OAuthHTTPError as exc:
            return self._error_page(exc)

    @staticmethod
    def _append_query(url: str, values: Mapping[str, str]) -> str:
        parsed = urlsplit(url)
        query = list(parse_qsl(parsed.query, keep_blank_values=True))
        query.extend(values.items())
        return urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), "")
        )

    def _authenticate_client(
        self, request: web.Request, form: Mapping[str, Any]
    ) -> OAuthClient:
        form_client_id = str(form.get("client_id") or "")
        has_form_secret = "client_secret" in form
        client_secret = str(form.get("client_secret") or "")
        header = request.headers.get("Authorization", "")
        if header:
            if not header.startswith("Basic ") or has_form_secret:
                raise OAuthHTTPError(
                    "invalid_client", "Invalid client authentication", 401
                )
            try:
                raw = base64.b64decode(header[6:].strip(), validate=True).decode(
                    "utf-8"
                )
                header_id, header_secret = raw.split(":", 1)
            except Exception as exc:
                raise OAuthHTTPError(
                    "invalid_client", "Invalid client authentication", 401
                ) from exc
            if form_client_id and not constant_time_equal(form_client_id, header_id):
                raise OAuthHTTPError(
                    "invalid_client", "Invalid client authentication", 401
                )
            client = self._client_for_token_endpoint(header_id)
            if client.token_endpoint_auth_method == "none" or not constant_time_equal(
                header_secret, self.config.oauth_client_secret
            ):
                raise OAuthHTTPError(
                    "invalid_client", "Invalid client authentication", 401
                )
            return client

        client = self._client_for_token_endpoint(form_client_id)
        if client.token_endpoint_auth_method == "none":
            if has_form_secret:
                raise OAuthHTTPError(
                    "invalid_client", "Invalid client authentication", 401
                )
            return client
        if not constant_time_equal(client_secret, self.config.oauth_client_secret):
            raise OAuthHTTPError("invalid_client", "Invalid client authentication", 401)
        return client

    def _client_for_token_endpoint(self, client_id: str) -> OAuthClient:
        try:
            return self._client(client_id)
        except OAuthHTTPError as exc:
            # Token endpoints report unknown registrations as invalid_client,
            # not authorization-endpoint unauthorized_client.
            raise OAuthHTTPError(
                "invalid_client", "Invalid client authentication", 401
            ) from exc

    def _token_payload(
        self,
        *,
        subject: str,
        client_id: str,
        resource: str,
        scopes: frozenset[str],
    ) -> dict[str, Any]:
        self._validate_partner_grant(subject, client_id, resource, scopes)
        access_token, _ = mint_access_token(
            signing_key=self.config.signing_key,
            issuer=self.config.issuer,
            audience=resource,
            subject=subject,
            client_id=client_id,
            scopes=scopes,
            lifetime_seconds=self.config.access_ttl_seconds,
        )
        return {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": self.config.access_ttl_seconds,
            "scope": " ".join(sorted(scopes)),
        }

    async def handle_token(self, request: web.Request) -> web.Response:
        try:
            form = await request.post()
            client = self._authenticate_client(request, form)
            client_id = client.client_id
            grant_type = str(form.get("grant_type") or "")
            resource = self._validate_resource(str(form.get("resource") or ""), client)
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
                        client_id=client_id,
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
                if "offline_access" in grant.scopes:
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
                    client_fingerprint=secret_hash(client_id)[:12],
                    subject=grant.subject,
                    details={"grant_type": grant_type, "scopes": sorted(grant.scopes)},
                )
                return self._json_response(payload)
            if grant_type == "refresh_token":
                old_token = str(form.get("refresh_token") or "")
                if not old_token:
                    raise OAuthHTTPError("invalid_request", "Refresh token is required")
                requested_raw = str(form.get("scope") or "").strip()
                requested = (
                    self._parse_scopes(requested_raw, client) if requested_raw else None
                )
                new_refresh = random_token(48)
                try:
                    grant = await asyncio.to_thread(
                        self.store.rotate_refresh_token,
                        old_token=old_token,
                        new_token=new_refresh,
                        client_id=client_id,
                        resource=resource,
                        new_expires_at=now + self.config.refresh_ttl_seconds,
                        requested_scopes=requested,
                        allowed_scopes=client.allowed_scopes,
                    )
                except OAuthStoreError as exc:
                    error = str(exc)
                    raise OAuthHTTPError(error, "Refresh token is invalid") from exc
                payload = self._token_payload(
                    subject=grant.subject,
                    client_id=grant.client_id,
                    resource=grant.resource,
                    scopes=grant.scopes,
                )
                if "offline_access" in grant.scopes:
                    payload["refresh_token"] = new_refresh
                await asyncio.to_thread(
                    self.store.audit,
                    action="token",
                    outcome="refreshed",
                    client_fingerprint=secret_hash(client_id)[:12],
                    subject=grant.subject,
                    details={"grant_type": grant_type, "scopes": sorted(grant.scopes)},
                )
                return self._json_response(payload)
            raise OAuthHTTPError("unsupported_grant_type", "Unsupported grant type")
        except OAuthHTTPError as exc:
            response = self._json_response(
                {"error": exc.error, "error_description": exc.description},
                status=exc.status,
            )
            if exc.status == 401:
                response.headers["WWW-Authenticate"] = (
                    'Basic realm="private-events-mcp"'
                )
            return response

    def verify_authorization_header(
        self,
        header: str | None,
        *,
        expected_resource: str | None = None,
    ) -> AccessIdentity:
        if not header or not header.startswith("Bearer "):
            raise TokenValidationError("missing_token")
        token = header[7:].strip()
        if not token:
            raise TokenValidationError("missing_token")
        resource = expected_resource or self.config.resource
        identity = validate_access_token(
            token,
            signing_key=self.config.signing_key,
            issuer=self.config.issuer,
            audience=resource,
        )
        try:
            client = self._client(identity.client_id)
        except OAuthHTTPError as exc:
            raise TokenValidationError("wrong_client") from exc
        if resource not in client.allowed_resources:
            raise TokenValidationError("wrong_client")
        if not identity.scopes.issubset(client.allowed_scopes):
            raise TokenValidationError("wrong_scope")
        self._validate_partner_grant(identity.subject, identity.client_id, resource, identity.scopes, token=True)
        return identity

    def _validate_partner_grant(self, subject, client_id, resource, scopes, *, token=False):
        if resource != self.config.partner_resource:
            if subject.startswith("partner:"):
                if token:
                    raise TokenValidationError("wrong_resource")
                raise OAuthHTTPError("invalid_grant", "Partner audience required")
            return
        try:
            if self.partners is None:
                raise ToolExecutionError("PARTNER_ACCESS_REVOKED", "Partner access revoked")
            identity = AccessIdentity(subject, client_id, scopes, resource, "oauth-validation", int(time.time())+1)
            grant = self.partners.resolve(identity)
            if not scopes.issubset(grant.scopes):
                raise ToolExecutionError("PARTNER_SCOPE_DENIED", "Partner scope denied")
        except ToolExecutionError:
            if token:
                raise TokenValidationError("partner_access_revoked") from None
            raise OAuthHTTPError("invalid_grant", "Partner grant is no longer valid") from None

    def _partner_authorize_page(self, auth_request):
        grant = self.partners.get(client_id=auth_request.client_id)
        sealed = html.escape(self._seal_authorization_request(auth_request), quote=True)
        body = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1"><title>Партнёр EventsBot</title></head>
        <body><main><h1>Подключить {html.escape(grant.display_name)}</h1>
        <p>Организация: {html.escape(grant.organization_id)}. Только назначенные события и права.</p>
        <p>Запрашиваемые права: {html.escape(', '.join(sorted(auth_request.scopes)))}</p>
        <p>Приложение: {html.escape(auth_request.client_id)}.</p>
        <p>Возврат: {html.escape(auth_request.redirect_uri)}.</p>
        <form method="post"><input type="hidden" name="authorization_request" value="{sealed}">
        <label>Код партнёрского входа, выданный владельцем
        <input type="password" name="partner_login" required autocomplete="current-password"></label>
        <button type="submit">Подключить</button></form><p>Регистрация в Telegram не требуется.</p></main></body></html>"""
        return web.Response(text=body, content_type="text/html", headers={
            "Cache-Control": "no-store", "Pragma": "no-cache", "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY",
            "Content-Security-Policy": "default-src 'none'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
        })

    def challenge(
        self,
        *,
        error: str = "invalid_token",
        description: str = "Login required",
        resource_metadata_url: str | None = None,
    ) -> str:
        safe_description = description.replace('"', "'")[:180]
        metadata_url = resource_metadata_url or self.config.resource_metadata_url
        return (
            f'Bearer resource_metadata="{metadata_url}", '
            f'error="{error}", error_description="{safe_description}"'
        )

    async def handle_about(self, _request: web.Request) -> web.Response:
        return self._json_response(
            {
                "name": "Events Bot private MCP",
                "mode": "client_policy",
                "resources": [self.config.resource, self.config.codex_resource],
                "scopes": sorted(ALL_SCOPES),
                "core_provider_network_calls": False,
                "social_provider_operations": "injected_adapters_only",
                "database_mode": "sqlite_read_only",
            }
        )
