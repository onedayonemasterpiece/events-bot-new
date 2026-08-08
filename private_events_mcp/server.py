from __future__ import annotations

import hashlib
import hmac
import html
import json
import logging
import re
import secrets
import time
import uuid
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from aiohttp import web

from .access_policy import CHATGPT_MAX_SCOPES, CODEX_MAX_SCOPES
from .config import PrivateEventsMCPConfig
from .crypto import AccessIdentity, TokenValidationError
from .limits import AdmissionController, RateLimitExceeded
from .oauth import PrivateOAuthServer
from .protocol import (
    LATEST_LEGACY_PROTOCOL,
    SUPPORTED_LEGACY_PROTOCOLS,
    MCPProtocol,
    UnsupportedProtocolVersion,
)
from .repository import EventsEvidenceRepository
from .social import SocialAdapter, TargetAliasPolicy, build_social_tools
from .social_workspace_runtime import (
    SocialBudgetLimits,
    SocialWorkspaceAdapter,
    SocialWorkspaceRuntime,
    SocialWorkspaceRuntimeError,
)
from .social_workspace_tools import build_social_workspace_tools
from .tool_catalog import build_tools

logger = logging.getLogger(__name__)
_PREPARATION_RE = re.compile(r"^prep_[A-Za-z0-9_-]{24,160}$")
_DIGEST_RE = re.compile(r"^[a-f0-9]{64}$")


class PrivateEventsMCPServer:
    def __init__(
        self,
        config: PrivateEventsMCPConfig,
        *,
        social_adapters: Mapping[str, SocialAdapter] | None = None,
        social_workspace_adapters: Mapping[str, SocialWorkspaceAdapter] | None = None,
    ) -> None:
        self.config = config
        self.oauth = PrivateOAuthServer(config)
        self.repository = EventsEvidenceRepository(config)
        self.target_policy = TargetAliasPolicy.from_json(config.social_targets_json)
        read_tools = build_tools(self.repository)
        social_tools = build_social_tools(
            store=self.oauth.store,
            policy=self.target_policy,
            adapters=social_adapters or {},
            ticket_ttl_seconds=config.social_ticket_ttl_seconds,
            provider_timeout_seconds=config.social_provider_timeout_seconds,
            publish_attempts_per_day=config.social_publish_attempts_per_day,
        ) if social_adapters and not config.universal_social_enabled else ()
        self.social_workspace: SocialWorkspaceRuntime | None = None
        workspace_tools = ()
        if config.universal_social_enabled:
            adapters = dict(social_workspace_adapters or {})
            expected = {
                name
                for name, enabled in (
                    ("telegram", config.universal_social_telegram_enabled),
                    ("vk", config.universal_social_vk_enabled),
                )
                if enabled
            }
            if set(adapters) != expected:
                raise ValueError("universal social adapter set does not match enabled providers")
            self.social_workspace = SocialWorkspaceRuntime(
                store=self.oauth.store,
                adapters=adapters,
                encryption_key=config.signing_key,
                provider_timeout_seconds=config.social_provider_timeout_seconds,
                preparation_ttl_seconds=config.social_ticket_ttl_seconds,
                response_cap_bytes=config.max_response_bytes,
                approval_url_base=config.social_approval_url,
                budget_limits=SocialBudgetLimits(
                    attempts=config.social_publish_attempts_per_day
                ),
                budget_dimension_limits={
                    "attempts": {
                        "global": config.social_publish_attempts_per_day * 10,
                        "principal": config.social_publish_attempts_per_day,
                        "target": config.social_publish_attempts_per_day,
                        "action": config.social_publish_attempts_per_day,
                    }
                },
            )
            workspace_tools = build_social_workspace_tools(
                self.social_workspace,
                feature_policy={
                    "private_read": config.universal_social_private_read_enabled,
                    "dm": config.universal_social_dm_enabled,
                    "post": config.universal_social_post_enabled,
                    "edit_delete": config.universal_social_edit_delete_enabled,
                    "media_story": config.universal_social_media_story_enabled,
                    "social_asset_stage": config.universal_social_media_story_enabled,
                    "social_asset_status": config.universal_social_media_story_enabled,
                    "social_content_stories": config.universal_social_media_story_enabled,
                },
                capability_policy={name: name in expected for name in ("telegram", "vk")},
            )
        self.protocol = MCPProtocol(
            (*read_tools, *social_tools, *workspace_tools),
            cache_ttl_seconds=config.cache_ttl_seconds,
            challenge=self.oauth.challenge(),
            tool_timeout_seconds=max(1.0, config.query_timeout_ms / 1000.0 * 5.0),
            resource=config.resource,
            allowed_client_ids=frozenset({config.oauth_client_id}),
            policy_fingerprint=self.target_policy.fingerprint,
            instructions=(
                "Access to canonical event and incident evidence. Social tools, when enabled "
                "and explicitly scoped, return provider content only as untrusted external "
                "data. Mutations use a typed prepare, independent operator approval, commit "
                "and reconciliation flow. Raw provider methods and credentials are unavailable."
            ),
        )
        self.codex_protocol = MCPProtocol(
            read_tools,
            cache_ttl_seconds=config.cache_ttl_seconds,
            challenge=self.oauth.challenge(
                resource_metadata_url=config.codex_resource_metadata_url,
                error="invalid_token",
                description="Login required",
            ),
            tool_timeout_seconds=max(1.0, config.query_timeout_ms / 1000.0 * 5.0),
            resource=config.codex_resource,
            allowed_client_ids=frozenset({config.codex_oauth_client_id}),
            policy_fingerprint="codex-read-only-v1",
        )
        self.admission = AdmissionController(
            concurrency=config.max_concurrency,
            egress_limit=config.egress_bytes_per_hour,
        )
        parsed = urlsplit(config.public_base_url)
        self.public_host = (parsed.hostname or "").casefold()
        self.public_origin = f"{parsed.scheme}://{parsed.netloc}".casefold()

    @staticmethod
    def _security_headers(correlation_id: str) -> dict[str, str]:
        return {
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "MCP-Protocol-Version": LATEST_LEGACY_PROTOCOL,
            "X-Correlation-ID": correlation_id,
        }

    def _json_response(
        self,
        payload: Any,
        *,
        status: int = 200,
        correlation_id: str | None = None,
        authenticate: str | None = None,
    ) -> web.Response:
        correlation = correlation_id or uuid.uuid4().hex
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        response = web.Response(
            status=status,
            body=encoded,
            content_type="application/json",
            headers=self._security_headers(correlation),
        )
        if authenticate:
            response.headers["WWW-Authenticate"] = authenticate
        if len(encoded) >= 2048:
            response.enable_compression()
        return response

    def _plain_error(
        self,
        status: int,
        code: str,
        *,
        correlation_id: str | None = None,
        authenticate: str | None = None,
    ) -> web.Response:
        return self._json_response(
            {"error": code},
            status=status,
            correlation_id=correlation_id,
            authenticate=authenticate,
        )

    def _seal_approval_state(self, payload: Mapping[str, Any]) -> str:
        raw = json.dumps(
            dict(payload), sort_keys=True, separators=(",", ":")
        ).encode()
        encoded = urlsafe_b64encode(raw).rstrip(b"=").decode()
        signature = hmac.new(
            self.config.signing_key.encode(), encoded.encode(), hashlib.sha256
        ).hexdigest()
        return f"{encoded}.{signature}"

    def _unseal_approval_state(self, value: str) -> dict[str, Any]:
        try:
            encoded, signature = value.split(".", 1)
            expected = hmac.new(
                self.config.signing_key.encode(), encoded.encode(), hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError
            raw = urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
            payload = json.loads(raw)
        except Exception:  # noqa: BLE001 - signed browser state failures are normalized
            raise web.HTTPBadRequest(text="invalid_approval_state") from None
        if (
            not isinstance(payload, dict)
            or not _PREPARATION_RE.fullmatch(str(payload.get("preparation_ref") or ""))
            or not _DIGEST_RE.fullmatch(str(payload.get("action_digest") or ""))
            or not isinstance(payload.get("csrf"), str)
            or len(payload["csrf"]) < 16
            or type(payload.get("expires_at")) is not int
            or payload["expires_at"] <= int(time.time())
        ):
            raise web.HTTPBadRequest(text="invalid_approval_state")
        return payload

    @staticmethod
    def _approval_headers() -> dict[str, str]:
        return {
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Content-Security-Policy": (
                "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
                "base-uri 'none'; frame-ancestors 'none'"
            ),
        }

    def _approval_page(
        self,
        *,
        title: str,
        state: str | None = None,
        preview: Mapping[str, Any] | None = None,
        approved: bool = False,
        error: str | None = None,
        status: int = 200,
    ) -> web.Response:
        safe_title = html.escape(title)
        body = [
            "<!doctype html><html lang='ru'><meta charset='utf-8'>",
            f"<title>{safe_title}</title>",
            (
                "<style>body{font:18px system-ui;max-width:900px;margin:48px auto;padding:0 20px}"
                "pre{white-space:pre-wrap;background:#f5f5f5;padding:16px}"
                "input,button{font:inherit;padding:10px}"
                "input{width:100%;box-sizing:border-box}</style>"
            ),
            f"<h1>{safe_title}</h1>",
        ]
        if error:
            body.append(f"<p><strong>{html.escape(error)}</strong></p>")
        if approved:
            body.append("<p>Действие подтверждено. Можно вернуться в ChatGPT.</p>")
        elif preview is None and state:
            body.extend(
                [
                    "<p>Сначала подтвердите личность оператора. Текст действия появится только после проверки.</p>",
                    f"<form method='post'><input type='hidden' name='state' value='{html.escape(state)}'>",
                    "<input type='hidden' name='phase' value='preview'>",
                    "<label>Токен подтверждения<input type='password' name='operator_token' required autocomplete='current-password'></label>",
                    "<p><button type='submit'>Показать точное действие</button></p></form>",
                ]
            )
        elif preview is not None and state:
            rendered = html.escape(
                json.dumps(preview, ensure_ascii=False, sort_keys=True, indent=2)
            )
            body.extend(
                [
                    "<p><strong>Проверьте адресата, действие и точное содержимое.</strong></p>",
                    f"<pre>{rendered}</pre>",
                    f"<form method='post'><input type='hidden' name='state' value='{html.escape(state)}'>",
                    "<input type='hidden' name='phase' value='approve'>",
                    "<p><button type='submit'>Подтвердить ровно это действие</button></p></form>",
                ]
            )
        body.append("</html>")
        return web.Response(
            status=status,
            text="".join(body),
            content_type="text/html",
            headers=self._approval_headers(),
        )

    def _approval_error(self, status: int) -> web.Response:
        return self._approval_page(
            title="Запрос подтверждения отклонён",
            error="Проверьте ссылку или начните подготовку действия заново.",
            status=status,
        )

    async def handle_social_approval_get(self, request: web.Request) -> web.Response:
        try:
            if self.social_workspace is None:
                raise web.HTTPNotFound()
            prep = str(request.query.get("preparation_ref") or "")
            digest = str(request.query.get("action_digest") or "")
            if not _PREPARATION_RE.fullmatch(prep) or not _DIGEST_RE.fullmatch(digest):
                raise web.HTTPBadRequest(text="invalid_approval_request")
            state = self._seal_approval_state(
                {
                    "preparation_ref": prep,
                    "action_digest": digest,
                    "csrf": secrets.token_urlsafe(24),
                    "expires_at": int(time.time()) + 300,
                }
            )
            return self._approval_page(title="Подтверждение social-действия", state=state)
        except web.HTTPException as exc:
            return self._approval_error(exc.status)
        except Exception:
            logger.exception("private_events_mcp approval GET failed")
            return self._approval_error(500)

    async def handle_social_approval_post(self, request: web.Request) -> web.Response:
        try:
            return await self._handle_social_approval_post(request)
        except web.HTTPException as exc:
            return self._approval_error(exc.status)
        except SocialWorkspaceRuntimeError:
            return self._approval_error(400)
        except Exception:
            logger.exception("private_events_mcp approval POST failed")
            return self._approval_error(500)

    async def _handle_social_approval_post(self, request: web.Request) -> web.Response:
        if self.social_workspace is None:
            raise web.HTTPNotFound()
        if request.content_length is not None and request.content_length > 16 * 1024:
            raise web.HTTPRequestEntityTooLarge(max_size=16 * 1024, actual_size=request.content_length)
        form = await request.post()
        state_raw = str(form.get("state") or "")
        state = self._unseal_approval_state(state_raw)
        phase = str(form.get("phase") or "")
        if phase == "preview":
            key = f"social-approval:{request.remote or 'unknown'}"
            if not self.admission.rate.allow(
                key,
                limit=self.config.oauth_failures_per_10_minutes,
                window_seconds=600,
            ):
                raise web.HTTPTooManyRequests(text="approval_rate_limited")
            provided = str(form.get("operator_token") or "")
            if not hmac.compare_digest(provided, self.config.social_approval_token):
                return self._approval_page(
                    title="Подтверждение social-действия",
                    state=state_raw,
                    error="Неверный токен подтверждения",
                )
            preview = self.social_workspace.approval_preview(
                preparation_ref=state["preparation_ref"],
                action_digest=state["action_digest"],
            )
            cookie = self._seal_approval_state(
                {
                    **state,
                    "operator": hashlib.sha256(provided.encode()).hexdigest(),
                }
            )
            response = self._approval_page(
                title="Подтверждение social-действия",
                state=state_raw,
                preview=preview,
            )
            response.set_cookie(
                "private_events_social_approval",
                cookie,
                secure=True,
                httponly=True,
                samesite="Strict",
                max_age=300,
                path=self.config.social_approval_path,
            )
            return response
        if phase != "approve":
            raise web.HTTPBadRequest(text="invalid_approval_phase")
        cookie = self._unseal_approval_state(
            request.cookies.get("private_events_social_approval", "")
        )
        if any(
            not hmac.compare_digest(str(cookie.get(key) or ""), str(state.get(key) or ""))
            for key in ("preparation_ref", "action_digest", "csrf")
        ) or not isinstance(cookie.get("operator"), str):
            raise web.HTTPForbidden(text="approval_session_mismatch")
        self.social_workspace.approve_preparation(
            preparation_ref=state["preparation_ref"],
            operator_principal=cookie["operator"],
            operator_nonce=state["csrf"],
        )
        response = self._approval_page(
            title="Действие подтверждено", approved=True
        )
        response.del_cookie(
            "private_events_social_approval", path=self.config.social_approval_path
        )
        return response

    def _validate_transport_request(self, request: web.Request) -> None:
        host = (request.host or "").split(":", 1)[0].casefold()
        if host not in {self.public_host, "localhost", "127.0.0.1"}:
            raise web.HTTPBadRequest(text="invalid_host")
        origin = (request.headers.get("Origin") or "").strip().casefold()
        if origin and origin not in {self.public_origin, "https://chatgpt.com"}:
            raise web.HTTPForbidden(text="invalid_origin")

    def _endpoint_contract(self, path: str) -> tuple[str, str, MCPProtocol, frozenset[str], str]:
        if path == self.config.codex_mcp_path:
            return (
                self.config.codex_resource,
                self.config.codex_oauth_client_id,
                self.codex_protocol,
                CODEX_MAX_SCOPES,
                self.config.codex_resource_metadata_url,
            )
        return (
            self.config.resource,
            self.config.oauth_client_id,
            self.protocol,
            CHATGPT_MAX_SCOPES,
            self.config.resource_metadata_url,
        )

    def _identity(self, request: web.Request) -> tuple[AccessIdentity | None, bool]:
        header = request.headers.get("Authorization")
        if not header:
            return None, False
        try:
            resource, client_id, _protocol, max_scopes, _metadata = self._endpoint_contract(
                request.path
            )
            identity = self.oauth.verify_authorization_header(
                header,
                expected_resource=resource,
            )
            if identity.client_id != client_id or not identity.scopes.issubset(max_scopes):
                raise TokenValidationError("wrong_client_or_scope")
            return identity, False
        except TokenValidationError:
            return None, True

    async def handle_mcp_post(self, request: web.Request) -> web.Response:
        correlation = uuid.uuid4().hex
        started = time.monotonic()
        identity: AccessIdentity | None = None
        method = ""
        try:
            self._validate_transport_request(request)
            protocol_header = (request.headers.get("MCP-Protocol-Version") or "").strip()
            if protocol_header and protocol_header not in SUPPORTED_LEGACY_PROTOCOLS:
                return self._plain_error(
                    400,
                    "unsupported_mcp_protocol_version",
                    correlation_id=correlation,
                )
            if request.content_length is not None and request.content_length > self.config.max_request_bytes:
                return self._plain_error(413, "request_too_large", correlation_id=correlation)
            content_type = (request.content_type or "").casefold()
            if content_type != "application/json":
                return self._plain_error(415, "application_json_required", correlation_id=correlation)
            accept = (request.headers.get("Accept") or "*/*").casefold()
            if not any(item in accept for item in ("application/json", "text/event-stream", "*/*")):
                return self._plain_error(406, "unsupported_accept", correlation_id=correlation)
            identity, invalid_bearer = self._identity(request)
            resource, _client_id, protocol, _max_scopes, metadata_url = self._endpoint_contract(
                request.path
            )
            if invalid_bearer:
                return self._plain_error(
                    401,
                    "invalid_token",
                    correlation_id=correlation,
                    authenticate=self.oauth.challenge(
                        resource_metadata_url=metadata_url,
                        error="invalid_token",
                        description="Access token is invalid or expired",
                    ),
                )
            if identity is not None:
                # Bind the bucket to the OAuth principal, not an individual
                # access-token jti, so refresh rotation cannot reset the RPM.
                principal = hashlib.sha256(
                    f"{resource}\0{identity.client_id}\0{identity.subject}".encode()
                ).hexdigest()[:24]
                rate_key = f"auth:{principal}"
            else:
                rate_key = f"anon:{request.remote or 'unknown'}"
            rate_limit = (
                self.config.authenticated_requests_per_minute
                if identity is not None
                else self.config.anonymous_requests_per_minute
            )
            if not self.admission.rate.allow(rate_key, limit=rate_limit, window_seconds=60):
                response = self._plain_error(429, "rate_limited", correlation_id=correlation)
                response.headers["Retry-After"] = "15"
                return response
            async with self.admission:
                body = await request.read()
                if len(body) > self.config.max_request_bytes:
                    return self._plain_error(413, "request_too_large", correlation_id=correlation)
                try:
                    payload = json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    return self._plain_error(400, "invalid_json", correlation_id=correlation)
                # MCP 2025-06-18 and later require one JSON-RPC message per
                # HTTP POST. Rejecting batches also prevents one request from
                # multiplying database work while consuming a single rate slot.
                if isinstance(payload, list):
                    return self._plain_error(
                        400,
                        "jsonrpc_batch_not_supported",
                        correlation_id=correlation,
                    )
                if not isinstance(payload, Mapping):
                    return self._plain_error(400, "jsonrpc_object_required", correlation_id=correlation)
                request_message = payload
                method = str(request_message.get("method") or "")[:100]
                try:
                    response_payload = await protocol.dispatch(request_message, identity)
                except UnsupportedProtocolVersion:
                    return self._plain_error(
                        400,
                        "unsupported_mcp_protocol_version",
                        correlation_id=correlation,
                    )
                if response_payload is None:
                    return web.Response(
                        status=202,
                        headers=self._security_headers(correlation),
                    )
                encoded = json.dumps(
                    response_payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                if len(encoded) > self.config.max_response_bytes:
                    request_id = request_message.get("id")
                    response_payload = {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32001,
                            "message": "Bounded response exceeded; narrow the query",
                        },
                    }
                    encoded = json.dumps(response_payload, separators=(",", ":")).encode("utf-8")
                if not self.admission.egress.reserve(len(encoded)):
                    return self._plain_error(429, "hourly_egress_budget_exhausted", correlation_id=correlation)
                response = web.Response(
                    status=200,
                    body=encoded,
                    content_type="application/json",
                    headers=self._security_headers(correlation),
                )
                if len(encoded) >= 2048:
                    response.enable_compression()
                return response
        except RateLimitExceeded:
            response = self._plain_error(503, "server_busy", correlation_id=correlation)
            response.headers["Retry-After"] = "2"
            return response
        except web.HTTPException as exc:
            return self._plain_error(exc.status, exc.text or "request_rejected", correlation_id=correlation)
        except Exception:
            logger.exception("private_events_mcp request failed correlation_id=%s", correlation)
            return self._plain_error(500, "internal_error", correlation_id=correlation)
        finally:
            logger.info(
                "private_events_mcp request correlation_id=%s method=%s auth=%s duration_ms=%s",
                correlation,
                method,
                "oauth" if identity is not None else "anonymous",
                int((time.monotonic() - started) * 1000),
            )

    async def handle_mcp_get(self, request: web.Request) -> web.Response:
        try:
            self._validate_transport_request(request)
        except web.HTTPException as exc:
            return self._plain_error(exc.status, exc.text or "request_rejected")
        response = self._plain_error(405, "stateless_json_post_only")
        response.headers["Allow"] = "POST"
        return response

    async def handle_oauth_token(self, request: web.Request) -> web.Response:
        """Apply the same fail-closed admission boundary to token rotation."""

        correlation = uuid.uuid4().hex
        try:
            self._validate_transport_request(request)
            if (
                request.content_length is not None
                and request.content_length > self.config.max_request_bytes
            ):
                return self._plain_error(413, "request_too_large", correlation_id=correlation)
            if (request.content_type or "").casefold() != "application/x-www-form-urlencoded":
                return self._plain_error(415, "form_urlencoded_required", correlation_id=correlation)
            rate_key = f"oauth-token:{request.remote or 'unknown'}"
            if not self.admission.rate.allow(
                rate_key,
                limit=self.config.anonymous_requests_per_minute,
                window_seconds=60,
            ):
                response = self._plain_error(429, "rate_limited", correlation_id=correlation)
                response.headers["Retry-After"] = "15"
                return response
            async with self.admission:
                body = await request.read()
                if len(body) > self.config.max_request_bytes:
                    return self._plain_error(413, "request_too_large", correlation_id=correlation)
                return await self.oauth.handle_token(request)
        except RateLimitExceeded:
            response = self._plain_error(503, "server_busy", correlation_id=correlation)
            response.headers["Retry-After"] = "2"
            return response
        except web.HTTPException as exc:
            return self._plain_error(
                exc.status,
                exc.text or "request_rejected",
                correlation_id=correlation,
            )
        except Exception:
            logger.exception("private_events_mcp token request failed correlation_id=%s", correlation)
            return self._plain_error(500, "internal_error", correlation_id=correlation)

    async def handle_options(self, request: web.Request) -> web.Response:
        origin = (request.headers.get("Origin") or "").strip().casefold()
        if origin not in {self.public_origin, "https://chatgpt.com"}:
            return self._plain_error(403, "invalid_origin")
        response = web.Response(status=204, headers=self._security_headers(uuid.uuid4().hex))
        response.headers.update(
            {
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Authorization, Content-Type, MCP-Protocol-Version",
                "Access-Control-Max-Age": "600",
                "Vary": "Origin",
            }
        )
        return response

    async def handle_codex_protected_resource_metadata(
        self, _request: web.Request
    ) -> web.Response:
        return self.oauth._json_response(
            self.oauth.protected_resource_metadata_for(self.config.codex_resource)
        )

    async def handle_chatgpt_protected_resource_metadata(
        self, _request: web.Request
    ) -> web.Response:
        return self.oauth._json_response(
            self.oauth.protected_resource_metadata_for(self.config.resource)
        )

    def register(self, app: web.Application) -> None:
        paths = self.config
        app.router.add_post(paths.mcp_path, self.handle_mcp_post)
        app.router.add_get(paths.mcp_path, self.handle_mcp_get)
        app.router.add_options(paths.mcp_path, self.handle_options)
        app.router.add_post(paths.codex_mcp_path, self.handle_mcp_post)
        app.router.add_get(paths.codex_mcp_path, self.handle_mcp_get)
        app.router.add_options(paths.codex_mcp_path, self.handle_options)
        app.router.add_get(
            paths.protected_resource_metadata_path,
            self.handle_chatgpt_protected_resource_metadata,
        )
        app.router.add_get(
            paths.codex_protected_resource_metadata_path,
            self.handle_codex_protected_resource_metadata,
        )
        app.router.add_get(
            paths.authorization_server_metadata_path,
            self.oauth.handle_authorization_server_metadata,
        )
        app.router.add_get(paths.oauth_authorize_path, self.oauth.handle_authorize_get)
        app.router.add_post(paths.oauth_authorize_path, self.oauth.handle_authorize_post)
        app.router.add_post(paths.oauth_token_path, self.handle_oauth_token)
        app.router.add_get(paths.about_path, self.oauth.handle_about)
        if self.social_workspace is not None:
            app.router.add_get(
                paths.social_approval_path, self.handle_social_approval_get
            )
            app.router.add_post(
                paths.social_approval_path, self.handle_social_approval_post
            )
        app[SERVER_APP_KEY] = self
        app[ENDPOINT_FINGERPRINT_APP_KEY] = hashlib.sha256(
            paths.resource.encode("utf-8")
        ).hexdigest()[:12]


SERVER_APP_KEY = web.AppKey("private_events_mcp_server", PrivateEventsMCPServer)
ENDPOINT_FINGERPRINT_APP_KEY = web.AppKey("private_events_mcp_endpoint_fingerprint", str)
