from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from typing import Any, Mapping
from urllib.parse import urlsplit

from aiohttp import web

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
from .tool_catalog import build_tools


logger = logging.getLogger(__name__)


class PrivateEventsMCPServer:
    def __init__(self, config: PrivateEventsMCPConfig) -> None:
        self.config = config
        self.oauth = PrivateOAuthServer(config)
        self.repository = EventsEvidenceRepository(config)
        self.protocol = MCPProtocol(
            build_tools(self.repository),
            cache_ttl_seconds=config.cache_ttl_seconds,
            challenge=self.oauth.challenge(),
            tool_timeout_seconds=max(1.0, config.query_timeout_ms / 1000.0 * 5.0),
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

    def _validate_transport_request(self, request: web.Request) -> None:
        host = (request.host or "").split(":", 1)[0].casefold()
        if host not in {self.public_host, "localhost", "127.0.0.1"}:
            raise web.HTTPBadRequest(text="invalid_host")
        origin = (request.headers.get("Origin") or "").strip().casefold()
        if origin and origin not in {self.public_origin, "https://chatgpt.com"}:
            raise web.HTTPForbidden(text="invalid_origin")

    def _identity(self, request: web.Request) -> tuple[AccessIdentity | None, bool]:
        header = request.headers.get("Authorization")
        if not header:
            return None, False
        try:
            return self.oauth.verify_authorization_header(header), False
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
            if invalid_bearer:
                return self._plain_error(
                    401,
                    "invalid_token",
                    correlation_id=correlation,
                    authenticate=self.oauth.challenge(
                        error="invalid_token", description="Access token is invalid or expired"
                    ),
                )
            if identity is not None:
                # Bind the bucket to the OAuth principal, not an individual
                # access-token jti, so refresh rotation cannot reset the RPM.
                principal = hashlib.sha256(
                    f"{identity.client_id}\0{identity.subject}".encode("utf-8")
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
                    response_payload = await self.protocol.dispatch(request_message, identity)
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

    def register(self, app: web.Application) -> None:
        paths = self.config
        app.router.add_post(paths.mcp_path, self.handle_mcp_post)
        app.router.add_get(paths.mcp_path, self.handle_mcp_get)
        app.router.add_options(paths.mcp_path, self.handle_options)
        app.router.add_get(
            paths.protected_resource_metadata_path,
            self.oauth.handle_protected_resource_metadata,
        )
        app.router.add_get(
            paths.authorization_server_metadata_path,
            self.oauth.handle_authorization_server_metadata,
        )
        app.router.add_get(paths.oauth_authorize_path, self.oauth.handle_authorize_get)
        app.router.add_post(paths.oauth_authorize_path, self.oauth.handle_authorize_post)
        app.router.add_post(paths.oauth_token_path, self.handle_oauth_token)
        app.router.add_get(paths.about_path, self.oauth.handle_about)
        app[SERVER_APP_KEY] = self
        app[ENDPOINT_FINGERPRINT_APP_KEY] = hashlib.sha256(
            paths.resource.encode("utf-8")
        ).hexdigest()[:12]


SERVER_APP_KEY = web.AppKey("private_events_mcp_server", PrivateEventsMCPServer)
ENDPOINT_FINGERPRINT_APP_KEY = web.AppKey("private_events_mcp_endpoint_fingerprint", str)
