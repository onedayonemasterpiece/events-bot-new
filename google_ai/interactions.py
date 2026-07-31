"""Strict async REST client for Google's managed-agent Interactions API.

The adapter intentionally does not use the GenerateContent SDK path.  Every
interaction-creating POST owns a distinct shared-ledger lease; GET polling and
environment snapshot downloads do not consume an interaction RPD reservation.
Provider terminal state and downstream semantic validity are separate concepts.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import re
import ssl
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Optional, Protocol, Sequence

from google_ai.client import ExternalCallLease, GoogleAIClient, UsageInfo
from google_ai.exceptions import ProviderError, ReservationError


ANTIGRAVITY_AGENT = "antigravity-preview-05-2026"
INTERACTIONS_API_REVISION = "2026-05-20"
DEFAULT_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"

INTERACTION_STATUSES = frozenset(
    {
        "queued",
        "in_progress",
        "requires_action",
        "completed",
        "failed",
        "cancelled",
        "incomplete",
        "budget_exceeded",
    }
)
ACTIVE_INTERACTION_STATUSES = frozenset({"queued", "in_progress"})
TERMINAL_INTERACTION_STATUSES = INTERACTION_STATUSES - ACTIVE_INTERACTION_STATUSES

_RESOURCE_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,512}$")


class InteractionsProtocolError(ProviderError):
    """The provider response did not satisfy the documented schema."""


class InteractionDeadlineExceeded(TimeoutError):
    """A background interaction did not stop before the caller deadline."""

    def __init__(
        self,
        interaction_id: str,
        *,
        cancel_result: Optional["ProviderInteraction"] = None,
        cancel_error: Optional[Exception] = None,
    ) -> None:
        super().__init__(f"interaction deadline exceeded: {interaction_id}")
        self.interaction_id = interaction_id
        self.cancel_result = cancel_result
        self.cancel_error = cancel_error


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


class AsyncHTTPTransport(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Optional[bytes],
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> HTTPResponse: ...


class _SafeGoogleRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow only HTTPS Google download redirects and never forward the key."""

    _ALLOWED_SUFFIXES = (".googleapis.com", ".googleusercontent.com")

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        old_host = (urllib.parse.urlsplit(req.full_url).hostname or "").lower()
        parsed = urllib.parse.urlsplit(newurl)
        new_host = (parsed.hostname or "").lower()
        if parsed.scheme != "https" or not any(
            new_host == suffix[1:] or new_host.endswith(suffix)
            for suffix in self._ALLOWED_SUFFIXES
        ):
            raise urllib.error.HTTPError(
                newurl,
                code,
                "unsafe redirect target",
                headers,
                fp,
            )
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and new_host != old_host:
            redirected.headers.pop("X-goog-api-key", None)
            redirected.unredirected_hdrs.pop("X-goog-api-key", None)
        return redirected


class UrllibAsyncHTTPTransport:
    """Dependency-free async transport backed by ``asyncio.to_thread``."""

    def __init__(self) -> None:
        context = ssl.create_default_context()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=context),
            _SafeGoogleRedirectHandler(),
        )

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Optional[bytes],
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> HTTPResponse:
        return await asyncio.to_thread(
            self._request_sync,
            method,
            url,
            dict(headers),
            body,
            timeout_seconds,
            max_response_bytes,
        )

    def _request_sync(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: Optional[bytes],
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> HTTPResponse:
        request = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            response = self._opener.open(request, timeout=timeout_seconds)
            with response:
                payload = response.read(max_response_bytes + 1)
                if len(payload) > max_response_bytes:
                    raise ProviderError(
                        error_type="response_too_large",
                        error_message="provider response exceeded the configured byte limit",
                    )
                return HTTPResponse(
                    status=int(response.status),
                    headers={str(k): str(v) for k, v in response.headers.items()},
                    body=payload,
                )
        except urllib.error.HTTPError as exc:
            payload = exc.read(min(max_response_bytes, 64 * 1024))
            return HTTPResponse(
                status=int(exc.code),
                headers={str(k): str(v) for k, v in (exc.headers or {}).items()},
                body=payload,
            )


@dataclass(frozen=True)
class ProviderInteraction:
    """One provider response; this object makes no semantic-quality claim."""

    id: str
    provider_status: str
    environment_id: Optional[str]
    steps: tuple[dict[str, Any], ...]
    usage: UsageInfo
    raw: dict[str, Any] = field(repr=False, compare=False)
    lease: ExternalCallLease = field(repr=False, compare=False)

    @property
    def status(self) -> str:
        """Compatibility alias that still names the provider status only."""

        return self.provider_status

    @property
    def is_active(self) -> bool:
        return self.provider_status in ACTIVE_INTERACTION_STATUSES

    @property
    def is_terminal(self) -> bool:
        return self.provider_status in TERMINAL_INTERACTION_STATUSES

    @property
    def output_text(self) -> str:
        pieces: list[str] = []
        for step in self.steps:
            if step.get("type") != "model_output":
                continue
            for content in step.get("content") or []:
                if isinstance(content, dict) and content.get("type") == "text":
                    text = content.get("text")
                    if isinstance(text, str):
                        pieces.append(text)
        return "".join(pieces)

    def to_checkpoint(self) -> dict[str, Any]:
        """Minimal JSON-safe state needed to resume polling after a restart."""

        return {
            "id": self.id,
            "provider_status": self.provider_status,
            "environment_id": self.environment_id,
            "lease": self.lease.to_dict(),
        }

    @classmethod
    def from_checkpoint(cls, value: Mapping[str, Any]) -> "ProviderInteraction":
        """Restore a pollable handle; the next GET refreshes steps and usage."""

        status = str(value["provider_status"])
        if status not in INTERACTION_STATUSES:
            raise ValueError("checkpoint contains an unknown provider status")
        lease_raw = value.get("lease")
        if not isinstance(lease_raw, dict):
            raise ValueError("checkpoint lease must be an object")
        environment_id = value.get("environment_id")
        interaction_id = str(value["id"])
        if not _RESOURCE_ID_RE.fullmatch(interaction_id):
            raise ValueError("checkpoint contains an invalid interaction id")
        if environment_id is not None and not _RESOURCE_ID_RE.fullmatch(
            str(environment_id)
        ):
            raise ValueError("checkpoint contains an invalid environment id")
        return cls(
            id=interaction_id,
            provider_status=status,
            environment_id=(
                str(environment_id) if environment_id is not None else None
            ),
            steps=(),
            usage=UsageInfo(),
            raw={},
            lease=ExternalCallLease.from_dict(lease_raw),
        )


class AntigravityInteractionsClient:
    """Quota-accounted client for ``antigravity-preview-05-2026``.

    ``key_envs`` is mandatory.  The pool is never widened to a default key and
    never spills into the generic overflow/fallback paths in ``GoogleAIClient``.
    """

    def __init__(
        self,
        rate_limiter: GoogleAIClient,
        *,
        key_envs: Sequence[str],
        transport: Optional[AsyncHTTPTransport] = None,
        base_url: str = DEFAULT_BASE_URL,
        request_timeout_seconds: float = 60.0,
        poll_interval_seconds: float = 5.0,
        max_json_response_bytes: int = 16 * 1024 * 1024,
        max_snapshot_bytes: int = 256 * 1024 * 1024,
        max_snapshot_unpacked_bytes: int = 512 * 1024 * 1024,
        max_snapshot_members: int = 20_000,
        cancel_path_style: str = "path",
    ) -> None:
        normalized = GoogleAIClient._normalize_overflow_envs(key_envs)
        if not normalized:
            raise ValueError("key_envs must be an explicit non-empty pool")
        if not base_url.startswith("https://"):
            raise ValueError("base_url must use HTTPS")
        if cancel_path_style not in {"path", "colon"}:
            raise ValueError("cancel_path_style must be path or colon")
        self.rate_limiter = rate_limiter
        self.key_envs = tuple(normalized)
        self.transport = transport or UrllibAsyncHTTPTransport()
        self.base_url = base_url.rstrip("/")
        self.request_timeout_seconds = max(0.1, float(request_timeout_seconds))
        self.poll_interval_seconds = max(0.0, float(poll_interval_seconds))
        self.max_json_response_bytes = max(1024, int(max_json_response_bytes))
        self.max_snapshot_bytes = max(1024, int(max_snapshot_bytes))
        self.max_snapshot_unpacked_bytes = max(
            self.max_snapshot_bytes,
            int(max_snapshot_unpacked_bytes),
        )
        self.max_snapshot_members = max(1, int(max_snapshot_members))
        self.cancel_path_style = cancel_path_style

    async def create(
        self,
        input: Any,
        *,
        max_total_tokens: int,
        environment: Any = "remote",
        tools: Optional[Sequence[Mapping[str, Any]]] = None,
        system_instruction: Optional[str] = None,
    ) -> ProviderInteraction:
        """Create a stored background interaction using one new RPD lease."""

        body = self._create_body(
            input=input,
            max_total_tokens=max_total_tokens,
            environment=environment,
            tools=tools,
            system_instruction=system_instruction,
        )
        return await self._post_interaction(body, key_envs=self.key_envs)

    async def continue_interaction(
        self,
        previous: ProviderInteraction,
        input: Any = "continue",
        *,
        max_total_tokens: int,
        tools: Optional[Sequence[Mapping[str, Any]]] = None,
        system_instruction: Optional[str] = None,
    ) -> ProviderInteraction:
        """Continue in the exact previous interaction and sandbox environment."""

        if previous.is_active:
            raise ValueError("cannot continue an active interaction")
        if not previous.environment_id:
            raise ValueError("previous interaction has no environment_id")
        body = self._create_body(
            input=input,
            max_total_tokens=max_total_tokens,
            environment=previous.environment_id,
            tools=tools,
            system_instruction=system_instruction,
        )
        body["previous_interaction_id"] = previous.id
        # An environment belongs to the key/project that provisioned it.  Do not
        # rotate a continuation onto another pool member.
        return await self._post_interaction(
            body,
            key_envs=(previous.lease.env_var_name,),
        )

    async def get(self, interaction: ProviderInteraction) -> ProviderInteraction:
        """Poll one interaction without reserving RPM/TPM/RPD in the model ledger."""

        interaction_id = self._resource_id(interaction.id, "interaction_id")
        api_key = self.rate_limiter.get_external_call_api_key(interaction.lease)
        payload = await self._request_json(
            "GET",
            f"{self.base_url}/interactions/{interaction_id}",
            api_key=api_key,
        )
        result = self._parse_interaction(payload, interaction.lease)
        await self._finalize_if_terminal(result)
        return result

    async def wait(
        self,
        interaction: ProviderInteraction,
        *,
        deadline_seconds: float,
        cancel_on_deadline: bool = True,
    ) -> ProviderInteraction:
        """Poll to a terminal/action state and optionally cancel at the deadline."""

        if deadline_seconds < 0:
            raise ValueError("deadline_seconds must be non-negative")
        current = interaction
        if current.is_terminal:
            return current
        deadline = time.monotonic() + float(deadline_seconds)
        while current.is_active:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                cancel_result: Optional[ProviderInteraction] = None
                cancel_error: Optional[Exception] = None
                if cancel_on_deadline:
                    try:
                        cancel_result = await self.cancel(current)
                    except Exception as exc:  # deadline remains the primary error
                        cancel_error = exc
                raise InteractionDeadlineExceeded(
                    current.id,
                    cancel_result=cancel_result,
                    cancel_error=cancel_error,
                )
            await asyncio.sleep(min(self.poll_interval_seconds, remaining))
            current = await self.get(current)
        return current

    async def cancel(self, interaction: ProviderInteraction) -> ProviderInteraction:
        """Cancel a running interaction without consuming an interaction RPD.

        The current reference uses ``/{id}/cancel``.  A 404/405 is retried once
        with the preview guide's older ``/{id}:cancel`` spelling (or vice versa).
        Both control-plane POSTs receive distinct request IDs.
        """

        interaction_id = self._resource_id(interaction.id, "interaction_id")
        api_key = self.rate_limiter.get_external_call_api_key(interaction.lease)
        styles = (
            ("path", "colon")
            if self.cancel_path_style == "path"
            else ("colon", "path")
        )
        last_error: Optional[ProviderError] = None
        for index, style in enumerate(styles):
            suffix = f"/{interaction_id}/cancel" if style == "path" else f"/{interaction_id}:cancel"
            try:
                payload = await self._request_json(
                    "POST",
                    f"{self.base_url}/interactions{suffix}",
                    api_key=api_key,
                    request_uid=str(uuid.uuid4()),
                )
                result = self._parse_interaction(payload, interaction.lease)
                await self._finalize_if_terminal(result)
                return result
            except ProviderError as exc:
                last_error = exc
                if index == 0 and exc.status_code in {404, 405}:
                    continue
                raise
        raise last_error or ProviderError(error_type="cancel_failed")

    async def download_environment(
        self,
        interaction: ProviderInteraction,
        destination_tar: os.PathLike[str] | str,
        *,
        extract_to: Optional[os.PathLike[str] | str] = None,
    ) -> Path:
        """Download an environment snapshot and optionally extract it safely.

        Downloads do not reserve an interaction request.  Extraction rejects
        absolute/traversal paths, links, devices, oversized archives, and member
        explosions before writing any archive member.
        """

        if not interaction.environment_id:
            raise ValueError("interaction has no environment_id")
        environment_id = self._resource_id(
            interaction.environment_id,
            "environment_id",
        )
        api_key = self.rate_limiter.get_external_call_api_key(interaction.lease)
        url = (
            f"{self.base_url}/files/environment-{environment_id}:download?alt=media"
        )
        response = await self.transport.request(
            "GET",
            url,
            headers=self._headers(api_key),
            body=None,
            timeout_seconds=self.request_timeout_seconds,
            max_response_bytes=self.max_snapshot_bytes,
        )
        self._raise_for_status(response, secret=api_key)
        if len(response.body) > self.max_snapshot_bytes:
            raise ProviderError(
                error_type="response_too_large",
                error_message="environment snapshot exceeded the configured byte limit",
            )
        destination = Path(destination_tar)
        await asyncio.to_thread(self._atomic_write, destination, response.body)
        if extract_to is not None:
            await asyncio.to_thread(
                self._safe_extract_tar,
                response.body,
                Path(extract_to),
            )
        return destination

    def _create_body(
        self,
        *,
        input: Any,
        max_total_tokens: int,
        environment: Any,
        tools: Optional[Sequence[Mapping[str, Any]]],
        system_instruction: Optional[str],
    ) -> dict[str, Any]:
        try:
            token_budget = int(max_total_tokens)
        except (TypeError, ValueError) as exc:
            raise ValueError("max_total_tokens must be an integer") from exc
        if not 1 <= token_budget <= 100_000:
            raise ValueError("max_total_tokens must be between 1 and 100000")
        if input is None or input == "":
            raise ValueError("input is required")
        if environment is None or environment == "":
            raise ValueError("environment is required")
        body: dict[str, Any] = {
            "agent": ANTIGRAVITY_AGENT,
            "input": input,
            "environment": environment,
            "background": True,
            "store": True,
            "agent_config": {
                "type": "antigravity",
                "max_total_tokens": token_budget,
            },
        }
        if tools is not None:
            body["tools"] = [dict(tool) for tool in tools]
        if system_instruction is not None:
            body["system_instruction"] = str(system_instruction)
        return body

    async def _post_interaction(
        self,
        body: dict[str, Any],
        *,
        key_envs: Sequence[str],
    ) -> ProviderInteraction:
        request_uid = str(uuid.uuid4())
        reserved_tpm = int(body["agent_config"]["max_total_tokens"])
        lease = await self.rate_limiter.reserve_external_call(
            model=ANTIGRAVITY_AGENT,
            reserved_tpm=reserved_tpm,
            key_envs=key_envs,
            request_uid=request_uid,
        )
        started = time.monotonic()
        try:
            api_key = self.rate_limiter.get_external_call_api_key(lease)
            await self.rate_limiter.mark_external_call_sent(lease)
            payload = await self._request_json(
                "POST",
                f"{self.base_url}/interactions",
                api_key=api_key,
                request_uid=request_uid,
                json_body=body,
            )
            interaction = self._parse_interaction(payload, lease)
        except Exception as exc:
            provider_error = self._as_provider_error(exc)
            await self._finalize_transport_failure(
                lease,
                provider_error,
                duration_ms=int((time.monotonic() - started) * 1000),
            )
            raise provider_error from exc

        if interaction.is_terminal:
            await self._finalize_if_terminal(
                interaction,
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        return interaction

    async def _finalize_transport_failure(
        self,
        lease: ExternalCallLease,
        error: ProviderError,
        *,
        duration_ms: int,
    ) -> None:
        try:
            await self.rate_limiter.finalize_external_call(
                lease,
                provider_interaction_id=None,
                provider_terminal_status="failed",
                usage=None,
                duration_ms=duration_ms,
                semantic_status="not_evaluated",
                error=error,
            )
        except Exception as finalize_error:
            raise ReservationError(
                f"provider call failed and accounting finalization also failed: "
                f"{str(finalize_error)[:300]}"
            ) from error

    async def _finalize_if_terminal(
        self,
        interaction: ProviderInteraction,
        *,
        duration_ms: Optional[int] = None,
    ) -> None:
        if not interaction.is_terminal:
            return
        elapsed = duration_ms
        if elapsed is None:
            elapsed = max(
                0,
                int(
                    (
                        time.time()
                        - interaction.lease.started_at.timestamp()
                    )
                    * 1000
                ),
            )
        error: Optional[ProviderError] = None
        if interaction.provider_status in {"failed", "cancelled"}:
            error = ProviderError(
                error_type=f"interaction_{interaction.provider_status}",
                error_message=self._interaction_error_message(interaction.raw),
            )
        await self.rate_limiter.finalize_external_call(
            interaction.lease,
            provider_interaction_id=interaction.id,
            provider_terminal_status=interaction.provider_status,
            usage=interaction.usage,
            duration_ms=elapsed,
            semantic_status="not_evaluated",
            error=error,
        )

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        api_key: str,
        request_uid: Optional[str] = None,
        json_body: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        body = None
        if json_body is not None:
            body = json.dumps(
                json_body,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        response = await self.transport.request(
            method,
            url,
            headers=self._headers(api_key, request_uid=request_uid),
            body=body,
            timeout_seconds=self.request_timeout_seconds,
            max_response_bytes=self.max_json_response_bytes,
        )
        self._raise_for_status(response, secret=api_key)
        if len(response.body) > self.max_json_response_bytes:
            raise ProviderError(
                error_type="response_too_large",
                error_message="Interactions API response exceeded the configured byte limit",
            )
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InteractionsProtocolError(
                error_type="invalid_json",
                error_message="Interactions API returned invalid JSON",
                status_code=response.status,
            ) from exc
        if not isinstance(payload, dict):
            raise InteractionsProtocolError(
                error_type="invalid_response",
                error_message="Interactions API response must be an object",
                status_code=response.status,
            )
        return payload

    @staticmethod
    def _headers(api_key: str, *, request_uid: Optional[str] = None) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
            "Api-Revision": INTERACTIONS_API_REVISION,
        }
        if request_uid:
            headers["X-Request-Id"] = request_uid
        return headers

    def _parse_interaction(
        self,
        payload: dict[str, Any],
        lease: ExternalCallLease,
    ) -> ProviderInteraction:
        interaction_id = payload.get("id")
        status = str(payload.get("status") or "").strip().lower()
        if not isinstance(interaction_id, str) or not interaction_id:
            raise InteractionsProtocolError(
                error_type="missing_interaction_id",
                error_message="Interactions API response has no id",
            )
        self._resource_id(interaction_id, "interaction_id")
        if status not in INTERACTION_STATUSES:
            raise InteractionsProtocolError(
                error_type="unknown_interaction_status",
                error_message=f"unknown Interactions API status: {status or '<empty>'}",
            )
        environment_id = payload.get("environment_id")
        if environment_id is not None:
            if not isinstance(environment_id, str):
                raise InteractionsProtocolError(
                    error_type="invalid_environment_id",
                    error_message="environment_id must be a string",
                )
            self._resource_id(environment_id, "environment_id")
        raw_steps = payload.get("steps") or []
        if not isinstance(raw_steps, list) or not all(
            isinstance(step, dict) for step in raw_steps
        ):
            raise InteractionsProtocolError(
                error_type="invalid_steps",
                error_message="steps must be an array of objects",
            )
        usage_raw = payload.get("usage") or {}
        if not isinstance(usage_raw, dict):
            usage_raw = {}
        input_tokens = self._nonnegative_int(usage_raw.get("total_input_tokens"))
        output_tokens = self._nonnegative_int(usage_raw.get("total_output_tokens"))
        total_tokens = self._nonnegative_int(usage_raw.get("total_tokens"))
        if total_tokens == 0 and (input_tokens or output_tokens):
            total_tokens = input_tokens + output_tokens
        return ProviderInteraction(
            id=interaction_id,
            provider_status=status,
            environment_id=environment_id,
            steps=tuple(dict(step) for step in raw_steps),
            usage=UsageInfo(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
            ),
            raw=dict(payload),
            lease=lease,
        )

    @staticmethod
    def _nonnegative_int(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _resource_id(value: str, label: str) -> str:
        if not _RESOURCE_ID_RE.fullmatch(value):
            raise ValueError(f"invalid {label}")
        return value

    @staticmethod
    def _interaction_error_message(payload: Mapping[str, Any]) -> Optional[str]:
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str):
                return message[:1000]
        return None

    @staticmethod
    def _as_provider_error(exc: Exception) -> ProviderError:
        if isinstance(exc, ProviderError):
            return exc
        return ProviderError(
            error_type=exc.__class__.__name__,
            error_message=str(exc)[:500],
            retryable=isinstance(exc, (TimeoutError, ConnectionError)),
        )

    @staticmethod
    def _raise_for_status(
        response: HTTPResponse,
        *,
        secret: Optional[str] = None,
    ) -> None:
        if 200 <= response.status < 300:
            return
        message = ""
        try:
            payload = json.loads(response.body.decode("utf-8"))
            error = payload.get("error") if isinstance(payload, dict) else None
            if isinstance(error, dict):
                message = str(error.get("message") or error.get("status") or "")
            elif error:
                message = str(error)
        except Exception:
            message = response.body.decode("utf-8", errors="replace")
        if secret and secret in message:
            message = message.replace(secret, "[REDACTED]")
        raise ProviderError(
            error_type="http_error",
            error_code=str(response.status),
            error_message=(message or "Google Interactions API request failed")[:500],
            retryable=response.status in {408, 429, 500, 502, 503, 504},
            status_code=response.status,
        )

    @staticmethod
    def _atomic_write(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temp.open("xb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        finally:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                pass

    def _safe_extract_tar(self, content: bytes, destination: Path) -> None:
        destination.mkdir(parents=True, exist_ok=True)
        destination_root = destination.resolve()
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:*") as archive:
            members = archive.getmembers()
            if len(members) > self.max_snapshot_members:
                raise ValueError("environment snapshot has too many members")
            total_size = 0
            for member in members:
                name = member.name
                posix = PurePosixPath(name)
                if (
                    not name
                    or "\\" in name
                    or posix.is_absolute()
                    or ".." in posix.parts
                    or member.issym()
                    or member.islnk()
                    or not (member.isdir() or member.isreg())
                ):
                    raise ValueError(f"unsafe environment snapshot member: {name!r}")
                target = (destination_root / Path(*posix.parts)).resolve()
                if target != destination_root and destination_root not in target.parents:
                    raise ValueError(f"snapshot member escapes destination: {name!r}")
                total_size += max(0, int(member.size))
                if total_size > self.max_snapshot_unpacked_bytes:
                    raise ValueError("environment snapshot expands beyond the configured limit")
            archive.extractall(destination_root, members=members, filter="data")


__all__ = [
    "ACTIVE_INTERACTION_STATUSES",
    "ANTIGRAVITY_AGENT",
    "AntigravityInteractionsClient",
    "AsyncHTTPTransport",
    "HTTPResponse",
    "INTERACTIONS_API_REVISION",
    "INTERACTION_STATUSES",
    "InteractionDeadlineExceeded",
    "InteractionsProtocolError",
    "ProviderInteraction",
    "TERMINAL_INTERACTION_STATUSES",
    "UrllibAsyncHTTPTransport",
]
