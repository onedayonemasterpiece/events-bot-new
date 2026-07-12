from __future__ import annotations

import hashlib
import logging
import os
import re
import socket
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from typing import Any, Mapping
from uuid import UUID

from .models import EmailMessage, Stream
from .providers.base import AmbiguousDelivery, ProviderConfigurationError, ProviderRejected
from .providers.postbox import PostboxAdapter, PostboxConfig
from .supabase_rpc import EmailControlRpcClient, EmailControlRpcError
from .yandex_iam import YandexIamError, YandexIamTokenProvider


logger = logging.getLogger(__name__)
_SAFE_ERROR = re.compile(r"^[a-z0-9_:-]{1,160}$")
_ALLOWED_KINDS = {
    "account_auth",
    "calendar_confirmation",
    "event_reminder_24h",
    "event_rescheduled",
    "event_cancelled",
}


def _flag(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    raw = value.strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError("boolean environment value invalid")


def _bounded_int(value: str | None, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(str(value or default).strip())
    except ValueError as exc:
        raise ValueError("integer environment value invalid") from exc
    if parsed not in range(minimum, maximum + 1):
        raise ValueError("integer environment value out of range")
    return parsed


@dataclass(frozen=True)
class EmailWorkerConfig:
    enabled: bool
    worker_id: str
    claim_limit: int
    lease_seconds: int
    max_attempts: int
    retry_base_seconds: int
    retry_max_seconds: int
    supabase_url: str
    supabase_secret_key: str
    postbox_enabled: bool
    postbox_endpoint: str
    postbox_from: str
    postbox_from_name: str
    postbox_reply_to: str
    postbox_configuration_set: str
    postbox_sa_key_json: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "EmailWorkerConfig":
        machine = str(env.get("FLY_MACHINE_ID") or socket.gethostname() or "worker").strip()
        worker_id = f"postbox:{machine}"[:120]
        return cls(
            enabled=_flag(env.get("ENABLE_EMAIL_OUTBOX_WORKER"), False),
            worker_id=worker_id,
            claim_limit=_bounded_int(env.get("EMAIL_OUTBOX_CLAIM_LIMIT"), 5, 1, 25),
            lease_seconds=_bounded_int(env.get("EMAIL_OUTBOX_LEASE_SECONDS"), 180, 30, 900),
            max_attempts=_bounded_int(env.get("EMAIL_OUTBOX_MAX_ATTEMPTS"), 5, 1, 20),
            retry_base_seconds=_bounded_int(env.get("EMAIL_OUTBOX_RETRY_BASE_SECONDS"), 300, 30, 3600),
            retry_max_seconds=_bounded_int(env.get("EMAIL_OUTBOX_RETRY_MAX_SECONDS"), 3600, 60, 86400),
            supabase_url=str(env.get("PERSONALIZATION_SUPABASE_URL") or "").strip(),
            supabase_secret_key=str(env.get("PERSONALIZATION_SUPABASE_SECRET_KEY") or "").strip(),
            postbox_enabled=_flag(env.get("POSTBOX_EMAIL_ENABLED"), False),
            postbox_endpoint=str(
                env.get("POSTBOX_EMAIL_ENDPOINT") or PostboxConfig.endpoint
            ).strip(),
            postbox_from=str(env.get("POSTBOX_EMAIL_FROM") or PostboxConfig.from_email).strip(),
            postbox_from_name=str(
                env.get("POSTBOX_EMAIL_FROM_NAME") or PostboxConfig.from_name
            ).strip(),
            postbox_reply_to=str(env.get("EMAIL_REPLY_TO") or PostboxConfig.reply_to).strip(),
            postbox_configuration_set=str(
                env.get("POSTBOX_EMAIL_CONFIGURATION_SET") or ""
            ).strip(),
            postbox_sa_key_json=str(env.get("POSTBOX_SA_KEY_JSON") or "").strip(),
        )


@dataclass
class EmailWorkerStats:
    claimed: int = 0
    accepted: int = 0
    dry_run: int = 0
    retryable: int = 0
    unknown: int = 0
    failed: int = 0
    recovered_retryable: int = 0
    recovered_unknown: int = 0
    errors: int = 0

    def public(self) -> dict[str, int]:
        return asdict(self)


def _outbox_hash(value: Any) -> str:
    return hashlib.sha256(str(value or "").encode()).hexdigest()[:16]


def _error_class(value: str) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    return normalized if _SAFE_ERROR.fullmatch(normalized) else "worker_error"


def _render_claim(claim: Mapping[str, Any], reply_to: str) -> EmailMessage:
    if claim.get("stream") != "transactional" or claim.get("provider") != "postbox":
        raise ValueError("claim_route_invalid")
    if claim.get("kind") not in _ALLOWED_KINDS:
        raise ValueError("claim_kind_invalid")
    if claim.get("template_version") != "transactional-plain-v1":
        raise ValueError("template_version_invalid")
    try:
        outbox_id = str(UUID(str(claim["outbox_id"])))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("outbox_id_invalid") from exc
    recipient = str(claim.get("recipient_email") or "").strip().lower()
    parsed = parseaddr(recipient)[1].strip().lower()
    if parsed != recipient or "@" not in parsed or len(parsed) > 320:
        raise ValueError("recipient_invalid")
    payload = claim.get("payload_json")
    if not isinstance(payload, Mapping) or set(payload) - {"subject", "text", "html"}:
        raise ValueError("payload_schema_invalid")
    subject = str(payload.get("subject") or "").strip()
    text = str(payload.get("text") or "")
    html = str(payload.get("html") or "")
    if not subject or len(subject) > 200 or "\r" in subject or "\n" in subject:
        raise ValueError("subject_invalid")
    if len(text.encode()) > 20_000 or len(html.encode()) > 50_000 or (not text and not html):
        raise ValueError("content_invalid")
    return EmailMessage(
        outbox_id=outbox_id,
        idempotency_key=outbox_id,
        stream=Stream.TRANSACTIONAL,
        to_email=recipient,
        subject=subject,
        text=text,
        html=html,
        reply_to=reply_to,
    )


class PostboxOutboxWorker:
    def __init__(
        self,
        config: EmailWorkerConfig,
        *,
        rpc: EmailControlRpcClient | None = None,
        token_provider: YandexIamTokenProvider | None = None,
        adapter_factory: Any = PostboxAdapter,
    ) -> None:
        self.config = config
        self.rpc = rpc or EmailControlRpcClient(config.supabase_url, config.supabase_secret_key)
        self._token_provider = token_provider
        self.adapter_factory = adapter_factory

    def _token(self) -> str:
        if self._token_provider is None:
            self._token_provider = YandexIamTokenProvider(self.config.postbox_sa_key_json)
        return self._token_provider.get_token()

    def _retry_at(self, attempt_number: int) -> str:
        seconds = min(
            self.config.retry_max_seconds,
            self.config.retry_base_seconds * (2 ** max(0, attempt_number - 1)),
        )
        return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()

    def _finish(
        self,
        claim: Mapping[str, Any],
        outcome: str,
        *,
        provider_message_id: str | None = None,
        response_code: int | None = None,
        error_class: str | None = None,
        retry_at: str | None = None,
    ) -> Any:
        return self.rpc.call(
            "email_finish_attempt_v1",
            {
                "p_outbox_id": claim["outbox_id"],
                "p_lease_token": claim["lease_token"],
                "p_outcome": outcome,
                "p_provider_message_id": provider_message_id,
                "p_response_code": response_code,
                "p_error_class": error_class,
                "p_retry_at": retry_at,
            },
        )

    def _preflight_failure(
        self,
        claim: Mapping[str, Any],
        error_class: str,
        *,
        retryable: bool,
    ) -> Any:
        attempt = int(claim.get("attempt_number") or 1)
        retry = retryable and attempt < self.config.max_attempts
        return self.rpc.call(
            "email_fail_postbox_claim_before_network_v1",
            {
                "p_outbox_id": claim["outbox_id"],
                "p_lease_token": claim["lease_token"],
                "p_error_class": _error_class(error_class),
                "p_retryable": retry,
                "p_retry_at": self._retry_at(attempt) if retry else None,
            },
        )

    def run_once(self) -> EmailWorkerStats:
        stats = EmailWorkerStats()
        if not self.config.enabled:
            return stats
        recovered = self.rpc.call("email_recover_expired_postbox_claims_v2") or []
        if isinstance(recovered, list) and recovered:
            stats.recovered_retryable = int(recovered[0].get("retryable_count") or 0)
            stats.recovered_unknown = int(recovered[0].get("unknown_count") or 0)
        claims = self.rpc.call(
            "email_claim_postbox_outbox_v2",
            {
                "p_worker_id": self.config.worker_id,
                "p_limit": self.config.claim_limit,
                "p_lease_seconds": self.config.lease_seconds,
            },
        ) or []
        if not isinstance(claims, list):
            raise EmailControlRpcError("claim_response_invalid")
        stats.claimed = len(claims)

        for claim in claims:
            if not isinstance(claim, Mapping):
                stats.errors += 1
                continue
            outbox_hash = _outbox_hash(claim.get("outbox_id"))
            try:
                message = _render_claim(claim, self.config.postbox_reply_to)
            except (TypeError, ValueError) as exc:
                code = _error_class(str(exc))
                try:
                    self._preflight_failure(claim, code, retryable=False)
                    stats.failed += 1
                except EmailControlRpcError:
                    stats.errors += 1
                logger.error("email_outbox_preflight_failed outbox_hash=%s code=%s", outbox_hash, code)
                continue

            if bool(claim.get("dry_run")):
                try:
                    self._finish(claim, "dry_run")
                    stats.dry_run += 1
                except EmailControlRpcError:
                    stats.errors += 1
                continue

            if not self.config.postbox_enabled:
                try:
                    self._preflight_failure(claim, "postbox_disabled", retryable=True)
                    stats.retryable += 1
                except EmailControlRpcError:
                    stats.errors += 1
                continue

            try:
                token = self._token()
                adapter = self.adapter_factory(
                    PostboxConfig(
                        enabled=True,
                        dry_run=False,
                        endpoint=self.config.postbox_endpoint,
                        iam_token=token,
                        from_email=self.config.postbox_from,
                        from_name=self.config.postbox_from_name,
                        reply_to=self.config.postbox_reply_to,
                        configuration_set=self.config.postbox_configuration_set,
                    )
                )
                request_body = adapter.prepare(message)
            except (YandexIamError, ProviderConfigurationError) as exc:
                code = "iam_token_failed" if isinstance(exc, YandexIamError) else "postbox_config_invalid"
                try:
                    self._preflight_failure(claim, code, retryable=True)
                    stats.retryable += 1
                except EmailControlRpcError:
                    stats.errors += 1
                logger.error("email_outbox_preflight_failed outbox_hash=%s code=%s", outbox_hash, code)
                continue
            except Exception:
                try:
                    self._preflight_failure(claim, "render_failed", retryable=False)
                    stats.failed += 1
                except EmailControlRpcError:
                    stats.errors += 1
                logger.error("email_outbox_preflight_failed outbox_hash=%s code=render_failed", outbox_hash)
                continue

            request_hash = hashlib.sha256(request_body).hexdigest()
            try:
                self.rpc.call(
                    "email_mark_network_started_v1",
                    {
                        "p_outbox_id": claim["outbox_id"],
                        "p_lease_token": claim["lease_token"],
                        "p_request_sha256": request_hash,
                    },
                )
            except EmailControlRpcError:
                stats.errors += 1
                logger.error("email_outbox_mark_network_failed outbox_hash=%s", outbox_hash)
                continue

            try:
                result = adapter.send_prepared(request_body)
                self._finish(
                    claim,
                    "accepted",
                    provider_message_id=result.provider_message_id,
                    response_code=result.response_code,
                )
                stats.accepted += 1
                logger.info("email_outbox_accepted outbox_hash=%s", outbox_hash)
            except ProviderRejected as exc:
                attempt = int(claim.get("attempt_number") or 1)
                retryable = bool(exc.retryable) and attempt < self.config.max_attempts
                outcome = "retryable" if retryable else "failed"
                try:
                    self._finish(
                        claim,
                        outcome,
                        response_code=exc.status,
                        error_class="postbox_rejected",
                        retry_at=self._retry_at(attempt) if retryable else None,
                    )
                    if retryable:
                        stats.retryable += 1
                    else:
                        stats.failed += 1
                except EmailControlRpcError:
                    stats.errors += 1
            except AmbiguousDelivery:
                try:
                    self._finish(claim, "unknown", error_class="postbox_ambiguous")
                    stats.unknown += 1
                except EmailControlRpcError:
                    stats.errors += 1
            except Exception:
                try:
                    self._finish(claim, "unknown", error_class="postbox_unexpected")
                    stats.unknown += 1
                except EmailControlRpcError:
                    stats.errors += 1
        return stats


def build_worker_from_env(env: Mapping[str, str] = os.environ) -> PostboxOutboxWorker:
    return PostboxOutboxWorker(EmailWorkerConfig.from_env(env))
