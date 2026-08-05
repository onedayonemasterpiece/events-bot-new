from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .supabase_rpc import EmailControlRpcClient
from .worker import EmailWorkerConfig, PostboxOutboxWorker


logger = logging.getLogger(__name__)
_last_alert_at: dict[str, float] = {}


def _flag(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int(value: str | None, default: int, minimum: int, maximum: int) -> int:
    try:
        result = int(str(value or default).strip())
    except ValueError:
        return default
    return min(max(result, minimum), maximum)


@dataclass(frozen=True)
class EmailMonitorConfig:
    enabled: bool
    interval_seconds: int
    alert_cooldown_seconds: int
    submitted_warning_seconds: int
    submitted_alarm_seconds: int
    retryable_due_warning: int
    supabase_url: str
    supabase_secret_key: str
    dlq_queue_url: str
    dlq_access_key_id: str
    dlq_secret_access_key: str
    dlq_endpoint: str
    state_path: str = "/data/email-postbox-monitor-state.json"
    static_reminder_seconds: int = 21_600

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "EmailMonitorConfig":
        return cls(
            enabled=_flag(env.get("ENABLE_EMAIL_OUTBOX_MONITOR"), False),
            interval_seconds=_int(env.get("EMAIL_OUTBOX_MONITOR_INTERVAL_SECONDS"), 300, 60, 3600),
            alert_cooldown_seconds=_int(env.get("EMAIL_OUTBOX_ALERT_COOLDOWN_SECONDS"), 900, 60, 86400),
            submitted_warning_seconds=_int(env.get("EMAIL_OUTBOX_SUBMITTED_WARNING_SECONDS"), 900, 300, 86400),
            submitted_alarm_seconds=_int(env.get("EMAIL_OUTBOX_SUBMITTED_ALARM_SECONDS"), 3600, 600, 172800),
            retryable_due_warning=_int(env.get("EMAIL_OUTBOX_RETRYABLE_DUE_WARNING"), 5, 1, 1000),
            supabase_url=str(env.get("PERSONALIZATION_SUPABASE_URL") or "").strip(),
            supabase_secret_key=str(env.get("PERSONALIZATION_SUPABASE_SECRET_KEY") or "").strip(),
            dlq_queue_url=str(env.get("POSTBOX_DLQ_QUEUE_URL") or "").strip(),
            dlq_access_key_id=str(env.get("POSTBOX_DLQ_AWS_ACCESS_KEY_ID") or "").strip(),
            dlq_secret_access_key=str(env.get("POSTBOX_DLQ_AWS_SECRET_ACCESS_KEY") or "").strip(),
            dlq_endpoint=str(
                env.get("POSTBOX_DLQ_ENDPOINT") or "https://message-queue.api.cloud.yandex.net"
            ).strip(),
            state_path=str(
                env.get("EMAIL_OUTBOX_MONITOR_STATE_PATH")
                or "/data/email-postbox-monitor-state.json"
            ).strip(),
            static_reminder_seconds=_int(
                env.get("EMAIL_OUTBOX_STATIC_REMINDER_SECONDS"), 21_600, 900, 604_800
            ),
        )


@dataclass(frozen=True)
class PostboxAlertSnapshot:
    initialized: bool = False
    dlq_total: int = 0
    codes: tuple[str, ...] = ()
    last_notified_at: float = 0.0
    observed_at: float = 0.0

    @classmethod
    def from_value(cls, value: Any) -> "PostboxAlertSnapshot":
        if not isinstance(value, Mapping) or value.get("schema") != 1:
            return cls()
        raw_codes = value.get("codes")
        codes = (
            tuple(str(code) for code in raw_codes if str(code).strip())
            if isinstance(raw_codes, list)
            else ()
        )
        try:
            dlq_total = max(0, int(value.get("dlq_total") or 0))
            last_notified_at = max(0.0, float(value.get("last_notified_at") or 0.0))
            observed_at = max(0.0, float(value.get("observed_at") or 0.0))
        except (TypeError, ValueError):
            return cls()
        return cls(
            initialized=bool(value.get("initialized")),
            dlq_total=dlq_total,
            codes=codes,
            last_notified_at=last_notified_at,
            observed_at=observed_at,
        )

    def public(self) -> dict[str, Any]:
        return {
            "schema": 1,
            "initialized": self.initialized,
            "dlq_total": self.dlq_total,
            "codes": list(self.codes),
            "last_notified_at": self.last_notified_at,
            "observed_at": self.observed_at,
        }


class PostboxAlertStateStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self) -> PostboxAlertSnapshot:
        try:
            return PostboxAlertSnapshot.from_value(
                json.loads(self.path.read_text(encoding="utf-8"))
            )
        except FileNotFoundError:
            return PostboxAlertSnapshot()
        except (OSError, UnicodeError, json.JSONDecodeError):
            logger.warning("email_monitor_state_load_failed", exc_info=True)
            return PostboxAlertSnapshot()

    def save(self, snapshot: PostboxAlertSnapshot) -> None:
        parent = self.path.parent
        parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        temporary.write_text(
            json.dumps(snapshot.public(), sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            logger.warning("email_monitor_state_chmod_failed", exc_info=True)
        os.replace(temporary, self.path)


@dataclass(frozen=True)
class PostboxNotificationDecision:
    kind: str
    codes: tuple[str, ...]
    dlq_total: int
    dlq_delta: int
    snapshot: PostboxAlertSnapshot

    @property
    def should_send(self) -> bool:
        return self.kind in {"alert", "recovery"}

    def notified(self, now_epoch: float) -> "PostboxNotificationDecision":
        return PostboxNotificationDecision(
            kind=self.kind,
            codes=self.codes,
            dlq_total=self.dlq_total,
            dlq_delta=self.dlq_delta,
            snapshot=PostboxAlertSnapshot(
                initialized=True,
                dlq_total=self.snapshot.dlq_total,
                codes=self.snapshot.codes,
                last_notified_at=now_epoch,
                observed_at=self.snapshot.observed_at,
            ),
        )


def decide_postbox_notification(
    previous: PostboxAlertSnapshot,
    alarms: list[tuple[str, str]],
    health: Mapping[str, Any],
    *,
    now_epoch: float,
    static_reminder_seconds: int,
) -> PostboxNotificationDecision:
    codes = tuple(code for _level, code in alarms)
    dlq_total = int(health.get("dlq_visible_count") or 0) + int(
        health.get("dlq_inflight_count") or 0
    )
    delta = dlq_total - previous.dlq_total if previous.initialized else dlq_total
    prior_incident = previous.initialized and bool(previous.codes)
    current_incident = bool(codes)

    kind = "none"
    if prior_incident and not current_incident:
        kind = "recovery"
    elif current_incident:
        changed = not previous.initialized or codes != previous.codes or delta != 0
        reminder_due = (
            previous.last_notified_at <= 0
            or now_epoch - previous.last_notified_at >= static_reminder_seconds
        )
        if changed or reminder_due:
            kind = "alert"

    snapshot = PostboxAlertSnapshot(
        initialized=True,
        dlq_total=max(0, dlq_total),
        codes=codes,
        last_notified_at=previous.last_notified_at,
        observed_at=now_epoch,
    )
    return PostboxNotificationDecision(kind, codes, dlq_total, delta, snapshot)


class PostboxHealthMonitor:
    def __init__(
        self,
        config: EmailMonitorConfig,
        *,
        rpc: EmailControlRpcClient | None = None,
        sqs_client: Any | None = None,
    ) -> None:
        self.config = config
        self.rpc = rpc or EmailControlRpcClient(config.supabase_url, config.supabase_secret_key)
        self._sqs_client = sqs_client

    def _sqs(self) -> Any:
        if self._sqs_client is None:
            if not (
                self.config.dlq_queue_url
                and self.config.dlq_access_key_id
                and self.config.dlq_secret_access_key
            ):
                raise RuntimeError("dlq_monitor_config_missing")
            import boto3

            self._sqs_client = boto3.client(
                "sqs",
                endpoint_url=self.config.dlq_endpoint,
                region_name="ru-central1",
                aws_access_key_id=self.config.dlq_access_key_id,
                aws_secret_access_key=self.config.dlq_secret_access_key,
            )
        return self._sqs_client

    def inspect(self) -> dict[str, Any]:
        health = self.rpc.call("email_postbox_health_v1")
        if not isinstance(health, dict):
            raise RuntimeError("email_health_response_invalid")
        response = self._sqs().get_queue_attributes(
            QueueUrl=self.config.dlq_queue_url,
            AttributeNames=["All"],
        )
        attributes = response.get("Attributes") or {}
        dlq_visible = int(attributes.get("ApproximateNumberOfMessages") or 0)
        dlq_inflight = int(attributes.get("ApproximateNumberOfMessagesNotVisible") or 0)
        health["dlq_visible_count"] = dlq_visible
        health["dlq_inflight_count"] = dlq_inflight
        return health

    def alarms(self, health: Mapping[str, Any]) -> list[tuple[str, str]]:
        alarms: list[tuple[str, str]] = []
        if int(health.get("dlq_visible_count") or 0) + int(health.get("dlq_inflight_count") or 0) > 0:
            alarms.append(("alarm", "postbox_dlq_nonempty"))
        if int(health.get("unknown_delivery_count") or 0) > 0:
            alarms.append(("alarm", "postbox_unknown_delivery"))
        if int(health.get("expired_claim_count") or 0) > 0:
            alarms.append(("alarm", "postbox_expired_claim"))
        oldest_submitted = int(health.get("oldest_submitted_seconds") or 0)
        if oldest_submitted >= self.config.submitted_alarm_seconds:
            alarms.append(("alarm", "postbox_delivery_event_lag"))
        elif oldest_submitted >= self.config.submitted_warning_seconds:
            alarms.append(("warning", "postbox_delivery_event_delayed"))
        if int(health.get("terminal_failed_24h_count") or 0) > 0:
            alarms.append(("warning", "postbox_terminal_failure"))
        if int(health.get("retryable_due_count") or 0) >= self.config.retryable_due_warning:
            alarms.append(("warning", "postbox_retry_backlog"))
        if int(health.get("postbox_missing_correlation_count") or 0) > 0:
            alarms.append(("alarm", "postbox_correlation_missing"))
        return alarms


async def _notify(db: Any, bot: Any, text: str) -> bool:
    if bot is None or not hasattr(bot, "send_message"):
        return False
    from admin_chat import resolve_superadmin_chat_id

    chat_id = await resolve_superadmin_chat_id(db)
    if not chat_id:
        return False
    await bot.send_message(int(chat_id), text, disable_web_page_preview=True)
    return True


async def run_email_outbox_worker(db: Any, bot: Any, *, run_id: str | None = None) -> dict[str, int]:
    del db, bot, run_id
    config = EmailWorkerConfig.from_env()
    if not config.enabled:
        return {}
    try:
        stats = await asyncio.to_thread(PostboxOutboxWorker(config).run_once)
    except Exception:
        logger.exception("email_outbox_worker_failed")
        raise
    result = stats.public()
    logger.info("email_outbox_worker_result %s", json.dumps(result, sort_keys=True, separators=(",", ":")))
    return result


async def run_email_outbox_monitor(db: Any, bot: Any, *, run_id: str | None = None) -> dict[str, Any]:
    del run_id
    config = EmailMonitorConfig.from_env()
    if not config.enabled:
        return {}
    monitor = PostboxHealthMonitor(config)
    try:
        health = await asyncio.to_thread(monitor.inspect)
        alarms = monitor.alarms(health)
    except Exception:
        logger.exception("email_outbox_monitor_failed")
        fingerprint = "email_monitor_unavailable"
        now = time.monotonic()
        if now - _last_alert_at.get(fingerprint, 0.0) >= config.alert_cooldown_seconds:
            _last_alert_at[fingerprint] = now
            await _notify(db, bot, "🚨 Email monitor unavailable\ncode=email_monitor_unavailable")
        raise

    public = {
        key: health.get(key)
        for key in (
            "ready_count",
            "retryable_due_count",
            "claimed_count",
            "expired_claim_count",
            "submitted_count",
            "submitted_over_15m_count",
            "submitted_over_60m_count",
            "unknown_delivery_count",
            "terminal_failed_24h_count",
            "delivered_24h_count",
            "oldest_pending_seconds",
            "oldest_submitted_seconds",
            "provider_events_24h_count",
            "postbox_auth_submitted_count",
            "postbox_auth_delivered_24h_count",
            "postbox_auth_terminal_failed_24h_count",
            "postbox_correlation_total_count",
            "postbox_correlation_unbound_count",
            "postbox_missing_correlation_count",
            "dlq_visible_count",
            "dlq_inflight_count",
        )
    }
    logger.info("email_outbox_health %s", json.dumps(public, sort_keys=True, separators=(",", ":")))

    now_epoch = time.time()
    store = PostboxAlertStateStore(config.state_path)
    previous = store.load()
    decision = decide_postbox_notification(
        previous,
        alarms,
        public,
        now_epoch=now_epoch,
        static_reminder_seconds=config.static_reminder_seconds,
    )

    sent = False
    if decision.kind == "alert":
        severity = "🚨" if any(level == "alarm" for level, _code in alarms) else "⚠️"
        sent = await _notify(
            db,
            bot,
            severity
            + " Email/Postbox alert\n"
            + "codes="
            + ",".join(decision.codes)
            + "\n"
            + "dlq="
            + str(decision.dlq_total)
            + " delta="
            + f"{decision.dlq_delta:+d}"
            + " unknown="
            + str(public.get("unknown_delivery_count") or 0)
            + " submitted_oldest_s="
            + str(public.get("oldest_submitted_seconds") or 0)
            + "\n"
            + "auth_submitted="
            + str(public.get("postbox_auth_submitted_count") or 0)
            + " correlation_missing="
            + str(public.get("postbox_missing_correlation_count") or 0),
        )
    elif decision.kind == "recovery":
        sent = await _notify(
            db,
            bot,
            "✅ Email/Postbox recovered\n"
            + "previous_codes="
            + (",".join(previous.codes) or "none")
            + "\n"
            + "dlq="
            + str(decision.dlq_total)
            + " delta="
            + f"{decision.dlq_delta:+d}",
        )

    snapshot = decision.notified(now_epoch).snapshot if sent else decision.snapshot
    try:
        store.save(snapshot)
    except OSError:
        logger.warning("email_monitor_state_save_failed", exc_info=True)
    return public
