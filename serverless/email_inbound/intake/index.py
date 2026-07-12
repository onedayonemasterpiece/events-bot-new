from __future__ import annotations

import logging
import os
from typing import Any, Mapping

try:  # Repository import.
    from ..common.contract import (
        ContractError,
        build_envelope_and_pointer,
        canonical_json,
    )
    from ..common.safe_logging import safe_log
except ImportError:  # Cloud Functions ZIP import.
    from common.contract import ContractError, build_envelope_and_pointer, canonical_json
    from common.safe_logging import safe_log


LOGGER = logging.getLogger(__name__)
DEFAULT_S3_ENDPOINT = "https://storage.yandexcloud.net"
DEFAULT_YMQ_ENDPOINT = "https://message-queue.api.cloud.yandex.net"


class IntakeError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _required_env(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name) or "").strip()
    if not value:
        raise IntakeError(f"env_missing:{name.lower()}")
    return value


def _env_int(
    env: Mapping[str, str], name: str, default: int, minimum: int, maximum: int
) -> int:
    raw = str(env.get(name) or default).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise IntakeError(f"env_invalid:{name.lower()}") from exc
    if value < minimum or value > maximum:
        raise IntakeError(f"env_invalid:{name.lower()}")
    return value


def _mail_messages(event: Any) -> list[Mapping[str, Any]]:
    if not isinstance(event, Mapping):
        raise IntakeError("event_not_object")
    messages = event.get("messages")
    if not isinstance(messages, list) or not messages:
        raise IntakeError("event_messages_invalid")
    if len(messages) > 10:
        raise IntakeError("event_batch_too_large")
    if not all(isinstance(message, Mapping) for message in messages):
        raise IntakeError("event_message_invalid")
    return messages


def process_mail_event(
    event: Any,
    *,
    s3_client: Any,
    sqs_client: Any,
    env: Mapping[str, str],
    logger: logging.Logger = LOGGER,
) -> dict[str, Any]:
    bucket = _required_env(env, "EMAIL_INBOUND_BUCKET")
    queue_url = _required_env(env, "EMAIL_INBOUND_QUEUE_URL")
    mailbox = _required_env(env, "EMAIL_INBOUND_MAILBOX")
    idempotency_secret = _required_env(env, "EMAIL_INBOUND_IDEMPOTENCY_SECRET")
    max_body_bytes = _env_int(
        env, "EMAIL_INBOUND_MAX_BODY_BYTES", 220_000, 1, 220_000
    )
    retention_days = _env_int(env, "EMAIL_INBOUND_RETENTION_DAYS", 30, 1, 365)

    messages = _mail_messages(event)
    accepted: list[str] = []
    for message in messages:
        try:
            _envelope, pointer, envelope_bytes = build_envelope_and_pointer(
                message,
                mailbox=mailbox,
                bucket=bucket,
                idempotency_secret=idempotency_secret,
                max_body_bytes=max_body_bytes,
                retention_days=retention_days,
            )
            inbound_id = pointer["inbound_id"]
            object_ref = pointer["object"]
            s3_client.put_object(
                Bucket=bucket,
                Key=object_ref["key"],
                Body=envelope_bytes,
                ContentType="application/json; charset=utf-8",
                CacheControl="no-store",
                Metadata={
                    "schema": "email-inbound-envelope-v1",
                    "inbound-id": inbound_id,
                },
            )
            queue_body = canonical_json(pointer)
            if len(queue_body) > 32_768:
                raise ContractError("queue_pointer_too_large")
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=queue_body.decode("utf-8"),
            )
            queue_message_id = str(response.get("MessageId") or "")
            safe_log(
                logger,
                logging.INFO,
                stage="intake_enqueued",
                inbound_id=inbound_id,
                queue_message_id=queue_message_id,
                body_bytes=pointer["body"]["bytes"],
                attachment_count=pointer["attachments"]["count"],
            )
            accepted.append(inbound_id)
        except ContractError as exc:
            safe_log(
                logger,
                logging.ERROR,
                stage="intake_rejected",
                error_code=exc.code,
            )
            raise IntakeError(exc.code) from exc
        except IntakeError:
            raise
        except Exception:
            safe_log(
                logger,
                logging.ERROR,
                stage="intake_failed",
                error_code="provider_operation_failed",
            )
            # Do not chain provider exceptions: Cloud Functions prints unhandled
            # tracebacks and SDK error text may echo mail/object identifiers.
            raise IntakeError("provider_operation_failed") from None
    return {"ok": True, "accepted": len(accepted), "inbound_ids": accepted}


def _aws_clients(env: Mapping[str, str]) -> tuple[Any, Any]:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - deployment packaging gate.
        raise IntakeError("boto3_unavailable") from exc
    credentials = {
        "aws_access_key_id": _required_env(env, "EMAIL_INBOUND_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": _required_env(
            env, "EMAIL_INBOUND_AWS_SECRET_ACCESS_KEY"
        ),
        "region_name": str(env.get("EMAIL_INBOUND_AWS_REGION") or "ru-central1"),
    }
    s3_client = boto3.client(
        "s3",
        endpoint_url=str(env.get("EMAIL_INBOUND_S3_ENDPOINT") or DEFAULT_S3_ENDPOINT),
        **credentials,
    )
    sqs_client = boto3.client(
        "sqs",
        endpoint_url=str(env.get("EMAIL_INBOUND_YMQ_ENDPOINT") or DEFAULT_YMQ_ENDPOINT),
        **credentials,
    )
    return s3_client, sqs_client


def handler(event: Any, context: Any) -> dict[str, Any]:
    del context
    env = os.environ
    s3_client, sqs_client = _aws_clients(env)
    return process_mail_event(
        event,
        s3_client=s3_client,
        sqs_client=sqs_client,
        env=env,
    )
