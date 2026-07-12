from __future__ import annotations

import email
import imaplib
import json
import os
import re
from datetime import UTC, datetime
from email.message import Message
from typing import Any, Mapping

try:
    from ..intake.index import IntakeError, _aws_clients, process_mail_event
except ImportError:
    from intake.index import IntakeError, _aws_clients, process_mail_event


STATE_SCHEMA = "kenigevents.email_inbound.imap_cursor.v1"
_UIDVALIDITY_RE = re.compile(rb"UIDVALIDITY\s+(\d+)")


class CollectorError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name) or "").strip()
    if not value:
        raise CollectorError(f"env_missing:{name.lower()}")
    return value


def _decode_part(part: Message) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        value = part.get_payload()
        return value if isinstance(value, str) else ""
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def normalized_body(message: Message, max_bytes: int = 220_000) -> str:
    plain: list[str] = []
    html: list[str] = []
    parts = message.walk() if message.is_multipart() else [message]
    for part in parts:
        if part.is_multipart() or part.get_filename() or part.get_content_disposition() == "attachment":
            continue
        media = part.get_content_type().lower()
        if media == "text/plain":
            plain.append(_decode_part(part))
        elif media == "text/html":
            html.append(_decode_part(part))
    value = "\n".join(plain or html)
    encoded = value.encode("utf-8")
    if len(encoded) > max_bytes:
        raise CollectorError("body_too_large")
    return value


def trigger_message(raw_message: bytes, *, received_at: datetime | None = None) -> dict[str, Any]:
    parsed = email.message_from_bytes(raw_message)
    headers = [
        {"name": name, "values": parsed.get_all(name, [])}
        for name in sorted(set(parsed.keys()), key=str.lower)
    ]
    return {
        "received_at": (received_at or datetime.now(UTC)).isoformat().replace("+00:00", "Z"),
        "headers": headers,
        "attachments": {},
        "message": normalized_body(parsed),
    }


def _state(s3_client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise CollectorError("state_read_failed") from None
    try:
        value = json.loads(response["Body"].read(16_384))
    except Exception:
        raise CollectorError("state_invalid") from None
    if not isinstance(value, dict) or value.get("schema") != STATE_SCHEMA:
        raise CollectorError("state_invalid")
    if not isinstance(value.get("last_uid"), int) or value["last_uid"] < 0:
        raise CollectorError("state_invalid")
    return value


def _put_state(s3_client: Any, bucket: str, key: str, uidvalidity: str, last_uid: int) -> None:
    body = json.dumps(
        {
            "schema": STATE_SCHEMA,
            "uidvalidity": uidvalidity,
            "last_uid": last_uid,
            "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        },
        separators=(",", ":"),
    ).encode()
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="no-store",
        Metadata={"schema": "email-imap-cursor-v1"},
    )


def _uids(client: Any, start: int | None = None) -> list[int]:
    criterion = "ALL" if start is None else f"UID {start}:*"
    status, data = client.uid("search", None, criterion)
    if status != "OK":
        raise CollectorError("imap_search_failed")
    return [int(value) for value in (data[0] or b"").split() if value.isdigit() and (start is None or int(value) >= start)]


def collect(
    *,
    env: Mapping[str, str],
    s3_client: Any,
    sqs_client: Any,
    imap_factory: Any = imaplib.IMAP4_SSL,
) -> dict[str, Any]:
    bucket = _required(env, "EMAIL_INBOUND_BUCKET")
    state_key = str(env.get("EMAIL_INBOUND_IMAP_STATE_KEY") or "state/spaceweb-info-cursor.json")
    host = _required(env, "EMAIL_INBOUND_IMAP_HOST")
    user = _required(env, "EMAIL_INBOUND_IMAP_LOGIN")
    password = _required(env, "EMAIL_INBOUND_IMAP_PASSWORD")
    limit = min(max(int(env.get("EMAIL_INBOUND_IMAP_BATCH_LIMIT") or "20"), 1), 50)
    state = _state(s3_client, bucket, state_key)
    try:
        with imap_factory(host, int(env.get("EMAIL_INBOUND_IMAP_PORT") or "993"), timeout=20) as client:
            if client.login(user, password)[0] != "OK":
                raise CollectorError("imap_login_failed")
            if client.select("INBOX", readonly=True)[0] != "OK":
                raise CollectorError("imap_select_failed")
            status, raw_status = client.status("INBOX", "(UIDVALIDITY)")
            match = _UIDVALIDITY_RE.search((raw_status or [b""])[0]) if status == "OK" else None
            if not match:
                raise CollectorError("imap_uidvalidity_missing")
            uidvalidity = match.group(1).decode()
            all_uids = _uids(client)
            current_uid = max(all_uids, default=0)
            if state is None or str(state.get("uidvalidity")) != uidvalidity:
                _put_state(s3_client, bucket, state_key, uidvalidity, current_uid)
                return {"ok": True, "bootstrapped": True, "collected": 0, "last_uid": current_uid}
            pending = _uids(client, state["last_uid"] + 1)[:limit]
            collected = 0
            last_uid = state["last_uid"]
            for uid in pending:
                status, data = client.uid("fetch", str(uid), "(BODY.PEEK[])")
                if status != "OK":
                    raise CollectorError("imap_fetch_failed")
                raw = next((item[1] for item in data if isinstance(item, tuple)), None)
                if not isinstance(raw, bytes):
                    raise CollectorError("imap_fetch_invalid")
                process_mail_event(
                    {"messages": [trigger_message(raw)]},
                    s3_client=s3_client,
                    sqs_client=sqs_client,
                    env=env,
                )
                last_uid = uid
                _put_state(s3_client, bucket, state_key, uidvalidity, last_uid)
                collected += 1
            return {"ok": True, "bootstrapped": False, "collected": collected, "last_uid": last_uid}
    except CollectorError:
        raise
    except IntakeError as exc:
        raise CollectorError(exc.code) from None
    except Exception:
        raise CollectorError("imap_operation_failed") from None


def handler(event: Any, context: Any) -> dict[str, Any]:
    del event, context
    s3_client, sqs_client = _aws_clients(os.environ)
    return collect(env=os.environ, s3_client=s3_client, sqs_client=sqs_client)
