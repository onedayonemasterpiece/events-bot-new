from __future__ import annotations

import json
import logging
import re
from typing import Any


_INBOUND_ID_RE = re.compile(r"^[0-9a-f]{64}$")
_CODE_RE = re.compile(r"^[a-z0-9_.:-]{1,80}$")
_ALLOWED_FIELDS = {
    "attachment_count",
    "body_bytes",
    "error_code",
    "inbound_id",
    "message_count",
    "queue_message_id",
    "stage",
}


def _safe_value(name: str, value: Any) -> Any:
    if name in {"attachment_count", "body_bytes", "message_count"}:
        if not isinstance(value, int) or value < 0:
            return None
        return value
    text = str(value or "")
    if name == "inbound_id":
        return text if _INBOUND_ID_RE.fullmatch(text) else None
    if name == "queue_message_id":
        return text[:128] if _CODE_RE.fullmatch(text) else None
    return text if _CODE_RE.fullmatch(text) else None


def safe_log(logger: logging.Logger, level: int, **fields: Any) -> None:
    payload: dict[str, Any] = {"component": "email_inbound"}
    for name, value in fields.items():
        if name not in _ALLOWED_FIELDS:
            continue
        safe = _safe_value(name, value)
        if safe is not None:
            payload[name] = safe
    logger.log(level, json.dumps(payload, sort_keys=True, separators=(",", ":")))
