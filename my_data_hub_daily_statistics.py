"""Opt-in daily aggregate producer for the my-data-hub connector intake.

The producer reads only non-sensitive aggregate dimensions from the events-bot SQLite
database.  It never connects to the my-data-hub database: the only outbound path is the
versioned HTTPS intake envelope retained in a durable local filesystem spool.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

CONTRACT_VERSION = "my-data-hub-data-connector.v1"
CONNECTOR_ID = "events-bot.daily-statistics"
DATA_PRODUCT = "events-bot.daily-statistics.v1"
SCHEMA_VERSION = "events-bot-daily-statistics.v1"
DEFAULT_TIMEZONE = "Europe/Kaliningrad"
MAX_SAFE_JSON_INTEGER = (2**53) - 1
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
SOURCE_REVISION_RE = re.compile(r"^[a-f0-9]{40}$")
MAX_DIMENSION_VALUES = 500
ENVELOPE_KEYS = {
    "batch_id",
    "connector_id",
    "contract_version",
    "data_product",
    "delivery_mode",
    "idempotency_key",
    "inline_records",
    "observed_period",
    "payload_sha256",
    "produced_at",
    "record_count",
    "schema_version",
    "source_cursor",
    "trace",
}
RECEIPT_KEYS = {
    "accepted_at",
    "batch_id",
    "connector_id",
    "envelope_sha256",
    "idempotency_key",
    "payload_sha256",
    "receipt_id",
    "status",
}


class ProducerError(RuntimeError):
    """Base error for a fail-closed producer operation."""


class ConfigurationError(ProducerError):
    """The opt-in producer configuration is incomplete or unsafe."""


class SpoolIntegrityError(ProducerError):
    """Durable spool evidence is missing, malformed or inconsistent."""


class SpoolConflict(ProducerError):
    """An idempotency identity is already bound to different content."""


def _canonical_number(value: float) -> str:
    if isinstance(value, int):
        if not -MAX_SAFE_JSON_INTEGER <= value <= MAX_SAFE_JSON_INTEGER:
            raise ValueError("integer is outside the RFC 8785 interoperable range")
        return str(value)
    if not math.isfinite(value):
        raise ValueError("RFC 8785 does not permit non-finite numbers")
    if value == 0:
        return "0"
    sign = "-" if value < 0 else ""
    absolute = abs(value)
    rendered = repr(absolute).lower()
    if "e" not in rendered:
        return sign + rendered.removesuffix(".0")
    mantissa, exponent_text = rendered.split("e", 1)
    exponent = int(exponent_text)
    digits = mantissa.replace(".", "").rstrip("0")
    if 1e-6 <= absolute < 1e21:
        decimal_position = 1 + exponent
        if decimal_position <= 0:
            fixed = "0." + ("0" * -decimal_position) + digits
        elif decimal_position >= len(digits):
            fixed = digits + ("0" * (decimal_position - len(digits)))
        else:
            fixed = digits[:decimal_position] + "." + digits[decimal_position:]
        return sign + fixed
    scientific = digits if len(digits) == 1 else digits[0] + "." + digits[1:]
    exponent_sign = "+" if exponent >= 0 else ""
    return f"{sign}{scientific}e{exponent_sign}{exponent}"


def canonical_json_bytes(value: Any) -> bytes:
    """Return RFC 8785-compatible canonical UTF-8 JSON bytes.

    This intentionally matches the implementation in the my-data-hub connector contract:
    UTF-16 object-key ordering, I-JSON numbers and no insignificant whitespace.
    """

    def encode(item: Any) -> str:
        if item is None:
            return "null"
        if item is True:
            return "true"
        if item is False:
            return "false"
        if isinstance(item, (int, float)):
            return _canonical_number(item)
        if isinstance(item, str):
            try:
                item.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValueError("lone Unicode surrogates are not permitted") from exc
            return json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        if isinstance(item, list):
            return "[" + ",".join(encode(element) for element in item) + "]"
        if isinstance(item, dict):
            if not all(isinstance(key, str) for key in item):
                raise ValueError("JSON object keys must be strings")
            keys = sorted(item, key=lambda key: key.encode("utf-16-be", errors="surrogatepass"))
            return "{" + ",".join(f"{encode(key)}:{encode(item[key])}" for key in keys) + "}"
        raise ValueError(f"unsupported JSON value type: {type(item).__name__}")

    return encode(value).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _format_time(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must include a timezone offset")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise TypeError("timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone offset")
    return parsed


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _dimension(value: Any) -> str:
    if not isinstance(value, str):
        return "unknown"
    cleaned = value.strip()
    if not cleaned or len(cleaned) > 100 or any(ord(character) < 32 for character in cleaned):
        return "unknown"
    return cleaned


@dataclass(frozen=True, slots=True)
class DailyAggregate:
    reporting_date: date
    timezone: str
    events_added_total: int
    counts_by_city: dict[str, int]
    counts_by_type: dict[str, int]
    source_revision: str

    def as_record(self) -> dict[str, Any]:
        return {
            "counts_by_city": self.counts_by_city,
            "counts_by_type": self.counts_by_type,
            "events_added_total": self.events_added_total,
            "reporting_date": self.reporting_date.isoformat(),
            "source_revision": self.source_revision,
            "timezone": self.timezone,
        }


def observed_period(reporting_date: date, timezone_name: str) -> tuple[datetime, datetime]:
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ConfigurationError(f"unknown reporting timezone: {timezone_name}") from exc
    start = datetime.combine(reporting_date, datetime.min.time(), tzinfo=timezone)
    return start.astimezone(UTC), (start + timedelta(days=1)).astimezone(UTC)


def read_daily_aggregate(
    database_path: Path,
    *,
    reporting_date: date,
    timezone_name: str,
    source_revision: str,
) -> DailyAggregate:
    """Read a bounded non-sensitive aggregate from events-bot SQLite in read-only mode."""
    if not SOURCE_REVISION_RE.fullmatch(source_revision):
        raise ConfigurationError("source revision must be a non-secret release identifier")
    if not database_path.is_file():
        raise ConfigurationError(f"events-bot database does not exist: {database_path}")
    start, end = observed_period(reporting_date, timezone_name)
    uri = f"{database_path.resolve().as_uri()}?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True, timeout=30) as connection:
            connection.execute("PRAGMA query_only=ON")
            rows = connection.execute(
                """
                SELECT city, event_type
                FROM event
                WHERE added_at >= ? AND added_at < ?
                """,
                (
                    start.strftime("%Y-%m-%d %H:%M:%S"),
                    end.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            ).fetchall()
    except sqlite3.Error as exc:
        raise ProducerError(f"cannot read daily event aggregate: {exc}") from exc

    cities = Counter(_dimension(row[0]) for row in rows)
    event_types = Counter(_dimension(row[1]) for row in rows)
    if len(cities) > MAX_DIMENSION_VALUES or len(event_types) > MAX_DIMENSION_VALUES:
        raise ProducerError("daily aggregate exceeds the bounded dimension cardinality")
    return DailyAggregate(
        reporting_date=reporting_date,
        timezone=timezone_name,
        events_added_total=len(rows),
        counts_by_city=dict(sorted(cities.items())),
        counts_by_type=dict(sorted(event_types.items())),
        source_revision=source_revision,
    )


def idempotency_key(reporting_date: date, timezone_name: str) -> str:
    partition = timezone_name.replace("/", "-")
    return f"{CONNECTOR_ID}:{reporting_date.isoformat()}:{partition}:v1"


def build_envelope(aggregate: DailyAggregate) -> bytes:
    records = [aggregate.as_record()]
    payload_hash = sha256_bytes(canonical_json_bytes(records))
    start, end = observed_period(aggregate.reporting_date, aggregate.timezone)
    identity = idempotency_key(aggregate.reporting_date, aggregate.timezone)
    envelope = {
        "batch_id": str(uuid5(NAMESPACE_URL, f"my-data-hub:{identity}")),
        "connector_id": CONNECTOR_ID,
        "contract_version": CONTRACT_VERSION,
        "data_product": DATA_PRODUCT,
        "delivery_mode": "push",
        "idempotency_key": identity,
        "inline_records": records,
        "observed_period": {
            "end": _format_time(end),
            "start": _format_time(start),
            "timezone": aggregate.timezone,
        },
        "payload_sha256": payload_hash,
        # Period end is deterministic and states when this completed daily observation
        # first became producible. Retries never invent a new produced_at identity.
        "produced_at": _format_time(end),
        "record_count": 1,
        "schema_version": SCHEMA_VERSION,
        "source_cursor": {
            "partition": "daily",
            "sequence": int(aggregate.reporting_date.strftime("%Y%m%d")),
            "watermark": aggregate.reporting_date.isoformat(),
        },
        "trace": {},
    }
    exact_bytes = canonical_json_bytes(envelope)
    validate_envelope_bytes(exact_bytes)
    return exact_bytes


@dataclass(frozen=True, slots=True)
class ValidatedEnvelope:
    value: dict[str, Any]
    exact_bytes: bytes
    exact_bytes_sha256: str
    envelope_sha256: str


def validate_envelope_bytes(raw: bytes) -> ValidatedEnvelope:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SpoolIntegrityError("envelope is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict) or set(value) != ENVELOPE_KEYS:
        raise SpoolIntegrityError("envelope fields do not match the v1 daily-statistics contract")
    if raw != canonical_json_bytes(value):
        raise SpoolIntegrityError("spooled envelope bytes are not canonical JSON")
    try:
        UUID(value["batch_id"])
        start = _parse_time(value["observed_period"]["start"])
        end = _parse_time(value["observed_period"]["end"])
        produced_at = _parse_time(value["produced_at"])
        records = value["inline_records"]
        if value["contract_version"] != CONTRACT_VERSION:
            raise ValueError("wrong contract_version")
        if value["connector_id"] != CONNECTOR_ID or value["data_product"] != DATA_PRODUCT:
            raise ValueError("wrong connector identity")
        if value["schema_version"] != SCHEMA_VERSION or value["delivery_mode"] != "push":
            raise ValueError("wrong product schema or delivery mode")
        if not isinstance(records, list) or len(records) != value["record_count"]:
            raise ValueError("record_count does not match inline_records")
        if value["record_count"] != 1:
            raise ValueError("daily-statistics envelope must contain one aggregate")
        if start >= end or produced_at < end:
            raise ValueError("invalid observed period or produced_at")
        payload_hash = sha256_bytes(canonical_json_bytes(records))
        if value["payload_sha256"] != payload_hash:
            raise ValueError("payload_sha256 mismatch")
        if not SHA256_RE.fullmatch(value["payload_sha256"]):
            raise ValueError("invalid payload_sha256")
    except (KeyError, TypeError, ValueError) as exc:
        raise SpoolIntegrityError(f"invalid daily-statistics envelope: {exc}") from exc
    exact_hash = sha256_bytes(raw)
    return ValidatedEnvelope(value, raw, exact_hash, exact_hash)


@dataclass(frozen=True, slots=True)
class Receipt:
    receipt_id: str
    status: str
    connector_id: str
    batch_id: str
    idempotency_key: str
    payload_sha256: str
    envelope_sha256: str
    accepted_at: str

    def as_dict(self) -> dict[str, str]:
        return {
            "accepted_at": self.accepted_at,
            "batch_id": self.batch_id,
            "connector_id": self.connector_id,
            "envelope_sha256": self.envelope_sha256,
            "idempotency_key": self.idempotency_key,
            "payload_sha256": self.payload_sha256,
            "receipt_id": self.receipt_id,
            "status": self.status,
        }


def validate_receipt(value: Any, *, expected: ValidatedEnvelope, status: str) -> Receipt:
    if isinstance(value, dict) and isinstance(value.get("receipt"), dict):
        value = value["receipt"]
    if not isinstance(value, dict) or set(value) != RECEIPT_KEYS:
        raise SpoolIntegrityError("intake response does not contain an exact v1 receipt")
    envelope = expected.value
    try:
        UUID(value["receipt_id"])
        UUID(value["batch_id"])
        _parse_time(value["accepted_at"])
        receipt = Receipt(
            receipt_id=value["receipt_id"],
            status=status,
            connector_id=value["connector_id"],
            batch_id=value["batch_id"],
            idempotency_key=value["idempotency_key"],
            payload_sha256=value["payload_sha256"],
            envelope_sha256=value["envelope_sha256"],
            accepted_at=value["accepted_at"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SpoolIntegrityError("intake receipt contains invalid typed fields") from exc
    if receipt.status not in {"accepted", "replayed"}:
        raise SpoolIntegrityError("intake receipt has an invalid status")
    if (
        receipt.connector_id != CONNECTOR_ID
        or receipt.batch_id != envelope["batch_id"]
        or receipt.idempotency_key != envelope["idempotency_key"]
        or receipt.payload_sha256 != envelope["payload_sha256"]
        or receipt.envelope_sha256 != expected.envelope_sha256
    ):
        raise SpoolIntegrityError("intake receipt does not attest the exact spooled envelope")
    return receipt


@dataclass(frozen=True, slots=True)
class SpoolState:
    queued_at: datetime
    attempts: int = 0
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    envelope_sha256: str = ""

    def to_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "attempts": self.attempts,
                "envelope_sha256": self.envelope_sha256,
                "last_error": self.last_error,
                "next_attempt_at": (
                    _format_time(self.next_attempt_at) if self.next_attempt_at else None
                ),
                "queued_at": _format_time(self.queued_at),
            }
        )

    @classmethod
    def from_bytes(cls, raw: bytes) -> SpoolState:
        try:
            value = json.loads(raw)
            return cls(
                queued_at=_parse_time(value["queued_at"]),
                attempts=int(value["attempts"]),
                next_attempt_at=(
                    _parse_time(value["next_attempt_at"])
                    if value.get("next_attempt_at")
                    else None
                ),
                last_error=value.get("last_error"),
                envelope_sha256=value["envelope_sha256"],
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SpoolIntegrityError("invalid durable spool state") from exc


@dataclass(frozen=True, slots=True)
class SpoolItem:
    spool_id: str
    envelope_path: Path
    state_path: Path
    validated: ValidatedEnvelope
    state: SpoolState


def spool_id_for_identity(identity: str) -> str:
    return sha256_bytes(
        canonical_json_bytes({"connector_id": CONNECTOR_ID, "idempotency_key": identity})
    )


class DurableDailyStatisticsSpool:
    """Filesystem outbox that never reserializes an envelope during retry."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.pending_dir = root / "pending"
        self.delivered_dir = root / "delivered"
        self.receipts_dir = root / "receipts"
        self.quarantine_dir = root / "quarantine"
        for directory in (
            self.root,
            self.pending_dir,
            self.delivered_dir,
            self.receipts_dir,
            self.quarantine_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(directory, 0o700)

    @contextmanager
    def producer_lock(self) -> Iterator[None]:
        lock_path = self.root / ".producer.lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise ProducerError("another daily-statistics producer owns the spool lock") from exc
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def paths_for(self, identity: str) -> dict[str, Path]:
        spool_id = spool_id_for_identity(identity)
        return {
            "delivered": self.delivered_dir / f"{spool_id}.json",
            "pending": self.pending_dir / f"{spool_id}.json",
            "quarantine": self.quarantine_dir / f"{spool_id}.json",
            "receipt": self.receipts_dir / f"{spool_id}.json",
            "state": self.pending_dir / f"{spool_id}.state.json",
        }

    def contains(self, identity: str) -> bool:
        paths = self.paths_for(identity)
        return any(paths[name].exists() for name in ("pending", "delivered", "receipt", "quarantine"))

    def enqueue(self, exact_bytes: bytes, *, queued_at: datetime) -> SpoolItem:
        validated = validate_envelope_bytes(exact_bytes)
        identity = validated.value["idempotency_key"]
        paths = self.paths_for(identity)
        spool_id = spool_id_for_identity(identity)
        if paths["pending"].exists():
            existing = validate_envelope_bytes(paths["pending"].read_bytes())
            if existing.envelope_sha256 != validated.envelope_sha256:
                raise SpoolConflict("idempotency identity is spooled with different content")
            state = SpoolState.from_bytes(paths["state"].read_bytes())
            return SpoolItem(spool_id, paths["pending"], paths["state"], existing, state)
        if paths["receipt"].exists() or paths["delivered"].exists():
            raise SpoolConflict("idempotency identity already has a durable receipt")
        if paths["quarantine"].exists():
            raise SpoolConflict("idempotency identity is quarantined")
        state = SpoolState(queued_at=queued_at, envelope_sha256=validated.envelope_sha256)
        _atomic_write(paths["pending"], exact_bytes)
        _atomic_write(paths["state"], state.to_bytes())
        return SpoolItem(spool_id, paths["pending"], paths["state"], validated, state)

    def _read_item(self, envelope_path: Path) -> SpoolItem:
        spool_id = envelope_path.stem
        state_path = self.pending_dir / f"{spool_id}.state.json"
        validated = validate_envelope_bytes(envelope_path.read_bytes())
        if spool_id_for_identity(validated.value["idempotency_key"]) != spool_id:
            raise SpoolIntegrityError(f"pending identity mismatch for {spool_id}")
        if state_path.is_file():
            state = SpoolState.from_bytes(state_path.read_bytes())
        else:
            state = SpoolState(
                queued_at=datetime.fromtimestamp(envelope_path.stat().st_mtime, tz=UTC),
                last_error="state_recovered_after_interrupted_enqueue",
                envelope_sha256=validated.envelope_sha256,
            )
            _atomic_write(state_path, state.to_bytes())
        if state.envelope_sha256 != validated.envelope_sha256:
            raise SpoolIntegrityError(f"pending envelope hash mismatch for {spool_id}")
        return SpoolItem(spool_id, envelope_path, state_path, validated, state)

    def _finish_receipted_item(self, item: SpoolItem, receipt_path: Path) -> None:
        value = json.loads(receipt_path.read_bytes())
        status = value.get("status") if isinstance(value, dict) else None
        validate_receipt(value, expected=item.validated, status=status)
        delivered_path = self.delivered_dir / f"{item.spool_id}.json"
        if delivered_path.exists():
            if delivered_path.read_bytes() != item.validated.exact_bytes:
                raise SpoolIntegrityError("delivered history conflicts with pending envelope")
        else:
            _atomic_write(delivered_path, item.validated.exact_bytes)
        item.envelope_path.unlink(missing_ok=True)
        item.state_path.unlink(missing_ok=True)
        _fsync_directory(self.pending_dir)

    def pending(self, *, ready_at: datetime) -> list[SpoolItem]:
        items: list[SpoolItem] = []
        for envelope_path in sorted(self.pending_dir.glob("*.json")):
            if envelope_path.name.endswith(".state.json"):
                continue
            item = self._read_item(envelope_path)
            receipt_path = self.receipts_dir / f"{item.spool_id}.json"
            if receipt_path.exists():
                self._finish_receipted_item(item, receipt_path)
                continue
            if item.state.next_attempt_at is None or item.state.next_attempt_at <= ready_at:
                items.append(item)
        return sorted(items, key=lambda item: (item.state.queued_at, item.spool_id))

    def record_retry(self, item: SpoolItem, *, error: str, next_attempt_at: datetime) -> None:
        state = SpoolState(
            queued_at=item.state.queued_at,
            attempts=item.state.attempts + 1,
            next_attempt_at=next_attempt_at,
            last_error=error[:1000],
            envelope_sha256=item.validated.envelope_sha256,
        )
        _atomic_write(item.state_path, state.to_bytes())

    def acknowledge(self, item: SpoolItem, receipt: Receipt) -> None:
        validate_receipt(receipt.as_dict(), expected=item.validated, status=receipt.status)
        receipt_path = self.receipts_dir / f"{item.spool_id}.json"
        _atomic_write(receipt_path, canonical_json_bytes(receipt.as_dict()))
        self._finish_receipted_item(item, receipt_path)

    def quarantine(self, item: SpoolItem, *, reason: str, message: str, now: datetime) -> None:
        envelope_path = self.quarantine_dir / f"{item.spool_id}.json"
        state_path = self.quarantine_dir / f"{item.spool_id}.state.json"
        evidence = {
            "envelope_sha256": item.validated.envelope_sha256,
            "message": message[:1000],
            "quarantined_at": _format_time(now),
            "reason": reason,
        }
        _atomic_write(envelope_path, item.validated.exact_bytes)
        _atomic_write(state_path, canonical_json_bytes(evidence))
        item.envelope_path.unlink(missing_ok=True)
        item.state_path.unlink(missing_ok=True)
        _fsync_directory(self.pending_dir)

    def health(self) -> dict[str, Any]:
        pending = [
            path
            for path in self.pending_dir.glob("*.json")
            if not path.name.endswith(".state.json")
        ]
        oldest = min((path.stat().st_mtime for path in pending), default=None)
        receipts = list(self.receipts_dir.glob("*.json"))
        latest_receipt = max((path.stat().st_mtime for path in receipts), default=None)
        return {
            "last_receipt_at": (
                _format_time(datetime.fromtimestamp(latest_receipt, tz=UTC))
                if latest_receipt is not None
                else None
            ),
            "oldest_spooled_at": (
                _format_time(datetime.fromtimestamp(oldest, tz=UTC))
                if oldest is not None
                else None
            ),
            "pending": len(pending),
            "quarantined": len(list(self.quarantine_dir.glob("*.state.json"))),
        }


class DeliveryDisposition(StrEnum):
    ACCEPTED = "accepted"
    REPLAYED = "replayed"
    RETRY = "retry"
    CONFLICT = "conflict"
    REJECTED = "rejected"
    AUTH_FAILURE = "auth_failure"


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    disposition: DeliveryDisposition
    receipt: Receipt | None = None
    message: str = ""
    retry_after_seconds: float | None = None


class Transport(Protocol):
    def submit(self, item: SpoolItem) -> DeliveryResult: ...


@dataclass(slots=True)
class HttpTransport:
    intake_url: str
    service_token: str
    timeout_seconds: float = 15.0

    def _message(self, body: bytes) -> str:
        try:
            value = json.loads(body)
            if isinstance(value, dict):
                rendered = str(value.get("detail") or value.get("message") or "")
            else:
                rendered = ""
        except (UnicodeDecodeError, json.JSONDecodeError):
            rendered = ""
        return rendered.replace(self.service_token, "[REDACTED]")[:1000]

    def _success(self, item: SpoolItem, status_code: int, body: bytes) -> DeliveryResult:
        try:
            value = json.loads(body)
            status = "replayed" if status_code in {200, 201} else "accepted"
            receipt = validate_receipt(value, expected=item.validated, status=status)
        except (UnicodeDecodeError, json.JSONDecodeError, SpoolIntegrityError) as exc:
            return DeliveryResult(
                DeliveryDisposition.RETRY,
                message=f"successful intake response had an invalid receipt: {exc}",
            )
        disposition = (
            DeliveryDisposition.REPLAYED
            if status == "replayed"
            else DeliveryDisposition.ACCEPTED
        )
        return DeliveryResult(disposition, receipt=receipt)

    def submit(self, item: SpoolItem) -> DeliveryResult:
        request = Request(
            self.intake_url,
            data=item.validated.exact_bytes,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.service_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read(256 * 1024)
                if response.status in {200, 201, 202}:
                    return self._success(item, response.status, body)
                return DeliveryResult(
                    DeliveryDisposition.RETRY,
                    message=f"unexpected intake HTTP status {response.status}",
                )
        except HTTPError as exc:
            body = exc.read(256 * 1024)
            message = self._message(body) or f"intake HTTP {exc.code}"
            if exc.code == 409:
                return DeliveryResult(DeliveryDisposition.CONFLICT, message=message)
            if exc.code == 422:
                return DeliveryResult(DeliveryDisposition.REJECTED, message=message)
            if exc.code in {401, 403}:
                return DeliveryResult(DeliveryDisposition.AUTH_FAILURE, message=message)
            if exc.code in {429, 502, 503, 504}:
                retry_after = exc.headers.get("Retry-After")
                try:
                    retry_seconds = min(86400.0, max(0.0, float(retry_after)))
                except (TypeError, ValueError):
                    retry_seconds = None
                return DeliveryResult(
                    DeliveryDisposition.RETRY,
                    message=message,
                    retry_after_seconds=retry_seconds,
                )
            return DeliveryResult(DeliveryDisposition.REJECTED, message=message)
        except (TimeoutError, URLError) as exc:
            return DeliveryResult(
                DeliveryDisposition.RETRY,
                message=f"intake unavailable: {type(exc).__name__}",
            )


def _retry_seconds(item: SpoolItem) -> float:
    base = min(3600.0, 60.0 * (2**item.state.attempts))
    digest = hashlib.sha256(f"{item.spool_id}:{item.state.attempts}".encode()).digest()
    jitter = 0.8 + (int.from_bytes(digest[:2], "big") / 65535.0) * 0.4
    return base * jitter


@dataclass(frozen=True, slots=True)
class DeliverySummary:
    attempted: int = 0
    delivered: int = 0
    deferred: int = 0
    quarantined: int = 0


def deliver_ready(
    spool: DurableDailyStatisticsSpool,
    transport: Transport,
    *,
    now: datetime,
    limit: int = 20,
) -> DeliverySummary:
    attempted = delivered = deferred = quarantined = 0
    for item in spool.pending(ready_at=now)[:limit]:
        attempted += 1
        try:
            result = transport.submit(item)
        except Exception as exc:  # noqa: BLE001 - ambiguous transport failures retain exact bytes
            result = DeliveryResult(
                DeliveryDisposition.RETRY,
                message=f"transport exception: {type(exc).__name__}",
            )
        if result.disposition in {DeliveryDisposition.ACCEPTED, DeliveryDisposition.REPLAYED}:
            if result.receipt is None:
                raise SpoolIntegrityError("successful delivery omitted its receipt")
            spool.acknowledge(item, result.receipt)
            delivered += 1
        elif result.disposition is DeliveryDisposition.RETRY:
            delay = result.retry_after_seconds
            if delay is None:
                delay = _retry_seconds(item)
            spool.record_retry(
                item,
                error=result.message or "retryable intake failure",
                next_attempt_at=now + timedelta(seconds=delay),
            )
            deferred += 1
        else:
            spool.quarantine(
                item,
                reason=result.disposition.value,
                message=result.message,
                now=now,
            )
            quarantined += 1
    return DeliverySummary(attempted, delivered, deferred, quarantined)


def _enabled(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class ProducerConfig:
    enabled: bool
    database_path: Path
    spool_root: Path
    intake_url: str
    service_token: str
    timezone_name: str
    source_revision: str
    timeout_seconds: float

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ProducerConfig:
        enabled = _enabled(env.get("MY_DATA_HUB_DAILY_STATISTICS_ENABLED"))
        try:
            timeout_seconds = float(
                env.get("MY_DATA_HUB_DAILY_STATISTICS_TIMEOUT_SECONDS", "15")
            )
        except ValueError as exc:
            raise ConfigurationError("connector timeout must be numeric") from exc
        config = cls(
            enabled=enabled,
            database_path=Path(env.get("MY_DATA_HUB_DAILY_STATISTICS_DB_PATH", "/data/db.sqlite")),
            spool_root=Path(
                env.get(
                    "MY_DATA_HUB_DAILY_STATISTICS_SPOOL_DIR",
                    "/data/my-data-hub-connectors/events-bot.daily-statistics",
                )
            ),
            intake_url=env.get("MY_DATA_HUB_EVENTS_BOT_INTAKE_URL", "").strip(),
            service_token=env.get("MY_DATA_HUB_EVENTS_BOT_SERVICE_TOKEN", "").strip(),
            timezone_name=env.get(
                "MY_DATA_HUB_DAILY_STATISTICS_TIMEZONE", DEFAULT_TIMEZONE
            ).strip(),
            source_revision=env.get(
                "MY_DATA_HUB_EVENTS_BOT_SOURCE_REVISION", ""
            ).strip(),
            timeout_seconds=timeout_seconds,
        )
        if enabled:
            if not config.intake_url or not config.service_token:
                raise ConfigurationError(
                    "enabled producer requires its dedicated intake URL and service token"
                )
            parsed = urlparse(config.intake_url)
            if (
                parsed.scheme != "https"
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
                or not parsed.path.endswith("/intake/v1/batches")
            ):
                raise ConfigurationError(
                    "my-data-hub connector intake must be the credential-free HTTPS "
                    "/intake/v1/batches URL"
                )
            if not 1 <= config.timeout_seconds <= 120:
                raise ConfigurationError("connector timeout must be between 1 and 120 seconds")
            observed_period(datetime.now(UTC).date(), config.timezone_name)
            if not SOURCE_REVISION_RE.fullmatch(config.source_revision):
                raise ConfigurationError("source revision is not a safe release identifier")
        return config


def default_reporting_date(now: datetime, timezone_name: str) -> date:
    if now.tzinfo is None:
        raise ValueError("now must include a timezone offset")
    return now.astimezone(ZoneInfo(timezone_name)).date() - timedelta(days=1)


def run_once(
    config: ProducerConfig,
    *,
    reporting_date: date | None = None,
    now: datetime | None = None,
    transport: Transport | None = None,
) -> dict[str, Any]:
    if not config.enabled:
        return {"enabled": False, "status": "disabled"}
    now = now or datetime.now(UTC)
    selected_date = reporting_date or default_reporting_date(now, config.timezone_name)
    spool = DurableDailyStatisticsSpool(config.spool_root)
    with spool.producer_lock():
        identity = idempotency_key(selected_date, config.timezone_name)
        created = False
        if not spool.contains(identity):
            aggregate = read_daily_aggregate(
                config.database_path,
                reporting_date=selected_date,
                timezone_name=config.timezone_name,
                source_revision=config.source_revision,
            )
            spool.enqueue(build_envelope(aggregate), queued_at=now)
            created = True
        delivery = deliver_ready(
            spool,
            transport
            or HttpTransport(
                config.intake_url,
                config.service_token,
                timeout_seconds=config.timeout_seconds,
            ),
            now=now,
        )
        return {
            "created": created,
            "delivery": {
                "attempted": delivery.attempted,
                "deferred": delivery.deferred,
                "delivered": delivery.delivered,
                "quarantined": delivery.quarantined,
            },
            "enabled": True,
            "health": spool.health(),
            "reporting_date": selected_date.isoformat(),
            "status": (
                "quarantined"
                if delivery.quarantined
                else "deferred"
                if delivery.deferred
                else "ok"
            ),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reporting-date", type=date.fromisoformat)
    args = parser.parse_args(argv)
    try:
        result = run_once(
            ProducerConfig.from_env(os.environ),
            reporting_date=args.reporting_date,
        )
    except ProducerError as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] in {"disabled", "ok"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
