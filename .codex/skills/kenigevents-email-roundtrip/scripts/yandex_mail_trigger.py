#!/usr/bin/env python3
"""Read-only operator helper for the KenigEvents Yandex Mail Trigger."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


DEFAULT_FOLDER_ID = "b1g0v4ur96gis5kot6ku"
DEFAULT_TRIGGER_NAME = "kenigevents-email-mail-trigger"
DEFAULT_BUCKET_PREFIX = "kenigevents-email-inbound-prod-"
OTP_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")


class RoundtripError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise RoundtripError("since_invalid") from exc
    if parsed.tzinfo is None:
        raise RoundtripError("since_timezone_required")
    return parsed.astimezone(UTC)


def find_yc(explicit: str | None = None) -> str:
    candidates = [explicit, shutil.which("yc"), "/home/dev/yandex-cloud/bin/yc"]
    for candidate in candidates:
        if candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise RoundtripError("yc_cli_missing")


def yc_json(yc: str, args: list[str], profile: str | None = None) -> Any:
    command = [yc]
    if profile:
        command += ["--profile", profile]
    command += args + ["--format", "json"]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=45, check=False)
    if completed.returncode:
        raise RoundtripError(f"yc_command_failed:{args[0]}:{args[1] if len(args) > 1 else 'unknown'}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RoundtripError("yc_output_invalid") from exc


def mail_trigger(rows: Iterable[dict[str, Any]], name: str) -> dict[str, Any]:
    matches = [row for row in rows if row.get("name") == name]
    if len(matches) != 1:
        raise RoundtripError("trigger_not_found" if not matches else "trigger_not_unique")
    rule = matches[0].get("rule") or {}
    mail = rule.get("mail") or {}
    if not mail.get("email"):
        raise RoundtripError("trigger_email_missing")
    return matches[0]


def private_bucket(rows: Iterable[dict[str, Any]], prefix: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row.get("name") or "").startswith(prefix)]
    if len(matches) != 1:
        raise RoundtripError("bucket_not_found" if not matches else "bucket_not_unique")
    flags = matches[0].get("anonymous_access_flags") or {}
    if any(flags.get(key) for key in ("read", "list", "config_read")):
        raise RoundtripError("bucket_not_private")
    return matches[0]


def mask_address(value: str) -> str:
    local, _, domain = value.partition("@")
    return f"{local[:8]}…@{domain}" if domain else "<invalid>"


def header_values(envelope: dict[str, Any], name: str) -> list[str]:
    wanted = name.casefold()
    result: list[str] = []
    for row in envelope.get("headers") or []:
        if isinstance(row, dict) and str(row.get("name") or "").casefold() == wanted:
            result.extend(str(value) for value in row.get("values") or [])
    return result


def extract_otp(envelope: dict[str, Any]) -> str:
    body = str((envelope.get("trigger_body") or {}).get("value") or "")
    values = sorted(set(OTP_RE.findall(body)))
    if not values:
        raise RoundtripError("otp_not_found")
    if len(values) != 1:
        raise RoundtripError("otp_ambiguous")
    return values[0]


def envelope_matches(
    envelope: dict[str, Any],
    *,
    since: datetime,
    from_pattern: re.Pattern[str],
    subject_pattern: re.Pattern[str],
    recipient: str | None,
) -> bool:
    try:
        received_at = parse_iso(str(envelope.get("received_at") or ""))
    except RoundtripError:
        return False
    if received_at < since:
        return False
    senders = "\n".join(header_values(envelope, "From"))
    subjects = "\n".join(header_values(envelope, "Subject"))
    recipients = "\n".join(header_values(envelope, "To"))
    return bool(
        from_pattern.search(senders)
        and subject_pattern.search(subjects)
        and (not recipient or recipient.casefold() in recipients.casefold())
    )


def date_prefixes(since: datetime, now: datetime) -> list[str]:
    day = since.astimezone(UTC).date()
    final = now.astimezone(UTC).date()
    result = []
    while day <= final:
        result.append(f"messages/{day:%Y/%m/%d}/")
        day += timedelta(days=1)
    return result


def list_keys(yc: str, profile: str | None, bucket: str, prefixes: Iterable[str]) -> list[str]:
    keys: set[str] = set()
    for prefix in prefixes:
        page = yc_json(
            yc,
            ["storage", "s3api", "list-objects", "--bucket", bucket, "--prefix", prefix, "--max-keys", "1000"],
            profile,
        )
        if page.get("is_truncated"):
            raise RoundtripError("object_listing_truncated")
        keys.update(str(row.get("key")) for row in page.get("contents") or [] if row.get("key"))
    return sorted(keys)


def fetch_envelope(yc: str, profile: str | None, bucket: str, key: str) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="kenigevents-mail-") as directory:
        output = Path(directory) / "envelope.json"
        args = ["storage", "s3api", "get-object", "--bucket", bucket, "--key", key, str(output)]
        command = [yc]
        if profile:
            command += ["--profile", profile]
        completed = subprocess.run(command + args, capture_output=True, text=True, timeout=45, check=False)
        if completed.returncode:
            raise RoundtripError("object_read_failed")
        try:
            value = json.loads(output.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise RoundtripError("envelope_invalid") from exc
    if not isinstance(value, dict):
        raise RoundtripError("envelope_invalid")
    return value


def live_inventory(args: argparse.Namespace) -> tuple[str, dict[str, Any], dict[str, Any]]:
    yc = find_yc(args.yc)
    triggers = yc_json(yc, ["serverless", "trigger", "list", "--folder-id", args.folder_id], args.profile)
    buckets = yc_json(yc, ["storage", "bucket", "list", "--folder-id", args.folder_id], args.profile)
    return yc, mail_trigger(triggers, args.trigger_name), private_bucket(buckets, args.bucket_prefix)


def command_status(args: argparse.Namespace) -> int:
    _, trigger, bucket = live_inventory(args)
    email = trigger["rule"]["mail"]["email"]
    print(json.dumps({
        "ok": trigger.get("status") == "ACTIVE",
        "folder_id": args.folder_id,
        "trigger_name": trigger.get("name"),
        "trigger_status": trigger.get("status"),
        "address_masked": mask_address(email),
        "bucket_name": bucket.get("name"),
        "bucket_private": True,
    }, ensure_ascii=False))
    return 0 if trigger.get("status") == "ACTIVE" else 2


def command_address(args: argparse.Namespace) -> int:
    if not args.reveal_address:
        raise RoundtripError("reveal_address_required")
    _, trigger, _ = live_inventory(args)
    print(trigger["rule"]["mail"]["email"])
    return 0


def command_checkpoint(args: argparse.Namespace) -> int:
    _, trigger, _ = live_inventory(args)
    if trigger.get("status") != "ACTIVE":
        raise RoundtripError("trigger_not_active")
    print(json.dumps({"checkpoint_at": iso_z(utc_now()), "trigger_status": "ACTIVE"}))
    return 0


def command_extract(args: argparse.Namespace) -> int:
    value = json.loads(Path(args.envelope).read_text())
    otp = extract_otp(value)
    if not args.emit_otp:
        print(json.dumps({"otp_length": len(otp), "otp_present": True}))
    else:
        if args.github_mask:
            print(f"::add-mask::{otp}")
        print(json.dumps({"otp": otp, "otp_length": len(otp)}))
    return 0


def command_wait(args: argparse.Namespace) -> int:
    if not args.emit_otp:
        raise RoundtripError("emit_otp_required")
    since = parse_iso(args.since)
    from_pattern = re.compile(args.from_pattern, re.IGNORECASE)
    subject_pattern = re.compile(args.subject_pattern, re.IGNORECASE)
    yc, trigger, bucket = live_inventory(args)
    if trigger.get("status") != "ACTIVE":
        raise RoundtripError("trigger_not_active")
    deadline = time.monotonic() + max(5, min(args.timeout, 300))
    seen: set[str] = set()
    matches: list[tuple[dict[str, Any], str]] = []
    while time.monotonic() < deadline:
        for key in list_keys(yc, args.profile, bucket["name"], date_prefixes(since, utc_now())):
            if key in seen:
                continue
            seen.add(key)
            envelope = fetch_envelope(yc, args.profile, bucket["name"], key)
            if envelope_matches(
                envelope,
                since=since,
                from_pattern=from_pattern,
                subject_pattern=subject_pattern,
                recipient=args.expected_recipient,
            ):
                matches.append((envelope, extract_otp(envelope)))
        if len(matches) > 1:
            raise RoundtripError("mail_duplicate_messages")
        if len(matches) == 1:
            envelope, otp = matches[0]
            received = parse_iso(str(envelope["received_at"]))
            message_id = "\n".join(header_values(envelope, "Message-ID"))
            result = {
                "otp": otp,
                "otp_length": len(otp),
                "matching_message_count": 1,
                "delivery_latency_ms": max(0, int((received - since).total_seconds() * 1000)),
                "message_id_hash": hashlib.sha256(message_id.encode()).hexdigest()[:16],
            }
            if args.github_mask:
                print(f"::add-mask::{otp}")
            print(json.dumps(result))
            return 0
        time.sleep(max(1, min(args.poll, 10)))
    raise RoundtripError("mail_delivery_timeout")


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--yc")
    value.add_argument("--profile")
    value.add_argument("--folder-id", default=DEFAULT_FOLDER_ID)
    value.add_argument("--trigger-name", default=DEFAULT_TRIGGER_NAME)
    value.add_argument("--bucket-prefix", default=DEFAULT_BUCKET_PREFIX)
    commands = value.add_subparsers(dest="command", required=True)
    commands.add_parser("status").set_defaults(handler=command_status)
    address = commands.add_parser("address")
    address.add_argument("--reveal-address", action="store_true")
    address.set_defaults(handler=command_address)
    commands.add_parser("checkpoint").set_defaults(handler=command_checkpoint)
    extract = commands.add_parser("extract-otp")
    extract.add_argument("--envelope", required=True)
    extract.add_argument("--emit-otp", action="store_true")
    extract.add_argument("--github-mask", action="store_true")
    extract.set_defaults(handler=command_extract)
    wait = commands.add_parser("wait-otp")
    wait.add_argument("--since", required=True)
    wait.add_argument("--from-pattern", required=True)
    wait.add_argument("--subject-pattern", required=True)
    wait.add_argument("--expected-recipient")
    wait.add_argument("--timeout", type=int, default=120)
    wait.add_argument("--poll", type=int, default=3)
    wait.add_argument("--emit-otp", action="store_true")
    wait.add_argument("--github-mask", action="store_true")
    wait.set_defaults(handler=command_wait)
    return value


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (RoundtripError, re.error, OSError, json.JSONDecodeError) as exc:
        code = str(exc) if isinstance(exc, RoundtripError) else "input_invalid"
        print(json.dumps({"ok": False, "error": code}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
