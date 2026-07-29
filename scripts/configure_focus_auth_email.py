#!/usr/bin/env python3
"""Apply and verify the hosted Supabase Auth magic-link/OTP email contract.

The script reads the project reference and Management API token from the
personalization environment. It never prints the token or live template body.
Apply mode is explicitly gated and emits only hashes/boolean verification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


CONFIRMATION = "APPLY-focus-auth-email-v1"
API_ROOT = "https://api.supabase.com/v1"
OTP_LENGTH = 6
OTP_EXPIRY_SECONDS = 600
ROOT = Path(__file__).resolve().parents[1]
SUBJECT_PATH = ROOT / "supabase/templates/focus-magic-link.subject.txt"
HTML_PATH = ROOT / "supabase/templates/focus-magic-link.html"


class ConfigError(RuntimeError):
    pass


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _request(
    method: str,
    *,
    project_ref: str,
    token: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{API_ROOT}/projects/{project_ref}/config/auth",
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "events-bot-new-focus-auth-email/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            value = json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        raise ConfigError(f"management_api_http_{exc.code}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError("management_api_unavailable") from exc
    if not isinstance(value, dict):
        raise ConfigError("management_api_invalid_response")
    return value


def _desired() -> dict[str, Any]:
    subject = SUBJECT_PATH.read_text(encoding="utf-8").strip()
    html = HTML_PATH.read_text(encoding="utf-8").strip()
    if "{{ .Token }}" not in subject:
        raise ConfigError("subject_token_missing")
    if "{{ .Token }}" not in html or "{{ .ConfirmationURL }}" not in html:
        raise ConfigError("template_dual_path_missing")
    return {
        "mailer_subjects_confirmation": subject,
        "mailer_templates_confirmation_content": html,
        "mailer_subjects_magic_link": subject,
        "mailer_templates_magic_link_content": html,
        "mailer_otp_length": OTP_LENGTH,
        "mailer_otp_exp": OTP_EXPIRY_SECONDS,
    }


def _receipt(config: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any]:
    subject = str(config.get("mailer_subjects_magic_link") or "")
    html = str(config.get("mailer_templates_magic_link_content") or "")
    confirmation_subject = str(config.get("mailer_subjects_confirmation") or "")
    confirmation_html = str(config.get("mailer_templates_confirmation_content") or "")
    return {
        "subject_sha256": _sha(subject),
        "template_sha256": _sha(html),
        "subject_matches": subject == desired["mailer_subjects_magic_link"],
        "template_matches": html == desired["mailer_templates_magic_link_content"],
        "confirmation_subject_sha256": _sha(confirmation_subject),
        "confirmation_template_sha256": _sha(confirmation_html),
        "confirmation_subject_matches": (
            confirmation_subject == desired["mailer_subjects_confirmation"]
        ),
        "confirmation_template_matches": (
            confirmation_html == desired["mailer_templates_confirmation_content"]
        ),
        "subject_has_token": "{{ .Token }}" in subject,
        "template_has_token": "{{ .Token }}" in html,
        "template_has_confirmation_url": "{{ .ConfirmationURL }}" in html,
        "otp_length": config.get("mailer_otp_length"),
        "otp_expiry_seconds": config.get("mailer_otp_exp"),
        "smtp_configured": bool(config.get("smtp_host")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()

    project_ref = str(os.environ.get("PERSONALIZATION_SUPABASE_PROJECT_REF") or "").strip()
    token = str(os.environ.get("PERSONALIZATION_SUPABASE_ACCESS_TOKEN") or "").strip()
    if not project_ref:
        raise ConfigError("project_ref_missing")
    if not token:
        raise ConfigError("management_token_missing")
    desired = _desired()
    before = _request("GET", project_ref=project_ref, token=token)
    before_receipt = _receipt(before, desired)

    if args.apply:
        if args.confirm != CONFIRMATION:
            raise ConfigError("apply_confirmation_invalid")
        _request("PATCH", project_ref=project_ref, token=token, payload=desired)

    after = _request("GET", project_ref=project_ref, token=token)
    after_receipt = _receipt(after, desired)
    receipt = {
        "schema": "kenigevents.focus_auth_email_config.v1",
        "applied": bool(args.apply),
        "before": before_receipt,
        "after": after_receipt,
        "verified": all(
            (
                after_receipt["subject_matches"],
                after_receipt["template_matches"],
                after_receipt["confirmation_subject_matches"],
                after_receipt["confirmation_template_matches"],
                after_receipt["otp_length"] == OTP_LENGTH,
                after_receipt["otp_expiry_seconds"] == OTP_EXPIRY_SECONDS,
            )
        ),
    }
    text = json.dumps(receipt, ensure_ascii=False, indent=2)
    if args.receipt:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if receipt["verified"] or not args.apply else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ConfigError as exc:
        print(f"focus_auth_email_config_error:{exc}", file=sys.stderr)
        raise SystemExit(2)
