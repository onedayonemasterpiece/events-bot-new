from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "configure_focus_auth_email",
    ROOT / "scripts/configure_focus_auth_email.py",
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_focus_email_contract_has_link_code_and_six_digit_provider_minimum() -> None:
    desired = MODULE._desired()
    assert desired["mailer_otp_length"] == 6
    assert desired["mailer_otp_exp"] == 600
    assert "{{ .Token }}" in desired["mailer_subjects_confirmation"]
    assert "{{ .Token }}" in desired["mailer_templates_confirmation_content"]
    assert "{{ .Token }}" in desired["mailer_subjects_magic_link"]
    assert "{{ .Token }}" in desired["mailer_templates_magic_link_content"]
    assert "{{ .TokenHash }}" in desired["mailer_templates_magic_link_content"]
    assert "{{ .RedirectTo }}" in desired["mailer_templates_magic_link_content"]


def test_redacted_receipt_exposes_hashes_not_template_contents() -> None:
    desired = MODULE._desired()
    receipt = MODULE._receipt(desired, desired)
    assert receipt["subject_matches"] is True
    assert receipt["template_matches"] is True
    assert receipt["confirmation_subject_matches"] is True
    assert receipt["confirmation_template_matches"] is True
    assert receipt["smtp_configured"] is False
    serialized = repr(receipt)
    assert desired["mailer_templates_magic_link_content"] not in serialized
    assert desired["mailer_subjects_magic_link"] not in serialized
