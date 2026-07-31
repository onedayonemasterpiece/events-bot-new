from __future__ import annotations

import json
from pathlib import Path

from scripts.inspect import audit_google_ai_provider_paths as audit


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_approved_gateway_is_inventoried_separately(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "google_ai/client.py",
        "from google import genai\nclient = genai.Client(api_key='not-printed')\n",
    )

    report = audit.audit_repository(tmp_path)

    assert report.passed
    assert report.summary["approved_gateway"] == 2
    assert {finding.disposition for finding in report.findings} == {"approved_gateway"}


def test_new_direct_sdk_path_fails_without_exposing_key_value(tmp_path: Path) -> None:
    secret = "AIza-VERY-SECRET-TEST-VALUE"
    _write(
        tmp_path,
        "service/direct_google.py",
        "from google import genai\n"
        f"client = genai.Client(api_key='{secret}')\n"
        "response = client.models.generate_content(model='gemini', contents='hello')\n",
    )

    report = audit.audit_repository(tmp_path)
    rendered = audit.render_text(report)
    encoded = json.dumps(report.public_dict(), sort_keys=True)

    assert not report.passed
    assert report.summary["unapproved"] == 3
    assert secret not in rendered
    assert secret not in encoded


def test_debt_rule_is_line_shaped_and_count_bounded(tmp_path: Path) -> None:
    allowed = (
        'endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/'
        '{model}:embedContent"'
    )
    _write(tmp_path, "event_identity.py", allowed + "\n")
    assert audit.audit_repository(tmp_path).passed

    _write(tmp_path, "event_identity.py", allowed + "\n" + allowed + "\n")
    report = audit.audit_repository(tmp_path)
    assert not report.passed
    assert report.summary["allowlisted_debt"] == 1
    assert report.summary["unapproved"] == 1

    _write(
        tmp_path,
        "event_identity.py",
        'url = "https://generativelanguage.googleapis.com/v1beta/models/x:embedContent"\n',
    )
    report = audit.audit_repository(tmp_path)
    assert not report.passed
    assert report.summary["allowlisted_debt"] == 0
    assert report.summary["unapproved"] == 1


def test_only_serialized_gateway_assignment_is_approved_in_notebook(tmp_path: Path) -> None:
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "metadata": {},
                "outputs": [],
                "execution_count": None,
                "source": [
                    '_TG_EMBEDDED_GOOGLE_AI = {"client.py": "from google import genai"}\n'
                ],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "outputs": [],
                "execution_count": None,
                "source": ["from google import genai\n"],
            },
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    _write(
        tmp_path,
        "kaggle/TelegramMonitor/telegram_monitor.ipynb",
        json.dumps(notebook),
    )

    report = audit.audit_repository(tmp_path)

    assert not report.passed
    assert report.summary["approved_embedded_gateway"] == 1
    assert report.summary["unapproved"] == 1


def test_invalid_notebook_fails_closed(tmp_path: Path) -> None:
    _write(tmp_path, "kaggle/broken.ipynb", "{not-json")

    report = audit.audit_repository(tmp_path)

    assert not report.passed
    assert report.unreadable_files == ("kaggle/broken.ipynb",)
