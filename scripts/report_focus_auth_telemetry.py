#!/usr/bin/env python3
"""Write a PII-free focus Auth health bundle for operator/ChatGPT review."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROMPT = """Проанализируй приложенный отчёт focus-auth-telemetry. Открой summary.json и дай:\n1. сколько было попыток и успешных входов через email и Яндекс;\n2. долю прямого и резервного маршрута отдельно для запроса и проверки OTP;\n3. ошибки, неоднозначные ответы и отсутствующую телеметрию;\n4. использование Postbox и NotiSend;\n5. фактическую занятость лимита 200 уникальных получателей NotiSend в текущем расчётном периоде: provider_reported + admitted_after_reconcile, остаток и routing_ready;\n6. итог PASS/WARN/FAIL и три следующих действия.\nЕсли routing_ready=false, поставь FAIL для маршрутизации новых адресов через NotiSend и потребуй сверку фактического счётчика в кабинете провайдера.\nОтчёт агрегированный: не пытайся восстанавливать адреса или иные персональные данные.\n"""


class ReportError(RuntimeError):
    pass


def _request(url: str, secret: str, since: str) -> dict:
    request = urllib.request.Request(
        f"{url.rstrip('/')}/rest/v1/rpc/focus_auth_operator_summary_v1",
        data=json.dumps({"p_since": since}, separators=(",", ":")).encode(),
        method="POST",
        headers={
            "apikey": secret,
            "Authorization": f"Bearer {secret}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "kenigevents-focus-auth-report/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        raise ReportError(f"summary_http_{exc.code}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ReportError("summary_unavailable") from exc
    if not isinstance(payload, dict) or payload.get("schema") != "kenigevents.focus_auth_operator_summary.v1":
        raise ReportError("summary_contract_invalid")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.hours <= 24 * 90:
        raise ReportError("hours_out_of_range")

    url = str(os.environ.get("PERSONALIZATION_SUPABASE_URL") or "").strip()
    secret = str(os.environ.get("PERSONALIZATION_SUPABASE_SECRET_KEY") or "").strip()
    if not url or not secret:
        raise ReportError("personalization_service_config_missing")
    since = (datetime.now(timezone.utc) - timedelta(hours=args.hours)).isoformat()
    payload = _request(url, secret, since)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.output or Path("artifacts/codex") / f"focus-auth-telemetry-{stamp}"
    output.mkdir(parents=True, exist_ok=False)
    (output / "summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "CHATGPT_PROMPT.txt").write_text(PROMPT, encoding="utf-8")
    (output / "README.md").write_text(
        "# Focus Auth telemetry\n\nOpen `summary.json`; then use `CHATGPT_PROMPT.txt`. "
        "The bundle contains aggregates only: no email, OTP, token, IP, user-agent or provider message id.\n",
        encoding="utf-8",
    )
    print(output)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ReportError as exc:
        print(f"focus_auth_report_error:{exc}", file=sys.stderr)
        raise SystemExit(2)
