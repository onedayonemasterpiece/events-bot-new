#!/usr/bin/env python3
"""Redacted readiness check for the Yandex-auth authorized event search gate.

The script is intentionally safe to run from a dirty operator shell: it loads
`.env`, never prints secret values and only performs live probes when explicitly
requested.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def first_env(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


@dataclass
class Check:
    name: str
    ok: bool
    detail: str

    def to_json(self) -> dict[str, Any]:
        return {"name": self.name, "ok": self.ok, "detail": self.detail}


def present(name: str) -> bool:
    return bool((os.getenv(name) or "").strip())


def check_env() -> list[Check]:
    public_url = first_env(
        "STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_URL",
        "PUBLIC_PERSONALIZATION_SUPABASE_URL",
        "PERSONALIZATION_SUPABASE_URL",
    )
    public_key = first_env(
        "STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY",
        "PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY",
        "PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY",
    )
    return [
        Check(
            "static_build_public_auth_env",
            bool(public_url and public_key),
            "Kaggle/Astro can render AuthorizedEventSearch"
            if public_url and public_key
            else "Need PUBLIC/STATIC_SITE_PUBLIC Supabase URL + publishable key",
        ),
        Check(
            "yandex_oauth_credentials",
            present("YANDEX_CLIENT_ID") and present("YANDEX_CLIENT_SECRET"),
            "Yandex OAuth app credentials are present"
            if present("YANDEX_CLIENT_ID") and present("YANDEX_CLIENT_SECRET")
            else "Need YANDEX_CLIENT_ID and YANDEX_CLIENT_SECRET to create custom:yandex provider",
        ),
        Check(
            "supabase_deploy_credentials",
            bool(first_env("PERSONALIZATION_SUPABASE_ACCESS_TOKEN", "SUPABASE_ACCESS_TOKEN")) and present("PERSONALIZATION_SUPABASE_PROJECT_REF"),
            "Supabase CLI/API deploy credentials are present"
            if bool(first_env("PERSONALIZATION_SUPABASE_ACCESS_TOKEN", "SUPABASE_ACCESS_TOKEN")) and present("PERSONALIZATION_SUPABASE_PROJECT_REF")
            else "Need PERSONALIZATION_SUPABASE_ACCESS_TOKEN (or SUPABASE_ACCESS_TOKEN) and PERSONALIZATION_SUPABASE_PROJECT_REF to deploy/configure Edge Function",
        ),
        Check(
            "edge_function_runtime_env",
            present("PERSONALIZATION_SUPABASE_URL")
            and present("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY")
            and bool(first_env("GOOGLE_API_KEY4", "GOOGLE_API_KEY", "GEMINI_API_KEY")),
            "Edge Function runtime env can call Supabase Auth/RPC and Gemini embeddings"
            if present("PERSONALIZATION_SUPABASE_URL")
            and present("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY")
            and bool(first_env("GOOGLE_API_KEY4", "GOOGLE_API_KEY", "GEMINI_API_KEY"))
            else "Need personalization Supabase URL/publishable key and a Google embedding key",
        ),
        Check(
            "vector_sync_backend_env",
            present("PERSONALIZATION_SUPABASE_URL")
            and bool(first_env("PERSONALIZATION_SUPABASE_SECRET_KEY", "PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY"))
            and bool(first_env("GOOGLE_API_KEY4", "GOOGLE_API_KEY", "GEMINI_API_KEY")),
            "Backend/Kaggle vector sync env is present"
            if present("PERSONALIZATION_SUPABASE_URL")
            and bool(first_env("PERSONALIZATION_SUPABASE_SECRET_KEY", "PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY"))
            and bool(first_env("GOOGLE_API_KEY4", "GOOGLE_API_KEY", "GEMINI_API_KEY"))
            else "Need personalization Supabase backend key and Google embedding key",
        ),
    ]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


def http_status(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, timeout: int = 20) -> tuple[int, str]:
    opener = urllib.request.build_opener(NoRedirect)
    req = urllib.request.Request(url, method=method, headers=headers or {})
    try:
        with opener.open(req, timeout=timeout) as response:
            return int(response.status), response.read(300).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read(300).decode("utf-8", "replace")


def probe_edge() -> Check:
    base_url = first_env("PERSONALIZATION_SUPABASE_URL").rstrip("/")
    key = first_env("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY")
    if not base_url or not key:
        return Check("edge_function_probe", False, "Skipped: missing personalization Supabase URL/publishable key")
    status, body = http_status(
        f"{base_url}/functions/v1/event-search",
        method="OPTIONS",
        headers={"apikey": key, "Authorization": f"Bearer {key}"},
    )
    ok = 200 <= status < 300
    return Check(
        "edge_function_probe",
        ok,
        f"OPTIONS /functions/v1/event-search status={status}" if ok else f"Edge Function not ready, status={status}, body={body[:120]!r}",
    )


def probe_yandex_provider() -> Check:
    base_url = first_env("PERSONALIZATION_SUPABASE_URL").rstrip("/")
    key = first_env("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY")
    if not base_url or not key:
        return Check("yandex_provider_probe", False, "Skipped: missing personalization Supabase URL/publishable key")
    params = urllib.parse.urlencode(
        {
            "provider": first_env("PUBLIC_YANDEX_AUTH_PROVIDER", "STATIC_SITE_PUBLIC_YANDEX_AUTH_PROVIDER") or "custom:yandex",
            "redirect_to": "https://kenigevents.ru/__auth-smoke",
        }
    )
    status, body = http_status(
        f"{base_url}/auth/v1/authorize?{params}",
        headers={"apikey": key},
    )
    ok = status in {302, 303, 307, 308}
    return Check(
        "yandex_provider_probe",
        ok,
        f"Auth authorize redirects to provider, status={status}" if ok else f"Provider not ready, status={status}, body={body[:120]!r}",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--probe-edge", action="store_true")
    parser.add_argument("--probe-yandex-provider", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any check fails.")
    args = parser.parse_args()

    load_env(Path(args.env_file))
    checks = check_env()
    if args.probe_edge:
        checks.append(probe_edge())
    if args.probe_yandex_provider:
        checks.append(probe_yandex_provider())

    if args.json:
        print(json.dumps({"ok": all(check.ok for check in checks), "checks": [check.to_json() for check in checks]}, ensure_ascii=False, indent=2))
    else:
        for check in checks:
            print(f"{'OK' if check.ok else 'MISSING'} {check.name}: {check.detail}")
    return 1 if args.strict and not all(check.ok for check in checks) else 0


if __name__ == "__main__":
    raise SystemExit(main())
