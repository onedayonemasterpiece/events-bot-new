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


DEFAULT_EVENT_SEARCH_EMBEDDING_KEY_ENVS = (
    "GOOGLE_API_KEY5",
    "GOOGLE_API_KEY4",
    "GOOGLE_API_KEY3",
    "GOOGLE_API_KEY2",
    "GOOGLE_API_KEY",
)

DEFAULT_EVENT_SEARCH_LLM_KEY_ENVS = (
    "GOOGLE_API_KEY5",
    "GOOGLE_API_KEY4",
    "GOOGLE_API_KEY3",
    "GOOGLE_API_KEY",
)


def event_search_google_key_envs(kind: str) -> list[str]:
    raw = (
        os.getenv(f"EVENT_SEARCH_{kind}_KEY_ENVS")
        or os.getenv("EVENT_SEARCH_GOOGLE_KEY_ENVS")
        or ""
    ).strip()
    names = (
        [item.strip() for item in raw.split(",") if item.strip()]
        if raw
        else list(DEFAULT_EVENT_SEARCH_EMBEDDING_KEY_ENVS if kind == "EMBEDDING" else DEFAULT_EVENT_SEARCH_LLM_KEY_ENVS)
    )
    out: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def present_event_search_google_key_envs(kind: str) -> list[str]:
    return [name for name in event_search_google_key_envs(kind) if present(name)]


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
    embedding_key_envs = present_event_search_google_key_envs("EMBEDDING")
    llm_key_envs = present_event_search_google_key_envs("LLM")
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
            and bool(embedding_key_envs)
            and bool(llm_key_envs),
            f"Edge Function runtime env can call Supabase Auth/RPC and Google providers (embedding lanes={embedding_key_envs}, llm lanes={llm_key_envs})"
            if present("PERSONALIZATION_SUPABASE_URL")
            and present("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY")
            and bool(embedding_key_envs)
            and bool(llm_key_envs)
            else "Need personalization Supabase URL/publishable key and configured Google key envs for event-search embedding/LLM rotation",
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


def http_response(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, timeout: int = 20, read_limit: int = 300) -> tuple[int, str, dict[str, str]]:
    opener = urllib.request.build_opener(NoRedirect)
    request_headers = {"User-Agent": "codex-authorized-search-readiness"}
    request_headers.update(headers or {})
    req = urllib.request.Request(url, method=method, headers=request_headers)
    try:
        with opener.open(req, timeout=timeout) as response:
            return int(response.status), response.read(read_limit).decode("utf-8", "replace"), dict(response.headers.items())
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read(read_limit).decode("utf-8", "replace"), dict(exc.headers.items())


def http_status(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, timeout: int = 20) -> tuple[int, str]:
    status, body, _headers = http_response(url, method=method, headers=headers, timeout=timeout)
    return status, body


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


def probe_auth_config() -> Check:
    ref = first_env("PERSONALIZATION_SUPABASE_PROJECT_REF")
    token = first_env("PERSONALIZATION_SUPABASE_ACCESS_TOKEN", "SUPABASE_ACCESS_TOKEN")
    if not ref or not token:
        return Check("auth_redirect_config_probe", False, "Skipped: missing Supabase project ref/access token")
    status, body, _headers = http_response(
        f"https://api.supabase.com/v1/projects/{urllib.parse.quote(ref)}/config/auth",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=30,
        read_limit=200000,
    )
    if not (200 <= status < 300):
        return Check("auth_redirect_config_probe", False, f"Auth config fetch failed, status={status}, body={body[:120]!r}")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return Check("auth_redirect_config_probe", False, "Auth config fetch returned non-JSON payload")
    site_url = str(data.get("site_url") or "")
    allow_list = str(data.get("uri_allow_list") or "")
    required = "https://kenigevents.ru/**"
    ok = site_url == "https://kenigevents.ru" and required in [item.strip() for item in allow_list.split(",") if item.strip()]
    if ok:
        return Check("auth_redirect_config_probe", True, "Auth Site URL and redirect allow-list point to kenigevents.ru")
    return Check(
        "auth_redirect_config_probe",
        False,
        f"Need site_url=https://kenigevents.ru and uri_allow_list containing {required}; current site_url={site_url!r}",
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
    status, body, headers = http_response(
        f"{base_url}/auth/v1/authorize?{params}",
        headers={"apikey": key},
    )
    location = headers.get("Location") or headers.get("location") or ""
    location_host = urllib.parse.urlparse(location).netloc
    ok = status in {302, 303, 307, 308} and "oauth.yandex" in location_host and "localhost" not in location
    return Check(
        "yandex_provider_probe",
        ok,
        f"Auth authorize redirects to Yandex, status={status}, no localhost fallback"
        if ok
        else f"Provider not ready or redirect is wrong, status={status}, location_host={location_host!r}, body={body[:120]!r}",
    )



def probe_yandex_userinfo_adapter() -> Check:
    base_url = first_env("PERSONALIZATION_SUPABASE_URL").rstrip("/")
    admin_key = first_env("PERSONALIZATION_SUPABASE_SECRET_KEY", "PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY")
    if not base_url or not admin_key:
        return Check("yandex_userinfo_adapter_probe", False, "Skipped: missing personalization Supabase URL/service key")
    expected_url = f"{base_url}/functions/v1/yandex-userinfo"
    status, body, _headers = http_response(
        f"{base_url}/auth/v1/admin/custom-providers/custom:yandex",
        headers={"apikey": admin_key, "Authorization": f"Bearer {admin_key}", "Accept": "application/json"},
        timeout=30,
        read_limit=200000,
    )
    if not (200 <= status < 300):
        return Check("yandex_userinfo_adapter_probe", False, f"Custom provider fetch failed, status={status}, body={body[:120]!r}")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return Check("yandex_userinfo_adapter_probe", False, "Custom provider fetch returned non-JSON payload")
    scopes = data.get("scopes") or []
    userinfo_url = str(data.get("userinfo_url") or "")
    email_required = data.get("email_optional") is False
    ok = (
        data.get("identifier") == "custom:yandex"
        and data.get("enabled") is True
        and userinfo_url == expected_url
        and email_required
        and "login:email" in scopes
        and "login:info" in scopes
    )
    if ok:
        # Runtime smoke: the adapter is public for Supabase Auth server-to-server calls,
        # but it must not succeed without an OAuth token.
        smoke_status, smoke_body = http_status(expected_url, headers={"Accept": "application/json"}, timeout=20)
        if smoke_status == 401 and "missing_yandex_token" in smoke_body:
            return Check("yandex_userinfo_adapter_probe", True, "custom:yandex uses the Yandex userinfo adapter and the adapter rejects missing tokens")
        return Check("yandex_userinfo_adapter_probe", False, f"Adapter smoke failed, status={smoke_status}, body={smoke_body[:120]!r}")
    return Check(
        "yandex_userinfo_adapter_probe",
        False,
        f"Need custom:yandex userinfo_url={expected_url} and email_optional=false with login:email/login:info scopes; current userinfo_url={userinfo_url!r}, email_optional={data.get('email_optional')!r}",
    )

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--probe-edge", action="store_true")
    parser.add_argument("--probe-auth-config", action="store_true")
    parser.add_argument("--probe-yandex-provider", action="store_true")
    parser.add_argument("--probe-yandex-userinfo-adapter", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any check fails.")
    args = parser.parse_args()

    load_env(Path(args.env_file))
    checks = check_env()
    if args.probe_edge:
        checks.append(probe_edge())
    if args.probe_auth_config:
        checks.append(probe_auth_config())
    if args.probe_yandex_provider:
        checks.append(probe_yandex_provider())
    if args.probe_yandex_userinfo_adapter:
        checks.append(probe_yandex_userinfo_adapter())

    if args.json:
        print(json.dumps({"ok": all(check.ok for check in checks), "checks": [check.to_json() for check in checks]}, ensure_ascii=False, indent=2))
    else:
        for check in checks:
            print(f"{'OK' if check.ok else 'MISSING'} {check.name}: {check.detail}")
    return 1 if args.strict and not all(check.ok for check in checks) else 0


if __name__ == "__main__":
    raise SystemExit(main())
