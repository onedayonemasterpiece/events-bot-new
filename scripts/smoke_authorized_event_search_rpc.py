#!/usr/bin/env python3
"""Live smoke for authorized Supabase pgvector event search.

This intentionally exercises the same protected pieces the Edge Function uses,
without requiring a deployed Edge Function or Yandex OAuth credentials:

1. create a temporary Supabase Auth user with the backend secret key;
2. sign in with the publishable key to get a real authenticated JWT;
3. reserve search quota as that user;
4. build a Gemini Embedding 2 query vector;
5. call search_events_by_embedding_v1 through PostgREST with the user JWT;
6. optionally record compact audit metadata;
7. delete the temporary user and clean smoke quota/audit rows when a direct
   Postgres connection is available.

No secrets or raw token values are printed.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import secrets
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from google_ai import GoogleAIClient, SecretsProvider

DEFAULT_QUERY = "урбанистика будущее города"
DEFAULT_EXPECTED_EVENT_ID = 6310
EMBEDDING_DIM = 768


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def request_json(url: str, *, method: str = "POST", headers: dict[str, str] | None = None, body: Any | None = None, timeout: int = 60) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read()
            if not raw:
                return response.status, None
            return response.status, json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read(800).decode("utf-8", "replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env: {name}")
    return value


def create_temp_user(base_url: str, secret_key: str) -> tuple[str, str, str]:
    email = f"codex-search-smoke-{int(time.time())}-{secrets.token_hex(4)}@example.invalid"
    password = "Smoke-" + secrets.token_urlsafe(18) + "1a!"
    _, payload = request_json(
        f"{base_url}/auth/v1/admin/users",
        headers={"apikey": secret_key, "Authorization": f"Bearer {secret_key}", "Content-Type": "application/json"},
        body={"email": email, "password": password, "email_confirm": True, "user_metadata": {"purpose": "codex_event_search_smoke"}},
    )
    user_id = str(payload.get("id") or "")
    if not user_id:
        raise RuntimeError("Supabase admin create user returned no id")
    return user_id, email, password


def delete_temp_user(base_url: str, secret_key: str, user_id: str) -> None:
    request_json(
        f"{base_url}/auth/v1/admin/users/{user_id}",
        method="DELETE",
        headers={"apikey": secret_key, "Authorization": f"Bearer {secret_key}"},
        body=None,
        timeout=30,
    )


def cleanup_smoke_db_rows(base_url: str, secret_key: str, user_id: str) -> None:
    # Service-role REST cleanup keeps this smoke independent from local psycopg
    # availability and avoids leaving orphan quota rows after deleting the temp user.
    headers = {
        "apikey": secret_key,
        "Authorization": f"Bearer {secret_key}",
        "Prefer": "return=minimal",
    }
    for table in ("event_search_requests", "user_search_quota_ledger"):
        url = f"{base_url}/rest/v1/{table}?user_id=eq.{user_id}"
        req = urllib.request.Request(url, headers=headers, method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                print(f"cleanup_{table}=ok status={response.status}")
        except urllib.error.HTTPError as exc:
            detail = exc.read(300).decode("utf-8", "replace")
            print(f"cleanup_{table}=failed status={exc.code} detail={detail[:120]}")


def sign_in(base_url: str, publishable_key: str, email: str, password: str) -> str:
    _, payload = request_json(
        f"{base_url}/auth/v1/token?grant_type=password",
        headers={"apikey": publishable_key, "Content-Type": "application/json"},
        body={"email": email, "password": password},
    )
    token = str(payload.get("access_token") or "")
    if not token:
        raise RuntimeError("Supabase password sign-in returned no access_token")
    return token


def build_google_ai_client(key_env: str) -> GoogleAIClient:
    """Build the legacy-project shared limiter for a personalization smoke."""

    limiter_url = require_env("SUPABASE_URL")
    limiter_service_key = require_env("SUPABASE_SERVICE_KEY")
    try:
        from supabase import create_client
    except ImportError as exc:
        raise RuntimeError("supabase package is required for shared Google AI limiting") from exc
    client = GoogleAIClient(
        supabase_client=create_client(limiter_url, limiter_service_key),
        secrets_provider=SecretsProvider(),
        consumer="authorized_event_search_rpc_smoke",
        account_name=(os.getenv("GOOGLE_API_LOCALNAME") or "authorized-event-search-smoke").strip(),
        default_env_var_name=key_env,
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client.allow_local_limiter_on_reserve_error = False
    client.provider_timeout_seconds = 60.0
    return client


async def embed_query(
    query: str,
    model: str,
    *,
    google_ai_client: GoogleAIClient,
) -> list[float]:
    text = f"task: search result | query: {query}"
    values, _usage = await google_ai_client.embed_content_async(
        model=model,
        text=text,
        output_dimensionality=EMBEDDING_DIM,
    )
    if len(values) != EMBEDDING_DIM:
        raise RuntimeError(f"Bad embedding dimension: {len(values)}")
    return [float(value) for value in values]


def rpc(base_url: str, publishable_key: str, token: str, name: str, payload: dict[str, Any]) -> Any:
    _, data = request_json(
        f"{base_url}/rest/v1/rpc/{name}",
        headers={"apikey": publishable_key, "Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        body=payload,
        timeout=60,
    )
    return data


async def _main(args: argparse.Namespace) -> int:
    key_env = str(args.google_key_env or "").strip()
    if not re.fullmatch(r"GOOGLE_API_KEY(?:_?[2-9][0-9]*)?", key_env):
        raise RuntimeError("--google-key-env must name a registered GOOGLE_API_KEY lane")

    load_env(Path(args.env_file))
    base_url = require_env("PERSONALIZATION_SUPABASE_URL").rstrip("/")
    secret_key = require_env("PERSONALIZATION_SUPABASE_SECRET_KEY")
    publishable_key = require_env("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY")
    google_ai_client = build_google_ai_client(key_env)

    user_id = ""
    try:
        user_id, email, password = create_temp_user(base_url, secret_key)
        token = sign_in(base_url, publishable_key, email, password)
        print(f"auth=ok user_id_hash={hashlib.sha256(user_id.encode()).hexdigest()[:12]}")

        quota = None
        if not args.skip_quota:
            quota = rpc(base_url, publishable_key, token, "reserve_event_search_quota_v1", {"p_plan_id": "registered", "p_use_llm": False})
            print(f"quota=ok remaining={(quota[0] if isinstance(quota, list) and quota else quota).get('day_remaining') if quota else 'n/a'}")

        vector = await embed_query(
            args.query,
            args.embedding_model.replace("models/", ""),
            google_ai_client=google_ai_client,
        )
        print(f"embedding=ok dim={len(vector)} model={args.embedding_model}")

        rows = rpc(
            base_url,
            publishable_key,
            token,
            "search_events_by_embedding_v1",
            {
                "p_query_embedding": vector,
                "p_match_count": args.limit,
                "p_offset_count": 0,
                "p_date_from": args.date_from,
                "p_date_to": None,
                "p_city_filter": None,
                "p_category_filter": None,
                "p_embedding_model": args.embedding_model.replace("models/", ""),
                "p_embedding_dim": EMBEDDING_DIM,
                "p_weekday_iso": args.weekday_iso,
                "p_time_of_day_filter": args.time_of_day,
                "p_admission_filter": args.admission,
            },
        )
        if not isinstance(rows, list):
            raise RuntimeError("Search RPC returned non-list payload")
        top = [(int(row.get("event_id")), round(float(row.get("similarity") or 0), 4), str(row.get("title") or "")) for row in rows[: args.expected_top_n]]
        print("search=ok top=" + json.dumps(top, ensure_ascii=False))
        top_ids = [item[0] for item in top]
        if args.expected_event_id not in top_ids:
            raise RuntimeError(f"Expected event {args.expected_event_id} in top {args.expected_top_n}, got {top_ids}")

        if not args.skip_audit:
            query_hash = hashlib.sha256(args.query.lower().encode("utf-8")).hexdigest()
            audit = rpc(
                base_url,
                publishable_key,
                token,
                "record_event_search_request_v1",
                {
                    "p_request_kind": "vector_search",
                    "p_query_hash": query_hash,
                    "p_query_length": len(args.query),
                    "p_result_count": len(rows),
                    "p_llm_used": False,
                    "p_status": "ok",
                    "p_error_code": None,
                    "p_metadata": {"smoke": "authorized_event_search_rpc", "expected_event_id": args.expected_event_id, "top_ids": top_ids},
                },
            )
            print(f"audit=ok payload_type={type(audit).__name__}")

        return 0
    finally:
        if user_id and not args.keep_user:
            try:
                cleanup_smoke_db_rows(base_url, secret_key, user_id)
            finally:
                delete_temp_user(base_url, secret_key, user_id)
                print("temp_user=deleted")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--expected-event-id", type=int, default=DEFAULT_EXPECTED_EVENT_ID)
    parser.add_argument("--expected-top-n", type=int, default=3)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--date-from", default="2026-06-29")
    parser.add_argument("--weekday-iso", type=int, default=None, help="Optional query weekday facet, ISO 1..7")
    parser.add_argument("--time-of-day", choices=["morning", "day", "evening", "night"], default=None)
    parser.add_argument("--admission", choices=["free", "registration_required", "paid"], default=None)
    parser.add_argument("--embedding-model", default=os.getenv("EVENT_SEARCH_EMBEDDING_MODEL", "gemini-embedding-2"))
    parser.add_argument("--google-key-env", default="GOOGLE_API_KEY4")
    parser.add_argument("--keep-user", action="store_true")
    parser.add_argument("--skip-quota", action="store_true")
    parser.add_argument("--skip-audit", action="store_true")
    args = parser.parse_args()
    return asyncio.run(_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
