"""Shared durable Gemini runtime primitives for the Region Talk funnel.

The Supabase gateway remains the cross-service quota authority.  The YDB
ledger below adds the Region Talk daily/product ceiling and deterministic
request replay across the local finalizer and the Kaggle visual adjudicator.
"""
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any


def _parse_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def completed_llm_result_is_replayable(result: dict[str, Any]) -> bool:
    """Replay only completed semantic/visual verdicts, never provider errors."""

    gate_status = str(result.get("llm_gate_status") or result.get("vlm_gate_status") or "").strip().lower()
    if gate_status == "ok":
        return True
    if gate_status in {"error", "rate_limited", "unknown"}:
        return False
    decision = str(result.get("llm_decision") or result.get("vlm_decision") or "").strip().lower()
    return decision in {"accept", "reject", "review", "needs_review"}


class DurableGeminiBudget:
    """Atomic cumulative Region Talk budget and request-idempotency ledger."""

    def __init__(self, pool: Any, ydb: Any, table: str, *, budget_id: str, budget_max: int, owner_prefix: str = "region-talk") -> None:
        self.pool = pool
        self.ydb = ydb
        self.table = table
        self.budget_id = re.sub(r"[^A-Za-z0-9_.:-]+", "-", budget_id).strip("-") or "region-talk-debug"
        self.budget_max = min(100, max(0, int(budget_max)))
        safe_owner = re.sub(r"[^A-Za-z0-9_.:-]+", "-", owner_prefix).strip("-") or "region-talk"
        self.owner = safe_owner + "-" + uuid.uuid4().hex
        self.used_total = 0
        self.replayed_total = 0
        self.blocked_total = 0
        self._request_payloads: dict[str, dict[str, Any]] = {}

    @property
    def budget_pk(self) -> str:
        return "region_talk_llm_budget_item:" + self.budget_id

    def request_pk(self, fingerprint: str) -> str:
        return f"region_talk_llm_request_item:{self.budget_id}:{fingerprint}"

    @staticmethod
    def _payload(result_sets: Any) -> dict[str, Any]:
        rows = result_sets[0].rows if result_sets else []
        if not rows:
            return {}
        value = rows[0].payload_json
        return json.loads(value) if isinstance(value, str) else dict(value or {})

    def reserve(self, fingerprint: str) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_until = (now + timedelta(minutes=10)).isoformat()
        request_pk = self.request_pk(fingerprint)

        def op(session: Any) -> dict[str, Any]:
            select = session.prepare(
                f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{self.table}` WHERE pk = $pk;"
            )
            upsert = session.prepare(
                f"DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8; "
                f"UPSERT INTO `{self.table}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);"
            )
            tx = session.transaction(self.ydb.SerializableReadWrite())
            budget = self._payload(tx.execute(select, {"$pk": self.budget_pk}, commit_tx=False))
            request = self._payload(tx.execute(select, {"$pk": request_pk}, commit_tx=False))
            if (
                str(request.get("status") or "") == "completed"
                and isinstance(request.get("result"), dict)
                and completed_llm_result_is_replayable(request["result"])
            ):
                tx.commit()
                return {"status": "replay", "result": request["result"], "request": request, "budget": budget}
            lease = _parse_time(request.get("lease_until"))
            if request and lease and lease > now and str(request.get("lease_owner") or "") != self.owner:
                tx.commit()
                return {"status": "busy", "request": request, "budget": budget}
            used = int(budget.get("reserved_total") or 0)
            is_new = not bool(request)
            if is_new and used >= self.budget_max:
                tx.commit()
                return {"status": "exhausted", "budget": budget}
            if is_new:
                used += 1
            budget = {
                **budget,
                "budget_id": self.budget_id,
                "budget_max": self.budget_max,
                "reserved_total": used,
                "remaining": max(0, self.budget_max - used),
                "updated_at": now_iso,
            }
            request = {
                **request,
                "budget_id": self.budget_id,
                "request_fingerprint": fingerprint,
                "status": "reserved",
                "lease_owner": self.owner,
                "lease_until": lease_until,
                "reserved_at": request.get("reserved_at") or now_iso,
                "updated_at": now_iso,
            }
            tx.execute(upsert, {
                "$pk": self.budget_pk,
                "$kind": "region_talk_llm_budget_item",
                "$payload_json": json.dumps(budget, ensure_ascii=False),
                "$updated_at": now_iso,
            }, commit_tx=False)
            tx.execute(upsert, {
                "$pk": request_pk,
                "$kind": "region_talk_llm_request_item",
                "$payload_json": json.dumps(request, ensure_ascii=False),
                "$updated_at": now_iso,
            }, commit_tx=False)
            tx.commit()
            return {"status": "reserved", "request": request, "budget": budget}

        result = self.pool.retry_operation_sync(op)
        self.used_total = int((result.get("budget") or {}).get("reserved_total") or self.used_total)
        if result.get("status") == "replay":
            self.replayed_total += 1
        elif result.get("status") in {"busy", "exhausted"}:
            self.blocked_total += 1
        if isinstance(result.get("request"), dict):
            self._request_payloads[fingerprint] = dict(result["request"])
        return result

    def complete(self, fingerprint: str, result: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        request = {
            **self._request_payloads.get(fingerprint, {}),
            "budget_id": self.budget_id,
            "request_fingerprint": fingerprint,
            "status": "completed",
            "result": result,
            "completed_at": now,
            "lease_until": "",
            "updated_at": now,
        }
        pk = self.request_pk(fingerprint)

        def op(session: Any) -> None:
            query = session.prepare(
                f"DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8; "
                f"UPSERT INTO `{self.table}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);"
            )
            session.transaction(self.ydb.SerializableReadWrite()).execute(query, {
                "$pk": pk,
                "$kind": "region_talk_llm_request_item",
                "$payload_json": json.dumps(request, ensure_ascii=False),
                "$updated_at": now,
            }, commit_tx=True)

        self.pool.retry_operation_sync(op)
        self._request_payloads[fingerprint] = request


class _SupabaseRestResult:
    def __init__(self, data: Any):
        self.data = data


class _SupabaseRestQuery:
    def __init__(self, client: "SupabaseRestClient", table: str):
        self.client = client
        self.table = table
        self.params: dict[str, str] = {}
        self.order_parts: list[str] = []

    def select(self, columns: str = "*") -> "_SupabaseRestQuery":
        self.params["select"] = columns
        return self

    def eq(self, column: str, value: Any) -> "_SupabaseRestQuery":
        self.params[column] = "eq." + (str(value).lower() if isinstance(value, bool) else str(value))
        return self

    def in_(self, column: str, values: list[Any]) -> "_SupabaseRestQuery":
        self.params[column] = "in.(" + ",".join(str(value) for value in values) + ")"
        return self

    def order(self, column: str) -> "_SupabaseRestQuery":
        self.order_parts.append(column)
        return self

    def limit(self, value: int) -> "_SupabaseRestQuery":
        self.params["limit"] = str(int(value))
        return self

    def execute(self) -> _SupabaseRestResult:
        params = dict(self.params)
        if self.order_parts:
            params["order"] = ",".join(self.order_parts)
        return _SupabaseRestResult(self.client._request("GET", f"/rest/v1/{self.table}", params=params))


class _SupabaseRestRpc:
    def __init__(self, client: "SupabaseRestClient", function: str, payload: dict[str, Any]):
        self.client = client
        self.function = function
        self.payload = payload

    def execute(self) -> _SupabaseRestResult:
        return _SupabaseRestResult(
            self.client._request("POST", f"/rest/v1/rpc/{self.function}", json_body=self.payload)
        )


class SupabaseRestClient:
    """Small PostgREST surface required by :class:`google_ai.GoogleAIClient`."""

    def __init__(self, url: str, key: str, *, schema: str = "public") -> None:
        self.url = url.rstrip("/")
        self.key = key
        self.schema = schema or "public"

    def table(self, table: str) -> _SupabaseRestQuery:
        return _SupabaseRestQuery(self, table)

    def rpc(self, function: str, payload: dict[str, Any]) -> _SupabaseRestRpc:
        return _SupabaseRestRpc(self, function, payload)

    def _request(self, method: str, path: str, *, params: dict[str, str] | None = None, json_body: Any = None) -> Any:
        import requests

        headers = {
            "apikey": self.key,
            "Authorization": "Bearer " + self.key,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Profile": self.schema,
            "Content-Profile": self.schema,
        }
        response = requests.request(
            method,
            self.url + path,
            headers=headers,
            params=params,
            json=json_body,
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"supabase_rest_{response.status_code}: {response.text[:500]}")
        return response.json() if response.text.strip() else None


def build_supabase_rest_client() -> SupabaseRestClient:
    from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client

    def legacy_factory() -> SupabaseRestClient:
        url = str(os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
        key = str(os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
        if not url or not key:
            raise RuntimeError("missing_SUPABASE_URL_or_service_key")
        return SupabaseRestClient(
            url,
            key,
            schema=str(os.getenv("SUPABASE_SCHEMA") or "public").strip() or "public",
        )

    return build_google_ai_limiter_supabase_client(
        fallback_factory=legacy_factory,
        require_configured=True,
        client_factory=lambda url, key: SupabaseRestClient(url, key),
    )


def build_google_ai_client(*, default_env_var_name: str, consumer: str) -> Any:
    from google_ai import GoogleAIClient, SecretsProvider

    client = GoogleAIClient(
        supabase_client=build_supabase_rest_client(),
        secrets_provider=SecretsProvider(),
        consumer=consumer,
        account_name=os.getenv("GOOGLE_API_LOCALNAME_REGION_TALK") or os.getenv("GOOGLE_API_LOCALNAME"),
        default_env_var_name=default_env_var_name,
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client.allow_local_limiter_on_reserve_error = False
    return client
