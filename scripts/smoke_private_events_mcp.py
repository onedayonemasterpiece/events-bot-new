#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import html
import json
import re
import secrets
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import aiohttp


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:12]


async def run(credentials_path: Path) -> dict:
    payload = json.loads(credentials_path.read_text(encoding="utf-8"))
    app = payload["chatgpt"]
    endpoint = app["mcp_url"]
    client_id = app["oauth_client_id"]
    client_secret = app["oauth_client_secret"]
    operator_token = app["bootstrap_operator_token"]
    resource = endpoint
    base_prefix = endpoint.rsplit("/mcp", 1)[0]
    authorize = base_prefix + "/oauth/authorize"
    token_endpoint = base_prefix + "/oauth/token"
    endpoint_parts = urlsplit(endpoint)
    metadata = (
        f"{endpoint_parts.scheme}://{endpoint_parts.netloc}"
        f"/.well-known/oauth-protected-resource{endpoint_parts.path}"
    )
    callback = "https://chatgpt.com/connector/oauth/private-events-smoke"
    verifier = b64url(secrets.token_bytes(48))
    challenge = b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    state = secrets.token_urlsafe(24)

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(metadata) as response:
            resource_metadata = await response.json()
            assert response.status == 200 and resource_metadata["resource"] == resource

        query = urlencode({
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": callback,
            "state": state,
            "resource": resource,
            "scope": "events:read incidents:read operations:read offline_access",
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        })
        async with session.get(authorize + "?" + query) as response:
            page = await response.text()
            assert response.status == 200
        match = re.search(r'name="authorization_request" value="([^"]+)"', page)
        assert match, "authorization_request field missing"
        sealed = html.unescape(match.group(1))

        async with session.post(
            authorize,
            data={"authorization_request": sealed, "operator_token": operator_token},
            allow_redirects=False,
        ) as response:
            assert response.status == 302
            location = response.headers["Location"]
        redirect_query = parse_qs(urlsplit(location).query)
        assert redirect_query.get("state") == [state]
        code = redirect_query["code"][0]

        basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        async with session.post(
            token_endpoint,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": callback,
                "resource": resource,
                "code_verifier": verifier,
            },
            headers={"Authorization": "Basic " + basic},
        ) as response:
            tokens = await response.json()
            assert response.status == 200, tokens
        access = tokens["access_token"]

        async def rpc(request_id: int, method: str, params: dict) -> dict:
            async with session.post(
                endpoint,
                json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
                headers={"Authorization": "Bearer " + access},
            ) as response:
                result = await response.json()
                assert response.status == 200, result
                return result

        initialized = await rpc(1, "initialize", {"protocolVersion": "2025-06-18"})
        listed = await rpc(2, "tools/list", {})
        event_search = await rpc(
            3,
            "tools/call",
            {"name": "events_search", "arguments": {"include_past": True, "limit": 1}},
        )
        incident_search = await rpc(
            4,
            "tools/call",
            {"name": "incidents_search", "arguments": {"query": "incident", "limit": 1}},
        )
        snapshot = await rpc(
            5,
            "tools/call",
            {"name": "operations_snapshot", "arguments": {}},
        )

    return {
        "ok": True,
        "endpoint": endpoint,
        "protocol": initialized["result"]["protocolVersion"],
        "tools": [tool["name"] for tool in listed["result"]["tools"]],
        "event_result_count": len(event_search["result"]["structuredContent"]["events"]),
        "incident_result_count": len(incident_search["result"]["structuredContent"]["incidents"]),
        "database_mode": snapshot["result"]["structuredContent"]["database"]["mode"],
        "provider_calls": snapshot["result"]["structuredContent"]["network"]["provider_calls"],
        "access_token_fingerprint": fingerprint(access),
        "refresh_token_fingerprint": fingerprint(tokens["refresh_token"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a non-interactive OAuth + MCP production smoke.")
    parser.add_argument("--credentials", required=True, type=Path)
    args = parser.parse_args()
    result = asyncio.run(run(args.credentials))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
