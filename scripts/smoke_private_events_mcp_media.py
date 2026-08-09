#!/usr/bin/env python3
"""Bounded Private Events MCP media/story smoke.

The default path lists the granted catalogue only. Provider reads require
explicit opaque refs. Story preparation/commit additionally require
``--allow-write`` and use an owner-only receipt file so exact preparation
values are never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import html
import json
import os
import re
import secrets
import stat
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import aiohttp

EVIDENCE_SCOPES = ("events:read", "incidents:read", "operations:read", "offline_access")
MEDIA_TOOLS = frozenset(
    {
        "social_content_stories",
        "social_content_analytics",
        "social_asset_stage",
        "social_asset_status",
        "social_asset_preview",
        "social_action_prepare",
        "social_action_commit",
        "social_action_status",
    }
)


class SmokeError(RuntimeError):
    """A sanitized smoke failure safe to report without response bodies."""


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _sanitized_endpoint(endpoint: str) -> dict[str, str]:
    parsed = urlsplit(endpoint)
    suffix = "/codex/mcp" if parsed.path.endswith("/codex/mcp") else "/mcp"
    return {
        "public_origin": f"{parsed.scheme}://{parsed.netloc}",
        "mcp_path": f"/_private/<redacted>{suffix}",
        "endpoint_fingerprint": _fingerprint(endpoint),
    }


def _load_json(path: Path, *, owner_only: bool = False) -> dict[str, Any]:
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SmokeError("input_not_regular_file")
    if owner_only and info.st_mode & 0o077:
        raise SmokeError("receipt_permissions_not_owner_only")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SmokeError("invalid_json_input") from exc
    if not isinstance(value, dict):
        raise SmokeError("invalid_json_object")
    return value


def _write_private_receipt(path: Path, payload: dict[str, Any]) -> None:
    parent = path.parent
    parent_info = parent.lstat()
    if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
        raise SmokeError("receipt_parent_not_real_directory")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise SmokeError("receipt_create_failed") from exc
    try:
        os.fchmod(fd, 0o600)
        encoded = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(fd, remaining)
            if written <= 0:
                raise SmokeError("receipt_write_failed")
            remaining = remaining[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _structured(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result")
    if not isinstance(result, dict) or result.get("isError") is True:
        raise SmokeError("tool_call_failed")
    structured = result.get("structuredContent")
    if not isinstance(structured, dict):
        raise SmokeError("structured_content_missing")
    return structured


async def _oauth_session(
    session: aiohttp.ClientSession,
    credentials: dict[str, Any],
    scopes: tuple[str, ...],
) -> tuple[str, str, dict[str, Any]]:
    app = credentials.get("chatgpt")
    if not isinstance(app, dict):
        raise SmokeError("chatgpt_credentials_missing")
    required = ("mcp_url", "oauth_client_id", "oauth_client_secret", "bootstrap_operator_token")
    if any(not isinstance(app.get(key), str) or not app[key] for key in required):
        raise SmokeError("chatgpt_credentials_incomplete")

    endpoint = app["mcp_url"]
    client_id = app["oauth_client_id"]
    client_secret = app["oauth_client_secret"]
    operator_token = app["bootstrap_operator_token"]
    parsed = urlsplit(endpoint)
    if parsed.scheme != "https" or not parsed.netloc or not parsed.path.endswith("/mcp"):
        raise SmokeError("invalid_mcp_endpoint")
    base_prefix = endpoint[: -len("/mcp")]
    authorize = base_prefix + "/oauth/authorize"
    token_endpoint = base_prefix + "/oauth/token"
    callback = "https://chatgpt.com/connector/oauth/private-events-smoke"
    verifier = _b64url(secrets.token_bytes(48))
    challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    state = secrets.token_urlsafe(24)

    query = urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": callback,
            "state": state,
            "resource": endpoint,
            "scope": " ".join(scopes),
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
    )
    async with session.get(authorize + "?" + query) as response:
        if response.status != 200:
            raise SmokeError("oauth_authorize_get_failed")
        page = await response.text()
    match = re.search(r'name="authorization_request" value="([^"]+)"', page)
    if match is None:
        raise SmokeError("oauth_authorization_request_missing")

    async with session.post(
        authorize,
        data={
            "authorization_request": html.unescape(match.group(1)),
            "operator_token": operator_token,
        },
        allow_redirects=False,
    ) as response:
        if response.status != 302:
            raise SmokeError("oauth_authorize_post_failed")
        location = response.headers.get("Location", "")
    redirect = parse_qs(urlsplit(location).query)
    if redirect.get("state") != [state] or not redirect.get("code"):
        raise SmokeError("oauth_redirect_invalid")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode("ascii")
    async with session.post(
        token_endpoint,
        data={
            "grant_type": "authorization_code",
            "code": redirect["code"][0],
            "redirect_uri": callback,
            "resource": endpoint,
            "code_verifier": verifier,
        },
        headers={"Authorization": "Basic " + basic},
    ) as response:
        if response.status != 200:
            raise SmokeError("oauth_token_exchange_failed")
        tokens = await response.json()
    access = tokens.get("access_token") if isinstance(tokens, dict) else None
    if not isinstance(access, str) or not access:
        raise SmokeError("oauth_access_token_missing")
    return endpoint, access, app


async def run(args: argparse.Namespace) -> dict[str, Any]:
    credentials = _load_json(args.credentials, owner_only=True)
    scopes = list(EVIDENCE_SCOPES)
    if args.platform:
        scopes.append(f"{args.platform}:read")
        if args.allow_write:
            scopes.append(f"{args.platform}:publish")

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        endpoint, access, _app = await _oauth_session(session, credentials, tuple(scopes))
        request_id = 0

        async def rpc(method: str, params: dict[str, Any]) -> dict[str, Any]:
            nonlocal request_id
            request_id += 1
            async with session.post(
                endpoint,
                json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
                headers={"Authorization": "Bearer " + access},
            ) as response:
                if response.status != 200:
                    raise SmokeError("mcp_http_failure")
                value = await response.json()
            if not isinstance(value, dict) or "error" in value:
                raise SmokeError("mcp_rpc_failure")
            return value

        await rpc("initialize", {"protocolVersion": "2025-06-18"})
        listed = await rpc("tools/list", {})
        result = listed.get("result")
        tools = result.get("tools") if isinstance(result, dict) else None
        if not isinstance(tools, list):
            raise SmokeError("tool_catalogue_invalid")
        names = sorted(
            tool["name"]
            for tool in tools
            if isinstance(tool, dict) and isinstance(tool.get("name"), str)
        )
        receipt: dict[str, Any] = {
            "ok": True,
            **_sanitized_endpoint(endpoint),
            "mode": "nonmutating" if not args.allow_write else "explicit_write_gate",
            "tool_count": len(names),
            "media_story_tools": [name for name in names if name in MEDIA_TOOLS],
        }

        async def call(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
            return _structured(
                await rpc("tools/call", {"name": name, "arguments": arguments})
            )

        if args.preview_asset_ref:
            preview_response = await rpc(
                "tools/call",
                {
                    "name": "social_asset_preview",
                    "arguments": {
                        "platform": args.platform,
                        "asset_ref": args.preview_asset_ref,
                    },
                },
            )
            structured = _structured(preview_response)
            result = preview_response.get("result")
            content = result.get("content") if isinstance(result, dict) else None
            image = content[0] if isinstance(content, list) and content else None
            if (
                not isinstance(image, dict)
                or image.get("type") != "image"
                or image.get("mimeType") != "image/jpeg"
                or not isinstance(image.get("data"), str)
                or len(image["data"]) > 90_000
            ):
                raise SmokeError("story_image_preview_invalid")
            receipt["story_image_preview"] = {
                "asset_ref_fingerprint": _fingerprint(args.preview_asset_ref),
                "mime_type": structured.get("mime_type"),
                "byte_length": structured.get("byte_length"),
                "width": structured.get("width"),
                "height": structured.get("height"),
            }

        if args.read_stories:
            stories = await call(
                "social_content_stories",
                {
                    "platform": args.platform,
                    "operation": "list_stories",
                    "target_ref": args.target_ref,
                    "limit": args.limit,
                },
            )
            items = stories.get("results")
            if not isinstance(items, list):
                raise SmokeError("story_page_invalid")
            receipt["story_read"] = {
                "target_ref_fingerprint": _fingerprint(args.target_ref),
                "result_count": len(items),
                "next_cursor_present": isinstance(stories.get("next_cursor"), str),
            }

        if args.read_statistics:
            arguments: dict[str, Any] = {
                "platform": args.platform,
                "operation": "get_statistics",
                "item_kinds": ["story"],
            }
            ref = args.item_ref or args.target_ref
            arguments["item_ref" if args.item_ref else "target_ref"] = ref
            statistics = await call("social_content_analytics", arguments)
            metrics = statistics.get("basic_metrics")
            if not isinstance(metrics, dict):
                raise SmokeError("story_statistics_invalid")
            receipt["story_statistics"] = {
                "ref_fingerprint": _fingerprint(ref),
                "aggregate_metrics": {
                    key: value
                    for key, value in metrics.items()
                    if isinstance(key, str) and type(value) in {int, float}
                },
            }

        if args.prepare_story:
            prepared = await call(
                "social_action_prepare",
                {
                    "platform": args.platform,
                    "action": "story",
                    "idempotency_key": args.idempotency_key,
                    "target_ref": args.target_ref,
                    "content": {"media": [{"asset_ref": args.asset_ref, "role": "image"}]},
                },
            )
            required = ("preparation_ref", "action_digest", "expires_at")
            if any(not isinstance(prepared.get(key), str) or not prepared[key] for key in required):
                raise SmokeError("story_preparation_invalid")
            if prepared.get("status") != "approved" or "approval_url" in prepared:
                raise SmokeError("story_preparation_not_directly_authorized")
            private_receipt = {
                "schema": 1,
                "platform": args.platform,
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
                "expires_at": prepared["expires_at"],
            }
            _write_private_receipt(args.receipt_file, private_receipt)
            receipt["story_prepare"] = {
                "preparation_ref_fingerprint": _fingerprint(prepared["preparation_ref"]),
                "action_digest_fingerprint": _fingerprint(prepared["action_digest"]),
                "secure_receipt_written": True,
                "provider_attempted": False,
                "next_step": "commit_exact_user_requested_preparation",
            }

        if args.commit_story:
            prepared = _load_json(args.preparation_receipt, owner_only=True)
            if prepared.get("schema") != 1 or prepared.get("platform") != args.platform:
                raise SmokeError("preparation_receipt_invalid")
            preparation_ref = prepared.get("preparation_ref")
            action_digest = prepared.get("action_digest")
            if not isinstance(preparation_ref, str) or not isinstance(action_digest, str):
                raise SmokeError("preparation_receipt_invalid")
            committed = await call(
                "social_action_commit",
                {"preparation_ref": preparation_ref, "action_digest": action_digest},
            )
            operation_ref = committed.get("operation_ref")
            if not isinstance(operation_ref, str):
                raise SmokeError("story_commit_invalid")
            receipt["story_commit"] = {
                "operation_ref_fingerprint": _fingerprint(operation_ref),
                "status": committed.get("status") if isinstance(committed.get("status"), str) else "unknown",
            }

    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a sanitized, nonmutating-by-default MCP media/story smoke."
    )
    parser.add_argument("--credentials", required=True, type=Path)
    parser.add_argument("--platform", choices=("telegram", "vk"))
    parser.add_argument("--target-ref")
    parser.add_argument("--item-ref")
    parser.add_argument("--limit", type=int, default=5, choices=range(1, 26), metavar="1..25")
    parser.add_argument("--read-stories", action="store_true")
    parser.add_argument("--read-statistics", action="store_true")
    parser.add_argument("--preview-asset-ref")
    parser.add_argument("--allow-write", action="store_true")
    write_mode = parser.add_mutually_exclusive_group()
    write_mode.add_argument("--prepare-story", action="store_true")
    write_mode.add_argument("--commit-story", action="store_true")
    parser.add_argument("--asset-ref")
    parser.add_argument("--idempotency-key")
    parser.add_argument("--receipt-file", type=Path)
    parser.add_argument("--preparation-receipt", type=Path)
    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if (
        args.read_stories
        or args.read_statistics
        or args.preview_asset_ref
        or args.prepare_story
        or args.commit_story
    ) and not args.platform:
        parser.error("provider reads/writes require --platform")
    if args.read_stories and not args.target_ref:
        parser.error("--read-stories requires --target-ref")
    if args.read_statistics and not (args.item_ref or args.target_ref):
        parser.error("--read-statistics requires --item-ref or --target-ref")
    if (args.prepare_story or args.commit_story) and not args.allow_write:
        parser.error("story prepare/commit requires the explicit --allow-write gate")
    if args.allow_write and not (args.prepare_story or args.commit_story):
        parser.error("--allow-write requires exactly one write mode")
    if args.prepare_story and not all(
        (args.target_ref, args.asset_ref, args.idempotency_key, args.receipt_file)
    ):
        parser.error(
            "--prepare-story requires --target-ref, --asset-ref, --idempotency-key and --receipt-file"
        )
    if args.commit_story and args.preparation_receipt is None:
        parser.error("--commit-story requires --preparation-receipt")


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    _validate_args(parser, args)
    try:
        result = asyncio.run(run(args))
    except (SmokeError, aiohttp.ClientError, asyncio.TimeoutError, OSError):
        # Never render exception strings: HTTP/library errors can include the
        # confidential endpoint, refs, local paths or response fragments.
        print(json.dumps({"ok": False, "error_code": "media_story_smoke_failed"}))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
