#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
from pathlib import Path
from urllib.parse import urlsplit


def token(prefix: str, bytes_count: int) -> str:
    return prefix + secrets.token_urlsafe(bytes_count)


def validate_base_url(value: str) -> str:
    value = value.strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path not in {"", "/"}:
        raise argparse.ArgumentTypeError("base URL must be an HTTPS origin without a path")
    return value


def write_private_text(path: Path, content: str) -> None:
    """Create a new credential artifact with mode 0600 from its first inode."""

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            fd = -1
            stream.write(content)
    finally:
        if fd >= 0:
            os.close(fd)


def create_private_output_dir(path: Path) -> None:
    """Create a fresh owner-only directory; never mix with prior artifacts."""

    path.mkdir(parents=True, mode=0o700, exist_ok=False)
    # umask may only have made the new directory stricter. The inode is newly
    # owned by this process, so normalizing it to owner rwx is safe.
    os.chmod(path, 0o700)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate deploy, ChatGPT and Codex credentials for the private events MCP."
    )
    parser.add_argument("--base-url", required=True, type=validate_base_url)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--enable-chatgpt-social",
        action="store_true",
        help=(
            "Explicitly request Telegram/VK read+publish and offline_access scopes for "
            "the generated ChatGPT connector profile."
        ),
    )
    args = parser.parse_args()

    output = args.output_dir.resolve()
    try:
        create_private_output_dir(output)
    except FileExistsError:
        parser.error("--output-dir must be a fresh path that does not already exist")
    path_secret = token("mcp_", 32)
    client_id = token("chatgpt-events-", 18)
    client_secret = token("client_", 48)
    codex_client_id = token("codex-events-", 18)
    operator_token = token("operator_", 48)
    social_approval_token = token("approval_", 48)
    signing_key = token("signing_", 64)
    endpoint = f"{args.base_url}/_private/{path_secret}/mcp"
    codex_endpoint = f"{args.base_url}/_private/{path_secret}/codex/mcp"

    chatgpt_scopes = ["events:read", "incidents:read", "operations:read"]
    if args.enable_chatgpt_social:
        for platform in ("telegram", "vk"):
            chatgpt_scopes.extend(
                f"{platform}:{suffix}"
                for suffix in (
                    "discover",
                    "read:public",
                    "read:private",
                    "read:dialogs",
                    "dm:send",
                    "post:publish",
                    "edit",
                    "delete",
                    "forward",
                    "reaction",
                    "comment",
                    "schedule",
                    "analytics",
                    "audience",
                )
            )
        chatgpt_scopes.append("offline_access")
    chatgpt = {
        "name": "Events Bot — private production evidence",
        "description": (
            "Event/incident analysis plus a typed Telegram/VK analyst and editorial "
            "workspace with independent browser approval for mutations"
        ),
        "mcp_url": endpoint,
        "authentication": "OAuth",
        "oauth_client_id": client_id,
        "oauth_client_secret": client_secret,
        "bootstrap_operator_token": operator_token,
        "social_approval_operator_token": social_approval_token,
        "oauth_scopes": chatgpt_scopes,
        "available_optional_scopes": [
            "offline_access",
            *[
                f"{platform}:{suffix}"
                for platform in ("telegram", "vk")
                for suffix in (
                    "discover",
                    "read:public",
                    "read:private",
                    "read:dialogs",
                    "dm:send",
                    "post:publish",
                    "edit",
                    "delete",
                    "forward",
                    "reaction",
                    "comment",
                    "schedule",
                    "analytics",
                    "audience",
                )
            ],
        ],
        "notes": [
            "The bootstrap operator token is entered once in the authorization browser page.",
            "ChatGPT receives short-lived access and rotating refresh tokens automatically.",
            "Rotate PRIVATE_EVENTS_MCP_OPERATOR_TOKEN after the first successful connection.",
            "Never paste the social approval operator token into ChatGPT; enter it only on the server approval page.",
        ],
    }
    codex = {
        "name": "Events Bot — private production evidence",
        "mcp_url": codex_endpoint,
        "oauth_client_id": codex_client_id,
        "token_endpoint_auth_method": "none",
        "redirect_uri_contract": "http://127.0.0.1:<explicit-port>/callback/<opaque>",
        "smoke_redirect_uri": "http://127.0.0.1:1455/callback/private-events-smoke",
        "bootstrap_operator_token": operator_token,
        "oauth_scopes": [
            "events:read",
            "incidents:read",
            "operations:read",
            "offline_access",
        ],
        "notes": [
            "Codex is a static public OAuth client: do not configure or send a client secret.",
            "Authorization-code and refresh exchanges send oauth_client_id in the form body.",
            "The bootstrap operator token is entered only on the authorization browser page.",
        ],
    }
    deploy = {
        "PRIVATE_EVENTS_MCP_ENABLED": "1",
        "PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL": args.base_url,
        "PRIVATE_EVENTS_MCP_PATH_SECRET": path_secret,
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID": client_id,
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET": client_secret,
        "PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID": codex_client_id,
        "PRIVATE_EVENTS_MCP_OPERATOR_TOKEN": operator_token,
        "PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN": social_approval_token,
        "PRIVATE_EVENTS_MCP_SIGNING_KEY": signing_key,
        "PRIVATE_EVENTS_MCP_AUTH_DB_PATH": "/data/private-events-mcp-auth.sqlite",
        "PRIVATE_EVENTS_MCP_REPOSITORY_ROOT": "/app",
        "PRIVATE_EVENTS_MCP_REPOSITORY_SLUG": "onedayonemasterpiece/events-bot-new",
        "PRIVATE_EVENTS_MCP_REPOSITORY_SHA_FILE": "/app/.static-site-repo-sha",
    }

    credentials_path = output / "chatgpt-private-app-credentials.json"
    write_private_text(
        credentials_path,
        json.dumps(
            {"chatgpt": chatgpt, "codex": codex, "deploy": deploy},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
    )
    env_path = output / "fly-secrets.env"
    write_private_text(
        env_path,
        "\n".join(f"{key}={value}" for key, value in deploy.items()) + "\n",
    )
    config_path = output / "chatgpt-private-app-config.json"
    write_private_text(
        config_path,
        json.dumps(chatgpt, ensure_ascii=False, indent=2) + "\n",
    )
    codex_config_path = output / "codex-private-mcp-config.json"
    write_private_text(
        codex_config_path,
        json.dumps(codex, ensure_ascii=False, indent=2) + "\n",
    )

    print(json.dumps({
        "credentials_file": str(credentials_path),
        "deploy_env_file": str(env_path),
        "chatgpt_config_file": str(config_path),
        "codex_config_file": str(codex_config_path),
        "public_origin": args.base_url,
        "mcp_path": "/_private/<redacted>/mcp",
        "endpoint_fingerprint": hashlib.sha256(endpoint.encode()).hexdigest()[:12],
        "codex_endpoint_fingerprint": hashlib.sha256(
            codex_endpoint.encode()
        ).hexdigest()[:12],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
