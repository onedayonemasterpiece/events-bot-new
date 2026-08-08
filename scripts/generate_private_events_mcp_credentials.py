#!/usr/bin/env python3
from __future__ import annotations

import argparse
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate deploy and ChatGPT credentials for the private events MCP."
    )
    parser.add_argument("--base-url", required=True, type=validate_base_url)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    path_secret = token("mcp_", 32)
    client_id = token("chatgpt-events-", 18)
    client_secret = token("client_", 48)
    operator_token = token("operator_", 48)
    signing_key = token("signing_", 64)
    endpoint = f"{args.base_url}/_private/{path_secret}/mcp"

    public = {
        "name": "Events Bot — private production evidence",
        "description": "Read-only event, incident and operations analysis",
        "mcp_url": endpoint,
        "authentication": "OAuth",
        "oauth_client_id": client_id,
        "oauth_client_secret": client_secret,
        "bootstrap_operator_token": operator_token,
        "oauth_scopes": [
            "events:read",
            "incidents:read",
            "operations:read",
            "offline_access",
        ],
        "notes": [
            "The bootstrap operator token is entered once in the authorization browser page.",
            "ChatGPT receives short-lived access and rotating refresh tokens automatically.",
            "Rotate PRIVATE_EVENTS_MCP_OPERATOR_TOKEN after the first successful connection.",
        ],
    }
    deploy = {
        "PRIVATE_EVENTS_MCP_ENABLED": "1",
        "PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL": args.base_url,
        "PRIVATE_EVENTS_MCP_PATH_SECRET": path_secret,
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID": client_id,
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET": client_secret,
        "PRIVATE_EVENTS_MCP_OPERATOR_TOKEN": operator_token,
        "PRIVATE_EVENTS_MCP_SIGNING_KEY": signing_key,
        "PRIVATE_EVENTS_MCP_AUTH_DB_PATH": "/data/private-events-mcp-auth.sqlite",
        "PRIVATE_EVENTS_MCP_REPOSITORY_ROOT": "/app",
        "PRIVATE_EVENTS_MCP_REPOSITORY_SLUG": "onedayonemasterpiece/events-bot-new",
        "PRIVATE_EVENTS_MCP_REPOSITORY_SHA_FILE": "/app/.static-site-repo-sha",
    }

    credentials_path = output / "chatgpt-private-app-credentials.json"
    credentials_path.write_text(
        json.dumps({"chatgpt": public, "deploy": deploy}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    env_path = output / "fly-secrets.env"
    env_path.write_text(
        "\n".join(f"{key}={value}" for key, value in deploy.items()) + "\n",
        encoding="utf-8",
    )
    config_path = output / "chatgpt-private-app-config.json"
    config_path.write_text(json.dumps(public, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for path in (credentials_path, env_path, config_path):
        os.chmod(path, 0o600)

    print(json.dumps({
        "credentials_file": str(credentials_path),
        "deploy_env_file": str(env_path),
        "chatgpt_config_file": str(config_path),
        "mcp_url": endpoint,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
