#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import stat
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

_MAX_CREDENTIAL_BYTES = 1024 * 1024
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
_DEPLOY_KEYS = frozenset(
    {
        "PRIVATE_EVENTS_MCP_ENABLED",
        "PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL",
        "PRIVATE_EVENTS_MCP_PATH_SECRET",
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID",
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET",
        "PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID",
        "PRIVATE_EVENTS_MCP_OPERATOR_TOKEN",
        "PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN",
        "PRIVATE_EVENTS_MCP_SIGNING_KEY",
        "PRIVATE_EVENTS_MCP_AUTH_DB_PATH",
        "PRIVATE_EVENTS_MCP_REPOSITORY_ROOT",
        "PRIVATE_EVENTS_MCP_REPOSITORY_SLUG",
        "PRIVATE_EVENTS_MCP_REPOSITORY_SHA_FILE",
    }
)


def token(prefix: str, bytes_count: int) -> str:
    return prefix + secrets.token_urlsafe(bytes_count)


def validate_base_url(value: str) -> str:
    value = value.strip()
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise argparse.ArgumentTypeError("base URL must be a canonical HTTPS origin") from exc
    hostname = parsed.hostname
    if (
        parsed.scheme != "https"
        or hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.netloc.casefold() != hostname.casefold()
        or re.fullmatch(
            r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?", hostname
        )
        is None
    ):
        raise argparse.ArgumentTypeError("base URL must be a canonical HTTPS origin")
    return f"https://{hostname.casefold()}"


def write_private_text(path: Path, content: str) -> None:
    """Create a new credential artifact with mode 0600 from its first inode."""

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            fd = -1
            stream.write(content)
    finally:
        if fd >= 0:
            os.close(fd)


def create_private_output_dir(path: Path) -> None:
    """Create a fresh owner-only directory; never mix with prior artifacts."""

    path = _absolute_path(path)
    if path.parent == path:
        raise FileExistsError("output directory must not be a filesystem root")
    _reject_symlink_components(path.parent)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    parent_fd = os.open(path.parent, flags)
    directory_fd = -1
    try:
        os.mkdir(path.name, 0o700, dir_fd=parent_fd)
        directory_fd = os.open(path.name, flags, dir_fd=parent_fd)
        os.fchmod(directory_fd, 0o700)
    finally:
        if directory_fd >= 0:
            os.close(directory_fd)
        os.close(parent_fd)


def _absolute_path(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _reject_symlink_components(path: Path) -> None:
    """Reject symlinks in an existing path without resolving through them."""

    absolute = _absolute_path(path)
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        metadata = os.lstat(current)
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError("credential paths must not contain symlinks")


def _read_full_credentials(path: Path) -> dict[str, Any]:
    source = _absolute_path(path)
    _reject_symlink_components(source)
    if not stat.S_ISREG(os.lstat(source).st_mode):
        raise ValueError("existing credentials must be a regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(source, flags)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("existing credentials must be a regular file")
        if metadata.st_size <= 0 or metadata.st_size > _MAX_CREDENTIAL_BYTES:
            raise ValueError("existing credentials size is invalid")
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            fd = -1
            raw = stream.read(_MAX_CREDENTIAL_BYTES + 1)
    finally:
        if fd >= 0:
            os.close(fd)
    if len(raw.encode("utf-8")) > _MAX_CREDENTIAL_BYTES:
        raise ValueError("existing credentials size is invalid")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("existing credentials contain duplicate keys")
            value[key] = item
        return value

    try:
        credentials = json.loads(raw, object_pairs_hook=reject_duplicate_keys)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValueError("existing credentials are not valid JSON") from exc
    _validate_full_credentials(credentials)
    return credentials


def _required_text(mapping: dict[str, Any], key: str, section: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise ValueError(f"existing credentials have invalid {section}.{key}")
    return value


def _validate_full_credentials(value: Any) -> None:
    required_sections = {"chatgpt", "codex", "deploy"}
    if not isinstance(value, dict) or not required_sections <= set(value):
        raise ValueError("existing credentials must contain chatgpt, codex and deploy")
    chatgpt = value["chatgpt"]
    codex = value["codex"]
    deploy = value["deploy"]
    if not all(isinstance(section, dict) for section in (chatgpt, codex, deploy)):
        raise ValueError("existing credential sections must be objects")
    missing = sorted(_DEPLOY_KEYS - set(deploy))
    if missing:
        raise ValueError("existing credentials are missing full deploy fields")
    for key in _DEPLOY_KEYS:
        _required_text(deploy, key, "deploy")
    for key, item in deploy.items():
        if not isinstance(key, str) or _ENV_NAME_RE.fullmatch(key) is None:
            raise ValueError("existing credentials have an invalid deploy environment name")
        if not isinstance(item, str) or any(marker in item for marker in ("\0", "\n", "\r")):
            raise ValueError(f"existing credentials have invalid deploy.{key}")
    for key in (
        "mcp_url",
        "oauth_client_id",
        "oauth_client_secret",
        "bootstrap_operator_token",
        "social_approval_operator_token",
    ):
        _required_text(chatgpt, key, "chatgpt")
    for key in ("mcp_url", "oauth_client_id", "bootstrap_operator_token"):
        _required_text(codex, key, "codex")

    base_url = _required_text(
        deploy, "PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL", "deploy"
    )
    try:
        validated_base_url = validate_base_url(base_url)
    except argparse.ArgumentTypeError as exc:
        raise ValueError("existing credentials contain an invalid public base URL") from exc
    path_secret = _required_text(deploy, "PRIVATE_EVENTS_MCP_PATH_SECRET", "deploy")
    expected_chatgpt_url = f"{validated_base_url}/_private/{path_secret}/mcp"
    expected_codex_url = f"{validated_base_url}/_private/{path_secret}/codex/mcp"
    consistency_pairs = (
        (chatgpt["mcp_url"], expected_chatgpt_url),
        (codex["mcp_url"], expected_codex_url),
        (chatgpt["oauth_client_id"], deploy["PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID"]),
        (
            chatgpt["oauth_client_secret"],
            deploy["PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET"],
        ),
        (codex["oauth_client_id"], deploy["PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID"]),
        (
            chatgpt["social_approval_operator_token"],
            deploy["PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN"],
        ),
        (
            chatgpt["bootstrap_operator_token"],
            deploy["PRIVATE_EVENTS_MCP_OPERATOR_TOKEN"],
        ),
        (
            codex["bootstrap_operator_token"],
            deploy["PRIVATE_EVENTS_MCP_OPERATOR_TOKEN"],
        ),
    )
    if any(actual != expected for actual, expected in consistency_pairs):
        raise ValueError("existing full credentials are internally inconsistent")


def _paths_overlap(source: Path, output: Path) -> bool:
    source_text = os.fspath(_absolute_path(source))
    output_text = os.fspath(_absolute_path(output))
    common = os.path.commonpath((source_text, output_text))
    return common in {source_text, output_text}


def _build_new_credentials(
    base_url: str, *, enable_chatgpt_social: bool
) -> dict[str, Any]:
    path_secret = token("mcp_", 32)
    client_id = token("chatgpt-events-", 18)
    client_secret = token("client_", 48)
    codex_client_id = token("codex-events-", 18)
    operator_token = token("operator_", 48)
    social_approval_token = token("approval_", 48)
    signing_key = token("signing_", 64)
    endpoint = f"{base_url}/_private/{path_secret}/mcp"
    codex_endpoint = f"{base_url}/_private/{path_secret}/codex/mcp"

    chatgpt_scopes = ["events:read", "incidents:read", "operations:read"]
    if enable_chatgpt_social:
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
        chatgpt_scopes.append("vk:notifications:read")
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
            "vk:notifications:read",
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
        "PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL": base_url,
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
    return {"chatgpt": chatgpt, "codex": codex, "deploy": deploy}


def _rotate_bootstrap_only(credentials: dict[str, Any]) -> dict[str, Any]:
    operator_token = token("operator_", 48)
    credentials["deploy"]["PRIVATE_EVENTS_MCP_OPERATOR_TOKEN"] = operator_token
    credentials["chatgpt"]["bootstrap_operator_token"] = operator_token
    credentials["codex"]["bootstrap_operator_token"] = operator_token
    return credentials


def _write_credentials(output: Path, credentials: dict[str, Any]) -> None:
    chatgpt = credentials["chatgpt"]
    codex = credentials["codex"]
    deploy = credentials["deploy"]
    write_private_text(
        output / "chatgpt-private-app-credentials.json",
        json.dumps(credentials, ensure_ascii=False, indent=2) + "\n",
    )
    write_private_text(
        output / "fly-secrets.env",
        "\n".join(f"{key}={value}" for key, value in deploy.items()) + "\n",
    )
    write_private_text(
        output / "chatgpt-private-app-config.json",
        json.dumps(chatgpt, ensure_ascii=False, indent=2) + "\n",
    )
    write_private_text(
        output / "codex-private-mcp-config.json",
        json.dumps(codex, ensure_ascii=False, indent=2) + "\n",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate deploy, ChatGPT and Codex credentials for the private events MCP."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--new-install",
        action="store_true",
        help="Create a completely new stable endpoint and OAuth identity.",
    )
    mode.add_argument(
        "--rotate-bootstrap-only",
        type=Path,
        metavar="FULL_CREDENTIALS_JSON",
        help=(
            "Read an existing full credentials JSON and rotate only the shared "
            "bootstrap operator token."
        ),
    )
    # Validate after parsing so argparse never reflects a secret-bearing invalid
    # URL value into stderr.
    parser.add_argument("--base-url")
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

    output = _absolute_path(args.output_dir)
    if args.new_install:
        if args.base_url is None:
            parser.error("--new-install requires --base-url")
        try:
            base_url = validate_base_url(args.base_url)
        except argparse.ArgumentTypeError:
            parser.error("--base-url must be a canonical HTTPS origin")
        credentials = _build_new_credentials(
            base_url, enable_chatgpt_social=args.enable_chatgpt_social
        )
        receipt_mode = "new_install"
    else:
        if args.base_url is not None:
            parser.error("--base-url cannot be used with --rotate-bootstrap-only")
        if args.enable_chatgpt_social:
            parser.error(
                "--enable-chatgpt-social cannot be used with --rotate-bootstrap-only"
            )
        source = _absolute_path(args.rotate_bootstrap_only)
        if _paths_overlap(source, output):
            parser.error("credential source and output directory must not overlap")
        try:
            credentials = _rotate_bootstrap_only(_read_full_credentials(source))
        except (OSError, ValueError) as exc:
            parser.error(str(exc))
        receipt_mode = "rotate_bootstrap_only"

    try:
        create_private_output_dir(output)
    except (FileExistsError, NotADirectoryError, OSError, ValueError):
        parser.error("--output-dir must be a fresh non-symlink path with an existing parent")
    _write_credentials(output, credentials)

    deploy = credentials["deploy"]
    endpoint = credentials["chatgpt"]["mcp_url"]
    codex_endpoint = credentials["codex"]["mcp_url"]
    receipt = {
        "mode": receipt_mode,
        "credentials_file": str(output / "chatgpt-private-app-credentials.json"),
        "deploy_env_file": str(output / "fly-secrets.env"),
        "chatgpt_config_file": str(output / "chatgpt-private-app-config.json"),
        "codex_config_file": str(output / "codex-private-mcp-config.json"),
        "public_origin": deploy["PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL"],
        "mcp_path": "/_private/<redacted>/mcp",
        "endpoint_fingerprint": hashlib.sha256(endpoint.encode()).hexdigest()[:12],
        "codex_endpoint_fingerprint": hashlib.sha256(
            codex_endpoint.encode()
        ).hexdigest()[:12],
    }
    print(json.dumps(receipt, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
