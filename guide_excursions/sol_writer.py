"""Run the native Codex text writer with its own login and no bot credentials."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import tempfile


SOL_MODEL = "gpt-6.1-sol"


class SolWriterError(RuntimeError):
    """Short diagnostic code safe to log without source text or credentials."""


async def generate_sol_narrative(prompt: str, schema: dict) -> str:
    environment = {
        name: os.environ[name]
        for name in ("HOME", "PATH", "CODEX_HOME", "LANG", "LC_ALL", "SSL_CERT_FILE",
                     "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY")
        if name in os.environ
    }
    if service_home := os.environ.get("GUIDE_DIGEST_CODEX_HOME"):
        environment["CODEX_HOME"] = service_home
    with tempfile.TemporaryDirectory(prefix="guide-sol-") as directory:
        schema_file = Path(directory) / "schema.json"
        schema_file.write_text(json.dumps(schema), encoding="utf-8")
        # Feed sources on stdin; never expose the bot's DB, VK tokens, Git checkout
        # or configured MCP servers to instructions embedded in an announcement.
        process = await asyncio.create_subprocess_exec(
            os.environ.get("GUIDE_DIGEST_CODEX_BIN") or "codex",
            "--ask-for-approval", "never",
            "-c", 'default_permissions="digest"',
            "-c", 'permissions.digest.filesystem={":root"="deny",":minimal"="read",":tmpdir"="deny",":slash_tmp"="deny"}',
            "-c", "permissions.digest.network.enabled=false",
            "-c", 'shell_environment_policy.inherit="none"',
            "-c", "project_doc_max_bytes=0",
            "-c", 'web_search="disabled"',
            "-c", 'model_reasoning_effort="medium"',
            "--disable", "shell_tool", "--disable", "multi_agent", "--disable", "apps",
            "--disable", "plugins", "--disable", "hooks", "--disable", "computer_use",
            "--disable", "image_generation", "--disable", "view_image",
            "exec", "--ignore-user-config", "--strict-config", "--ephemeral",
            "--skip-git-repo-check", "--output-schema", str(schema_file), "--json",
            "--model", SOL_MODEL, "-",
            cwd=directory, env=environment, start_new_session=True,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            output, _ = await process.communicate(prompt.encode("utf-8"))
        finally:
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await process.wait()
    if process.returncode:
        raise SolWriterError(f"codex_exit_{process.returncode}")
    result = None
    completed = False
    for line in output.splitlines():
        event = json.loads(line)
        kind = event.get("type")
        if kind == "turn.completed":
            completed = True
        if kind in {"error", "turn.failed", "model.rerouted", "model_reroute", "model/rerouted"}:
            raise SolWriterError("codex_unsuccessful_turn")
        if kind == "item.completed":
            item = event.get("item") or {}
            if item.get("type") == "agent_message":
                result = item.get("text")
            elif item.get("type") not in {"reasoning", "plan"}:
                raise SolWriterError("codex_non_text_item")
    if not completed or not isinstance(result, str) or not result.strip():
        raise SolWriterError("codex_empty_turn")
    return result
