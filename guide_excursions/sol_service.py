"""Run only the excursion narrative writer over a private Unix socket."""
from __future__ import annotations

import asyncio
import json
import sys

from .sol_writer import SolWriterError, generate_sol_narrative


MAX_REQUEST = 1024 * 1024
MAX_RESPONSE = 128 * 1024


async def request_narrative(socket_path: str, payload, *, issue_id: int) -> str:
    message = json.dumps({"issue_id": issue_id, "payload": payload}, ensure_ascii=False).encode() + b"\n"
    if len(message) > MAX_REQUEST:
        raise SolWriterError("codex_service_request_too_large")
    reader, writer = await asyncio.open_unix_connection(socket_path, limit=MAX_RESPONSE)
    try:
        writer.write(message)
        await writer.drain()
        reply = json.loads(await reader.readline())
    finally:
        writer.close()
        await writer.wait_closed()
    if not isinstance(reply, dict) or not isinstance(reply.get("text"), str) or not reply["text"].strip():
        raise SolWriterError("codex_service_no_narrative")
    return reply["text"]


async def write_edition(message: bytes) -> str:
    from .digest_writer import _VK_NARRATIVE_SOL_SCHEMA, _vk_narrative_prompt

    if len(message) > MAX_REQUEST or not message.endswith(b"\n"):
        raise ValueError("invalid edition size")
    request = json.loads(message)
    if not isinstance(request, dict) or set(request) != {"issue_id", "payload"}:
        raise ValueError("invalid edition")
    issue_id, payload = request["issue_id"], request["payload"]
    if type(issue_id) is not int or issue_id <= 0 or not isinstance(payload, dict):
        raise ValueError("invalid edition identity")
    if not isinstance(payload.get("excursions"), list) or not payload["excursions"]:
        raise ValueError("empty edition")
    prompt = _vk_narrative_prompt(payload, issue_id=issue_id)
    return await generate_sol_narrative(prompt, _VK_NARRATIVE_SOL_SCHEMA)


def main() -> None:
    # systemd accepts one connection per process. Its socket is private to the
    # deployment user. CLI stdout and stderr never enter the wire protocol.
    try:
        text = asyncio.run(asyncio.wait_for(write_edition(sys.stdin.buffer.readline(MAX_REQUEST + 1)), 175))
        reply = json.dumps({"text": text}, ensure_ascii=False).encode() + b"\n"
        if len(reply) > MAX_RESPONSE:
            raise ValueError("oversized narrative")
    except Exception as exc:
        print(f"excursion writer failed: {type(exc).__name__}", file=sys.stderr)
        reply = b'{"error":"writer_failed"}\n'
    sys.stdout.buffer.write(reply)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
