from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path
from typing import Any, Mapping

_BEARER = re.compile(r"(?i)\bBearer\s+[^\s\"']+")
_TOKEN_PARAM = re.compile(r"(?i)(access_token|token|secret|signature|sig)=([^&\s]+)")


class RuntimeEvidenceError(RuntimeError):
    pass


class RuntimeEvidenceReader:
    """Literal, tail-bounded search over the active runtime log only."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            directory = os.getenv("RUNTIME_LOG_DIR", "/data/runtime_logs")
            basename = os.getenv("RUNTIME_LOG_BASENAME", "events-bot.log")
            path = Path(directory) / basename
        self._path = Path(path)

    @staticmethod
    def _sanitize(line: str) -> str:
        line = _BEARER.sub("Bearer <redacted>", line)
        line = _TOKEN_PARAM.sub(lambda match: f"{match.group(1)}=<redacted>", line)
        return line[:1200] + ("…<truncated>" if len(line) > 1200 else "")

    async def trace(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        needle = str(arguments.get("needle") or "").strip()
        if not 3 <= len(needle) <= 128 or "\n" in needle or "\r" in needle:
            raise RuntimeEvidenceError("needle must be a 3-128 character literal")
        if not any(character.isalnum() for character in needle):
            raise RuntimeEvidenceError("needle must contain an alphanumeric character")
        limit = min(50, max(1, int(arguments.get("limit") or 20)))
        max_bytes = min(
            1024 * 1024,
            max(16 * 1024, int(arguments.get("max_scan_bytes") or 256 * 1024)),
        )
        return await asyncio.wait_for(
            asyncio.to_thread(self._trace_sync, needle, limit, max_bytes),
            timeout=0.5,
        )

    def _trace_sync(self, needle: str, limit: int, max_bytes: int) -> dict[str, Any]:
        if not self._path.is_file():
            return {
                "items": [],
                "count": 0,
                "evidence_gaps": ["active_runtime_log_missing"],
            }
        size = self._path.stat().st_size
        with self._path.open("rb") as stream:
            start = max(0, size - max_bytes)
            stream.seek(start)
            payload = stream.read(max_bytes)
        text = payload.decode("utf-8", errors="replace")
        lines = text.splitlines()
        # The first line can be partial when reading from a non-zero offset.
        if start and lines:
            lines = lines[1:]
        matches = [self._sanitize(line) for line in lines if needle in line]
        matches = matches[-limit:]
        return {
            "items": matches,
            "count": len(matches),
            "active_log_only": True,
            "scanned_bytes": len(payload),
            "file_size_bytes": size,
            "literal_match": True,
        }
