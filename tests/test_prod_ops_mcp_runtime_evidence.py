from __future__ import annotations

import pytest

from prod_ops_mcp.runtime_evidence import RuntimeEvidenceReader


@pytest.mark.asyncio
async def test_runtime_trace_is_literal_bounded_and_redacted(tmp_path):
    path = tmp_path / "events-bot.log"
    path.write_text(
        "ignore\nrun_id=abc access_token=secret-value\n"
        "run_id=abc Authorization: Bearer top-secret\n",
        encoding="utf-8",
    )
    reader = RuntimeEvidenceReader(path)
    result = await reader.trace({"needle": "run_id=abc", "limit": 10, "max_scan_bytes": 16384})
    assert result["count"] == 2
    joined = "\n".join(result["items"])
    assert "secret-value" not in joined
    assert "top-secret" not in joined
    assert result["scanned_bytes"] <= 16384
