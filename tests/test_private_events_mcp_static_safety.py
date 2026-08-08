from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import pytest

from private_events_mcp.readonly_sqlite import QueryBudgetExceeded, ReadOnlySQLite


PACKAGE = Path(__file__).resolve().parents[1] / "private_events_mcp"


def test_mcp_package_has_no_outbound_network_or_process_execution() -> None:
    banned_import_roots = {
        "httpx",
        "requests",
        "urllib3",
        "socket",
        "subprocess",
        "telethon",
        "aiogram",
        "supabase",
        "boto3",
    }
    banned_calls = {"eval", "exec", "compile", "system", "popen", "spawn", "fork"}
    violations: list[str] = []
    for path in PACKAGE.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".", 1)[0] in banned_import_roots:
                        violations.append(f"{path.name}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.split(".", 1)[0] in banned_import_roots:
                    violations.append(f"{path.name}: from {node.module}")
            elif isinstance(node, ast.Call):
                target = node.func
                if isinstance(target, ast.Name) and target.id.casefold() in {"eval", "exec", "compile"}:
                    violations.append(f"{path.name}: call {target.id}")
                elif isinstance(target, ast.Attribute) and target.attr.casefold() in {"system", "popen", "spawn", "fork"}:
                    violations.append(f"{path.name}: call {target.attr}")
    assert not violations, violations


@pytest.mark.asyncio
async def test_event_database_adapter_rejects_write_statements(
    event_db, event_db_digest, monkeypatch
) -> None:
    reader = ReadOnlySQLite(str(event_db), query_timeout_ms=1000)
    with pytest.raises(ValueError):
        await reader.query("UPDATE event SET title='changed' WHERE id=42")
    assert hashlib.sha256(event_db.read_bytes()).hexdigest() == event_db_digest

    # Schema discovery is a sequence of small PRAGMAs. A progress handler on
    # each statement alone cannot bound their cumulative time, so the adapter
    # must also enforce the deadline between statements.
    monkeypatch.setattr(reader, "_deadline_expired", lambda _deadline: True)
    with pytest.raises(QueryBudgetExceeded, match="sqlite_schema_budget_exceeded"):
        await reader.schema()
