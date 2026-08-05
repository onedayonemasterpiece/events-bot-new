"""Fail-closed inventory gate for production Smart Update callers.

A new direct caller is a correctness boundary: it must be intentionally added
here together with a typed-outcome behavior test.  Ad-hoc diagnostics under
scripts/ are intentionally outside the production inventory.
"""
from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_DIRECT_CALLERS = {
    ("festival_queue.py", "_process_vk_item"),
    ("main.py", "add_events_from_text"),
    ("main.py", "handle_add_event_raw"),
    ("source_parsing/telegram/handlers.py", "process_telegram_results"),
    ("ticket_sites_queue.py", "_smart_update_from_theatre_event"),
    ("vk_intake.py", "persist_event_and_pages"),
}


def _production_direct_callers() -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith(("tests/", "scripts/")) or "__pycache__" in rel:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        scope: list[str] = []

        class Visitor(ast.NodeVisitor):
            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                scope.append(node.name)
                self.generic_visit(node)
                scope.pop()

            visit_AsyncFunctionDef = visit_FunctionDef

            def visit_Call(self, node: ast.Call) -> None:
                name = node.func.id if isinstance(node.func, ast.Name) else ""
                if name == "smart_event_update" and scope:
                    found.add((rel, ".".join(scope)))
                self.generic_visit(node)

        Visitor().visit(tree)
    return found


def test_all_production_smart_update_callers_are_explicitly_inventoried() -> None:
    assert _production_direct_callers() == EXPECTED_DIRECT_CALLERS


def test_production_boundaries_use_typed_outcome_adapters() -> None:
    required_markers = {
        "festival_queue.py": "smart_update_result_allows_caller_side_effects",
        "main.py": "smart_update_result_allows_caller_side_effects",
        "source_parsing/telegram/handlers.py": "smart_update_result_allows_caller_side_effects",
        "ticket_sites_queue.py": "classify_smart_update_status",
        "vk_intake.py": "smart_update_result_allows_caller_side_effects",
    }
    for relative, marker in required_markers.items():
        assert marker in (ROOT / relative).read_text(encoding="utf-8"), relative
