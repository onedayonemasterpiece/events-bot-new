from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CALLER_FILES = (
    "source_parsing/handlers.py",
    "source_parsing/telegram/handlers.py",
    "vk_intake.py",
    "vk_auto_queue.py",
    "ticket_sites_queue.py",
    "festival_queue.py",
    "main.py",
    "main_part2.py",
    "scheduling.py",
)
SMART_RESULT_NAMES = {
    "result",
    "update_result",
    "smart_result",
    "context_result",
}


def _trees():
    for relative in CALLER_FILES:
        source = (ROOT / relative).read_text(encoding="utf-8")
        yield relative, source, ast.parse(source)


def test_production_smart_update_callers_do_not_read_free_form_status() -> None:
    violations: list[str] = []
    for relative, _source, tree in _trees():
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute) or node.attr != "status":
                continue
            if isinstance(node.value, ast.Name) and node.value.id in SMART_RESULT_NAMES:
                violations.append(f"{relative}:{node.lineno}:{node.value.id}.status")
    assert violations == []


def test_production_smart_update_callers_do_not_use_event_id_as_success_test() -> None:
    violations: list[str] = []
    for relative, _source, tree in _trees():
        for node in ast.walk(tree):
            if not isinstance(node, ast.If):
                continue
            # Comparisons such as `event_id is None` are validation, not a
            # success gate. Reject only bare truthiness in an if-test.
            test = node.test
            if (
                isinstance(test, ast.Attribute)
                and test.attr == "event_id"
                and isinstance(test.value, ast.Name)
                and test.value.id in SMART_RESULT_NAMES
            ):
                violations.append(f"{relative}:{node.lineno}")
    assert violations == []


def test_all_direct_boundaries_use_typed_helpers_or_terminal_enum() -> None:
    expected_markers = {
        "source_parsing/handlers.py": ("update_result.is_accepted", "occurrence_key"),
        "source_parsing/telegram/handlers.py": (
            "result.is_changed",
            "result.is_retry",
            "candidate.producer_ordinal",
        ),
        "vk_intake.py": ("update_result.is_accepted", "smart_result=update_result"),
        "vk_auto_queue.py": ("smart_result.is_accepted", "producer_ordinal"),
        "ticket_sites_queue.py": ("smart_result.is_accepted", "smart_result.is_rejected"),
        "festival_queue.py": ("result.is_accepted", "producer_ordinal"),
        "main.py": ("update_result.is_accepted", "producer_ordinal"),
        "main_part2.py": ("smart_result.is_accepted", "producer_ordinal"),
        "scheduling.py": ("retry_due_smart_update_candidates", "SMART_UPDATE_RETRY_BATCH_SIZE"),
    }
    for relative, markers in expected_markers.items():
        source = (ROOT / relative).read_text(encoding="utf-8")
        for marker in markers:
            assert marker in source, f"missing {marker!r} in {relative}"


def test_forbidden_operator_terminals_are_absent_from_caller_adapters() -> None:
    forbidden = ("review_required", "skipped_identity_gate", "skipped_context_only")
    violations: list[str] = []
    for relative, source, _tree in _trees():
        for terminal in forbidden:
            if terminal in source:
                violations.append(f"{relative}:{terminal}")
    assert violations == []
