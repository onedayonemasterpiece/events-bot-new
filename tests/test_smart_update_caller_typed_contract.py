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
ACCEPTED_OUTCOMES = {"CREATED", "MERGED", "NOOP_EXACT_REPLAY"}


def _trees():
    for relative in CALLER_FILES:
        source = (ROOT / relative).read_text(encoding="utf-8")
        yield relative, source, ast.parse(source)


def _result_attribute(node: ast.AST, result_name: str, attribute: str) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == attribute
        and isinstance(node.value, ast.Name)
        and node.value.id == result_name
    )


def _accepted_outcome_comparison(node: ast.AST, result_name: str) -> bool:
    if not isinstance(node, ast.Compare) or len(node.ops) != 1 or len(node.comparators) != 1:
        return False
    if not isinstance(node.ops[0], (ast.Is, ast.Eq)):
        return False
    left, right = node.left, node.comparators[0]
    if not _result_attribute(left, result_name, "outcome"):
        return False
    return (
        isinstance(right, ast.Attribute)
        and right.attr in ACCEPTED_OUTCOMES
        and isinstance(right.value, ast.Name)
        and right.value.id == "SmartUpdateTerminalOutcome"
    )


def _condition_proves_accepted(node: ast.AST, result_name: str, *, truth: bool) -> bool:
    if truth and _result_attribute(node, result_name, "is_accepted"):
        return True
    if (
        not truth
        and isinstance(node, ast.UnaryOp)
        and isinstance(node.op, ast.Not)
        and _result_attribute(node.operand, result_name, "is_accepted")
    ):
        return True
    if truth and _accepted_outcome_comparison(node, result_name):
        return True
    if isinstance(node, ast.BoolOp):
        # Every term of an ``and`` is true in its body. For ``or`` no one term
        # is guaranteed, so it cannot establish acceptance.
        return isinstance(node.op, ast.And) and truth and any(
            _condition_proves_accepted(value, result_name, truth=True)
            for value in node.values
        )
    return False


def _block_always_exits(statements: list[ast.stmt]) -> bool:
    if not statements:
        return False
    tail = statements[-1]
    if isinstance(tail, (ast.Return, ast.Raise, ast.Continue, ast.Break)):
        return True
    if isinstance(tail, ast.If):
        return _block_always_exits(tail.body) and _block_always_exits(tail.orelse)
    return False


def _inside_observability_call(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    current = node
    while current in parents:
        current = parents[current]
        if not isinstance(current, ast.Call):
            continue
        func = current.func
        return (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id in {"logger", "logging"}
        )
    return False


def _event_id_read_is_acceptance_dominated(
    node: ast.Attribute,
    *,
    result_name: str,
    parents: dict[ast.AST, ast.AST],
) -> bool:
    current: ast.AST = node
    while current in parents:
        parent = parents[current]
        if isinstance(parent, ast.If):
            if current in parent.body and _condition_proves_accepted(
                parent.test, result_name, truth=True
            ):
                return True
            if current in parent.orelse and _condition_proves_accepted(
                parent.test, result_name, truth=False
            ):
                return True
        # A preceding fail-closed guard such as
        # ``if not result.is_accepted: return`` dominates later statements in
        # the same lexical block.
        for field in ("body", "orelse", "finalbody"):
            statements = getattr(parent, field, None)
            if not isinstance(statements, list) or current not in statements:
                continue
            index = statements.index(current)
            for previous in statements[:index]:
                if (
                    isinstance(previous, ast.If)
                    and _condition_proves_accepted(
                        previous.test, result_name, truth=False
                    )
                    and _block_always_exits(previous.body)
                ):
                    return True
        current = parent
    return False


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


def test_every_smart_result_event_id_side_effect_is_acceptance_dominated() -> None:
    """Diagnostic IDs cannot cross any production side-effect boundary.

    This is a structural proof over every direct caller, rather than a marker
    assertion: each SmartUpdateResult.event_id read must be inside a typed
    accepted branch (or after a fail-closed non-accepted exit). Logging is an
    observer and is intentionally exempt.
    """

    violations: list[str] = []
    for relative, _source, tree in _trees():
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute) or node.attr != "event_id":
                continue
            if not isinstance(node.value, ast.Name) or node.value.id not in SMART_RESULT_NAMES:
                continue
            if _inside_observability_call(node, parents):
                continue
            if not _event_id_read_is_acceptance_dominated(
                node,
                result_name=node.value.id,
                parents=parents,
            ):
                violations.append(f"{relative}:{node.lineno}:{ast.unparse(node)}")
    assert violations == []


def test_diagnostic_event_id_cannot_be_assigned_or_passed_through() -> None:
    """A diagnostic identity is observability-only, including indirect taint.

    Rejecting the read at its source also rejects assignment, return, container
    storage and helper pass-through before a downstream side effect can hide it.
    Direct logger calls remain the sole exception.
    """

    violations: list[str] = []
    for relative, _source, tree in _trees():
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        for node in ast.walk(tree):
            direct = (
                isinstance(node, ast.Attribute)
                and node.attr == "diagnostic_event_id"
                and isinstance(node.value, ast.Name)
                and node.value.id in SMART_RESULT_NAMES
            )
            indirect = (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id in SMART_RESULT_NAMES
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == "diagnostic_event_id"
            )
            if not (direct or indirect):
                continue
            if _inside_observability_call(node, parents):
                continue
            violations.append(f"{relative}:{node.lineno}:{ast.unparse(node)}")
    assert violations == []


def test_all_direct_boundaries_use_typed_helpers_or_terminal_enum() -> None:
    expected_markers = {
        "source_parsing/handlers.py": ("update_result.is_accepted", "occurrence_key"),
        "source_parsing/telegram/handlers.py": (
            "result.is_accepted",
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
