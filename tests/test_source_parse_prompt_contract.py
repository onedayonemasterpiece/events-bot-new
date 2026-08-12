from __future__ import annotations

from pathlib import Path
import shutil

import pytest

from scripts.inspect.audit_source_parse_prompt_contract import find_violations


ROOT = Path(__file__).resolve().parents[1]


def _prompt_fixture(tmp_path: Path) -> Path:
    for relative in (
        "docs/llm/prompts.md",
        "vk_intake.py",
        "kaggle/TelegramMonitor/telegram_monitor.py",
        "source_parse_contract.py",
        "main.py",
    ):
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)
    return tmp_path


def test_live_source_parse_prompts_pass_contract_gate() -> None:
    assert find_violations(ROOT) == []


@pytest.mark.parametrize(
    ("relative", "anchor", "legacy_instruction"),
    [
        (
            "docs/llm/prompts.md",
            "MASTER-PROMPT for Codex",
            "If unsure, return ` [ ] `.",
        ),
        (
            "vk_intake.py",
            "Обязательный source-level verdict:",
            " При сомнении верните пустой   массив. ",
        ),
        (
            "kaggle/TelegramMonitor/telegram_monitor.py",
            "Return one strict SourceParseDecision JSON object and no prose.",
            "Fail CLOSED ( [ ] ).",
        ),
    ],
)
def test_gate_mutation_catches_normalised_legacy_empty_markers(
    tmp_path: Path,
    relative: str,
    anchor: str,
    legacy_instruction: str,
) -> None:
    root = _prompt_fixture(tmp_path)
    path = root / relative
    source = path.read_text(encoding="utf-8")
    path.write_text(source.replace(anchor, anchor + legacy_instruction, 1), encoding="utf-8")
    violations = find_violations(root)
    assert any("legacy carrier-empty instruction" in item for item in violations)


def test_legacy_tg_extract_events_text_is_excluded_while_unreachable(tmp_path: Path) -> None:
    root = _prompt_fixture(tmp_path)
    path = root / "kaggle/TelegramMonitor/telegram_monitor.py"
    source = path.read_text(encoding="utf-8")
    anchor = "async def extract_events("
    source = source.replace(anchor, "# return [] is legacy-only\n" + anchor, 1)
    path.write_text(source, encoding="utf-8")
    assert find_violations(root) == []


def test_gate_fails_if_legacy_tg_extractor_becomes_reachable(tmp_path: Path) -> None:
    root = _prompt_fixture(tmp_path)
    path = root / "kaggle/TelegramMonitor/telegram_monitor.py"
    source = path.read_text(encoding="utf-8")
    source += "\nasync def _mutation_reactivates_legacy():\n    return await extract_events('x')\n"
    path.write_text(source, encoding="utf-8")
    violations = find_violations(root)
    assert any("became reachable" in item for item in violations)


def test_n9_gate_fails_if_mandatory_no_event_reason_instruction_is_removed(tmp_path: Path) -> None:
    root = _prompt_fixture(tmp_path)
    path = root / "docs/llm/prompts.md"
    source = path.read_text(encoding="utf-8")
    source = source.replace("`no_event_reason` is mandatory", "`no_event_reason` is optional", 1)
    path.write_text(source, encoding="utf-8")
    assert any("not explicitly mandatory" in item for item in find_violations(root))


def test_n9_gate_fails_on_shared_no_event_enum_drift(tmp_path: Path) -> None:
    root = _prompt_fixture(tmp_path)
    path = root / "source_parse_contract.py"
    source = path.read_text(encoding="utf-8").replace(
        'OUT_OF_SCOPE = "OUT_OF_SCOPE"',
        'OUT_OF_SCOPE = "OUT_OF_SCOPE"\n    UNKNOWN = "UNKNOWN"',
        1,
    )
    path.write_text(source, encoding="utf-8")
    assert any("enum drift" in item for item in find_violations(root))
