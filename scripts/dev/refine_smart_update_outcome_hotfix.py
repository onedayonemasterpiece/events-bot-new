#!/usr/bin/env python3
"""Finish the focused Smart Update caller-boundary patch.

This file is a one-shot connector workaround for editing very large source
files. It is deleted, together with its workflow, before the product commit.
Every edit is assertion-guarded and the workflow runs focused tests before it
can push the materialized patch.
"""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one anchor, found {count}")
    return text.replace(old, new, 1)


def refine_smart_event_update() -> None:
    path = ROOT / "smart_event_update.py"
    text = path.read_text(encoding="utf-8")

    text = replace_once(
        text,
        '''    # Evidence-only identity selected before a fail-closed result. Callers must
    # never use this field to authorize domain or publication side effects.
    matched_event_id: int | None = None


''',
        "",
        label="remove matched_event_id compatibility field",
    )

    start = text.index("def _caller_safe_smart_update_result(")
    end = text.index("\n\nclass SourceBindingConflict", start)
    text = text[:start] + text[end + 2 :]
    text = replace_once(
        text,
        "    return _caller_safe_smart_update_result(result)\n",
        "    return result\n",
        label="preserve diagnostic event_id",
    )
    path.write_text(text, encoding="utf-8")


def refine_parser_caller() -> None:
    path = ROOT / "source_parsing/handlers.py"
    text = path.read_text(encoding="utf-8")

    text = replace_once(
        text,
        "            from smart_event_update import EventCandidate, PosterCandidate, smart_event_update\n",
        '''            from smart_event_update import (
                EventCandidate,
                PosterCandidate,
                smart_event_update,
                smart_update_result_allows_caller_side_effects,
            )
''',
        label="parser smart update import",
    )

    call_at = text.index(
        "            update_result = await _smart_event_update_with_lock_retry("
    )
    event_line = "            event_id = update_result.event_id\n"
    event_at = text.index(event_line, call_at)
    gate = '''            if not smart_update_result_allows_caller_side_effects(update_result):
                status = str(getattr(update_result, "status", "") or "not_accepted")
                logger.warning(
                    "source_parsing: smart_update not accepted title=%s status=%s reason=%s matched_event_id=%s",
                    theatre_event.title[:80],
                    status,
                    getattr(update_result, "reason", None),
                    getattr(update_result, "event_id", None),
                )
                return None, False, status
'''
    text = text[:event_at] + gate + text[event_at:]
    path.write_text(text, encoding="utf-8")


def refine_vk_caller() -> None:
    path = ROOT / "vk_intake.py"
    text = path.read_text(encoding="utf-8")

    text = replace_once(
        text,
        "    from smart_event_update import EventCandidate, PosterCandidate, smart_event_update\n",
        '''    from smart_event_update import (
        EventCandidate,
        PosterCandidate,
        smart_event_update,
        smart_update_result_allows_caller_side_effects,
    )
''',
        label="VK smart update import",
    )

    call_at = text.index("    update_result = await smart_event_update(")
    status_line = (
        '    if str(getattr(update_result, "status", "") or "").startswith("rejected_"):\n'
    )
    status_at = text.index(status_line, call_at)
    gate = '''    if not smart_update_result_allows_caller_side_effects(update_result):
        raise RuntimeError(
            "smart_update not accepted: "
            f"status={getattr(update_result, 'status', None)} "
            f"reason={getattr(update_result, 'reason', None)} "
            f"matched_event_id={getattr(update_result, 'event_id', None)}"
        )
'''
    text = text[:status_at] + gate + text[status_at:]
    path.write_text(text, encoding="utf-8")


def refine_tests() -> None:
    path = ROOT / "tests/test_smart_update_outcome_boundary_hotfix.py"
    text = path.read_text(encoding="utf-8")

    text = replace_once(
        text,
        "async def test_nonaccepted_result_cannot_export_event_id_to_callers(monkeypatch) -> None:\n",
        "async def test_nonaccepted_result_is_diagnostic_only_for_callers(monkeypatch) -> None:\n",
        label="test name",
    )
    text = replace_once(
        text,
        '''    assert result.status == "review_required"
    assert result.event_id is None
    assert result.matched_event_id == 7024
    assert result.created is False
''',
        '''    assert result.status == "review_required"
    assert result.event_id == 7024
    assert result.created is False
''',
        label="nonaccepted result evidence assertion",
    )

    parser_test_at = text.index(
        "async def test_official_parser_caller_has_zero_side_effects_after_review("
    )
    draft_at = text.index("    draft = SimpleNamespace(\n", parser_test_at)
    time_line = '        time="19:00",\n'
    time_at = text.index(time_line, draft_at) + len(time_line)
    next_draft_end = text.index("    )\n", draft_at)
    if "        end_date=None,\n" not in text[draft_at:next_draft_end]:
        text = text[:time_at] + "        end_date=None,\n" + text[time_at:]

    text = replace_once(
        text,
        '    with pytest.raises(RuntimeError, match="returned no event_id"):\n',
        '    with pytest.raises(RuntimeError, match="smart_update not accepted"):\n',
        label="VK rejection message",
    )
    path.write_text(text, encoding="utf-8")


def refine_incident_doc() -> None:
    path = (
        ROOT
        / "docs/reports/incidents/INC-2026-08-04-smart-update-caller-boundary-followup.md"
    )
    text = path.read_text(encoding="utf-8")
    text = replace_once(
        text,
        '''A `not_accepted` result moves the candidate match from `event_id` to the
non-authorizing `matched_event_id`. Existing callers therefore stop before
their success path. The append-only identity decision log remains the durable
source of evidence. Unknown future statuses fail closed until explicitly added
to the contract.
''',
        '''A `not_accepted` result may retain `event_id` as diagnostic evidence, but
callers must classify the outcome before using that ID. The official parser and
VK persist paths now return or raise before ticket mutation, linked-event
recompute, Telegraph rebuild, publication scheduling or successful import
state. Unknown future statuses fail closed until explicitly added to the
contract.
''',
        label="incident outcome wording",
    )
    path.write_text(text, encoding="utf-8")


def remove_temporary_files() -> None:
    for relative in (
        "scripts/dev/refine_smart_update_outcome_hotfix.py",
        ".github/workflows/materialize-smart-update-outcome-hotfix-v2.yml",
    ):
        path = ROOT / relative
        if path.exists():
            path.unlink()


def main() -> None:
    refine_smart_event_update()
    refine_parser_caller()
    refine_vk_caller()
    refine_tests()
    refine_incident_doc()
    remove_temporary_files()


if __name__ == "__main__":
    main()
