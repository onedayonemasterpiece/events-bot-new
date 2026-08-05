#!/usr/bin/env python3
"""One-shot final review corrections for PR #336; deleted before commit."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one anchor, found {count}")
    return text.replace(old, new, 1)


def main() -> None:
    smart_path = ROOT / "smart_event_update.py"
    smart = smart_path.read_text(encoding="utf-8")
    smart = replace_once(
        smart,
        "    queue_notes: list[str] = field(default_factory=list)\nclass SmartUpdateOutcomeKind",
        "    queue_notes: list[str] = field(default_factory=list)\n\n\nclass SmartUpdateOutcomeKind",
        label="SmartUpdateResult spacing",
    )
    smart_path.write_text(smart, encoding="utf-8")

    identity_path = ROOT / "smart_update_identity.py"
    identity = identity_path.read_text(encoding="utf-8")
    identity = replace_once(
        identity,
        '''        candidate_ticket_identity is not None
        and existing_ticket_identity is not None
        and candidate_ticket_identity != existing_ticket_identity
''',
        '''        candidate_ticket_identity is not None
        and existing_ticket_identity is not None
        # Cross-vendor ticket IDs may still describe one event; the LLM remains
        # authoritative there. Only contradictory occurrence identities issued
        # by the same vendor are a deterministic impossibility rail.
        and candidate_ticket_identity[0] == existing_ticket_identity[0]
        and candidate_ticket_identity != existing_ticket_identity
''',
        label="same-vendor ticket rail",
    )
    identity_path.write_text(identity, encoding="utf-8")

    vk_path = ROOT / "vk_intake.py"
    vk = vk_path.read_text(encoding="utf-8")
    old_block = '''    if not smart_update_result_allows_caller_side_effects(update_result):
        raise RuntimeError(
            "smart_update not accepted: "
            f"status={getattr(update_result, 'status', None)} "
            f"reason={getattr(update_result, 'reason', None)} "
            f"matched_event_id={getattr(update_result, 'event_id', None)}"
        )
    if str(getattr(update_result, "status", "") or "").startswith("rejected_"):
        raise RuntimeError(
            f"smart_update rejected: {getattr(update_result, 'status', None)} "
            f"reason={getattr(update_result, 'reason', None)}"
        )
'''
    new_block = '''    if str(getattr(update_result, "status", "") or "").startswith("rejected_"):
        # Preserve the established expected-rejection contract consumed by the
        # VK queue. Identity review/skip outcomes use the generic fail-closed
        # branch below and must never be reported as successful imports.
        raise RuntimeError(
            f"smart_update rejected: {getattr(update_result, 'status', None)} "
            f"reason={getattr(update_result, 'reason', None)}"
        )
    if not smart_update_result_allows_caller_side_effects(update_result):
        raise RuntimeError(
            "smart_update not accepted: "
            f"status={getattr(update_result, 'status', None)} "
            f"reason={getattr(update_result, 'reason', None)} "
            f"matched_event_id={getattr(update_result, 'event_id', None)}"
        )
'''
    vk = replace_once(vk, old_block, new_block, label="VK rejection ordering")
    vk_path.write_text(vk, encoding="utf-8")

    tests_path = ROOT / "tests/test_smart_update_outcome_boundary_hotfix.py"
    tests = tests_path.read_text(encoding="utf-8")
    addition = '''


def test_cross_vendor_specific_ticket_ids_remain_llm_first() -> None:
    verdict = identity.build_merge_identity_gate_verdict(
        {
            "title": "Один концерт",
            "date": "2026-09-10",
            "time": "19:00",
            "event_type": "концерт",
            "ticket_link": "https://tickets-a.example/buy/event/100/2026-09-10/19:00:00",
        },
        {
            "id": 100,
            "title": "Один концерт",
            "date": "2026-09-10",
            "time": "19:00",
            "event_type": "концерт",
            "ticket_link": "https://tickets-b.example/buy/event/900/2026-09-10/19:00:00",
        },
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.99,
            "reason_code": "same_event_two_vendors",
        },
    )

    assert verdict.action is MergeIdentityAction.ALLOW_MERGE
    assert not verdict.should_skip_side_effects
'''
    if "test_cross_vendor_specific_ticket_ids_remain_llm_first" in tests:
        raise RuntimeError("cross-vendor test already exists")
    tests_path.write_text(
        tests.rstrip() + addition.rstrip() + "\n",
        encoding="utf-8",
    )

    for relative in (
        "scripts/dev/finalize_smart_update_outcome_hotfix.py",
        ".github/workflows/finalize-smart-update-outcome-hotfix.yml",
    ):
        target = ROOT / relative
        if target.exists():
            target.unlink()


if __name__ == "__main__":
    main()
