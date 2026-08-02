#!/usr/bin/env python3
"""Offline failure/fallback drill for the facts-v3 collection stage.

The harness deliberately injects both provider boundaries.  It never reads API
keys and cannot make a network request: the primary adapter always records one
send and fails, while the existing GPT-4o fallback boundary returns a local
fixture (or a local failure).  The result still travels through the production
collection adjudicator, strict validator and apply function.

This is a deterministic code-path drill, not the real-source Gate-C replay.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import os
import shlex
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import Event, EventSource
import smart_event_update as collection_core


REPORT_SCHEMA_VERSION = "static-collection-facts-v3-fallback-drill-v1"
SCENARIOS = (
    "valid_fallback",
    "malformed_fallback",
    "evidence_mismatch",
    "total_unavailable",
)
SOURCE_TEXT = "Приходите всей семьёй."


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _repo_sha() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _unknown_fact() -> dict[str, Any]:
    return {
        "value": "unknown",
        "confidence": 0.0,
        "evidence_quote": "",
        "reason_code": "insufficient_evidence",
    }


def _fallback_payload(*, quote: str = SOURCE_TEXT) -> dict[str, Any]:
    return {
        "schema_version": collection_core.STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "admission_decision": {
            "value": "unknown",
            "evidence_quote": "",
            "reason_code": "insufficient_evidence",
        },
        "child_directed_decision": _unknown_fact(),
        "family_suitable_decision": {
            "value": "confirmed",
            "confidence": 0.95,
            "evidence_quote": quote,
            "reason_code": "explicit_family_invitation",
        },
        "joint_family_activity_decision": _unknown_fact(),
        "people_appearances": [],
    }


def _existing_truth() -> dict[str, Any]:
    return {
        "schema_version": collection_core.STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "child_directed_decision": {
            "value": "confirmed",
            "confidence": 0.91,
            "evidence_quote": "Для детей",
            "reason_code": "explicit_child_audience",
            "source_id": 7,
            "source_url": "https://example.test/accepted",
            "source_type": "telegram",
            "source_trust": "official",
            "input_hash": "accepted-truth-input-hash",
            "policy_version": collection_core.STATIC_COLLECTION_FACTS_POLICY_VERSION,
            "decided_at": "2026-08-01T00:00:00Z",
            "manual_lock": False,
        },
    }


class _ForcedPrimaryFailure:
    def __init__(self) -> None:
        self.send_count = 0

    async def generate_content_async(self, **kwargs: Any) -> tuple[str, Any]:
        self.send_count += 1
        observer = kwargs.get("attempt_observer")
        if callable(observer):
            observer(
                {
                    "attempt_no": self.send_count,
                    "requested_model": kwargs.get("model"),
                    "provider_model_name": "models/gemma-4-31b-it",
                }
            )
        raise RuntimeError("offline drill: forced primary failure")


class _InjectedFallback:
    def __init__(self, scenario: str) -> None:
        self.scenario = scenario
        self.send_count = 0

    async def ask_4o(self, *_args: Any, **_kwargs: Any) -> str:
        self.send_count += 1
        if self.send_count > 1:
            raise AssertionError("offline drill attempted more than one GPT-4o fallback send")
        if self.scenario == "valid_fallback":
            return json.dumps(_fallback_payload(), ensure_ascii=False)
        if self.scenario == "malformed_fallback":
            return '{"schema_version":'
        if self.scenario == "evidence_mismatch":
            return json.dumps(
                _fallback_payload(quote="Приглашаем всю семью."),
                ensure_ascii=False,
            )
        if self.scenario == "total_unavailable":
            raise RuntimeError("offline drill: forced GPT-4o unavailability")
        raise AssertionError(f"unsupported scenario: {self.scenario}")


@contextmanager
def _injected_provider_boundaries(
    primary: _ForcedPrimaryFailure,
    fallback: _InjectedFallback,
) -> Iterator[None]:
    """Replace both provider boundaries and restore all process state."""

    old_get_client = collection_core._get_gemma_client
    old_disabled = collection_core.SMART_UPDATE_LLM_DISABLED
    old_model = collection_core.SMART_UPDATE_MODEL
    old_force_staged = collection_core.SMART_UPDATE_FORCE_STAGED_GEMINI
    old_main = sys.modules.get("main")
    env_names = (
        "SMART_UPDATE_4O_FALLBACK",
        "SMART_UPDATE_4O_FALLBACK_MAX_PER_HOUR",
        "SMART_UPDATE_GEMMA_RETRIES",
    )
    old_env = {name: os.environ.get(name) for name in env_names}

    injected_main = ModuleType("main")
    injected_main.ask_4o = fallback.ask_4o  # type: ignore[attr-defined]

    async def _notify_llm_incident(*_args: Any, **_kwargs: Any) -> None:
        return None

    injected_main.notify_llm_incident = _notify_llm_incident  # type: ignore[attr-defined]
    try:
        collection_core._get_gemma_client = lambda: primary
        collection_core.SMART_UPDATE_LLM_DISABLED = False
        collection_core.SMART_UPDATE_MODEL = "gemma-4-31b-it"
        collection_core.SMART_UPDATE_FORCE_STAGED_GEMINI = False
        os.environ["SMART_UPDATE_4O_FALLBACK"] = "1"
        os.environ.pop("SMART_UPDATE_4O_FALLBACK_MAX_PER_HOUR", None)
        os.environ["SMART_UPDATE_GEMMA_RETRIES"] = "1"
        sys.modules["main"] = injected_main
        yield
    finally:
        collection_core._get_gemma_client = old_get_client
        collection_core.SMART_UPDATE_LLM_DISABLED = old_disabled
        collection_core.SMART_UPDATE_MODEL = old_model
        collection_core.SMART_UPDATE_FORCE_STAGED_GEMINI = old_force_staged
        if old_main is None:
            sys.modules.pop("main", None)
        else:
            sys.modules["main"] = old_main
        for name, value in old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _event_and_source() -> tuple[Event, EventSource]:
    event = Event(
        id=100,
        title="Offline facts-v3 drill",
        description="Offline facts-v3 drill",
        date="2026-08-10",
        time="18:00",
        location_name="Test venue",
        source_text=SOURCE_TEXT,
        source_url="https://example.test/drill/100",
        collection_decisions=_existing_truth(),
    )
    source = EventSource(
        id=700,
        event_id=100,
        source_type="telegram",
        source_url="https://example.test/drill/100",
        source_text=SOURCE_TEXT,
        trust_level="official",
    )
    return event, source


async def run_scenario(scenario: str) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario: {scenario}")

    primary = _ForcedPrimaryFailure()
    fallback = _InjectedFallback(scenario)
    event, source = _event_and_source()
    before = copy.deepcopy(event.collection_decisions)
    candidate = collection_core.EventCandidate(
        source_type=source.source_type,
        source_url=source.source_url,
        source_text=source.source_text or "",
        title=event.title,
        date=event.date,
        time=event.time,
        location_name=event.location_name,
        trust_level=source.trust_level or "medium",
        collection_adjudication_reasons=["audience"],
    )
    input_hash = collection_core.collection_adjudication_input_hash(candidate)

    collection_core.reset_smart_update_llm_trace()
    with _injected_provider_boundaries(primary, fallback):
        validated = await collection_core.adjudicate_collection_candidate(candidate)
    trace_rows = collection_core.get_smart_update_llm_trace()
    trace = trace_rows[-1] if trace_rows else {}

    apply_attempted = validated is not None
    applied = False
    if apply_attempted:
        applied = collection_core.apply_collection_decisions(
            event,
            validated,
            source=source,
            source_corpus=source.source_text or "",
            input_hash=input_hash,
            decided_at=datetime(2026, 8, 2, tzinfo=timezone.utc),
            reasons={"audience"},
        )

    after = copy.deepcopy(event.collection_decisions)
    before_child = (before or {}).get("child_directed_decision")
    after_child = (after or {}).get("child_directed_decision")
    truth_preserved = before_child == after_child
    unchanged = before == after
    actual_models = [str(value) for value in trace.get("actual_models", [])]
    physical_sends = int(trace.get("physical_sends") or 0)
    common_ok = (
        primary.send_count == 1
        and fallback.send_count <= 1
        and physical_sends == primary.send_count + fallback.send_count
        and actual_models[:1] == ["models/gemma-4-31b-it"]
        and (fallback.send_count == 0 or actual_models[-1:] == ["gpt-4o"])
        and truth_preserved
    )
    if scenario == "valid_fallback":
        scenario_ok = validated is not None and apply_attempted and applied
        validator_result = "accepted"
    elif scenario in {"malformed_fallback", "evidence_mismatch"}:
        scenario_ok = validated is None and not apply_attempted and not applied and unchanged
        validator_result = "rejected"
    else:
        scenario_ok = validated is None and not apply_attempted and not applied and unchanged
        validator_result = "abstained"

    return {
        "scenario": scenario,
        "status": "pass" if common_ok and scenario_ok else "fail",
        "input_hash": input_hash,
        "source_text_sha256": hashlib.sha256(SOURCE_TEXT.encode("utf-8")).hexdigest(),
        "provider": {
            "primary": {
                "requested_model": "gemma-4-31b-it",
                "actual_model": actual_models[0] if actual_models else None,
                "forced_failure": True,
                "physical_sends": primary.send_count,
            },
            "fallback": {
                "provider": "existing_ask_4o_boundary",
                "actual_model": "gpt-4o" if fallback.send_count else None,
                "behavior": scenario,
                "physical_sends": fallback.send_count,
            },
            "trace": {
                "label": trace.get("label"),
                "status": trace.get("status"),
                "physical_sends": physical_sends,
                "actual_models": actual_models,
                "provider_errors": int(trace.get("provider_errors") or 0),
                "rate_limit_waits": int(trace.get("rate_limit_waits") or 0),
            },
        },
        "validator_result": validator_result,
        "apply_attempted": apply_attempted,
        "applied": applied,
        "existing_truth_preserved": truth_preserved,
        "whole_collection_unchanged": unchanged,
        "before_sha256": _sha256(before),
        "after_sha256": _sha256(after),
    }


async def run_drill(
    scenarios: tuple[str, ...] = SCENARIOS,
    *,
    generated_at: datetime | None = None,
    generator_command: str | None = None,
) -> dict[str, Any]:
    cases = [await run_scenario(scenario) for scenario in scenarios]
    passed = sum(case["status"] == "pass" for case in cases)
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "repo_sha": _repo_sha(),
        "generated_at": _stamp(generated_at or datetime.now(timezone.utc)),
        "generator_command": generator_command or shlex.join(sys.argv),
        "transport_mode": "offline_injected_no_network",
        "facts_policy_version": collection_core.STATIC_COLLECTION_FACTS_POLICY_VERSION,
        "adjudication_schema_version": collection_core.STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "status": "pass" if passed == len(cases) else "fail",
        "summary": {
            "cases": len(cases),
            "passed": passed,
            "failed": len(cases) - passed,
            "real_provider_calls": 0,
            "maximum_primary_sends_per_case": max(
                (case["provider"]["primary"]["physical_sends"] for case in cases),
                default=0,
            ),
            "maximum_fallback_sends_per_case": max(
                (case["provider"]["fallback"]["physical_sends"] for case in cases),
                default=0,
            ),
        },
        "cases": cases,
        "gate_claim": "offline_harness_only_real_gate_c_not_claimed",
        "publication_status": "blocked",
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=SCENARIOS,
        help="Run only this scenario; repeat for more than one (default: all).",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--generated-at",
        help="Optional fixed RFC3339 timestamp for byte-reproducible test reports.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    generated_at = None
    if args.generated_at:
        generated_at = datetime.fromisoformat(args.generated_at.replace("Z", "+00:00"))
        if generated_at.tzinfo is None:
            raise SystemExit("--generated-at must include a timezone")
    command = shlex.join([sys.executable, str(Path(__file__).relative_to(ROOT)), *(argv or sys.argv[1:])])
    report = asyncio.run(
        run_drill(
            tuple(args.scenario or SCENARIOS),
            generated_at=generated_at,
            generator_command=command,
        )
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(_canonical_json(report) + b"\n")
    print(json.dumps(report["summary"], ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
