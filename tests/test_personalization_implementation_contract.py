from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DOC_ROOT = ROOT / "docs" / "features" / "static-site-pages" / "personalizaion"
SCHEMA_ROOT = DOC_ROOT / "schemas"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    assert isinstance(value, dict), f"{path} must contain a JSON object"
    return value


def property_names(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        properties = value.get("properties")
        if isinstance(properties, dict):
            yield from properties
        for child in value.values():
            yield from property_names(child)
    elif isinstance(value, list):
        for child in value:
            yield from property_names(child)


def test_personalization_document_index_is_complete() -> None:
    required = [
        DOC_ROOT / "README.md",
        DOC_ROOT / "requirements.md",
        DOC_ROOT / "personalization-to-be.md",
        DOC_ROOT / "personalization-research-traceability.md",
        DOC_ROOT / "personalization-implementation-contract.md",
        DOC_ROOT / "personalization-current-runtime-audit-2026-08-02.md",
        DOC_ROOT / "implementation-status.yml",
        DOC_ROOT / "tasks" / "personalization-wave-0.md",
        DOC_ROOT / "collection-surfaces-v1.example.json",
        SCHEMA_ROOT / "personalization-browser-state-v1.schema.json",
        SCHEMA_ROOT / "personalization-action-batch-v1.schema.json",
        SCHEMA_ROOT / "personalization-profile-projection-v1.schema.json",
    ]
    missing = [str(path.relative_to(ROOT)) for path in required if not path.is_file()]
    assert not missing, f"personalization implementation package is incomplete: {missing}"


def test_target_research_precedence_and_legacy_quarantine_are_explicit() -> None:
    index = (DOC_ROOT / "README.md").read_text(encoding="utf-8")
    traceability = (DOC_ROOT / "personalization-research-traceability.md").read_text(encoding="utf-8")
    wave_zero = (DOC_ROOT / "tasks" / "personalization-wave-0.md").read_text(encoding="utf-8")
    compact_wave_zero = " ".join(wave_zero.split())

    assert index.index("personalization-to-be.md") < index.index("personalization-implementation-contract.md")
    assert "не может менять продуктовый или модельный смысл" in index
    assert "Если целевой документ оставляет вопрос открытым" in index
    assert "legacy/profile-v1.ts" in index
    assert "legacy/scorer-v1.ts" in index

    required_research = [
        "Golden personas — soft mixture",
        "Session/short/mid/long horizons",
        "Rescue не более 10%",
        "Interest percentage UI",
        "Campaign/easter-egg interactions",
        "Variants control/facets/hard-persona/soft-persona/hybrid",
        "NDCG/MRR/coverage/diversity/novelty/worst-group/latency",
        "A/B заранее регистрирует",
        "Путь `legacy code → inferred product truth` запрещён",
    ]
    missing = [fragment for fragment in required_research if fragment not in traceability]
    assert not missing, f"fresh personalization research lost from traceability: {missing}"

    assert "site/src/lib/personalization/legacy/scorer-v1.ts" in wave_zero
    assert "site/src/lib/personalization/scorer.ts" in wave_zero
    assert "В Wave 0 **не создавать**" in wave_zero
    assert "переименование `legacy/scorer-v1.ts` в `scorer.ts`" in compact_wave_zero
    assert "`not target quality`" in wave_zero
    assert "research-delta review" in wave_zero
    assert "не выводи продуктовую истину из EventLayout" in compact_wave_zero


def test_browser_state_schema_keeps_one_compact_bounded_state() -> None:
    schema = load_json(SCHEMA_ROOT / "personalization-browser-state-v1.schema.json")
    assert schema["properties"]["schema_version"]["const"] == "personalization-browser-state-v1"
    assert schema["properties"]["explicit"]["maxItems"] <= 384
    assert schema["properties"]["overlay"]["maxItems"] <= 64

    projection = schema["$defs"]["projection"]
    assert projection["properties"]["sensitive_facets_present"]["const"] is False
    assert projection["properties"]["horizons"]["additionalProperties"] is False

    forbidden = {
        "anon_id",
        "email",
        "raw_event_log",
        "raw_history",
        "session_id",
        "subject_id",
        "token",
    }
    present = forbidden.intersection(property_names(schema))
    assert not present, f"browser state schema exposes forbidden durable fields: {sorted(present)}"


def test_action_batch_is_bounded_idempotent_and_does_not_trust_subject_fields() -> None:
    schema = load_json(SCHEMA_ROOT / "personalization-action-batch-v1.schema.json")
    assert schema["properties"]["schema_version"]["const"] == "personalization-action-batch-v1"
    assert schema["properties"]["actions"]["maxItems"] == 16

    action = schema["$defs"]["action"]
    assert {"id", "seq", "type", "surface", "occurred_at"}.issubset(action["required"])
    assert action["properties"]["id"]["$ref"] == "#/$defs/uuid"
    assert action["properties"]["seq"]["minimum"] == 1

    forbidden = {
        "anon_id",
        "bearer_token",
        "email",
        "full_profile",
        "profile",
        "session_id",
        "subject_id",
        "user_id",
    }
    present = forbidden.intersection(property_names(schema))
    assert not present, f"action payload trusts or duplicates forbidden identity/profile fields: {sorted(present)}"


def test_profile_projection_is_sparse_versioned_and_sensitive_fail_closed() -> None:
    schema = load_json(SCHEMA_ROOT / "personalization-profile-projection-v1.schema.json")
    required = set(schema["required"])
    assert {
        "revision",
        "etag",
        "compatibility",
        "horizons",
        "explicit_authoritative",
        "sensitive_facets_present",
    }.issubset(required)
    assert schema["properties"]["sensitive_facets_present"]["const"] is False
    assert schema["$defs"]["horizon"]["properties"]["facets"]["maxItems"] <= 48
    assert schema["properties"]["explicit_authoritative"]["properties"]["states"]["maxItems"] <= 384

    forbidden = {"dense_embedding", "raw_actions", "raw_history", "raw_telemetry"}
    present = forbidden.intersection(property_names(schema))
    assert not present, f"profile projection contains unbounded/raw fields: {sorted(present)}"


def test_surface_registry_fails_static_and_never_reranks_calendar_primary() -> None:
    registry = load_json(DOC_ROOT / "collection-surfaces-v1.example.json")
    policies = registry["policies"]
    assert registry["default_policy"] in policies

    unknown = policies[registry["default_policy"]]
    assert unknown["ranking_mode"] == "identity"
    assert unknown["profile_strength_bp"] == 0
    assert unknown["reorder_scope"] == "none"
    assert unknown["network_on_page_view"] is False
    assert unknown["signal_collection"] == "none"

    calendar = policies["calendar-exact-only"]
    assert calendar["ranking_mode"] == "chronological"
    assert calendar["profile_strength_bp"] == 0
    assert calendar["reorder_scope"] == "none"
    assert calendar["exact_hide"] == "global"

    assert all(policy["network_on_page_view"] is False for policy in policies.values())

    route_families = registry["route_families"]
    assert route_families
    assert all(item["policy"] in policies for item in route_families)

    route_policy = {item["id"]: item["policy"] for item in route_families}
    assert route_policy["today-primary"] == "calendar-exact-only"
    assert route_policy["tomorrow-primary"] == "calendar-exact-only"
    assert route_policy["weekend-primary"] == "calendar-exact-only"
    assert route_policy["date-page-personal-tail"] == "calendar-personal-tail"


def test_normative_contract_keeps_storage_and_transport_hard_gates() -> None:
    text = (DOC_ROOT / "personalization-implementation-contract.md").read_text(encoding="utf-8")
    required_fragments = [
        "steady-state target | `<= 24 KiB`",
        "aggregate emergency ceiling",
        "POST /api/personalization/v1/actions:batch",
        "Browser durable profile/actions идут через same-origin API",
        "No per-impression rows",
        "Calendar primary lists — exact-hide-only",
        "direct Supabase path unavailable, Yandex relay path healthy",
        "Yandex relay unavailable, direct path healthy",
        "оба network paths недоступны",
        "Hard NO-GO",
    ]
    missing = [fragment for fragment in required_fragments if fragment not in text]
    assert not missing, f"normative personalization gates were weakened or removed: {missing}"


def test_wave_zero_forbids_remote_or_product_behavior_changes() -> None:
    text = (DOC_ROOT / "tasks" / "personalization-wave-0.md").read_text(encoding="utf-8")
    assert "без изменения пользовательского поведения" in text
    assert "без remote writes" in text
    assert "DB migrations/RLS/RPC" in text
    assert "unknown surface → static/no-signal" in text
    assert "production behavior change=0" in text
    assert "target scorer/model weights" in text
    assert "legacy_policy_promoted_to_target = 0" in text


def run_contract_tests() -> list[str]:
    tests = sorted(
        (name, value)
        for name, value in globals().items()
        if name.startswith("test_") and callable(value)
    )
    for _, test_function in tests:
        test_function()
    return [name for name, _ in tests]


if __name__ == "__main__":
    completed = run_contract_tests()
    print(json.dumps({"status": "PASS", "tests": completed}, ensure_ascii=False))
