from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "features" / "region-talk-channel"
SCHEMA = DOCS / "publisher-profile-enrichment.schema.json"
PACKAGES = sorted(DOCS.glob("region-talk-publisher-profile-enrichment-*.json"))
EXPECTED_SOURCE_KEYS = {
    "domain:archi.ru",
    "domain:peasantstudies.ru",
    "domain:rg.ru",
}


def test_publisher_profile_packages_validate_and_resolve_evidence() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    assert len(PACKAGES) == 3
    observed_source_keys: set[str] = set()

    for path in PACKAGES:
        payload = json.loads(path.read_text(encoding="utf-8"))
        errors = sorted(
            validator.iter_errors(payload),
            key=lambda item: [str(value) for value in item.absolute_path],
        )
        assert not errors, [error.message for error in errors]
        assert payload["schema_version"] == "region_talk_publisher_profile_enrichment.v1"
        assert payload["run"]["source_count"] == len(payload["profiles"]) == 1

        profile = payload["profiles"][0]
        source_key = profile["canonical_source_key"]
        observed_source_keys.add(source_key)
        evidence_ids = {item["evidence_id"] for item in profile["evidence"]}
        assert len(evidence_ids) == len(profile["evidence"])

        referenced: set[str] = set()

        def collect(value: object) -> None:
            if isinstance(value, dict):
                for key, item in value.items():
                    if key == "evidence_refs":
                        assert isinstance(item, list)
                        referenced.update(str(ref) for ref in item)
                    else:
                        collect(item)
            elif isinstance(value, list):
                for item in value:
                    collect(item)

        collect(profile)
        for correction in payload["candidate_corrections"]:
            assert correction["linked_source_key"] == source_key
            collect(correction)
        assert referenced <= evidence_ids

    assert observed_source_keys == EXPECTED_SOURCE_KEYS


def test_publisher_packages_do_not_match_candidate_auto_import_mask() -> None:
    for path in PACKAGES:
        assert not path.name.startswith("region-talk-external-research-result-")


def test_external_prompt_requires_dossier_and_article_producer_gate() -> None:
    prompt = (DOCS / "external-publication-research.prompt.txt").read_text(
        encoding="utf-8"
    )
    for required in (
        "PUBLISHER DOSSIER — MANDATORY UPSTREAM EVIDENCE",
        "publisher.distinctive_value",
        "publisher.locality",
        "LOCAL EDITION / ARTICLE PRODUCER GATE",
        "local correspondent",
    ):
        assert required in prompt
