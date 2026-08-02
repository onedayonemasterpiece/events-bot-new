#!/usr/bin/env python3
"""Guarded, dry-run-first importer for Region Talk publisher dossiers.

This importer never publishes, promotes, regenerates, or mutates an external
publication candidate.  Full reusable publisher profiles and candidate-level
correction requests are separate durable records committed atomically with an
exact-byte request receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_external_publication_import import (  # noqa: E402
    ContractError as ExternalImportContractError,
    canonical_url_identity,
    canonicalize_http_url,
    stable_hash,
)
from scripts.region_talk_goal_notify import (  # noqa: E402
    ensure_ydb_module,
    load_env,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_table_path,
)
from scripts.region_talk_publisher_profile import (  # noqa: E402
    PublisherProfileConflict,
    canonical_json_sha256,
    canonical_publisher_domain,
    merge_publisher_profile_rows,
    publisher_evidence_fingerprint,
    publisher_profile_id,
    runtime_publisher_source_key,
)


SCHEMA_VERSION = "region_talk_publisher_profile_enrichment.v1"
IMPORT_VERSION = "region_talk_publisher_profile_import.v1"
SCHEMA_PATH = ROOT / "docs" / "features" / "region-talk-channel" / "publisher-profile-enrichment.schema.json"
RG_CORRECTION_URL = (
    "https://rg.ru/2025/09/16/reg-szfo/"
    "kak-segodnia-vosstanavlivaiut-istoricheskie-doma-i-pamiatniki-v-rossijskom-eksklave.html"
)


class ContractError(ValueError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _canonical_url(value: Any) -> str:
    try:
        return canonicalize_http_url(value)
    except ExternalImportContractError as exc:
        raise ContractError(str(exc)) from exc


def _parse_exact_json(raw: bytes) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ContractError("input must be exact UTF-8 JSON bytes") from exc

    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in pairs:
            if key in out:
                raise ContractError(f"duplicate JSON object key: {key}")
            out[key] = value
        return out

    try:
        parsed = json.loads(text, object_pairs_hook=no_duplicates)
    except json.JSONDecodeError as exc:
        raise ContractError(f"invalid JSON bytes: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise ContractError("top-level JSON must be an object")
    return parsed


def _schema_validate(payload: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(payload),
        key=lambda item: [str(value) for value in item.absolute_path],
    )
    if errors:
        rendered = []
        for error in errors:
            path = ".".join(str(value) for value in error.absolute_path) or "$"
            rendered.append(f"schema {path}: {error.message}")
        raise ContractError("; ".join(rendered))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ContractError(f"schema_version must be {SCHEMA_VERSION}")


def _collect_refs(value: Any) -> list[str]:
    refs: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "evidence_refs" and isinstance(item, list):
                refs.extend(str(ref) for ref in item)
            elif key != "evidence":
                refs.extend(_collect_refs(item))
    elif isinstance(value, list):
        for item in value:
            refs.extend(_collect_refs(item))
    return refs


def _semantic_validate(payload: dict[str, Any]) -> None:
    run = payload["run"]
    profiles = payload["profiles"]
    corrections = payload["candidate_corrections"]
    if int(run.get("source_count") or -1) != len(profiles):
        raise ContractError("run.source_count must equal profiles length")
    if int(run.get("linked_candidate_count") or -1) != len(corrections):
        raise ContractError("run.linked_candidate_count must equal candidate_corrections length")

    profile_by_input_key: dict[str, dict[str, Any]] = {}
    for index, profile in enumerate(profiles):
        try:
            domain = canonical_publisher_domain(profile.get("source_domain"))
        except PublisherProfileConflict as exc:
            raise ContractError(f"profiles[{index}].source_domain: {exc}") from exc
        input_key = str(profile.get("canonical_source_key") or "")
        expected_input_key = "domain:" + domain
        if input_key != expected_input_key:
            raise ContractError(
                f"profiles[{index}].canonical_source_key does not match source_domain"
            )
        if str(profile.get("source_domain") or "") != domain:
            raise ContractError(f"profiles[{index}].source_domain must already be canonical")
        canonical_url = _canonical_url(profile.get("canonical_url"))
        url_domain = (urlsplit(canonical_url).hostname or "").removeprefix("www.")
        if canonical_publisher_domain(url_domain) != domain:
            raise ContractError(f"profiles[{index}].canonical_url does not match source_domain")
        if input_key in profile_by_input_key:
            raise ContractError(f"duplicate publisher profile source key: {input_key}")
        profile_by_input_key[input_key] = profile
        evidence = profile.get("evidence") or []
        evidence_ids = [str(row.get("evidence_id") or "") for row in evidence if isinstance(row, dict)]
        if len(set(evidence_ids)) != len(evidence_ids):
            raise ContractError(f"profiles[{index}] contains duplicate evidence_id")
        missing = sorted(set(_collect_refs(profile)) - set(evidence_ids))
        if missing:
            raise ContractError(f"profiles[{index}] unresolved evidence refs: {','.join(missing)}")

    correction_urls: set[str] = set()
    for index, correction in enumerate(corrections):
        input_key = str(correction.get("linked_source_key") or "")
        profile = profile_by_input_key.get(input_key)
        if profile is None:
            raise ContractError(f"candidate_corrections[{index}] links an unknown publisher profile")
        url = _canonical_url(correction.get("canonical_url"))
        if url in correction_urls:
            raise ContractError(f"duplicate candidate correction URL: {url}")
        correction_urls.add(url)
        evidence_ids = {str(row.get("evidence_id") or "") for row in profile.get("evidence") or []}
        missing = sorted(set(correction.get("evidence_refs") or []) - evidence_ids)
        if missing:
            raise ContractError(f"candidate_corrections[{index}] unresolved evidence refs: {','.join(missing)}")
        if correction.get("requires_live_ydb_revalidation") is not True:
            raise ContractError(f"candidate_corrections[{index}] must require live YDB revalidation")

        if canonical_url_identity(url) == canonical_url_identity(RG_CORRECTION_URL):
            required_codes = {
                "regional_local_edition", "local_correspondent", "federal_brand_not_sufficient",
            }
            if (
                input_key != "domain:rg.ru"
                or correction.get("recommended_action") != "re_adjudicate_externality"
                or not required_codes.issubset(set(correction.get("reason_codes") or []))
                or profile.get("scope") != "mixed"
                or profile.get("profile_status") != "needs_review"
                or profile.get("copy_projection", {}).get("public_copy_eligibility") != "candidate_specific_review"
            ):
                raise ContractError("RG exact-article correction must remain mixed and fail closed")


def _profile_row(
    profile: dict[str, Any],
    *,
    request_id: str,
    input_sha256: str,
    imported_at: str,
) -> dict[str, Any]:
    domain = canonical_publisher_domain(profile["source_domain"])
    runtime_key = runtime_publisher_source_key(domain)
    profile_id = publisher_profile_id(runtime_key)
    evidence = deepcopy(profile.get("evidence") or [])
    profile_hash = canonical_json_sha256(profile)
    evidence_fingerprint = publisher_evidence_fingerprint(evidence)
    copy_projection = deepcopy(profile.get("copy_projection") or {})
    public_copy_eligibility = str(copy_projection.get("public_copy_eligibility") or "blocked")
    usable = bool(
        profile.get("profile_status") == "ready"
        and profile.get("scope") == "external"
        and public_copy_eligibility == "allowed"
    )
    row = {
        "pk": "publisher_profile_item:" + profile_id,
        "publisher_profile_id": profile_id,
        "canonical_source_key": runtime_key,
        "input_canonical_source_key": profile["canonical_source_key"],
        "source_domain": domain,
        "source_name": profile["source_name"],
        "canonical_url": _canonical_url(profile["canonical_url"]),
        "entity_type": profile["entity_type"],
        "scope": profile["scope"],
        "profile_status": profile["profile_status"],
        "profile_origin": "publisher_profile_sidecar",
        "profile_payload": deepcopy(profile),
        "profile_hash": profile_hash,
        "profile_json_sha256": profile_hash,
        "profile_payload_sha256": profile_hash,
        "profile_hashes": [profile_hash],
        "evidence": evidence,
        "evidence_fingerprint": evidence_fingerprint,
        "evidence_json_sha256": canonical_json_sha256(evidence),
        "evidence_fingerprints": [evidence_fingerprint],
        "evidence_item_hashes": sorted(canonical_json_sha256(row) for row in evidence),
        "profile_dimensions": {
            "outlet_identity": profile["identity_summary"],
            "intended_audience": deepcopy(profile["intended_audiences"]),
            "distinctive_value": deepcopy(profile["distinctive_value"]),
            "editorial_scope": deepcopy(profile["editorial_subjects"]),
            "recurring_formats": deepcopy(profile["recurring_formats"]),
            "operating_model": deepcopy(profile["operating_model"]),
            "locality_guard": deepcopy(profile["locality_guard"]),
        },
        "copy_projection": copy_projection,
        "public_copy_eligibility": public_copy_eligibility,
        "usable_without_profile_llm": usable,
        "review_status": "unreviewed",
        "publication_permission": "not_granted",
        "request_id": request_id,
        "input_json_sha256": input_sha256,
        "provenance_observations": [{
            "request_id": request_id,
            "input_json_sha256": input_sha256,
            "profile_hash": profile_hash,
            "evidence_fingerprint": evidence_fingerprint,
            "observed_at": imported_at,
        }],
        "import_version": IMPORT_VERSION,
        "imported_at": imported_at,
        "first_imported_at": imported_at,
        "updated_at": imported_at,
    }
    return row


def _correction_row(
    correction: dict[str, Any],
    *,
    request_id: str,
    input_sha256: str,
    imported_at: str,
) -> dict[str, Any]:
    url = _canonical_url(correction["canonical_url"])
    domain = correction["linked_source_key"].split(":", 1)[1]
    source_key = runtime_publisher_source_key(domain)
    identity_key = "url:" + canonical_url_identity(url)
    identity_pk = "external_publication_identity_item:" + hashlib.sha256(identity_key.encode("utf-8")).hexdigest()
    correction_hash = canonical_json_sha256(correction)
    correction_id = "rtpublishercorr_" + stable_hash(source_key, canonical_url_identity(url))
    return {
        "pk": "publisher_profile_candidate_correction_item:" + correction_id,
        "publisher_profile_correction_id": correction_id,
        "canonical_url": url,
        "canonical_url_identity": canonical_url_identity(url),
        "linked_source_key": source_key,
        "input_linked_source_key": correction["linked_source_key"],
        "publisher_profile_id": publisher_profile_id(source_key),
        "observed_surface": correction["observed_surface"],
        "recommended_action": correction["recommended_action"],
        "reason_codes": list(correction["reason_codes"]),
        "reason_short": correction["reason_short"],
        "evidence_refs": list(correction["evidence_refs"]),
        "requires_live_ydb_revalidation": True,
        "review_status": "unreviewed",
        "live_revalidation_status": "pending_live_revalidation",
        "revalidation_status": "pending_live_revalidation",
        "publication_permission": "not_granted",
        "candidate_mutation_allowed": False,
        "regeneration_allowed": False,
        "next_action": (
            "operator_re_adjudicate_externality"
            if correction["recommended_action"] == "re_adjudicate_externality"
            else "operator_review_publisher_profile_correction"
        ),
        "correction_hash": correction_hash,
        "request_id": request_id,
        "input_json_sha256": input_sha256,
        "live_identity_key_sha256": hashlib.sha256(identity_key.encode("utf-8")).hexdigest(),
        "live_identity_pk": identity_pk,
        "live_intake_found": False,
        "live_external_publication_id": "",
        "live_intake_pk": "external_publication_intake_item:extpub_" + stable_hash(identity_key),
        "live_intake_snapshot_sha256": "",
        "import_version": IMPORT_VERSION,
        "queued_at": imported_at,
        "updated_at": imported_at,
    }


def prepare_import(
    payload: Any,
    *,
    input_bytes: bytes,
    expected_input_sha256: str | None = None,
    imported_at: str | None = None,
) -> dict[str, Any]:
    imported_at = imported_at or utc_now_iso()
    parsed = _parse_exact_json(input_bytes)
    if payload != parsed:
        raise ContractError("payload does not match exact input bytes")
    input_sha256 = hashlib.sha256(input_bytes).hexdigest()
    if expected_input_sha256:
        expected = str(expected_input_sha256).strip().lower()
        if not re.fullmatch(r"[a-f0-9]{64}", expected) or expected != input_sha256:
            raise ContractError("exact input SHA-256 does not match --expected-input-sha256")
    _schema_validate(parsed)
    _semantic_validate(parsed)

    request_id = str(parsed["run"]["request_id"])
    batch_id = "rtpublisherbatch_" + stable_hash(request_id)
    profiles = [
        _profile_row(row, request_id=request_id, input_sha256=input_sha256, imported_at=imported_at)
        for row in parsed["profiles"]
    ]
    corrections = [
        _correction_row(row, request_id=request_id, input_sha256=input_sha256, imported_at=imported_at)
        for row in parsed["candidate_corrections"]
    ]
    batch = {
        "pk": "publisher_profile_import_batch:" + batch_id,
        "publisher_profile_import_batch_id": batch_id,
        "request_id": request_id,
        "schema_version": parsed["schema_version"],
        "import_version": IMPORT_VERSION,
        "input_json_sha256": input_sha256,
        "profile_count": len(profiles),
        "correction_count": len(corrections),
        "runtime_source_keys": sorted(row["canonical_source_key"] for row in profiles),
        "profile_hashes": sorted(row["profile_hash"] for row in profiles),
        "evidence_fingerprints": sorted(row["evidence_fingerprint"] for row in profiles),
        "evidence_json_sha256s": sorted(row["evidence_json_sha256"] for row in profiles),
        "publication_permission": "not_granted",
        "publication_effect": "none",
        "imported_at": imported_at,
        "updated_at": imported_at,
    }
    receipt = {
        "pk": "publisher_profile_import_receipt_item:" + batch_id,
        "publisher_profile_import_batch_id": batch_id,
        "request_id": request_id,
        "input_json_sha256": input_sha256,
        "profile_count": len(profiles),
        "correction_count": len(corrections),
        "runtime_source_keys": batch["runtime_source_keys"],
        "profile_hashes": batch["profile_hashes"],
        "evidence_fingerprints": batch["evidence_fingerprints"],
        "evidence_json_sha256s": batch["evidence_json_sha256s"],
        "sanitized": True,
        "publication_effect": "none",
        "created_at": imported_at,
        "updated_at": imported_at,
    }
    rows: list[tuple[str, str, dict[str, Any]]] = [
        (row["pk"], "publisher_profile_item", row) for row in profiles
    ] + [
        (row["pk"], "publisher_profile_candidate_correction_item", row) for row in corrections
    ] + [
        (batch["pk"], "publisher_profile_import_batch", batch),
        (receipt["pk"], "publisher_profile_import_receipt_item", receipt),
    ]
    return {
        "batch": batch,
        "receipt": receipt,
        "profiles": profiles,
        "corrections": corrections,
        "ydb_rows": rows,
    }


def _json_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ContractError("YDB payload_json is not an object")
        return parsed
    if isinstance(value, dict):
        return dict(value)
    raise ContractError("YDB payload_json has an unsupported type")


def execute_import(prepared: dict[str, Any]) -> dict[str, Any]:
    """Strongly reread every affected key and commit all records atomically."""
    batch = prepared["batch"]
    receipt = prepared["receipt"]
    profiles = prepared["profiles"]
    corrections = prepared["corrections"]
    raw_sha256 = str(batch["input_json_sha256"])

    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    select_text = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"
    upsert_text = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    def op(session: Any) -> dict[str, Any]:
        select = session.prepare(select_text)
        upsert = session.prepare(upsert_text)
        tx = session.transaction(ydb.SerializableReadWrite())
        exact_payload_hashes: dict[str, str] = {}

        def read(pk: str) -> dict[str, Any] | None:
            response = tx.execute(select, {"$pk": pk}, commit_tx=False)
            if len(response or []) != 1:
                tx.rollback()
                raise ContractError(f"incomplete strong YDB read for {pk}")
            rows = response[0].rows
            if len(rows) > 1:
                tx.rollback()
                raise ContractError(f"non-unique strong YDB read for {pk}")
            if not rows:
                return None
            raw_payload = rows[0].payload_json
            exact_bytes = (
                raw_payload.encode("utf-8")
                if isinstance(raw_payload, str)
                else json.dumps(raw_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            )
            exact_payload_hashes[pk] = hashlib.sha256(exact_bytes).hexdigest()
            return _json_payload(raw_payload)

        current_batch = read(batch["pk"])
        current_receipt = read(receipt["pk"])
        current_profiles = {row["pk"]: read(row["pk"]) for row in profiles}
        current_corrections = {row["pk"]: read(row["pk"]) for row in corrections}

        intake_snapshots: dict[str, tuple[str, dict[str, Any] | None, str]] = {}
        for correction in corrections:
            identity = read(correction["live_identity_pk"])
            external_id = str((identity or {}).get("external_publication_id") or "")
            intake_pk = (
                "external_publication_intake_item:" + external_id
                if external_id else correction["live_intake_pk"]
            )
            intake_snapshots[correction["pk"]] = (intake_pk, read(intake_pk), external_id)

        if bool(current_batch) != bool(current_receipt):
            tx.rollback()
            raise ContractError("incomplete durable publisher import batch/receipt pair")
        if current_batch:
            if str(current_batch.get("input_json_sha256") or "") != raw_sha256:
                tx.rollback()
                raise ContractError("request_id conflict: durable batch has different exact input SHA-256")
            if str(current_receipt.get("input_json_sha256") or "") != raw_sha256:
                tx.rollback()
                raise ContractError("request_id conflict: durable receipt has different exact input SHA-256")
            for expected in profiles:
                current = current_profiles.get(expected["pk"])
                if not current:
                    tx.rollback()
                    raise ContractError("incomplete exact replay: durable publisher profile is missing")
                if str(current.get("canonical_source_key") or "") != str(expected["canonical_source_key"]):
                    tx.rollback()
                    raise ContractError("profile replay maps to another source key")
                durable_hashes = {
                    str(value) for value in list(current.get("profile_hashes") or [])
                    + [current.get("profile_hash")]
                    if str(value or "")
                }
                if str(expected["profile_hash"]) not in durable_hashes:
                    tx.rollback()
                    raise ContractError("incomplete exact replay: durable profile hash is missing")
            for expected in corrections:
                current = current_corrections.get(expected["pk"])
                if not current or str(current.get("correction_hash") or "") != str(expected["correction_hash"]):
                    tx.rollback()
                    raise ContractError("incomplete exact replay: durable candidate correction is missing")
            tx.rollback()
            return {
                "status": "identical_replay",
                "written_ydb_rows": 0,
                "profile_count": len(profiles),
                "correction_count": len(corrections),
                "conflict_count": 0,
            }

        rows_to_write: list[tuple[str, str, dict[str, Any]]] = []
        for incoming in profiles:
            try:
                merged = merge_publisher_profile_rows(current_profiles[incoming["pk"]] or {}, incoming)
            except PublisherProfileConflict as exc:
                tx.rollback()
                raise ContractError(str(exc)) from exc
            rows_to_write.append((incoming["pk"], "publisher_profile_item", merged))

        for incoming in corrections:
            current = current_corrections[incoming["pk"]]
            if current and str(current.get("correction_hash") or "") != str(incoming["correction_hash"]):
                tx.rollback()
                raise ContractError("candidate correction conflict: queued record has different content")
            if current and (
                str(current.get("review_status") or "unreviewed") != "unreviewed"
                or str(current.get("live_revalidation_status") or "pending_live_revalidation")
                != "pending_live_revalidation"
            ):
                tx.rollback()
                raise ContractError("candidate correction was already reviewed; refusing to overwrite it")
            intake_pk, intake, external_id = intake_snapshots[incoming["pk"]]
            queued = deepcopy(incoming)
            queued["live_intake_pk"] = intake_pk
            queued["live_intake_found"] = intake is not None
            queued["live_external_publication_id"] = external_id
            queued["live_intake_snapshot_sha256"] = exact_payload_hashes.get(intake_pk, "")
            queued["live_intake_snapshot"] = ({
                key: intake.get(key)
                for key in (
                    "external_publication_id", "canonical_url", "intake_status",
                    "review_status", "publication_permission", "decision",
                )
                if key in intake
            } if intake else None)
            rows_to_write.append((incoming["pk"], "publisher_profile_candidate_correction_item", queued))

        rows_to_write.extend([
            (batch["pk"], "publisher_profile_import_batch", batch),
            (receipt["pk"], "publisher_profile_import_receipt_item", receipt),
        ])
        if not rows_to_write:
            tx.rollback()
            raise ContractError("prepared publisher import has no durable rows")
        for index, (pk, kind, row) in enumerate(rows_to_write):
            tx.execute(
                upsert,
                {
                    "$pk": pk,
                    "$kind": kind,
                    "$payload_json": json.dumps(row, ensure_ascii=False, separators=(",", ":")),
                    "$updated_at": str(row.get("updated_at") or utc_now_iso()),
                },
                commit_tx=index == len(rows_to_write) - 1,
            )
        return {
            "status": "committed",
            "written_ydb_rows": len(rows_to_write),
            "profile_count": len(profiles),
            "correction_count": len(corrections),
            "conflict_count": 0,
        }

    try:
        return dict(pool.retry_operation_sync(op) or {})
    finally:
        driver.stop(timeout=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate/import Region Talk publisher profiles")
    parser.add_argument("input", type=Path)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "artifacts" / "codex" / "region-talk-publisher-profile-import.json",
    )
    parser.add_argument("--expected-input-sha256")
    parser.add_argument("--execute", action="store_true", help="Commit guarded rows; default is dry-run")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw = args.input.read_bytes()
    input_sha256 = hashlib.sha256(raw).hexdigest()
    try:
        payload = _parse_exact_json(raw)
        prepared = prepare_import(
            payload,
            input_bytes=raw,
            expected_input_sha256=args.expected_input_sha256,
        )
    except (ContractError, PublisherProfileConflict) as exc:
        report = {
            "input_json_sha256": input_sha256,
            "executed": bool(args.execute),
            "execution_status": "validation_failed_no_write" if args.execute else "validation_failed",
            "execution_error": str(exc),
            "planned_ydb_rows": 0,
            "written_ydb_rows": 0,
            "profile_count": 0,
            "correction_count": 0,
            "conflict_count": 0,
            "publication_effect": "none",
        }
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 2

    execution = {
        "status": "validated",
        "written_ydb_rows": 0,
        "profile_count": len(prepared["profiles"]),
        "correction_count": len(prepared["corrections"]),
        "conflict_count": 0,
    }
    execution_error = ""
    exit_code = 0
    if args.execute:
        load_env(args.env_file)
        try:
            execution = execute_import(prepared)
        except ContractError as exc:
            execution = {
                "status": "conflict_no_write",
                "written_ydb_rows": 0,
                "profile_count": 0,
                "correction_count": 0,
                "conflict_count": 1,
            }
            execution_error = str(exc)
            exit_code = 4
    report = {
        "request_id": prepared["batch"]["request_id"],
        "input_json_sha256": input_sha256,
        "profile_hashes": prepared["batch"]["profile_hashes"],
        "evidence_fingerprints": prepared["batch"]["evidence_fingerprints"],
        "runtime_source_keys": prepared["batch"]["runtime_source_keys"],
        "executed": bool(args.execute),
        "execution_status": execution["status"],
        "execution_error": execution_error or None,
        "planned_ydb_rows": len(prepared["ydb_rows"]),
        "written_ydb_rows": int(execution.get("written_ydb_rows") or 0),
        "profile_count": int(execution.get("profile_count") or 0),
        "correction_count": int(execution.get("correction_count") or 0),
        "conflict_count": int(execution.get("conflict_count") or 0),
        "publication_effect": "none",
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
