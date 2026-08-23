#!/usr/bin/env python3
"""Manifest-driven repair for INC-2026-08-22 August dedup regressions.

The tool deliberately does not discover duplicates.  It accepts only manually
adjudicated clusters whose complete pre-write state is pinned by a census hash,
row hashes, candidate-state rows, and source occurrence bindings.  Dry-run is
the CLI default.  Apply is fail-closed and uses ``BEGIN IMMEDIATE``.

The same reviewed cluster may carry narrowly allowlisted, before/after-pinned
date-anchor corrections.  They share the merge transaction, backup, verify,
rollback, and exact-second-apply receipt instead of using an unguarded SQL
sidecar.

No public or social API is called.  Public URLs on obsolete Event rows are
retained and returned as a cleanup handoff mapping.
"""

from __future__ import annotations

import argparse
from itertools import combinations
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any, Iterable, Sequence


INCIDENT = "INC-2026-08-22-sos-dedup-veto-location-tyunin-farm"
MANIFEST_VERSION = 1
BACKUP_TABLE = "incident_20260822_dedup_before"
RECEIPT_TABLE = "incident_20260822_dedup_receipt"
ACCEPTED_OUTCOMES = {"CREATED", "MERGED", "NOOP_EXACT_REPLAY"}
PUBLIC_URL_COLUMNS = (
    "telegraph_url",
    "ics_url",
    "ics_post_url",
    "tg_event_post_url",
    "vk_repost_url",
    "source_post_url",
    "source_vk_post_url",
)
JOB_STABLE_FIELDS = (
    "id",
    "event_id",
    "task",
    "status",
    "attempts",
    "payload",
    "last_error",
    "last_result",
    "coalesce_key",
    "depends_on",
)
JOB_VOLATILE_FIELDS = ("updated_at", "next_run_at")
PUBLICATION_OWNERSHIP_FIELDS = (
    "id",
    "event_id",
    "platform",
    "target",
    "stored_url",
    "live_url",
    "stored_post_id",
    "live_post_id",
)
PAIR_RELATIONS = {
    "MERGE",
    "KEEP_DISTINCT_RELATED",
    "KEEP_DISTINCT_OCCURRENCE",
    "PARENT_CHILD",
}
CONTENT_UPDATE_FIELDS = {
    "date",
    "time",
    "end_date",
    "end_date_is_inferred",
    "location_name",
    "location_address",
    "ticket_link",
    "lifecycle_status",
    "silent",
    "identity_status",
    "merged_into_event_id",
}
STATE_REPAIR_FIELDS = {
    "event_source": {"event_id", "source_role"},
    "event_source_fact": {"event_id", "source_id"},
    "eventposter": {
        "event_id",
        "review_status",
        "duplicate_of_id",
        "review_reason",
    },
    "smart_update_candidate_state": {
        "accepted_event_id",
        "diagnostic_event_id",
        "current_outcome",
        "reason",
    },
}


class RepairBlocked(RuntimeError):
    """Raised before commit when an incident safety contract is not satisfied."""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha(value: str | bytes) -> str:
    raw = value if isinstance(value, bytes) else value.encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def row_hash(row: sqlite3.Row | dict[str, Any]) -> str:
    """Stable hash used by manifests to pin an entire SQLite row."""

    return _sha(_json(dict(row)))


def census_hash(cutoff: str, clusters: Sequence[dict[str, Any]]) -> str:
    """Hash the complete adjudication payload and its exclusive census cutoff."""

    return _sha(_json({"cutoff": cutoff, "clusters": list(clusters)}))


def _table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in con.execute(f'PRAGMA table_info("{table}")')]


def _require_schema(con: sqlite3.Connection) -> None:
    required = {
        "event": {"id", "title", "date", "time", "lifecycle_status", "silent", "identity_status", "merged_into_event_id", "linked_event_ids"},
        "event_source": {"id", "event_id", "source_type", "source_url", "canonical_source_url", "candidate_key", "occurrence_key", "smart_update_candidate_id"},
        "event_source_fact": {"id", "event_id", "source_id", "fact"},
        "eventposter": {"id", "event_id", "poster_hash", "raw_sha256", "review_status", "duplicate_of_id", "review_reason"},
        "joboutbox": {*JOB_STABLE_FIELDS, *JOB_VOLATILE_FIELDS},
        "event_publication": set(PUBLICATION_OWNERSHIP_FIELDS),
        "smart_update_candidate_state": {"id", "occurrence_key", "current_outcome", "accepted_event_id", "diagnostic_event_id"},
        "smart_update_attempt": {"id", "candidate_state_id", "terminal_outcome", "accepted_event_id", "diagnostic_event_id"},
        "event_identity_decision_log": {"id", "event_id", "candidate_event_id", "source_id", "decision", "decision_reason", "confidence", "decided_by", "decision_payload"},
    }
    for table, expected in required.items():
        actual = set(_table_columns(con, table))
        missing = sorted(expected - actual)
        if missing:
            raise RepairBlocked(f"required_schema_missing:{table}:{','.join(missing)}")


def _rows(con: sqlite3.Connection, table: str, where: str = "1=1", params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    cur = con.execute(f'SELECT * FROM "{table}" WHERE {where} ORDER BY id', tuple(params))
    return [dict(row) for row in cur.fetchall()]


def _event(con: sqlite3.Connection, event_id: int) -> sqlite3.Row:
    row = con.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone()
    if row is None:
        raise RepairBlocked(f"event_missing:{event_id}")
    return row


def _job_semantic_projection(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    return {field: item.get(field) for field in JOB_STABLE_FIELDS}


def _job_timestamp_projection(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    return {field: item.get(field) for field in JOB_VOLATILE_FIELDS}


def _publication_ownership_projection(
    row: sqlite3.Row | dict[str, Any],
) -> dict[str, Any]:
    """Pin the event-to-publication resource binding, not reconcile metadata."""

    item = dict(row)
    return {field: item.get(field) for field in PUBLICATION_OWNERSHIP_FIELDS}


def cluster_graph_hash(con: sqlite3.Connection, event_ids: Sequence[int]) -> str:
    """Hash every incident-relevant row attached to an adjudicated cluster."""

    ids = sorted({int(item) for item in event_ids})
    if not ids:
        raise RepairBlocked("cluster_graph_ids_empty")
    p = ",".join("?" for _ in ids)
    sources = _rows(con, "event_source", f"event_id IN ({p})", ids)
    source_ids = [int(row["id"]) for row in sources]
    candidates = _rows(
        con,
        "smart_update_candidate_state",
        f"accepted_event_id IN ({p}) OR diagnostic_event_id IN ({p})",
        (*ids, *ids),
    )
    candidate_ids = [int(row["id"]) for row in candidates]
    graph: dict[str, Any] = {
        "event": _rows(con, "event", f"id IN ({p})", ids),
        "event_source": sources,
        "event_source_fact": [],
        "eventposter": _rows(con, "eventposter", f"event_id IN ({p})", ids),
        # Scheduler timestamps are deliberately not graph identity.  Their
        # transaction-bound comparison is recorded separately at apply time.
        "joboutbox": [
            _job_semantic_projection(row)
            for row in _rows(con, "joboutbox", f"event_id IN ({p})", ids)
        ],
        "event_publication": [
            _publication_ownership_projection(row)
            for row in _rows(con, "event_publication", f"event_id IN ({p})", ids)
        ],
        "smart_update_candidate_state": candidates,
        "smart_update_attempt": [],
        "event_identity_decision_log": _rows(
            con,
            "event_identity_decision_log",
            f"event_id IN ({p}) OR candidate_event_id IN ({p})",
            (*ids, *ids),
        ),
    }
    if candidate_ids:
        cp = ",".join("?" for _ in candidate_ids)
        graph["smart_update_attempt"] = _rows(con, "smart_update_attempt", f"candidate_state_id IN ({cp})", candidate_ids)
    if source_ids:
        sp = ",".join("?" for _ in source_ids)
        graph["event_source_fact"] = _rows(
            con,
            "event_source_fact",
            f"event_id IN ({p}) OR source_id IN ({sp})",
            (*ids, *source_ids),
        )
        source_decisions = _rows(con, "event_identity_decision_log", f"source_id IN ({sp})", source_ids)
        by_id = {int(row["id"]): row for row in graph["event_identity_decision_log"]}
        by_id.update({int(row["id"]): row for row in source_decisions})
        graph["event_identity_decision_log"] = [by_id[key] for key in sorted(by_id)]
    else:
        graph["event_source_fact"] = _rows(con, "event_source_fact", f"event_id IN ({p})", ids)
    return _sha(_json(graph))


def _parse_json_list(value: Any, *, field: str) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError) as exc:
        raise RepairBlocked(f"invalid_json_list:{field}") from exc
    if not isinstance(parsed, list):
        raise RepairBlocked(f"invalid_json_list:{field}")
    return parsed


def _load_manifest(path: str | Path) -> tuple[dict[str, Any], str]:
    manifest_path = Path(path).expanduser().resolve()
    if not manifest_path.is_file():
        raise RepairBlocked("manifest_not_found")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RepairBlocked("manifest_invalid_json") from exc
    if not isinstance(manifest, dict):
        raise RepairBlocked("manifest_not_object")
    return manifest, _sha(_json(manifest))


def _validate_manifest_shape(
    manifest: dict[str, Any],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    if manifest.get("schema_version") != MANIFEST_VERSION:
        raise RepairBlocked("manifest_schema_version")
    if manifest.get("incident") != INCIDENT:
        raise RepairBlocked("manifest_incident")
    prevention_sha = str(manifest.get("prevention_sha") or "")
    if not re.fullmatch(r"[0-9a-f]{7,40}", prevention_sha):
        raise RepairBlocked("prevention_sha_required")
    census = manifest.get("census")
    if not isinstance(census, dict) or not str(census.get("cutoff") or ""):
        raise RepairBlocked("census_cutoff_required")
    clusters = manifest.get("clusters")
    if not isinstance(clusters, list) or not clusters:
        raise RepairBlocked("clusters_required")
    expected_census = census_hash(str(census["cutoff"]), clusters)
    if census.get("sha256") != expected_census:
        raise RepairBlocked("census_hash_mismatch")

    merges: list[dict[str, Any]] = []
    reviews: list[dict[str, Any]] = []
    validation_units: list[dict[str, Any]] = []
    content_updates: list[dict[str, Any]] = []
    content_update_ids: set[int] = set()
    state_repairs: list[dict[str, Any]] = []
    state_repair_keys: set[tuple[str, int]] = set()
    seen_ids: dict[int, str] = {}
    cluster_ids: set[str] = set()
    for cluster in clusters:
        if not isinstance(cluster, dict):
            raise RepairBlocked("cluster_not_object")
        is_component = "pair_verdicts" in cluster or "component_id" in cluster
        cluster_id = str(
            (cluster.get("component_id") if is_component else cluster.get("cluster_id"))
            or ""
        )
        if not cluster_id or cluster_id in cluster_ids:
            raise RepairBlocked("cluster_id_invalid")
        cluster_ids.add(cluster_id)
        if is_component:
            try:
                event_ids = [int(value) for value in cluster.get("event_ids", [])]
            except (TypeError, ValueError) as exc:
                raise RepairBlocked(f"component_event_ids_invalid:{cluster_id}") from exc
            if (
                len(event_ids) < 2
                or any(value <= 0 for value in event_ids)
                or len(set(event_ids)) != len(event_ids)
            ):
                raise RepairBlocked(f"component_event_ids_invalid:{cluster_id}")
        else:
            relation = str(cluster.get("relation") or "").upper()
            if relation not in {"MERGE", "SAME_EVENT", "KEEP_DISTINCT"}:
                raise RepairBlocked(f"cluster_relation_invalid:{cluster_id}")
            try:
                canonical_id = int(cluster["canonical_id"])
                obsolete_ids = [int(item) for item in cluster.get("obsolete_ids", [])]
            except (KeyError, TypeError, ValueError) as exc:
                raise RepairBlocked(f"cluster_event_ids_invalid:{cluster_id}") from exc
            if canonical_id <= 0 or not obsolete_ids or canonical_id in obsolete_ids or len(set(obsolete_ids)) != len(obsolete_ids):
                raise RepairBlocked(f"cluster_event_ids_invalid:{cluster_id}")
            event_ids = [canonical_id, *obsolete_ids]
        for event_id in event_ids:
            previous = seen_ids.get(event_id)
            if previous is not None:
                raise RepairBlocked(f"cross_cluster_event_id:{event_id}:{previous}:{cluster_id}")
            seen_ids[event_id] = cluster_id
        if not str(cluster.get("reason") or "") or not isinstance(cluster.get("evidence"), list):
            raise RepairBlocked(f"cluster_adjudication_missing:{cluster_id}")
        confidence = cluster.get("confidence")
        if not isinstance(confidence, (int, float)) or not 0 <= float(confidence) <= 1:
            raise RepairBlocked(f"cluster_confidence_invalid:{cluster_id}")
        if not isinstance(cluster.get("conflicts"), list):
            raise RepairBlocked(f"cluster_evidence_shape:{cluster_id}")
        expected_hashes = cluster.get("expected_row_hashes")
        if not isinstance(expected_hashes, dict) or set(expected_hashes) != {str(x) for x in event_ids}:
            raise RepairBlocked(f"expected_row_hashes_incomplete:{cluster_id}")
        anchors = cluster.get("anchors")
        if not isinstance(anchors, dict) or set(anchors) != set(expected_hashes):
            raise RepairBlocked(f"anchors_incomplete:{cluster_id}")
        if not re.fullmatch(r"[0-9a-f]{64}", str(cluster.get("expected_graph_sha256") or "")):
            raise RepairBlocked(f"expected_graph_hash_required:{cluster_id}")
        expected_jobs = cluster.get("expected_job_rows")
        if not isinstance(expected_jobs, list) or any(
            not isinstance(row, dict)
            or not set(JOB_STABLE_FIELDS + JOB_VOLATILE_FIELDS).issubset(row)
            for row in expected_jobs
        ):
            raise RepairBlocked(f"job_constraints_required:{cluster_id}")
        expected_publications = cluster.get("expected_event_publications")
        if not isinstance(expected_publications, list) or any(
            not isinstance(row, dict)
            or not set(PUBLICATION_OWNERSHIP_FIELDS).issubset(row)
            for row in expected_publications
        ):
            raise RepairBlocked(f"publication_constraints_required:{cluster_id}")
        validation_units.append(cluster)

        unit_content_updates = cluster.get("content_updates", [])
        if not isinstance(unit_content_updates, list):
            raise RepairBlocked(f"content_updates_invalid:{cluster_id}")
        for update in unit_content_updates:
            if not isinstance(update, dict):
                raise RepairBlocked(f"content_update_invalid:{cluster_id}")
            try:
                update_event_id = int(update["event_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise RepairBlocked(f"content_update_event_id_invalid:{cluster_id}") from exc
            before = update.get("before")
            after = update.get("after")
            if (
                update_event_id not in event_ids
                or update_event_id in content_update_ids
                or not isinstance(before, dict)
                or not isinstance(after, dict)
                or not before
                or set(before) != set(after)
                or not set(before).issubset(CONTENT_UPDATE_FIELDS)
                or before == after
            ):
                raise RepairBlocked(f"content_update_invalid:{cluster_id}:{update_event_id}")
            content_update_ids.add(update_event_id)
            content_updates.append(
                {
                    "cluster_id": cluster_id,
                    "event_ids": list(event_ids),
                    "event_id": update_event_id,
                    "before": dict(before),
                    "after": dict(after),
                }
            )

        unit_state_repairs = cluster.get("state_repairs", [])
        if not isinstance(unit_state_repairs, list):
            raise RepairBlocked(f"state_repairs_invalid:{cluster_id}")
        for repair in unit_state_repairs:
            if not isinstance(repair, dict):
                raise RepairBlocked(f"state_repair_invalid:{cluster_id}")
            table = str(repair.get("table") or "")
            try:
                row_id = int(repair["id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise RepairBlocked(f"state_repair_id_invalid:{cluster_id}") from exc
            before = repair.get("before")
            after = repair.get("after")
            key = (table, row_id)
            if (
                table not in STATE_REPAIR_FIELDS
                or row_id <= 0
                or key in state_repair_keys
                or not isinstance(before, dict)
                or not isinstance(after, dict)
                or not before
                or set(before) != set(after)
                or not set(before).issubset(STATE_REPAIR_FIELDS[table])
                or before == after
            ):
                raise RepairBlocked(f"state_repair_invalid:{cluster_id}:{table}:{row_id}")
            state_repair_keys.add(key)
            state_repairs.append(
                {
                    "cluster_id": cluster_id,
                    "event_ids": list(event_ids),
                    "table": table,
                    "id": row_id,
                    "before": dict(before),
                    "after": dict(after),
                }
            )

        if is_component:
            pair_verdicts = cluster.get("pair_verdicts")
            if not isinstance(pair_verdicts, list) or not pair_verdicts:
                raise RepairBlocked(f"pair_verdicts_required:{cluster_id}")
            seen_pairs: set[tuple[int, int]] = set()
            component_merges: list[dict[str, Any]] = []
            for verdict in pair_verdicts:
                if not isinstance(verdict, dict):
                    raise RepairBlocked(f"pair_verdict_invalid:{cluster_id}")
                try:
                    left_id = int(verdict["left_id"])
                    right_id = int(verdict["right_id"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise RepairBlocked(f"pair_verdict_invalid:{cluster_id}") from exc
                if left_id == right_id or left_id not in event_ids or right_id not in event_ids:
                    raise RepairBlocked(f"pair_verdict_ids_invalid:{cluster_id}")
                pair_key = tuple(sorted((left_id, right_id)))
                if pair_key in seen_pairs:
                    raise RepairBlocked(f"pair_verdict_duplicate:{cluster_id}:{pair_key[0]}:{pair_key[1]}")
                seen_pairs.add(pair_key)
                pair_relation = str(verdict.get("relation") or "").upper()
                if pair_relation not in PAIR_RELATIONS:
                    raise RepairBlocked(f"pair_relation_invalid:{cluster_id}:{left_id}:{right_id}")
                adjudication = {
                    "reason": verdict.get("reason", cluster["reason"]),
                    "confidence": verdict.get("confidence", confidence),
                    "evidence": verdict.get("evidence", cluster["evidence"]),
                    "conflicts": verdict.get("conflicts", cluster["conflicts"]),
                }
                if (
                    not str(adjudication["reason"] or "")
                    or not isinstance(adjudication["confidence"], (int, float))
                    or not 0 <= float(adjudication["confidence"]) <= 1
                    or not isinstance(adjudication["evidence"], list)
                    or not isinstance(adjudication["conflicts"], list)
                ):
                    raise RepairBlocked(f"pair_adjudication_invalid:{cluster_id}:{left_id}:{right_id}")
                if pair_relation == "MERGE":
                    try:
                        pair_canonical = int(verdict["canonical_id"])
                    except (KeyError, TypeError, ValueError) as exc:
                        raise RepairBlocked(f"pair_merge_canonical_invalid:{cluster_id}:{left_id}:{right_id}") from exc
                    if pair_canonical not in {left_id, right_id}:
                        raise RepairBlocked(f"pair_merge_canonical_invalid:{cluster_id}:{left_id}:{right_id}")
                    obsolete = right_id if pair_canonical == left_id else left_id
                    component_merges.append(
                        {
                            "cluster_id": f"{cluster_id}:merge:{pair_key[0]}:{pair_key[1]}",
                            "component_id": cluster_id,
                            "canonical_id": pair_canonical,
                            "obsolete_ids": [obsolete],
                            "reason": str(adjudication["reason"]),
                            "confidence": float(adjudication["confidence"]),
                            "evidence": list(adjudication["evidence"]),
                            "conflicts": list(adjudication["conflicts"]),
                            "source_policy": cluster.get("source_policy"),
                            "poster_policy": cluster.get("poster_policy"),
                            "poster_exclusions": [],
                            "public_mapping": {
                                "canonical_id": pair_canonical,
                                "obsolete_ids": [obsolete],
                            },
                        }
                    )
                else:
                    if "canonical_id" in verdict:
                        raise RepairBlocked(f"pair_nonmerge_canonical_forbidden:{cluster_id}:{left_id}:{right_id}")
                    if (
                        float(adjudication["confidence"]) < 0.8
                        or not adjudication["evidence"]
                        or not adjudication["conflicts"]
                    ):
                        raise RepairBlocked(
                            f"pair_keep_distinct_evidence_missing:{cluster_id}:{left_id}:{right_id}"
                        )
                    reviews.append(
                        {
                            "cluster_id": cluster_id,
                            "component_id": cluster_id,
                            "left_id": left_id,
                            "right_id": right_id,
                            "relation": (
                                "related_but_distinct"
                                if pair_relation == "KEEP_DISTINCT_RELATED"
                                else (
                                    "distinct_occurrence"
                                    if pair_relation == "KEEP_DISTINCT_OCCURRENCE"
                                    else "parent_child"
                                )
                            ),
                            **adjudication,
                        }
                    )
            canonical_ids = {int(item["canonical_id"]) for item in component_merges}
            obsolete_ids_in_merges = {
                int(value) for item in component_merges for value in item["obsolete_ids"]
            }
            if canonical_ids & obsolete_ids_in_merges or len(obsolete_ids_in_merges) != len(component_merges):
                raise RepairBlocked(f"pair_merge_execution_order_unsafe:{cluster_id}")
            if component_merges:
                if cluster.get("source_policy") != "move_unique_collapse_exact":
                    raise RepairBlocked(f"source_policy_invalid:{cluster_id}")
                if cluster.get("poster_policy") != "move_preserve_graph":
                    raise RepairBlocked(f"poster_policy_invalid:{cluster_id}")
                poster_exclusions = cluster.get("poster_exclusions", [])
                if not isinstance(poster_exclusions, list) or any(
                    not isinstance(item, dict)
                    or int(item.get("id") or 0) <= 0
                    or not str(item.get("reason") or "")
                    for item in poster_exclusions
                ):
                    raise RepairBlocked(f"poster_exclusions_invalid:{cluster_id}")
                merge_obsolete_ids = {
                    int(value)
                    for item in component_merges
                    for value in item["obsolete_ids"]
                }
                seen_exclusion_ids: set[int] = set()
                for exclusion in poster_exclusions:
                    poster_id = int(exclusion["id"])
                    try:
                        exclusion_obsolete_id = int(exclusion["obsolete_id"])
                    except (KeyError, TypeError, ValueError) as exc:
                        raise RepairBlocked(
                            f"poster_exclusion_obsolete_required:{cluster_id}:{poster_id}"
                        ) from exc
                    if (
                        poster_id in seen_exclusion_ids
                        or exclusion_obsolete_id not in merge_obsolete_ids
                    ):
                        raise RepairBlocked(
                            f"poster_exclusion_component_invalid:{cluster_id}:{poster_id}"
                        )
                    seen_exclusion_ids.add(poster_id)
                for merge in component_merges:
                    obsolete_id = int(merge["obsolete_ids"][0])
                    merge["poster_exclusions"] = [
                        dict(exclusion)
                        for exclusion in poster_exclusions
                        if int(exclusion["obsolete_id"]) == obsolete_id
                    ]
                if not isinstance(cluster.get("expected_candidate_states"), list):
                    raise RepairBlocked(f"candidate_constraints_required:{cluster_id}")
                if not isinstance(cluster.get("expected_source_bindings"), list):
                    raise RepairBlocked(f"occurrence_constraints_required:{cluster_id}")
            merges.extend(sorted(component_merges, key=lambda item: (int(item["canonical_id"]), int(item["obsolete_ids"][0]))))
            continue

        public_mapping = cluster.get("public_mapping")
        if not isinstance(public_mapping, dict):
            raise RepairBlocked(f"cluster_evidence_shape:{cluster_id}")
        if public_mapping.get("canonical_id") != canonical_id or public_mapping.get("obsolete_ids") != obsolete_ids:
            raise RepairBlocked(f"public_mapping_mismatch:{cluster_id}")
        if relation in {"MERGE", "SAME_EVENT"}:
            if cluster.get("source_policy") != "move_unique_collapse_exact":
                raise RepairBlocked(f"source_policy_invalid:{cluster_id}")
            if cluster.get("poster_policy") != "move_preserve_graph":
                raise RepairBlocked(f"poster_policy_invalid:{cluster_id}")
            poster_exclusions = cluster.get("poster_exclusions", [])
            if not isinstance(poster_exclusions, list) or any(
                not isinstance(item, dict)
                or int(item.get("id") or 0) <= 0
                or not str(item.get("reason") or "")
                for item in poster_exclusions
            ):
                raise RepairBlocked(f"poster_exclusions_invalid:{cluster_id}")
            if not isinstance(cluster.get("expected_candidate_states"), list):
                raise RepairBlocked(f"candidate_constraints_required:{cluster_id}")
            if not isinstance(cluster.get("expected_source_bindings"), list):
                raise RepairBlocked(f"occurrence_constraints_required:{cluster_id}")
            merges.append(cluster)
        else:
            distinct_relation = str(cluster.get("distinct_relation") or "distinct_event")
            if distinct_relation not in {"distinct_event", "distinct_occurrence"}:
                raise RepairBlocked(f"distinct_relation_invalid:{cluster_id}")
            if float(confidence) < 0.8 or not cluster["evidence"] or not cluster["conflicts"]:
                raise RepairBlocked(f"keep_distinct_evidence_missing:{cluster_id}")
            for left_id, right_id in combinations(event_ids, 2):
                reviews.append(
                    {
                        "cluster_id": cluster_id,
                        "component_id": None,
                        "left_id": left_id,
                        "right_id": right_id,
                        "relation": distinct_relation,
                        "reason": cluster["reason"],
                        "confidence": float(confidence),
                        "evidence": list(cluster["evidence"]),
                        "conflicts": list(cluster["conflicts"]),
                    }
                )
    if not merges:
        raise RepairBlocked("merge_cluster_required")
    return merges, reviews, validation_units, content_updates, state_repairs


def _candidate_projection(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    return {
        "id": int(item["id"]),
        "occurrence_key": item.get("occurrence_key"),
        "current_outcome": item.get("current_outcome"),
        "accepted_event_id": item.get("accepted_event_id"),
        "diagnostic_event_id": item.get("diagnostic_event_id"),
        "row_sha256": row_hash(item),
    }


def _source_projection(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    return {
        "id": int(item["id"]),
        "event_id": int(item["event_id"]),
        "canonical_source_url": item.get("canonical_source_url"),
        "candidate_key": item.get("candidate_key"),
        "occurrence_key": item.get("occurrence_key"),
        "smart_update_candidate_id": item.get("smart_update_candidate_id"),
        "row_sha256": row_hash(item),
    }


def _unit_id_and_event_ids(unit: dict[str, Any]) -> tuple[str, list[int]]:
    if "pair_verdicts" in unit or "component_id" in unit:
        return str(unit["component_id"]), [int(value) for value in unit["event_ids"]]
    return str(unit["cluster_id"]), [
        int(unit["canonical_id"]),
        *(int(value) for value in unit["obsolete_ids"]),
    ]


def _validate_preconditions(
    con: sqlite3.Connection,
    manifest: dict[str, Any],
    merges: list[dict[str, Any]],
    validation_units: list[dict[str, Any]],
    content_updates: list[dict[str, Any]],
    state_repairs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    _require_schema(con)
    selected_ids: set[int] = set()
    for cluster in validation_units:
        cluster_id, ids = _unit_id_and_event_ids(cluster)
        selected_ids.update(ids)
        for event_id in ids:
            row = _event(con, event_id)
            expected_hash = str(cluster["expected_row_hashes"][str(event_id)])
            if row_hash(row) != expected_hash:
                raise RepairBlocked(f"event_row_hash_mismatch:{cluster_id}:{event_id}")
            anchor = cluster["anchors"][str(event_id)]
            if not isinstance(anchor, dict) or not anchor:
                raise RepairBlocked(f"event_anchor_missing:{cluster_id}:{event_id}")
            for field, expected in anchor.items():
                if field not in row.keys() or row[field] != expected:
                    raise RepairBlocked(f"event_anchor_mismatch:{cluster_id}:{event_id}:{field}")

    for update in content_updates:
        row = _event(con, int(update["event_id"]))
        unit_ids = {int(value) for value in update["event_ids"]}
        for merged_target in (
            update["before"].get("merged_into_event_id"),
            update["after"].get("merged_into_event_id"),
        ):
            if merged_target is not None and int(merged_target) not in unit_ids:
                raise RepairBlocked(
                    f"content_update_target_outside_unit:{update['cluster_id']}:{update['event_id']}:merged_into_event_id"
                )
        for field, expected in update["before"].items():
            if field not in row.keys() or row[field] != expected:
                raise RepairBlocked(
                    f"content_update_before_mismatch:{update['cluster_id']}:{update['event_id']}:{field}"
                )

    for repair in state_repairs:
        row = con.execute(
            f'SELECT * FROM "{repair["table"]}" WHERE id=?',
            (int(repair["id"]),),
        ).fetchone()
        if row is None:
            raise RepairBlocked(
                f"state_repair_row_missing:{repair['cluster_id']}:{repair['table']}:{repair['id']}"
            )
        unit_ids = {int(value) for value in repair["event_ids"]}
        event_reference_fields = (
            ("event_id",)
            if repair["table"]
            in {"event_source", "event_source_fact", "eventposter"}
            else ("accepted_event_id", "diagnostic_event_id")
        )
        saw_event_reference = False
        for field in event_reference_fields:
            current = row[field] if field in row.keys() else None
            after = repair["after"].get(field, current)
            for value in (current, after):
                if value is None:
                    continue
                saw_event_reference = True
                if int(value) not in unit_ids:
                    raise RepairBlocked(
                        f"state_repair_target_outside_unit:{repair['cluster_id']}:{repair['table']}:{repair['id']}:{field}"
                    )
        if not saw_event_reference:
            raise RepairBlocked(
                f"state_repair_outside_unit:{repair['cluster_id']}:{repair['table']}:{repair['id']}"
            )
        if repair["table"] == "event_source_fact":
            source_id = repair["after"].get("source_id", row["source_id"])
            source = con.execute(
                "SELECT event_id FROM event_source WHERE id=?", (int(source_id),)
            ).fetchone()
            if source is None or int(source["event_id"]) not in unit_ids:
                raise RepairBlocked(
                    f"state_repair_reference_outside_unit:{repair['cluster_id']}:event_source_fact:{repair['id']}:source_id"
                )
        if repair["table"] == "eventposter":
            duplicate_of_id = repair["after"].get(
                "duplicate_of_id", row["duplicate_of_id"]
            )
            if duplicate_of_id is not None:
                duplicate = con.execute(
                    "SELECT event_id FROM eventposter WHERE id=?",
                    (int(duplicate_of_id),),
                ).fetchone()
                if duplicate is None or int(duplicate["event_id"]) not in unit_ids:
                    raise RepairBlocked(
                        f"state_repair_reference_outside_unit:{repair['cluster_id']}:eventposter:{repair['id']}:duplicate_of_id"
                    )
        for field, expected in repair["before"].items():
            if field not in row.keys() or row[field] != expected:
                raise RepairBlocked(
                    f"state_repair_before_mismatch:{repair['cluster_id']}:{repair['table']}:{repair['id']}:{field}"
                )

    seen_excluded_posters: set[int] = set()
    for cluster in merges:
        obsolete_ids = {int(value) for value in cluster["obsolete_ids"]}
        for exclusion in cluster.get("poster_exclusions", []):
            poster_id = int(exclusion["id"])
            if poster_id in seen_excluded_posters:
                raise RepairBlocked(f"poster_exclusion_duplicate:{poster_id}")
            seen_excluded_posters.add(poster_id)
            row = con.execute(
                "SELECT event_id FROM eventposter WHERE id=?", (poster_id,)
            ).fetchone()
            if row is None or int(row["event_id"]) not in obsolete_ids:
                raise RepairBlocked(
                    f"poster_exclusion_owner_mismatch:{cluster['cluster_id']}:{poster_id}"
                )

    touched = sorted(selected_ids)
    placeholders = ",".join("?" for _ in touched)
    running = con.execute(
        f"SELECT id FROM joboutbox WHERE event_id IN ({placeholders}) AND status='running' ORDER BY id",
        tuple(touched),
    ).fetchall()
    if running:
        raise RepairBlocked("affected_job_running:" + ",".join(str(row[0]) for row in running))

    observed_timestamp_drift: list[dict[str, Any]] = []
    for cluster in validation_units:
        cluster_id, ids = _unit_id_and_event_ids(cluster)
        p = ",".join("?" for _ in ids)
        if "expected_candidate_states" in cluster:
            actual_candidates = [
                _candidate_projection(row)
                for row in con.execute(
                    f"SELECT * FROM smart_update_candidate_state WHERE accepted_event_id IN ({p}) OR diagnostic_event_id IN ({p}) ORDER BY id",
                    (*ids, *ids),
                ).fetchall()
            ]
            expected_candidates = sorted(cluster["expected_candidate_states"], key=lambda row: int(row["id"]))
            if actual_candidates != expected_candidates:
                raise RepairBlocked(f"candidate_state_constraint_mismatch:{cluster_id}")
        if "expected_source_bindings" in cluster:
            actual_sources = [
                _source_projection(row)
                for row in con.execute(f"SELECT * FROM event_source WHERE event_id IN ({p}) ORDER BY id", tuple(ids)).fetchall()
            ]
            expected_sources = sorted(cluster["expected_source_bindings"], key=lambda row: int(row["id"]))
            if actual_sources != expected_sources:
                raise RepairBlocked(f"source_occurrence_constraint_mismatch:{cluster_id}")

        actual_job_rows = _rows(con, "joboutbox", f"event_id IN ({p})", ids)
        expected_job_rows = sorted(
            cluster["expected_job_rows"], key=lambda row: int(row["id"])
        )
        actual_semantic = [_job_semantic_projection(row) for row in actual_job_rows]
        expected_semantic = [_job_semantic_projection(row) for row in expected_job_rows]
        if actual_semantic != expected_semantic:
            raise RepairBlocked(f"job_semantic_constraint_mismatch:{cluster_id}")
        expected_jobs_by_id = {int(row["id"]): row for row in expected_job_rows}
        if len(expected_jobs_by_id) != len(expected_job_rows):
            raise RepairBlocked(f"job_constraints_duplicate:{cluster_id}")
        for actual in actual_job_rows:
            job_id = int(actual["id"])
            expected_timestamps = _job_timestamp_projection(expected_jobs_by_id[job_id])
            observed_timestamps = _job_timestamp_projection(actual)
            if expected_timestamps != observed_timestamps:
                observed_timestamp_drift.append(
                    {
                        "id": job_id,
                        "expected": expected_timestamps,
                        "observed": observed_timestamps,
                    }
                )

        actual_publications = [
            _publication_ownership_projection(row)
            for row in _rows(con, "event_publication", f"event_id IN ({p})", ids)
        ]
        expected_publications = sorted(
            (
                _publication_ownership_projection(row)
                for row in cluster["expected_event_publications"]
            ),
            key=lambda row: int(row["id"]),
        )
        if actual_publications != expected_publications:
            raise RepairBlocked(f"event_publication_constraint_mismatch:{cluster_id}")
        if cluster_graph_hash(con, ids) != cluster["expected_graph_sha256"]:
            raise RepairBlocked(f"cluster_graph_hash_mismatch:{cluster_id}")

    return sorted(observed_timestamp_drift, key=lambda item: int(item["id"]))


def _public_identity(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    urls = {column: item.get(column) for column in PUBLIC_URL_COLUMNS if column in item and item.get(column)}
    return {"event_id": int(item["id"]), "title": item.get("title"), "date": item.get("date"), "time": item.get("time"), "urls": urls}


def _cleanup_mapping(con: sqlite3.Connection, merges: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    mapping = []
    for cluster in merges:
        mapping.append(
            {
                "cluster_id": cluster["cluster_id"],
                "canonical": _public_identity(_event(con, int(cluster["canonical_id"]))),
                "obsolete": [_public_identity(_event(con, int(item))) for item in cluster["obsolete_ids"]],
                "social_actions_performed": False,
            }
        )
    return mapping


def _backup_rows(
    con: sqlite3.Connection,
    manifest_sha: str,
    merges: Sequence[dict[str, Any]],
    validation_units: Sequence[dict[str, Any]],
    content_updates: Sequence[dict[str, Any]],
    state_repairs: Sequence[dict[str, Any]],
) -> tuple[int, int]:
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {BACKUP_TABLE}(manifest_sha TEXT NOT NULL, table_name TEXT NOT NULL, row_id INTEGER NOT NULL, row_json TEXT NOT NULL, row_sha256 TEXT NOT NULL, PRIMARY KEY(manifest_sha,table_name,row_id))"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {RECEIPT_TABLE}(manifest_sha TEXT PRIMARY KEY, incident TEXT NOT NULL, prevention_sha TEXT NOT NULL, census_sha TEXT NOT NULL, status TEXT NOT NULL, baseline_fk INTEGER NOT NULL, baseline_orphans INTEGER NOT NULL, after_hashes_json TEXT, audit_ids_json TEXT NOT NULL DEFAULT '[]', diff_json TEXT NOT NULL DEFAULT '[]', observed_job_timestamp_drift_json TEXT NOT NULL DEFAULT '[]', applied_at TEXT, rolled_back_at TEXT)"
    )
    receipt_columns = set(_table_columns(con, RECEIPT_TABLE))
    if "observed_job_timestamp_drift_json" not in receipt_columns:
        con.execute(
            f"ALTER TABLE {RECEIPT_TABLE} ADD COLUMN observed_job_timestamp_drift_json TEXT NOT NULL DEFAULT '[]'"
        )
    touched = sorted(
        {
            event_id
            for unit in validation_units
            for event_id in _unit_id_and_event_ids(unit)[1]
        }
        | {int(c["canonical_id"]) for c in merges}
        | {int(x) for c in merges for x in c["obsolete_ids"]}
        | {int(update["event_id"]) for update in content_updates}
    )
    p = ",".join("?" for _ in touched)
    selections: list[tuple[str, str, tuple[Any, ...]]] = [
        ("event", f"id IN ({p})", tuple(touched)),
        ("event_source", f"event_id IN ({p})", tuple(touched)),
        ("eventposter", f"event_id IN ({p})", tuple(touched)),
        ("joboutbox", f"event_id IN ({p})", tuple(touched)),
        ("event_publication", f"event_id IN ({p})", tuple(touched)),
        (
            "smart_update_candidate_state",
            f"accepted_event_id IN ({p}) OR diagnostic_event_id IN ({p})",
            (*touched, *touched),
        ),
        (
            "event_identity_decision_log",
            f"event_id IN ({p}) OR candidate_event_id IN ({p})",
            (*touched, *touched),
        ),
    ]
    selections.extend(
        (str(repair["table"]), "id=?", (int(repair["id"]),))
        for repair in state_repairs
    )
    selected_candidate_ids = [
        int(row[0])
        for row in con.execute(
            f"SELECT id FROM smart_update_candidate_state WHERE accepted_event_id IN ({p}) OR diagnostic_event_id IN ({p}) ORDER BY id",
            (*touched, *touched),
        )
    ]
    if selected_candidate_ids:
        cp = ",".join("?" for _ in selected_candidate_ids)
        selections.append(
            (
                "smart_update_attempt",
                f"candidate_state_id IN ({cp})",
                tuple(selected_candidate_ids),
            )
        )
    touched_source_ids = [
        int(row[0])
        for row in con.execute(f"SELECT id FROM event_source WHERE event_id IN ({p}) ORDER BY id", tuple(touched))
    ]
    if touched_source_ids:
        sp = ",".join("?" for _ in touched_source_ids)
        selections.append(
            ("event_source_fact", f"event_id IN ({p}) OR source_id IN ({sp})", (*touched, *touched_source_ids))
        )
        selections.append(
            ("event_identity_decision_log", f"source_id IN ({sp})", tuple(touched_source_ids))
        )
    else:
        selections.append(("event_source_fact", f"event_id IN ({p})", tuple(touched)))
    # Linked rows outside a cluster are part of the narrow mutation surface.
    for row in con.execute("SELECT * FROM event ORDER BY id").fetchall():
        links = _parse_json_list(row["linked_event_ids"], field=f"event:{row['id']}:linked_event_ids")
        if any(str(item).lstrip("-").isdigit() and int(item) in touched for item in links):
            if int(row["id"]) not in touched:
                selections.append(("event", "id=?", (int(row["id"]),)))
    seen: set[tuple[str, int]] = set()
    for table, where, params in selections:
        for row in _rows(con, table, where, params):
            key = (table, int(row["id"]))
            if key in seen:
                continue
            seen.add(key)
            raw = _json(row)
            con.execute(
                f"INSERT INTO {BACKUP_TABLE}(manifest_sha,table_name,row_id,row_json,row_sha256) VALUES(?,?,?,?,?)",
                (manifest_sha, table, int(row["id"]), raw, _sha(raw)),
            )
    baseline_fk = len(con.execute("PRAGMA foreign_key_check").fetchall())
    baseline_orphans = int(
        con.execute("SELECT COUNT(*) FROM event_source_fact f LEFT JOIN event_source s ON s.id=f.source_id WHERE s.id IS NULL").fetchone()[0]
    )
    return baseline_fk, baseline_orphans


def _apply_content_updates(
    con: sqlite3.Connection, content_updates: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    diff: list[dict[str, Any]] = []
    for update in content_updates:
        event_id = int(update["event_id"])
        before = dict(update["before"])
        after = dict(update["after"])
        row = _event(con, event_id)
        for field, expected in before.items():
            if row[field] != expected:
                raise RepairBlocked(
                    f"content_update_before_mismatch:{update['cluster_id']}:{event_id}:{field}"
                )
        fields = sorted(after)
        con.execute(
            f"UPDATE event SET {','.join(f'{field}=?' for field in fields)} WHERE id=?",
            tuple(after[field] for field in fields) + (event_id,),
        )
        diff.append(
            {
                "table": "event",
                "id": event_id,
                "action": "content_update",
                "fields": {field: {"before": before[field], "after": after[field]} for field in fields},
            }
        )
    return diff


def _apply_state_repairs(
    con: sqlite3.Connection, state_repairs: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    diff: list[dict[str, Any]] = []
    table_order = {
        "event_source": 0,
        "event_source_fact": 1,
        "eventposter": 2,
        "smart_update_candidate_state": 3,
    }
    for repair in sorted(
        state_repairs,
        key=lambda item: (table_order[str(item["table"])], int(item["id"])),
    ):
        table = str(repair["table"])
        row_id = int(repair["id"])
        row = con.execute(f'SELECT * FROM "{table}" WHERE id=?', (row_id,)).fetchone()
        if row is None:
            raise RepairBlocked(
                f"state_repair_row_missing:{repair['cluster_id']}:{table}:{row_id}"
            )
        before = dict(repair["before"])
        after = dict(repair["after"])
        for field, expected in before.items():
            if row[field] != expected:
                raise RepairBlocked(
                    f"state_repair_before_mismatch:{repair['cluster_id']}:{table}:{row_id}:{field}"
                )
        fields = sorted(after)
        con.execute(
            f'UPDATE "{table}" SET {",".join(f"{field}=?" for field in fields)} WHERE id=?',
            tuple(after[field] for field in fields) + (row_id,),
        )
        diff.append(
            {
                "table": table,
                "id": row_id,
                "action": "state_repair",
                "fields": {
                    field: {"before": before[field], "after": after[field]}
                    for field in fields
                },
            }
        )
    return diff


def _replace_links(value: Any, replacements: dict[int, int], self_id: int) -> str:
    links = _parse_json_list(value, field=f"event:{self_id}:linked_event_ids")
    normalized: list[int] = []
    for item in links:
        if not str(item).lstrip("-").isdigit():
            continue
        target = replacements.get(int(item), int(item))
        if target != self_id and target not in normalized:
            normalized.append(target)
    return _json(normalized)


def _retained_poster_identity(
    con: sqlite3.Connection,
    *,
    canonical_event_id: int,
    poster: sqlite3.Row,
    cluster_id: str,
) -> tuple[sqlite3.Row | None, list[str]]:
    """Resolve production poster uniqueness without discarding audit evidence.

    Production has two independent identities: the derived ``poster_hash`` and
    the non-empty raw-byte SHA-256.  A move must therefore be suppressed when
    either identity already exists on the canonical event.  If the two keys
    unexpectedly point at different retained rows, fail closed rather than
    choosing an arbitrary media owner.
    """

    matches: dict[int, tuple[sqlite3.Row, set[str]]] = {}

    def remember(row: sqlite3.Row | None, identity: str) -> None:
        if row is None:
            return
        poster_id = int(row["id"])
        retained, identities = matches.setdefault(poster_id, (row, set()))
        identities.add(identity)
        matches[poster_id] = (retained, identities)

    remember(
        con.execute(
            "SELECT * FROM eventposter WHERE event_id=? AND poster_hash=? ORDER BY id LIMIT 1",
            (canonical_event_id, poster["poster_hash"]),
        ).fetchone(),
        "poster_hash",
    )
    raw_sha256 = str(poster["raw_sha256"] or "").strip()
    if raw_sha256:
        remember(
            con.execute(
                "SELECT * FROM eventposter WHERE event_id=? AND raw_sha256=? ORDER BY id LIMIT 1",
                (canonical_event_id, raw_sha256),
            ).fetchone(),
            "raw_sha256",
        )
    if len(matches) > 1:
        retained_ids = ",".join(str(item) for item in sorted(matches))
        raise RepairBlocked(
            f"poster_identity_collision_ambiguous:{cluster_id}:{int(poster['id'])}:{retained_ids}"
        )
    if not matches:
        return None, []
    retained, identities = next(iter(matches.values()))
    return retained, sorted(identities)


def _apply_cluster(con: sqlite3.Connection, cluster: dict[str, Any]) -> tuple[list[dict[str, Any]], list[int]]:
    cluster_id = str(cluster["cluster_id"])
    canonical = int(cluster["canonical_id"])
    obsolete_ids = [int(x) for x in cluster["obsolete_ids"]]
    diff: list[dict[str, Any]] = []
    audit_ids: list[int] = []
    poster_exclusions = {
        int(item["id"]): str(item["reason"])
        for item in cluster.get("poster_exclusions", [])
    }

    for obsolete in obsolete_ids:
        sources = con.execute("SELECT * FROM event_source WHERE event_id=? ORDER BY id", (obsolete,)).fetchall()
        for source in sources:
            duplicate = con.execute(
                "SELECT id FROM event_source WHERE event_id=? AND source_url=? ORDER BY id LIMIT 1",
                (canonical, source["source_url"]),
            ).fetchone()
            if duplicate is None:
                con.execute("UPDATE event_source SET event_id=? WHERE id=?", (canonical, int(source["id"])))
                con.execute("UPDATE event_source_fact SET event_id=? WHERE source_id=?", (canonical, int(source["id"])))
                diff.append({"table": "event_source", "id": int(source["id"]), "action": "move", "from_event_id": obsolete, "to_event_id": canonical})
            else:
                retained_source = int(duplicate["id"])
                con.execute(
                    "UPDATE event_source_fact SET event_id=?,source_id=? WHERE source_id=?",
                    (canonical, retained_source, int(source["id"])),
                )
                con.execute(
                    "UPDATE event_identity_decision_log SET source_id=? WHERE source_id=?",
                    (retained_source, int(source["id"])),
                )
                con.execute("DELETE FROM event_source WHERE id=?", (int(source["id"]),))
                diff.append({"table": "event_source", "id": int(source["id"]), "action": "collapse_exact_binding", "retained_source_id": retained_source})

        posters = con.execute("SELECT * FROM eventposter WHERE event_id=? ORDER BY id", (obsolete,)).fetchall()
        for poster in posters:
            poster_id = int(poster["id"])
            exclusion_reason = poster_exclusions.get(poster_id)
            if exclusion_reason is not None:
                con.execute(
                    "UPDATE eventposter SET review_status='rejected',duplicate_of_id=NULL,review_reason=? WHERE id=?",
                    (f"{INCIDENT}:{exclusion_reason}", poster_id),
                )
                diff.append(
                    {
                        "table": "eventposter",
                        "id": poster_id,
                        "action": "preserve_rejected_evidence",
                        "event_id": obsolete,
                    }
                )
                continue
            duplicate, matching_identities = _retained_poster_identity(
                con,
                canonical_event_id=canonical,
                poster=poster,
                cluster_id=cluster_id,
            )
            if duplicate is None:
                con.execute("UPDATE eventposter SET event_id=? WHERE id=?", (canonical, int(poster["id"])))
                diff.append({"table": "eventposter", "id": int(poster["id"]), "action": "move", "to_event_id": canonical})
            else:
                # Retain both evidence rows.  The collision stays attached to the
                # obsolete audit shell but is explicitly linked to the retained
                # canonical media row.
                con.execute(
                    "UPDATE eventposter SET review_status='duplicate',duplicate_of_id=?,review_reason=? WHERE id=?",
                    (int(duplicate["id"]), f"{INCIDENT}:preserved_duplicate_media", int(poster["id"])),
                )
                diff.append(
                    {
                        "table": "eventposter",
                        "id": int(poster["id"]),
                        "action": "preserve_duplicate_evidence",
                        "duplicate_of_id": int(duplicate["id"]),
                        "matching_identities": matching_identities,
                    }
                )

        jobs = con.execute(
            "SELECT id FROM joboutbox WHERE event_id=? AND status IN ('pending','paused') ORDER BY id",
            (obsolete,),
        ).fetchall()
        for job in jobs:
            con.execute(
                "UPDATE joboutbox SET status='error',last_error=? WHERE id=?",
                (f"{INCIDENT}:cancelled_obsolete_event:{obsolete}->:{canonical}", int(job["id"])),
            )
            diff.append({"table": "joboutbox", "id": int(job["id"]), "action": "cancel_obsolete"})

        candidates = con.execute(
            "SELECT id,current_outcome FROM smart_update_candidate_state WHERE accepted_event_id=? ORDER BY id",
            (obsolete,),
        ).fetchall()
        for candidate in candidates:
            if str(candidate["current_outcome"]) not in ACCEPTED_OUTCOMES:
                raise RepairBlocked(f"candidate_owner_not_accepted:{int(candidate['id'])}")
            con.execute(
                "UPDATE smart_update_candidate_state SET accepted_event_id=?,current_outcome='MERGED',reason=? WHERE id=?",
                (canonical, f"{INCIDENT}:manifest_repair:{cluster_id}", int(candidate["id"])),
            )
            diff.append({"table": "smart_update_candidate_state", "id": int(candidate["id"]), "action": "accepted_owner", "to_event_id": canonical})

        # Public publication fields are intentionally absent from this UPDATE.
        con.execute(
            "UPDATE event SET lifecycle_status='cancelled',silent=1,identity_status='merged',merged_into_event_id=? WHERE id=?",
            (canonical, obsolete),
        )
        payload = {
            "incident": INCIDENT,
            "cluster_id": cluster_id,
            "relation": "same_event",
            "canonical_event_id": canonical,
            "obsolete_event_id": obsolete,
            "reason": cluster["reason"],
            "confidence": cluster["confidence"],
            "evidence": cluster["evidence"],
            "conflicts": cluster["conflicts"],
            "social_actions_performed": False,
        }
        if cluster.get("component_id"):
            payload["component_id"] = str(cluster["component_id"])
        cursor = con.execute(
            "INSERT INTO event_identity_decision_log(event_id,candidate_event_id,decision,decision_reason,confidence,decided_by,decision_payload) VALUES(?,?,?,?,?,?,?)",
            (canonical, obsolete, "repair_merge", str(cluster["reason"]), float(cluster["confidence"]), "incident_manifest_repair", _json(payload)),
        )
        audit_ids.append(int(cursor.lastrowid))
        diff.append({"table": "event", "id": obsolete, "action": "mark_merged", "merged_into_event_id": canonical})

    replacements = {obsolete: canonical for obsolete in obsolete_ids}
    all_rows = con.execute("SELECT id,linked_event_ids FROM event ORDER BY id").fetchall()
    for row in all_rows:
        before = str(row["linked_event_ids"] or "[]")
        after = _replace_links(before, replacements, int(row["id"]))
        if _json(_parse_json_list(before, field=f"event:{row['id']}:linked_event_ids")) != after:
            con.execute("UPDATE event SET linked_event_ids=? WHERE id=?", (after, int(row["id"])))
            diff.append({"table": "event", "id": int(row["id"]), "action": "rewrite_linked_event_ids"})
    return diff, audit_ids


def _record_pair_review(
    con: sqlite3.Connection,
    review: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[int]]:
    """Persist one explicitly adjudicated, source-grounded hard negative.

    Mixed components must never infer a blanket distinct verdict.  MERGE pairs
    are executed separately; only explicit relationship pairs arrive here.
    """

    cluster_id = str(review["cluster_id"])
    component_id = review.get("component_id")
    relation = str(review["relation"])
    event_id = int(review["left_id"])
    distinct_id = int(review["right_id"])
    diff: list[dict[str, Any]] = []
    audit_ids: list[int] = []
    payload = {
        "stage": "manual_pair_review_v1",
        "incident": INCIDENT,
        "cluster_id": cluster_id,
        "action": "FINAL_DISTINCT",
        # Audit-compatible identity verdict.  The more specific semantic edge
        # remains explicit and is not flattened into a component-wide rule.
        "relation": relation,
        "owner_event_id": event_id,
        "candidate_event_id": distinct_id,
        "evidence": list(review["evidence"]),
        "blocking_conflicts": list(review["conflicts"]),
        "confidence": float(review["confidence"]),
        "social_actions_performed": False,
    }
    if component_id:
        payload["component_id"] = str(component_id)
    if relation == "parent_child":
        payload["parent_event_id"] = event_id
        payload["child_event_id"] = distinct_id
    cursor = con.execute(
        "INSERT INTO event_identity_decision_log("
        "event_id,candidate_event_id,decision,decision_reason,confidence,"
        "decided_by,decision_payload) VALUES(?,?,?,?,?,?,?)",
        (
            event_id,
            distinct_id,
            "FINAL_DISTINCT",
            str(review["reason"]),
            float(review["confidence"]),
            "incident_manifest_repair",
            _json(payload),
        ),
    )
    audit_id = int(cursor.lastrowid)
    audit_ids.append(audit_id)
    diff.append(
        {
            "table": "event_identity_decision_log",
            "id": audit_id,
            "action": "record_pair_relation",
            "event_id": event_id,
            "candidate_event_id": distinct_id,
            "relation": relation,
        }
    )
    return diff, audit_ids


def _backup_current_hashes(con: sqlite3.Connection, manifest_sha: str) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for row in con.execute(
        f"SELECT table_name,row_id FROM {BACKUP_TABLE} WHERE manifest_sha=? ORDER BY table_name,row_id",
        (manifest_sha,),
    ):
        current = con.execute(f'SELECT * FROM "{row["table_name"]}" WHERE id=?', (int(row["row_id"]),)).fetchone()
        if current is None:
            current_hash = None
        elif str(row["table_name"]) == "joboutbox":
            current_hash = row_hash(_job_semantic_projection(current))
        elif str(row["table_name"]) == "event_publication":
            current_hash = row_hash(_publication_ownership_projection(current))
        else:
            current_hash = row_hash(current)
        result[f"{row['table_name']}:{int(row['row_id'])}"] = current_hash
    return result


def _receipt_current_hashes(
    con: sqlite3.Connection,
    manifest_sha: str,
    audit_ids: Sequence[int],
    *,
    include_created_audits: bool,
    scope_event_ids: Sequence[int] = (),
    include_scope_graph: bool = False,
) -> dict[str, str | None]:
    result = _backup_current_hashes(con, manifest_sha)
    if include_created_audits:
        for audit_id in audit_ids:
            row = con.execute(
                "SELECT * FROM event_identity_decision_log WHERE id=?",
                (int(audit_id),),
            ).fetchone()
            result[f"created_audit:{int(audit_id)}"] = row_hash(row) if row is not None else None
    if include_scope_graph:
        result["scope_graph"] = cluster_graph_hash(con, scope_event_ids)
    return result


def _verify(
    con: sqlite3.Connection,
    manifest_sha: str,
    merges: Sequence[dict[str, Any]],
    content_updates: Sequence[dict[str, Any]],
    state_repairs: Sequence[dict[str, Any]],
    scope_event_ids: Sequence[int],
) -> dict[str, Any]:
    receipt = con.execute(f"SELECT * FROM {RECEIPT_TABLE} WHERE manifest_sha=?", (manifest_sha,)).fetchone()
    if receipt is None or receipt["status"] != "applied":
        raise RepairBlocked("applied_receipt_missing")
    receipt_audit_ids = [int(value) for value in json.loads(str(receipt["audit_ids_json"] or "[]"))]
    expected_after_hashes = json.loads(str(receipt["after_hashes_json"] or "{}"))
    include_created_audits = any(
        str(key).startswith("created_audit:") for key in expected_after_hashes
    )
    include_scope_graph = "scope_graph" in expected_after_hashes
    if _receipt_current_hashes(
        con,
        manifest_sha,
        receipt_audit_ids,
        include_created_audits=include_created_audits,
        scope_event_ids=scope_event_ids,
        include_scope_graph=include_scope_graph,
    ) != expected_after_hashes:
        raise RepairBlocked("verification_cas_mismatch")
    missing_receipt_audits = 0
    if receipt_audit_ids:
        ap = ",".join("?" for _ in receipt_audit_ids)
        present = int(
            con.execute(
                f"SELECT COUNT(*) FROM event_identity_decision_log WHERE id IN ({ap})",
                tuple(receipt_audit_ids),
            ).fetchone()[0]
        )
        missing_receipt_audits = len(receipt_audit_ids) - present
    if missing_receipt_audits:
        raise RepairBlocked(f"receipt_audit_rows_missing:{missing_receipt_audits}")
    for update in content_updates:
        row = _event(con, int(update["event_id"]))
        for field, expected in update["after"].items():
            if row[field] != expected:
                raise RepairBlocked(
                    f"content_update_after_mismatch:{update['cluster_id']}:{update['event_id']}:{field}"
                )
    for repair in state_repairs:
        row = con.execute(
            f'SELECT * FROM "{repair["table"]}" WHERE id=?',
            (int(repair["id"]),),
        ).fetchone()
        if row is None:
            raise RepairBlocked(
                f"state_repair_row_missing:{repair['cluster_id']}:{repair['table']}:{repair['id']}"
            )
        for field, expected in repair["after"].items():
            if row[field] != expected:
                raise RepairBlocked(
                    f"state_repair_after_mismatch:{repair['cluster_id']}:{repair['table']}:{repair['id']}:{field}"
                )
    touched_obsolete = [int(x) for c in merges for x in c["obsolete_ids"]]
    for cluster in merges:
        canonical = int(cluster["canonical_id"])
        for obsolete in cluster["obsolete_ids"]:
            row = _event(con, int(obsolete))
            if row["lifecycle_status"] == "active" or int(row["silent"] or 0) != 1 or row["identity_status"] != "merged" or int(row["merged_into_event_id"] or 0) != canonical:
                raise RepairBlocked(f"obsolete_event_contract:{obsolete}")
            if any(int(item) == int(obsolete) for item in _parse_json_list(row["linked_event_ids"], field=f"event:{obsolete}:linked_event_ids") if str(item).lstrip("-").isdigit()):
                raise RepairBlocked(f"obsolete_self_link:{obsolete}")
    if touched_obsolete:
        p = ",".join("?" for _ in touched_obsolete)
        pending = int(con.execute(f"SELECT COUNT(*) FROM joboutbox WHERE event_id IN ({p}) AND status IN ('pending','paused')", tuple(touched_obsolete)).fetchone()[0])
        if pending:
            raise RepairBlocked(f"pending_obsolete_jobs:{pending}")
        stale_links = 0
        for row in con.execute("SELECT id,linked_event_ids FROM event"):
            links = _parse_json_list(row["linked_event_ids"], field=f"event:{row['id']}:linked_event_ids")
            stale_links += sum(str(x).lstrip("-").isdigit() and int(x) in touched_obsolete for x in links)
        if stale_links:
            raise RepairBlocked(f"obsolete_linked_event_ids:{stale_links}")
    touched_fact_ids = [
        int(row["row_id"])
        for row in con.execute(f"SELECT row_id FROM {BACKUP_TABLE} WHERE manifest_sha=? AND table_name='event_source_fact'", (manifest_sha,))
    ]
    inconsistent = 0
    orphans = 0
    if touched_fact_ids:
        p = ",".join("?" for _ in touched_fact_ids)
        inconsistent = int(con.execute(f"SELECT COUNT(*) FROM event_source_fact f JOIN event_source s ON s.id=f.source_id WHERE f.id IN ({p}) AND f.event_id<>s.event_id", tuple(touched_fact_ids)).fetchone()[0])
        orphans = int(con.execute(f"SELECT COUNT(*) FROM event_source_fact f LEFT JOIN event_source s ON s.id=f.source_id WHERE f.id IN ({p}) AND s.id IS NULL", tuple(touched_fact_ids)).fetchone()[0])
    if inconsistent or orphans:
        raise RepairBlocked(f"fact_source_inconsistent:{inconsistent}:{orphans}")
    canonical_by_obsolete = {
        int(obsolete): int(cluster["canonical_id"])
        for cluster in merges
        for obsolete in cluster["obsolete_ids"]
    }
    bad_source_moves = 0
    for backup in con.execute(
        f"SELECT row_json FROM {BACKUP_TABLE} WHERE manifest_sha=? AND table_name='event_source'",
        (manifest_sha,),
    ):
        before = json.loads(str(backup["row_json"]))
        expected_owner = canonical_by_obsolete.get(int(before["event_id"]))
        if expected_owner is None:
            continue
        current = con.execute("SELECT event_id FROM event_source WHERE id=?", (int(before["id"]),)).fetchone()
        if current is not None and int(current["event_id"]) != expected_owner:
            bad_source_moves += 1
    if bad_source_moves:
        raise RepairBlocked(f"source_owner_contract:{bad_source_moves}")
    touched_poster_ids = [
        int(row["row_id"])
        for row in con.execute(
            f"SELECT row_id FROM {BACKUP_TABLE} WHERE manifest_sha=? AND table_name='eventposter'",
            (manifest_sha,),
        )
    ]
    poster_orphans = 0
    bad_poster_moves = 0
    excluded_poster_ids = {
        int(item["id"])
        for cluster in merges
        for item in cluster.get("poster_exclusions", [])
    }
    if touched_poster_ids:
        pp = ",".join("?" for _ in touched_poster_ids)
        poster_orphans = int(
            con.execute(
                f"SELECT COUNT(*) FROM eventposter p LEFT JOIN event e ON e.id=p.event_id LEFT JOIN eventposter d ON d.id=p.duplicate_of_id WHERE p.id IN ({pp}) AND (e.id IS NULL OR (p.duplicate_of_id IS NOT NULL AND d.id IS NULL))",
                tuple(touched_poster_ids),
            ).fetchone()[0]
        )
        for backup in con.execute(
            f"SELECT row_json FROM {BACKUP_TABLE} WHERE manifest_sha=? AND table_name='eventposter'",
            (manifest_sha,),
        ):
            before = json.loads(str(backup["row_json"]))
            if int(before["id"]) in excluded_poster_ids:
                current = con.execute(
                    "SELECT event_id,review_status FROM eventposter WHERE id=?",
                    (int(before["id"]),),
                ).fetchone()
                if (
                    current is None
                    or int(current["event_id"]) != int(before["event_id"])
                    or current["review_status"] != "rejected"
                ):
                    bad_poster_moves += 1
                continue
            expected_owner = canonical_by_obsolete.get(int(before["event_id"]))
            if expected_owner is None:
                continue
            current = con.execute("SELECT * FROM eventposter WHERE id=?", (int(before["id"]),)).fetchone()
            if current is None:
                bad_poster_moves += 1
                continue
            if int(current["event_id"]) == expected_owner:
                continue
            retained = con.execute(
                "SELECT * FROM eventposter WHERE id=? AND event_id=?",
                (current["duplicate_of_id"], expected_owner),
            ).fetchone()
            same_hash = retained is not None and current["poster_hash"] == retained["poster_hash"]
            current_raw = str(current["raw_sha256"] or "").strip()
            same_raw = retained is not None and bool(current_raw) and current_raw == str(retained["raw_sha256"] or "").strip()
            if (
                int(current["event_id"]) != int(before["event_id"])
                or current["review_status"] != "duplicate"
                or retained is None
                or not (same_hash or same_raw)
            ):
                bad_poster_moves += 1
    if poster_orphans:
        raise RepairBlocked(f"poster_graph_orphans:{poster_orphans}")
    if bad_poster_moves:
        raise RepairBlocked(f"poster_owner_contract:{bad_poster_moves}")
    touched_decision_ids = [
        int(row["row_id"])
        for row in con.execute(
            f"SELECT row_id FROM {BACKUP_TABLE} WHERE manifest_sha=? AND table_name='event_identity_decision_log'",
            (manifest_sha,),
        )
    ]
    decision_source_orphans = 0
    if touched_decision_ids:
        dp = ",".join("?" for _ in touched_decision_ids)
        decision_source_orphans = int(
            con.execute(
                f"SELECT COUNT(*) FROM event_identity_decision_log d LEFT JOIN event_source s ON s.id=d.source_id WHERE d.id IN ({dp}) AND d.source_id IS NOT NULL AND s.id IS NULL",
                tuple(touched_decision_ids),
            ).fetchone()[0]
        )
    if decision_source_orphans:
        raise RepairBlocked(f"decision_source_orphans:{decision_source_orphans}")
    touched_candidate_ids = [
        int(row["row_id"])
        for row in con.execute(
            f"SELECT row_id FROM {BACKUP_TABLE} WHERE manifest_sha=? AND table_name='smart_update_candidate_state'",
            (manifest_sha,),
        )
    ]
    bad_owner = 0
    if touched_candidate_ids:
        cp = ",".join("?" for _ in touched_candidate_ids)
        bad_owner = int(
            con.execute(
                f"SELECT COUNT(*) FROM smart_update_candidate_state c JOIN event e ON e.id=c.accepted_event_id WHERE c.id IN ({cp}) AND c.accepted_event_id IS NOT NULL AND (c.current_outcome NOT IN ('CREATED','MERGED','NOOP_EXACT_REPLAY') OR e.identity_status<>'canonical' OR e.merged_into_event_id IS NOT NULL)",
                tuple(touched_candidate_ids),
            ).fetchone()[0]
        )
    if bad_owner:
        raise RepairBlocked(f"candidate_owner_contract:{bad_owner}")
    fk_count = len(con.execute("PRAGMA foreign_key_check").fetchall())
    if fk_count > int(receipt["baseline_fk"]):
        raise RepairBlocked(f"foreign_key_baseline_increased:{receipt['baseline_fk']}:{fk_count}")
    global_orphans = int(con.execute("SELECT COUNT(*) FROM event_source_fact f LEFT JOIN event_source s ON s.id=f.source_id WHERE s.id IS NULL").fetchone()[0])
    if global_orphans > int(receipt["baseline_orphans"]):
        raise RepairBlocked(f"orphan_baseline_increased:{receipt['baseline_orphans']}:{global_orphans}")
    quick = str(con.execute("PRAGMA quick_check").fetchone()[0])
    if quick.lower() != "ok":
        raise RepairBlocked(f"quick_check:{quick}")
    return {
        "quick_check": quick,
        "foreign_key_violations": fk_count,
        "baseline_foreign_key_violations": int(receipt["baseline_fk"]),
        "fact_source_inconsistencies": inconsistent,
        "touched_fact_orphans": orphans,
        "source_owner_violations": bad_source_moves,
        "poster_graph_orphans": poster_orphans,
        "decision_source_orphans": decision_source_orphans,
        "candidate_owner_violations": bad_owner,
        "pending_obsolete_jobs": 0,
        "receipt_audit_rows": len(receipt_audit_ids),
    }


def _restore_row(con: sqlite3.Connection, table: str, data: dict[str, Any]) -> None:
    columns = list(data)
    existing = con.execute(f'SELECT 1 FROM "{table}" WHERE id=?', (int(data["id"]),)).fetchone()
    if existing:
        if table == "joboutbox":
            update_columns = [
                column for column in JOB_STABLE_FIELDS if column != "id"
            ]
        elif table == "event_publication":
            update_columns = [
                column for column in PUBLICATION_OWNERSHIP_FIELDS if column != "id"
            ]
        else:
            update_columns = [column for column in columns if column != "id"]
        assignments = ",".join('"{}"=?'.format(column) for column in update_columns)
        con.execute(
            f'UPDATE "{table}" SET {assignments} WHERE id=?',
            tuple(data[column] for column in update_columns) + (int(data["id"]),),
        )
    else:
        column_sql = ",".join('"{}"'.format(column) for column in columns)
        con.execute(
            f'INSERT INTO "{table}"({column_sql}) VALUES({",".join("?" for _ in columns)})',
            tuple(data[column] for column in columns),
        )


def _rollback(
    con: sqlite3.Connection,
    manifest_sha: str,
    scope_event_ids: Sequence[int],
) -> dict[str, Any]:
    receipt = con.execute(f"SELECT * FROM {RECEIPT_TABLE} WHERE manifest_sha=?", (manifest_sha,)).fetchone()
    if receipt is None or receipt["status"] != "applied":
        raise RepairBlocked("rollback_receipt_not_applied")
    expected_after = json.loads(str(receipt["after_hashes_json"] or "{}"))
    audit_ids = [int(x) for x in json.loads(str(receipt["audit_ids_json"] or "[]"))]
    include_created_audits = any(
        str(key).startswith("created_audit:") for key in expected_after
    )
    include_scope_graph = "scope_graph" in expected_after
    if _receipt_current_hashes(
        con,
        manifest_sha,
        audit_ids,
        include_created_audits=include_created_audits,
        scope_event_ids=scope_event_ids,
        include_scope_graph=include_scope_graph,
    ) != expected_after:
        raise RepairBlocked("rollback_cas_mismatch")
    if audit_ids:
        p = ",".join("?" for _ in audit_ids)
        con.execute(f"DELETE FROM event_identity_decision_log WHERE id IN ({p})", tuple(audit_ids))
    backups: dict[str, list[dict[str, Any]]] = {}
    for row in con.execute(f"SELECT table_name,row_json FROM {BACKUP_TABLE} WHERE manifest_sha=? ORDER BY table_name,row_id", (manifest_sha,)):
        backups.setdefault(str(row["table_name"]), []).append(json.loads(str(row["row_json"])))
    # Parents/sources precede facts; event rows also repair external links.
    for table in (
        "event",
        "event_source",
        "event_identity_decision_log",
        "event_source_fact",
        "eventposter",
        "joboutbox",
        "smart_update_candidate_state",
    ):
        for data in backups.get(table, []):
            _restore_row(con, table, data)
    for table, rows in backups.items():
        for data in rows:
            current = con.execute(f'SELECT * FROM "{table}" WHERE id=?', (int(data["id"]),)).fetchone()
            if table == "joboutbox":
                matches = current is not None and row_hash(
                    _job_semantic_projection(current)
                ) == row_hash(_job_semantic_projection(data))
            elif table == "event_publication":
                matches = current is not None and row_hash(
                    _publication_ownership_projection(current)
                ) == row_hash(_publication_ownership_projection(data))
            else:
                matches = current is not None and row_hash(current) == _sha(_json(data))
            if not matches:
                raise RepairBlocked(f"rollback_restore_mismatch:{table}:{data['id']}")
    quick = str(con.execute("PRAGMA quick_check").fetchone()[0])
    if quick.lower() != "ok":
        raise RepairBlocked(f"rollback_quick_check:{quick}")
    con.execute(f"UPDATE {RECEIPT_TABLE} SET status='rolled_back',rolled_back_at=? WHERE manifest_sha=?", (datetime.now(timezone.utc).isoformat(), manifest_sha))
    return {"quick_check": quick, "restored": {table: len(rows) for table, rows in sorted(backups.items())}}


def run(db_path: str | Path, manifest_path: str | Path, mode: str = "dry-run") -> dict[str, Any]:
    if mode not in {"dry-run", "apply", "verify", "rollback"}:
        raise RepairBlocked("mode_invalid")
    manifest, manifest_sha = _load_manifest(manifest_path)
    (
        merges,
        reviews,
        validation_units,
        content_updates,
        state_repairs,
    ) = _validate_manifest_shape(manifest)
    scope_event_ids = sorted(
        {
            event_id
            for unit in validation_units
            for event_id in _unit_id_and_event_ids(unit)[1]
        }
    )
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise RepairBlocked("database_not_found")
    uri = path.as_uri() + ("?mode=ro" if mode == "dry-run" else "?mode=rw")
    con = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=30000")
    try:
        if mode == "dry-run":
            con.execute("PRAGMA query_only=ON")
            observed_timestamp_drift = _validate_preconditions(
                con,
                manifest,
                merges,
                validation_units,
                content_updates,
                state_repairs,
            )
            planned = [
                {"cluster_id": c["cluster_id"], "canonical_id": int(c["canonical_id"]), "obsolete_ids": [int(x) for x in c["obsolete_ids"]], "action": "merge"}
                for c in merges
            ]
            planned.extend(
                {
                    "cluster_id": review["cluster_id"],
                    "left_id": int(review["left_id"]),
                    "right_id": int(review["right_id"]),
                    "relation": str(review["relation"]),
                    "action": "record_pair_relation",
                }
                for review in reviews
            )
            planned.extend(
                {
                    "cluster_id": update["cluster_id"],
                    "event_id": int(update["event_id"]),
                    "action": "content_update",
                    "fields": update["after"],
                }
                for update in content_updates
            )
            planned.extend(
                {
                    "cluster_id": repair["cluster_id"],
                    "table": repair["table"],
                    "id": int(repair["id"]),
                    "action": "state_repair",
                    "fields": repair["after"],
                }
                for repair in state_repairs
            )
            return {
                "schema_version": MANIFEST_VERSION,
                "incident": INCIDENT,
                "mode": mode,
                "status": "ready",
                "changed": False,
                "manifest_sha256": manifest_sha,
                "prevention_sha": manifest["prevention_sha"],
                "census": manifest["census"],
                "diff": planned,
                "pair_review_count": len(reviews),
                "keep_distinct_count": len(reviews),
                "observed_job_timestamp_drift": observed_timestamp_drift,
                "cleanup_mapping": _cleanup_mapping(con, merges),
                "social_actions_performed": False,
            }

        con.execute("BEGIN IMMEDIATE")
        if mode in {"verify", "rollback"} and RECEIPT_TABLE not in {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
            raise RepairBlocked("repair_receipt_table_missing")
        if mode == "verify":
            verification = _verify(
                con,
                manifest_sha,
                merges,
                content_updates,
                state_repairs,
                scope_event_ids,
            )
            con.commit()
            return {"schema_version": MANIFEST_VERSION, "incident": INCIDENT, "mode": mode, "status": "verified", "changed": False, "diff": [], "manifest_sha256": manifest_sha, "verification": verification, "cleanup_mapping": _cleanup_mapping(con, merges), "social_actions_performed": False}
        if mode == "rollback":
            rolled_back = _rollback(con, manifest_sha, scope_event_ids)
            con.commit()
            return {"schema_version": MANIFEST_VERSION, "incident": INCIDENT, "mode": mode, "status": "rolled_back", "changed": True, "manifest_sha256": manifest_sha, "rollback": rolled_back, "social_actions_performed": False}

        existing = None
        if RECEIPT_TABLE in {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
            existing = con.execute(f"SELECT * FROM {RECEIPT_TABLE} WHERE manifest_sha=?", (manifest_sha,)).fetchone()
        if existing is not None and existing["status"] == "applied":
            verification = _verify(
                con,
                manifest_sha,
                merges,
                content_updates,
                state_repairs,
                scope_event_ids,
            )
            observed_timestamp_drift = json.loads(
                str(
                    existing["observed_job_timestamp_drift_json"]
                    if "observed_job_timestamp_drift_json" in existing.keys()
                    else "[]"
                )
            )
            con.commit()
            return {"schema_version": MANIFEST_VERSION, "incident": INCIDENT, "mode": mode, "status": "noop", "changed": False, "diff": [], "manifest_sha256": manifest_sha, "verification": verification, "observed_job_timestamp_drift": observed_timestamp_drift, "cleanup_mapping": _cleanup_mapping(con, merges), "social_actions_performed": False}
        if existing is not None:
            raise RepairBlocked(f"manifest_receipt_status:{existing['status']}")
        # This reread occurs only after BEGIN IMMEDIATE acquired the write lock.
        # Therefore timestamp-only scheduler drift can be observed safely while
        # every stable semantic field and publication owner remains pinned.
        observed_timestamp_drift = _validate_preconditions(
            con,
            manifest,
            merges,
            validation_units,
            content_updates,
            state_repairs,
        )
        baseline_fk, baseline_orphans = _backup_rows(
            con,
            manifest_sha,
            merges,
            validation_units,
            content_updates,
            state_repairs,
        )
        con.execute(
            f"INSERT INTO {RECEIPT_TABLE}(manifest_sha,incident,prevention_sha,census_sha,status,baseline_fk,baseline_orphans,observed_job_timestamp_drift_json) VALUES(?,?,?,?,?,?,?,?)",
            (manifest_sha, INCIDENT, manifest["prevention_sha"], manifest["census"]["sha256"], "applying", baseline_fk, baseline_orphans, _json(observed_timestamp_drift)),
        )
        diff: list[dict[str, Any]] = []
        audit_ids: list[int] = []
        diff.extend(_apply_content_updates(con, content_updates))
        diff.extend(_apply_state_repairs(con, state_repairs))
        for cluster in merges:
            cluster_diff, cluster_audits = _apply_cluster(con, cluster)
            diff.extend(cluster_diff)
            audit_ids.extend(cluster_audits)
        for review in reviews:
            cluster_diff, cluster_audits = _record_pair_review(con, review)
            diff.extend(cluster_diff)
            audit_ids.extend(cluster_audits)
        after_hashes = _receipt_current_hashes(
            con,
            manifest_sha,
            audit_ids,
            include_created_audits=True,
            scope_event_ids=scope_event_ids,
            include_scope_graph=True,
        )
        con.execute(
            f"UPDATE {RECEIPT_TABLE} SET status='applied',after_hashes_json=?,audit_ids_json=?,diff_json=?,applied_at=? WHERE manifest_sha=?",
            (_json(after_hashes), _json(audit_ids), _json(diff), datetime.now(timezone.utc).isoformat(), manifest_sha),
        )
        verification = _verify(
            con,
            manifest_sha,
            merges,
            content_updates,
            state_repairs,
            scope_event_ids,
        )
        cleanup = _cleanup_mapping(con, merges)
        con.commit()
        return {
            "schema_version": MANIFEST_VERSION,
            "incident": INCIDENT,
            "mode": mode,
            "status": "applied",
            "changed": True,
            "manifest_sha256": manifest_sha,
            "prevention_sha": manifest["prevention_sha"],
            "census": manifest["census"],
            "diff": diff,
            "verification": verification,
            "observed_job_timestamp_drift": observed_timestamp_drift,
            "cleanup_mapping": cleanup,
            "social_actions_performed": False,
        }
    except Exception:
        if con.in_transaction:
            con.rollback()
        raise
    finally:
        con.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="explicit SQLite database path")
    parser.add_argument("--manifest", required=True, help="explicit adjudicated JSON manifest path")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--apply", action="store_true", help="apply the guarded transaction")
    modes.add_argument("--verify", action="store_true", help="verify an applied manifest")
    modes.add_argument("--rollback", action="store_true", help="CAS-guarded restore from incident backup")
    parser.add_argument("--receipt", help="optional path for the sanitized JSON result/cleanup receipt")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    mode = "apply" if args.apply else "verify" if args.verify else "rollback" if args.rollback else "dry-run"
    try:
        result = run(args.db, args.manifest, mode)
    except RepairBlocked as exc:
        print(_json({"incident": INCIDENT, "mode": mode, "status": "blocked", "error": str(exc)}), file=sys.stderr)
        return 2
    rendered = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2)
    if args.receipt:
        target = Path(args.receipt).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
