"""Versioned prompts for independent Antigravity festival collectors."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Sequence


PROMPT_VERSION = "festival-antigravity-ab-v1"
CONTRACT_VERSION = "festival-web-research-v2"
NORMALIZER_VERSION = "festival-text-normalizer-v1"
TAXONOMY_VERSION = "festival-taxonomy-v2"


@dataclass(frozen=True)
class ResearchTarget:
    name_hint: str
    edition_hint: str | None
    urls: tuple[str, ...]
    target_key: str


def prompt_sha256() -> str:
    return hashlib.sha256(PROMPT_VERSION.encode()).hexdigest()


def build_lane_prompt(target: ResearchTarget, lane: str) -> str:
    if lane not in {"A", "B", "C"}:
        raise ValueError("lane must be A, B or C")
    root = f"/workspace/festival_research_{lane.lower()}"
    emphasis = {
        "A": "Act as the primary evidence collector. Maximise recall but never invent facts.",
        "B": "Act as an independent skeptical verifier. Search independently, challenge edition identity, topology and every proposed child Event.",
        "C": "Resolve only the supplied A/B conflicts from saved evidence. Do not broaden discovery.",
    }[lane]
    target_json = json.dumps(
        {"name_hint": target.name_hint, "edition_hint": target.edition_hint, "seed_urls": list(target.urls)},
        ensure_ascii=False,
    )
    return f"""
You are collector lane {lane} in a production-oriented festival research pipeline.
{emphasis}

TARGET: {target_json}
The target hints are hypotheses, not facts. Work only with the current edition. You may browse public non-social web pages and official documents. Never use social networks as factual sources. Prefer official edition/programme/document/organizer/venue pages; use media/aggregators only as secondary evidence. Explicitly reject stale or wrong-edition pages.

You have no apply authority. Never emit keys named operator_approval or smart_update and never change any external site/database. Write all work immediately and incrementally under {root}. Use UTF-8 JSON and plain text. Preserve downloaded/normalized source text so every accepted fact can be checked.

Semantic taxonomy (primary_topology has exactly these seven values, or null when unknown):
series_season, lineup, grid_showcase, territory, market, route_promenade, network_pass.
programme_structure: identity_only, single_compound_event, standalone_events, schedule_only, hybrid, continuous_experience, distributed_cycle, unknown.

Do not call everything in a programme an Event. For every programme item choose exactly one entity_role and disposition. Event materialization (create_event_candidate or link_existing_event) is allowed only when ALL seven event_gate values are pass: current_edition, independent_choice, event_grade_occurrence, meaningful_identity, access_compatibility, topology_guardrail, evidence_validation. Participants, works, route points, products, zones/continuous activities and service information are not Events.

Required source/evidence discipline:
- Save normalized visible text for each accepted source as sources/<source_id>.txt.
- content_sha256 is SHA-256 of those exact UTF-8 bytes.
- Every Claim quote must occur exactly at quote_start:quote_end in that text; offsets are Python/Unicode character offsets and quote_end-quote_start == len(quote).
- Accepted claims may only reference accepted current-edition sources.
- Every lane-model Decision must cite claim IDs.
- Never merge conflicting facts silently. Record uncertainty/conflict.

Create these checkpoints as you work: state.json, source_ledger.json, for each source a source_review/claims/subjects file, topology.json, programme_inventory.json, candidate.json, run_summary.json. Also create checkpoint_manifest.json as an array ordered from sequence 0 with records {{checkpoint_id,kind,sequence,relative_path,content_sha256,byte_count,created_at_utc,parent_sha256}}. Hash the exact checkpoint bytes; parent_sha256 is null for state and then the preceding checkpoint hash. Kinds/order are state, source_ledger, repeated source_review→claims→subjects triplets, topology, programme_inventory, candidate, run_summary. If incomplete, the tail may stop after programme_inventory or candidate.

candidate.json MUST be one JSON object with exactly this shape (arrays may be empty where truthful):
{{
  "schema_version":"festival-web-research-v2",
  "lane":"{lane}",
  "festival":{{"name":"...","edition_label":null,"description_facts":[],"start_date":null,"end_date":null,"official_url":null,"venue_names":[],"organizer_names":[],"claim_ids_by_field":{{"name":["C1"]}}}},
  "classification":{{"primary_topology":null,"secondary_topologies":[],"programme_structure":"unknown","claim_ids":["C1"],"decision_ids":["D1","D2"]}},
  "sources":[{{"source_id":"S1","requested_url":"https://...","resolved_url":"https://...","canonical_url":"https://...","source_role":"official_home","edition_status":"accepted","content_sha256":"64hex","normalizer_version":"festival-text-normalizer-v1","snapshot_ref":"sources/S1.txt","retrieved_at_utc":"ISO UTC","content_type":"text/plain"}}],
  "subjects":[{{"source_id":"S1","local_subject_id":"festival","subject_kind":"festival"}}],
  "claims":[{{"claim_id":"C1","source_id":"S1","local_subject_id":"festival","subject_kind":"festival","field":"title","raw_value":"...","normalized_value":"...","normalization":"trim","evidence":{{"quote":"...","quote_start":0,"quote_end":3}},"content_sha256":"same 64hex","normalizer_version":"festival-text-normalizer-v1","status":"accepted"}}],
  "decisions":[{{"decision_id":"D1","decision_kind":"discovery_topology","subject_ref":"festival","selected_value":"lineup","alternatives_rejected":[],"evidence_claim_ids":["C1"],"reason_codes":[],"status":"supported","actor_kind":"lane_model"}},{{"decision_id":"D2","decision_kind":"programme_structure","subject_ref":"festival","selected_value":"unknown","alternatives_rejected":[],"evidence_claim_ids":["C1"],"reason_codes":[],"status":"supported","actor_kind":"lane_model"}}],
  "programme_items":[{{"item_id":"item:1","entity_role":"child_event","disposition":"create_event_candidate","identity_claim_ids":["..."],"logistics_claim_ids":["..."],"decision_ids":["..."],"event_gate":{{"current_edition":"pass","independent_choice":"pass","event_grade_occurrence":"pass","meaningful_identity":"pass","access_compatibility":"pass","topology_guardrail":"pass","evidence_validation":"pass"}}}}],
  "uncertainties":[],
  "source_exclusions":[]
}}
Use only enum values demonstrated/defined above and in the shape. Every non-empty festival fact field needs accepted Claim IDs in claim_ids_by_field. Classification needs matching evidence-backed discovery_topology and programme_structure decisions. Each programme item needs exactly one programme_item_disposition decision whose subject_ref is `programme_item:<item_id>` and selected_value equals disposition. Event dispositions require accepted programme_item Claims whose local_subject_id exactly equals item_id: an identity title Claim and logistics Claims for date, start time and place, in addition to all seven pass gates. Be conservative: programme_only/schedule_slot/continuous_activity/service_information/reject may have unknown/not_applicable gate values.

At completion, respond briefly with the candidate path and counts. The JSON files, not prose response, are authoritative.
""".strip()


def build_conflict_prompt(target: ResearchTarget, conflicts: Sequence[dict]) -> str:
    target_json = json.dumps(
        {"name_hint": target.name_hint, "edition_hint": target.edition_hint, "seed_urls": list(target.urls)},
        ensure_ascii=False,
    )
    return f"""
You are the bounded no-network adjudicator C for festival research target {target_json}.
Do not browse and do not add facts. Use only the two already host-validated lane candidates and conflicts below. Never emit operator_approval or smart_update. If evidence does not settle a conflict, mark it unresolved rather than guessing.

Write /workspace/festival_research_c/adjudication.json:
{{"schema_version":"festival-web-research-v2","lane":"C","resolutions":[{{"field":"...","selected_lane":"A","evidence_claim_ids":["..."],"reason":"..."}}],"unresolved_fields":[]}}
selected_lane is A or B. evidence_claim_ids must occur in the selected candidate. Respond only with the path and counts.

HOST-VALIDATED INPUT:
{json.dumps(list(conflicts), ensure_ascii=False, indent=2)}
""".strip()
