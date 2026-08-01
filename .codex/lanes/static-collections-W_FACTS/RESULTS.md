# W_FACTS results

## Scope

- Lane: `W_FACTS`
- Requirements: `R04`, `R05`
- Base SHA: `23c1702bb565f693f7022f3d7ac2e3455d6d412c`
- Implementation head SHA: `336a4dd8` (before this results-only commit)
- Branch: `agent/static-collections-data-prep/W_FACTS`
- Push: not performed, as instructed

## Outcome

Implemented a backward-compatible nullable `Event.collection_decisions` JSON container while retaining indexed `Event.is_free` as its materialized admission projection. SQLite bootstrap and Alembic migration both add the nullable column.

Added one compact candidate-only semantic adjudicator for admission, audience, and people facts. It is not appended to the existing rich-facts or merge schemas. Routing is bounded to explicit/backfill candidates, free/paid correction candidates, FAMILY/KIDS_SCHOOL or audience-BGE candidates, PERSONALITIES/people-BGE candidates, and rows with an existing decision that a newly accepted source may update. `ticket_status`/ticket link and age alone do not route or prove a semantic decision.

Validation requires a strict versioned schema and exact contiguous source/OCR quotes. Admission supports `confirmed_free|confirmed_paid|unknown`; optional donation can be grounded free, while ticket availability/sale alone is rejected as paid evidence. Audience supports `kids|family|none|unknown`; age/topic/BGE stay routing signals, not proof. People retain distinct `confirmed|mentioned|unknown` appearance and `russia_nonlocal|foreign|local|unknown` origin scope, with separate exact origin evidence for non-unknown origin.

Decision application is source-bound and fail-closed. It requires the exact persisted `EventSource` attached to the same event, injects source id/url/type/trust, input hash, policy version, timestamp, and lock state, then reassigns the entire JSON value. Unknown/provider failure preserves prior values; the same input hash is a no-op; manual locks win; confirmed conflicts replace only at higher trust or equal trust with a newer accepted decision. Creation now flushes before source attachment and commits source, decision JSON, and materialized bool together. Ordinary merge uses the same source-bound apply path.

## `smart_event_update.py` symbols / hunks changed

New/extended symbols:

- `EventCandidate`: added `topics`, `collection_bge_signals`, `collection_adjudication_reasons`, `collection_semantic_decisions`.
- `STATIC_COLLECTION_FACTS_POLICY_VERSION`
- `STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION`
- `COLLECTION_ADJUDICATION_JSON_SCHEMA`
- `_collection_source_corpus`
- `collection_adjudication_input_hash`
- `build_collection_adjudication_request`
- `route_collection_adjudication_reasons`
- `_strict_keys`
- `_exact_collection_quote`
- `validate_collection_adjudication_output`
- `_decision_timestamp`
- `_decision_wins`
- `deep_merge_collection_decisions`
- `_collection_provenance`
- `apply_collection_decisions`
- `adjudicate_collection_candidate`
- `_attached_collection_source`

Integration hunks in `_smart_event_update_impl`:

- after final match/dedup selection: bounded candidate routing and fail-closed adjudicator call, shared by create and ordinary merge;
- create transaction: replaced intermediate commit/refresh with flush, then attached exact source and applied collection decisions before final commit;
- merge transaction: after `_ensure_event_source`, flushed, selected exact same-event/source/type attachment, then applied decisions and tracked `collection_decisions`/`is_free` updates.

No location-grounding or non-event guard hunks from newer `origin/main` were absorbed or modified.

## Changed files

- `models.py`
- `db.py`
- `alembic/versions/20260801_static_collection_facts.py`
- `smart_event_update.py`
- `tests/test_smart_event_update.py`
- `tests/test_event_update_merge.py`
- `.codex/lanes/static-collections-W_FACTS/RESULTS.md`

## Evidence and commands

- `python3 -m py_compile models.py db.py smart_event_update.py alembic/versions/20260801_static_collection_facts.py tests/test_smart_event_update.py tests/test_event_update_merge.py` — passed.
- `uv run --with-requirements requirements.txt pytest -q tests/test_smart_event_update.py tests/test_event_update_merge.py tests/test_db.py tests/test_smart_update_native_schema.py tests/test_event_age_rating_db.py tests/test_smart_update_merge_identity_gate.py` — `77 passed`, one pre-existing Pydantic deprecation warning.
- `uv run --with ruff ruff check alembic/versions/20260801_static_collection_facts.py tests/test_smart_event_update.py tests/test_event_update_merge.py` — passed.
- `git diff --check` — passed before implementation commit.
- No external model calls were made in tests.

Coverage includes DB roundtrip/whole-JSON reassignment, migration metadata, free-to-paid correction, false-to-free, unknown preservation, trust and equal-trust recency, manual lock, same hash, sibling/unpersisted source rejection, ticket-sale negative, optional-donation free, audience age/topic/BGE hard negatives, no-call age-only/irrelevant routing, people mention versus confirmed appearance, grounded origin, provider failure, and Smart Update create/ordinary-merge integration.

## Risks / integration notes

- `models.py`, `db.py`, and `smart_event_update.py` are shared conflict files with later lanes. Integrator must preserve the exact symbols/hunks listed above while reconciling `origin/main` commit `a52a1a8a` and W_CLUBS.
- This lane intentionally does not wire the exporter, backfill runner, public participant overlays, docs, or `CHANGELOG.md`; those are integrator-owned. Export must consume structured adapters rather than exposing raw `people_appearances` JSON.
- The candidate-only stage uses the existing `_ask_gemma_json` provider path and its shared Google AI limiter. It remains fail-closed and is never invoked for an unrouted event.
- Opus consultation was already attempted and blocked (Antigravity location; Claude not logged in), per assignment. Implementation therefore followed `docs/llm/request-guide.md` and did not substitute a lower-class consultant.
