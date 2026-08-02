# Intake ledger lane results

## Lane contract

- Lane ID: `intake-ledger`
- Requirement IDs: `R2`, `R4`, `R5`, `R8`
- Branch: `agent/region-talk-live-intake/intake-ledger`
- Base SHA: `e404e9fa754704d1f6c9e38946d6afa4a884329f`
- Validated implementation head SHA: `36c6c5d4e3801230d9a61c9d7bf4eb7d306515e0`
- Status: complete

## Requirement evidence

### R2 — explicit new-intake review state

- Clean arrivals retain `decision.import_status=ready_for_region_talk_scoring` only as normal CandidateReport routing.
- Every new intake persists `intake_status=new_intake`, `review_status=unreviewed`, and `publication_permission=not_granted`; no arrival is represented as LLM-confirmed or publication-approved.
- `manual_review_required` remains unchanged and routes to operator review; it is never auto-promoted.

### R4 — exact deterministic identity and provenance

- Added conservative canonical URL identity (tracking removal, query sorting, slash/default-port handling, HTTP/HTTPS and leading-www transport aliases), normalized DOI identity, and exact NFKC/casefold/whitespace-normalized title plus ordered authors identity.
- No fuzzy, semantic, or keyword matching was introduced.
- Added durable identity reservation rows keyed by full SHA-256 identity hashes.
- Intake rows preserve canonical `request_id`, exact-byte `input_json_sha256` (plus compatibility alias), stable `external_publication_id`, canonical evidence URLs, and canonical `intake_at` (plus compatibility timestamps).
- Live request/registry projections now preserve authors, normalized title/authors, and external publication IDs; compatible optional fields were added to both strict public schemas.
- Mixed identity keys resolving to different durable publications fail closed rather than merge arbitrarily.

### R5 — replay and fail-closed execution

- Batch request IDs are durable reservations; identical request ID plus identical exact-byte SHA is an explicit zero-write `identical_replay` with zero new IDs.
- Same request ID with different bytes fails closed before mutation.
- Accepted importer rows, batch receipt, seen/source rows, and identity reservations commit in one `SerializableReadWrite` transaction.
- The transaction rereads batch and identity reservations before any write, so concurrent URL/DOI/title-author races either serialize or fail closed.
- Any rejected or conflicting execute batch writes zero rows and does not publish the registry; dry-run still reports row-local issues.

### R8 — machine/operator observability

- Batch, CLI report, autonomous-run receipt, marker, GitHub Actions receipt, and step summary expose `new_intake_count`, sorted `new_intake_ids`, replay count/IDs, conflict count, execution status, and exact input SHA.
- Rejected/conflicting autonomous executions are not marked successful and do not write a cooldown success marker.
- GitHub Actions passes its independently computed exact input SHA into both validation and execute invocations.

## Validation

Commands run from the lane worktree:

```text
/home/dev/.venvs/events-bot-region-talk/bin/python -m py_compile \
  scripts/region_talk_external_publication_import.py \
  scripts/region_talk_external_research_request.py \
  scripts/region_talk_external_research_autorun.py
# passed

/home/dev/.venvs/events-bot-region-talk/bin/python - <<'PY'
# Draft202012Validator.check_schema for both changed schemas;
# yaml.safe_load for the changed GitHub Actions workflow.
PY
# schemas and workflow parse: ok

/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q \
  tests/test_region_talk_external_publication_import.py \
  tests/test_region_talk_external_research_request.py \
  tests/test_region_talk_external_research_registry.py \
  tests/test_region_talk_external_research_autorun.py \
  tests/test_region_talk_external_publication_review.py
# 43 passed in 3.43s

git diff --check
# passed
```

Focused regressions cover explicit unreviewed arrival, unchanged manual review, URL/DOI/title-author variants and negatives, mixed-key conflicts, exact-byte SHA provenance, identical replay, changed-byte request conflict, transactional identity races, execute-time all-or-nothing rejection, request/registry schema propagation, autorun failure semantics, and workflow receipts.

## Risks and integration notes

- No live YDB mutation or registry publication was performed. Serializable transaction behavior is covered with an in-memory YDB contract fake and follows the repository's established multi-statement transaction API; integration should retain a live non-production smoke gate.
- Large manually supplied batches now intentionally commit atomically; an oversized YDB transaction fails closed rather than partially staging rows.
- Canonical Region Talk prose documentation and `CHANGELOG.md` were forbidden in this lane and remain integrator-owned.
- The live-decisions lane was sent the exact clean-intake contract so it can admit unreviewed clean rows to normal scoring without treating arrival as publication permission.

## Changed files

- `.github/workflows/region-talk-external-publication-import.yml`
- `scripts/region_talk_external_publication_import.py`
- `scripts/region_talk_external_research_autorun.py`
- `scripts/region_talk_external_research_request.py`
- `docs/features/region-talk-channel/external-publication-research-request.schema.json`
- `docs/features/region-talk-channel/external-publication-research-registry.schema.json`
- `tests/test_region_talk_external_publication_import.py`
- `tests/test_region_talk_external_research_request.py`
- `tests/test_region_talk_external_research_registry.py`
- `tests/test_region_talk_external_research_autorun.py`
- `.codex/lanes/intake-ledger/RESULTS.md`
