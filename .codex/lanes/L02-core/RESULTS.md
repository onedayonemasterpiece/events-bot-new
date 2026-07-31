# L02-core results

## Scope

- Lane: `L02-core`
- Requirement IDs: `R02`, `R04`
- Base SHA: `cf76303d97d665ece2df1cc8afa69121c952f26b`
- Tested implementation SHA: `9b03646d`
- Result: complete

## Delivered

- Closed Pydantic contracts for exactly seven primary topologies (`unknown` is
  represented by `null`, not an eighth enum), programme structures, entity
  roles, seven dispositions, source/snapshot/subject/claim/decision types,
  semantic Event gates, and checkpoint records.
- Host-owned `operator_approval` and `smart_update` rejection for untrusted
  agent payloads.
- Exact snapshot hash + pinned normalizer + quote offset reproduction, bounded
  canonical JSON/fingerprints/candidate semantic hash, claim normalization,
  graph/reference checks, A/B inventory conservation, and mandatory checkpoint
  chain validation.
- Pending non-social URL currentness selection using only explicit ISO event
  periods; grouping uses only supplied identity hints; deterministic target
  fingerprint binds queue IDs, canonical URLs, snapshots, and version hashes.
- HTTP(S) URL canonicalization, literal/private/internal host rejection,
  injectable all-public DNS validation for every fetch/redirect, plus bounded
  traversal/link-safe ZIP/TAR extraction and artifact manifests.
- Host-owned registry:
  `festival_web_research/schemas/festival-taxonomy-registry-v2.json`.

## Verification evidence

Interpreter (shared existing environment; no dependency installation):

```text
/home/dev/projects/events-bot-new-wt-tg-stale-lease/.venv/bin/python
```

Commands:

```bash
/home/dev/projects/events-bot-new-wt-tg-stale-lease/.venv/bin/python -m py_compile festival_web_research/*.py
/home/dev/projects/events-bot-new-wt-tg-stale-lease/.venv/bin/python -m pytest --noconftest -q \
  tests/test_festival_web_research_contracts.py \
  tests/test_festival_web_research_selection.py \
  tests/test_festival_web_research_sources.py \
  tests/test_festival_web_research_evidence.py
git diff --check
```

Result: `37 passed in 0.45s`; compile and diff checks passed.

`--noconftest` was necessary only in the sparse lane worktree because the
repository-wide `tests/conftest.py` imports runtime modules excluded from this
lane's sparse checkout. The tests themselves are provider-free and do not use
fixtures from that conftest.

## Model shapes used by the coordinator

All raw agent dictionaries must first pass
`assert_no_agent_apply_authority(payload)`. Parse with `model_validate`; emit
JSON-native values with `model_dump(mode="json")`.

- `FestivalClassification`: `primary_topology`, `secondary_topologies`,
  `programme_structure`, `claim_ids`, `decision_ids`.
- `ProgrammeItem`: `item_id`, `entity_role`, `disposition`,
  `identity_claim_ids`, `logistics_claim_ids`, `decision_ids`, `event_gate`.
- `SourceSnapshot`: `source_id`, `requested_url`, `resolved_url`,
  `canonical_url`, `source_role`, `edition_status`, `content_sha256`,
  `normalizer_version`, `snapshot_ref`, `retrieved_at_utc`, `content_type`.
- `Claim`: `claim_id`, `source_id`, `local_subject_id`, `subject_kind`,
  `field`, `raw_value`, `normalized_value`, `normalization`, `evidence`,
  `content_sha256`, `normalizer_version`, `status`.
- `Decision`: `decision_id`, `decision_kind`, `subject_ref`, `selected_value`,
  `alternatives_rejected`, `evidence_claim_ids`, `reason_codes`, `status`,
  `actor_kind`.
- No top-level edition candidate model is defined in this lane. L04 owns the
  coordinator envelope; it must compose these closed subcontracts and call the
  validators before persistence.

## Risks / integration notes

- DNS-free canonicalization rejects unsafe literal/internal forms, but a caller
  must supply its resolver with `require_dns=True` immediately before every
  fetch and redirect to close DNS rebinding. No DNS/network was invoked here.
- Currentness deliberately accepts only explicit structured ISO periods. L04
  must supply extracted date signals; queue timestamps, titles, URL words and
  genre keywords are never freshness or identity evidence.
- Grouping never guesses identity. Missing series+edition hints produce one
  unresolved target per queue row for review.
- Full suite should be rerun after merge in the non-sparse integration tree.

## Changed files

- `festival_web_research/__init__.py`
- `festival_web_research/artifacts.py`
- `festival_web_research/contracts.py`
- `festival_web_research/evidence.py`
- `festival_web_research/schemas/festival-taxonomy-registry-v2.json`
- `festival_web_research/selection.py`
- `festival_web_research/sources.py`
- `festival_web_research/validators.py`
- `tests/test_festival_web_research_contracts.py`
- `tests/test_festival_web_research_evidence.py`
- `tests/test_festival_web_research_selection.py`
- `tests/test_festival_web_research_sources.py`
- `.codex/lanes/L02-core/RESULTS.md`
