# core-verdict lane results

- Status: **Done** (implementation/tests committed; no push)
- Lane: `core-verdict`
- Requirements: R2 / R3 / R4
- Branch: `agent/dedup-regression/core-verdict`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/core-verdict`
- Base SHA: `ddd75b74105e0fea778ef25e3a1ff234d96d1e7a`
- Implementation head SHA: `8e9104f5f45ecfe6b89515b1616f2df7f9a01d26`
- Results receipt: committed separately after the implementation head above.

## Delivered

- Replaced the ambiguous `identity_gate_adjudicated` boolean with typed `FINAL_MATCH`, `FINAL_DISTINCT`, and `FINAL_RETRY` results.
- Extended the existing single dedup adjudicator contract with typed relation, source-grounded evidence, and blocking conflicts. No extra dedup LLM call was added.
- A CREATE after identity concern now requires an explicit, confidence-gated distinct-event/distinct-occurrence relation, validated source quote, and concrete blocking conflict. `no_candidate_match`, abstention, invalid schema/provider output, and rejected/low-confidence match remain retry.
- Persisted final owner/action/relation/confidence/evidence/conflicts plus existing candidate-state ID and append-only attempt number in `EventIdentityDecisionLog.decision_payload`; no model/table/migration change.
- Removed semantic-unknown attempt-exhaustion/invocation force-create behavior. Narrow identity retry classes remain `RETRY_SCHEDULED`, due, claimable, and non-exhausted after arbitrary attempt counts.
- Preserved accepted-only side effects, existing occurrence/source binding guards, exact fingerprint no-op, and candidate leases.
- Extended vector evidence/handoff to a bounded top five merged owners. Rank 2/3 are retained and considered by gate/adjudicator. An empty exact city/type result performs one relaxed recall with the same embedding; embedding call count remains one.
- Added sanitized corpus manifest for SOS, both qTickets pairs, Baltic Odyssey, teachers reminder, Dürer, Живая нить, and all required hard-negative classes.

## Changed files

- `smart_event_update.py`
- `smart_update_identity.py`
- `smart_update_state.py`
- `tests/test_dedup_adjudicator.py`
- `tests/test_smart_update_identity_incident_replay.py`
- `tests/test_smart_update_identity_persistence.py`
- `tests/test_smart_update_identity_vector_recall.py`
- `tests/replays/INC-2026-08-22-sos-dedup-veto-location-tyunin-farm/dedup_cases.json`
- `.codex/lanes/core-verdict/RESULTS.md` (this receipt)

## Commands and evidence

TDD red phase:

```text
python -m pytest -q tests/test_dedup_adjudicator.py \
  tests/test_smart_update_identity_persistence.py \
  tests/test_smart_update_identity_vector_recall.py
# collection errors: missing _dedup_adjudicator_final_result / IdentityFinalAction
```

Green focused regression suite:

```text
/home/dev/.codex/worktrees/events-bot-new/integration-dedup-regression/.venv/bin/python -m pytest -q \
  tests/test_dedup_adjudicator.py \
  tests/test_smart_update_identity_persistence.py \
  tests/test_smart_update_identity_vector_recall.py \
  tests/test_smart_update_identity_incident_replay.py \
  tests/test_smart_update_identity_gate.py \
  tests/test_smart_update_terminal_contract.py \
  tests/test_smart_update_candidate_state_db.py \
  tests/test_smart_update_source_identity_contract.py
# 81 passed in 18.38s
```

Validation:

```text
/home/dev/.codex/worktrees/events-bot-new/integration-dedup-regression/.venv/bin/python -m py_compile \
  smart_event_update.py smart_update_identity.py smart_update_state.py \
  tests/test_dedup_adjudicator.py \
  tests/test_smart_update_identity_persistence.py \
  tests/test_smart_update_identity_vector_recall.py \
  tests/test_smart_update_identity_incident_replay.py
# pass

git diff --check
# pass
```

Existing broader test audit:

```text
python -m pytest -q tests/test_smart_update_automatic_identity_resolution.py
# 15 passed, 3 failed
```

The three failures are stale expectations intentionally superseded by this incident contract:

1. identity provider unavailable expected `FAILED_TECHNICAL`, now durable `RETRY_SCHEDULED`;
2. semantic unknown expected inline distinct CREATE, now fail-closed retry;
3. invalid dedup schema expected `FAILED_TECHNICAL`, now durable retry.

These assertions are outside this lane's writable scope and must be updated during integration.

## Requirement evidence

- SOS-shaped veto + `create/no_candidate_match`: retry, zero new Event, final attempt-correlated log.
- Accepted typed match: existing owner merge, one Event.
- Explicit grounded exhibition-vs-excursion hard negative: distinct Event created.
- Rejected/low-confidence match and `None` provider/schema result: durable retry.
- Retry after configured attempt budget: remains due/non-exhausted, zero Event.
- Exact replay and source/candidate lease behavior: covered by focused source-identity and candidate-state suites.
- Two `Database` handles for one candidate: one created owner.
- Rank 2/3 propagation and Baltic/SOS-shaped metadata-filter fallback: covered with one embedding call.
- Single existing dedup call invariant: `test_smart_update_terminal_contract.py` passes.

## Risks / merge notes

- Integration must update the three superseded assertions listed above; reverting behavior to satisfy them would reintroduce the incident.
- The relaxed vector fallback runs only when the exact city/type query returns zero rows and keeps the current similarity/top-k bounds. It adds RPC work only on that miss path, not another embedding or LLM call.
- The full named corpus is recorded as a sanitized manifest; focused end-to-end replays prioritize SOS/Dürer positives and hard-negative distinct/retry classes. Production census/repair/deploy are explicitly outside this lane.
- No schema migration, UNIQUE(source_url), sync-owner change, coverage change, docs/CHANGELOG, production mutation, paid provider call, deploy, or push was performed.
