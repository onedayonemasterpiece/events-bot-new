# INC-2026-08-22 August Smart Update deduplication regression

Status: investigating
Severity: sev1
Service: Smart Update identity / Telegram Monitoring / public projections
Opened: 2026-08-22
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-05-30-active-duplicate-events-recall-gate`, `INC-2026-07-11-event-vector-sidecar-sync-stalled`, `INC-2026-08-04-smart-update-identity-source-replay-corruption`, `INC-2026-08-10-smart-update-identity-terminal-loss`, `INC-2026-08-22-tyunin-farm-location-drift`
Related docs: `docs/features/smart-event-update/README.md`, `docs/features/smart-event-update/identity-state-machine.md`, `docs/features/unsigned-personalization/semantic-vector-retrieval.md`, `docs/operations/smart-update-prod-audit.md`

## Summary

An August control-flow regression allowed ordinary Event creation after the
existing identity gate found an owner and returned `VETO_CREATE`. The widened
vector recall and existing LLM dedup adjudicator both ran, but the caller
treated any non-empty adjudicator response as resolved. When merge acceptance
rejected `no_merge`, retry was bypassed and ordinary CREATE stayed reachable.

The production smoking gun is the Barn/SOS packet from
`https://t.me/kldevents/3619`: owner `8117` was found, a
`deterministic_same_ticket_slot` veto was persisted, `no_merge` followed, and
event `8242` was inserted about twenty seconds later. The same defect class is
present in qTickets refreshes `7580 → 7753` and `7603 → 7705`.

This is the systemic P0/SEV-1 record. The Tyunin Farm location defect reported
at the same time is a separate linked incident, not a root cause.

## User / Business Impact

- Duplicate active/upcoming rows can fan out to Telegram, VK, Telegraph, ICS,
  static pages, vectors and digests.
- A persisted veto did not describe the committed result, making the identity
  ledger incomplete.
- Reminder updates and vendor-range refreshes could create a new Event after
  recalling an existing owner.
- Broad repair is dangerous: one source can legitimately contain many children,
  so source URL and heuristic similarity are recall signals, not merge proof.

## Detection

Operator review found five public rows around the 22 August 21:00 Barn/SOS
programme. Production runtime-file logs supplied the end-to-end trace. An exact
checkpoint matrix and control-flow diff localized the first bad commit.

## Timeline

- `2026-07-02..11` — the separate vector-sidecar incident leaves `217/334`
  eligible documents projected. Post-Smart-Update enqueue and an independent
  three-hour reconciliation restore `334/334` for both kinds.
- `2026-07-31` — late-July replay remains fail-closed after unresolved veto.
- `2026-08-07` — `12e4e7c36c25ac250e9cd5c03972420c782dced7` first
  leaves CREATE reachable. Parent `04823317c8cafd438db01bb519cbf96d63f7bf00`
  is last good.
- `2026-08-10` — PR #494 merge `69ec40342` brings the regression to `main`
  alongside the required occurrence-scoped candidate/attempt work.
- `2026-08-22 01:51:35Z` — runtime records owner `8117`, `VETO_CREATE`
  (`deterministic_same_ticket_slot`, confidence `0.92`) and one existing dedup
  LLM call.
- `2026-08-22 01:51:38Z` — adjudicator returns `no_merge/llm_create`, confidence
  `0.90`, without source-grounded distinct proof.
- `2026-08-22 01:51:58Z` — ordinary create commits event `8242`.
- `2026-08-22 13:54:39Z` — pre-fix vector reconciliation `ops_run=6937`
  finishes successfully.

## Root Cause

1. First-bad commit `12e4e7c36` introduced `identity_gate_adjudicated` and
   set it for every non-null adjudicator response before establishing a merge
   or evidence-grounded distinct result.
2. `_dedup_adjudicator_accept_merge(...)` could reject that response and
   continue as `no_merge`.
3. The later fail-closed guard ran only when the boolean was false.
4. Ordinary CREATE therefore remained reachable after an owner and enforced
   create veto.

| checkpoint | unresolved owner/veto result |
| --- | --- |
| late-July `6775815de` | fail-closed |
| `5082a50` | fail-closed |
| `86c4a62` | fail-closed |
| last good `04823317c` | fail-closed |
| first bad `12e4e7c36` | ordinary CREATE reachable |
| PR #494 `69ec40342` | bad path reaches `main` |
| pre-fix main `ddd75b741` | bad path present |

Artifacts: `artifacts/codex/INC-2026-08-22-dedup-regression/first-bad-checkpoint-matrix.txt`,
`first-bad-control-flow.diff`, `first-bad-annotated-trace.txt`,
`runtime-sos-control-flow.txt`, and `runtime-sos-excerpts.txt`.

## Contributing Factors

- `no_merge` conflated explicit distinct, abstention and provider/schema
  uncertainty; it required no source-grounded occurrence/event difference.
- The RPC returned a small ranked list per vector kind, but Smart Update kept
  only rank one. Known true owners occur at ranks 2–3.
- Exact city/type filters hide owners after canonical-field drift (SOS and
  Baltic Odyssey probes).
- Documentation had temporarily replaced durable retry with inline distinct
  creation, contradicting the fail-closed contract.
- The final adjudicator evidence/conflicts were not persisted with the attempt.

## Vector Recall Evidence

Vectors are recall only. `related_v1` and `search_v3` widen discovery; SQLite
ownership plus the existing LLM adjudicator decide identity. Similarity alone
never authorizes merge.

| measure | fresh pre-fix production result |
| --- | --- |
| eligible documents | 703 |
| `related_v1` | 703/703; zero missing/extra/stale-hash |
| `search_v3` | 703/703; zero missing/extra/stale-hash |
| 2026-07-15..31 | 122/122 each kind |
| 2026-08-01..11 | 83/83 each kind |
| 2026-08-12..22 | 431/431 each kind |
| last reconciliation | `ops_run=6937`; 14 changed, 1392 unchanged, no cap remainder |
| current provider/RPC failures | no terminal failures; recovered RPM waits only |

The post-Smart-Update coalesced enqueue and independent 180-minute owner are
present. Event `7997` is intentionally export-ineligible because its ranged
date is malformed.

A stored-vector proxy over seven positive families used no provider call. The
true owner was top-1 for 2/7 and top-5 for 5/7; three owners ranked 2–3. Exact
type/city filtering removed SOS and Baltic Odyssey. Prevention must therefore
preserve a small top-k and bounded relaxed-filter fallback in the same existing
RPC/embedding pass, without an extra LLM call.

## Regression Corpus

### Required positives (`FINAL_MATCH`)

| family | anchors |
| --- | --- |
| Barn/SOS | `8117 → 8242`, `t.me/kldevents/3619` |
| qTickets 247858 | `7580`, `7601`, `7753` |
| qTickets 251796 | `7603`, `7705`, `7996`, `8183` |
| Baltic Odyssey | `8055`, `8108` |
| «Великие учителя» reminder | `3216`, `8244` |
| Dürer reminder | `5703`, `7690` |
| «Живая нить традиций» | `7609`, `7749` |

### Required hard negatives (`FINAL_DISTINCT`)

- exhibition versus its excursion;
- exhibition versus a separate lecture/activity/closing;
- different same-day sessions;
- different dates in a recurring series;
- festival parent versus independently attendable child;
- multiple child Events in one source post;
- different exhibitions at one venue.

The gate also requires exact replay twice, genuine concurrent two-worker
replay, zero hard-negative false merges and unchanged normal-path LLM counters.

## Production Census And Cause Classification

The frozen read-only pre-deploy census covered all 704 active/upcoming public
rows: 1,837 sources, 15,079 facts, 4,443 poster/audit rows, 18,813 jobs, 658
publications and 2,071 identity decisions. `PRAGMA quick_check=ok`. The
recall-biased all-pairs pass emitted 1,742 pairs:

| overlapping signal / preliminary class | count |
| --- | ---: |
| shared poster | 887 |
| shared identity source | 1,116 |
| related title | 338 |
| same ticket | 211 |
| same title | 123 |
| same-slot related | 113 |
| legitimate/multi-child hint | 967 |
| needs adjudication | 601 |
| veto candidate | 130 |
| reminder candidate | 6 |
| hard-duplicate candidate | 38 |

Signals overlap and are deliberately **not** merge decisions. Closure needs a
post-prevention untruncated census classified as recall miss, LLM error, veto
fall-through, legitimate occurrence, reminder update, replay race or heuristic
false positive. No bulk heuristic merge is allowed. Artifacts include
`prod-active-upcoming-census-raw.json.gz`,
`prod-active-upcoming-census-candidates.json`, and
`strong-cluster-source-evidence.md`.

## Automation Contract

### Treat as regression guard when

- create/merge gate, adjudicator, final create, candidate/attempt, occurrence,
  fingerprint or retry code changes;
- vector identity RPC/filter/top-k or sync ownership changes;
- Event merge/repair and obsolete-job handling changes;
- a named positive or hard-negative family is touched.

### Affected surfaces

- `smart_event_update.py`, `smart_update_identity.py`, `smart_update_state.py`;
- Fly SQLite identity/Event ledgers and personalization Supabase vector recall;
- Telegram/VK intake and accepted-only public fanout;
- incident-safe repair and separate social-cleanup handoff.

### Mandatory checks before closure or deploy

1. Named fixtures pass; exact replay twice adds zero Events; concurrent replay
   leaves one owner.
2. Owner/`VETO_CREATE` ends only as `FINAL_MATCH`, grounded
   `FINAL_DISTINCT`, or durable `FINAL_RETRY`; ordinary CREATE is unreachable.
3. Normal-path provider/LLM counters are unchanged.
4. Both vector kinds have 100% eligible coverage/current hashes; top-1/top-5
   recall is reported.
5. Manifest repair uses row hashes, preserves sources/facts/posters/audit,
   cancels only obsolete pending work, and second apply changes zero.
6. Telegraph, ICS, static, vector/search and digest are rebuilt; Telegram/VK
   cleanup uses a separate mapping.
7. SHA is on `origin/main`; deploy is clean exact-main; `/healthz`, quick-check,
   logs and two full ingestion iterations pass.

### Required evidence

- first-bad matrix/diff/runtime trace;
- tests, replay counts, LLM counters and corpus metrics;
- vector coverage/lag/rank/error receipts;
- classified census and reviewed manifest;
- backup/hash, apply/verify/second-apply, projection and social receipts;
- exact-main SHA/image, health, quick-check and two ingestion run IDs.

## Immediate Mitigation

- Evidence collection was read-only; no heuristic bulk merge was run.
- Event `8242` and its pending publication work were marked as repair risks.
- Prevention must deploy before canonical data repair.
- The generic duplicate utility is prohibited: it deletes rows and can erase
  facts/poster history or repoint historical jobs.

## Corrective Actions

- [ ] Replace the boolean with `FINAL_MATCH`, `FINAL_DISTINCT`, `FINAL_RETRY`.
- [ ] Require grounded distinct proof for CREATE after identity concern; bare
  `no_merge`, abstention, provider/schema errors become durable retry.
- [ ] Persist owner, action, relation, confidence, evidence/conflicts and
  attempt ID in the existing ledger.
- [ ] Preserve exact occurrence/fingerprint no-op and avoid
  `UNIQUE(source_url)`.
- [ ] Preserve small vector top-k/relaxed fallback without extra LLM calls.
- [ ] Deploy prevention, repair reviewed true duplicates, rebuild projections.

## Acceptance Criteria

- vector coverage both kinds 100%; critical fixtures 100%; duplicate recall
  at least 99%; distinct preservation at least 99%; hard-negative false merges
  zero;
- owner/veto to ordinary CREATE zero; exact replay zero new Events; concurrent
  replay one owner; normal-path LLM count unchanged;
- no unresolved hard duplicates after full census/repair;
- SHA reachable from `origin/main`; deploy, health, quick-check and two full
  ingestion iterations pass.

## Release And Closure Evidence

- prevention SHA/tests: pending integration
- production repair/projections/social cleanup: blocked until prevention deploy
- deploy and `/healthz`: pending
- two ingestion iterations: pending
- closure: open until every acceptance gate has evidence

## Prevention

Once identity concern exists, the existing adjudication operation must produce
one explicit final outcome. Only grounded distinct authorizes CREATE; match
uses the owner; uncertainty is durable retry. Vectors remain recall evidence
and accepted-only side effects remain unchanged.
