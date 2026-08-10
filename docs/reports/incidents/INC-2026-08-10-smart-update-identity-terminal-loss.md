# INC-2026-08-10 Smart Update automatic identity terminal loss

Status: open
Severity: sev1
Service: Smart Update automatic event ingestion
Opened: 2026-08-10
Closed: —
Owners: Smart Update service owner / production incident owner
Related incidents: `INC-2026-08-04-smart-update-identity-source-replay-corruption`
Related docs: `docs/features/smart-event-update/README.md`,
`docs/operations/smart-update-prod-audit.md`,
`docs/operations/release-smoke-smart-update.md`

## Summary

The fully automatic LLM-first ingestion system acquired a terminal
`review_required` state even though the product has no event-review operator.
Create and merge identity protection also acted as two independent vetoes
without a mandatory automatic resolution. Valid candidates could therefore end
as `review_required`, `skipped_identity_gate`, `skipped_context_only`, or a
generic downstream `failed` instead of being merged, created distinctly, or
retained for an automatic retry.

The identity guard correctly treats a false merge as the higher-risk outcome;
the defect is that uncertainty stopped ingestion rather than converging on a
distinct Event after a bounded retry.

## User / Business Impact

- Product-valid events were absent from the canonical catalogue and public
  surfaces after an identity safety decision.
- Repeated automatic source and VK runs revisited the same carriers without a
  durable candidate-level terminal receipt.
- A diagnostic matched Event ID could be interpreted as successful persistence
  by callers, allowing downstream counters, notices, or queue state to diverge
  from the actual Smart Update result.
- One Telegram/VK carrier could not safely represent several independent event
  occurrences because identity-bearing ownership was unique by canonical URL
  as a whole.

## Detection

A read-only audit was frozen for
`[2026-08-04T00:00:00Z, 2026-08-10T07:17:05Z)`. The production SQLite database
was opened with `mode=ro` and `query_only=ON`; `PRAGMA quick_check` returned
`ok`. Both non-secret runtime gates were observed as `enforce`:

- `SMART_UPDATE_IDENTITY_GATE=enforce`;
- `SMART_UPDATE_MERGE_IDENTITY_GATE=enforce`.

The identity ledger recorded no `source_binding_conflict` rows, while VK run
details and the retained runtime mirror did record the conflicts. This is an
observability and terminal-correlation gap, not evidence that the conflicts did
not happen.

## Production Baseline Before The Fix

### Identity decision ledger by day

| UTC day | allow_create | allow_merge | review_required | skip side effects | veto_create |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2026-08-04 | 44 | 25 | 0 | 3 | 0 |
| 2026-08-05 | 28 | 44 | 1 | 6 | 0 |
| 2026-08-06 | 28 | 42 | 2 | 4 | 0 |
| 2026-08-07 | 41 | 34 | 7 | 1 | 1 |
| 2026-08-08 | 16 | 38 | 2 | 2 | 0 |
| 2026-08-09 | 6 | 33 | 1 | 1 | 0 |
| 2026-08-10 | 1 | 2 | 0 | 0 | 0 |

Totals were 413 rows: 164 `allow_create`, 218 `allow_merge`, 13
`review_required`, 16 skipped merge side effects, and one `veto_create`.

### Identity reasons

| decision | reason | rows |
| --- | --- | ---: |
| allow_create | `no_identity_veto` | 164 |
| allow_merge | `same_ticket_source_update` | 112 |
| allow_merge | `same_event_update` | 106 |
| review_required | `merge_identity_blocking_conflict` | 7 |
| review_required | `specific_ticket_occurrence_conflict` | 5 |
| review_required | `merge_identity_llm_unavailable` | 1 |
| skip merge side effects | `unrelated_title_type_conflict` | 9 |
| skip merge side effects | `long_running_vs_single_slot` | 6 |
| skip merge side effects | `exhibition_vs_lecture_sibling` | 1 |
| skip merge side effects | `location_conflict` | 1 |
| veto_create | `deterministic_same_source_identity` | 1 |

### Downstream terminal evidence

- Window-created VK rows observed at audit time: 92 `failed`, 57 `imported`,
  122 `rejected`, one `deferred`, and one `pending`.
- Thirteen VK inbox carriers occurred in at least two import runs. Twelve rows
  currently marked failed/deferred already had both an imported Event pointer
  and mapping, proving a terminal-state regression.
- Daily VK `processed/imported/rejected/failed/deferred` counts were
  `32/15/13/3/1`, `42/16/8/15/3`, `54/12/5/28/9`, `40/6/6/26/2`,
  `27/11/1/10/5`, `12/2/0/8/2`, and `5/1/1/2/1` for August 4–10.
- Parser failure/skip totals were Dramteatr `44/4`, Muzteatr `32/0`, Sobor
  `2/0`, Tretyakov `0/4`, and Yantar Hall `3/4`. Parser event 5829 emitted
  eight source-binding conflicts on both August 8 and 9.
- Telegram Monitoring was not healthy-by-omission: since August 4 its current
  scan rows included 124 `skipped`, 43 `done`, and eight `partial` carriers.
  A second schema-first read-only audit identified five concrete legacy
  identity/technical-loss messages (16 extracted occurrences, one imported):
  one deterministic same-source gate, one location conflict, one
  long-running/single-slot conflict, and two merge blocking conflicts.
- Ten recently updated QTickets rows were still `active`, with zero attempts
  and errors; the runtime scheduler repeatedly reported that
  `ENABLE_TICKET_SITES_QUEUE` was not enabled, so this is not health proof.
- EventSource roles were 10,305 legacy null, 373 `identity_bearing`, and 103
  `context_only`; no blanket historical role/key inference is safe.
- Thirteen nonblank canonical-source collision groups represented 51 bindings;
  five groups mixed identity-bearing and context-only roles. Three current VK
  carriers legitimately represented multiple Event rows.

The redacted full baseline, SQL, runtime coverage, and checksums are retained
under
`artifacts/codex/INC-2026-08-10-smart-update-identity-terminal-loss/d2/`.
The runtime file mirror covered only `2026-08-08T02:37:34Z` onward; older
runtime claims rely on DB/ops evidence rather than pretending full log
retention.

### Final all-source recovery dry-run (read-only, 2026-08-10)

The recovery command was executed against production as
`--dry-run --since 2026-08-04 --batch-size 10000` with SQLite `mode=ro`,
`query_only=1`, and `quick_check=ok`:

| surface | eligible | selected | would change | changed |
| --- | ---: | ---: | ---: | ---: |
| durable candidate state | unavailable before migration | 0* | 0* | 0 |
| Telegram Monitoring identity/technical losses | 5 | 5 | 5 | 0 |
| legacy VK failed/deferred technical rows | 104 | 104 | 104 | 0 |
| official parser affected sources | 4 | 0** | 0** | 0 |
| ticket queue technical errors | 0 (14 already available) | 0 | 0 | 0 |
| festival queue technical errors | 0 (41 already available) | 0 | 0 | 0 |
| currently actionable aggregate | 109 | 109 | 109 | 0 |

`*` The candidate-state table does not exist in the current production schema;
the reported zero is schema-unavailable, not evidence of no durable work.
`**` Four official-parser sources (`dramteatr`, `muzteatr`, `sobor`, and
`yantarhall`) had 102 failed item observations in 14 parse runs. The new
source-level recovery-request table is additive and not deployed, so the CLI
correctly returned `deployment_required=true` and did not pretend to enqueue
them. After migration, its apply mode creates idempotent full-catalogue refresh
requests consumed automatically by the scheduled parser.

One selected Telegram row and 13 selected VK rows already had imported
pointers/mappings. Explicit imported/rejected/product-policy terminals were
excluded where the legacy schema exposes that evidence. The full currently
actionable batch fit with 9,891 rows remaining. No row was changed.

Redacted evidence and manifest:
`artifacts/codex/INC-2026-08-10-smart-update-identity-terminal-loss/d4c-all-source-recovery-dry-run/`;
manifest SHA-256
`cb75b598960968244353a8ff091436e21b5c624e8ccad128a54f5fd20e4adbf3`.
The exact CLI source SHA-256 was
`2cc2d19bd7024e12201fbb45574aa43c39cd6260c88e8dda50ab24e7ed485b78`;
`mode=ro`, `query_only=1`, and `quick_check=ok` were independently verified.

## Timeline

- 2026-08-04: the source/replay corruption incident established the initial
  identity regression contract.
- 2026-08-05–09: VK and source-parser runs repeatedly surfaced identity/source
  conflicts as failed, deferred, or skipped work.
- 2026-08-10: this terminal-loss incident was formalized; production audit,
  caller inventory, and implementation began from exact latest `main`.

## Root Cause

1. A human-review terminal was introduced into a product that has no human
   reviewer or operator queue.
2. Create and merge gates could veto canonical writes but had no required
   transition to automatic retry or distinct creation.
3. Smart Update returned free-form statuses and sometimes put a matched owner
   in `event_id`; callers inferred success from strings or ID truthiness.
4. Candidate identity and retries were not durable. A canonical source URL was
   treated as the identity rather than an envelope containing keyed child
   occurrences.
5. The identity decision ledger was not correlated one-to-one with attempts, so
   starts and automatic terminals could not be balanced.

## Contributing Factors

- Source parsing, Telegram, VK, ticket, festival, and manual ingestion each
  translated Smart Update outcomes independently.
- `context_only` mixed provenance role with ingestion intent and could discard
  a complete Event candidate.
- Process-local serialization did not provide a cross-process claim or retry
  authority.
- A uniqueness race and the final duplicate probe returned veto/review instead
  of rolling back and converging through the same automatic state machine.

## Automation Contract

### Treat as regression guard when

- changing Smart Update candidate matching, source binding, replay, retry,
  queue adaptation, typed results, or EventSource schema;
- changing any producer that derives a child occurrence/candidate key;
- changing the existing match/create bundle, dedup adjudicator, identity gates,
  or occurrence-scope stage.

### Affected surfaces

- `smart_event_update.py`, `smart_update_identity.py`, state/schema models and
  SQLite initialization;
- official source parsing, Telegram Monitoring, VK intake/auto queue, ticket
  sites, festival import, and manual/forwarded bot ingestion;
- Smart Update metrics, recovery, and incident audit tooling.

### Mandatory checks before closure or deploy

- the fourteen terminal/key/retry/caller/balance/recovery cases in the feature
  contract;
- all mandatory replay and repair checks from
  `INC-2026-08-04-smart-update-identity-source-replay-corruption`;
- raw-boundary multi-event and exact/edited replay fixtures with a negative
  control;
- fresh and legacy SQLite initialization, repeated initialization, uniqueness,
  `PRAGMA quick_check`, and rollback drill;
- full relevant CI plus explicit proof that no new model, provider, or LLM call
  was added;
- a production-snapshot recovery dry-run before any future apply.

### Required evidence

- exact base/branch/commit and focused PR;
- caller inventory and old-to-new terminal map;
- redacted production baseline and recovery dry-run;
- hermetic/relevant CI logs and incident replay receipts;
- after merge, an exact-`origin/main` deployed SHA and current-day catch-up if a
  scheduled slot was lost. This implementation request explicitly forbids
  deploy and production mutation, so those closure fields remain pending.

## Immediate Mitigation

- Production was audited read-only; no manual review queue, deploy, replay,
  repair, or production write was performed.
- PR #338 remains unmerged and was not continued because its operator-review
  design conflicts with the automatic product invariant.

## Corrective Actions

- [x] Make the five automatic outcomes the only public terminal result type.
- [x] Store accepted and diagnostic Event IDs in separate fields.
- [x] Persist candidate/occurrence identity plus balanced retry attempts and
  cross-process leases.
- [x] Allow one carrier URL to bind multiple keyed occurrences.
- [x] Separate `UPSERT_EVENT` from target-bound `ATTACH_CONTEXT`.
- [x] Migrate every discovered production caller and queue to the typed facade.
- [x] Add balanced funnel/audit metrics and idempotent recovery.
- [x] Rearm Telegram/VK legacy technical rows and enqueue actionable
  source-level official-parser refreshes; retain ticket/festival queue recovery.
- [x] Revalidate and accept a final-transaction duplicate in the same facade
  operation without a second LLM pass.

## Follow-up Actions

- [ ] After review/merge, deploy exact `origin/main` under the two incident
  contracts and execute the required recovery dry-run before an approved apply.
- [ ] After deploy, perform compensating catch-up for affected current-day
  scheduled imports and verify the canonical/public results.
- [ ] Keep the incident open until production recovery and exact-main release
  evidence are complete.

## Release And Closure Evidence

- initial audit/inventory base: `d7731ab4235b325e9ca52d13c45fba83eaf5de0b`
- final focused branch base: `origin/main`
  `96784bd572c03b965f303366c4ff0bb85d1b9a3f`
- integration branch: `integration/smart-update-identity-state-machine`
- rebased core/caller/recovery commits: `c0488d9ba`, `fb84b2308`,
  `be65b0ca3`; reconciliation commit: `3899970f4`
- draft PR: `#494`; implementation/rebase evidence head before PR metadata:
  `beda1283226d70059e7f9bf71d0f9a7b666d2461`
- deployed SHA: not performed (explicitly out of scope)
- production recovery apply: not performed (explicitly out of scope)
- post-rebase local regression checks: production-boundary compile PASS; full
  relevant suite `216 passed in 140.53s`; `git diff --check` PASS. Ignored log
  `artifacts/codex/INC-2026-08-10-smart-update-identity-terminal-loss/d5-local-validation/relevant-pytest-rebased.log`,
  SHA-256
  `369baa7a28dbe428a6afb6f4f848e9e909313a21cc791af523127aac0cca9041`
- GitHub current-head CI: run `31443999735` PASS for `python-ci`, dedicated
  `smart-update-identity-state-machine` job `93634283480`, and
  `static-browser-release-gate`; PR `#494` remains draft and unmerged
- post-deploy verification: pending

## Rollback

Roll back code first in a compatibility build that understands the additive
tables and keys. Keep new nullable EventSource columns, candidate state, and
attempt ledger: destructive SQLite column removal requires a table rebuild and
adds no safety. Once multiple keyed children share one source URL, do not
restore the old global URL-uniqueness assumption unless an explicit audit proves
one identity-bearing Event per canonical URL. Pause ingestion rather than run an
old binary that cannot represent the new bindings.

## Prevention

Stable child keys, an append-only attempt ledger, accepted-ID separation,
automatic retry/distinct-create convergence, exhaustive caller tests, and a
balanced five-terminal funnel become mandatory Smart Update regression rails.
