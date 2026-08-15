# Smart Update identity state machine

This is the canonical automatic-ingestion contract for Smart Update. The
product has no Event-review operator. The 2026-08-15 owner decision supersedes
the background-retry part of the 2026-08-10 contract: one invocation now makes
the LLM-first semantic decision and closes it. Identity uncertainty resolves
inline to a distinct Event; infrastructure uncertainty becomes a visible
technical terminal rather than invisible queued work.

Related regression contracts:

- `INC-2026-08-04-smart-update-identity-source-replay-corruption`;
- `INC-2026-08-10-smart-update-identity-terminal-loss`.

## Public result type

Every submitted candidate receives exactly one public result per attempt:

| terminal | accepted Event ID | meaning |
| --- | --- | --- |
| `CREATED` | required | a distinct canonical Event was inserted |
| `MERGED` | required | the packet was accepted against an existing Event |
| `NOOP_EXACT_REPLAY` | required | the same candidate key and packet fingerprint were already accepted |
| `REJECTED_PRODUCT_POLICY` | forbidden | a confirmed permanent product exclusion, with an explicit reason |
| `FAILED_TECHNICAL` | forbidden | provider/schema/storage failure exhausted the current invocation and needs an explicit re-drive/incident action |

A matched/conflicting Event may be recorded only as `diagnostic_event_id`.
Callers must never treat it as persisted success. Free-form
`review_required`, `skipped_identity_gate`, `skipped_context_only`, or terminal
generic `failed` are not public outcomes.

The identity-uncertainty budget is inline: one normal adjudication followed by
at most one distinct-create resolution in the same facade call. Provider and
storage failures are never relabelled as product rejection, but they also never
enter an automatic background queue. `RETRY_SCHEDULED` is retained in SQLite
only as a provisional/legacy recovery value and is not a new public result.

## Intents

- `UPSERT_EVENT`: an identity-bearing Event candidate enters the existing
  match/create/merge pipeline.
- `ATTACH_CONTEXT`: requires `target_event_id`, implies `context_only`, and
  attaches only source provenance. It does not run identity/mutation LLM work,
  alter Event fields, or schedule public projections.

`source_role` describes provenance. It does not silently turn a complete
upsert candidate into a skip.

## Identity resolution

No model, provider, or semantic LLM call is added by this state machine. It
reuses:

1. `occurrence_scope_text` review when already required;
2. the match/create bundle;
3. the widened dedup adjudicator;
4. deterministic evidence already supplied to the create/merge identity gates.

Deterministic code is restricted to stable key construction, exact replay,
explicit vendor occurrence incompatibility, source ownership, schema/lease
invariants, and retry routing. It does not replace semantic matching with broad
keywords or regexes.

| semantic/evidence result | automatic action |
| --- | --- |
| `SAME_EVENT` / `SOURCE_UPDATE` | merge and return `MERGED` |
| `RELATED_BUT_DISTINCT` | create distinct |
| `FESTIVAL_CONTEXT_SIBLING` | create distinct |
| `UNSAFE_TO_MERGE` or blocking structural conflict | create distinct |
| different explicit same-vendor occurrence IDs | create distinct |
| semantic uncertain/unknown identity result | run the distinct-create resolution once inline |
| LLM unavailable/invalid schema/vector/gate/DB exception | `FAILED_TECHNICAL`; explicit re-drive only |

When the create gate finds a matched Event, that pair is passed through the
existing adjudication operation: same merges, distinct creates, and transient
failure closes visibly as technical. `VETO_CREATE` has no terminal product meaning.

The final duplicate/race probe reloads and revalidates the authoritative Event
inside the same facade operation. A confirmed duplicate attaches the keyed
source packet and returns `MERGED` without another LLM pass; a reload that
disproves the stale match proceeds with distinct `CREATED`; a missing row or
storage failure rolls back and returns `FAILED_TECHNICAL`. It never emits a
veto. No SQLite transaction is held across an LLM await.

## Candidate and occurrence identity

Keys are derived before the first provider call:

- `candidate_key` identifies a producer envelope child across retries and
  edits;
- `occurrence_key` identifies the source-local occurrence.

Producers should supply a stable child ordinal and an explicit vendor
occurrence ID when available. The fallback uses canonical source identity plus
structured date, time, end date/scope and child ordinal. Free LLM prose alone is
never the identity key. A same-time sibling needs a distinct producer child
slot; a key collision is retryable rather than semantic merge permission.

The packet fingerprint is separate from both keys:

- same canonical source + same key + same fingerprint →
  `NOOP_EXACT_REPLAY`;
- same source + same key + changed fingerprint → accepted update/merge;
- same source + different key → a distinct Event may be created.

`EventSource` stores nullable candidate/occurrence/state linkage. Legacy rows
stay null because historical identity/context roles and child slots cannot be
inferred safely. New keyed ownership is unique by
`(canonical_source_url, candidate_key)`; the accompanying
`(canonical_source_url, occurrence_key)` invariant detects producer child-slot
collisions. Neither treats the source URL alone as a new-row owner. Context
provenance may be shared when its targeted producers supply different stable
child keys.

## Durable attempts and balance

SQLite owns the cross-process authority:

- `smart_update_candidate_state` keeps the unique candidate key, action,
  occurrence/source identity, replayable payload/locator, current terminal,
  accepted versus diagnostic IDs, bounded retry/lease fields, and timestamps;
- `smart_update_attempt` is append-only and unique by candidate plus attempt
number. Each started attempt finishes with exactly one of the five public
outcomes.

Internal claim states may be pending/running, but they are not public
terminals. Candidate registration still uses the legacy `RETRY_SCHEDULED`
value provisionally so a crash cannot erase the payload. An expired interrupted
row is selected by the bounded post-deploy legacy drain exactly once and ends
accepted, product-rejected, or `FAILED_TECHNICAL`; normal runtime does not poll
it forever. A process-local lock is only an optimization; DB claims and unique
indexes are the authority.

The Event/EventSource domain transaction never spans an LLM await. If a process
loses the short attempt acknowledgement after an accepted domain commit, the
caller keeps the accepted result (it must not regress an imported queue row to
failed/deferred). The provisional row remains available for the controlled
legacy drain or next exact packet replay, which closes the interrupted ledger
row and converges to `NOOP_EXACT_REPLAY`; it is not polled indefinitely.

The structured funnel reports both current candidates and attempts. Its
candidate balance is:

```text
candidates = created + merged + exact_noop + product_rejected + failed_technical + legacy_retry_scheduled
terminal_unresolved = 0
```

`terminal_unresolved != 0`, duplicate active leases, key collisions, or an
unfinished attempt whose lease has expired are readiness/incident failures.
The production audit reports candidate balance, retry due/exhausted, accepted
ID contract violations, attempt starts/terminals/unresolved, and orphaned
attempts separately.

Runtime controls retain `SMART_UPDATE_MAX_ATTEMPTS` only for compatibility.
`SMART_UPDATE_RETRY_WORKER_ENABLED` is default off and may be enabled only for
the controlled one-time legacy drain;
`SMART_UPDATE_RETRY_INTERVAL_SECONDS` (default 60), and
`SMART_UPDATE_RETRY_BATCH_SIZE` (default 25). Disabling the worker is an
explicit operational override, not a terminal-state fallback.

## Changed terminal outcomes

| old boundary result | automatic result now |
| --- | --- |
| `review_required` / diagnostic `event_id` | inline semantic resolution; known distinct evidence converges to `CREATED`, technical uncertainty to `FAILED_TECHNICAL` |
| create-gate `VETO_CREATE` / `skipped_identity_gate` | existing dedup adjudication in the same operation: `MERGED`, `CREATED`, or `FAILED_TECHNICAL` |
| merge `skip_merge_side_effects` for a known sibling/conflict | distinct `CREATED` in the same facade attempt; no worker/delay dependency |
| `skipped_context_only` | explicit `UPSERT_EVENT` or target-bound `ATTACH_CONTEXT` |
| generic Smart Update `error`/caller `failed` | visible `FAILED_TECHNICAL` |
| no-change/status aliases | `NOOP_EXACT_REPLAY` only for identical key plus fingerprint; accepted same-event no-change is `MERGED` |
| confirmed non-event/out-of-region/past/policy exclusion | `REJECTED_PRODUCT_POLICY(reason)` |

## Caller and queue contract

All direct boundaries—official parsers, Telegram Monitoring, VK intake/auto,
ticket sites, festival intake, and manual/forwarded ingestion—consume the typed
result. They never branch on a free-form Smart Update status and never use a
diagnostic ID for downstream work.

The frozen AST inventory and migration gate are maintained in
[`caller-inventory.md`](caller-inventory.md).

- `CREATED`, `MERGED`, and `NOOP_EXACT_REPLAY` resolve a queue item
  successfully;
- `FAILED_TECHNICAL` records an observable non-product failure and requires an
  explicit bounded re-drive; it is not selected by an automatic worker;
- `REJECTED_PRODUCT_POLICY` records its explicit permanent reason;
- retry/reject results produce no Event publication, Telegraph, ICS, poster,
  festival-activity, notification, or outbox side effect.

## Recovery

`scripts/ops/recover_smart_update_identity_losses.py` provides bounded
`--since`, `--dry-run`, and `--batch-size` recovery. It releases due durable
candidate claims and conservatively rearms identifiable legacy technical losses
across Telegram Monitoring, VK, ticket-site, festival, and official-parser
surfaces. Telegram rows become idempotent force-message requests. Parser losses
become one source-level `source_parser_recovery_request`; the scheduled parser
claims it, bypasses the unchanged-page guard, performs a full current-catalogue
refresh, and keeps an incomplete source due. Confirmed product-policy rejects,
imported/rejected queue terminals, and unrelated failures are not reintroduced.
The operation is idempotent; a second apply is a no-op.

The final pre-migration production dry-run on 2026-08-10 selected 109 actionable
rows in a 10,000-row batch: five Telegram identity/technical losses and 104 VK
failed/due-deferred rows. One Telegram and 13 VK selections already had imported
mappings, so accepted replay convergence is required. Four parser sources had
102 failed item observations in 14 runs; because the additive parser recovery
queue was not yet deployed, the report explicitly returned
`deployment_required=true` instead of claiming those sources were requeued.
Ticket and festival queues already had 14 and 41 available rows respectively.
Durable candidate recovery was schema-unavailable, not a real zero. The strict
read-only run changed zero rows.

Before any production apply:

1. run against a copied production snapshot;
2. inspect every per-surface structured dry-run section and funnel balance;
3. run `PRAGMA quick_check`;
4. obtain explicit approval for the production mutation.

The 2026-08-10 task authorizes read-only production audit and recovery dry-run
only; it does not authorize an apply or deploy.

## SQLite migration and rollback

The migration is idempotent in `Database.init()`:

1. add candidate/occurrence/state columns to `event_source`;
2. create candidate-state and attempt-ledger tables/checks/indexes;
3. create the source-level official-parser recovery-request queue;
4. retain legacy null rows without blanket backfill;
5. replace global new-row source ownership with candidate-key and
   occurrence-key scoped uniqueness;
6. fail readiness if a required invariant cannot be activated—never warn and
   continue without it.

Rollback is code-first. Keep the additive schema and deploy a compatibility
build. Do not restore global URL uniqueness after multiple keyed children have
been accepted unless a separate audit proves one identity-bearing Event per
canonical URL. SQLite column removal requires a destructive table rebuild and
is intentionally not part of the normal rollback.

## Regression matrix

The required automated suite covers:

1. exact keyed replay;
2. changed keyed packet update;
3. two occurrences in one carrier;
4. different same-vendor ticket occurrences;
5. related-but-distinct;
6. festival-context sibling;
7. unsafe/structural conflict;
8. provider/schema/vector/DB technical terminal and inline distinct resolution;
9. legacy null role classification/retry;
10. multi-event source binding;
11. diagnostic-ID isolation at every caller;
12. ticket exact-noop completion;
13. funnel balance;
14. recovery idempotency.

The older Boyko/Pianissimo, Tretyakov occurrence, shared context, exact/edited
Telegram/VK replay, gate failure, repair, and Event 7151 negative-control tests
remain mandatory.
