# Smart Update identity state machine

This is the canonical automatic-ingestion contract for Smart Update. The
product has no Event-review operator. Identity uncertainty is resolved by the
existing LLM-first identity stages, bounded automatic retry, and ultimately a
distinct Event when semantic identity cannot safely be established.

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
| `RETRY_SCHEDULED` | forbidden | a technical/provider/storage/identity uncertainty is durable for automatic retry |

A matched/conflicting Event may be recorded only as `diagnostic_event_id`.
Callers must never treat it as persisted success. Free-form
`review_required`, `skipped_identity_gate`, `skipped_context_only`, or terminal
generic `failed` are not public outcomes.

An exhausted infrastructure retry remains durable `RETRY_SCHEDULED` with no
next-attempt time and an exhaustion flag; it is observable/recoverable and is
never relabelled as a product rejection.

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
| uncertain/unknown provider result | schedule bounded retry; create distinct after the identity-uncertainty budget |
| LLM unavailable/invalid schema/vector/gate/DB exception | durable bounded retry; never review |

When the create gate finds a matched Event, that pair is passed through the
existing adjudication operation: same merges, distinct creates, and transient
failure retries. `VETO_CREATE` has no terminal product meaning.

The final duplicate/race probe reloads the authoritative Event. A race rolls
back the attempted canonical write and returns `RETRY_SCHEDULED`; the next
attempt uses the normal match/merge/create operation. No SQLite transaction is
held across an LLM await.

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
inferred safely. New identity-bearing uniqueness is scoped to canonical source
plus occurrence key, not the source URL globally. Context provenance may be
shared between targeted Events.

## Durable attempts and balance

SQLite owns the cross-process authority:

- `smart_update_candidate_state` keeps the unique candidate key, action,
  occurrence/source identity, replayable payload/locator, current terminal,
  accepted versus diagnostic IDs, bounded retry/lease fields, and timestamps;
- `smart_update_attempt` is append-only and unique by candidate plus attempt
  number. Each started attempt finishes with exactly one of the five public
  outcomes.

Internal claim states may be pending/running, but they are not public
terminals. Candidate registration makes an unresolved interruption durable for
automatic retry. A process-local lock is only an optimization; DB claims and
unique indexes are the authority.

The structured funnel reports both current candidates and attempts. Its
candidate balance is:

```text
candidates = created + merged + exact_noop + product_rejected + retry_scheduled
terminal_unresolved = 0
```

`terminal_unresolved != 0`, duplicate active leases, key collisions, or an
attempt without one terminal are readiness/incident failures.

## Caller and queue contract

All direct boundaries—official parsers, Telegram Monitoring, VK intake/auto,
ticket sites, festival intake, and manual/forwarded ingestion—consume the typed
result. They never branch on a free-form Smart Update status and never use a
diagnostic ID for downstream work.

- `CREATED`, `MERGED`, and `NOOP_EXACT_REPLAY` resolve a queue item
  successfully;
- `RETRY_SCHEDULED` remains available to the automatic durable worker and does
  not become terminal `failed`, review, or product reject;
- `REJECTED_PRODUCT_POLICY` records its explicit permanent reason;
- retry/reject results produce no Event publication, Telegraph, ICS, poster,
  festival-activity, notification, or outbox side effect.

## Recovery

`scripts/ops/recover_smart_update_identity_losses.py` provides bounded
`--since`, `--dry-run`, and `--batch-size` recovery. It recovers due durable
retry candidates and only conservatively rearms identifiable legacy technical
losses. Confirmed product-policy rejects, imported rows, and unrelated failures
are not reintroduced. The operation is idempotent; a second apply is a no-op.

Before any production apply:

1. run against a copied production snapshot;
2. inspect the structured dry-run and funnel balance;
3. run `PRAGMA quick_check`;
4. obtain explicit approval for the production mutation.

The 2026-08-10 task authorizes read-only production audit and recovery dry-run
only; it does not authorize an apply or deploy.

## SQLite migration and rollback

The migration is idempotent in `Database.init()`:

1. add candidate/occurrence/state columns to `event_source`;
2. create candidate-state and attempt-ledger tables/checks/indexes;
3. retain legacy null rows without blanket backfill;
4. replace global new-row source ownership with occurrence-scoped uniqueness;
5. fail readiness if a required invariant cannot be activated—never warn and
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
8. provider/schema/vector/DB retry and bounded distinct resolution;
9. legacy null role classification/retry;
10. multi-event source binding;
11. diagnostic-ID isolation at every caller;
12. ticket exact-noop completion;
13. funnel balance;
14. recovery idempotency.

The older Boyko/Pianissimo, Tretyakov occurrence, shared context, exact/edited
Telegram/VK replay, gate failure, repair, and Event 7151 negative-control tests
remain mandatory.
