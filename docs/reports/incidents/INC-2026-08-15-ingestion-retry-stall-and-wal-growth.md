# INC-2026-08-15 ingestion retry stall and WAL growth recurrence

Status: open
Severity: sev1
Service: configured-source ingestion, Smart Update, Fly SQLite, StaticSiteBuilder handoff
Opened: 2026-08-15
Closed: —
Owners: events-bot production / ingestion / Smart Update / static-site pipeline
Related incidents: `INC-2026-08-10-smart-update-identity-terminal-loss`, `INC-2026-08-12-data-volume-ingestion`, `INC-2026-08-14-cherryflash-terminal-lock-and-smart-update-visibility`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/vk-auto-queue/README.md`, `docs/features/smart-event-update/README.md`, `docs/operations/cron.md`, `docs/operations/kaggle-static-site-builder.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On August 15 the operator reported another production wave in which configured
source monitoring appeared to produce implausibly few new events and VK
auto-import visibly deferred rows to Smart Update retry. The same report raised
concern that official-source parser results were not reaching new canonical
events, due Smart Update retries might not be consumed, the SQLite WAL had
grown again, and StaticSiteBuilder remained constrained by the full local
SQLite snapshot required by the current immutable Kaggle handoff.

This is an initial incident record, not a final diagnosis. Telegram's raw
message count is not an event-bearing denominator, the screenshot alone does
not prove that a retry is abandoned, and the previously reported disk/WAL
figures have not yet been re-measured in the current investigation. Root cause,
current backlog balance and the exact affected source set remain provisional.

## User / Business Impact

- current event announcements may be missing or delayed across Telegram, VK
  and official-source ingestion;
- an operator sees VK candidates leave the linear import report as an
  automatic retry without a visible terminal receipt;
- source-level scan success can look green while carrier children remain
  deferred, merged, rejected or otherwise absent from the public catalogue;
- renewed WAL or snapshot pressure could again reduce `/data` operating margin
  and block a fresh static-site build.

## Detection

- the operator challenged a reported Telegram run with 57 sources, 133
  messages, four created events and two merges as implausibly low yield; the
  event-bearing/extracted/no-event denominator is not yet available;
- a Telegram UI screenshot at 10:15–10:17 showed VK posts progressing through
  a 15-row batch. Post `wall-139238690_13534` was reported as LLM-confirmed
  no-event, while `wall-32547811_11187` was reported as an automatic Smart
  Update retry with reason `location_grounding_review:llm_keep`;
- the operator separately questioned whether theatres/official sources create
  new events and whether any Smart Update retry consumer actually drains the
  queue;
- a previous status summary reported roughly 420 MiB free and a roughly
  687 MiB WAL. These numbers are unverified inputs to the current incident and
  must be replaced by timestamped live filesystem and SQLite evidence.

Observability gaps are part of the incident: raw messages are not separated
from event-bearing carriers in the operator summary, retry messages do not show
their next claim/attempt/terminal fate, and the identity/age of a WAL-blocking
reader is not exposed.

## Timeline

- 2026-08-15, before 10:15 UTC — the operator reports unexpectedly low
  Telegram event yield and questions VK/parser/Smart Update progress.
- 2026-08-15 10:15–10:17 UTC — the captured VK batch shows one explicit
  confirmed no-event and one `location_grounding_review:llm_keep` automatic
  retry among the visible rows.
- 2026-08-15 — incident workflow is reopened as a distinct recurrence rather
  than rewriting the August 10 or August 12 incident history. Evidence
  collection and root-cause localization are in progress.

## Root Cause

Not yet proven. The following are investigation hypotheses only and must not be
treated as accepted fixes or closure evidence:

1. a Smart Update verification/grounding state may be returning a positive
   child to durable retry without a prompt claim or bounded terminal receipt;
2. Telegram and parser summaries may hide child-level loss behind source/run
   success counters;
3. scheduler heavy-job contention may have delayed one or more official-source
   obligations;
4. a long-lived SQLite reader or equivalent checkpoint starvation may again be
   preventing WAL reset/reuse;
5. the current whole-SQLite immutable snapshot contract may consume more Fly
   staging capacity than the public static projection actually needs.

Semantic eventness, venue, identity and merge/create decisions remain LLM-first.
No keyword/regex shortcut is accepted as a remedy for this incident.

## Contributing Factors

- operator reports expose scanned/processed counts without a complete
  carrier-to-child balance;
- a durable retry protects uncertain work from loss but becomes user-visible
  degradation when due rows lack a bounded, observable consumer;
- the August 10 record and changelog retained historical “Draft / not deployed”
  wording after PR #494 had merged, obscuring the actual production baseline;
- the latest incident-index auto-sync removed the August 10, 12 and 14
  regression entries, weakening automatic routing to their contracts;
- the August 12 WAL-reader observability and owner-backed static capacity
  follow-ups were still open when this recurrence was reported.

## Target Behaviour

- every admitted event-bearing child reaches one explicit accepted create,
  merge, exact no-op or source-grounded confirmed no-event outcome;
- semantic uncertainty is resolved in the current claim whenever complete
  evidence and the configured provider are available;
- only a genuinely transient technical dependency may remain due, and every
  such row has a visible next attempt, owner and bounded terminal receipt—no
  indefinite or unclaimed retry loop;
- Telegram, VK and official parsers report the same carrier/child outcome
  vocabulary so source success cannot hide event loss;
- WAL reuse stays bounded across real ingestion, and the static handoff uses a
  capacity-safe immutable input whose identity and coverage are verifiable.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring extraction/import, VK queue selection/import,
  official-source parsers or their scheduled catch-up paths;
- changing Smart Update verification, retry claims, retry scheduling,
  occurrence identity or accepted-result reporting;
- changing SQLite connection lifetime/checkpoint behavior, `/data` health
  floors or high-volume writers;
- changing the StaticSiteBuilder snapshot/export/input-fingerprint, Kaggle
  handoff, adoption, publication or retention path;
- deploying or executing production recovery on any of those surfaces.

### Affected surfaces

- `kaggle/TelegramMonitor/`, `source_parsing/telegram/`, `vk_auto_queue.py`,
  `vk_intake.py`, `source_parsing/`, `smart_event_update.py`, `scheduling.py`;
- production carrier/candidate/attempt/recovery ledgers, `ops_run`, JobOutbox
  and configured-source cursors;
- `/data/db.sqlite`, WAL/SHM, runtime file mirror and `/healthz` disk checks;
- `static_site_build_state`, immutable snapshot/export, Kaggle status ledger,
  checked build result and Yandex Object Storage publication;
- Telegram operator reports, `/start`, webhook and Private Events MCP smoke.

### Mandatory checks before closure or deploy

- preserve minimal raw offending Telegram/VK/parser artifacts in
  `tests/replays/INC-2026-08-15-ingestion-retry-stall-and-wal-growth/` or a
  linked fixture and replay them through the same production import boundary
  plus `smart_event_update.py` on a production snapshot copy/shadow DB;
- include at least one positive and one negative/opposite control, with pre/post
  DB diff showing created, merged, no-op, confirmed no-event and retry states;
- reconcile Telegram scanned messages into event-bearing carriers, extracted
  children, accepted creates/merges, exact no-ops, confirmed no-events and
  technical retries. Use only `TELEGRAM_AUTH_BUNDLE_S22` for the production
  monitor and run no duplicate catch-up;
- replay the visible VK grounding case and a representative current/history
  batch; prove current carriers progress under backlog and every row has one
  terminal or bounded-due receipt;
- account for each configured official parser source and any missed current-day
  slot; record processed, created, merged/updated, confirmed no-event and due
  counts, then perform the required compensating catch-up after deploy;
- prove the due Smart Update cohort is claimed and drains without overdue,
  ownerless or indefinitely repeated rows; accepted `CREATED`/`MERGED` retry
  results retain the August 14 exactly-one operator-report contract;
- run the applicable August 10 typed-ingestion identity/state-machine suite,
  provider-path audit, fresh/legacy SQLite init×2, uniqueness/conflict probes,
  quick-check and replay/rollback rehearsal;
- before any checkpoint containment, capture WAL bytes, checkpoint
  `(busy, log, checkpointed)` evidence, active reader/end-mark evidence and
  oldest-reader age. A manual `wal_checkpoint(TRUNCATE)` alone cannot close the
  incident; WAL must stay bounded through a real write/crawl window;
- capture `df`/`du`, Fly volume/snapshot inventory, `/tmp` fsync probe and
  `PRAGMA quick_check=ok`; do not delete unknown/canonical data;
- if the whole-DB static snapshot remains, complete the August 12 capacity-backed
  exact-main canary. If it is replaced by a minimal immutable projection, prove
  source coverage, canonical hash/fingerprint, frozen-clock consistency,
  adoption/restart identity, checked Kaggle output and create-only Yandex
  publication with no root mutation;
- verify public `/healthz` ready/db/disk, Fly 1/1, webhook, live `/start`,
  Private Events MCP and exact clean deployed `origin/main` SHA.

### Required evidence

- original operator screenshot and exact source URLs/packet revisions;
- timestamped Telegram/VK/parser/Smart Update funnel and retry-age/attempt
  inventories before and after the fix;
- raw replay fixtures, commands, pre/post DB diff and positive/negative-control
  receipts;
- runtime file mirror excerpts correlated by run/ops/job/source/time, plus
  SQLite/WAL/checkpoint/reader and disk evidence;
- exact branch, commit, PR, merged-main SHA, CI checks, Fly version and
  in-container SHA;
- same-day catch-up terminal receipts and static build/publication receipt;
- explicit proof that no production DB, WAL/SHM or unknown artifact was
  deleted to manufacture capacity.

## Immediate Mitigation

No new mitigation is claimed in this initial record. Do not launch overlapping
heavy catch-ups, delete storage evidence, force a checkpoint before reader
evidence is captured, or convert semantic uncertainty into a deterministic
terminal skip while root-cause collection is in progress.

## Corrective Actions

- [ ] localize the first broken boundary independently for Telegram, VK,
  official parsers, Smart Update retry consumption and WAL checkpoint reuse;
- [ ] implement only evidence-backed changes with failing regression replays;
- [ ] decide, document and verify whether StaticSiteBuilder continues to use a
  whole SQLite snapshot or a minimal immutable public projection;
- [ ] deploy exact merged main and complete bounded current-day recovery.

## Follow-up Actions

- [ ] ingestion owner — expose one carrier/child funnel and oldest-due/attempt
  metrics consistently across Telegram, VK and official parsers;
- [ ] DB owner — expose WAL bytes, checkpoint tuple and oldest active reader;
- [ ] static owner — measure and document the minimal data projection required
  by the Kaggle build instead of treating whole-DB staging as self-evident;
- [ ] docs owner — keep active incident index generation from dropping current
  open or recently closed regression contracts.

## Release And Closure Evidence

- deployed SHA: pending;
- deploy path: pending, exact clean `origin/main` required;
- regression checks: pending;
- post-deploy verification: pending;
- catch-up and backlog terminal receipts: pending;
- WAL bounded-write-window evidence: pending;
- exact-main static canary: pending.

The incident remains open until root cause is proven, prevention is deployed,
the current-day obligations are compensated, offending raw replays pass, the
retry cohort reaches bounded terminal receipts, WAL remains bounded under real
load, and a fresh exact-main static build completes.

## Prevention

Pending investigation. This section will record only implemented and verified
guardrails; no proposed architecture is presented as shipped behavior.
