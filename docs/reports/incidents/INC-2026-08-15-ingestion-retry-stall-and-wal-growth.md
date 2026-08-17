# INC-2026-08-15 ingestion retry stall and WAL growth recurrence

Status: open
Severity: sev1
Service: configured-source ingestion, Smart Update, Fly SQLite, StaticSiteBuilder handoff
Opened: 2026-08-15
Closed: —
Owners: events-bot production / ingestion / Smart Update / static-site pipeline
Related incidents: `INC-2026-07-27-future-event-source-coverage-drop`, `INC-2026-08-10-smart-update-identity-terminal-loss`, `INC-2026-08-12-data-volume-ingestion`, `INC-2026-08-14-cherryflash-terminal-lock-and-smart-update-visibility`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/vk-auto-queue/README.md`, `docs/features/smart-event-update/README.md`, `docs/operations/cron.md`, `docs/operations/kaggle-static-site-builder.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On August 15 the operator reported another production wave in which configured
source monitoring appeared to produce implausibly few new events and VK
auto-import visibly deferred rows to Smart Update retry. The same report raised
concern that official-source parser results were not reaching new canonical
events, due Smart Update retries might not be consumed, the SQLite WAL had
grown again, and StaticSiteBuilder remained constrained by the full local
SQLite snapshot required by the current immutable Kaggle handoff.

The investigation confirmed several independent regressions. Telegram's 133
"processed" messages mixed 110 forced replays and six metrics-only old rows
with only six event-bearing messages; all six were accepted (four creates, two
merges), so `4/133` was a misleading denominator rather than a 3% event yield.
At the same time, Telegram force rows, VK source evidence, Smart Update
candidates and the Sobor parser really did contain unbounded retry loops, and
the official full-parser slot had been skipped four days in a row.

The WAL spike was also reproduced and explained: the 12-hour full SQLite
`VACUUM`, hourly checkpoint and vector sync shared the same interval anchor.
`VACUUM` rewrote the roughly 648 MiB DB into WAL while the vector reader pinned
the checkpoint end mark, producing the observed roughly 687 MiB WAL. PR #506
default-disabled that maintenance job and Fly v1975 deployed the exact merged
main SHA. The ingestion-linear and compact static-projection fixes are being
validated for a subsequent exact-main release; the incident remains open.

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
  messages, four created events and two merges as implausibly low yield;
- a Telegram UI screenshot at 10:15–10:17 Europe/Kaliningrad (08:15–08:17 UTC)
  showed VK posts progressing through a 15-row batch. Post
  `wall-139238690_13534` was reported as LLM-confirmed
  no-event, while `wall-32547811_11187` was reported as an automatic Smart
  Update retry with reason `location_grounding_review:llm_keep`;
- the operator separately questioned whether theatres/official sources create
  new events and whether any Smart Update retry consumer actually drains the
  queue;
- live evidence recorded a roughly 687 MiB WAL and roughly 420 MiB available
  on `/data`; after the pinned reader ended, the next truncating checkpoint
  reduced WAL to roughly 4 MiB and restored roughly 1.06 GiB available.

Observability gaps are part of the incident: raw messages are not separated
from event-bearing carriers in the operator summary, retry messages do not show
their next claim/attempt/terminal fate, and the identity/age of a WAL-blocking
reader is not exposed.

## Timeline

- 2026-08-15 06:42 UTC — vector sync, full `VACUUM` and WAL checkpoint start on
  the same interval anchor; checkpoint times out after 30 seconds, `VACUUM`
  completes after 41 seconds and vector sync releases its reader at 06:43:51.
- 2026-08-15 07:42 UTC — the scheduled truncating checkpoint succeeds and
  returns WAL to its normal small baseline.
- 2026-08-15 08:15–08:17 UTC — the captured VK batch shows one explicit
  confirmed no-event and one `location_grounding_review:llm_keep` automatic
  retry among the visible rows.
- 2026-08-15 — incident workflow is reopened as a distinct recurrence rather
  than rewriting the August 10 or August 12 incident history.
- 2026-08-17 02:50–02:58 UTC — all-source parser ops 6167 processes all eight
  configured sources, but one accepted Sobor merge cannot acknowledge its
  Smart Update attempt (`candidate_state_ack_failed`, diagnostic event 7635).
  Six later Sobor/Tretyakov candidates fail registration as
  `candidate_state_unavailable`, and the Tretyakov missing-date recovery write
  also fails. Runtime mirror traces show `database is locked` only on the
  cached raw connection while ordinary ORM parser writes continue.
- 2026-08-17 — the same run exposes one separate product-state loss:
  Tretyakov's producer had already marked `РОССИЯ - ПУТИ ВРЕМЕНИ (12+)` as
  `no_dates` after checking both its calendar and detail page, but the host
  adapter discarded that disposition and scheduled a generic `missing_date`
  retry. The adapter now preserves the producer evidence and reports a
  separate `confirmed_no_active_schedule` terminal; unknown missing dates
  remain fail-closed.
- 2026-08-17 05:07–05:35 UTC — the post-deploy all-source qualification run
  6184 processes 231 occurrences from all eight official sources. Seven
  sources are terminally clean, but two Sobor rows re-enter extraction and
  return an empty draft. Both exact ticket URLs were already attached to the
  correct same-date/same-time Events; the canonical titles had only gained the
  presentation prefix `Концерт`. This is a false technical result, not missing
  source evidence, so the incident remains open.
- 2026-08-17 06:19–06:25 UTC — exact recovery ops 6193 re-drives all 17 VK
  rows that were previously `FAILED_TECHNICAL`. All 17 leave the queue with
  no lock, due retry or deferral; one real 18 August event is created as 7787,
  13 rows receive source-grounded product/no-event terminals, and three remain
  visible technical outcomes. The residuals are exact and reproducible: a
  grounded library address plus a non-verbatim reviewer citation, an online
  Chernyakhovsk stream whose city was omitted, and a three-child museum source
  whose accepted exhibition binding plus two date-less daily-excursion
  children were re-adjudicated by the anchor reviewer.
- 2026-08-17 — production inventory shows 4,818 pending VK carriers, including
  485 published in the last seven days. Five batches of 15 with the 4:1
  fresh/history selector provide only 60 fresh decisions/day, below the
  observed roughly 69 fresh carriers/day. This is a throughput deficit, not a
  safe queue reserve; production batch capacity is raised to 25 while exact
  catch-up remains a required live gate.
- 2026-08-17 06:53 UTC — PR #537 is deployed from clean exact merged main
  `2ed3713a18b19df17041ba1379185bda3a9d2461` as Fly v2004. Three health probes
  are HTTP 200 ready/db/disk ok, Fly is 1/1, `quick_check=ok`, WAL is empty,
  `/data` has about 1.01 GiB available, and production reads
  `VK_AUTO_IMPORT_LIMIT=25`.
- 2026-08-17 06:54–06:56 UTC — exact postdeploy replay ops 6195 resolves the
  grounded library carrier into existing event 7767 and correctly classifies
  the online-only municipal stream under the documented product exclusion.
  The museum exhibition binds and updates event 7690, but one excursion child
  still becomes technical because the occurrence-scope reviewer, before the
  already-fixed anchor reviewer, selected a block with no exact start date.
  This is a new exact residual; it is not accepted as a successful gate.
- 2026-08-17 07:07 UTC — PR #538 is deployed from clean exact merged main
  `6af244abb6b1fb952c2ea4bef8292b54a6560335` as Fly v2005. Exact replay ops
  6198 closes the remaining museum child as typed `missing_date`; its accepted
  exhibition binding stays attached to event 7690 and the carrier has zero
  failed/deferred/unresolved children.
- 2026-08-17 07:16–07:26 UTC — the first 25-row production qualification on
  v2005 creates six events, updates one and gives 18 source-grounded product
  terminals, with zero deferrals. One row is falsely reported technical:
  `wall-214027639_11760` had already committed event 7788 and EventSource
  12101288, but a later topics update waited 30 seconds and lost a concurrent
  SQLite write race. The outer facade discarded the accepted `event_id` and
  marked inbox 19775 `smart_update_processing_error`; this is an accounting
  and replay defect after a real accepted write, not a missing event parse.
- 2026-08-17 — queue-quality audit identifies a separate admission regression
  introduced by the August 11 raw-first change: the crawler continued to
  preserve every immutable source packet (correct), but also projected every
  fetched revision into `vk_inbox` (incorrect). Post-August-11 auto-import
  conversion consequently fell from the historical high-yield shape while
  obvious administrative, retrospective and past-only posts consumed the
  bounded 15/25-row product batch. Historical code confirms the former
  keyword+date gate; replaying the actual recent positive cohort shows nine of
  ten still pass it, while bare nominative `концерт` is the one regex miss.
- 2026-08-17 10:33–10:38 UTC — PR #540 merges as
  `bbb4e4ffa869fcade43ddb296fc20ea585bce789` after all three CI gates pass and
  deploys as Fly v2007 from clean exact main. Three public health probes are
  ready/DB/disk OK, Fly reports 1/1 passing, SQLite `quick_check=ok`, the three
  admission columns exist, `/data` has about 1033 MiB free and WAL is zero.
  The first 20-row dry-run changed no DB rows but exposed a standalone CLI
  bootstrap defect (`main.get_tz_offset` was not loaded); the recovery command
  is held until that entrypoint fix is merged and deployed.
- 2026-08-15 08:50 UTC — PR #506 merges as
  `c655156664edcfe91da11a4b9405d4fa59573f20` with all required CI checks green.
- 2026-08-15 08:54 UTC — Fly v1975 deploys exact merged main; startup records
  `VACUUM schedule disabled`, health is HTTP 200 / Fly 1 of 1, DB quick-check is
  `ok`, WAL is 24,752 bytes and `/data` has 1,119,805,440 bytes available.
- 2026-08-15 11:42–12:59 UTC — controlled Telegram catch-up ops run 5941 scans
  57 sources / 141 messages but closes as partial: 104 forced replays and 36
  metrics-only rows dominate the denominator; only one raw-new message arrives.
  The importer reports 101 visible terminal errors, zero creates and one merge.
- 2026-08-15 13:00 UTC — exact Kaggle log/output review attributes the failed
  linear run to 123 primary parse, 33 OCR and six verification calls rejected
  by the shared `tpm` limiter before provider send (log lines are duplicated in
  Kaggle output). Blank successful OCR and orphaned media-group force members
  are confirmed as two additional repeat amplifiers.
- 2026-08-16 — a fresh production audit separates actual accepted writes from
  scan denominators: Telegram creates/merges events but still leaves provider,
  evidence and Smart reviewer terminal errors; VK creates events but its six
  latest technical rows map to region/bundle/anchor review boundaries; the
  official parser creates events but under-reports them and leaves four exact
  canonical-source identity failures. The operator explicitly prioritizes
  zero lost new events; the static canary is deferred as a separate open
  obligation, not treated as ingestion success.
- 2026-08-16 — implementation adds same-claim Telegram quota/model recovery,
  uncapped scanned media and video evidence before parse, terminal-only final
  adjudication, Smart reviewer correction/source-grounded conservative
  fallbacks, recurring parser URL matching and authoritative parser create/
  update metrics. Merge, exact-main deploy and live compensating runs remain
  required before these changes may be called effective.
- 2026-08-16 12:15–12:47 UTC — all-source parser run 6024 processes 238 rows
  across all eight configured sources and creates six events, but leaves one
  Sobor and three Qtickets rows as visible `FAILED_TECHNICAL`; parser recovery
  is therefore still required.
- 2026-08-16 12:51 UTC — PR #514 is deployed from clean exact
  `origin/main@3458b549d326d373cb3181e3672c4b7c8b2c739c` as Fly v1983;
  Fly is 1/1, health is HTTP 200, `quick_check=ok`, WAL is about 2.4 MiB and
  `/data` has about 1.07 GiB available.
- 2026-08-16 12:54–12:56 UTC — exact four-row VK replay run 6033 proves the
  OCR/final-adjudication fixes on two rows (`RECAP_ONLY` and
  `NO_ATTENDABLE_EVENT`) but exposes two remaining boundaries: a 19.445-second
  TPM Retry-After was not honoured, and a later carrier failure hid a durable
  successful parse receipt, causing a duplicate successful `parse_key`
  attempt. Both are prevention blockers, not accepted product outcomes.
- 2026-08-16 13:51–13:53 UTC — after PR #516 / Fly v1985, exact inbox 19444
  still closes `FAILED_TECHNICAL`. A read-only reconstruction of the exact
  complete carrier (444 source chars plus four cached OCR blocks, 5,630 OCR
  chars) and provider `countTokens` proves 9,733 input tokens and a 16,422-token
  calibrated reservation against Gemma's 15,000 TPM bucket. This is an
  impossible admission, not a transient quota window. VK's explicit default
  model spelling had accidentally disabled the already-supported large-post
  Flash-Lite route.
- 2026-08-16 23:26 UTC — PR #530 is deployed from clean exact
  `origin/main@8ca032631ed0193abb25fd959c221f1fa6679832` as Fly v1999. The
  impossible historical VK range repair then closes inbox 15300 as typed
  `past_event`; a fresh five-row VK qualification closes all five rows with
  zero technical/deferred/unresolved outcomes.
- 2026-08-17 00:26–01:01 UTC — the one regular S22 Telegram result is adopted
  after deploy without a second Kaggle launch. Ops 6158 scans all 57 sources
  and imports 117 messages: 17 events created, 27 merged, no transport errors,
  but six message-level terminal errors keep the acceptance gate open. Exact
  child-state audit reduces real unexplained positive-child loss to
  `tretyakovka_kaliningrad/3618` and `kozia_gorka/1556`.
- 2026-08-17 02:13–02:15 UTC — after PR #531 / Fly v2000, exact bounded
  Telegram recovery imports create events 7773 and 7774 for those two positive
  children. Both ops 6164/6165 are successful with one create, zero merges and
  zero terminal errors; their exact scan ledgers are complete.
- 2026-08-17 02:23–02:45 UTC — a product-outcome audit (not a terminal-status
  audit) finds seven additional current/future Telegram children incorrectly
  rejected as `missing_location`: six films from `kldevents/3319,3321` at
  `Заря, Мира 41-43` and `№13` from `dramteatr39/4542`. The producer discarded
  the four-letter venue because its name-only token check ignored the exact
  address; it also treated two names resolving to the same maintained Drama
  Theatre row as conflicting. Production-shaped red tests reproduce both
  losses before the bounded producer fix.

## Root Cause

1. Smart Update deliberately clamped technical attempts below the configured
   maximum and kept `RETRY_SCHEDULED` due forever. A grounding verifier also
   treated a grounded low-confidence LLM `KEEP` as failure, so unchanged inputs
   could execute hundreds of times instead of reaching a terminal decision.
2. Telegram treated incomplete OCR/media evidence as a reason to recreate a
   force row. The next nightly monitor counted those old carriers as processed
   messages, rescanned them and recreated the same force state.
3. VK counted only non-empty OCR text blocks, not successfully processed blank
   OCR results, falsely marking ordinary photo evidence incomplete. Exact parse
   replay also reused a successful `parse_key` under a uniqueness constraint.
4. The official parser and nightly page sync were both scheduled at 02:30 UTC
   under a skip-on-heavy guard. The parser lost every full slot from August 12
   through 15; its day guard then restricted changed-source runs to the pending
   recovery subset. Sobor also looped on same-event source attachment conflicts.
5. Periodic full `VACUUM` was unconditionally scheduled at the same process
   anchor as vector sync and checkpoint. SQLite must rewrite the full DB, and
   the concurrent vector reader prevented WAL reset until it finished.
6. StaticSiteBuilder unnecessarily copied the full production DB on Fly into
   an immutable snapshot and runner dataset, copied it into Kaggle output, then
   downloaded it to Fly again. Yandex capacity was never the blocker; the Fly
   handoff could exceed 2 GiB although exporter-visible tables are much smaller.
7. The first linear Telegram remediation converted every producer technical
   result into a visible terminal result but did not wait for the shared
   limiter's pre-send minute `retry_after`. A normal high-volume catch-up thus
   terminalized carriers which had never reached OCR/LLM. Separately, blank
   successful OCR was counted as missing because only non-empty text blocks
   were retained, and a merged album cleared only its anchor force id.
8. Telegram source-profile restoration and location verification disagreed on
   evidence ownership. The producer restored the configured Tretyakovka venue,
   but the validator accepted LLM `keep` only when the full venue/address was
   repeated in the individual post; a valid museum-cinema announcement became
   `FAILED_TECHNICAL` despite the maintained high-trust source profile.
9. A shared source anchor set `anchor_forced` before the create identity gate.
   When the gate then named a concrete same-ticket/same-slot owner, the code
   suppressed the only typed same/distinct adjudicator. A separate yoga child
   in the same festival post therefore ended technical instead of receiving an
   LLM `create` decision.
10. The Telegram consumer calculated original producer indexes but then replaced
    `producer_ordinal` with the transformed local list position. A filtered
    replay of child N therefore became child zero and could reuse a sibling's
    candidate identity. This was reproduced at the full consumer/Smart/SQLite
    boundary and corrected to preserve the producer ordinal.
11. Telegram's final known-venue gate checked only significant name tokens.
    A short exact venue such as `Заря` produced no tokens and was dropped even
    with exact address `Мира 41-43` in the same carrier. Separately, the
    extractor's official long Drama Theatre name and its configured shorter
    name were compared as raw strings, despite both resolving to the same
    curated venue. Seven positive children therefore reached a product
    `missing_location` rejection instead of create/merge.
12. `Database.raw_conn()` returned one cached `aiosqlite.Connection` to every
    asyncio worker. Transaction ownership is connection-scoped, so concurrent
    `BEGIN IMMEDIATE`/commit sequences from Smart Update state, VK continuation,
    parser recovery and outbox work could interleave. Ops 6167 reproduced the
    failure: the accepted domain merge committed, but its separate state
    acknowledgement and six following candidate registrations used the
    contended shared connection and failed immediately. This is the same
    transaction-ownership class recorded by the July 27 parser incident, but
    the unsafe primitive remained available globally.
13. The Tretyakov notebook encoded a closed `no_dates` disposition, but
    `parse_theatre_json()` overwrote the producer's `source_type` with the host
    source name. The handler could therefore see only a missing date and
    created a recovery request for a catalogue item already proven to have no
    active occurrence.
14. The official-parser exact-source fast path checked presentation-title
    equality before honoring an already attached, identity-bearing exact
    ticket URL. A harmless `Концерт` prefix therefore sent two known Sobor
    occurrences back through extraction; an empty draft was then surfaced
    under the obsolete `RETRY_SCHEDULED` compatibility label even though no
    durable retry was created.
15. Location review tied a semantic LLM `keep` to the formatting quality of
    `evidence_quote` even when the candidate's exact address was already
    mechanically present in source evidence. The online region repair ignored
    full selected source text, so `#черняховск` could not restore a missing
    city for a VK livestream.
16. Anchor-role review had no closed `reject_missing_date` product action and
    could re-litigate date/range/time anchors already owned by the exact
    accepted occurrence binding. Separately, the production VK batch cap was
    lower than the measured fresh-carrier arrival rate after applying the
    required history fairness ratio.
17. Topic classification runs after the canonical Event/EventSource commit,
    but the create and merge callers did not isolate that derived observer.
    An ordinary concurrent SQLite writer made the topics commit time out; the
    facade then converted an already durable accepted event into a technical
    terminal with no accepted `event_id` in the VK batch receipt.
18. The August 11 raw-first migration coupled two different guarantees. It
    correctly persisted every fetched VK revision in `vk_source_packet`, but
    it also unconditionally upserted every revision into the expensive
    `vk_inbox` auto-import queue. Deterministic keyword/date/history fields were
    reduced to hints with no collection-time semantic fallback, so obvious
    non-events and past-only posts consumed the same bounded slots as likely
    future events. The historical keyword expression also omitted bare
    nominative `концерт`, proving that restoring the old regex as a rejection
    gate would create a recall loss.

Semantic eventness, venue, identity and merge/create decisions remain LLM-first.
Keyword/date logic may admit an obvious high-recall positive, but no
keyword/regex shortcut may reject a post: unresolved cases require the bounded
LLM admission decision and uncertainty must fail open.

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
- the production acceptance metric is not merely "terminal": across the
  current catch-up window every source-grounded new event must reach a DB
  `CREATED` or correct existing-event `MERGED/NOOP` receipt, with zero
  unexplained/technical child outcomes; `CONFIRMED_NO_EVENT` is valid only for
  a complete-evidence carrier that truly contains no event;
- semantic uncertainty is resolved in the current claim whenever complete
  evidence and the configured provider are available;
- a transient dependency may receive a bounded inline retry inside the current
  invocation; exhaustion becomes a visible terminal technical/needs-operator
  receipt, never a background product retry queue;
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
  DB diff showing created, merged, no-op, confirmed no-event and terminal
  technical states;
- reconcile Telegram scanned messages into event-bearing carriers, extracted
  children, accepted creates/merges, exact no-ops, confirmed no-events and
  technical retries. Use only `TELEGRAM_AUTH_BUNDLE_S22` for the production
  monitor and run no duplicate catch-up;
- replay the visible VK grounding case and a representative current/history
  batch; prove current carriers progress under backlog and every imported row
  has one terminal receipt;
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

PR #506 / Fly v1975 default-disabled periodic full `VACUUM` before its next
18:42 UTC collision. Production started with the expected disabled marker,
exact merged-main SHA, HTTP 200 health, `quick_check=ok`, roughly 1.04 GiB free
and a small WAL. No DB/WAL/snapshot or unknown artifact was deleted. Keep
`ENABLE_DB_FULL_VACUUM` unset; the opt-in path is maintenance-only.

## Corrective Actions

- [x] localize the first broken boundary independently for Telegram, VK,
  official parsers, Smart Update retry consumption and WAL checkpoint reuse;
- [x] deploy the independently safe WAL recurrence containment from exact main;
- [x] implement evidence-backed linear Smart/VK/TG/parser changes with focused
  regression replays, including positive/opposite VK controls through the full
  persist boundary on a production shadow copy; exact-main CI, deploy and
  production catch-up remain due;
- [x] replace the whole-DB StaticSiteBuilder handoff in the integration branch
  with a bounded immutable static-only projection and ephemeral Fly staging;
- [ ] merge/deploy the remaining exact-main prevention and complete bounded
  Telegram, VK, parser, Smart legacy-drain and static canary recovery.
- [x] reproduce cached raw-connection transaction aliasing with two concurrent
  `BEGIN IMMEDIATE` owners and implement per-context connections for every
  file-backed `Database.raw_conn()` call; focused DB/Smart/parser regression
  tests cover both generic transaction isolation and accepted-state
  acknowledgement waiting behind an unrelated writer.
- [x] preserve Tretyakov's explicit `no_dates` producer disposition separately
  from the canonical source name and terminalize it as a visible no-active-
  schedule product outcome; regression tests keep unexplained missing dates
  fail-closed.
- [ ] deploy the raw-connection isolation from exact main, reconcile the open
  ops-6167 attempt, and replay all seven Sobor/Tretyakov technical outcomes to
  accepted create/merge/no-op or an evidence-backed product exclusion with no
  open attempt/recovery row.
- [x] identify the post-deploy Telegram false-skip boundary from exact run 5941
  logs/output and implement pre-send inline quota wait, blank-OCR evidence
  cardinality and all-member album force settlement with focused tests;
- [ ] deploy that Telegram correction and repeat a bounded S22 catch-up with a
  carrier outcome balance and no unexplained source-evidence terminal wave.
- [x] implement the second-wave completion fixes for provider fallback,
  video/album evidence, final source adjudication, Smart review correction,
  exact region hints, recurring official-source identity and truthful parser
  create/update metrics; focused local regression suites are green.
- [x] merge PR #513 and deploy exact `origin/main@225a5ccf9` as Fly v1982;
  post-deploy health converged HTTP 200, DB quick-check passed and the WAL was
  small before the real qualification workload.
- [x] execute VK qualification ops run 6020: all 15 selected rows reached a
  terminal receipt and three new events were created, but four technical
  outcomes proved ingestion was not yet healthy (two OCR timeouts, one mixed
  lifecycle no-match masking a created sibling, one malformed parse/verifier).
- [x] implement the run-6020 residual corrections: bounded retryable OCR with
  per-image evidence preservation, one schema-strict same-invocation VK final
  adjudication, and explicit unmatched-lifecycle product no-op semantics.
- [x] merge/deploy the run-6020 residual corrections from exact main as PR
  #514 / Fly v1983.
- [x] re-drive the exact four carriers in run 6033; two closed correctly and
  two exposed exact provider-wait/parse-receipt preservation blockers.
- [x] implement bounded same-claim provider Retry-After and immutable
  successful-receipt preservation with focused regression tests.
- [x] merge/deploy the live-qualification follow-up as PR #515 / Fly v1984;
  exact receipt replay for 19488 then reached event 7694 without a duplicate
  parse, but 19444 exposed repeated coarse provider throttles and the lifecycle
  pre-pass exposed same-event source-row non-idempotency.
- [x] implement the resulting bounded multi-throttle loop, same-event exact
  source-row reuse and canonical-parser identity-adjudicator routing.
- [x] merge/deploy the PR #516 live residuals as exact main
  `b8277f5390145ac3349aa57194e20e359820ff42` / Fly v1985; scheduled run 6041
  then processed 15/15 terminal rows with four created events and zero
  technical/deferred outcomes.
- [x] merge/deploy the exact oversized-carrier route correction as PR #517 /
  Fly v1986 and re-drive inbox 19444 to a typed no-event terminal with zero
  technical/deferred outcomes.
- [x] run an all-source official-parser catch-up (ops 6050): 236 processed
  across eight sources, three created, ten updated, 223 unchanged, zero failed,
  retry or errors; the overdue Sobor recovery request reached `done`.
- [x] run bounded VK qualification ops 6057: 15/15 reached terminal receipts
  and five events were created, but one technical identity result exposed two
  real sibling films lost from a four-event source post.
- [x] deploy the same-source sibling identity correction and re-drive exact
  inbox 18558 to four distinct child bindings without duplicates.
- [x] deploy the exact-receipt routing correction exposed by recovery ops 6063:
  an unchanged packet must reuse its immutable successful parse when requested
  and actual routed model names differ, without another provider call or a
  duplicate `parse_key` failure.
- [x] deploy the positive-child boundary correction exposed by recovery ops
  6066: preserve the upstream `EVENTS_FOUND` decision into Smart Update and do
  not treat the generic word `кинопоказ` as identity between different films.
- [x] re-drive exact inbox 18558 on Fly v1989: all four film children now have
  distinct source occurrence bindings; two existing events were merged and
  the two previously lost siblings were created as events 7713 and 7714.
- [ ] deploy the standalone-city scope correction exposed by qualification ops
  6071, re-drive `wall-29891284_14297`, and repeat a bounded VK batch with zero
  technical outcomes.
- [ ] run one S22 Telegram catch-up and require a complete carrier/child outcome
  balance plus verified new DB writes before reporting ingestion healthy.
- [ ] deploy the exact-source roundup scope correction exposed by qualification
  ops 6099, re-drive `wall-149955604_24253`, and repeat the bounded VK gate
  until it has zero unexplained technical child outcomes.
- [ ] static-site projection canary remains open but is explicitly deferred by
  the operator; it must not block the ingestion deployment and must not be
  silently marked complete.
- [x] audit the regular 16/17 August S22 run by child outcome rather than raw
  message denominator: 17 creates and 27 merges are durable, while two real
  positive children remain technical and therefore keep ingestion unhealthy.
- [x] implement exact prevention for both remaining Telegram children: a
  configured source-profile venue can ground an LLM `keep` only when the post
  names no conflicting place, and a concrete identity-gate owner must reach
  the typed same/distinct adjudicator even under a source anchor. Filtered
  replays also preserve the producer child ordinal rather than rebinding it as
  child zero.
- [ ] merge/deploy that Telegram correction from exact main, re-drive the two
  exact children through the Telegram consumer, and require accepted/product
  terminals with no technical/deferred state.
- [x] make an already attached identity-bearing exact ticket URL authoritative
  for the same explicit date/time even when the canonical presentation title
  has gained a format prefix; normalize parser compatibility retry shapes to
  visible `FAILED_TECHNICAL` terminals.
- [x] merge/deploy the official-parser exact-source correction as PR #536 /
  Fly v2003, reconcile duplicate Sobor events 7636/7637 into canonical
  7299/7322, and qualify Sobor ops 6191 with 22 processed, two updated, 20
  unchanged and zero failed/retry/errors.
- [x] reproduce the remaining exact VK failures in ops 6193 and implement
  source-grounded KEEP citation recovery, single-place online city recovery,
  accepted-anchor replay stability and typed `reject_missing_date`.
- [ ] merge/deploy the final-decision correction, re-drive exact VK rows
  18129/18815/18911 and current-day pending carriers, and require zero
  technical/deferred/locked outcomes plus verified created/merged EventSource
  receipts.
- [ ] deploy the follow-up occurrence-scope terminal mapping, re-drive exact VK
  row 18911, and require the existing exhibition binding plus both date-less
  excursion children to finish without any technical/deferred/locked outcome.
- [ ] confirm the production 25-row cadence keeps fresh pending age bounded
  while the historical lane decreases; do not call the 4,818-carrier debt
  resolved merely because fresh items are prioritized.
- [x] reproduce the ops-6199 post-commit observer downgrade with a red
  create-path regression and protect both create and merge: topics failure is
  logged after the accepted commit and cannot change its terminal outcome.
- [ ] merge/deploy that observer isolation from exact main, replay inbox 19775
  through the supported VK boundary and require the existing event 7788 to
  settle as accepted exact replay with no failed/deferred/unresolved row.
- [x] localize the VK queue-quality regression to unconditional
  `vk_source_packet -> vk_inbox` projection and confirm against the June code
  and the actual recently accepted production cohort; fix bare `концерт` in
  the positive regex without using deterministic semantics for rejection.
- [x] implement collection-time hybrid admission while preserving the two
  schedules: raw packet first; deterministic future positive direct to queue;
  small batched LLM only for unresolved posts; grounded high-confidence
  `PAST_ONLY/NON_EVENT` outside the queue; all uncertainty/visual/provider/schema
  cases fail-open. The continuation path uses the same gate, and a bounded
  operator entrypoint requalifies legacy pending rows without parsing Events.
- [x] deploy the first admission release as Fly v2008 at exact main
  `c156839debd367493778f1059c7e81e5b87a1000`; three public health probes were
  ready with DB/disk OK, SQLite `quick_check=ok`, WAL zero and about 1033 MiB
  free. The first bounded recovery samples exposed a real acceptance bug: both
  newest and oldest 20-row batches rejected 0 rows because the code overrode
  grounded LLM decisions whenever any visual attachment existed (24/40 rows
  failed open for this reason/uncertainty). Keep the incident open and remove
  that blanket veto before continuing the backlog drain.
- [x] deploy grounded visual-safe admission as Fly v2009 at exact main
  `c4af46d5964db6a900ed5707db29fb267b7997c1`. A real separate scheduled crawl
  scanned 162 revisions, rejected 37 before queueing and admitted 35 new inbox
  rows; 29 uncertain rows failed open. Bounded legacy recovery then reduced
  selectable unclassified pending rows from 4,692 to zero and rejected 1,840
  grounded past/non-event rows, but exposed 955 additional unlocked `pending`
  rows carrying stale `review_batch` markers that the recovery selector alone
  skipped. Align the operator selector with auto-import: only an actual lock,
  not a stale batch label, protects a row.
- [ ] merge/deploy the VK admission gate from exact main, requalify the legacy
  pending cohort in bounded batches, then prove on a real crawl and subsequent
  separate scheduled auto-import that queue size/no-event share fall while all
  known recent positive controls still reach created/merged receipts.

## Follow-up Actions

- [ ] ingestion owner — expose one carrier/child funnel and oldest-due/attempt
  metrics consistently across Telegram, VK and official parsers;
- [ ] DB owner — expose WAL bytes, checkpoint tuple and oldest active reader;
- [ ] static owner — measure and document the minimal data projection required
  by the Kaggle build instead of treating whole-DB staging as self-evident;
- [ ] docs owner — keep active incident index generation from dropping current
  open or recently closed regression contracts.

## Release And Closure Evidence

- PR #536 merged as `ea5ed9815ba07a7586c095af0cb909870a311994` and was
  deployed through `scripts/deploy_fly_main.sh` as Fly v2003 from a clean
  exact-main worktree. Postdeploy health returned HTTP 200 three times, Fly was
  1/1, `quick_check=ok`, WAL was empty at the initial read and `/data` had
  about 1.04 GiB available. Sobor qualification ops 6191 then processed all 22
  rows with zero failed/retry/errors.
- exact VK failed-row replay ops 6193 processed 17/17 terminal rows with zero
  deferred/unresolved/locked rows, created event 7787, produced 13 valid
  product/no-event terminals and isolated three exact technical residuals.
  This is the pre-fix failure receipt for the final-decision patch, not a
  successful ingestion gate.

- mitigation deployed SHA: `c655156664edcfe91da11a4b9405d4fa59573f20`,
  PR #506, Fly v1975, clean exact `origin/main` via
  `scripts/deploy_fly_main.sh`;
- mitigation regression checks: GitHub `python-ci`,
  `smart-update-identity-state-machine` and `static-browser-release-gate` all
  passed; focused scheduler test passed locally;
- mitigation post-deploy verification: Fly 1/1, `/healthz` HTTP 200 ready/db/
  disk ok, exact in-image SHA, `VACUUM schedule disabled`, `quick_check=ok`, WAL
  24,752 bytes, `/data` available 1,119,805,440 bytes;
- predeploy VK shadow replay: raw `wall-32547811_11187` created one event/source
  through `vk_intake.persist_event_and_pages`; the opposite festival-as-venue
  control closed as `REJECTED_PRODUCT_POLICY/missing_location`. The copied
  production DB remained `quick_check=ok`; 32 legacy retry states and 15
  pre-existing open attempts did not increase. Ignored receipt:
  `artifacts/codex/INC-2026-08-15-vk-smart/prod-shadow-boundary-replay.json`;
- configured-source prevention: PR #513 merged as
  `225a5ccf933f7bce4438639bf87d9874556a6a29` and was deployed from a clean
  exact-main worktree as Fly v1982; `/healthz` returned HTTP 200 three times,
  Fly was 1/1, `quick_check=ok`, `/data` had about 1.10 GiB available and the
  post-start WAL was 609,792 bytes;
- VK qualification ops run 6020 processed 15/15 terminal rows, created events
  7692/7693/7694 and updated 7130, with `deferred=0`, but also produced four
  `FAILED_TECHNICAL` receipts. Those four exact carriers are the mandatory
  post-residual-deploy replay cohort; ingestion is not declared healthy yet;
- PR #514 merged as `3458b549d326d373cb3181e3672c4b7c8b2c739c` and Fly v1983
  runs that exact in-image SHA. Postdeploy `/healthz` is HTTP 200, Fly is 1/1,
  `quick_check=ok`, WAL is 2,504,992 bytes and `/data` available is
  1,147,731,968 bytes;
- all-source parser run 6024: 238 processed / eight sources / six created /
  five updated / 223 unchanged / four `FAILED_TECHNICAL` / zero deferred;
- exact VK replay run 6033: four processed / four terminal / two confirmed
  no-event / two `FAILED_TECHNICAL` / zero deferred. The remaining exact IDs
  are 19444 (provider TPM Retry-After) and 19488 (successful parse receipt
  hidden by later carrier terminal); neither is accepted as ingestion health;
- PR #515 merged as `40ea51fba58f0fa9bd3a3d65a2d46dc9ea2556bb`
  and was deployed from clean exact main as Fly v1984. Three `/healthz` probes
  returned HTTP 200, Fly was 1/1, `quick_check=ok`, WAL was 148,352 bytes and
  `/data` available was 1,149,550,592 bytes;
- exact VK replay run 6038 processed two terminal rows: immutable parse replay
  correctly imported/updated event 7694 for inbox 19488, while inbox 19444
  remained technical because two consecutive coarse 1ms TPM throttles exceeded
  the old one-retry implementation. This is evidence for the multi-throttle
  same-claim correction, not a successful ingestion gate;
- PR #516 merged/deployed as
  `b8277f5390145ac3349aa57194e20e359820ff42` / Fly v1985. Scheduled VK run
  6041 processed 15 rows, created events 7701–7704, produced 11 typed product
  rejections and zero technical/deferred/unresolved rows. Exact replay 6045
  still left inbox 19444 technical; exact read-only prompt reconstruction then
  proved `input_tokens=9733`, calibrated `reserved_tpm=16422`, while every
  registered Gemma project lane is capped at `tpm=15000`;
- PR #517 merged as
  `238dd5f3756b17812c461234a13455ca0f0ff518` and deployed from clean exact
  main as Fly v1986. Three health probes returned HTTP 200, Fly was 1/1,
  `quick_check=ok`, and `/data` had about 1.09 GiB available;
- exact oversized-carrier replay ops 6049 routed inbox 19444 to
  `gemini-3.1-flash-lite` and closed as
  `CONFIRMED_NO_EVENT:RECAP_ONLY`, with zero technical/deferred/unresolved
  outcomes;
- official-parser catch-up ops 6050 processed all eight configured sources:
  236 processed, three created, ten updated, 223 unchanged and zero failed,
  retry or errors; the Sobor recovery request is terminal `done`;
- bounded VK qualification ops 6057 processed 15/15 terminal rows and created
  events 7708–7712, but one `deterministic_same_source_identity` technical
  result proved real recall loss in `wall-53460968_11826`: the first and third
  titled films were created while the second and fourth sibling films sharing
  the same source/date/time/venue were vetoed. This is failure evidence, not a
  successful ingestion gate;
- PR #518 merged as
  `99782ea21e26e326948e631c4dd7e5d1216fa78a` and deployed from clean exact
  main as Fly v1987. CI and 339 focused Smart/VK regressions passed; Fly was
  1/1, three health probes returned HTTP 200, `quick_check=ok`, WAL was 49,472
  bytes and `/data` had about 1.10 GiB available;
- exact carrier recovery ops 6063 did not reach the sibling identity code:
  its immutable successful parse was stored under routed model
  `gemini-3.1-flash-lite`, while lookup required the configured requested model
  `models/gemma-4-31b-it`. The unchanged packet was redundantly parsed and its
  successful `parse_key` collided with the existing receipt. This is a newly
  proven replay-boundary blocker and the carrier remains failed technical;
- PR #519 merged as
  `7d98ca54282febb6a1398aaea69f05dd914e4550` and was deployed from clean exact
  main as Fly v1988. CI and 128 focused VK/Smart tests passed; Fly was 1/1,
  three health probes returned HTTP 200, `quick_check=ok`, WAL was 49,472 bytes
  and `/data` had about 1.10 GiB available;
- recovery ops 6066 proved parse replay itself fixed (`exact successful source
  parse replay packet=5063`, no source-parse provider call), then exposed two
  later loss boundaries: the VK persist adapter omitted the upstream positive
  source disposition, so Smart eventness rejected specific children because
  the carrier was a multi-film programme; and short titles `Малыш`/`Буратино`
  were falsely related only by the generic word `кинопоказ`. The carrier is
  still failed technical and this run is not accepted as recovery;
- PR #520 merged as
  `12563a55d7aa569c052c329d14682c009dea358c` and was deployed from clean exact
  main as Fly v1989. Exact recovery ops 6069 persisted the complete product
  result before its SSH stdout pipe closed: the source has four distinct
  occurrence bindings, existing events 7709/7710 were merged and missing
  siblings 7713/7714 were created. The ops transport receipt is `error`, so the
  DB/source-binding readback is the authoritative recovery evidence.
- bounded VK qualification ops 6071 processed 15/15 terminal rows, created
  events 7715-7717 and updated 7212/7681, with zero deferred/unresolved rows;
  one carrier still closed `FAILED_TECHNICAL`. Exact state 7423 shows a valid
  15 August museum excursion was rejected as
  `occurrence_scope_review:llm_scope_missing_target_city` because the generic
  substring check mistook `Калининградская область` in an unrelated exhibition
  title for an explicit mention of city `Калининград`. This is failure evidence
  and keeps the VK gate open.
- PR #521 merged as
  `d9809f8c3becf49a5a9d31b5f01509bdd0041b4a` and was deployed from clean exact
  main as Fly v1990. Three `/healthz` probes returned HTTP 200, Fly was 1/1,
  the callback validation endpoint returned the expected HTTP 400 contract,
  `quick_check=ok`, WAL stayed near 4 MiB and `/data` had about 1.10 GiB
  available. Exact replay ops 6078 closed the city-grounding carrier as a
  correct merge into event 7711 with zero technical/deferred/unresolved rows.
- bounded qualification ops 6079 then processed 15/15 terminal rows, created
  five events (7718-7722) and produced zero technical/deferred/unresolved
  outcomes. The next automatic scheduled batch, ops 6085, created event 7723
  and updated 3798 but exposed three new `FAILED_TECHNICAL` carriers. Readback
  proved two concrete evidence-availability boundaries: a stale VK/OK
  `first_frame` video preview returned HTTP 404, and one photo in each of two
  galleries exhausted three OpenAI OCR attempts. The strict incomplete-
  evidence guard then correctly refused to guess `CONFIRMED_NO_EVENT`; this is
  failure evidence and keeps the ingestion gate open until the same-claim
  visual fallback is deployed and all three exact carriers are replayed.
- PR #522 merged as
  `ff6f264a64e2b3a4983031071d8f5ca5ff92c299` and was deployed from clean exact
  main as Fly v1991. Fly was 1/1; three health probes returned HTTP 200; the
  validation callback returned HTTP 400; `quick_check=ok`; WAL was 325,512
  bytes and `/data` had about 1.10 GiB available. Exact replay ops 6093 still
  closed all three carriers `FAILED_TECHNICAL`: the primary OCR daily budget
  was already exhausted, so the provider-failure-only Google fallback was not
  reached, and both stale and freshly resolved VK/OK preview URL families
  returned HTTP 404. This is failure evidence, not recovery.
- Follow-up prevention now treats a primary daily-budget refusal as an inline
  fallback trigger and resolves short video evidence through user-token
  `video.get` low-resolution MP4 plus one bounded Google multimodal analysis in
  the same carrier claim. Unit/contract gate: 158 affected tests pass. Merge,
  deploy, exact replay of inbox 15304/17943/17991 and a fresh bounded scheduled
  qualification remain pending; no successful recovery is claimed yet.
- PR #523 merged as
  `3363d8c2d046214d4915f3a611da596262a0c046` and was deployed from clean exact
  main as Fly v1992. Exact replay ops 6098 closed inbox 15304/17943/17991 as
  three grounded product no-events with zero technical/deferred outcomes: two
  used the independently limited Google poster-OCR fallback after the primary
  daily budget was exhausted, and the video carrier used user-token
  `video.get` plus a 437,269-byte inline MP4 analysis.
- bounded VK qualification ops 6099 processed 15/15 terminal rows, created five
  events (7724-7728), updated/merged four event ids and left zero deferred or
  unresolved rows, but one of seven children from
  `wall-149955604_24253` closed `FAILED_TECHNICAL` as
  `occurrence_scope_review:llm_uncertain`. Six siblings completed. Readback
  proves the missing child is a complete typed 14 August 20:00 `Руки Вверх!`
  event under an exact source date heading/time/title, while the parser's
  `raw_excerpt` is a generated summary without the date. This is failure
  evidence and the VK health gate remains open.
- PR #524 merged as
  `b7ee76fb4d131c7bbe09fe89ab87217bc9b19e83` and was deployed from clean exact
  main as Fly v1993. The exact replay reached the new grounded source-line
  fallback and created the missing `Руки Вверх!` event 7729, but two already
  accepted sibling children (`NEW VERSION`, state 7442, and the Viktor Tsoi
  tribute, state 7447) then closed `FAILED_TECHNICAL` as
  `source_binding_conflict`. Their first attempts had already merged into
  events 7591 and 7539 and persisted unique candidate/occurrence bindings;
  the second, richer parse changed the packet fingerprint and title, fuzzy
  matching discarded the exact owner, and the create path collided with its
  own authoritative binding. This is failure evidence, not a successful gate.
- Prevention for that replay conflict now resolves the unique
  `(canonical_source_url, candidate_key)` owner before fuzzy title/venue
  matching; a changed packet updates the same child while a different
  candidate key remains a distinct sibling. A production-shaped regression
  reproducing the `Рок-хиты` -> `NEW VERSION` title refinement is green, along
  with 148 focused Smart/VK tests. Its merge/deploy/replay receipt follows.
- PR #525 merged as
  `8e08db05ec4670ee2138ed7c01af55462e4fcb51` and was deployed from clean exact
  main as Fly v1994. Exact VK replay ops 6115 processed the one carrier with
  zero technical/deferred/unresolved rows: all seven children now have unique
  authoritative bindings; states 7442/7447 merged back into events 7591/7539.
- Telegram recovery import ops 6117 adopted the already-terminal COMPLETE
  remote result without launching a second kernel. It processed 56 messages
  (3 new raw + 53 forced replays), created 21 events and merged 64, with no
  transport errors. It nevertheless left two proven real children as
  `FAILED_TECHNICAL`: film `1+1` from `zaryakinoteatr/964` because a sibling
  title had been written as venue before scope validation, and exhibition
  `Цветные сны немолодого романтика` from `koihm/6041` because the nominative
  catalogue venue did not match its ordinary Russian locative form in the
  source. This is failure evidence; the Telegram gate remains open.
- Follow-up prevention restores the configured/extracted Telegram venue before
  multi-event scope validation, rejects sibling titles as venues, and accepts
  an inflected multi-token venue only when every significant token is present
  in source evidence. Focused regression tests cover both exact lost children.
  Merge, exact-main deploy, bounded exact-carrier replay and the regular 21:40
  UTC monitoring slot remain pending.
- PR #526 merged as
  `fb0b7ce31c8a3501fc8e7dd6b2c8469c3b993415` and was deployed from clean exact
  main as Fly v1995. The first exact state replay closed the original scope
  failure and created event 7751, but exposed a later independent product bug:
  generic-title recovery treated the exact film title `1+1` as weak and renamed
  the event to the carrier programme `Большое кино`. Event 7751 is not accepted
  as correct recovery evidence until the exact title is restored through the
  ingestion boundary.
- Follow-up prevention treats a compact symbolic/numbered title as authoritative
  when that exact title is present in child/source/OCR evidence, so title
  recovery cannot replace it with a sibling or programme title. PR #527 merged
  as `bfff9a1244fcabd97d1410898150cbfe65e4632f` and was deployed from clean
  exact main as Fly v1996; Fly was 1/1 and three health probes returned HTTP
  200 with about 1.05 GiB free on `/data`.
- The first v1996 exact packet replay correctly produced
  `NOOP_EXACT_REPLAY`, proving idempotency, but could not repair the historical
  event title because the accepted packet fingerprint had not changed. A
  source-corrected replay (same exact child, configured venue restored) reached
  the unique `candidate_key` owner, then the generic merge gate incorrectly
  called that owner a programme sibling and left state 7205
  `FAILED_TECHNICAL:source_binding_conflict`; event 7751 still read
  `Большое кино`. This is new failure evidence, not closure.
- Follow-up prevention now treats the existing unique child binding as final
  identity for changed packets: it bypasses the generic merge gate and permits
  only an exact source-grounded title to repair that same Event. A regression
  reproduces `Большое кино` -> `1+1` with one EventSource and no retry. PR #528
  merged as `922812dca4ce8a99d03762ef96bb4d7cf9756548` and was deployed from clean
  exact main as Fly v1997. The corrected replay closed state 7205 as `MERGED`
  and restored event 7751 to exact title `1+1`, venue `Заря`, address
  `Мира 41-43`, with one EventSource and no retry.
- targeted current-day Qtickets parser catch-up ops 6139 completed `success`:
  36 processed, 2 created, 34 unchanged, zero failures/retries/errors. The
  three older Qtickets `FAILED_TECHNICAL` attempts are superseded by accepted
  current-occurrence Events with exact canonical source bindings; they are not
  active missing-event work.
- bounded VK qualification ops 6143 processed all 5 selected rows terminally:
  2 imported carriers, 3 typed product exclusions, zero technical/deferred or
  unresolved rows. It created one valid current event 7755, but also created
  stale 2022 events 7756/7757 from historical inbox row 15300. Root cause is a
  regression in Smart Update: `_should_skip_past_smart_update_candidate`
  emitted only a `possible_past_event` log hint and continued into create.
  These two rows are invalid product output and must be repaired through the
  ingestion boundary; ops 6143 is failure evidence, not a successful gate.
- prevention restores the existing typed product contract: an automated child
  whose extracted occurrence has fully ended before the current local date
  terminates in the same claim as `REJECTED_PRODUCT_POLICY/past_event`, creates
  no Event/EventSource and schedules no retry. A production-shaped regression
  uses the exact historical source `wall-216003600_16`. Merge, exact-main
  deploy, repair/replay of inbox 15300, the regular 21:40 Telegram slot and a
  fresh bounded VK qualification remain pending.
- PR #529 merged as
  `62b9048dc52036d3a2f5e3d52e059afe8b4a61d9` and was deployed from clean exact
  main as Fly v1998. Public cleanup retracted both erroneous Telegraph pages
  and removed ICS channel message 8300; the two bad Events were backed up and
  removed. Exact replay then exposed a second date-normalization defect before
  it could be accepted as recovery: both candidates had old start
  `2022-11-10` but fabricated end `2026-12-10`, so the ordinary `end < today`
  guard did not fire and the first child was recreated. The replay was stopped;
  its open second attempt remains explicit incident work. Follow-up prevention
  classifies an automated range longer than two years with a past start as the
  same typed `past_event` product exclusion. The Telegram scheduled slot must
  not be interrupted by another deploy; merge/deploy/replay continues after
  that exact-once run is safely handed off.
- PR #530 merged as
  `8ca032631ed0193abb25fd959c221f1fa6679832` and was deployed from clean exact
  main as Fly v1999. Three health probes returned HTTP 200, Fly was 1/1,
  `quick_check=ok`, WAL was about 280 KiB and `/data` had about 1.02 GiB
  available. Exact inbox 15300 replay closed both old children as typed
  `past_event`; fresh bounded VK ops 6157 closed five of five without technical,
  deferred or unresolved results.
- regular S22 Telegram recovery-import ops 6158 completed after the remote run
  scanned all 57 sources. Of 117 messages, 113 were new raw and four metrics-only;
  61 had typed candidates. The run created 17 events, merged 27 and reported no
  transport errors. Six message terminals were audited individually: the only
  two unexplained positive-child losses are state 7639
  (`tretyakovka_kaliningrad/3618`, configured venue rejected as absent from the
  post) and state 7765 (`kozia_gorka/1556`, distinct yoga child blocked by the
  festival owner's shared ticket/slot). This is failure localization, not an
  ingestion-health claim.
- predeploy production-shadow replay at 2026-08-17 01:55 UTC used a read-only
  online backup of `/data/db.sqlite` under `/tmp` and loaded the patched
  Telegram consumer/Smart code from an isolated overlay. Both exact failed
  children crossed Telegram result -> Smart Update -> SQLite and became two
  new events (`created=2`, `merged=0`, `terminal_errors=0`), event count moved
  7363 -> 7365 and `quick_check=ok`. The shadow and overlay were removed;
  production remained HTTP 200/Fly 1/1 with 1028 MiB available and an 11.4 MiB
  WAL. Receipt:
  `artifacts/codex/INC-2026-08-15-tg-terminal-completeness/prod-shadow-boundary-replay.json`
  (local ignored evidence; production catch-up is still pending).
- PR #533 merged as
  `578c528009342f2128e7350c50cfec851e4f67f3` and was deployed from clean exact
  main as Fly v2001. It isolates file-backed `Database.raw_conn()` calls and
  makes the official parser's `no_dates` result terminal. Exact Telegram
  boundary recovery ops 6170/6172/6173 created one and merged six lost event
  children with zero errors. Official Sobor+Tretyakov catch-up ops 6176 then
  completed `success`: 30 processed, three created, four updated/merged,
  22 unchanged, one confirmed no-event and zero retry/errors.
- Scheduled VK qualification ops 6178 processed 15/15 rows to finite receipts:
  four events created (7779-7782), two updated, nine product/no-event outcomes,
  zero deferred/unresolved and one `FAILED_TECHNICAL`. The sole technical row
  is historical inbox 15296 / `wall-216003600_20`: source publication and
  packet evidence identify a course starting 2022-11-10, but the candidate
  reached `anchor_role_review` before the existing later `past_event` guard and
  failed `llm_evidence_not_verbatim`. State 7828 is closed and not retrying,
  but this is not an acceptable ingestion result because the deterministic
  product exclusion was already knowable.
- PR #534 merged as
  `6506fa847c2459471c94867103a22fcd1e1e560d` (deployment pending together with
  the next prevention patch). It closes a separate Telegram force-row leak:
  after a proven successful source scan, a forced message absent from the
  returned carrier set becomes visible terminal
  `source_message_unavailable_after_successful_scan`; a failed/unproven scan
  retains the row. This preserves evidence instead of silently discarding or
  looping deleted source messages.
- current prevention moves the unequivocal fully-past guard before
  occurrence/anchor/location semantic review and retains the later repeated
  guard after semantic narrowing. The production-shaped regression uses exact
  source `wall-216003600_20`, proves it would otherwise enter anchor-role
  review, and requires `REJECTED_PRODUCT_POLICY/past_event`, no Event or
  EventSource, no retry and no LLM review. Merge/deploy and exact inbox 15296
  replay remain pending.
- all-source qualification ops 6184 processed 231 rows across all eight
  official sources: 223 unchanged, five updated, one confirmed no-event, zero
  newly created, zero durable retry and zero run-level errors. It is still
  `partial` because `(Нео)Органика 3.0` and `Духовное восхождение` returned
  false extraction terminals despite their exact URLs already being attached
  to canonical Events 7299/7322. Duplicate cards 7636/7637 are separately
  visible and require bounded canonical repair; this run is failure evidence,
  not the final parser gate.
- remaining prevention deployed SHA: pending;
- parser ops 6167 failure evidence: seven item-level technical failures, one
  open Smart state (`7805`, `attempt_started`) and a failed Tretyakov
  missing-date recovery write; runtime mirror stack traces point to
  `BEGIN IMMEDIATE` on the shared cached raw connection. Prevention is local
  and tested but not yet deployed, so none of these rows is recovery evidence;
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
