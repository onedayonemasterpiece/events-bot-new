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
- [ ] merge/deploy those final live residuals and re-drive inbox 19444 plus the
  four run-6024 parser carriers to zero technical/unresolved outcomes.
- [ ] run one
  S22 Telegram catch-up, bounded current/history VK drain and all-source parser
  catch-up, and require zero unexplained/technical outcomes plus verified new
  DB writes before reporting ingestion healthy.
- [ ] static-site projection canary remains open but is explicitly deferred by
  the operator; it must not block the ingestion deployment and must not be
  silently marked complete.

## Follow-up Actions

- [ ] ingestion owner — expose one carrier/child funnel and oldest-due/attempt
  metrics consistently across Telegram, VK and official parsers;
- [ ] DB owner — expose WAL bytes, checkpoint tuple and oldest active reader;
- [ ] static owner — measure and document the minimal data projection required
  by the Kaggle build instead of treating whole-DB staging as self-evident;
- [ ] docs owner — keep active incident index generation from dropping current
  open or recently closed regression contracts.

## Release And Closure Evidence

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
- remaining prevention deployed SHA: pending;
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
