# INC-2026-09-19 Telegram Monitoring runtime starvation

Status: open
Severity: sev1
Service: Telegram Monitoring / Smart Update source ingestion
Opened: 2026-09-19
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-06-12-tg-monitoring-deploy-crash-no-watchdog`, `INC-2026-09-13-guide-vk-monitoring-user-token-flood`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/operations/cron.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Telegram Monitoring stopped delivering source candidates to Smart Update after
the 2026-09-07 local-day import. The scheduled Kaggle kernel still starts and
emits heartbeats, but the configured Gemma primary began returning repeated
503/high-demand responses and timeouts. Retries inside the shared provider
client were multiplied by a second retry loop in the serial notebook before it
tried the healthier Gemini fallback. The scan therefore no longer finishes all
57 sources within Kaggle's execution window. Because source cursors advance
only after the final result bundle is imported, each failed run replays the
same growing backlog.

## User / Business Impact

- From 2026-09-08 through 2026-09-18 Europe/Kaliningrad, production recorded
  zero Telegram Smart Update attempts and zero Telegram `event_source` imports.
- New canonical events fell from 307 in 2026-09-01..07 to 133 in
  2026-09-12..18, a decline of 174 events (56.7%).
- Mutually exclusive Smart Update `CREATED` outcomes explain the decline:
  Telegram fell 132 to 0 and accounts for 75.9% of the total loss; VK fell
  139 to 99 and accounts for 23.0%; parser sources fell 36 to 34 and account
  for 1.1%.
- Later sources in the fixed alphabetical scan order are starved entirely;
  failed runs repeatedly spend their lifetime rescanning the early sources.

## Detection

- The operator reported a fall in the number of new events after VK managed
  announcement recovery work began.
- The canonical `event.added_at` series localized the step change to 8
  September, while `smart_update_attempt` localized it to `source_type=telegram`.
- `ops_run(kind='tg_monitoring')` shows daily timeout/error runs with zero
  processed messages after the last successful recovery import on 7 September.
- `kaggle_run_ledger` proves the kernels continued heartbeating in `scan`; this
  is runtime starvation, not a scheduler-start or Telegram-auth outage.

## Timeline

- 2026-09-06 21:41 UTC: scheduled run exceeded the host wait window but the
  kernel completed at 03:19 UTC; recovery import on 7 September delivered 103
  messages and four created events.
- 2026-09-07 21:40 UTC: first unrecovered run started. It reached source 56 of
  57 after about 11 hours, then stopped at the provider execution limit without
  writing the final report.
- 2026-09-08 local day: Telegram Smart Update attempts and Telegram
  `event_source` imports dropped to zero.
- 2026-09-08 through 2026-09-18: daily scheduled/catch-up runs either timed out
  after about 286 minutes or failed early; no final source result was imported.
- 2026-09-18 21:40 UTC: current run started. At the 2026-09-19 diagnostic
  snapshot it was still `RUNNING`, had completed only 16 of 57 sources after
  about 8.5 hours, and had accumulated 106 messages before source 17.
- 2026-09-19 06:37 UTC: operator cancelled the stalled run. Kaggle became
  terminal, but its exact `telegram_session:s22` lease remained active until
  the three-hour TTL because recovery did not recognize `CANCEL_ACKNOWLEDGED`
  or reconcile terminal leases before its result grace window.
- 2026-09-19 06:49 UTC: a no-import canary started for `agropark39` and
  `ambermuseum`, limited to two messages per source. The first source alone
  required about 7m44s of processing; the complete canary required 13m20s,
  scanned four messages and extracted seven events without importing them.
  Its de-duplicated log contains six primary-to-fallback transitions, four
  quota waits and two outer transient recoveries.

## Root Cause

1. The Kaggle producer scans all enabled Telegram sources serially and writes
   `telegram_results.json` only after the complete 57-source loop.
2. The server imports results and advances durable source cursors only after
   that final bundle exists; there is no per-source or bounded-shard durable
   checkpoint.
3. Per-message LLM latency crossed the fixed runtime budget. The cancelled
   kernel log contains, after removing duplicated transport lines, 559 Gemma
   call errors versus 250 successes, including explicit 503/high-demand and
   timeout failures. Gemini fallback recorded 29 call errors versus 184
   successes. The notebook then added 77 outer transient recoveries and 171
   inline quota waits totalling about 69 minutes on top of provider-client
   retries. A baseline processed 87 messages across all 57 sources in about
   3h48m; the degraded run reached only source 17 after about 8.6 hours.
4. Each failed all-or-nothing run leaves cursors unchanged, so the backlog is
   replayed and grows. This converts a transient throughput slowdown into a
   persistent ingestion outage.

## Contributing Factors

- Host timeout is computed from source count, not observed message count or
  remaining work, and is capped below the provider runtime.
- Health treats live heartbeats as progress even when projected completion is
  beyond the provider deadline.
- Fixed source ordering repeatedly favors early sources and starves later ones.
- The status ledger exposes source progress but not provider failures; the
  retained Kaggle output had to be downloaded to attribute the slowdown.
- Terminal cancellation aliases differed between the session guard and
  Telegram recovery, leaving a verified-dead run's resource lease active.
- No alert fired on zero Telegram imports or zero Telegram Smart Update
  attempts for more than one daily window.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring scan order, cursor persistence, Kaggle result
  format, source sharding, provider deadlines/retries, recovery import, or
  critical scheduler timeout/watchdog behavior.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py::main` and `scan_source`
- `source_parsing/telegram/service.py` launch, poll, result download/import,
  recovery, and cursor advancement
- `telegram_source.last_scanned_message_id`
- `ops_run(kind='tg_monitoring')`, `kaggle_run_ledger`, `kaggle_run_event`
- Smart Update source type `telegram` and downstream publication fanout

### Mandatory checks before closure

- Replay the captured production backlog through the production import boundary
  on a snapshot/shadow DB; unit tests alone are insufficient.
- Enforce per-message/provider deadlines with typed retry evidence; do not add
  keyword-based semantic shortcuts.
- Verify a normal production run imports fresh Telegram candidates and produces
  Telegram Smart Update attempts again.
- Keep stale catch-up announcements from entering VK/Telegram public fanout;
  ingestion recovery and public announcement freshness need separate evidence.

### Required evidence

- Deployed SHA reachable from `origin/main` and Fly image produced through
  `scripts/deploy_fly_main.sh`.
- Production `ops_run` plus `kaggle_run_ledger`/event evidence for a bounded
  successful run.
- Production DB evidence for fresh Telegram `event_source` rows and accepted
  Smart Update results.
- Public/ledger evidence that only fresh eligible announcements fan out.
- Retained diagnostic artifact:
  `/home/dev/artifacts/events-bot-new/20260919T050649Z-vk-publishing-runtime-logs/`.

## Immediate Mitigation

- No second Telegram Monitor was launched while the current kernel owned the
  shared S22 Telegram session.
- No stale Telegram backlog was replayed into public announcement fanout.
- The cancelled run's exact stale lease was released through the existing host
  failure reconciler only after Kaggle terminality was rechecked.
- A two-source canary uses the production notebook with import replaced by a
  local capture, so it cannot create events or enqueue public announcements.
  Baseline result: four messages scanned, seven events extracted, import=false
  in 13m20s.

## Corrective Actions

- Preserve Gemma primary with the existing Gemini fallback. The initial
  Gemini-primary change was not authorized and is being reverted at the
  operator's direction; reserve Gemini primarily for Smart Update.
- Stop nested retry multiplication: one physical provider send per model in the
  shared client, one short quota wait, then the explicit fallback.
- Recognize all Kaggle cancellation terminal aliases and immediately reconcile
  only the exact run's resource lease; retain the existing result grace window.
- Keep the existing one/two-source and per-source message limit as the release
  canary rather than adding a second orchestration system.

## Follow-up Actions

- [ ] Consider bounded, restart-safe source shards only if the retry/model fix
  does not restore the normal sub-two-hour run.
- [ ] Add a zero-Telegram-import alert after the incident is stable.
- [ ] Plan a freshness-filtered catch-up that cannot publicly announce stale events.
- [ ] Verify the first fresh post-fix Telegram import and Smart Update acceptance.

## Release And Closure Evidence

- deployed SHA: `953218e576ddc789a27f3318b8e2e202492b3e3a`, reachable from
  `origin/main`; Fly machine version 2072, image
  `deployment-01M2W7QK37JYG58ARAAGJNTHC2`.
- deploy path: `scripts/deploy_fly_main.sh --remote-only`, clean `main`.
- regression checks: 126 targeted service/recovery/producer/notebook tests
  passed; Python compilation, notebook JSON and provider-path audit passed
  (`unapproved=0`, `allowlisted_debt=0`).
- post-deploy health: ready, DB healthy, machine health check passing; paid
  volume size unchanged. Static-site tree unchanged.
- Operator cancelled the old-notebook watchdog catch-up
  `catchup-tg-monitoring-99ee3b7b37ad4e4599546e2da7765699`; Kaggle terminality
  was verified and the existing exact-run reconciler released one session
  lease. The corrected canary holds the existing cross-process monitor lock.
- post-fix canary: `canary-provider-fix-20260919-after-cancel`, same two sources
  and two-message limit, no import/publication; passed the runtime check.
  Kernel log duration fell from 799.8s to 146.0s (5.48x faster). All four
  source/message IDs match the baseline. The log contains 13 unique successful
  provider requests, zero provider errors, zero quota waits and zero fallbacks.
  Notebook scan ran 07:21:16–07:22:54 UTC; exact session lease released at
  07:22:56 UTC. Captured stats: two sources, four messages, eight candidate
  events; nothing imported or published.
- Quality comparison is not a blanket semantic approval: all six dated
  Agropark occurrences retain the same titles/dates/times. The undated
  oil-tasting poster changed from no-event to one undated candidate, and the
  ongoing museum exhibition now has its explicit 20 September end date with
  no invented start date. These require consumer-side checks before treating
  all eight candidates as publishable events. Canary output is retained and
  was not replayed into production.
- Evidence directory: retained artifact subdirectory
  `tg-canary-postfix-20260919T0721Z/`; local and Fly SHA256 values match:
  results `7c9831ac5e4b3a59621156dc1d9ae6621a0c5af6ce2a477a30d739772d5e5229`,
  kernel log `0cfc60e5479b0322b972955dd4628d85381fad707bbccc11136b9d94c69f6434`,
  status events `bba24f4da56c193dac825dd0d3b943bd0873327ffad1c7de275cce7bc9a207ed`.
- A normal all-source run completing within two hours and fresh Smart Update
  acceptance remain unverified; the incident stays open.

## Prevention

### Real Gemma-primary import and public readback, 19 September

- Runtime restored to Gemma primary at `4a5ea7daed79e797aae70ff450aadc744d686628`
  (Fly image `deployment-01M2W96TZFSK2MSATE9DYTDJ3M`); no Smart Update model change.
- Real run `fresh-gemma-e2e-20260919-post2249`, ops 8996, processed the current
  announcement `https://t.me/agropark39/2249`: 6 extracted children, 3 creates
  (9158–9160), 1 merge (9152), 1 rejected and 1 durable identity retry. It took
  1004.693 seconds including Smart Update; the producer alone took 230.53 seconds.
- Event 9158 actually published in Telegram at 07:52:13 UTC:
  `https://t.me/kldevents/4205`, verified with the shared human E2E session.
- VK API readback verified the same new event live at 08:03 UTC:
  `https://vk.com/wall-231920894_11060`, `post_type=post`, photo present.
  Its postponed id was 11057; a stored postponed link alone is not acceptance.
- This is not full closure. Live inspection found sibling description bleed,
  a lost producer disposition in the TG candidate adapter, and a scope router
  missing same-day/shared-date programmes. The 19-and-20 date list also exposed
  an incomplete deterministic date-grounding helper. Fixes remain LLM-first;
  date normalization does not decide eventness/identity.
- Approved CDN media existed for 9152, but an early media-worker return left
  its old Telegraph dependency waiting on next-day optional pair review.
- 128 targeted tests pass for the bounded handoff/scope/date/media fixes.
  Production replay/deploy evidence will be appended after verification.
- Isolated real-provider replay created all three 19 September occurrences.
  The 20 September String Art occurrence exposed a further false retry:
  `FINAL_RETRY/distinct_not_grounded` despite the LLM's explicit
  `distinct_occurrence/session_split_keep` decision. The prompt allowed
  paraphrased facts while validation required literal quotes; supplied OCR was
  also absent from that validator's corpus. The correction tightens quotation
  instructions and validates supplied OCR, preserving fail-closed negatives.
- Evidence retained at
  `/home/dev/artifacts/events-bot-new/20260919T075901Z-tg-publication-end-to-end-repair/`.

### Operator correction: model policy and end-to-end acceptance

The 146-second no-import canary was Gemini-primary and is not acceptance for
Gemma-primary throughput or production delivery. Restore only the model-order
change, retaining bounded retries and exact cancelled-lease reconciliation.
Do not change Smart Update model settings. Acceptance requires a fresh bounded
Gemma-primary source run through the real Smart Update import, persisted event
rows and verified Telegram/VK publication URLs. Never rearm the contained old
VK queue to obtain publication evidence. Incident remains open.

Closure requires a fresh bounded canary and a normal production import within
the existing runtime budget. Increasing the timeout is explicitly not a fix.
