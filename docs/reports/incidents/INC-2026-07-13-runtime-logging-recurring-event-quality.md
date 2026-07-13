# INC-2026-07-13 Runtime logging disabled during recurring event-quality regressions

Status: open
Severity: sev1
Service: production observability / VK auto-import / Smart Update / future-event quality
Opened: 2026-07-13
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-04-16-prod-disk-pressure-runtime-logs.md`, `INC-2026-05-05-cherryflash-disk-full.md`, `INC-2026-07-07-new-event-quality-degradation.md`, `INC-2026-07-10-future-event-semantic-audit.md`, `INC-2026-07-11-event-vector-sidecar-sync-stalled.md`, `INC-2026-07-12-autoretro-one-day-exhibition-location-period.md`
Related docs: `docs/operations/runtime-logs.md`, `docs/operations/e2e-testing.md`, `docs/features/smart-event-update/README.md`, `docs/operations/release-governance.md`

## Summary

Repeated future-event defects were being audited and repaired while production runtime file logging was disabled. The earlier disk incident was contained by turning observability off, but the durable correction did not combine a hard log budget, retention and a free-space floor. This left daily Smart Update/import investigations without the decision stream needed to separate legacy bad rows from fresh regressions.

The operator also rejected the temporary E2E authorization blocker: the approved E2E Telegram identity may receive a reversible production admin grant for the bounded live import check. This incident therefore owns volume cleanup, bounded permanent logging, live Telegram-UI E2E and a new complete future-event audit.

## User / Business Impact

- wrong dates, periods, locations, duplicates and non-events can remain public until a manual full audit;
- after the Fly log buffer expires, exact LLM/Smart Update decisions cannot be reconstructed from canonical rows alone;
- the required live VK-import/Smart Update acceptance path was not exercised because the E2E user had no production role;
- repeatedly repairing rows without measuring whether they are legacy debt or post-fix recurrence undermines release confidence.

## Detection

- operator explicitly reported that disabling logs contradicts release preparation and daily incident monitoring;
- production DB showed E2E user `8336351413` present, unblocked and `is_superadmin=0`; the real Telegram UI returned `Not authorized`;
- runtime mirror evidence for the previous incident ended on 2026-07-08 while new imports continued;
- a fresh audit and disk inventory were started from exact production state.

## Root Cause

1. **Containment became policy.** `INC-2026-04-16` disabled runtime file logging after `/data` filled, but the implementation had no hard total-byte budget or free-space floor. The safer logging design was not delivered, so loss of evidence became the long-term workaround.
2. **Retention alone was not a disk guarantee.** Hourly time rotation limits file age but one noisy active hour can still grow without a byte cap; unrelated backups/recovery/render artifacts also share the SQLite volume.
3. **Quality acceptance was too narrow.** The scheduled exhibition duplicate audit covers one event type and high-confidence duplicate heuristics. It cannot attest eventness, occurrence roles, source-grounded location/date/description/media, or all future rows.
4. **Vector recall was not durable incident memory.** Active-catalog embeddings are useful for duplicate recall but repaired/cancelled regression examples are pruned; vectors do not themselves approve semantic correctness.
5. **Live acceptance lacked reversible role automation.** The E2E harness correctly failed on authorization, but the authorized temporary grant/restore procedure was not completed in the incident run.

## Contributing Factors

- SQLite, logs and several generated/recovery artifact stores share a small Fly volume.
- Old incident backup tables/files and terminal temp outputs need explicit ownership and retention rather than ad-hoc accumulation.
- Many rows repaired on 2026-07-12 predated the latest prevention deploy; raw counts must not be presented as “new daily regressions” without `added_at`/source-run attribution.

## Automation Contract

### Treat as regression guard when

- changing runtime logging, Fly volume artifacts/retention, deploy cleanup or SQLite backup behavior;
- changing VK/TG import, Smart Update LLM stages, vector identity/sync, or scheduled quality audits;
- claiming an incident/release is verified through Telegram UI.

### Affected surfaces

- `runtime_logging.py`, `fly.toml`, `/data/runtime_logs`, Fly root overlay and `/data`;
- `/data/db.sqlite`, backup/tmp/recovery directories;
- `vk_auto_queue.py`, `vk_intake.py`, `smart_event_update.py`, event-vector sidecar;
- production `user` role for the approved E2E identity;
- Telegram UI, `ops_run`, managed Telegram/VK/Telegraph/static projections.

### Mandatory checks before closure or deploy

- inventory disk by path/size/mtime/ownership and delete only proven terminal/regenerable data;
- `PRAGMA quick_check=ok`, `/healthz ready=true`, SQLite/app write path healthy before and after cleanup/deploy;
- runtime mirror enabled with explicit max-file, max-total, retention and minimum-free-space settings;
- active log grows, total log bytes remain within budget and fresh logs have no `Errno 28`;
- snapshot and reversibly grant the exact E2E user role, run `/vk_auto_import --limit=1` through `@events_love39_bot`, reconcile UI, runtime logs, `ops_run`, inbox/source/event ids and both vector kinds, then restore the prior role;
- freeze and account for the complete current/future catalog; vector-first recall must be followed by source/OCR/public-surface LLM adjudication;
- replay every newly confirmed import defect through the responsible importer + Smart Update with an opposite control;
- deployed SHA must be reachable from `origin/main`.

### Required evidence

- before/after `df`/`du` inventory and exact deleted-path manifest;
- production log env/files/sizes/mtimes and startup budget line;
- E2E user role before/grant/restore plus Telegram UI artifact and correlated `ops_run`/log rows;
- audit denominator, cutoff/hash, vector coverage and source-confirmed findings;
- focused tests, production replay for any new prevention fix, deployed SHA/release/health.

## Immediate Mitigation

- Safe first-pass cleanup removed 71 proven stale runtime/source-debug/carousel artifacts (`35,902,184` bytes); SQLite `quick_check` and `/healthz` remained healthy. The dominant persistent pressure was `/data/guide_media` without DB-aware retention.
- The DB-aware production dry-run then selected 192 old unprotected guide files (`152,264,570` bytes), protected 248 referenced paths and predicted the configured budget would be met. Apply deleted exactly that manifest and healed 506 `guide_monitor_post` rows / 1,295 aligned stale asset references. The post-pass was idempotent (zero candidates/stale paths), all current/future persisted assets existed, `quick_check=ok`, `/healthz ready=true`, and `/data` free space rose to `374,222,848` bytes.
- Runtime logging was redesigned with size rotation, a hard total budget, age pruning and a volume free-space floor. `/healthz` now exposes the same disk floor.
- Guide media now has DB-aware bounded retention: current/future occurrences, recent source posts and current digest issues are protected; only old unprotected regular files are candidates and stale JSON links are healed transactionally.
- The E2E production row remains unchanged until logging is available and its prior state is backed up.

## Corrective Actions

- [x] complete the managed guide-media production dry-run/apply and record final before/after evidence;
- [ ] deploy bounded runtime logging and verify ongoing writes/rotation guards;
- [ ] execute reversible E2E role grant, bounded live import and role restoration;
- [ ] complete the new all-future source/public audit and repair confirmed defects;
- [ ] deliver LLM-first/vector-first prevention for every confirmed fresh recurrence, not blanket regex mutation;
- [ ] distinguish legacy-debt repairs from rows created after each prevention SHA in monitoring metrics.

## Follow-up Actions

- [ ] add a persistent, non-public incident-prototype vector corpus so cancelled regression fixtures remain recall candidates for LLM adjudication;
- [ ] expand daily quality acceptance beyond exhibition duplicates to recent imports plus rotating full-catalog source-grounded sampling, with `ops_run` and operator alerts;
- [x] add disk budget/low-free-space health before SQLite write failure;

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: runtime logging/disk/source-debug `9 passed`; guide-media retention and adjacent guide publishing `13 passed`; remaining incident gates pending
- post-deploy verification: pending

## Prevention

The operational rule is “logs bounded, not disabled.” The quality rule remains LLM-first and vector-first: vectors retrieve active neighbors and durable incident prototypes; an LLM compares exact source/OCR occurrence roles and evidence; narrow deterministic code only enforces budgets, provenance and fail-closed validation.
