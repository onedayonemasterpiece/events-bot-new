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
6. **The primary Smart Update model could exhaust output on hidden reasoning.** Hosted Gemma 4 rejected `thinking_budget`, while small capped contracts sometimes returned only thought/MAX_TOKENS. The generic 4o JSON fallback also reused an incident-specific bundle example and allowed unrelated generated fields to survive after only title rejection; one dynamic schema name contained a provider-invalid colon.
7. **Occurrence and anchor roles were under-adjudicated.** Retrieval could find the right source/event, but the pipeline did not require one LLM decision to bind date and city to the same occurrence or distinguish doors/opening from event-wide start/range time.
8. **The live replay exposed a VK idempotency regression.** `wall.getById` and `wall.get(filter=all)` did not expose an existing postponed managed post, although the same authenticated user saw it through `filter=postponed`. The helper incorrectly treated `all` as a superset and scheduled a second post.

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
- A light Guide run completed during cleanup and persisted a new ~64 MB recovery bundle beside the prior ~69 MB partial bundle. The existing guard's 256 MB / 256 MiB-free defaults were below the new warning floor. Both already-imported terminal bundles were removed, raising free space to `428,863,488` bytes; production policy is now two runs / 128 MiB / 350 MiB free, with the currently persisted bundle always excluded.
- Runtime logging was redesigned with size rotation, a hard total budget, age pruning and a volume free-space floor. `/healthz` now exposes the same disk floor.
- Guide media now has DB-aware bounded retention: current/future occurrences, recent source posts and current digest issues are protected; only old unprotected regular files are candidates and stale JSON links are healed transactionally.
- The E2E production row remains unchanged until logging is available and its prior state is backed up.

## Corrective Actions

- [x] complete the managed guide-media production dry-run/apply and record final before/after evidence;
- [x] deploy bounded runtime logging and verify ongoing writes/rotation guards;
- [x] execute reversible E2E role grant, bounded live import and role restoration;
- [ ] complete the new all-future source/public audit and repair confirmed defects (audit complete; production repair set remains open);
- [x] deliver LLM-first/vector-first prevention for every confirmed fresh recurrence, not blanket regex mutation;
- [ ] distinguish legacy-debt repairs from rows created after each prevention SHA in monitoring metrics.
- [x] delete the replay-created postponed duplicate `wall-231920894_7265`, restore event `6857` to `wall-231920894_7250`, and make postponed lookup use the correct authenticated collection;

## Follow-up Actions

- [ ] add a persistent, non-public incident-prototype vector corpus so cancelled regression fixtures remain recall candidates for LLM adjudication;
- [ ] expand daily quality acceptance beyond exhibition duplicates to recent imports plus rotating full-catalog source-grounded sampling, with `ops_run` and operator alerts;
- [x] add disk budget/low-free-space health before SQLite write failure;

## Release And Closure Evidence

- deployed SHA: `e278e072` (`origin/main`), including `8fd50472` Smart Update prevention and the earlier bounded logging/retention commits
- deploy path: Fly releases `1630` (logging/retention), `1632` (staged Smart Update) and `1633` (VK postponed idempotency/E2E acceptance)
- regression checks: runtime logging/disk/source-debug `9 passed`; guide-media retention and adjacent guide publishing `13 passed`; Smart Update focused contracts `42 passed`; VK postponed/E2E focused contracts `3 passed`; full Smart Update set `77 passed` with four date-sensitive fixtures now past on 2026-07-13 (unrelated to this patch). The broad VK test files also expose 13 pre-existing mock-signature failures for newer location-marker/reservation kwargs; those are test-debt, not accepted as a pass.
- post-deploy verification: `/healthz ready=true`, disk `408 MiB` free, `PRAGMA quick_check=ok`, runtime mirror enabled/growing; exact VK API probe proved `filter=postponed` contains `7250` while `filter=all` does not. Live Telegram E2E `ops_run=3678` processed `vk_inbox=10052`, updated events `6856/6857` with no errors; vector sync `ops_run=3679` wrote four changed embeddings (`search_v3` + `related_v1`) and `3680` then proved all 532 documents unchanged/fresh. E2E role was restored to `is_superadmin=0, blocked=0`.

## 2026-07-13 complete future audit

Frozen denominator was **266/266** active canonical current/future events, with
1,277 source rows, 781 poster/media rows, both current vector documents and
direct Telegram/VK/comment inspection. Embeddings were **266/266 fresh for both
`search_v3` and `related_v1`**; the defect was semantic adjudication/use, not
vector coverage. The audit confirmed 33 semantic repair targets affecting 35
canonical IDs plus four live-publication defects on four more IDs (**39 affected
IDs**). Exact source truth, classifications, surfaces and safe repair actions are
frozen in the ignored artifact
`artifacts/codex/INC-2026-07-13-runtime-logging-quality/future-audit/SUMMARY.md`.

Confirmed repair inventory (survivor/meaning is source-adjudicated in the
artifact): duplicates/teasers `6222→6277`, `6696→6766`, `6472→5665`,
`6774→2884`, `3459→2864`, `5937→5830`, and pseudo-event `6421`;
occurrence/date/range/eventness IDs `6830`, `6661`, `6745`, `3934`, `4648`,
`3999`, `4327`, `5765`, `6467`, `6582`, `6595`, `6624`, `6794`, `6112`,
`6337`, `5344`, `3177`, `2601`, `2182`, `6028`; location IDs `4961`, `4671`,
`6425`, `3132`; source/publication mixing `5754`, `5757`, `6592`, `6851`.
Rows created after recent prevention cutovers include real recurrence examples
`6745`, `6766`, `6774`, `6830` and `6851`; the rest must be reported as legacy
debt or an earlier audit miss, not as one day's new regressions.

The replay itself found one additional live-publication defect: postponed
`wall-231920894_7250` was invisible through VK `filter=all`, so the old helper
created `wall-231920894_7265`. The duplicate was deleted before its publish
slot, event `6857` was restored to `7250`, and the production/API post-check is
`7250 present, 7265 absent`.

## Prevention

The operational rule is “logs bounded, not disabled.” The quality rule remains LLM-first and vector-first: vectors retrieve active neighbors and durable incident prototypes; an LLM compares exact source/OCR occurrence roles and evidence; narrow deterministic code only enforces budgets, provenance and fail-closed validation.
