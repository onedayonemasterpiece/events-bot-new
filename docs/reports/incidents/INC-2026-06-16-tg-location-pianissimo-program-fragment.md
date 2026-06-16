# INC-2026-06-16 TG Pianissimo Program Fragment As Location

Status: open
Severity: sev2
Service: Telegram Monitoring + Smart Event Update + public Telegram event publishing
Opened: 2026-06-16
Closed: —
Owners: events-bot runtime / import pipeline owner
Related incidents: `INC-2026-04-26-daily-location-fragments`, `INC-2026-05-05-event-quality-regression`, `INC-2026-05-09-event-location-alias-free-dup-regressions`, `INC-2026-05-16-tg-location-prose-cityjazz-recurrence`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/telegram-publishing/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-06-16 the operator reported a wrong public location in Telegram event post `https://t.me/kldevents/613`. Production DB row `event.id=6060` (`Первый концерт нового сезона Pianissimo`, source `https://t.me/tretyakovka_kaliningrad/3201`) had `location_name="🎵 С. В. Рахманинов – Музыкальные моменты"` and `location_address="соч. 16"`, copying a repertoire line and a catalogue number from the program instead of the Tretyakov Gallery venue.

The correct attendee venue is the official Tretyakov Gallery Kaliningrad branch context: `Филиал Третьяковской галереи, Парадная наб. 3, Калининград`; the source text also says the concert is `в атриуме музея`.

## User / Business Impact

- Public `@kldevents` subscribers saw a misleading location line for a future event.
- Venue/address mistakes directly affect attendance and trust in the event feed.
- The same wrong fields propagated to Telegraph/ICS/VK/TG event surfaces until repaired.

## Detection

- Detected by operator report against `https://t.me/kldevents/613`.
- Production evidence:
  - `event.id=6060`, `tg_event_post_id=613`, source `tretyakovka_kaliningrad/3201`;
  - `event.location_name="🎵 С. В. Рахманинов – Музыкальные моменты"`;
  - `event.location_address="соч. 16"`;
  - `event_source_fact` included `Площадка: атриум музея`, but the public row kept the repertoire line as venue.
- Runtime file mirror was available and enabled during investigation: `ENABLE_RUNTIME_FILE_LOGGING=1`, `/data/runtime_logs/events-bot.log`.

## Timeline

- 2026-06-15 12:56 UTC — official source post `@tretyakovka_kaliningrad/3201` published.
- 2026-06-15 23:53 UTC — production Telegram Monitoring / Smart Update created event `6060`; runtime log recorded `smart_update.start ... location=🎵 С. В. Рахманинов – Музыкальные моменты`.
- 2026-06-15 23:54 UTC — Telegraph/ICS jobs completed with the wrong location fields.
- 2026-06-16 08:44 UTC — `tg_event_publish` published public event post `@kldevents/613`.
- 2026-06-16 UTC — operator reported the location problem.
- 2026-06-16 UTC — production DB/log evidence collected; prevention patch added LLM-first venue-review hardening and import-boundary regression tests.

## Root Cause

1. The LLM extractor/venue-review path allowed a source-grounded repertoire line to survive as `location_name`; being present in the source text was insufficient because the string was not a venue.
2. The suspicious-location review prompt did not explicitly forbid repertoire/program items, musical work titles, or catalogue numbers such as `соч. 16` as venue/address fields.
3. The import-boundary prose/location guard did not classify this short program-line shape as invalid, so existing source default recovery was not applied before Smart Update/publication.

## Contributing Factors

- Prior location guards focused on long prose, date/time fragments, generic room labels, unsupported defaults, and person-name leaks; this incident was a short, source-grounded list item.
- The official Tretyakovka source has a safe `default_location`, but the candidate path treated the extracted non-venue as explicit event-local location and did not fall back to that source context.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/TelegramMonitor/telegram_monitor.py` extraction prompts, venue-review trigger, or venue-review schema;
- changing `source_parsing/telegram/handlers.py` candidate build, source default handling, or location/prose guards;
- changing `docs/reference/locations.md` / `docs/reference/location-aliases.md` for Tretyakovka or museum venues;
- changing public `tg_event_publish`, Telegraph, ICS, VK sync, or event rebuild paths that render `event.location_*`.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- production SQLite `event`, `event_source`, `event_source_fact`, `joboutbox`
- public `@kldevents` event post, Telegraph page, ICS calendar post/file, VK managed post

### Mandatory checks before closure or deploy

- Regression test that the Telegram Monitor LLM venue-review trigger fires for a repertoire/program list item used as `location_name` with `соч. 16` as address.
- Regression test that server-side candidate build for `@tretyakovka_kaliningrad/3201` drops the program item and recovers `Филиал Третьяковской галереи, Парадная наб. 3, Калининград` from source default.
- Existing Telegram Monitoring and candidate-location grounding tests.
- `py_compile` for touched modules and `git diff --check`.
- Release-governance checks: clean worktree based on `origin/main`, fix committed/pushed, deployed SHA reachable from `origin/main`.
- Production repair evidence for event `6060`: DB row, Telegraph rebuild, ICS rebuild, and corrected public Telegram post/edit or explicit replacement evidence.

### Required evidence

- Test output for `tests/test_tg_candidate_location_grounding.py` and `tests/test_tg_monitor_gemma4_contract.py`.
- Runtime/DB evidence identifying `event.id=6060` and `@kldevents/613`.
- Deployed SHA and Fly machine/version evidence.
- Post-repair verification query showing corrected `event.location_name/location_address/city`.
- Public-surface verification that `@kldevents/613` no longer shows the program line as location.

## Immediate Mitigation

- Prevention patch prepared on a clean hotfix worktree from `origin/main`.
- The patch keeps the semantic venue decision LLM-first: deterministic code only triggers venue-review for the bad syntactic shape and invalidates an obvious program/catalogue fragment at the import boundary; it does not infer a new venue from program text.

## Corrective Actions

- Tightened Telegram Monitor extraction and venue-review prompts to explicitly forbid repertoire/program items, musical work titles, and catalogue numbers such as `соч. 16` in venue fields.
- Broadened the LLM venue-review trigger for short program-item/list-line venue smells so the LLM repair stage owns the semantic correction.
- Added a narrow import-boundary safety guard that rejects program/catalogue fragments as venue strings and then uses existing source-default/reference recovery.
- Added regression coverage for the exact Tretyakovka/Pianissimo failure shape.

## Follow-up Actions

- [ ] Consider an operator-facing warning when a candidate location is dropped as a program/catalogue fragment and recovered from source default.
- [ ] Add this case to a production-equivalent Telegram Monitoring replay/eval pack if a maintained replay harness is available for Kaggle extraction outputs.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - pending
- post-deploy verification:
  - pending

## Prevention

- Location grounding now distinguishes `present in source text` from `is a venue/place name` for short source-grounded repertoire lines.
- The LLM venue-review prompt has an explicit contract against musical program/catalogue leakage.
- Server import has a narrow final guard so a future remote extraction artifact with the same shape cannot directly become a public event location.
