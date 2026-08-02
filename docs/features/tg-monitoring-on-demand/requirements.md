# Requirements: TG monitoring on demand

Status: reconciled

## Product intent

- Add an operational fast-path for selected Telegram sources where the bot is present as a channel admin: a new post in an allowlisted source should trigger near-real-time processing instead of waiting for the scheduled Telegram Monitoring slot.
- Primary initial source: `https://t.me/kraftmarket39` / `@kraftmarket39`; the mechanism must support adding more Telegram sources later through configuration, without hard-coding a single channel.
- On-demand is an accelerator only. Existing scheduled Telegram Monitoring remains the authoritative catch-up path for missed messages, disabled on-demand, or non-terminal failures.

## Requirements

### Source scope and signal

- The bot must listen to Bot API `channel_post` updates for allowlisted Telegram sources where it is an admin.
- On-demand sources are configured as a source allowlist; default source is `kraftmarket39`.
- A Bot API update is only a durable signal. It must not run a separate extraction/import pipeline directly from the Bot API payload and must not synthesize a standalone `telegram_results.json` as the main v1 path.
- The signaled source must exist in `telegram_source` and be `enabled=1`; missing or disabled sources are ignored with diagnostics.

### Processing path

- The signal schedules a source-specific run of the existing Telegram Monitoring pipeline: `run_telegram_monitor(..., source_usernames=[<source>], trigger="on_demand")` or equivalent.
- The existing Telegram Monitoring → Smart Update → import/publication path must be reused for event extraction, enrichment, Telegram/VK publication side effects, metrics, scanned-message marks, and JobOutbox handling.
- The signaled `message_id` must be added to the existing force-message mechanism for that source so the source-specific run reads the new post even if cursor/window logic would otherwise skip it.
- The initial run is delayed by a 10-minute debounce (`600` seconds) to give the channel author time to edit/fix the post before extraction.

### Queueing and resource-busy behaviour

- On-demand requests are durable and coalesced per source: repeated posts for the same source update one pending source row and keep the latest known `message_id`/date.
- If local Telegram Monitoring is already running, the global heavy-operation lock is busy, or the shared remote Telegram/Kaggle session is busy, the on-demand row remains pending and is retried later.
- Busy retry interval is 10 minutes (`600` seconds). The earlier 30-minute idea is superseded by the later resolved requirement.
- Non-busy terminal failures may mark the queue row as `error`, but scheduled Telegram Monitoring must still remain the catch-up safety net.
- On-demand must not bypass existing resource protections: local `_RUN_LOCK`, global heavy-operation lock, Kaggle/remote Telegram session registry, and auth-bundle/session separation rules.

### Cursor and duplicate-prevention

- After a successful on-demand Telegram Monitoring/import pass for a source, the same cursor state used by scheduled Telegram Monitoring must be updated for that source.
- Specifically, the processed post(s) must be recorded in `telegram_scanned_message`, and `telegram_source.last_scanned_message_id` / `last_scan_at` must advance to the highest successfully processed `message_id` for that source.
- The purpose is to prevent the later scheduled scan for `@kraftmarket39` or any future on-demand source from re-scanning/re-importing posts already handled by on-demand.
- Do not advance the cursor for a queue row that never reached Telegram Monitoring/import because of a terminal pre-run error; scheduled catch-up should still be able to see such posts.

### Operator and channel UX

- On-demand progress/final reports, if enabled, go to the superadmin/operator chat, not into the source channel.
- Channel/group reposts or forwards used as on-demand signals must not fall through into the legacy private-chat manual add-event flow and must not produce service messages such as `Event added`, `Festival added`, or publication progress in the source chat.

### Configuration and observability

- Provide configuration for: enabling/disabling on-demand, source allowlist, debounce interval, busy retry interval, queue poll interval, max source-specific runs per dispatcher tick, and optional progress reports.
- Persist enough queue state for recovery and diagnostics: source username/id, latest message id/date, first seen/updated timestamps, next run time, attempts, status, last run time, and last error.
- Record on-demand runs in the existing `ops_run` / Telegram Monitoring reporting surface with `trigger="on_demand"` and source-scope details.

## Technical findings / recommendations

- Existing project primitives support the selected path: Bot API `channel_post` updates are already allowed, Telegram Monitoring already has `telegram_source`, `telegram_source_force_message`, `telegram_scanned_message`, and `last_scanned_message_id` cursor mechanics, and source-scoped `run_telegram_monitor(..., source_usernames=[...])` is the natural extension point.
- Therefore the best default is to reuse the existing Telegram Monitoring + Smart Update pipeline rather than create a second Bot API extraction/import pipeline.
- Cursor synchronization is not a separate product choice: it is required for correctness because scheduled monitoring reads the same source and cursor state.

## Open questions

- None after the 2026-06-20 reconciliation. New questions should be added only if a future intake changes product/UX behaviour, not for implementation details that can be resolved from existing project primitives.

## Decisions log

- 2026-06-20: Initial intake established `@kraftmarket39` as the first on-demand source and required future multi-source support.
- 2026-06-20: On-demand role resolved as fast-path only; scheduled Telegram Monitoring remains catch-up.
- 2026-06-20: Processing path resolved to Bot API signal → source-specific existing Telegram Monitoring after debounce. This supersedes the earlier direct `telegram_results` synthesis idea for v1.
- 2026-06-20: Resource-busy behaviour resolved as durable pending queue with 10-minute retry.
- 2026-06-20: Added cursor synchronization requirement so on-demand-successfully processed posts are not re-scanned by scheduled Telegram Monitoring.

## Archived intake 2026-06-20T07:03:22+00:00

Status: resolved / archived

Resolution: Integrated as product intent, source scope, Bot API admin-channel signal, resource-safety requirements, existing-pipeline reuse, and `@kraftmarket39` initial source.

Source files:

- [source/voice_AgADcKAAAoIEsEk.oga](source/voice_AgADcKAAAoIEsEk.oga)
- [source/voice_AgADcaAAAoIEsEk.oga](source/voice_AgADcaAAAoIEsEk.oga)
- [source/voice_AgADcqAAAoIEsEk.oga](source/voice_AgADcqAAAoIEsEk.oga)
- [source/voice_AgADdKAAAoIEsEk.oga](source/voice_AgADdKAAAoIEsEk.oga)
- [source/voice_AgADe6AAAoIEsEk.oga](source/voice_AgADe6AAAoIEsEk.oga)

## Archived intake 2026-06-20T07:34:57+00:00

Status: resolved / archived

Resolution: Integrated the selected fast-path/catch-up split and the later refined implementation path: Bot API update queues a source-specific Telegram Monitoring run after debounce; resource-busy rows retry every 10 minutes.

## Archived intake 2026-06-20T07:44:42+00:00

Status: resolved / archived

Resolution: No standalone requirement text; treated as a request to reconcile and verify the accumulated requirements.

## Archived intake 2026-06-20T07:49:29+00:00

Status: resolved / archived

Resolution: No standalone requirement text; treated as a request to fix the reconciled requirements in this canonical file.

## Archived intake 2026-06-20T08:00:59+00:00

Status: resolved / archived

Resolution: Applied the recorded decision that the Bot API update only queues source-specific Telegram Monitoring for the nearest post-debounce run. This resolves the earlier conflict in favour of the new source-specific/debounced monitoring path and 10-minute retry cadence.

## Archived intake 2026-06-20T09:44:38+00:00

Status: resolved / archived

Resolution: Integrated the cursor/duplicate-prevention requirement: after successful on-demand processing, update the same Telegram Monitoring cursor for the source so scheduled monitoring does not scan the already processed `@kraftmarket39` messages again.
