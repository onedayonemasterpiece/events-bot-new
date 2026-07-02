# INC-2026-07-02-buynov-cancellation-lifecycle-gap Buynov cancellation did not change event lifecycle

Status: monitoring
Severity: sev2
Service: events-bot Smart Update / VK auto-import / public event fanout
Opened: 2026-07-02
Closed: —
Owners: events-bot
Related incidents: `INC-2026-05-07-vk-time-reschedule-wrong-match`, `INC-2026-05-17-vk-retrospective-reschedule-wrong-postponement`
Related docs: `docs/features/vk-auto-queue/README.md`, `docs/features/event-email-notifications/README.md`, `docs/llm/prompts.md`

## Summary

Cancellation information for event `5988` (Александр Буйнов, 2026-07-14 19:00, Янтарь холл) reached the system and even appeared as source facts/poster media, but the canonical row stayed `lifecycle_status='active'`. Public surfaces therefore remained shaped like an active event instead of an explicit cancellation.

## User / Business Impact

- Users could see an active-looking Telegram/VK/Telegraph event for a cancelled concert.
- Calendar/ICS flows could keep active reminders for an event that should be cancelled.
- The system had no implemented email outbox for notifying users who added an event to calendar.

## Detection

- User reported a VK post/image with `ОТМЕНА КОНЦЕРТА` for Александра Буйнова.
- Production DB inspection confirmed `event.id=5988` was still `active` while facts/source text said the concert was cancelled.
- `vk_inbox.id=9546` for `https://vk.com/wall-100137391_165125` was still `pending`; Telegram source `https://t.me/yantarholl/4743` had already merged cancellation facts but not lifecycle.

## Timeline

- 2026-07-02 — user reported cancellation requirements and later clarified that transactional email must use Yandex Cloud Postbox serverless with queue statistics.
- 2026-07-02 — production SQL showed event `5988` active, cancellation poster already primary, and source fact `Статус события: отменено` present.
- 2026-07-02 — VK API confirmed our managed post `https://vk.com/wall-231920894_5432` exists and the external source cancellation post is `https://vk.com/wall-100137391_165125`.
- 2026-07-02 — root fix prepared: LLM-first lifecycle field in event parsing and Smart Update merge, public fanout edits for existing non-active posts, controlled `target_post_url` replay.

## Root Cause

1. Smart Update merge could store cancellation as ordinary facts, but the merge schema/payload did not carry an event-level `lifecycle_status` decision.
2. VK auto-import had a pre-LLM deterministic cancellation shortcut; this was unsafe for LLM-first semantics and could both miss cases and wrongly deactivate events.
3. Existing public post fanout treated non-active events as “do not publish” and skipped editing already-public Telegram/VK posts, so lifecycle changes could fail to reach public surfaces.
4. Email/calendar notification requirements were not implemented/documented as a queued Postbox/YDB transactional pipeline.

## Contributing Factors

- `ticket_status` and event lifecycle were not explicitly separated in all LLM contracts.
- Managed VK DB URL could become stale (`5401`) while the visible current managed post was `5432`, so repair must verify via VK API before edits.
- Existing cancellation facts in `event_source_fact` did not automatically imply lifecycle.

## Automation Contract

### Treat as regression guard when

- Changing `docs/llm/prompts.md` event parse contract, `vk_intake.EventDraft`, `smart_event_update.EventCandidate`, Smart Update merge schema, or lifecycle propagation.
- Changing `vk_auto_queue.run_vk_auto_import`, especially cancellation/reschedule handling or target replay.
- Changing `schedule_event_update_tasks`, `job_publish_tg_event_post`, `job_sync_vk_source_post`, Telegraph lifecycle rendering, or Telegram/VK public event formatting.
- Implementing static event pages, calendar-follow, Yandex OAuth email capture, Postbox sender, email queue, or YDB mail statistics.

### Affected surfaces

- `docs/llm/prompts.md`
- `vk_intake.py`
- `smart_event_update.py`
- `vk_auto_queue.py`
- `main.py`
- `main_part2.py`
- public Telegram `@kldevents`
- managed VK `klgdevents`
- Telegraph pages
- future static event/calendar/email surfaces

### Mandatory checks before closure or deploy

- `pytest tests/test_vk_auto_queue_import.py::test_vk_auto_import_cancellation_notice_uses_llm_first_path`
- `pytest tests/test_vk_auto_queue_import.py::test_vk_auto_import_target_post_url_processes_only_requested_row`
- `pytest tests/test_tg_event_publish.py::test_schedule_event_update_tasks_edits_existing_cancelled_public_posts`
- `pytest tests/test_smart_event_update_ticket_fields.py::test_normalize_lifecycle_status_update_from_llm_values`
- Production replay of exact external source `https://vk.com/wall-100137391_165125`, not the managed result post.
- Production verification: event `5988` is `lifecycle_status='cancelled'`; Telegraph/TG/VK public surfaces visibly say cancelled; managed VK URL is reconciled to the actual existing post if needed; `/healthz` OK.
- Postbox/YDB email requirements remain linked from docs routes.

### Required evidence

- deployed SHA
- test command output
- production SQL before/after for event `5988`, `vk_inbox.id=9546`, relevant `joboutbox`
- VK API evidence for `wall-100137391_165125` and managed `wall-231920894_5432`
- public Telegram/Telegraph/VK smoke links

## Immediate Mitigation

- Prepared code to let LLM decide `lifecycle_status` from source semantics (`cancelled`/`postponed`) and to pass that field through VK parse → Smart Update.
- Disabled deterministic cancellation shortcut by default; legacy matcher is only explicit emergency fallback via `VK_AUTO_IMPORT_LEGACY_CANCEL_MATCHER=1`.
- Added `run_vk_auto_import(..., target_post_url=...)` to replay exactly one source VK post during incident repair without consuming neighboring queue rows.

## Corrective Actions

- Event parse prompt now instructs LLM to extract cancellation/reschedule notices as lifecycle updates instead of returning `[]`.
- `EventDraft` and `EventCandidate` now carry `lifecycle_status`.
- Smart Update merge schema distinguishes `lifecycle_status` from `ticket_status` and applies it to existing rows.
- Public Telegram/VK event builders display `❌ Отменено` / `⏸ Перенесено`.
- Existing Telegram/VK posts can be edited for non-active events; new first-time non-active promo posts remain blocked.
- Requirements for calendar-follow email notifications, Postbox transport, durable queue, rate limits, and YDB statistics were documented in `docs/features/event-email-notifications/README.md`.

## Follow-up Actions

- [ ] Implement Yandex OAuth notification email persistence and fallback email capture UI.
- [ ] Implement calendar-follow durable outbox, Yandex Cloud Postbox sender, provider callbacks/suppression, YDB stats sink, and admin queue status.
- [ ] Add static event page lifecycle badge and cancellation/reschedule note.
- [ ] Add actual email tests for confirmation, 24h reminder, reschedule diff, cancellation disclaimer, idempotency, rate limits, retry/backoff, and suppression.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: Fly app `events-bot-new-wngqia`
- regression checks: pending final run/deploy evidence
- post-deploy verification: pending production replay and public smoke

## Prevention

- Lifecycle is an LLM-owned field in both extraction and merge.
- Deterministic regex cancellation is no longer the default decision path.
- Incident replay can target one exact external VK source post.
- Email notification delivery must be queued/statistical before production enablement, not sent inline.
