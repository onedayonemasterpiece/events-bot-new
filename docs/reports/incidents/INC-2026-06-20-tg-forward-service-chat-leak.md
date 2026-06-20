# INC-2026-06-20 TG forwarded-post service messages leaked to chat

Status: open
Severity: sev2
Service: Telegram forwarded-post intake / TG monitoring on demand
Opened: 2026-06-20
Closed: —
Owners: events-bot maintainer
Related incidents: `INC-2026-06-20-tg-on-demand-scheduler-run-id.md`
Related docs: `docs/features/tg-monitoring-on-demand/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

After TG monitoring on demand rollout, a repost/forward intended as a channel signal was also handled by the legacy manual forwarded-post add-event flow. That flow sends operator/service replies to `message.chat.id`, so messages like `Festival added`, `Event added`, and publication progress were posted into the chat/channel context instead of staying in the bot/admin surface.

## User / Business Impact

- Service/operator messages became visible outside the intended bot/admin conversation.
- The event itself was valid and was created/publication jobs were queued, but the UX contract for on-demand signals was broken.
- Risk: every future repost into an automation chat/channel could create noisy public/service messages and bypass the intended 10-minute debounce/Kaggle Telegram Monitoring route.

## Detection

- User reported visible bot service messages at 2026-06-20 10:57 local time.
- Runtime logs around 2026-06-20 08:57 UTC showed `forward parsed 2 events`, `FLOW [E6244] start add_event | user=185169715`, and service response sending for event `6244`.
- Production DB showed event `6244` source `https://t.me/kenigevents/4104`, confirming it came through forwarded-post intake rather than `tg_monitoring` import.

## Timeline

- 2026-06-20 08:51 UTC — source post/repost `@kraftmarket39/292` appeared, forwarded from `@kenigevents/4104`.
- 2026-06-20 08:57 UTC — legacy forwarded-post handler parsed it immediately via add-event flow and sent service messages to the message chat.
- 2026-06-20 09:00 UTC — user reported service messages leaked to chat.
- 2026-06-20 09:05 UTC — logs/DB localized root cause to forwarded-post handler routing, not LLM event semantics.

## Root Cause

1. The legacy `forward_wrapper` accepted forwarded messages regardless of chat type.
2. Reposts in non-private chats/channels can therefore enter `_process_forwarded(...)`, which calls `add_events_from_text(...)` and replies to `message.chat.id`.
3. TG monitoring on demand introduced a channel-signal workflow, but the legacy manual forward handler was not restricted to private bot conversations.

## Contributing Factors

- Existing tests covered on-demand queue/dispatch but not interaction with the global forwarded-post handler.
- Service reply paths in `_process_forwarded` are designed for private operator use and are unsafe for public/group/channel contexts.

## Automation Contract

### Treat as regression guard when

- Changing forwarded Telegram message routing.
- Changing TG monitoring on demand channel/group signal handling.
- Registering new broad `dp.message` handlers that can run in non-private chats.

### Affected surfaces

- `main_part2.py` forwarded message handler registration.
- `source_parsing/telegram/on_demand.py` routing filters.
- Public/channel/group chats where the bot is present.
- Manual private bot forward-to-add-event workflow.

### Mandatory checks before closure or deploy

- Unit test proving private forwarded messages still pass the manual flow filter.
- Unit test proving group/channel forwarded/reposted messages do not pass the manual flow filter.
- Targeted pytest for TG on-demand tests.
- `py_compile` changed modules.
- Post-deploy runtime log/health smoke.

### Required evidence

- deployed SHA:
- Fly release version:
- test output:
- runtime/log evidence:
- `origin/main` reachability:

## Immediate Mitigation

- Restrict forwarded-post manual add-event handler to private bot chats only.
- Channel/group reposts remain available as automation signals without service replies.

## Corrective Actions

- Added `is_private_forward_message(...)` filter.
- Replaced broad forwarded-message registration with the private-only filter.
- Added regression tests for private vs group/channel forwarded messages.

## Follow-up Actions

- [ ] If cleanup is required for already leaked service messages, collect exact chat id/message ids from the user or via authenticated Telegram inspection and delete only bot-owned service messages.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Manual add-event conversational flows that reply to `message.chat.id` must be private-chat scoped unless a feature explicitly designs group/channel UX.
