# INC-2026-06-13 TG Calendar Private Link

Status: mitigated
Severity: sev2
Service: Telegram event publishing / calendar CTA
Opened: 2026-06-13
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-13-poll-repost-wrong-date-and-copy`
Related docs: `docs/features/tg-publishing/README.md`, `docs/operations/release-governance.md`

## Summary

Telegram event posts in `@kldevents` rendered a visible calendar button, but
the button URL could point to a private internal `https://t.me/c/...` asset
channel message. For ordinary channel readers the CTA was therefore present but
not usable. The issue was observed on `https://t.me/kldevents/239` and then
also surfaced through Poll to Repost forwarding of the same event.

## User / Business Impact

- Readers saw a calendar CTA that did not open for them.
- Poll to Repost recommendations inherited the broken event-post affordance,
  even though forwarding itself was not the cause.
- Existing tests asserted the field used by the button, not whether the URL was
  publicly reachable by the audience.

## Detection

- Detected by operator review of the live post `https://t.me/kldevents/239`.
- Production DB evidence for event `5858` showed:
  - `tg_event_post_id=239`;
  - `ics_url=https://lnvfarbbofsnkedbfhlt.supabase.co/storage/v1/object/public/events-ics/event-5858-2026-06-14.ics?`;
  - `ics_post_url=https://t.me/c/2807919036/6579`.
- Runtime file mirror was enabled and available for the incident window.

## Timeline

- 2026-06-13: Poll to Repost forwarded event `5858` / `@kldevents` post `239`
  as a debug recommendation.
- 2026-06-13: operator reported that the calendar button did not work in the
  forwarded post and then confirmed it also did not work in the source
  `@kldevents/239` post.
- 2026-06-13: production SQL confirmed the event had a public Supabase `.ics`
  URL but the visible Telegram button used a private `t.me/c` asset-post URL.

## Root Cause

1. `build_tg_event_reply_markup` treated any HTTP(S) `event.ics_post_url` as a
   valid public CTA target.
2. The Telegram asset calendar channel stores message links as `t.me/c/...`,
   which are not public username links and are not a reliable audience-facing
   CTA for `@kldevents` subscribers.
3. The regression contract from `INC-2026-06-07` focused on avoiding raw `.ics`
   file links, but did not check public reachability of the chosen Telegram
   post URL.

## Contributing Factors

- The DB model keeps both `ics_url` and `ics_post_url`, but the public
  publisher did not distinguish service/internal URLs from audience-facing URLs.
- Unit tests used synthetic `https://t.me/c/asset/...` URLs as acceptable
  calendar-button targets.

## Automation Contract

### Treat as regression guard when

- changing `build_tg_event_reply_markup`, `_tg_event_calendar_post_url`,
  `update_source_post_keyboard`, `tg_ics_post`, `ics_publish`, or event
  calendar URL fields.

### Affected surfaces

- `main_part2.py` Telegram event reply markup.
- `main.py::update_source_post_keyboard`.
- Telegram channel `@kldevents`.
- Telegram asset calendar channel.
- Poll to Repost forwarded recommendations that expose source event CTAs.

### Mandatory checks before closure or deploy

- `tests/test_tg_event_publish.py`
- `tests/test_source_keyboard.py`
- Production SQL check for event `5858` or equivalent live event: private
  `ics_post_url` must not be selected as the public button URL when public
  `ics_url` exists.
- Post-deploy `/healthz` and release SHA evidence.

### Required evidence

- deployed SHA;
- targeted test command output;
- production SQL/runtime smoke showing event `5858` calendar URL resolves to
  public `ics_url`;
- confirmation that the fix is reachable from `origin/main`.

## Immediate Mitigation

- Public Telegram calendar CTA selection now rejects private internal
  `t.me/c/...` links and falls back to public `event.ics_url` when available.
- New `tg_ics_post` rows store `https://t.me/<asset-channel>/<message>` when
  the asset calendar channel has a username, so the preferred `ics_post_url`
  path remains audience-facing.

## Corrective Actions

- Added `_tg_event_public_calendar_url` for public calendar CTA selection.
- Updated Telegram event publishing and source-post keyboard paths to use the
  public calendar URL helper.
- Updated `tg_ics_post` URL persistence to use the asset channel username when
  it exists instead of always deriving an internal `/c/` link from chat id.
- Updated tests so `t.me/c` asset links no longer count as valid public
  calendar-button targets when a public `.ics` URL exists.

## Follow-up Actions

- [ ] Decide whether the Telegram calendar asset channel should become a public
  username channel with stable `https://t.me/<username>/<id>` links, or remain
  service-only while public CTAs use Supabase `.ics`.
- [ ] Repair already-published `@kldevents` posts whose calendar buttons point
  to private `t.me/c/...` URLs.
- [ ] Split Supabase ICS and Telegram ICS hashes/timestamps. Full
  `tests/test_ics_pipeline.py` currently exposes an older issue where
  `ics_publish` can update shared `ics_hash` before `tg_ics_post`, causing the
  Telegram file refresh to skip after an event date change.
- [ ] Stabilize `tests/test_ics_pipeline.py::test_ics_coalesced_jobs_and_semaphore`,
  which currently assumes deterministic `asyncio.gather` ordering.

## Release And Closure Evidence

- deployed SHA: `b5dc1f5798ae1530b2cc796b838fa9e6b1997930`
  (`origin/main` contains it).
- deploy path: manual `flyctl deploy --remote-only --detach` from clean
  worktree `codex/poll-debug-window-20260612`; Fly image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KV15Q9A7CC8QF137N1TNKQDP`,
  machine `48e42d5b714228`, version `1388`.
- regression checks:
  - `python3 -m py_compile main.py main_part2.py tests/test_tg_event_publish.py tests/test_source_keyboard.py tests/test_ics_pipeline.py`
  - `PYTHONDONTWRITEBYTECODE=1 /tmp/events-bot-poll-test-venv/bin/python -m pytest -q -p no:cacheprovider tests/test_tg_event_publish.py tests/test_source_keyboard.py tests/test_ics_pipeline.py::test_tg_ics_post_stores_public_channel_url_when_asset_has_username` (`52 passed in 9.63s`; local pytest PTY was interrupted after emitting the success summary because the process kept the terminal open).
  - Full `tests/test_ics_pipeline.py` was also sampled and exposed two older
    non-hotfix failures now tracked as follow-up actions:
    `test_ics_updates_on_change` and
    `test_ics_coalesced_jobs_and_semaphore`.
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`.
  - Fly status reported machine `48e42d5b714228` started with `1 total, 1 passing`.
  - Runtime file mirror was enabled and available at `/data/runtime_logs/events-bot.log`.
  - Production repair updated event `5858` from
    `ics_post_url=https://t.me/c/2807919036/6579` to
    `https://t.me/kenigeventscalendar/6579` and edited `@kldevents/239`
    reply markup successfully.
  - Production runtime smoke for event `5858` selected
    `button_url=https://t.me/kenigeventscalendar/6579` with button text
    `📅 14 июня 19:30 · Добавить в календарь`.

## Prevention

- Calendar CTA tests now distinguish public Telegram message links from
  private internal `t.me/c/...` links and require a public fallback.
- ICS pipeline coverage now asserts that an asset channel username is persisted
  as a public Telegram calendar-post URL.
