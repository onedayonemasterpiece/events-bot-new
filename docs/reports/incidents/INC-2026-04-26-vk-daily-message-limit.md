# INC-2026-04-26 VK Daily Message Limit

Status: open
Severity: sev2
Service: VK daily announcements
Opened: 2026-04-26
Closed: —
Owners: Codex
Related incidents: `INC-2026-04-14-daily-delay-vk-auto-queue-lock-storm`
Related docs: `docs/operations/cron.md`, `docs/reports/incidents/README.md`

## Summary

On 2026-04-26 the scheduled VK daily announcement attempted to publish one `wall.post` message of about 46k characters. VK rejected it with `message_character_limit`, so the daily VK "today" slot was not delivered at that attempt.

## User / Business Impact

- VK subscribers did not receive the scheduled daily announcement for the affected attempt.
- The Telegram daily surface stayed healthy; the impact is scoped to VK daily crosspost.
- The failure can repeat on busy days because event volume and long Smart Update descriptions can make a single VK post exceed VK's wall message limit.

## Detection

- Detected during post-deploy production log review after the VK auto queue Gemma 4 migration.
- Production log evidence showed `post_to_vk start ... len=46395`, followed by VK API `code=100` / `Violated: message_character_limit` and `vk daily today failed`.
- `/healthz` remained healthy, so this is a scheduled publication degradation rather than serving downtime.

## Timeline

- 2026-04-26 08:16 UTC: scheduled `vk_scheduler` attempted VK daily `today` publication.
- 2026-04-26 08:16 UTC: `wall.post` rejected the 46k-character message with `message_character_limit`.
- 2026-04-26 08:18 UTC: incident noticed while reviewing post-deploy Fly logs.
- 2026-04-26 08:20 UTC: incident workflow started and scoped to VK daily announcements.

## Root Cause

1. `build_daily_sections_vk()` produced one full VK "today" section containing many expanded event cards.
2. `send_daily_announcement_vk()` sent that whole section through one `post_to_vk()` call.
3. Unlike Telegram daily posts, the VK daily path had no atomic text splitter and no post-size guard before `wall.post`.
4. The scheduler marked success only after the function returned, but the path had no compensating split/retry on VK `message_character_limit`.

## Contributing Factors

- Busy day volume and long event descriptions made the generated VK section much larger than normal.
- The VK path did not reuse the Telegram daily atomic splitting pattern.
- `post_to_vk()` returning no URL was not treated as a hard failure by `send_daily_announcement_vk()`.

## Automation Contract

### Treat as regression guard when

- Changing `build_daily_sections_vk()`, `send_daily_announcement_vk()`, `post_to_vk()`, VK daily scheduler timing/state, or daily event-card formatting.
- Adding longer text to VK daily event cards.
- Changing VK daily env/config in `fly.toml` or `.env.example`.

### Affected surfaces

- `main_part2.py::build_daily_sections_vk`
- `main_part2.py::send_daily_announcement_vk`
- `main_part2.py::post_to_vk`
- `main_part2.py::vk_scheduler`
- `fly.toml` / `VK_DAILY_POST_MAX_CHARS`
- `docs/operations/cron.md`
- production SQLite VK daily state (`vk_last_today`)

### Mandatory checks before closure or deploy

- Unit test that oversized VK daily text is split into chunks under `VK_DAILY_POST_MAX_CHARS`.
- Unit test that `send_daily_announcement_vk(section="today")` posts all chunks and does not call `wall.post` with an oversized payload.
- `python -m py_compile main.py main_part2.py`.
- Targeted VK daily tests.
- Production `/healthz` after deploy.
- Production evidence that `VK_DAILY_POST_MAX_CHARS` is present in Fly config.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Targeted test output.
- Fly deploy evidence.
- Post-deploy `/healthz` response.
- Compensating rerun/catch-up evidence for the missed same-day VK daily slot, unless production state shows it was already delivered.

## Immediate Mitigation

- Add an atomic VK daily splitter before `wall.post`, preserving event cards when possible.
- Treat a missing VK post URL as a failed chunk so `vk_last_today` is not advanced after a silent no-op.

## Corrective Actions

- Added `split_vk_daily_text_atomic()` and `VK_DAILY_POST_MAX_CHARS` so oversized VK daily sections are split into bounded chunks before `wall.post`.
- `send_daily_announcement_vk()` now posts every chunk and raises if any chunk does not return a VK post URL, preventing silent advancement of `vk_last_today`.

## Follow-up Actions

- [ ] Consider adding a compact VK daily mode if busy days produce too many wall posts even after safe splitting.
- [ ] Add daily publication metrics for VK chunk count and total generated length.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Keep VK daily size limiting as a regression contract for daily formatting changes.
