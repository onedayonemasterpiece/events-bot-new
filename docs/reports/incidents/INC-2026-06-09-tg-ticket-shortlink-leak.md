# INC-2026-06-09-tg-ticket-shortlink-leak

Status: monitoring
Severity: sev3
Service: `events-bot-new` Telegram event publishing
Opened: 2026-06-09
Closed: —
Owners: events-bot maintainer
Related docs: `docs/features/tg-publishing/README.md`, `docs/features/vk-publishing/README.md`

## Summary

On 2026-06-09 Telegram event post `https://t.me/kldevents/98` rendered a VK shortener ticket link (`https://vk.cc/cYaxjc`) even though Telegram event posts must not use VK-only analytics shortlinks when a canonical ticket URL is known.

Production DB mapped post `98` to event `5333`, `Концерт классической музыки «Зимний путь»`. The row had both:

- `ticket_link=https://vk.cc/cYaxjc`
- `vk_ticket_short_url=https://vk.cc/cYaxjc`

The same event also had a parser source with the canonical ticket URL:

- `https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/46430/2026-06-13/21:00:00`

No repair was performed as part of this prevention fix.

## Root Cause

Telegram rendering was already using `event.ticket_link`, not `event.vk_ticket_short_url`. The leak happened earlier: Smart Update ticket merge logic allowed a VK shortener candidate to replace an existing non-short ticket URL when trust priority was higher.

This made the VK shortener become the canonical `event.ticket_link`, so Telegram correctly rendered the wrong stored value.

## Corrective Actions

- `_apply_ticket_fields` now treats VK shortener URLs (`vk.cc`, `vk.link`, `go.vk.com`, `l.vk.com`) as non-canonical for replacement decisions.
- A VK shortener URL can no longer replace an existing non-short `event.ticket_link`.
- A non-short parser/site ticket URL can replace an existing VK shortener URL even at equal trust, and clears stale `vk_ticket_short_*` fields.

## Verification

- `/tmp/events-bot-test-venv/bin/python -m pytest tests/test_smart_event_update_ticket_fields.py tests/test_tg_event_publish.py::test_build_tg_event_announcement_uses_original_ticket_link_not_vk_short -q` -> `5 passed`

## Prevention

- Regression coverage now includes both directions:
  - a real ticket URL is not overwritten by `https://vk.cc/cYaxjc`;
  - `https://vk.cc/cYaxjc` is replaced by the canonical Tretyakov ticket URL when that source appears later.
