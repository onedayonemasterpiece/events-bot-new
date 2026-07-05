# Publication queue

Status: future MVP-3+ design. MVP-1 writes report/favorites only.

## Queue rules

- Max 4 posts per day total.
- Publish to both Telegram and VK where possible and allowed.
- Do not publish the same source more than once per day.
- Max 1–2 posts per source per 7 days.
- Prefer diverse sources and topics.
- Prefer new high-quality external sources.
- Avoid same topic/location back-to-back.
- Candidates can expire.
- Dry-run mode is required.
- Auto-publish is disabled by default until explicitly configured.

Suggested slots:

- `10:30`
- `13:30`
- `17:30`
- `20:30`

## State machine

`pending → locked → published|failed|skipped|cancelled`

Idempotency:

- same `candidate_id + target_platform` cannot publish twice unless explicit `force_republish` is set and logged;
- every publish attempt writes/updates queue state and then publication log;
- partial success must be visible (e.g. Telegram published, VK failed).

## Publication lock

Future publisher needs a `region_talk_publication_lock` or YDB coordination node. A discovery job can run without publication lock; publisher needs it before external API calls.

## Rollback limitations

Telegram/VK publication cannot be transactionally rolled back with YDB. Deletion/edit can be attempted later, but ledger must preserve original API responses and deletion status.
