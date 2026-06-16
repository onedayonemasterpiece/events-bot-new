# INC-2026-06-16 VK quality: duplicates, non-events, near-duplicate posters

Status: open
Severity: sev1
Service: VK auto-import / Smart Update / managed VK event publishing
Opened: 2026-06-16
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-12-future-event-quality-llm-first-repair.md`, `INC-2026-06-07-tg-event-publishing-media-calendar-dedup.md`, `INC-2026-06-14-vk-publication-cta-plain-duplicate.md`
Related docs: `docs/features/source-parsing/README.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

16 июня в production surfaced several quality regressions in managed `klgdevents` VK event posts and the event inventory:

- `https://vk.com/wall-231920894_3502` published two visually near-duplicate illustrations for one event.
- `https://vk.com/wall-231920894_3484` and `https://vk.com/wall-231920894_3485` were public posts for rubric/digest stubs (`Дайджест`), not concrete attendable events.
- `https://vk.com/wall-231920894_3494` and `https://vk.com/wall-231920894_3496` were duplicate managed VK posts for the same event `Снаружи всех измерений…`.
- Future listings contained duplicate `Калининградская музыкальная ночь` entries for 20 июня, with the same title/date/time but different extracted venues/Telegraph URLs.

The fix must remain LLM-first for semantic eventness and duplicate/match decisions. Deterministic code may only provide narrow recall/safety guardrails: media near-duplicate filtering, idempotency of managed VK post URLs, and routing suspicious candidates to LLM review.

## User / Business Impact

- Public channels received bad managed event posts: non-events looked like events, and duplicate wall posts inflated feed noise.
- Poll/recommendation and digest features can select poor candidates if false events and duplicates remain active.
- Duplicate posters reduce perceived quality of VK cards.
- Database bloat grows through repeated active rows for the same real-world event.

## Detection

Reported manually by the operator with concrete public VK URLs and duplicate listing examples. Runtime/file-log and production DB checks are required; public VK HTML alone is not evidence.

## Timeline

- 2026-06-16 — operator reports duplicate illustrations, non-event digest posts, duplicate VK posts, and duplicate Music Night listings.
- 2026-06-16 — authenticated VK API inspection confirms exact posts and attachment counts.
- 2026-06-16 — production DB correlation finds active false-event rows `6058`/`6059`, duplicate-post event `5925`, and Music Night candidates `6048`/`6067`.

## Root Cause

1. Weak rubric/digest Telegram/VK candidates could reach Smart Update with hallucinated anchors (`title=Дайджест`, `location_name=приходи`) without a dedicated LLM-first eventness review at the weak-candidate boundary.
2. Duplicate matching shortlist recall was too venue-centric for citywide/festival-like events: same title/date/time candidates could be hidden from the LLM matcher when one source extracted a contextual phrase as venue.
3. VK managed post idempotency depended on resolving existing managed URLs; postponed/inaccessible wall items must be treated as existing instead of triggering a second public post.
4. Poster dedupe threshold was too strict for generated/resized near-duplicate illustrations: the incident pair differed by perceptual hash distance above the old default while still being visually duplicate.

## Contributing Factors

- Source posts can be very short (`Дайджест - посмотри, приходи`) while upstream extraction still emits event candidates.
- `source_vk_post_url` can point to managed repeaters and must not be treated as a factual source.
- Same citywide/festival event can be reposted by multiple sources with different contextual venue text.

## Automation Contract

### Treat as regression guard when

- Changing Telegram/VK extraction, Smart Update non-event handling, LLM event creation prompts, duplicate matching, event source fanout, VK `vk_sync`, or poster/media dedupe.
- Touching managed `klgdevents` VK publication idempotency or postponed-post resolution.
- Adding deterministic non-event/duplicate guards that could bypass LLM semantic decisions.

### Affected surfaces

- `smart_event_update.py`: weak candidate eventness review; shortlist recall; duplicate match flow; poster dedupe.
- `main.py` / `main_part2.py`: `vk_sync`, managed VK URL existence, postponed lookup, VK photo upload dedupe.
- Production DB tables: `event`, `event_source`, `joboutbox`, `event_publication`, `event_poster`.
- VK API: `wall.getById`, `wall.get filter=postponed`, `wall.edit`, `wall.delete`.
- Smoke paths: source replay through production import boundary + Smart Update; future active listing audit.

### Mandatory checks before closure or deploy

- Unit tests for LLM-first weak rubric/digest eventness review:
  - `Дайджест - посмотри, приходи` is routed to LLM and skipped when LLM says `non_event`.
  - A real concise event invite remains importable when LLM says `event`.
- Unit/integration tests for Music Night-style duplicate recall:
  - same title/date/time, citywide/festival signals, different extracted venues => existing event is visible to LLM and can match.
  - distinct same-day venue-specific events are not blanket-merged without LLM.
- Unit tests for near-duplicate poster/media dedupe at the incident hash distance.
- Regression test for managed VK postponed URL idempotency: `wall.getById` empty + postponed found => no duplicate post/requeue.
- Production repair verification:
  - false digest events are no longer active and pending fanout is cancelled;
  - bad managed VK posts are deleted or edited;
  - duplicate event rows are merged/silenced according to LLM-first decision;
  - no replacement duplicate jobs remain pending.
- Release governance checks: clean worktree, branch based on `origin/main`, fix committed/pushed and reachable from `origin/main` before production closure.

### Required evidence

- Authenticated VK API evidence for reported URLs before/after repair.
- Production DB query output for affected event/job rows before/after repair.
- Test command output for targeted tests and replay.
- Deployed SHA and Fly deploy/log evidence.

## Immediate Mitigation

Pending. Required actions:

- Delete or edit the public bad managed VK posts after owner verification.
- Mark false digest rows non-active and cancel their pending publication jobs.
- Merge/silence duplicate Music Night row(s) only after LLM-first duplicate decision.

## Corrective Actions

Pending implementation:

- Add LLM-first eventness review for weak rubric/digest candidates before create/update.
- Add shortlist recall for citywide/festival same-title/date/time candidates so LLM can decide merge instead of creating duplicates.
- Raise/verify perceptual dedupe threshold for managed event poster publication.
- Keep managed VK postponed resolution as an idempotency guard and test it against this incident class.

## Follow-up Actions

- [ ] Add scheduled future-active audit for generic/rubric titles (`Дайджест`, `Афиша`, `Куда сходить`) with public-post containment.
- [ ] Add observability counter for LLM eventness review decisions and fail-closed weak-candidate skips.
- [ ] Consider a richer citywide/festival entity model so umbrella events and venue-specific programme items do not fight in one duplicate matcher.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

This incident is the regression contract for future changes touching eventness, duplicate matching, VK idempotency, and media dedupe. Closure requires replay through the Smart Update boundary; prompt-only changes or manual SQL repairs are insufficient.
