# INC-2026-07-05 Guide Visual Digest Stale VK Booking Link

Status: open
Severity: sev2
Service: guide excursions visual digest / VK and Telegram fanout
Opened: 2026-07-05
Closed: —
Owners: bot operations / guide excursions
Related incidents: `INC-2026-07-05-guide-visual-digest-phone-link`, `INC-2026-04-23-guide-digest-extraction-loss`, `INC-2026-04-21-guide-gemma4-partial-monitoring`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/incident-management.md`

## Summary

Guide visual digest issue `#148` was published to Telegram and VK with the first item, `Путешествие по следам советского кино`, linked to `https://vk.ru/wall-190663987_8037`. That URL is a 2025 source post for an older October occurrence, while the 2026 schedule source is `https://vk.com/wall-190663987_9010` and announces the future occurrence on 2026-07-08.

## User / Business Impact

- Telegram/VK subscribers clicking the first digest item were sent to a stale last-year event post.
- The digest card itself advertises a future July 2026 excursion, so the text link created a public mismatch between current schedule and details/registration surface.
- The issue affects the guide visual digest link-selection contract, not only one rendered card.

## Detection

- Detected by operator review of guide visual digest `#148` in VK.
- Confirmed through production DB rows (`guide_digest_issue.id=148`, `guide_occurrence.id=400`) and authenticated VK API checks for wall posts `-238875824_94`, `-190663987_9010`, and `-190663987_8037`.

## Timeline

- 2025-10-07 09:11 UTC: source group published old detailed post `wall-190663987_8037` for an October 2025 occurrence.
- 2026-07-04 16:51 UTC: source group published July 2026 schedule post `wall-190663987_9010`; the July 8 line links the route title to the old `wall-190663987_8037` post.
- 2026-07-05 07:17–07:19 UTC: production imported occurrence `#400` from Telegram/VK source mirrors.
- 2026-07-05 08:30 UTC: visual digest issue `#148` was created/published.
- 2026-07-05 08:40 UTC: VK wall post `https://vk.com/wall-238875824_94` was published with first link `wall-190663987_8037`.
- 2026-07-05: operator reported the stale first link; investigation confirmed root cause.

## Root Cause

1. The guide LLM extraction treated the inline route-title link in the multi-event schedule block (`[..._8037|путешествие по следам советского кино]`) as `booking_url` for the July 8, 2026 occurrence.
2. That inline link is a stale 2025 detailed announcement, not a safe current registration/details URL for the 2026 occurrence.
3. The visual digest caption builder prioritizes `booking_url` over `source_post_url`/`channel_url`, so the stale `booking_url` became the clickable digest title.
4. There was no guard that demoted historical source-internal wall links in preliminary schedule roundup posts.

## Contributing Factors

- The source schedule reused a historical VK wall link as an inline descriptive link for a repeated route.
- The visual digest stores and reuses `booking_url` in `media_items_json.item_states`, making this field viewer-facing.
- Existing guide digest regression contracts focus on extraction loss, duplicates, partial runs, phones and media, but not stale per-item action links in multi-event source schedules.

## Automation Contract

### Treat as regression guard when

- Changing guide LLM extraction of `booking_url`, `channel_url`, `source_post_url`, or multi-event schedule links.
- Changing `visual_schedule` text builders or `_primary_link` link-priority logic.
- Changing guide dedup/enrichment for repeated routes with historical source links.

### Affected surfaces

- `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`
- `guide_excursions/visual_digest.py`
- guide production tables: `guide_occurrence`, `guide_occurrence_source`, `guide_digest_issue`
- Telegram/VK fanout for `visual_schedule`

### Mandatory checks before closure or deploy

- Add/replay a source fixture for `wall-190663987_9010` where a future schedule line links to a historical wall post.
- Verify occurrence `#400`-equivalent output keeps `date=2026-07-08` but does not expose the 2025 `wall-190663987_8037` as the primary action link.
- Verify visual digest VK and Telegram captions use a current source/safe contact link when `booking_url` is absent, stale, or unsupported.
- Verify authenticated VK API output and Telegram DB/message evidence for the repaired digest surface.

### Required evidence

- Production DB evidence for repaired `guide_occurrence`/`guide_digest_issue` rows or intentionally superseded digest post.
- VK API evidence for public/scheduled post text after repair.
- Telegram message/DB evidence for public captions after repair.
- Regression test or replay output for the stale-link fixture.
- Deployed SHA reachable from `origin/main` if code changes are made.

## Immediate Mitigation

- Pending in this branch: repair occurrence `#400`, issue `#148` captions, and VK wall post `https://vk.com/wall-238875824_94` to use the current schedule source `https://vk.com/wall-190663987_9010`.

## Corrective Actions

- Added a visual digest primary-link guard that demotes stale source-internal VK wall links from preliminary multi-event schedules and falls back to the current source post.
- Added regression coverage for issue `#148` shape.
- Pending production data/public repair.

## Follow-up Actions

- [ ] Add operator/report visibility for `booking_url` vs `source_post_url` in visual digest previews.
- [ ] Consider a guide extraction prompt tightening so stale repeated-route links are not stored as canonical `booking_url` in the first place.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Keep visual digest primary-link selection source-grounded: a booking/details link must be current for the occurrence date, and historical route links from roundup/schedule posts must not outrank the current source post unless the source explicitly says they are the current registration/details page.
