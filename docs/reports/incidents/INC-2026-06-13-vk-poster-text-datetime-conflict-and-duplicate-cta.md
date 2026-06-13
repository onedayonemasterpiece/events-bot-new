# INC-2026-06-13 VK Poster/Text Datetime Conflict And Duplicate CTA

Status: mitigated
Severity: sev1
Service: VK auto-import / Smart Update / public Telegram and VK event surfaces
Opened: 2026-06-13
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-12-future-event-quality-llm-first-repair`, `INC-2026-06-12-vk-partial-media-family-cta`
Related docs: `docs/features/smart-event-update/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

`@kldevents/417` published event `5960` with date `2026-06-18`, empty time, and broad venue `Музей Мирового океана`, while the attached poster OCR clearly contains `19 ИЮНЯ 16:00`, `ОБРАЗОВАТЕЛЬНЫЙ ЦЕНТР «ОКЕАНиЯ»`, `наб. Петра Великого, 1Б`.

The source text says only the relative anchor `в этот четверг`, without time. Posters can also be wrong, so the fix must not blindly treat posters as truth. The required heuristic is:

- explicit text date such as `18 июня` or `18.06` is strong and must not be silently overwritten by a poster;
- relative text date such as `в этот четверг`, with no explicit day/month and no time, is weaker than exactly one poster OCR anchor containing `DD month HH:MM`;
- when that narrow relative-vs-poster shape is detected, import must use the poster date/time/location instead of publishing a half-populated relative-date card.

The same incident also exposed publication duplicates:

- VK event posts `https://vk.com/wall-231920894_3237` and `https://vk.com/wall-231920894_3238` were created back-to-back for the same real event, one without CTA and one with CTA. The CTA version is the intended priority public VK post. This is a duplicate-publication/fanout coalescing failure, not a separate content event.
- VK postponed queue contained a duplicate `Кормление колобусов`: DB pointed at the already-published live post that VK remapped from `3182` to `3243`, while another postponed copy `3183` remained in queue.
- VK postponed queue also contained 72 `AfishaEngagement` debug copies marked `[AFISHAENGAGEMENT DEBUG COPY — DELETE BEFORE PUBLISH]`; cleanup only matched the current `debug_marker`, so older marker/build variants survived.
- A same-source feeding series from `https://t.me/kldzoo/7521` was represented in DB as separate real events `5974..5980`, which is correct for event storage, but public Telegram/VK fanout incorrectly produced individual posts instead of one schedule post. Fresh VK duplicates included `3244` and `3245`; the canonical schedule post is `https://vk.com/wall-231920894_3243`.

## User / Business Impact

- Readers see wrong or incomplete event logistics for a public card.
- Telegram/Telegraph omitted the start time even though poster OCR had it.
- VK received two consecutive posts for the same event, creating duplicated public communication and weakening trust in managed fanout.
- Serial events from one source/photo set/location created feed spam instead of one public schedule, even though the user-facing object is a single day schedule.
- The issue is not solvable by manual review because no review workflow is planned for this pipeline; prevention must be encoded in LLM-first prompts and deterministic guardrails.

## Detection

- Operator reported `https://t.me/kldevents/417` as an incident with different date/time data in text and poster.
- Production DB confirmed event `5960` had:
  - `date=2026-06-18`
  - `time=''`
  - `location_name=Музей Мирового океана`
  - `location_address=наб. Петра Великого 1`
  - `tg_event_post_url=https://t.me/c/3954607218/417`
- Event poster OCR confirmed:
  - `19 ИЮНЯ 16:00`
  - `ОБРАЗОВАТЕЛЬНЫЙ ЦЕНТР «ОКЕАНиЯ»`
  - `наб. Петра Великого, 1Б`
- Operator additionally reported duplicate managed VK posts:
  - `https://vk.com/wall-231920894_3237`
  - `https://vk.com/wall-231920894_3238`
- Authenticated VK API verification confirmed:
  - `3237` and `3238` had identical text hash, but `3238` used the CTA image and was kept as canonical.
  - `3183` was the leftover postponed duplicate for `Кормление колобусов`; live canonical post was `3243`.
  - 72 AfishaEngagement debug postponed posts carried the explicit delete-before-publish banner.
- Production DB confirmed feeding rows `5974..5980` share:
  - `source_post_url=https://t.me/kldzoo/7521`
  - date `2026-06-13`
  - location `Калининградский зоопарк`, `пр-т Мира 26`, `Калининград`
  - identical source photo signature
- Authenticated VK API confirmed canonical feeding schedule post `3243`; stale individual posts `3244` and `3245` were deleted, and postponed feed contains no remaining feeding duplicates.

## Root Cause

1. VK parser prompt did not explicitly rank relative text date anchors against exact poster OCR anchors.
2. Deterministic post-parse normalization filled missing time from weak hints but did not have a safe exact-poster override for the single-event, single-poster case.
3. Public AfishaEngagement CTA fanout created a second managed VK post for an event that already had a managed VK post, instead of editing the existing post with the CTA attachment.
4. AfishaEngagement debug cleanup only matched the current `debug_marker`, not the explicit debug-copy banner, so older debug builds accumulated in postponed queue.
5. Same-source serial event fanout only coalesced explicit `linked_event_ids`; it did not infer a public schedule from repeated source/photo/location rows. Existing VK edit behavior appended text revisions, which left stale single-event copy above the new schedule when repairing an existing post.
6. A public AfishaEngagement duplicate could cause the candidate loop to fall through into a shadow/debug candidate, creating a new scheduled debug copy even after the public CTA already existed.
7. The first AfishaEngagement public duplicate fix only handled events that already had a managed VK post URL. For a brand-new `vk_sync`, the plain `post_to_vk` still ran before CTA generation, so a successful public CTA could still leave a plain postponed post plus a CTA postponed post.

## Automation Contract

### Treat as regression guard when

- changing VK auto-import LLM prompt, poster OCR handoff, date/time normalization, or Smart Update handoff;
- changing `tg_event_publish`, `vk_sync`, CTA injection, or managed VK post coalescing;
- changing same-source serial event grouping, especially repeated source/photo/location event rows such as zoo feeding schedules;
- rebuilding or repairing public Telegram/VK event posts.

### Mandatory checks before closure or deploy

- Add regression coverage for a VK source whose text says `в этот четверг`, while poster OCR says `19 ИЮНЯ 16:00`.
- Prove the relative-date case becomes `2026-06-19 16:00`, with poster-local venue/address.
- Add a negative guard: an explicit text date must not be overwritten solely because a poster shows a different date.
- Verify `event 5960` is repaired in DB and public surfaces.
- Verify only one managed VK post remains active/primary for the event; if one cannot be deleted, edit it to point to the canonical card.
- Verify same-source serial rows can remain separate in DB while Telegram/VK receive one schedule post and copy that publication URL/hash back to all covered rows.

## Corrective Actions

- VK LLM prompt now states that relative text anchors like `в этот четверг` are weaker than a single exact poster OCR anchor `DD month HH:MM`, but explicit text dates are not automatically weaker than posters.
- VK post-parse normalization now extracts a single poster datetime/location anchor and applies it only when the source date is missing, matching, or relative-only without explicit day/month.
- Regression test added for the `kldevents/417` shape.
- Public AfishaEngagement CTA fanout now edits an existing managed event VK post when one is supplied, instead of creating a duplicate public post with the same text.
- AfishaEngagement cleanup now deletes postponed posts carrying the `AFISHAENGAGEMENT DEBUG COPY` banner even when the old debug marker differs from the current marker.
- Added a Codex skill `events-bot-vk-posts` so future VK post checks use authenticated VK API/tokens instead of public `vk.com` HTML.
- Same-source feeding series now coalesces at publication time when active same-day rows share source URL, exact photo signature, real location key, and feeding-style titles. DB events stay separate; Telegram/VK public surfaces use one schedule post.
- VK sync for inferred serial schedules replaces existing post text instead of appending a new revision block.
- AfishaEngagement suppresses shadow/debug fallback candidates after a public duplicate is already detected for the event.
- Fresh managed VK event posts now run AfishaEngagement public CTA as a preflight before the plain `wall.post`. When CTA generation succeeds, the CTA URL is returned as the event's managed VK post and the plain create is not executed; if the CTA preflight skips/fails, normal VK publication remains the fallback. Existing managed VK URLs stay on the update/repair path and are not retroactively converted into CTA by this fix.

## Follow-up Actions

- [x] Repair production event `5960` to `2026-06-19 16:00`, `Образовательный центр «ОКЕАНиЯ»`, `наб. Петра Великого, 1Б`.
- [x] Refresh Telegraph and Telegram post `@kldevents/417`.
- [x] Verify VK posts `3237/3238`; keep canonical CTA post `3238`, delete duplicate `3237`, and update DB source link.
- [x] Repair `История становления калининградского здравоохранения`: keep canonical CTA post `3240`, delete duplicate plain post `3239`, and update event `5989` DB source link to `3240`.
- [x] Repair `Кормление колобусов`: update DB source link `3182 -> 3243`, delete postponed duplicate `3183`.
- [x] Delete postponed AfishaEngagement debug copies marked with the delete-before-publish banner.
- [x] Add fanout coalescing guard so CTA public engagement changes edit existing managed VK posts instead of publishing a second post.
- [x] Add fresh-create preflight guard so a successful public CTA suppresses the plain managed VK create for the same publication pass.
- [x] Coalesce feeding series `5974..5980` to one public schedule post while preserving separate DB events.
- [x] Delete fresh individual feeding VK duplicates `3244` and `3245`.
- [x] Verify VK postponed feed has no remaining feeding duplicates and no AfishaEngagement debug markers.

## Release And Closure Evidence

- deployed images:
  - `registry.fly.io/events-bot-new-wngqia:deployment-01KV00H8PG979N9QY2A8HTVFZJ`
  - `registry.fly.io/events-bot-new-wngqia:deployment-01KV00X0SE03YX9R503JCC99P5`
- deploy path: `fly deploy --remote-only -a events-bot-new-wngqia`
- regression checks:
  - `python3 -m py_compile main.py afishaengagement.py main_part2.py vk_intake.py tests/test_tg_event_publish.py tests/test_afishaengagement.py`
  - local pytest unavailable in this incident worktree: `/usr/bin/python3: No module named pytest`
- production repair evidence:
  - `@kldevents/417` embed shows `19 июня 16:00` and `ОКЕАНиЯ`.
  - `@kldevents/366` embed shows `Кирха Гердауэн, #Железнодорожный`.
  - VK API `wall.getById -231920894_3208` shows `📅 19 июня 16:00` and `📍 Образовательный центр «ОКЕАНиЯ», наб. Петра Великого 1Б`.
  - VK API keeps `3238` and `3243`; `3237` is deleted, `3183` is absent from active verification.
  - VK postponed debug banner verification: `[]`.
  - Feeding rows `5974..5980` all point to `https://vk.com/wall-231920894_3243` and `https://t.me/c/3954607218/436`.
  - VK API `wall.getById -231920894_3243` shows one clean schedule post with all seven times `11:30`, `13:30`, `14:00`, `14:30`, `15:00`, `15:30`, `16:00`; stale `Ветеринарный экспресс` and `ОРГАНИЗАЦИОННЫЕ ДЕТАЛИ` blocks are absent.
  - VK owner-feed verification shows only canonical feeding schedule `3243`; user-token postponed-feed verification for feeding duplicates returns `[]`; debug marker verification returns `[]`.
