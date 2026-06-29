# INC-2026-06-29 Telegram promo compensation CTA/premium loss and popular repost repeat

Status: open
Severity: sev2
Service: Telegram promo publishing (`@kldevents`, `@kenigevents`)
Opened: 2026-06-29
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-29-80-stories-telegram-promo-gap.md`, `INC-2026-06-15-tg-promo-media-drop-and-bullet-copy.md`, `INC-2026-06-29-tg-premium-ticket-calendar-icon.md`
Related docs: `docs/features/promo-campaigns/README.md`, `docs/features/tg-publishing/README.md`, `docs/features/tg-premium-emojis-update/README.md`

## Summary

Two Telegram promo regressions were reported on 2026-06-29:

1. The broad popular `tg_repost` campaign forwarded the same real title family
   (`Мюзикл «Алиса в Стране чудес»`) to `@kenigevents` twice within 24 hours:
   `https://t.me/kenigevents/4206` on 2026-06-28 18:04 local and
   `https://t.me/kenigevents/4213` on 2026-06-29 12:42 local.
2. Manual compensation posts for the 80 Stories Telegram gap used the promo
   direct publication path instead of the ordinary event-post publisher:
   `https://t.me/kldevents/1611` / album continuation `1612` for event `5077`
   and `https://t.me/kldevents/1613` for event `4417`. They lost the direct
   registration link in the visible/link entity payload; `1613` also had a
   `Подробнее` inline button, and the one-off process did not reliably execute
   the delayed premium/custom emoji editor.

## User / Business Impact

- `@kenigevents` daily/promo feed diversity regressed: readers saw the same
  musical title repeated the next day even though other candidates existed.
- Registration-required 80 Stories compensation posts in `@kldevents` were less
  actionable because they did not carry the direct `kgd80.ru/.../?register=1`
  link.
- Operator confidence in manual compensation degraded because the publication
  bypassed the same post-publication premium emoji behavior as ordinary event
  posts.

## Detection

- Reported by the operator with public Telegram links and screenshot search for
  `алиса` in `@kenigevents`.
- Telethon inspection confirmed `@kldevents/1611`, `1612`, `1613`,
  `@kenigevents/4206`, and `4213` content and entities.
- Production DB confirmed:
  - duplicate repost exposures `promo_exposure.id=576` (event `5290`) and
    `id=584` (event `5291`), both campaign `11`, activity `31`;
  - compensation exposures `id=585` (event `5077`) and `id=586` (event `4417`)
    were created by manual incident compensation;
  - events `5077` and `4417` do have direct registration URLs in
    `event.ticket_link`.

## Timeline

- 2026-06-28 18:04 Europe/Kaliningrad: `@kenigevents/4206` forwarded event
  `5290` (`Алиса`, 2026-07-08).
- 2026-06-29 12:42 Europe/Kaliningrad: `@kenigevents/4213` forwarded event
  `5291` (`Алиса`, 2026-07-11), same normalized title inside 24 hours.
- 2026-06-29 13:05 Europe/Kaliningrad: manual 80 Stories compensation created
  `@kldevents/1611`/`1612` and `@kldevents/1613`.
- 2026-06-29: operator reported both regressions.

## Root Cause

1. `tg_repost` deduplication only remembered exact `details_json.source_url` for
   `dedup_hours`. Different event rows with the same normalized title and
   different `@kldevents` source message ids were eligible on consecutive days.
2. The weighted-popularity selector filtered to candidates with positive source
   popularity scores before diversity was evaluated. A repeated high-scoring
   title could beat a different forwardable candidate whose popularity metric
   was below the per-source median.
3. Manual compensation used `publish_tg_promo_event_publication`, a promo-direct
   renderer, not the ordinary `job_publish_tg_event_post` / 
   `publish_tg_event_announcement` path. That bypassed the canonical ticket line
   and relied on a background delayed premium editor from a short-lived process.

## Contributing Factors

- Existing promo docs mentioned source URL dedup but did not define a title
  repeat cooldown for broad editorial amplification.
- The premium emoji skill/docs did not explicitly warn that one-off repair
  scripts must await or manually run the delayed editor.
- There was no unit test forcing `tg_repost` to choose a lower-scored diverse
  title over a repeated title within a week.

## Automation Contract

### Treat as regression guard when

- changing `promo.py` `tg_repost`, `weighted_popularity`, or broad/all-target
  campaign selection;
- changing `publish_tg_promo_event_publication`, `job_publish_tg_event_post`,
  `publish_tg_event_announcement`, or compensation runbooks;
- making manual/incident compensation Telegram posts in `@kldevents`.

### Affected surfaces

- `promo.py` `PROMO_SURFACE_TG_REPOST` selection and exposure details;
- Telegram event publication formatter and promo direct formatter in
  `main_part2.py`;
- production DB `event.tg_event_post_*`, `promo_exposure`;
- public Telegram channels `@kldevents` and `@kenigevents`;
- Telethon premium/custom emoji editor.

### Mandatory checks before closure or deploy

- Unit tests:
  - `tests/test_promo.py::test_tg_repost_weighted_popularity_prefers_diverse_title_within_week`
  - `tests/test_promo.py::test_tg_repost_weighted_popularity_uses_owned_vk_boost_and_tme_c_source`
  - `tests/test_tg_event_publish.py::test_build_tg_promo_event_publication_formats_markdown_body`
  - `tests/test_tg_event_publish.py::test_tg_promo_event_publish_sends_media_when_full_text_exceeds_caption_limit`
- Post-deploy production config/evidence: activity `31` has title-repeat
  cooldown behavior (default or explicit config) and `/healthz` is OK.
- Public repair smoke for compensation posts: direct registration link entity is
  present, media-group rule is respected (no inline button on albums), and
  premium/custom emoji editor has run or blocker is recorded.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Test output for the mandatory unit tests.
- Telethon reread of repaired `@kldevents` posts and `@kenigevents` duplicate
  evidence.
- Production DB rows for repaired event ids and exposure rows.

## Immediate Mitigation

- Code fix in progress: add normalized-title repeat cooldown for `tg_repost` and
  schedule premium editor from promo direct posts.
- Production repair required: republish events `5077` and `4417` through the
  standard event-post publisher, delete/supersede bad compensation posts
  `1611`/`1612`/`1613`, and reconcile DB/exposure rows.

## Corrective Actions

- Add `tg_repost` repeat cooldown (`repeat_cooldown_days` /
  `repeat_cooldown_hours`, default 7 days). Exact source URL dedup remains;
  same-title repeats are allowed only when no other forwardable candidate exists.
- Include zero/low-scored forwardable candidates in the weighted selector so
  diversity can beat a repeated high-scoring title.
- Make promo direct Telegram posts reuse the canonical ticket line and schedule
  the premium emoji editor on sent message ids.
- Document compensation requirements in Telegram publishing and premium emoji
  runbooks/skill.

## Follow-up Actions

- [ ] Finish production repair for `@kldevents/1611`/`1612`/`1613`.
- [ ] After deploy, verify the next `tg_repost` run does not choose an `Алиса`
      title again unless the candidate inventory has no alternatives.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: —
- post-deploy verification: —

## Prevention

- Regression tests and docs now encode the one-week diversity cooldown and the
  compensation publication contract.
