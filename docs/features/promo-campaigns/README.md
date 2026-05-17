# Promo Campaigns

> **Status:** MVP implemented  
> **Scope:** editorial promotion campaigns for existing future events and existing festival event sets.

Promo is an explicit campaign layer for events that need more reach than normal
date/popularity selectors give them. The first live use case is
`80 историй о главном`: when the festival exists in the DB and has future active
events, the bot can seed an active campaign through 2026-07-18.

The full product design and researched promo practices remain in
`docs/backlog/features/promo-campaigns/README.md`; this document is the
implemented behavior contract.

## Data Model

- `promo_campaign`: title, status (`draft | active | paused | archived`), goal,
  start/end dates, priority (`0..3`, where `0` is highest), optional exposure
  caps/disclosure fields.
- `promo_target`: either one real `event.id` or one existing `festival.name`.
- `promo_activity`: where the campaign can act. MVP surfaces are
  `video_general`, `daily_highlight`, `telegraph_month`, `telegraph_weekend`.
- `promo_exposure`: normalized exposure audit rows. MVP writes video exposure
  rows when a promoted video item reaches a viewer-facing publication target:
  `PUBLISHED_MAIN`, or the scheduled CherryFlash target that is still stored by
  the legacy video-announcement status machine as `PUBLISHED_TEST`.
- `videoannounce_item` stores promo provenance:
  `promo_campaign_id`, `promo_activity_id`, `promo_placement_kind`.

Targets are never abstract: a festival campaign is refused until the name is
grounded by an existing `festival` row or by future active event rows whose
`event.festival` already equals that name. Event/festival rotation uses only
events whose start date is today or later.

## Operator Commands

- `/promo` or `/promo list` lists non-archived campaigns and seeds the
  `80 историй о главном` campaign if the festival is eligible.
- `/promo seed80` explicitly creates/returns the initial campaign for
  `80 историй о главном` through 2026-07-18.
- `/promo report` lists all campaigns, including archived, with current future
  event count, video publication count, promo-show count, and per-session
  CherryFlash publication details: date/time, profile, session id, stored
  status, target count, positions, and event ids/titles.
- `/promo add festival НАЗВАНИЕ [до ДАТА]` creates an active festival campaign.
- `/promo add event ПРИМЕРНОЕ НАЗВАНИЕ [до ДАТА]` finds a future event and
  creates an active event campaign. If several future events are too similar,
  the bot asks the operator to уточнить instead of guessing.
- `/promo pause ID`, `/promo start ID`, `/promo archive ID` change status.

Dates accept `YYYY-MM-DD`, `DD.MM[.YYYY]`, and Russian forms like `18 июля`.
When no date is supplied, the campaign ends after 90 days.

## `/a` Entry

The admin assistant allowlist includes `promo`.

Deterministic routing handles clear forms before LLM:

- `/a продвигай события фестиваля "80 историй о главном" до 18 июля`
  routes to `/promo seed80`.
- `/a продвигай события фестиваля Кантата` routes to
  `/promo продвигай события фестиваля Кантата`.
- `/a добавь в промо событие примерное название` routes to `/promo ...`.
- `/a покажи отчёт по промо` routes to `/promo report`.

All `/a` actions still require operator confirmation.

## CherryFlash

CherryFlash (`popular_review`) calls the promo resolver after collecting normal
popular-post candidates. General promo:

- has a global video promo budget of two events per CherryFlash release;
- honors campaign-level `total_exposure_goal` and local-day
  `daily_exposure_cap` (`Europe/Kaliningrad`) before choosing activities;
- honors activity-level `target_exposure_goal`, `daily_cap`, and `slot=1` /
  `selection_policy=first_slot` for one guaranteed first-slot exposure;
- resolves campaigns by priority first (`0` highest, `3` lowest), then creation
  order, so when many campaigns are active the two available video promo seats
  go to the highest-priority eligible campaigns;
- is interleaved with organic popularity picks;
- starts a promo item in position 1 only when the activity explicitly requests
  `slot=1` or `selection_policy=first_slot`; otherwise guaranteed-any-position
  promo replaces tail organic items when needed;
- avoids filling positions 1 and 2 with two promo items when an organic event is
  available;
- does not bypass future-date checks;
- does not bypass renderable-poster checks;
- stores promo provenance on `VideoAnnounceItem`;
- uses festival rotation by viewer-facing exposure count and stable daily
  shuffle among equally exposed future events.

Event promo campaigns may end before the promoted event date. The campaign end
date limits when promotion can run; the event itself only needs to be an active
future event that is renderable.

`80 историй о главном` is the initial special policy:

- campaign priority is `1`;
- the video activity uses `selection_policy=guaranteed_any_position`;
- it does not force slot 1 or 2;
- it is guaranteed into CherryFlash at any available/lower position, replacing
  tail organic items if the normal organic list is already full;
- it may contribute up to two future events when the promo budget has room.
- for `partner_eco_nature_001`, a non-eco `80 историй` promo item may appear
  only under the eco partner exception: at least three profile-matched
  eco/nature/local-history events are already selected, and only one off-filter
  promo candidate is admitted at any-position placement, never as first-slot
  pressure.

`Спектакль 8 ЖЕНЩИН` (event `4617`, show date 2026-05-22) is a live one-off
campaign created on production on 2026-05-17:

- campaign ends at the end of 2026-05-21 local day;
- campaign total exposure goal is `2`, with daily cap `1`;
- one activity is `selection_policy=first_slot`, `slot=1`,
  `target_exposure_goal=1`, `daily_cap=1`;
- one activity is `selection_policy=guaranteed_any_position`,
  `target_exposure_goal=1`, `daily_cap=1`;
- the two activities must therefore be satisfied on different local days.

This directly covers the regression contract from
`INC-2026-05-05-80-stories-video-promo-gap`: a promoted future festival event
can enter the video candidate set, while failed/test sessions are not counted as
public exposure. The scheduled `@keniggpt` CherryFlash target is an exception to
the old status wording: it is viewer-facing production output even when the row
status is `PUBLISHED_TEST`, so it is reported and recorded as a promo show.

## Daily Marker

The "добавили в анонс" daily section can mark promoted events with standard
Unicode `✨`. It intentionally does not print the word `promo`.

The marker is subtle editorial highlighting, not an advertising disclosure. If a
future campaign is paid/sponsored, it must use explicit disclosure text before
activation.

## Current Limits

- MVP implements CherryFlash general boost and daily-section highlighting.
- Telegraph month/weekend activities are stored for campaigns, but page-render
  adapters are still pending.
- First-slot video promo is implemented through `slot=1` /
  `selection_policy=first_slot`; broader named video slots are still pending.
- `/promo report` reconstructs viewer-facing CherryFlash publications from
  `videoannounce_session`/`videoannounce_item` and uses `promo_exposure` as the
  normalized audit trail. Story target fanout details can be expanded from
  `story_publish_report.json` in a later pass.
- `/promo` now sends an inline management keyboard with report, seed80,
  pause/start/archive and priority buttons. The same menu is reachable from
  `/v` through `✨ Промо-кампании`.
