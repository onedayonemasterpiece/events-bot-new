# Promo Campaigns

> Status: design reference; MVP implemented in `docs/features/promo-campaigns/README.md`
>
> Scope: editorial promotion campaigns for specific future events or existing
> festival event sets across video announcements, daily announcements, and
> Telegraph pages.

## Why

Some events need intentionally higher reach than the normal popularity/date
selectors provide. This includes major festivals, single important events, and
partner/editorial pushes where the operator wants a measurable result instead
of a one-off manual tweak.

`INC-2026-05-05-80-stories-video-promo-gap` is the regression contract for this
feature: events from `80 историй о главном` existed in production, but the
system had no explicit mechanism to guarantee public video-announcement
exposure. Do not close that incident by hardcoding the festival into selectors;
promo must be a generic campaign mechanism.

## Research Notes

External promo practice maps well to this feature:

- A campaign should be a coordinated set of actions with one objective, a time
  frame, and reporting, not a hidden flag on one item. Sprout Social describes a
  social campaign as coordinated actions across channels toward measurable
  objectives: https://sproutsocial.com/insights/social-media-campaigns/
- Objectives should define the KPI before launch. For awareness, useful KPIs
  include impressions, reach, share of voice, and video views; for action, use
  clicks, comments, leads, registrations, or conversions. Same source:
  https://sproutsocial.com/insights/social-media-campaigns/
- Content mix matters. A calendar should keep formats varied and match content
  types to goals instead of repeating the same promotion until fatigue:
  https://sproutsocial.com/insights/social-media-calendar/
- If promotion is paid/sponsored/native advertising, visual hints are not
  enough. FTC guidance says required disclosures must be clear, prominent,
  close to the promoted item, and understandable:
  https://www.ftc.gov/business-guidance/resources/native-advertising-guide-businesses

Product interpretation for this bot:

- internal editorial promo may use subtle operator/public highlighting;
- paid or materially sponsored promo must carry an explicit viewer-facing
  disclosure, configured per campaign, instead of hiding behind emoji.

## Product Model

### Campaign

A campaign is the top-level unit the operator manages in `/promo`.

Fields:

- `id`
- `title`
- `status`: `draft | active | paused | archived`
- `goal_comment`: free-text target such as "дать больше охвата фестивалю"
- `starts_at`, `ends_at`
- optional `total_exposure_goal`
- optional `daily_exposure_cap`
- optional `budget_note` / `sponsorship_disclosure`
- `created_by`, `created_at`, `updated_at`, `archived_at`

Status rules:

- `draft`: saved but not eligible for selectors.
- `active`: eligible for placement.
- `paused`: visible in active list but selectors ignore it.
- `archived`: hidden from default list, reports remain available.

### Target

Campaign targets resolve to real database rows. No abstract future festival may
be promoted.

Supported target types:

- `event`: one `event.id`.
- `festival`: one existing `festival.name` plus the future active `event` rows
  where `event.festival == festival.name`.

Validation:

- only future or today events are eligible;
- `lifecycle_status='active'`;
- `silent=false`;
- sold-out events are ineligible for video by default;
- video placements require renderable posters and OCR when that video mode
  requires OCR;
- festival campaign creation is blocked if the festival has no future events in
  the system.

### Activity

A campaign may contain one or many activities. Activities describe where and how
promo is allowed to act.

Fields:

- `campaign_id`
- `surface`: `video_general | video_slot | daily_highlight | telegraph_month | telegraph_weekend | placeholder`
- `profile_key`: optional video profile (`default`, `popular_review`,
  `cherryflash_libsvtav1`, future profiles)
- `slot`: optional integer for slot-bound video placements
- `max_per_publish`: default `1`, hard max `2` for `video_general`
- `target_exposure_goal`: optional count
- `daily_cap`: optional count
- `selection_policy`: `diverse_shuffle | least_recent | fixed_event`
- `enabled`

## Video Placement Contract

There are two video promotion modes.

### General Boost

General boost means "try to add this event/festival into the video if it was not
selected naturally."

Rules:

- default cap: at most `1` general promo event per video release;
- absolute cap: at most `2` general promo events per video release;
- cap is release-wide across all active campaigns;
- promo never bypasses poster/renderability quality gates;
- promo never bypasses the period requirement: the event must fit the release
  candidate window;
- when promoting a festival, rotate through distinct eligible events before
  repeating any event;
- when all eligible festival events have exposure, repeat by least-recent public
  exposure with a small random shuffle among the least-recent bucket;
- general boost should not displace named slot promo; slot promo has its own
  quota and audit line.

This should replace the current `video_include_count` semantics over time.
`video_include_count` can be migrated as a legacy/manual seed into a temporary
campaign, but new implementation should not keep adding product meaning to that
column.

### Named Slot

Named slot means "if eligible, place a promo target into a specific scene
position."

Initial allowed slots:

- CherryFlash / `popular_review`: slots `1..3`;
- CrumpleVideo / default: slots `1..3`;
- future profiles: add explicitly, not by wildcard.

Rules:

- slot placement may exceed the general boost cap because the operator explicitly
  reserved the slot;
- if the requested event/festival has no eligible candidate for that release,
  the slot is skipped and the report records `missed: no eligible event`;
- slot placement must still pass quality gates;
- if several campaigns ask for the same slot, choose by campaign priority and
  then older campaign first; the loser records `missed: slot conflict`.

Implementation anchor:

- default `/v` selection currently has `SelectionContext.promoted_event_ids`,
  `_apply_repeat_limit()`, `mandatory_ids`, and `VideoAnnounceItem.is_mandatory`;
- CherryFlash selection currently has a separate `build_popular_review_selection()`
  with `POPULAR_REVIEW_ANTI_REPEAT_DAYS`;
- the promo service should feed both paths through one resolver and write promo
  provenance into `VideoAnnounceItem` or a join table, not infer it later from
  `video_include_count`.

## Daily Announcement Highlight

Promo highlighting in `/daily` should be subtle unless disclosure is required.

For the "добавили в анонс" section, use standard Unicode emoji, not custom
Telegram emoji, and do not print the word "promo".

Recommended marker:

- `✨` before the title or before the event card.

Rejected markers:

- `⭐`: already means post popularity in post-metrics reports;
- `👍`: already means likes above baseline;
- `🔥`: too close to the old explicit `🔥PROMO` video UI and reads as hype;
- `📌`: better reserved for pinned/manual internal UI.

If a campaign has `sponsorship_disclosure`, daily cards must show explicit text
near the item, for example `Партнёрский материал`, because emoji-only disclosure
is not enough.

## Telegraph Month And Weekend Highlight

Do not create a separate duplicated event block. The event remains in the normal
date order, with one lightweight marker:

- default editorial marker: `✨`;
- sponsored campaigns: explicit disclosure text near the event card;
- festival page/index can additionally show the festival as a highlighted
  current festival, but the source of truth remains the campaign/activity table.

The month/weekend builders should receive a precomputed set of highlighted
`event_id`s from the promo service so Telegraph rendering does not contain ad
hoc campaign logic.

## `/promo` Operator UX

Default screen:

- Active campaigns
- Paused campaigns
- `Создать`
- `Архив`
- `Отчёт`

Campaign card:

- title and status;
- target summary: event title or festival name + count of future eligible events;
- period or remaining exposure quota;
- active activities summary;
- buttons: `Запустить`, `Пауза`, `Архив`, `Отчёт`, `Настроить`.

Create flow:

1. Choose target type: event or festival.
2. Search/resolve target.
3. Enter goal comment.
4. Choose duration or exposure count.
5. Choose activities.
6. Confirm.

Duration default:

- if the operator does not specify a date, `ends_at = now + 3 months`;
- natural-language dates without year resolve to the nearest future date.

## `/a` Natural-Language Entry

The admin assistant should expose promo through allowlisted actions, still with
operator confirmation before mutation.

Examples:

- `/a добавь в промо событие примерное название события`
- `/a продвигай события фестиваля "80 историй о главном" до 18 июля`
- `/a продвигай события фестиваля Кантата`

Intent handling:

- first use deterministic routing for clear promo verbs:
  `продвигай`, `продвинь`, `добавь в промо`, `поставь в продвижение`;
- use LLM only to structure ambiguous text into
  `{target_type, target_query, end_date, exposure_goal, placements}`;
- resolve target against existing DB rows before creating anything;
- if several events/festivals match, show choices instead of guessing;
- if a festival has no future active events, return a clear refusal and do not
  create a campaign;
- if no end date is supplied, use the 3-month default;
- if Gemma 4 parsing fails, times out, or returns low confidence, retry once
  with `gemini-3.1-flash-lite` through an explicit assistant fallback model env,
  then show a clarification instead of creating a weakly grounded campaign.

This routing is command intent, not event semantic extraction, so it does not
replace LLM-first event parsing or Smart Update decisions.

## Reporting

Reports must distinguish planned actions, successful public exposure, test
exposure, and misses.

For each campaign:

- campaign status, target, period/quota;
- activity configuration;
- total eligible events now;
- exposures by surface;
- missed opportunities with reasons;
- remaining exposure goal.

For video exposure:

- event id/title;
- video profile (`default`, `popular_review`, etc.);
- session id;
- session status;
- selected position;
- whether it was natural, general boost, or named slot;
- publication date/time;
- count of actual public publish targets;
- target list: main channel, story targets, Telegram Business targets if present;
- story/report period from `story_publish_report.json` when available.

Only count public release statuses as public exposure:

- count `PUBLISHED_MAIN`;
- count story publish targets from the attached story publish report;
- do not count `FAILED`;
- do not count `PUBLISHED_TEST` unless the campaign activity explicitly targets a
  test audience.

Implementation note: add a normalized `promo_exposure` table instead of parsing
old logs. The video poller should write exposure records when the session reaches
publication state and when story publish reports are downloaded.

Daily summary:

- a morning scheduled promo report can be appended to `/general_stats` rather
  than pinning/editing a separate admin message;
- if a pinned admin-message mechanism is later added, the report can update the
  pinned text, but this is optional. The current pinned-button scheduler is for
  public channel navigation, not admin-report state.

## Suggested SQLite Schema

```sql
CREATE TABLE promo_campaign (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',
  goal_comment TEXT,
  starts_at TIMESTAMP NOT NULL,
  ends_at TIMESTAMP,
  total_exposure_goal INTEGER,
  daily_exposure_cap INTEGER,
  sponsorship_disclosure TEXT,
  created_by BIGINT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  archived_at TIMESTAMP
);

CREATE TABLE promo_target (
  id INTEGER PRIMARY KEY,
  campaign_id INTEGER NOT NULL REFERENCES promo_campaign(id) ON DELETE CASCADE,
  target_type TEXT NOT NULL,
  event_id INTEGER REFERENCES event(id),
  festival_name TEXT,
  query_text TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE promo_activity (
  id INTEGER PRIMARY KEY,
  campaign_id INTEGER NOT NULL REFERENCES promo_campaign(id) ON DELETE CASCADE,
  surface TEXT NOT NULL,
  profile_key TEXT,
  slot INTEGER,
  max_per_publish INTEGER NOT NULL DEFAULT 1,
  target_exposure_goal INTEGER,
  daily_cap INTEGER,
  selection_policy TEXT NOT NULL DEFAULT 'diverse_shuffle',
  enabled BOOLEAN NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE promo_exposure (
  id INTEGER PRIMARY KEY,
  campaign_id INTEGER NOT NULL REFERENCES promo_campaign(id),
  activity_id INTEGER REFERENCES promo_activity(id),
  event_id INTEGER NOT NULL REFERENCES event(id),
  surface TEXT NOT NULL,
  placement_kind TEXT NOT NULL,
  video_session_id INTEGER REFERENCES videoannounce_session(id),
  video_item_id INTEGER REFERENCES videoannounce_item(id),
  position INTEGER,
  publish_status TEXT NOT NULL,
  public_target_count INTEGER NOT NULL DEFAULT 0,
  public_targets_json JSON NOT NULL DEFAULT '[]',
  period_start TIMESTAMP,
  period_end TIMESTAMP,
  published_at TIMESTAMP,
  details_json JSON NOT NULL DEFAULT '{}',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

Indexes:

- `promo_campaign(status, starts_at, ends_at)`
- `promo_target(campaign_id, target_type, event_id, festival_name)`
- `promo_activity(surface, profile_key, enabled)`
- `promo_exposure(campaign_id, published_at)`
- `promo_exposure(event_id, surface, published_at)`

## Initial Test Campaign

Create this campaign when the feature lands and the resolver confirms that the
festival already exists with future active events:

- title: `80 историй о главном / summer visibility`
- target: festival `80 историй о главном`
- status after confirmation: `active`
- starts_at: implementation date
- ends_at: `2026-07-18`
- goal_comment: `дать фестивалю устойчивое присутствие в видеоанонсах и заметность в ежедневных/Telegraph поверхностях до 18 июля`
- activities:
  - `video_general`, profile `popular_review`, max `1` per release, diverse
    festival rotation;
  - `video_general`, profile `default`, max `1` per release, only when event is
    inside the current `/v tomorrow` period and passes poster quality;
  - optional named slots can be enabled manually later, slots `1..3`;
  - `daily_highlight` with marker `✨`;
  - `telegraph_month` and `telegraph_weekend` with marker `✨`.

Do not create this campaign if the festival has no future rows yet. The operator
should first import/backfill festival events, then retry `/a продвигай события
фестиваля "80 историй о главном" до 18 июля`.

## Rollout Plan

1. Add models, migrations, and `promo_service.py` target resolver.
2. Add `/promo` read-only list/report against seeded fake data in tests.
3. Add campaign create/pause/archive flows.
4. Add `/a` allowlist action + deterministic promo routing + Gemini-lite
   fallback for parse failures.
5. Integrate promo resolver into default `/v` selection.
6. Integrate promo resolver into CherryFlash selection.
7. Replace `🔥PROMO`/legacy `video_include_count` UI wording with standard
   markers and provenance from campaign activity.
8. Add daily/Telegraph highlight adapters.
9. Add `promo_exposure` writes from video poller and daily/Telegraph publishers.
10. Add `/promo report` and morning `/general_stats` promo block.
11. Seed the `80 историй о главном` campaign only after validation sees future
    festival events.

Release companion note:

- when this promo project is deployed, also include the already prepared guide
  excursions LLM replacement from the parallel Opus work, unless that change has
  already reached `origin/main` and production separately.

## Tests And Regression Checks

Unit tests:

- festival target creation refuses unknown/no-future festivals;
- no-date `/a` promo command uses `now + 3 months`;
- `18 июля` resolves to `2026-07-18` when run before that date;
- video general boost caps at 1 by default and 2 maximum per release;
- festival rotation covers distinct events before repeats;
- named slot conflicts produce a miss record;
- `PUBLISHED_TEST` is not public exposure by default;
- sponsored campaigns require explicit disclosure text on public surfaces.

Integration tests:

- default `/v` can include one active promo event that was not naturally selected
  but passes the normal quality gates;
- CherryFlash can include one active festival promo event while preserving
  `POPULAR_REVIEW_ANTI_REPEAT_DAYS` for non-promo events;
- `/daily` "добавили в анонс" shows `✨` and never prints `promo`;
- month/weekend pages mark highlighted events without duplicating cards;
- `/promo report` counts a video story fanout from `story_publish_report.json`.

Incident regression:

- for `INC-2026-05-05-80-stories-video-promo-gap`, prove a promoted
  `80 историй о главном` future event can enter a video candidate set and that
  reports do not count failed/test sessions as public exposure.

## Open Questions

- Is every promo editorial, or can some campaigns be paid/sponsored? If paid is
  possible, the UI must require `sponsorship_disclosure` before activation.
- Should named slots have campaign priority, or is oldest-active-first enough?
- Should daily highlight reorder events upward, or only mark them in normal date
  order? Conservative default: mark only, no reorder.
- Should the morning promo summary be folded into `/general_stats` only, or do
  we want a separate admin pinned message later?
