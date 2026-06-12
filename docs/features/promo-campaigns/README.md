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

Partner-facing UX (the `🎬` button on `/events`, the 6-step creation FSM, the
shared `/promo` management menu, the upcoming KONB-CherryFlash auto-promote
rule, and the VK-repost activity type) lives in a dedicated canonical spec:
[partner-promo.md](partner-promo.md).

## Data Model

- `promo_campaign`: title, status (`draft | active | paused | archived`), goal,
  start/end dates, priority (`0..3`, where `0` is highest), optional exposure
  caps/disclosure fields.
- `promo_target`: one real `event.id`, one existing `festival.name`, or a
  Telegram source author (`target_type='tg_chat_author'`, `query_text`
  `"<chat>:<author>"`). See "Author-in-chat trigger" below.
- `promo_activity`: where the campaign can act. MVP surfaces are
  `video_general`, `daily_highlight`, `telegraph_month`, `telegraph_weekend`,
  `daily_recommend_today`, `vk_publication`, `tg_event_publish`, `tg_repost`,
  `vk_repost`, `vk_story`, and `afishaengagement`. Social activity parameters
  live in `promo_activity.config_json` (`target_group`, `source_group`,
  `target_chat`, `source_chat`, `window_hours`, `active_start_hour`,
  `active_end_hour`, dedup policy).
- Telegram event publishing is channel-default behavior, not a separate
  campaign activity: when an event is covered by any active `promo_target`, the
  `@kldevents` publisher renders that event with its promo intro/CTA style.
  Campaign authors must not add a separate `tg_event_publish` activity just to
  make ordinary Telegram copy richer. The `tg_event_publish` promo activity is
  only for an explicit extra campaign slot in an event-flow channel; its output
  can then be used as a source by `tg_repost`.
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
  status, target count, positions, and event ids/titles. VK promo activities
  are shown as concrete exposure rows with event id/date, status, source URL
  for reposts, and target VK post URL.
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
  order;
- distributes the global video promo budget fairly across eligible
  campaigns/activities before giving any single campaign a second item. A
  campaign with `max_per_publish=2` must not consume both seats while another
  eligible active campaign has a renderable candidate;
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
- it is guaranteed into CherryFlash at a stable daily pseudo-random lower
  position, replacing tail organic items if the normal organic list is already
  full. The resolver must not always append this placement at the end of the
  video;
- it may contribute up to two future events when the promo budget has room, but
  the fair-budget rule above lets another eligible campaign take one seat first.

Partner CherryFlash tracks may consider global promo campaigns, but their
topic/profile filters are hard gates: a promo item that fails the partner
filter is skipped, not admitted as an off-filter exception. Promo must never be
used as aggressive pressure that violates the partner track's subject contract.

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

## VK Activities

`vk_publication` keeps a rolling-window minimum of event posts in a configured
VK community. The lightweight `promo_vk` scheduler runs every 30 minutes by
default, but new actions are started only inside the activity's local active
window (default 09:00-21:00 Europe/Kaliningrad). For each activity the daily
minimum is split into even due-slots at the midpoint of equal slices: for
`max_per_publish=2`, the slots are about 12:00 and 18:00; for `1`, about
15:00. One scheduler tick can create at most one missing post per activity, so
catch-up is gradual rather than a same-minute batch.

For each active campaign/activity the scheduler:

- resolves the target campaign events (festival campaigns rotate through future
  active event rows);
- counts organic Smart Update posts in the target community during the last
  `window_hours` (default 24) via `event.source_vk_post_url` + VK `wall.getById`;
  if the stored URL is a stale VK postponed id, it is resolved to the live wall
  id and saved back to the event before the count is evaluated;
- counts already recorded promo VK exposures for due-slot fulfilment in the
  activity's current local calendar day, not in a rolling 24-hour window, so an
  evening exposure from yesterday cannot suppress today's 15:00 local slot;
- if the count is below the number of slots due by the current local time,
  publishes one missing post as a postponed community wall post through the
  standard VK wall contract (`post_to_vk`, community author, postponed queue),
  using the same source-style event message format as Smart Update;
- treats missing media for Telegram-origin events, and empty VK upload for any
  event that already has media URLs, as a promo data incident rather than a
  reason to silently degrade reach. The runner first tries to recover renderable
  media from the event Telegraph page when `event.photo_urls` is empty; if no VK
  photo attachment is still available, it fails closed with
  `vk_sync_missing_media_for_telegram_event`, records `FAILED_NO_MEDIA` audit
  evidence (`source_post_url`, `photo_urls_count`, `attachments_count`, recovery
  action), and lets the rolling slot fall through to another campaign event with
  media. Operator follow-up must investigate the source media path and rehydrate
  the original promo event before treating the campaign as repaired.
- records each scheduled post in `promo_exposure` with
  `surface='vk_publication'`, `publish_status='VK_SCHEDULED'` and
  `details_json.target_url`. Because the VK wall/story call is an external side
  effect, exposure recording retries transient SQLite locks before the scheduler
  is allowed to treat the action as failed. Rolling-window counts consider only
  public-success statuses (`VK_SCHEDULED`, `PUBLISHED`, `PUBLISHED_MAIN`,
  `PUBLISHED_TEST`); failed/invalidated rows such as `FAILED_NO_MEDIA` remain
  audit evidence but do not satisfy the promo minimum.

`vk_repost` watches a source community and reposts a recent campaign event post
to a target community. It considers both organic `event.source_vk_post_url` rows
and `vk_publication` exposure URLs from the last `window_hours` for source
selection and dedup, but counts already delivered repost exposure against the
current local calendar day. It only selects a source after VK `wall.getById`
shows that the source post's publish date is not in the future. If VK
reassigned the wall id when a postponed post became public, the runner resolves
`postponed_id -> live id` before source selection; organic event URLs are
persisted back to `event.source_vk_post_url`, and promo exposure URLs are
reconciled in `promo_exposure`. The repost caption uses the short VK rewrite helper's text-only summary
(`build_short_vk_text`): no title infoblock, no logistics block, no hashtags.
The repost result is recorded in `promo_exposure` with
`surface='vk_repost'`, `details_json.source_url` and
`details_json.target_url`.

`vk_story` watches the same kind of source-community event posts and publishes a
caption-free image story into a configured target community. The story media is
the source wall post's first photo, with fallback to the promoted event's stored
poster; title/date/venue text is not rendered into a white card. The story links
do not pass the source wall URL as VK `link_url`: VK renders wall links as a
large white post/caption card under the image, which breaks the poster-only
story contract.
Like reposts, story source selection waits until the source wall post is public
and reconciles stale postponed ids before publishing, while daily story fulfilment
is counted against the current local calendar day. Story delivery is complete
only after `stories.save`; the exposure row uses `surface='vk_story'`,
`public_targets_json.type='vk_story'`, and stores `details_json.source_url`,
`details_json.target_url`, `owner_id`, `story_id`, and `expires_at`.

`tg_event_publish` publishes a full promo event post into a configured Telegram
event-flow channel, normally `@kldevents`. It is scheduled by the same
lightweight promo runner as VK activities and respects the same local active
window / due-slot rules. The activity is intentionally separate from ordinary
Smart Update Telegram event publication: it creates an explicit campaign post,
records `promo_exposure.surface='tg_event_publish'` with
`publish_status='TG_PUBLISHED'`, and stores the resulting source post URL back
on the event for downstream reposts.

`tg_repost` forwards an existing source-channel post, normally
`@kldevents -> @kenigevents`, instead of rendering a new text post in the
daily/digest channel. It looks at the promoted event's stored
`event.tg_event_post_url` and recent `tg_event_publish` exposures, applies
`dedup_hours` to source URLs, forwards with Telegram Bot API, and records
`promo_exposure.surface='tg_repost'` / `publish_status='TG_FORWARDED'` with
`details_json.source_url` and `details_json.target_url`.

The initial `80 историй о главном` campaign now includes:

- `vk_publication` to `https://vk.com/klgdevents`, `max_per_publish=2`,
  `daily_cap=2`, 24-hour window, active window 09:00-21:00;
- `vk_repost` from `https://vk.com/klgdevents` to
  `https://vk.com/kenigeventsofficial`, `max_per_publish=1`, `daily_cap=1`,
  24-hour window, active window 09:00-21:00, and 72-hour source-post dedup.
- `vk_story` from `https://vk.com/klgdevents` to
  `https://vk.com/klgdevents`, `max_per_publish=2`, `daily_cap=2`, 24-hour
  window, active window 09:00-21:00, and 72-hour source-post dedup;
- `vk_story` from `https://vk.com/klgdevents` to
  `https://vk.com/kenigeventsofficial`, `max_per_publish=2`, `daily_cap=2`,
  24-hour window, active window 09:00-21:00, and 72-hour source-post dedup.
- `afishaengagement` for `https://vk.com/klgdevents`, debug shadow enabled,
  `apply_rate=0.70`, likes-only registration CTA
  `Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}.`, and all
  visual formats enabled for the first VK visual-debug pass. Existing legacy
  `klgdevents:motivation:80stories` activities are synchronized into the
  canonical `klgdevents:afishaengagement` profile instead of creating a second
  active `afishaengagement` activity.

### Card UI and per-activity statistics

The interactive `/promo` card (no-args / partner+admin menu, served by
`handlers/partner_promo_cmd.py`) renders `vk_publication`, `vk_repost`, and `vk_story`
activities with human-readable labels: target community, repost
`source → target`, story target, and the rolling minimum (`минимум N/окно`). The «📊
Статистика» screen is a per-activity breakdown: for each activity it shows the
total action count, a rolling-window counter (`промо-действий за Nч: X / цель`),
and the latest posts/reposts/stories as clickable VK links (the repost/story
line also links its source post). Exposures are attributed by
`promo_exposure.activity_id`;
counts include `VK_SCHEDULED` (promo VK posts sit in the community postponed
queue), and exposures without a current `activity_id` fold into a «Прочее»
section. Note the separate admin text interface `/promo <args>`
(`handlers/promo_cmd.py`) renders its own VK report in `/promo report`.

## Author-in-chat trigger (video announce)

`promo_target.target_type='tg_chat_author'` promotes events by their Telegram
provenance instead of by event/festival id. `query_text` is
`"<chat_username>:<author_username>"` (both lowercased, no `@`). `_events_for_target`
selects active future events that have an `event_source` row with
`source_chat_username == chat` **and** `event.tg_source_author == author`.

- **Author capture**: `Event.tg_source_author` is set at ingest
  (`source_parsing/telegram/handlers.py` → `EventCandidate` →
  `smart_event_update` create path) from the Telegram message `post_author`
  **only for `group`/`supergroup` sources** with a resolved user author. If a
  Telethon-style payload lacks `post_author` but carries a user `sender` object,
  ingest falls back to that sender username (the live `kraftmarket39` supergroup
  exposes `@LANGEANNA` this way). Channels post as the channel itself, so they
  get `None` and never trigger. Future-only — there is no backfill for events
  imported before this shipped. The Kaggle `TelegramMonitor` already includes
  `post_author` in its payload, so no monitor change is needed.
- **Reporting**: `/promo report` counts future active `tg_chat_author` targets
  by the same `event_source.source_chat_username` + `event.tg_source_author`
  pair used by the video promo resolver, so an author campaign no longer looks
  empty just because it is not an event/festival target.
- **Filters still apply**: matched events go through the normal video promo
  pipeline (`resolve_video_promo_candidates` + `video_announce/popular_review.py`),
  which filters every promo candidate by the announce's own content filter
  (КОНБ/eco). An event that fails a filter does not appear in that announce.
- **Surface boundary**: this trigger is a CherryFlash-family promo. It can enter
  public CherryFlash even when the event is outside the organic popularity/date
  window, subject to future-date and renderable-poster checks. CrumpleVideo
  `/v tomorrow` must not expand its period for this seed; it can include the
  event only if the normal CrumpleVideo selection window already contains it.
- **Concrete campaign**: `ensure_kraftmarket_langeanna_campaign` idempotently
  seeds `kraftmarket39 · @LANGEANNA → видеоанонс` — target
  `tg_chat_author`/`kraftmarket39:langeanna`, one `video_general` activity with
  `selection_policy=guaranteed_any_position`, `max_per_publish=1`,
  `profile_key=None` (eligible across general tracks), no daily cap (so the same
  day's several announces can each include it where it passes filters). Seeded on
  every `resolve_video_promo_candidates` run.

## Daily Marker

The "добавили в анонс" daily section can mark promoted events with standard
Unicode `✨`. It intentionally does not print the word `promo`.

The marker is subtle editorial highlighting, not an advertising disclosure. If a
future campaign is paid/sponsored, it must use explicit disclosure text before
activation.

`daily_recommend_today` is a separate daily surface for editorial summary
recommendations in the "НЕ ПРОПУСТИТЕ СЕГОДНЯ" section. It appends a compact
block before the section hashtags:

`ИТОГО РЕКОМЕНДУЕМ ПОСЕТИТЬ СЕГОДНЯ`

with one-line Telegraph links to at most two promoted events that actually occur
on the local date (`Europe/Kaliningrad`). The surface records
`publish_status='DAILY_RECOMMENDED'` after a successful daily send and supports
`config_json.preferred_event_ids_by_date` for date-specific editorial ordering.

## Current Limits

- MVP implements CherryFlash general boost, daily-section highlighting, and
  daily "today" summary recommendations.
- VK publication/repost/story and Telegram publication/repost promo activities
  are implemented through the scheduled `promo_vk` runner and normalized
  `promo_exposure` reporting. The current VK publication formatter uses the
  source-style Smart Update VK message; Telegram promo publication uses a full
  event post in the event-flow channel, while `tg_repost` forwards that source
  post into the daily/digest channel.
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
