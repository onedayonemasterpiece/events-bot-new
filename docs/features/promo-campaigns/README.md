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
- `promo_target`: one real `event.id`, one existing `festival.name`, all active
  future events (`target_type='all'`, only for explicitly broad editorial
  selectors such as weighted popular reposts), or a Telegram source author
  (`target_type='tg_chat_author'`, `query_text` `"<chat>:<author>"`). See
  "Author-in-chat trigger" below.
- `promo_activity`: where the campaign can act. MVP surfaces are
  `video_general`, `daily_highlight`, `telegraph_month`, `telegraph_weekend`,
  `daily_recommend_today`, `vk_publication`, `vk_festival_carousel`,
  `tg_event_publish`, `tg_repost`, `tg_button_highlight`,
  `vk_channel_publish`, `vk_repost`, `vk_story`, and `afishaengagement`.
  Social activity parameters live in
  `promo_activity.config_json` (`target_group`, `source_group`, `target_chat`,
  `source_chat`, `window_hours`, `active_start_hour`, `active_end_hour`, dedup
  policy). `vk_publication`, `tg_event_publish`, and
  `daily_recommend_today` can also use
  `preferred_event_ids_by_date={"YYYY-MM-DD": [event_id, ...]}` to keep a
  multi-day educational/festival campaign aligned with the programme calendar.
  For publication surfaces, a configured date acts as that day's allow-list:
  once those ids are exhausted, the runner does not fill remaining daily slots
  with later programme dates.
- Telegram event publishing has two independent promo effects. Richer `@kldevents`
  intro copy is channel-default behavior: when an event is covered by any active
  `promo_target`, the publisher may use the promo-intro prompt/context without
  requiring a `tg_event_publish` activity. Moving `Подробнее` from the text
  footer into the inline `✨ Подробнее` button is **not** implied by campaign
  membership; it is controlled by the marker activity
  `promo_activity.surface='tg_button_highlight'` /
  `profile_key='kldevents:details-button'`. Default campaign constructors add
  that marker enabled, but operators can disable that activity for broad
  campaigns where the extra button is too noisy. The `tg_event_publish` promo
  activity is only for an explicit extra campaign slot in an event-flow channel;
  its output can then be used as a source by `tg_repost`.
- `promo_exposure`: normalized exposure audit rows. MVP writes video exposure
  rows when a promoted video item reaches a viewer-facing publication target:
  `PUBLISHED_MAIN`, or the scheduled CherryFlash target that is still stored by
  the legacy video-announcement status machine as `PUBLISHED_TEST`.
- `videoannounce_item` stores promo provenance:
  `promo_campaign_id`, `promo_activity_id`, `promo_placement_kind`.

Targets are never abstract: a festival campaign is refused until the name is
grounded by an existing `festival` row or by future active event rows whose
`event.festival` already equals that name. Event/festival rotation uses only
events whose start date is today or later. For timed one-day events, VK promo
surfaces also require that the local start time has not passed yet; a 15:00
event must not be selected for a 16:00+ VK publication just because its date is
still today. Date-only same-day events remain eligible because there is no
reliable start time to compare against.

Live festival/program campaigns must not be modelled as `event.id`-only
campaigns while the programme is still being imported or corrected. A fixed
event-id target set is valid only for a closed, already-audited set of concrete
events. For an open educational/festival programme, the campaign needs a
dynamic anchor (`festival`, `festival_series`, source/author trigger, or another
explicit semantic programme target) so newly imported programme events become
eligible without an operator remembering to edit the campaign. Per-surface
event-id lists such as `preferred_event_ids_by_date`, `carousel_event_ids`, or
`celebrity_event_ids` may curate a particular publication, but they must not be
the only eligibility mechanism for the whole live campaign.

For `Кантата`, public/source communication uses the festival marker
`event.festival="Кантата"`. The educational programme is a segment inside that
festival, not a replacement festival name. Promo eligibility for that programme
therefore needs two stages: first anchor on `Кантата`, then apply an education
programme filter/classifier that separates lectures/talks/education events from
concerts. Until a structured programme field exists, this filter must still be
dynamic and auditable; it must not be approximated by a frozen list of the event
ids known at campaign creation time.

## Operator Commands

- `/promo` or `/promo list` lists current/future non-archived campaigns
  (`ends_at` is empty or not in the past) and seeds the `80 историй о главном`
  campaign if the festival is eligible. Ended active campaigns are hidden from
  the default management list so stale rows do not look runnable.
- `/promo seed80` explicitly creates/returns the initial campaign for
  `80 историй о главном` through 2026-07-18.
- `/promo report` lists all campaigns, including archived and ended active
  campaigns, with current future event count, video publication count,
  promo-show count, and per-session CherryFlash publication details: date/time,
  profile, session id, stored status, target count, positions, and event
  ids/titles. VK promo activities are shown as concrete exposure rows with
  event id/date, status, source URL for reposts, and target VK post URL.
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
  first lets public `afishaengagement` preflight choose a CTA variant for that
  exact publication pass. If the CTA preflight succeeds, that CTA wall post is
  the `vk_publication` result and the plain `post_to_vk` call is skipped. If it
  misses or fails, the runner publishes one missing plain post as a postponed
  community wall post through the standard VK wall contract (`post_to_vk`,
  community author, postponed queue), using the same source-style event message
  format as Smart Update;
- after a plain `vk_publication` fallback, only explicit debug/shadow
  `afishaengagement` activities may run (`shadow_only=True`). Public CTA
  activities must not create a second wall post after the plain post already
  exists;
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

`vk_channel_publish` is reserved for compact event promos in the VK community
Channel of the `klgdevents` community ("Полюбить Калининград Афиша"). The
intended copy is Telegram-channel-like but VK-safe: title,
date/time/location infoblock, short plain-text description, and exactly one
event CTA link; no footer link block and no hashtags.

Current production contract: **operator-assisted manual draft**. VK community Channels are the
surface shown in Messenger → "Каналы" and are created from the community UI via
"Создать пост" → "Пост в канал". Public VK Open API docs for `messages.send`
describe Messenger recipients, and `wall.post` describes community wall posts;
neither documents a parameter or method that publishes into the community
Channel. Therefore `messages.send` is allowed only as a **non-public draft
delivery to the operator's VK Messenger/Favorites** (`delivery_mode=
vk_messages_manual_copy_draft`), so the operator can copy the prepared post and
publish it manually through the VK UI. When the selected event has
`event.photo_urls`, the draft attaches the first successfully uploaded event
poster/афишу as a VK message photo; if poster upload fails for all candidate
URLs, the draft fails instead of silently sending a text-only copy. Draft
delivery records
`publish_status='VK_CHANNEL_DRAFT_SENT'`, keeps `public_target_count=0`, and
creates no public target rows. If the activity is not explicitly in manual-draft
mode, it still fails closed with
`reason='vk_community_channel_post_api_unsupported'`. The one CTA link in the
copy prefers registration/ticket links over Telegraph details pages. For
registration-required events, including the `80 историй о главном` campaign,
Telegraph is not an acceptable CTA fallback: if no direct registration/ticket
URL is present in `event.ticket_link`/registration fields or source text, the
manual draft is skipped/failed instead of sending a misleading details link.
Candidate selection rotates by exposure count but, among equally suitable
events, prefers the nearest date/time before applying stable daily shuffling.

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

`vk_festival_carousel` publishes a VK carousel at festival/program level. It is
intended for non-aggressive program promotion: the first card is a hook/question,
the middle cards are selected event posters plus optional curated person cards,
and, when there is room, the last card can be a CTA card pointing readers to
the post text. Non-celebrity carousels can use up to ten VK attachments;
`hook_variant=celebrity` is capped at nine cards total so the first card,
poster cards, person cards, and final CTA stay reviewable. The activity config
supports:

- `target_group`: destination VK community;
- `carousel_event_ids` / `event_ids`: explicit event order for poster cards;
- `hook_variant`: `visited`, `registration`, `celebrity`, or `all_posters`;
- `program_phrase`, `program_name`, `festival_name`: reusable wording inputs;
- `hook_text` / `hook_texts`: exact operator-approved copy overrides;
- `llm_hook_enabled`: optional LLM hook generation when no override is present;
- `program_url`, `program_vk_url`, `cta_urls_by_event_id`: CTA sources;
- `celebrity_event_ids`: explicit event ids for `hook_variant=celebrity`; when
  absent, the activity keeps only events with a clear person/role signal;
- `celebrity_poster_urls_by_event_id` /
  `celebrity_photo_urls_by_event_id`: explicit image-evidence URLs for
  celebrity carousels; by default celebrity hooks fail closed when an event has
  no curated poster where the person/role is visible on the image;
- `celebrity_requires_image_evidence`: keeps the celebrity image gate enabled
  by default;
- `celebrity_person_cards_llm_enabled`: enables LLM extraction of missing
  people for celebrity carousels when no explicit `celebrity_person_cards`
  override is configured. The LLM receives the remaining card budget, but the
  code still hard-caps the selected cards before rendering. By default the
  extraction runs per event: events without poster cards are inspected first,
  then events already represented by poster cards;
- `celebrity_person_cards_llm_per_event`: keeps the per-event extraction mode
  enabled by default so one broad LLM response cannot skip later source events;
- `celebrity_person_source_event_ids` / `person_source_event_ids`: optional
  event ids used as the LLM extraction scope for FIO/role cards when poster
  cards are intentionally limited to a smaller evidence-backed set;
- `celebrity_person_source_from_campaign_targets`: lets the LLM extraction
  scope use all event targets from the campaign when source ids are not listed
  explicitly;
- `celebrity_person_cards`: operator-approved override cards for missing
  people. Each item must include `name` and `role` (optionally `event_id`), or
  the value can be a mapping from event id to such items. Use this only when the
  LLM output needs a curated correction;
- `covered_celebrity_names_by_event_id` /
  `celebrity_names_on_posters_by_event_id`: names already visible on selected
  poster cards. These are skipped when filling `celebrity_person_cards`, so the
  carousel can add missing figures without duplicating people already present
  on the афиши;
- `palette_id`, `palette_ids`, `palette_id_by_hook_variant`: stable palette
  selection controls using `afishaengagement` editorial palettes;
- `poster_swipe_badge`, `swipe_label`: poster-card carousel cue controls,
  enabled by default with `листай` + right arrow;
- `include_cta_card`, `cta_card_title`, `cta_card_subtitle`: final-card
  controls;
- `scheduled_at` / `publish_at`: optional ISO datetime or Unix timestamp for a
  normal production carousel. When set in the future, it is passed to VK as the
  postponed `publish_date`; otherwise the shared VK postponed-slot reserver
  chooses the next available slot;
- `debug_shadow`, `debug_marker`, `debug_publish_delay_days`,
  `debug_slot_spacing_minutes`: shadow review scheduling.

VK carousel links use VK short links where possible. Event links prefer
`event.vk_ticket_short_url`, otherwise the activity shortens `event.ticket_link`
through the existing VK shortener helper; configured per-event URLs are also
shortened before rendering into VK text. Telegram surfaces must continue using
the original expanded `event.ticket_link` and must not reuse VK-only shortlinks.
If a carousel belongs to a live programme campaign, configured event-id lists are
publication-level curation only. The parent campaign still needs a dynamic
programme target so new imports are visible to other promo surfaces and future
carousel revisions.

When `debug_shadow=true`, `vk_festival_carousel` schedules a marked postponed
copy several days ahead using the same shadow-slot pattern as
`afishaengagement`, records `promo_exposure.surface='vk_festival_carousel'` and
`publish_status='VK_SCHEDULED_DEBUG'`, and keeps it out of public exposure
counts. This is the required review mode before switching the activity to normal
`VK_SCHEDULED` publication.

The carousel card visuals intentionally reuse the `afishaengagement` visual
system: Cygre fonts, editorial CTA palettes, text fitting, grain, and edge
treatment. Hook cards use the compact `листай` right-arrow cue from the
`hook_swipe_cta` pattern; poster cards keep the source poster first and do not
add a full-width bottom rail by default. CTA cards use a large central
down-arrow cue when links live in the post text; any CTA footer/rule line must
leave a central gap around that arrow so the arrow never visually crosses the
line. Palette selection must be stable but varied across activities so parallel
carousel hooks do not all render in the same color scheme. The activity does
**not** call `afishaengagement` publishing and must not layer an engagement CTA
over these carousel cards; `afishaengagement` remains a separate surface for
like/comment/repost motivators on event posts.

`tg_event_publish` keeps a rolling minimum of event-flow channel mentions,
normally in `@kldevents`, using the same lightweight promo runner as VK
activities and the same local active window / due-slot rules. It first counts
recent organic Smart Update Telegram event publications in the target channel
using completed `JobOutbox.tg_event_publish` rows for events that already have a
matching `event.tg_event_post_url`, plus today's public promo exposures for that
activity. If those posts already satisfy the due slots, it does nothing. If a
slot is still missing and the selected event already has an older source-channel
post, the activity forwards that original message back into the same target
channel instead of rendering duplicate text; this preserves cross-message
Telegram view accounting on the first event post. The self-forward is recorded
as `promo_exposure.surface='tg_event_publish'`,
`publish_status='TG_FORWARDED'`, `placement_kind='rolling_window_self_forward'`
with `details_json.source_url` and `details_json.target_url`, and does not
replace `event.tg_event_post_url`. Only when no forwardable source-channel post
exists, or the self-forward fails, the activity falls back to the full promo
event publication path, records `publish_status='TG_PUBLISHED'`, and stores the
new source post URL back on the event for downstream reposts. Full-body text is
sanitized for Telegram HTML: Markdown section headings become bold headings,
bullets are normalized, and raw service markers such as `###`, `**`, or list
`*` must not leak to the public post. If promoted event media exists, the media
is mandatory for the public surface: overlong full text is reduced to a concise
media caption with the `Подробнее` button instead of falling back to a text-only
post or exposing a long Smart Update bullet dump as the primary public copy.

`tg_repost` forwards an existing source-channel post, normally
`@kldevents -> @kenigevents`, instead of rendering a new text post in the
daily/digest channel. It does not by itself enable the source post's
`✨ Подробнее` button; broad popular-forward campaigns should keep their separate
`tg_button_highlight` marker disabled when every source post would otherwise get
a large extra button. It looks at the promoted event's stored
`event.tg_event_post_url` and recent `tg_event_publish` exposures, applies
`dedup_hours` to source URLs, forwards with Telegram Bot API, and records
`promo_exposure.surface='tg_repost'` / `publish_status='TG_FORWARDED'` with
`details_json.source_url` and `details_json.target_url`. It must only forward
future events: when an event has a concrete start time, the repost is eligible
only before the `min_lead_hours` cutoff (`4` hours by default), so daily/digest
channels do not amplify events that have already started or are about to start.
For a stored bot event post, eligibility is also checked against its immutable
`event_source` snapshot. The generated post's explicit Russian date must match
the current canonical `event.date`; if it does not, the candidate fails closed
and logs `promo.tg repost skip stale source snapshot`. This prevents a later
bad lifecycle mutation from making an expired post look future again. The
exposure's `source_published_at` uses the earliest matching source observation
timestamp instead of the current selector time; it is observation evidence,
not a claim that Telegram's exact publication timestamp is stored in SQLite.
To keep broad amplification diverse, `tg_repost` also applies a same-title
repeat cooldown. `dedup_hours` still protects the exact source URL, while
`repeat_cooldown_days` / `repeat_cooldown_hours` (default: `7` days) suppresses
another event with the same normalized title if a non-repeat forwardable
candidate exists. A repeat is allowed only as a fail-open fallback when the
activity has no other forwardable candidate for the due slot.
For broad editorial amplification, a `tg_repost` activity may use
`selection_policy='weighted_popularity'` with an `all` target. In that mode the
candidate is the event, not the source post: the selector sums
`/popular_posts`-style normalized popularity from the original TG/VK source
posts and adds owned-audience VK activity for the same event with a higher
weight. Production uses `owned_vk_group_ids=[231920894, 231828790]`
(`klgdevents`, `kenigeventsofficial`) and
`owned_vk_popularity_weight=4`, because reactions in the bot's own VK audiences
are a stronger signal than reactions in arbitrary internet sources while both
signals still count. The forwarded post itself is still taken from the event's
`@kldevents` publication (`event.tg_event_post_url` or recent
`tg_event_publish` exposure). If a highly ranked event has no forwardable
`@kldevents`/`t.me/c/...` post yet, the activity skips it and tries the next
ranked event; it does not create a new source post just to satisfy the repost
slot. Diversity cooldown is evaluated after ranking: a lower-scored different
title beats a repeated title within the cooldown window, but if every
forwardable candidate is a repeat the highest-ranked repeat may be used and is
recorded with `details_json.repeat_cooldown_bypassed=true`.

The initial `80 историй о главном` campaign now includes:

- `vk_publication` to `https://vk.com/klgdevents`, `max_per_publish=2`,
  `daily_cap=2`, 24-hour window, active window 09:00-21:00;
- `vk_channel_publish` to the VK Channel of `https://vk.com/klgdevents`
  ("Полюбить Калининград Афиша"), `max_per_publish=1`, `daily_cap=1`,
  24-hour window, active window 09:00-21:00. Until VK community Channel posting
  has a verified non-Messenger API path, it sends a manual-copy draft to the
  operator via `VK_AFISHA_CHANNEL_DRAFT_PEER_ID` and never counts that Messenger
  delivery as public Channel publication; the manual draft includes the event
  poster attachment when available and must use the event registration page as
  the CTA rather than a Telegraph details page;
- `vk_repost` from `https://vk.com/klgdevents` to
  `https://vk.com/kenigeventsofficial`, `max_per_publish=1`, `daily_cap=1`,
  24-hour window, active window 09:00-21:00, and 72-hour source-post dedup.
- `vk_story` from `https://vk.com/klgdevents` to
  `https://vk.com/klgdevents`, `max_per_publish=2`, `daily_cap=2`, 24-hour
  window, active window 09:00-21:00, and 72-hour source-post dedup;
- `vk_story` from `https://vk.com/klgdevents` to
  `https://vk.com/kenigeventsofficial`, `max_per_publish=2`, `daily_cap=2`,
  24-hour window, active window 09:00-21:00, and 72-hour source-post dedup.
- `tg_event_publish` to `https://t.me/kldevents`, `max_per_publish=2`,
  `daily_cap=2`, 24-hour window, active window 09:00-21:00. For each selected
  event it first reuses a forwardable existing `@kldevents` event post by
  self-forwarding it into the same channel; if no source post exists, it
  publishes a new full Telegram event post.
- `tg_repost` from `https://t.me/kldevents` to `https://t.me/kenigevents`,
  `max_per_publish=1`, `daily_cap=1`, 72-hour source/dedup window, active
  window 09:00-21:00.
- `afishaengagement` for `https://vk.com/klgdevents`, public canary enabled
  through a higher-priority public activity and a lower-priority shadow
  fallback. The initial public rollout rate is `0.50`, with shadow fallback
  `1.0`, likes-only registration CTA
  `Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}.`, and all
  visual formats enabled for VK visual-debug monitoring. Existing legacy
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
