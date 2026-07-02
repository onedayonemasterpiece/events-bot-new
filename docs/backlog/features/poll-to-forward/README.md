# Poll to Repost

> Status: production-only rollout; public debug mode disabled on Fly
> Source requirements: `docs/backlog/features/poll-to-forward/requirements.md`

## Product Goal

Poll to Repost gives the `@kenigevents` audience a daily lightweight choice:
which kind of event should be recommended for tomorrow. The feature should feel
like real participation, not a decorative poll. If tomorrow has too few eligible
events to offer a meaningful choice, the poll is skipped.

The feature operates on existing `Event` rows in the database. Upstream source
facts are out of scope for this feature. The only source constraint inside Poll
to Repost is publication availability: the chosen event must have a repostable
Telegram message in `@kldevents`.

## Runtime Contract

Production:

- publish one poll per day in `@kenigevents`;
- default production poll time is `16:00 Europe/Kaliningrad`
  (`POLL_TO_FORWARD_PROD_POLL_TIME_LOCAL`);
- default production result time is `19:55 Europe/Kaliningrad`
  (`POLL_TO_FORWARD_PROD_RESULT_TIME_LOCAL`);
- require a production-only minimum answer threshold before publishing a
  recommendation: `10` at rollout start, then `+1` every full week from
  `2026-06-12`;
- if the threshold is not met, reply to the poll that too few votes were
  collected for today's threshold and no repost is made;
- if there is a winner, reply to the poll with a short line naming the chosen
  topic, then forward the selected `@kldevents` post into `@kenigevents`.

Debug:

- disabled in production Fly configuration (`ENABLE_POLL_TO_FORWARD_DEBUG=0`);
- no public hourly polls are published to `@keniggpt` during the stable
  production rollout;
- if debug is temporarily needed again, use an isolated operator sandbox or an
  explicitly planned one-cycle smoke, not a long-running public hourly loop.

The public repost must use Telegram `forward_message` so the repost carries the
source header. `copy_message` is not a normal success path for this product.

Implemented entrypoints:

- `poll_to_forward.py`;
- `db.py` table bootstrap for `poll_repost_run`;
- `scheduling.py` job `poll_to_forward_debug`, enabled only when
  `ENABLE_POLL_TO_FORWARD_DEBUG=1`, running at minutes `0,30`;
- `scheduling.py` jobs `poll_to_forward_prod_create` and
  `poll_to_forward_prod_resolve`, enabled by `ENABLE_POLL_TO_FORWARD_PROD=1`;
- production Fly target: production surface `@kenigevents`, source forward chat
  `@kldevents`; public debug surface `@keniggpt` is off by default.

Run identity is profile-scoped: debug uses `debug:YYYY-MM-DDTHH`, production
uses `prod:YYYY-MM-DD` for the target event date. The resolver reads only runs
from its own profile, so debug and production cannot consume each other's polls.

## Eligibility

An event can participate only when all conditions hold:

- its repostable `@kldevents` post is for the target recommendation date;
- it is not cancelled/archived/past and, when a concrete start time is known,
  it is still at least `POLL_TO_FORWARD_MIN_LEAD_HOURS` hours away (`4` by
  default), so the evening repost does not recommend an event that has already
  started or is about to start;
- it has a known Telegram post in `@kldevents` that can be forwarded;
- it was not recently reposted by this same feature;
- it is suitable for a public recommendation after basic quality checks.

The candidate pool is therefore "tomorrow's DB events whose `@kldevents` source
message also reads as tomorrow", not every event in the database. A multi-day
event can be generally active tomorrow, but Poll to Repost must not forward a
start-date post if that visible post looks like yesterday/today and does not
show the active range. In that case the event is eligible only after it has a
target-date `@kldevents` post, or after the source post itself is repaired to
show the date range clearly.

If the eligible pool is too small, the poll is skipped:

- production default: at least `5` eligible events;
- debug default: at least `3` eligible events;
- after LLM topic generation there must still be enough distinct non-empty
  options, production default `4`, debug default `3`.
- when a free-events option is possible (at least two `is_free=true` candidates)
  and the eligible pool has more than six events, the effective minimum becomes
  `6` options. The free option is an additional axis of choice, not a
  replacement for one of the ordinary themes.

This avoids asking the audience to choose when the system cannot honestly offer
varied outcomes.

## Topic Generation

Topic generation is LLM-only. The LLM receives the eligible tomorrow events and
returns a compact poll plan:

- `options`, each with public text and hidden candidate event ids;
- short rationale for why the option is relevant today;
- warnings when the topic set is weak or underfilled.

The poll question itself is product copy, not a semantic topic decision. Its
primary promise is: today the audience chooses the type/theme/category of
events where they can go tomorrow, and in the evening the channel author
chooses one concrete announcement inside that theme. The copy should feel
friendly and a little blogger-like, not like an algorithm or a marketing banner.

When `POLL_TO_FORWARD_QUESTION_LLM_REVIEW_ENABLED=1`, question copy goes through
a writer/reviewer LLM loop before publication. The reviewer must accept only
wording where it is clear that the choice happens today, the choice is a
category/theme of events, the event is for tomorrow, and the evening result is
one concrete announcement from the winning category. Rejected variants are fed
back into the next writer attempt; after the configured attempts the code falls
back to an explicit safe question. Deterministic guardrails still reject known
ambiguous phrases such as "что завтра подсветить", "рекомендация на завтра",
"завтрашней рекомендации" and "разберусь с выбором".

Production rotates several participation frames by slot, for
example:

- `Сегодня выбираем категорию событий, куда можно сходить завтра. Вечером возьму один анонс из варианта, за который будет больше голосов.`
- `Давайте выберем тему событий, куда завтра можно пойти. Вечером покажу один конкретный анонс из варианта с большинством голосов.`
- `Помогите выбрать категорию событий для завтрашнего выхода: музыка, выставки, прогулки или что-то ещё. Вечером возьму один анонс из победившей темы.`
- `Голосуем за тип событий, на которые можно сходить завтра. Вечером выберу один анонс внутри темы, где будет больше голосов.`
- `Куда завтра хочется выбраться? Голосуйте за категорию событий, а вечером я покажу один анонс из варианта большинства.`
- `Голосуем за категорию событий на завтра. Вечером будет один конкретный анонс из темы, за которую проголосует большинство.`

The rotation can be replaced with `POLL_TO_FORWARD_QUESTION_VARIANTS` (`||`
separator) or pinned with `POLL_TO_FORWARD_QUESTION_TEXT`. The default rotation
also avoids repeating the same question text in adjacent debug hourly slots if
debug is explicitly re-enabled. The LLM still owns the meaningful option set.

Avoid false mechanics such as "найду/поищу" when the feature operates on
events already stored in the database. Avoid pseudo-personal mood wording such
as "по вашему настроению", neural clichés such as "завтрашний план звучит",
and generic marketing phrases. Do not attach `завтра` to the recommendation
action itself (`что завтра порекомендовать`, `завтра сделать рекомендацию`):
the recommendation is published today evening and points to tomorrow. Also
avoid "what to show/highlight tomorrow" frames: the audience chooses the theme
now, and the channel publishes the resulting recommendation this evening. The
public question should not make subscribers choose a "category of tomorrow's
recommendation"; it should explicitly say that the category is for tomorrow's
event itself.

Good options are audience jobs-to-be-done, not raw database categories. Examples:

- "вечер с музыкой";
- "с детьми";
- "бесплатно или почти бесплатно";
- a playful free-events option when several candidates have `is_free=true`,
  e.g. "куда угодно, только бесплатно";
- "у побережья";
- "фестиваль";
- "что-то необычное";
- "в помещении";
- "загород / восток области".

Avoid advertising-style option text and superlatives. The poll should feel like
the channel asks subscribers what they want the next recommendation to cover,
not like a generic promo banner.

When the LLM adds a free-events option, it should not compress the whole poll to
five choices. With enough eligible events (more than six after popularity filtering), the expected shape is 6-8 options:
the usual thematic directions plus the extra free axis. Code rejects a
free-axis plan that comes back under six options, and free-labelled options are
filtered to `is_free=true` candidate ids only.

Deterministic code may normalize dates, remove empty options, and enforce
minimum candidate counts. It must not generate semantic poll topics. If the LLM
is unavailable or returns an invalid/underfilled plan, the slot is skipped and
no public poll is published.

## Winner Resolution

The resolver stops the poll and stores a final result snapshot.

Rules:

- production requires at least `production_min_vote_threshold(target_date)`
  total answers before reposting. The default formula is:
  `10 + floor((target_date - 2026-06-12).days / 7)`, with negative values
  clamped to `10`; this grows the minimum by `1` every full week. Operators can
  override the rollout anchor and base with
  `POLL_TO_FORWARD_PROD_MIN_VOTES_START_DATE` and
  `POLL_TO_FORWARD_PROD_MIN_VOTES_BASE`;
- debug does not require a minimum vote threshold;
- the highest vote count wins;
- if there is a tie, LLM compares tied topics and their candidate events, then
  chooses the topic that gives the stronger public recommendation;
- even without a tie, LLM chooses the final event inside the winning option;
- if LLM winner/event selection fails, or if the winning topic has no
  still-eligible candidate at resolve time, skip the repost and record the
  reason; do not use deterministic fallback.

Before the repost, the bot sends a short reply to the original poll message.
The public reply is LLM-first: after the LLM chooses the final event, a separate
LLM composer writes the whole comment with a strict `{{EVENT_LINK}}`
placeholder. Code only validates the contract, escapes text, inserts the HTML
event link, disables web preview, and falls back to a neutral deterministic text
if the composer fails. The visible text of that link is also LLM-authored through
`event_link_text`: it must be a natural mention of the selected event, not a raw
deterministic DB title. This lets the composer normalize all-caps names (for
example `ОТКРЫТЫЙ МИКРОФОН` -> `«Открытый микрофон»`) and fit the linked words
into the sentence organically. Code only validates and wraps that span in safe
HTML.

Required meaning:

- thank people for voting;
- say which theme won, or that votes were tied;
- say that one concrete announcement was chosen inside that theme/tied themes;
- briefly explain why this event fits;
- ask for 👍/👎 feedback;
- end with `Сейчас перешлю анонс 👇`.

Example LLM composer output before link rendering:
`Спасибо за голоса — берём тему «музыка».

Для этой темы сегодня беру концерт {{EVENT_LINK}}. В субботу фестиваль продолжается, можно спокойно зайти на классику в атмосферном месте.

Если рекомендация зашла — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.

Сейчас перешлю анонс 👇`

If the poll result is tied, the reply must say so directly instead of naming a
single false winner, for example: `Голоса разделились поровну между
«выставки» и «экскурсии». Беру один конкретный анонс из этих тем.` The LLM
composer should then explain why the selected event still makes sense as the
final recommendation. It may mention
popularity or public interest only when such signals are grounded in passed
metrics or event text. If an exhibition starts on the target date, natural
wording such as "как раз открывается выставка" is preferred over generic
"сходить на выставку".

The `{{EVENT_LINK}}` placeholder must appear exactly once and only in
natural phrases with a generic event word/type before the linked title, for
example:

- `поэтому сегодня выбрал турнир {{EVENT_LINK}}`;
- `для этой темы подходит лекция {{EVENT_LINK}}`;
- `можно присмотреться к игре {{EVENT_LINK}}`;
- `беру в рекомендацию анонс {{EVENT_LINK}}`.

Avoid constructions that require inflecting the event title around the
placeholder, such as `я бы предложил {{EVENT_LINK}}`, `сходить на
{{EVENT_LINK}}`, or `расскажу про {{EVENT_LINK}}`. Also avoid dumping the event
title after punctuation, such as `сегодня рекомендация такая: {{EVENT_LINK}}`,
`выбрал вот что: {{EVENT_LINK}}`, `остановился на этом: {{EVENT_LINK}}`, or any
other `: {{EVENT_LINK}}` pattern: it reads like a template leak, not a
blogger-style recommendation. Avoid marketing reason leads such as "отличный
вариант" or "интересный вариант"; prefer concrete human phrasing like "для тех,
кто голосовал за гастро-отдых, на ферме как раз праздник".

The composer may only use facts present in the event context or LLM selection
reason. It must not infer an open-air/street/park format from the word
"festival"; mixed-format festivals are common, and unsupported claims such as
"под открытым небом" are rejected by code and replaced with safe fallback copy.
It also must preserve poll causality: when there is one winning option, the
reply should treat it as one chosen direction. Do not split a mixed option into
several supposed audience requests or write that the author "combined these
requests"; that language is allowed only for a real tie between separate
options.

The intro copy should make voters feel that the recommendation is a consequence
of their choice. Avoid generic promo phrasing and avoid exposing the technical
`forward_message`/repost mechanic to the audience. Telegraph links are attached
to the LLM-authored event mention in HTML and sent with web preview disabled.
Separate the thanks, recommendation, feedback prompt, and forwarding line with
blank lines so the Telegram message does not render as one dense block.

Do not debug this feature by publishing several polls in a burst to the public
debug channel. Live verification must use one visible cycle at a time unless the
target chat is an isolated private operator sandbox.

## Event Selection

The final event choice is LLM-only over the winning option's candidate events.
The prompt may use these signals:

- popularity from the existing post metrics framework;
- recency and anti-repeat;
- promo/festival boost only when the event genuinely fits the winning topic;
- repost availability from `@kldevents` as a hard requirement.

The selected event must be potentially interesting on its own. Popularity should
help rank strong candidates, not rescue a weak or off-topic event.

For Poll to Repost, repost availability is stricter than general event
activity. Long-running events with `date < target_date <= end_date` remain valid
for ordinary event listings, but are not valid repost candidates unless the
forwarded `@kldevents` message date matches the target date. This prevents a
"recommendation for tomorrow" from forwarding a post whose visible infoblock
shows only the start date.

## Feedback Signals

The reply before the forwarded announcement asks readers for a lightweight
reaction: 👍 if the recommendation landed, 👎 if it did not. These reactions can
be monitored later with the same Telegram metrics foundation that already stores
`telegram_post_metric.reactions_json`; the bounded long-term shape is to snapshot
reactions for the Poll to Repost reply and forwarded announcement, aggregate them
by `poll_repost_run`, and delete old raw snapshots under the existing post
metrics retention policy.

## Popularity Signals

Poll to Repost should reuse the existing post metrics foundation:

- `source_parsing/post_metrics.py`;
- `telegram_post_metric`;
- `vk_post_metric`;
- `/popular_posts` ranking principles.

For Poll to Repost, `source_vk_post_url` is treated as a legacy pointer and is
not authoritative for engagement. The primary VK stats path is a live published
wall scan of `kldevents` (`VK_EVENTS_GROUP_ID=231920894`, default scan limit
`POLL_TO_FORWARD_KLDEVENTS_WALL_SCAN_LIMIT=1000`) before poll creation. Stored
URLs are used only as a fast direct lookup and diagnostic signal.

Live wall matching requires strict anchors:

- event title and target date must be present;
- time or venue must also match when the event has that anchor;
- title-only matches are rejected;
- duplicate DB events mapped to one live wall post share one popularity group,
  so they do not inflate poll inventory.

Resolved repeat-publication mappings are stored separately from factual event
sources in `event_publication`:

- `event_id`;
- `platform='vk'`;
- `target='klgdevents'`;
- `stored_url` / `live_url`;
- `stored_post_id` / `live_post_id`;
- `match_method`;
- `match_confidence`;
- `status`;
- `resolved_at`.

The `vk_post_metric` snapshots store `views`, `likes`, `comments`, and
`reposts`. A live `kldevents` post above its recent median contributes with
weight `4x`; existing `/popular_posts` source-wall signals remain as a `1x`
fallback. An event qualifies for Poll to Repost if either signal is above its
own median. The kldevents median should come from recent resolved event posts,
but only after a dedicated sample threshold
(`POLL_TO_FORWARD_KLDEVENTS_BASELINE_MIN_SAMPLE`, default `30`) is reached. If
the stored resolved-post metric sample is smaller, the current published wall
scan becomes the low-confidence bootstrap baseline instead of letting a tiny DB
sample such as 4 posts make weak engagement look strong. Diagnostics record
`kldevents_baseline_source`, `kldevents_baseline_sample`,
`kldevents_baseline_min_sample`, and `kldevents_baseline_confidence`.

Poll options are built from popularity-qualified events when a popularity
surface is available. Each option must contain at least two distinct candidate
groups (`POLL_TO_FORWARD_MIN_POPULAR_CANDIDATES_PER_OPTION`, default `2`).
Single-candidate themes are omitted. A free-events option is allowed only when
at least two free candidates exist in the active planning inventory. On richer
popular inventories, the required number of content options grows with distinct
popularity groups: roughly 4 options for 8+ groups, 5 for 10+ groups and 12+
events, and 6 for 12+ groups and 18+ events, capped by the "at least two groups
per option" constraint. This prevents a busy date from being collapsed into a
tiny poll.

Popularity is a ranking and quality signal, not a single point of failure for
poll visibility or resolution. If popularity-qualified events underfill but the
raw eligible repost inventory still meets the profile minimum, creation relaxes
the strict popularity filter (`POLL_TO_FORWARD_RELAX_POPULARITY_ON_UNDERFILL=1`
by default) and records `popularity_filter_relaxed=true` in diagnostics. Popular
events keep their scores for final ranking; non-popular eligible events can
participate in topic options so a day with partial metric coverage does not
silently lose the whole poll. The resolver applies the same principle after
votes are known: if sparse popularity coverage removed the winning option's
valid candidates, it reopens the raw eligible pool for the LLM winner-selection
step instead of dropping the voted topic. This preserves LLM-first final choice
without turning popularity into an availability gate.

The LLM topic planner remains primary. If it is unavailable, the poll is skipped
instead of publishing a fully deterministic topic set. If the first LLM planner
is available but returns an underfilled/over-fragmented plan, creation makes one
LLM repair pass with stricter grounding instructions: reuse only the supplied
event IDs, satisfy the candidate-count constraints, and rebuild a full topic set
before any deterministic fallback is considered. Only if that LLM repair also
underfills may code apply a bounded fallback topic builder for conservative,
coherent multi-candidate themes (for example concerts/music, exhibitions/art,
lectures/meetings, excursions/walks, master classes, games/quizzes, family/kids,
or free). The fallback must not invent mood/time buckets like "вечером" and
must not over-merge unrelated formats merely to reach the minimum option count.
If the inventory supports only one or two coherent choices, the run should
underfill and alert rather than publish a weak poll. This fallback exists only to
keep a truly usable inventory visible; it must not select the final event and
must keep the final recommendation inside the winning option.
The final recommendation stays inside the winning poll option and uses a
weighted pick from that option's deduplicated TOP-3 candidates. The pick seed
includes the concrete run/poll, so repeated debug cycles with the same category
and candidate pool can rotate within the TOP-3 instead of always selecting the
same event.

For audits, the full selection trace is stored in `poll_repost_run.result_json`.
Successful debug resolves also write a compact `poll_to_forward.selection_trace`
runtime log line with the chosen popularity source, score, metrics, medians,
match method, and reason.

The poll always adds a lightweight feedback option
`Другое — в этот раз темы не попали`
(`POLL_TO_FORWARD_FEEDBACK_OPTION_ENABLED=1` by default). This option has no
event candidates and exists to measure disagreement with the prepared category
set. The poll question explicitly mentions that subscribers can pick
`Другое` if the themes are wrong. If this option wins outright, the run is
closed as `skipped_feedback_other`: the bot replies under the poll that the
topics did not land and that the next poll should be assembled differently, but
it does not forward an unrelated event. This terminal state does not block the
next poll slot. The next poll on the same surface reads the previous option
texts from `options_json`, tells subscribers that the previous themes did not
land, and passes those themes to the LLM topic planner as a set to rethink
rather than repeat. Non-winning but meaningful `Другое` feedback is also
retained as a softer signal: by default, at least two `Другое` votes and at
least 15% of votes (`POLL_TO_FORWARD_FEEDBACK_SIGNAL_MIN_VOTES`,
`POLL_TO_FORWARD_FEEDBACK_SIGNAL_MIN_SHARE`) are enough to ask the next LLM
planner to rethink the topic cut without cancelling the current recommendation.
This applies to production as well as debug. If `Другое` ties for first place
with real content options, it is recorded as feedback but removed from the LLM
winner-candidate set, so the result can still stay inside a real voted category.

## Data Model

Use one lightweight persisted run row per poll slot. Do not store per-user votes.
Telegram already owns voter-level state; this feature needs only the public
poll result snapshot.

Proposed table: `poll_repost_run`

- `id`;
- `profile_key` (`prod`, `debug`);
- `run_key` (local date for production, local datetime slot for debug);
- `target_event_date`;
- `status`;
- `poll_chat_id`;
- `poll_message_id`;
- `poll_id`;
- `question_text`;
- `options_json`;
- `result_json`;
- `winner_option_id`;
- `winner_text`;
- `chosen_event_id`;
- `kldevents_chat_id`;
- `kldevents_message_id`;
- `kldevents_post_url`;
- `reply_message_id`;
- `forwarded_message_id`;
- `error_json`;
- `created_at`;
- `updated_at`.

Retention should delete old runs after 60-90 days. The row count is bounded by
schedule frequency, so this feature should not materially increase database
size.

## Scheduling

Implement as an idempotent state machine:

- `create_poll` creates a run only when no active run exists for the same
  profile and slot; Poll to Repost DB writes retry transient SQLite writer locks
  (`POLL_TO_FORWARD_DB_LOCK_RETRY_SEC`, default 180 seconds) because Telegram
  `send_poll` is a public side effect and must not be orphaned by a short
  post-send `database is locked` error;
- debug `create_poll` also refuses to publish a new visible poll when the latest
  visible debug poll is still `open`, `failed`, or otherwise ended without a
  public forwarded result; the next poll should follow the previous public
  result, not a silent internal skip after votes were collected. Operator-
  invalidated rows with `error_json.invalidated_reason` are treated as terminal
  and do not block the next slot;
- `resolve_poll` finds due open runs, stops the Telegram poll, and moves the run
  to `resolved`, `skipped`, or `failed`;
- scheduler restarts must be able to continue from the persisted run.

The resolver tick can be infrequent. Debug does not need tight polling; every
30 minutes is enough. Debug `resolve_after` is rounded to a whole minute so a
tick at `HH:30:00` does not miss a due poll because of scheduler milliseconds.

Current debug defaults:

- `ENABLE_POLL_TO_FORWARD_DEBUG=0` in `fly.toml`; public hourly debug is off;
- create ticks only in local hours `9 <= hour < 24` (`24` is exclusive, so
  the last create slot is `23:00 Europe/Kaliningrad`; the quiet night window is
  `00:00-08:30`);
- scheduler checks at minutes `0,30`;
- debug minimum eligible events: `3`;
- debug minimum LLM options: `3`, raised to `6` when a free-events axis is
  possible and inventory supports it;
- LLM model: `gemini-3.1-flash-lite`;
- anti-repeat window: `7` days.

Historical debug instability on 2026-06-15/18 was caused by two separate
mechanics: many hourly slots legitimately self-skipped on low inventory or LLM
topic underfill, and then one manually invalidated visible debug poll
(`skipped_topic_underfill` with `invalidated_reason`) became the latest visible
poll without a forwarded result, so every later hourly debug tick refused to
publish. Operator-invalidated visible rows are now treated as terminal for slot
blocking, and the long-running public debug loop remains disabled to avoid
noisy public regressions while production is the authoritative smoke surface.

## Observability

Each run should record enough evidence for an operator to answer:

- why was the poll skipped;
- what eligible events were considered;
- which poll option won;
- why the final event was selected;
- which `@kldevents` message was forwarded;
- whether the public target message exists.

Admin reports should be concise and link to the poll, selected event, and
forwarded post when available.

## Open Implementation Questions

- Where exactly is the durable mapping from `Event` to the published
  `@kldevents` Telegram message stored today, and does it cover all ordinary
  Smart Update event posts?
- Should low-inventory production days publish a non-poll editorial fallback, or
  should the product stay silent when the poll is skipped?
- What daytime window should debug hourly polls use?
