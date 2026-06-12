# Poll to Repost

> Status: debug mode implemented, production mode still backlog  
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
- resolve it at the evening slot, for example `19:30 Europe/Kaliningrad`;
- require a production-only minimum answer threshold before publishing a
  recommendation: `10` at rollout start, then `+1` every full week from
  `2026-06-12`;
- if the threshold is not met, reply to the poll that too few votes were
  collected for today's threshold and no repost is made;
- if there is a winner, reply to the poll with a short line naming the chosen
  topic, then forward the selected `@kldevents` post into `@kenigevents`.

Debug:

- publish polls to `@keniggpt` at most hourly during a configured daytime window;
- resolve each debug poll about 30 minutes later, or by a resolver tick every
  30 minutes;
- do not publish polls at night;
- if there are no poll results, do nothing publicly;
- forward the selected event post from `@kldevents`.

The public repost must use Telegram `forward_message` so the repost carries the
source header. `copy_message` is not a normal success path for this product.

Implemented debug entrypoints:

- `poll_to_forward.py`;
- `db.py` table bootstrap for `poll_repost_run`;
- `scheduling.py` job `poll_to_forward_debug`, enabled by
  `ENABLE_POLL_TO_FORWARD_DEBUG=1`;
- production Fly debug target: poll/repost test surface `@keniggpt`, source
  forward chat `@kldevents`.

## Eligibility

An event can participate only when all conditions hold:

- it is scheduled for tomorrow, or active tomorrow for multi-day events;
- it is not cancelled/archived/past;
- it has a known Telegram post in `@kldevents` that can be forwarded;
- it was not recently reposted by this same feature;
- it is suitable for a public recommendation after basic quality checks.

The candidate pool is therefore "tomorrow's DB events that have a repostable
`@kldevents` message", not every event in the database.

If the eligible pool is too small, the poll is skipped:

- production default: at least `5` eligible events;
- debug default: at least `3` eligible events;
- after LLM topic generation there must still be enough distinct non-empty
  options, production default `4`, debug default `3`.

This avoids asking the audience to choose when the system cannot honestly offer
varied outcomes.

## Topic Generation

Topic generation is LLM-only. The LLM receives the eligible tomorrow events and
returns a compact poll plan:

- `options`, each with public text and hidden candidate event ids;
- short rationale for why the option is relevant today;
- warnings when the topic set is weak or underfilled.

The poll question itself is product copy, not a semantic topic decision. Its
primary promise is: today the audience chooses the type/theme of tomorrow's
event, and in the evening the channel author chooses one concrete announcement
inside that theme. The copy should feel friendly and a little blogger-like, not
like an algorithm or a marketing banner. Debug and production rotate several
participation frames by slot, for example:

- `Сегодня вечером подберу рекомендацию на завтра. Давайте выберем тип события вместе.`
- `Давайте выберем тему для завтрашней рекомендации. Вечером я возьму один анонс из варианта, за который будет больше голосов.`
- `Что завтра порекомендовать в канале? Вы выбираете тематику, а вечером я выберу один конкретный анонс из неё.`
- `Давайте вместе решим, из какой темы завтра сделать рекомендацию. Я вечером выберу один анонс и перешлю его сюда.`
- `Что берём для завтрашней рекомендации? Вы выбираете тему, я вечером выбираю один анонс из неё.`

The rotation can be replaced with `POLL_TO_FORWARD_QUESTION_VARIANTS` (`||`
separator) or pinned with `POLL_TO_FORWARD_QUESTION_TEXT`. The default rotation
also avoids repeating the same question text in adjacent debug hourly slots.
The LLM still owns the meaningful option set.

Avoid false mechanics such as "найду/поищу" when the feature operates on
events already stored in the database. Avoid pseudo-personal mood wording such
as "по вашему настроению", neural clichés such as "завтрашний план звучит",
and generic marketing phrases.

Good options are audience jobs-to-be-done, not raw database categories. Examples:

- "вечер с музыкой";
- "с детьми";
- "бесплатно или почти бесплатно";
- "у побережья";
- "фестиваль";
- "что-то необычное";
- "в помещении";
- "загород / восток области".

Avoid advertising-style option text and superlatives. The poll should feel like
the channel asks subscribers what they want the next recommendation to cover,
not like a generic promo banner.

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

Before the repost, the bot sends a short reply to the original poll message
with the winning topic and, when LLM provides it, a compact reason for the final
event choice. Example:
`Спасибо за голоса — берём тему «музыка».

Из этой темы я бы предложил <a href="https://telegra.ph/...">Название события</a> — хорошо подходит под выбранную тему.

Если рекомендация зашла — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.

Сейчас перешлю анонс 👇`

If the poll result is tied, the reply must say so directly instead of naming a
single false winner, for example: `Голоса разделились поровну между
«выставки» и «экскурсии». Беру один из этих вариантов.` The LLM reason should
then explain why the selected event still makes sense as the final
recommendation. It may mention
popularity or public interest only when such signals are grounded in passed
metrics or event text. If an exhibition starts on the target date, natural
wording such as "как раз открывается выставка" is preferred over generic
"сходить на выставку". Keep the LLM reason compact, ideally under 90
characters, and do not repeat the event title in the reason. Avoid marketing
reason leads such as "отличный вариант" or "интересный вариант"; prefer
concrete human phrasing like "для тех, кто голосовал за гастро-отдых, на ферме
как раз праздник".

The intro copy should make voters feel that the recommendation is a consequence
of their choice. Avoid generic promo phrasing and avoid exposing the technical
`forward_message`/repost mechanic to the audience. Telegraph links are attached
to the event title in HTML and sent with web preview disabled. Separate the
thanks, recommendation, feedback prompt, and forwarding line with blank lines so
the Telegram message does not render as one dense block.

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

The two owned VK communities, `@kldevents` and
`https://vk.com/kenigeventsofficial`, should contribute engagement signals
without becoming event sources for this feature. Their views, likes, and reposts
should be compared against their own median baseline and weighted `4x` in the
popularity aggregate used by `popular_posts` and Poll to Repost.

Long-term preferred shape:

- add `reposts` support to VK metric snapshots if missing;
- add per-source metric weights/config for owned audience channels;
- keep retention bounded by the existing post metrics cleanup policy.

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
  profile and slot;
- `resolve_poll` finds due open runs, stops the Telegram poll, and moves the run
  to `resolved`, `skipped`, or `failed`;
- scheduler restarts must be able to continue from the persisted run.

The resolver tick can be infrequent. Debug does not need tight polling; every
30 minutes is enough.

Current debug defaults:

- `ENABLE_POLL_TO_FORWARD_DEBUG=1` in `fly.toml`;
- create ticks only in local hours `9 <= hour < 24` (`24` is exclusive, so
  the last create slot is `23:00 Europe/Kaliningrad`; the quiet night window is
  `00:00-08:30`);
- scheduler checks at minutes `0,30`;
- debug minimum eligible events: `3`;
- debug minimum LLM options: `3`;
- LLM model: `gemini-3.1-flash-lite`;
- anti-repeat window: `7` days.

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
