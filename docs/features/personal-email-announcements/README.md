# Personal email announcements

> **Status:** architecture/design only (2026-07-06). No schema migration, sender, static-page generator or production send is implemented in this document.
>
> **Product goal:** возвращать пользователя на `kenigevents.ru` и повышать точность рекомендаций через регулярное письмо + персональную статическую страницу + явную обратную связь.

## Scope

Фича отправляет пользователю регулярный персональный анонс событий:

- письмо раз в выбранный период, базовый MVP cadence — **weekly**;
- в письме: один hero-event и три compact recommendations;
- ссылка на заранее собранную приватную статическую страницу с несколькими десятками личных рекомендаций;
- на странице — продуктовая обратная связь по точности: `всё интересно`, `частично`, `неточная рекомендация`, плюс per-item feedback;
- обратная связь агрегируется и анализируется LLM offline, чтобы корректировать профиль персонализации и будущие рекомендации.

Это **не** transactional reminder about a followed event. Transactional calendar/follow reminders and cancellation/reschedule notices are a separate feature class. Personal email announcements are subscription/marketing-like recommendations and therefore require stricter opt-in, unsubscribe, frequency caps and deliverability gates.

## Requirement traceability

| ID | Requirement | Decision / coverage |
| --- | --- | --- |
| R01 | Static site launch context | Static-first: site remains crawlable and useful without this feature; email pages are generated as static artifacts. |
| R02 | Personalization from authenticated and unauthenticated actions | Reuse `unsigned-personalization` profile horizons; auth identity is an optional merge layer, not a prerequisite. |
| R03 | Authenticated user gives email and may receive mail | Use Supabase Auth/Yandex email or verified manual notification email, only after explicit personal-announcement opt-in. |
| R04 | Anonymous user may provide email and receive mail | Allow anonymous subscription only after explicit email capture + double opt-in; link to current `anon_id` cautiously and never assume cross-device identity. |
| R05 | Increase return visits and recommendation accuracy | Primary metrics are return visits from email/static page and feedback-corrected recommendation accuracy, not raw send volume. |
| R06 | Recommend genuinely interesting events | Ranking uses multi-horizon profile + hard freshness/status filters + diversity/fatigue + explicit feedback corrections. |
| R07 | Periodic email, for example weekly | MVP default weekly; no-send if recommendation quality/freshness is below threshold; user preference can later change cadence. |
| R08 | Email has hero and 3 recommendations | MVP renders **hero = rank #1** plus three compact cards = four visible ranked items. Analytics records ranks 1–4; A/B may later test hero counted as one of three. |
| R09 | Link to pre-generated personal static page with dozens of recs | Generate private, unindexed static pages/manifests before outbox send; email links point to the already published artifact. |
| R10 | Product feedback + LLM analysis for personalization correction | Feedback is stored as explicit labels; LLM runs offline to classify causes/corrections, with deterministic guardrails before profile updates. |

## User and consent model

### Authenticated users

Source of email:

1. Supabase Auth email from Yandex OAuth/custom provider when present and verified enough for notification use.
2. Manual fallback email entered on the static site.

Required state before sending:

- `personal_announcement_subscribed_at` is set;
- current email is present, valid, and not suppressed;
- user accepted this recommendation-email channel separately from event-follow transactional notifications;
- unsubscribe/preference URL is available for the issue.

### Anonymous users

Anonymous subscription is allowed, but it is not the same as account ownership.

Required state before sending:

- browser has consented personalization profile (`anon_id`, `session_id`) compatible with the static-site contract;
- user enters an email on site;
- system sends double-opt-in confirmation; no recommendation issue is sent before confirmation;
- subscription row records the source `anon_id`, consent version, signup page/surface and confirmation timestamp;
- if the anonymous user later authenticates, merge is explicit and auditable.

Guardrail: do not leak the user profile through predictable URLs, public Data API reads, search indexing or email forwarding metadata.

## Architecture overview

```text
Static site interactions
  -> local profile + compact telemetry/feedback
  -> personalization Supabase/Postgres (profiles, subscriptions, outbox, feedback)

Batch recommendation job (weekly or operator-triggered)
  -> reads future event catalog snapshot from Fly SQLite/static export
  -> reads compact profile/subscription snapshots from personalization DB
  -> ranks candidates and applies freshness/diversity/fatigue/suppression gates
  -> writes recommendation issue rows
  -> renders private static recommendation pages/manifests to kenigevents.ru storage
  -> enqueues email_outbox rows only after static artifacts are published

Email worker
  -> rate-limit/suppression/dry-run gates
  -> sends through approved provider
  -> records provider/delivery events

Feedback page
  -> captures issue-level and recommendation-level feedback
  -> updates local profile immediately when safe
  -> queues offline LLM feedback analysis
  -> accepted corrections feed future profile/ranking snapshots
```

## Storage and database ownership

Follow the project dual-DB boundary:

- **Fly SQLite** remains source of truth for canonical events, source imports, lifecycle, public page generation state and scheduler state tied to the event catalog.
- **Personalization Supabase/Postgres** owns visitor/user profiles, email subscription state, recommendation issues, static personal-page metadata, email outbox/delivery state, feedback and LLM correction evidence.
- **Object Storage/CDN** owns static HTML/JSON artifacts for personal pages. Do not store large rendered pages in either DB.

Browser-exposed code may use only publishable personalization keys. Backend/batch/worker paths may use direct Postgres or secret keys. Public direct table writes/reads for profiles, subscriptions, outbox and feedback are forbidden; use same-origin endpoints or tightly reviewed RPCs with explicit grants/RLS.

Current Supabase platform direction makes explicit grants part of the contract: Data API reachability is controlled by Postgres grants first, then RLS. Personalization tables/views exposed to browser roles must include explicit `GRANT` statements plus RLS policies. RPC/functions need explicit `EXECUTE` grants, safe implementation/review, and RLS on underlying tables where applicable.

## Draft entities

Names are architecture placeholders; apply only after migration review.

| Entity | DB | Purpose | Public access |
| --- | --- | --- | --- |
| `personal_announcement_subscription` | personalization Postgres | email, hash, auth user id or anon id, consent, cadence, locale, unsubscribe state | no direct table access |
| `personal_announcement_preference` | personalization Postgres | cadence, topics/cities/date windows, pause state, email channel settings | own-user read/update only through safe endpoint/RPC |
| `personal_announcement_issue` | personalization Postgres | one planned/sent digest instance per subscription and period | service only |
| `personal_announcement_recommendation` | personalization Postgres | ranked event ids, score bands, reason masks, hero flag, freshness/fatigue metadata | service only |
| `personal_static_page` | personalization Postgres | opaque page token/hash, storage key, expiry, build id, noindex flag, issue id | service only; page served from storage |
| `personal_announcement_feedback` | personalization Postgres | issue-level and per-card labels from user | append through safe endpoint/RPC |
| `personal_feedback_llm_review` | personalization Postgres | offline LLM classification of why recommendation was right/wrong and proposed corrections | service only |
| `email_outbox` / delivery events | personalization Postgres | durable send queue and provider evidence | service only |

Email addresses should be encrypted or stored only where strictly needed for sending; hashes are used for joins, rate limits and suppression checks. Suppression records must outlive ordinary profile retention enough to prevent accidental resubscribe sends.

## Static personal page contract

Personal page URL requirements:

- unguessable token/path, for example `/personal/r/<token>/` or preview-prefixed equivalent;
- `noindex, nofollow`, omitted from sitemap and internal crawlable navigation;
- expires or becomes stale after the recommendation window;
- does not embed raw profile vectors, hidden tags, email address or internal score details;
- contains enough rendered HTML to work without client-side ranking;
- feedback controls use POST/same-origin endpoint or safe RPC, not direct public table writes;
- page works if telemetry write fails: user can still open event cards.

Recommended content blocks:

1. Hero recommendation with clear reason text and CTA.
2. `Подобрали ещё` grid/feed with 20–60 future events.
3. Diversity sections: `На этой неделе`, `Похоже на сохранённое`, `Можно попробовать`, `Бесплатное/недорого` when relevant.
4. Feedback block: issue-level quality rating + optional short text.
5. Per-card actions: `интересно`, `не подходит`, `уже видел(а)`, `не моя тема`, `слишком далеко/не то время`.
6. Unsubscribe/preferences/reset personalization links.

## Recommendation generation

Hard gates before ranking:

- future-only event instances for the recommendation window;
- lifecycle/status not cancelled; ticket/register link availability considered but not required;
- dedupe linked event dates and exact repeats;
- suppress events already strongly negative for the user;
- suppress recently emailed events unless resurfacing is justified by explicit feedback or upcoming deadline;
- cap one venue/organizer/category dominance;
- avoid sending if fewer than a minimum number of fresh, acceptable recommendations exist.

Scoring inputs:

- local/server profile horizons: session, short, mid, long;
- explicit likes/not-interested/hides/ticket clicks/share/copy;
- city/date/time/price affinities;
- taxonomy/golden facets from `unsigned-personalization`;
- popularity/source signals as weak priors, never as the only reason;
- exploration slots for high-confidence adjacent interests.

LLM is not in the per-recipient hot path. It may run offline for:

- event enrichment and facet normalization;
- feedback reason classification;
- weekly quality audit of sample issues;
- wording/reason generation after deterministic facts are already chosen.

## Feedback and LLM correction loop

Feedback labels should be typed before free text:

```text
issue_quality = all_interesting | partially_interesting | inaccurate | too_many | too_few
item_feedback = interesting | not_relevant | already_seen | wrong_theme | wrong_place | wrong_time | too_expensive | duplicate | bad_source | other
```

LLM analysis receives only the minimal safe packet:

- recommendation issue id, event public fields/facets, score reasons;
- user feedback labels/free text after PII stripping;
- compact profile facets, not email or raw identity;
- previous accepted corrections when needed.

LLM output must be structured and bounded:

```json
{
  "quality_label": "mostly_good|mixed|poor",
  "likely_failure_modes": ["wrong_theme", "wrong_time"],
  "profile_corrections": {
    "positive_facets": [{"facet":"jazz","delta":0.2}],
    "negative_facets": [{"facet":"kids","delta":0.4}],
    "time_window_preferences": [{"value":"weekend_evening","delta":0.2}]
  },
  "do_not_repeat_event_ids": [123],
  "needs_human_review": false
}
```

Deterministic guards own the final write:

- never let LLM invent uncontrolled taxonomy terms without mapping/review;
- cap correction deltas;
- no profile update from crawler/preview/automation feedback;
- no correction from a single ambiguous free-text note without explicit label;
- keep audit rows so a bad correction batch can be reverted.

## Email content contract

MVP layout:

1. Subject: specific and non-deceptive, e.g. `Ваши события на неделю: джаз, лекции и выходные`.
2. Preheader: one-line value proposition.
3. Hero card: top-ranked event, large image if safe, date/place/CTA/reason.
4. Three compact cards: ranks 2–4 with concise reasons.
5. Link: `Открыть мою подборку` to the pre-generated static page.
6. Feedback micro-CTA: `Подборка попала в интересы?` linking to the same page feedback anchor.
7. Preference/unsubscribe footer.

The hero is rank #1 in the recommendation issue, but the email still shows three additional compact cards to give enough choice. This keeps analytics simple while preserving the user's proposed `hero + 3` format.

## Deliverability, compliance and safety gates

Treat these emails as promotional/subscription messages:

- explicit opt-in per channel;
- visible unsubscribe in the email body;
- RFC 8058-style one-click `List-Unsubscribe` / `List-Unsubscribe-Post` headers for marketing/promotional sends;
- suppression list checked before enqueue and immediately before send;
- accurate sender/from/reply-to/subject;
- physical mailing address or legally approved sender identity block before production;
- frequency caps per recipient and sender;
- bounce/complaint ingestion before scale-up;
- dry-run/canary gates before real weekly sends.

Implementation source checks for this design stage:

- [Gmail sender FAQ](https://support.google.com/mail/answer/14229414?hl=en) says one-click unsubscribe is required for marketing/promotional messages, not transactional ones.
- [FTC CAN-SPAM compliance guide](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business) treats commercial email as requiring truthful headers/subjects and a working opt-out path.
- [Supabase securing your API](https://supabase.com/docs/guides/api/securing-your-api) and [RLS docs](https://supabase.com/docs/guides/database/postgres/row-level-security) require explicit grants + RLS for Data API access control.

## Metrics

Primary:

- email-to-site return rate;
- personal page open rate;
- recommendation click/ticket/share/save rate;
- `all_interesting` / `partially_interesting` / `inaccurate` split;
- per-item negative feedback rate;
- unsubscribe/complaint rate;
- repeat return after receiving a digest.

Quality guardrails:

- no-send rate due to low-quality pool;
- stale/past/cancelled event leakage count = 0;
- diversity cap violations = 0;
- LLM correction accepted/rejected/reverted counts;
- feedback-to-profile-latency for accepted corrections.

Operational:

- static page build success before email enqueue;
- outbox pending/sent/failed/suppressed counts;
- provider bounce/complaint rates;
- Postgres storage growth and retention job success.

## MVP phases

### Phase 0 — docs and contracts

- This document.
- Product decisions for consent/cadence/hero format/private page/feedback labels.
- Data model draft and migration review checklist.

### Phase 1 — dry-run generator

- Offline job builds recommendation issues for internal test subscribers only.
- Static pages render to preview prefix with no emails sent.
- Human QA verifies recommendation quality and feedback UX.

### Phase 2 — internal email canary

- Durable outbox dry-run first, then real sends to operators only.
- Unsubscribe/preference links verified.
- Delivery events and feedback rows verified.

### Phase 3 — limited opt-in beta

- Allow authenticated users to subscribe.
- Anonymous email capture remains behind double opt-in and rate limits.
- Weekly send cap and no-send quality threshold enabled.

### Phase 4 — learning loop

- LLM feedback analysis runs offline on beta feedback.
- Accepted corrections update profile snapshots with audit/revert support.
- Ranking experiments compare baseline vs feedback-corrected issues.

## Open decisions

1. Provider/runtime: reuse future project email outbox/Postbox worker if already merged, or create a dedicated announcement sender with the same suppression/rate-limit primitives.
2. Whether anonymous email subscriptions should be available in the first public beta or delayed until authenticated beta proves value.
3. Exact default recommendation count on the static page: 24, 36 or 60.
4. Whether preference center supports topic/date/city controls in MVP or only unsubscribe/pause/reset.
5. How long private static pages remain accessible after the issue window.
