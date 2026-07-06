# Personal email announcements

> **Status:** architecture/design only (2026-07-06). No YDB schema, sender, static-page generator or production send is implemented in this document.
>
> **Product goal:** возвращать пользователя на `kenigevents.ru` и повышать точность рекомендаций через регулярное письмо + персональную статическую страницу + явную обратную связь.

## Scope

Фича отправляет пользователю регулярный персональный анонс событий:

- письмо раз в выбранный период, базовый MVP cadence — **weekly**;
- в письме: один hero-event и три compact recommendations;
- ссылка на заранее собранную secret-link/static-gated страницу с несколькими десятками личных рекомендаций;
- на странице — продуктовая обратная связь по точности: `всё интересно`, `частично`, `неточная рекомендация`, плюс per-item feedback;
- обратная связь агрегируется и анализируется LLM offline, чтобы корректировать профиль персонализации и будущие рекомендации.

Это **не** transactional reminder about a followed event. Transactional calendar/follow reminders and cancellation/reschedule notices are a separate feature class. Personal email announcements are subscription/marketing-like recommendations and therefore require stricter opt-in, unsubscribe, frequency caps, proof-of-consent and deliverability gates.

## Email message type and funnel

This feature introduces a separate email type:

```text
email_kind = personal_announcement_weekly
channel    = personal_email_announcement
message purpose = weekly personalized event recommendations
```

It must not reuse transactional kinds such as `calendar_confirmation`, `event_reminder_24h`, `event_rescheduled` or `event_cancelled`. Provider streams, suppression rules, unsubscribe text, statistics and dashboards must be able to split this kind from transactional mail.

The core success funnel is explicit:

```text
issue_created
  -> page_published
  -> email_enqueued
  -> provider_accepted/sent
  -> provider_delivered | provider_deferred | provider_bounced | provider_complained
  -> email_open_announcement_click  # click on the email button that opens the personal announcement
  -> personal_page_open
  -> event_card_click | ticket_click | save | share | feedback
```

The product-critical email click is the button/link that says, for example, `Открыть мою подборку` / `Показать персональный анонс`. This click is measured separately from generic email opens and from downstream event-card clicks.

## Requirement traceability

| ID | Requirement | Decision / coverage |
| --- | --- | --- |
| R01 | Static site launch context | Static-first: site remains crawlable and useful without this feature; personal recommendation pages are generated as static artifacts or served through a thin token gate. |
| R02 | Personalization from authenticated and unauthenticated actions | Reuse static-site profile horizons/signals; auth identity is an optional merge layer, not a prerequisite. Feature-owned state is in YDB. |
| R03 | Authenticated user gives email and may receive mail | Supabase/Yandex Auth may provide an external identity/email source, but the verified email, consent and subscription state must be materialized in YDB before any send. |
| R04 | Anonymous user may provide email and receive mail | Allow anonymous subscription only after explicit email capture + double opt-in; bind to current `anon_id` cautiously and never assume cross-device identity. |
| R05 | Increase return visits and recommendation accuracy | Primary metrics are email-link return, personal-page activity and feedback-corrected recommendation accuracy, not raw send/open volume. |
| R06 | Recommend genuinely interesting events | Ranking uses multi-horizon profile + hard freshness/status filters + diversity/fatigue + explicit feedback corrections. |
| R07 | Periodic email, for example weekly | MVP default weekly; no-send if recommendation quality/freshness is below threshold; user preference can later change cadence. |
| R08 | Email has hero and 3 recommendations | MVP renders **hero = rank #1** plus three compact cards = four visible ranked items. Analytics records ranks 1–4; A/B may later test hero counted as one of three. |
| R09 | Link to pre-generated personal static page with dozens of recs | Generate unindexed pages/manifests before outbox send; email links point only to already published artifacts or a thin token gate for those artifacts. |
| R10 | Product feedback + LLM analysis for personalization correction | Feedback is stored as explicit labels in YDB; LLM runs offline to classify causes/corrections, with deterministic guardrails before profile updates. |
| R11 | YDB ownership blocker from review | All feature-owned subscriptions, consent proof, recommendation issues, outbox, feedback, metrics and LLM evidence are YDB-owned; Object Storage is artifact-only; Supabase/Auth is identity-only when used. |
| R12 | Production-quality gates | Consent evidence, outbox state machine, page-token threat model, YDB retention/TTL, metrics schema, deliverability and phased rollout gates are explicit before implementation. |
| R13 | Separate email type and full statistics from deliverability to CTA click | Use `email_kind=personal_announcement_weekly`; metric funnel tracks generation, delivery, `email_open_announcement_click`, page open and downstream clicks. |
| R14 | Personal page uses same card style as similar events | Personal pages reuse the static-site event-card / related-event visual grammar; they are not a separate editorial template. |
| R15 | Honest motivation to provide email | Email-capture copy says plainly that the user can leave email to receive a weekly selection based on watched/clicked events. |
| R16 | Cross-site promo campaign for email capture | Model as an internal promo campaign with related-feed placements, caps and exposure/click metrics. |
| R17 | Promo placement in related-event feed | Optional single card, not always shown, random-tail placement no earlier than the 5th related item or at the list end. |
| R18 | Jobs/queue for offer formation, page generation and send | YDB generation jobs + Kaggle build/offload + page publish + send outbox are separate leased steps. |
| R19 | Reuse Kaggle mechanisms and respect resource competition | Reuse StaticSiteBuilder/Kaggle status patterns, private datasets, callbacks, resource leases, no Telegram session borrowing, low-priority/coalesced runs. |
| R20 | Operational stats must show whether sending works | Delivery, provider webhook, CTA-click and page-open dashboards are first-class YDB metrics with lag/failure counters. |

## User and consent model

### Authenticated users

Possible source of email:

1. External auth provider email, for example Supabase Auth backed by Yandex OAuth/custom provider, when present and suitable for notification use.
2. Manual fallback email entered on the static site.

Required state before sending:

- YDB `pa_subscription` is active for `channel = personal_email_announcement`;
- current verified/manual email and `email_hmac` are present in YDB;
- no active YDB `pa_suppression` row exists for the email HMAC;
- separate YDB consent evidence exists for personal-data/email processing and recommendation-email/ad-like subscription;
- unsubscribe/preference URL is available for the issue.

Supabase/Auth, if used by another part of the site, is only an external identity/email source. It is not the source of truth for this feature, and a valid auth email alone is not permission to send.

### Anonymous users

Anonymous subscription is allowed, but it is not the same as account ownership and is **not** part of the first public beta.

Required state before sending:

- browser has consented personalization profile (`anon_id`, `session_id`) compatible with the static-site contract;
- user enters an email on site;
- system sends double-opt-in confirmation; no recommendation issue is sent before confirmation;
- YDB consent evidence records the source `anon_id`/subject hash, consent versions, signup surface/path and confirmation timestamp;
- if the anonymous user later authenticates, merge is explicit and auditable in YDB.

Guardrail: do not leak the user profile through predictable URLs, public DB/API reads, search indexing or email forwarding metadata.

### Consent evidence

Production design must store consent proof as its own YDB entity, not only booleans on a subscription row. Minimum consent evidence fields:

| Field | Purpose |
| --- | --- |
| `consent_id` | Stable evidence id. |
| `subscription_id` | Subscription linked to the consent event. |
| `email_hmac` | Recipient join/suppression key without exposing raw email in metrics. |
| `subject_type` | `auth_user` or `anonymous`. |
| `subject_id_hmac` | Auth user id hash or anonymous id hash. |
| `channel` | `personal_email_announcement`. |
| `consent_kind` | `personal_data_processing`, `advertising_email`, `recommendation_email`, `unsubscribe`, `revocation`. |
| `consent_text_version` | Exact text/version shown to the user. |
| `privacy_policy_version` | Privacy policy version accepted at signup. |
| `signup_surface` / `signup_url_path` | Site surface and path where consent was given. |
| `ip_hash` / `user_agent_hash` | Minimal abuse/proof metadata, never raw by default. |
| `double_opt_in_token_hash` | Confirmation token hash, not token plaintext. |
| `double_opt_in_sent_at` / `double_opt_in_confirmed_at` | Double-opt-in evidence. |
| `revoked_at` / `created_at` | Revocation and creation timestamps. |

MVP UI must use at least two separate checkbox/consent events: (1) processing the email/personal data for recommendations and (2) receiving regular personal email announcements.

## Email-capture promo campaign

This feature includes a cross-site product promo campaign whose goal is to motivate a user to provide an email for weekly personalized announcements. It is not a partner/sponsored ad; it is an internal product campaign tied to the static site and must still be measurable and capped like other promo surfaces.

Canonical honest copy direction:

> Укажите email — раз в неделю мы пришлём подборку событий, которые реально подходят вам по тому, что вы смотрели и открывали на сайте.

Allowed variants may be shorter, but they must preserve the promise: weekly, personalized, based on site behavior, optional, and unsubscribeable. Do not imply that the user already has a ready high-quality profile if the profile is too sparse; in that case the CTA should say that the selection improves as the user uses the site.

Logical campaign definition:

```text
campaign_key = personal_email_announcement_signup
campaign_kind = internal_product_promo
goal = collect explicit opt-in emails for weekly personal announcements
primary_activity = related_feed_email_capture_card
owner_storage = YDB (`pa_email_capture_campaign`, `pa_email_capture_placement`, metrics)
```

Placement rules for `related_feed_email_capture_card`:

- eligible surfaces: event-detail related/continuation feed, listing personal-feed continuation, and other static-site recommendation rails after they have at least 5 organic cards;
- never insert before the 5th related item; use a pseudo-random position from item 5 onward or append at the end;
- not mandatory and not always shown: cap by session, day, page type and campaign budget;
- at most one email-capture card per page/view;
- hide for users already subscribed, recently dismissed, unsubscribed/suppressed, or in bot/preview/crawler class;
- label as a product suggestion such as `Персональная подборка`, not as an organic event;
- if placed among cards, it must use the same card dimensions and interaction affordances as event cards while clearly being a signup CTA.

The placement is treated as a promo activity for reporting: `promo_exposure`, `promo_click`, `email_signup_started`, `double_opt_in_sent`, `double_opt_in_confirmed`, `email_signup_dismissed`. If a future implementation mirrors this into the existing core `promo_campaign` admin UI, it must not move personal subscription state out of YDB.

## Architecture overview

```text
Static site interactions
  -> local profile + compact telemetry/feedback
  -> same-origin API endpoint
  -> YDB (subscriptions, consent evidence, metrics, feedback, LLM evidence)

Planner job (weekly/daily)
  -> creates YDB pa_generation_job batches for due subscriptions
  -> coalesces work by cadence period and quality/resource gates

Kaggle generation job (preferred heavy path)
  -> reads future event catalog snapshot from Fly SQLite/static export
  -> reads compact subscription/profile/feedback snapshots from YDB export/input
  -> ranks candidates and applies freshness/diversity/fatigue/suppression gates
  -> renders personal pages with static-site card templates
  -> writes checked output manifest/archive for server ingestion

Server ingestion/publish job
  -> validates Kaggle output, writes recommendation issue/card rows to YDB
  -> uploads static/gated page artifacts to object storage
  -> marks pa_static_page as page_published
  -> enqueues YDB outbox rows only after static artifacts are published

Email worker
  -> polls YDB outbox with due-shards and leases
  -> rechecks subscription, consent, suppression, artifact and event validity in YDB/Fly export
  -> sends through approved provider after rate-limit/dry-run gates
  -> records provider ids, delivery webhooks and suppressions in YDB

Feedback page
  -> captures issue-level and recommendation-level feedback through same-origin endpoint
  -> writes labels/metrics to YDB
  -> may update local profile immediately when safe
  -> queues offline LLM feedback analysis
  -> accepted corrections feed future profile/ranking snapshots with audit/revert rows
```

## Storage and database ownership

Follow the feature boundary below:

- **Fly SQLite** remains the source of truth for canonical events, source imports, lifecycle/status, public static event-page generation state and scheduler state tied to the event catalog.
- **YDB** owns all feature-owned data for personal email announcements: subscriptions, consent evidence, verified/manual emails, suppressions, profile snapshots/signals, recommendation issues, ranked recommendation cards, static personal-page metadata, email outbox, provider delivery events, feedback, clicks, page opens, attribution events, daily/weekly aggregates, quality metrics, LLM review evidence and accepted/reverted correction audit.
- **Object Storage/CDN** owns only rendered static HTML/JSON artifacts for personal pages. Do not store large rendered pages in YDB. Do not treat object storage as authoritative subscription, consent, feedback, outbox or metrics storage.
- **Supabase/Auth provider**, if used elsewhere in the project, is only an external identity/email source. It is not the source of truth for this feature. Before any send, the verified email, subscription state, consent evidence and suppression checks must pass in YDB.

Browser-exposed code must not receive YDB credentials and must not write to YDB directly. Public interactions use same-origin endpoints with rate limits, idempotency keys, bot/preview guards, least-privilege service accounts and service-side validation. YDB tokens stay server-side; prefer token-rotation-capable service-account/metadata flows over fixed long-lived access tokens.

YDB design choices for MVP:

- use **row-oriented tables** for subscription/outbox/feedback/metrics OLTP point/range workloads;
- design primary keys for access patterns and avoid hot monotonically increasing keys;
- use due-shards for outbox polling instead of one hot `status = ready` partition;
- use TTL for short-lived raw metrics and debug evidence where supported/configured;
- do not use column-oriented tables for critical subscription/outbox state in MVP; keep columnar/OLAP experiments outside the send-critical path.

## YDB entity groups

Names are architecture placeholders; apply only after YDB schema review.

| Group | YDB tables | Primary access pattern |
| --- | --- | --- |
| Subscription | `pa_subscription`, `pa_subscription_by_identity`, `pa_subscription_by_email_hmac` | send eligibility, preference center, dedupe |
| Email-capture promo | `pa_email_capture_campaign`, `pa_email_capture_placement`, `pa_email_capture_exposure` | cross-site signup CTA campaign, placement, exposure/click/dismiss metrics |
| Consent | `pa_consent_evidence` | prove opt-in/revocation |
| Suppression | `pa_suppression` | immediate no-send by email HMAC |
| Issue | `pa_recommendation_issue`, `pa_recommendation_card` | render email/page, debug ranking |
| Static page | `pa_static_page` | token validation, expiry, artifact state |
| Outbox | `pa_email_outbox`, `pa_email_outbox_by_due_shard` | worker polling with leases |
| Generation jobs | `pa_generation_job`, `pa_generation_artifact`, `pa_generation_job_by_due_shard` | planner/build/publish queue and artifact handoff |
| Delivery | `pa_delivery_event`, `pa_provider_webhook_dedupe` | bounces/complaints/provider evidence |
| Feedback | `pa_feedback`, `pa_feedback_by_subscription` | learning loop, quality metrics |
| Profile snapshot | `pa_profile_snapshot`, optional `pa_profile_signal_raw` | consented compact profile used by the email ranker |
| Metrics | `pa_metric_event_raw`, `pa_metric_daily`, `pa_metric_by_issue`, `pa_quality_guardrail_daily` | return/quality/ops dashboards |
| LLM review | `pa_llm_review`, `pa_profile_correction_audit` | bounded offline correction loop |

### Draft entities

| Entity | DB | Purpose | Public access |
| --- | --- | --- | --- |
| `pa_subscription` | YDB row table | email channel subscription, cadence, locale, auth/anon linkage, active/pause/unsub state | no direct browser access |
| `pa_email_capture_campaign` | YDB row table | internal promo campaign for collecting explicit weekly-announcement opt-ins | service/admin only |
| `pa_email_capture_placement` | YDB row table | eligible surfaces, random-tail placement rules, caps and copy versions | service/static build input only |
| `pa_email_capture_exposure` | YDB row table with TTL/aggregation | signup CTA exposures, clicks, dismissals and conversions | service only |
| `pa_consent_evidence` | YDB row table | auditable proof of personal-data/recommendation-email consent, double opt-in and revocation | service only |
| `pa_suppression` | YDB row table | email HMAC suppression for unsubscribe, bounce, complaint, abuse/manual block | service only |
| `pa_recommendation_issue` | YDB row table | one planned/sent digest instance per subscription and period | service only |
| `pa_recommendation_card` | YDB row table | ranked event ids, score bands, reason masks, hero flag, freshness/fatigue metadata | service only |
| `pa_static_page` | YDB row table | page token hash, storage key, expiry, build id, noindex flag, issue id, publish state | token-gated endpoint only |
| `pa_email_outbox` | YDB row table | durable send queue, idempotency, leases, retries and provider message ids | service only |
| `pa_generation_job` | YDB row table | leased planner/Kaggle/publish queue for issue and page generation | service only |
| `pa_generation_artifact` | YDB row table | Kaggle output manifest/archive/object-storage handoff evidence | service only |
| `pa_delivery_event` | YDB row table | provider webhook evidence, bounce/complaint/delivery state | service only |
| `pa_feedback` | YDB row table | issue-level and per-card labels/free text after PII stripping | append through same-origin endpoint |
| `pa_profile_snapshot` | YDB row table | compact consent-compatible feature snapshot consumed by the weekly email ranker | service only |
| `pa_profile_signal_raw` | YDB row table with TTL | optional short-lived raw profile-signal materialization before compaction | service only |
| `pa_metric_event_raw` | YDB row table with TTL | short-lived raw opens/clicks/feedback/delivery events | service only |
| `pa_metric_daily` | YDB row table | long-lived daily aggregates for product/quality/ops dashboards | service/admin only |
| `pa_llm_review` | YDB row table | offline LLM classification packet/result and bounded correction proposal | service only |
| `pa_profile_correction_audit` | YDB row table | accepted/rejected/reverted profile correction audit | service only |

Email addresses should be encrypted or stored only where strictly needed for sending; HMACes are used for joins, rate limits and suppression checks. Suppression records must outlive ordinary profile retention enough to prevent accidental resubscribe sends.

### Hashing and identifier policy

Never use plain/unsalted SHA for email addresses, subject identifiers or bearer tokens. These values are low-entropy or security-sensitive enough to require keyed hashing:

- `email_hmac = HMAC-SHA256(email_hash_key_version, normalized_email)`;
- `subject_id_hmac = HMAC-SHA256(subject_hash_key_version, stable_subject_id)`;
- `token_hash = HMAC-SHA256(token_hash_key_version, raw_random_token)`;
- keep key-version columns so HMAC keys can be rotated;
- raw email, if needed for sending, is encrypted and never copied into metrics, LLM packets, rendered artifacts or logs.

Email normalization is intentionally conservative: trim whitespace, normalize Unicode/IDNA domain handling and lowercase the domain. Do not globally strip Gmail dots or plus tags unless provider-specific behavior is explicitly documented and tested for this product.

### Primary key sketches

These sketches are not migrations; they exist to force schema review around YDB access patterns and hot-key avoidance. YDB row tables are sorted by primary key, so first key components must match point/range reads without concentrating all writes on one partition.

```text
pa_subscription
  PK: (subscription_id)

pa_subscription_by_identity
  PK: (subject_type, subject_id_hmac, channel, subscription_id)

pa_subscription_by_email_hmac
  PK: (email_hmac, channel, subscription_id)

pa_email_capture_campaign
  PK: (campaign_id)

pa_email_capture_placement
  PK: (campaign_id, surface, placement_id)

pa_email_capture_exposure
  PK: (event_date, shard, occurred_at, exposure_id)

pa_suppression
  PK: (email_hmac, channel)

pa_consent_evidence
  PK: (subscription_id, created_at, consent_id)
  optional lookup: (email_hmac, channel, created_at, consent_id)

pa_profile_snapshot
  PK: (subject_type, subject_id_hmac, snapshot_version)
  latest pointer/update strategy decided in schema review

pa_profile_signal_raw
  PK: (event_date, shard, occurred_at, signal_id)
  TTL target: 30-90 days before compaction

pa_recommendation_issue
  PK: (issue_id)
  lookup: (subscription_id, period_start, channel, issue_id)

pa_recommendation_card
  PK: (issue_id, rank)

pa_static_page
  PK: (token_hash)

pa_feedback
  PK: (issue_id, created_at, feedback_id)
  lookup: (subscription_id, created_at, feedback_id)

pa_generation_job
  PK: (generation_job_id)

pa_generation_job_by_due_shard
  PK: (status, due_shard, next_attempt_at, generation_job_id)

pa_generation_artifact
  PK: (generation_job_id, artifact_kind, artifact_id)

pa_email_outbox
  PK: (message_id)
  unique/send guard: (send_idempotency_key) must reject duplicate sends for the same subscription+period+channel

pa_email_outbox_by_due_shard
  PK: (status, due_shard, next_attempt_at, message_id)
  due_shard = hash(message_id) % 64

pa_delivery_event
  PK: (message_id, created_at, delivery_event_id)
  lookup: (provider, provider_message_id, created_at, delivery_event_id)

pa_provider_webhook_dedupe
  PK: (provider, provider_event_id)

pa_metric_event_raw
  PK: (event_date, shard, occurred_at, metric_event_id)
  TTL target: 30-90 days on occurred_at, according to actual YDB table capabilities/configuration

pa_metric_daily
  PK: (metric_date, metric_name, dimension_key)

pa_metric_by_issue
  PK: (issue_id, metric_name, dimension_key)

pa_metric_by_subscription_week
  PK: (subscription_id, period_start, metric_name)

pa_quality_guardrail_daily
  PK: (metric_date, guardrail_name, dimension_key)

pa_llm_review
  PK: (review_id)
  lookup: (issue_id, created_at, review_id)

pa_profile_correction_audit
  PK: (subject_type, subject_id_hmac, created_at, correction_id)
  lookup: (review_id, correction_id)
```

Long-lived analytics should live in aggregate tables (`pa_metric_daily`, `pa_metric_by_issue`, `pa_metric_by_subscription_week`, `pa_quality_guardrail_daily`) rather than keeping raw click/open rows forever.

### Profile materialization boundary

The personal email ranker must not read live Supabase/PostgREST profile tables. If static-site anonymous personalization continues to use localStorage/Supabase for other surfaces, this feature consumes only YDB-materialized, consent-compatible `pa_profile_snapshot` rows. Optional `pa_profile_signal_raw` rows are short-lived and compacted into snapshots; `pa_profile_snapshot` is the compact ranked-feature input for weekly generation.

## Static personal page contract and token threat model

The MVP implementation default for all real emails is the **thin dynamic gate** (`/personal/r/{token}`) backed by YDB checks. A pure object-storage URL behind an unguessable token is only a **secret-link static artifact**, not a truly private page; it is allowed only for preview/internal dry-run or as an explicit release-owner fallback. Any secret-link mode must follow these limitations and controls:

- token has at least 128 bits of randomness;
- URL contains no `anon_id`, `user_id`, `email_hmac`, sequential `issue_id` or internal score id;
- YDB stores only `token_hash`, never token plaintext;
- page token, unsubscribe token and feedback token are separate;
- artifact contains no email address, raw profile vectors, hidden tags or internal score details;
- artifact is `noindex, nofollow`, omitted from sitemap and not linked from public navigation;
- expiry/revocation are explicit in YDB;
- pure static expiry is best-effort through object deletion/manifest removal, not instant access revocation.

Real-email access gate:

```text
GET /personal/r/{token}
  -> HMAC token
  -> check YDB pa_static_page state, expiry, revoked/suppressed flags
  -> stream static artifact or 302 to a short-lived signed object URL
```

YDB TTL is a cleanup mechanism, not access control: expired/revoked pages must be rejected by query-level filtering/access checks even if TTL deletion has not yet physically removed old rows.

Every token-bearing personal page response, including the thin dynamic gate and any secret-link fallback, must prevent bearer-token leakage through outbound navigation:

- send `Referrer-Policy: no-referrer` where headers are controllable;
- include `<meta name="referrer" content="no-referrer">` in static HTML;
- external links use `rel="noreferrer noopener"`;
- preferred ticket/register click flow is same-origin click recording followed by redirect to the external URL without the page token in the destination URL.

Recommended page content blocks:

1. Hero recommendation with clear reason text and CTA.
2. `Подобрали ещё` grid/feed with 20–60 future events.
3. Diversity sections: `На этой неделе`, `Похоже на сохранённое`, `Можно попробовать`, `Бесплатное/недорого` when relevant.
4. Feedback block: issue-level quality rating + optional short text.
5. Per-card actions: `интересно`, `не подходит`, `уже видел(а)`, `не моя тема`, `слишком далеко/не то время`.
6. Unsubscribe/preferences/reset personalization links.

### Personal page card/template contract

The personal page must look like a natural continuation of the static-site recommendation UI. It reuses the same card grammar as `Похожие события` / continuation feed cards:

- same event-card proportions, image treatment, date/place/price facts and CTA hierarchy;
- same disclosure for non-event cards: the email-capture promo card is a product CTA, not an event;
- same mobile-first vertical feed behavior and desktop grid/list adaptation as static recommendations;
- same source-safe CTA policy (`ticket/register`, event detail, save/share) and no hidden score/profile internals;
- same medallions/badges only when already available in static event card data.

The generated page should be built from card projections, not from raw event rows or source texts. `pa_recommendation_card` stores rank, reason masks and public card fields sufficient to render the page and email.

## Recommendation generation

Hard gates before ranking:

- future-only event instances for the recommendation window;
- lifecycle/status not cancelled; ticket/register link availability considered but not required;
- dedupe linked event dates and exact repeats;
- suppress events already strongly negative for the user;
- suppress recently emailed events unless resurfacing is justified by explicit feedback or upcoming deadline;
- cap one venue/organizer/category dominance;
- avoid sending if fewer than 4 visible valid email recommendations or fewer than the configured page minimum exist.

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

Deterministic reason text is required in first sends. Do not let LLM invent event facts, dates, prices, source URLs or hidden profile explanations in email/static artifacts.

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
- keep YDB audit rows so a bad correction batch can be reverted.

## Email content contract

MVP layout:

1. Subject: specific and non-deceptive, e.g. `Ваши события на неделю: джаз, лекции и выходные`.
2. Preheader: one-line value proposition.
3. Hero card: top-ranked event, large image if safe, date/place/CTA/reason.
4. Three compact cards: ranks 2–4 with concise reasons.
5. Link: `Открыть мою подборку` to the pre-generated static/gated page.
6. Feedback micro-CTA: `Подборка попала в интересы?` linking to the same page feedback anchor.
7. Preference/unsubscribe footer.

The hero is rank #1 in the recommendation issue, but the email still shows three additional compact cards to give enough choice. This keeps analytics simple while preserving the user's proposed `hero + 3` format.

## Jobs, queues and Kaggle offload

Heavy recommendation/page generation should be offloaded to Kaggle when possible, reusing the existing events-bot Kaggle mechanics instead of adding load to the Fly web/runtime process. The Fly process remains a coordinator, thin token gate and sender worker; it does not render dozens of personal pages or run ranking loops inline with user requests.

### Queue/state model

| Step | Owner state | Executor | Output | Notes |
| --- | --- | --- | --- | --- |
| `pa_due_subscription_scan` | YDB `pa_generation_job` | Fly scheduler / lightweight worker | due generation batches | Coalesced by cadence period; skips sparse profiles and suppressed/unsubscribed recipients. |
| `pa_profile_snapshot_materialize` | YDB `pa_profile_snapshot` | lightweight worker or batch | compact ranker inputs | May run before Kaggle so Kaggle reads compact inputs only. |
| `pa_kaggle_generation_launch` | YDB job + Fly `kaggle_run_ledger` | Fly coordinator | Kaggle run id/status dataset | Uses existing `KaggleClient.push_kernel(...)` and `kaggle_status_client`. |
| `pa_kaggle_generate_pages` | Kaggle output manifest | Kaggle CPU | pages archive, issue/card manifest, QA report | Renders card-style static/gated pages and email preview data. |
| `pa_generation_ingest_publish` | YDB `pa_generation_artifact`, `pa_static_page` | Fly/worker | object-storage artifacts + page metadata | Validates archive, uploads artifacts, writes token hashes and publish state. |
| `pa_email_enqueue` | YDB `pa_email_outbox` | Fly/worker | send-ready rows | Only after `page_published` and quality gates. |
| `pa_email_send_worker` | YDB `pa_email_outbox`, `pa_delivery_event` | Fly/worker | provider sends/events | Small, leased, rate-limited; no page generation here. |
| `pa_delivery_webhook_ingest` | YDB `pa_provider_webhook_dedupe`, `pa_suppression` | Fly endpoint/worker | bounce/complaint/delivery metrics | Idempotent webhook handling. |

### Kaggle reuse contract

The personal-announcement generator should follow the StaticSiteBuilder/Kaggle status pattern:

- create a unique private input dataset per run, never a fixed mutable dataset;
- ship a compact event snapshot, card renderer/static-site source slice, YDB-exported subscription/profile/feedback snapshot, and campaign config;
- push with `video_announce.kaggle_client.KaggleClient.push_kernel(...)`;
- create a status dataset with `create_kaggle_run_config(...)` / `create_kaggle_status_dataset(...)` for production runs;
- include `kaggle_status_client.py` and emit `kernel_started`, `preflight_ok`, `alive`, `report_written` and terminal failure events;
- output only a compact archive/manifest/QA report, not a full repo or large dependency tree;
- use encrypted split private datasets for any short-lived service credentials if direct YDB/object-storage writes are explicitly selected, but the preferred MVP handoff is Kaggle output -> server validation/ingestion so Kaggle does not need long-lived YDB write credentials.

### Resource and session constraints

This job competes for Kaggle account CPU/runtime quota with StaticSiteBuilder, Telegram Monitoring, guide monitoring, CherryFlash/CrumpleVideo and other notebooks. It must be lower priority than production monitoring and scheduled public social/video publications.

Required coordination:

- use resource lease key `personal_email:builder`;
- also respect a shared static-generation/CPU lease such as `kaggle_cpu:static_generation` before launching large batches;
- do not use or borrow Telegram auth bundles; this job should not require `telegram_session:s22` or E2E sessions;
- do not run concurrently with `static_site:builder` if both need the same site source/package cache or if Kaggle quota is near exhaustion;
- coalesce weekly generation into batches and skip/carry over low-priority batches when Kaggle is saturated;
- cap batch size by estimated output pages, profile count and timeout; split into multiple due-shards rather than one huge kernel;
- include business progress counters: `subscriptions_done/total`, `pages_rendered/total`, `issues_written/total`, `skipped_low_quality`, `artifact_bytes`, `progress_label`;
- if Kaggle fails after producing a partial manifest, server ingestion accepts only explicitly complete per-subscription artifacts and marks the rest retryable.

### Failure policy

- If page generation fails, do not enqueue email.
- If page publish succeeds but email enqueue fails, keep `pa_static_page` and retry enqueue through the YDB generation job.
- If send fails, do not rebuild the page unless the issue expires or visible events become invalid.
- If Kaggle is unavailable or quota-blocked, skip the weekly send window rather than rendering in the Fly web process; record `no_send_resource_unavailable`.

## Outbox state machine

The outbox is not just a table of rows; it is a leased state machine in YDB:

```text
planned
  -> page_building
  -> page_published
  -> ready_to_send
  -> sending_locked
  -> sent | failed_retryable | failed_permanent | suppressed | cancelled
```

Required fields:

| Field | Purpose |
| --- | --- |
| `send_idempotency_key` | `subscription_id + cadence_period_start + channel`; unique no-duplicate send guard. |
| `content_version` | Issue/content version; recomputing before send may update content behind the same send key. |
| `intentional_resend_key` | Explicit operator/campaign resend id; absent by default. |
| `lease_owner` / `lease_until` | Worker lock. |
| `attempt_count` / `next_attempt_at` | Retry control. |
| `provider` / `provider_message_id` | Provider correlation. |
| `last_error_code` / `last_error_class` | Retry/suppression decision. |
| `created_at` / `updated_at` / `sent_at` | Audit and latency metrics. |

Recomputing an issue before send may update the issue/content version behind the same `send_idempotency_key`. Sending more than once for the same subscription, period and channel requires an explicit `intentional_resend_key`; changing `content_version` alone must never create a second send.

Before every send, the worker must recheck in YDB and the current event snapshot/export:

- subscription active and not paused/unsubscribed;
- consent not revoked;
- email HMAC not suppressed;
- issue is still valid and `send_idempotency_key` has not sent;
- static artifact is published;
- visible events are not cancelled/past;
- at least 4 visible valid recommendations remain;
- sender/recipient/provider rate limits allow send.

## Deliverability, compliance and safety gates

Treat these emails as promotional/subscription messages:

- explicit opt-in per channel;
- visible unsubscribe in the email body;
- idempotent one-click unsubscribe that does not require auth;
- RFC 8058-style `List-Unsubscribe` / `List-Unsubscribe-Post` headers for marketing/promotional sends;
- suppression checked before enqueue and immediately before send;
- accurate sender/from/reply-to/subject;
- physical mailing address or legally approved sender identity block before production;
- frequency caps per recipient and sender;
- provider webhook ingestion for delivery/deferred/bounce/complaint before scale-up;
- dry-run/canary gates before real weekly sends.

Production deliverability gates:

- SPF and DKIM configured and verified;
- DMARC configured and aligned for the sending domain before public beta;
- TLS supported by the provider path;
- PTR/reverse DNS/provider reputation checked when applicable to the sending setup;
- Postmaster Tools or equivalent spam-rate monitoring configured before scale;
- marketing/recommendation From domain/stream is separated from transactional reminders where provider supports it;
- domain warmup and send caps defined;
- provider webhooks dedupe into `pa_provider_webhook_dedupe`; complaints and permanent/hard bounces suppress affected HMACes in YDB, while deferred/temporary failures retry according to provider classification and suppress only after a configured threshold or permanent provider code.

Russian compliance gates for this design stage:

- email recommendation messages are treated as ad/information subscription unless counsel classifies otherwise;
- consent to receive advertising/recommendation emails is separate from consent to process the email/personal data;
- YDB keeps evidence sufficient to prove prior consent and revocation;
- unsubscribe/revocation must stop further sends immediately in product behavior, even if another jurisdiction allows a longer handling window;
- privacy/consent text versions are immutable and linked from every consent evidence row.

Implementation source checks for this design stage:

- [YDB architecture](https://ydb.tech/docs/en/concepts/architecture) documents strong-consistency/multi-row transaction use cases and primary-key physical sorting/partitioning behavior.
- [YDB table model](https://ydb.tech/docs/en/concepts/datamodel/table) distinguishes row-oriented vs column-oriented tables; [CDC](https://ydb.tech/docs/en/concepts/cdc) and [secondary-index docs](https://ydb.tech/docs/en/yql/reference/syntax/alter_table/indexes) currently emphasize row-oriented support for these operational features.
- [YDB row primary-key guidance](https://ydb.tech/docs/en/dev/primary-key/row-oriented) warns against monotonically increasing hot keys; [TTL docs](https://ydb.tech/docs/en/concepts/ttl) describe TTL-driven deletion/eviction.
- [YDB authentication docs](https://ydb.tech/docs/en/security/authentication) state that token privacy is central and token-rotation modes are safer than fixed access-token mode; [Yandex Cloud IAM token docs](https://yandex.cloud/en/docs/iam/concepts/authorization/iam-token) describe short-lived IAM tokens.
- [Gmail sender FAQ](https://support.google.com/mail/answer/14229414?hl=en) says one-click unsubscribe is required for marketing/promotional messages, not transactional ones.
- [FTC CAN-SPAM compliance guide](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business) is a useful international baseline for truthful headers/subjects and working opt-out.
- [Russian advertising law Article 18](https://www.consultant.ru/document/cons_doc_LAW_58968/f892dec1383709792452f18d36e7043306e2be0a/) requires prior consent for advertising over telecommunication networks and puts proof burden on the sender/distributor.
- [Russian personal-data law Article 9](https://www.consultant.ru/document/cons_doc_LAW_61801/6c94959bc017ac80140621762d2ac59f6006b08c/) requires personal-data consent to be specific, informed, conscious and unambiguous, and documented separately from other information/documents.

## Metrics

Primary success metrics:

- email link click rate;
- personal page server open rate;
- event detail click rate;
- ticket click / save / share rate from the personal page;
- `all_interesting` / `partially_interesting` / `inaccurate` split;
- per-item negative feedback rate;
- repeat return after receiving a digest.

Open rate is diagnostic only; it is not a primary success metric.

Quality guardrails:

- no-send rate due to low-quality pool;
- stale/past/cancelled event leakage count = 0;
- diversity cap violations = 0;
- LLM correction accepted/rejected/reverted counts;
- feedback-to-profile-latency for accepted corrections;
- unsubscribe and complaint rates.

Operational:

- static page build success before email enqueue;
- outbox pending/sent/failed/suppressed counts;
- provider bounce/complaint/defer rates;
- YDB read/write latency, throttling/overload count, table storage growth, TTL cleanup health and aggregate-build lag;
- object-storage artifact publish/delete success.

Schema-first metric events in YDB:

| Metric event | Purpose |
| --- | --- |
| `issue_created` | Recommendation issue generated. |
| `page_published` | Static/gated page artifact ready before enqueue. |
| `email_enqueued` / `email_sent` / `email_suppressed` | Outbox funnel. |
| `provider_delivered` / `provider_deferred` / `provider_bounced` / `provider_complained` | Provider feedback. |
| `email_open_announcement_click` | Click on the email button/link that opens the personal announcement page; product-critical CTA metric. |
| `email_link_click` | Other return attribution from email. |
| `personal_page_open` | Server-side personal page gate/open after CTA click. |
| `event_card_click` / `ticket_click` / `save` / `share` | Recommendation outcome. |
| `email_capture_promo_exposed` / `email_capture_promo_clicked` / `email_capture_promo_dismissed` | Cross-site signup promo performance. |
| `double_opt_in_sent` / `double_opt_in_confirmed` | Email signup conversion and verification. |
| `generation_job_started` / `generation_job_completed` / `generation_job_failed` | Kaggle/batch generation observability. |
| `no_send_resource_unavailable` | Kaggle/quota/resource lease prevented a safe send window. |
| `feedback_issue_quality` / `feedback_item_label` | Explicit quality labels. |
| `unsubscribe` | Channel stop event. |
| `no_send_low_quality` | Quality gate prevented send. |

All `pa_metric_event_raw` rows use a minimal envelope:

| Field | Notes |
| --- | --- |
| `metric_event_id` | Unique event id. |
| `occurred_at` | Server-side event time where possible. |
| `event_type` | One of the schema-first event names above. |
| `channel` | `personal_email_announcement`. |
| `issue_id` | Recommendation issue id when applicable. |
| `subscription_id_hmac` or `subject_id_hmac` | HMAC only, never raw identity. |
| `event_id` / `rank` | Present for card/event interactions. |
| `surface` | `email`, `personal_page`, `preference_center`, or `related_feed_promo`. |
| `token_id_hash` | HMAC-SHA256 of token id when applicable, never raw token and never plain hash. |
| `user_agent_class` | `human`, `bot`, `preview`, or `unknown`. |
| `request_id` | Trace id for endpoint/provider correlation. |
| `provider` / `provider_message_id` | Present for delivery events. |

Retention policy:

| Data | Retention target |
| --- | ---: |
| `pa_metric_event_raw` | 30-90 days via TTL, then aggregates only. |
| `pa_email_capture_exposure` | 30-90 days raw, then campaign aggregates. |
| `pa_generation_job` / `pa_generation_artifact` | Keep until send window closes + operational audit window; retain compact terminal report longer. |
| `pa_delivery_event` | 180+ days or longer if provider disputes/compliance require. |
| `pa_feedback` | Longer-lived because it trains personalization; PII-stripped where possible. |
| `pa_consent_evidence` / `pa_suppression` | Long-lived/audit retention; do not TTL like raw metrics. |
| `pa_llm_review` | Keep packet/result/audit until correction is superseded/reverted and policy allows deletion. |

## MVP phases

### Phase 0 — docs and contracts

- This document.
- Product decisions for consent/cadence/hero format/token model/private page/feedback labels.
- YDB entity groups, profile snapshot materialization, HMAC policy, consent evidence schema, outbox state machine, token-gated page model, retention/TTL and acceptance gates.

### Phase 1 — YDB dry-run only

- Generate issues/cards/static artifacts for internal test subscriptions.
- Store all issues, page metadata, QA labels and dry-run metrics in YDB.
- No emails are sent.

### Phase 2 — authenticated internal canary

- Real email only to operators/internal authenticated test subscriptions.
- Validate unsubscribe, suppression, delivery webhooks, page token expiry and bounce/complaint ingestion.
- Postbox/provider dry-run must pass before any real send.

### Phase 3 — limited authenticated opt-in beta

- Allow authenticated users to subscribe.
- Anonymous email capture remains disabled.
- Preference center supports unsubscribe/pause/reset at minimum.
- Weekly send cap and no-send quality threshold enabled.

### Phase 4 — anonymous double opt-in beta

- Add anonymous subscriptions only after abuse/rate-limit/legal proof is stable.
- Double opt-in and consent evidence are mandatory.
- Merge into authenticated identity is explicit and auditable.

### Phase 5 — learning loop

- LLM feedback analysis runs offline only after enough real feedback exists.
- Deterministic corrections first; LLM suggestions are bounded, auditable and reversible.
- Ranking experiments compare baseline vs feedback-corrected issues.

## Acceptance gates

### Before implementation starts

- this spec keeps YDB as the only feature-state owner;
- `email_kind=personal_announcement_weekly` is separate from transactional email kinds;
- `docs/routes.yml` says `ydb_and_static_object_storage`;
- YDB consent evidence schema exists;
- YDB `pa_profile_snapshot` materialization boundary exists;
- HMAC/hash policy for email/subject/token identifiers exists;
- outbox state machine and lease/idempotency fields exist;
- YDB retention/TTL policy exists;
- email-capture promo campaign, placement caps and related-feed insertion rules exist;
- Kaggle generation/resource lease plan exists;
- thin dynamic gate is the default for real emails; secret-link fallback has a token/referrer threat model.

### Before first real email

- SPF/DKIM/DMARC configured and verified;
- one-click unsubscribe POST is idempotent and does not require auth;
- unsubscribe body link works;
- suppression checked before enqueue and immediately before send;
- provider webhook dedupe works;
- complaints and permanent/hard bounces suppress in YDB; temporary/deferred failures retry and suppress only by threshold/permanent provider code;
- no email is sent unless static artifact is published;
- no email is sent if Kaggle generation status is missing, partial without complete per-subscription artifacts, or resource-failed;
- no email is sent if issue has fewer than 4 visible valid recommendations;
- recomputing `content_version` cannot bypass `send_idempotency_key`;
- `email_open_announcement_click` and `personal_page_open` metrics are recorded before beta;
- no email is sent if any visible event is past/cancelled.

### Recommendation quality

- at least 4 visible valid recs for email;
- at least 12-24 valid recs for personal page, depending on chosen page size;
- personal page cards reuse static-site related-event card style;
- rank #1 is hero and ranks 2-4 are compact cards;
- max venue/category dominance enforced;
- recently emailed event fatigue enforced;
- deterministic reason text only; no LLM-invented facts;
- no raw profile, token, identifier HMAC or internal score details in HTML/JSON artifacts;
- outbound links cannot leak page tokens via Referer headers or URL parameters.

## Open decisions

1. Provider/runtime: reuse future project email outbox/Postbox worker if already merged, or create a dedicated announcement sender with the same suppression/rate-limit primitives.
2. Exact default recommendation count on the static page: 24, 36 or 60.
3. Whether preference center supports topic/date/city controls in MVP or only unsubscribe/pause/reset.
4. How long static personal artifacts remain accessible after the issue window.
5. Exact email-capture promo caps: per session/day/week and per related-feed surface.
6. Kaggle batch size and timeout defaults for the first internal canary.
