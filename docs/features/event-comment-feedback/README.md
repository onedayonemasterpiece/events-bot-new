# Сигналы обсуждения события

> Canonical slug: `event-comment-feedback`. English working name: **Event Comment Feedback**. Status: **MVP-1 Kaggle probe runner**; production YDB sidecar, static-site UI and optional LLM verifier are still not implemented. The current runnable slice is `scripts/run_event_comment_feedback_kaggle.py` + `kaggle/EventCommentFeedback/` for an offline Kaggle CPU probe/export.

**Короткое название фичи:** **Сигналы обсуждения**. Оно намеренно не использует слово “отзывы”: фича показывает не оценки посетителей и не рейтинг, а агрегированные смысловые сигналы из открытых комментариев к источникам события.

Публичная формула блока: **“В комментариях отмечают, что …”** / **“В комментариях спрашивают …”** / **“В комментариях обсуждают …”**.

## Документы фичи

- [Phrase bank v1](phrase-bank-v1.md) — фиксированная библиотека публичных фраз, prototypes и hard negatives.
- [YDB schema draft](ydb-schema.md) — sidecar-хранилище комментариев, embeddings, matches, verifier cache и public state.
- [LLM verifier contract](llm-verifier-contract.md) — group-level batch verifier, cache keys, forbidden patterns.
- [Static-site contract](static-site-contract.md) — JSON manifest, UI block, carousel, icon semantics.
- [Probe/evaluation plan](probe-plan.md) — first offline probe and acceptance gates.

## Product intent

Фича добавляет на статическую страницу события компактный блок **“Что видно по обсуждению”**:

```text
🙂 В комментариях отмечают, что это очень ожидаемое мероприятие
На основе 6 комментариев из 2 источников
```

Цель — помочь пользователю быстрее понять общественный контекст события: ожидание, интерес к артисту/программе, вопросы о билетах, практические уточнения, сомнения или барьеры. Это качественное продолжение post-metrics: количественные `views/likes` показывают “сколько внимания”, а **Сигналы обсуждения** показывают “о чём именно говорят”.

## Non-goals

- Не внедрять YDB adapter, static-site Astro UI или unattended public publication in MVP-1. Comment collection/export exists only as an offline Kaggle probe runner.
- Не создавать новые SQLite-таблицы для комментариев, embeddings, phrase matches, verifier cache или public feedback.
- Не называть блок “отзывы посетителей”, “рейтинг”, “оценка события”, “зрители говорят”, если событие ещё не прошло.
- Не публиковать прямые цитаты, имена, user ids, аватары, ссылки на отдельные комментарии, raw payload/debug fields.
- Не использовать LLM на каждый комментарий и не давать LLM генерировать публичный summary.
- Не обновлять `Event.ticket_status`, `is_free`, факты события или Smart Update writer output на основе комментариев без отдельной source-grounded repair workflow.
- Не использовать обсуждения как автоматическое разрешение на acquisition replies/спам.

## Relationship to existing internal features

### Static site pages

Parent feature: [`docs/features/static-site-pages/README.md`](../static-site-pages/README.md). Public event pages are static-first: HTML/JSON/data artifacts are built offline and served from `kenigevents.ru`/CDN. This feature must follow the same rule:

```text
YDB sidecar + existing event/source snapshot
  → offline/Kaggle comment-feedback discovery
  → static comment-feedback manifest
  → Astro static render
  → public page reads only static HTML/JSON
```

No public page view may call YDB, Telegram, VK, embedding providers or LLMs.

### Post Metrics & Popularity

Related docs/code: [`docs/features/post-metrics/README.md`](../post-metrics/README.md), `source_parsing/post_metrics.py`, `models.py::TelegramPostMetric`, `models.py::VkPostMetric`.

Current post metrics store raw `views`, `likes` and Telegram `reactions_json`; there is no canonical `comments_count` field yet. Comment feedback should be a **qualitative sibling** of post-metrics, not a replacement:

- reuse source-post identity and raw-count discipline where possible;
- use `comments_count` only as a fetch/ranking/precheck signal when available;
- do not expose technical source/service splits in public UI;
- if `comments_count` is added later, document it in post-metrics and keep public counter policy explicit.

### Event / EventSource model

Relevant current anchors in `models.py`:

- `Event`: `source_text`, `source_texts`, `source_post_url`, `source_vk_post_url`, `source_chat_id`, `source_message_id`, `ticket_status`, `ticket_link`, `is_free`, `lifecycle_status`.
- `EventSource`: canonical multi-source table with `event_id`, `source_type`, `source_url`, `source_chat_username`, `source_chat_id`, `source_message_id`, `source_text`, `trust_level`.
- `EventSourceFact`: source-scoped fact log with statuses `added|duplicate|conflict|note`.

Comment-feedback evidence must anchor to `event_source` / structured TG/VK post keys, not only legacy `Event.source_post_url`. Discussion signals are **not canonical event facts**; if later reflected in source facts, `note` is the safer conceptual status.

### Unsigned personalization / semantic retrieval

Related docs: [`docs/features/unsigned-personalization/README.md`](../unsigned-personalization/README.md), [`semantic-vector-retrieval.md`](../unsigned-personalization/semantic-vector-retrieval.md), [`event-detail-related.md`](../unsigned-personalization/event-detail-related.md), [`production-integration.md`](../unsigned-personalization/production-integration.md).

Reuse the architectural pattern, not the storage backend:

```text
offline retrieval/vector work
  → optional verifier over candidates only
  → static JSON/HTML
  → no LLM/vector DB/provider calls during ordinary page views
```

Unlike personalization search/recommendations, this feature stores new persistent data in **YDB**, not Supabase/Postgres and not Fly SQLite. Supabase personalization remains for browser telemetry/profiles/recommendations; YDB sidecar owns comment-feedback state.

### Subscriber acquisition

Related docs: [`docs/features/subscriber-acquisition/requirements.md`](../subscriber-acquisition/requirements.md). Comments and discussion threads are social surfaces, but this feature is **read/aggregate/publish-static only**. It must not trigger automatic replies, DMs, user harvesting, personal-wall crawling or mass posting. Acquisition safety concepts still apply: conservative rate limits, sparse use of social signals, manual review for risky actions, and no reach inflation.

### Kaggle/offline infrastructure

Related docs/code: [`docs/operations/kaggle-static-site-builder.md`](../../operations/kaggle-static-site-builder.md), `scripts/run_static_site_builder_kaggle.py`, `kaggle/StaticSiteBuilder/static_site_builder.py`, `kaggle_registry.py`, `kaggle_status.py`.

The MVP-1 runner is:

- launcher: `scripts/run_event_comment_feedback_kaggle.py`;
- Kaggle kernel: `kaggle/EventCommentFeedback/event_comment_feedback_discovery.py`;
- Kaggle kernel slug: `event-comment-feedback-discovery`;
- job kind: `event_comment_feedback_discovery`;
- required Telegram bundle env: `TELEGRAM_AUTH_BUNDLE_DISCOVERY` by default;
- resource lease when status callback is configured: `telegram_session:env:TELEGRAM_AUTH_BUNDLE_DISCOVERY`.

The launcher creates three per-run private Kaggle inputs: payload (`run_config.json`, `prod_source_manifest_full.json.gz`, `phrase-bank-v1.md`), encrypted secret cipher, and Fernet key. It then pushes the kernel, verifies attached dataset sources, polls `kernels_status`, downloads output on terminal completion/failure, and can attach a status dataset created through `create_kaggle_run_config(...)` when `--status-db` and `--status-callback-url` are provided.

The runner is intentionally not a local embedding/API shortcut: embeddings are computed inside Kaggle with the two local sentence-transformer models `intfloat/multilingual-e5-base` and `BAAI/bge-m3`; `embedding_api_allowed=false` is written to the run config and report summary.

The current source reading mode is **API-read with conservative throttling**, not a UI/browser “human-like” reader: Telegram comments are read through Telethon `iter_messages(..., reply_to=...)`; VK comments through `wall.getComments`; the kernel sleeps between source posts and records FloodWait/API errors. A future production increment must add persistent source state/cursors/capability tracking in YDB before this becomes scheduled crawling.

The discovery job feeds the static-site builder by manifest artifact, not by runtime API.

## External references

These references inform product/safety trade-offs; they are not copied as architecture.

- [Google Places AI-powered review summaries](https://developers.google.com/maps/documentation/places/web-service/review-summaries): high-level summaries are based on user reviews and require attribution/disclosure. Product lesson: disclose source basis and avoid implying unsupported certainty.
- [Yelp Review Insights](https://www.yelp-press.com/press-releases/press-release-details/2024/Yelp-Unveils-a-Series-of-New-AI-powered-Features-to-Enhance-Discovery-and-Connection-with-Local-Businesses/default.aspx): aggregated sentiment by business aspect/topic with positive/neutral/critical scores and related review exploration. Product lesson: topic-level clusters are clearer than vague praise.
- [Trustpilot Review Insights](https://business.trustpilot.com/features/review-insights): AI categorizes review topics and positive/negative/neutral sentiment. Product lesson: separate topics from sentiment and track changes over time.
- [NN/g AI Summaries of Reviews](https://www.nngroup.com/articles/ai-reviews/): summaries work when they are specific, transparent and do not hide critical feedback. Product lesson: show concrete topics and allow negative/concern signals.
- [FTC Consumer Reviews and Testimonials Rule Q&A](https://www.ftc.gov/business-guidance/resources/consumer-reviews-testimonials-rule-questions-answers): fake/false reviews and testimonials create legal and trust risk. Product lesson: do not fabricate “visitor reviews”, direct quotes or claims of experience.
- [Telegram discussion groups/comments](https://core.telegram.org/api/discussion): channel comments depend on linked discussion groups/message threads and may be unavailable per post. Technical lesson: keep per-source capability state and skip unavailable threads gracefully.
- [YDB TTL/table docs](https://yandex.cloud/en/docs/ydb/terraform/tables) and [YDB secondary indexes](https://ydb.tech/docs/en/reference/ydb-cli/commands/secondary_index): schema should model TTL/index needs explicitly, but queries must still filter obsolete state because TTL deletion is asynchronous.

## Core architectural decision

Forbidden design:

```text
comments → LLM per comment → LLM writes public summary → site publishes summary
```

Accepted design:

```text
new comments
  → normalization / dedup / redaction
  → embed only new or changed comments
  → vector match against approved phrase bank
  → aggregate by event_id + phrase_id
  → choose publishable candidate phrase groups
  → vector-only publication for low-risk high-confidence phrases
  → optional LLM batch verifier only for medium/high-risk or ambiguous groups
  → verifier approve/reject/downgrade/needs_review; no new public phrases
  → static JSON export
  → Astro static site reads static JSON / renders HTML
```

Optimization formula:

```text
LLM calls scale with risky publishable phrase groups, not with raw comments.

100 comments → 20 vector matches → 4 phrase groups → 0–1 LLM calls
not: 100 comments → 20 LLM calls
```

## Data flow

1. **Select future/actionable events** from the existing canonical event/static snapshot: active lifecycle, public/searchable/renderable on static site, future or current, with `EventSource`/source URL/source ids and preferably a raw comment-count signal.
2. **Resolve sources** from `EventSource`, legacy `Event.source_post_url/source_vk_post_url`, `source_chat_id/source_message_id`, and static preview/source URLs.
3. **Fetch comments incrementally** by source capability. If comments are forbidden/unavailable/private, mark source state and continue.
4. **Normalize/dedup/redact** empty/link-only/spam-like/ticket-resale-like/bot-like comments; store hashes and author hashes only.
5. **Embed only changed comments** with model/dim/document version and `text_hash`.
6. **Vector match** comment embeddings against phrase-bank prototypes in memory; YDB stores vectors/cache but is not the MVP vector DB.
7. **Aggregate** by `event_id + phrase_id`: evidence counts, unique authors, source diversity, score/margin, risk/conflict flags.
8. **Decide publication path**: low-risk/high-confidence vector-only; medium/high-risk verifier; weak/ambiguous suppressed.
9. **Verify groups when needed** via cached batch LLM verifier over phrase groups, not comments.
10. **Export static manifest** with aggregate items only.
11. **Render static site**; browser never calls YDB/LLM/VK/TG for this block.

## YDB storage model

All new persistent data lives in the YDB sidecar described in [YDB schema draft](ydb-schema.md): crawl source state, normalized comments, hashes/dedup, comment embeddings, phrase-bank snapshots/prototypes, vector matches, phrase groups, verifier cache, current public feedback state and run/audit rows. Core Fly SQLite remains the canonical event/source/publication DB and may be read only as an input snapshot.

## Prototype / embedding strategy

- Phrase prototypes are curated examples from [phrase bank v1](phrase-bank-v1.md), embedded with the same model/dimension/document version recorded in YDB.
- Comment embedding input is the normalized comment text; a compact event-context prefix may be tested only if the probe proves quality gain without overfitting.
- Unchanged comments are not re-embedded: `text_hash + embedding_model + embedding_document_version` is the cache key.
- Do not mix embedding models in one score set; each run records model id and dimensions.
- Phrase bank is small, so matching runs offline in Kaggle/Python memory; YDB is a cache/store, not a required ANN/vector-search engine for MVP.

## Incremental pipeline design

Proposed job kind: `event_comment_feedback_discovery` (`comment_feedback_discovery` as a short module/CLI alias). It runs offline/Kaggle and is not part of the public-page hot path.

1. **Select future/actionable events**: active lifecycle, public/searchable/renderable static pages, future/current dates, source URLs/TG/VK ids, optional comment-count signal from post metrics or platform metadata.
2. **Resolve sources**: prefer `EventSource` rows; fall back to legacy `Event.source_post_url`, `source_vk_post_url`, `source_chat_id`, `source_message_id` and static export source URLs.
3. **Fetch incrementally**: skip unchanged sources when comment count/cursor/TTL says no refresh; limited refresh when state is unclear; forbidden/private/unavailable source does not fail the whole run.
4. **Normalize/dedup/redact**: drop empty/link-only/spam-like/ticket-resale-like/bot-like comments, store hashes and redacted text for backend-only audit.
5. **Embed changed comments only** with recorded model/dim/document version.
6. **Vector match** top-K phrase candidates with positive/hard-negative/next-best margins.
7. **Aggregate by `event_id + phrase_id`** and compute evidence/diversity/conflict/risk flags.
8. **Decide publication path**: strict vector-only for low-risk/high-confidence; verifier/manual review for medium/high-risk; suppress weak/ambiguous.
9. **Export static JSON** allowlisted aggregate items only.
10. **Trigger/hand off static-site rebuild** through the existing Kaggle/static-site builder protocol when public state changes.

## Vector matching strategy

The phrase bank is not a keyword list. Each public phrase has positive prototypes, hard negatives and policy thresholds.

For each comment embedding:

```text
positive_score  = max cosine(comment, positive_prototypes[phrase_id])
negative_score  = max cosine(comment, hard_negative_prototypes[phrase_id])
next_best_score = max cosine(comment, prototypes[other_phrase_id])
margin          = positive_score - max(negative_score, next_best_score)
```

Accept evidence only if all hold:

- `positive_score >= phrase.threshold`;
- `margin >= phrase.margin_threshold`;
- `negative_score < phrase.negative_threshold`;
- comment is not spam/off-topic/duplicate/link-only/ticket-resale;
- source/event relationship is strong enough.

Risk classes:

- **low**: lower threshold, vector-only possible;
- **medium**: higher threshold + group verifier for reputation/past-experience/factual-sensitive phrases;
- **high**: highest threshold + verifier/manual review.

Deterministic rules are guardrails only: dedup, source caps, min evidence, author diversity, PII/link filtering, retention, conflict suppression. They must not become broad semantic regex classifiers.

## Aggregation strategy

A public phrase is an aggregate claim. Do not publish “В комментариях отмечают…” from one weak comment.

Group metrics:

- `evidence_count`, `weighted_evidence_count`, `unique_authors_count`, `sources_count`;
- `avg_positive_score`, `avg_margin`, `max_negative_score`, confidence;
- representative internal evidence keys/snippets for review only;
- `risk_flags_json`, `conflict_flags_json`.

Weighting rules:

- one `author_hash` cannot contribute unbounded weight;
- duplicate text does not strengthen evidence;
- several source threads are stronger than one;
- freshness may be a small boost;
- comment likes/reactions are optional later signal, not MVP requirement.

Default publication thresholds:

- show block only when `comments_seen_count >= 5`, `comments_used_count >= 2`, and at least one item is publishable;
- phrase must pass its `min_evidence_count`, `min_unique_authors`, confidence threshold and conflict checks;
- max 5 public items per event, preferably 1–3 in MVP.

## No-LLM/vector-only mode

If LLM verifier is unavailable:

- publish only low-risk/high-confidence `vector_strict_v1` phrases from the allowlist;
- do not publish new medium/high-risk insights;
- keep previously verified non-stale items until TTL;
- record degraded mode in `event_comment_feedback_run`.

Vector-only examples: `anticipation_high`, `anticipation_long_wait`, `intent_to_attend`, `already_planning_visit`, `group_visit`, `family_visit`, `ticket_availability_question`, `registration_interest`, `time_questions`, `age_limit_questions`, `children_questions`, `location_questions`, `payment_questions`, `pushkin_card_questions`, `format_unclear`, `information_missing`.

Vector-only forbidden examples: `sold_out_disappointment`, `high_demand_from_ticket_friction`, `organizer_quality`, `organizer_trust`, `past_event_positive`, `artist_loved`, `actor_loved`, `price_concern`, `organization_concern`, `accessibility_concern`, `refund_exchange_questions`.

## Ticket / sold-out special policy

Do not mix these phrases:

1. “В комментариях спрашивают о наличии билетов”
2. “В комментариях обсуждают доступность билетов”
3. “В комментариях расстраиваются, что билеты быстро закончились”
4. “В комментариях видно, что событие вызвало высокий интерес к билетам”

Rules:

- “Билеты закончились?” is not proof of sold-out.
- “Продам билет” is spam/resale evidence, not demand evidence.
- If canonical `Event.ticket_status` conflicts with comments, downgrade or suppress.
- If `ticket_status=available` and comments say “нет билетов”, do not publish a strong factual phrase without review.
- Safe downgrade phrase: “В комментариях обсуждают доступность билетов”.
- Internal flag: `possible_ticket_status_conflict=true`.

## Safety, privacy and compliance constraints

Public site must not contain:

- author names, user ids, avatars;
- direct quotes or raw comment text;
- links to individual comments unless separately approved;
- raw payloads, debug data, YDB keys, private ids;
- unprocessed PII.

Forbidden public copy:

- “Все ждут это событие”;
- “Зрители в восторге”;
- “Событие точно стоит посетить”;
- “Лучшее мероприятие месяца”;
- “Билеты раскуплены”;
- “Организаторы лучшие в городе”.

Allowed framing:

- “В комментариях отмечают…”;
- “В комментариях спрашивают…”;
- “В комментариях обсуждают…”;
- “В комментариях расстраиваются…”.

## Architectural pitfalls and mitigations

| Pitfall | Why it matters here | Mitigation |
|---|---|---|
| LLM cost explosion | Telegram/VK threads can have many comments per event. | Vector match first, aggregate groups, LLM only group-level, cache verifier decisions. |
| False social proof | One comment could become “people say”. | Min evidence, unique authors, source diversity, cautious wording and suppression thresholds. |
| Fake review/testimonial risk | Generated text can look like visitor reviews. | No direct quotes/names, no “reviews”, fixed phrase bank only, disclosure as open-comment summary. |
| Sold-out factual conflict | Comments may contradict `Event.ticket_status`. | Downgrade to availability discussion; internal conflict flag; no strong factual phrase without review. |
| Sarcasm/negation | Vector similarity may catch sarcastic “лучшие организаторы”. | Hard negatives + risk flags + verifier for reputation/past-experience phrases. |
| Spam/resale leakage | “Продам билет” could falsely boost demand. | Internal `spam_ticket_resale` class excludes evidence. |
| Telegram comments availability | Not every channel post has a linked discussion/thread. | `event_comment_source_state.comments_capability`, skip gracefully, no run-wide failure. |
| VK/TG API limits | Fetching comments can be expensive and session-sensitive. | Incremental fetch, raw count precheck, `next_fetch_after`, per-source throttles. |
| YDB as vector DB | MVP phrase bank is small; ANN in YDB would overcomplicate. | Store vectors/cache in YDB; match in Kaggle/Python memory via numpy/faiss/sklearn. |
| Static build bloat | Embedding feedback into every `PreviewEvent` can inflate `preview-events.json`. | Separate compact `comment-feedback.json` manifest; per-event JSON only if it grows. |
| Privacy leak | Static JSON is public and crawlable. | Export allowlist, schema checks, tests forbidding raw comments/authors/private ids. |
| Stale feedback | Comments can change after static build. | `generated_at`, `updated_at`, `stale` status, scheduled rerun/rebuild triggers. |
| Phrase-bank drift | Changing phrase text/prototypes invalidates embeddings/cache. | `phrase_bank_version`, prototype version, cache invalidation. |
| Overly deterministic semantics | Regex rules can silently change meaning. | Vector prototypes primary; deterministic logic only guardrails. |
| Vague UX | “Людям нравится” adds little value. | Specific phrase categories: tickets, artist, organizers, age limits, time, sold-out friction. |

## Open questions

1. Which YDB database/folder/credentials lane will own this sidecar, and what env naming convention should be used?
2. What first-source allowlist is approved for comment fetching: only event-owned sources, public Telegram channel discussions, VK community posts, or broader discovered surfaces?
3. What is the exact retention window for raw normalized comments: 30/60/90 days?
4. Should production UI show evidence counts (“6 комментариев из 2 источников”) for all items, or hide counts for low evidence/high sensitivity items?
5. Who approves medium/high-risk phrase groups during canary if LLM verifier says `needs_review`?

## Suggested implementation milestones

### MVP-0 Documentation

- docs only;
- phrase bank v1;
- YDB schema draft;
- verifier contract;
- static JSON contract;
- probe plan;
- architecture review checklist.

### MVP-1 Offline Kaggle probe

- `scripts/run_event_comment_feedback_kaggle.py` builds/accepts a production event-source manifest, uploads it as a Kaggle payload dataset, runs `kaggle/EventCommentFeedback/event_comment_feedback_discovery.py`, polls Kaggle status and downloads XLSX/CSV/JSON outputs;
- vector matching runs only inside Kaggle with `intfloat/multilingual-e5-base` and `BAAI/bge-m3`; no local or provider embedding calls;
- optional `--status-db` + `--status-callback-url` enables the shared Kaggle status-ledger/callback path; without it the launcher still performs local Kaggle API polling but does not claim production UI observability;
- no public site change;
- probe report and downloaded outputs stay under `artifacts/codex/event-comment-feedback/` and are not committed.

### MVP-2 YDB storage + incremental fetch skeleton

- YDB adapter;
- source state;
- comments storage;
- embeddings storage;
- no UI.

### MVP-3 Aggregation + static export

- phrase groups;
- vector-only low-risk publication;
- static JSON export;
- schema checks.

### MVP-4 Optional LLM group verifier

- only medium/high-risk phrase groups;
- batch/cached verifier;
- no per-comment LLM.

### MVP-5 Site carousel

- `EventCommentFeedbackCarousel` component;
- green/gray/red icons;
- static JSON read;
- accessibility and reduced-motion checks;
- preview checks.

### MVP-6 QA and rollout

- probe on real future events;
- manual top-N review;
- canary static preview;
- production gate.

## First implementation acceptance checklist

- [ ] No new SQLite tables for comments/embeddings/matches/verifier/public feedback.
- [ ] No public page runtime dependency on YDB/LLM/VK/TG/embedding provider.
- [ ] No per-comment LLM architecture.
- [ ] No generated public phrases outside phrase bank.
- [ ] Static export contains no raw comments, author names, user ids or private ids.
- [ ] Medium/high-risk phrases require verifier/manual review or are suppressed.
- [ ] Ticket/sold-out conflict policy is tested.
- [ ] Phrase precision and unsafe-publication gates pass in probe.
