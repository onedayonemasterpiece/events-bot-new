# Сигналы обсуждения события

> Canonical slug: `event-comment-feedback`. English working name: **Event Comment Feedback**. Status: **explicit post-release research/implementation scope**; live canary proved non-zero decision-fact value, but production YDB collection, typed fact ledger, static block and discussion medallion are not implemented. This capability is not a blocker for the first public static-site presentation.

**Короткое название фичи:** **Сигналы обсуждения**. После продуктового аудита фича разделена на два независимых публичных продукта: **подтверждённые практические факты** и краткоживущий медальон **«Активно обсуждают»**. Ни один из них не называется отзывом, рейтингом или редакционной рекомендацией.

## Документы фичи

- [Personalization data ownership](../../architecture/personalization-data-ownership.md) — YDB owns this independent comment sidecar, not the site user profile.
- [Phrase bank v1](phrase-bank-v1.md) — фиксированная библиотека публичных фраз, prototypes и hard negatives.
- [YDB schema draft](ydb-schema.md) — sidecar-хранилище комментариев, embeddings, matches, verifier cache и public state.
- [LLM verifier contract](llm-verifier-contract.md) — group-level batch verifier, cache keys, forbidden patterns.
- [Static-site contract](static-site-contract.md) — JSON manifest, UI block, carousel, icon semantics.
- [Research and minimal implementation plan](probe-plan.md) — live-canary evidence, 30-day research programme, BGE/E5 gates, typed-fact lifecycle, minimal post-release slices and acceptance checklist.
- [Region Talk reuse audit and skill-first gate](region-talk-reuse-audit.md) — mandatory F14-0 audit, adoption matrix and reusable-skill creation before implementation.

## Product intent

### Primary: verified decision facts

The highest-value surface is a compact static block **«Важно знать»** containing only a new, official, typed practical fact that can change attendance, registration or ticket-purchase behavior:

```text
Важно знать
Организатор уточнил: продолжительность спектакля — около одного часа.
```

Initial families are deliberately narrow: duration/intermission, transfer, doors/entry/parking, venue format, accessibility and TTL-controlled ticket-sector rules. Extraction is BGE/E5 closed-taxonomy routing plus deterministic exact-span slot parsing; no comment LLM extraction is allowed.

### Secondary: «Активно обсуждают»

A separate rolling attention projection may render the medallion **«Активно обсуждают»** with tooltip «За последние 7 дней событие активно обсуждали в открытых источниках». It is not a quality claim, fact, recommendation or ranking input in the first experiment. Giveaways, polls, engagement bait, bots, official answers, duplicates, crossposts, shared-series ambiguity and controversy-dominated threads are suppressed.

### Deferred product surface

The earlier carousel **«Что видно по обсуждению»** with broad positive/neutral/concern phrases remains design evidence, not the minimal implementation target. It may return only after the decision-fact block and medallion have real product metrics and safety evidence.

The canonical research sequence and go/no-go thresholds are in [the probe plan](probe-plan.md).

## Non-goals

- Не внедрять сбор комментариев, YDB adapter, embedding provider, LLM verifier, export scripts или Astro UI в первом docs-first проходе.
- Не создавать новые SQLite-таблицы для комментариев, embeddings, phrase matches, verifier cache или public feedback.
- Не называть блок “отзывы посетителей”, “рейтинг”, “оценка события”, “зрители говорят”, если событие ещё не прошло.
- Не публиковать прямые цитаты, имена, user ids, аватары, ссылки на отдельные комментарии, raw payload/debug fields.
- Не использовать LLM для извлечения фактов из комментариев и не давать LLM генерировать публичный summary.
- Не иметь ручной production-очереди: полностью автоматический режим обязан быть fail-closed и жертвовать recall. Ручная разметка допустима только в research/calibration/canary evidence.
- Не передавать raw comment blob в обычный Smart Update extraction/matching path. Critical fields (`date/time/place/cancellation`) остаются в существующем repair/incident workflow.
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

Current post metrics snapshots already have platform-specific raw `comments` counters for TG/VK posts alongside `views`, `likes`, reposts/reactions where available. What does **not** exist yet is a single canonical/public `comments_count` field on the `Event` or static-preview event layer. Comment feedback should be a **qualitative sibling** of post-metrics, not a replacement:

- reuse source-post identity and raw-count discipline where possible;
- use platform `comments` counters only as a fetch/ranking/precheck signal for the one-off discovery run;
- do not expose technical source/service splits or raw comment-count internals in public UI unless separately approved;
- if a canonical/public `comments_count` is added later, document it in post-metrics and keep public counter policy explicit.

### Event / EventSource model

Relevant current anchors in `models.py`:

- `Event`: `source_text`, `source_texts`, `source_post_url`, `source_vk_post_url`, `source_chat_id`, `source_message_id`, `ticket_status`, `ticket_link`, `is_free`, `lifecycle_status`.
- `EventSource`: canonical multi-source table with `event_id`, `source_type`, `source_url`, `source_chat_username`, `source_chat_id`, `source_message_id`, `source_text`, `trust_level`.
- `EventSourceFact`: source-scoped fact log with statuses `added|duplicate|conflict|note`.

Comment evidence must anchor to `event_source` / structured TG/VK post keys, not only legacy `Event.source_post_url`. Attention signals are **never canonical event facts**. An official decision fact may become a public typed fact only through a dedicated authority/scope/TTL/retraction ledger; the current flat `EventSourceFact(fact,status)` is insufficient and must not silently absorb comment text.

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

The discovery job should be a separate offline job, proposed job kind: `event_comment_feedback_discovery` (short CLI/module alias: `comment_feedback_discovery`). It should publish run/audit state through the same status-ledger style as other Kaggle jobs and feed the static-site builder by manifest artifact, not by runtime API.

### Region Talk experience

Region Talk is mandatory prior art, not a merge dependency. Before implementing this feature, audit its compact YDB state, queue/funnel orchestration, stable identities, pre-network dedup, vector-first/LLM-late gates, Telegram cooldown/session discipline, data minimization, compaction and delivery metrics. Classify each pattern as `reuse|adapt|reject|defer`, then create and validate the required reusable project skills.

Do not copy Region Talk's source frontier, blogger/nonlocal gates, image pipeline, publication queue or generated post writer: F14 starts from current `EventSource` rows and exports only fixed-phrase aggregate feedback to static event pages. The complete pre-implementation contract is [Region Talk reuse audit and skill-first gate](region-talk-reuse-audit.md).

## External references

These references inform product/safety trade-offs; they are not copied as architecture.

- [Google Places AI-powered review summaries](https://developers.google.com/maps/documentation/places/web-service/review-summaries): high-level summaries are based on user reviews and require attribution/disclosure. Product lesson: disclose source basis and avoid implying unsupported certainty.
- [Yelp Review Insights](https://www.yelp-press.com/press-releases/press-release-details/2024/Yelp-Unveils-a-Series-of-New-AI-powered-Features-to-Enhance-Discovery-and-Connection-with-Local-Businesses/default.aspx): aggregated sentiment by business aspect/topic with positive/neutral/critical scores and related review exploration. Product lesson: topic-level clusters are clearer than vague praise.
- [Trustpilot Review Insights](https://business.trustpilot.com/features/review-insights): AI categorizes review topics and positive/negative/neutral sentiment. Product lesson: separate topics from sentiment and track changes over time.
- [NN/g AI Summaries of Reviews](https://www.nngroup.com/articles/ai-reviews/): summaries work when they are specific, transparent and do not hide critical feedback. Product lesson: show concrete topics and allow negative/concern signals.
- [FTC Consumer Reviews and Testimonials Rule Q&A](https://www.ftc.gov/business-guidance/resources/consumer-reviews-testimonials-rule-questions-answers): fake/false reviews and testimonials create legal and trust risk. Product lesson: do not fabricate “visitor reviews”, direct quotes or claims of experience.
- [Telegram discussion groups/comments](https://core.telegram.org/api/discussion): channel comments depend on linked discussion groups/message threads and may be unavailable per post. Technical lesson: keep per-source capability state and skip unavailable threads gracefully.
- [YDB TTL/table docs](https://yandex.cloud/en/docs/ydb/terraform/tables) and [YDB secondary indexes](https://ydb.tech/docs/en/reference/ydb-cli/commands/secondary_index): schema should model TTL/index needs explicitly, but queries must still filter obsolete state because TTL deletion is asynchronous.

## Deferred carousel architecture (not the minimal post-release target)

This section preserves the earlier broad phrase-carousel design as future research. PFR-1–PFR-4 follow the typed-fact/medallion plan in [probe-plan.md](probe-plan.md) and do not use the optional LLM verifier below.

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

## Deferred carousel data flow

The monitoring source list is **not a manually maintained chat/community list**. For each one-off Kaggle discovery run the job builds a bounded seed list from the current event database/static export: future/current active events that are renderable on the static site, plus their linked source posts. The run answers “which comments attached to current event sources should be analyzed now?”, not “which social spaces should we monitor forever?”.

1. **Select future/actionable events** from the existing canonical event/static snapshot: active lifecycle, public/searchable/renderable on static site, future or current, with `EventSource`/source URL/source ids and preferably a raw comment-count signal.
2. **Resolve event sources** from `EventSource` rows first, then legacy `Event.source_post_url/source_vk_post_url`, `source_chat_id/source_message_id`, and static preview/source URLs. Normalize each TG/VK post into a `platform_post_key`, deduplicate posts across events, and keep event-source links for later event-level interpretation.
3. **Build the one-off Kaggle source manifest** for this run: `event_id`, `event_source_id`, `source_url`, platform, `platform_post_key`, optional last known `comments` counter, source fingerprint and fetch capability/status from previous YDB state if present. No unrelated discovery seeds are added.
4. **Fetch comments incrementally** by source capability. If comments are forbidden/unavailable/private, mark source state and continue.
5. **Normalize/dedup/redact** empty/link-only/spam-like/ticket-resale-like/bot-like comments; store hashes and author hashes only.
6. **Embed only changed comments** with model/dim/document version and `text_hash`.
7. **Vector match** comment embeddings against phrase-bank prototypes in memory; YDB stores vectors/cache but is not the MVP vector DB.
8. **Aggregate** by `event_id + phrase_id`: evidence counts, unique authors, source diversity, score/margin, risk/conflict flags.
9. **Decide publication path**: low-risk/high-confidence vector-only; medium/high-risk verifier; weak/ambiguous suppressed.
10. **Verify groups when needed** via cached batch LLM verifier over phrase groups, not comments.
11. **Export static manifest** with aggregate items only.
12. **Render static site**; browser never calls YDB/LLM/VK/TG for this block.

## Deferred carousel YDB storage model

All new persistent data lives in the YDB sidecar described in [YDB schema draft](ydb-schema.md): crawl source state, normalized comments, hashes/dedup, comment embeddings, phrase-bank snapshots/prototypes, vector matches, phrase groups, verifier cache, current public feedback state and run/audit rows. Core Fly SQLite remains the canonical event/source/publication DB and may be read only as an input snapshot.

## Deferred carousel prototype / embedding strategy

- Phrase prototypes are curated examples from [phrase bank v1](phrase-bank-v1.md), embedded with the same model/dimension/document version recorded in YDB.
- Comment embedding input is the normalized comment text; a compact event-context prefix may be tested only if the probe proves quality gain without overfitting.
- Unchanged comments are not re-embedded: `text_hash + embedding_model + embedding_document_version` is the cache key.
- Do not mix embedding models in one score set; each run records model id and dimensions.
- Phrase bank is small, so matching runs offline in Kaggle/Python memory; YDB is a cache/store, not a required ANN/vector-search engine for MVP.

## Deferred carousel incremental pipeline design

Proposed job kind: `event_comment_feedback_discovery` (`comment_feedback_discovery` as a short module/CLI alias). It runs offline/Kaggle and is not part of the public-page hot path.

1. **Select future/actionable events**: active lifecycle, public/searchable/renderable static pages, future/current dates, source URLs/TG/VK ids, optional platform `comments` signal from post metrics or platform metadata.
2. **Resolve event sources**: prefer `EventSource` rows; fall back to legacy `Event.source_post_url`, `source_vk_post_url`, `source_chat_id`, `source_message_id` and static export source URLs. This step creates the one-off Kaggle source manifest for the selected event set and does not depend on a hardcoded list of monitored chats.
3. **Deduplicate source posts but preserve links**: one `platform_post_key` may be reused by several events; source-level fetch state is shared, while event-level phrase interpretation stays tied to `event_id + event_source_id`. Multi-event source posts can be suppressed/manual-review in MVP if the event relation is ambiguous.
4. **Fetch incrementally**: skip unchanged sources when comment count/cursor/TTL says no refresh; limited refresh when state is unclear; forbidden/private/unavailable source does not fail the whole run.
5. **Normalize/dedup/redact**: drop empty/link-only/spam-like/ticket-resale-like/bot-like comments, store hashes and redacted text for backend-only audit.
6. **Embed changed comments only** with recorded model/dim/document version.
7. **Vector match** top-K phrase candidates with positive/hard-negative/next-best margins.
8. **Aggregate by `event_id + phrase_id`** and compute evidence/diversity/conflict/risk flags.
9. **Decide publication path**: strict vector-only for low-risk/high-confidence; verifier/manual review for medium/high-risk; suppress weak/ambiguous.
10. **Export static JSON** allowlisted aggregate items only.
11. **Trigger/hand off static-site rebuild** through the existing Kaggle/static-site builder protocol when public state changes.

## Deferred carousel vector matching strategy

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

## Deferred carousel aggregation strategy

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

## Deferred carousel no-LLM/vector-only mode

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

## Open decisions before post-release implementation

1. Approve the isolated YDB folder/database and production credential/session lane; no Region Talk `DISCOVERY1/2`, E2E or S22 bundle reuse.
2. Approve raw/redacted comment retention after measuring the 30-day shadow footprint.
3. Decide whether PFR-3 may use the existing Smart Update writer only to format already verified typed facts; PFR-1/PFR-2 remain zero-LLM extraction and deterministic public rendering.
4. Approve the first public fact-family allowlist after per-family precision evidence.
5. Approve the medallion entry/exit thresholds only after a real 30-day source/age-bucket baseline.

## Post-release delivery order

The detailed work packages, metrics, thresholds and stop/go gates are canonical in [Research and minimal implementation plan](probe-plan.md). The release order is:

1. **PFR-1 — collector and typed shadow ledger:** daily EventSource-derived TG/VK delta fetch, full Q/A/authority metadata, isolated YDB, BGE/E5 routing, literal slots, no public UI.
2. **PFR-2 — deterministic «Важно знать»:** publish 1–3 active verified facts through a separate static manifest; changed fact-set hash schedules one checked rebuild; no main-description/JSON-LD/critical-field changes.
3. **PFR-3 — Smart Update shadow experiment:** dedicated verified snapshot ingress that bypasses event matching and comment LLM extraction; public prose only after retraction/idempotency/last-good acceptance.
4. **PFR-4 — «Активно обсуждают»:** independent qualified-human rolling score and medallion, no initial ranking effect.
5. **Deferred:** broad discussion carousel, arbitrary fact extraction, generated summaries, critical field repair, ranking boost and «Особо обсуждают».

## First implementation acceptance checklist

- [ ] Region Talk reuse/adoption audit and clean-port boundary are exact-SHA-bound and accepted; required project skills pass their canonical validation/forward tests.
- [ ] 30-day daily shadow proves collection health, authority metadata, product yield and fact lifecycle.
- [ ] No new SQLite comment store; isolated YDB state remains compact and independently retained.
- [ ] No public page runtime dependency on YDB/LLM/VK/TG/embedding provider.
- [ ] No LLM comment extraction, raw-comment Smart Update input or manual production review queue.
- [ ] Accepted-fact precision is `>=0.99` per family with zero wrong-author/event/question/uncertainty/negation/stale-current failures.
- [ ] Static export contains no raw comments, names, ids, comment links, phone numbers or debug/provider payloads.
- [ ] Duplicate/unchanged runs are no-ops; correction/retraction/expiry removes stale active state.
- [ ] PFR-2 does not modify the main description, JSON-LD, critical fields or public source count.
- [ ] «Активно обсуждают» has a real baseline, giveaway/spam/crosspost suppression and no initial ranking effect.
- [ ] Separate post-release RC, checked preview, accessibility/no-JS/mobile evidence, canary, rollback and owner sign-off exist.
