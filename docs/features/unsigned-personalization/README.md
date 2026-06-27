# Anonymous Personalization for Static Event Pages

> **Status:** MVP-0 design hardened; ready for engineering implementation spike, not product-quality proven
> **MVP:** anonymous-only, no auth, consent/banner with “OK” before personalization telemetry  
> **Primary product goal:** пользователь быстрее находит интересное событие, чем у конкурентов.

## Контекст

Персонализация подключается к static event pages на `kenigevents.ru` только после того, как SEO/GEO-критичный HTML уже отдан пользователю. Она не должна ломать индексируемость, первый экран или CTA.

Связанный static-site contract: `docs/features/static-site-pages/README.md`.

Canonical deploy target for this personalized static site: static HTML/CSS/JS/manifests are published to Yandex Object Storage bucket `kenigevents.ru` and served from `https://kenigevents.ru/`. Do not publish the site to the media/poster bucket default `kenigevents`.

Исследовательская заметка про рекомендации/LLM/embeddings: `docs/features/unsigned-personalization/alanytics.md`.
Решение по выбору моделей и статусу доступов: `docs/features/unsigned-personalization/model-selection.md`.
Контролируемая таксономия признаков: `docs/features/unsigned-personalization/taxonomy.md`.
Итоговый флоу с нейросетями и стадиями внедрения: `docs/features/unsigned-personalization/neural-flow.md`.
Картинка-схема флоу: `docs/features/unsigned-personalization/assets/neural-flow.svg`.
Контракт первого проверочного surface: `docs/features/unsigned-personalization/event-detail-related.md`.
Антибот/automation contract: `docs/features/unsigned-personalization/bots-and-automation.md`.
UI reference board для static event pages: `docs/features/static-site-pages/interface-references.md`.
Thin-runtime architectural gate / production integration: `docs/features/unsigned-personalization/production-integration.md`.

Дополнительные проектные артефакты:

- схема БД/RLS/retention: `docs/features/unsigned-personalization/database.md`;
- контракт влияния на Smart Update: `docs/features/unsigned-personalization/smart-update-contract.md`;
- LLM stress scenario на production sample: `docs/features/unsigned-personalization/llm-stress-scenarios.md`;
- MVP-0 probe report: `docs/features/unsigned-personalization/event-detail-related-probe.md`;
- generated static manifest example: `docs/features/unsigned-personalization/samples/event-detail-related-manifest.sample.json`;
- probe script: `scripts/probe_event_detail_related.py`;
- critical self + Gemini write-path review: `docs/features/unsigned-personalization/gemini-review-2026-06-27.md`;
- Opus architecture gate review: `docs/features/unsigned-personalization/opus-review-2026-06-27.md`;
- Gherkin сценарии: `tests/e2e/features/static_site_personalization.feature`;
- reference client module/demo: `static_site/personalization/personalization.js` and `static_site/personalization/demo.html`;
- Playwright contract test: `tests/playwright/static_personalization_contract.spec.ts`;
- anti-bot/automation contract: `docs/features/unsigned-personalization/bots-and-automation.md`;
- static event page interface references: `docs/features/static-site-pages/interface-references.md`;
- thin-runtime architectural gate / production integration: `docs/features/unsigned-personalization/production-integration.md`.

Documentation rule: links to implementation/test artifacts must be accurate. A
document must not say that a reference client, Gherkin scenario, or Playwright
test is already added unless that file exists in the same commit/PR. Otherwise
the link must remain explicitly marked as planned.

Локальный референс динамики поверх static HTML — соседний проект `/home/dev/projects/kdg80`:

- `RegistrationClient.astro` подгружает state/API после статического рендера;
- `docs/registration-system-requirements.md` фиксирует manifest-first паттерн: read-heavy публичные статусы лучше отдавать same-origin JSON-файлом из bucket, а backend API держать fallback;
- `deploy-preview-to-yc.sh` показывает, что preview-prefix должен переписывать URL, но не должен переписывать same-origin динамические корневые пути, если они intentionally живут в root.

Для personalization это означает: не делать Supabase/PostgREST единственным массовым read-path для каждого посетителя, если тот же результат можно выдать как короткоживущий static/recommendation manifest.

## External review hardening

Feedback from the external review is accepted into the design. The latest review changed the conclusion from “complete” to a narrower statement: MVP-0 is ready for an engineering implementation spike, while recommendation quality, bot resistance and real mobile/desktop usability remain to be proven.

Must-fix items now reflected in the reference artifacts:

- `audience_exclusion_tags` are event exclusions only and never feed user `negative_interest_tags` scoring;
- production core no longer seeds demo preferences; demo seed lives in `demo.html`;
- `served_list_id` / `served_list_hash` are created before card rendering and reused by strong actions;
- `served_list_summary` is deduped by list hash to avoid resize/render telemetry spam;
- legacy `negative_tags` profiles are rejected instead of silently scored;
- incompatible profiles without `feature_schema_version` or `taxonomy_version` fall back to static related order;
- localStorage unavailable/corrupt paths keep static fallback instead of breaking the page;
- resize rerender is debounced and only rerenders on viewport-class breakpoint changes.

Earlier accepted design points remain:

- taxonomy/schema comes before LLM enrichment; LLM tags are proposals, not production taxonomy;
- legacy `negative_tags` is no longer shared between event fields and user dislikes; use `audience_exclusion_tags` for events and `negative_interest_tags` for visitors;
- compact telemetry now includes both `session_summary` and `served_list_summary`, so future rankers get exposure context;
- server profile snapshots are analytics/post-MVP ranker evidence, not an MVP browser-read dependency while public SELECT by `anon_id` is forbidden;
- public Supabase table writes remain forbidden; Supabase RPC ingest is allowed only as a gated append-only write path, with same-origin endpoint still the production default;
- LLM eval is reviewer evidence only; deterministic assertions and human/golden personas are required for acceptance;
- early offline semantic embedding eval is part of MVP hardening, but no embedding provider is in the online hot path;
- MVP-0 starts from `event_detail_related` on an event page, not from a personalized homepage/feed.
- Bot/preview/search actors get static fallback only and cannot train personalization; see `bots-and-automation.md`.
- The expanded probe is an automated golden-smoke, not a human quality proof.

Traceability for the review:

| Review point | Design response |
| --- | --- |
| Missing implementation/test links in an immutable commit | Links to `static_site/*`, Gherkin and Playwright point to concrete files in this branch; planned-only links must stay explicitly marked. |
| Tag drift / LLM-invented tags | `taxonomy.md` defines controlled categories/tags/aliases; `neural-flow.md` puts taxonomy/schema before LLM enrichment. |
| Event exclusion vs user dislike ambiguity | Event field is `audience_exclusion_tags`; user field is `negative_interest_tags`; original research wording is treated as legacy. |
| Need exposure context for future ranker training | `database.md` and `neural-flow.md` add `personalization_served_list_summary` / `served_list_id`. |
| Server profile snapshots do not power anonymous MVP reads | MVP product path is local-first; server snapshots are analytics/post-MVP evidence only. |
| Public Supabase write abuse risk | Production default is same-origin rate-limited endpoint; direct browser table writes are forbidden; browser -> Supabase RPC ingest is allowed only as a dedicated compact append-only function with grants/quota/dedupe/storage guards. |
| Embeddings should be evaluated early, but not online | Early offline comparison against `gemini-embedding-001` and local/Kaggle candidates is an MVP hardening gate. |
| LLM eval is not enough | Acceptance requires deterministic assertions + human/golden personas + optional LLM reviewer. |
| MVP too broad for first proof | Add MVP-0 surface `event_detail_related`: static related block first, local rerank after consent, browser prototype before full Astro integration. |

## Принятые решения на MVP

- Авторизации нет.
- Используется anonymous visitor id + session id; пока SQL использует `uuid`, оба значения должны быть UUID-compatible без префиксов.
- Consent banner допустим: простой “ОК” включает analytics/personalization.
- Personalization state живёт в двух местах:
  - localStorage — лёгкий, быстрый, browser-local профиль;
  - Supabase/Postgres — compact accepted telemetry, debugging/eval, aggregates, and server snapshots for analytics/post-MVP ranker (not a browser read dependency in MVP).
- Supabase не является source of truth для событий; event catalog приходит из Fly SQLite через static export/snapshots.
- Персонализация должна иметь fallback: если выбранный trusted write path (Fly endpoint или Supabase RPC ingest) недоступен, пользователь видит обычный static/local feed и CTA работает.
- Для read-heavy динамики предпочтителен **manifest-first** подход: статический сайт сначала читает same-origin JSON snapshot/recommendation manifest с коротким cache-control. Для записи compact telemetry production default — same-origin endpoint; допустимый lightweight mode — dedicated Supabase RPC ingest. Supabase не является массовым read-path из браузера, а прямые table writes запрещены.
- Первый проверочный MVP surface — `event_detail_related` на странице конкретного события: static fallback “Похожие события” + local rerank after consent. Персонализированная главная/бесконечная лента не входит в MVP-0. Fly web runtime остаётся thin: static/API минимум, без LLM/ML/static-build/analytics aggregation в hot path.

## Что оптимизируем

Не “максимальный CTR”, а **time-to-interest**:

- меньше времени до первого клика по действительно интересному событию;
- меньше скролла до релевантной карточки;
- меньше повторных просмотров нерелевантных событий;
- больше ticket clicks/save/share/return visits без роста hide/quick-skip.

Минимальные метрики MVP:

- time to first relevant action;
- scroll depth before first event click;
- recommendation CTR;
- ticket click rate;
- hide/not interested rate;
- return visitor rate;
- fallback rate из-за Supabase/API timeout;
- p95 latency персональной подгрузки.

Метрики обязательно режутся по `viewport_class` и `layout_mode`: mobile feed может улучшиться, а desktop grid — ухудшиться, и наоборот. Общий средний показатель без этого разреза не является acceptance evidence.

## Mobile feed vs desktop personalization contract

В рамках продукта **«лента» = мобильный паттерн discovery**, а не универсальная форма выдачи. Персонализация должна иметь общий профиль интересов, но разные presentation rules и немного разные сигналы по устройствам.

### Mobile

Цель: за 1–2 экрана быстрее показать событие, по которому пользователь сделает meaningful action.

- Layout: single-column vertical feed, card chunks, sticky lightweight filters/chips, optional bottom sheet для расширенных фильтров.
- Сильные сигналы: `ticket_click`, `share`, `copy_link`, `save`/future favorite, `event_detail_view`, long dwell.
- Средние сигналы: card tap, scroll-stop/dwell checkpoint, повторное появление похожих тегов в сессии.
- Отрицательные сигналы: explicit hide/not interested, quick skip после valid impression, repeated skips same category/venue.
- Ranking guardrails: разнообразие каждые N карточек, не показывать 10 концертов подряд, не повторять venue слишком часто, exploration 10–20%, never block first paint.

### Desktop

Цель: сохранить ожидаемую desktop-афишу с контекстом и контролем, но персонально упорядочить и подсветить релевантное.

- Layout: 2–4 column grid/list, видимые дата/категория/город/цена filters, search, breadcrumbs, optional right rail/modules.
- Персонализация: сортировка в grid/list, dedicated modules «Рекомендуем вам», «Похоже на просмотренное», «На эти выходные для вас», а не растянутая мобильная feed-card.
- Сильные сигналы: detail open, ticket click, copy/share, filter-to-click path, search query -> click, opening card in new tab followed by dwell.
- Слабые/шумные сигналы: hover, focus, mouse movement. Их можно логировать как UI diagnostics, но нельзя давать им большой вес.
- Desktop must-have: keyboard navigation, visible focus, back preserves scroll/filter state, no hover-only critical controls.

### Common telemetry fields

Каждое interaction event должно содержать layout context:

```text
viewport_class = mobile | tablet | desktop
layout_mode    = feed | grid | list | module
surface        = home_feed | home_grid | event_detail_related | date_page | category_page | search_results
position       = zero-based index in the visible surface
page_cursor    = static build/version + client feed cursor/chunk id
algorithm_id   = static_fallback | local_rerank_v1 | rpc_personal_v1 | experiment key
```

Без этих полей нельзя корректно сравнивать mobile и desktop персонализацию.

## Data model draft

### Browser/localStorage

Хранить компактно:

```json
{
  "anon_id": "uuid-v4-compatible",
  "session_id": "uuid-v4-compatible",
  "consent_ok": true,
  "profile_version": "anon-profile-v1",
  "feature_schema_version": "event-features-v1",
  "taxonomy_version": "event-taxonomy-v1",
  "vector_dim": 384,
  "created_at": "2026-06-24T12:00:00Z",
  "last_updated_at": "2026-06-24T12:10:00Z",
  "expires_at": "2026-12-24T00:00:00Z",
  "session_vector": [0.01, -0.02],
  "short_vector": [0.04, 0.11],
  "mid_vector": [0.03, 0.05],
  "long_vector": [0.02, 0.04],
  "negative_interest_vector": [-0.01, 0.08],
  "recent_event_ids": [123, 456],
  "positive_tags": {"concert": 0.7, "jazz": 0.4},
  "negative_interest_tags": {"kids": 0.8},
  "liked_event_ids": [5878],
  "not_interested_event_ids": [6093],
  "feedback_log_tail": [
    {"ts": "2026-06-27T14:00:00Z", "event_id": 5878, "action": "like_event"},
    {"ts": "2026-06-27T14:02:00Z", "event_id": 6093, "action": "not_interested"}
  ],
  "city_affinity": {"Калининград": 1.0},
  "hidden_event_ids": []
}
```

Не хранить секреты, токены backend, raw source texts, большие payloads. Если
`profile_version`, `feature_schema_version`, `taxonomy_version`, `vector_dim`,
`anon_id` или `session_id` несовместимы с текущим manifest/DB contract — выполнить
reset или migrate-known-fields-only. UX обязан иметь действие «Сбросить персонализацию».

### Supabase/Postgres

Черновые сущности:

- `anonymous_visitor` — UUID anonymous id, first/last seen, consent state/version;
- `anonymous_session` — UUID session id, visitor id, started/ended, device/context, rollup state;
- `personalization_session_summary` — основной browser-facing append-only payload: компактный итог сессии/интервала, а не каждое событие скролла;
- `personalization_served_list_summary` — compact exposure/served-list summary для обучения будущего ranker;
- `personalization_event_reaction` — компактный raw-журнал **только сильных явных действий** (`like_event`, `unlike_event`, `not_interested`, `undo_not_interested`) с временем, anonymous/session id, event id, surface и position; из него строится отчёт “сколько лайков сделал конкретный anonymous visitor и когда”;
- `personalization_event_reaction_counter` / static aggregate snapshot — счётчики лайков/негативных реакций по событию для показа на карточках и статического build/export;
- `interaction_event` — опциональная sampled/debug raw telemetry, выключена по умолчанию для слабых impression/skip;
- `visitor_profile_snapshot` — compact `session`/`short`/`mid`/`long` profile horizons; each snapshot keeps positive vectors/maps and the separate negative-interest axis; в MVP это analytics/eval/post-MVP server-ranker evidence, а не browser read dependency;
- `event_feature_snapshot` — lightweight snapshot event features for ranking;
- `recommendation_request` / `recommendation_result` — debug/E2E evidence;
- daily aggregates для retention и аналитики.

Raw telemetry не является основным продуктовым хранилищем: иначе free tier
Supabase быстро заполнится. Базовый ориентир: weak raw impressions/skips
выключены; strong raw actions — 30–90 дней;
`personalization_session_summary` — 30–90 дней или до сворачивания в профиль;
`personalization_served_list_summary` — 14–30 дней full, затем daily aggregate;
profile snapshots / daily aggregates — дольше, потому что они компактные.

## Interaction events

Это словарь сигналов для local ring buffer, compact session summary и
опциональной sampled/debug raw telemetry. Он **не означает**, что каждое событие
скролла должно навсегда писаться отдельной строкой в Supabase.

Минимальные сигналы:

- `page_view`;
- `valid_impression`;
- `event_card_click`;
- `event_detail_view`;
- `dwell_checkpoint`;
- `ticket_click`;
- `hide_event` / `not_interested`;
- `like_event` / `unlike_event`;
- `undo_not_interested`;
- `share` / `copy_link`;
- `recommendation_feed_loaded`;
- `recommendation_fallback_used`.

Каждое событие должно включать:

- anon/session id;
- event id/slug;
- page URL/context;
- position/surface;
- timestamp;
- consent version;
- minimal device class;
- `viewport_class`, `layout_mode`, `surface`, `position`, `page_cursor`;
- `algorithm_id` / experiment bucket, если есть.

## Ranking MVP

Первый ranking должен быть простым и контролируемым:

```text
score =
  dot_session_event + dot_short_event + dot_mid_event + dot_long_event
  + tag/category affinity
  + semantic_embedding_similarity   # low-weight optional score part after eval
  + freshness/date proximity
  + city/venue match
  + price match
  + popularity baseline
  + explicit_like_boost
  + exploration_bonus
  - negative_interest_match
  - fatigue/already_seen penalty
  - explicit_not_interested hard filter
  - explicit_hide hard filter
```

Правила:

- не показывать 10 событий одного типа подряд;
- не повторять одно venue слишком часто;
- сохранять 10–20% exploration;
- явно скрытые события не возвращать в ближайших выдачах;
- explicit like — самый сильный положительный browser-сигнал: он сразу поднимает событие в локальной выдаче и при rollup усиливает `positive_tags`/profile snapshots;
- unlike снимает только явный boost и не считается отрицательным интересом;
- `not_interested` — явный отрицательный сигнал: событие демотируется/скрывается локально и при rollup влияет на `negative_interest_tags`;
- ticket_click сильнее простого card click;
- quick skip/hide — отрицательный сигнал, но quick skip считается только после valid impression.

LLM/embedding/ranker — не MVP online dependency. LLM полезна для offline event enrichment, но не должна вызываться на каждый page view.

Presentation-level reranking отличается по layout:

- **mobile feed**: более агрессивный top-N rerank, diversity/fatigue guardrails, chunk cursor, быстрый hide/skip loop;
- **desktop grid/list**: меньше резких перестановок, больше объяснимых секций и фильтров, персональная сортировка внутри выбранной пользователем категории/даты;
- **event detail related**: больше similarity к текущему событию, меньше long-term profile, чтобы блок был понятен из контекста страницы.

## LLM / model strategy

Основное правило: **LLM не должна быть online dependency для каждого visitor/feed request**. Для большого числа anonymous users дорогая генеративная модель в hot path даст latency, стоимость и quota risk, а не устойчивую персонализацию.

Где LLM нужна:

1. **Offline event enrichment** после импорта/при обновлении события:
   - нормализованные теги, аудитория, настроение/формат, тематики, price/free semantics, tourist/local fit;
   - короткий embedding input text: `title + venue + event_type + search_digest + normalized tags`;
   - объяснимые признаки для ranker/debug, не только свободный текст.
2. **Quality/eval reviewer** для сложных случаев и prompt/schema design; acceptance требует deterministic + human/golden checks, не только LLM.
3. **Cold-start personas** и synthetic evaluation packs.

Где LLM не нужна в MVP:

- не вызывать Gemma/Gemini/OpenAI на каждый `page_view`;
- не отправлять всю историю пользователя в prompt для «выбери карточки»;
- не хранить профиль пользователя как единственный LLM-generated paragraph; нужен вектор + объяснимые affinity maps.

### Live-checked model availability

As of 2026-06-25, availability was smoke-checked with real provider calls from
this repository environment. This is **availability evidence**, not a product
quality decision: final choice still requires Russian event/persona eval.

Live probe artifacts stay local under `artifacts/codex/` and must not be
committed. Public/review docs should contain only sanitized availability
evidence: model id, date, pass/fail class, quota source, and no secret/env-value
fragments. Any accidental secret exposure in local tool output is handled as a
separate security action, not as a public design footnote.

Observed:

- `models/gemma-4-31b-it` answered through all three Google keys
  (`GOOGLE_API_KEY`, `GOOGLE_API_KEY2`, `GOOGLE_API_KEY3`) and through the repo
  `GoogleAIClient`.
- `Gemma 4` must not be treated as “just another text model” in this runtime:
  without explicit thinking config, a simple prompt returned thought-only parts /
  empty direct text. The working config is:

  ```json
  {
    "thinking_config": {
      "include_thoughts": false,
      "thinking_level": "MINIMAL"
    }
  }
  ```

  `thinking_budget=0` was live-rejected by the provider with
  `400 INVALID_ARGUMENT` (“Thinking budget is not supported for this model”).
- `gemini-3.1-flash-lite` answered through `GOOGLE_API_KEY2` and passed
  `reserve -> provider -> finalize` through the Supabase-backed limiter.
- Supabase limiter RPC routes are present: `google_ai_reserve`,
  `google_ai_mark_sent`, `google_ai_finalize`; end-to-end limiter calls passed
  for `gemma-4-31b-it` and `gemini-3.1-flash-lite`.
- OpenAI key availability was checked: `gpt-4o-mini` and `gpt-4o` answered via
  Chat Completions; `gpt-5.4-mini`, `gpt-5.4-nano`, and `gpt-5.5` answered via
  Responses API.
- Embedding availability was checked: OpenAI `text-embedding-3-small`
  (`1536` dimensions), Google `gemini-embedding-001` and `gemini-embedding-2`
  (`3072` dimensions). `text-embedding-3-large` was present in the model list
  but not called in this low-cost smoke.

### Candidate model families to evaluate

Model choice is intentionally **TBD by eval**, not a hardcoded architecture
decision. Availability has been checked as above; quality/cost/latency still
need the real Russian-event eval pack before production use. Current candidates:

| Use case | Primary candidate | Why | Fallback/alternative | Notes |
| --- | --- | --- | --- | --- |
| Event semantic enrichment in existing pipeline | Existing repo Google stack: `gemma-4-31b-it` / stages that already use `gemini-3.1-flash-lite` | Live provider + limiter smoke passed; integrated with secrets, rate limits, schema validation and repo LLM-first policy | OpenAI GPT-5.4 mini/nano for cheaper structured batch tasks; GPT-5.5 for hard eval/prompt audit | For Gemma 4 include `thinking_config.include_thoughts=false` + `thinking_level=MINIMAL`; do not use `thinking_budget=0`. |
| Embeddings for Russian event similarity | OpenAI `text-embedding-3-small` for cheap baseline; `text-embedding-3-large` if quality gap matters | Official OpenAI docs list embeddings for recommendations/search/classification and multilingual model options | Google `gemini-embedding-001` / `gemini-embedding-2` | Pick by Russian-language top-k eval + storage/cost. |
| Multimodal poster/photo similarity later | Google `gemini-embedding-2` or offline vision caption -> text embedding | Multimodal embedding can map image/video/audio/docs/text into one space | Keep out of MVP unless poster similarity proves product value | MVP can use text + poster URL only. |
| Online ranking | No generative LLM | Deterministic/local ranker is faster, cheaper, debuggable | Later CatBoost/LightGBM/two-tower after data | LLM can explain/debug, not decide every request. |

Official docs used for the candidate list:

- OpenAI model selection currently recommends GPT-5.5 as default for complex reasoning/coding and smaller variants such as GPT-5.4 mini/nano for latency/cost: <https://developers.openai.com/api/docs/models>.
- OpenAI embeddings docs describe embeddings for search, recommendations, clustering and classification; `text-embedding-3-small`/`large` are the current embedding model candidates: <https://developers.openai.com/api/docs/guides/embeddings>.
- Google Gemini model docs and Gemma docs are moving targets; use current official pages before changing defaults: <https://ai.google.dev/gemini-api/docs/models> and <https://ai.google.dev/gemma/docs/core>.
- Google Gemini embeddings docs include `gemini-embedding-001` and multimodal `gemini-embedding-2` candidates: <https://ai.google.dev/gemini-api/docs/embeddings>.
- Supabase stores/query vectors through pgvector if/when we need server-side vector search: <https://supabase.com/docs/guides/database/extensions/pgvector>.

### Evaluation before implementation

Перед выбором модели собрать небольшой eval pack:

- 200–500 будущих/недавних событий из production SQLite;
- 8–12 synthetic personas: джаз/концерты, театр, дети negative, турист, бесплатные, выходные, выставки, nightlife;
- expected top-k / must-not-show assertions;
- отдельный split для mobile feed и desktop grid;
- latency/cost/storage estimates;
- JSON/schema validity for enrichment.

Acceptance для модели: качество top-k и объяснимых тегов важнее «самая умная модель в целом». Если дешёвый embedding + heuristics проходит threshold, online LLM не добавляем.

### Manifest-first delivery

По аналогии с kdg80 state manifest:

- generic recommendations можно публиковать рядом со static build как `/data/recommendations/generic.json`;
- event feature snapshot можно публиковать как `/data/events/features.json` или разбить по датам/городам;
- персональный client-side слой сначала берёт static manifest и localStorage profile;
- Supabase RPC вызывается только после consent и только как специально спроектированный ingest/profile/cache endpoint; direct table writes из браузера запрещены;
- при Supabase timeout UI остаётся на static/local rerank.

Это снижает read-нагрузку на Supabase free tier и делает деградацию честной.

## Supabase/RLS policy draft

- Frontend использует только `PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`.
- Backend/migrations используют secret/direct connection string.
- Все exposed tables с RLS.
- Public browser table insert/update/select запрещены в production; таблицы остаются закрыты для `anon`/`authenticated`.
- Production default предпочитает same-origin endpoint с rate-limit и service-role/direct DB insert/RPC.
- Lightweight mode разрешает только dedicated append-only Supabase RPC ingest (`supabase_rpc_ingest_v1`): explicit execute grant, fixed `search_path` при `security definer`, compact typed payload, dedupe/quota/storage guards, no raw JSON row.
- Сырые profiles не доступны на SELECT по anon id.
- Public RPC/view возвращает только безопасный ingest result/recommendation result, а не полный профиль.
- Для anonymous telemetry обязательны rate limits/abuse guard на уровне write path/CDN/WAF/SQL quota, payload caps, cleanup и table-growth alerts.

## Consent UX

MVP banner:

- короткий текст: сайт использует anonymous analytics/personalization, чтобы быстрее показывать интересные события;
- кнопка “ОК”;
- ссылка “Подробнее” на будущую privacy page;
- до ОК не писать персональные telemetry events в Supabase, кроме строго необходимого non-personal operational event, если он нужен;
- пользователь должен иметь возможность сбросить локальную персонализацию.

## MVP-0 surface: event detail related recommendations

Первый персонализируемый surface — не главная страница и не бесконечная лента, а блок “Похожие события” на странице конкретного события:

```text
/sobytiya/<slug>/
  -> static event page
  -> static fallback “Похожие события”
  -> optional local rerank after consent, if a compatible localStorage profile exists
```

Почему так:

- у страницы события уже есть контекст, поэтому cold start проще;
- fallback block полезен без JS/API/consent;
- контекст текущего события можно валидировать на реальном каталоге и synthetic personas;
- ошибки персонализации ограничены маленьким блоком, а не всей главной страницей.

MVP-0 contract:

- `surface = event_detail_related`;
- `layout_mode = module`;
- `algorithm_id = static_related_v1 | local_related_rerank_v1 | local_related_rerank_v1_fallback | semantic_related_v1`;
- `layout_mode = module`, а device-specific представление задаётся `presentation_mode = vertical_related | grid_related`;
- similarity к текущему событию доминирует над long-term profile;
- other dates of the same event уходят в “Другие даты”, не в related;
- hidden/cancelled/current events не показываются;
- desktop использует modules/grid/right rail, mobile — короткий vertical block + “Показать ещё”.

Детальный контракт: `docs/features/unsigned-personalization/event-detail-related.md`.

## Validation probes before Astro implementation PR

Проектирование не считается завершённым, пока нет маленьких проверок на реальном каталоге. MVP-0 probe уже оформлен для `event_detail_related`:

- script: `scripts/probe_event_detail_related.py`;
- report: `docs/features/unsigned-personalization/event-detail-related-probe.md`;
- generated manifest example: `docs/features/unsigned-personalization/samples/event-detail-related-manifest.sample.json`;
- input evidence: production SQLite snapshot `artifacts/db/event_quality_audit_20260624_prod.sqlite` (not committed).

Probe отвечает минимум:

- сколько future active events участвует;
- проходят ли hard invariants: no current/cancelled/hidden in related;
- понижаются ли `negative_interest_tags`;
- соблюдаются ли diversity caps;
- проходит ли local feature baseline hard-invariants as an engineering spike candidate;
- нужен ли `semantic_related_v1` после human/golden top-10 review, а не по архитектурной привычке.

Сравниваемые rankers:

1. `static_related_v1` — current-event similarity + deterministic rules.
2. `local_related_rerank_v1` — static related + localStorage profile + negative interests.
3. `semantic_related_v1` — static/local features + semantic embedding similarity; eval only, not online dependency.

Browser reference prototype уже покрывает `event_detail_related`, а не всю персонализированную главную:

- `static_site/personalization/personalization.js`;
- `static_site/personalization/demo.html`;
- `tests/playwright/static_personalization_contract.spec.ts`.

Последний локальный contract run: `8 passed` (Playwright Chromium).

## Implementation work breakdown

Минимальные work packages для реализации после согласования дизайна:

1. **Event export contract** из Fly SQLite: event JSON, feature JSON, static similarity candidates, stable slug metadata.
2. **Astro static site skeleton** по kdg80-паттерну: layout, EventCard, event leaf pages, listing pages, sitemap/robots/llms, preview deploy rewrite.
3. **Feature enrichment pipeline**: LLM/offline tagging + embedding input text + deterministic safeguards; результаты хранятся как snapshot, а не меняют core event смысл без LLM-first правил.
4. **Static recommendation manifests**: generic/top/date/category/persona-neutral JSON рядом со static build.
5. **Consent + local profile island**: localStorage profile, reset personalization, no telemetry before OK.
6. **Supabase schema/RLS**: compact session summaries, served-list summaries, optional sampled strong actions, profile snapshots for analytics/post-MVP ranker, retention jobs/aggregates, abuse controls.
7. **Client reranker**: local-first ranking, mobile feed chunking, desktop grid/module sorting, fallback on timeout.
8. **Write path / server profile / RPC**: choose `same_origin_endpoint_v1` or gated `supabase_rpc_ingest_v1` for compact telemetry; optional post-MVP RPC for profile snapshots and recommendation cache; not required for first paint.
9. **Metrics/E2E**: personas, viewport split, no-consent/no-Supabase fallback, cross-persona isolation.
10. **Rollout**: preview, internal QA, 1-month Telegraph dual-run, canonical switch criteria.

## Reference client module and Playwright contract test

До появления полноценного Astro-приложения есть reusable browser reference implementation для MVP-0:

- `static_site/personalization/personalization.js` — local-first controller/ranker/telemetry contract;
- `static_site/personalization/demo.html` — static demo page that mimics future Astro island wiring;
- `tests/playwright/static_personalization_contract.spec.ts` — Playwright contract test against the demo/module, not an inline-only mock.

Проверка:

```bash
NODE_PATH=/opt/node-v22.22.3-linux-x64/lib/node_modules \
PLAYWRIGHT_HTML_OPEN=never \
npx playwright test tests/playwright/static_personalization_contract.spec.ts --browser=chromium --reporter=line
```

Минимальная проверка должна проходить Playwright/contract test. Тест фиксирует текущий MVP-0 contract:

- без consent — static related fallback и нет telemetry/localStorage profile;
- mobile после consent — `surface=event_detail_related`, `layout_mode=module`, `presentation_mode=vertical_related`, local rerank, served-list telemetry, hide_event;
- event-side `audience_exclusion_tags` do not trigger `negative_interest_tags` penalties;
- strong actions reuse the same `served_list_id` / `served_list_hash` as the served summary;
- repeated resize/render does not spam `served_list_summary`;
- no-seed consent creates an empty profile, legacy `negative_tags` profiles and missing `taxonomy_version` profiles are rejected;
- blocked localStorage does not enable trusted personalization;
- desktop — `presentation_mode=grid_related`, не mobile feed/infinite feed, current-event context dominates long-term profile;
- selected write path/Supabase timeout — local fallback и CTA/buttons остаются доступны.

Этот тест не заменяет будущие E2E на реальном `kenigevents.ru`, но уже защищает ключевой контракт персонализации при разработке client island.

## E2E/debug требования

Нужны synthetic personas:

- “концерты/джаз”;
- “театр/спектакли”;
- “детские события — negative”;
- “турист/экскурсии”;
- “бесплатные события”;
- “короткая сессия без устойчивого интереса”;
- desktop persona с поиском/фильтрами/открытием в новой вкладке;
- mobile persona с быстрым scroll/skip и одним strong positive action.

Для каждой persona нужно проверять:

- static fallback показывается без Supabase;
- после localStorage профиль влияет на ordering;
- telemetry пишется только после consent;
- recommendation debug snapshot объясняет, почему карточка поднялась/скрылась;
- чужая persona не подмешивается в текущую;
- mobile и desktop assertions проверяются отдельно (`viewport_class/layout_mode`).

## Open questions

- Точный текст consent/privacy для `kenigevents.ru`.
- Какой retention raw telemetry: 30, 60 или 90 дней?
- Сколько event feature fields храним в Supabase, чтобы не дублировать core DB?
- Делаем ли first MVP recommendation полностью local-first или сразу Supabase RPC?
- Какой write path выбираем для telemetry canary: `same_origin_endpoint_v1` или `supabase_rpc_ingest_v1`, и где будут rate/quota/storage guards?
- Где будет backend endpoint для rate-limited recommendation RPC: Fly app, Supabase Edge Function или прямой PostgREST RPC?
- Какой desktop layout выбираем для MVP: grid + filters, grid + right rail, или несколько персональных секций на главной?
- Какой embedding provider выбираем после Russian/persona eval: OpenAI, Google, локальный open-source или staged fallback?
