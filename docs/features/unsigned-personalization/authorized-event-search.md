# Authorized event search with Supabase pgvector

> Status, 2026-07-27: backend P0 infrastructure is deployed; the static client
> now has one origin-scoped Supabase/Yandex PKCE controller shared by Search,
> Personal and the mobile menu. Enter/search-key submission and bounded
> bounded response-header/stream failure states are implemented without duplicate POST rescue. Production UX is still
> conditional on a fresh immutable-candidate real Yandex round-trip and real
> Edge-result browser acceptance; historical preview evidence is not that gate.

## Product contract

Authenticated users get **Умный поиск** on a dedicated `/poisk/` page and, for
the preview canary, on listing/index pages. The standalone mobile page uses a
large multiline intent field; compact embedded placements may keep the
one-line control. The mobile terracotta tag drawer, desktop header and footer
expose a plain navigation link **Поиск** to `/poisk/`; the search form itself is
not placed inside the drawer so the header remains compact. The user can type a
natural-language intent, for example “урбанистика”, “детский мастер-класс” or
“джаз вечером”. Results are rendered as the same event cards used in
`Смотрите дальше`:

- card opens the event detail page;
- like / unlike updates local personalization state;
- `Не интересно` feeds the negative-interest profile and hides/downranks future cards;
- share uses the already accepted image+text+link Web Share path;
- calendar action remains available only for calendar-eligible events.

When vector results are exhausted, the UI starts a separate section **«Возможно, вам будет интересно»**. This is fallback/discovery, not a continuation of exact search relevance.

Anonymous users have quota `0`, but the intent field remains editable. On
submit the current query is saved for no longer than 30 minutes and the user is
sent through the existing Yandex PKCE flow. After the callback the query is
restored, consumed and submitted automatically with the valid Supabase
session. Results are never replaced with anonymous/demo data.

The structural skeleton is initially hidden and may become visible only after
the user submits a request and the runtime enters loading. An empty untouched
query is not a loading state. The standalone desktop and mobile surface share
the same multiline field and dark full-width submit treatment; a disabled
preview must not reveal a specimen skeleton or a second visual language.

### v29 immutable-preview runtime configuration

The unified noindex candidate uses the real authorized Search path rather than
a read-only visual specimen. `site/scripts/preview-public-env.mjs` resolves only
the browser-safe personalization Supabase URL, publishable key and auth
provider from the supported `PUBLIC_*`, `STATIC_SITE_PUBLIC_*` and
`PERSONALIZATION_*` aliases. It may read the repository `.env` while building
a linked worktree, but it never forwards service-role keys or provider secrets
into the Astro client bundle.

`PREVIEW_REQUIRE_AUTHORIZED_SEARCH=1` is a release gate: the preview build must
fail instead of publishing a cosmetically enabled form when the safe public
configuration is absent. Acceptance requires editable anonymous input, saved
PKCE intent, hidden pre-submit skeleton and a real Edge Function result smoke;
the final human Yandex round-trip remains an identity-provider acceptance step.

### Mobile point-of-intent entry and query cloud (v22 research)

На mobile `/poisk/` — самостоятельный пункт нижней навигации; он активен на
странице поиска, а календарная date-strip там не дублируется. Поле запроса видно
анонимному пользователю сразу, поэтому можно сформулировать намерение до входа.
Auth раскрывается inline после submit, запрос сохраняется в поле, но результаты
до действующей session не подменяются демо-данными. Это point-of-value gate:
постоянный большой login CTA в каждой шапке не нужен. Те же auth entry points
могут появляться в `Для меня` и при явном durable save/sync; после входа общим
сквозным контролом становится компактный avatar/account menu.

Порядок inline auth: поле email и `Получить код`, разделитель `или`, затем
`Войти через Яндекс`. Yandex PKCE возвращает на очищенный текущий URL и сохраняет
введённый запрос. **Production status на 2026-07-21:** реально подключён только
`custom:yandex`; email OTP/code UI в `v22` — исследовательская демонстрация
состояний из ещё не применённого identity-controller и не должен называться
рабочей production-авторизацией до отдельного rollout/acceptance.

Облако над поиском обозначается как `Подборки по запросам` и разделяется с
персональными сохранениями. Его целевой источник — централизованно
нормализованные LLM и одобренные редакцией общеполезные запросы, которые после
накопления становятся регулярно обновляемыми статическими tag pages. Пока таких
страниц нет, prototype обязан показывать `Демо` и
`data-prototype-simulated=true`: chips только подставляют текст в query/`?q`, не
ведут на вымышленные `/tag/` и не утверждают, что пользователь их сохранял.
Личные сохранённые поиски — отдельный auth-only объект.

Mobile acceptance: touch targets не меньше `44px`, input font не меньше `16px`,
нет overflow на `320/390px`, auth error/status объявляется через `aria-live`, а
browser evidence охватывает anonymous, auth-required и signed-in/result states.

### v23 donor correction and query-learning pages

Самостоятельный Search UI из calendar `v22` superseded: arrow внутри input,
fake Yandex/email states и маленькие bespoke result rows не переносятся дальше.
Канонический mobile donor — визуальный v58 (`abbcf7a13d…`) и актуальная Search
revision `2ef8dd834d…`. Текущий Astro `AuthorizedEventSearch` сохраняет отдельный
full-width submit под input; `::before` отображает `--search-progress`, а
результаты строятся только общим runtime `EventCard`. Yandex/Supabase PKCE,
session restore и NDJSON/vector-first не переписываются. Автоматический
stalled-stream JSON rescue superseded: cost-bearing Search POST отправляется
ровно один раз по заранее выбранному доступному маршруту. Email не является частью этого donor и не показывается.

Ниже поиска располагается тихая секция `Готовые подборки` с полными живыми
фразами, которые одновременно учат формулировать запрос. Централизованно
нормализованный и одобренный запрос с materialized result set — обычная
crawlable ссылка на регулярно обновляемую static page; просмотр такой страницы
не расходует online embedding/LLM quota. Если static page ещё не создана,
пример может только подставить текст в input без auto-submit, network request
или вымышленного URL. Личные сохранённые поиски — отдельная auth-only секция и
не называются публичными подборками.

Mobile gates: input и progress-submit остаются раздельными; submit получает
`aria-busy` и видимый percent/progress; live/mocked results используют крупный
canonical `EventCard`; materialized links реально открывают static pages;
fill-only example не навигирует и не отправляет форму; на `320/390px` отсутствует
horizontal overflow.

В noindex mobile research preview страницы `/poisk/` и `/podborki/*` монтируют
один `MobileSearchBottomNav`: `Афиша / Даты / Поиск / Для меня`, где `Поиск`
активен. Он не переписывает `AuthorizedEventSearch`; на mobile скрывается только
конфликтующий горизонтальный `.site-nav`, а существующая brand/top-sheet
механика остаётся. Между раздельными calendar/Search preview используются
`PUBLIC_MOBILE_CALENDAR_BASE_URL` и `PUBLIC_MOBILE_SEARCH_BASE_URL`; без них
компонент возвращается к обычным `withBase(...)` links.

Research materialization может получить явную дату среза через
`PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE`. Страница обязана одновременно
показывать дату обновления source catalog и дату, на которую рассчитана
подборка: это не разрешение маскировать старые данные. Перед пользовательской
передачей относительный запрос вроде `ближайшие выходные` не может содержать
прошедший weekend. В v23 public preview срез `2026-07-21` даёт карточки 25–26
июля. Research routes остаются `noindex`; production crawlable materialization
разрешается только после подключения регулярно обновляемого canonical job.

### v24 runtime crop and progress contract

Search-result cards use one shared **mobile large-card** media resolver; they
are not one-column desktop recommendation rows. `visual_only` media receives a
stable horizontal `5:4` frame (`width / height = 1.25`) with focal-aware
`cover`. This is the product's earlier “4:5 horizontal” convention expressed
unambiguously in CSS aspect-ratio order. OCR/document media is never cropped:
known dimensions reserve the intrinsic ratio, while a missing snapshot first
reserves a poster-like `4:5` skeleton and is reconciled from decoded
`naturalWidth / naturalHeight`. This removes
artificial top/bottom fields without cutting poster text. Search maps ranked
items directly through `resolveMobileEventCardMedia`, so result order stays
authoritative and no related-grid coordinates enter the linear DOM. Static
materialized Search collections opt into the same resolver.

The Supabase search snapshot contract is `event-card-v3-media-layout` and
includes `image_media_role`, `image_width`, `image_height` and `focal_y`.
Known `visual_only` media is focal-aware `cover`. OCR and unknown text mode
remain fail-closed `contain`; missing dimensions are not invented as crop
evidence, but decoded browser geometry may replace the provisional skeleton
ratio. The exporter and browser renderer must be upgraded together; a newer
renderer may not invent semantic crop permission. The desktop related-card
optimizer and its `6408` crop gates remain unchanged and surface-specific.

The backend vector-sync verification must inspect the whole requested corpus
even when `--max-provider-calls 0`. That value forbids new Gemini calls; it does
not permit an early exit after the first document. A zero-call
`--require-complete` audit is green only when every requested document kind has
an existing embedding with the current text hash.

The static Search release uses the dedicated vector owner's durable
`event_vector_sync_receipt_v2`, not a second vector write inside Kaggle. The
receipt binds `catalog_revision`, `corpus_revision`,
`search_document_revision`, the two document-kind corpus hashes and complete
projection coverage. An authorized secret candidate receives this non-secret
receipt through its private immutable input dataset and fails closed unless its
exported catalog revision matches. Production therefore keeps
`STATIC_SITE_SYNC_PGVECTOR_VECTORS=0` while still publishing exact Search
revision evidence.

Search progress has a single owner: backend NDJSON stages. The old client-side
28/55/74/92% timers are removed. Until the first frame the adjacent semantic
progressbar is indeterminate; after that its value and stage rank are monotonic,
and `result` completes at 100%. Every request owns an epoch, `AbortController`
and completion-reset timer: stale chunks or a previous run's delayed reset
cannot mutate the current button. The submit remains a button with
`aria-busy`; progress semantics live on a separate referenced
`role="progressbar"`. Reduced-motion disables cosmetic interpolation without
removing state feedback. Search/auth/quota explanations remain inline because
they are contextual form state, not transient global toasts.

The editorial fill-only examples include the compact intent `послушать хор`.
Like the other rows marked `пример`, it only fills the input and never submits
the query without an explicit press on `Искать`.

Mobile chrome and transient-message ownership are defined once in
[`mobile-shell.md`](../static-site-pages/mobile-shell.md); admission/audience
queries and the explicit decision not to ship an unverified child medallion are
canonical in
[`audience-admission-discovery.md`](audience-admission-discovery.md).

### v25 large-card loading and result-endcap contract

Mobile Search renders the same canonical large `EventCard` / `split-actions`
component as `Смотрите дальше`; compact calendar rails and copied
`.event-row` markup are not valid Search results. The shared mobile resolver
remains authoritative: `visual_only` media reserves a horizontal `5:4` cover
with focal protection, while OCR/document media keeps its intrinsic ratio and
`contain` treatment.

An initial request immediately shows a structural large-card skeleton: two
full cards and a short preview of the third, each with a `5:4` media slot,
text lines and split-action placeholders. Provisional vector candidates update
only the backend-owned monotonic progress/status; they do not replace the
skeleton or reshuffle temporary cards. The visible progress fill lives inside
the submit button. A visually hidden adjacent `role=progressbar` retains the
same monotonic values and labels for assistive technology, so there is still
one progress owner and one visible progress surface.

Result order is fixed:

1. `Результаты поиска` and the exact `items` pages;
2. while `has_more=true`, only `Показать ещё` is added — fallback is buffered;
3. after exact exhaustion, `Нашли то, что искали?` with `Да, нашёл` →
   `matched` and `Нет, не нашёл` → `missed`;
4. only then `Ещё можно посмотреть` and deduplicated `fallback_items`.

If exact `items` are empty, the endcap begins with
`По вашему запросу ничего не найдено`, still records the explicit user verdict,
and may continue into honest discovery. Generic `fallback_items` are never
called personalized: `По вашим интересам` is reserved for a separately sourced
personal-feed response whose mode actually confirms personalization.

### v26 unified mobile-shell integration

The v25 Search sequence and card renderer remain unchanged. The correction is
outside Search: `EventLayout` now mounts the accepted reference-4 v13 mobile
menu as `Reference4MobileMenu.astro`, suppresses the legacy desktop footer on
mobile navigation surfaces, and composes the review prefix with the accepted
v23 Calendar/Popular donor. This prevents a functional Search page from
visually reverting as soon as the user opens the header or follows the bottom
dock. Canonical geometry, routes and assembly/gate evidence are documented in
[`mobile-shell.md`](../static-site-pages/mobile-shell.md#astro-integration-and-unified-searchcalendar-preview-v14-2026-07-22).

### v27 standalone Search intent field correction

The unified shell had ported the real Search runtime but not the accepted
standalone field composition from mobile-shell unification v2. That omission
made `/poisk/` look like the earlier compact/card form even though results,
skeletons and backend progress were current.

On mobile standalone Search the accepted contract is now restored in the real
`AuthorizedEventSearch` component:

- a flat page canvas rather than a rounded white form card;
- heading `Найти событие` and visible label `Что хочется сделать?`;
- a three-row, `82px` minimum-height `textarea` with the project font and a
  strong bottom rule;
- the existing full-width submit with visible backend-owned progress **inside**
  the dark button;
- normal document flow into the existing large-card skeleton, exact results,
  feedback and discovery endcap.

This is a presentation correction, not a second Search implementation.
Yandex/Supabase PKCE, session restore, the NDJSON stream, monotonic progress,
request epochs, skeleton and canonical large result cards remain unchanged.
Fill-only query examples accept both the standalone `textarea` and the compact
embedded `input`; they still never auto-submit.

### v28 visible in-button progress correction

The progress geometry and backend-owned NDJSON stages were working in v27, but
the standalone button's fill was effectively invisible: translucent near-black
was painted over a near-black button (about `1.01:1` contrast). In addition, the
indeterminate `36%` segment spent part of its cycle completely outside the
button, so a screenshot—or a user's glance—could show no progress at all.

The mobile standalone button now uses the existing opaque shell terracotta
`#98401f` for its `::before` fill. Its indeterminate travel stays at least
partially within the clipped button (`-70%` to `180%`); determinate widths remain
monotonic and owned only by streamed backend stages. There is no percentage
label and no second visible progress bar: the submit button itself is the one
progress surface, with the separate visually hidden `role=progressbar` retained
for assistive technology.

## Search feedback and public tag candidates

The dedicated `/poisk/` page now exposes seed query chips and, after enough
cards are rendered, asks the signed-in user whether the results matched their
intent. The browser always writes a small local fallback item to
`ke_search_feedback_queue_v1`; when the Supabase session is available it also
calls `record_event_search_feedback_v1(...)`.

Migration `20260701_event_search_feedback_tags.sql` adds private
`event_search_feedback` rows and aggregated `event_search_tag_candidates`.
Raw feedback stays server-side: browser roles do not get table access, RLS is
enabled, and the only browser grant is authenticated execute on the compact RPC.
Positive feedback increments a candidate query hash; a later LLM/moderation job
must canonicalize, merge and approve tags before a public static page is built.

## P1 product idea: saved search as public tag page

Registered users should be able to save a successful search as a compact public tag candidate:

1. After useful results, the UI offers **«Сохранить как тег»**.
2. The service thanks the user and adds the candidate to their visible saved-search/tag list.
3. A background LLM curation job reviews the global candidate-tag pool with the smartest available reviewer lane, not the fast runtime verifier:
   - merge near-duplicates and strong overlaps;
   - keep names short, ёмкие and human-readable;
   - reject private/overly narrow/noisy wording;
   - preserve raw user phrasing only as private audit, not public page copy.
4. Accepted tags become non-individual static pages, for example `/t/dzhaz-na-vyhodnyh/` or `/tag/detyam-v-vyhodnye/`.
5. The tag job runs the same vector + Gemma/LLM verification pipeline deeply enough to produce several dozen ordered cards. The public page is then rebuilt regularly and is available to anonymous users without spending per-view embedding/LLM quota.
6. Personalization on a tag page is mostly subtractive/ordering: hide `Не интересно`, demote negative interests, optionally top-up from the already materialized tag result set. It must not call LLM on ordinary page views.

This is intentionally separate from per-user saved searches. The public tag page exists only after curation accepts the phrase as broadly useful.

## Auth design: Yandex through Supabase custom OAuth

Supabase has no built-in Yandex provider in the social-login list, so the project uses **Custom OAuth/OIDC Providers** with identifier `custom:yandex`.

Configured in the personalization Supabase project on 2026-06-29. Manual/Dashboard setup contract:

1. Auth → Providers → New Provider → Manual configuration.
2. Identifier: `custom:yandex`.
3. Type: OAuth2 unless Yandex OIDC discovery is configured separately.
4. Client ID / Client Secret: from the Yandex OAuth application.
5. Authorization URL: `https://oauth.yandex.ru/authorize`.
6. Token URL: `https://oauth.yandex.ru/token`.
7. UserInfo URL: `https://<project-ref>.supabase.co/functions/v1/yandex-userinfo`, not direct Yandex JSON.
8. Scopes: `login:email login:info` (adjust if Yandex app requires a different minimal set).
9. `email_optional=true`: email is useful if Yandex returns it, but the product needs a stable authenticated Yandex identity first.
10. Add Supabase callback URL shown by the provider form to the Yandex app redirect URLs.
11. Add site redirect URLs such as `https://kenigevents.ru/*` and current preview prefixes to Supabase Auth URL allow-list.

Frontend uses Supabase Auth PKCE, not implicit-hash parsing. The page is static HTML, so all auth work is done by browser JavaScript against Supabase Auth: login calls `/auth/v1/authorize`, callback handling exchanges the one-time `code` through `/auth/v1/token?grant_type=pkce`, then authenticated search calls the `event-search` Edge Function. On login it sends the cleaned current URL as `redirectTo` (same page, without stale `code/error/state` params); on return it explicitly calls `exchangeCodeForSession(code)`, persists the session, cleans the callback URL with `history.replaceState`, and then unlocks the search form. This prevents the UX regression where the browser returned to `/poisk/` but the page still looked anonymous because the OAuth `code` had not been exchanged into a Supabase session.

Static PKCE hardening in v52:

- the Supabase client still uses browser-side `flowType: 'pkce'`;
- the short-lived PKCE `*-code-verifier` is stored by Supabase in browser storage and mirrored by our custom storage adapter into a `Secure; SameSite=Lax; Path=/; Max-Age=900` cookie on `kenigevents.ru`; this is only for the one-time verifier, not for access/refresh tokens;
- after `exchangeCodeForSession(code)` returns a session, the UI explicitly calls `setSession(...)` as a belt-and-suspenders write before rendering the authorized state;
- callback errors are no longer overwritten by the initial `onAuthStateChange(null)` emission; if the code verifier is missing/expired, the user sees a clear retry message instead of the plain login button.

```ts
const supabase = createClient(url, publishableKey, {
  auth: {
    flowType: "pkce",
    detectSessionInUrl: false,
    persistSession: true,
    autoRefreshToken: true,
  },
});

await supabase.auth.signInWithOAuth({
  provider: "custom:yandex",
  options: { redirectTo: cleanAuthRedirectUrl() },
});

// On return to the same page:
await supabase.auth.exchangeCodeForSession(code);
```

As of 2026-06-29 the local/private environment contains the Yandex client credentials and the Supabase provider `custom:yandex` is configured. These secrets are not committed; readiness is checked by `scripts/check_authorized_search_readiness.py --probe-yandex-provider`.

### Yandex userinfo adapter

Direct `https://login.yandex.ru/info?format=json` is **not** a compatible Supabase custom OAuth2 userinfo endpoint for our flow. Yandex JSON returns non-standard keys such as `id` and `default_email`, while Supabase Auth's generic OAuth2 provider expects OIDC-like claims, especially `sub` for provider identity and `email` when email is not optional. The observed callback URL was:

```text
?error=server_error&error_code=unexpected_failure&error_description=Error+getting+user+email+from+external+provider
```

Implemented adapter: `supabase/functions/yandex-userinfo/index.ts`, deployed with `--no-verify-jwt` because it is called server-to-server by Supabase Auth with the Yandex access token. The adapter:

- accepts `Authorization: Bearer <token>` or `Authorization: OAuth <token>` from Supabase Auth;
- calls Yandex with `Authorization: OAuth <token>` and a Bearer fallback;
- maps Yandex JSON to Supabase/OIDC-like JSON: `id -> sub`, `default_email/emails[0] -> email`, `real_name/display_name/login -> name`, plus optional avatar/name fields;
- returns `Cache-Control: no-store` and never logs or returns the OAuth token;
- rejects missing tokens with `401 {"error":"missing_yandex_token"}` for readiness smoke.

Current production-like provider config on 2026-06-29:

- `custom:yandex.userinfo_url = https://epyznmylqmchteykjsqj.supabase.co/functions/v1/yandex-userinfo`;
- `email_optional = true`;
- scopes still include `login:email` and `login:info`.

Regression guard: `scripts/check_authorized_search_readiness.py --probe-yandex-userinfo-adapter` fetches the live custom provider config and checks both the adapter URL and the adapter's missing-token 401 smoke.

## Retrieval architecture

### Data flow

```text
Fly SQLite / static export
  -> scripts/sync_event_search_vectors_to_supabase.py
  -> Google embedding for each public event document
  -> event_search_documents + event_embeddings(vector(768)) in personalization Supabase
  -> authenticated Edge Function event-search
  -> Google embedding for the user's query
  -> RPC search_events_by_embedding_v1
  -> pgvector HNSW/cosine recall over stored event vectors
  -> Gemini Lite verifier first, Gemma 4 26B only as overflow
  -> event-card snapshots in browser
```

Important implementation fact: pgvector is the Postgres vector index/search
engine; it does **not** create semantic vectors by itself. In the current P0
implementation, vectors are created by `gemini-embedding-2`: offline for event
documents in `scripts/sync_event_search_vectors_to_supabase.py`, and online once
per explicit authenticated query in `supabase/functions/event-search/index.ts`.
The Edge Function passes that query vector to `search_events_by_embedding_v1` as
`p_query_embedding`; the RPC orders candidates by `event_embeddings.embedding <=>
p_query_embedding`. Gemini Lite/Gemma do not replace this recall step: they
classify the pgvector candidate IDs after recall.

Postgres-native text search is a different tool. `to_tsvector`/`tsquery` can
build a lexical full-text index over titles/digests, and `pg_trgm` can help with
fuzzy string matching, but neither creates the semantic 768-dimensional vector
that pgvector compares. The 2026-07-01 Supabase extension check found `vector`
installed and `pg_trgm` available, but no installed/available `pgai`,
`pg_vectorize` or `vectorize` extension in this personalization project. A quick
A/B probe over the current public/searchable catalogue showed why we should not
remove `gemini-embedding-2` without a golden-quality replacement: lexical FTS
returned `0` rows for `интересно детям`, only the exact-title event for
`урбанистика будущее города`, and only one literal jazz title for
`джаз на выходных`, while the current semantic vector recall returned relevant
families such as children/family events, urban-planning events and jazz-related
events for the same natural-language queries. FTS/trigram may become a cheap
hybrid prefilter or fallback, but not a no-quality-loss replacement until a
proper golden-query evaluation proves parity.

### pgvector schema

Migrations: `supabase/migrations/20260628_event_search_pgvector.sql` plus hardening migrations
`20260628_event_search_weekday_and_related_rpc.sql`,
`20260628_event_search_public_fields_and_model_filter.sql`,
`20260629_event_search_query_facets.sql` and
`20260630_event_search_embedding_doc_kind.sql`, plus
`20260701_event_search_feedback_tags.sql` for feedback/tag-candidate intake and
`20260731174313_harden_event_search_internal_rpc.sql` for the service-only
execution boundary, idempotent quota reservations and capped feedback.

Tables:

- `public.event_search_documents` — compact factual `search_digest`, cleaner `related_digest`, controlled facets and trusted `card_snapshot`; no raw OCR/source text;
- `public.event_embeddings` — `gemini-embedding-2` vectors, `vector(768)`, `embedding_doc_kind`, partial HNSW cosine indexes for `search_v3` and `related_v1`;
- `public.search_quota_plans` — default registered quota plan, including active hourly limits;
- `public.user_search_quota_hourly_ledger` — active one-hour cooling-window counters per Supabase user;
- `public.user_search_quota_ledger` — day counters per Supabase user; legacy month rows may exist only for compatibility;
- `public.event_search_result_cache` — private service-role short-lived result cache keyed by salted query hash and result-shaping signature;
- `public.event_search_requests` — audit log with query hash and length only, no raw query text.
- `public.event_search_feedback` — private authenticated feedback rows with raw query text for moderation only;
- `public.event_search_tag_candidates` — private aggregated candidate tags keyed by query hash.
- `public.event_search_quota_operation` — private 48-hour per-user operation
  ledger; one `(user_id, client_request_id)` consumes quota at most once.

RPCs:

- `search_events_by_embedding_v1(...)` and
  `event_search_fallback_cards_v1(...)` — service-role-only primitives; the
  authenticated Edge Function reaches them through wrappers bound to the
  already verified `auth.users.id`;
- `get_event_search_quota_v2(...)` — visible quota state with hourly/daily remaining counts and hour reset time;
- `reserve_event_search_quota_v3(...)` — service-role-only atomic primitive;
  `reserve_event_search_quota_internal_v1(...)` adds a verified user id and
  idempotent client request id before provider calls; cached results do not
  reserve quota;
- `get_event_search_result_cache_v1(...)` / `upsert_event_search_result_cache_v1(...)` / `purge_event_search_result_cache_v1(...)` — private service-role result-cache operations;
- `record_event_search_request_v1(...)` — service-role-only compact audit,
  reached through the verified-user internal wrapper.
- `record_event_search_feedback_v1(...)` — the remaining authenticated write
  API: owner-scoped operation-id dedupe, 30 rows/user/hour, 90-day raw-feedback
  retention, at most 40 distinct event IDs and an allowlisted compact metadata
  shape; raw feedback tables remain unreadable to browser roles.

Direct browser `select` on raw tables and execution of vector/quota-reservation/
audit primitives are forbidden by grants/RLS. The Edge Function first validates
the caller with its user-scoped client, only then constructs its internal
service client. It accepts an optional UUID `client_request_id` (body or
`X-Client-Request-Id`), echoes it, and otherwise generates one for compatibility.
The request body is rejected above 16 KiB before parsing. The service key is
never returned to or embedded in the browser.

### Embedding model

Accepted P0 model: `gemini-embedding-2`, `outputDimensionality=768`.

Reasons:

- dimension fits pgvector’s ordinary vector index budget with room below pgvector’s common vector-size limits;
- current Google AI Studio quota includes `Gemini Embedding 2` (`100 RPM / 30K TPM / 1K RPD` from the project quota screen);
- one event catalogue backfill of tens/hundreds of future events is feasible;
- query embedding is only on explicit authenticated search, never on ordinary page view.

Google Embedding 2 does **not** use the `taskType` field; task intent is included in text:

```text
Document: title: {title} | text: {search_digest}
Query:    task: search result | query: {user_query}
```

Authorized search uses only `embedding_doc_kind=search_v3`. Static event-page related generation uses `related_v1`; the Edge Function passes `p_embedding_doc_kind` to the RPC and defaults to `search_v3` so a future related-vector backfill cannot pollute user search results.

Raw poster OCR is not embedded or sent to the online `/poisk/` verifier. OCR can affect authorized search only after Smart Update promotes a source-grounded poster fact into canonical public event fields such as `title`, `search_digest`, venue/address, ticket status or topics. This boundary is intentional because posters often contain commercial venue/partner labels that would otherwise over-weight unrelated events in vector recall.

## LLM verifier

The Edge Function runs an LLM verifier after pgvector retrieval when the user has LLM quota. This verifier is an operational classifier over already retrieved IDs, not an external consultant review. Runtime contract:

- `EVENT_SEARCH_LLM_ENABLED=1` enables the verifier;
- primary online verifier is fast Gemini Lite (`EVENT_SEARCH_LLM_LITE_MODEL=gemini-3.1-flash-lite`), because it can classify the compact candidate batch in about a second and has enough effective quota after KEY5 rotation;
- overflow verifier is Gemma 4 26B (`EVENT_SEARCH_LLM_GEMMA_OVERFLOW_MODEL=gemma-4-26b-a4b-it`), used only if all configured Lite key lanes fail with quota/capacity/retryable provider errors or Lite returns an unusable classification;
- legacy `EVENT_SEARCH_LLM_MODEL` is no longer used as the primary model in the Lite-first strategy; if present, it is treated only as an overflow fallback source after the explicit `EVENT_SEARCH_LLM_GEMMA_OVERFLOW_*` envs;
- `EVENT_SEARCH_VERIFICATION_WINDOW=10` remains the backend default, and the public static `/poisk/` page requests `limit=8, candidate_window=10, use_llm_verifier=true, allow_llm_fallback=false`; Gemini Lite stays enabled, but the browser path is bounded by compact facts, adaptive no-recursion shrink `10→5→3`, a 4.3s Lite budget and vector-first streaming so users see pgvector cards before verifier completion;
- model policy is `lite_first_gemma_overflow`: try Gemini Lite first across all configured Google key lanes, then Gemma 4 26B overflow.

Operational knobs: `EVENT_SEARCH_LLM_LITE_MODEL(S)`, `EVENT_SEARCH_LLM_LITE_ATTEMPTS`, `EVENT_SEARCH_LLM_LITE_TIMEOUT_MS`, `EVENT_SEARCH_LLM_LITE_RETRY_BACKOFF_MS`, `EVENT_SEARCH_LLM_GEMMA_OVERFLOW_MODEL(S)`, `EVENT_SEARCH_LLM_GEMMA_OVERFLOW_TIMEOUT_MS`, `EVENT_SEARCH_LLM_GEMMA_OVERFLOW_ENABLED`.
Prompt/latency knobs: `EVENT_SEARCH_LLM_MAX_OUTPUT_TOKENS` (default `384`),
`EVENT_SEARCH_LLM_THINKING_LEVEL` (default `MINIMAL`),
`EVENT_SEARCH_LLM_FACT_MAX_CHARS` (default `180`),
`EVENT_SEARCH_LLM_LITE_CANDIDATE_COUNTS` (defaults to the adaptive public shrink profile `10,5,3` when `candidate_window=10`),
`EVENT_SEARCH_LLM_LITE_TIMEOUT_PROFILE_MS` (default `2600,1200,700`),
`EVENT_SEARCH_LLM_LITE_TOTAL_BUDGET_MS` (default `4300`),
`EVENT_SEARCH_LLM_FALLBACK_CANDIDATE_COUNTS` (default `2`) and
`EVENT_SEARCH_LLM_MAX_CANDIDATES` (upper safety cap, default `20`).

High-match contract:

1. pgvector returns a bounded candidate window from `gemini-embedding-2` vectors.
2. The LLM receives candidate IDs + compact facts from `search_digest`; it returns exactly three buckets: `exact_event_ids`, `possible_event_ids`, `rejected_event_ids` plus `query_interpretation`.
3. Only `exact_event_ids` are rendered under **«Результаты поиска»** (`items`).
4. Weak/uncertain matches are rendered only under **«Возможно, вам будет интересно»** (`fallback_items`).
5. The Edge Function emits a `vector_results` NDJSON event immediately after pgvector recall; those cards are provisional but real results, not skeletons.
6. If the LLM verifier succeeds within budget, only `exact_event_ids` remain under **«Результаты поиска»** and weak matches stay in fallback. If the verifier times out/fails, the terminal response keeps the bounded pgvector window as `items` so users are not left at 92% with no cards.
7. The public first page is deliberately bounded (`candidate_window=10`, `limit=8`) and Lite retries shrink the prompt `10→5→3` without recursion. “Показать ещё” requests the next vector offset instead of sending one heavy 20–40 candidate prompt.

P1 progressive UX contract:

- the public path streams the bounded pgvector window first, then lets the online verifier continue within the Lite budget;
- provisional vector cards are visually marked by the search-status copy and may be replaced by the verified final list only when the terminal `result` arrives;
- if Gemini Lite does not answer inside the bounded budget, the final response remains vector-backed instead of blank;
- if unverified vector candidates and verified candidates are mixed in one UI, the provisional state must be explicit and must not suddenly move or remove the card the user is currently interacting with;
- a future server-side `search_session_id`/cursor can cache verified result sets so repeated “Показать ещё” calls reuse one classification job instead of creating inconsistent independent LLM pages.

The verifier uses Gemini structured output (`responseMimeType: application/json` + `responseJsonSchema`) and still post-validates IDs against the retrieved candidate map. Broad queries can legitimately return many exact matches, so the previous default “over-approval” demotion is disabled by default; if a future incident proves rubber-stamping, it can be enabled explicitly with `EVENT_SEARCH_LLM_OVER_APPROVAL_DEMOTE_ENABLED=1` and a high `EVENT_SEARCH_LLM_OVER_APPROVAL_RATIO`.
Every provider try is recorded in `llm_verifier.attempts[]` and search metadata
with `{model, role: primary|fallback, attempt, status, elapsed_ms}`. The response
also exposes `llm_verifier.model`, `llm_verifier.policy` and
`llm_verifier.gemma_overflow_allowed` so product/debug review can see whether
Gemma overflow was available after the Lite-first attempt. If all attempts fail on the optional verifier path, the response falls back to bounded pgvector candidates rather than blocking the public SLA path.
For Lite/Gemma latency analysis, each attempt also records
`timeout_ms`, `prompt_chars`, `prompt_fact_chars` and
`compact_candidate_count`. A direct SQL probe can summarize the history:

```sql
select
  created_at,
  attempt->>'model' as model,
  attempt->>'role' as role,
  (attempt->>'attempt')::int as attempt_no,
  attempt->>'status' as status,
  (attempt->>'elapsed_ms')::int as elapsed_ms,
  (attempt->>'timeout_ms')::int as timeout_ms,
  (attempt->>'prompt_chars')::int as prompt_chars,
  (attempt->>'prompt_fact_chars')::int as prompt_fact_chars,
  metadata->>'llm_policy' as llm_policy
from public.event_search_requests
cross join lateral jsonb_array_elements(metadata->'llm_attempts') as attempt
where attempt->>'model' in ('gemini-3.1-flash-lite', 'gemma-4-26b-a4b-it')
order by created_at desc;
```

2026-06-29 live evidence after high-match hardening:

- `Концерт классической музыки`: exact `5201 Концерт «Фестиваль Pianissimo: Константин Емельянов»`, 3 possible, LLM stage ≈1.0s.
- `Чтобы было интересно детям`: exact `4512 С чего начинается Родина`, 3 possible, urban-planning events no longer appear as exact results.
- `джаз на выходных`: 0 exact, 4 possible; with the current limited corpus this is preferable to showing non-jazz music as exact.
- NDJSON/progress path emits backend stages: `auth`, `validate`, `quota`, `embedding`, `vector_search`, optional `llm_verify`, `finalize`, `result`.
- Historical 2026-06-29/2026-07-01 audit rows prove why the KEY5 branch switches
  the runtime order: the deployed Gemma-first cascade often spends `3.3–4.9s` on
  Gemma, and one late path spent about `25.6s` before Lite returned `ok` in
  about `1.1s`. The branch corrects this to Lite-first and keeps Gemma only as
  slower overflow.

Consultant traceability:

- Gemini Pro review was attempted only on allowed Pro models (`gemini-3.1-pro-preview`, `gemini-3-pro-preview`) and was blocked by `429 RESOURCE_EXHAUSTED`; evidence is stored in `artifacts/codex/search-consultants-20260629*/gemini-*-error.txt` and is not treated as completed Gemini review.
- `a-opus` reviewed the implementation twice. The first review found the fatal raw-vector-as-exact fallback; the second high-match review accepted the fail-closed architecture and flagged the now-fixed model fallback and over-approval guard, with remaining P1s around dedicated provider quota/key lane, prompt-injection hardening of candidate text, unreserving failed LLM quota, and proper verified-window pagination. Artifacts: `artifacts/codex/search-consultants-20260629/a-opus-review.md` and `artifacts/codex/search-consultants-20260629-high-match/a-opus-full-review.md`.

## Query facets

The event documents embed weekday/time/admission fields in the deterministic search text. In addition, the Edge Function extracts a very small set of explicit query facets so words like “пятница”, “вечером”, “утром”, “бесплатно” or “по регистрации” can improve ordering without introducing a separate keyword-search path:

- weekday: ISO `1..7` plus Russian weekday label for logs/metadata;
- time of day: `morning`, `day`, `evening`, `night`;
- admission: `free`, `registration_required`, `paid`.

The facets are not used to store raw query text. They are passed to `search_events_by_embedding_v1` and written only as compact metadata in Edge logs / audit rows. The RPC first asks pgvector for the nearest semantic candidates and only then applies a bounded boost (`weekday` > `admission` > `time_of_day`); therefore a facet cannot create events outside the trusted `card_snapshot` catalogue and cannot replace semantic retrieval with broad deterministic filtering.


### 2026-07-02 live batch-size probe and bounded verifier path

Incident `INC-2026-07-02-static-search-92-percent-no-cards` showed that the previous 20-candidate online verifier window could leave the browser at the synthetic `92%` progress state while Lite lanes timed out. Live probes compared larger windows, smaller batches and compact 2-candidate prompts; they showed that prompt size was only part of the problem, because Gemini Lite can still timeout while Gemma overflow can add a 9s+ tail.

The accepted public contract is therefore not “disable LLM”, but “stream vector first and cap the verifier”. The frontend uses NDJSON streaming and requests `limit=8`, `candidate_window=10`, `use_llm_verifier=true`, `allow_llm_fallback=false`. The Edge Function emits `vector_results` immediately after the pgvector RPC, then tries Gemini Lite with compact facts and no-recursion shrink `10→5→3` inside the 4.3s Lite budget. If the verifier succeeds, final cards are the LLM-confirmed `exact_event_ids`; if it fails or times out, final cards remain the bounded vector candidates rather than an empty answer.

The client-side SLA is bounded and honest. Already delivered vector cards remain visible, but a header or stream stall must not issue a second cost-bearing request: the first request may already be executing and consuming quota. The shared transport selects one healthy direct/relay route before submit, sends the Search POST once, cancels a stalled response and returns the form to an explicit retryable error state. A retry is always a new user action. “Показать ещё” stays visible as a disabled `Загружаю ещё…` control during the next batch instead of disappearing silently. Search feedback remains optimistic but its local fallback queue is compact, capped and expiring.


### Query embedding cache

The online search Edge Function caches query embeddings by a salted hash, not by raw query text. The stable audit `query_hash` remains unchanged, while the embedding cache uses `sha256(EVENT_SEARCH_QUERY_HASH_SALT + normalized_query)` with a non-secret fallback salt for non-production development. Cache rows are keyed by `(query_hash, embedding_model, embedding_dim)` and store only the 768-dimensional vector plus small metadata/hit counters. Direct `anon`/`authenticated` access is revoked; the Edge Function reads/writes through service-role RPCs.

On every search the Edge Function first calls `get_event_search_query_embedding_v1`; on hit it skips the Google embedding request and reports `embedding_cache_status=hit`. On miss it calls Gemini Embedding 2, then `upsert_event_search_query_embedding_v1`, and reports `miss` or `store_failed`. This saves repeated-query latency and embedding quota without storing the user's search phrase.

### Short-lived result cache

Repeated fully answered searches can be served from a short-lived result cache before quota reservation, embedding provider calls or LLM verification. The cache key uses the same salted query hash family as the query-embedding cache and also includes all result-shaping dimensions: embedding model, embedding document kind, current search date, limit, offset, verifier window, parsed facets, fallback flag and LLM policy signature. The cache stores only public result payload JSON and small metadata; it never stores raw query text and is not directly readable by `anon` or `authenticated`.

The active TTL is intentionally short (`EVENT_SEARCH_RESULT_CACHE_TTL_SECONDS`, capped to `1 minute..6 hours`, default `3 hours`). A cache hit returns `result_cache_status=hit`, `served_from_cache=true` and does **not** consume the one-hour search window. A miss stores only successful full-quality responses (`llm_verifier.used=true`, or explicitly vector-only calls when LLM is disabled), so temporary LLM-quota fallback does not poison later verified searches.

To avoid stale result payloads occupying the database after event changes, `event_search_result_cache` is physically cleared by statement-level triggers on `event_search_documents` and `event_embeddings`. In practice the recent vector-sidecar history has long quiet windows between rebuild bursts: the last seven days showed quiet gaps of about `27h40m`, `20h04m`, `16h49m`, `8h54m` and `7h08m` between indexed/vector update minute buckets; core event ingestion also had multiple `4–12h` gaps in the same period. A several-hour cache therefore protects against temporary visitor bursts while still being invalidated on the next event/vector corpus update.

## Quotas and privacy

Registered plan is dynamic, not a fixed “tiny” per-user constant. Migration
`20260629_event_search_quota_plan_dynamic.sql` added service-role RPC
`refresh_registered_search_quota_v1(...)`, which recalculates
`search_quota_plans.registered` from effective product registrations, provider
RPD inputs, reserves for non-search workloads and per-user abuse caps. Migration
`20260701180316_event_search_key5_quota_capacity.sql` updates the default inputs
after `GOOGLE_API_KEY5` was added and smoke-tested for query embedding, Gemini
Lite verification and Gemma overflow. It also fixes the user-count basis: the
site product registration count is the number of distinct `auth.users` with a
`custom:yandex` identity, not every historical `auth.users` row. On 2026-07-01
Supabase Auth had `47` rows, but only `1` `custom:yandex` identity; the other
`46` rows were email/test/smoke users and must not dilute the product quota.

2026-07-01 quota calculation for smart search uses effective Yandex-registered
site users as the unit of allocation and keeps only the guide fixed lane out of
the normal LLM verifier budget:

| Component                                           |         Provider/day fact |                                                                                          Active key lanes |                     Gross RPD |                                                                                    Protected reserve | Online search RPD |
| --------------------------------------------------- | ------------------------: | --------------------------------------------------------------------------------------------------------: | ----------------------------: | ---------------------------------------------------------------------------------------------------: | ----------------: |
| Query embedding (`gemini-embedding-2`)              |            `1000` per key | all 5 lanes: `GOOGLE_API_KEY5`, `GOOGLE_API_KEY4`, `GOOGLE_API_KEY3`, `GOOGLE_API_KEY2`, `GOOGLE_API_KEY` |                        `5000` |                                           `1000` for static/vector backfills, diagnostics and bursts |            `4000` |
| Normal fast verifier pool (`gemini-3.1-flash-lite`) |   defensive `450` per key |         shared non-guide lanes: `GOOGLE_API_KEY5`, `GOOGLE_API_KEY4`, `GOOGLE_API_KEY3`, `GOOGLE_API_KEY` |                        `1800` | `800` for Smart Update, Telegram Monitoring/static overlap, emergency capacity and provider variance |            `1000` |
| Guide fixed / LLM reserve lane                      | mixed Lite/Gemma capacity |                                                                                         `GOOGLE_API_KEY2` | not counted into normal quota |                                                                      guide monitoring fixed-key path |     failover only |

For the current `1` effective registered site user, the normal shared search
pool can serve `1000` fast Lite-verified searches/day while query embedding has
`4000` searches/day after buffer. The applied safety/abuse ceiling is therefore
`1000` searches/day and `1000` LLM verifications/day per registered Yandex user.
The active product contract no longer exposes or enforces a monthly user-facing
limit: monthly columns remain only as legacy compatibility fields.

The live abuse-control surface is a one-hour cooling window implemented by
`get_event_search_quota_v2(...)` and `reserve_event_search_quota_v3(...)`.
The registered plan currently allows `60` searches/hour and `60` LLM verifier
reservations/hour, still bounded by the existing `1000/day` server-side budget.
When the hour is exhausted, the product copy tells the user to come back after
the hour window resets. If a request is served completely from result cache, it
does not increment the hourly or daily ledgers.
This keeps user-facing quota inside the shared non-guide Lite pool while leaving
substantial capacity for the other production services and preventing one user
from burning the full daily pool in one burst.

Dynamic formula after this fix:

```text
daily_search_limit = min(
  1000,
  max(10, floor(4000 / effective_yandex_users)),
  max(10, floor(1000 / effective_yandex_users))
)
daily_llm_rerank_limit = min(daily_search_limit, 1000)
```

If the active Lite pool is provider-degraded or exhausted, the Edge Function may
try the guide reserve lane as late failover, then Gemma 4 26B as slower verifier
overflow. That fallback protects availability but is not included in the normal
quota budget.

Reserve rationale:

- embedding is model-specific/new for online search, so it rotates across all
  five keys and still keeps `1000 RPD` for backfills/diagnostics;
- Lite verification rotates across the same shared non-guide style as other
  Google AI consumers instead of artificially pinning `/poisk/` to KEY5;
- `GOOGLE_API_KEY2` remains the fixed guide-monitoring lane and is LLM
  reserve/failover only for `/poisk/`;
- the `800 RPD` Lite buffer is intentionally large enough to leave room for
  Smart Update, Telegram Monitoring/static overlap, emergency bursts and uneven
  hash distribution.

The Edge Function supports direct multi-key rotation/failover for this site path
without exposing secrets to the browser:

- `EVENT_SEARCH_GOOGLE_KEY_ENVS` — shared comma-separated active list;
- `EVENT_SEARCH_EMBEDDING_KEY_ENVS` — embedding-specific active list; current
  live value uses all five keys;
- `EVENT_SEARCH_LLM_KEY_ENVS` — LLM-specific active list; current live value uses
  `GOOGLE_API_KEY5,GOOGLE_API_KEY4,GOOGLE_API_KEY3,GOOGLE_API_KEY`;
- `EVENT_SEARCH_LLM_RESERVE_KEY_ENVS` — LLM reserve/failover list; current live
  value is `GOOGLE_API_KEY2`;
- `EVENT_SEARCH_GOOGLE_RESERVE_KEY_ENVS` / `EVENT_SEARCH_EMBEDDING_RESERVE_KEY_ENVS`
  — optional shared/embedding reserve overrides, intentionally unused for the
  current embedding-all rotation.

The current query-embedding pool is all five Google keys. The current LLM active
pool is `GOOGLE_API_KEY5,GOOGLE_API_KEY4,GOOGLE_API_KEY3,GOOGLE_API_KEY`; the
function hash-rotates within that active group by query/model so spend is
balanced. `GOOGLE_API_KEY2` is appended only after every active LLM key fails
with quota/capacity/retryable provider errors. If no explicit key lists are
configured, the function defaults to the same capacity plan: all five keys for
embedding, four non-guide keys for normal Lite verification, and `GOOGLE_API_KEY2`
as LLM reserve/failover.

2026-07-01 live rollout evidence from branch
`feature/smart-search-quota-key5-site`:

- initial rollout SHA `4bc1b5b0` proved Lite-first behavior but still used one
  five-key rotating list; SHA `72c69421` fixed active-vs-reserve ordering;
- follow-up rollout restores the intended capacity model: embedding all keys,
  LLM active non-guide shared pool, guide key reserve only;
- quota migration `20260701180316_event_search_key5_quota_capacity.sql` is sized
  and applied live for that model: `auth.users=47`, effective `custom:yandex=1`,
  registered plan `1000/day`, `10000/month`, LLM verifier `1000/day`,
  `10000/month`;
- Edge Function `event-search` is deployed with `--no-verify-jwt` and the
  Lite-first code path; the runtime behavior is controlled by live secrets for
  active/reserve key lists;
- readiness probe covers auth config, Yandex provider, userinfo adapter and Edge
  OPTIONS; local runtime env contract must show embedding lanes all five keys,
  LLM active lanes `GOOGLE_API_KEY5,GOOGLE_API_KEY4,GOOGLE_API_KEY3,GOOGLE_API_KEY`
  and LLM reserve `GOOGLE_API_KEY2`;
- final live Edge smoke after the all-keys embedding/shared-LLM fix
  (`интересно детям`, JSON response) returned HTTP 200 in `2587ms` wall /
  `2306ms` backend, `retrieved_count=20`, `items=11`, `fallback_items=6`,
  `llm_model=gemini-3.1-flash-lite`, `policy=lite_first_gemma_overflow`,
  embedding key `GOOGLE_API_KEY5`, first LLM attempt key `GOOGLE_API_KEY3` `ok`
  in `1202ms`, quota `999/999` remaining; smoke auth user, quota ledger and
  audit rows were cleaned up after the run.

Search quota is reserved **before** Gemini embedding provider calls. The optional LLM verifier has a separate day/month quota; if that verifier quota is exhausted while ordinary search quota remains, the Edge Function must still answer, but in high-match mode it fails closed: exact `items=[]`, unverified pgvector candidates are placed in `fallback_items` with `llm_verifier.status=llm_quota_exhausted` and `llm_verifier.used=false`. Query text is never stored; only SHA-256 hash, length, result count and status are written to `event_search_requests`.

## Frontend integration

Component: `site/src/components/AuthorizedEventSearch.astro`.

Inserted on:

- `/poisk/` — dedicated search entry point linked from the mobile tag drawer / desktop nav / footer;
- `/__preview/`;
- `/segodnya/`;
- `/zavtra/`;
- `/vyhodnye/`.

Build-time public env required for the component to render:

```bash
PUBLIC_PERSONALIZATION_SUPABASE_URL=...
PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY=...
PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex
```

The component uses `@supabase/supabase-js` and invokes Supabase Edge Function `event-search`. If public env is missing, the dedicated page renders a disabled explanatory state instead of an inert-looking active form.

Kaggle StaticSiteBuilder handoff also accepts the same public values through
`--public-personalization-supabase-url`, `--public-personalization-supabase-publishable-key`
and `--public-yandex-auth-provider`. In production Smart Update handoff these are filled from
`STATIC_SITE_PUBLIC_*`, then `PUBLIC_*`, then the browser-safe personalization URL/publishable
key envs. Only URL + publishable key are exposed to Astro; Supabase secret/service keys remain
backend-only for vector sync and Edge Function deployment.

Production-root and secret-candidate builders normalize the same browser-safe
aliases through `preview-public-env.mjs` before Astro starts. They never copy a
secret/service-role key into the bundle. Release automation can set
`PRODUCTION_REQUIRE_AUTHORIZED_SEARCH=1` and
`SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH=1` to fail closed when the URL or
publishable key is absent; review candidates no longer blank an otherwise valid
public search configuration.

Search results are rendered through the same global `window.KenigEventsRenderEventCard`
renderer used by dynamic discovery/personal feeds. This is part of the acceptance
contract: cards returned by authorized search must keep detail-link navigation,
like/unlike, share, “не интересно” and calendar actions instead of using a separate
minimal search-result layout. The results container stores `request_id`,
`served_list_id`, `served_list_hash`, `algorithm_id` and `surface=authorized_event_search`;
feedback/share actions read that context so later investigation can connect strong
actions with the exact served search list.

Signed-in identity is shown as a compact avatar/account control, not as a large
always-visible logout button near the query field. The avatar uses Yandex picture
metadata when Supabase returns a safe HTTPS avatar URL, otherwise a user initial,
otherwise a neutral inline SVG fallback. Logout is available only inside the
account popover and the popover closes on outside click/Escape; this avoids an
accidental logout tap while typing/searching on mobile.

The production browser calls `event-search` with `Accept: application/json`.
NDJSON remains an explicit diagnostics opt-in; the normal mobile path must not
wait for streamed response headers/chunks that Android Chrome or an in-app
WebView may buffer. Search is cost-bearing and therefore uses the shared
`selected-once` policy: no automatic second POST is allowed after a timeout or
transport failure. Earlier v57 temporarily
used `use_llm_verifier=false` as a production-safety rollback after live mobile
evidence showed two different failure modes:

1. at `2026-06-29T14:28Z` and `14:29Z` the backend wrote successful
   `event_search_requests` rows with `12` results in `<1s`, but Chrome/WebView
   did not deliver/render the terminal streamed result;
2. after switching to JSON, a later mobile request reached the fake 88%
   “Собираю карточки” progress state and timed out with no audit row, which is
   consistent with a provider/Edge request stuck before completion.

After hardening, the visible path can request `use_llm_verifier=true` again:
LLM verification is bounded by `EVENT_SEARCH_LLM_TIMEOUT_MS`, uses provider-side
JSON schema, and must fall back to vector order on timeout/provider failure. The
frontend must not calculate pagination from the filtered result count: the Edge
Function returns `retrieved_count` and `next_offset`, so a page where LLM keeps
8 of 12 candidates can still expose “Показать ещё” without overlapping the next
vector page. Fallback/personal-feed cards are emitted only after the raw vector
candidate stream is exhausted, not merely because LLM filtered the current page.
Until the feature has job/polling progress, the progress bar is a bounded
browser-stage indicator rather than a true backend-stage feed.

2026-06-29 diagnostic timings on the live Edge Function after schema/timeout hardening:

- pgvector-only: roughly `0.65–1.12s` total in backend timings for tested queries;
- pgvector + LLM verifier: roughly `2.74–2.87s` backend total, with `llm_ms≈1.98–2.00s`;
- examples: `джаз на выходных` kept 8 verified items, `выставка куда можно пойти с детьми` kept 5, `урбанистика будущее города` kept 2.

The Edge Function still supports NDJSON when requested with
`Accept: application/x-ndjson`; use that for controlled diagnostics only. The
production mobile UX should not depend on streamed final payload delivery.

The terminal JSON contract may contain zero exact `items` and non-empty
`fallback_items`. Those discovery cards are rendered immediately under the
honest fallback heading; they must not be buffered behind `has_more`, because
that produced a blank page for real queries such as `Хор мальчиков`. If a later
exact page succeeds, the client replaces/deduplicates the provisional fallback
set rather than showing stale duplicates. The regression queries `Концерт
итальянца` and `Хор мальчиков` are release smokes for visible cards and absence
of a network alert.

## Edge Function response/log contract

`supabase/functions/event-search` returns and logs investigation IDs for every successful request:

- `request_id` — per-call UUID;
- `served_list_id` — UUID for the returned list;
- `served_list_hash` — SHA-256 over `query_hash`, returned event ids and fallback ids;
- `query_facets` — compact parsed facets (`weekday_iso`, `weekday_ru`, `time_of_day`, `admission`), never raw query text;
- `embedding_key_env` and `llm_attempts[].key_env` — non-secret key lane names used for provider rotation/failover analysis;
- `embedding_cache_status` / `result_cache_status` — whether the query embedding or whole result payload came from the private salted-hash cache;
- `served_from_cache` — true only when the whole result payload was returned before quota reservation/provider calls;
- `timings_ms` — quota, embedding provider, pgvector RPC, optional LLM verifier, fallback RPC and total latency;
- `llm_verifier` — `{requested, used, status}` so a search can be distinguished between pure pgvector and verified/reranked results.

Structured logs are emitted as JSON lines:

- `event_search_completed`;
- `event_search_quota_exceeded`;
- `event_search_failed`.

Logs and audit rows use `query_hash`/length and a short `user_hash`; raw search text and access tokens are not logged or stored.

### Mobile search failure evidence, 2026-06-29

The user-visible “no result / timeout” reports around 16:28–16:30 local browser
time were checked against `event_search_requests`. For the same anonymized user
hash, the backend was healthy:

- `2026-06-29T14:28:05Z`: `status=ok`, `kind=vector_search`,
  `result_count=12`, `llm_status=llm_quota_exhausted`, total `≈939ms`;
- `2026-06-29T14:29:35Z`: `status=ok`, `kind=vector_search`,
  `result_count=12`, `llm_status=llm_quota_exhausted`, total `≈914ms`.

That proves the failure was the browser/static delivery path after the Edge
Function response, not pgvector retrieval, not authorization, and not ordinary
search quota. v56 changes the public page to JSON response mode and was verified
with a public Playwright smoke that scrolled through the rendered cards.

## Current verification evidence

Applied to the personalization Supabase project on 2026-06-28:

- `vector` extension installed in schema `extensions`;
- `event_search_documents`: 76 rows after v48 canary syncs;
- `event_embeddings`: 76 rows for `gemini-embedding-2`, dim `768`;
- relation sizes after backfill: embeddings about `672 kB`, documents about `640 kB`.

Security smoke:

- anonymous direct table select on `event_search_documents` returns `401 permission denied`;
- anonymous call to quota RPC returns `401 permission denied` (current contract: `get_event_search_quota_v2`).

Golden semantic smoke for event `6447` (“Как договориться о будущем города”): backend pgvector RPC returns `6310` “Архитектурно-урбанистическая студия...” as the first non-self candidate (`vector_similarity≈0.8592` in the v48 build), ahead of `5261` “Музыка нашего города”. The published discovery JSON for 6447 also keeps `6310` first after Gemma 4 26B verification (`llm_semantic_score=0.92`).

This fixes the specific lexical failure where “Музыка нашего города” outranked the urban-planning studio solely because of the token “город”.

## v49 auth/search navigation canary

Public preview: <https://kenigevents.ru/preview-20260629-event-pages-v49-auth-pgvector/poisk/>.

Evidence from 2026-06-29 UTC:

- v48 had pgvector/Gemma related-event data, but no visible auth/search UI because the static build was produced without browser-safe `PUBLIC_PERSONALIZATION_SUPABASE_*` envs;
- v49 republishes the same 70-event real-data canary with public Supabase URL/publishable key and `PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex`;
- the mobile tag drawer and desktop/footer navigation now include **Поиск** → `/poisk/`;
- public `/poisk/` contains `data-authorized-search`, `custom:yandex`, `data-supabase-url` and the “Войти через Яндекс” button;
- `npm run check:preview` passed for `preview-20260629-event-pages-v49-auth-pgvector`;
- mocked browser smoke passed: `authorized_search_ui_smoke=ok`, first rendered search card `6310`, `request_calls=1`;
- live Edge Function smoke with a temporary Supabase Auth user passed: query `урбанистика будущее города` returned `[6447, 6310]`, `algorithm_id=pgvector_gemini_embedding_2_llm_verify_v1`, `llm_verifier.status=ok`, duplicate ids absent;
- readiness probe passed: static public auth env, Yandex OAuth credentials/provider redirect, Edge Function OPTIONS and vector-sync backend env are all present.

Gemma verifier hardening in the Edge Function uses Google structured output (`responseMimeType=application/json` + `responseSchema`) and a fallback JSON-object extractor. The rejected `responseFormat` field was removed after a provider `400` probe; this keeps the deployed `generateContent` call compatible with the current v1beta endpoint.

## v48 canary evidence

Public Kaggle-built preview: <https://kenigevents.ru/preview-20260628-event-pages-v48-pgvector-gemma-kaggle/__preview/>.

Evidence from 2026-06-29 UTC:

- local vector sync: `70` documents upserted, `12` new/changed Gemini Embedding 2 vectors after weekday/category hardening;
- live personalization Supabase: `event_search_documents=76`, `event_embeddings=76` for `gemini-embedding-2/vector(768)`;
- related retrieval: `event_pgvector_related_chain_v1`, `retrieval_method=supabase_pgvector_hnsw_cosine_v1`, `semantic_embeddings=true`;
- Gemma 4 26B verifier: local canary `status=ok`, `audited_anchors=15`, `provider_calls=7`, `cache_hits=8`, `errors=[]`; the subsequent Kaggle run used the persisted verifier cache (`cache_hit_no_provider`, `provider_calls=0`);
- Kaggle CPU canary: `preview-20260628-event-pages-v48-pgvector-gemma-kaggle`, `ok=true`, `event_count=70`, `npm run check:preview` passed inside the notebook;
- live public smoke: `/data/discovery/6447.json` returns `algorithm_id=event_pgvector_related_chain_v1` and first candidate `6310` with `vector_similarity≈0.8592`, `llm_semantic_score=0.92`.

## Authorized RPC smoke evidence

Before 2026-06-29 the Edge Function deploy was blocked without `PERSONALIZATION_SUPABASE_ACCESS_TOKEN` and `PERSONALIZATION_SUPABASE_PROJECT_REF`; the live-auth proof below remains useful as a backend RPC regression smoke for the path used by the Edge Function.

Script: `scripts/smoke_authorized_event_search_rpc.py`.

Verified on 2026-06-29 UTC against the live personalization Supabase project:

```bash
python3 scripts/smoke_authorized_event_search_rpc.py   --env-file .env   --query "урбанистика будущее города"   --expected-event-id 6310   --expected-top-n 3
```

Result:

- temporary Supabase Auth user created and signed in with a real authenticated JWT;
- `reserve_event_search_quota_v1` succeeded (`day_remaining=4` for the temp user);
- Gemini Embedding 2 returned `768` dimensions;
- authenticated `search_events_by_embedding_v1` returned top results:
  1. `6447` “Как договориться о будущем города” (`similarity≈0.7426`),
  2. `6310` “Архитектурно-урбанистическая студия...” (`similarity≈0.7055`),
  3. `5690` “Открытие выставки-экзамена «Обход 2.0»” (`similarity≈0.6127`);
- compact audit RPC succeeded;
- smoke quota/audit rows and the temporary user were removed after the run.

This proves the authenticated pgvector RPC path and quota/audit path independently of the browser OAuth UX.

Additional facet smoke after `20260629_event_search_query_facets.sql`:

```bash
python3 scripts/smoke_authorized_event_search_rpc.py \
  --env-file .env \
  --query "урбанистика в четверг вечером по регистрации" \
  --weekday-iso 4 \
  --time-of-day evening \
  --admission registration_required \
  --expected-event-id 6310 \
  --expected-top-n 3
```

Result: authenticated pgvector RPC returned `6310` as top-1 with boosted similarity `≈0.9255`, proving that explicit weekday/time/admission facets influence order while still searching through `gemini-embedding-2/vector(768)` candidates and trusted snapshots.

## Mocked browser UI smoke evidence

Script: `scripts/smoke_authorized_search_ui.py`.

This is a browser smoke for the static Astro UI with mocked Supabase network responses. It is intentionally **not** a substitute for the final live Yandex OAuth + deployed Edge Function E2E; it catches frontend integration regressions without requiring an interactive Yandex login session.

Verified on 2026-06-29 UTC against a preview build rendered with browser-safe public env:

```bash
PUBLIC_PERSONALIZATION_SUPABASE_URL=https://example.supabase.co \
PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY=sb_publishable_test \
PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex \
npm --prefix site run build:preview

python3 scripts/smoke_authorized_search_ui.py \
  --dist site/dist/preview-20260629t015724-4c2d398a \
  --supabase-url https://example.supabase.co
```

Result:

- simulated a Supabase PKCE OAuth callback with `?code=...`, mocked `/auth/v1/token?grant_type=pkce` plus `/auth/v1/user`;
- verified the root switches to authenticated state, hides the Yandex login button and shows the one-line search form;
- submitted query `урбанистика в четверг вечером по регистрации`;
- verified the UI calls `event-search` with `use_llm_verifier=true`;
- verified returned results render through the shared split-action event-card renderer, including detail-link card, like, share, `Не интересно` and calendar actions;
- verified the search result container keeps `surface=authorized_event_search`, `request_id`, `served_list_id`, `served_list_hash` and `algorithm_id`;
- verified fallback starts as a separate **«Возможно, вам будет интересно»** section.

The smoke also fixed a real renderer bug: `escapeHtml(value || '')` erased numeric `0`, so the first result card rendered `data-rank=""`. The renderer now preserves zero values with `value == null ? '' : value`.

## Deploy/browser readiness check

Script: `scripts/check_authorized_search_readiness.py`.

Use it before claiming the browser/Yandex UX gate:

```bash
python3 scripts/check_authorized_search_readiness.py --env-file .env
python3 scripts/check_authorized_search_readiness.py --env-file .env --probe-edge --probe-yandex-provider --probe-yandex-userinfo-adapter --strict
```

The checker is redacted: it prints only `OK`/`MISSING` and never prints secret values. It verifies:

- static/Kaggle build can expose only browser-safe public Supabase URL + publishable key;
- Yandex OAuth app credentials are available for `custom:yandex`;
- Supabase deploy credentials are available for Edge Function deployment/configuration;
- Edge runtime env has Supabase Auth/RPC + Gemini embedding access;
- backend vector sync env has service/secret access.

On 2026-06-29 UTC, readiness is green for static public env, Yandex credentials, Supabase deploy credentials, Edge runtime env and vector sync env. Live probes passed: `OPTIONS /functions/v1/event-search` returns 200, unauthenticated POST returns `401 auth_required`, Supabase Auth URL Configuration has `site_url=https://kenigevents.ru` plus `https://kenigevents.ru/**` in the redirect allow-list, and Supabase Auth authorize for `custom:yandex` redirects to Yandex (`302`) without a localhost fallback.

## Remaining gates before production UX claim

1. Re-run live browser auth/search E2E on mobile against the latest preview after each auth/search UI change: preview page → login through Yandex → return to preview URL → quota visible → search → cards render → like/share/not-interested still work. The v55 fake-PKCE real-Edge smoke proves the static UI + deployed Edge path without consuming a real Yandex round-trip, but it does not replace the final real-device Yandex acceptance check.
2. Enable automatic Smart Update → Kaggle artifact → CDN promotion after artifact checks. The Smart Update → Kaggle command handoff already passes pgvector/vector-sync/search public envs; publishing the checked artifact to CDN remains a separate release gate.

## v50 search UX hardening canary

Public preview: <https://kenigevents.ru/preview-20260629-event-pages-v50-search-ux/poisk/>.

Changes from v49:

- `/poisk/` is now a dedicated search surface only: it does **not** render sample/static event cards, “Пока без запроса” copy, or a “Показать ещё” button before an actual authenticated query.
- The unauthenticated state is explicit and non-confusing: only the Yandex login CTA and explanatory text are visible; the search form, result feed and load-more control stay hidden.
- A global `[hidden] { display: none !important; }` regression guard is part of the static layout because component-level grid/flex CSS can otherwise override browser hidden semantics.
- The Yandex login CTA is full-width in the search card, uses a recognizable red `Я` icon, and is separated from logout state.
- On the dedicated page the input and search submit button are full-width/large controls; results appear below only after a query and use the shared split-action event cards.
- The mobile terracotta tag drawer wraps navigation links to additional rows instead of horizontal scrolling; the `Поиск` link remains available in the drawer/header/footer.
- If a user submits without a valid Supabase Auth session, the component does not call `event-search`; it clears stale results and asks to sign in. Non-2xx Edge Function errors are converted to product copy instead of leaking raw provider/Supabase errors.

Verification evidence on 2026-06-29:

- `npm --prefix site run check:preview` passed for `preview-20260629-event-pages-v50-search-ux`.
- `scripts/smoke_authorized_search_ui.py` now covers both states: unauthenticated hidden form/results/no prefilled cards, then a mocked Supabase Auth callback and mocked `event-search` response rendered as split-action cards.
- Public Playwright smoke on <https://kenigevents.ru/preview-20260629-event-pages-v50-search-ux/poisk/> passed: unauthenticated controls hidden correctly, authenticated mocked query rendered event `6310`, and the mobile drawer had no horizontal overflow.
- Readiness probe passed: `OPTIONS /functions/v1/event-search = 200`, Yandex provider authorize redirect `302`.
- Live Supabase RPC smoke with a temporary authenticated user passed for query `урбанистика будущее города`: pgvector top-3 included `6447`, `6310`, `5690`.
- Live Edge Function smoke with a temporary authenticated user returned `200`, algorithm `pgvector_gemini_embedding_2_llm_verify_v1`, ids `[6447, 6310]`, `llm_verifier.status=ok`; temporary user/quota/audit rows were cleaned up.

## Auth redirect incident: localhost fallback

On 2026-06-29 a real mobile OAuth attempt returned to `localhost:3000/?error=...` after Yandex consent. Root cause: the personalization Supabase Auth URL Configuration still had the default `site_url=http://localhost:3000` and an empty `uri_allow_list`, so Supabase fell back to the local development URL instead of the `redirectTo` preview URL.

Fixed through Supabase Management API:

- `site_url=https://kenigevents.ru`;
- `uri_allow_list=https://kenigevents.ru/**,https://www.kenigevents.ru/**`.

Regression guard: `scripts/check_authorized_search_readiness.py --probe-auth-config --probe-yandex-provider --probe-edge --strict` now checks the Auth URL Configuration and verifies the authorize redirect points to Yandex without `localhost` in the redirect chain.

## v51 PKCE callback fix canary

Public preview: <https://kenigevents.ru/preview-20260629-event-pages-v51-auth-pkce/poisk/>.

Why this exists: after the localhost redirect fix, a real mobile flow returned to the same `/poisk/` URL but the UI still stayed anonymous. Root cause: the frontend relied on automatic implicit callback detection; the actual Supabase/Yandex return path used an authorization `code`, so no session was available when the UI checked `getSession()`.

Fix:

- `AuthorizedEventSearch.astro` now creates the Supabase client with `flowType: 'pkce'` and `detectSessionInUrl: false`;
- login uses a cleaned same-page `redirectTo`, preserving the page the user started from;
- on return, the component explicitly calls `supabase.auth.exchangeCodeForSession(code)`, then removes `code/error/state` params from the URL and only then updates the auth-dependent UI;
- auth callback errors are shown as product copy instead of leaving the user on a silent anonymous page.

Verification evidence on 2026-06-29:

- `npm --prefix site run check:preview` passed for `preview-20260629-event-pages-v51-auth-pkce`;
- `scripts/smoke_authorized_search_ui.py` passed with mocked PKCE token exchange and mocked `event-search`: `authorized_search_ui_smoke=ok`, first card `6310`, `request_calls=1`;
- readiness probe passed: `scripts/check_authorized_search_readiness.py --env-file .env --probe-auth-config --probe-yandex-provider --probe-yandex-userinfo-adapter --probe-edge --strict`;
- deployed public preview smoke passed with mocked Supabase PKCE callback on `https://kenigevents.ru/preview-20260629-event-pages-v51-auth-pkce/poisk/?code=...`: the page switched to `is-authorized`, displayed the search form, submitted a query and rendered a split-action event card.
- after a real Yandex attempt returned `Error getting user email from external provider`, `custom:yandex` was reconfigured to the `yandex-userinfo` adapter and the new adapter readiness probe passed.

## v52 static PKCE hardening canary

Public preview: <https://kenigevents.ru/preview-20260629-event-pages-v52-auth-static-pkce/poisk/>.

Why this exists: a real Yandex login attempt reached the static page with `?code=...`, and Supabase created a real authenticated user/session in the personalization project, but the mobile UI still fell back to the anonymous state. This confirmed that the server-side OAuth/userinfo part was fixed, while the static-page browser callback needed stronger client-side handling and diagnostics.

Fix:

- custom Supabase auth storage mirrors only the short-lived PKCE code verifier into a SameSite=Lax Secure cookie so a mobile OAuth round-trip has a second same-origin verifier source;
- after successful `exchangeCodeForSession(code)`, the page explicitly calls `setSession` before unlocking the search form;
- callback handling marks the page as “auth callback in progress” before waiting for Supabase, so the initial anonymous auth-state event cannot overwrite the callback status;
- failed/expired verifier callbacks clean the stale `code` from the URL and show a clear retry message.

Verification evidence on 2026-06-29:

- Supabase Auth DB showed the previous real attempt created user/session/identity for `custom:yandex`, proving the Yandex adapter and provider callback were no longer the blocker;
- `npm --prefix site run check:preview` passed for `preview-20260629-event-pages-v52-auth-static-pkce`;
- `scripts/smoke_authorized_search_ui.py` passed against the v52 build with the real personalization Supabase URL mocked at network layer; the smoke now covers both missing-verifier error UX and a successful mocked PKCE callback/search;
- public smoke for `https://kenigevents.ru/preview-20260629-event-pages-v52-auth-static-pkce/poisk/?code=missing-verifier-code` returns to clean `/poisk/` and shows the explicit “сессия входа устарела…” retry message.

## v53 backend-progress search canary

Public preview: <https://kenigevents.ru/preview-20260629-event-pages-v53-search-progress/poisk/>.

Why this exists: after v52 a real Yandex login reached the authorized UI and quota was visible, but a submitted search did not render results and the UI could stay in an unrecoverable loading state. Database inspection showed the real Yandex user/session existed, while no new `event_search_requests` rows were recorded for that attempt, so the page needed stronger request diagnostics, visible backend stages and guaranteed error/timeout recovery.

Search progress contract:

- the static page no longer treats the button progress as decorative; it calls `event-search` directly with `Accept: application/x-ndjson`;
- the Edge Function streams compact NDJSON events with real backend stages: `accepted`, `auth`, `validate`, `quota`, `embedding`, `vector_search`, `llm_verify`, `fallback`, `finalize`, then either `result` or `error`;
- the UI updates the button progress/status only from those streamed backend events, then renders the final result payload through the same split-action event cards;
- if streaming is unavailable, the UI falls back to a normal JSON response, so older/non-stream responses fail gracefully instead of leaving a dead button;
- if a mobile browser/proxy accepts NDJSON but stalls, the browser cancels that response, clears loading state and offers an explicit retry; it never duplicates the cost-bearing request automatically.

Validation and error handling:

- client and Edge Function both normalize control characters/whitespace and enforce a 3..180 character query;
- obviously technical/unsafe input is rejected before provider/RPC work: HTML/script tags, `javascript:`, SQL-comment markers, broad SQL command patterns, template-injection markers and direct prompt-injection phrases;
- Edge Function validation is authoritative and returns `query_too_short`, `query_too_long`, `query_unsafe` or `query_bad_characters`;
- the browser maps backend errors (`quota_exceeded`, `auth_required`, query validation errors, provider/search failures, timeout) to product copy and always re-enables the input/search button;
- authorized state shows the Yandex display name/login/email as “Вошли как …”, so the logout button is no longer ambiguous.

Verification evidence on 2026-06-29:

- deployed `event-search` with `--no-verify-jwt` and verified live NDJSON streaming with a temporary authenticated user: unsafe query streamed `accepted/auth/validate/error:query_unsafe`; normal query streamed `accepted/auth/validate/quota/embedding/vector_search/llm_verify/fallback/finalize/result`;
- `npm --prefix site run check:preview` passed for `preview-20260629-event-pages-v53-search-progress`;
- `scripts/smoke_authorized_search_ui.py` passed for v53 and now covers signed-in identity display, client-side unsafe-query rejection without calling `event-search`, NDJSON progress/result handling and final button reset;
- readiness probe passed for static env, Yandex provider redirect, userinfo adapter and Edge Function OPTIONS.

## v54 saved-session restore and callback non-blocking hardening

Public preview: <https://kenigevents.ru/preview-20260629-event-pages-v54-auth-restore/poisk/>.

Why this exists: after v53 a real mobile flow could return to the static `/poisk/` page and stay stuck with visible “Войти через Яндекс” plus status “Завершаю вход через Яндекс…”. A new preview URL on the same `kenigevents.ru` origin also did not reliably show the saved authenticated state. The intended product contract is: preview path changes must not log the user out; Supabase session storage is origin/project based, not preview-path based.

Fix:

- the page now writes a compact `ke_yandex_auth_intent_v1` marker to `localStorage` when the user starts Yandex login and updates it on callback/signed-in/failure states;
- on every static page load the UI first checks local auth signals (Supabase session key or our intent marker) and only then performs the saved-session check, avoiding blind auth rechecks for anonymous users;
- existing Supabase sessions are restored across new preview links on the same `kenigevents.ru` origin before asking the user to log in again;
- the PKCE callback exchange is bounded by a 20s timeout and always cleans stale `code/error/sb` URL params, so the page cannot stay forever at “Завершаю вход…”;
- the Supabase `onAuthStateChange` callback no longer awaits Supabase calls inside the callback. It renders from the callback session payload and defers quota RPCs with `setTimeout`, following Supabase JS guidance and avoiding callback deadlocks;
- while auth is being checked, the Yandex login CTA is hidden/disabled instead of showing a contradictory login button next to “Завершаю вход…”.

Verification evidence on 2026-06-29:

- `scripts/smoke_authorized_search_ui.py` now verifies that after a mocked successful PKCE callback, navigating to a fresh preview URL without `?code=` restores the same signed-in UI from stored Supabase session/local auth state;
- readiness probe still covers Auth URL config, `custom:yandex` provider redirect, userinfo adapter and Edge Function OPTIONS.

## v55 avatar account menu and real-Edge search smoke

Public preview: <https://kenigevents.ru/preview-20260629-event-pages-v55-auth-search-smoke/poisk/>.

Why this exists: after v54 the backend could complete real searches in about 3 seconds and write `event_search_requests(status=ok)`, while the mobile UI could still show a timeout/dead search state. Separately, the visible full-width “Выйти из аккаунта” button sat directly above the query input and created a high-risk accidental logout target.

Fix:

- signed-in state now uses a compact avatar/account menu in the search card header; the user identity is visible, while logout is hidden inside the popover instead of being a primary page action;
- avatar fallback order is Yandex/Supabase HTTPS picture URL → first initial → neutral inline user SVG;
- account popover closes on outside click and Escape;
- NDJSON handling returns as soon as the `result` event is received and cancels the reader, so a completed Edge Function response cannot be lost while waiting for stream EOF;
- `scripts/smoke_authorized_search_ui.py --real-edge` now performs a browser smoke with a real Supabase Auth session and real deployed `event-search`, while only the static PKCE token exchange is mocked. This is intentionally opt-in because it consumes live search quota and creates a temporary auth user.

Verification evidence on 2026-06-29:

- Gemini Pro UI consultation (`gemini-3.1-pro-preview`) agreed that the visible logout button near search is a UX antipattern and recommended the avatar/dropdown pattern; artifact: `artifacts/codex/authorized-search-ui-review-20260629/`;
- `npm --prefix site run check:preview` passed for `preview-20260629-event-pages-v55-auth-search-smoke`;
- mocked browser smoke passed: `authorized_search_ui_smoke=ok dist=preview-20260629-event-pages-v55-auth-search-smoke cards=2 first_event=6310 request_calls=1`;
- real Edge browser smoke passed: `authorized_search_real_edge_smoke=ok dist=preview-20260629-event-pages-v55-auth-search-smoke cards=16 first_event=5201 status="Осталось поисков: 4 сегодня, 29 в этом месяце."`;
- readiness probe passed with Auth URL config, `custom:yandex` provider redirect, userinfo adapter and Edge Function OPTIONS;
- live audit rows after the smoke show `event_search_requests.status=ok`, `request_kind=llm_rerank`, `result_count=8`, `llm_used=true`, with total backend time about `2.7–3.1s` for `query_length=16`.


## v58/v20260702 recovery UI and quota smoke

Public preview: <https://kenigevents.ru/preview-20260702t0755-fresh-ui-fixes/poisk/>.

This recovery preview intentionally merges the latest static-site polish with the KEY5 smart-search quota branch without changing the deployed Edge Function contract:

- `/poisk/` keeps the avatar/account menu from v55 and restores the newer mobile layout where the submit button sits below the input and carries the live progress bar;
- the static build must be produced with `PUBLIC_PERSONALIZATION_SUPABASE_URL`, `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY` and `PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex`; `check:preview` fails if the browser HTML does not contain those public markers;
- readiness for the live project shows embedding lanes `GOOGLE_API_KEY5`, `GOOGLE_API_KEY4`, `GOOGLE_API_KEY3`, `GOOGLE_API_KEY2`, `GOOGLE_API_KEY`, and Lite verifier lanes `GOOGLE_API_KEY5`, `GOOGLE_API_KEY4`, `GOOGLE_API_KEY3`, `GOOGLE_API_KEY`; `GOOGLE_API_KEY2` stays guide-reserved for LLM except late failover;
- the personalization database is the separate Supabase/Postgres project with `event_search_documents`, `event_embeddings`, `event_search_requests`, `user_search_quota_ledger` and hourly/cache search tables; no Yandex YDB integration is wired into this search path.

Verification evidence for the recovery build is kept under `artifacts/codex/static-site-ui-fixes-20260702/`: redacted readiness passed after deploy, the fresh static build/check passed for `preview-20260702t0755-fresh-ui-fixes`, mocked browser UI smoke passed (`cards=1`, first event `6310`), and the live Edge smoke for `интересно детям` passed with `18` rendered cards, first event `5618`, scrolled event `6215`, and quota text `999/9999` after the smoke.

## LLM quota fallback hotfix, 2026-06-29

Observed user-visible failure: the page showed ordinary search quota still available (`3` searches today), but the next request returned **«Лимит поисков на сегодня закончился»** and no cards. Supabase audit showed the same anonymized user had two successful LLM-reranked searches (`llm_request_count=2`) and then a `quota_exceeded` row for the third query while `request_count=2`. Root cause: the Edge Function reserved search quota with `p_use_llm=true`, so the smaller optional LLM verifier quota (`2/day`) blocked the whole search even though the main search quota (`5/day`) was not exhausted.

Fix:

- added `reserve_event_search_quota_v2(...)` in `supabase/migrations/20260629_event_search_llm_quota_fallback.sql`;
- the RPC always enforces ordinary search quota, but returns `llm_reserved=false` instead of raising when only the optional LLM quota is exhausted;
- `event-search` now skips `llmVerify(...)` when `llm_reserved=false`, returns trusted pgvector cards, and records `llm_status=llm_quota_exhausted` / `request_kind=vector_search`;
- deployed the updated `event-search` Edge Function to the personalization Supabase project.

Verification evidence:

- reproduced the original user pattern in audit rows: successful LLM search at `2026-06-29T14:01:10Z`, then pre-fix `quota_exceeded` at `2026-06-29T14:02:11Z` for the same anonymized user while ordinary quota remained;
- after the fix, a same-user-smoke performed three searches with one auth user: first two used LLM rerank, third succeeded as pgvector-only with `llm_status=llm_quota_exhausted`, `result_count=12`, `day_remaining=2`, `llm_day_remaining=0`;
- local real-Edge Playwright smoke now proves scrollability, not only first-card render: `cards=12 first_event=5237 scrolled_event=6310 scroll_y=6073` on the third search after LLM quota exhaustion;
- public-page real-Edge Playwright smoke on <https://kenigevents.ru/preview-20260629-event-pages-v55-auth-search-smoke/poisk/> rendered and scrolled through cards: `cards=16 first_event=5201 scrolled_event=698 scroll_y=9143`; screenshot artifact: `artifacts/codex/authorized-search-public-smoke-20260629/public-v55-scrolled-results.png`;
- readiness probe and `npm --prefix site run check:preview` stayed green.

## R11 bounded mobile liveness and identifiable account, 2026-07-24

Telegram evidence `669–671` showed a real authenticated mobile request stuck at
`Ищу… / Открываю поиск…`. Readiness and a real Edge smoke against the same R10
build succeeded, so the failure was a client liveness hole rather than a claim
that the deployed search backend was unavailable.

Every search attempt has bounded response-header, NDJSON read and overall phases.
A stall cancels only the current epoch, clears skeleton/progress and restores an
editable form with honest retry copy. It does **not** run an automatic rescue:
the server may already have accepted the cost-bearing POST. Request epochs still
prevent a late response from repainting a newer query; logout and page exit
invalidate pending session and network continuations.

The account control is identity, not decoration. It exposes
`Профиль: <name/email>` to assistive technology and keeps the full identity in
the account popover. A meaningful human name is preferred; otherwise the email
is shown and its local part supplies the deterministic initial. A one-letter
provider username cannot mask a known email. Opaque provider imagery is not the
only explanation of the session, and an image failure falls back to the same
initial.


## R15 shared resilient-client contract, 2026-07-31

`AuthorizedEventSearch` no longer owns route fallback. It uses the same
configuration-keyed data-client singleton as Auth, personal-feed reads and
idempotent telemetry, while the singleton itself remains independent of Auth.
Safe reads may try the alternate healthy route once. Search and email OTP are
non-idempotent or cost-bearing operations and use `selected-once`: they are sent
on exactly one preselected route and an ambiguous timeout is reported honestly.
Only an explicit new user action may retry them.

The local feedback fallback is versioned, capped at 12 compact entries / 5 KiB
and expires after seven days. It contains no session token. Browser-side
cooldowns and caps are UX/egress controls, never the abuse boundary: Edge rate
limits, authorization, validation and database policies remain authoritative.


## R14 global auth and header-stall recovery, 2026-07-27

Auth ownership is no longer local to `AuthorizedEventSearch`. The browser
singleton in `site/src/lib/staticSiteAuth.ts` owns the single Supabase client,
PKCE callback exchange, origin-scoped saved session, sign-in and sign-out.
`StaticSiteAuthRuntime.astro`, mounted exactly once by `EventLayout`, binds the
same state to Search, `/dlya-menya/` and `Reference4MobileMenu`. Menu and
Personal provide direct `custom:yandex` entry points and never route login
through Search. Session tokens are not copied into markup, DOM events or
page-specific stores.

Search keeps the accepted standalone textarea, but an unmodified Enter is a
search action: it calls native `form.requestSubmit()` and exposes
`enterkeyhint="search"`. `Shift+Enter`, active IME composition and key code
`229` do not submit.

Search uses the Auth-independent, configuration-keyed resilient data client.
Safe health probes run in parallel and their selected route is briefly reused.
The Search POST itself is `selected-once`: if neither route is healthy, nothing
is sent; after dispatch, timeout is ambiguous and cannot trigger a second POST.
The form always returns to an editable, explicitly retryable state.

Required release evidence remains: real mobile Yandex login → return to the
same immutable candidate → menu, Personal and Search all show the same identity
→ Enter submits → a real Edge result renders canonical cards. Mocked callback
and network-stall tests are regression gates, not a substitute.
