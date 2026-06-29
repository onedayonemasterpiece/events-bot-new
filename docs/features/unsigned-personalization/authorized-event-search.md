# Authorized event search with Supabase pgvector

> Status: P0 infrastructure implemented. On 2026-06-29 the personalization Supabase project has `custom:yandex` configured and Edge Function `event-search` deployed. v50 has passed static checks, mocked browser UI search, public preview UI smoke, authenticated pgvector RPC smoke and authenticated Edge Function smoke; the remaining manual gate is a real end-user Yandex OAuth click-through in the browser.

## Product contract

Authenticated users get a one-line **Умный поиск** on a dedicated `/poisk/` page and, for the preview canary, on listing/index pages. The mobile terracotta tag drawer, desktop header and footer expose a plain navigation link **Поиск** to `/poisk/`; the search form itself is not placed inside the drawer so the header remains compact. The user can type a natural-language intent, for example “урбанистика”, “детский мастер-класс” or “джаз вечером”. Results are rendered as the same event cards used in `Смотрите дальше`:

- card opens the event detail page;
- like / unlike updates local personalization state;
- `Не интересно` feeds the negative-interest profile and hides/downranks future cards;
- share uses the already accepted image+text+link Web Share path;
- calendar action remains available only for calendar-eligible events.

When vector results are exhausted, the UI starts a separate section **«Возможно, вам будет интересно»**. This is fallback/discovery, not a continuation of exact search relevance.

Anonymous users have quota `0`: the UI shows “Войти через Яндекс”. Search is not available until Supabase Auth has a valid session.

## Auth design: Yandex through Supabase custom OAuth

Supabase has no built-in Yandex provider in the social-login list, so the project uses **Custom OAuth/OIDC Providers** with identifier `custom:yandex`.

Configured in the personalization Supabase project on 2026-06-29. Manual/Dashboard setup contract:

1. Auth → Providers → New Provider → Manual configuration.
2. Identifier: `custom:yandex`.
3. Type: OAuth2 unless Yandex OIDC discovery is configured separately.
4. Client ID / Client Secret: from the Yandex OAuth application.
5. Authorization URL: `https://oauth.yandex.ru/authorize`.
6. Token URL: `https://oauth.yandex.ru/token`.
7. UserInfo URL: `https://login.yandex.ru/info?format=json`.
8. Scopes: `login:email login:info` (adjust if Yandex app requires a different minimal set).
9. Add Supabase callback URL shown by the provider form to the Yandex app redirect URLs.
10. Add site redirect URLs such as `https://kenigevents.ru/*` and current preview prefixes to Supabase Auth URL allow-list.

Frontend uses:

```ts
supabase.auth.signInWithOAuth({
  provider: 'custom:yandex',
  options: { redirectTo: window.location.href },
})
```

As of 2026-06-29 the local/private environment contains the Yandex client credentials and the Supabase provider `custom:yandex` is configured. These secrets are not committed; readiness is checked by `scripts/check_authorized_search_readiness.py --probe-yandex-provider`.

## Retrieval architecture

### Data flow

```text
Fly SQLite / static export
  -> scripts/sync_event_search_vectors_to_supabase.py
  -> event_search_documents + event_embeddings(vector(768)) in personalization Supabase
  -> authenticated Edge Function event-search
  -> Gemini query embedding
  -> RPC search_events_by_embedding_v1
  -> optional LLM verifier/reranker over returned IDs only
  -> event-card snapshots in browser
```

### pgvector schema

Migrations: `supabase/migrations/20260628_event_search_pgvector.sql` plus hardening migrations
`20260628_event_search_weekday_and_related_rpc.sql`,
`20260628_event_search_public_fields_and_model_filter.sql` and
`20260629_event_search_query_facets.sql`.

Tables:

- `public.event_search_documents` — compact factual `search_digest`, controlled facets and trusted `card_snapshot`; no raw OCR/source text;
- `public.event_embeddings` — `gemini-embedding-2` vectors, `vector(768)`, HNSW cosine index;
- `public.search_quota_plans` — default registered quota plan;
- `public.user_search_quota_ledger` — day/month counters per Supabase user;
- `public.event_search_requests` — audit log with query hash and length only, no raw query text.

RPCs:

- `search_events_by_embedding_v1(...)` — authenticated vector retrieval through `SECURITY DEFINER`, no direct table reads; it accepts optional query facets `p_weekday_iso`, `p_time_of_day_filter` and `p_admission_filter` and applies them as small boosts after the nearest pgvector candidate set is retrieved;
- `event_search_fallback_cards_v1(...)` — authenticated fallback cards for “Возможно вам будет интересно”;
- `get_event_search_quota_v1(...)` — visible quota state;
- `reserve_event_search_quota_v1(...)` — atomic quota reservation before provider calls;
- `record_event_search_request_v1(...)` — compact search audit.

Direct browser `select` on raw tables is forbidden and currently rejected by grants/RLS. Browser access is through Auth + Edge Function/RPC only.

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

## LLM verifier

The Edge Function can run an optional LLM verifier/reranker only after vector candidates exist and quota is reserved. The verifier:

- receives only candidate IDs + compact card facts;
- may reorder or reject candidates;
- cannot create new events;
- returns IDs only, cards are hydrated from trusted `card_snapshot`.

Env gate:

- `EVENT_SEARCH_LLM_ENABLED=1` enables verifier;
- `EVENT_SEARCH_LLM_MODEL` defaults to `gemma-4-26b-a4b-it` for product reranking. This verifier is an operational reranker over already retrieved IDs, not an external consultant review.

If the LLM call fails, results fall back to vector order and the request remains usable.

## Query facets

The event documents embed weekday/time/admission fields in the deterministic search text. In addition, the Edge Function extracts a very small set of explicit query facets so words like “пятница”, “вечером”, “утром”, “бесплатно” or “по регистрации” can improve ordering without introducing a separate keyword-search path:

- weekday: ISO `1..7` plus Russian weekday label for logs/metadata;
- time of day: `morning`, `day`, `evening`, `night`;
- admission: `free`, `registration_required`, `paid`.

The facets are not used to store raw query text. They are passed to `search_events_by_embedding_v1` and written only as compact metadata in Edge logs / audit rows. The RPC first asks pgvector for the nearest semantic candidates and only then applies a bounded boost (`weekday` > `admission` > `time_of_day`); therefore a facet cannot create events outside the trusted `card_snapshot` catalogue and cannot replace semantic retrieval with broad deterministic filtering.

## Quotas and privacy

Default registered plan:

- search: `5/day`, `30/month`;
- LLM verifier: `2/day`, `10/month`.

Quota is reserved **before** Gemini embedding/LLM provider calls. Query text is never stored; only SHA-256 hash, length, result count and status are written to `event_search_requests`.

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

The component uses `@supabase/supabase-js` and invokes Supabase Edge Function `event-search`. If public env is missing, the component renders nothing and the static pages stay unchanged.

Kaggle StaticSiteBuilder handoff also accepts the same public values through
`--public-personalization-supabase-url`, `--public-personalization-supabase-publishable-key`
and `--public-yandex-auth-provider`. In production Smart Update handoff these are filled from
`STATIC_SITE_PUBLIC_*`, then `PUBLIC_*`, then the browser-safe personalization URL/publishable
key envs. Only URL + publishable key are exposed to Astro; Supabase secret/service keys remain
backend-only for vector sync and Edge Function deployment.

Search results are rendered through the same global `window.KenigEventsRenderEventCard`
renderer used by dynamic discovery/personal feeds. This is part of the acceptance
contract: cards returned by authorized search must keep detail-link navigation,
like/unlike, share, “не интересно” and calendar actions instead of using a separate
minimal search-result layout. The results container stores `request_id`,
`served_list_id`, `served_list_hash`, `algorithm_id` and `surface=authorized_event_search`;
feedback/share actions read that context so later investigation can connect strong
actions with the exact served search list.

## Edge Function response/log contract

`supabase/functions/event-search` returns and logs investigation IDs for every successful request:

- `request_id` — per-call UUID;
- `served_list_id` — UUID for the returned list;
- `served_list_hash` — SHA-256 over `query_hash`, returned event ids and fallback ids;
- `query_facets` — compact parsed facets (`weekday_iso`, `weekday_ru`, `time_of_day`, `admission`), never raw query text;
- `timings_ms` — quota, embedding provider, pgvector RPC, optional LLM verifier, fallback RPC and total latency;
- `llm_verifier` — `{requested, used, status}` so a search can be distinguished between pure pgvector and verified/reranked results.

Structured logs are emitted as JSON lines:

- `event_search_completed`;
- `event_search_quota_exceeded`;
- `event_search_failed`.

Logs and audit rows use `query_hash`/length and a short `user_hash`; raw search text and access tokens are not logged or stored.

## Current verification evidence

Applied to the personalization Supabase project on 2026-06-28:

- `vector` extension installed in schema `extensions`;
- `event_search_documents`: 76 rows after v48 canary syncs;
- `event_embeddings`: 76 rows for `gemini-embedding-2`, dim `768`;
- relation sizes after backfill: embeddings about `672 kB`, documents about `640 kB`.

Security smoke:

- anonymous direct table select on `event_search_documents` returns `401 permission denied`;
- anonymous call to `get_event_search_quota_v1` returns `401 permission denied`.

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

- simulated a Supabase implicit auth callback in the browser URL hash and mocked `/auth/v1/user`;
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
python3 scripts/check_authorized_search_readiness.py --env-file .env --probe-edge --probe-yandex-provider --strict
```

The checker is redacted: it prints only `OK`/`MISSING` and never prints secret values. It verifies:

- static/Kaggle build can expose only browser-safe public Supabase URL + publishable key;
- Yandex OAuth app credentials are available for `custom:yandex`;
- Supabase deploy credentials are available for Edge Function deployment/configuration;
- Edge runtime env has Supabase Auth/RPC + Gemini embedding access;
- backend vector sync env has service/secret access.

On 2026-06-29 UTC, readiness is green for static public env, Yandex credentials, Supabase deploy credentials, Edge runtime env and vector sync env. Live probes passed: `OPTIONS /functions/v1/event-search` returns 200, unauthenticated POST returns `401 auth_required`, and Supabase Auth authorize for `custom:yandex` redirects to the provider (`302`).

## Remaining gates before production UX claim

1. Pass live browser auth/search E2E on mobile: preview page → login through Yandex → return to preview URL → quota visible → search → cards render → like/share/not-interested still work.
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
