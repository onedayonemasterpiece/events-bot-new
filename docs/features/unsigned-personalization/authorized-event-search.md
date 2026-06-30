# Authorized event search with Supabase pgvector

> Status: P0 infrastructure implemented. On 2026-06-29 the personalization Supabase project has `custom:yandex` configured and Edge Function `event-search` deployed. Current hotfix hardens the authorized search UX/test gate and quota behavior: account actions are hidden behind an avatar menu, the smoke suite proves scrollable cards against the real deployed `event-search`, exhausted optional LLM-rerank quota no longer blocks pgvector search results, and the mobile frontend requests one JSON result response instead of relying on the streamed final payload that failed in Chrome/WebView despite successful backend audit rows.

## Product contract

Authenticated users get a one-line **Умный поиск** on a dedicated `/poisk/` page and, for the preview canary, on listing/index pages. The mobile terracotta tag drawer, desktop header and footer expose a plain navigation link **Поиск** to `/poisk/`; the search form itself is not placed inside the drawer so the header remains compact. The user can type a natural-language intent, for example “урбанистика”, “детский мастер-класс” or “джаз вечером”. Results are rendered as the same event cards used in `Смотрите дальше`:

- card opens the event detail page;
- like / unlike updates local personalization state;
- `Не интересно` feeds the negative-interest profile and hides/downranks future cards;
- share uses the already accepted image+text+link Web Share path;
- calendar action remains available only for calendar-eligible events.

When vector results are exhausted, the UI starts a separate section **«Возможно, вам будет интересно»**. This is fallback/discovery, not a continuation of exact search relevance.

Anonymous users have quota `0`: the UI shows “Войти через Яндекс”. Search is not available until Supabase Auth has a valid session.

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
`20260628_event_search_public_fields_and_model_filter.sql`,
`20260629_event_search_query_facets.sql` and
`20260630_event_search_embedding_doc_kind.sql`.

Tables:

- `public.event_search_documents` — compact factual `search_digest`, cleaner `related_digest`, controlled facets and trusted `card_snapshot`; no raw OCR/source text;
- `public.event_embeddings` — `gemini-embedding-2` vectors, `vector(768)`, `embedding_doc_kind`, partial HNSW cosine indexes for `search_v3` and `related_v1`;
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

Authorized search uses only `embedding_doc_kind=search_v3`. Static event-page related generation uses `related_v1`; the Edge Function passes `p_embedding_doc_kind` to the RPC and defaults to `search_v3` so a future related-vector backfill cannot pollute user search results.

## LLM verifier

The Edge Function runs an LLM verifier after pgvector retrieval when the user has LLM quota. This verifier is an operational classifier over already retrieved IDs, not an external consultant review. Runtime contract:

- `EVENT_SEARCH_LLM_ENABLED=1` enables the verifier;
- primary verifier model is Gemma 4 26B (`EVENT_SEARCH_LLM_PRIMARY_MODEL=gemma-4-26b-a4b-it`; legacy `EVENT_SEARCH_LLM_MODEL` is still accepted). This protects the scarce `gemini-3.1-flash-lite` lane, which has only `500 RPD` and is shared with other critical processes;
- fallback verifier model is `EVENT_SEARCH_LLM_FALLBACK_MODEL=gemini-3.1-flash-lite`, but it is a protected rescue lane, not the normal runtime;
- `EVENT_SEARCH_VERIFICATION_WINDOW=48` by default for the current high-match canary (bounded by the Edge Function and UI request);
- model policy is user-state aware:
  - first/onboarding searches may use `fast_onboarding_fallback`: one short Gemma attempt, then Lite fallback if Gemma quickly returns `5xx/429/timeout`;
  - after the user has already spent several searches today, the function switches to `gemma_priority_late_fallback`: multiple longer Gemma attempts with backoff before any Lite fallback;
  - the threshold is controlled by `EVENT_SEARCH_LLM_FAST_FALLBACK_DAY_REMAINING_MIN` (default `45` after quota reservation for the current `50/day` canary plan).

Operational knobs: `EVENT_SEARCH_LLM_FAST_PRIMARY_ATTEMPTS`, `EVENT_SEARCH_LLM_FAST_PRIMARY_TIMEOUT_MS`, `EVENT_SEARCH_LLM_LATE_PRIMARY_ATTEMPTS`, `EVENT_SEARCH_LLM_LATE_PRIMARY_TIMEOUT_MS`, `EVENT_SEARCH_LLM_FALLBACK_TIMEOUT_MS`, `EVENT_SEARCH_LLM_PRIMARY_RETRY_BACKOFF_MS`, `EVENT_SEARCH_LLM_FALLBACK_ENABLED`, `EVENT_SEARCH_LLM_LATE_FALLBACK_ENABLED`.
Prompt/latency knobs: `EVENT_SEARCH_LLM_MAX_OUTPUT_TOKENS` (default `768`),
`EVENT_SEARCH_LLM_THINKING_LEVEL` (default `MINIMAL`) and
`EVENT_SEARCH_LLM_FACT_MAX_CHARS` (default `320`).

High-match contract:

1. pgvector returns a bounded candidate window from `gemini-embedding-2` vectors.
2. The LLM receives candidate IDs + compact facts from `search_digest`; it returns exactly three buckets: `exact_event_ids`, `possible_event_ids`, `rejected_event_ids` plus `query_interpretation`.
3. Only `exact_event_ids` are rendered under **«Результаты поиска»** (`items`).
4. Weak/uncertain matches are rendered only under **«Возможно, вам будет интересно»** (`fallback_items`).
5. If the LLM times out, provider returns non-2xx, facts are insufficient, quota is not reserved, the LLM rubber-stamps too many candidates as exact, or the verifier is disabled, the Edge Function fails closed: `items=[]`, candidates become possible/fallback only. Raw pgvector candidates must not be shown as exact search results in the public high-match mode.
6. `has_more=false` in this MVP high-match mode because repeated per-page LLM calls produced inconsistent page boundaries. The next production step is a cached/cursor verified window so “Показать ещё” paginates within one LLM-classified set instead of re-verifying each page.

P1 progressive UX target:

- first response should return the high-confidence Gemma-verified window as soon
  as the first verifier pass completes;
- while the user starts reading/scrolling, the backend may continue verifying
  the next candidate window(s) and append accepted cards below the current list;
- unverified vector candidates can be shown only under a separate provisional
  discovery heading, never as exact **«Результаты поиска»**;
- when later verifier passes finish, the UI may remove/reclassify lower
  unverified/provisional cards below the user’s current viewport, but must not
  suddenly move or remove the card the user is currently interacting with;
- this requires a server-side `search_session_id`/cursor and a cached verified
  result set, so repeated “Показать ещё” calls reuse one classification job
  instead of creating inconsistent independent LLM pages.

The verifier uses Gemini structured output (`responseMimeType: application/json` + `responseJsonSchema`) and still post-validates IDs against the retrieved candidate map. Broad queries can legitimately return many exact matches, so the previous default “over-approval” demotion is disabled by default; if a future incident proves rubber-stamping, it can be enabled explicitly with `EVENT_SEARCH_LLM_OVER_APPROVAL_DEMOTE_ENABLED=1` and a high `EVENT_SEARCH_LLM_OVER_APPROVAL_RATIO`.
Every provider try is recorded in `llm_verifier.attempts[]` and search metadata
with `{model, role: primary|fallback, attempt, status, elapsed_ms}`. The response
also exposes `llm_verifier.model`, `llm_verifier.policy` and
`llm_verifier.fast_fallback_allowed` so product/debug review can see whether a
Lite fallback was spent intentionally. If all attempts fail, the high-match
contract still fails closed: exact `items=[]`, possible candidates remain under
the separate fallback/discovery heading.
For Gemma 4 26B latency analysis, each attempt also records
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
where attempt->>'model' = 'gemma-4-26b-a4b-it'
order by created_at desc;
```

2026-06-29 live evidence after high-match hardening:

- `Концерт классической музыки`: exact `5201 Концерт «Фестиваль Pianissimo: Константин Емельянов»`, 3 possible, LLM stage ≈1.0s.
- `Чтобы было интересно детям`: exact `4512 С чего начинается Родина`, 3 possible, urban-planning events no longer appear as exact results.
- `джаз на выходных`: 0 exact, 4 possible; with the current limited corpus this is preferable to showing non-jazz music as exact.
- NDJSON/progress path emits backend stages: `auth`, `validate`, `quota`, `embedding`, `vector_search`, `llm_verify`, `finalize`, `result`.
- After the 2026-06-29 model-cascade fix, live Edge JSON smoke proves both
  policies:
  - fresh user / first search: `policy=fast_onboarding_fallback`, Gemma 4 26B
    primary attempt timed out at `3502ms`, Lite fallback returned `ok` in
    `1027ms`, exact `[5201]`;
  - same-user late path after 5 non-LLM searches:
    `policy=gemma_priority_late_fallback`, `fast_fallback_allowed=false`,
    Gemma 4 26B was tried twice at about `6500ms` each before Lite fallback
    returned `ok` in `991ms`, exact `[5201]`.
- Prompt-size/timeout follow-up on the same day showed the earlier failures were
  timeout/telemetry configuration problems, not proof that Gemma 26B cannot do
  the task. Direct provider probes returned the full 12-candidate verifier prompt
  in about `3.5–3.8s`. After deploying `maxOutputTokens=768`,
  `thinkingLevel=MINIMAL`, `EVENT_SEARCH_LLM_FACT_MAX_CHARS=320`,
  `FAST_PRIMARY_TIMEOUT_MS=5000` and `LATE_PRIMARY_TIMEOUT_MS=12000`, live Edge
  searches returned through Gemma 4 26B itself:
  `fast_onboarding_fallback` in `3379ms` and `gemma_priority_late_fallback` in
  `3394ms`, both with `prompt_chars=8508`, `prompt_fact_chars=3839`,
  `compact_candidate_count=12`, exact `[5201, 5478, 5479, 3730]`, and no Lite
  fallback spent.

Consultant traceability:

- Gemini Pro review was attempted only on allowed Pro models (`gemini-3.1-pro-preview`, `gemini-3-pro-preview`) and was blocked by `429 RESOURCE_EXHAUSTED`; evidence is stored in `artifacts/codex/search-consultants-20260629*/gemini-*-error.txt` and is not treated as completed Gemini review.
- `a-opus` reviewed the implementation twice. The first review found the fatal raw-vector-as-exact fallback; the second high-match review accepted the fail-closed architecture and flagged the now-fixed model fallback and over-approval guard, with remaining P1s around dedicated provider quota/key lane, prompt-injection hardening of candidate text, unreserving failed LLM quota, and proper verified-window pagination. Artifacts: `artifacts/codex/search-consultants-20260629/a-opus-review.md` and `artifacts/codex/search-consultants-20260629-high-match/a-opus-full-review.md`.

## Query facets

The event documents embed weekday/time/admission fields in the deterministic search text. In addition, the Edge Function extracts a very small set of explicit query facets so words like “пятница”, “вечером”, “утром”, “бесплатно” or “по регистрации” can improve ordering without introducing a separate keyword-search path:

- weekday: ISO `1..7` plus Russian weekday label for logs/metadata;
- time of day: `morning`, `day`, `evening`, `night`;
- admission: `free`, `registration_required`, `paid`.

The facets are not used to store raw query text. They are passed to `search_events_by_embedding_v1` and written only as compact metadata in Edge logs / audit rows. The RPC first asks pgvector for the nearest semantic candidates and only then applies a bounded boost (`weekday` > `admission` > `time_of_day`); therefore a facet cannot create events outside the trusted `card_snapshot` catalogue and cannot replace semantic retrieval with broad deterministic filtering.

## Quotas and privacy

Registered plan is dynamic, not a fixed “tiny” per-user constant. Migration
`20260629_event_search_quota_plan_dynamic.sql` adds service-role RPC
`refresh_registered_search_quota_v1(...)`, which recalculates
`search_quota_plans.registered` from:

- current Supabase Auth user count (`auth.users`);
- documented current provider project limits:
  - `gemini-embedding-2`: `100 RPM / 30k TPM / 1000 RPD`;
  - `gemma-4-26b-a4b-it` / `gemma-4-31b-it`: `15 RPM / unlimited TPM / 1500 RPD`; Gemma 4 26B is the primary online verifier lane;
  - `gemini-3.1-flash-lite`: `15 RPM / 250k TPM / 500 RPD`; this is a protected fallback/rescue lane because the same project already uses it for other critical processes.
- reserves for static generation/backfill (`400` embedding RPD, `500` LLM RPD by default);
- abuse caps for the still-private canary (`max 50` searches/day and `max 40` LLM reranks/day per registered user).

Current applied plan after the 2026-06-29 recalculation:

- search: `50/day`, `500/month`;
- LLM verifier: `40/day`, `400/month`.

Important boundary: the Edge Function currently uses a direct Google key path
(`GOOGLE_API_KEY4 || GOOGLE_API_KEY || GEMINI_API_KEY`) rather than the shared
Python `GoogleAIClient` limiter/key-rotation stack. Therefore the dynamic plan
conservatively counts one available provider lane plus reserves. If/when
`event-search` is routed through a central multi-key limiter, the same RPC can be
called with higher effective RPD inputs instead of hardcoding a new plan.

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

Signed-in identity is shown as a compact avatar/account control, not as a large
always-visible logout button near the query field. The avatar uses Yandex picture
metadata when Supabase returns a safe HTTPS avatar URL, otherwise a user initial,
otherwise a neutral inline SVG fallback. Logout is available only inside the
account popover and the popover closes on outside click/Escape; this avoids an
accidental logout tap while typing/searching on mobile.

The browser currently calls `event-search` with `Accept: application/json` and
renders cards from the single JSON response. v57 temporarily used
`use_llm_verifier=false` as a production-safety rollback after live mobile
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
- if streaming is unavailable, the UI falls back to a normal JSON response, so older/non-stream responses fail gracefully instead of leaving a dead button.

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
