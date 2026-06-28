# Authorized event search with Supabase pgvector

> Status: P0 infrastructure implemented on 2026-06-28; Yandex provider credentials and Edge Function deploy are the remaining external configuration gates.

## Product contract

Authenticated users get a one-line **Умный поиск** on listing/index pages. The user can type a natural-language intent, for example “урбанистика”, “детский мастер-класс” or “джаз вечером”. Results are rendered as the same event cards used in `Смотрите дальше`:

- card opens the event detail page;
- like / unlike updates local personalization state;
- `Не интересно` feeds the negative-interest profile and hides/downranks future cards;
- share uses the already accepted image+text+link Web Share path;
- calendar action remains available only for calendar-eligible events.

When vector results are exhausted, the UI starts a separate section **«Возможно, вам будет интересно»**. This is fallback/discovery, not a continuation of exact search relevance.

Anonymous users have quota `0`: the UI shows “Войти через Яндекс”. Search is not available until Supabase Auth has a valid session.

## Auth design: Yandex through Supabase custom OAuth

Supabase has no built-in Yandex provider in the social-login list, so the project uses **Custom OAuth/OIDC Providers** with identifier `custom:yandex`.

Required external setup in Supabase Dashboard:

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

Current local `.env` does **not** contain `YANDEX_CLIENT_ID` / `YANDEX_CLIENT_SECRET`, so provider creation cannot be completed automatically by Codex without those credentials.

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

Migration: `supabase/migrations/20260628_event_search_pgvector.sql`.

Tables:

- `public.event_search_documents` — compact factual `search_digest`, controlled facets and trusted `card_snapshot`; no raw OCR/source text;
- `public.event_embeddings` — `gemini-embedding-2` vectors, `vector(768)`, HNSW cosine index;
- `public.search_quota_plans` — default registered quota plan;
- `public.user_search_quota_ledger` — day/month counters per Supabase user;
- `public.event_search_requests` — audit log with query hash and length only, no raw query text.

RPCs:

- `search_events_by_embedding_v1(...)` — authenticated vector retrieval through `SECURITY DEFINER`, no direct table reads;
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
- `EVENT_SEARCH_LLM_MODEL` defaults to `gemini-3.1-flash-lite` for product reranking (this is not an external consultant review).

If the LLM call fails, results fall back to vector order and the request remains usable.

## Quotas and privacy

Default registered plan:

- search: `5/day`, `30/month`;
- LLM verifier: `2/day`, `10/month`.

Quota is reserved **before** Gemini embedding/LLM provider calls. Query text is never stored; only SHA-256 hash, length, result count and status are written to `event_search_requests`.

## Frontend integration

Component: `site/src/components/AuthorizedEventSearch.astro`.

Inserted on:

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

## Current verification evidence

Applied to the personalization Supabase project on 2026-06-28:

- `vector` extension installed in schema `extensions`;
- `event_search_documents`: 70 rows;
- `event_embeddings`: 70 rows for `gemini-embedding-2`, dim `768`;
- relation sizes after backfill: embeddings about `672 kB`, documents about `640 kB`.

Security smoke:

- anonymous direct table select on `event_search_documents` returns `401 permission denied`;
- anonymous call to `get_event_search_quota_v1` returns `401 permission denied`.

Golden semantic smoke for event `6447` (“Как договориться о будущем города”): querying with that event embedding returns:

1. `6447` self-match;
2. `6310` “Архитектурно-урбанистическая студия...” similarity `0.8530`;
3. `5261` “Музыка нашего города” similarity `0.8352`.

This fixes the specific lexical failure where “Музыка нашего города” outranked the urban-planning studio solely because of the token “город”.

## Remaining gates before production UX claim

1. Create/enable Supabase custom OAuth provider `custom:yandex` after Yandex credentials are provided.
2. Deploy `supabase/functions/event-search` to the personalization Supabase project and configure Edge envs.
3. Pass live browser auth/search E2E on mobile: login → quota visible → search → cards render → like/share/not-interested still work.
4. Wire the vector sync script into the Smart Update/Kaggle static-site sequence after the static export snapshot is produced.
5. Add nightly/full recompute policy: because a new event can become similar to older events, related/static search snapshots must be refreshed for the whole active/future set, not only changed anchors.
