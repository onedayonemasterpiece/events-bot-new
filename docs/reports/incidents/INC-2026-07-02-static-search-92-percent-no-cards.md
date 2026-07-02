# INC-2026-07-02 Static smart search stalls at 92% without cards

Status: open
Severity: sev2
Service: Static site authorized smart search (`/poisk/`, personalization Supabase Edge Function `event-search`)
Opened: 2026-07-02
Closed: —
Owners: static site / personalization search
Related incidents: —
Related docs: `docs/features/unsigned-personalization/authorized-event-search.md`, `docs/features/unsigned-personalization/semantic-vector-retrieval.md`, `docs/operations/release-governance.md`

## Summary

Users reported that authorized static-site smart search can stay at the frontend progress state `92%` and never show result cards for queries such as `На природу с детьми` and `искусство у моря`, while other queries such as `в пятницу бесплатно` do render. Production entrypoint verification also found that `https://kenigevents.ru/poisk/` returns Object Storage `404 NoSuchKey`, and the production root still links to an older preview build that does not include `/poisk/`.

Server-side audit rows for the named queries were recorded as `ok`, so the initial evidence points to a frontend/browser delivery/rendering and production-entrypoint gap rather than a pure vector/LLM backend no-result condition.

## User / Business Impact

- Authenticated users can perceive smart search as hung: the button/progress reaches `92%` (`Собираю карточки`) without visible cards.
- The main production domain does not expose the working search page at `/poisk/`.
- The production root CTA points to an older preview build that lacks the dedicated search page.
- This undermines the product promise of a reliable static-site smart search MVP.

## Detection

- User report on 2026-07-02 after several live searches.
- Playwright/DB investigation mapped the reported queries by SHA-256 query hash because raw query text is intentionally not stored.
- Observability gap: Edge audit rows record backend success and stage timings, but the frontend has no durable client-side event for `response received`, `cards rendered`, `render error`, or `stuck at progress`.

## Timeline

- 2026-07-02 ~20:14 UTC: `На природу с детьми` backend audit row `ok`, total `2388ms`, LLM `1691ms`.
- 2026-07-02 ~20:15 UTC: `искусство у моря` backend audit row `ok`, total `3862ms`, LLM `2938ms`.
- 2026-07-02 ~20:17 UTC: `в пятницу бесплатно` backend audit row `ok`, total `2507ms`, LLM `1164ms`.
- 2026-07-02 investigation: Playwright reproduced all three queries on the fresh preview successfully; `искусство у моря` reproduced slower when the first Gemini Flash-Lite lane timed out at `3500ms` and the second lane succeeded.
- 2026-07-02 investigation: `https://kenigevents.ru/poisk/` returned `404 NoSuchKey`; root CTA linked to `preview-20260628-event-pages-v48-pgvector-gemma-kaggle` where `/poisk/` is absent.

## Root Cause

Open. Current evidence supports multiple contributing roots:

1. The current JSON response path waits for embedding + vector RPC + digest fetch + LLM verifier before returning any cards, so users can sit at the synthetic frontend `92%` progress while the backend is still finishing LLM verification.
2. The frontend has CSS for `.authorized-search__skeletons`, but no skeleton DOM/toggle is wired in `AuthorizedEventSearch.astro`; therefore there is no card-shaped shimmer/halo placeholder while results are being verified.
3. Production entrypoint promotion is incomplete: root `/poisk/` is not published and root CTA points to an older preview build without the search page.
4. Client-side observability is insufficient to prove whether reported stuck sessions failed on network delivery, stale JS, render exception, or user impatience during a slow LLM lane.

## Contributing Factors

- LLM verifier is the dominant latency stage; recent rows show LLM taking from ~1s to multi-second timeouts before fallback/lane retry.
- Query verifier prompt currently includes up to 20 candidates and ~13–14k prompt chars.
- A single slow Gemini lane can consume the full `EVENT_SEARCH_LLM_LITE_TIMEOUT_MS` before another key succeeds.
- Static search uses a preview prefix in production-like traffic, so root routing/promotion must be verified separately from preview health.

## Automation Contract

### Treat as regression guard when

- Changing `site/src/components/AuthorizedEventSearch.astro` loading/progress/rendering behavior.
- Changing `supabase/functions/event-search/index.ts` response mode, LLM verifier, vector/LLM sequencing, or timeouts.
- Promoting or repointing static site preview builds/root links involving `/poisk/`.
- Changing personalization Supabase search quota/audit/timing RPCs.

### Affected surfaces

- `site/src/components/AuthorizedEventSearch.astro`
- `site/src/layouts/EventLayout.astro` search loading CSS
- `supabase/functions/event-search/index.ts`
- personalization Supabase `event_search_requests` audit rows
- static Object Storage/CDN routing for `/poisk/` and root CTA
- browser/mobile Playwright smoke path

### Mandatory checks before closure or deploy

- Live Playwright mobile E2E on a production URL: auth session → submit `На природу с детьми` → cards rendered → scroll through cards → open a card.
- Same for `искусство у моря` and `в пятницу бесплатно`, with captured browser timings.
- Backend audit rows must show stage timings and model/lane attempts for the three queries.
- UI must show a card-shaped shimmer/halo placeholder while the backend is still working, not only a button progress bar.
- `/poisk/` production entrypoint or root CTA must point to the build under test; no stale old preview link for the primary user path.
- If vector-first rendering is implemented, verify cards appear after vector stage before LLM refinement completes or degrades.

### Required evidence

- Playwright artifact JSON/screenshot under `artifacts/codex/`.
- Redacted event-search audit rows with `timings_ms`, `llm_model`, `llm_attempts`.
- Public URL tested.
- Deployed SHA/release path if a fix is deployed.
- Confirmation that fix is reachable from `origin/main` if promoted to production.

## Immediate Mitigation

None deployed yet. Investigation confirms backend success for reported queries on the fresh preview and identifies the missing production entrypoint plus missing frontend skeleton wiring.

## Corrective Actions

Pending.

## Follow-up Actions

- [ ] Implement card-shaped shimmer/halo placeholder in `AuthorizedEventSearch.astro` and verify it appears during slow search.
- [ ] Split response path into vector-first render plus LLM refinement, or add a hard fast fallback that returns vector/possible cards before LLM lane timeout blocks the UI.
- [ ] Add client-side telemetry for search submit/response/render/error/stuck states without storing raw query text.
- [ ] Promote or repoint production `/poisk/` and root CTA to the current build.
- [ ] Tune LLM verifier candidate window/timeouts/cache so first visible result target is consistently sub-2s when vector candidates exist.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Keep this record as the regression contract for static smart-search perceived hangs, frontend loading placeholders, and production search entrypoint promotion.
