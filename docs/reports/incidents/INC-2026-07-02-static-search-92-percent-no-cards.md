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

- 2026-08-08 Android run `31273078355` completed broker callback, same-storage
  auth and the first real vector Search response, then failed only at
  `search_real_scroll_missing`: UiAutomator2 `mobile: scrollGesture` acknowledged
  the command but did not move Chrome document `scrollY`. iOS run `31273246165`
  also completed callback/WebKit attachment and reached the Search input, but a
  WebKit click followed by web-context `isKeyboardShown()` produced false
  keyboard evidence. Both failures exposed feature-local mechanics that were
  still weaker than the accepted OTP mobile input path.
- 2026-08-08 follow-up Android run `31274053708` proved that switching the W3C
  swipe itself to `NATIVE_APP` was insufficient: its coordinates had already
  been calculated from WebView `getWindowSize()`, so CSS/web viewport geometry
  was applied as native device pixels and again produced zero DOM `scrollY`.
  Official UiAutomator2 guidance requires native actions to consume native
  coordinates; viewport measurement is now inside the same native context.
- 2026-08-08 iOS run `31274142041` failed before product navigation: its single
  Appium new-session POST exhausted the full 300-second cold-start budget. This
  is an unambiguous simulator/WDA infrastructure failure, not Search or keyboard
  evidence; no automatic second session was created with the issued credential.
- 2026-08-08 Android run `31274932745` is the first terminal L2 PASS on the
  accepted immutable Search target: all three query families produced 10/10
  request/response/route receipts, validation produced zero POST, cache repeats
  had zero provider attempts, pagination stayed duplicate-free, and native
  1080x2400 W3C swipes produced positive DOM scroll deltas. The native viewport
  correction is therefore live-proven rather than only unit-tested.
- 2026-08-08 iOS run `31275022837` repeated the exact pre-product 300-second
  WebDriver session-create timeout with empty query cases/counters. Official
  XCUITest documentation confirms startup retries are ineffective with
  `usePreinstalledWDA`; Search now reuses OTP's single restart pattern only when
  a closed receipt proves callback/search side effects are zero, and reduces the
  local Appium log to phase booleans before deleting it.
- 2026-08-08 iOS run `31276217370` proved the WDA/session recovery path itself:
  WebDriver session creation, native Safari stabilization, WebView attachment,
  broker callback authorization and Search surface all completed. It then
  failed in scroll preparation because XCUITest returned its exact
  `Did not know how to dismiss the keyboard` response. OTP already treated
  Safari `hideKeyboard` as best-effort; the shared helper had accidentally made
  Search stricter. The shared helper now tolerates only that exact iOS response,
  while real scroll delta/final-card acceptance still fails closed.
- 2026-08-08 iOS run `31277104640` passed WDA/session startup, browser callback,
  same-storage auth, Search input/submit and the corrected keyboard-dismiss
  boundary, then failed only at `search_real_scroll_missing`. Twenty-four
  successful XCUITest `mobile: scroll` calls left Safari DOM `scrollY`
  unchanged and the final card invisible. The command is a native table/control
  shortcut, not browser-document evidence; iOS Search now uses the same
  native-viewport W3C touch path already live-proven by Android.

- 2026-08-08 exact-main degraded browser journey полностью прошёл UI/Search
  контракт для трёх запросов, но GitHub job завершился красным после journey:
  serialized browser runtime probe не переносил безопасный `receipt_id`, хотя
  root-wrapper требовал его перед owner-scoped receipt RPC. Regression теперь
  исполняет реальный serialized probe и доказывает сохранение receipt identity.
- 2026-08-08 два exact-current-SHA candidate run полностью собирали 1,339 Astro
  pages, но после 17–19 минут падали на `Search corpus/catalog revision
  mismatch`: mutable vector receipt проверялся до online snapshot и мог уже не
  соответствовать полному каталогу snapshot. Третий run с успевшим обновиться
  receipt прошёл production/candidate gates. Prevention теперь замораживает
  одно чтение receipt после snapshot, вычисляет exact full-catalog revision до
  Kaggle и локально откладывает stale pair как retryable.

- 2026-08-08 the new cross-platform harness completed a real browser vector
  journey on the working immutable candidate for all three regression queries:
  one POST per submit, eight response IDs mapped to eight rendered cards, real
  scrolling, pagination/deduplication, cache repeat and typed-empty validation
  with zero POST all passed. Evidence was sanitized and the auth state removed.
- 2026-08-08 the first exact-current-SHA candidate reached the final production
  packaging gate but failed because `build:secret-candidate` correctly required
  Search catalog/corpus revisions. Root cause: the dedicated Fly vector sync
  report computed those revisions and complete coverage, but its v1 durable
  receipt discarded them and the Kaggle handoff never packaged the receipt.
  The repair upgrades that owner receipt to v2 and hands it to Kaggle without
  re-enabling duplicate vector writes in the static builder.
- 2026-08-08 the repaired exact-SHA candidate passed Kaggle, but the 5400-second
  outbox deadline expired during create-only publication/verification after all
  3,314 private objects were uploaded. Durable recovery completed that exact
  candidate successfully. The runtime contract is now four hours end to end,
  while the remote Kaggle wait remains independently bounded at 90 minutes.

- 2026-08-07 recovery audit: public `/poisk/` still returned Object Storage 404,
  while the durable accepted immutable candidate resolved to build
  `production-secret-20260807T213802-a8a9b11e`, repo SHA
  `e7f02bf83f4b94d250be7cc6b792495de3be1984`, and served `/poisk/` with Search enabled.
- 2026-08-07 no-mail fixture probe on that exact candidate passed real
  `auth.getUser`, one owner-filtered `user_saved_event` RLS read, session
  restoration and cleanup with `/auth/v1/otp=0`, mail `0/0`.
- 2026-08-07 direct authenticated Edge recovery probe ran all three mandatory
  queries in vector-only mode: every request returned HTTP 200, eight useful
  IDs and `llm_requested=false`, `llm_used=false`, `llm_attempts=0`; repeating
  the first query returned the same IDs with a result-cache hit. This proves the
  live vector baseline, but is not the final browser/card/revision receipt for
  the new harness SHA.

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
5. Static release barrier проверял только event subset из request payload до
   создания immutable snapshot. Unrelated full-catalog drift поэтому проходил
   precheck и обнаруживался лишь после дорогого remote Astro build.
6. Первый unattended GitHub Search canary 2026-08-08 прошёл GitHub OIDC и
   atomic issuance claim, но broker вернул `503`: он вызывал raw GoTrue Admin
   REST с supabase-js-shaped `options.redirectTo` и ожидал SDK wrapper
   `properties.*`. Реальный endpoint вернул `200` с плоскими `email_otp` и
   `action_link`, поэтому успешная no-mail выдача ошибочно трактовалась как
   invalid issuer response.
7. Full-site secret candidate freezes Search revisions before a 2–3 hour
   build/publication path. A live catalog change during that interval makes an
   otherwise healthy multi-query UI journey fail only at the final exact
   revision assertion. Iterative Search debugging must therefore be separated
   from the final full-release revision gate.

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

Partial mitigation deployed to the working preview path and Supabase Edge Function on 2026-07-02: smaller first-page verifier batches, offset-based pagination and visible shimmer card placeholders. The root `/poisk/` promotion gap remains open.

## Corrective Actions

- Reduced the online verifier first page to `limit=8` / `candidate_window=10` based on a live batch-size probe.
- Changed the Edge Function default verification window fallback from 20 to 10 and made pgvector search honor `offset` for subsequent batches.
- Exposed `has_more` while later vector windows remain available.
- Wired the existing shimmer-card CSS into actual search skeleton DOM/JS so users see card-shaped loading placeholders.
- 2026-07-22 regression hardening: restored immediate initial skeleton display,
  made it structurally match the canonical large Search card, kept provisional
  vector candidates behind that stable loading surface, and moved the visible
  monotonic progress fill into the submit button. The semantic progressbar
  remains available to assistive technology. Public preview evidence is still
  required before this incident can close.
- 2026-08-08 release hardening: Search receipt теперь snapshot-scoped,
  hash-receipted и сверяется с exact full exported catalog до remote launch;
  replacement mutable receipt после precheck не влияет на handoff.
- 2026-08-08 auth broker recovery: raw GoTrue request использует
  `redirect_to`, плоский ответ проверяется напрямую, а SDK-wrapped shape остаётся
  совместимым fallback. Regression test фиксирует точные request fields и не
  допускает возврат к `options.redirectTo` на raw endpoint.
- 2026-08-08 debug-cycle correction: manual Search canaries may use a
  `live_consistent` policy that keeps exact page SHA, stable live revisions and
  owner-scoped receipts mandatory without rebuilding 1300+ unrelated pages.
  Scheduled/post-deploy acceptance remains `release_exact` and blocking.
- 2026-08-08 cache-debug correction: an explicitly manual
  `live_consistent + cached_vector` journey may bootstrap a cache entry cleared
  by corpus updates, but must prove a same-journey repeat hit with zero provider
  attempts. Strict scheduled/release acceptance still rejects the initial miss.
- 2026-08-08 iOS L2 correction: the shared exact-target resolver no longer uses
  Bash-4-only `mapfile`, so a macOS Bash 3.2 runner reaches session issuance and
  Safari/Appium instead of failing before the Search journey.
- 2026-08-08 mobile transport correction: Android/iOS capabilities force
  WebDriver Classic, preventing WebdriverIO from selecting Appium's incomplete
  BiDi surface and failing on `script.addPreloadScript` before UI acceptance.
- 2026-08-08 iOS cold-start correction: the single Appium new-session request
  has a five-minute client budget and no ambiguous automatic retry, instead of
  WebdriverIO aborting a valid cold simulator/WDA launch at 120 seconds. This
  historical no-retry policy was superseded only after runs `31274142041` and
  `31275022837` both exhausted the outer five-minute bound before callback:
  the current policy permits one fail-closed Appium restart with retained
  first-attempt evidence as documented below.
- 2026-08-08 mobile scroll correction: L2 uses a bounded sequence of real
  Appium touch gestures until the final rendered card intersects the viewport;
  a single gesture is no longer treated as sufficient for an eight-card page.
- 2026-08-08 mobile callback correction: the device adapter waits for the
  returned `/poisk/` surface to become authorized before performing the
  same-storage reload, so it cannot interrupt one-shot session persistence.
- 2026-08-08 mobile transport correction: Search no longer owns a weaker copy
  of Android/iOS Appium capabilities. It consumes the neutral profile extracted
  from terminal OTP acceptance. iOS therefore launches Safari native-first,
  handles only the exact allowlisted first-run dialog, and attaches WebKit with
  the proven 60-second/120-probe bounds instead of XCUITest's five-second
  default; the live Search CI failure occurred at 5.749 seconds. The project
  `mobile-web-e2e` skill makes shared-transport discovery mandatory for future
  mobile scenarios.
- 2026-08-08 mobile auth callback correction: two Android runs reached the
  exact target but could not restore a session because the broker's default
  hosted confirmation GET returned an implicit URL fragment while static auth
  deliberately sets `detectSessionInUrl=false`. The runner now validates the
  broker confirmation URL, converts only its one-time hash/type into the
  allowlisted target callback, and lets `StaticSiteAuth.verifyOtp` persist the
  session inside the real device browser. No access/refresh token crosses the
  runner and raw Appium logs are deleted rather than uploaded.
- 2026-08-08 mobile input/gesture correction: the neutral mobile-web transport
  now owns exact native iOS web-input focus, native keyboard polling,
  restore-on-finally context switching, IME dismissal and absolute-coordinate
  W3C touch swipes. Search and OTP consume these same primitives. Android no
  longer treats a native-scrollable UiAutomator2 shortcut as Chrome document
  input, and iOS no longer treats WebKit focus acknowledgement as native
  software-keyboard proof.
- 2026-08-08 native viewport correction: the Android swipe helper now resolves
  its ratio-based start/end points from `getWindowSize()` only while already in
  `NATIVE_APP`. A sanitized failure receipt retains numeric viewport/gesture
  geometry, gesture count and DOM `scrollY` delta so another no-op cannot remain
  an undifferentiated `search_real_scroll_missing` artifact.
- 2026-08-08 iOS startup observability/retry correction: the shared mobile
  layer converts a failed Appium session creation into an allowlisted receipt
  containing only stage, elapsed time, attempt number and simulator/WDA
  booleans; a truncated log is explicitly non-retryable. The Search workflow
  may restart Appium exactly once only before the
  device callback is opened and before any Search traffic, matching the accepted
  OTP infrastructure-retry boundary without copying OTP feature mechanics.
- 2026-08-08 iOS keyboard-dismiss correction: Search and OTP now share the same
  Safari best-effort boundary for the exact unsupported WDA hide-keyboard
  response. Lifecycle evidence identifies input, terminal, keyboard-dismiss and
  scroll phases separately; an unsupported hide command cannot itself pass the
  scenario because the following native scroll must still move the DOM and
  reveal the final card.
- 2026-08-08 cross-platform browser-scroll correction: Android Chrome and iOS
  Safari now share `performNativeTouchSwipe`; both resolve geometry in
  `NATIVE_APP`, dispatch W3C touch actions, restore WebView and require positive
  DOM movement plus final-card visibility. Neither UiAutomator2
  `mobile: scrollGesture` nor XCUITest `mobile: scroll` can satisfy Search page
  acceptance.

## Follow-up Actions

- [x] Implement card-shaped shimmer/halo placeholder in `AuthorizedEventSearch.astro` and verify it appears during slow search.
- [ ] Split response path into true vector-first render plus LLM refinement, or add a hard fast fallback that returns vector/possible cards before LLM lane timeout blocks the UI.
- [ ] Add client-side telemetry for search submit/response/render/error/stuck states without storing raw query text.
- [ ] Promote or repoint production `/poisk/` and root CTA to the current build.
- [ ] Tune LLM verifier candidate window/timeouts/cache so first visible result target is consistently sub-2s when vector candidates exist.

## Release And Closure Evidence

- deployed SHA: commit `0c9fd9e6` on branch `recovery/static-site-smart-search-full-20260701`; Edge Function `event-search` deployed to personalization Supabase project `epyznmylqmchteykjsqj` from the same diff before commit.
- deploy path: static preview `https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/poisk/` (`npm --prefix site run build:preview`, `check:preview`, `deploy:preview` passed; public preview verification ok).
- regression checks: live batch-size probe artifact `artifacts/codex/prod-smart-search-20260702/window-tuning-live.json`; post-deploy mobile Playwright artifact `artifacts/codex/prod-smart-search-20260702/post-deploy-batch-smoke.json`.
- post-deploy verification: query `искусство у моря` on public preview returned first page in `2572ms` backend total with `limit=8`, `candidate_window=10`, `retrieved_count=10`, `has_more=true`; skeleton became visible at `96ms` and hid after result cards rendered. “Показать ещё” sent `offset=10` and appended cards.

## Prevention

Keep this record as the regression contract for static smart-search perceived hangs, frontend loading placeholders, and production search entrypoint promotion.

For mobile Search, terminal acceptance additionally requires that input and
scroll evidence cross the native/WebView boundary through
`site/e2e/mobile-web/appium-browser.mjs`: exact native iOS input match + keyboard
observation and a shared Android/iOS native W3C finger swipe + restored WebView
`scrollY`. Feature-local copies, `mobile: scrollGesture` for Chrome document
content, and `mobile: scroll` for Safari document content are regressions.
