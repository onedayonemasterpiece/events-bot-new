# INC-2026-07-02 Static smart search stalls at 92% without cards

Status: open
Severity: sev2
Service: Static site authorized smart search (`/poisk/`, personalization Supabase Edge Function `event-search`)
Opened: 2026-07-02
Closed: —
Owners: static site / personalization search
Related incidents: —
Related docs: `docs/features/static-site-pages/smart-vector-search/README.md`, `docs/features/unsigned-personalization/authorized-event-search.md`, `docs/features/unsigned-personalization/semantic-vector-retrieval.md`, `docs/operations/release-governance.md`

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

- 2026-08-09 browser+iOS workflow
  [`31338952963`](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31338952963)
  crossed the native-sheet fix but exposed two independent iOS infrastructure
  boundaries. Attempt 1 timed out while creating the WebDriver session after
  300 seconds and produced a closed `UNKNOWN_IOS_INFRA` receipt with zero
  broker/Auth/Search operations. Production health had not actually wired the
  already-proven OTP rule that permits one Appium/WDA restart only after such a
  zero-side-effect, cleaned-session receipt; it now reuses that shared contract
  and records only its closed retry metadata. The explicitly safe rerun then
  passed native Safari/WebKit preflight and callback authorization with zero
  Search POSTs, but Mobile Safari returned no usable result from the synchronous
  Promise-based `getUser`/owner-RLS probe. That Search-specific proof now uses
  WebDriver's asynchronous callback command inside the same neutral OTP/Search
  Appium session. Browser stayed `HEALTHY/PASS`; iOS acceptance remains pending
  a fresh merged-SHA run, so incident acceptance is still `1/2` and schedules
  remain disabled.

- 2026-08-09 merged-SHA browser+iOS run `31338416105` crossed the former
  `search_choice_action_missing` boundary. The exact native sheet was
  recognized and the final closed inspection after its single action reported
  zero sheet/title/action, but preflight ended as the now-specific
  `safari_first_run_ui_search_choice_dialog_stuck`. The default shared helper
  re-read the full XCTest hierarchy on every post-dismissal sample; four such
  reads could not supply three stable absence samples inside the five-second
  bound. Search still made zero broker/Auth/Search operations. The common
  OTP/Search transport now captures the clean-simulator source once, as the
  proven OTP adapter already did, while all post-click decisions use fresh
  exact element queries. A fresh merged-SHA iOS run remains required.

- 2026-08-09 workflow `31337041139` is the first accepted Stage-2 proof:
  browser and Android both completed `HEALTHY/PASS` against the same immutable
  target/runtime/backend/catalog/corpus identity. Each platform made exactly
  one HTTP-200 vector-only Search POST, rendered five response IDs as five
  cards, performed a real wheel/native scroll, opened the exact event route at
  HTTP 200 and observed zero LLM/pagination/receipt/storage or product-network
  errors. This closes acceptance `1/2`; automation remains default-off.

- 2026-08-09 browser+iOS workflows `31337291226` and `31337643487` both kept
  the browser green on the same product identity but stopped iOS before broker,
  Auth or Search in the fresh-simulator Safari first-run UI. WDA reported no
  current alert while the exact allowlisted search-choice title was present,
  so the shared transport rejected it as `safari_first_run_ui` and the old
  evidence truncation hid the closed suffix. Root-cause review found that the
  earlier OTP-to-neutral transport extraction had retained the safe native
  source reducer but had not wired it into default Search preflight or a
  non-alert sheet action. The common OTP/Search helper now accepts only one
  exact native sheet with one exact `Продолжить` button, revalidates its element
  id before click, rejects ambiguity, and exposes only closed counters and a
  safe infrastructure code. A new merged-SHA browser+iOS live proof is still
  required; these two zero-side-effect infrastructure runs are not Search
  product failures and do not close acceptance `2/2`.

- 2026-08-09 merged-SHA run `31336453210` crossed the previous Android
  observer blocker: Android completed Auth/owner-RLS and exactly one real
  vector-only Search POST returned HTTP 200 with five IDs equal to five cards,
  cache hit, zero LLM/pagination/receipt/storage and 8,219 observed Supabase
  bytes. It stopped before native scroll/event navigation because the generic
  Appium diagnostics counted two failed requests; its closed class receipt
  simultaneously proved zero failed document/Auth/Edge/REST/RPC requests and
  zero error responses. These were therefore unrelated/decorative subresources,
  not Search product failures. Appium diagnostics now correlate private CDP
  request metadata across drains and count only target-document or allowlisted
  Supabase failures, excluding the same exact disposable capability probes as
  browser. Browser in the workflow again completed full `HEALTHY/PASS` with
  one POST, five cards, event HTTP 200, zero errors and 9,343 bytes. Android is
  still not terminal acceptance until the full scroll/event journey passes.

- 2026-08-09 merged-SHA run `31335563487` again completed browser
  `HEALTHY/PASS` with one HTTP-200 Search POST, five response IDs equal to five
  cards, cache hit, real wheel/event HTTP 200, zero forbidden/network/console
  activity and 9,343 observed Supabase bytes. Android proved that the corrected
  Appium `/goog/cdp/execute` route works: it installed the pre-document probe,
  completed callback authorization plus `getUser`/owner-RLS and accounted
  3,339 bytes, but made zero Search POSTs. It then stopped as
  `UNKNOWN_ANDROID_INFRA/search_physical_observation_missing` because the
  whole-cell idle fence still relied on ChromeDriver response/terminal events
  after Auth; the same bucket again supplied request starts only. The observer
  is therefore extended across every Android document and all allowlisted
  Supabase Auth/Edge/REST/RPC operations, with host-side cumulative counters,
  exact Search/Storage/receipt counts and closed body bytes. Protocol logs
  remain navigation/diagnostic evidence, but are no longer the Android byte
  completion authority. This run is browser proof only; a fresh merged-SHA
  browser+Android run is still required.

- 2026-08-09 merged-SHA run `31335122827` completed browser
  `HEALTHY/PASS`: one HTTP-200 Search POST, five response IDs equal to five
  cards, real wheel scroll, same-origin event HTTP 200, zero LLM/pagination,
  zero console/network errors and 9,343 observed Supabase bytes. This also
  verified that the prior Edge/RPC failures were disposable probe-race noise.
  Android completed emulator/Appium/WebView preflight but stopped before Auth
  callback and Search because Appium returned 404 for ChromeDriver's legacy
  `/chromium/send_command_and_get_result` route. Appium base registers the
  current Chromium extension as `/goog/cdp/execute`; the adapter now uses that
  route and emits a closed typed infrastructure code if it is unavailable.
  Browser evidence is valid, but the workflow is not Android acceptance.

- 2026-08-09 diagnostic run `31334260547` closed the repeated Android boundary
  as `UNKNOWN_ANDROID_INFRA/mobile_auth_terminal_bytes_timeout_mixed_request_only`:
  the emulator/Appium/WebView preflight and product authorization completed,
  zero Search POSTs were made, but ChromeDriver's performance bucket exposed
  only Auth request starts and never response/terminal events. This disproved
  both the prior `loadingFinished`-only and cancellation hypotheses. Android
  now installs a pre-document, allowlisted Auth fetch byte observer through the
  exact Chromium CDP command; it keeps only closed counters and total bytes and
  is removed after callback. The same run's browser Search returned HTTP 200
  with five IDs equal to five cards and zero LLM/pagination, but was stopped by
  one failed Edge-class and one failed RPC-class diagnostic before scroll. The
  only expected aborts now excluded are the resilient transport's three exact
  disposable capability-probe paths; final operations remain strict. Because
  the prior closed evidence intentionally omitted the paths, whether those two
  failures were expected probe races must be verified by the next live run.
  This run is diagnostic evidence, not mobile acceptance; a new exact
  merged-SHA browser+Android run is required.

- 2026-08-09 run `31333753972` again produced a complete browser
  `HEALTHY/PASS`, but Android reproduced
  `mobile_auth_terminal_bytes_timeout` before Search. Handling
  `Network.loadingFailed` was correct contract hardening but did not close the
  live pending record, so the prior cancellation hypothesis was incomplete.
  The next bounded diagnostic keeps the same zero-Search fail-closed behavior
  and emits only Auth path class plus request/response/data phase; no request
  id, URL, token, body, or raw protocol/error text is retained.

- 2026-08-09 run `31333074131` produced the first current-contour complete
  browser acceptance: `HEALTHY/PASS`, exactly one HTTP-200 Search POST, five
  response IDs equal to five rendered cards, real scroll, same-origin event
  route HTTP 200, zero LLM/pagination/receipt/storage activity, and 9,342
  client-observed Supabase bytes. Android passed emulator/Appium/WebView
  preflight and reached the authorised callback, but made zero Search POSTs and
  stopped as `UNKNOWN_ANDROID_INFRA/mobile_auth_terminal_bytes_timeout`.
  Contract review against the CDP terminal-event contract found that the
  tracker handled `Network.loadingFinished` but ignored `Network.loadingFailed`;
  that cancellation branch could therefore remain pending forever. The tracker
  now closes it using only summed `Network.dataReceived` encoded bytes and drops
  request ids, URLs, bodies and raw error text. Run `31333753972` subsequently
  showed that this was valid hardening but not the complete live root cause.
  This run is browser evidence only and is not terminal mobile acceptance.

- 2026-08-09 run `31332306409` proved the Android Bash/Appium/context fixes far
  enough to complete the side-effect-free transport preflight, then stopped
  before callback/Auth/Search. Targeted review against WebdriverIO's current
  official contract found that standalone v9 removes deprecated JSONWP
  `getLogs` even with Classic forced; the adapter had treated the absent method
  as an unavailable Android performance bucket. It now installs only the exact
  Appium log command through pinned `webdriver@9.30.0`. Browser again completed
  one HTTP-200 Search with five matching rendered cards, zero LLM and cache
  hit, but stopped before scroll on two closed critical request failures. The
  prior artifact exposed only a total, so browser diagnostics now retain
  allowlisted document/Auth/Edge/REST/RPC counts and an internal sanitized
  failure code for the next bounded proof. No Android Search POST was made and
  this run is not terminal acceptance.

- 2026-08-09 live run `31331701360` proved the merged browser correction:
  browser finished `HEALTHY/PASS` on one real vector-only POST (HTTP 200), five
  response IDs exactly matching five rendered cards, zero LLM/pagination, an
  opened same-origin event route and 9,217 observed Supabase bytes. Android now
  correctly entered the checked-in Bash wrapper, booted the API-35 emulator and
  started Appium, but stopped in side-effect-free mobile infrastructure before
  broker/Auth/Search as `UNKNOWN_ANDROID_INFRA`. The fresh Chrome/WebView
  context was previously sampled only once and session-start diagnostics were
  discarded with the raw Appium log. Preflight now bounded-waits for both
  contexts and publishes only a closed Appium/Chrome startup classification.
  This run is a valid browser product proof but not the required Android
  terminal proof; the incident remains open.

- 2026-08-09 follow-up live run `31331011185` confirmed the first Android shell
  fix was incomplete: `android-emulator-runner` invokes each line separately
  with `/bin/sh -c`, so an inline Bash heredoc did not group the script. The
  workflow now calls one checked-in Bash wrapper as a single action command.
  Browser correctly made zero Search calls because the previous completed run
  still owned `search-cached-browser` under the 20-minute persona lease and the
  broker returned typed `UNKNOWN_AUTH_BROKER`; this was admission state, not a
  Search regression. Completion now shortens that active lease to the same
  two-minute encrypted replay window while preserving the uncompleted-owner
  crash bound and global workflow serialization. Run `31331011185` is not an
  acceptance proof.

- 2026-08-09 first merged Stage-2 live run `31330520373` executed against the
  accepted target after the broker migration, exact Edge deployment and
  exact-main Fly deployment. Browser Auth/getUser/owner-RLS succeeded; its one
  real vector-only Search POST returned HTTP 200 with five response IDs that
  exactly matched five rendered cards, `LLM=0`, pagination `0`, cache telemetry
  `stored` and 9,162 client-observed Supabase bytes. The cell then false-failed
  `BROKEN_SEARCH_REQUEST` because two unrelated failed decorative subresources
  were included in the generic Playwright failure counter. Android booted the
  API-35 emulator but the third-party action ran the multiline script through
  `/usr/bin/sh`, where `set -o pipefail` exited before Appium, broker, Auth or
  Search. The browser diagnostic is now scoped to target-document and
  allowlisted Supabase failures; the Android script explicitly enters Bash.
  This run is regression/root-cause evidence, not either of the two terminal
  daily acceptance proofs, so the incident remains open pending clean
  browser+Android and browser+iOS runs on the same product identity.

- 2026-08-09 PR #441 merged as
  `dd5ffc2eb5327cb52eb62e232e1e927dbe4c9c66`. Stage 2 is implemented in
  PR #451 with two bounded schedules, one-query browser/Android/iOS journeys,
  platform-bound broker admission, byte/redaction gates and typed reporting.
  Deterministic regression suites are green, but no Stage-2 production-health
  workflow has run yet. The incident therefore remains open and
  `PRODUCT_HEALTH_UNCONFIRMED`; exact live run IDs and platform metrics must be
  appended before closure.

- 2026-08-09 production-health architecture stage 1 separated deterministic
  CI, current-target product health and selective release qualification. The
  noisy automatic cached/cold/LLM/mobile schedules and generic issue reporter
  were disabled; new health/qualification workflows remain dry/manual until
  the bounded stage-2 live validation. This is an orchestration correction, not
  incident closure: `PRODUCT_HEALTH_UNCONFIRMED`. Canonical contract and AS-IS
  evidence are in `docs/features/static-site-pages/smart-vector-search/README.md#16`.

- 2026-08-09 browser run `31293895462` is a terminal Search-only
  `cold_vector + live_consistent` PASS on the accepted immutable candidate:
  all three incident query families produced 10/10 request/response/route
  receipts, the first family paginated without duplicate ids/families, every
  response id matched a rendered card, all real wheel-scroll gates passed and
  every same-journey repeat was a cache hit. The exact-SHA browser fixture also
  passed `auth.getUser`, the owner-scoped RLS probe, session restoration,
  cleanup and redaction with product OTP `0` and external mail `0/0`; all ten
  owner-scoped server receipts matched.
- 2026-08-09 browser run `31294346370` is a terminal
  `degraded_vector_fallback + live_consistent` PASS. All three query families
  returned useful vector pages with actual mode `degraded_vector_fallback` and
  the exact closed verifier status `degraded:deterministic_canary_failure`;
  10/10/10 request/response/route receipts, pagination, final-card scroll,
  cache-hit repeats and validation zero-POST all remained green. This proves
  the deterministic LLM failure path without spending an LLM attempt or
  weakening the ordinary result-card assertions.
- 2026-08-09 iOS job in run `31293896454` is the first terminal Mobile Safari
  Search PASS: the shared native-first OTP transport created a fresh simulator
  session, completed the callback in Safari, restored the same-storage session,
  focused/submitted through the native keyboard and completed all three query
  families with 10/10/10 request/response/route receipts. Pagination,
  response-id/card matching, duplicate rejection, validation zero-POST and
  three cache-hit repeats passed; every final-card gate used the accepted
  application-level `xcuitest_native_swipe` route and positive WebKit DOM
  movement. The uploaded artifact contains `.redaction-ok` and no raw Appium,
  target bearer, credential or session material.
- 2026-08-09 the immediately parallel Android job in run `31293896454` moved
  Chrome DOM by 5516 px through 24 real 1080x2400 W3C native-touch swipes but
  did not observe the final card in the viewport, so the strict scroll gate
  correctly rejected it as `search_real_scroll_missing`. This is isolated from
  Search/API correctness: exact-target run `31293081092` had just completed all
  three Android query families, pagination, cache repeats and positive final
  card gates, then failed only at the independently expected stale-release
  revision assertion. One unchanged Android-only control rerun is required
  before changing the already live-proven shared gesture mechanics.
- 2026-08-09 unchanged Android-only control run `31294660651` reproduced the
  same boundary: 24 accepted absolute-coordinate W3C native-touch swipes moved
  Chrome DOM by 6162 px, but the final card was not observed in the accepted
  viewport gate. Official UiAutomator2 guidance confirms W3C Actions with valid
  absolute native coordinates are the supported route for complex gestures;
  the positive DOM delta independently proves delivery. The only bounded lever
  supported by those receipts was the feature helper's 24-gesture ceiling, so
  it is now 40, matching the accepted browser wheel bound while final-card and
  positive-DOM predicates remain unchanged. The old receipt did not retain a
  card rect, so it could not distinguish a still-below tall card from transient
  visibility/layout timing and must not be presented as sole-cause proof.
- 2026-08-09 post-change Android run `31295423796` is a terminal
  `cached_vector + live_consistent` PASS on the current candidate: all three
  query families produced 10/10/10 request/response/route receipts, pagination
  stayed duplicate-free, every repeat was a cache hit, validation made zero
  POST and all final-card gates passed through real `w3c_native_touch`. The
  three scrolls needed only 4/6/9 gestures with positive 4.0–5.1k px DOM
  deltas; this proves the wider loop did not regress accepted mechanics, but it
  does not claim that gesture 25+ was required in the successful sample.
- 2026-08-09 browser run `31295063983` is a terminal bounded
  `cold_vector_llm + live_consistent` PASS. The workflow intentionally limited
  the journey to three non-paginated queries; each used exactly one embedding
  and one LLM provider attempt, returned useful cards in actual mode
  `cold_vector_llm`, and the immediate repeat was a cache hit. The server daily
  budget reservation remained authoritative and no unbounded provider lane was
  introduced.
- 2026-08-09 browser run `31295732615` is a terminal manual
  `cached_vector + live_consistent` PASS. Its three initial responses were
  safely re-stored with zero embedding/vector/LLM attempts after catalog cache
  invalidation, then all three immediate repeats were cache hits with the same
  zero provider deltas. The strict scheduled run `31294951925` independently
  proved all three *initial* responses were cache hits with zero provider
  attempts, then failed only at the known stale candidate-vs-live revision
  barrier. Thus the cache behavior is proven while release-exact acceptance
  remains correctly unclaimed until the next candidate publishes.
- 2026-08-09 delayed scheduled runs overlapped manual runs using the same
  persona and were rejected by the broker with `409` while the prior 20-minute
  issuance claim was active. This is a scheduling/admission collision, not a
  Search or mailbox failure: waiting for claim expiry produced terminal PASS.
  These `409` runs must not be counted as product stability PASS and still need
  a neutral deferred/serialized schedule classification before blocking-mode
  threshold accounting.
- 2026-08-09 exact-release attempts `31297004355` and `31302524785`
  isolated a release-pipeline race rather than a Search failure. Both complete
  three-query journeys reached useful terminal cards but the mutable vector
  projection advanced while the immutable candidate was building, so the
  response receipts correctly rejected the older target revisions. Vector
  sync now defers while a static build has an active durable owner and for a
  bounded 15-minute window after candidate publication. The pending request is
  preserved and resumes after exact-target acceptance; this replaces the
  one-off operator defer used to prove the race on job `49936`.
- 2026-08-09 post-deploy run `31293081526` proved the accepted candidate's
  Search UI and server receipts for both `cold_vector` and deterministic
  `degraded_vector_fallback`, including useful vector cards with the verifier
  intentionally degraded. Both jobs failed only after the ten-response journey
  because the candidate froze catalog/corpus revisions before the later vector
  sync completed. The blocking terminal created observable incident issue
  `#425`; the fast live-consistent browser recovery then automatically closed
  the matching cold-browser alert `#392`.
- 2026-08-09 an exact Search candidate completed every Kaggle gate and wrote a
  terminal `done` ledger receipt, while Kaggle REST briefly still returned
  `RUNNING`. Exact-owner recovery deferred for five minutes, but a newer Smart
  Update fingerprint then superseded the terminal claim and launched a second
  full build. Root cause was `claim_static_site_build` treating any terminal
  ledger row as permission to replace the owner before recovery. Terminal
  owners now remain single-flight blockers until their exact recovery finishes
  and clears the claim.
- 2026-08-09 two exact-main Search-fix candidate attempts failed
  deterministically at the legacy `check:preview` gate after successfully
  exporting the current Search catalog and building 1,300+ preview pages. The
  exact assertion was an unconditional read of the expired
  `date-2026-08-08` route for real event `4211` (`Море внутри`). The release
  gate now uses that real rendered row only while its generated date route
  exists; later builds retain the same OCR-containment and structured festival
  medallion proof through immutable contracts, without resurrecting expired
  pages.
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
- 2026-08-08 follow-up iOS run `31277971410` disproved that cross-platform
  assumption without weakening acceptance: `w3c_native_touch` returned success
  24 times on the 393x852 native viewport, while Safari DOM delta remained zero
  and the final card stayed invisible. The accepted OTP adapter already records
  the same Safari/WebKit W3C delivery limitation. After the two no-op routes,
  the next implementation was bound to the official XCUITest contract:
  application-level `mobile: swipe` for a simple native swipe, while
  table/collection `mobile: scroll` remains forbidden.
- 2026-08-08 iOS run `31278836719` used that exact
  `xcuitest_native_swipe` route but again left DOM delta zero after 24 commands.
  The safe job log placed XCUITest's exact unsupported `hideKeyboard` response
  immediately before the no-op scroll, while the shared helper treated that
  response as success without re-reading keyboard visibility. The remaining
  boundary is therefore IME dismissal, not another scroll API: the helper now
  performs one official user-equivalent downward native swipe and must observe
  the keyboard absent before Search can start its upward document swipes.
- 2026-08-08 iOS run `31279646307` validated the new fail-closed classification:
  it stopped at `search_scroll_keyboard_dismiss` with
  `mobile_keyboard_dismiss_unconfirmed`; auth callback, Safari, WebView and
  Search surface were all confirmed, and no document swipe was attempted. A
  generic application swipe down therefore does not dismiss this Safari search
  keyboard. Search now uses the other official user-equivalent route: resolve
  exactly one allowlisted non-actionable `Найти событие` heading in native
  accessibility, tap its rect, then require IME absence. It cannot hit submit,
  resend, result text or a guessed coordinate.

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
8. The legacy preview release gate also bound an unrelated mobile-rail visual
   canary to the literal one-off route `date-2026-08-08`. Once that route
   correctly expired, every later production candidate failed before the
   Search-specific browser gate even though the Search page and corpus were
   healthy.

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
- 2026-08-09 preview-gate expiry correction: the `Море внутри` rendered rail is
  checked only when its source event's generated date route exists. After
  expiry, the gate still fails closed on the same OCR natural-geometry resolver
  contract and on the `listing_ready` / `festival` medallion manifest binding,
  but no longer demands a past page from the current release catalog.
- 2026-08-08 iOS L2 correction: the shared exact-target resolver no longer uses
  Bash-4-only `mapfile`, so a macOS Bash 3.2 runner reaches session issuance and
  Safari/Appium instead of failing before the Search journey.
- 2026-08-09 Android tall-card correction: the native-touch loop remains
  fail-closed on positive DOM movement plus final-card visibility, but its
  bounded ceiling is 40 rather than 24. Two live receipts proved the accepted
  W3C gesture moved Chrome 5.5–6.1k px while exhausting the old bound without a
  final-card observation; a unit regression requires a synthetic final card
  beyond gesture 24 to be reached rather than weakening the viewport predicate.
  Terminal run `31295423796` passed with fewer gestures and therefore validates
  compatibility, not a stronger causal claim about the prior card position.
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
- 2026-08-08 iOS keyboard-dismiss correction: Search and OTP now share a
  fail-closed transition for the exact unsupported WDA hide-keyboard response.
  It triggers one exact caller-bounded non-actionable native tap (or a downward
  swipe only when no safe target exists) and must then prove the IME absent; the
  response itself is never success. Lifecycle evidence identifies
  input, terminal, keyboard-dismiss and scroll phases separately, and the
  following document swipe must still move DOM and reveal the final card.
- 2026-08-08 iOS keyboard-target diagnosis: live run `31280455371` reached an
  authorized Search surface but found zero eligible visible exact
  `StaticText` headings and stopped before the fallback tap or document scroll.
  The
  shared helper now records only total/visible counts across four closed
  XCTest types on this fail-closed path; no hierarchy, label or page text is
  retained, and the diagnostic cannot authorize a tap.
- 2026-08-08 iOS product-focus correction: diagnostic run `31281345474`
  proved the exact heading mapped to `StaticText total=1` but `visible=0` after
  submit. Search now blurs only a validated non-pagination input before its
  single POST, matching the ordinary native Search/Enter completion lifecycle;
  validation retains focus and the adapter still requires native IME absence
  before any document swipe.
- 2026-08-08 cross-platform browser-scroll correction: Android Chrome and iOS
  Safari now share the routing primitive `performNativeDocumentSwipe`; Android
  dispatches absolute-coordinate W3C touch, while iOS uses the XCUITest
  application-level native swipe because Safari did not receive W3C pointer
  actions. Both restore WebView and require positive DOM movement plus
  final-card visibility. Neither UiAutomator2 `mobile: scrollGesture` nor
  XCUITest `mobile: scroll` can satisfy Search page acceptance.

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
observation and the shared platform-routed native document swipe + restored
WebView `scrollY`. Feature-local copies, `mobile: scrollGesture` for Chrome
document content, `mobile: scroll` for Safari document content, and Safari W3C
pointer acknowledgement without DOM movement are regressions.
