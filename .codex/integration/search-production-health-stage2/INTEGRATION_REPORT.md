# Search production-health Stage 2 integration report

## Base and safety

- Integration base: `origin/main@66bc0d43e36299417626f992021cfb7299ddf704`;
  it contains merged PR #441 at
  `dd5ffc2eb5327cb52eb62e232e1e927dbe4c9c66`.
- Branch/PR: `integration/search-production-health-stage2-20260809` / #451.
- PR #451 merged to `main` as `ad0a1f3bb12a63805aec65f52489151e3f382b83`.
- Live acceptance workflows so far: `1 / 2` accepted. Run `31337041139`
  completed browser+Android `HEALTHY/PASS` on an unchanged immutable product.
  Browser+iOS runs `31337291226` and `31337643487` stopped side-effect-free in
  the fresh-simulator Safari first-run sheet before broker/Auth/Search.
  Run `31338952963` then passed that common native startup path: attempt 1
  stopped during side-effect-free WebDriver session creation and its safe rerun
  passed Safari/callback but exposed the iOS synchronous Promise-return boundary
  in the Search-only owner proof, still before any Search POST.
  Merged-main run `31340311566` proved the newly wired one-time recovery itself:
  the first 300-second session timeout restarted Appium once with zero side
  effects and the second session passed Safari/callback. It then exposed an
  effective 1 ms XCUITest async-script timeout before the owner proof could
  settle; the adapter now explicitly sets the documented bounded 15 seconds.
- The post-merge activation migration and exact Edge/Fly deployments are
  complete; their verification is recorded below.
- PR #436: untouched and not a dependency.

## Integrated lanes

| Lane | Implementation | Receipt | Result |
|---|---|---|---|
| workflow/release marker | `d7eb39e97` | `3a55b8995` | two schedules, manual/runtime marker, no generation triggers |
| journey/evidence | `0938baed8` | `967c74c4e` | one query/POST, target pin, bytes, strict evidence |
| mobile transport | `583ea5d0a` | `c27b983c8` | real Appium preflight/scroll/card open/cleanup |
| broker | `a22f01b62` | `1e8188427` | platform identity, typed admission, migration |
| reporter | `70754d633`, `ec02e0fc2` | `a557a6d0a` | platform disposition and REST mutation |
| broker purpose/replay | `b3db5eff1`, `7954b1f98` | `ef00d10d9`, `330e20bcb` | distinct qualification persona, exact SQL identity, bounded replay and pg_cron erasure |
| whole-cell observer | `b72cedeba` | `c07da8303` | pre-Auth through event-route physical POST/bytes/forbidden traffic boundary |
| integration hardening | current branch | this report | Appium secret-log containment, exact Edge source-revision activation gate, reporter history/labels/aggregate and canonical docs |

## Deterministic acceptance

Required before live:

- Search production-health aggregate suite: **151/151 PASS** after the shared
  OTP/Search Safari native-sheet, pre-side-effect retry, async owner-proof and
  closed failure-evidence regressions;
- legacy Search harness: **32/32 PASS**;
- focused broker/HTTP/SQL/security suite **48/48 PASS**; combined broker,
  security and static source/release regression **135/135 PASS**; Edge contract
  group **26/26 PASS** and Auth
  Node **16/16 PASS**;
- static source-binding/release regression: **87/87 PASS**;
- workflow YAML and shell/node syntax;
- GitHub Actions semantic validation with actionlint 1.7.12, including the
  job/step context boundary for ephemeral Android/iOS Appium homes;
- broker migration plus its canonical SQL replay/expiry contract on ephemeral
  PostgreSQL 17, and diff check;
- independent checklist audit and fresh GitHub Actions.

## Production activation gate

1. Merge #451 into current `main` after green checks.
2. Apply migrations `20260809143602_static_site_auth_broker_platform_claims.sql`
   and `20260809191607_static_site_auth_broker_short_active_claim.sql` in
   order; verify the widened identity, v1 compatibility, two-minute completed
   claim lease and named minute `pg_cron` ciphertext-erasure job.
3. Add legacy, `search-production-health.yml@refs/heads/main` and
   `search-release-qualification.yml@refs/heads/main` to the broker workflow
   allowlist; set the exact event allowlist to
   `workflow_dispatch,schedule,repository_dispatch`; verify the three cached
   health personas plus distinct `search-cold-browser` account.
4. Generate/check the immutable `event-search` source digest, deploy
   `supabase/functions/event-search` from exact merged `origin/main` via the
   pinned Supabase CLI, and verify the side-effect-free HEAD contract exposes
   that exact `X-KenigEvents-Search-Revision` separately from the stable
   contract version.
5. Deploy exact merged `origin/main` through `scripts/deploy_fly_main.sh` with
   validation profile `none`.
6. Run manual `browser_android`, then manual `browser_ios`. Debugging runs are
   allowed by the owner, but every run remains one Search POST per selected
   platform and each failure is investigated rather than blindly retried.
7. Before enabling automation, require both platform sets HEALTHY/PASS and exact
   equality of target fingerprint/immutable tuple, site runtime SHA, backend
   revision and content/index generation ids across the accepted proofs.
8. Set `SEARCH_PRODUCTION_HEALTH_ENABLED=true`, then update the Search incident
   regression record and issue #431 with exact run IDs.

Current disposition: `STAGE2_IMPLEMENTED_LIVE_ACCEPTANCE_PENDING / PRODUCT_HEALTH_UNCONFIRMED`.

Acceptance is now `1/2`: run `31337041139` is the accepted browser+Android
proof. Runs `31337291226` and `31337643487` repeated an iOS-only infrastructure
failure with zero broker/Auth/Search operations. The shared OTP/Search Safari
transport now handles the allowlisted first-run UI as either one exact WDA
alert or one exact native sheet; a fresh merged-SHA browser+iOS workflow is the
remaining live gate. `SEARCH_PRODUCTION_HEALTH_ENABLED` stays absent until that
proof matches the first run's immutable product identity.

Follow-up run `31338416105` proved the native-sheet action but exhausted its
five-second stable-dismissal window by re-reading the multi-second XCTest source
for every absence sample. The shared helper now reuses the OTP pattern: one
clean source snapshot plus fresh exact element queries. No broker/Auth/Search
traffic occurred, so acceptance remains `1/2` pending one new iOS proof.

Run `31338952963` then proved that shared native-sheet path on a fresh simulator.
Attempt 1 timed out in WebDriver session creation with a closed zero-side-effect
receipt; production health now invokes the same single pre-side-effect
Appium/WDA retry boundary as OTP and records its safe receipt. The safe rerun
passed Safari/WebKit preflight and callback authorization but iOS returned no
closed result for the synchronous Promise-based owner-RLS probe. The Search-only
probe now uses WebDriver's asynchronous callback inside the same shared session.
It does not issue a second credential, product OTP or mail. Acceptance remains
`1/2` until a fresh merged-SHA browser+iOS workflow passes and matches the first
proof's immutable identity.

Merged-main run `31340311566` subsequently exercised that retry in one workflow:
attempt 1 stopped during side-effect-free session creation, the shared retry
restarted Appium once, and attempt 2 crossed Safari/WebKit plus callback. Its
Search-only async owner proof then failed with an observed 1 ms session script
timeout. The adapter now sets `script: 15000` immediately before that callback
and classifies WebDriver timeout/config/command failures as iOS infrastructure.
The run made zero iOS Search POSTs, so it is diagnostic evidence rather than the
second acceptance proof.

Steps 1–5 were completed from exact merged `main`: the v2 broker migration and
minute cleanup are active, broker event/workflow allowlists and distinct
personas were verified, Edge HEAD exposes
`sha256:7ab0bd272925e959531bdcf679e995c65ee672165bd05788d51f7d13d35875d7`,
and Fly reports the baked merge SHA with ready health/DB/disk checks. First
diagnostic run `31330520373` proved one real browser Search POST, five matching
cards, zero LLM/pagination and 9,162 observed bytes, then false-failed on two
decorative subresource failures. Android booted but `/bin/sh` rejected
`pipefail` before Appium or Search. Both causes now have deterministic fixes;
clean live acceptance is still pending.

Runs `31333074131` and `31333753972` each completed the full browser journey as
`HEALTHY/PASS` with one HTTP-200 Search POST, five response IDs equal to five
cards, real scroll, event route HTTP 200, zero LLM/pagination/receipt/storage
and less than 10 KiB observed Supabase traffic. Their Android cells completed
transport preflight and product authorization but stopped before Search on an
unclosed Auth network byte record. Diagnostic run `31334260547` then proved the
exact ChromeDriver boundary was `mixed_request_only`: response/terminal events
were absent even though product Auth completed. The Android adapter now uses an
allowlisted pre-document page observer for received Auth bytes and removes it
after callback; no body, URL, request id, token or session reaches evidence.
The same run's browser failure was one Edge-class plus one RPC-class request;
closed evidence intentionally omitted their paths. Only the three exact losing
disposable capability-probe paths are now excluded from product-network
failure, while final operations remain strict. The next live run must prove
whether this was that expected race. Neither cell from `31334260547` is
terminal acceptance, so the disposition remains unchanged pending a fresh
merged-SHA browser+Android proof.

Merged-SHA run `31335122827` then completed browser `HEALTHY/PASS` with one
HTTP-200 Search POST, five matching cards, real scroll/event HTTP 200, zero
console/network errors and 9,343 bytes. Android stopped before callback/Search:
Appium returned 404 for the legacy ChromeDriver
`/chromium/send_command_and_get_result` extension. The checked Appium base
contract registers `/session/:sessionId/goog/cdp/execute`; the adapter now uses
that route and exposes route/receipt absence only as typed Android
infrastructure. A fresh main-SHA browser+Android run remains required.

Merged-SHA run `31335563487` confirmed the corrected Chromium CDP route and
again completed browser `HEALTHY/PASS` (one HTTP-200 Search POST, five matching
cards, real wheel/event HTTP 200, zero forbidden/network/console activity,
9,343 bytes). Android completed callback authorization, `getUser` and owner-RLS
with 3,339 observed bytes and zero Search POSTs, then failed closed as
`UNKNOWN_ANDROID_INFRA/search_physical_observation_missing`: ChromeDriver again
provided request starts without response/terminal events, now after Auth. The
pre-document observer therefore remains installed through every Android
document and authoritatively accumulates closed Auth/Edge/REST/RPC bytes plus
Search/Storage/receipt counts. Performance logs remain independent navigation
and diagnostic evidence, not Android's byte-completion fence. A fresh
main-SHA browser+Android proof remains required.

Merged-SHA run `31336453210` then crossed that observer boundary. Android
completed Auth/owner-RLS and exactly one HTTP-200 vector Search POST with five
IDs equal to five cards, zero LLM/pagination/receipt/storage and 8,219 bytes.
Two generic Appium failed requests stopped the cell before native scroll/event
navigation, while its closed class receipt showed zero document/Auth/Edge/REST/
RPC failures and zero error responses. Appium diagnostics now correlate request
metadata privately across log drains and count only target-document or
allowlisted Supabase failures, excluding unrelated/decorative resources and the
three exact disposable probes. Browser again passed the full journey with one
POST, five cards, event HTTP 200, zero errors and 9,343 bytes. A fresh main-SHA
browser+Android proof remains required.

Follow-up run `31331011185` then reached neither Search path: browser was
correctly typed `UNKNOWN_AUTH_BROKER` because the completed prior claim still
held the original 20-minute persona lease, and the Android action again split
the inline script into separate `/bin/sh -c` commands. The next patch uses a
single checked-in Bash wrapper and shortens only a successfully completed
claim to the two-minute replay window; uncompleted owners remain fail-closed.

The current hardening pass additionally proves one physical POST is observed
once from pre-Auth through event navigation, accepts bounded cache-write
telemetry outcomes, rejects real skeleton/placeholder UI,
aligns the Appium preflight/diagnostic receipt, preserves failed-journey bytes,
gates cleanup, rereads pointer on failure, suppresses superseded issue mutation,
keeps pre-runner UNKNOWN streaks, refuses missing-artifact BROKEN proof, pins
Appium drivers, makes full qualification synchronous, and provides a bounded
encrypted durable broker idempotency replay window with the full SQL identity
and minute ciphertext erasure. It also verifies the
active exact Edge source revision with a side-effect-free HEAD before
Auth/Search, rechecks the revision on the one already-received Search response
to close the HEAD-to-POST race, enables
the iOS Safari console bucket, rejects mobile redirect chains, and keeps an
adapter-level physical Search observer alive through final event-page
diagnostics so the complete journey proves exactly one Search POST. Unknown
pre-runner cells retain the exact sanitized summary schema with explicit
closed null/zero values. Mobile protocol receipts also ingest CDP
`redirectResponse` for the document chain and correlate terminal
`loadingFinished.encodedDataLength` when Content-Length is absent. Direct and
relay transport probes, discarded retries, Auth/RLS, Search and post-navigation
traffic are counted exactly once; pending measurements and the hard cap are
closed before the one Search dispatch. When Android ChromeDriver supplies only
request starts, a pre-document allowlisted physical observer spans callback,
Search and event documents, closes already-received response bytes inside each
page and exports only document-delta totals/counters.
Losing disposable capability-probe aborts are excluded from product errors but
any received bytes remain metered. These are deterministic results;
live acceptance remains `0 / 2`; activation changed only the explicitly listed
migration, Edge and exact-main Fly release state, while scheduled health remains
default-off.
