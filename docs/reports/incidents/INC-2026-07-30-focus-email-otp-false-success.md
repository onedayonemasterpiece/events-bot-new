# INC-2026-07-30 Focus email OTP false success and pre-Auth request loss

Status: open
Severity: sev1
Service: KenigEvents focus-group onboarding / Supabase Auth email OTP
Opened: 2026-07-30
Closed: —
Owners: static-site / auth / focus-group operations
Related incidents: `INC-2026-07-27-pwa-presentation-install-missing`
Related docs: `docs/features/static-site-focus-group/README.md`, `docs/operations/e2e-scenarios.md`, `docs/operations/email-delivery.md`

## Summary

During the 30 July presentation multiple participants entered an email address,
saw the interface advance to code entry, but received no message. Retained
Supabase and Postbox evidence proves that these reports include failures before
Supabase Auth accepted an OTP request. The onboarding UI nevertheless advanced
after any failed transport result and therefore communicated a false success.

## User / Business Impact

- Three reported addresses had no Supabase Auth request, Auth user or Postbox
  event despite the participants seeing the email flow continue.
- Seven requests that did reach Auth were rejected by the per-address resend
  guard with `429`.
- A total of 59 `/verify` requests failed as expired/invalid OTP attempts,
  making code entry unreliable and confusing during the live onboarding.
- Some successfully authenticated participants were not durably projected into
  the focus-group contact table because membership registration remained a
  separate browser-side request.

## Detection

- Participants showed empty mailboxes during the presentation.
- The UI message and code screen were initially treated as evidence that the
  email send had been accepted. Code inspection disproved that assumption.
- Supabase Management API access was restored on 31 July while the Free-plan
  log retention still covered the presentation window.
- Observability gap: the browser stored only an overwritable local hint. There
  was no independent attempt id/control path to distinguish a browser/network
  failure from Auth, SMTP and mailbox delivery.

## Timeline

- 2026-07-30 17:40–18:35 UTC — presentation OTP activity: 46 Auth requests,
  39 accepted and 7 rate-limited; 86 verification requests, 59 rejected as
  expired/invalid.
- 2026-07-30 — participants report that several messages never arrived.
- 2026-07-31 09:10 UTC — retained Supabase Auth/edge logs recovered through the
  current ClickHouse `logs` Management API.
- 2026-07-31 09:20 UTC — exact Postbox reconciliation proves all 39 accepted
  Auth requests produced 39 provider `Send` events.
- 2026-07-31 09:35 UTC — dedicated read-only API Gateway → YDB control deployed
  and verified; source for an unlinked noindex connectivity page added.
- 2026-07-31 11:32–13:37 UTC — ten returned `KE2` receipts collected from
  phone browsers: eight completed all four checks and two timed out on all
  direct Supabase checks while the Yandex control completed.
- 2026-07-31 12:17 UTC — for the failing `C6DB-202B` receipt, Supabase edge
  logs recorded the transport-only request and both Auth/Data preflights from
  the MTS route twice (HTTP/3, then HTTP/2), all answered in 31–59 ms. The
  browser received none of those responses within 20 seconds, did not issue
  the checked Auth/Data GETs, and did complete the Yandex control in 1.2 s.
- 2026-07-31 12:25 UTC — the control logs retained one additional diagnostic
  code that was not pasted into Telegram; its Supabase Auth/Data and control
  requests all completed. This proves that a copied receipt is useful for
  product communication but is not the only available diagnostic evidence.
- 2026-07-31 12:49 UTC — `3D6A-BCDD` completed the direct, Auth, Data and
  Yandex paths in 300/572/590/1177 ms. Supabase edge logs independently show
  the transport probe plus successful Auth and Data requests; this raises the
  returned-phone total to eight healthy routes and one reproduced failing
  route without changing the localized failure boundary.
- 2026-07-31 13:36 UTC — the participant who had experienced the missing-email
  problem returned `C03E-CB61`: direct/Auth/Data timed out after
  20018/20016/20015 ms while the Yandex control completed in 1552 ms. The
  Gateway log independently records the same code with HTTP 200 and 1008 ms
  processing. In the surrounding 13:30–13:45 UTC Supabase edge window, 39
  other entries exist but there is no probe-code, Auth-health or diagnostic
  Data API request from this run. This is a second reproduced failing route
  and, unlike `C6DB-202B`, the checked requests did not reach Supabase edge at
  all.
- 2026-07-31 13:10 UTC — Auth transport design was corrected to preserve the
  static/thin-client contract. A live custom-Yandex authorize response and the
  current Supabase Auth source both show that the OAuth provider callback is
  derived from Auth's configured external URL, not an arbitrary proxy `Host`.
  A transparent relay can protect email OTP/verify/Data traffic, but cannot be
  represented as a replacement for an activated Supabase custom domain for
  the complete OAuth callback flow.
- 2026-07-31 13:11–13:40 UTC — an isolated, non-production Yandex API Gateway
  canary was deployed as a fixed-upstream relay with no service key or state.
  A real Chromium page loaded from `https://kenigevents.ru` received Auth
  health and an RLS Data API read through that relay with HTTP 200; invalid
  verify/refresh probes also reached Auth and returned the expected 403/400 in
  747 ms total. A direct and relayed custom-Yandex authorize smoke both
  redirected to Yandex and advertised the same existing Supabase callback.
  This proves browser CORS/header/body forwarding, and also proves the relay
  does not silently solve the OAuth callback-host dependency. The canary is
  not connected to onboarding or production personalization and was deleted
  after the evidence was captured, so it creates no persistent public route.
- 2026-07-31 14:29 UTC — permanent stateless gateway
  `kenigevents-supabase-relay` was deployed with request logging disabled and
  no service account. Production-origin browser smoke returned 200 for Auth
  health and the tiny RLS Data read; invalid verify/refresh reached Supabase
  once and returned expected 403/400 responses. The shared client now selects
  the first healthy route with safe probes, preserves the exact historical Auth
  storage key and forbids cross-route retry for non-safe methods. A new `KE3`
  diagnostic compares raw direct reads with framework reads; affected-phone
  acceptance and live email issuances are still pending.
- 2026-07-31 14:49 UTC — first post-publication `KE3` browser run exposed a
  client release defect before phone acceptance: native `fetch` had been stored
  unbound and failed immediately (`NET/0`) in Chromium. The client now binds
  browser `fetch`, with a regression test for the native receiver. A parallel
  relay-header probe initially reported `401`, but that probe had read the wrong
  env lane and sent an empty public key; rerunning with the configured key
  returned Auth/Data 200 through the unchanged stateless Gateway. Smoke scripts
  must therefore fail before network I/O when the key is empty.
- 2026-07-31 15:08 UTC — the corrected root diagnostic passed real Chromium at
  390×844 and 1440×900. Receipts `325F-E907` and `FEF9-B695` reported all six
  checks `OK`, including the raw second route and the actual resilient Auth/Data
  framework path; there were no failed requests, console errors or horizontal
  overflow. This is release evidence for the diagnostic/framework on the
  current network, not a substitute for the affected participant-phone run.
- 2026-07-31 15:24–15:25 UTC — affected-phone receipts `809F-00BF` and
  `6549-882C` proved both raw routes were reachable (`DA/DD/RA=OK`) but the
  resilient Data request, then both framework requests, ended at the exact
  12-second diagnostic deadline. Review found an algorithm defect rather than
  a channel failure: the outer probe aborted when the primary route's own
  12-second timer expired, so the alternate inherited an already-aborted
  signal and had no chance to run.
- 2026-07-31 15:57 UTC — the replacement `KE4` diagnostic was exercised with
  Playwright before publication. It launches direct, relay and control
  measurements concurrently, renders each completed result immediately and
  races both framework health checks rather than serially waiting for a
  fallback. Normal framework access completed in `0.9 s`; with every direct
  request deliberately held open, relay-backed framework access completed in
  `1.4 s`; when the already-selected direct route was broken after its health
  ping, the one safe recovery completed in `4.9 s` and cached relay for the next
  request. Independent raw hang classification finished at its five-second
  ceiling. These are deterministic local-origin failure-shape checks; a live
  production-origin run remains required after publication.
- 2026-07-31 16:09–16:10 UTC — PR `#158` merged as `0d636991`; only the two
  root diagnostic HTML files and their two content-hashed JS dependencies were
  published. Live-origin Playwright at 390 px returned `KE4 6DF7-39FA` in
  `2.25 s` with every route OK. With direct requests deliberately held open,
  `KE4 DCC1-C0ED` selected relay and produced working framework answers in
  `1.90 s`; the independent direct hang classification completed at `5.28 s`.
  With the selected direct route broken after its health ping,
  `KE4 D45E-2A0A` recovered through relay in `5.00 s` and retained `PATH=2`.
  Desktop 1440 px returned `KE4 113D-7E39` in `2.21 s`. All live cases had no
  console errors or horizontal overflow and were visually inspected. A new
  affected-participant phone receipt remains the incident acceptance gate.
- 2026-07-31 16:24–16:57 UTC — six participant-supplied `KE4` phone
  receipts (`82DC-0753`, `2F08-11DC`, `A33B-8D1B`, `4216-D659`, `831F-D5FA`,
  `731F-07A1`) completed every framework Auth/Data read without an HTTP/network
  error or framework timeout. The framework retained direct routing in four
  sessions and relay routing in two. Five framework pairs completed in
  `77–432 ms`. `A33B-8D1B` is the useful direct-degradation outlier: raw direct
  reads had just completed in about `300 ms`, but both subsequent framework
  reads needed the bounded safe-read recovery and completed through relay in
  `4.47–4.48 s`, leaving `PATH=2`. `831F-D5FA` supplies the inverse failure
  shape: the independent relay Data read timed out at `5 s`, while direct and
  both framework reads succeeded and retained `PATH=1` in `144–155 ms`.
  `731F-07A1` also found relay slow at about `4.8 s`, selected direct and
  completed both framework reads in about `430 ms`. Together these validate
  bidirectional route choice and recovery, and remove the previous 12-second
  algorithm failure. The receipts are real phone evidence; whether one belongs
  to the originally affected participant is not established by the PII-free
  result format, so that narrower gate and the separate live OTP journeys
  remain open.
- 2026-07-31 19:26–19:49 UTC — the production Supabase migration sequence was
  reconciled and the resilient-client schema changes were applied; the
  authenticated `event-search` function and narrowed permanent relay were
  deployed. Integration smoke then found that the upstream reflected an
  arbitrary browser `Origin` despite the correct preflight rule. The relay now
  replaces `Origin` in all 29 fixed integrations. Live Chromium accepted the
  production origin and blocked `https://example.com`; unknown RPC/Auth admin/
  Function/Storage routes failed at Gateway. Regression evidence is 42/42
  resilient-client, 71/71 focus product, 88/88 Python, 18/18 Edge Function and
  a successful 466-page Astro build. This delivers the data plane but does not
  close the incident: separate live code/link issuances and the root/current
  static release are still required.
- 2026-08-01 08:39–08:47 UTC — operator clean-retest on the newly generated
  immutable candidate still displayed the previous `lovekgd@mail.ru` session,
  then participant registration waited and ended with “Не получилось сохранить
  участие”. Candidate inspection proved that the reset treated local sign-out
  as best effort and never verified removal of the persisted Auth key. The same
  generated HTML contained the direct Supabase URL/key but no relay URL: the
  Smart Update command builder did not forward the already-supported relay
  argument, and Fly did not have the public relay environment value. Thus the
  phone exercised neither a clean identity state nor the accepted resilient
  registration route.
- 2026-08-01 10:13 UTC — the corrective Smart Update request was initially
  deferred at the volume capacity gate while an exact completed remote handoff
  output was retained for host reconciliation. Recovery is now ordered before
  the new-build capacity gate so that checked output can publish and remove
  itself; blind deletion of active handoff evidence remains forbidden.
- 2026-08-01 10:28 UTC — the first reordered recovery reached the adoption
  runner but its own preflight still ran before replacing the retained local
  duplicate. Adoption now verifies the exact remote result, removes only that
  replaceable duplicate, rechecks capacity, and downloads the authoritative
  output; fresh submissions remain gated before staging.
- 2026-08-01 16:19:57 UTC — a phone OTP submit reached Supabase through the
  selected route and completed upstream with HTTP `200` in 1161 ms. The browser
  UI nevertheless reported failure. A second submit at 16:20:03 reached the
  same endpoint and received `429`. Code review proved the v2 transport stopped
  its deadline as soon as `fetch()` returned headers and left body consumption
  to `supabase-js`, outside transport accounting. The exact phone-level loss
  may be body-stream/CORS/abort related, but the architectural ambiguity defect
  is independently proven.
- 2026-08-01 — external code review and local verification assigned the current
  transport `NO-GO`. The accepted replacement contract is Transport v3 in
  `docs/features/unsigned-personalization/production-integration.md`: closed
  operation catalog, capability-specific route health, full-response lifecycle,
  per-operation typed outcomes, provider receipt for ambiguous OTP issue and
  ordered idempotent outbox for product commands.
- 2026-08-01 19:52–20:20 UTC — the onboarding-only Transport v3 candidate
  confirmed email OTP, then reported participant persistence failure. Live
  REST evidence found `transport_probe_v1` missing (`PGRST202`) even though the
  browser bundle already required it; the committed migration and Function had
  never been applied. The migration was applied and reconciled in remote
  history, the nonce Function was deployed, and the exact gateway allowlist was
  updated. Complete production probes now return HTTP 200 with the same nonce
  through direct and relay Data and Function routes. The UI additionally
  separates confirmed identity from pending membership persistence and offers
  an explicit retry.

## Root Cause

1. `FocusGroupInviteIntake.astro` called `showEmailCode()` unconditionally
   after `signInWithEmailOtp()`, including when it returned failure.
2. The browser called Supabase Auth directly without a bounded independent
   control/attempt ledger. Seven edge client signatures completed
   `/auth/v1/otp` preflight but produced no matching POST in the presentation
   window.
   Live diagnostics now confirm two related failure shapes. On `C6DB-202B`,
   Supabase edge processed and answered the simple GET and both preflights, but
   the phone browser did not complete those responses and therefore never sent
   the checked Auth/Data GETs. On affected-participant run `C03E-CB61`, none of
   the three checked Supabase requests appears at edge at all. Both phones
   completed the Yandex control request. The precise ISP/in-app-browser
   filtering mechanism is not yet proven, but Supabase processing time and the
   mail provider are downstream of the observed loss and cannot be its cause.
3. Resends could generate several active-looking messages while the UI did not
   clearly bind code entry to the latest accepted issuance. This contributed
   to 59 `otp_expired` verification failures; the exact distribution still
   requires a live reproduction with separate issuance ids.
4. Hosted Auth SMTP messages do not originate in the application
   `email_control.email_outbox`, while the Postbox event consumer requires an
   outbox correlation row. Provider events existed, but the normal delivery
   projection could not account for them.
5. The first resilient diagnostic gave both the transport's primary attempt and
   the caller-wide probe the same 12-second deadline. On a primary timeout the
   caller abort fired first, and the alternate route received that aborted
   signal. The UI therefore described a fallback framework that could not
   actually fall back in this failure shape.
6. `resetForOnboardingTest()` swallowed a failed local sign-out and immediately
   claimed success. It did not unconditionally remove and verify the exact
   Supabase storage key, chunked session fragments or PKCE verifier, so a
   network failure could preserve the old account through the reload.
7. The static builder runner supported a relay value, but
   `_static_site_build_kaggle_command()` never passed that argument and the
   authorized-candidate gate required only URL/key. A candidate could therefore
   pass while silently compiling out the entire accepted fallback route.
8. `ResilientSupabaseTransport.rawRequest()` disposed its abort timer in a
   `finally` immediately after `fetch()` produced a `Response`. It neither read
   nor validated the response body. A later stream/decode failure was converted
   by `supabase-js` after the transport had already recorded success. Shared
   `lastAmbiguousAt`/`lastNoHealthyAt` timestamps could not reliably bind that
   failure to the originating operation, especially under concurrency.
9. Transport v3 frontend delivery was not atomically gated on its database,
   Edge Function and API Gateway prerequisites. Tests passed against source
   contracts while production still lacked the Data probe, so route selection
   failed after Auth had already succeeded.

## Contributing Factors

- A successful CORS preflight was treated operationally as if the actual OTP
  POST had followed.
- Only final Auth/provider stages were observable; absent browser requests left
  no server-side evidence and lost the typed address.
- The live E2E covered successful code and link paths but did not require
  network failure, timeout and `429` product states.
- Completion and focus-participant registration were two separate
  browser-originated operations rather than one server-side idempotent
  activation.

## Automation Contract

### Treat as regression guard when

- changing focus onboarding, shared static Auth, email templates/resend rules,
  Supabase Auth configuration, Postbox event ingestion, focus membership
  activation or browser connectivity diagnostics.

### Affected surfaces

- `site/src/components/FocusGroupInviteIntake.astro`
- `site/src/lib/staticSiteAuth.ts`
- `/fokus-gruppa/priglashenie/`
- `/fokus-gruppa/diagnostika/`
- Supabase `/auth/v1/otp` and `/auth/v1/verify`
- custom SMTP/Postbox provider events
- `register_focus_group_participant_v1`
- Yandex API Gateway `kenigevents-focus-connectivity`

### Mandatory checks before closure or deploy

- An accepted `/otp` result alone opens the code step.
- The clean-retest URL removes the persisted project Auth key, chunks, verifier
  and intent before redirect; the next identity screen contains no former
  email. Failure to clear storage is visible and cannot redirect as success.
- Every authorized production candidate contains both direct and relay URLs;
  missing relay configuration fails the build before publication.
- Transport success requires the complete bounded response body and declared
  decode, not only HTTP headers. Headers-then-stall, partial-body close and
  invalid-JSON fault injection are regression tests.
- Operation semantics come from the closed central catalog. Feature code cannot
  select a route or request `idempotentReplay` directly.
- Auth/Data/Functions route state is capability-specific; one healthy Auth GET
  does not certify a Data RPC or Function POST.
- A selected-once operation produces one per-request typed result and exactly
  one upstream attempt. No shared timestamp can change another request's result.
- Network error, timeout and `429` remain on the address step with honest copy;
  none says or implies that a message was sent.
- Six digits cause one in-flight verify request. An old/replayed code fails
  without repeated automatic submissions.
- Code and magic-link journeys use two separate live issuances and both create
  a session.
- A verified email and a Yandex account each activate the focus participant
  idempotently on the server.
- A real phone run compares Supabase Auth, a tiny Supabase Data API read and
  the YDB control on Wi-Fi, mobile data and the reported VPN mode.
- Accepted Auth sends reconcile to provider Send/Delivery/Bounce evidence.
- No diagnostic receipt contains email, OTP, JWT, key, raw user agent or IP.
- All fixes are reachable from `origin/main` and the live root/current focus
  release.

### Required evidence

- redacted Supabase Auth/edge query receipt;
- provider correlation counts;
- deployed source SHA and API Gateway/YDB inventory;
- local unit/build results and live mobile screenshots/receipts;
- post-deploy Auth/provider/focus-contact rows for dedicated test users.

## Immediate Mitigation

- Changed the source contract so only an accepted Auth request advances to code
  entry. Timeout, network failure and `429` now remain on the email form.
- Added a 20-second send timeout rather than allowing an indefinite pending
  state.
- Deployed a dedicated read-only YDB control and added an unlinked noindex
  diagnostic page that performs three bounded reads per service in parallel
  without sending email or collecting personal data. Its phone-sized result
  carries one opaque correlation code and is designed to be returned as a
  single screenshot.

## Corrective Actions

- Add independent, PII-minimal request-stage telemetry with an opaque attempt
  id so “browser did not send”, “Auth rejected”, “SMTP not handed off” and
  “provider did not deliver” are distinct.
- Preserve Supabase Auth as the only issuer/verifier of email OTP, OAuth
  identities, access tokens and refresh sessions. Do **not** build a second
  Auth service in YDB, Yandex Functions or Fly.
- Remove the browser's mandatory direct dependency on the Supabase hostname
  for email onboarding with a stateless Yandex API Gateway HTTP relay. The
  relay forwards only public client headers/body to the existing Supabase
  `/auth/v1/otp`, `/verify` and `/token` endpoints; it owns no users, OTPs,
  sessions, service-role key or authorization decision.
- Keep one selected transport for a non-idempotent OTP issuance. Never fire a
  direct request and then blindly repeat the same issuance through the relay
  after an ambiguous timeout. Verification and resend remain bound to the
  latest accepted issuance.
- Treat complete custom-Yandex OAuth hostname independence separately. A plain
  reverse proxy does not change the provider callback generated from Supabase
  Auth's configured external URL. Before broad release either activate the
  supported Supabase custom domain and register its callback in Yandex, or
  retain the current Supabase callback as an explicitly tested dependency.
- Use the Supabase Send Email Hook only as a thin delivery adapter: provider
  selection/fallback and a PII-minimal provider receipt may run in a serverless
  function, but Supabase still creates and verifies the token. Changing the
  mail provider alone is not a fix for the pre-Auth browser loss.
- Give each email attempt an opaque client-generated id in `redirect_to`. The
  Send Email Hook can read that id, write a PII-minimal `provider_accepted` or
  definitive-failure receipt and expose a bounded status lookup. The browser
  may use that receipt to distinguish an ambiguous lost HTTP response from a
  request that never reached the mail boundary; the receipt never contains
  email, OTP, JWT or provider credentials.
- Bind every OTP verification to the latest accepted issuance in the UI and
  suppress duplicate automatic submissions.
- Replace the client-only participant projection with an idempotent
  server-side activation coupled to the verified identity.
- Add a hosted-Auth-aware Postbox projection that can correlate provider
  message ids without fabricating an application outbox row.

## Follow-up Actions

- [x] P0 deploy the read-only diagnostic and collect phone evidence; two phone
      routes reproduced direct Supabase loss while the Yandex control stayed
      available, including a participant affected by the missing-email issue.
- [ ] P0 deploy the false-success fix to the focus onboarding itself.
- [ ] P0 replace the rejected v2 transport with the Transport v3 full-response
      executor, operation catalog and fault-injection acceptance suite.
- [ ] P0 deploy and phone-verify unconditional clean reset plus relay-bearing
      participant registration on the current candidate.
- [ ] P0 run affected-phone acceptance through the deployed stateless API
      Gateway relay without moving Auth state out of Supabase. The first real
      phone `KE3` exposed the fallback-budget defect; rerun after the corrected
      diagnostic is published.
- [x] P0 give Supabase clients one explicit stable auth storage key before any
      API base URL changes, so the existing PWA session survives transport
      migration without reinstall or repeated login.
- [ ] P0 complete separate live code and magic-link E2E issuances.
- [ ] P0 add an opaque mail-attempt receipt so an ambiguous response cannot
      cause either a false “sent” message or a blind duplicate issuance.
- [ ] P0 reconcile all confirmed Auth users into the focus participant table.
- [ ] P1 add a bounded local outbox plus idempotent RPC contract for saved,
      feedback and personalization actions; use direct/relay delivery without
      making navigation wait for synchronization.
- [ ] P1 decide and activate the supported Supabase custom domain before the
      OAuth callback hostname becomes a broad-release requirement.
- [ ] P1 add PII-minimal stage telemetry and delivery correlation for hosted Auth.
- [ ] P1 investigate the 59 expired verifies by issuance/order, not only aggregate count.
- [ ] P1 make verified identity + focus activation one server-side idempotent operation.

## Release And Closure Evidence

- deployed SHAs: framework/main `d1628867c0d8b3485e7ae9a8b622239f932682a8`;
  browser-fetch hotfix `625bfc3b` (root diagnostic assets verified live)
- relay deploy path: Yandex API Gateway `kenigevents-supabase-relay`
- regression checks: local focus suite, relay infra tests and Astro build pass;
  live production-origin CORS/Auth/Data/invalid-verify/invalid-refresh smoke pass
- post-deploy verification: corrected root `KE3` passes all six checks in real
  Chromium at mobile and desktop widths; affected-phone acceptance remains open

## Prevention

This incident is the required regression contract for focus email/Auth changes.
“Code form visible” and “Supabase preflight succeeded” are explicitly forbidden
as delivery evidence; closure requires independent evidence at every boundary.
