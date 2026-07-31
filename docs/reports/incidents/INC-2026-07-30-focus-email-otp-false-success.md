# INC-2026-07-30 Focus email OTP false success and pre-Auth request loss

Status: open
Severity: sev1
Service: KenigEvents focus-group onboarding / Supabase Auth email OTP
Opened: 2026-07-30
Closed: —
Owners: static-site / auth / focus-group operations
Related incidents: `INC-2026-07-27-pwa-presentation-install-missing`
Related docs: `docs/backlog/features/static-site-focus-group/README.md`, `docs/operations/e2e-scenarios.md`, `docs/operations/email-delivery.md`

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
- 2026-07-31 11:32–12:30 UTC — eight returned `KE2` receipts collected from
  phone browsers: seven completed all four checks and one timed out on all
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

## Root Cause

1. `FocusGroupInviteIntake.astro` called `showEmailCode()` unconditionally
   after `signInWithEmailOtp()`, including when it returned failure.
2. The browser called Supabase Auth directly without a bounded independent
   control/attempt ledger. Seven edge client signatures completed
   `/auth/v1/otp` preflight but produced no matching POST in the presentation
   window.
   A live diagnostic now confirms this failure class on one MTS route: the
   Supabase edge processed and answered the simple GET and both preflights,
   but the phone browser did not complete those responses and therefore never
   sent the checked Auth/Data requests. The same phone completed the Yandex
   control request. The precise ISP/browser filtering mechanism is not yet
   proven, but Supabase processing time and the mail provider are downstream
   of the observed loss and cannot be its cause.
3. Resends could generate several active-looking messages while the UI did not
   clearly bind code entry to the latest accepted issuance. This contributed
   to 59 `otp_expired` verification failures; the exact distribution still
   requires a live reproduction with separate issuance ids.
4. Hosted Auth SMTP messages do not originate in the application
   `email_control.email_outbox`, while the Postbox event consumer requires an
   outbox correlation row. Provider events existed, but the normal delivery
   projection could not account for them.

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
- Remove direct browser dependence on the Supabase hostname for onboarding:
  send OTP initiation and verification through the already reachable
  KenigEvents/Yandex control boundary, then call Auth and the selected mail
  provider server-to-server. Keep an explicit provider result and fallback
  decision in the attempt ledger rather than changing only the SMTP provider.
- Bind every OTP verification to the latest accepted issuance in the UI and
  suppress duplicate automatic submissions.
- Replace the client-only participant projection with an idempotent
  server-side activation coupled to the verified identity.
- Add a hosted-Auth-aware Postbox projection that can correlate provider
  message ids without fabricating an application outbox row.

## Follow-up Actions

- [x] P0 deploy the read-only diagnostic and collect phone evidence; one MTS
      route reproduced the pre-Auth response loss while the Yandex control
      stayed available.
- [ ] P0 deploy the false-success fix to the focus onboarding itself.
- [ ] P0 move OTP initiation and verification behind the reachable
      KenigEvents/Yandex boundary and run phone acceptance on the failing route.
- [ ] P0 complete separate live code and magic-link E2E issuances.
- [ ] P0 reconcile all confirmed Auth users into the focus participant table.
- [ ] P1 add PII-minimal stage telemetry and delivery correlation for hosted Auth.
- [ ] P1 investigate the 59 expired verifies by issuance/order, not only aggregate count.
- [ ] P1 make verified identity + focus activation one server-side idempotent operation.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: local focus suite and Astro build in progress
- post-deploy verification: pending

## Prevention

This incident is the required regression contract for focus email/Auth changes.
“Code form visible” and “Supabase preflight succeeded” are explicitly forbidden
as delivery evidence; closure requires independent evidence at every boundary.
