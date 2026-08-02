# External focus email OTP harness — implementation report

Date: 2026-08-01
Source branch: `hotfix/focus-auth-write-route-pwa-truth-20260801`
Source HEAD: `976803ad1281b31990322419fa3b49ee847f0886`
Implementation branch: `agent/external-focus-email-otp-e2e`

## Scope delivered

- Manual protected GitHub Actions journey using real Chromium and a real IMAPS
  mailbox.
- Random provider-delivered six-digit OTP entered digit by digit without Enter.
- Fixed returning identity by default; unique identities require an explicit
  `{run_id}` recipient template.
- One issuance, one verification and one idempotent participant-registration
  assertion, followed by a reload assertion without reissuing mail.
- Exact target allowlist and optional/full-SHA deployment attestation (the live
  workflow requires the expected SHA).
- MIME fixtures for plain, HTML, base64 and quoted-printable messages.
- Sanitized, short-retention evidence designed for a separate ChatGPT review.

## Evidence levels

The onboarding sixth-digit behavior has an internal deployed Auth integration
PASS on the isolated `otp-r5` page. It used a fresh hosted-Auth code and confirmed
verify `200` plus participant registration `200`; it is not mailbox-delivery
evidence.

The new harness unit/configuration gates are local test evidence. Until the
`external-e2e` Environment and controlled mailbox are configured and one manual
workflow run passes, the external-delivery status is
`BLOCKED_EXTERNAL_CONFIGURATION`. No mock is represented as a live E2E.

## Security decisions

- No Supabase service key, access token, Postbox key or NotiSend key is available
  to the workflow.
- `workflow_dispatch` only, trusted checked-out branch, read-only repository
  permission, pinned actions, one global run and bounded timeout.
- No arbitrary ref, off-origin navigation, non-KenigEvents URL, raw email, HAR,
  trace or video.
- Artifact upload is conditional on an explicit redaction success marker.

## Remaining production work

This harness does not itself select an Auth email provider or add product
telemetry. Provider selection for repeat/test identities must be server-side,
idempotent and receipt-backed; browser code must not choose Postbox or NotiSend.
Auth-method/attempt/route/outcome telemetry must contain no email, OTP or token.

