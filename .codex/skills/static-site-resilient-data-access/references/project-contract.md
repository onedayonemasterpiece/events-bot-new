# Project contract and evidence

Canonical sources:

- `docs/architecture/personalization-data-ownership.md`
- `docs/features/static-site-focus-group/README.md`
- `docs/features/unsigned-personalization/production-integration.md`
- `docs/features/unsigned-personalization/authorized-event-search.md`
- `docs/operations/e2e-scenarios.md`
- `docs/reports/incidents/INC-2026-07-30-focus-email-otp-false-success.md`

Required implementation properties:

- local-first UI with bounded idempotent outbox;
- direct Supabase plus fixed stateless relay, not a thick Fly backend;
- safe-read recovery only, unless a stable idempotency key permits replay;
- no ambiguous replay of OTP or other selected-once operations;
- exact RPC/Edge/Auth allowlist, RLS and rate-limit enforcement;
- compact application storage and measured Supabase egress;
- candidate build carries public endpoint, publishable key and relay URL;
- browser acceptance covers the actual product surface, not diagnostics alone.

Acceptance status on 2026-08-01: **not yet accepted by phone E2E**. The first
retest was blocked at Android PWA installation before resilient data access was
exercised. Do not convert the design/test evidence into a product acceptance
claim until that end-to-end journey reaches and verifies the remote actions.
