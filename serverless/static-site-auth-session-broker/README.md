# Static-site auth session broker

This HTTPS function exchanges a verified GitHub Actions OIDC identity for one
short-lived Supabase email OTP and one-time action link generated through the admin `generate_link`
endpoint. It never calls the product `/auth/v1/otp` endpoint and never sends
mail.

The deployment must configure exact allowlists for repository, ref,
`workflow_ref`, protected environment, event, persona and redirect. Scheduled
runs use the special `github-claim-bound` run policy: the numeric request
`run_id` must equal the signed GitHub claim. The broker then atomically claims
`claim_static_site_auth_session_issue_v1` with a hard limit of one credential
per run/persona before asking Supabase to generate a credential.

The production transport is the existing Fly aiohttp service, enabled only by
`ENABLE_STATIC_SITE_AUTH_SESSION_BROKER=1` at
`POST /internal/e2e/static-site-auth-session`. The route has no CORS/OPTIONS
surface, caps JSON bodies at 16 KiB and runs OIDC/JWKS/Supabase I/O off the
event loop. A deployment with the flag enabled validates the complete policy at
startup and fails closed. Fly port 80 is configured with `force_https=true`, so
the credential exchange is never served over plaintext HTTP.

Required policy values are `AUTH_SESSION_BROKER_OIDC_AUDIENCE`, exact
`AUTH_SESSION_BROKER_ALLOWED_{REPOSITORIES,REFS,WORKFLOW_REFS,ENVIRONMENTS,EVENTS,RUNS}`,
`AUTH_SESSION_BROKER_PERSONAS_JSON`, exact redirect templates,
`AUTH_SESSION_BROKER_PER_RUN_PERSONA_LIMIT=1` and a random audit HMAC key. Auth
Admin access uses the broker-only
`AUTH_SESSION_BROKER_SUPABASE_SERVICE_ROLE_KEY`; generic environment selection
is never used. For the current Supabase compatibility lane this is the
project-wide legacy JWT whose payload role is `service_role`, not an
`sb_secret_*` key. The distinct env name limits accidental exposure but does
not make the legacy project key independently rotatable; replacing it with a
named modern secret remains the follow-up after the observed Admin endpoint
compatibility failure is resolved.

A duplicate `(run_id, run_attempt, persona)` claim is rejected before
`generate_link`. After an ambiguous issuance failure the workflow must use a
new GitHub `run_attempt`; the active-persona cap can intentionally require
waiting for the 20-minute claim expiry (or using a different allocated
persona).

Audit records contain only keyed hashes. The OIDC token, email, OTP, action
link, redirect path and raw run id must never be logged.
