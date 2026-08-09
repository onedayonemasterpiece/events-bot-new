# Static-site auth session broker

This HTTPS function exchanges a verified GitHub Actions OIDC identity for one
short-lived Supabase email OTP and one-time action link generated through the
admin `generate_link` endpoint. It never calls the product `/auth/v1/otp`
endpoint and never sends mail.

## Stage 2 identity and admission contract

The request body has exactly two fields:

```json
{"platform":"browser|android|ios","redirect_to":"https://..."}
```

Repository, `workflow_ref`, `run_id`, and `run_attempt` are accepted only from
the verified GitHub OIDC token. Caller-supplied mirrors such as `persona_id` or
`run_id` are rejected. The closed platform value is mapped server-side to the
existing dedicated personas `search-cached-browser`, `search-cached-android`,
and `search-cached-ios`. `AUTH_SESSION_BROKER_PERSONAS_JSON` must contain all
three persona IDs with three distinct email addresses; duplicate or missing
accounts fail startup validation.

`claim_static_site_auth_session_issue_v2` atomically binds
`(repository, workflow_ref, run_id, run_attempt, platform)` to the dedicated
persona and returns one of:

- `new` — the caller owns the new issuance;
- `duplicate_inflight` — the identical active identity already claimed it;
- `persona_busy` — another active identity owns that platform persona.

Only `new` can reach `generate_link`. Duplicate, persona-busy, and HTTP overload
responses are typed `product_health=UNKNOWN`, `execution_status=BLOCKED`, and
`failure_class=UNKNOWN`; they are infrastructure state, never evidence that
Search is broken. The process admits three overlapping HTTP calls (one per
platform) and rejects a fourth immediately rather than queueing it.

Overlapping identical calls inside one client issuer are coalesced to one
broker POST. Independently overlapping identical calls reaching one broker
process are coalesced to one ledger call and one `generate_link`. Results exist
only while those callers are in flight and are removed immediately afterward:
there is no credential/session cache, escrow, serialized session, or artifact.
The workflow exports the masked one-time browser callback only through the
current job's `GITHUB_ENV` and clears credential fields after use.

## Deployment policy

The production transport is the existing Fly aiohttp service, enabled only by
`ENABLE_STATIC_SITE_AUTH_SESSION_BROKER=1` at
`POST /internal/e2e/static-site-auth-session`. The route has no CORS/OPTIONS
surface, caps JSON bodies at 16 KiB and runs OIDC/JWKS/Supabase I/O off the
event loop. A deployment with the flag enabled validates the complete policy at
startup and fails closed. Fly port 80 has `force_https=true`.

Required policy values are `AUTH_SESSION_BROKER_OIDC_AUDIENCE`, exact
`AUTH_SESSION_BROKER_ALLOWED_{REPOSITORIES,REFS,WORKFLOW_REFS,ENVIRONMENTS,EVENTS,RUNS}`,
`AUTH_SESSION_BROKER_PERSONAS_JSON`, exact redirect templates,
`AUTH_SESSION_BROKER_PER_RUN_PERSONA_LIMIT=1`, and a random audit HMAC key.
Scheduled runs use `github-claim-bound`; exact numeric run allowlists remain
supported. Auth Admin access uses only
`AUTH_SESSION_BROKER_SUPABASE_SERVICE_ROLE_KEY`, a broker-only legacy JWT whose
payload role is `service_role`; generic environment key selection and public
clients are forbidden.

The claim ledger contains identity metadata only. Audit records contain only
keyed hashes plus the non-sensitive closed platform. OIDC tokens, email
addresses, OTPs, action links, redirect paths, raw run IDs, cookies, and
serialized sessions must never be logged or persisted.
