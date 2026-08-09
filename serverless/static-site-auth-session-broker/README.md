# Static-site auth session broker

This HTTPS function exchanges a verified GitHub Actions OIDC identity for one
short-lived Supabase email OTP and one-time action link generated through the
admin `generate_link` endpoint. It never calls the product `/auth/v1/otp`
endpoint and never sends mail.

## Stage 2 identity and admission contract

The request body has exactly three fields:

```json
{"purpose":"production_health|release_qualification|legacy_debug","platform":"browser|android|ios","redirect_to":"https://..."}
```

Repository, `workflow_ref`, `run_id`, and `run_attempt` are accepted only from
the verified GitHub OIDC token. Caller-supplied mirrors such as `persona_id` or
`run_id` are rejected. The server maps the closed purpose/platform pair to a
persona: production health uses `search-cached-browser`,
`search-cached-android`, or `search-cached-ios`; release qualification allows
only browser and uses `search-cold-browser`; legacy debug uses the cached
persona for its selected platform. Unknown purposes and unsupported pairs such
as release qualification on mobile fail closed. The four mapped persona IDs in
`AUTH_SESSION_BROKER_PERSONAS_JSON` must have four distinct email addresses;
duplicate or missing accounts fail startup validation. Production health and
legacy debug share one workflow concurrency group, so reuse of their cached
personas is intentional; the separately callable release qualification gets
the distinct cold browser persona.

`claim_static_site_auth_session_issue_v2` atomically binds
`(repository, workflow_ref, run_id, run_attempt, platform, persona)` and returns
one of:

- `new` — the caller owns the new issuance;
- `replay` — the identical still-unconsumed identity receives the same durable
  encrypted credential result;
- `duplicate_inflight` — the identical active identity already claimed it;
- `duplicate_consumed` — the bounded durable replay expired;
- `persona_busy` — another active identity owns that platform persona.

Only `new` can reach `generate_link`. Duplicate, persona-busy, and HTTP overload
responses are typed `product_health=UNKNOWN`, `execution_status=BLOCKED`, and
`failure_class=UNKNOWN`; they are infrastructure state, never evidence that
Search is broken. The process admits three overlapping HTTP calls (one per
platform) and rejects a fourth immediately rather than queueing it.

Overlapping identical calls inside one client issuer are coalesced to one
broker POST. Independently overlapping identical calls reaching one broker
process are coalesced to one ledger call and one `generate_link`. To close the
ordinary lost-HTTP-response window, a successful result may be replayed
once from broker process memory for at most 30 seconds. In addition, the claim
ledger accepts replay of a Fernet-encrypted result for at most two minutes. A
named `pg_cron` job runs every minute and physically nulls expired ciphertext,
independently of later broker traffic; the migration fails closed if Supabase
Cron cannot be installed or resolved. The key is derived in memory from the
broker-only audit secret; plaintext OTP/action link is never stored. Exact retries from another process or after restart receive
the same one-time credential during the TTL and never call `generate_link`
again; after expiry they are `duplicate_consumed`. The database row is
service-role only and is never an
evidence artifact, filesystem state or Actions cache. The workflow
exports the masked one-time browser callback only through the current job's
`GITHUB_ENV` and clears credential fields after use.

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

The claim ledger contains identity metadata plus only the bounded encrypted
idempotency ciphertext. Audit records contain keyed hashes plus the
non-sensitive closed purpose and platform. OIDC tokens, email addresses, plaintext OTPs or
action links, redirect paths, raw run IDs, cookies, and serialized sessions
must never be logged or persisted.
