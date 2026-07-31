# W3 — Security hardening results

## Scope

- **Lane:** W3
- **Requirement:** R03 — close broad relay/database mutation surfaces and add bounded, idempotent authenticated operations without turning the static site into a thick backend.
- **Base SHA:** `cc7c213f5e49173b24029b00dabb1359c1f1059b`
- **Validated implementation SHA:** `8fbbc8ce5392e8f1946e93f6d8e32e5b61e60ee6`
- **Branch:** `agent/static-site-resilient-egress/W3`
- **Production apply/deploy:** not performed in this lane.

## Delivered

1. Replaced the Yandex API Gateway greedy Supabase proxy with a method/path allowlist for the product's current Auth lifecycle, two read views, six public RPCs, one Edge Function, and one narrowly scoped private feedback-upload prefix.
2. Added explicit magic-link callback/verification, refresh-token and logout routes so security narrowing does not break authentication lifecycle operations.
3. Allowed only `POST`/`DELETE` under `/storage/v1/object/focus-feedback`; all other Storage, Realtime, admin, unknown REST/RPC, and unknown Edge routes fail closed at the gateway. JWT and `apikey` remain upstream Supabase inputs; Cookie/Host/spoofable forwarding headers are stripped.
4. Removed direct authenticated INSERT/UPDATE/DELETE privileges on durable saved events. The owner-only desired-state RPC now serializes concurrent owner writes and enforces a hard 500-active-event bound.
5. Revoked browser execution of expensive search/quota/telemetry primitives. The Edge Function verifies the caller JWT before constructing a service client and calls service-only wrappers with the verified `auth.users.id`.
6. Added request-body limit (16 KiB), client operation UUID validation, idempotent quota reservation, 48-hour/1000-row-per-owner operation-ledger bounds, and safe retry behavior.
7. Hardened feedback writes with owner-scoped operation-id deduplication, 30 writes/user/hour, 90-day bounded cleanup, max 40 event IDs, and metadata allowlisting.
8. Updated canonical architecture/feature documentation and `CHANGELOG.md`.

## Evidence and commands

### Static and runtime-contract tests

```text
uv run --with pytest pytest --noconftest -q \
  tests/test_supabase_relay_infra.py \
  tests/test_supabase_security_hardening.py
8 passed in 0.13s
```

```text
npx --yes deno check supabase/functions/event-search/index.ts
PASS
```

```text
npx --yes deno test --allow-read \
  supabase/functions/event-search/occurrence-families.test.mjs
5 passed, 0 failed
```

```text
uv run --with pyyaml python ...
relay manifests parse: OK
```

```text
npx --yes @redocly/cli lint \
  infra/yandex/supabase-relay/openapi.yaml --extends=minimal
API description valid (30 non-blocking operation-summary warnings)
```

```text
git diff --check
PASS
```

### Local database execution

An isolated local Supabase stack was started and reset from the complete repository migration chain. Both new migrations applied successfully, then:

```text
psql ... -f supabase/tests/durable_saved_events_contract.sql
PASS

psql ... -f supabase/tests/event_search_security_hardening_contract.sql
PASS
```

The temporary W3 containers/volumes were removed afterward. The first reset filled the host Docker cache; only unused Docker images/volumes created for this isolated run were pruned, restoring approximately 5.4 GiB. No repository files from another lane were removed.

### Read-only infrastructure evidence

- Current Supabase Auth limits observed: OTP 200/hour, email send 200/hour, verify 30/hour, refresh 150/hour; OTP expiry 600 seconds and length 6. They were not changed.
- Yandex Smart Web Security profile list in the target folder was empty. The deprecated API Gateway `rateLimit` extension was therefore not introduced, and no fictional SWS profile was attached.

## Integration order

1. Apply both timestamped database migrations.
2. Configure the Edge Function service-role secret and deploy the hardened `event-search` function.
3. Smoke Auth callback/refresh/logout, search, saved-event mutation, and private screenshot upload.
4. Only then deploy the narrowed relay OpenAPI/desired state.

Deploying the narrowed relay before the database/function changes would deny older direct search calls before their replacement is available.

## Remaining risks / acceptance gates

- Gateway-wide low rate limiting is unsafe because many users can share relay egress IPs. Supabase Auth limits and per-owner DB caps are active downstream, but volumetric gateway billing protection still requires an approved Yandex SWS profile. No profile exists today.
- The exact `focus-feedback` transport exception assumes the integration branch's bucket is private and its authenticated RLS policy is already present. The relay does not weaken or replace Storage RLS; verify upload/delete with a real user JWT and verify anonymous/other-prefix rejection before release.
- `SUPABASE_SERVICE_ROLE_KEY` must be present only in the Edge Function environment. A missing key now fails closed with HTTP 500 rather than falling back to browser-callable expensive RPCs.
- Every future product RPC/Auth endpoint must be intentionally added to both the OpenAPI and desired-state allowlists with tests; otherwise it will correctly fail closed.
- Redocly's remaining 30 warnings are documentation-only missing operation summaries; schema validation succeeds.

## Changed files

- `CHANGELOG.md`
- `docs/architecture/personalization-data-ownership.md`
- `docs/features/event-favorites-calendar/README.md`
- `docs/features/unsigned-personalization/authorized-event-search.md`
- `docs/features/unsigned-personalization/production-integration.md`
- `infra/yandex/supabase-relay/README.md`
- `infra/yandex/supabase-relay/desired-state.json`
- `infra/yandex/supabase-relay/openapi.yaml`
- `supabase/functions/event-search/index.ts`
- `supabase/migrations/20260731174310_harden_saved_event_mutations.sql`
- `supabase/migrations/20260731174313_harden_event_search_internal_rpc.sql`
- `supabase/tests/durable_saved_events_contract.sql`
- `supabase/tests/event_search_security_hardening_contract.sql`
- `tests/test_supabase_relay_infra.py`
- `tests/test_supabase_security_hardening.py`
- `.codex/lanes/W3/RESULTS.md`
