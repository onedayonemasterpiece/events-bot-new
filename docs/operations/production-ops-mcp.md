# Production Operations MCP

Status: **phase 1 implemented, disabled by default**.

## Purpose

The gateway provides a bounded, read-only MCP surface over the production event
ledger without exposing raw SQLite, shell access or provider credentials. It is
also the policy boundary for future Telegram, VK and MAX reading/publication
adapters.

Phase one tools:

- `prod_health_snapshot` — tiny local database and recent-job snapshot;
- `events_find` — bounded event lookup, maximum 20 rows;
- `event_explain` — event fields, provenance, identity decisions, jobs and public surfaces;
- `source_trace` — exact local source trace without fetching Telegram/VK;
- `jobs_inspect` — bounded durable-outbox view;
- `ops_runs_inspect` — bounded operational-run ledger;
- `runtime_trace` — literal search in at most 1 MiB of the active log tail;
- `social_capabilities` — fail-closed Telegram/VK/MAX adapter policy status.

No phase-one tool performs a provider network request or changes production.

## Runtime topology

```text
Fly Machine / one volume
├── main bot process (primary, owns writes and provider credentials)
└── prod_ops_mcp sidecar (optional)
    ├── separate aiohttp listener on port 8091
    ├── SQLite mode=ro + PRAGMA query_only
    ├── no ORM mutators and no provider adapters
    └── bounded stateless JSON responses
```

`prod_ops_mcp.entrypoint` preserves the old runtime when the feature is off: it
`exec`s `python main.py`. When enabled it starts the bot as the primary process
and the MCP listener as an isolated, tightly bounded sidecar. Invalid MCP security settings
disable the sidecar without preventing the bot from starting. A crashed sidecar
is not automatically restarted, preventing a restart/log storm; the bot stays up.

## Authentication

The endpoint is:

```text
https://<host>:8443/<PROD_OPS_MCP_PATH_SECRET>/mcp
```

The secret path is **not authentication by itself**. Normal mode also requires:

```http
Authorization: Bearer <PROD_OPS_MCP_BEARER_TOKEN>
```

Rules:

- path and bearer secrets must be independent and at least 32 characters;
- bearer values are never accepted in query parameters;
- access logging is disabled for the sidecar so the path secret is not written by aiohttp;
- requests with an `Origin` header are rejected unless the exact origin is allowlisted;
- every request is re-authenticated; the MVP is stateless and has no server session;
- TLS termination is provided by Fly;
- responses use `no-store`, `no-referrer` and `nosniff` headers.

`PROD_OPS_MCP_ALLOW_PATH_ONLY_AUTH=1` exists only for clients that cannot send an
Authorization header. It exposes only `prod_health_snapshot`, `events_find` and
`social_capabilities`, clamps the request rate to 4/minute and outbound responses
to 256 KiB/hour, and can never enable writes.

## Resource and traffic budgets

Defaults are intentionally small:

| Guard | Default |
|---|---:|
| global ingress before authentication | 30/minute, burst 5 |
| concurrent requests | 1 |
| bearer requests | 12/minute, burst 3 |
| path-only requests | 4/minute |
| request body | 32 KiB |
| response body | 192 KiB |
| bearer response egress | 1 MiB/hour |
| path-only response egress | 256 KiB/hour |
| SQLite execution deadline | 300 ms |
| result cache | 10 seconds, 128 entries |
| rows per list tool | 20 |
| runtime log scan | 256 KiB default, 1 MiB hard maximum |
| SSE / long polling | disabled |
| Telegram/VK/MAX calls | zero in phase one |

Invalid-auth traffic is rejected by a cheap global token bucket before any database work, and rejected requests are not logged at INFO level. Large responses fail closed and ask the client to narrow the query. Repeated
bounded reads may be served from the in-memory TTL cache. Compression is enabled
only for JSON bodies above 2 KiB.

## Environment

Generate independent secrets without printing them into shell history:

```bash
python - <<'PY'
import secrets
print("PATH=" + secrets.token_urlsafe(32))
print("BEARER=" + secrets.token_urlsafe(32))
PY
```

Required for normal mode:

```text
ENABLE_PROD_OPS_MCP=1
PROD_OPS_MCP_PATH_SECRET=<independent URL-safe secret>
PROD_OPS_MCP_BEARER_TOKEN=<independent bearer secret>
PROD_OPS_MCP_PORT=8091
DB_PATH=/data/db.sqlite
```

Optional guards:

```text
PROD_OPS_MCP_ALLOWED_ORIGINS=
PROD_OPS_MCP_MAX_CONCURRENCY=1
PROD_OPS_MCP_INGRESS_REQUESTS_PER_MINUTE=30
PROD_OPS_MCP_INGRESS_BURST=5
PROD_OPS_MCP_REQUESTS_PER_MINUTE=12
PROD_OPS_MCP_BURST=3
PROD_OPS_MCP_EGRESS_BYTES_PER_HOUR=1048576
PROD_OPS_MCP_MAX_REQUEST_BYTES=32768
PROD_OPS_MCP_MAX_RESPONSE_BYTES=196608
PROD_OPS_MCP_DB_TIMEOUT_MS=300
PROD_OPS_MCP_CACHE_TTL_SECONDS=10
PROD_OPS_MCP_ALLOW_PATH_ONLY_AUTH=0
```

Do not set `PROD_OPS_MCP_ENABLE_WRITE`: phase one refuses to start a write-enabled
gateway.

## Fly exposure

The repository includes `infra/prod_ops_mcp/fly-service.example.toml`. It is a
reviewed snippet, not an automatically active production service. Add it to the
real Fly config only after setting the secrets and validating the endpoint on a
canary. The custom external port avoids competing with the bot's existing 443
service while keeping both processes on the one Machine that owns `/data`.

The listener must not receive its own health check that can restart the primary
bot. MCP failure is non-critical to bot availability.

## Social capability gate

Telegram, VK and MAX are represented as capabilities, never as generic HTTP or
raw target IDs.

Future read flow:

```text
platform + named surface
→ capability policy
→ cached/local evidence first
→ optional live-read budget
→ redaction + bounded response
```

Future publication flow:

```text
allowlisted target alias
→ render exact preview
→ durable plan with payload/state hash and expiry
→ explicit operator confirmation
→ execute through the bot's existing adapter
→ verify provider receipt/public URL
```

Required target form is an alias such as `telegram:kenigevents` or
`vk:kenigevents_afisha`. Arbitrary `chat_id`, `owner_id`, URL, raw API method,
SQL and shell commands remain forbidden. Provider tokens stay in the bot process
and are never returned to MCP clients.

Phase two must reuse existing Telegram/VK send logic rather than implement a
parallel publisher. MAX is a disabled adapter placeholder until a reviewed
transport exists.

## Acceptance gate before production exposure

1. Unit tests for auth, path-only scope, redaction, query bounds and social fail-closed policy pass.
2. Listener is unreachable on the bot's normal 443 route.
3. No MCP call produces Telegram/VK/MAX network traffic in phase one.
4. A one-hour canary stays within configured request, SQLite-time and response-byte budgets.
5. Bot webhook latency and scheduled-job latency show no material regression.
6. Secret path is absent from application and proxy logs.
7. Invalid/missing bearer receives 401; unknown Origin receives 403.
8. Full source text, descriptions, personal data and secrets are absent from responses.
9. Disabling `ENABLE_PROD_OPS_MCP` restores exact single-process `python main.py` behavior.

## Rollback

Set `ENABLE_PROD_OPS_MCP=0` and restart the Machine. The wrapper immediately
executes the original bot entrypoint and does not start a second process.
