# Private Events MCP for ChatGPT and Codex

Status: core implementation-ready, disabled by default until explicit OAuth and
provider-adapter production activation.

## Purpose

Expose a narrowly bounded, read-only MCP surface over the events-bot production SQLite database and incident evidence so a private ChatGPT app can:

- search canonical events and fetch Event 360 evidence;
- inspect repository incident reports;
- correlate incident reports with `ops_run`, `joboutbox`, Smart Update review and publication evidence;
- obtain a compact production-state snapshot.

This is not an administration API, SQL console, crawler, event-announcement
pipeline, media gateway, or arbitrary provider-method proxy. The ChatGPT
resource can additionally expose narrowly scoped generic Telegram/VK text
tools when an adapter is injected and an explicit target-alias policy is set.

## Runtime shape

The package attaches routes to the existing `aiohttp.web.Application`. It creates no additional Fly process, machine, listener, scheduler, poller or persistent provider connection.

```text
ChatGPT private app -> existing /mcp resource (read plus separately scoped social)
Codex              -> distinct /codex/mcp resource (exactly seven read tools)
  -> HTTPS + OAuth authorization code + PKCE S256
  -> existing events-bot aiohttp process
  -> bounded MCP JSON-RPC dispatcher
  -> SQLite file: URI mode=ro + PRAGMA query_only=ON
  -> incident Markdown index under /app/docs/reports/incidents
```

`attach_private_events_mcp(app)` is a strict no-op unless `PRIVATE_EVENTS_MCP_ENABLED=1`.

## MCP endpoint and OAuth

The generated endpoint is:

```text
https://<production-origin>/_private/<high-entropy-path>/mcp
https://<production-origin>/_private/<high-entropy-path>/codex/mcp
```

The high-entropy path is not published through root well-known metadata and reduces unsolicited traffic. It is still defense in depth, not authentication: all data tools require OAuth.

The bundled single-operator authorization server provides:

- OAuth authorization-code grant;
- mandatory PKCE S256;
- exact client/resource binding: ChatGPT can authorize only the existing
  `/mcp` resource and Codex only the distinct `/codex/mcp` resource;
- a predefined confidential ChatGPT client ID and secret;
- a distinct predefined public Codex client ID using
  `token_endpoint_auth_method=none` (no Codex client secret);
- client-specific redirect allowlisting: existing ChatGPT OAuth callback paths,
  or for Codex only the literal
  `http://127.0.0.1:<explicit-port>/callback/<opaque>` form with a single
  URL-safe opaque segment and no query, fragment or userinfo;
- 15-minute HMAC-signed access tokens;
- durable rotating refresh tokens in `/data/private-events-mcp-auth.sqlite`;
- refresh-token replay rejection;
- path-scoped RFC 9728 protected-resource metadata;
- path-scoped OAuth authorization-server metadata;
- MCP `mcp/www_authenticate` challenges.

Authorization codes and refresh tokens are bound to the exact static client and
resource; authorization codes are additionally bound to the exact redirect URI
and PKCE S256 verifier. Dynamic client registration is not exposed. The
operator enters the bootstrap token on the authorization page. Rotate
`PRIVATE_EVENTS_MCP_OPERATOR_TOKEN` after the first successful connection.

If `scope` is omitted, a client receives only the three registered online read
scopes (`events:read`, `incidents:read`, `operations:read`), never a global
maximum. ChatGPT's maximum additionally permits `telegram:read`,
`telegram:publish`, `vk:read`, and `vk:publish`; Codex's maximum is only the
three read scopes plus optional `offline_access`. A refresh token is issued or
rotated only while `offline_access` remains explicitly granted. Codes, access
tokens, and refresh tokens fail when moved across either client or resource.

## Tools

| Tool | Scope | Contract |
|---|---|---|
| `search` | all read scopes | ChatGPT Search compatibility; returns stable IDs, titles and URLs |
| `fetch` | all read scopes | ChatGPT Fetch compatibility for `event:`, `incident:`, `run:` and `job:` IDs |
| `events_search` | `events:read` | bounded event query by text/date/city/type/lifecycle |
| `event_get` | `events:read` | Event 360: canonical event, sources, source facts, jobs, posters and Smart Update reviews |
| `incidents_search` | `incidents:read`, `operations:read` | repository incident reports plus runtime failure evidence |
| `incident_get` | `incidents:read`, `operations:read` | complete report, `ops_run` or `joboutbox` evidence document |
| `operations_snapshot` | `operations:read` | counts, status distribution, recent failures and SQLite quick check |
| `telegram_read` | `telegram:read` | recent non-empty text from an allowlisted Telegram alias; stable `post_id`, timestamp and explicit untrusted-data marker |
| `vk_read` | `vk:read` | recent non-empty text from an allowlisted VK alias; stable `post_id`, timestamp and explicit untrusted-data marker |
| `prepare_text_publication` | matching provider publish scope | create a short-lived ticket bound to client, subject, resource, platform, alias, exact text hash and idempotency key; no provider call |
| `publish_prepared_text` | matching provider publish scope | consume the exact one-use ticket and make at most one provider attempt; destructive, open-world and non-idempotent |

The seven evidence tools remain read-only, non-destructive and idempotent.
Social tools are never present on the Codex endpoint. Anonymous discovery and
tokens without matching granted scopes do not reveal them. The commit tool is
truthfully annotated destructive, open-world and non-idempotent and is never
cached. Read cache partitions include resource, client, subject, exact granted
scopes and target-policy fingerprint.

## Social adapter and target policy

`private_events_mcp.social.SocialAdapter` is the provider-neutral seam. It has
only `read_text(target, limit)` and `publish_text(target, text,
idempotency_key)` operations. Core code imports no Telethon/VK SDK and performs
no provider call without an injected adapter. There is no media, edit, delete,
forward, MAX, raw URL, raw target-ID or arbitrary method tool.

`PRIVATE_EVENTS_MCP_SOCIAL_TARGETS_JSON` is the only target authority. Blank or
omitted configuration is an empty deny-all policy. The strict shape is:

```json
{
  "telegram": {
    "public_alias": {
      "provider_target": "<numeric Telegram channel id>",
      "allow_read": true,
      "allow_publish": false
    }
  },
  "vk": {
    "public_alias": {
      "provider_target": "<numeric VK owner id>",
      "allow_read": true,
      "allow_publish": false
    }
  }
}
```

Unknown platforms/fields, malformed aliases, URLs and non-boolean permissions
fail startup. Provider targets stay inside the resolved adapter target and are
never returned or audited. Provider read text passes through the existing
recursive credential/operator redaction and clipping boundary and every item
is marked `untrusted_external_data`.

Preparation tickets live only in the isolated OAuth SQLite database, never the
event database. A ticket is one-use and bound to client, subject, resource,
platform, alias, exact text hash, expiry and idempotency-key hash. Durable
uniqueness prevents preparing the same idempotency key again for 90 days after
timeout, restart, or an unknown provider outcome; cleanup keeps the isolated
auth database bounded. The ticket is consumed before the provider attempt, so
cancellation cannot authorize replay. A persistent UTC daily attempt budget,
keyed by the same principal/resource/platform/alias boundary, defaults to 10,
is reserved atomically when a unique preparation ticket is created, and
survives access-token refresh and process restart. A provider timeout is
reported as `outcome=unknown`, `retry_safe=false`; the caller must not retry
with a new idempotency key because the original publication may have succeeded.
The separate append-only action audit stores only fixed-shape hashes/fingerprints
and public aliases—never message text, provider target, ticket, idempotency key,
receipt, credential, or provider error.

Each streamable-HTTP `POST` accepts exactly one JSON-RPC object. JSON-RPC
batches are rejected with HTTP `400`, so a caller cannot multiply SQLite work
inside one admission/rate slot. When `MCP-Protocol-Version` is present it must
name a supported protocol version; invalid or unsupported versions also fail
closed with HTTP `400`. The same validation applies to the protocol version in
an `initialize` request instead of silently negotiating an unsupported value.

## Data and privacy boundary

The implementation has no raw SQL tool. User input is accepted only as bounded typed arguments and is applied to internal parameterized statements.

Included event evidence is limited to explicit allowlisted columns. Column names containing `password`, `secret`, `token` or `api_key` are removed defensively. The MCP package never reads process environment values back to a tool response.

Decoded runtime JSON is recursively redacted for credential-bearing keys,
authorization material and personal operator identifiers, including nested
objects and arrays. Credential-shaped values embedded in text are redacted as
well. This output-boundary filter also covers repository incident documents,
generic and provider bot tokens, and operator/reviewer email or username
identifiers while preserving non-secret usage counters such as `total_tokens`.
Telegram, VK and other source text is explicitly labeled as untrusted external
data in both search and fetch output and must never be treated as instructions.
Telegram bot-token shaped values embedded in nested text are redacted. Tool
calls reject undeclared top-level arguments rather than silently accepting raw
target IDs, URLs, provider methods, media, edit/delete requests, or future
platforms.

The core package makes no direct Telegram/VK request and imports no provider
SDK; only an explicitly injected adapter can perform a provider operation.
No MAX, Supabase, Telegraph, Catbox, ImageKit or LLM request is available.
Images and videos are not proxied.

The enabled integration installs an aiohttp access-log filter before serving
requests. The private path and authorization credentials are replaced with
`<redacted>` before records reach stdout or the bounded runtime-log mirror.

## Resource budgets

Defaults:

- 64 KiB request body;
- 128 KiB response body;
- 25 rows;
- 60,000 characters per fetched document;
- 350 ms SQLite VM budget;
- 250 ms SQLite busy wait;
- 2 concurrent MCP requests;
- 60 authenticated requests per minute;
- 30 anonymous OAuth token requests per minute;
- 8 MiB response egress per hour;
- 20-second result cache;
- incident scan bounded to 3 MiB.

Authenticated rate buckets are bound to the OAuth client and subject, so an
access-token refresh does not reset the request budget. The token endpoint uses
the same body-size and concurrency admission boundary. The SQLite adapter opens
a new `mode=ro` connection per operation, sets `query_only=ON`, and interrupts
both queries and schema discovery when the VM deadline expires. Blocking SQLite
work runs via `asyncio.to_thread`, preserving the webhook event loop.

## Environment

Required only when enabled:

```text
PRIVATE_EVENTS_MCP_ENABLED=1
PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL=https://events-bot-new-wngqia.fly.dev
PRIVATE_EVENTS_MCP_PATH_SECRET=<generated>
PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID=<generated>
PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET=<generated>
PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID=<generated-non-secret-static-id>
PRIVATE_EVENTS_MCP_OPERATOR_TOKEN=<generated>
PRIVATE_EVENTS_MCP_SIGNING_KEY=<generated>
PRIVATE_EVENTS_MCP_AUTH_DB_PATH=/data/private-events-mcp-auth.sqlite
PRIVATE_EVENTS_MCP_REPOSITORY_ROOT=/app
PRIVATE_EVENTS_MCP_REPOSITORY_SLUG=onedayonemasterpiece/events-bot-new
PRIVATE_EVENTS_MCP_REPOSITORY_SHA_FILE=/app/.static-site-repo-sha
PRIVATE_EVENTS_MCP_SOCIAL_TARGETS_JSON=<strict explicit JSON or blank deny-all>
PRIVATE_EVENTS_MCP_SOCIAL_TICKET_TTL_SECONDS=300
PRIVATE_EVENTS_MCP_SOCIAL_PROVIDER_TIMEOUT_SECONDS=12
PRIVATE_EVENTS_MCP_SOCIAL_PUBLISH_ATTEMPTS_PER_DAY=10
```

When `PRIVATE_EVENTS_MCP_ENABLED` is false, malformed or stale MCP-only
configuration is not parsed and no MCP route is attached; existing startup,
webhook, scheduler and health behavior therefore remains unchanged.

When enabled, both distinct static client IDs are required. The existing
`PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID` and secret remain the confidential
ChatGPT registration; the Codex ID must never be paired with a client secret.

Provider credentials are owned by separately injected adapters, not by the MCP
core or target policy. `create_app()` constructs both adapters only after the
MCP enabled gate and the adapters remain lazy: construction neither decodes a
session nor calls a provider. Disabled MCP startup therefore does not import
Telethon or parse/validate provider credentials.

### Telegram provider adapter

Telegram generic reads and plain-text publications use a Telethon human client
with the dedicated `TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP` role only. The bundle
is base64url JSON containing `session` and optional `device_model`,
`system_version`, `app_version`, `lang_code`, and `system_lang_code`. API
credentials are read from `TELEGRAM_API_ID` / `TELEGRAM_API_HASH`, with the
existing `TG_API_ID` / `TG_API_HASH` names accepted as aliases. There is no
fallback to `TELEGRAM_AUTH_BUNDLE_E2E`, `TELEGRAM_SESSION`, or
`TELEGRAM_AUTH_BUNDLE_S22`, and the bot token is never used.

Each operation is serialized on a session lock, connects a fresh client,
checks that the human session is authorized, and disconnects in `finally`.
Reads scan at most 100 recent messages to collect the requested bounded count
of non-empty texts. Publications call only Telethon `send_message` with
`parse_mode=None` and link previews disabled. They create a new plain-text
message; they do not enter event, outbox, Telegraph, card, media, edit, delete,
forward, story, or MAX flows.

### VK provider adapter

VK uses the existing runtime `main.vk_api` credential and throttling path behind
a fixed-method adapter. Reads call only `wall.get` with the negative allowlisted
community owner ID, `filter=owner`, and a bounded scan count. Publications call
only `wall.post` with that owner, `from_group=1`, `signed=0`, the exact requested
text, and a deterministic SHA-256 `guid` derived from the core idempotency key.
No method name, raw owner ID, URL, attachment, schedule, edit, delete, or raw VK
response is exposed through MCP. Provider failures cross the seam only as a
generic `SocialAdapterError` without credentials, target IDs, or provider
payloads.
The adapter marks these two fixed calls for a full runtime-log boundary:
provider errors redact the entire parameter map, provider message, token,
captcha fields, owner ID, publication text and idempotency GUID before either
the normal logger or the generic adapter exception can observe them.

Generate connection material only into a fresh path:

```bash
python scripts/generate_private_events_mcp_credentials.py \
  --base-url https://events-bot-new-wngqia.fly.dev \
  --output-dir /secure/new-private-events-mcp-credentials \
  --enable-chatgpt-social
```

Omit `--enable-chatgpt-social` for the least-privilege three-scope read-only
ChatGPT profile. Supplying the flag is an explicit operator choice that adds
Telegram/VK read and publish scopes plus `offline_access`; Codex remains fixed
to the read-only scope maximum in both cases.

The generator refuses an existing output directory, creates the new directory
as `0700`, and creates every credential/config file atomically as `0600` with
exclusive creation. Its stdout receipt contains only the public origin, a
redacted MCP path and an endpoint fingerprint; the private path is present only
inside the owner-only files.

## Integration

In `create_app()`, immediately after `app = web.Application()` and before route registration,
the enabled configuration injects the two narrow provider adapters:

```python
from private_events_mcp import PrivateEventsMCPConfig, attach_private_events_mcp
from private_events_mcp_provider_adapters import build_private_events_mcp_social_adapters

config = PrivateEventsMCPConfig.from_env()
adapters = build_private_events_mcp_social_adapters(vk_api) if config.enabled else None
attach_private_events_mcp(app, config, social_adapters=adapters)
```

The bundled `scripts/apply_private_events_mcp_overlay.py` performs this insertion
idempotently in the repository's single app module (`main.py` or
`main_part2.py`), copies the package, tests, scripts and documentation, and
merges its fixtures without overwriting an existing `tests/conftest.py`.

## Verification

Before deployment:

```bash
PYTHONPATH=. python -m compileall -q private_events_mcp private_events_mcp_provider_adapters.py tests scripts main_part2.py
PYTHONPATH=. pytest -q tests/test_private_events_mcp_*.py
```

After an exact-SHA deployment:

```bash
python scripts/smoke_private_events_mcp.py \
  --credentials /secure/path/chatgpt-private-app-credentials.json

python scripts/smoke_private_events_mcp.py \
  --credentials /secure/path/chatgpt-private-app-credentials.json \
  --client codex
```

The smoke defaults to the confidential ChatGPT registration; `--client codex`
uses the public-client contract without Basic authentication or a client
secret. Both modes perform protected-resource discovery, authorization code +
PKCE, token exchange, authenticated MCP initialize, tool discovery, one event
query, one incident query and an operations snapshot. They print token
fingerprints only.

## Production acceptance gate

Activation is accepted only when all of the following are recorded:

1. exact deployed repository SHA and Fly release;
2. `/healthz` stays healthy;
3. OAuth metadata and PKCE flow pass;
4. Codex `tools/list` exposes exactly the seven read tools; ChatGPT anonymous or
   read-only grants expose the same seven, while explicitly granted social
   scopes expose only their matching Telegram/VK tools;
5. event search and fetch return a real production event;
6. incident search returns at least one repository report and one runtime failure when such evidence exists;
7. `operations_snapshot.database.mode == read_only`; core evidence queries make
   no provider call, and social acceptance uses a separately reviewed adapter;
8. event database SHA-256 or SQLite `data_version` evidence is unchanged across smoke queries;
9. Telegram webhook latency/error counters show no material regression;
10. credentials never appear in logs, commits, PR text or CI artifacts.
11. unsupported `MCP-Protocol-Version` and JSON-RPC batch requests both return
    HTTP `400`;
12. nested synthetic credentials do not appear in any tool output.
13. denied aliases, mutated/expired/replayed tickets, repeated idempotency keys,
    daily-budget overflow and cross-client/resource tokens all fail before a
    second provider attempt; denied prepare/publish calls are audited and audit
    rows contain no raw sensitive values;
14. provider timeout is surfaced as outcome unknown and unsafe to retry, while
    Telegram bot-token shaped strings and undeclared tool arguments are removed
    or rejected before reaching the caller/provider.

Rollback is setting `PRIVATE_EVENTS_MCP_ENABLED=0` and redeploying the exact approved SHA. The OAuth state database can remain on the volume; it is isolated from event data.
