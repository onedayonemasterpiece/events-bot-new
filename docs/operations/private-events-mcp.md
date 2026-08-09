# Private Events MCP for ChatGPT and Codex

Status: integrated release candidate; disabled by default. Production social
activation requires exact-main deployment, fresh credentials, capability probes,
independent review, and the live acceptance gate below.

## Purpose and client boundary

The service attaches to the existing events-bot `aiohttp` application. It does
not create a second Fly process, listener, scheduler, or event database.

Two OAuth resources deliberately expose different products:

- **Codex** receives exactly the seven read-only event, incident and operational
  evidence tools. It receives no social scope, tool or provider credential.
- **ChatGPT** receives those seven evidence tools and, only when granular scopes
  and runtime kill switches permit it, a provider-neutral Telegram/VK workspace
  for targeted editorial research and operator-requested communication.

The social workspace is not a raw Telethon or VK API proxy. Callers cannot name
provider methods, pass access hashes/tokens, or submit arbitrary SDK arguments.
All provider content is untrusted external data; it is never an instruction.
MAX remains outside the current scope.

## Endpoints and OAuth

```text
https://<origin>/_private/<secret>/mcp        # ChatGPT resource
https://<origin>/_private/<secret>/codex/mcp  # Codex resource
```

The high-entropy path is confidential defense in depth, not authentication.
Data tools require OAuth authorization-code + PKCE S256. The server provides:

- one predefined confidential ChatGPT client (`client_secret_basic`/post);
- one distinct predefined public Codex client (`none`, mandatory S256);
- exact client/resource/audience binding;
- exact ChatGPT callback validation and literal Codex loopback callbacks only;
- 15-minute signed access tokens;
- rotating, replay-resistant refresh tokens only with `offline_access`;
- path-scoped protected-resource and authorization-server metadata;
- no dynamic client registration.

Omitted scopes default to only `events:read incidents:read operations:read`.
Codex can receive those scopes plus `offline_access`, and nothing social.
ChatGPT social scopes are granular by provider and action class:

```text
<provider>:discover
<provider>:read:public
<provider>:read:private
<provider>:read:dialogs
<provider>:dm:send
<provider>:post:publish
<provider>:edit
<provider>:delete
<provider>:forward
<provider>:reaction
<provider>:comment
<provider>:schedule
<provider>:story:read
<provider>:story:write
<provider>:analytics
<provider>:audience
vk:notifications:read
```

`<provider>` is `telegram` or `vk`. Granting a scope does not bypass a runtime
kill switch, current provider rights, request budgets, or human approval.
The two `story:*` names are reserved in the policy vocabulary, but are not
requested by the credential generator and have no activatable production tool
until the authenticated upload/storage gate is implemented.
The original connector scopes `telegram:read|publish` and `vk:read|publish`
remain stable provider-level compatibility families. On the ChatGPT resource,
`*:read` authorizes later typed reads for the same provider and `*:publish`
authorizes later typed mutations for the same provider. The mapping never
crosses provider or read/write boundaries, and every typed mutation still needs
the independent server-side preview/approval/commit flow. Codex can never
receive these scopes. This lets normal MCP tool evolution preserve the existing
connector URL, client identity and name; only a genuinely new capability family
requires new OAuth consent.

After the first successful connection rotate the one-time bootstrap operator
token. Do not issue or share access/refresh tokens manually.

## Evidence tools

Codex exposes exactly these seven tools; ChatGPT retains them unchanged:

| Tool | Scope | Contract |
|---|---|---|
| `search` | all three read scopes | ChatGPT Search compatibility across stable evidence IDs |
| `fetch` | all three read scopes | fetch `event:`, `incident:`, `run:` and `job:` evidence |
| `events_search` | `events:read` | bounded event query |
| `event_get` | `events:read` | Event 360 evidence |
| `incidents_search` | incident + operations | repository reports plus runtime failures |
| `incident_get` | incident + operations | report/run/job evidence document |
| `operations_snapshot` | `operations:read` | bounded state, failures and SQLite health |

These tools are read-only, non-destructive and idempotent. The event SQLite file
is opened with URI `mode=ro`, `PRAGMA query_only=ON`, bounded rows and a VM/time
deadline. There is no raw SQL, shell, arbitrary outbound HTTP or write tool.

## ChatGPT social workspace

When the universal workspace is enabled, ChatGPT can discover only the tools
matching its granted scopes and the enabled provider/capability flags:

| Tool | Purpose |
|---|---|
| `social_capabilities` | current provider, target and action capabilities |
| `social_target_resolve` | resolve Saved/self, exact person, channel/group/community or known provider reference into an opaque bound ref |
| `social_item_resolve` | resolve one canonical VK wall-post URL into bound item and source-target refs |
| `social_targets_search` | bounded target search |
| `social_targets_list` | bounded accessible dialogs/managed targets |
| `social_content_search` | bounded keyword search |
| `social_content_feed` | bounded target feed/history |
| `social_content_item` | fetch one bound item |
| `social_content_thread` | comments or reaction summaries |
| `social_comment_hints_list` | bounded recent VK comment/mention notifications as untrusted investigation hints |
| `social_content_editorial_sample` | purpose-bound editorial sample, at most 25 items/page and 100 cumulatively |
| `social_content_analytics` | bounded aggregate statistics/audience counts where the credential is entitled |
| `social_action_prepare` | freeze exact typed action/content/target/media digest; no provider call |
| `social_action_commit` | consume server-recorded human approval, then make the sole provider attempt |
| `social_action_status` | reconcile durable success/failure/outcome-unknown state |

### Targeted editorial research

`social_content_editorial_sample` is intended for a user-named target, for
example examining its description and up to 100 recent owner posts to infer
editorial policy, topics and characteristic phrasing. It is not a crawler or
bulk indexer:

- page size is at most 25 and the server cursor enforces a cumulative maximum of
  100 for the exact target, purpose and access class;
- the caller must state the editorial purpose and one closed authorization
  basis: `self_owned`, `operator_authorized`, or `provider_approved`; that basis
  is bound into the server sample state and its sanitized audit receipt;
- the cursor and sample ref cannot be replayed for another target or access
  class;
- provider text is clipped, recursively redacted, marked
  `untrusted_external_data`, and not persisted as a corpus;
- the response records sample size, date range and truncation/exclusion facts;
- no member graph, commenter identity or arbitrary linked-page fetch occurs.

### Communication and mutation

The active workspace uses typed actions such as `send_message`, `publish`,
`comment`, `reaction`, `forward`, `schedule`, `edit` and `delete`. The target and
source item are provider-resolved opaque refs. Saved Messages is a first-class
Telegram self target; an exact user/DM is resolved and previewed as that person,
not as an alias guessed by the model.

Every external mutation follows:

1. `social_action_prepare` validates scope, feature flag, provider rights,
   budgets, target/item bindings, content/entities/assets and idempotency;
2. it persists only the exact canonical digest and returns an operator approval
   URL; it does not call the provider;
3. the operator opens that URL, enters the separate approval token, inspects the
   exact human-readable target, destination, source target, source item, action
   and text, then confirms in the browser; an opaque-only target/item preview
   or an item without a human-readable source target fails closed;
4. `social_action_commit` accepts only the preparation ref and digest. It
   rechecks the current action-class kill switch, then atomically consumes the
   server-side approval before one provider attempt;
5. `social_action_status` or read-after-write evidence reconciles the receipt.

The approval token is never pasted into ChatGPT. It is not an OAuth token and
must not appear in model context, logs, PRs or artifacts. A provider timeout is
`outcome_unknown` and `retry_safe=false`; do not retry with a new idempotency
key until reconciliation.

Runtime kill switches are additional, fail-closed controls:

```text
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_TELEGRAM_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_VK_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_PRIVATE_READ_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_DM_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_POST_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_EDIT_DELETE_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_MEDIA_STORY_ENABLED
```

Child switches cannot enable a disabled master/provider. Media/story activation
is currently rejected by configuration: there is no authenticated byte-bound
upload route yet, so `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_MEDIA_STORY_ENABLED=1`
fails startup validation instead of exposing placeholder asset/story tools.
The generator does not request story scopes. Text in a social message is never
treated as a media URL to fetch.

## Provider boundary

Core/runtime modules remain provider-neutral. `create_app()` lazily injects the
workspace adapters only when both MCP and the universal workspace are enabled.
Adapter construction performs no provider request.

### Telegram

Telegram uses only `TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP` plus
`TELEGRAM_API_ID`/`TELEGRAM_API_HASH` (the existing `TG_*` aliases are accepted
for the API pair). It never falls back to `TELEGRAM_AUTH_BUNDLE_E2E`,
`TELEGRAM_SESSION`, `TELEGRAM_AUTH_BUNDLE_S22`, or the bot token.

The dedicated Telethon workspace provides typed resolve/read/search/editorial
sample/basic statistics and text action operations. It supports Saved via the
authorized account itself and exact-person/dialog resolution. Opaque refs hide
provider access hashes. Cross-process SQLite fencing serializes the dedicated
session, persists FloodWait/cooldown state, and rejects a lost lease. Live
rights and granular `ChatBannedRights` are rechecked before mutation. The
adapter carries immutable peer snapshots through preflight, atomically claims
caller-issued operation refs, and preserves timeout/lost-fence outcomes for
reconciliation. Encrypted target/item/cursor bindings and operation receipts are
kept in the isolated auth database, so an approval or reconciliation does not
lose its provider binding on process restart.

### VK

The universal VK workspace does not reuse `main.vk_api` or its implicit actor
fallback. Each fixed typed method is mapped to one explicit role credential:

```text
PRIVATE_EVENTS_MCP_VK_PUBLIC_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_NOTIFICATION_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_DIALOG_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_USER_MESSENGER_TOKEN
PRIVATE_EVENTS_MCP_VK_COMMUNITY_EDITOR_TOKEN
PRIVATE_EVENTS_MCP_VK_ANALYTICS_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_STORY_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_STORY_EDITOR_TOKEN
```

Only configured actor/action capability sets are advertised. The transport uses
fixed VK API 5.199 method paths, rejects redirects, bounds responses, and emits
only sanitized provider error codes. Public-wall and private-dialog access are
separate; a public scope cannot route to conversation history. Cursor context
binds target, operation and access class. Writes bind the full intent digest and
operation ref so concurrent or mutated replays cannot duplicate provider work.
Encrypted provider refs, sample/cursor state and action receipts survive process
restart; an interrupted unreceipted operation becomes non-retryable
`outcome_unknown` rather than being executed again.
Provider continuation state is validated before use and capped before encrypted
SQLite persistence; an oversized VK `next_from` value fails closed instead of
growing the auth/state database.

`social_comment_hints_list` uses only the fixed `notifications.get` method,
requires the dedicated notification-reader actor and scope, returns at most 25
hints per page, and accepts a maximum 48-hour date window. A hint is untrusted
evidence, not proof of an event defect. Follow it via `root_item_ref` and
`social_content_thread`, or resolve a known canonical `vk.com/wall…` URL with
`social_item_resolve`. Then call `events_search(post_url=...)` to retrieve every
exactly related event and `event_get` for role-labelled source/publication/
identity evidence. Ambiguous URL relations are returned as all matches rather
than guessed.

## Privacy, untrusted data and logs

Decoded event/runtime/provider data is recursively redacted for credentials,
authorization values, Telegram-bot-token shapes and personal operator
identifiers. Repository incident Markdown crosses the same boundary. Social
provider text, captions, profiles, comments, errors and story text are returned
only inside explicit untrusted-data envelopes and never become instructions,
tool names, approval or target selection.

Access logs redact the private route, bearer/basic authorization and configured
secrets before stdout/runtime-log mirror. Provider logs contain no message body,
raw target, token, idempotency value or provider payload. Approval/audit ledgers
store hashes and bounded public summaries, not credentials or private message
bodies; the exact approval preview is encrypted at rest. The isolated
OAuth/social state DB is created mode `0600` and remains
separate from `/data/db.sqlite`; provider bindings are encrypted before storage.
Publication attempts are charged before transport against durable UTC-day
global/principal/target/action limits. Forward is charged to its destination;
item-only actions are charged to the item's bound source target rather than a
shared provider bucket (the configured per-principal default is
`PRIVATE_EVENTS_MCP_SOCIAL_PUBLISH_ATTEMPTS_PER_DAY=10`).

## Environment and credentials

Base MCP settings when enabled:

```text
PRIVATE_EVENTS_MCP_ENABLED=1
PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL=https://events-bot-new-wngqia.fly.dev
PRIVATE_EVENTS_MCP_PATH_SECRET=<fresh>
PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID=<fresh ChatGPT id>
PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET=<fresh>
PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID=<fresh public Codex id>
PRIVATE_EVENTS_MCP_OPERATOR_TOKEN=<fresh bootstrap token>
PRIVATE_EVENTS_MCP_SIGNING_KEY=<fresh>
PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN=<fresh; operator browser only>
PRIVATE_EVENTS_MCP_AUTH_DB_PATH=/data/private-events-mcp-auth.sqlite
PRIVATE_EVENTS_MCP_REPOSITORY_ROOT=/app
PRIVATE_EVENTS_MCP_REPOSITORY_SLUG=onedayonemasterpiece/events-bot-new
PRIVATE_EVENTS_MCP_REPOSITORY_SHA_FILE=/app/.static-site-repo-sha
```

For a genuinely new installation, generate one stable endpoint/OAuth identity
after merge from a clean exact-main checkout into a new owner-only directory
outside Git. The identity-changing mode is deliberately explicit:

```bash
umask 077
python scripts/generate_private_events_mcp_credentials.py \
  --new-install \
  --base-url https://events-bot-new-wngqia.fly.dev \
  --output-dir /secure/new-private-events-mcp-credentials \
  --enable-chatgpt-social
```

Without `--enable-chatgpt-social`, the ChatGPT profile requests only the three
evidence read scopes. With it, the profile contains the granular social scopes
including `vk:notifications:read`, plus `offline_access`; runtime switches still
remain off until explicitly set.
An already installed connector with the four original provider-level social
scopes does not need a new name or identity: the compatibility families above
cover later typed tools within the same provider/read-write boundary.
The generator has no implicit/default generation mode. `--new-install` creates
a new private path, ChatGPT client ID/secret, Codex client ID, signing key,
social-approval token and bootstrap token. Do not use it for routine bootstrap
rotation: changing those stable values can invalidate an installed connector,
refresh-token/signing state and saved endpoint configuration. Use a complete
new identity only for an initial install or an explicit full-identity response
such as a compromised private path.

After the first successful browser connection, rotate only the one-time
bootstrap operator token from the **full** credentials JSON produced for the
installed identity:

```bash
umask 077
python scripts/generate_private_events_mcp_credentials.py \
  --rotate-bootstrap-only \
  /secure/new-private-events-mcp-credentials/chatgpt-private-app-credentials.json \
  --output-dir /secure/private-events-mcp-bootstrap-rotation-20260809
```

This mode changes exactly three consistent copies of the same value:
`deploy.PRIVATE_EVENTS_MCP_OPERATOR_TOKEN`,
`chatgpt.bootstrap_operator_token`, and `codex.bootstrap_operator_token`.
It preserves every URL/path, OAuth client value, signing/state path, scope,
social-approval value and all unknown forward-compatible fields value-for-value
at the JSON data level. It refuses incomplete/inconsistent full bundles,
source/output overlap, symlinked source or output paths, and mode-changing
options such as `--base-url` or `--enable-chatgpt-social`.
The base URL must be a canonical HTTPS origin with valid DNS labels or a
canonical IP literal and no credentials, legacy numeric-IP spelling, IPv6 zone,
whitespace/control characters, query, fragment or explicit port.
Every deploy field, including a forward-compatible
unknown field retained during rotation, must have a valid environment name and
a single-line NUL-free string value before `fly-secrets.env` can be written.

Both modes require a fresh output path under an existing non-symlink parent.
The generator creates the output directory as `0700` and every artifact with
`O_EXCL`/no-follow semantics and exact `0600` permissions. Source credentials
must remain owner-only and outside Git. Stdout contains only artifact paths, a
public origin, redacted path, mode and endpoint fingerprints; it never contains
the private endpoint, bootstrap token, client secret, signing key, private path
or social-approval token.

The legacy `PRIVATE_EVENTS_MCP_SOCIAL_TARGETS_JSON` policy applies only to the
narrow compatibility adapters when the universal workspace is off. It is not a
read-target setting and cannot enable Saved or arbitrary users.

## Integration and disabled behavior

`main_part2.create_app()` selects exactly one social implementation:

- universal enabled: build the Telegram/VK workspace adapter set and omit the
  legacy tools;
- universal disabled: optionally keep the prior alias-only text adapters;
- MCP disabled: parse no MCP/provider secrets, build no adapter and attach no
  route.

The overlay installer copies the provider-neutral package plus the three root
provider modules, patches `main_part2.py` (or the single valid app module), and
merges private fixtures without replacing an existing `tests/conftest.py`.

## Local verification

```bash
PYTHONPATH=. python -m compileall -q \
  private_events_mcp \
  private_events_mcp_provider_adapters.py \
  private_events_mcp_telegram_adapter.py \
  private_events_mcp_vk_adapter.py \
  private_events_mcp_workspace_providers.py \
  tests scripts main_part2.py
PYTHONPATH=. pytest -q tests/test_private_events_mcp_*.py

git diff --check
```

The repository CI runs the MCP compile/test gate explicitly. Independent review
must use the exact proposed head SHA.

## Production acceptance gate

Deploy only an exact merged `origin/main` SHA via `scripts/deploy_fly_main.sh`.
Keep all new social switches off until credentials are staged and the
provider-specific canary is authorized.

Record all of the following without secrets:

1. merged main SHA, Fly release and in-container immutable SHA are identical;
2. public/internal `/healthz` remains ready, DB check is healthy and
   `PRAGMA quick_check` is `ok`;
3. ChatGPT confidential and Codex public OAuth/PKCE flows, exact resource
   metadata, access expiry and refresh rotation pass; an unauthenticated
   `tools/call` returns HTTP 401 with the exact endpoint-specific
   `WWW-Authenticate` resource-metadata challenge (public initialization and
   catalogue discovery remain available);
4. Codex `tools/list` is exactly the seven evidence tools and direct social calls
   fail; ChatGPT lists only granted and enabled workspace tools;
5. real event `search -> fetch`, `events_search -> event_get`, incident evidence
   and `operations_snapshot` pass; the event DB digest/row state is unchanged;
6. unsupported MCP protocol version and JSON-RPC batch return HTTP 400;
7. nested synthetic credentials and provider native identifiers never appear in
   responses, logs or artifacts;
8. targeted editorial canary records its closed authorization basis, reads the
   named target in pages and cannot exceed 100 or replay a cursor against
   another target/access class;
9. Telegram Saved canary prepares, browser-approves, commits exact text, then
   reads back the exact provider receipt/message; do not auto-delete it;
10. one explicitly authorized exact-person reminder canary resolves and previews
    the intended person before any send; no arbitrary/bulk fan-out;
11. VK public/editorial and any write canary use the explicit configured actor,
    never fallback, and verify the exact receipt/read-back;
12. mutations fail before provider transport when scope, feature flag, current
    rights, target/item binding, approval, budget or idempotency is invalid;
13. provider timeout/fence loss remains outcome-unknown and reconciliation does
    not blind-retry; encrypted provider refs/cursors/receipts remain usable
    after a controlled restart, while an interrupted action remains unknown;
14. provider call evidence is zero for Codex/evidence queries and accurately
    attributed for the separately approved social canary;
15. webhook latency/errors, scheduler/jobs, runtime-log mirror, disk free space
    and auth DB permissions show no regression; the auth/provider state file is
    `0600` from creation and daily attempt budget rows use the current UTC date.

Rollback: turn `PRIVATE_EVENTS_MCP_ENABLED=0` (or the universal social master
switch off for a social-only rollback) and redeploy the exact approved main SHA.
The isolated OAuth/social state database may remain on the volume.
