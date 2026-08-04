# Smart Update production health audit

> **Status (2026-08-04 UTC):** one-shot, protected, read-only production
> observer. This is the canonical operator runbook for
> `.github/workflows/smart-update-prod-audit.yml` and
> `scripts/ops/smart_update_prod_audit.py`; it is not a deploy, repair or replay
> procedure.

## Purpose and hard boundary

The audit describes what Smart Update did in one exact UTC window and whether
its production dependencies and downstream state are healthy. It must not
change Smart Update production logic or state.

The observer must never:

- restart, replace, update or deploy a Fly Machine;
- execute `INSERT`, `UPDATE`, `DELETE`, DDL, a write-capable RPC, or a SQLite
  connection without `mode=ro`;
- start an LLM/provider request, Kaggle kernel, builder or other job;
- enqueue/retry/claim/finalize an outbox row;
- publish or edit the static site, Telegraph, Telegram, VK or ICS;
- copy/download `/data/db.sqlite`, its WAL/SHM, or any production database;
- create files below `/data` (including audit scripts, DB copies and output);
- emit secrets, full environment dumps, source text, facts containing source
  prose, prompts/completions, Telegram IDs, user data, raw source URLs, key IDs
  or private provider payloads.

All emitted evidence is PII-free and sanitized. Production access is only for
observation; a finding never authorizes a repair in the same run.

## Security and threat model

The workflow is manual-only (`workflow_dispatch`), has `permissions:
contents: read`, uses concurrency group `smart-update-prod-audit` with
`cancel-in-progress: false`, and binds its job to the protected GitHub
Environment `production-readonly`.

The job installs `flyctl` on the ephemeral runner before use; it does not rely
on a preinstalled or repository-bundled binary.

The environment must allow deployment only from protected `main` and require
an independent reviewer. Branch protection and review are part of the safety
boundary: the workflow rejects every ref except `refs/heads/main`, checks out
the dispatch SHA, and requires `expected_repo_sha` to equal that exact full SHA.
Never approve a run whose diff or SHA has not been reviewed.

The Fly credential is an app-scoped, 24-hour **SSH token**. “SSH-only” does not
mean “filesystem/DB read-only”: successful SSH grants shell-level power inside
the application Machine and exposes the process environment. The token can
therefore reach writable resources even though it cannot deploy. Safety relies
on the protected Environment, exact reviewed `main` SHA, fixed audited script,
restricted workflow inputs, and prompt token revocation—not on the token
preventing shell mutations.

Additional controls:

- `FLY_API_TOKEN` exists only as an Environment secret and is passed only to
  `flyctl`; it is never printed or copied into evidence;
- remote Python is encoded/streamed and executed in memory with `python3 -c`;
  it is not deployed and is never written to `/data`;
- only the fixed app from `FLY_APP_NAME` is addressed; no input accepts a
  machine, command, path, SQL string, URL or table name;
- SQLite is opened exactly as `file:/data/db.sqlite?mode=ro`, with URI mode,
  `PRAGMA query_only=ON`, and one consistent read transaction;
- limiter access uses the production `GOOGLE_AI_LIMITER_SUPABASE_URL` and
  `GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY` inside Fly and HTTP
  `GET ...?select=...` only. No RPC or mutating method is allowed;
- raw remote output is treated as untrusted/private. Only allowlisted fields
  survive redaction, and the redaction audit is a fail-closed artifact gate.

## GitHub configuration

Exact names:

| Kind | Name | Value / policy |
|---|---|---|
| Environment | `production-readonly` | required reviewer; protected `main` only |
| Environment secret | `FLY_API_TOKEN_SMART_UPDATE_AUDIT` | app SSH token, expiry `24h`; delete after terminal run |
| Repository variable | `FLY_APP_NAME` | `events-bot-new-wngqia` |

Set the non-secret repository variable once:

```bash
gh variable set FLY_APP_NAME \
  --repo onedayonemasterpiece/events-bot-new \
  --body events-bot-new-wngqia
```

Do not substitute an organization read token, personal token, deploy token or
`fly auth token` for the named Environment secret.

## Dispatch contract and time window

The workflow accepts only:

- `hours`: positive integer, default `24`, within the workflow's fixed bound;
- `end_utc`: optional strict RFC 3339 UTC instant (`YYYY-MM-DDTHH:MM:SSZ`). An
  empty value is resolved once to the runner's current UTC time;
- `expected_repo_sha`: required, lowercase/uppercase-insensitive exact 40-hex
  commit SHA.

The resolved window is half-open: **`[end_utc - hours, end_utc)`**. The resolved
start and end are frozen in `run.json` before any observer call. Every SQLite,
limiter and runtime-log timestamp predicate uses the same instants; no query
silently widens to “last N hours from now”. Boundary rows at `start_utc` are
included and rows at `end_utc` are excluded.

Naive SQLite application timestamps are interpreted as UTC by the existing
application contract. Unparseable or timezone-ambiguous timestamps are counted
and reported, not silently coerced. Limiter request rows use `created_at` and
physical attempt rows use `started_at`. Runtime log lines without a parseable
UTC timestamp cannot enter the exact-window excerpt or metric numerator.

## Observer sources and preflight

The run records each attempted command/query in the manifest using a stable
command/query ID and a redacted template (never token values, service keys or
private arguments).

1. Publicly request `https://${FLY_APP_NAME}.fly.dev/healthz`; record HTTP and
   allowlisted health fields only.
2. Record non-secret Fly Machine/version/image identity. Do not restart or
   change it.
3. Use `flyctl ssh console` on the existing Machine and check internal
   `/healthz` on loopback.
4. Read the in-container deployed repository SHA from its existing identity
   source. It must be a full exact SHA, not a release label or image tag.
5. Run `df`/`du` observation and inventory `events-bot.log*` names, sizes and
   mtimes. Do not remove or rotate anything.
6. Open SQLite as `file:/data/db.sqlite?mode=ro`; set `query_only`; begin one
   read transaction; inventory schema, calculate the schema hash, run `PRAGMA
   quick_check`, then run all SQLite queries in that same transaction.
7. Read only the exact-window lines from the active and rotated production
   runtime mirror. File availability is evaluated from actual env/path plus
   file inventory; the expected default is `/data/runtime_logs/events-bot.log*`.
8. From inside Fly, use the existing limiter URL/service key only for SELECT
   projections from its ledgers. Never print the URL, key, raw key registry IDs,
   aliases, notes or metadata.

Public and internal `/healthz` are separate evidence. A reachable unhealthy
response is a health finding; inability to observe the mandatory sources below
is an observer-access block.

## Required SQLite metrics

All counts include explicit denominators, the table/timestamp evidence used,
and `unavailable` where the deployed schema lacks an optional table. Schema is
discovered before querying; guessed table/column names are not retried.

### Imports and identity

- `event_source` imports in the UTC window grouped by `source_type`;
- distinct touched `event_id`;
- create versus merge-existing: compute the first-ever
  `event_source.imported_at` for each touched event. First source in the window
  is `create`; a first source before the window is `merge_existing`;
- `event_source_fact` rows linked to the window's source iterations, grouped by
  status. Never emit `fact` text;
- if `event_identity_decision_log` (or a schema-compatible identity decision
  table) exists, normalized create/merge/review/reject/conflict counts, stage,
  confidence bands and lifecycle/identity outcome. Do not emit reasons or JSON
  payloads that can contain source/private text;
- changed public field **names** per touched event, but only when supported by
  sanitized log/decision evidence. `updated_at` alone does not prove a field
  changed, and field values are never emitted.

An affirmative critical false merge is always `FAIL`. A semantically ambiguous
identity case must be `pending_review`; evidence of ambiguity followed by an
automatic merge is `FAIL`, never merely `WATCH`.

### Outbox and enrichments

- `joboutbox` counts by `task`, `status` and attempts band for touched events;
- stale `pending`, `running` and `error` rows relative to `next_run_at`,
  `updated_at`, and the deployed/configured per-task `JOB_MAX_RUNTIME` (falling
  back to the documented default only when configuration evidence is absent);
- `last_error` only after secret/ID/URL/text redaction and grouping by stable
  sanitized signature. Emit signature alias, exception class/stage when known,
  and count—not the raw message;
- pending/error media, source-fact, age assessment and collection/relation state
  for touched events, using deployed tables/tasks and separate `unavailable`
  flags rather than treating missing schema as zero.

### Static build and Kaggle

- `static_site_build_state`, relevant build history, and
  `joboutbox(static_site_build)`: current state, active job/claim, remote
  handoff presence, terminal receipt presence and candidate lag from the newest
  touched import/effect to the most recent accepted candidate/watermark;
- `kaggle_run_ledger` terminal versus nonterminal counts in the window and any
  currently nonterminal related run. Raw token hashes, error text, dataset refs,
  kernel refs and private progress are not emitted.

This audit observes existing build/Kaggle rows. It does not claim, adopt, poll
with a stateful callback, cancel, retry or launch them.

## Required runtime-log metrics

Only timestamped lines inside the frozen half-open window are read. The audit
scans all available `events-bot.log*` files and deduplicates rotated overlaps.
Sanitized metrics include:

- Smart Update starts and terminal outcomes;
- create, merge, no-op, reject and pending-review decisions;
- stable correlation/run aliases without accompanying private payload;
- exception classes and pipeline stages;
- retries and timeouts;
- anchor conflicts and hard vetoes;
- exact-packet replay signals and repeated prose-mutation signals;
- downstream enqueue and terminal state for touched event IDs.

`sanitized-runtime-excerpts.log` is a minimal allowlisted excerpt, not a log
dump. It may contain UTC time, severity, stage, outcome, event ID, redacted
correlation alias, exception class and redacted signature. URLs, source prose,
request/response bodies and arbitrary exception messages are suppressed.

## Google limiter metrics

The required observer tables are `google_ai_requests`,
`google_ai_request_attempts`, `google_ai_provider_cooldowns`, and the minimum
key-registry projection needed to map quota scopes. Only SELECT requests are
allowed. Key UUIDs, `key_alias`, `env_var_name`, account names, service key and
quota/project identifiers are replaced with stable run-salted aliases/hashes;
the salt is not emitted.

Report:

- logical requests and physical attempts by consumer, operation, model and
  status;
- finalized versus unfinalized requests/attempts;
- provider 429, timeout, 5xx and admission-denied counts;
- distinct quota-scope count and redacted scope aliases;
- logical requests with more than one physical attempt in one quota scope;
- unfinished reservations and active/expired cooldown counts at `end_utc`.

### Known limiter limitation (2026-08-04)

The deployed canonical ledger has **no first-class `operation` column**.
Therefore the operation dimension is populated only when a sanitized,
explicitly allowlisted operation value exists in metadata; otherwise it is
reported as `unknown`, with an `operation_observability` limitation. The audit
must not infer an operation from consumer/model or inspect prompts/completions
to fill the gap. This limitation is not permission to omit the dimension.

## Product samples

`samples.jsonl` contains at most 20 touched-event sample rows stratified across
create/merge and source types. Each row has only:

- `event_id`;
- normalized decision;
- changed public field names (never values);
- source type;
- stable redacted source-URL hash;
- lifecycle/identity status.

Exact warm-replay rows are a distinct `sample_kind=warm_replay` stratum and do
not consume the meaning of ordinary create/merge rows. A raw source URL or
source text is never included, even when hashing it internally.

### Known replay/correlation limitations (2026-08-04)

- Existing runtime logs do not guarantee one correlation ID on every Smart
  Update start, decision, outbox enqueue and terminal line. Cross-line joins are
  best-effort and report matched/unmatched denominators; event/time proximity is
  not promoted to certain causality.
- If the exact same packet is not observed at least twice inside the window,
  warm-replay status is `indeterminate`, not “no mutation” or “replay passed”.
  The warm-replay stratum is still present with an empty/indeterminate reason.
- These limitations may yield `WATCH`, but present logs still count as an
  available mandatory source. Missing/inaccessible runtime logs yield
  `BLOCKED_OBSERVER_ACCESS`.

## Evidence bundle and redaction gate

The GitHub artifact is named
`smart-update-prod-audit-<full-sha>-<run-id>` and contains exactly these nine
top-level files:

1. `manifest.json`
2. `run.json`
3. `metrics.json`
4. `findings.json`
5. `samples.jsonl`
6. `sanitized-runtime-excerpts.log`
7. `redaction-audit.json`
8. `qa-summary.json`
9. `smart-update-prod-audit.md`

`manifest.json` contains:

- exact tested repository SHA and exact in-container SHA;
- non-secret Fly Machine/version/image identity;
- resolved UTC start/end and window hours;
- schema hash and table inventory;
- `evidence_policy: restricted`;
- stable command/query IDs and redacted command/query templates;
- SHA-256 for each of the other eight files.

The manifest intentionally omits its own hash to avoid recursive self-hashing.
The workflow's artifact upload supplies the external SHA-256 for the complete
uploaded evidence artifact; record that digest with the run ID and full tested
SHA. Do not replace it with a hash of a later extracted directory.

`redaction-audit.json` contains allowlist results and counts by forbidden-data
category, never the matched literal. If forbidden material remains, rewrite it
to a suppression marker and re-scan. If a PII-free bundle cannot be proven, do
not upload the unsafe payload: fail the workflow with a sanitized minimal error
receipt. Artifact retention and access must reflect `restricted` evidence.

`smart-update-prod-audit.md` is generated only from the sanitized JSON/JSONL
artifacts. It must not quote raw DB rows or runtime lines.

## Classification

Exactly one final classification appears in `run.json`, `qa-summary.json` and
the Markdown report:

| Result | Meaning |
|---|---|
| `PASS` | all mandatory observer sources and both SHAs are present; integrity/health gates pass; no critical or watch finding remains |
| `WATCH` | all mandatory sources are present, but non-critical degradation, retry/staleness, evidence gap covered by an explicit limitation, or follow-up risk exists |
| `FAIL` | evidence proves an unhealthy/integrity condition, critical false merge, ambiguous auto-merge, failed `quick_check`, unsafe mutation/replay signal, or redaction-policy failure |
| `BLOCKED_OBSERVER_ACCESS` | the observer cannot establish the required production evidence; this is never converted to `PASS` |

The workflow **must** classify `BLOCKED_OBSERVER_ACCESS` and finish non-zero if
any of these is missing/inaccessible/unverifiable:

1. production runtime logs for the requested window;
2. production SQLite and its consistent read transaction/`quick_check`;
3. Google limiter ledger;
4. exact in-container deployed repository SHA.

A window with valid log files but zero matching Smart Update lines is not the
same as missing logs; report the zero denominator and other evidence. Conversely,
fallback `fly logs`, DB rows or an image tag cannot silently satisfy a missing
mandatory file-log source or exact deployed SHA.

## One-time post-merge execution

Run only after the workflow and audit script are merged to protected `main`.
Use current `flyctl` and `gh`; never enable shell tracing. The example loads the
operator's existing Fly credential without displaying it, streams the new SSH
token directly into the protected Environment secret (no token variable/file),
and installs cleanup before token creation.

```bash
set -euo pipefail
set +x

REPO=onedayonemasterpiece/events-bot-new
APP=events-bot-new-wngqia
ENVIRONMENT=production-readonly
SECRET=FLY_API_TOKEN_SMART_UPDATE_AUDIT
WORKFLOW=smart-update-prod-audit.yml
TOKEN_NAME="smart-update-prod-audit-$(date -u +%Y%m%dT%H%M%SZ)-$$"
CLEANED=0

export PATH="$HOME/.fly/bin:$PATH"
set -a
source /home/dev/.config/fly/release.env
set +a

cleanup() {
  if [ "$CLEANED" -eq 1 ]; then return; fi
  CLEANED=1
  set +e
  gh secret delete "$SECRET" --env "$ENVIRONMENT" --repo "$REPO"
  mapfile -t token_ids < <(
    flyctl tokens list --app "$APP" 2>/dev/null |
      awk -v name="$TOKEN_NAME" '$2 == name { print $1 }'
  )
  if [ "${#token_ids[@]}" -gt 0 ]; then
    flyctl tokens revoke "${token_ids[@]}"
  fi
  set -e
}
trap cleanup EXIT INT TERM

# stdout of flyctl is only the token and goes directly to GitHub encryption.
flyctl tokens create ssh \
  --app "$APP" --expiry 24h --name "$TOKEN_NAME" |
  gh secret set "$SECRET" --env "$ENVIRONMENT" --repo "$REPO"

TESTED_SHA="$(gh api "repos/$REPO/commits/main" --jq .sha)"
[[ "$TESTED_SHA" =~ ^[0-9a-fA-F]{40}$ ]] || {
  echo "Refusing non-SHA main identity" >&2
  exit 1
}
END_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

RUN_URL="$(gh workflow run "$WORKFLOW" --repo "$REPO" --ref main \
  --raw-field hours=24 \
  --raw-field end_utc="$END_UTC" \
  --raw-field expected_repo_sha="$TESTED_SHA")"
RUN_ID="${RUN_URL##*/}"
case "$RUN_ID" in (*[!0-9]*|'') echo "Missing run ID" >&2; exit 1;; esac

# Environment approval happens in GitHub; confirm SHA before approving it.
gh run view "$RUN_ID" --repo "$REPO" \
  --json databaseId,headBranch,headSha,status,url

set +e
gh run watch "$RUN_ID" --repo "$REPO" --compact --exit-status
WATCH_RC=$?
set -e

RUN_STATE="$(gh run view "$RUN_ID" --repo "$REPO" \
  --json databaseId,headSha,status,conclusion,url)"
printf '%s\n' "$RUN_STATE"
[ "$(jq -r .headSha <<<"$RUN_STATE")" = "$TESTED_SHA" ] || exit 1
[ "$(jq -r .status <<<"$RUN_STATE")" = completed ] || exit 1

ARTIFACT_NAME="smart-update-prod-audit-${TESTED_SHA}-${RUN_ID}"
ARTIFACT_META="$(gh api "repos/$REPO/actions/runs/$RUN_ID/artifacts")"
ARTIFACT_DIGEST="$(jq -r --arg name "$ARTIFACT_NAME" \
  '.artifacts[] | select(.name == $name) | .digest' <<<"$ARTIFACT_META")"
case "$ARTIFACT_DIGEST" in (sha256:*) ;; (*) echo "Missing artifact digest" >&2; exit 1;; esac

EVIDENCE_DIR="artifacts/codex/smart-update-prod-audit-$RUN_ID"
mkdir -p "$EVIDENCE_DIR"
gh run download "$RUN_ID" --repo "$REPO" --name "$ARTIFACT_NAME" \
  --dir "$EVIDENCE_DIR"
CLASSIFICATION="$(jq -r '.classification // .status // empty' \
  "$EVIDENCE_DIR/qa-summary.json")"
case "$CLASSIFICATION" in
  PASS|WATCH|FAIL|BLOCKED_OBSERVER_ACCESS) ;;
  *) echo "Missing final audit classification" >&2; exit 1 ;;
esac

printf 'run_id=%s\ntested_sha=%s\nclassification=%s\nartifact_digest=%s\n' \
  "$RUN_ID" "$TESTED_SHA" "$CLASSIFICATION" "$ARTIFACT_DIGEST"

# Terminal cleanup is mandatory even for WATCH/FAIL/BLOCKED.
cleanup
trap - EXIT INT TERM

if gh secret list --env "$ENVIRONMENT" --repo "$REPO" \
     --json name --jq '.[].name' | grep -Fxq "$SECRET"; then
  echo "Environment secret deletion was not verified" >&2
  exit 1
fi
if flyctl tokens list --app "$APP" 2>/dev/null |
     awk -v name="$TOKEN_NAME" '$2 == name && NF < 11 { found=1 } END { exit !found }'; then
  echo "Fly token still appears active" >&2
  exit 1
fi
exit "$WATCH_RC"
```

The exact-name token lookup is piped through `awk`, so the token inventory's
creator metadata is not printed. The token ID is non-secret revocation metadata;
the token value is never stored. The active-row verification relies on flyctl's
current five-column table (`ID`, `Name`, `Created By`, `Expires At`, `Revoked
At`) and the token name intentionally contains no spaces. If flyctl changes that
format, inspect only the exact-name row and confirm a non-empty `Revoked At`
without copying creator metadata into evidence. If the terminal shell is
interrupted, the trap deletes the Environment secret and revokes every
exact-name match. If cleanup reports an error, do not treat expiry as
sufficient: revoke the named token in Fly and delete the Environment secret
manually, then verify both actions before closing the audit.

Download restricted evidence only when necessary, under ignored artifacts:

```bash
mkdir -p "artifacts/codex/smart-update-prod-audit-$RUN_ID"
gh run download "$RUN_ID" --repo onedayonemasterpiece/events-bot-new \
  --name "$ARTIFACT_NAME" \
  --dir "artifacts/codex/smart-update-prod-audit-$RUN_ID"
```

Never commit or republish the evidence. The operator's final handoff must state
only the run ID, full tested SHA, final classification/status, external
`sha256:` artifact digest, and confirmation that the SSH token was revoked and
the Environment secret deleted.

## Closure checklist

- [ ] Run was dispatched from `main` for the exact reviewed 40-hex SHA.
- [ ] Protected Environment review occurred before secret access.
- [ ] Public/internal health, Machine identity and in-container SHA were checked.
- [ ] SQLite URI was `file:/data/db.sqlite?mode=ro`; one read transaction and
      `PRAGMA quick_check` completed; no DB copy was made.
- [ ] Exact-window runtime logs and limiter SELECT ledgers were available.
- [ ] No restart, deploy, mutation, new job or publication occurred.
- [ ] All nine evidence files passed redaction; manifest hashes eight payload
      files and the upload supplies the external artifact digest.
- [ ] Final classification follows the mandatory blocked-source rule.
- [ ] Run ID, full tested SHA, status and artifact SHA-256 were recorded.
- [ ] Fly token revoked and `FLY_API_TOKEN_SMART_UPDATE_AUDIT` deleted after the
      terminal run, including failure/cancellation paths.
