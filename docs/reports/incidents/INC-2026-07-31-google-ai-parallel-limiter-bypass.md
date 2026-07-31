# INC-2026-07-31 Google AI Parallel Limiter Bypass

Status: open
Severity: sev2
Service: events-bot Google AI shared limiter / parallel agent, Fly, Kaggle and Edge consumers
Opened: 2026-07-31
Closed: —
Owners: events-bot maintainers
Related incidents: `INC-2026-06-28-google-ai-gemma4-rpm-overrun.md`
Related docs: `docs/features/llm-gateway/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Google AI Studio screenshots showed provider peaks above configured Flash-Lite
RPM/RPD and Gemma TPM limits while several Codex agents and scheduled jobs were
working in parallel. The investigation confirmed that the repository does not
yet have one mandatory cross-process admission gate: the shared client failed
open to a process-local/direct-key mode by default, and several deployed paths
call Google directly. The exact request(s) that produced every screenshot peak
cannot be attributed from local evidence alone.

This incident is separate from the five bounded Antigravity probes. Those five
creating POSTs each had a shared reservation and ended with provider poll 403;
no Antigravity limiter bypass was found.

## User / Business Impact

- Parallel tasks can exceed provider project/model RPM or TPM even when each
  process believes it is inside its own local limit.
- Direct consumers are absent from `google_ai_requests`, so the internal ledger
  cannot reliably explain or prevent provider quota exhaustion.
- A quota storm in event search, vector sync, an agent benchmark or a legacy
  Kaggle parser can starve unrelated festival and event pipelines sharing the
  same Google project.
- Internal reservations also overstate actual provider usage when a request is
  reserved but rejected before usage is returned.

## Detection

- Operator supplied Google AI Studio project screenshots for 1-day/28-day
  windows. Examples included Flash-Lite `19/15`, `17/15` and `15/15` peak RPM,
  `508/500` peak RPD, Gemma TPM above the displayed cap, and embedding RPD 573.
- Antigravity rows were low: normally `0` or `1` RPD per displayed project.
- Local source/session/process audit reconstructed provider-capable paths
  without issuing any new Google request after the operator stop instruction.

## Timeline

- 2026-07-31 13:45:23 UTC — a Codex session made one proven direct
  `gemini-3.1-pro-preview` POST on `GOOGLE_API_KEY4`; it bypassed the shared
  client and returned provider 429. It proves agent-level bypass capability but
  is not the Flash-Lite/Gemma screenshot peak.
- 2026-07-31 15:06:26–15:08:52 UTC — five Antigravity creating POSTs, one per
  registered key env, were reserved in the shared ledger and finalized as
  provider 403 with no returned usage.
- 2026-07-31 15:07:47–15:11:20 UTC — another agent made five successful
  Flash-Lite video probes on KEY2, totalling 12,450 actual tokens. All five used
  shared reserve/finalize accounting; this was parallel activity, not a proven
  bypass.
- 2026-07-31 15:44:31 UTC — a saved ledger snapshot showed Flash-Lite daily
  reservations KEY1=450, KEY2=71, KEY3=189, KEY4=450, KEY5=74 against internal
  safe cap 450. These are reservations, not a provider-call truth source.
- 2026-07-31 16:09 UTC — local process snapshot showed no active Gemini/Gemma
  provider client. Region Talk orchestration was active, but recorded runs had
  not reached their LLM stages.
- 2026-07-31 16:xx UTC — code audit found the fail-open shared-client defaults,
  direct Edge search/vector sync paths and multiple legacy agent/Kaggle paths.
  Containment changes were implemented locally without live provider calls.

## Root Cause

1. `GoogleAIClient` enabled reserve and process-local fallback by default.
   Supabase absence, missing key metadata, missing RPC or a reserve RPC error
   could therefore become a provider call controlled only by one process.
2. The process-local limiter is not shared across Codex agents, Fly machines or
   Kaggle kernels. Parallel processes can independently spend the same key.
3. Several consumers bypass `GoogleAIClient` entirely. The highest-risk active
   paths are Supabase `event-search` (direct multi-key rotation) and production
   event-vector sync (direct embeddings with up to six attempts per successful
   item).
4. `migrations/008_google_ai_atomic_reserve.sql`, which serializes shared
   check+increment, has not been applied to the live limiter database because
   the available management credential lacks database-write permission.
5. The ledger is keyed by API-key row while Google quota dashboards are scoped
   to Cloud project/model. Without an explicit key-to-project quota-scope map,
   multiple keys from one project can be admitted independently and then summed
   by Google.

## Contributing Factors

- June's incident fix changed unlimited fallback into a local 15-RPM limiter,
  which protected one process but did not close the cross-process failure mode.
- `event_vector_sync --max-provider-calls` counted successful documents, not
  failed HTTP attempts hidden inside its retry loop.
- `event-search` rotates across up to five raw keys on retry and has user-search
  quota, but no `google_ai_reserve/finalize` provider accounting.
- Direct benchmarks, AfishaThumb modules, Universal Festival Parser and local
  smokes remain callable by agents outside the repository ledger.
- The dashboard screenshots do not provide a safe deterministic mapping from
  local env aliases to Google Cloud project IDs.

## Evidence Boundaries

- **Proven:** mandatory-gateway violations exist; one direct agent Pro POST was
  observed; five Antigravity and five video-probe calls were reconstructed.
- **Strong architectural explanation, not exact attribution:** event vector sync
  can explain embedding RPD near 573; Edge search and local fallbacks can create
  cross-project Flash-Lite/Gemma bursts.
- **Not proven:** which individual task produced each Flash-Lite/Gemma peak, or
  that Antigravity made an unreserved request.

## Automation Contract

### Treat as regression guard when

- Changing any Google/Gemini/Gemma/Antigravity provider client or retry loop.
- Adding an agent, Kaggle, Fly, Edge Function or local probe that can read a
  `GOOGLE_API_KEY*` secret.
- Changing shared limiter SQL, key allocation or Cloud project mapping.

### Affected surfaces

- `google_ai/client.py`, `migrations/008_google_ai_atomic_reserve.sql`
- `scripts/sync_event_search_vectors_to_supabase.py`
- `supabase/functions/event-search/index.ts`
- legacy festival parser, AfishaThumb, benchmarks, probes and agent CLIs
- `fly.toml` and all remote runtime envs

### Mandatory checks before closure or deploy

- Shared client must fail closed by default when Supabase/RPC/key metadata is
  unavailable; no direct env-key success result is permitted.
- Apply and verify migration 008 before concurrent shared-limiter traffic.
- Route all deployed direct Google paths, including Edge search, through shared
  reserve/mark/finalize or disable them.
- Inventory API key → Google Cloud project quota scope and enforce limits at the
  provider scope rather than assuming every key has an independent quota.
- Static audit must fail CI for new unapproved direct Google endpoints/SDK calls.
- Post-deploy logs must show non-null key/reservation metadata for every Google
  provider call; provider dashboard and internal sent/finalized counts must be
  reconciled over one bounded observation window.
- Release SHA must be reachable from `origin/main` before closure.

### Required evidence

- targeted tests and static bypass-audit result;
- SQL migration application/capability evidence;
- redacted key-to-project scope inventory;
- post-deploy shared-ledger/provider reconciliation;
- deployed SHA and `origin/main` ancestry.

## Immediate Mitigation

- No further Google/provider diagnostics were issued after the operator asked
  to stop all requests.
- Shared-client fallback defaults were changed locally to fail closed; Fly
  config explicitly disables the three fallback modes.
- Production vector sync was changed locally to use the shared embedding gateway
  instead of direct REST/retries, and its Fly schedule was disabled pending the
  atomic rollout gate.
- No deployment was attempted: migration 008 and the direct Edge Function gap
  remain release blockers.

## Corrective Actions

- Make process-local limiting explicit dev-only opt-in.
- Never return `ok=true` with a raw env key when the shared limiter is absent.
- Account each embedding provider attempt through shared reserve/finalize.
- Add project-level quota scopes to key metadata and atomic SQL admission.
- Migrate or disable every deployed direct provider path.

## Follow-up Actions

- [ ] Apply migration 008 with an authorized database-write credential.
- [ ] Migrate Supabase `event-search` embedding and LLM calls to the shared gate.
- [ ] Add and populate a redacted API-key → Cloud-project quota-scope registry.
- [ ] Migrate/disable Universal Festival Parser, AfishaThumb, benchmarks and
  smokes that still call Google directly.
- [ ] Add static CI policy plus runtime alert for any successful provider call
  lacking `api_key_id`, `minute_bucket` or `used_after`.
- [ ] Reconcile or sweep stale/overreserved counters without refunding sent RPD.

## Release And Closure Evidence

- deployed SHA: not deployed
- deploy path: blocked by unapplied migration 008 and direct Edge search path
- regression checks: local targeted suite `48 passed`
- post-deploy verification: not performed; external requests explicitly stopped

## Prevention

This record supersedes the single-process assumption in the June incident.
Local limiting may be useful for isolated tests, but is never a valid safety
boundary for parallel agents or remote runtimes.
