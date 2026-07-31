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
- 2026-07-31 17:06 UTC — the canonical limiter schema was applied to the
  administrable personalization Supabase project. Capability, five-key
  registry, 17-model registry and a rollback-only Antigravity reservation were
  verified without a Google request.
- 2026-07-31 17:08 UTC — Supabase advisors found seven mutable function search
  paths; an additive migration pinned all limiter RPCs to `public, pg_temp` and
  the repeated advisor check returned zero `google_ai_*` findings.
- 2026-07-31 17:xx UTC — all inventoried runtime, Edge, benchmark and probe
  paths were migrated or retired. Static audit reached
  `allowlisted_debt=0`, `unapproved=0` without provider traffic.
- 2026-07-31 17:41 UTC — the old ledger's current-day reservations were copied
  into the canonical ledger before cutover. The shared scope now carries
  Antigravity `4 RPD`, Flash-Lite `900 RPD`, Gemma 4 31B `519 RPD` and Gemini
  3 Flash Preview `1 RPD`; no provider request was made by this import.
- 2026-07-31 19:48 UTC — operator-provided `GOOGLE_API_KEY6` was registered
  without its secret as a separately accounted scope
  `google:key6-operator-isolated-20260731`. A rollback-only atomic reservation
  proved the required limiter contract and was removed without a provider call.
- 2026-07-31 20:14 UTC — normal key rotation was moved from Smart Update
  configuration ownership to the shared gateway. Ordinary clients inherit
  `GOOGLE_AI_NORMAL_KEY_ENVS`; explicit default/scoped lanes remain overrides.

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
4. The old/core limiter project was not administrable with the available
   management credential, so its migration state could not be made the
   production safety boundary. A separate administrable canonical ledger was
   required.
5. The ledger is keyed by API-key row while Google quota dashboards are scoped
   to Cloud project/model. Without an explicit key-to-project quota-scope map,
   multiple keys from one project can be admitted independently and then summed
   by Google.
6. The stored Gemma 4 limits were materially wrong: TPM was represented as
   `2147483647` and RPD as `1500`, while the supplied dashboard showed
   `15000 TPM / 14000 RPD`. The limiter therefore could not stop the observed
   Gemma TPM overrun even when the RPC path itself was used.

## Contributing Factors

- June's incident fix changed unlimited fallback into a local 15-RPM limiter,
  which protected one process but did not close the cross-process failure mode.
- `event_vector_sync --max-provider-calls` counted successful documents, not
  failed HTTP attempts hidden inside its retry loop.
- Before remediation, `event-search` rotated across up to five raw keys on
  retry and had user-search quota but no provider accounting.
- Direct benchmarks, AfishaThumb modules, Universal Festival Parser and local
  smokes were callable by agents outside the repository ledger.
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
- Apply and verify the canonical
  `google_ai_project_model_atomic_v1` bootstrap before concurrent traffic.
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
- The dedicated schema and conservative registry are live; all five keys remain
  in one `google:unmapped-shared` scope until a project mapping is proved.
- The sixth operator-confirmed fresh key is registered in its own redacted
  scope and its Fly secret is staged for the gateway-owned normal pool. Smart
  Update is only a consumer of that pool and contains no key rotation list.
- Current-day counters were imported before cutover, so the new ledger does not
  incorrectly treat 2026-07-31 as an unused day. In particular, the shared
  Flash-Lite scope is already above its conservative daily cap and must deny
  further Flash-Lite admission today.
- The unrelated `PosterCandidate.url` production regression discovered during
  this work was fixed and deployed separately under
  `INC-2026-07-31-poster-candidate-url`; it was not caused by limiter rollout.

## Corrective Actions

- Made process-local limiting explicit dev-only opt-in.
- Dedicated limiter configuration is an atomic URL/service-key pair and always
  wins over explicitly transitional legacy factories.
- Successful reserve requires exact contract and non-empty project scope before
  a key is read.
- Added project-level quota scopes and atomic SQL admission.
- Routed Edge search, embeddings, Universal Festival Parser, AfishaThumb and
  benchmark consumers through the gateway; permanently retired the raw
  GemmaKey2 probe.
- Added an offline repository audit and agent policy that reject any newly
  introduced direct endpoint/SDK path.
- Centralized ordinary key-pool ownership in `GoogleAIClient` through
  `GOOGLE_AI_NORMAL_KEY_ENVS`; reservation scans and provider-429 rotation are
  fail-fast and never sleep until a quota window resets.

## Follow-up Actions

- [x] Apply and verify the canonical project-scoped limiter schema.
- [x] Migrate Supabase `event-search` embedding and LLM calls to the shared gate.
- [x] Add a conservative redacted key registry without secret values.
- [x] Migrate/disable Universal Festival Parser, AfishaThumb, benchmarks and
  smokes that called Google directly.
- [ ] Replace `google:unmapped-shared` with verified per-project scopes only
  after key → Google Cloud project evidence is available.
- [ ] Deploy the runtime/Edge cutover from an `origin/main`-reachable SHA and
  distribute the dedicated limiter pair to every encrypted Kaggle payload.
- [ ] Add static CI policy plus runtime alert for any successful provider call
  lacking `api_key_id`, `minute_bucket` or `used_after`.
- [ ] Reconcile or sweep stale/overreserved counters without refunding sent RPD.

## Release And Closure Evidence

- deployed SHA: pending application/Edge cutover
- deploy path: canonical Supabase schema applied; Fly/Edge release pending
- regression checks: targeted Python/Edge suites and offline static audit
  (`122` targeted Python tests plus the Region Talk secret-contract test,
  `13` Edge tests, TypeScript check; audit of `808` files with
  `allowlisted_debt=0`, `unapproved=0`)
- post-deploy verification: not performed; external requests explicitly stopped
- key6 pre-deploy verification: live atomic rollback probe completed in
  `46.39 ms`, required contract/scope present, persisted rows after rollback
  `0`; targeted gateway/Smart Update/festival tests `119 + 73 passed`, limiter
  and bypass-audit tests `24 passed`, static audit `808` files,
  `allowlisted_debt=0`, `unapproved=0`

## Prevention

This record supersedes the single-process assumption in the June incident.
Local limiting may be useful for isolated tests, but is never a valid safety
boundary for parallel agents or remote runtimes.
