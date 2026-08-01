# Festival Antigravity runtime integration report — 2026-07-31

Integration branch: `integration/festival-antigravity-runtime-20260731`

## Delivered

- R01 **Implemented, rollout blocked** — `AntigravityInteractionsClient` uses an explicit registered
  key-env pool, one UUID/lease per creating POST, shared Supabase reserve/sent/
  finalize accounting and no local/default/overflow fallback. Migration 008
  serializes check+increment per key/model. Safe cap remains
  `54 RPM / 96000 TPM / 90 RPD` per key over the provider's `60/100000/100`.
- R02 **Done** — the queue now calls explicit-date currentness before spending,
  groups complete editions before applying the group cap, includes queue IDs,
  snapshot/prompt/normalizer hashes in fingerprints, and performs public DNS
  preflight.
- R03 **Done offline; live polling blocked** — independent A and B, at most
  one evidence-conflict C with search/url tools disabled, durable interaction
  handles, a resume consumer, mandatory checkpoint-chain and inventory
  conservation, and distinct provider/semantic operational states. C can select
  a whole validated lane only when every conflict resolves consistently;
  otherwise it remains explicit review evidence.
- R04 **Done for approval-gated collection, not auto-apply** — exactly seven
  primary topologies; exact hash/quote/reference validation; every non-empty
  festival fact and classification decision is claim-bound; Event candidates
  require same-programme-subject identity/logistics claims plus the seven-part gate. Agent apply
  authority is rejected recursively. Agent-fetched source bodies are still
  operator-reviewed rather than treated as host-certified auto-apply evidence.
- R05 **Done for preproduction scope** — main/manual and scheduler construct the
  strict service when enabled; additive run/lane/item/source tables,
  manual collect and review CLIs, approve/reject state, and opt-in queue routing
  that stops at `review`. No Festival/Event/Telegraph/static-page apply path was
  added. Kaggle+Gemma was not changed.
- R06 **Blocked by unresolved poll permission after bounded debug** — create is
  admitted and returns handles/environments, while subsequent GET polling is
  rejected with 403; the evidence does not establish quota exhaustion or a
  provider-wide Antigravity eligibility failure.
- R07 **Done locally** — docs/changelog, negative safety tests and independent
  review findings are closed; rollout evidence remains subject to R01/R06
  external blockers below.

## Verification

The combined regression selection reached a clean assertion summary:

```text
135 targeted tests passed (final regression summary)
```

It covered existing Google client/limits, Interactions transport, festival
contracts/selection/source/evidence/schema, coordinator/service/queue seam,
persisted-handle resume without a new POST, legacy festival context/parser and
DB regressions. `py_compile` and
`git diff --check` also passed.

Runner caveat: after printing the all-passed summary, the combined process did
not terminate naturally in Python thread shutdown. Isolated changed-surface
sets (`27 passed`), Google transport/limits (`50 passed`) and provider-free
festival core (`40 passed`) all returned exit 0. Isolated legacy
`tests/test_db.py` cases also print `passed` but reproduce the same post-summary
thread-shutdown timeout; this is recorded as harness teardown evidence, not as
a failed assertion or an Antigravity runtime failure.

## Live debug and quota evidence

The first «Балтийская Ухана» A+B canary used `35000` token budgets. Create
succeeded for keys 1 and 2 and returned interaction/environment handles, but
polling returned provider HTTP 403 `permission_denied`. After targeted official
documentation/forum research, one minimal `1000`-token probe was sent on each
remaining registered key. Keys 3–5 failed identically.

Shared ledger after cleanup:

```text
GOOGLE_API_KEY   RPD 1  failed_provider 403
GOOGLE_API_KEY2  RPD 1  failed_provider 403
GOOGLE_API_KEY3  RPD 1  failed_provider 403
GOOGLE_API_KEY4  RPD 1  failed_provider 403
GOOGLE_API_KEY5  RPD 1  failed_provider 403
```

No key value was logged. Non-retryable poll rejection now finalizes the
original reservation; no request is left in progress. This is not quota
exhaustion: each key consumed 1 of the safe 90 RPD and the provider message was
explicitly permission-related.

### Official SDK canary 2026-08-01

After rechecking the current Google tutorial, one minimal foreground canary was
run from the production `iad` machine with the installed official
`google-genai==2.16.0` client, without the repository REST adapter:

```python
client.interactions.create(
    agent="antigravity-preview-05-2026",
    input="Return exactly: OK",
    environment="remote",
)
```

The call used `GOOGLE_API_KEY6` only after a shared-ledger reservation and
returned `PermissionDeniedError: The caller does not have permission` in
5.9 seconds. Request UID:
`145cabe1-b29c-40c3-8cdd-22afdf72b3ab`. The ledger finalized the single
physical attempt as `failed_provider`, `reserved_rpd=1`, `reserved_tpm=1000`;
no retry or second background canary was sent. Evidence is retained on the
production volume at
`/data/festival-web-research/official-sdk-canary-20260801.json`.

This removes the custom REST body, polling implementation, extra request
headers and festival prompt as explanations for the current 403. It does not
erase the 2026-07-29 evidence that earlier Antigravity interactions executed,
used tokens and wrote sandbox artifacts; the current failure is therefore an
access/backend regression or changed provider entitlement, not a demonstrated
client-contract error.

Migration 007 could not be applied with the available Management API token:
the limiter project returned HTTP 403 insufficient database-write privileges.
Strict production mode therefore remains fail-closed. The bounded canary used
the explicit `--allow-legacy-accounting` compatibility bridge: reserve/RPD/TPM
and terminal failure were recorded in the existing shared ledger, while the
new operational DB retained provider/semantic separation. Queue/scheduler code
never enables this bridge.

## Stop condition / next external action

Do not spend more Antigravity requests. The broader parallel Google limiter
incident is tracked as
`INC-2026-07-31-google-ai-parallel-limiter-bypass`; no new provider diagnostic
is authorized in the current incident. After atomic limiter rollout and a
separate explicit operator authorization:

1. apply migration 007 using a credential with Supabase database-write access;
2. apply/verify migration 008 and close all direct production limiter bypasses;
3. diagnose the create-versus-poll permission/resource contract with one
   bounded request on one mapped Cloud project;
4. only after it completes, rerun «Балтийская Ухана» A+B and review its evidence;
5. use C only if host validation finds a real A/B conflict.
