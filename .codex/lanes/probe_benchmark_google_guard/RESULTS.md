# R06 — Probe / Benchmark Google Guard Results

## Lane contract

- **Lane ID:** `probe_benchmark_google_guard`
- **Requirement IDs:** `R06`
- **Status:** Done
- **Base SHA:** `86a0a8382f0dd9cbb644cd02540bf503e012332c`
- **Implementation head SHA:** `07348800889f0a9dfe235f483b610402c0bf3038`
- **Branch:** `lane/probe-benchmark-google-guard`
- **Worktree:** `/home/dev/.codex/worktrees/events-bot-new/probe-benchmark-google-guard`
- **Final lane tip:** the immediate child of the implementation head containing this evidence report; final SHA is reported to the integrator.
- **External access:** no network, Google/provider, Supabase, secret, deploy, or push operations were performed.

## Delivered

- Removed the raw Gemini embedding REST transport from `event_identity.py`.
  The compatibility helper now requires an injected `GoogleAIClient`, calls its
  limiter-accounted `embed_content_async()`, and rejects raw API keys.
- Replaced both legacy `google.generativeai` benchmark helpers with one lazy,
  fail-closed `GoogleAIClient` configured against `SUPABASE_URL` plus
  `SUPABASE_SERVICE_KEY`. Process-local/reserve-error fallbacks are explicitly
  disabled. Provider timeout is restored after every call, and JSON repair is a
  separate gateway request, so every physical SDK retry/send is represented by
  the gateway's reserve/mark/finalize attempt accounting.
- Added `--google-key-env` validation to the benchmark so it selects an explicit
  registered `GOOGLE_API_KEY*` lane rather than silently widening across env
  keys.
- Removed the raw Gemini embedding REST call from the authorized personalization
  search smoke. Its target Auth/RPC traffic remains on the personalization
  Supabase contour, while Google quota admission deliberately uses the legacy
  shared-limiter Supabase contour.
- Kept the historical Kaggle Gemma Key2 direct probe only as explicit migration
  debt. Routine notebook execution now fails before transport. A manual send
  requires both exact config values:
  - `dangerously_allow_unaccounted_google_provider_call=true`
  - `dangerously_max_unaccounted_provider_send_attempts=1`
  The attempt counter increments before `requests.post`, so timeout/connection
  uncertainty still consumes the only permitted send.
- Added provider-free tests for gateway routing, fail-closed behavior, notebook
  guard ordering, and manual attempt-budget enforcement.

## Incident regression contract

- **Incident ID:** `INC-2026-07-31-google-ai-parallel-limiter-bypass`
- **Current status:** remains open; this lane closes only its R06 surfaces.
- **Affected surfaces in this lane:** identity embedding debug helper, lollipop
  benchmark, authorized-search smoke, Gemma Key2 Kaggle probe.
- **Target behavior:** routine Google sends either use shared
  reserve/mark/finalize accounting or fail closed before transport.
- **Regression checks completed:** gateway/fail-closed unit suite and offline
  static provider-path audit (details below).
- **Release evidence:** local committed implementation only; no deployment or
  provider observation was allowed.
- **Follow-ups outside this lane:** migration 008 application/capability proof,
  key-to-project quota-scope inventory, other repository bypass migrations,
  post-deploy ledger/provider reconciliation, canonical docs and changelog.

## Commands and evidence

Shared pre-existing test interpreter:
`/home/dev/projects/events-bot-new-wt-tg-stale-lease/.venv/bin/python`.

```text
/home/dev/projects/events-bot-new-wt-tg-stale-lease/.venv/bin/python -m pytest -q \
  tests/test_google_ai_client.py \
  tests/test_probe_benchmark_google_guard.py \
  tests/test_event_identity.py \
  tests/test_lollipop_legacy.py
```

Result: `83 passed in 1.68s`.

```text
python3 -m py_compile \
  event_identity.py \
  scripts/inspect/benchmark_lollipop_g4.py \
  scripts/smoke_authorized_event_search_rpc.py \
  tests/test_probe_benchmark_google_guard.py
python3 -m json.tool kaggle/GemmaKey2Probe/gemma_key2_probe.ipynb
```

Result: passed.

Offline static audit (audit implementation supplied by the parallel audit lane):

```text
python3 /home/dev/projects/events-bot-new/.codex/worktrees/direct_provider_static_audit/\
scripts/inspect/audit_google_ai_provider_paths.py --root .
```

Result: `PASS`; `scanned_files=797`, `unapproved=0`,
`unreadable_files=0`. The three routine Python surfaces contributed no raw
Google findings. The one Gemma Key2 endpoint remains visible as one narrowly
allowlisted debt finding; its runtime path is now guarded and one-attempt-only.
The audit also reported unrelated remaining repository debt outside this lane.

Additional checks:

```text
git diff --check
rg -n -i 'google\.generativeai|GenerativeModel|generativelanguage\.googleapis\.com|x-goog-api-key|\.models\.generate_content|requests\.post' \
  event_identity.py scripts/inspect/benchmark_lollipop_g4.py \
  scripts/smoke_authorized_event_search_rpc.py kaggle/GemmaKey2Probe
```

Result: no raw Google SDK/REST markers in routine Python surfaces. Remaining
matches are the explicitly guarded Gemma Key2 notebook endpoint/send and the
benchmark's unrelated OpenAI 4o HTTP path.

## Changed files

- `event_identity.py`
- `scripts/inspect/benchmark_lollipop_g4.py`
- `scripts/smoke_authorized_event_search_rpc.py`
- `kaggle/GemmaKey2Probe/gemma_key2_probe.ipynb`
- `tests/test_probe_benchmark_google_guard.py`
- `.codex/lanes/probe_benchmark_google_guard/RESULTS.md`

## Risks and remaining debt

- Gemma Key2 remains a raw provider probe by design, but it is no longer
  routine-callable. Its manual override is intentionally unaccounted and should
  ultimately be retired or supplied with the shared limiter package/credentials.
- The whole incident is not closed by this lane. Other allowlisted direct
  consumers, atomic migration verification, quota-scope mapping, and live
  reconciliation remain integration/release blockers.
- No live validation was performed because the lane explicitly prohibited
  network/provider/Supabase calls. The integration/release owner must collect
  post-deploy shared-ledger evidence before incident closure.
- Canonical documentation and `CHANGELOG.md` were outside writable scope and
  remain integration-owner work.
