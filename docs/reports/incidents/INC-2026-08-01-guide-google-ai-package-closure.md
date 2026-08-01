# INC-2026-08-01 Guide Google AI Package Closure

Status: open
Severity: sev2
Service: scheduled Guide Excursions monitoring / Kaggle LLM extraction
Opened: 2026-08-01
Closed: —
Owners: bot operations / guide excursions
Related incidents: `INC-2026-07-31-google-ai-parallel-limiter-bypass`, `INC-2026-04-21-guide-gemma4-partial-monitoring`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The scheduled Guide Excursions Kaggle notebook received an incomplete embedded
`google_ai` package after the shared limiter cutover. Every prefiltered post
failed before LLM extraction with `ModuleNotFoundError`, so three consecutive
scheduled scans produced no occurrence imports or updates.

## User / Business Impact

- 46 unique candidate posts from 12 Telegram/VK sources did not reach guide LLM
  extraction across the affected runs.
- The failed 2026-07-31 full scan and both 2026-08-01 light scans produced zero
  occurrence creates/updates, leaving fresh excursion announcements and status
  changes undiscovered.
- The serving bot and existing digest inventory remained available. Digest
  issue `#194` was published after the first failed full run from occurrences
  already imported by the successful 07:05 UTC scan; this reduced visible
  outage but did not recover later source updates.

## Detection

- Production `ops_run #5047` finished `partial` with `llm_ok=0` and
  `llm_error=34`.
- The enabled Fly file mirror and persisted Kaggle result bundle exposed the
  exact exception; the compact `ops_run` report exposed only
  `llm_error:ModuleNotFoundError`.
- `/healthz` stayed green because the failure was isolated to post-level Kaggle
  LLM processing, so scheduler health alone did not detect product data loss.

## Timeline

- 2026-07-31 17:08 UTC: `a21e3cf5` added
  `google_ai/limiter_supabase.py` and imports from the shared client and Guide
  runner, while the generated Guide notebook kept its old four-file embed list.
- 2026-07-31 18:10–18:16 UTC: scheduled full `ops_run #4960`, run
  `f5f8153ec281`, finished `partial`: 37 prefiltered posts, `llm_ok=0`,
  `llm_error=37`, zero occurrence changes.
- 2026-08-01 07:05–07:10 UTC: scheduled light `ops_run #5035`, run
  `e5c1feff0da7`, repeated the failure for 34 posts.
- 2026-08-01 11:20–11:25 UTC: scheduled light `ops_run #5047`, run
  `80ce868f7758`, repeated the failure for 34 posts.
- 2026-08-01 11:39 UTC: the persisted Kaggle log showed the exact repeated
  exception `No module named 'google_ai.limiter_supabase'`; the downloaded
  output contained only `__init__.py`, `client.py`, `exceptions.py`, and
  `secrets.py` in `embedded_repo_bundle/google_ai`.
- 2026-08-01 11:45 UTC: code inspection confirmed the same fixed allowlist in
  production `guide_excursions/kaggle_service.py`.

## Root Cause

1. `_embedded_google_ai_sources()` used a hand-maintained allowlist of four
   files instead of packaging the complete Python source tree.
2. The shared limiter cutover made `google_ai.client` depend on
   `google_ai.limiter_supabase`, but the notebook builder was not updated.
3. `google_ai.__init__` also imports `google_ai.interactions`; adding only the
   first missing file would therefore leave a second latent import failure.
4. Copying a complete auxiliary package into the kernel staging directory was
   not a sufficient runtime contract: the generated notebook reconstructs its
   own embedded package in Kaggle working storage.

## Contributing Factors

- No test executed the generated notebook bootstrap in an isolated Python
  process and imported the resulting `google_ai` package.
- The fallback for a flat input bundle repeated the same four-file copy list.
- `partial` is a valid non-fatal state for isolated provider errors, so
  scheduler health did not distinguish 34/34 terminal import failures from a
  useful partial run.

## Automation Contract

### Treat as regression guard when

- adding, removing, or changing imports between `google_ai` modules;
- changing the shared limiter, Guide Kaggle notebook generation, kernel
  staging, or repo-bundle bootstrap;
- changing Guide scheduled partial/error classification.

### Affected surfaces

- `guide_excursions/kaggle_service.py`
- `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`
- generated `guide_excursions_monitor.ipynb`
- private Kaggle Guide kernel and encrypted runtime datasets
- scheduled `guide_monitoring` full/light runs and downstream digest creation

### Mandatory checks before closure or deploy

- Package every Python source under `google_ai/` in deterministic relative-path
  order, excluding bytecode and local artifacts.
- Execute the generated notebook code in an isolated Python interpreter and
  successfully import `GoogleAIClient`, `limiter_supabase`, and `interactions`.
- Keep the flat-bundle fallback free of a hand-maintained module copy list.
- Run the focused Guide Kaggle service/schema/OCR test suites.
- After deploy, run one production-equivalent `full` catch-up with the normal
  remote `TELEGRAM_AUTH_BUNDLE_S22` ownership; do not substitute a local E2E or
  discovery session.
- Require no `ModuleNotFoundError`, `llm_ok > 0`, and inspect occurrence changes
  plus digest publication outcome.

### Required evidence

- failing `ops_run` IDs `4960`, `5035`, `5047` and persisted Kaggle log path
  `/data/guide_monitoring_results/guide-excursions-80ce868f7758/guide-excursions-monitor.log`;
- focused test output and generated-notebook isolated import result;
- fixed full-run `ops_run` / Kaggle `run_id`, result path and source/post error
  inventory;
- digest issue/publication evidence or an explicit verified empty-candidate
  result;
- deployed SHA reachable from `origin/main`.

## Immediate Mitigation

- The fault is isolated to Guide extraction; the serving bot remains healthy
  and the failed remote Kaggle lease was released normally.
- A surgical hotfix replaces both four-file lists with deterministic complete
  Python-source packaging. Deployment and production catch-up remain pending.

## Corrective Actions

- Build notebook and staged package payloads from every `google_ai/**/*.py`
  source with stable relative paths.
- Materialize parent directories for future nested package modules.
- Copy every available Python source in the legacy flat-bundle fallback.
- Add a generated-notebook subprocess test that proves the complete package can
  be imported without the repository on `PYTHONPATH`.

## Follow-up Actions

- [ ] Deploy the hotfix from a clean `origin/main`-reachable SHA.
- [ ] Run and verify the five-day full catch-up for the 46 affected candidate
  URLs before accepting the next digest result.
- [ ] Add an operational alert for a Guide run where prefiltered posts are
  non-zero but `llm_ok=0` and every LLM outcome has the same exception class.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending final branch test evidence
- post-deploy verification: pending full catch-up and digest evidence

## Prevention

- The package boundary is source-tree driven rather than a module allowlist.
- Generated artifact acceptance executes the same self-contained import
  boundary used by Kaggle instead of checking source strings only.
- This incident remains open until the missed full scan is compensated and the
  resulting publication state is verified.
