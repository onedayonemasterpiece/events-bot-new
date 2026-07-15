# Merge reviewer results — typed briefing artist/unusual iteration

## Audit identity

- Role: independent merge reviewer
- Base: `a9b829d6a865bcf08bc267aa5360103298e461fc`
- Audited head: `32664fcd64226beedefe198e65f765c960c01587`
- Integration branch/worktree: `integration/typed-briefing-artist-unusual-20260715` / `/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration`
- Initial integration status: clean.
- Data worker: `agent/typed-briefing-artist-unusual-20260715/data-product`, head `5bb0a05d4f1633a96021e69b64cecee0720bab6f`; worker status was clean.
- Integration commits: `a4b0c52e`, `e4b5dd2a`, `f7d99384`, `32664fcd`.
- The worker implementation/results were actually integrated: stable patch IDs match `ad768d56 -> a4b0c52e` and `5bb0a05d -> e4b5dd2a`; the selected data-lane files have no tree difference between worker head and integration head.

## Requirement closure

| ID | Status | Evidence | Missing / residual risk |
|---|---|---|---|
| R01 | **Done** | Supplied XLSX is normalized by `scripts/convert_artist_registry_xlsx.py` into `docs/reference/data/artist_registry_batch_001.canonical.json`; source artifact name/SHA/date/sheet are preserved, the workbook itself is not tracked, and the snapshot contains 1,235 entities. Independent `python3 -m unittest tests.test_artist_registry_converter -v` returned `4/4 OK`; converter `--check` reproduced the committed JSON from the supplied XLSX and verified SHA `c40b2389...75843ac`; JSON parsing passed. | The seed has only 47 alias-enriched rows and no row-level QID/activity/locality evidence. This limits production matching but does not invalidate the canonical conversion. |
| R02 | **Done** (safe design foundation, not a live classifier) | `docs/reference/artist-visit-registry.md` defines participant extraction, exact/fuzzy candidate matching, ambiguity review, row-level enrichment, five-state locality, freshness/evidence, audit fields and fail-closed handling. All 1,235 seed records are intentionally `locality.status=unknown`; membership or absence is explicitly forbidden as proof of non-locality. | A public non-local claim still requires on-demand row-level identity/locality evidence. No production classifier is claimed or justified by this workbook alone. |
| R03 | **Done** (documented future function) | `docs/backlog/features/static-typed-briefing/artist-arrivals-and-unusual-events.md` specifies a rolling 14-day visiting-artist digest with participant/role/locality gates, grouping/dedupe, manifest, grounded copy, cadence, observability, correction/retraction behavior, four-cycle shadow review and precision acceptance gates. | This is deliberately backlog/design scope. Runtime generation and automatic publication remain unimplemented until enrichment and shadow gates pass. |
| R04 | **Done** | Exact rejected `anticipated_person_named` and `weather_water_demo` desktop states received a fresh screenshot-based agy review, rather than inheriting the earlier wide/mobile result. Run receipts identify `/home/dev/.local/bin/a-gemini`, display model `Gemini 3.1 Pro (High)`, status `0`, empty stderr, and times. Durable evidence records `FAIL/FAIL/overall FAIL`, then six-viewport `PASS WITH CONDITIONS`, then focused crop `blocker CLOSED` / `PUBLISH PASS`; committed prompt/response hashes match the consultation README. | The wrapper does not expose its provider-internal model ID or sampling parameters; this limitation is explicitly disclosed. The result is visual acceptance, not proof of product desirability. |
| R05 | **Done** for the isolated lab | `f7d99384` restores the complete external `announcements-wordmark-ui.svg` to the isolated build/allowlist, changes named small media to a flat 4:5 grid element with no radius/shadow, strengthens bounded named/weather typography, and demotes terminal Next to a ghost action. Playwright adds 1366x768, 1440x900, 1920x900, long-name, wordmark, crop/grid and CTA hierarchy gates. Public build `preview-20260715t2005-briefing-lab-f7d99384` returns HTTP 200, `noindex,nofollow,noarchive`; manifest binds `gitSha=f7d99384...`; wordmark and wide-O assets return HTTP 200. Reviewer inspection of the public 1366 named, 1440 weather, 320 and 390 captures confirms the intended composition and visible categories/feed. Independent isolated build and allowlist check passed. Canonical evidence records the integrator's full Playwright result `11/11`. | The reviewer began, then intentionally stopped, a redundant full-suite rerun after 2 passing tests on orchestration instruction; the next test is recorded as **interrupted**, not a product failure. The durable full-run evidence remains the integrator's recorded `11/11`. Human review must still assess motion smoothness/jitter and desirability. This is an immutable lab, not production approval. |
| R06 | **Done** (design contract) | `docs/llm/unusual-event-detection.md` defines LLM-first semantic-signature extraction, category/season-aware baselines, vector recall as diagnostic only, separate grounded adjudication, evidence IDs, blockers/negative controls, `unusual_public` vs `distinctive_fact_only`, fail-closed writer rules, versioned audit fields, human labels and precision/grounding gates. Strawberry day, poppy bloom and lantern-lighter walk are explicitly historical type illustrations, not current facts. | Detector, baseline dataset and shadow evaluation are not implemented; automatic publication remains prohibited until the documented gates pass. |

## Scope and integration review

- Diff from base is confined to the requested artist/unusual documentation and converter/tests, the briefing-lab visual correction/build allowlist/Playwright coverage, consultation evidence, mobile-review evidence references, lane metadata and `CHANGELOG.md`.
- No supplied XLSX, secrets, production route, personalization runtime, production API, database migration or generated operational artifact was committed.
- Code/behavior changes are synchronized with canonical feature/preview/mobile-review documentation and `[Unreleased]` changelog entries.
- The worker lane ended clean and integrated; no worker patch is stranded.
- Public mobile evidence was delivered and verified in Telegram topic `6`: build/scope `#77`, 320/390 screenshots and motion `#78-80`, desktop evidence `#81-82`; the recorded reread contains no newer incoming user comment after `#82`.
- Scope-leakage finding: `.codex/integration/INTEGRATION_REPORT.md` at audited head is an older unrelated incident report and was not replaced with the current R01-R06 integration report. The lane map also still marks `ui-integrator: in_progress` and `merge-reviewer: planned`. These are closure bookkeeping gaps, not implementation regressions.

## Commands / checks reviewed or run

- `git status`, `git log`, `git diff --stat/name-status a9b829d6..HEAD`, worktree/branch inspection.
- Stable patch-ID and selected-tree equality checks for both data-worker commits.
- `python3 -m unittest tests.test_artist_registry_converter -v` -> `4 tests, OK`.
- Converter `--expected-sha256 ... --check` -> reproducible; `python3 -m json.tool` -> valid.
- `git diff --check a9b829d6..HEAD` -> clean.
- Independent `build:lab` plus `check:lab` -> build succeeded; allowlist `OK (6 files)`.
- Public HTTP/noindex/manifest/wordmark/wide-O checks -> HTTP 200 and source-bound immutable manifest.
- Visual inspection of committed/public evidence and validation of agy run receipts, prompts, responses and SHA-256 table.
- A redundant Playwright rerun was stopped by instruction after 2 passing tests; its third test reports only `interrupted`. The audit therefore relies on the already recorded complete `11/11` run for full-suite closure.

## Recommendation

**Implementation/public-lab recommendation: PASS.** It is safe to keep and share the immutable lab build and to push the integration commits after the root integrator:

1. replaces or adds a current task-specific integration report for R01-R06;
2. updates lane statuses to `merged/completed` and this reviewer lane to completed;
3. commits this reviewer result and confirms a clean worktree;
4. rereads Telegram topic `6` once more immediately before final closure.

Do **not** describe R02/R03/R06 as a production detector/digest, do not merge the 46k-line seed as locality truth, and do not treat the lab/Gemini verdict as production rollout or user-value validation.
