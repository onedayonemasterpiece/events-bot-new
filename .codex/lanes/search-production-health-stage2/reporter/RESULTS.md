# Stage 2 reporter lane results

- Lane ID: `reporter`
- Requirement IDs: `R07` (with the R08 sanitized-summary boundary consumed, not owned)
- Base SHA: `dd1e0ad9072acdad46f01459ba4ab0ff171e0318`
- Initial planner head SHA: `01642339ae35cd3504488fabf79b671c7f8e95a1`
- REST applier head SHA: `6738e1b91f60ba8dd1d62922b9e6e1ac47759207`
- Branch: `agent/search-stage2-reporter`
- Status: complete

## Delivered

- Strict, fail-closed `search_production_health_summary_v1` normalizer with an exact field allowlist: platform, independent product/execution/failure dimensions, non-secret target/runtime fingerprints, and GitHub Actions run id/URL.
- Pure deterministic disposition planner and stdin/file CLI; no GitHub calls and no live operations.
- Immediate platform-scoped product plan for proven `BROKEN_*`: `search-product:<platform>:<failure_class>`.
- Healthy/PASS/null proof closes only product issues matching the exact platform prefix; it never closes or suppresses another platform or non-product issue.
- Platform-local `UNKNOWN_*` streak; only the third consecutive identical terminal class opens `search-infra:<platform>:<failure_class>`. Other-platform cells are ignored, while a different class on the same platform resets the streak.
- Immediate independent cost and security/evidence plans; evidence-redaction plans forbid artifact upload. Blocked-release outcomes plan no incident.
- Fixed generated issue body; raw target URL, query, card, session, logs, credentials, email, and artifact URL are not accepted by the input schema.
- Strict side-effect-boundary plan validator: modified bodies, titles, labels, fingerprints, actions, or extra fields fail before REST.
- GitHub REST applier using `GITHUB_TOKEN` / `GITHUB_REPOSITORY`: exact fingerprint create/reopen/update, or exact-platform open product-issue closure. Pull requests and unrelated platform/cost/security issues are excluded.
- `none` performs zero REST calls; `--dry-run` performs lookup only. CLI output contains only the action/fingerprint or prefix/issue numbers/dry-run flag, never the issue body or token.

## Changed files

- `site/e2e/search/production-health-disposition/summary.mjs`
- `site/e2e/search/production-health-disposition/report-plan.mjs`
- `site/e2e/search/production-health-disposition/cli.mjs`
- `site/tests/search-production-health-disposition.test.mjs`
- `.github/scripts/apply-search-health-report-plan.mjs`
- `site/tests/search-production-health-report-apply.test.mjs`
- `.codex/lanes/search-production-health-stage2/reporter/RESULTS.md`

## Evidence

- `npm ci --no-audit --no-fund` — installed the isolated worktree dependencies.
- `node --test tests/search-production-health-disposition.test.mjs tests/search-production-health-report-apply.test.mjs` — 18/18 passed.
- `npm run test:search-production-health` — 48/48 passed.
- `node --check e2e/search/production-health-disposition/{summary,report-plan,cli}.mjs` — passed.
- `node --check ../.github/scripts/apply-search-health-report-plan.mjs` — passed.
- `git diff --check` — passed.

The REST shape was checked against the current official GitHub Issues and issue-comments endpoint documentation. API version `2022-11-28` remains officially supported through 2028-03-10.

The first aggregate-suite attempt failed before tests because this new worktree had no installed `yaml` package. After the standard locked `npm ci`, the unchanged workflow-contract test and the complete production-health suite passed. No source/config workaround was used.

## Integration contract

The workflow lane supplies a JSON envelope `{ "summary": ..., "history": [...] }` to:

`node site/e2e/search/production-health-disposition/cli.mjs`

`history` is chronological and excludes the current summary. The workflow must persist or retrieve prior sanitized terminal summaries, then pass only the produced strict plan to:

`node .github/scripts/apply-search-health-report-plan.mjs --input <plan.json>`

The applier matches the exact fingerprint (open/update) or exact platform product prefix (healthy closure). `--dry-run` resolves the intended mutation with GET requests but sends no POST/PATCH.

## Risks / intentionally deferred

- Cross-run sanitized history persistence remains owned by workflow integration.
- The workflow must grant `issues: write`; the three fixed labels in each selected plan must already exist or be provisioned separately. The applier never broadens matching when a label is absent.
- Lookup is fail-closed after 1,000 labeled issues and on duplicate exact fingerprints. Workflow concurrency must prevent a list/create race between simultaneous runs.
- Closure posts the fixed sanitized close comment before closing. A retry after a partial comment-success/close-failure can add a duplicate close comment, but cannot close another platform/kind.
- Run URLs deliberately accept only exact GitHub Actions run pages. Reusable-job or check URLs require an explicit schema revision rather than relaxed validation.
- No `gh issue` call, workflow run, artifact upload, or other live action was performed.
