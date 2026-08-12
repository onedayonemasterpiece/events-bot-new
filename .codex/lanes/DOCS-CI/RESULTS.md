# DOCS-CI lane results

## Status

**Done.** Documentation and the focused CI gate are committed on
`agent/smart-update-final-code/docs-ci`. Nothing was pushed or deployed.

- Base: `688fefb3efc6fca3b1ab73014c989ea5e120d9b1`
- Docs/CI commit: `a33929b9bea96114e7bc819adc1037145aa995c0`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/smart-update-final-docs-ci`

## Requirement checklist

| ID | Status | Result |
|---|---|---|
| DOC-01 | Done | Root, architecture, source parsing, Telegram, VK, cron, audit, and release-smoke docs now describe typed automatic ingestion; manual/legacy UI is diagnostic/admin only. |
| DOC-02 | Done | Continuations preserve the deepest `(date, post_id)`; repeated/full-duplicate/non-deeper pages retry/rebase as `OFFSET_DRIFT`/`NO_PROGRESS`; only empty/short/horizon/original-cursor evidence is terminal; legacy exact rows reopen. |
| DOC-03 | Done | The shared fact-only seven-reason contradiction collector, exact Telegram staging, maximum-one verifier, positive-child preservation, and uncertainty retry are canonical; docs link to `docs/llm/prompts.md` instead of duplicating prompts. |
| DOC-04 | Done | `SourceNoEventReason` is mandatory iff confirmed no-event, closed to seven values, and missing/unknown/misplaced values fail receipts, metrics, cursor, and terminal gates closed. |
| DOC-05 | Done | VK raw envelope v1 documents outer/copy text, attachment inventories, link/doc/video previews, hashes, secret denylist, fresh/deleted replay, and legacy-incomplete retry. |
| DOC-06 | Done | Audit/recovery examples use `--read-only` alone, a half-open window, and distinct carrier/occurrence/action units. |
| DOC-07 | Done | `[Unreleased]` contains four concise Fixed bullets for continuation drift, shared contradiction facts, typed no-event reasons, and truthful raw evidence/recovery. |
| CI-01 | Done | The existing 20-minute focused job compiles the final boundary, runs the prompt static audit, and explicitly runs source/event, TG, add-events, VK envelope/raw/continuation/queue/review, census, recovery, and rehearsal contracts. |
| REL-01 | Done | Docs remain explicit that the incident is open and the candidate is not deployed or deploy-ready. |

## Changed files

- `.github/workflows/ci.yaml`
- `CHANGELOG.md`
- `README.md`
- `docs/architecture/overview.md`
- `docs/features/source-parsing/README.md`
- `docs/features/telegram-monitoring/README.md`
- `docs/features/vk-auto-queue/README.md`
- `docs/operations/cron.md`
- `docs/operations/release-smoke-smart-update.md`
- `docs/operations/smart-update-prod-audit.md`
- `.codex/lanes/DOCS-CI/RESULTS.md` (this follow-up receipt)

## Verification

- Final workflow test inventory: **575 passed in 101.53s**.
- New final-contract subset: **423 passed in 59.60s**.
- `python -m py_compile` for the workflow's final automatic-ingestion inventory: passed.
- `python3 scripts/inspect/audit_source_parse_prompt_contract.py --root .`: `OK` (four live surfaces, shared enum parity, legacy TG extractor unreachable).
- PyYAML workflow parse: passed; focused job path inventory resolved (72 references, none missing).
- Added relative Markdown links: passed (5 checked).
- Forbidden `--read-only --dry-run` combination grep: absent.
- `git diff --check`: passed.

An exploratory broader add-events inventory included the legacy topics fixture and
found two stale monkeypatch-signature failures. That unrelated compatibility file
is not final-remediation-owned and was not placed in the focused job; the final
committed workflow inventory is the fully passing 575-test command above.

## Open release gates

This lane supplies documentation and CI evidence only. It does **not** close the
incident or authorize deployment. All four external blockers remain:

1. real provider quota/tier proof on the production route;
2. atomic rehearsal on a fresh production snapshot;
3. explicit FK-orphan disposition;
4. model-derived recovery replay through typed Smart Update with receipts.

The parent reported a small raw-envelope follow-up after this lane's base; the
documented behavior is unchanged, so the integrator should reconcile this commit
onto the final implementation head. The parent also reported that PR draft state
was restored; this lane did not mutate or independently verify PR state.
