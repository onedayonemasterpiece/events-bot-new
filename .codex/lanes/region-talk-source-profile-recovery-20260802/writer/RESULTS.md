# R06 Writer vNext lane results

## Scope

- Lane: `region-talk-source-profile-recovery-20260802/writer` (R06)
- Branch: `agent/region-talk-source-profile-recovery/writer-vnext`
- Base SHA: `ba8ab078ba9894ccd5810045b1b8787ecb29d743`
- Implementation head SHA: `6177721a` (`feat(region-talk): require source profiles in writer vNext`)
- Production publication effect: **none**. No deploy, provider call, Telegram delivery, YDB mutation, or autopublish was executed.

## Delivered

- Bumped Writer, output, stage-execution, backfill, review-payload, legacy-review migration, and footer-repair versions so v10 drafts/reactions cannot replay as vNext revisions.
- Enforced reusable profile-before-Writer for social and publisher lanes, including strict imported publisher-profile gates (`ready`, `usable_without_profile_llm`, external scope, allowed public-copy eligibility) and persisted `source_profile_fingerprint`.
- Enforced vNext body contract after normalization and footer repair:
  - paragraph 1 exactly two sentences;
  - sentence 1 is a 45–110-character current-content hook and cannot use source/profile facts or source-name/prestige padding;
  - sentence 2 is grounded in reusable source-profile evidence;
  - paragraph 2 is one or two content-grounded detail sentences;
  - no body URL, CTA/metatext, forbidden contrast cliché, unsupported prestige claim, or incomplete/truncated final sentence.
- Added deterministic source-aware CTA rendering for author, blog, channel, outlet, journal, and safe fallback, followed by one blank-line-separated linked `О Калининграде говорят` footer.
- Added live reusable-profile/correction attachment in the notifier so stale profile fingerprints and pending correction rows cannot be delivered.
- Added current-unpublished invalidation and exact strong reads in backfill. Published rows remain excluded.
- Added RG/general candidate-correction fail-closed handling using exact correction rows only; verdict fields are preserved, provider count remains zero, and the candidate is routed to explicit externality re-adjudication.
- Closed races with a final serializable candidate CAS. The same transaction rereads the complete correction kind and substitutes a correction block instead of ready copy if a late correction appeared. Candidate changes or publication before final mutation abort the write.
- Preserved review identity semantics: new copy/profile/link metadata rotates the exact draft/operator fingerprint, archives legacy review state, resets reaction projection, and grants no publication permission.

## TDD and validation evidence

Failing P0 tests were added and observed before implementation:

```text
7 failed, 75 deselected
```

Commands run:

```bash
python3 -m py_compile \
  scripts/region_talk_goal_notify.py \
  scripts/region_talk_publication_draft_backfill.py \
  scripts/region_talk_preproduction_footer_repair.py

/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_region_talk_publication_draft_backfill.py \
  tests/test_region_talk_goal_notify.py \
  tests/test_region_talk_preproduction_footer_repair.py \
  tests/test_region_talk_publication_plan.py \
  tests/test_region_talk_orchestrator.py
# 202 passed in 4.36s

/home/dev/.codex/venvs/events-bot-new/bin/pytest -q tests/test_region_talk*.py
# 778 passed in 28.23s

git diff --check
# clean
```

The focused suite includes concurrent-candidate CAS and late-correction arrival regressions.
Generated candidate/report files created by the broad test suite were removed before commit.

## Changed files

- `scripts/region_talk_publication_draft_backfill.py`
- `scripts/region_talk_goal_notify.py`
- `scripts/region_talk_preproduction_footer_repair.py`
- `tests/test_region_talk_publication_draft_backfill.py`
- `tests/test_region_talk_goal_notify.py`
- `tests/test_region_talk_preproduction_footer_repair.py`
- `tests/test_region_talk_publication_plan.py`
- `tests/test_region_talk_orchestrator.py`
- `.codex/lanes/region-talk-source-profile-recovery-20260802/writer/RESULTS.md`

## Risks / integration notes

- The serializable multi-statement YDB CAS/correction transaction is covered with contract-level fakes but was not exercised against live YDB in this code-only lane. Integration should run its guarded live dry-run/readback before regeneration.
- This lane intentionally did not edit the finalizer, importer, CandidateReport, canonical docs, or `CHANGELOG.md`; the integration lane owns those cross-lane updates.
- Imported publisher profiles use runtime key `web:<domain>`; the writer accepts the importer's `domain:<domain>` alias and binds `profile_hash` plus `evidence_fingerprint`-grounded dimensions.
