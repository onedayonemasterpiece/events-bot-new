# Lane PROMPT results

## Scope

- Lane ID: `PROMPT`
- Requirement ID: `R04`
- Outcome: completed
- Base SHA: `2e9996f4ba8fca9fd8cf436b0b1fbd8b319e8802`
- Implementation head SHA: `493da7d73ca1210dd9bb2b083ef8c85b1b2b4dfc`
- Branch: `agent/region-talk-editorial/prompt`
- Push: intentionally not performed

## Delivered

Created the self-contained Russian manual-consultation prompt:

- `docs/features/region-talk-channel/editorial-onboarding-writer-gemini-review.prompt.md`

It asks Gemini Pro for concrete production prompt/schema/stage-split artifacts rather than abstract advice and includes:

- complete Region Talk product and current-contract context;
- an integrated, strictly two-paragraph social/article onboarding concept;
- verified nonlocal source angle, evidence grounding, third-person source narration and Russian-only public copy;
- history-aware throughline modes and anti-template diversity checks;
- visually grounded publication merit separated from media-reuse rights;
- proposed strict input/output contracts and a staged lollipop architecture;
- 20 named failure modes and an acceptance/evaluation rubric;
- 14 varied synthetic before/after examples covering travel, east/west geography, nature, architecture, a personal blog, café boundary cases, an English source, editorial and academic articles, strong photos, ambiguous identity, history traps and constructive criticism;
- explicit requests for ready-to-embed system/developer prompts, strict schemas, deterministic validators, migration/backfill/versioning and reaction-label handling.

## Evidence and commands

Inspected for consistency:

- `docs/features/region-talk-channel/README.md`
- `docs/features/region-talk-channel/publication-queue.md`
- `docs/features/region-talk-channel/mvp-candidate-report.md`
- `docs/features/region-talk-channel/external-publications.md`
- `docs/features/region-talk-channel/telegram-vk-publishing.md`
- `docs/features/region-talk-channel/source-onboarding-profile.md`
- `docs/features/region-talk-channel/llm-verifier-contract.md`
- current writer/normalizer surfaces in `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`, `scripts/region_talk_publication_finalizer.py`, and `scripts/region_talk_publication_draft_backfill.py`

Validation commands:

```text
wc -l -w -c docs/features/region-talk-channel/editorial-onboarding-writer-gemini-review.prompt.md
# 615 lines, 4832 words, 66578 UTF-8 bytes

git diff --check
# clean

python3 inline content-contract check
# all 14 required topic/contract checks true; 14 example sections
```

No runtime tests were needed because this lane changes only a standalone consultation prompt and its lane report.

## Risks / handoff

- This file intentionally does not modify the production writer. Gemini's response still needs human/product review before prompt/schema migration and draft backfill.
- The examples are explicitly synthetic; production facts remain evidence-gated.
- Visual-copy guidance does not grant or infer media reuse rights.
- The lane did not edit canonical behavior docs, code, runtime configuration, schedules, reactions, media acquisition, or publication state.

## Changed files

- `docs/features/region-talk-channel/editorial-onboarding-writer-gemini-review.prompt.md`
- `.codex/lanes/prompt/RESULTS.md`
