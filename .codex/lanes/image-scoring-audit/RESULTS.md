# Lane image-scoring-audit Results

## Status

completed-read-only / integrated

## Requirement IDs

- R03

## Branch

N/A — read-only reviewer lane.

## Worktree

Shared repository and local audit-artifact inspection only; no files edited.

## Base SHA

`f7abc1c29d2522831e768d8af5d94b94033be210`

## Head SHA

N/A — no commit.

## Files changed

None by reviewer. Findings were integrated by the owning integration lane into the consultant handoff and golden fixture.

## Commands / evidence checked

- `kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py` acquisition, CLIP, score finalization and source rollup paths.
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py` image/product/publication gates.
- `scripts/region_talk_orchestrator.py` threshold projection.
- Local audit evidence in `artifacts/codex/region-talk-image-false-reject-20260714/` (not committed).

## Verification

- Production scores one anchor frame per Telegram/VK post, even for albums.
- All four locked positives are albums (10, 6, 10 and 5 frames).
- `visual_consensus_score` is never produced; postcardness falls back to CLIP prompt softmax mass.
- Overall is an arithmetic mean of heterogeneous, uncalibrated signals with a model-availability-dependent denominator.
- The current gate lacks labelled source-disjoint calibration; documented VLM/safety semantics are not the runtime contract.
- Source exclusion after three posts compounds post-level false rejects.

## Risks

- Lowering the global threshold alone increases unsafe/low-value admission without repairing album sampling or calibration.
- Four operator confirmations prove a 22.2% lower bound within the 18-row rejection cohort, not the overall classifier FNR.

## Merge notes

Accepted. The integration lane preserved the four labels, full cohort, root-cause code map, safe guardrails and golden-set/shadow protocol. Image thresholds were intentionally not changed.
