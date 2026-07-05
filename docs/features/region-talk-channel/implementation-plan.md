# Implementation plan — Region Talk Channel

## MVP-0 Documentation

Done when:

- feature docs exist;
- YDB schema draft exists;
- source/post discovery design exists;
- image scoring design exists;
- Telegram/VK publication contracts exist;
- risk register exists;
- no production code/tokens/publishing introduced.

## MVP-1 Candidate report only

- Manual seed sources.
- Fetch recent posts under strict caps.
- Score text/media.
- Store YDB test/dev rows or dry-run equivalents.
- Export `candidates-latest.xlsx`, `.csv`, `.json`, `.md/.html`.
- Include visible image model reports.
- No publishing.

## MVP-2 Favorites + manual approval

- Import/manual edit favorites decisions.
- Render Telegram/VK preview cards where rights allow.
- Add approval status.
- Still no auto-publish.

## MVP-3 Telegram dry-run/controlled publishing

- Create channel and bot admin setup outside docs.
- Publish only manually approved candidates.
- Ledger and idempotency.
- Dry-run first; controlled live only after evidence.

## MVP-4 VK dry-run/controlled publishing

- Verify VK image upload token path.
- Render carousel cards.
- Publish manually approved candidates.
- Ledger and fallback modes.

## MVP-5 Autonomous publishing

- Strict gates.
- Max 4 posts/day.
- Source diversity caps.
- Both platforms where allowed.
- Canary monitoring and rollback procedures.

## Open questions

1. Canonical public brand/handle for Telegram/VK surfaces.
2. Which YDB project/folder and credential lane should own the sidecar.
3. Initial manual seed list.
4. Fusion policy for dual-model recall: top-K per model, score normalization, union/rerank weights and disagreement handling for e5-base + bge-m3 enrichment.
5. Final Flash-Lite model id/env lane and quota budget.
6. Media rights policy thresholds for `media_reuse_allowed`.
7. Whether MVP-1 should write to real YDB or dry-run JSON first.
