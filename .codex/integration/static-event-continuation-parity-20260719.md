# Static event continuation parity — integration report

## Scope

- R01: reclaim local disk from obsolete merged worktrees and old build evidence.
- R02: remove clearly obsolete immutable preview prefixes without touching
  production root, media, ICS or current secret candidates.
- R03: make the Editorial CTA motion state machine independent of the optional
  thumbnail rail.
- R04: remove the handwritten runtime event-card tree while preserving the
  separate finite desktop broad-discovery section.
- R05/R06: synchronize docs/CHANGELOG and run targeted checks without a full
  catalog rebuild or publish.

Keyboard shortcuts, hero arrow policy, `N`, Enter and focus behavior are
explicitly outside this change pending the separate product concept.

## Lane integration

| Lane | Requirement IDs | Branch | Status | Head SHA | Integration | Evidence |
|---|---|---|---|---|---|---|
| Editorial | R03 | `agent/static-event-continuation-parity/editorial` | merged | `0a6e5e7a` | cherry-picked as `fe2e3588`, `f7ff22f0` | `.codex/lanes/R03-editorial/RESULTS.md` |
| Cards/continuation | R04 | `agent/static-event-continuation-parity/cards` | merged | `738d7132` | cherry-picked as `8cf0c621`, `9159bbc0` | `.codex/lanes/R04-cards/RESULTS.md` |

## Cleanup evidence

### Local filesystem

- before: `/dev/vda2` at 98%, about 1.7 GiB available;
- removed 15 clean worktrees whose heads were ancestors of `origin/main` via
  `git worktree remove`, then pruned worktree metadata;
- removed 1.2 GiB ignored v11 build evidence from the retained clean historical
  worktree; no tracked or dirty files were removed;
- after cleanup plus creation of three temporary integration worktrees:
  `/dev/vda2` at 86%, about 9.3 GiB available.

Dirty and clean-unmerged worktrees were preserved.

### Yandex Object Storage

- applied manifest:
  `artifacts/codex/static-event-followup-analysis-20260719/object-storage-cleanup-plan.json`;
- deleted only 94 `preview-*` prefixes whose newest object predates
  `2026-07-12T00:00:00`, totalling 47,498 objects / 5,227,249,763 bytes
  (4.868 GiB);
- retained 82 newer `preview-*` prefixes, which is substantially more than the
  requested last 8–10 versions;
- protected production-root reference
  `preview-20260717-interest-clubs-prod-canary/` and verified HTTP 200;
- did not address `_review/`, `p/`, `ics/` or production root; current review
  candidate `me1oSM4a...` was verified HTTP 200;
- oldest and newest deleted prefixes return `KeyCount=0`.

## Implementation outcome

- one-photo and multi-photo Editorial pages share the same
  `hold/join/docked/release` CTA state machine; a present rail retains the
  accepted `rail bottom + 12px` anchor, while absence uses the sticky-header
  fallback;
- `EventLayout` no longer contains `eventCardHtml()` or its duplicated SVG/DOM;
- runtime cards clone inert `EventCard.astro` templates and populate text,
  datasets and allow-listed HTTP(S) URLs without interpreting event text as
  HTML;
- runtime template media uses `withBase()` and the template calendar link is
  inert, preventing secret-candidate root/stable-ICS leaks;
- desktop retains a separate `Ещё события` / `По вашим интересам` transition
  after `Смотрите дальше`, capped at six cards (two three-column rows), with
  dedupe and `3/category`, `2/venue` anti-bubble caps and no load-more;
- mature desktop personalization is labelled `По вашим интересам`; the broad
  fallback remains `Ещё события`;
- the separate event-detail broad module is hidden on mobile for every state,
  including a mature personal profile, so the established mobile continuation
  is not duplicated;
- authorized search no longer has a handwritten EventCard lookalike fallback:
  renderer failure produces an explicit non-card unavailable status;
- all keyboard handlers are unchanged.

## Verification

Passed:

```text
node --test \
  site/tests/desktop-editorial-motion.test.mjs \
  site/tests/desktop-event-cta.test.mjs \
  site/tests/event-gallery-interactions.test.mjs \
  site/tests/event-media-quality.test.mjs \
  site/tests/event-continuation-contract.test.mjs

22 passed, 0 failed

node --test --test-name-pattern='personal feed keeps|event-detail continuation uses|personal feed endpoint|runtime cards|mature personalization|desktop keeps' \
  site/tests/personal-feed-surface.test.mjs

6 passed, 0 failed
```

Also passed: `git diff --check`; changed-source audit found no keyboard/arrow/`N`
handler changes.

Not run by product constraint: full catalog export/build, secret-candidate
generation, browser screenshot matrix and publish.

Astro type-check was attempted without changing repository dependencies, but
the ephemeral npm resolver selected TypeScript 7.0.2 while `@astrojs/check`
accepts TypeScript 5/6 and failed dependency resolution. No second guessed
dependency installation was applied. The source-only and behavior tests above
remain the acceptance evidence for this no-regeneration pass.

## Review closure

The first independent checklist review found three R04 gaps: mature profiles
could expose the desktop-only module on mobile, the mature heading was not
personalized, and authorized search retained a handwritten fake-card fallback.
Follow-up commit `77e5d174` closes all three and extends the targeted contracts.
The generic historical `.codex/integration/INTEGRATION_REPORT.md` was restored;
this task uses this feature-specific report instead of overwriting v12 evidence.
