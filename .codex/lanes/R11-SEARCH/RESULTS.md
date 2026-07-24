# R11-SEARCH results

## Status

Done and committed.

## Requirement ID

- `R11-SEARCH`

## Lane

- Branch: `agent/unified-r11/search`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/r11-search`
- Base SHA: `7c34d29a2ad65fc6532d934a49d4d48604f79e82`
- Implementation SHA: `cbe89d157d66cd8c636bad62c08fc61a980f92a6`
- Final lane SHA: reported in the handoff because recording it here would
  change the commit SHA.

## Outcome

- Added a bounded fetch/header watchdog backed by a request-local
  `AbortController` that remains linked to the owning search controller after
  headers arrive, so logout and `pagehide` still stop a live response stream.
- Added a fresh idle timeout around every `reader.read()`. A stream that emits
  progress and then stalls is cancelled without awaiting cancellation and gets
  exactly one controlled JSON rescue with `stream_rescue=true` and the verifier
  disabled.
- Added a bounded overall search watchdog. Timeout, transport failure, rescue
  failure, ordinary backend error, and abort paths all converge on epoch-owned
  `finally` cleanup for the input, submit button, progress state, pagination
  state, and skeleton.
- Preserved duplicate-submit protection, request epochs, logout cancellation,
  and `pagehide` cancellation. No backend or quota implementation changed.
- Replaced the ambiguous provider image/icon stack with a deterministic first
  grapheme of the displayed identity. The account control now visibly says
  `Аккаунт`, and its `aria-label` and `title` expose `Вошли как …`; the popover
  retains the same displayed identity. `alex@…` renders as `A`.

## Changed files

- `site/src/components/AuthorizedEventSearch.astro`
- `site/src/layouts/EventLayout.astro`
- `site/tests/search-recovery.test.mjs`
- `site/tests/search-recovery.playwright.mjs`
- `.codex/lanes/R11-SEARCH/RESULTS.md`

Per lane ownership, no Edge Function, canonical docs, `CHANGELOG.md`, auth
configuration, production data, or secrets were changed.

## Validation

Deterministic Node/search suite:

```text
cd site
node --test \
  tests/search-recovery.test.mjs \
  tests/search-initial-state.test.mjs \
  tests/search-learning.test.mjs \
  tests/search-progress-button.test.mjs
```

Result: `21 passed`.

Mocked browser recovery smoke:

```text
cd site
node --test tests/search-recovery.playwright.mjs
```

Result: `1 passed`. It covers:

- fetch never resolving;
- a progress frame followed by a stalled stream;
- exactly one successful JSON rescue;
- stalled JSON body stopped by the overall watchdog;
- JSON rescue failure restoring all controls;
- deterministic email avatar `A` and account identity accessibility.

Astro component compilation:

```text
node --input-type=module < Astro compiler transform check
```

Result:

```text
src/components/AuthorizedEventSearch.astro: compiled (0 non-error diagnostics)
src/layouts/EventLayout.astro: compiled (1 pre-existing inline-script hint)
```

Repository hygiene:

```text
git diff --check
```

Result: passed.

## Additional evidence and risks

- A configured Astro build completed type generation and both Vite compilation
  phases without an error, then was stopped during the repository's lengthy
  full static-route emission rather than waiting for hundreds of unrelated
  event pages. The focused Astro compiler checks above were rerun on the final
  source.
- An attempt to reuse the interrupted normal build with the older progress
  Playwright smoke timed out at its logout-abort assertion. That exposed a real
  post-header linkage issue: the fetch helper had removed its parent abort
  listener as soon as headers arrived. The final implementation keeps that
  linkage for the response body lifetime, and the deterministic regression
  `parent epoch cancellation still reaches the response stream after headers
  resolve` passes.
- The final dedicated mocked browser smoke passed. A full regenerated
  production/preview build and live authenticated Edge smoke remain integration
  gates; the task evidence stated that the real authenticated Edge smoke was
  already green, so this lane intentionally did not change or re-deploy the
  backend.
