# Lane result: egg-prototype

## Scope

- Lane: `egg-prototype`
- Requirement: `R20`
- Base SHA: `b7d5910d75e898a14aa96f9fec25a673cd44f9bf`
- Implementation SHA: `8b94f2bc568b713f4245d6dcf9c02a0047144a14`
- Status: committed; ready for integration

## Delivered

- Mobile-first `/fokus-gruppa/kollektsiya/` prototype with:
  - collection coverage as the primary result;
  - participation shown separately and capped at `40`;
  - participation breadth shown separately and capped at `7`;
  - `locked`, `eligible`, `found`, and `unavailable` examples;
  - honest `Правила готовятся` copy for one pending prize of two theatre tickets;
  - explicit `На этом устройстве · не конкурсный результат` boundary.
- Reusable collection card, artifact, and saved-list demo components.
- Pure versioned state/placement model with bounded parsing, idempotent found IDs,
  stable placement bundle, collection progress, and participation caps.
- `FG-E12` prototype:
  - absent for fewer than three distinct renderable current events;
  - one semantic anchor immediately after canonical item three;
  - no fallback after a short list;
  - prior find remains recorded if the current list later shrinks;
  - accessible link to the same anchor without hover, gesture, QR, or a second device.
- Collection card added to the closed-hub directory.

## Verification evidence

1. Focused tests:

   ```text
   node --experimental-strip-types --test tests/focus-easter-eggs.test.mjs
   9 tests, 9 passed, 0 failed
   ```

2. Static build:

   ```text
   npm run build
   436 pages built
   ```

   The build emitted the existing unrelated Vite warning about inconsistent JSON
   import attributes in `relatedCardLayout.mjs`; it completed successfully.

3. Final mobile browser smoke on Astro dev server with installed Playwright
   Chromium, viewport `390x844`:

   - no horizontal overflow;
   - `FG-E12` hidden at two items;
   - visible between item three and item four at three items;
   - keyboard/screen-reader alternative visible under the same prerequisite;
   - one local found ID after activation;
   - shrinking back to two hides the placement without deleting the find;
   - restoring three renders the artifact as found;
   - no console or page errors.

4. Repository hygiene:

   ```text
   git diff --check
   ```

   Passed.

## Changed files

- `site/src/components/FocusEggArtifact.astro`
- `site/src/components/FocusEggCollectionCard.astro`
- `site/src/components/FocusEggSavedListDemo.astro`
- `site/src/lib/focus-easter-eggs.ts`
- `site/src/pages/fokus-gruppa/kollektsiya/index.astro`
- `site/src/pages/zakrytaya-afisha/index.astro`
- `site/tests/focus-easter-eggs.test.mjs`
- `.codex/lanes/egg-prototype/RESULTS.md`

## Risks and integration notes

- This is intentionally a browser-local product prototype. It is not membership,
  analytics, a prize ledger, or reward-grade evidence.
- The saved/calendar list is an isolated demonstration fixture; no existing
  listing or calendar surface was wired in this lane.
- Local state uses one versioned key and never claims to be authorization or
  proof of participation.
- Prize/legal/partner/privacy rules remain pending; integration must not replace
  the pending copy with an approved-contest claim.
