# L02-R4 desktop event chrome results

- **Status:** Done
- **Requirement IDs:** R02, R04
- **Branch:** `agent/r4-event-chrome/L02`
- **Worktree:** `/home/dev/.codex/worktrees/events-bot-new/r4-event-chrome`

## Outcome

- Made desktop breadcrumbs secondary and compact without changing their
  ordered-nav semantics. Split pages now use a 26px link target inside the
  breadcrumb trail, a compact 0.7rem treatment, tighter gaps, and substantially
  less top padding so event type/date/title enter the first viewport earlier.
- Added explicit `Main` / `Secondary` classification after the existing
  fail-closed resolver. Priority is deterministic: structured festival brand,
  festival, organizer, then venue fallback. No title/summary/description
  inference or Unicode-boundary behavior was changed.
- Added desktop `TopSlot` and `InlineSlot` rendering:
  - Main is centered exactly on the information-card top seam;
  - Main + Secondary renders both slots;
  - one Main renders TopSlot only;
  - Secondary-only renders InlineSlot only;
  - empty renders neither.
- Compact fact pills remain in the accepted mobile inline presentation but do
  not create a desktop InlineSlot. The current MUMOD event 6529 therefore has
  one centered Main medallion and no desktop InlineSlot.
- Added explicit `data-medallion-slot`, `data-medallion-role`,
  `data-medallion-category`, evidence, and principal-slug diagnostics.

## Changed files

- `site/src/components/DesktopEventPage.astro`
- `site/src/components/EventTokenMedallions.astro`
- `site/src/lib/eventMedallions.ts`
- `site/src/lib/event-medallions.test.mjs`
- `site/tests/event-detail-runtime-regressions.test.mjs`
- `.codex/lanes/L02-R4/RESULTS.md`

## Verification

```text
node --test src/lib/event-medallions.test.mjs
```

Result: `10/10` passed, including real preview event 6529, a structured
festival + venue case, and fail-closed ambiguity.

```text
node --test --test-name-pattern='desktop medallion wrapper|desktop event chrome|desktop breadcrumbs' \
  tests/event-detail-runtime-regressions.test.mjs
```

Result: `3/3` passed.

Astro dev compiled and served both review routes. Playwright Chromium at
1440x900 verified:

```text
6686: family=split, TopSlot=0, InlineSlot=0, Main=0
6529: family=editorial, TopSlot=1, InlineSlot=0, Main=1
```

Visual screenshots confirmed the 6686 breadcrumb/type/date/title hierarchy and
the 6529 left breadcrumb plus exactly centered MUMOD medallion.

`git diff --check` passed.

## Build note

An isolated full preview attempt compiled Astro and generated the target event
routes, then stopped later in the catalog on shared-workspace `ENOSPC`. The
partial output was deleted. The integration owner will run the clean full
catalog build after merge, as requested.

## Merge notes

- Desktop opts into `layout="desktop-slots"`; the component default remains the
  existing inline/mobile layout.
- Canonical docs and `CHANGELOG.md` were outside lane ownership and remain
  integration-owner work.
- No deployment, production mutation, or mobile redesign was performed.
