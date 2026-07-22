# Design-system CopyAction lane results

## Scope

Implemented the design-system side of the reusable icon-only clipboard action on
`feature/static-design-system-catalog-20260717` without touching the diverged
product `DesktopEventActionPanel` implementation.

## Delivered

- `CopyAction.astro` owns exact Clipboard API copying plus deterministic textarea/
  `execCommand` fallback, accessible hidden live feedback and fixed-geometry state.
- canonical `copy` and `check` SVG icons; success swaps icons in the same box and
  error adds a visible `!`, so feedback is not colour-only;
- secondary and inverse tokenized variants inheriting the 44 px `Button` target;
- real light, inverse, success and error fixtures plus a candidate registry row in
  `/lab/design-system/`;
- source/build assertions, canonical design-system docs, `ADD-DS-08` and changelog.

## Validation

- `npm run check:design-system` — passed: 18 core tokens, 5 primitives and 8 AA
  contrast pairs.
- `PREVIEW_BUILD_ID=preview-20260717t-design-system-copy-action npm run build:preview`
  — passed: 424 pages; catalog emitted at `lab/design-system/index.html`.
- focused generated-catalog assertions for `data-ke-copy-action`, copy/check icons,
  secondary/inverse and success/error/live states — passed through the added build
  checks up to the unrelated search gate.
- `PREVIEW_BUILD_ID=... npm run check:preview` — CopyAction/design-system checks
  passed; the broader command later stopped at the pre-existing authorized-search
  env gate because this isolated worktree build had no public Supabase/Yandex env.

## Integration note

The product branch also changed `Icon.astro` for transport icons. Integration must
preserve both icon unions and use `CopyAction` from the product phone-number row;
do not retain its page-local clipboard listener.
