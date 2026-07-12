# Desktop media families results

## Status
ready-for-commit

## Requirement IDs
- R01–R06

## Branch
`feature/event-page-desktop-media-families-20260712`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/event-page-desktop-media-families`

## Base SHA
`e736cf0eb3127c361f094d9e516775fdecca4888`

## Head SHA
To be recorded after commit.

## Files changed
- desktop-only lab page and lab component;
- preview structural checks;
- canonical static-page docs and changelog;
- lane map/results.

## Commands run
- real preview corpus audit over 312 event pages;
- `npm --prefix site ci`;
- `npm --prefix site run build:preview`;
- `npm --prefix site run check:preview -- preview-20260712t-desktop-media-families`;
- Playwright crop/title/action/overlap/overflow matrix at six desktop viewports;
- Playwright screenshot review with one real image enabled per isolated page.

## Tests / verification
- 434 static pages built.
- Preview structural checks passed.
- Viewports `1366×768`, `1920×1080`, `2560×1440`, `3440×1440`, `1440×650`, `1920×600`: zero horizontal overflow; all eight titles and action clusters are inside the stage; no title/place/medallion overlaps an action cluster.
- All no-OCR specimens resolve to `cover`.
- OCR safe-cover is enabled only for the explicitly reviewed Split specimen and measures `0.05–0.08%`; every other OCR specimen remains `contain`.
- Production `EventHero.astro`, `EventLayout.astro` and mobile styles are absent from the diff.

## Risks
- The lab uses Steam’s Windows survey as a QA proxy, not first-party KenigEvents viewport analytics.
- `image_text_mode` is not assumed infallible; unknown assets must fail toward OCR-safe treatment.

## Merge notes
- This is a noindex review surface. Do not promote a single layout globally; route by media contract and ratio as documented.
