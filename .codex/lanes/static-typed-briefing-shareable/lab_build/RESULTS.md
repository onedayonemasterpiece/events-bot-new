# Lab build lane results

## Outcome

Implemented a shareable, isolated static briefing lab without deploying it or changing production listing/event-detail code.

- One-route Astro build at `dist-lab/$PREVIEW_BUILD_ID/lab/briefing/` using a separate `srcDir` and thin wrapper around the canonical lab page.
- Allowlist contains only the lab page, generated `_astro` CSS, lab manifest, and exact copied favicon/wordmark assets.
- Manifest records `kind=briefing-lab`, build ID, full Git SHA, generation timestamp, and public path.
- Production-dimensional shell uses the actual `EventLayout`, `.page-shell`, `.source-links`, and actual `EventListItem`.
- Exact stable `getEvents()` fixtures: `6607,5373,6020`; fail closed if missing; `6045` excluded.
- A/B/C and legacy variant names supported; exact eight canonical scenarios plus neutral fallback supported by `?scenario=`.
- Lab CTA resolves to `#events`; card activation is intercepted and recorded only as `event_detail_activate`.
- Local telemetry is capped at 24 records, has a JSON download, qualified visibility/dedupe/BFCache behavior, and performs no remote send.
- No-JS, reduced-motion, pointer/focus/scroll interruption, and non-replay behavior retained.
- No deployment was run.

## Validation

Commands:

```bash
cd site
npm run build:lab
npm run check:lab

cd ..
NODE_PATH=/opt/node-v22.22.3-linux-x64/lib/node_modules \
  npx --yes playwright test tests/playwright/static_briefing_lab.spec.ts \
  --reporter=line --workers=1

NODE_PATH=/opt/node-v22.22.3-linux-x64/lib/node_modules \
  npx --yes playwright test tests/playwright/static_briefing_lab.spec.ts \
  --reporter=line --workers=1 -g 'A/B/C|no-JS'
```

Results:

- isolated Astro build: **passed**, exactly 1 route;
- allowlist check: **passed**, exactly 5 files;
- full Playwright suite: **3 passed** in 1.2 minutes;
- focused post-assertion run: **2 passed** in 12.8 seconds;
- `git diff --check`: **passed**.

The matrix covers all 8 canonical scenarios plus fallback in both B/C across `320x568`, `375x667`, `390x844`, and `1440x900`. It checks horizontal/vertical briefing overflow, identical B/C geometry, production shell/header/card dimensions and inherited typography/tokens. It also compares identical A/B/C fixtures/categories and rejects POST, beacon, XHR, Supabase, analytics, telemetry network calls, and HTTP 4xx responses.

Representative `exhibitions_count` static geometry (`x,y,w,h,bottom`, px):

| Viewport | Shell w | Header h | Briefing | First production card | Title | Decision body |
|---|---:|---:|---|---|---|---|
| 320x568 | 296 | 64 | 12,84.2,296,150,234.2 | 12,328.4,296,354.8,683.2 | 138.2,341,156.7,71.6,412.6 | 125,329.4,182,352.8,682.2 |
| 375x667 | 351 | 64 | 12,84.2,351,150,234.2 | 12,328.4,351,336.5,664.9 | 153.7,341,196.2,78.9,419.9 | 140.5,329.4,221.5,334.5,663.9 |
| 390x844 | 366 | 64 | 12,84.2,366,150,234.2 | 12,328.4,366,320.2,648.6 | 158.8,341,206.1,82.1,423 | 145.6,329.4,231.4,318.2,647.6 |
| 1440x900 | 1180 | 56 | 130,89,1180,190,279 | 130,378.9,1180,170,548.9 | 359.2,394.5,934.6,24.2,418.7 | 343,379.9,966,168,547.9 |

The production mobile card intentionally was not shrunk to force full-card first-screen visibility; title/decision geometry is reported honestly.

## Environment limitation

The host had very low free disk space (previous full static build context had failed with `ENOSPC`). Per scope, no full catalog build and no `npm ci` were run. Validation used the already installed global Astro/Playwright packages through a local ignored module-resolution shim. `dist-lab/`, test output, and module caches remain ignored/uncommitted.
