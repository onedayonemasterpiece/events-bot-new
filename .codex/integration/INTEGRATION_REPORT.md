# Static unified prototype corrections — integration report

Base: `5c2db86811c34355a1894748b87af73fdb5b19e3`

Integration branch: `integration/static-unified-prototype-corrections-20260723`

Verification owner: `/root`

## Lane reconciliation

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| L01 | R01 | `agent/static-unified-corrections/exhibitions` | merged | `9ac1e227657cf5fdd12c22b4a8f021e1c3c1d212` | cherry-picked as `04b0dd70` | `.codex/lanes/L01/RESULTS.md`; 4 focused tests, 10 occurrence tests, 311-page worker build |
| L02 | R03 | `agent/static-unified-corrections/search` | merged | `e002eb58dd50afed86c40f7254f9bd07a7266e94` | cherry-picked as `d6f4d4e8` | `.codex/lanes/L02/RESULTS.md`; 13 tests, 311-page worker build, desktop/mobile computed-style smoke |
| L03 | R04, R05 | `agent/static-unified-corrections/clubs-partners` | merged | `6c8f6dd84a0a3c5ba153c250af26af7f979c1fbc` | cherry-picked as `61295c61` | `.codex/lanes/L03/RESULTS.md`; 4 Node + 3 Python tests, 314-page worker build |
| L04 | R08 | `agent/static-unified-corrections/transport` | merged | `a1e22643f7f1976478505481d4ce2efbe130b2c1` | cherry-picked as `5cfdc4df` | `.codex/lanes/L04/RESULTS.md`; 5 Node + 4 Python tests, 311-page worker build |
| L05 | R02, R07 | `agent/static-unified-corrections/cards-ocr` | merged | `6deb8dd63308b880a83f35e36b0ab0c3c51b7421` | cherry-picked as `cf5f8991` | `.codex/lanes/L05/RESULTS.md`; 22 Python + 37 Node tests, 311-page worker build |
| L06 | R06 | integration branch | merged | `31245333b6b8be4a4ac80c72de36097c2f5d7a89` plus final corrections | serial integration | shared component/helper, route/SEO wiring, primary-source research and valid Gemini Pro review |

All worker worktrees were clean at handoff. No worker branch was merged
wholesale; only the listed commits were cherry-picked.

## Real-data source

- Snapshot:
  `artifacts/db/static-unified-corrections-clubs-partners-20260723.sqlite`
- Snapshot SHA-256:
  `f49c5e829d6c230a92b76f3dc4a937d18991b6659c60fa73b85d648cc7953175`
- Size: `279195648` bytes
- Export cutoff: `2026-07-23T09:00` Europe/Kaliningrad
- Export: 288 eligible real events; current interest-club projection enabled.
- Required specimens materialized: events `6686`, `6529`, `6990`.

## Integrated verification

- Occurrence resolver/formatter suite: 10/10 passed.
- `INC-2026-07-18-dramteatr-same-day-event-glue` identity replay/positive
  control: 15/15 passed.
- Focused integrated Node suite: 47/47 passed after the immutable preview build.
- Preview build: 389 pages.
- `check:preview`: passed (fresh full catalog has no expired control fixture).
- `check:unified-prototype`: passed for 18 primary routes, 288 event pages and
  369 compact related cards.
- Local browser acceptance: 16/16 route/viewport checks passed at 1440×1000
  and 390×844, including zero horizontal overflow, initial-hidden Search
  skeleton, responsive breadcrumbs, shared personal-feed packing, protected
  event 6686 media and same-day event 6529 returns.
- Immutable preview deployment:
  `https://kenigevents.ru/preview-20260723-unified-corrections-r1/__preview/`;
  all eight reviewed routes returned HTTP 200 with `noindex`. The deployment
  script wrote only the immutable preview prefix and did not modify the
  production root or stable `/ics`.
- Live browser acceptance: 16/16 route/viewport checks passed against the
  public URL.
- Final external acceptance: **PASS** from valid
  `gemini-3.1-pro-low` (`Gemini 3.1 Pro (Low)` in provider log), with R01–R08
  all PASS and no mandatory pre-review fixes. The only non-blocking
  recommendation was to add exhibitions-specific card metrics to the browser
  collector.
- Browser screenshots/metrics and Gemini prompt, response and provider log are
  recorded in task-local non-committed artifacts under
  `artifacts/codex/static-unified-corrections-20260723/`.

## Closure audit

| ID | Requirement | Status | Evidence | Missing / risk |
|---|---|---|---|---|
| R01 | Exhibitions donor on review `/vystavki/` | Done | dynamic projection + generated donor markers | Root promotion intentionally not performed |
| R02 | Large-card `/dlya-menya/` | Done | shared optimized three-card desktop rows + one-card mobile flow | Review-only route |
| R03 | Search initial state/style | Done | initially hidden skeleton + shared control styling | Live auth/backend acceptance remains separate |
| R04 | Current clubs | Done | three policy-current clubs; event 6990 internal link | Count may change on future snapshots by policy |
| R05 | ICAE partner | Done | local official SVG + official center URL | Logo-use authorization required before root promotion |
| R06 | Breadcrumb product contract | Done | selective deep-page component, JSON-LD, desktop/mobile rules | No invented category hierarchy |
| R07 | Event 6686 poster protection | Done | exporter `unknown_document/error`; large hero `contain` | Semantic reclassification may later replace fail-closed state |
| R08 | Event 6529 return shortlist | Done | labelled estimate; `18:56`, `19:43`; no next morning | Estimate is explicitly review-only and must be source-confirmed for production |
