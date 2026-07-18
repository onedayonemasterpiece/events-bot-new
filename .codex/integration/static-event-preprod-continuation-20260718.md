# Static event preproduction continuation — integration report

Date: 2026-07-18  
Branch: `integration/static-event-preprod-continuation-20260718`  
Base: `38401787645584c2508579bd61dc3c345d0b207d`

## Requirement ledger

| ID | Outcome | Integrated change | Evidence |
|---|---|---|---|
| R01 | completed | Accepted compact graphite `SiteFooter` is global; lab route remains a regression specimen only. | root commit `d56e5310`; component/runtime tests and preview/public checks below |
| R02 | completed | Medallion wrapper exposes decorative ring/shadow overflow without changing fail-closed identity resolution. | lane implementation `5f161b8c`, integrated as `06a21881`; `.codex/lanes/media_polish/RESULTS.md` |
| R03 | completed | Desktop static related cards emit skeleton/busy state before load and clear it on load/error with fixed geometry. | lane implementation `5f161b8c`, integrated as `06a21881`; `.codex/lanes/media_polish/RESULTS.md` |
| R04 | completed | Split portrait/OCR pages use measured one-row CTA; Editorial wide-photo pages keep the stacked panel with bottom utility row. | lane implementation `edaba366`, integrated as `7664831b`; 4/4 Playwright geometry in `.codex/lanes/portrait_cta/RESULTS.md` |
| R05 | completed | Desktop event continuation is bounded at six cards, honest personal/popular-fallback modes, no load-more, with terminal `Все анонсы`; mobile fallback is not duplicated. | root commit `d56e5310`; Gemini 3.1 Pro synthesis verdict `PASS` in ignored artifact `artifacts/codex/static-event-preprod-continuation/agy/desktop-continuation-synthesis-validation-gemini-3.1-pro-high.md` |
| R06 | completed | Accepted v11/v12 templates and latest verified immutable secret candidate are canonical for bot/operator review links. | lane implementation `cec13af1`, integrated as `c7d7459a`; `.codex/lanes/preprod_release/RESULTS.md` |
| R07 | completed | Smart Update/preproduction release keeps a durable fail-closed pointer and does not advance on failed/no-op/incomplete/artifact-only builds. | lane implementation `cec13af1`, integrated as `c7d7459a`; focused 39 Python + 5 Node tests |

## Integration order

1. `98b53251` / `38401787` — isolated then compact accepted footer specimen.
2. `7664831b` / `ee590292` — portrait CTA implementation/evidence.
3. `d56e5310` — footer rollout and bounded continuation.
4. `06a21881` / `341a984e` — medallion and related-media polish/evidence.
5. `c7d7459a` / `076348c6` — durable candidate review routing/evidence.
6. root documentation, regression tests, candidate build and live verification.

No worker patch was dropped. Lane worktrees were read-only after their clean committed handoff; integration conflicts were limited to a changelog reconciliation.

## Safety boundary

- Immutable noindex `_review/<token>/` candidate only.
- Production root, `current` and stable ICS are not publication targets.
- Candidate publication must pin the exact pushed `origin/main` SHA and pass accepted-template, noindex, no-referrer, prefix-isolation and root-isolation gates.
- Telegram handoff occurs only after public Playwright acceptance of the fresh candidate.

## Final validation

Pending final integration commands, clean-main deploy SHA, fresh Smart Update/Kaggle receipt, public candidate URL and checklist review. These are appended before closure; the incident remains open until then.

### Local validation (integration source)

- Preview build: `380` pages / `303` events, `PREVIEW_BUILD_ID=preview-20260718t-static-event-preprod-continuation-local` — passed.
- Preview checker: `303 events`, `strict_related=false` — passed.
- Event runtime + personal-feed Node tests: `14/14` — passed.
- Static-release Node tests: `5/5` — passed.
- Static release/handoff/debounce/public-gate pytest: `39/39` — passed.
- Content/media tests: `6/6`; event-media quality tests: `6/6` — passed.
- Python compilation, shell syntax and `git diff --check` — passed.
- CTA Playwright at `1536×864`: `4/4` (Split `6876/4783`; Editorial `6551/5374`) — passed.
- Local browser acceptance at `1536×864`: one global footer partnership link, two legal placeholders, coloured MAX, zero overflow; immature continuation mode `popular_fallback`, `Ещё события`, exactly six unique cards, no load-more, `Все анонсы` — passed.
- Local browser acceptance at `390×844`: fallback continuation hidden, accepted mobile renderer visible, zero overflow — passed.

Fresh public candidate, exact deployed SHA and Telegram handoff remain release-phase evidence and are intentionally not claimed by the source integration checks.
