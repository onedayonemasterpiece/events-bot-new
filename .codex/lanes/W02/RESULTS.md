# W02 Results — mobile v21 research prototype

## Lane contract

- Lane: `W02`
- Requirements: `P01`–`P06`
- Base SHA: `da95738c55b931780e8dd94baec36b441f434df9` (`origin/main`)
- Head: `agent/calendar-occurrences/mobile-v21` lane result commit (exact SHA reported by integrator/parent after commit; a commit cannot embed its own SHA).
- Writable implementation scope used: ignored research artifacts only, plus this result record.
- Production site and occurrence lane were not modified.

## Outcome

- **P01 Done:** `Europe/Kaliningrad` build date with optional `SITE_TODAY`; Today=2026-07-21 and Tomorrow=2026-07-22. Strictly older rows carry “Прошло”, a neutral light surface and desaturated media while text/actions remain full opacity. Same-day rows without explicit end are not marked ended.
- **P02 Done:** both cue boxes are 48px/right 6px; gallery SVG lengthens only the shaft (`M0` vs normal `M2`) and preserves head/endpoint geometry.
- **P03 Done:** reviewed 340px/three-line long summary retains the location line. Kant/Brachert renders `Светлогорск · Дом-музей Германа Брахерта`.
- **P04 Done:** right-corner hero is 11×6, 372×202 at 390px, proportional/square at 320px, near-left extension ≤0.06, date-copy intersections ≤0.04, actual 1×/2×/3× assets, crypto reload variation, early nonlinear fade with identical endpoint, face/crop fail-closed and reduced-motion retained.
- **P05 Done:** separate `/date-2026-07-24-parallax/` only, factor exactly 0.15, disabled for reduced motion. Base date page is non-parallax. Product recommendation is not to ship by default because it competes with the meaningful disappearance timeline.
- **Gemini acceptance fix Done:** user-visible counts use correct Russian inflection in static and filtered states (`1 событие`, `2/4/24 события`, `5 событий`, `21 событие`); Popular renders `24 события`.
- **P06 Done:** empirical 80px (~95% brand tag) sticky shelf subheader alternative; title/icon/brand intersections are zero across all five shelves, while event rails remain 112px.

## Artifact

- Pack: `/home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-city-popular-v21-research-20260721`
- Public-ready directory: `/home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-city-popular-v21-research-20260721/public`
- Builder: `build-v21.py`
- Validator: `validate-v21.cjs`
- Design/product notes: `README-v21.md`
- Machine report: `v21-local-validation.json`

## Evidence / commands

```bash
cd /home/dev/projects/events-bot-new
SITE_TODAY=2026-07-21 python3 /home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-city-popular-v21-research-20260721/build-v21.py
# {"files": 1042, "retina_media": 894}

cd /home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-city-popular-v21-research-20260721
python3 -m http.server 8158 --directory "$PWD/public"
node validate-v21.cjs
# 105 checks, PASS
```

Validated Playwright mobile contexts:

- 320×667 @ DPR2
- 390×844 @ DPR2
- hero/retina gate at 390×844 @ DPR3
- reduced-motion context for parallax

Material checks: Russian count forms and rendered `24 события`, zero horizontal overflow, all event/medallion images decoded, same-origin/non-404 hero backgrounds, non-distorted media boxes, date/past state, cue endpoint invariant, long-card city/location, hero geometry/caps/randomness/fade, isolated parallax, shelf safe zone, old medallions/crop metadata/city multiselect/ephemeral rail state.

Screenshots:

- `v21-date24-hero-390x844-3x.png`
- `v21-date24-hero-320x667-2x.png`
- `v21-today-390x844-2x.png`
- `v21-kant-location-390x844-2x.png`
- `v21-popular-sticky-safe-zone-390x844-2x.png`
- `v21-date24-parallax-390x844-2x.png`
- `v21-gallery-arrow-alignment-390x844-2x.png`

## Changed files

Tracked:

- `.codex/lanes/W02/RESULTS.md`

Ignored research artifact additions/updates:

- `artifacts/codex/mobile-calendar-city-popular-v21-research-20260721/build-v21.py`
- `artifacts/codex/mobile-calendar-city-popular-v21-research-20260721/validate-v21.cjs`
- `artifacts/codex/mobile-calendar-city-popular-v21-research-20260721/README-v21.md`
- generated `public/`, screenshots and `v21-local-validation.json`

## Risks / limits

- Research prototype only; no production code changed in W02.
- Parallax is intentionally an experiment and is not recommended as the default.
- The UI/UX skill CLI was unavailable because the installed `scripts` entry is an unresolved plain-text pointer; the available skill checklist, accepted v20 system and supplied Gemini 3.1 Pro High constraints were applied instead.
- Gemini 3.1 Pro final acceptance found the count-inflection defect; it is fixed and covered by two added gates while all original 103 checks remain green.
- Public-host validation remains for the integrator after upload; `validate-v21.cjs` accepts `BASE=<public URL>`.
