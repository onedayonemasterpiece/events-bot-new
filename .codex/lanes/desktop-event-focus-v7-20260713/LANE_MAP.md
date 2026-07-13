# Desktop event focus v7 — lane map

Base: `d67db2e550c4975048fb5977f69a19547604b06c`

Branch: `feature/event-page-desktop-focus-v7-20260713`

Scope boundary: desktop laboratory routes at `min-width: 1024px` only. The production mobile event composition, `EventHero.astro`, `EventLayout.astro`, shared mobile media rules, and breakpoints below 1024px are immutable for this task.

## Requirement matrix

| ID | Requirement | Owner | Verification |
| --- | --- | --- | --- |
| R01 | Editorial pinned comparison: strong internal image motion while the media is sticky; when the containing stage releases it, media exits at normal document speed with no extra JS acceleration | serial integrator | Playwright scroll telemetry + screenshots |
| R02 | Editorial continuous comparison: stronger uniform scroll-linked image motion, no sticky end-state and no release jump | serial integrator | separate public route + scroll telemetry |
| R03 | Preserve the meaningful lower framing of the 1280×853 stage photo; do not cut the actors' legs | serial integrator | source dimensions + top/mid screenshot |
| R04 | Split OCR poster owns exactly 50% desktop width and reveals its natural lower part through scroll-linked movement | serial integrator | 1440/1600 desktop geometry assertions |
| R05 | Split poster gets compact image rail below it, capped cells and a final `+N`; each cell opens the selected gallery image | serial integrator | click selected index in Playwright |
| R06 | `О событии` and source content headings are subordinate to the event title | serial integrator | semantic/visual DOM assertions |
| R07 | Sticky CTA retains a safe lower gap before `Смотрите дальше` and never touches the graphite section | serial integrator | release-boundary screenshots + geometry |
| R08 | OCR + horizontal media uses the horizontal visual as Editorial hero and a compact, non-parallax OCR companion that opens fullscreen | serial integrator | dedicated scenario + click/scroll checks |
| R09 | OCR without horizontal and weak/low-resolution non-OCR media route to compact Split/fallback media instead of a giant weak hero | serial integrator | dedicated real-event scenarios |
| R10 | Review surface focuses on the two selected families and removes obsolete Gallery/Reading/Bento candidate links | serial integrator | route/index/check-preview audit |
| R11 | Gemini 3.1 Pro supplies pre-implementation design contract and browser-based acceptance critique | Gemini consultant lane | saved model/status/output artifacts |
| R12 | Mobile is unchanged | serial integrator + checklist reviewer | forbidden-file diff + 390px screenshot/hash smoke |

## Execution lanes

1. **Consultant lane (read-only):** Gemini 3.1 Pro reviews the v6 public pages and the real media inventory, then defines motion/framing/companion ordering and routing criteria. No repository writes.
2. **Code-map lane (read-only):** mapper reports selector and coupling risks while the consultant runs.
3. **Integration lane (serial writes):** all component, route, check, documentation, and changelog changes stay in this branch because they converge on one Astro component and must not race.
4. **Acceptance lane (read-only):** Playwright desktop scroll/click checks, Gemini visual acceptance, then a checklist reviewer maps the final diff to R01–R12.

## Integration order

Consultant contract → component API/markup → desktop CSS/motion runtime → focused scenario routes/index → preview checks → docs/changelog → local/public Playwright → Gemini acceptance → requirement closure → commit/push.

## Forbidden writes

- `site/src/components/EventHero.astro`
- `site/src/layouts/EventLayout.astro`
- shared production event-page mobile styles or scripts
- any selector whose effective range includes widths below `1024px`
