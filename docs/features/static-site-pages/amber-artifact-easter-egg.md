# Amber artifact easter egg: mobile rail placement research

> Status: noindex research prototype, **not a production gamification
> contract**. Accepted Calendar/Popular rails remain the v23 donor.
> Date: 2026-07-22.

> 2026-07-27: specimen найден и включён первым onboarding-объектом в prototype
> коллекции «Знаки Янтарного края». Канонический source-to-series контракт и
> prompt для новых изображений:
> [artifact-visual-generation.md](../static-site-easter-eggs/artifact-visual-generation.md).
> Историческая continuous motion ниже остаётся acceptance evidence noindex
> прототипа; production default определяется более строгим
> [measurement-and-state-contract.md](../static-site-easter-eggs/measurement-and-state-contract.md).

## Question

The supplied `amber-cosmonavt (3).png` is tested as a rare collectible in the
horizontal event rail. It must feel unlike an event medallion, fit entirely
inside the existing `112 CSS px` rail and remain sharp at phone DPR `1–3`.
Two placements use the same asset, found state and motion so placement—not
ornament—is the variable:

| Variant | Physical order | Product hypothesis | Main risk |
|---|---|---|---|
| A — tail | event content → medallions → like → artifact | rewards deliberate exploration and reads as service-level gamification | low discovery and a longer path to the existing end-of-rail like overpull |
| B — after medallion | event content → medallions → artifact → like | discovered sooner | looks like an event property/sponsor badge and competes with the primary like |

The artifact is always a sibling `<button>`, never nested in the event link.
The original click-anywhere event navigation and the large like retain their
own targets.

## Product verdict

**A — tail is the MVP leader.** An easter egg should reward curiosity without
pretending to describe the event. The like acts as a semantic boundary between
event content and the service-level collectible. B is useful as a deliberately
strong challenger for visibility, but its proximity to medallions creates the
wrong meaning and makes raw collection rate an unfair success metric.

Do not select B merely because it gets more taps. Compare:

- discovery among rails that were actually scrolled far enough to expose the
  assigned placement;
- collection after exposure;
- accidental immediate reversal/repeated taps;
- event-open and like conversion with/without an artifact;
- horizontal depth on later rails after the first discovery;
- repeated exposure after a collected state.

For A, release is blocked if the extra tail makes the existing physical-end
like gesture difficult to discover or measurably reduces likes. For B, release
is blocked if testing shows that users describe it as an event badge, partner
logo or a space-themed event marker.

## Visual and motion contract

- Slot: `94×112 CSS px`; transparent art: `74×96 CSS px`; no anisotropic
  scaling and no rail-height change.
- Source-derived assets: `74×96`, `149×192`, `223×288` WebP selected through
  `1x/2x/3x srcset`. CSS pixels stay constant on retina displays.
- A broad warm halo, a bright thin lower ring and two faint masked rays sit
  under the object. They reproduce the supplied reference's light coming from
  below without adding a rectangular shine layer.
- Motion begins only when at least `72%` of the horizontally hidden control is
  visible. Entry is `420ms` with a small `18px`/scale arrival. Until collection,
  the object keeps a calm `3000ms` alternate float (`2.5px`, at most `0.65°`),
  coupled to the halo/ring. A brief masked glint recurs inside a restrained
  `5200ms` cycle. It does not stop after one gesture: the collectible remains
  legible as a game object until the user taps it, while avoiding continuous
  high-amplitude jumping.
- Tap interrupts the idle, gives one `430ms` scale/brightness acknowledgement,
  then leaves a quieter object plus a visible check and `Найден` label.
- `prefers-reduced-motion: reduce` removes arrival, float, glow pulse, shimmer
  and transition transforms. The static object and non-colour found state stay
  available.

The whole `94×112` button is the touch target. It has `aria-pressed`, a changing
descriptive `aria-label`, keyboard focus and an `aria-live` found announcement.
Motion is restricted to transform/opacity/filter and must not intercept a
horizontal scroll gesture.

## State and production boundary

The prototype stores only a placement-scoped found bit in browser
`localStorage` and emits `kenigevents:artifact-collected` with artifact id,
placement and event id. It writes nothing to SQLite or Supabase. This keeps the
two links independently testable and avoids inventing a persistence schema.

### Direct noindex `/vyhodnye/` research trigger

The accepted A-tail specimen is also available directly on the real mobile
weekend listing, but only in a non-production build with the explicit research
gate:

```text
PUBLIC_SITE_MODE=preview # or secret_candidate
PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail
```

Open `/vyhodnye/` at a mobile width, find **«Теремок»** (event `6939`) in the
**«Суббота, 25 июля»** shelf, and swipe that event rail all the way right. The
`94×112` artifact is the sibling immediately after the large like. It wakes
when at least `72%` is visible; tap it to set the local found state. With
reduced motion it is fully static.

The deployed research preview must retain its `noindex` boundary. The artifact
is absent from ordinary preview builds without the flag and is hard-blocked in
production even if the flag is accidentally present. If event `6939` is no
longer in the current data snapshot, the build deterministically uses the first
Saturday event (or the first weekend event) so the explicitly enabled research
surface remains directly discoverable. The direct placement preserves the
prototype's browser-only `localStorage` state and does not add a database write.

Before production:

1. confirm redistribution/derivative rights and provenance for the supplied
   raster source;
2. define a sparse deterministic assignment so a refresh cannot farm an
   unlimited number of artifacts;
3. design cross-device identity only if the reward has durable account value;
4. aggregate analytics instead of storing rail coordinates or raw swipe paths;
5. test GPU cost and scroll continuity on a low-end Android device;
6. re-run the rail physical-end like/negative-swipe regression gates.

## Exact v28 specimens

Build: `preview-20260722-mobile-search-artifact-menu-v28`.

- **A — tail:** `/artifact-tail/`, event `6907`,
  `Эдит Пиаф. На Балу удачи`. Swipe to the physical end: the artifact is
  after the large like.
- **B — after medallion:** `/artifact-after-medallion/`, event `5511`,
  `Рок-опера «Орфей и Эвридика»`.
  The artifact is immediately after the Signal medallion and before the like.

Targets are selected pseudorandomly but deterministically from real 24 July
rows with medallions using the immutable build id. `artifact-prototypes.json`
records the event ids/titles for QA and handoff.

## Acceptance evidence

- focused component/search/shell tests: `17/17`;
- Astro build: `386` pages; generated-output gate: pass (`303` events);
- authorized Search browser smoke: skeleton, two canonical large result cards,
  exact/feedback/discovery sequence and monotonic backend progress;
- Playwright at `320×700` and `390×844`, DPR `3`: rail remains `112px`, artifact
  remains `94×112`, order is exact, button is not link-nested, tap leaves the
  URL stable and sets the non-colour found state, no horizontal page overflow;
- reduced-motion browser context: no artifact animation or shimmer;
- Gemini 3.1 Pro (High) product/motion formation review selected A as the MVP
  leader and identified B's badge/like ambiguity as the critical risk. The v28
  review requires a persistent but low-amplitude game-motion cue until tap,
  synchronised glow and an entirely static reduced-motion fallback. The only
  remaining production gate is manual scroll/tap/FPS testing on low-end Android;
  this does not block the isolated noindex research preview. Its final v28
  implementation acceptance returned **PASS / GO** for Search progress,
  artifact visual/motion and the reference-4 menu rollout.
