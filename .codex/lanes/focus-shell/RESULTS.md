# Lane results: focus-shell

- **Lane ID:** `focus-shell`
- **Requirement IDs:** `R03`, `R04`, `R05`, `R08`, `R10`
- **Base SHA:** `6e844eb3084bfd7e6787066fb291637f68ee4b93`
- **Implementation head SHA:** `c662d8ce36c53270f9a3ad1d4ee180d5ac297df9`
- **Branch:** `agent/focus-group/focus-shell`
- **Effort:** high

`Implementation head SHA` is the reviewed implementation commit. This results
record is added by a follow-up evidence-only commit; the final branch SHA is
reported to the integrator.

## Outcome

### R03 — invite / QR preview path

- Added `/fokus-gruppa/priglashenie/`.
- Its first client action inspects and removes the fragment with
  `history.replaceState`.
- The token value is never returned by the helper, logged, copied into the
  marker, or persisted.
- A bounded marker stores only fixed fields (`version`, `kind`, `source`,
  `createdAt`, `expiresAt`), is capped at 384 bytes, and expires after 72 hours.
- Added an explicit demo-share specimen. It generates 32 random bytes in memory
  and explains that no server-side expiry/max-use/revoke exists in the prototype.

### R04 — root stub and secret current-site hub

- Replaced the ordinary Astro root specimen with a polished noindex focus-group
  testing stub. This is prototype page behavior only; no production root build
  or deploy claim is made.
- Added `/zakrytaya-afisha/` as a marker-gated entry/hub for current site routes.
- Direct entry without a valid marker fails closed to a recovery screen.
- Page copy explicitly says the local marker is not authorization and that a
  single build does not move or protect all existing routes under the prefix.

### R05 — feedback surfaces

- Added reusable `FocusGroupFeedback.astro` with accessible native dialog,
  visible labels, keyboard focus restoration, 44px controls and live status.
- Kept overall relationship NPS (weekly/exit), page usefulness and always-open
  general improvement as distinct UI/data concepts.
- Added a separate `event_fact_issue` specimen with the required typed
  categories (`date`, `time`, `place`, `price`, `ticket`, `status`, `media`,
  `duplicate`, `other`), optional 2,000-character body and explicit operator
  verification / no automatic event mutation copy.
- Prototype forms do not send or persist entered values.

### R08 — onboarding / auth-choice journey

- Represented invite → congratulations/badge → separate research-consent choice
  → email or Yandex path → secret hub.
- Email is not sent/stored and Yandex OAuth is not duplicated or launched.
- The continuation state says identity is not confirmed and grants only a
  prototype-shell view.
- Added a visible thank-you panel using the exact existing Act Opus partner
  asset. It describes one pair / two invitations for any performance as a
  pending product mechanic; separate terms are required and feedback, invites
  and sharing do not improve chances.

### R10 — lab badge and licensed icon

- Used the required SVG local-first workflow. The shared library had no lab
  result, so three CC0 SVG Repo candidates were rendered into a contact sheet
  and visually compared at 150px and 28px.
- Selected **SVG Repo 287837, Flask Laboratory** because its round blue outline
  and amber fill remain recognizable at badge size and fit the existing warm
  palette better than the denser monochrome candidates.
- Added the unadapted SVG and complete source/license/curation metadata under
  `site/public/assets/icons/`.
- Added reusable compact/hero `FocusLabBadge.astro` and inspected it in the
  final root, onboarding and hub compositions.

## Commands and evidence

```text
/home/dev/projects/svg-icon-library/scripts/find_icons.py laboratory flask beaker experiment science lab
  -> no local candidates

download_svgrepo_svg.py .../231513/test-tubes-lab
download_svgrepo_svg.py .../455759/lab-test-tube
download_svgrepo_svg.py .../287837/flask-laboratory
Playwright contact-sheet screenshot + visual inspection
  -> selected 287837

node --experimental-strip-types --test src/lib/focus-group-prototype.test.ts
  -> 4 tests passed, 0 failed

npm run build
  -> 434 static pages built successfully in 2m22s

git diff --check
  -> passed

Playwright CLI, Chromium, desktop 1440x1000 and mobile 390x844
  -> fragment removed before subsequent UI logic
  -> marker present, bounded fields only, token absent
  -> email/Yandex honesty state and hub continuation work
  -> direct hub without marker shows locked recovery state
  -> NPS/usefulness/improvement/event-fact dialog paths render and submit locally
  -> demo invite path/hash shape valid; token absent from localStorage
  -> mobile document scroll width equals viewport width
  -> 0 browser console errors
```

The full build emits one pre-existing Vite warning about inconsistent JSON
import attributes in `src/lib/listingPresentation.ts`; that file is outside this
lane and the build completes successfully.

## Changed files

- `site/src/pages/index.astro`
- `site/src/pages/fokus-gruppa/index.astro`
- `site/src/pages/fokus-gruppa/priglashenie/index.astro`
- `site/src/pages/zakrytaya-afisha/index.astro`
- `site/src/components/FocusLabBadge.astro`
- `site/src/components/FocusGroupInviteIntake.astro`
- `site/src/components/FocusGroupInviteShare.astro`
- `site/src/components/FocusGroupFeedback.astro`
- `site/src/components/FocusGroupThankYou.astro`
- `site/src/lib/focus-group-prototype.ts`
- `site/src/lib/focus-group-prototype.test.ts`
- `site/public/assets/icons/lab-flask-287837.svg`
- `site/public/assets/icons/lab-flask-287837.svg.metadata.json`
- `.codex/lanes/focus-shell/RESULTS.md`

## Risks / merge notes

- This lane deliberately implements page/product mechanics only. It does not
  implement production membership, email verification, OAuth, Supabase writes,
  invite redemption, capacity, feedback persistence or deployment.
- The marker and opaque hub path are UX cues only. Production tester privileges
  still require active server-side membership checks on every protected action.
- The demo share link has no server-enforced lifetime/use count; do not reuse it
  as a production invite implementation.
- The partner/prize panel remains `pending approval`: spelling, logo use, rules,
  exceptions, dates, eligibility and fulfilment require separate acceptance.
- Production/secret builders may replace `dist/index.html`; this lane does not
  claim the production canonical root changed.
- Canonical docs and `CHANGELOG.md` were explicitly forbidden in this lane and
  remain integration-owner work.
