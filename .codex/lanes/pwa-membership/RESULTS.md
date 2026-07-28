# Lane pwa-membership results

- Lane: `pwa-membership`
- Requirements: `R13`, `R14`, `R15`, `R16`
- Base SHA: `5435fb075439174f92c20ebc5e3de0f17651fecf`
- Implementation head SHA: `51af4b4917fb80d8d1c7c2b8fd87a7bd333a61c0`
- Status: committed; integration reconciliation required for two stale cross-lane source assertions

## Delivered

1. Copied `docs/reference/PWA-icon.png` byte-for-byte to
   `site/public/assets/pwa/focus-group-icon.png` and placed it prominently,
   centred at the top of the mobile join screen.
2. Added a focus-specific manifest at
   `/fokus-gruppa/manifest.webmanifest`. Its static start controller is
   `/fokus-gruppa/priglashenie/?launch=pwa`: an active local participant is
   immediately replaced to `/zakrytaya-afisha/`; absent/pending state remains
   on onboarding. A direct secret-hub shortcut is also declared.
3. Added `FocusPwaInstallAction.astro` and a thin focus controller that reuses
   the incident-tested one-shot `beforeinstallprompt` implementation. The UI
   never claims it can install or launch automatically and gives honest Android
   Chrome and iOS Safari manual guidance.
4. Reworked join into a mobile-first three-step flow: invite disposal, PWA
   installation, optional identity choice. Email is not retained, no message is
   sent, Yandex is not opened, and the user may explicitly continue without
   confirmation.
5. Added an independent, bounded programme marker
   `kenigevents:focus-participation:v1` with `joining`/`active` states and only
   enum identity intent. It contains no bearer or personal value, migrates the
   legacy valid 72-hour preview hint once, remains separate from the
   personalization key, and uses a 366-day local safety horizon so the normal
   research period is not cut short by the old 72-hour expiry. Earlier server
   closure remains authoritative in the production design.
6. Installed-PWA launch with missing/cleared local state fails back to the join
   screen and states that only future server membership can recover after
   browser data deletion.

## Verification

### Passing

- `node --experimental-strip-types --test src/lib/focus-group-prototype.test.ts tests/focus-pwa-membership.test.mjs`
  - `12/12` passed.
- `node --experimental-strip-types --test tests/focus-pwa-membership.test.mjs`
  - `5/5` passed on final source.
- Incident runtime subset:
  `node --test --test-name-pattern='platform helpers|install CTA|one install event|presentation QR flow|brand and maskable|telemetry' tests/pwa-install.test.mjs`
  - `7/7` passed; preserves waiting, real/synthetic event, one-shot prompt,
    accepted/manual fallback and global icon/telemetry behavior.
- `npm run build`
  - passed, `435` pages, focus manifest emitted.
- Static MIME probe against final build:
  - focus manifest `200 application/manifest+json`;
  - focus icon `200 image/png`, decoded `1254x1254`.
- Mobile Chromium smoke at `390x844`:
  - invite fragment removed before interaction;
  - supplied logo centred at `120x120`, no horizontal overflow;
  - explicit skip activates only the programme marker and opens secret hub;
  - PWA launch controller with active marker opens `/zakrytaya-afisha/`;
  - empty browser stays on onboarding recovery;
  - no console errors/warnings.
- Screenshot artifact (ignored, not committed):
  `artifacts/codex/focus-pwa-membership/onboarding-mobile-final.png`.
- `git diff --check`: passed.

### Integration-owned stale assertions

- Full `node --test tests/pwa-install.test.mjs`: `7/8`. The failing source-only
  assertion predates this lane: the integration focus root lacks the global
  `rel=manifest`/`PwaInstallAction` contract from
  `INC-2026-07-27-pwa-presentation-install-missing.md`. `site/src/pages/index.astro`
  is forbidden in this lane. Integration must restore that global root contract.
- `npm run test:focus-group-product`: `14/15`. The only failure is an old
  source-copy assertion requiring `временная метка просмотра на 72 часа` in
  `site/tests/focus-group-product-surface.test.mjs`; R16 intentionally replaces
  it with the programme-period marker. That test is outside this lane's writable
  scope and must be reconciled after cherry-pick.

## Risks and boundaries

- This is a product/page prototype: no live identity, server membership,
  outbound email, database write, production publish or deploy was added.
- Browser storage can still be deleted or evicted. Copy explicitly avoids a
  durability guarantee and points recovery to future server membership.
- A static manifest cannot inspect localStorage, hence the state-aware start
  controller route. It is behaviorally secret-first for active members and
  onboarding-first otherwise.
- Native PWA install/launch remain browser/user-controlled. Unit/browser smoke
  can synthesize `beforeinstallprompt` but cannot force a real OS install dialog.
- Canonical docs and `CHANGELOG.md` are forbidden in this lane and remain the
  integration owner's responsibility.

## Changed files

- `site/public/assets/pwa/focus-group-icon.png`
- `site/src/components/FocusGroupInviteIntake.astro`
- `site/src/components/FocusPwaInstallAction.astro`
- `site/src/lib/focus-group-prototype.ts`
- `site/src/lib/focus-group-prototype.test.ts`
- `site/src/lib/focus-pwa-install-controller.ts`
- `site/src/pages/fokus-gruppa/manifest.webmanifest.ts`
- `site/src/pages/fokus-gruppa/priglashenie/index.astro`
- `site/tests/focus-pwa-membership.test.mjs`
- `.codex/lanes/pwa-membership/RESULTS.md`
