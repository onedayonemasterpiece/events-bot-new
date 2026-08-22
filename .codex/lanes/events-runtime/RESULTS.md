# events-runtime lane results

## Scope

- Lane ID: `events-runtime`
- Requirements: R01, R04-caller, R08-runtime
- Base SHA: `1587a70b74d29e70308334f709f0f9f8fb6cf659`
- Executable implementation SHA: `54aea7a03cbbe5948aa60c40f12677483c6bcc09`
- Evidence/caller head before this receipt-only commit: `766127f7ad97ec8ca63af7b33a3d1908a8d3c835`

## Delivered

- Real Astro static-build chip inventory for all eight exact Golden Event Corpus
  v1 fixtures through pinned `EventCard.astro` at
  `22ebe3c5e92b13684cca32c14357ef7b91834977`.
- Deterministic checked report at
  `tests/fixtures/ui-conformance/event-card-large-chip-inventory.v1.json`.
- Separate immutable Astro-source/tooling checkout assertions in materialization.
- Strong fail-closed joins across case, resolved case, materialization, capture,
  Penpot receipt/export and design/Astro/tooling identities.
- Stable `run-current-v2.mjs` entrypoint with `chip-inventory`, `run-case` and
  exact seven-case `run-batch` orchestration.
- Caller interface: `design_sha`, `events_tooling_sha`, `astro_sha`, `batch`,
  `publish_telegram`, `trusted_source`; no mutable branch refs.
- No production `site/src` mutation, deploy, promotion, Penpot/comment write or
  Telegram publication.

## Astro-generated inventory evidence

Command executed with the exact historical Astro checkout, Golden v1 corpus,
lockfile-compatible installed dependencies and executable tooling SHA:

```text
node scripts/ui_conformance/run-current-v2.mjs chip-inventory \
  --astro-source-site <events@22ebe...>/site \
  --astro-source-sha 22ebe3c5e92b13684cca32c14357ef7b91834977 \
  --tooling-root <events-tooling@54aea...> \
  --tooling-sha 54aea7a03cbbe5948aa60c40f12677483c6bcc09 \
  --corpus-root <design>/catalog/fixtures/ui-reference-events/v1 \
  --node-modules <lockfile-compatible-node_modules> \
  --harness <disposable-path> \
  --output tests/fixtures/ui-conformance/event-card-large-chip-inventory.v1.json
```

Result: PASS, 8/8 fixtures, report SHA-256
`74ec329cba6b1885ba36e56f74a2eb50536243f489da60f3155a112d115b2446`.
Observed event types: `выставка`, `концерт`, `лекция`. Observed admission
labels: `1000 ₽`, `1500 ₽`, `Бесплатно · вход свободный`,
`Бесплатно · регистрация`, `Билеты`, `Запись по телефону`,
`Условия уточняются`. Both calendar present/absent and like/share zero/nonzero
branches were generated.

## Tests

```text
node --test tests/ui-conformance-current-v2-runtime.test.mjs \
  tests/ui-conformance-routing.test.mjs \
  tests/ui-conformance-archetype-specimen.test.mjs
```

PASS: 13 tests, 0 failed.

Additional checks:

- `node --check` on every changed runtime script: PASS.
- YAML parse of `.github/workflows/ui-three-way-conformance.yml`: PASS.
- `git diff --check`: PASS.
- `git diff --name-only 22ebe3c5e92b13684cca32c14357ef7b91834977 -- site/src`: empty.

## Changed files

- `.github/workflows/ui-three-way-conformance.yml`
- `CHANGELOG.md`
- `docs/features/static-site-pages/design-system/README.md`
- `scripts/ui_conformance/build-actual-tuple.mjs`
- `scripts/ui_conformance/immutable-checkout.mjs`
- `scripts/ui_conformance/inventory-event-card-chips.mjs`
- `scripts/ui_conformance/materialize-case.mjs`
- `scripts/ui_conformance/run-current-v2.mjs`
- `tests/fixtures/ui-conformance/event-card-large-chip-inventory.v1.json`
- `tests/ui-conformance-current-v2-runtime.test.mjs`
- `.codex/lanes/events-runtime/RESULTS.md`

## Remaining integration pin / risks

The caller deliberately uses the required temporary design SHA
`0882917a1328607c498d82e4c2a652bbd3df946d`. After design commit A exists, the
root integrator must repin **both** locations marked
`REPIN_AFTER_DESIGN_COMMIT_A` in
`.github/workflows/ui-three-way-conformance.yml`: the reusable-workflow `uses`
SHA and `design_sha`. Until that cross-repository pin is updated, the base
reusable workflow does not yet expose the new input contract, so a remote run is
not claimed here. The seven-case batch was not fabricated or claimed in this
lane; it must run after the design batch manifest/evidence paths and immutable
workflow SHA are integrated.

## Root integration closure

The reusable workflow is now pinned to design tooling/catalog commit
`a6b4042342ae70557bc9206d5709b1f039e2e449`, while the separate materialized
Penpot design identity remains `0882917a1328607c498d82e4c2a652bbd3df946d`.
The runtime checkout remains the immutable executable tooling SHA
`713a035a8aaa9ecfdcdd5fbd817fe504160df2f5`; no caller branch or mutable ref is
used. A real local one-case run and exact seven-case batch passed. The earlier
`REPIN_AFTER_DESIGN_COMMIT_A` blocker is resolved.

Remote run `32582038238` proved the real workflow selected seven cases and reached
browser capture, then failed fail-closed because GitHub's clean runner had no
Playwright Chromium binary. Design workflow commit
`1de19acf0c07bac87b437cff8845006f47fae1b0` adds the explicit pinned-runtime
`playwright install --with-deps chromium` step; the caller was repinned to it.

Remote run `32582230761` passed browser installation/capture and failed next at
raster comparison because Ubuntu 24.04 did not provide `identify`. Targeted
research against the GitHub runner image inventory and Ubuntu Noble package
catalog confirmed ImageMagick is not a guaranteed runner binary and Noble ships
ImageMagick 6 command names. Design workflow commit
`204acdf76dcc964222534e649a2972c59e8025fd` now installs Ubuntu's `imagemagick`
package and the pixel digest supports both ImageMagick 7 (`magick`) and 6
(`convert`).

Remote run `32582604760` then proved all seven source Astro/Penpot images match
the reviewed canonical decoded pixels. Only ImageMagick 6 versus 7 rounding in
the derived overlay/diff differed. Design commit
`7a26772828a5d74a9683c08e7e6774ff15ac61a5` keeps fail-closed source-image
review binding and permits the declared portable blend/difference transform to
be regenerated; each run's actual derivative hashes remain bound in its final
receipt.
