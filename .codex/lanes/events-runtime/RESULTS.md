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
