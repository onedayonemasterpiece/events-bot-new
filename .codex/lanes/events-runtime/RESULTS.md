# events-runtime lane results

## Scope

- Lane ID: `events-runtime`
- Requirements: `R01`, `R02`, `R03`
- Base SHA: `8ac67d6110870accf2d8b8295e0a9d83ef5ac87e`
- Implementation head before this amended results receipt: `305d9562e52ff4a7d6f15e9b772b931f7913595a`
- Writable boundary used: UI conformance adapters, checked aggregate fixtures, scoped tests, design-system consumer documentation, and `CHANGELOG.md`.
- Forbidden boundary respected: no production `site/src` UI behavior, deployment, Penpot, comments, or Telegram publication was changed.

## Delivered

1. Added `event-card-semantics.mjs`, a conformance-only semantic resolver:
   - one `event.meta.event-type` component variant with normalized semantic value and variable label;
   - the exact SoT admission states `ticket`, `free-entry`, `free-registration`, `registration-only`, `sold-out`, `phone`, `price`, and `absent`;
   - positive integer single/range amounts and explicit/default three-letter currency;
   - `0 ₽`, all other invalid prices, and unknown admission resolve to hidden `absent`;
   - `Условия уточняются` is retained only as an observed legacy-source anomaly;
   - EventCard Large does not claim an admission CTA component; href presence is non-component domain evidence, while interaction actions and social-proof counts remain separate.
2. Added durable exact-census fixture
   `tests/fixtures/ui-conformance/event-card-large-production-semantic-census.v1.json`:
   - exact resolver artifact SHA-256: `30c8ac5adfaeff17c463191714f660b3ed5d0a00aa8799e90f2be70cb1ca9993`;
   - exact artifact size: `11713` bytes;
   - compact fixture SHA-256: `28accbf5afad1f3272d5ff6ed076c2146f7b473b9d39562786ac4f14079aabb2`;
   - 703 current public-projection events, 31 exact rendered type labels, eight admission state groups, 61 exact price labels, social-proof and calendar aggregates;
   - broad raw-DB SHA `3578bee4…` is labeled supporting-only, not vocabulary authority.
3. Updated the render-case adapter and chip inventory to emit semantic component identities while preserving actual Astro output under `source_rendered`.
4. Regenerated the checked eight-fixture inventory by building the real pinned Astro `EventCard.astro` from SHA `22ebe3c5e92b13684cca32c14357ef7b91834977` in a disposable harness. Checked inventory SHA-256: `67f01f655dabad859f207fa6aa920e704a07b2a8addbdaaa622c0ee1103e2d38`; generator tooling SHA: `c4d9dca3e3a77c94e66df6a228f31fccb3871527`.
5. Documented the pre-Penpot-acceptance semantic boundary and added changelog coverage.

## Evidence and commands

Exact census provenance recheck:

```text
sha256sum /tmp/event-card-semantic-exact-public-census.json
30c8ac5adfaeff17c463191714f660b3ed5d0a00aa8799e90f2be70cb1ca9993
wc -c /tmp/event-card-semantic-exact-public-census.json
11713
```

Real Astro inventory generation (PASS, 8 fixtures):

```text
node scripts/ui_conformance/run-current-v2.mjs chip-inventory \
  --astro-source-site /home/dev/.codex/worktrees/events-bot-new/event-card-penpot-backport-20260820/site \
  --astro-source-sha 22ebe3c5e92b13684cca32c14357ef7b91834977 \
  --tooling-root /home/dev/.codex/worktrees/events-bot-new/event-card-semantic-runtime \
  --tooling-sha c4d9dca3e3a77c94e66df6a228f31fccb3871527 \
  --corpus-root /home/dev/.codex/worktrees/lovekgd-design-system/golden-event-corpus-v1/catalog/fixtures/ui-reference-events/v1 \
  --semantic-census tests/fixtures/ui-conformance/event-card-large-production-semantic-census.v1.json \
  --node-modules /home/dev/.codex/worktrees/events-bot-new/golden-event-corpus-v1/site/node_modules \
  --harness /tmp/event-card-semantic-inventory-c4d9dca3 \
  --output tests/fixtures/ui-conformance/event-card-large-chip-inventory.v1.json
```

Result: fixture count `8`; report hash `67f01f655dabad859f207fa6aa920e704a07b2a8addbdaaa622c0ee1103e2d38`. Valid semantic admission labels exclude `Условия уточняются`; the exact Astro-rendered legacy string remains under `source_rendered`; anomaly is `obsolete-unspecified-label-hidden`. The checked exact-vocabulary test rejects the prior underscore aliases, `unspecified`, and `invalid_price`.

Scoped test suite (PASS, 21/21):

```text
node --test \
  tests/ui-conformance-event-card-semantics.test.mjs \
  tests/ui-conformance-current-v2-runtime.test.mjs \
  tests/ui-conformance-routing.test.mjs \
  tests/ui-conformance-archetype-specimen.test.mjs
```

Syntax and scope checks (PASS):

```text
node --check scripts/ui_conformance/event-card-semantics.mjs
node --check scripts/ui_conformance/inventory-event-card-chips.mjs
node --check scripts/ui_conformance/resolve-render-case.mjs
node --check scripts/ui_conformance/run-current-v2.mjs
git diff --check
test -z "$(git diff --name-only 8ac67d6110870accf2d8b8295e0a9d83ef5ac87e -- site/src)"
```

## Risks and handoff

- Production Astro still renders the two invalid `0 ₽` observations and 96 unknown-admission fallbacks under its historical source contract. This lane deliberately does not mutate production UI before Penpot acceptance; semantic evidence fails closed and exposes the drift.
- `movie` and `therapy` are preserved as exact observed rendered labels and identified as source-language anomalies. The adapter does not invent a translation or conflate them with another type.
- The exact full census collector artifact was ephemeral/read-only; the durable aggregate fixture binds it by hash, size, schema and repository head and contains no rows/secrets.
- Integration must pin this lane's final commit in any cross-repository workflow/receipt that consumes the semantic adapter. The design contract integration reported by the parent (`849c3c…`) is not rewritten here.

## Changed files

- `.codex/lanes/events-runtime/RESULTS.md`
- `CHANGELOG.md`
- `docs/features/static-site-pages/design-system/README.md`
- `scripts/ui_conformance/event-card-semantics.mjs`
- `scripts/ui_conformance/inventory-event-card-chips.mjs`
- `scripts/ui_conformance/resolve-render-case.mjs`
- `scripts/ui_conformance/run-current-v2.mjs`
- `tests/fixtures/ui-conformance/event-card-large-chip-inventory.v1.json`
- `tests/fixtures/ui-conformance/event-card-large-production-semantic-census.v1.json`
- `tests/ui-conformance-current-v2-runtime.test.mjs`
- `tests/ui-conformance-event-card-semantics.test.mjs`
