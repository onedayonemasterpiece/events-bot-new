# R14-COLLECTIONS results

## Outcome

- Added the DB-export-backed `/podborki/besplatnye-sobytiya/` collection. It uses the exported `ticket.is_free` decision, retains active ongoing events through `end_date`, collapses explicit occurrence families, and deliberately does not apply the 24-card cap used by narrow examples.
- Kept `/podborki/dzhaz-na-vyhodnyh/` strict and truthful. An empty result names the exact evaluated weekend; when later title-confirmed Jazz events exist in the same export, the page shows them in a clearly separate follow-up section rather than widening the result set.
- Routed both `/poisk/*` and `/podborki/*` through the configured mobile Search base so split preview composition cannot create local broken collection URLs.
- Added source-contract, route-resolution, generated-route, free-event integrity, ongoing-event, and Jazz empty/fallback gates.

## Production evidence

Read-only Telegram comment 706 inspection showed the reviewed page used reference date `2026-07-27` and was empty. A read-only Fly SQLite probe found no `джаз` match on the evaluated weekend `2026-08-01..02`, including title, description, short description, source text, type, and topics. The empty state was legitimate.

## Verification

- `node --check scripts/check-production.mjs scripts/check-secret-candidate.mjs scripts/check-unified-prototype.mjs` (run separately by shell chaining): passed.
- `node --experimental-strip-types --test tests/search-learning.test.mjs tests/mobile-discovery-routes.test.mjs tests/mobile-shell-toast.test.mjs`: 21/21 passed.
- `npm run build`: passed; 430 pages built. Generated general-free page contained 37 occurrence-collapsed free cards from the committed full production export, proving it is not capped at 24.

## Integration note

`Reference4MobileMenu.astro` was intentionally not edited because it is shared with R14-SEARCH-AUTH. During integration, change only its `Бесплатно` row from `searchHref('бесплатные события')` to `route('/podborki/besplatnye-sobytiya/')`, preserving all Search/Auth lane changes. `mobileDiscoveryRoutes.ts` in this commit already sends that route to `searchBase`.

Docs and `CHANGELOG.md` remain integration-owned and were intentionally not edited.
