# Lane W_REGISTRY Results

## Status

**Done — committed.**

## Requirement IDs

- R02 — shared place/organization registry and exact theatre/venue membership with reasons.

## Scope

- Branch: `agent/static-collections-data-prep/W_REGISTRY`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/static-collections-W_REGISTRY`
- Base SHA: `23c1702bb565f693f7022f3d7ac2e3455d6d412c`
- Implementation head SHA: `2f5a38c2` (`Add exact place organization registry`)
- The final lane head additionally contains this evidence-only RESULTS commit.

## Delivered

- Added versioned `static_place_organization_registry_v1` data with exactly 11 reviewed entities:
  - 7 mixed theatre/home-place entities;
  - Act.Opus as organization-only with no owned venue;
  - Yantar Hall, Tretyakov Kaliningrad and Philharmonia as place-only.
- Enforced exactly 8 `official_theatre` entities and 6 `venue_page_candidate` entities.
- Added exact resolver support for structured parser types, Telegram usernames, VK screen/group identities, IDNA-normalized equal/subdomains, organizer aliases, canonical venue IDs/tuples and approved venue aliases.
- Kept organization and venue roles independent. Output preserves deterministic multi-membership and deduplicated reason evidence using only `official_source`, `organizer`, and `venue` reason codes.
- Added reviewed exclusions for Yantar-as-theatre, Solyonaya Vorona, ungrounded theatre candidates, cinema/amphitheatre lexical matches and festival/title wording.
- Added schema, uniqueness, v1 count and organizer-medallion reference validation.
- Registry semantic SHA-256: `f76155d4eaded013a94c374366c7b65e448744492b953e19524e297f31b94ce5`.

## Changed files

- `site/src/data/placeOrganizationRegistry.json`
- `site/scripts/static_place_org_registry.py`
- `tests/test_static_place_org_registry.py`
- `.codex/lanes/static-collections-W_REGISTRY/RESULTS.md`

## Commands run

- `pytest -q tests/test_static_place_org_registry.py` — unavailable in the base shell (`pytest: command not found`).
- `python3 -m pytest --version` — confirmed the base Python has no pytest module.
- `/home/dev/.venvs/events-bot-region-talk/bin/pytest -q tests/test_static_place_org_registry.py`
- `python3 -m py_compile site/scripts/static_place_org_registry.py`
- `python3 -m json.tool site/src/data/placeOrganizationRegistry.json >/dev/null`
- `git diff --check`
- Focused import/count/hash/Act.Opus-offsite probe using `python3`.
- Explicit scope audit with `git status --short --branch`.

## Tests / evidence

- PASS: focused pytest — **26 passed**.
- PASS: Python bytecode compilation.
- PASS: registry JSON parsing.
- PASS: schema/unique ID/unique slug/known medallion validation.
- PASS: exact counts — 11 entities, 8 official theatres, 6 venue-page candidates.
- PASS: Act.Opus official offsite source resolves its organization but produces no Dom Molodezhi venue membership.
- PASS: guest-at-home multi-membership keeps a source-bound organization separate from the exact home venue theatre.
- PASS: exact VK group wall binding and equal/subdomain IDNA domain binding; arbitrary/repost-query/lookalike cases fail closed.
- PASS: Yantar source evidence alone creates neither theatre nor venue membership; exact venue evidence creates place membership only.
- PASS: title/topic/festival/cinema/amphitheatre/substring exclusions fail closed.
- PASS: deterministic registry hash and registry-order resolution.
- PASS: `git diff --check` and writable-scope audit.

## Risks / integration notes

- Exporter wiring is intentionally not part of this lane; the integrator must pass structured source records and persist the returned role-specific memberships.
- Four theatre identities intentionally have `medallionSlug: null` and a reviewed fallback reason; membership must not be weakened or suppressed because artwork is absent.
- Venue-only entities retain official source facts in the registry for provenance, but the resolver deliberately ignores those facts for venue membership. Venue membership always requires exact venue evidence.
- Exact approved name-only aliases work only when no partial/conflicting address or city is supplied; a wrong or incomplete tuple fails closed.
- My Theatre and City-Theatre home tuples are reviewed registry facts from the R02 contract, not claimed imports from `docs/reference/locations.md`.
- Docs and `CHANGELOG.md` were forbidden in this lane and remain integration-owner work.
