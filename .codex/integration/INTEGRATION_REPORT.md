# Static-site review R14 — integration report

Snapshot: 2026-07-27 UTC

Integration branch: `integration/static-site-review-r14-20260727`

PR: <https://github.com/onedayonemasterpiece/events-bot-new/pull/125>

Release decision: **ready to merge into `origin/main`; production root remains
NO-GO**. The only permitted publication after merge is a fresh immutable
`/_review/<256-bit-token>/` noindex candidate produced by the production
candidate pipeline. This report does not treat a preview URL as a root release.

Operational follow-up on 2026-07-27: the normal Kaggle handoff is blocked before
kernel start by `400 INVALID_ARGUMENT: Invalid token` while creating its
short-lived private input dataset. The documented trusted-host fallback may
produce only an immutable secret/noindex review candidate from the same frozen
snapshot and clean main SHA. It does not count as Kaggle status-ledger evidence,
does not publish the root archive and does not change the NO-GO decision.

The previous 2026-07-23 report is preserved at
`static-unified-prototype-corrections-20260723-INTEGRATION_REPORT.md`.

## Integrated lanes

| Lane | Scope | Integration commit | Status |
|---|---|---:|---|
| R14-COLLECTIONS | complete DB-backed Free collection; truthful Jazz state | `19c7f5a0` | integrated |
| R14-VISUAL | OCR-safe heroes/rails, real gallery media, Clubs title, share composer | `322d9d4e` | integrated |
| R14-SEARCH-AUTH | Enter/IME/search recovery and one global Supabase/Yandex session | `21e37779` | integrated |
| R14-MEDALLIONS | bounded organizer identity, explicit source mappings and assets | `632a208e` | integrated; production rows require schema/backfill |
| R14-ARTIFACTS | one deterministic eligible event and five-slot local collection | `a379a931` | integrated; secret candidate only |
| R14-INTEGRATION | shared shell/menu/auth wiring, release gates and documentation | `fe0b8e65`, `7d96b428` | validated |

All worker commits are patch-equivalent in the integration branch. Shared files
were reconciled serially; old labs were not merged wholesale.

## Requirement closure

| ID | Result | Evidence / remaining boundary |
|---|---|---|
| TG691 | Done | Today remains one chronological stream with past-state styling regression tests. |
| R01/R03 | Partial | Resolver, schema, Smart Update and exact Profi-Tour/Hraniteli/Yantar Hall/Dom Iskusstv mappings are integrated. Fly schema/backfill and generated candidate proof remain release operations. |
| R02 | Done in code | Mobile OCR/unknown hero and rail media remain whole; classified visual media fills. Public Maria Stuart pixel acceptance remains part of candidate QA. |
| R04 | Done | Up to four real source-ordered, deduplicated rail images; OCR protection is per asset. |
| R05 | Done in code | Search Enter/requestSubmit, IME guard, `enterkeyhint=search`, bounded header and stream-idle rescue. Real Edge/public backend smoke remains candidate QA. |
| R06 | Done in code | Exactly one `StaticSiteAuthRuntime` in `EventLayout`; Search, menu and Personal use the same browser singleton; menu login/logout is wired. A real Yandex OAuth round trip needs the owner's browser session. |
| R07 | Done in code | Mobile Clubs shelf title is visible and sticky. Candidate geometry QA remains. |
| R08 | Done | Exact Jazz weekend is checked honestly and later Jazz fallback is labelled; no fabricated events. |
| R09 | Done in code | 1080×1350 share image carries KenigEvents brand, title, date/time, place and admission. Native Web Share remains device QA. |
| R10/R11 | Done for research candidate | One build-seeded eligible real weekend event; five slots, found/empty/detail states, local persistence and disabled `Поделиться артефактом · скоро`. Production/root is explicitly absent. |
| R12 | Done | All Free navigation opens `/podborki/besplatnye-sobytiya/`, never Search. |
| R13/R14 | Done | Canonical docs, `docs/routes.yml`, CHANGELOG, presentation checklist and release plan agree on current behavior and NO-GO root status. |
| TG708–715 | Partial | Consent/dedupe/bot-exclusion/denominator contracts are documented. No emitter, ingest, aggregate or reporting UI is claimed. |
| R15 | Partial until publication | Clean main merge, Fly schema/backfill, production-candidate run, public desktop/mobile QA and Telegram receipt remain sequential release steps. |

## Validation

- Focused integrated Node suites before the final gate: **103/103 passed**.
- Organizer/Smart Update Python suites: **40/40 passed**; Telegram medallion
  tests: **4/4 passed**.
- Exact Astro preview build on current source: **434 pages**, passed.
- Chromium release gate on the generated tree: all nine mandatory checks passed:
  hero/gallery crop, related geometry and decoded media, canonical EventCard,
  spatial and cold/pointer/Russian-layout keyboard paths, cross-document gallery,
  footer shortcuts and festival calendar at `1440×900`/`390×844`.
- Browser media receipt explicitly exercises `visual-cover`, bounded
  `document-safe-cover` (`<=20%`) and evidence-free `document-contain` fail-closed
  branches without fallback bleed.
- PR CI on the main-updated head: `python-ci` and
  `static-browser-release-gate` passed.
- The production artifact-leakage gate now distinguishes the actual bare
  `data-amber-artifact`/`data-artifact-collection` markers from the inert
  `data-amber-artifact-research="off"` configuration and
  `data-artifact-collection-unavailable` fallback; focused artifact tests cover
  both boundaries.
- `git diff --check`: passed.

Non-committed evidence is under `artifacts/codex/r14-*`.

## External consultant boundary

A fresh `a-gemini` request using the approved Gemini Pro lane and the allowed
`a-opus` fallback both failed before model execution with the Antigravity
eligibility response `not currently available in your location`. No Flash/Lite
or other model was substituted, and this report does **not** claim Gemini/Opus
acceptance. The internal high-effort checklist review is recorded separately and
is not presented as an external consultant review.

## Release handoff

After this report reaches `origin/main`:

1. deploy the exact clean main SHA to Fly so runtime DDL adds
   `event.organizer_names`;
2. idempotently backfill only source-grounded Profi-Tour/Hraniteli rows and keep
   event `6767` negative;
3. request one fresh full-catalog `production-candidate` build with immutable
   snapshot, exact repo SHA and secret publishing enabled;
4. run public desktop/mobile/browser acceptance against the bearer URL;
5. send the complete mutually linked route inventory to the Telegram review
   thread and record the receipt;
6. do not promote root/current/stable ICS. Root remains NO-GO until the separate
   atomic-promotion/lifecycle/rollback gates in `release-plan.md` close.
