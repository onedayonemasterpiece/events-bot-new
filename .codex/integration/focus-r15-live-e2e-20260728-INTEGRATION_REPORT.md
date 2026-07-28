# Focus-group / unified static-site R15 integration report

Date: 2026-07-28  
Integration branch: `integration/static-site-focus-r15-live-e2e-20260728`  
PR: `#144`  
Reviewed head: `45072818ffed271310ed3195a1d23cdf00752c38`

## Requirement status

| Requirement | Status | Evidence / boundary |
|---|---|---|
| Focus invitation link and QR | Done | `/fokus-gruppa/` and the closed focus hub immediately produce an exact fragment invitation URL, deterministic local SVG QR, copy/share/open and SVG download. No third-party QR service. |
| Logout and repeat enrolment | Done for the browser-local focus journey | Supabase logout ends only the account session. Explicit focus exit clears only focus participation, preserves personalization, and reopening an invitation enrols again. |
| Real email OTP / Yandex controls | Done in UI/controller | Focus onboarding calls the shared Supabase `signInWithOtp()` / Yandex OAuth and identity-linking controller when public Auth configuration is present. |
| Full real mailbox → OTP → callback → Yandex-link E2E | Blocked | `kenigevents.ru` MX is SpaceWeb. A dedicated SpaceWeb inbox, Supabase custom SMTP, a usable Supabase Management PAT and real interactive Yandex user consent are still required. Browser-local E2E must not be represented as this real external journey. |
| Participant / celebrity block | Done | The event-detail participant surface and static export contract from the parallel donor are present; desktop grid/mobile rail, roles/headliner and local like persistence are covered by static release tests. |
| Focus PWA / install route | Done locally | Focus manifest/start route and focus membership restoration passed product/PWA suites. Production-root PWA was not changed. |
| Search, Favorites and mobile state corrections | Done within candidate | JSON-first Search with bounded retry and honest fallback; viewport-preserving continuation; local-first future Favorites; shared logout state. |
| Today/calendar/free/clubs corrections | Done within candidate | Kaliningrad-date honesty, generated date inventory, Free identity shelf, exhibition tail separation, and fail-safe Clubs projection are integrated. |
| Recent event-detail links | Done | A separate 30-day noindex archive keeps canonical detail/ICS routes without leaking them into listings, Search, Popular, recommendations, sitemap or catalog ledger. |
| Popular current/family correctness | Done | Ordinary elapsed ranges are excluded; semantic exhibitions may remain through `end_date`; linked occurrence families collapse before the finite limit. |
| Unusual hard negatives | Partial | Controlled source-bound fixtures pass; no title-regex classifier was added. A fresh pinned-BGE production canary remains required before treating those new negatives as promotion evidence. |
| City Jazz false exhibition | Partial data repair | Production title/type were repaired, but the canonical row still has a stale `EXHIBITIONS` topic. It requires LLM-first topic reclassification and a fresh immutable export under `INC-2026-07-02-exhibition-duplicates-static-site`. |
| Keyboard/crop release gate | Done | Real static browser gate passes hero/gallery and row crop, loaded media, canonical cards, spatial keyboard ownership, Russian-layout copy shortcuts, footer shortcuts and festival calendar. Calendar-ineligible range cards correctly expose no misleading `K` hint. |
| Documentation / CHANGELOG | Done | Canonical feature, incident, E2E and changelog records were updated with implemented behavior and explicit external blockers. |
| Gemini Pro / Opus acceptance | Blocked externally | Both approved agy model paths failed the provider eligibility check. No Flash/Lite/Gemma result was substituted. |
| Immutable production-data candidate | Pending release phase | Safe only after PR merge to `origin/main`, clean manual Fly deploy of the exact main SHA, operator build request, stale-lease regression verification and immutable noindex candidate publication. Production root must remain unchanged. |

## Validation

- GitHub PR checks:
  - `python-ci`: passed.
  - `static-browser-release-gate`: passed.
- Focus product suite: `34/34`.
- Browser release gate source contracts: `10/10`.
- Browser release gate against a real generated preview: passed all 9 required checks.
- Static release Python contracts: `62/62`.
- Static release Node contracts: `12/12`.
- PWA install contracts: `8/8`.
- Occurrence contracts: `15/15`.
- Unusual semantic combined suite: `26/26`.
- Astro build: passed, 463 pages in the final local integration build.
- Playwright at 390px:
  - invitation URL and QR decode/download;
  - invitation → consent → anonymous entry;
  - logout/explicit focus exit separation;
  - repeat enrolment;
  - no horizontal overflow or browser errors.

## Release constraints

1. Merge only with both required PR checks green.
2. Deploy only an exact clean `origin/main` SHA by manual `flyctl`.
3. Re-run the static-site stale-lease incident checks and verify the exact
   `static_site:builder` lease reaches `released`.
4. Publish only an immutable `_review/<token>/` noindex candidate.
5. Do not activate the public root, stable assets or current ICS mapping.
6. Do not claim real email/Yandex E2E until the dedicated inbox/SMTP/PAT and
   real OAuth journey have all completed.
