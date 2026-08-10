# Lane L1 results — action-map-events

## Scope and status

- Lane: `L1` / `action-map-events`
- Requirements: `R01`, `R02`, `R03`, `R04`, `R06`
- Status: **complete**
- Base SHA: `d7731ab4235b325e9ca52d13c45fba83eaf5de0b`
- Validated implementation head SHA: `65bb9dbec21f511314adc1e32bb6d92fbdafab15`
- The RESULTS metadata is committed separately after the validated
  implementation head so this record can contain its exact SHA.

## Evidence

- Attachment source:
  `/home/dev/.codex/attachments/9511b37c-c15c-4db3-b8fc-0a6306c8a25e/pasted-text.txt`
- Canonical destination:
  `docs/features/static-site-pages/first-party-action-map.md`
- Exact size on both sides: `61558` bytes.
- Exact SHA-256 on both sides:
  `4ade21e6ad03d6e5d9bc934af17ad8bccb1463ebe595f16d8bafe75c0e88048a`.
- `cmp -s` returned success after copy and again before commit.
- Analytics now registers `action_map_diagnostic`, distinguishes forbidden raw
  coordinates/trajectories from campaign-only coarse local bins, and records
  zero-cost OFF, shared budgets/TTL, `ActionMapViewSummary`, reviewed evidence,
  low-sample handling and OFF-build tests.
- Personalization and ownership docs keep profile/presentation ownership
  unchanged, define the receipt as a read-only bridge, prohibit map-to-profile
  mutation, require benchmark/holdout/versioned promotion, retain raw summaries
  only in YDB TTL, keep Supabase raw rows at zero, prohibit browser YDB writes
  and route long-lived aggregate evidence to Object Storage.
- Release documentation defines AM-0 through AM-4, makes OFF proof mandatory,
  retains default-OFF as independent of the public release and enumerates
  capture NO-GO gates.
- The canonical document is routed through the static-site, feature and root
  indexes plus `docs/routes.yml`; `[Unreleased]` is synchronized.

## Commands and checks run

1. `git status --short --branch`; `git rev-parse HEAD`.
2. `wc -c` and `sha256sum` on source attachment and canonical destination.
3. `cmp -s <attachment> docs/features/static-site-pages/first-party-action-map.md`.
4. `git diff --check` and `git diff --cached --check` — PASS.
5. Inline `python3` relative Markdown-link scan over all changed documentation —
   PASS (`8` documentation files scanned; no missing relative links).
6. `python3` + PyYAML parse of `docs/routes.yml` and resolution of
   `features.static_site_pages.first_party_action_map` — PASS.
7. Inline `python3` requirement-marker assertions for analytics,
   personalization, ownership and AM-0…AM-4/NO-GO release contracts — PASS.
8. The first validation invocation used unavailable `python` and stopped before
   checks; it was rerun unchanged with installed `python3`, where all checks
   passed.
9. `git commit -m "docs: adopt first-party action-map contract"` — PASS,
   implementation commit `65bb9dbec21f511314adc1e32bb6d92fbdafab15`.

## Changed files

- `.codex/lanes/action-map-events/RESULTS.md`
- `CHANGELOG.md`
- `docs/README.md`
- `docs/architecture/personalization-data-ownership.md`
- `docs/features/README.md`
- `docs/features/static-site-pages/README.md`
- `docs/features/static-site-pages/analytics/README.md`
- `docs/features/static-site-pages/first-party-action-map.md`
- `docs/features/static-site-pages/personalizaion/personalization-to-be.md`
- `docs/features/static-site-pages/release-plan.md`
- `docs/routes.yml`

## Risks / limitations

- Documentation-only lane: no code, schema, runtime, deployment, production
  capture or live action-map behavior was implemented or tested.
- External HTTP links in the verbatim attachment were not fetched; local
  relative Markdown links and the YAML route were validated.
- The canonical attachment intentionally retains its original plain-text
  heading style and wording because R01 requires byte-for-byte preservation.
