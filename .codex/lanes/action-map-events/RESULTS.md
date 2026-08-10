# Lane L1 results — action-map-events

## Scope and status

- Lane: `L1` / `action-map-events`
- Requirements: `R01`, `R02`, `R03`, `R04`, `R06`
- Status: **complete**
- Base SHA: `d7731ab4235b325e9ca52d13c45fba83eaf5de0b`
- Validated implementation head SHA: `07753114a7174046a9d7019cdda14bc14690a8bc`
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
- Closure correction added the immutable reviewed
  `ProductAnalyticsEvidencePackage` to the canonical analysis-record contract,
  including scope/quality/facts/limitations/finding/options/decision/artifact/
  resource-link provenance and the non-automatic hotspot rule.
- Product Atlas explicitly reuses page 50 for reviewed maps/evidence and page
  40 for accepted findings/decisions, creates no page 45, imports only from a
  concrete immutable analysis record via `Обновить Product Atlas`, keeps
  Resource Graph deep links and forbids live/raw/background/hotspot automation.
- `docs/routes.yml` now exposes the product-model, analysis and Product Atlas
  entrypoints plus static-site analytics and action-map research routes.
- Final schema alignment uses canonical action-map package enums
  `finding.status=insufficient-data` and
  `decision.outcome=instrument-better`; the action-map Atlas projection uses
  `insufficient-data`, while unrelated general `insufficient_data` analysis
  states remain unchanged.

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
10. Closure-correction inline `python3` scan — PASS: all relative links in the
    three changed product-model docs resolve; PyYAML parsed all six required
    product/static-site route keys and each target exists.
11. Section-19 marker assertions — PASS for the package fields, immutable
    provenance, chain/anti-hotspot rule, pages 50/40, no page 45, explicit
    update command, no live/raw/background ingest and Resource Graph links.
12. `git commit -m "docs: complete action-map evidence routing"` — PASS,
    corrected implementation head `831336bf62e4fb3137d230db956ce785dd9d868d`.
13. Scoped enum assertions — PASS: canonical hyphenated values exist throughout
    the action-map package section and Atlas evidence paragraph; the general
    analysis rule and generic Atlas `analysis_state: insufficient_data` remain
    unchanged.
14. `git commit -m "docs: align action-map package enums"` — PASS, final
    validated implementation head `07753114a7174046a9d7019cdda14bc14690a8bc`.

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
- `docs/product-model/README.md`
- `docs/product-model/analysis/README.md`
- `docs/product-model/product-atlas-architecture.md`
- `docs/routes.yml`

## Risks / limitations

- Documentation-only lane: no code, schema, runtime, deployment, production
  capture or live action-map behavior was implemented or tested.
- External HTTP links in the verbatim attachment were not fetched; local
  relative Markdown links and the YAML route were validated.
- The canonical attachment intentionally retains its original plain-text
  heading style and wording because R01 requires byte-for-byte preservation.
