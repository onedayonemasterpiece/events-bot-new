# Video research lane results

## Scope

Owned files only:

- `docs/features/telegram-monitoring/video-quality.md`
- `.codex/lanes/video-research/RESULTS.md`

No runtime code, schema, routes, changelog, notebook or generated artifact was
changed in this lane.

## Delivered

- Source-backed definition of video aesthetics versus technical quality,
  relation relevance and public showcaseability.
- Quota-aware Kaggle sequence: event-confirmed and `<=10 MiB` gates, exact SHA
  versioned cache, one Flash-Lite call per unique video, shared limiter only,
  upload to Yandex CDN only after accept.
- Anchored `T/V/M/L/U/R` dimensions, deterministic technical formula, composite
  `aesthetic/showcase/rank` weights, conservative rollout thresholds and
  explicit rights/risk gates.
- Compact structured-output example and application-owned validation/
  arithmetic contract supporting one asset linked to several events.
- Human calibration, drift/quality monitoring, vertical-video requirements and
  documented LMM/video-sampling failure modes.

## Validation

- Documentation-only lane; no model API was called.
- Primary/official sources: NeurIPS/CVF papers, ITU-T P.910, Google Gemini video
  documentation, TikTok and Meta vertical-video specifications.
- Markdown links and owned-file diff reviewed locally.

## Integration notes

- Parent/integrator must add the canonical doc link to the Telegram Monitoring
  README/routes if desired; those files are outside this lane.
- Parent/integrator owns runtime implementation, tests, `CHANGELOG.md`, notebook
  synchronization and deployment.
