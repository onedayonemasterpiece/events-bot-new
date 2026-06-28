# Kaggle static-site builder

Status: implementation spike / production-publish gate pending.

## Position

Kaggle is an accepted **batch executor** in this project because the repo already uses Kaggle for monitored parser/video/social jobs. For static pages it may build Astro HTML, related/discovery manifests, golden-facet manifests, share-card artifacts and offline evaluation reports.

Kaggle must not be treated as an uncontrolled production publisher. Production trust belongs to the release protocol, not to the notebook itself.

It is acceptable for the Kaggle API actor to be a dedicated personal Kaggle user/account, but only as an execution/publisher identity inside this protocol: least-privileged credentials, immutable snapshot input, status ledger, staging-prefix upload, checked manifest, promotion gate and rollback. The personal account must not become the only place where production release state is known.

## Production protocol

```text
immutable Fly SQLite snapshot
  -> one Kaggle CPU build with status ledger
  -> staging/release prefix
  -> checks + release manifest
  -> promotion to current release
  -> rollback target retained
```

Rules:

- one private input dataset per run;
- one immutable snapshot per build; new updates during a build queue a later build, they do not mutate the running build;
- resource lease: `static_site:builder`; two production builds must not publish concurrently;
- Kaggle writes only to a unique staging/release prefix, never directly to production root with `--delete`;
- promotion requires machine-readable release manifest and passed checks;
- failed Kaggle jobs must not alter production;
- secrets must be least-privileged for the target bucket/prefix and never printed.

## Current implementation path

- Runner: `scripts/run_static_site_builder_kaggle.py`.
- Kernel script: `kaggle/StaticSiteBuilder/static_site_builder.py`.
- Data export: `site/scripts/export-production-preview-data.py`.
- Fly handoff: `JobTask.static_site_build` and `main.py` `job_static_site_build_kaggle`.
- Feature docs: `docs/features/static-site-pages/astro-preview.md`.

The current verified path produces a checked tarball artifact. CDN host `static.kenigevents.ru` is now configured for the static-site bucket and also serves mirrored event media `/p/...` plus stable calendar files `/ics/<event_id>.ics`. Production promotion is still a separate gate: build into a unique prefix, verify the release manifest, then upload/promote. For preview/focus-group builds pass:

- `PUBLIC_ASTRO_ASSET_BASE_URL=https://static.kenigevents.ru/{buildId}` or runner `--astro-asset-base-url`;
- `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru` or runner `--asset-base-url`;
- `PUBLIC_ICS_BASE_URL=https://static.kenigevents.ru/ics` when building locally; the default page logic also derives it from `PUBLIC_ASSET_BASE_URL`.

Before a CDN-enabled build, run/verify `scripts/migrate_static_media_to_cdn_bucket.py --db <snapshot> --active-on <date> --apply` so legacy `s3://kenigevents/p/...` objects referenced by active events exist in `s3://kenigevents.ru/p/...`.


## Current v47 evidence and open gate

`preview-20260628-event-pages-v47-sparse-fixes` verifies the CDN-enabled preview path on real production-snapshot data: 70 events, `npm run check:preview` passed, public Playwright regression passed, 957 referenced active media keys were present in bucket `kenigevents.ru`, and stable ICS files were uploaded under `https://static.kenigevents.ru/ics/<event_id>.ics`.

This does **not** mean Smart Update already publishes the production site to CDN automatically. The current production handoff schedules/runs the Kaggle builder and obtains a checked artifact. The remaining gate is automatic artifact upload/promotion to Object Storage/CDN with release manifest, non-concurrent promotion lock and rollback target.

## Static-site Gemma/related secrets

For API-started Kaggle static-site builds, do **not** depend on Kaggle UI Secrets for Gemma verification. The documented project pattern is encrypted split datasets. `scripts/run_static_site_builder_kaggle.py` now creates two short-lived private datasets when `--gemma-related-verify` is enabled:

- cipher dataset: `secrets.enc` plus non-secret `config.json`;
- key dataset: `fernet.key` and `fernet.keys` plus non-secret `config.json`.

Only the minimal runtime env is encrypted: the selected Google key env (default `GOOGLE_API_KEY4`) and Supabase limiter env (`SUPABASE_URL` plus `SUPABASE_KEY`/service key). The Kaggle kernel loads these envs in memory before running the exporter. A waited run deletes the secret datasets in `finally`; `--no-wait` keeps them because the kernel still needs them.

The build must still fail/skip loudly if limiter env is missing. Direct provider calls or local limiter fallback are not allowed for the related-chain Gemma audit.

## Release manifest contract

Every production candidate build must emit at least:

```json
{
  "schema_version": "static_release_manifest_v1",
  "build_id": "2026-06-28T14-20-00Z",
  "source_snapshot_id": "events_snapshot_2026_06_28_1415",
  "generated_at": "2026-06-28T14:26:13Z",
  "git_sha": "...",
  "paths": {
    "html_count": 842,
    "event_pages_count": 612,
    "listing_pages_count": 18,
    "persona_manifests_count": 96
  },
  "checks": {
    "astro_build": "ok",
    "check_preview": "ok",
    "freshness": "ok",
    "seo": "ok",
    "cdn_urls": "ok"
  },
  "artifacts": {
    "html_prefix": "releases/2026-06-28T14-20-00Z/",
    "card_snapshot": "data/cards/2026-06-28T14-20-00Z/cards.compact.json"
  }
}
```

## Status evidence

A production/Fly run must pass `/data/db.sqlite` as status DB and `/internal/kaggle/run-event` as callback so the existing poller sees:

- `kernel_started`;
- `preflight_ok`;
- meaningful `alive` progress;
- `report_written` or terminal failure;
- resource acquire/release for `static_site:builder`.

A local manual run without callback/status DB is useful build evidence, but it is not production status-ledger evidence.
