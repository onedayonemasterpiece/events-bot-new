# Kaggle static-site builder

Status: implementation spike / production-publish gate pending.

Current event-page release sequencing, top-five platform backlog and the planned
10-day Telegraph coexistence/cutover are canonical in
[`docs/features/static-site-pages/release-plan.md`](../features/static-site-pages/release-plan.md).
The mode names described there are a required implementation contract, not
already-existing production env flags.

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


## Current v59 strict pgvector evidence and open gate

`preview-20260629-event-pages-v59-related-gemma50` is the current strict related canary on real production-snapshot data: 50 events focused on 2026-06-30/2026-07-01, CDN-enabled assets/ICS, `npm run check:preview` passed, public Playwright smoke passed, and `/data/discovery/6447.json` shows `6310` “Архитектурно-урбанистическая студия...” as the strict Gemma-approved first related candidate (`llm_semantic_score=0.88`).

Smart Update handoff now passes the pgvector/Gemma flags from environment into `scripts/run_static_site_builder_kaggle.py`: `--related-mode`, `--sync-pgvector-vectors`, `--pgvector-*`, `--gemma-related-*`, status DB/callback, CDN asset/ICS bases, browser-safe AuthorizedEventSearch public env (`--public-personalization-supabase-url`, `--public-personalization-supabase-publishable-key`, `--public-yandex-auth-provider`) and the date-focus controls `--current-datetime`, `--focus-date-from`, `--focus-date-to`. This means the coalesced `static_site_build` job can reproduce the v59-style vector sync/retrieval/Gemma strict-verification process from `/data/db.sqlite` after Smart Update and can render the one-line authorized search UI in focus-group previews when Yandex/Supabase Auth is configured.

`INC-2026-07-11-event-vector-sidecar-sync-stalled` restored this optional
handoff after a merge dropped it. Regular production vector ingestion is owned
by the separate full-catalog `event_vector_sync` lane; do not enable a coupled
preview build merely to keep search vectors fresh.

Static related publication policy:

- Astro generation consumes cached strict related manifests and does not call Supabase/LLM on page view;
- a related cache is reusable only when the event id set, search fingerprints and Gemma policy signature match;
- if new/changed events are present, recompute the active/future related graph because the new event can be a candidate for old pages;
- same-day events that already started, past events and cancelled/deleted/duplicate rows are excluded at export/generation time;
- static related generation retries Gemma 4 26B with backoff and does not fall back to Gemini/Flash-Lite, preserving Lite quota for runtime flows.

This still does **not** mean Smart Update already publishes the production site to CDN automatically. The current production handoff schedules/runs the Kaggle builder and obtains a checked artifact. The remaining gate is automatic artifact upload/promotion to Object Storage/CDN with release manifest, non-concurrent promotion lock and rollback target.

## Static-site Gemma/related secrets

For API-started Kaggle static-site builds, do **not** depend on Kaggle UI Secrets for Gemma verification. The documented project pattern is encrypted split datasets. `scripts/run_static_site_builder_kaggle.py` now creates two short-lived private datasets when `--gemma-related-verify` is enabled:

- cipher dataset: `secrets.enc` plus non-secret `config.json`;
- key dataset: `fernet.key` and `fernet.keys` plus non-secret `config.json`.

Only the minimal runtime env is encrypted: the selected Google key env (default `GOOGLE_API_KEY4`) and Supabase limiter env (`SUPABASE_URL` plus `SUPABASE_KEY`/service key). The Kaggle kernel loads these envs in memory before running the exporter. A waited run deletes the secret datasets in `finally`; `--no-wait` keeps them because the kernel still needs them.

The build must still fail/skip loudly if limiter env is missing. Direct provider calls or local limiter fallback are not allowed for the related-chain Gemma audit. For pgvector related mode the encrypted runtime payload also carries the personalization Supabase URL/secret and selected embedding key env, because the Kaggle exporter must upsert changed search documents/vectors before calling the backend-only related RPC. The kernel copies only browser-safe URL/publishable-key values into `PUBLIC_*` for Astro; it must never expose `PERSONALIZATION_SUPABASE_SECRET_KEY` or service-role keys to the static bundle.

Before claiming the authorized-search browser gate, run:

```bash
python3 scripts/check_authorized_search_readiness.py --env-file .env --probe-edge --probe-yandex-provider --strict
```

Without probes it gives a redacted env-only status and is safe in local operator shells.

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
