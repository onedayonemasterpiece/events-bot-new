---
name: static-site-kaggle-builder
description: Use in events-bot-new when running, debugging, or handing off the KenigEvents Astro static-site generator on Kaggle CPU after Smart Update; triggers include StaticSiteBuilder, static_site_build outbox job, preview-*-kaggle builds, site_source.tarball, Kaggle status dataset for static pages, Node 22 on Kaggle, or publishing generated event pages from Kaggle artifacts.
---

# Static Site Kaggle Builder

Use this skill only in `/home/dev/projects/events-bot-new`.

## Canonical files

- Runner: `scripts/run_static_site_builder_kaggle.py`
- Kaggle kernel: `kaggle/StaticSiteBuilder/static_site_builder.py`
- Kernel metadata: `kaggle/StaticSiteBuilder/kernel-metadata.json`
- Data export: `site/scripts/export-production-preview-data.py`
- Page-class contract: `site/scripts/static-site-page-classes.v1.json`
- Page-class Astro adapter: `site/scripts/page-class-build-filter.mjs`
- Astro site: `site/`
- Outbox handoff: `models.py` `JobTask.static_site_build`, `main.py` `job_static_site_build_kaggle`
- Docs: `docs/features/static-site-pages/astro-preview.md`

## Required pattern

Reuse the existing events-bot Kaggle infrastructure:

1. Create a unique private input dataset per run, not a fixed mutable dataset.
2. Push with `video_announce.kaggle_client.KaggleClient.push_kernel(...)`.
3. If running from Fly/production DB, create a status dataset with `create_kaggle_run_config(...)` and `create_kaggle_status_dataset(...)` and include it in `dataset_sources`.
4. Let `kaggle_status_client` emit internal progress from the kernel; do not accept opaque `ERROR`/`FAILED` without logs/status evidence.
5. Use the resource lease key `static_site:builder` for production status-aware runs.
6. Publish previews only from the checked downloaded Kaggle artifact with
   `--publish-preview`; never upload a locally generated `site/dist` tree.
7. Use repeatable preview-only `--page-class` values for focused runs. A
   production candidate must remain `all`/full-catalog.
8. Read page-class names from the versioned contract; do not duplicate the
   allowlist in a caller, MCP facade or second runner.

## Known Kaggle pitfalls already solved

- Kaggle does not mount arbitrary copied repo folders under `/kaggle/src`; pass the site as dataset payload.
- Do not name the source payload `site_source.tar.gz`: Kaggle auto-extracts archive-looking dataset files and can break Astro dynamic route filenames. Use `site_source.tarball` with gzip tar content.
- `/kaggle/src` is read-only at runtime; extract the site under `/tmp/kenigevents-static-site`.
- Kaggle CPU currently ships Node 20; Astro requires Node `>=22.12.0`. The kernel installs local `node@22.12.0` and prepends it to `PATH` before build/check.
- Keep output small: only `<build_id>.tar.gz`, `static_site_build_result.json`, and the kernel log should remain in `/kaggle/working`.
- For CDN-enabled focus-group previews, pass `PUBLIC_ASTRO_ASSET_BASE_URL=https://static.kenigevents.ru/{buildId}` or runner `--astro-asset-base-url`, and pass `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru` / runner `--asset-base-url` after verifying `scripts/migrate_static_media_to_cdn_bucket.py --active-on <date> --apply` has mirrored referenced `/p/...` media into bucket `kenigevents.ru`. Calendar CTA links should resolve to `https://static.kenigevents.ru/ics/<event_id>.ics` after deploy.
- For `--gemma-related-verify`, do not rely on Kaggle UI Secrets. The runner creates encrypted split private datasets (`secrets.enc` + `fernet.key`/`fernet.keys`) with only the Google key env and Supabase limiter env, the kernel loads them in memory, and a waited run deletes them afterwards.
- If dataset status returns a transient 403 immediately after create, poll; if it persists, inspect whether Kaggle auto-extracted/rejected the payload.

## Manual verified run

Latest verified manual run on 2026-06-28:

```bash
set -a; source .env; set +a
python scripts/run_static_site_builder_kaggle.py \
  --db artifacts/codex/static-site-builder/prod-db-20260628.sqlite \
  --limit 50 \
  --current-date 2026-06-28 \
  --build-id preview-20260628-event-pages-prod50-kaggle-v44 \
  --timeout-minutes 45 \
  --poll-interval 30 \
  --download-output
```

Evidence:

- Kaggle status: `COMPLETE`
- Result: `artifacts/codex/static-site-builder/output-preview-20260628-event-pages-prod50-kaggle-v44/static_site_build_result.json`
- Archive: `artifacts/codex/static-site-builder/output-preview-20260628-event-pages-prod50-kaggle-v44/preview-20260628-event-pages-prod50-kaggle-v44.tar.gz`
- Event count: `50`
- `npm run check:preview`: passed inside Kaggle

This manual run had no production callback env, so status dataset creation was intentionally skipped. A production/Fly run must pass `--status-db /data/db.sqlite` and a callback URL.


## Current vector/Gemma verified run

Latest checked/published run on 2026-06-28:

```bash
python scripts/run_static_site_builder_kaggle.py \
  --db artifacts/codex/static-site-builder/prod-db-20260628.sqlite \
  --limit 50 \
  --current-date 2026-06-29 \
  --build-id preview-20260628-event-pages-v46d-vector-gemma-kaggle \
  --export-in-kaggle \
  --related-cache artifacts/codex/static-site-builder/event_related_chain_cache_v46d_gemma_tomorrow.json \
  --gemma-related-verify \
  --gemma-related-model models/gemma-4-26b-a4b-it \
  --gemma-related-key-env GOOGLE_API_KEY4 \
  --gemma-related-max-anchors 50 \
  --asset-base-url https://static.kenigevents.ru \
  --astro-asset-base-url 'https://static.kenigevents.ru/{buildId}' \
  --ics-base-url https://static.kenigevents.ru/ics \
  --timeout-minutes 90 \
  --poll-interval 30 \
  --download-output
```

Evidence:

- public preview: `https://kenigevents.ru/preview-20260628-event-pages-v46d-vector-gemma-kaggle/__preview/`;
- Kaggle result: `ok=true`, `event_count=50`;
- encrypted secret split datasets were used because API-started kernels cannot rely on Kaggle UI Secrets; waited runs delete those datasets afterwards;
- related-chain cache: `event_vector_related_chain_v2`, `local_tfidf_sparse_v1`, Gemma 4 26B partial audit `45` provider calls via Supabase limiter, cache rerun `provider_calls=0`;
- public Playwright regression: `artifacts/codex/static-site-builder/playwright-v46d-public-check.cjs`.

## Preview and production handoff

Both profiles use one rail: immutable data + exact SHA → Kaggle build/check →
downloaded hash-bound artifact → trusted host-side Object Storage publisher.
They target the same `kenigevents.ru` bucket. Preview publishes create-only to
`/<buildId>/` and verifies `/<buildId>/__preview/`; production candidates use
the protected `/_review/<token>/` candidate prefix and optional atomic root
promotion. Bucket credentials stay on the host, not in Kaggle.

For a focused preview add, for example:

```bash
--profile preview --catalog-mode slice --page-class date \
--download-output --publish-preview
```

Available page classes are `event`, `date`, `weekend`, `collection`,
`personal`, `focus`, `partner`, and `lab`. Direct `npm run deploy:preview` is
retired.

Enable with `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`. Smart Update enqueues one coalesced outbox task:

- task: `static_site_build`
- key: `static_site_build:prod`
- delay: 15 minutes after the latest event update
- handler: `job_static_site_build_kaggle`

The handler calls the runner with the immutable production projection,
`--status-db /data/db.sqlite`, the Fly callback
`/internal/kaggle/run-event`, and `--download-output`. Secret-candidate upload
is controlled separately by `ENABLE_STATIC_SITE_SECRET_PUBLISH`; root
activation remains a distinct atomic promotion gate.

## Verification checklist

- `python3 -m py_compile kaggle/StaticSiteBuilder/static_site_builder.py scripts/run_static_site_builder_kaggle.py main.py models.py`
- Runner output shows dataset ready and kernel dataset sources matched.
- Kaggle log includes Node 22 install when CPU image reports Node 20.
- `static_site_build_result.json` has `ok=true`, expected `build_id`, and expected `event_count`.
- Archive contains `__preview/`, `sobytiya/`, `segodnya/`, `zavtra/`, `vyhodnye/`, `data/discovery/`, `sitemap.xml`, `robots.txt`.
- For production status-aware runs, verify `kaggle_run_ledger` has `kernel_started`, `alive`, and terminal `report_written` events.
