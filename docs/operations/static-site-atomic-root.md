# Static-site atomic root: two buckets + Yandex ALB

Status: **implemented in code, default OFF; infrastructure and live cutover do
not exist yet**. This document is the only production runbook for the atomic
root publisher. It does not authorize DNS or production changes.

## Safety model

The canonical website root uses two complete, page-only Object Storage buckets:

```text
kenigevents-root-blue  (complete tree) ─┐
                                       ├─ Yandex ALB backend group ─ TLS/SWS ─ kenigevents.ru
kenigevents-root-green (complete tree) ─┘

kenigevents.ru (existing bucket) = media `/p/*`, stable `/ics/*`, review links
```

Exactly one root backend normally has positive weight. A release is extracted
from the hash-checked `production_root` archive, and every manifest file is
rechecked locally for path, SHA-256, size and MIME. The publisher then:

1. reads ALB inventory and identifies the active and inactive slots;
2. proves the active bucket is a complete manifest-bound tree;
3. writes/reuses files and removes stale keys **only in the inactive bucket**;
4. reads every inactive object back and proves exact inventory/hash/size/MIME;
5. enables the new complete backend before disabling the old complete backend;
6. smokes stable `/`, `/segodnya/`, `/poisk/` and
   `/manifest.webmanifest` against exact artifact hashes;
7. on smoke failure, restores the former backend weights and records
   `rolled_back`.

The bounded overlap during ALB convergence can serve the old complete tree or
the new complete tree only. It cannot serve a directory copied halfway. There
is no browser `current.json`, no sequential copy into the active bucket and no
mutation of the separate media/stable-ICS bucket.

The atomic JSON receipt defaults to
`/data/static_site_builder/atomic-root-receipt.json` and records the Yandex
operation IDs, verified control-plane revision, current and previous release
identities, inactive cleanup counts and rollback result. Current/previous trees
are not GC targets. The local lock file serializes Fly-side invocations; ALB
inventory and final weights remain the external source of truth for crash
adoption/idempotent retry.

## Required artifact gates

Promotion refuses an archive unless `static-release-manifest.json` has the
exact build/run/repo/snapshot identity and all of these checks are `ok`:

- `astro_build`, `template_matrix`, `production_contract`, `catalog_parity`;
- `fixture_isolation`, `canonical_and_indexing`, `tree_hashes`;
- `browser_visual` from the root-form Chromium release gate.

The route gate requires the home, Today/Tomorrow/Weekend, exhibitions,
festivals, Popular, Search, Personal, clubs, partnership, free collection,
robots, sitemap and PWA manifest files. The PWA gate binds MIME, root
`id/scope/start_url`, standalone mode and local PNG 192/512 icons. Root keys
under `_review/`, `_static/`, `ics/` and `p/`, plus release-control JSON, are
unexpressible.

The inventory and smoke set also require `/fokus-gruppa/priglashenie/` and
`/fokus-gruppa/manifest.webmanifest`. The focus manifest is separately bound
to `id=/fokus-gruppa/pwa`, `scope=/` and
`start_url=/fokus-gruppa/priglashenie/?launch=pwa`. This prevents an otherwise
complete root release from breaking the already installed focus-group app.
Root HTML and manifests remain short-revalidated network resources; immutable
long caching is restricted to content-hashed assets, so a stable URL does not
require a new PWA installation for every release.

## One-time infrastructure setup (not yet executed)

Do this through an reviewed Terraform/YC change, save before/after JSON under
ignored `artifacts/codex/`, and keep DNS on the existing origin until every
pre-DNS gate below passes.

1. Create two new buckets with versioning, encryption, anonymous listing off
   and public object read required by an ALB bucket backend. Do not place media,
   stable ICS, review candidates or control objects in them. Bucket names must
   be different from the existing shared asset bucket.
2. Before the ALB is public, bootstrap **the same fully checked production
   archive** into both buckets with each object's `sha256` metadata, exact MIME
   and cache policy, then verify its manifest exactly. Bootstrap is a
   one-time maintenance action while neither bucket is a production origin;
   after DNS cutover all writes must use this publisher's inactive-only path.
3. Create one HTTP ALB backend group containing exactly two Object Storage
   backends, with stable names such as `root-blue`/`root-green`. Start with
   weights `100/0`; do not configure session affinity. Yandex documents that
   Object Storage bucket backends are supported by HTTP backend groups and that
   backend weights distribute requests proportionally:
   <https://yandex.cloud/en/docs/application-load-balancer/concepts/backend-group>.
4. Create the HTTP router, `kenigevents.ru` virtual host, TLS listener and
   Certificate Manager binding. Route `/` to only this backend group. Do not
   create a route or origin rewrite through `current.json`.
5. Attach a Smart Web Security profile to the virtual host, with Smart
   Protection, WAF and an Advanced Rate Limiter policy approved against a
   measured baseline. Keep route-level security enabled. Yandex documents the
   virtual-host attachment and ARL/WAF model here:
   <https://yandex.cloud/en/docs/smartwebsecurity/operations/host-connect> and
   <https://yandex.cloud/en/docs/smartwebsecurity/>. Also configure ALB
   autoscaling, logging/Monitoring alerts and appropriate L3/L4 DDoS protection.
6. Give the runtime identity only Object Storage read/write/delete/list on the
   two root buckets and only ALB backend-group read/update plus operation-read.
   Keep S3 keys in Fly secrets; never put them in Kaggle data, logs or Git. The
   existing shared asset bucket credentials remain separate.
7. Capture the real folder, backend-group, backend and bucket identities in Fly
   secrets/config. Do **not** enable apply mode yet.

Inventory must show exactly two configured bucket backends. Extra backends,
wrong bucket mapping, no positive backend, incomplete active manifest, missing
bucket or missing credentials fail closed.

For compact egress, inventory/reuse checks use listing plus object metadata
(size, MIME, publisher SHA) rather than downloading the active tree. The newly
staged inactive tree is downloaded exactly once for the mandatory byte/hash
readback before it receives traffic; page media and stable ICS are not copied
into either root bucket.

## Configuration

All values remain commented/default-off in `.env.example`. The promotion path
also requires the existing secret-candidate publication to be enabled, so
visual review remains available independently of root outcome.

```text
ENABLE_STATIC_SITE_ROOT_PROMOTION=0
STATIC_SITE_ROOT_PROMOTION_MODE=plan        # plan | apply
STATIC_SITE_ROOT_BLUE_BUCKET=kenigevents-root-blue
STATIC_SITE_ROOT_GREEN_BUCKET=kenigevents-root-green
STATIC_SITE_ROOT_BLUE_BACKEND=root-blue
STATIC_SITE_ROOT_GREEN_BACKEND=root-green
STATIC_SITE_ROOT_SHARED_ASSET_BUCKET=kenigevents.ru
STATIC_SITE_ROOT_ALB_BACKEND_GROUP_ID=<id>
STATIC_SITE_ROOT_YC_FOLDER_ID=b1g5tck18cgqtjb7rn3s
STATIC_SITE_ROOT_PUBLIC_BASE_URL=https://kenigevents.ru
STATIC_SITE_ROOT_RECEIPT_PATH=/data/static_site_builder/atomic-root-receipt.json
STATIC_SITE_ROOT_SCRATCH_DIR=/data/static_site_builder/atomic-root-tmp
STATIC_SITE_ROOT_YC_CLI=/home/dev/yandex-cloud/bin/yc
STATIC_SITE_ROOT_YC_ENDPOINT=https://storage.yandexcloud.net
STATIC_SITE_ROOT_YC_REGION=ru-central1
STATIC_SITE_ROOT_YC_ACCESS_KEY_ID=<secret>
STATIC_SITE_ROOT_YC_SECRET_ACCESS_KEY=<secret>
```

`ENABLE_STATIC_SITE_ROOT_PROMOTION=0` means `_finish_static_site_candidate`
does not import or call the root publisher. `plan` performs archive and live
inventory validation and writes a plan receipt, but performs no Object Storage
write/delete and no ALB update. `apply` is the only mutating mode.

## Pre-DNS plan and acceptance

From a clean main-reachable checkout, with a real checked result directory:

```bash
export PATH="/home/dev/yandex-cloud/bin:$PATH"
python3 static_site_atomic_root.py \
  --mode plan \
  --archive /data/static_site_builder/output-<build>/production-root.tar.gz \
  --result /data/static_site_builder/output-<build>/static_site_build_result.json
```

Required before any DNS change:

- plan receipt says `planned` or `planned_noop` with exact current/target;
- authenticated listing of each root bucket equals its manifest and has no
  `p/`, `ics/`, `_review/` or control JSON;
- direct ALB test host returns exact hashes/MIME for stable smoke routes;
- Chromium root gate and Android PWA install smoke pass through the ALB host;
- SWS/ARL/WAF logs and ALB error/latency/RPS alerts are visible;
- a forced smoke failure demonstrates weight rollback;
- `INC-2026-07-19` lease/status regression suite and `INC-2026-07-08` dual-disk
  capacity/write probe pass.

Only after owner approval may an operator set `PROMOTION_MODE=apply`, enable
the flag for one controlled build and later change DNS. This implementation
does not create resources, deploy, or change DNS.

## Rollback and crash recovery

- Failed archive/inventory/upload/readback: ALB is untouched; active remains
  current.
- Crash after target weight becomes positive: both buckets are complete. Retry
  recognizes the one desired complete tree and converges weights without an
  upload.
- Stable smoke failure: publisher converges weights back to the former backend
  and records `rolled_back`; the checked review candidate remains current for
  diagnosis and the failed root tree remains inactive.
- Rollback-control-plane failure: fail loudly, stop automated publication and
  inspect ALB weights plus both complete manifests before any manual action.
- Never delete either bucket or run generic `sync --delete` against an active
  slot. Never GC the current/previous identities in the durable receipt.

## Regression commands

```bash
/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_static_site_atomic_root.py \
  tests/test_static_site_release.py \
  tests/test_static_site_build_handoff.py
```

Incident contracts:

- `INC-2026-07-19-static-site-stale-builder-lease`: root publication must not
  bypass exact-run host reconciliation or strand the builder lease.
- `INC-2026-07-08-prod-root-overlay-disk-full`: extraction stays under bounded
  configured persistent scratch, is removed after the attempt, and must never
  be redirected to unbudgeted `/tmp`.
