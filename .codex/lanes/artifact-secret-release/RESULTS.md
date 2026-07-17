# Artifact / secret-link release audit (R04, R06, R07, R08)

Date: 2026-07-17 UTC
Branch/base: `agent/static-site-production-pipeline-secret/artifact-secret-release` @ `2822a91d` (`origin/main`)
Scope: read-only audit and implementation handoff. No product code, bucket, CDN, DNS, root objects, or runtime flags were changed.

## Executive decision

The safe first release mode is **not** a root promotion. It is:

1. create and verify one immutable SQLite snapshot;
2. run a full-catalog **production-profile** export/build/check on Kaggle CPU;
3. return an immutable checked root-form artifact and rich release manifest;
4. derive/build a separately checked, noindex **secret-candidate view** from the same export and source SHA;
5. upload only that view to a new high-entropy immutable prefix with create-only writes;
6. return its unlisted bearer URL to the operator/requester;
7. leave `/`, root listings, root event pages, stable `/ics/*`, `current.json`, and rollback state untouched.

The existing `kenigevents.ru` Object Storage website cannot provide reader-atomic whole-tree root promotion. A JSON `current` pointer is not consumed by the website/CDN request path. Commits `62ba7110` and `b0307e32` improve recovery and writer concurrency, but still copy root keys one by one, so readers can observe a mixed release. **Do not clean-port or enable their root-mutating publisher as an atomic solution.** Root activation stays disabled until an edge/front-controller design makes one atomic pointer authoritative for reads.

The phrase “secret link” must be documented honestly: in the proposed first mode it is an **unlisted bearer URL, not authentication**. The current bucket additionally has anonymous listing enabled, which makes any random prefix enumerable and invalidates even the unlisted-link property. Secret-link publication is blocked until anonymous `list` is disabled while anonymous object `read` remains enabled, and this is verified through both the YC control plane and an anonymous S3 ListObjects request.

## Evidence reviewed

- Project instructions and `static-site-kaggle-builder` / `yandex-cloud-infra` skills.
- User attachment `pasted-text.txt` and its F1/F13 production-pipeline contract.
- `origin/docs/static-site-release-plan-20260717@8fecf7da`, especially:
  - `docs/features/static-site-pages/release-plan.md`;
  - `docs/features/static-site-pages/test-scenarios.md`;
  - release-readiness/context-recovery documents.
- Current main implementations:
  - `smart_event_update.py`, `main.py`, `models.py`;
  - `scripts/run_static_site_builder_kaggle.py`;
  - `kaggle/StaticSiteBuilder/static_site_builder.py`;
  - `site/scripts/export-production-preview-data.py`;
  - preview build/check/deploy scripts;
  - `tests/test_static_site_build_handoff.py` and `tests/test_static_site_public_gate.py`.
- Side implementation:
  - `62ba7110 feat(site): add safe production root promotion`;
  - orphan/local hardening commit `b0307e32 fix(site): harden production publisher recovery` (not reachable from the named remote side branch at audit time).
- Live read-only Yandex control-plane / HTTP / S3 observations on 2026-07-17.

The side publisher’s eight authored Node behavior tests were extracted to a temporary directory and passed `8/8`. This proves its lease/CAS/recovery unit contracts, **not** reader-atomic delivery.

## Current implementation gaps relevant to this lane

### Kaggle/build result

Current kernel is preview-only:

- forces `SITE_BASE_PATH=/<build_id>` and `PUBLIC_PREVIEW_BUILD_ID`;
- runs `build:preview` and `check:preview` only;
- archives `dist/<build_id>`;
- result JSON contains only `ok`, `build_id`, timestamps, archive, dist root and event count.

It does not produce the required repo/snapshot/run correlation, snapshot hash, output hash/size, max event revision, page/file counts, checks registry, related/vector revisions, failure class, freshness, or release manifest.

The runner copies the given DB path with `shutil.copy2`; the production handler currently gives it the mutable `db.path` (`/data/db.sqlite`). There is no SQLite backup snapshot, `quick_check`, snapshot SHA/size/max-update metadata, or verified snapshot manifest in this path.

### Full-catalog export/parity

The exporter defaults to a bounded preview slice (`--limit 50`) and labels output as such. The handler’s `5000` limit is only an upper bound, not a proof of the exact eligible catalog. `--limit 0` is not an all-catalog mode: current loop/return behavior yields an empty result. Add an explicit `--catalog-mode full` (or separate required positive full-catalog path), never overload zero.

`62ba7110` / `b0307e32` check that each event already present in `preview-events.json` has HTML/ICS/discovery files. They cannot prove:

- 100% of snapshot-eligible events were exported;
- zero ineligible rows leaked;
- no orphan event artifacts exist relative to an independent source ledger;
- linked occurrence parity (current exporter writes `other_date_ids=[]`);
- accepted age values survive source → export → HTML/JSON-LD;
- related/vector revision barrier freshness.

Production export must emit an independent catalog ledger from the same immutable snapshot: eligibility predicate/version, eligible IDs and source revisions/hashes, excluded IDs with bounded machine reason codes, max event update/revision, linked graph, age projection status, and counts. The production check compares the generated tree and sitemap against that ledger, not merely against its own exported event array.

### Manifest

The side manifest is a useful file inventory (`files[]`, per-file SHA/size/MIME/cache class, managed keys, stable ICS mapping), but is insufficient for the accepted `static_release_manifest_v1`. Missing/weak fields include:

- `run_id`, `snapshot_id`, snapshot SHA/size/created-at/quick-check/max event revision;
- exporter/data/template/schema versions;
- eligibility predicate and catalog-ledger hash/count parity;
- event/page/file counts by family;
- related/search/vector, transport, media, age and occurrence projection versions/revisions;
- named check results with versions/timestamps;
- archive/tree aggregate SHA and size;
- freshness/correlation evidence;
- immutable release/candidate prefix and publication mode;
- previous/rollback release (for future root release only);
- failure class on rejected results.

The manifest itself should be canonical-JSON hashed. Signing is optional only if artifact trust is otherwise rooted in the status ledger and trusted publisher; if a signature is claimed, specify the signing key boundary and verify it before upload.

## Live Object Storage/CDN audit

Read-only observed state:

- bucket: `kenigevents.ru`, 50 GiB limit;
- website index `index.html`, error `404.html`;
- versioning: `VERSIONING_DISABLED`;
- anonymous access: `read=true`, **`list=true`**, `config_read=false`;
- anonymous `https://storage.yandexcloud.net/kenigevents.ru?list-type=2` returned object keys;
- `https://kenigevents.ru/_static/releases/current.json` returned 404;
- main `https://kenigevents.ru/` is the Object Storage static website path;
- CDN resource `bc8rani5q2j4yfpl7oge` is for `static.kenigevents.ru`, with the website origin and query strings ignored; it is not a release-pointer router for canonical `kenigevents.ru` requests;
- no bucket version history exists to rescue overwritten root objects.

### Atomicity conclusion

Object Storage provides atomicity at an individual object write, not a transaction across hundreds/thousands of HTML/JSON/ICS/assets. The side publisher orders copies, leases the writer, verifies, restores on failure, and commits a pointer after copying. Those mechanisms prevent some concurrent-writer and failed-operation hazards, but during the copy interval readers may see new supporting files with old HTML, mixed HTML generations, or new root with old subordinate pages. The later pointer cannot repair that because readers never resolve through it.

Therefore:

- `ADD-BUILD-10 reader-atomic promotion` is **blocked by current delivery architecture**;
- root mutation and stable `/ics/*` mutation must remain disabled in this user-requested phase;
- rollback evidence for secret candidates is deletion/revocation/rotation of a never-current prefix, not a claim of production root rollback;
- native Object Storage versioning would improve recovery but still would not make a multi-object root overlay reader-atomic.

A future root design needs an authoritative read indirection, for example an edge/front controller that reads one strongly consistent/CAS-managed `current` release ID and internally maps every request to `releases/<id>/root/<path>`, with release-stamped assets and bounded pointer caching. CDN origin/DNS changes or sequential copies are not equivalent to this contract and require their own propagation-consistency proof.

## Secret-link threat model and required contract

### What it can guarantee after the prerequisite fix

Use at least 256 random bits from a CSPRNG (`randomBytes(32)`, base64url/hex), never an event ID, timestamp, build ID, hash prefix, or reusable user token. With anonymous bucket listing disabled, no root/sitemap/navigation reference, and no logs exposed publicly, the URL is impractical to guess. The prefix and every object under it are immutable/create-only; regeneration creates a new token/prefix.

Recommended shape (example only):

```text
https://kenigevents.ru/_review/<43-char-base64url-token>/
```

Operator state stores `sha256(token)` plus build/run/snapshot IDs, creation/expiry/revocation timestamps; plaintext is returned only where needed to deliver the link. The object key necessarily contains the plaintext token and remains visible to bucket administrators.

Secret-candidate HTML requirements:

- production page families and full catalog, but no lab/fixture routes;
- `meta robots=noindex,nofollow,noarchive,nosnippet` on every HTML file;
- global `meta name=referrer content=no-referrer` and `rel=noreferrer` for external navigation as defense in depth;
- candidate omitted from the root sitemap and every public listing/feed;
- no candidate sitemap submission; a prefixed `robots.txt` is not an origin robots policy and must not be presented as sufficient;
- auth/search/telemetry integrations disabled unless explicitly required for this review, so the bearer URL is not sent to unnecessary third parties;
- all internal links, assets, JSON and ICS stay within the same token prefix or point to already-public content-addressed CDN media; never write stable root `/ics/<id>.ics`;
- candidate objects use `Cache-Control: private, no-store` (privacy/hygiene, not access control) and exact MIME; immutable create-only write preconditions prevent replacement;
- no root canonical claiming the currently live root. Candidate canonical should be absent or self-prefix according to the explicit noindex candidate contract; the separately checked root-form artifact retains production canonicals for eventual release proof.

### What it cannot guarantee

This mode is not login, authorization, per-user ACL, DRM, or confidentiality after sharing. Anyone with the URL can forward it. It may leak through screenshots, clipboard history, browser history/sync, server/bucket access logs, extensions, support messages, Referer without the stated policy, or an accidental public link. `noindex` is a crawler instruction, not an access control and not a removal guarantee. `Cache-Control` is not access control. Deleting a prefix cannot recall downloaded content.

If actual confidentiality, named-user audit, expiry enforced at request time, or reliable revocation is required, use an authenticated gateway/edge authorization or a proven signed-CDN URL design. The current public Object Storage website is not that system.

### Hard preflight blocker

Before the first secret candidate upload:

1. change bucket anonymous access from `read=true,list=true` to `read=true,list=false` through a separately reviewed infra change;
2. verify anonymous ListObjects returns 403/AccessDenied while a known public object remains 200;
3. confirm preview deployment/scripts do not require anonymous list (they use authenticated AWS operations);
4. record the YC resource state and public probes in release evidence.

Until this passes, do not call the link secret or unlisted.

## Selective clean-port disposition

Do not merge the side branch wholesale.

| Side item | Disposition | Reason / clean-port change |
|---|---|---|
| `build-production.mjs` | **Adapt, not verbatim** | Keep explicit production mode, clean-tree/versioned source, QA-route exclusion, file hashing/MIME/cache inventory and automatic check. Replace `git` dependence inside Kaggle’s site-only payload with verified `repo_sha` from immutable build config. Do not string-rewrite `/segodnya/` HTML to fabricate root; use an explicit production-aware root route/build contract. Consume full-catalog/source-ledger metadata and emit the rich manifest above. Keep secret-candidate output separate from root-form artifact. |
| `check-production.mjs` | **Adapt and strengthen** | Keep symlink/path safety, per-file hash/size, no preview/noindex leakage in the root-form artifact, canonical/robots/sitemap checks, MIME metadata and exact tree inventory. Add independent eligibility/catalog ledger parity, orphan checks, linked graph symmetry/no-self/no-dangling, age parity, related/vector revision barrier, asset-host/budget checks, page-family counts and stable test IDs. Do not import publisher code solely for fixture assertions; move shared pure validators to a small side-effect-free module. |
| `deploy-production-yc.mjs` root copy/pointer/rollback | **Reject/supersede for this phase** | Sequential root copy is not reader-atomic. Lease/CAS/recovery are valuable writer-safety primitives but cannot satisfy reader atomicity. No root or stable ICS mutation is allowed now. |
| Pure validator / create-only upload helpers from publisher | **Clean-port selectively** | Reuse safe key/release ID validation, manifest validation, SHA/MIME public verification and conditional `If-None-Match:*` semantics in a new upload-only secret-candidate publisher. Use a token prefix and refuse every root/current/previous/lease/stable-ICS key. |
| `production-publisher.behavior.test.mjs` | **Split** | Preserve pure lease/CAS tests for future root architecture and validator/MIME tests. Do not label partial-copy recovery tests as `ADD-BUILD-10` atomicity. Add explicit negative test demonstrating the current root overlay can be mixed for readers and mark root promotion blocked. |
| Production-aware Astro diffs in `62ba7110` | **Reimplement on current main** | The branch predates subsequent UI/data work. Port only the mode abstractions (`preview`, root production, secret candidate) and route/SEO policy; resolve current-main changes file-by-file. Preview remains byte/behavior compatible and noindex. |

## Exact implementation file plan

Names can be adjusted during integration, but responsibilities must not be collapsed into the preview deployer.

### Snapshot / orchestration

- `static_site_release.py` (new pure/service module): snapshot IDs, SQLite backup API, `quick_check`, SHA/size/max-event revision, manifest records, failure classes and state transitions.
- `main.py`, `models.py`: correlate effectful Smart Update → coalesced/deferred job → snapshot/run/build/candidate; automatic and operator-requested builds use the same durable path; publication mode defaults to `secret_candidate`, root promotion flag defaults off.
- `scripts/request_static_site_build.py` (new operator CLI): on-demand enqueue/status using reason/correlation/idempotency key; never directly invokes Kaggle or writes the bucket.
- `scripts/run_static_site_builder_kaggle.py`: require snapshot manifest/hash, repo SHA, run/build/profile IDs, full catalog mode; unique dataset; validate downloaded bounded result/archive before handoff; no publisher credentials in Kaggle.
- `kaggle/StaticSiteBuilder/static_site_builder.py`: validate snapshot SHA/quick-check evidence, production export/build/check, separate secret-candidate build/check, aggregate hashes/counts/freshness/result; publication remains outside Kaggle.

### Export/build/check

- `site/scripts/export-production-preview-data.py`: explicit profile/catalog mode; independent eligibility/catalog ledger; full catalog; projection version/revision metadata. Preserve preview slice behavior.
- `site/src/lib/events.ts`, `site/src/layouts/EventLayout.astro`, `site/src/lib/seo.ts`, `site/src/pages/robots.txt.ts`, `site/src/pages/sitemap.xml.ts`, listing/event/brand links, and `site/src/pages/index.astro`: implement three explicit policies:
  - preview: prefixed, noindex, labs allowed;
  - production root artifact: root canonical/indexable, labs absent;
  - secret candidate: high-entropy prefix, noindex/no-referrer, production page families, labs absent, candidate-local links/ICS.
- `site/scripts/release-contract.mjs` (new pure module): safe paths, canonical manifest serialization/hash, MIME/cache policy, file walk/hash, catalog/tree checks shared without importing a mutating publisher.
- `site/scripts/build-production.mjs`, `site/scripts/check-production.mjs`: clean-port/adapt as above.
- `site/scripts/build-secret-candidate.mjs`, `site/scripts/check-secret-candidate.mjs` (new): same exported catalog/source SHA, token-prefix containment, noindex/referrer/no-root-link/no-stable-ICS checks.
- `site/scripts/deploy-secret-candidate-yc.mjs` (new upload-only publisher): generate/accept a CSPRNG token, hard preflight `anonymous list=false`, upload create-only under the one token prefix, verify every public object hash/MIME, write candidate receipt/state, return URL; prohibit root mutation by construction. It must not reuse `deploy-preview-yc.mjs`, because that script currently also writes stable root `/ics/<event_id>.ics`.
- `site/package.json`: add explicit production/candidate build/check and secret-candidate publish/plan commands; no `deploy:production` command until an atomic reader design is accepted.

### Tests / docs

- Extend `tests/test_static_site_build_handoff.py` and `tests/test_static_site_public_gate.py`.
- Add focused Python tests for snapshot/release state and on-demand enqueue.
- Add Node tests for production profile, candidate containment, rich manifest/tree/catalog parity and upload-only publisher.
- Update the canonical static-page README/release plan, Kaggle ops runbook, test-scenario inventory/E2E index/routes and `CHANGELOG.md`; import the release-doc branch canon rather than inventing a second checklist.

## Acceptance tests owned or materially affected by this lane

Tests must include the stable ID in the test name/metadata and must not overclaim evidence.

- `ADD-BUILD-07`: production root-form artifact is indexable/canonical/full-catalog; preview stays prefixed/noindex; secret candidate is production-family/full-catalog but noindex and prefix-contained; no lab/fixtures in either production or candidate.
- `ADD-BUILD-08`: snapshot SHA/manifest → unique Kaggle input → status/lease → checked root artifact and candidate artifact correlate one run/build ID; altered snapshot/artifact is rejected.
- `ADD-BUILD-09`: source eligibility ledger, generated tree, sitemap, manifest files/counts/hashes/projection versions and archive hash agree exactly; accepted age and linked graph checks included.
- `ADD-BUILD-10`: for this phase, assert failed candidate/upload/check leaves root/current/stable ICS byte-identical; immutable candidate rotation/revocation works. Reader-atomic root promotion remains blocked and must not be marked passed.
- `ADD-BUILD-11`: candidate/public object MIME, TLS, cache, asset host, ICS disposition and no external/raw storage leakage; secret prefix never appears in root sitemap/navigation.
- `ADD-BUILD-12`: snapshot/temp/Kaggle output/Object Storage quotas and retention preflight before work/upload; at least current candidate + audit evidence fit. Root last-good capacity remains future work.
- `ADD-BUILD-13`: missed/stale build reconciles to the newest snapshot and publishes one newer candidate; it never promotes root.
- `ADD-RELATED-01..04`: manifest carries expected/actual vector/search/related revisions and reverse-anchor coverage; provider failure preserves honest last-good/omits optional output and rejects stale verified labels.
- `ADD-OBS-01`: PII-free correlation Smart Update/request → outbox → snapshot → Kaggle → checks → manifest → candidate receipt; secret token is redacted/hashed in normal logs.

Required negative security tests:

- bucket list preflight true/public ⇒ publisher refuses;
- token <256 bits, unsafe prefix, reused prefix, or overwrite attempt ⇒ refuses;
- candidate HTML contains index/follow, Referer leakage policy, root internal event link, root stable ICS, lab/fixture URL, or appears in root sitemap ⇒ refuses;
- failed Kaggle/check/upload ⇒ no root/current/previous/stable-ICS operation;
- external-link request does not carry full bearer path under browser acceptance.

## Controlled real Kaggle CPU run plan (root promotion disabled)

Run only after all code/tests are on a clean pushed feature SHA and the anonymous-list blocker is fixed.

1. **Flags/gates**
   - keep production root activation/promotion flag absent/off;
   - set build publication mode to `secret_candidate` only;
   - automatic scheduler may be enabled only after the manual controlled run; start with the operator CLI;
   - publisher credentials stay on the trusted Fly/operator side, not in Kaggle;
   - do not use `deploy-preview-yc.mjs` because of its stable `/ics` writes.
2. **Snapshot**
   - create a SQLite backup snapshot from `/data/db.sqlite` through the new snapshot service;
   - verify `quick_check=ok` and record `snapshot_id`, SHA-256, size, created-at, max event revision/update and eligible count;
   - copy it to the per-run private Kaggle input only after hash verification.
3. **Run identity**
   - generate unique `run_id`, `production-...` `build_id`, and private Kaggle dataset;
   - bind exact pushed repo SHA, snapshot manifest SHA, expected catalog mode `full`, projection/version barrier and current Kaliningrad cutoff;
   - status DB/callback and `static_site:builder` lease are mandatory.
4. **Kaggle command shape after implementation**

   ```bash
   python scripts/run_static_site_builder_kaggle.py \
     --db artifacts/codex/static-site-prod-candidate/<run>/snapshot.sqlite \
     --snapshot-manifest artifacts/codex/static-site-prod-candidate/<run>/snapshot-manifest.json \
     --profile production-candidate \
     --catalog-mode full \
     --repo-sha <exact-40-char-sha> \
     --run-id <run-id> \
     --build-id production-<utc>-<sha8> \
     --export-in-kaggle \
     --related-mode <accepted-mode> \
     --status-db /data/db.sqlite \
     --status-callback-url <fly-internal-callback> \
     --timeout-minutes 90 \
     --poll-interval 30 \
     --download-output
   ```

   The runner should have no root-publish option. A missing profile/snapshot manifest/full-catalog flag fails closed.
5. **Artifact acceptance**
   - verify result JSON schema/bounds, repo/snapshot/run/build IDs, snapshot and archive hashes, counts, checks, revisions, freshness and status-ledger terminal event;
   - independently extract and run `check:production`, `check:secret-candidate`, manifest/tree/catalog parity and `git diff --check` on the exact SHA;
   - preserve bounded evidence under `artifacts/codex/...`, not git.
6. **Secret publication**
   - publisher preflights `read=true,list=false` anonymously;
   - generate the 256-bit review token outside Kaggle;
   - upload candidate objects create-only under exactly one `_review/<token>/` prefix;
   - verify all hashes/MIME and representative browser pages; confirm root, root sitemap, known root event, `current.json` absence/state, and stable `/ics` samples are unchanged before/after;
   - save a receipt containing token hash (not plaintext), build/run/snapshot/manifest hashes, prefix, verification summary and expiry/revocation state.
7. **Handoff/observation**
   - deliver only the unlisted URL through the approved Telegram review thread;
   - run mobile/desktop sample and full structural crawl from the candidate URL;
   - keep root promotion explicitly `not applied` in the report;
   - test revocation by deleting one disposable test candidate prefix or rotating to a new token, never by root rollback.
8. **Enable automation**
   - after the controlled run passes, enable effectful Smart Update → secret-candidate generation/publish only;
   - manual request uses the same durable job/state machine;
   - production root promotion remains off until the reader-atomic blocker is redesigned and separately accepted.

## Release/rollback status at end of this audit

- `origin/main` changes from this lane: none.
- Product code changed: no.
- Real Kaggle run: not run; implementation prerequisites are absent.
- Secret candidate uploaded: no; anonymous public listing is a blocker.
- Production promotion: not applied.
- Production root/DNS/CDN/bucket mutation: none.
- Rollback performed: none; no live state changed.
- Exact blocker: public Object Storage website with per-object reads, anonymous bucket listing enabled, versioning disabled, and no edge resolver consuming a release pointer.
