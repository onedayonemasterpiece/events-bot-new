# INC-2026-09-27 Static CDN TLS Regression

Status: open
Severity: sev1
Service: `static.kenigevents.ru`, static-site event media and owner review
Opened: 2026-09-27
Closed: —
Owners: static-site runtime, Yandex Cloud operations
Related incidents: `INC-2026-09-01-yandex-storage-cdn-media-outage`, `INC-2026-08-03-cherryflash-cdn-tls-retry-storm`
Related docs: `docs/features/static-site-pages/cdn-asset-delivery.md`, `docs/features/static-site-pages/home-owner-review-20260927.md`, `docs/operations/release-governance.md`

## Summary

During final R9 owner-review acceptance, strict browsers could not load event
media from `static.kenigevents.ru`. The public edge presented a certificate
for `*.yccdn.cloud.yandex.net` rather than a certificate whose SAN covers
`static.kenigevents.ru`. This reproduces the TLS symptom recorded in
`INC-2026-09-01-yandex-storage-cdn-media-outage`.

The R9 application itself is independently healthy: exact-main Fly runtime,
the noindex owner-review HTML surface and the authenticated production Live
Search canary all pass. The CDN failure therefore remains a separate production
delivery incident rather than a reason to replace HeroTalk mosaics with
text-only UI.

## User / Business Impact

- Event posters and HeroTalk mosaics requested from `static.kenigevents.ru`
  fail in strict browsers with `ERR_CERT_COMMON_NAME_INVALID`.
- R9 Home structure is reviewable through the Fly owner-review route, but its
  image-bearing visual acceptance cannot be closed while the CDN is broken.
- The normal Object Storage preview publisher is also unavailable: the currently
  configured static-site writer returns `AccessDenied` on `PutObject`.
- Search interaction/layout remains reviewable because its shell does not
  require poster delivery before sign-in; event-card media is affected by the
  same CDN outage once cards render.

## Detection

- Browser review of the public R9 Fly artifact showed repeated failed image
  requests with `net::ERR_CERT_COMMON_NAME_INVALID`.
- A strict `curl` request independently reproduced TLS error 60.
- `openssl s_client` showed the served certificate:
  - subject CN `*.yccdn.cloud.yandex.net`;
  - SANs `*.yccdn.cloud.yandex.net`, `yccdn.cloud.yandex.net`;
  - no SAN for `static.kenigevents.ru`.
- DNS still maps `static.kenigevents.ru` to
  `d1dbb74e06652ac7.topology.gslb.yccdn.ru`.

## Timeline

- 2026-09-27 14:15 UTC — public R9 Fly owner-review loads, but poster/mosaic
  requests fail with certificate-name mismatch.
- 2026-09-27 14:17 UTC — strict curl and OpenSSL independently confirm the
  wrong public edge certificate.
- 2026-09-27 14:17–14:20 UTC — direct current-bucket HTTP/Object Storage reads
  are denied; available runtime storage identity receives 403 for
  `kenigevents.ru`; the representative key is absent in legacy
  `kenigevents`.
- 2026-09-27 14:14–14:25 UTC — exact-main Fly runtime
  `a9383baa3da85081a791bf234b4ec1a1b94458b1` is healthy, R9 review root and
  `/poisk/` return 200, QA/service routes return 404, and real Live canary
  run `36325807189` passes with `search_events`, 4 cards, 7.08 s and an
  explicit session release.

## Root Cause

The immediate failure is the public CDN edge serving a certificate that does
not cover `static.kenigevents.ru`.

The underlying control-plane cause is still open. Current evidence also shows
credential/access drift for the static Object Storage path, but it is not yet
proven that the storage IAM state caused the certificate regression. Do not
collapse these into one causal claim until Yandex Cloud control-plane evidence
is available.

## Contributing Factors

- The previous 2026-09-01 follow-up to add strict external CDN SAN/HTTP alerts
  remained open.
- Existing Fly/GitHub static-site S3 credentials no longer prove write access
  to `kenigevents.ru`; review publication therefore failed over to a bounded
  Fly-served artifact.
- No currently available non-interactive identity exposes Certificate Manager /
  CDN control-plane mutation for this recovery. The Postbox service identity is
  deliberately not repurposed.

## Automation Contract

### Treat as regression guard when

- changing `static.kenigevents.ru` CDN, certificate, origin or DNS;
- changing Object Storage credentials/roles for `kenigevents.ru`;
- changing static-site preview/secret-candidate publication;
- changing event-card or HeroTalk media delivery.

### Affected surfaces

- CDN hostname `static.kenigevents.ru`;
- Yandex CDN resource previously recorded as `bc8rani5q2j4yfpl7oge`;
- bucket `kenigevents.ru`, poster prefix `p/` and owner-review publication;
- Home HeroTalk/PageEndHeroTalk and canonical EventCard media;
- static-site release/review transport.

### Mandatory checks before closure or deploy

- public TLS presents a certificate with SAN `static.kenigevents.ru` on
  10/10 independent handshakes;
- 20/20 strict HTTPS GETs for representative poster/media objects return 200
  without TLS bypass;
- bounded Object Storage PUT/HEAD/GET/DELETE canary passes using the intended
  least-privilege static-site writer;
- normal owner-review publication to the canonical site domain succeeds without
  `AccessDenied`;
- Home R9 browser acceptance shows HeroTalk and page-end mosaics with no failed
  CDN image requests and no horizontal overflow;
- Search browser shell remains clean and the production Live canary remains PASS;
- deployed/runtime SHA is reachable from `origin/main`.

### Required evidence

- strict OpenSSL/curl TLS series;
- Object Storage canary receipt with credential alias only, no secrets;
- public Browser Bridge network/console acceptance for Home and Search;
- exact main/deployed SHA and Fly health;
- production Live canary receipt.

## Immediate Mitigation

- Added the bounded Fly owner-review fallback for `preview-review-*` artifacts.
  It serves only the checked preview tree from persistent `/data`, returns
  noindex/no-cache headers and rejects `__preview`, `lab`, `_review`, dot
  paths and traversal.
- Kept the R9 HeroTalk mosaic contract intact; the application does not hide the
  CDN incident by silently turning event scenes into text-only scenes.
- Confirmed the Search shell and the authenticated Live/vector/verifier path
  independently of poster transport.

## Corrective Actions

- Pending Yandex Cloud control-plane recovery: re-bind/activate the issued
  Certificate Manager certificate for the exact CDN resource and purge the
  affected resource, following the successful 2026-09-01 recovery pattern.
- Pending storage IAM recovery: restore the intended scoped static-site writer
  and prove it with the bounded canary before returning owner-review publication
  from Fly fallback to Object Storage.

## Follow-up Actions

- [ ] Yandex Cloud operations: restore the exact certificate binding and collect
  10/10 TLS + 20/20 strict GET evidence.
- [ ] Static-site operations: restore least-privilege `kenigevents.ru`
  read/write access for the intended static-site publication identity.
- [ ] Add an external readiness alert that checks both the
  `static.kenigevents.ru` SAN and a representative strict HTTPS object GET.
- [ ] Add a bounded storage-writer readiness probe before preview/secret-candidate
  publication so AccessDenied is diagnosed before a full build/upload attempt.

## Release And Closure Evidence

- current exact main / deployed Fly SHA:
  `a9383baa3da85081a791bf234b4ec1a1b94458b1`
- Fly machine: started, 1/1 checks passing; `/healthz` = 200
- R9 Fly owner-review:
  - `/preview-review-20260927-r9/` = 200
  - `/preview-review-20260927-r9/poisk/` = 200
  - `/__preview/`, `/lab/`, `/_review/` = 404
  - public review responses carry noindex/no-cache
- R9 Search browser shell: 0 console errors, 0 failed network requests before
  event media is requested; desktop microphone button observed at 88×88.
- Live canary run `36325807189`: PASS, `gemini-3.8-live`,
  `search_events`, 4 cards, `has_more=true`, 7080 ms,
  `session_released=true`.
- CDN closure evidence: **not yet satisfied**; strict TLS still fails.

## Prevention

Do not treat a green static build, CDN control-plane READY state or Fly health
as proof that public poster delivery works. Release/review readiness for image-
bearing surfaces requires strict external TLS/SAN and representative object GET
evidence.
