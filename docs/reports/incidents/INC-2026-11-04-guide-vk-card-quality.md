# INC-2026-11-04 Guide VK schedule card reduced to 320×400

Status: open
Severity: sev3
Service: guide visual digest / VK photo upload
Opened: 2026-11-04
Closed: —
Owners: Codex
Registry: inc_f5d0a79f3a7ffa6073158f3c
Related incidents: inc_9b10802ccbe05795372dd983 (promo spacing)

## Summary and impact

Readers of edition 328 saw blurred schedule text in VK. The renderer produced
1080×1350 JPEG (181054 bytes), but the published photo's largest size was
320×400. This is a mechanical media transport problem, not event semantics.

## Evidence and timeline (UTC)

- 07:05: guide light run 11617 skipped because the heavy gate was occupied.
- 08:50–08:51: a separate visual catch-up built edition 328 from saved
  occurrences 711 and 699. It did not scan the newly registered sources.
- 08:50:58 and 08:51:01: VK returned empty photo results for the original
  JPEG and first reduced variant. The 44952-byte second variant succeeded.
- 09:00:38: published wall post became https://vk.com/wall-238875824_378;
  photo-238875824_457239682 exposes maximum size 320×400.
- Investigation: the same 1080×1350 pixels re-encoded as full JPEG 4:4:4
  failed, while PNG (505256 bytes) uploaded successfully through the normal
  bytes uploader. Asset receipt: photo868977531_457264569.

Retained evidence:
`/home/dev/artifacts/events-bot-new/20261104T093210Z-guide-digest-vk-quality-20261104/`
contains original-render.jpg, vk-published.jpg, vk-photo-evidence.json,
upload-probe-receipt.json and the exact bounded inspection/probe scripts.

## Root cause and correction

The existing upload fallback reduced pixel dimensions until VK accepted the
photo, sacrificing schedule legibility. Full-resolution PNG is accepted for
this exact card. Convert VK schedule payloads to PNG before the existing
uploader, and make its multipart MIME/extension match the attempt's format.
No new retry, queue, flag, mode, provider or schedule is needed.

## Automation contract

### Treat as regression guard when

Changing guide visual VK media serialization or the shared bytes uploader.

### Affected surfaces

`guide_excursions/visual_digest.py`, `main.upload_vk_photo_bytes`, VK wall photos.

### Mandatory checks before closure or deploy

- Distinguishing test: the actual VK publication boundary receives a PNG
  card at 1080×1350 and attaches its accepted asset to the wall request.
- Shared VK upload retry/source-image tests still pass.
- Live provider readback confirms the accepted PNG asset retains full size.
- Required CI and merged-main release/ancestry checks pass before deployment.
- Read back the affected normal publication path after deployment; a unit
  test or accepted asset alone does not prove deployed future behavior.

### Required evidence

Test results, immutable deployed SHA/image, full-size VK photo readback and
the original wall-post identity. Preserve existing publication receipts.

## Related observations and follow-up

New sources 23–30 have not been scanned yet. The next normal light slot is
13:20 Kaliningrad; owner-requested source-by-source import control is armed
for 14:20. Missing new guides in edition 328 does not mean no future tours.
Afisha repost is prepared for 13:26 local. Announcements activity has a
three-day cadence and last posted November 3; November 4 is not eligible.
Do not duplicate posts or collapse these independent schedules.


## 2026-11-15: returned VK wall owner differs from requested wall

- Incident registry: `inc_f5d0a79f3a7ffa6073158f3c`. Status: open; new-source acceptance and source-link repair are ongoing.
- Live `wall.get(owner_id=-237389053)` returned post `3892` with `owner_id=-211429559`, and personal posts owned by `41501067` and `408521495`. The scanner constructed URLs using the requested wall, so `wall-237389053_3892` did not resolve; `wall-211429559_3892` resolved with matching text through ordinary service API readback.
- This is a mechanical source identity error, not a semantic/event classification decision. Use the returned signed item owner when constructing source URLs; retain the requested owner only for older payloads without that field. No extra crawler, retry, state, source-date change or semantic bypass is needed.
- Regression: the actual cross-community case and a personal-owner case fail before the fix; both pass afterwards, alongside unchanged group/user fallback controls. Eight focused AST/runtime contract tests pass. These tests intentionally avoid notebook bootstrap and external providers.
- Evidence: retained `new-guide-source-post-links-check-20261115.log`, `new-guide-wall-owner-check-20261115.log` and `guide-recover-exact-1d1d54f10511-20261115.log` in `/home/dev/artifacts/events-bot-new/20261104T093210Z-guide-digest-vk-quality-20261104`.
- Release and product verification remain outstanding at this entry: merge with successful required CI, preserve actual runtime ancestry, deliver the corrected source used to generate the next notebook, then verify canonical source links and occurrences through the ordinary new-registry scan. Do not publish the previously frozen old-only issue 346 or count source variants as independent tours.

### Owner-link fix release, 15 November 2026

PR 992 passed all four required CI checks and merged as
`2fb79197ff45920efe03d55d2805a27b3934b515`. Events-only cutover deployed
`runcoveer/events:guide-vk-owner-20261115`, immutable image
`sha256:8ba755d32a58be1a48c090c158fab2a419976b6716b657b55e1a8e59041767e2`.
The prior actual runtime `944f34175003cabc34edc90e2b6b37a182cf76eb` is an
ancestor; Dockerfile and dependency inputs are unchanged. All 4,497 tracked
files matched exact main. Network-isolated startup with a fresh separate
database returned HTTP 200, `ok=true`, `issues=[]`; production readiness
then returned `ready=true`, `db=ok`, with scheduler jobs healthy. Effective
environment, data mounts and Kotopogoda container identity were preserved.
Rollback is immutable image
`sha256:a30316ba3d73ea336f7c86280acc1586b01bc3c264ee13195c42ef7711a9f1a1`;
original Compose and cutover receipt remain in the exact release directory.

Retained evidence: `guide-owner-release-build-20261115.log`,
`guide-owner-preflight-20261115.log`, `guide-owner-cutover-20261115.log`,
`guide-owner-live-health-20261115.json` in the incident artifact directory.
The completed preflight container/database and original fix worktree were
removed. The ordinary full-registry catch-up has started locally with
81 enabled sources and automatic publication disabled. Remote completion,
source-level acceptance and corrected product publication are separate
outstanding actions; this release entry does not close them. The first
one-off helper exited before submission due to Python import path; the
corrected invocation uses the existing `/app` module path.
