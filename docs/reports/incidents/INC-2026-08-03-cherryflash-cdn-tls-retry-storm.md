# INC-2026-08-03-cherryflash-cdn-tls-retry-storm CherryFlash CDN TLS Retry Storm

Status: mitigated
Severity: sev1
Service: CherryFlash `popular_review`, `static.kenigevents.ru`, production scheduler
Opened: 2026-08-03
Closed: —
Owners: video announce runtime / Yandex Cloud CDN operations
Related incidents: `INC-2026-04-27-cherryflash-missing-photo-urls`, `INC-2026-04-23-cherryflash-pre-handoff-loss`, `INC-2026-07-13-tg-media-downgrade-non-cdn-posters`
Related docs: `docs/features/cherryflash/README.md`, `docs/features/static-site-pages/cdn-asset-delivery.md`, `docs/operations/cron.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-08-03 the public CDN hostname `static.kenigevents.ru` began serving the
default `*.yccdn.cloud.yandex.net` certificate instead of its issued Certificate
Manager certificate. CherryFlash could no longer prefetch selected event posters,
Kaggle failed during render on a missing poster asset, and the ten-minute
`video_popular_review_watchdog` kept launching replacements because it had no
persisted failed-session retry cap.

## User / Business Impact

- The daily CherryFlash product did not publish.
- Production created 33 failed `popular_review` sessions (`#1005..#1041`) between
  07:44 and 13:27 UTC and repeatedly notified the operator.
- CDN image and ICS consumers using strict TLS were degraded.
- Repeated Kaggle launches consumed external capacity without changing the
  deterministic failure condition.

## Detection

- Operator reported an apparently endless sequence of `Видео не найдено` messages.
- Fly runtime mirror showed repeated `ClientConnectorCertificateError` for
  `static.kenigevents.ru`; Kaggle output showed
  `FileNotFoundError: CherryFlash poster asset is missing event_id=6401`.
- Public `openssl s_client` showed certificate SANs only for
  `*.yccdn.cloud.yandex.net`; strict `curl` failed hostname validation.
- Yandex control plane still showed the intended Certificate Manager certificate
  as `ISSUED` and the CDN binding as `CM/READY`, exposing edge/control-plane drift.

## Timeline

- 2026-08-02 11:37 UTC: retained logs show successful strict CDN poster downloads.
- 2026-08-03 03:52 UTC: Yandex CDN resource `updated_at` changed; the exact actor
  and change remain under investigation.
- 2026-08-03 07:44 UTC: first failed `popular_review` session (`#1005`).
- 2026-08-03 11:48 UTC onward: retained logs show certificate mismatch during
  poster prefetch; Kaggle later fails on the absent local poster.
- 2026-08-03 13:27 UTC: watchdog starts session `#1041`.
- 2026-08-03 13:29 UTC: containment sets
  `ENABLE_V_POPULAR_REVIEW_SCHEDULED=0`; Fly restarts healthy with both jobs disabled.
- 2026-08-03 13:30 UTC: Certificate Manager binding is re-applied and CDN cache is
  purged; Yandex documents up to 15 minutes for edge propagation.

## Root Cause

1. The CDN edge stopped presenting the valid certificate for
   `static.kenigevents.ru`, so strict HTTPS poster downloads failed before bundling.
2. Poster prefetch preserved the remote candidate; Kaggle retried the same strict
   HTTPS URL and failed because no local asset existed.
3. The general CherryFlash watchdog treated every terminal render failure as a
   missing daily slot and had no persisted retry budget, unlike partner tracks.

## Contributing Factors

- Scheduler `ops_run` rows became `success` after remote handoff, before terminal
  render/publish success.
- Session errors stored only `{'status': 'ERROR'}`; the actionable missing-poster
  exception lived in downloaded Kaggle output.
- The prior missing-photo guard covered empty canonical `photo_urls`, not outage of
  the only otherwise-renderable CDN candidate.

## Automation Contract

### Treat as regression guard when

- changing CherryFlash scheduled/watchdog retry and catch-up behavior;
- changing poster prefetch, remote candidates, or Kaggle missing-poster handling;
- changing `static.kenigevents.ru` CDN/DNS/certificate configuration.

### Affected surfaces

- `scheduling.py`
- `video_announce/scenario.py`
- CherryFlash Kaggle runtime and output poller
- Yandex Cloud CDN resource `static.kenigevents.ru`
- production `videoannounce_session`, `ops_run`, and runtime logs

### Mandatory checks before closure or deploy

- prove both general `popular_review` startup catch-up and its watchdog stop after
  two persisted failed sessions for one target date, including across restarts;
- preserve successful/live remote-handoff suppression checks;
- strict TLS must show `static.kenigevents.ru` in the served SAN;
- strict CDN poster and ICS requests must succeed;
- after enabling the product, perform one controlled same-day catch-up and verify
  terminal video plus required publication evidence;
- verify deployed SHA is reachable from `origin/main`.

### Required evidence

- Fly runtime and Kaggle output excerpts under
  `artifacts/codex/INC-2026-08-03-cherryflash-watchdog-retry-loop/`;
- retry-cap and existing CherryFlash scheduler test output;
- public TLS certificate/SAN and strict poster/ICS HTTP evidence;
- catch-up session id, Kaggle dataset/kernel, terminal video, publication report;
- deployed SHA and `origin/main` ancestry.

## Immediate Mitigation

- Disabled only scheduled `popular_review` and its watchdog; the serving bot and
  other scheduler surfaces stayed healthy.
- Re-applied the issued Certificate Manager certificate and purged CDN cache while
  waiting for edge propagation.

## Corrective Actions

- Added a persisted two-failed-session same-day cap shared by general
  `popular_review` startup catch-up and its watchdog.
- CherryFlash poster prefetch now retries canonical `static.kenigevents.ru`
  object paths through the same bucket's strict-TLS Object Storage origin when
  the CDN edge is unavailable; it never disables certificate verification.
- Added a regression test reproducing the retry storm with two failed remote
  CherryFlash sessions and a test for the CDN-to-origin poster fallback.

## Follow-up Actions

- [ ] Identify the actor/change behind the CDN resource update at 03:52 UTC.
- [ ] Add an external strict-TLS probe for `static.kenigevents.ru`.
- [ ] Persist actionable terminal Kaggle errors in `videoannounce_session.error`.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending CDN propagation and same-day catch-up

## Prevention

- A deterministic CherryFlash failure can consume at most the scheduled attempt
  plus one recovery attempt per local target date.
- CDN readiness is accepted only from the public strict-TLS edge, not from a
  control-plane `READY` flag alone.
