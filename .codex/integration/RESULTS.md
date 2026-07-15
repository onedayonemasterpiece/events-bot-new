# F18 footer service-share integration results

- Base: `origin/main@926dad8a91fc7f1070126d32a05281aa92ff1666`.
- Implementation head before this evidence commit:
  `8fafd9923f416fbd7cabb7cd31c0628376d4472d`.
- Public preview:
  `https://kenigevents.ru/preview-20260715t0752z-f18-service-share-footer/__preview/`.
- Lab:
  `https://kenigevents.ru/preview-20260715t0752z-f18-service-share-footer/lab/service-share/`.
- Header/mobile-menu work: **Deferred until V12 by user request**; no header code
  was changed for F18.

## Requirement closure

- **R01 Done** — product lead, mobile/desktop labels, canonical payload and exact
  success/cancel copy are fixed in one contract.
- **R02 Done** — one shared component/controller is mounted only in the common
  footer.
- **R03 Done** — mobile file/text/clipboard/link fallbacks and desktop D0/D1/D2
  are implemented; D0 remains the default.
- **R04 Done** — noindex `/lab/service-share/` exposes preview, capabilities,
  controlled paste targets and a bounded in-memory ledger.
- **R05 Done** — the accepted snapshot produced `284` current events, `15`
  normalized places and `84` exact trailing-168h additions. Unique face mix is
  honestly `4 popular / 1 promoted / 3 random`; the requested second promoted
  face was unavailable and is recorded as `promo_shortfall`, never duplicated or
  mislabeled.
- **R06 Done for preview implementation** — deterministic daily rotation,
  08:45 Europe/Kaliningrad scheduler hook, status heartbeats and verified Kaggle
  GPU debug → exact-bundle CPU final. Production schedule/publisher activation
  remains off and was not requested for this preview release.
- **R07 Done** — controller `5/5`, renderer `17/17`, public HTTPS Playwright
  `12/12`, static preview check, Python compile and diff check passed.
- **R08 Done for footer preview** — prefix-only deploy dry-run found `1,286`
  planned objects and `0` outside-prefix destinations; public MIME/cache/hash
  verification passed. Production/current pointers and stable ICS objects were
  not changed.

## Pending outside the footer-preview scope

- Native Android/iOS share-sheet and Windows/macOS paste matrices.
- V12 header/mobile-menu placement.
- Production owner decision for desktop D1/D2 and schedule/publisher enablement.
