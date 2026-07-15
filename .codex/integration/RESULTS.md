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
- **R03 Superseded by the refinement below** — the original desktop D0/D1/D2
  experiment was implemented and measured, then retired after real paste tests.
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
- Production owner decision for schedule/publisher enablement.

## Refinement after desktop paste testing — 2026-07-15

- The dominant tinted footer banner was removed. Desktop now shows a quiet
  `Поделиться афишей` utility row with equal outline actions
  `Скопировать карточку` and `Скопировать текст и ссылку`; mobile still shows one
  native `Поделиться` action.
- Clipboard contracts are intentionally disjoint: the card action writes one
  image-only PNG `ClipboardItem`, while the text action calls `writeText()` with
  the frozen service copy and canonical URL. Neither action silently falls back
  to the other intent.
- The previous visual treatment has no recorded Gemini approval. A new
  Antigravity visual response rejected its aggressiveness, but is classified as
  supplementary probe material because the CLI did not expose a canonical
  Gemini Pro model ID. The permitted Opus fallback was quota-blocked.
- Fresh snapshot `2026-07-15T07:10:00Z` remains data-bound rather than
  hard-coded: `284` eligible events, `15` normalized places and `84` additions in
  the exact trailing 168 hours. The accepted face mix is honestly
  `4 popular / 1 promoted / 3 random` with one recorded promo shortfall.
- Non-OCR/photo faces are center-cover cropped; explicit OCR posters remain
  contain-protected. The landscape classification-gap case `event_id=3216` now
  records `classification_gap_landscape_cover` and fills its square face.
- Exact-bundle Kaggle evidence: GPU debug
  `service-share:2026-07-15:debug:20260715T092428Z`, CPU final
  `service-share:2026-07-15:final:20260715T092658Z`, bundle
  `82d19d5f18cec0e593eb1e9ba39986ef85d140163f51a51fa295e5497db1a035`.
  Final CPU output passed scene gates and was delivered to Telegram Saved
  Messages as message `32270`.
- Verification: controller `5/5`, renderer `23/23`, local Playwright `14/14`,
  preview static contract check and `git diff --check` passed.
