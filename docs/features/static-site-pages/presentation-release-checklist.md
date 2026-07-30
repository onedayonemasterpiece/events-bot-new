# Official site presentation release checklist

> Status: active release gate. This is the canonical checklist for the first official KenigEvents static-site presentation. Generic repository/deploy rules remain in [`release-governance.md`](../../operations/release-governance.md); Kaggle build/publish mechanics remain in [`kaggle-static-site-builder.md`](../../operations/kaggle-static-site-builder.md).

Use `Done / Partial / Blocked` and attach a public URL, SHA, build manifest or run artifact to every completed item. The release owner freezes one `origin/main` SHA and one full-catalog build ID before the presentation.

## Current R14 checkpoint — 2026-07-27

- [x] **Done locally** — the unified source build compiles the shared
  Calendar/Popular/Search/Personal/Clubs/event-detail shell, materialized Free
  collection, deterministic noindex artifact collection, structured organizer
  medallions, OCR-safe hero/rail media and branded `visual_only` sharing.
- [x] **Done locally** — focused contract tests cover global PKCE ownership,
  Enter/IME submission, bounded Search rescue, complete Free routing, honest
  empty Jazz weekend, artifact persistence/detail, medallion fail-closed
  matching and OCR/multi-image rail rules.
- [x] **Done for visual review** — immutable noindex candidate
  `production-secret-host-fallback-r2-20260727T142927-930012ec` was built from
  main-reachable SHA `161c911f37a9ad52d8b97dd89390c41abeb41908` and immutable
  snapshot `snapshot-20260727T110420-r14manual` (`280` eligible events,
  `quick_check=ok`). The create-only publisher uploaded `1242` objects;
  public Playwright passed `46/46` route × viewport checks with zero broken
  loaded images, horizontal overflow or script errors. The bearer URL was
  delivered only to review topic `548` in messages `725–726`.
- [ ] **Partial** — the normal Kaggle handoff is blocked before kernel start by
  a provider `400 INVALID_ARGUMENT: Invalid token` while creating the
  short-lived private input dataset. A host-built immutable noindex candidate
  may unblock visual review under the documented emergency boundary, but it
  does not satisfy the Kaggle status-ledger or production-root gates.
- [ ] **Blocked** — real Yandex OAuth return plus real Edge Search result on
  that candidate, product/design owner sign-off, presentation-day schedule
  freshness and rollback drill.
- [ ] **Blocked** — canonical root promotion. A successful `_review/<token>/`
  handoff does not satisfy this item.

The older checked rows below retain their dates and exact candidate names as
historical regression evidence. They must not be read as acceptance of the
current R14 candidate. The host fallback above is visual-review evidence, not
Kaggle status-ledger or root-promotion evidence. Its root hash stayed
`2684c7dd72a265d75b059f43837baecc19ce750f39d962317fc5afec99a75449`
before and after candidate publication. Event `6710` is no longer a valid “upcoming demo” by
itself; presentation scenarios must be selected from the fresh frozen export.

## 1. Release candidate

- [x] **Done for secret candidate** — R14 runtime commits and its frozen build
  SHA are reachable from `origin/main`; later main commits are parser-only and
  do not rewrite the immutable candidate.
- [x] **Done for secret candidate** — a full-catalog production export was made
  from the immutable presentation-day SQLite snapshot; fixtures were rejected.
- [x] **Done for secret candidate** — the candidate result records repo SHA,
  input fingerprint, build clock, snapshot id/hash/size, related revision,
  manifest/tree hashes and immutable asset prefix. This does not provide a
  production rollback prefix.
- [ ] **Blocked** — promotion and rollback commands rehearsed against the same candidate; production canonical remains unchanged until approval.

### Desktop event-template regression gate

- [x] **Done locally for replacement candidate** — all `282/282` generated
  event pages mount the exact shared `DesktopEventPage.astro`; only accepted
  Continuous Editorial/Split families are present and mobile revision `v4`
  remains the separate fallback surface.
- [x] **Done locally for replacement candidate** — real pins `5294`, `6815`,
  `5658` and `4671` route respectively to low-resolution Split, portrait Split,
  Continuous Editorial and Editorial with a classified poster companion.
- [x] **Done locally for replacement candidate** — full-catalog Playwright has
  zero page errors; the `4 × 3` viewport matrix has visible H1/CTA and zero
  horizontal overflow; interaction checks cover exact gallery indices,
  thumbnail derivatives, CTA release, idle autorotation and transport.
- [x] **Done on the public replacement** — public HTTP is `200` for the index,
  four real pins and both transport pages; exact public Playwright repeats the
  `4 × 3` matrix with `12/12` passes and the interaction suite has no failures.
  All evidence targets `preview-20260715t-production-desktop-contract-v2`, not
  rejected `preview-20260715t-production-transport-mobile-real-events-v1`.
- [x] **Done with truthful consultant scope** — Gemini 3.1 Pro High direct
  browser review returned `BLOCKED` after the consultant sandbox Chromium
  crashed and is not presented as acceptance. A separate screenshot-based
  review inspected exact public captures plus public matrix/interaction JSON
  and returned `ACCEPT`; the artifact names the exact generated URLs and its
  external-Playwright limitation.

## 2. Demonstration scenarios

- [x] **Done in focus preview** — real Светлогорск event `6510`: compact whole-row calendar links for outbound/return trains, a 20–90 minute arrival window and no public schedule-verification links.
- [x] **Done in focus preview** — each suggested train has its own `.ics` with departure/arrival and a 30-minute reminder.
- [x] **Done in focus preview** — real late Светлогорск concert `6397`: no guessed duration; the page shows the factual last same-day train, absence of night service and first next-day train.
- [x] **Done in focus preview** — real production event `6710` at Сказочное Холмогорье: compact outbound/return chips for `118/118А/119`, one verified shared corridor/Северный note, arrival-window-filtered outbound buses, returns after at least 1h15 on site plus the real walk, a large unboxed bus icon, a desktop schedules+map grid and responsive square/portrait route maps.
- [ ] **Partial** — re-confirm event `6710` with the organizer before the official presentation: its direct 2026-07-06 post says `25 July`, while the venue site separately lists the Baba Yaga day on `26 July`.
- [x] **Done in checks** — a Kaliningrad/unsupported-location event renders no transport block.

## 3. Public UI and accessibility

- [x] **Done in focus preview** — no `Партнёрский маршрут` and no prominent carrier promotion; only one terse carrier line below train options.
- [ ] **Blocked** — final Pixel/desktop/keyboard/contrast review on the frozen presentation candidate.
- [ ] **Blocked** — presentation copy/sign-off by product owner, including estimate, night-service and explicit no-return wording.

### Presentation UI debt register

This is the central register for visual/product work that is intentionally
deferred until the final presentation freeze or the first post-presentation
iteration. New items receive stable `TD-PRESENTATION-UI-*` identifiers instead
of being left only in review chats.

- [x] **Done in R5 — `TD-PRESENTATION-UI-001`:** the desktop club-card label
  `Ближайших встреч` was correctly positioned but its intended lower glow was
  not visually legible over the media. R5 strengthens the compact,
  lower-directed glow without changing mobile placement or card geometry.
- [ ] **P0 after focus-group launch — `TD-PRESENTATION-UI-002`:** verify that
  the installed PWA home resolves to the real product root and visibly renders
  `HomeHeroTalk`. The source component remains mounted on `/`; the reported
  missing hero must be reproduced on the newly published candidate after a
  fresh uninstall/reinstall, then fixed without delaying the focus onboarding
  repair.
- [ ] **P1 — `TD-PRESENTATION-UI-003`:** replace the curated 28-line
  `HomeHeroTalk` launch bank with the documented Smart Update batch authoring
  pipeline (bounded event fact packs, LLM fragments, deterministic validation,
  editorial approval and versioned static output). Runtime/browser LLM calls
  and unreviewed publication remain forbidden.

## 4. Schedule freshness

- [x] **Done in reference audit** — all 13 direction/product pages from the official КППК schedule index inventoried with effective dates, exact image URLs and SHA-256; current coastal, Балтийск, eastern, Багратионовск, Мамоново, Железнодорожный and Краснолесье matrices manually reviewed.
- [x] **Done in policy directory** — Пионерский/Зеленоградск/Светлогорск are rail-primary, Балтийск is rail-primary while the current multi-pair table applies, sparse inland routes remain parallel with buses, and mixed-mode/transfer safety rules cover Ферма Тюниных and Бранденбург.
- [x] **Done in static generator** — ICS names expose their artifact type from the first segment: `rzd-*` for trains, the prepared `bus-*` contract for buses and `event-*` for the event itself; transport files are generated only for rendered actions, preserve VEVENT UIDs and enforce 4-file standard / 6-file dual-origin per-event ceilings without orphan files.
- [ ] **Partial** — export the prepared Пионерский, Балтийск and inland references into exact-date public service calendars and add real event regressions; directory presence alone must not enable a transport block.
- [ ] **Partial** — add a real ДС «Янтарный» regression before enabling the venue-specific `Елизаветинская` option: one train row must offer separately clickable Южный/Северный boarding times, origin-qualified concise ICS names, no more than six ICS files total, the official `35 ₽` fare once, the 650 m / 8–10 min walk and exclusion of unrelated Калининград events.
- [ ] **Blocked — P0:** close [`TD-STATIC-TRANSPORT-001`](event-transport-schedule.md#td-static-transport-001--automated-schedule-refresh-before-presentation).
- [ ] **Blocked** — latest successful rail+bus refresh is within the approved max age; public build manifest points to it.
- [ ] **Blocked** — last-known-good behavior, stale warning and admin alert verified by a failed/partial refresh drill.

## 5. Build and public acceptance

- [ ] **Blocked** — one real StaticSiteBuilder Kaggle CPU run completes with status-ledger heartbeats/report and `static_site:builder` lease.
- [x] **Done in gate tests** — ordinary production rejects a real bare
  `data-amber-artifact` or `data-artifact-collection` marker while accepting
  the inert `data-amber-artifact-research="off"` configuration and
  `data-artifact-collection-unavailable` fallback; the secret candidate remains
  the only surface allowed to render the research mechanic.
- [x] **Done for secret candidate** — production and secret generated-output
  gates, both mandatory browser-release gates, occurrence tests (`14/14`),
  artifact tests (`5/5`), PWA tests (`5/5`), static-release tests (`10/10`)
  and Smart Update duplicate guards (`14/14`) passed. The generated artifact
  test accepts the required robots directives as a set, so the stricter
  candidate-wide `nosnippet` directive cannot create a false failure.
- [x] **Done for secret candidate** — public `200`, noindex/no-referrer, manifest
  MIME, loaded-image and link checks pass for the reviewed route matrix.
- [x] **Done for secret candidate** — mobile/desktop public Playwright evidence
  is attached under ignored `artifacts/codex/r14-public-qa/`; messages
  `725–726` contain the complete reviewer route set.

## 6. Promotion and post-presentation smoke

- [ ] **Blocked** — approved candidate promoted from noindex prefix to production canonical.
- [ ] **Blocked** — sitemap/robots/canonical/TLS/CDN cache checks pass after promotion.
- [ ] **Blocked** — event detail, calendar, search, transport and rollback smoke completed; release SHA/build ID recorded.

## 7. Product analytics after presentation

- [ ] **Blocked** — consented compact telemetry ingest and daily aggregates are
  deployed for Web Vitals, rail exposure/depth, committed like/dislike swipes,
  artifact exposure/find/collection/detail and date-calendar open/select.
- [ ] **Blocked** — the first recurring readout uses denominators and excludes
  bots/acceptance traffic; raw coordinates, URLs, Search text and UA are not
  retained.

This gate does not block visual review of the noindex candidate, but it blocks
claims that these behaviors are already measured regularly.
