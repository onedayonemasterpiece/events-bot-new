# Official site presentation release checklist

> Status: active release gate. This is the canonical checklist for the first official KenigEvents static-site presentation. Generic repository/deploy rules remain in [`release-governance.md`](../../operations/release-governance.md); Kaggle build/publish mechanics remain in [`kaggle-static-site-builder.md`](../../operations/kaggle-static-site-builder.md).

Use `Done / Partial / Blocked` and attach a public URL, SHA, build manifest or run artifact to every completed item. The release owner freezes one `origin/main` SHA and one full-catalog build ID before the presentation.

## 1. Release candidate

- [ ] **Blocked** — presentation scope frozen and all candidate commits reachable from `origin/main`.
- [ ] **Blocked** — clean full-catalog production export made on presentation day; fixture-only or hand-added canary data is not the production candidate.
- [ ] **Blocked** — release manifest records repo SHA, event snapshot, schedule snapshot ID/hash/fetched time, asset prefix and rollback prefix.
- [ ] **Blocked** — promotion and rollback commands rehearsed against the same candidate; production canonical remains unchanged until approval.

## 2. Demonstration scenarios

- [x] **Done in focus preview** — real Светлогорск event `6510`: outbound/return trains, 20–40 minute arrival buffer and live recheck links.
- [x] **Done in focus preview** — each suggested train has its own `.ics` with departure/arrival and a 30-minute reminder.
- [x] **Done in focus preview** — real late Светлогорск concert `6397`: estimated end is visibly marked and the page explicitly says that no suitable return train exists.
- [x] **Done in focus preview** — real production event `6710` at Сказочное Холмогорье: routes `118/118А/119`, estimated journey and first/last-mile walk maps.
- [ ] **Partial** — re-confirm event `6710` with the organizer before the official presentation: its direct 2026-07-06 post says `25 July`, while the venue site separately lists the Baba Yaga day on `26 July`.
- [x] **Done in checks** — a Kaliningrad/unsupported-location event renders no transport block.

## 3. Public UI and accessibility

- [x] **Done in focus preview** — no `Партнёрский маршрут` and no prominent carrier promotion; only one terse carrier line below train options.
- [ ] **Blocked** — final Pixel/desktop/keyboard/contrast review on the frozen presentation candidate.
- [ ] **Blocked** — presentation copy/sign-off by product owner, including estimate/no-service wording.

## 4. Schedule freshness

- [ ] **Blocked — P0:** close [`TD-STATIC-TRANSPORT-001`](event-transport-schedule.md#td-static-transport-001--automated-schedule-refresh-before-presentation).
- [ ] **Blocked** — latest successful rail+bus refresh is within the approved max age; public build manifest points to it.
- [ ] **Blocked** — last-known-good behavior, stale warning and admin alert verified by a failed/partial refresh drill.

## 5. Build and public acceptance

- [ ] **Blocked** — one real StaticSiteBuilder Kaggle CPU run completes with status-ledger heartbeats/report and `static_site:builder` lease.
- [ ] **Blocked** — `npm run check:preview` passes against the frozen full catalog, including train/bus/ICS/no-return regressions.
- [ ] **Blocked** — public `200` and MIME checks pass for index, demo events, media, discovery JSON, event ICS and transport ICS.
- [ ] **Blocked** — mobile/desktop Playwright screenshots and link checks attached to the release evidence.

## 6. Promotion and post-presentation smoke

- [ ] **Blocked** — approved candidate promoted from noindex prefix to production canonical.
- [ ] **Blocked** — sitemap/robots/canonical/TLS/CDN cache checks pass after promotion.
- [ ] **Blocked** — event detail, calendar, search, transport and rollback smoke completed; release SHA/build ID recorded.
