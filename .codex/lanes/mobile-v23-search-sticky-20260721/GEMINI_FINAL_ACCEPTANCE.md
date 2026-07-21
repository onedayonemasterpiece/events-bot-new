# Mobile Product Review & Acceptance Gate Report

**Overall Gate Decision:** `GO WITH CHANGES`

The core interaction mechanics, sticky header redesign, Orpheus recap layout, desaturated started events, query learning UX, and test suites strictly pass specifications. However, **public release handoff requires resolving the semantic date mismatch on materialized collections and fixing 2 visual mobile regressions in the preserved Search donor.**

---

## Requirement-by-Requirement Audit

| # | Requirement | Status | Evidence & Verification Summary |
|---|---|---|---|
| 1 | **Today started events** | **PASS** | Verified in [`v23-today-started-row-390x112-2x.png`](file:///home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-v23-research-20260721/v23-today-started-row-390x112-2x.png): 11:00 event (*Завтрак с художником*) is desaturated/grayscale with factual `Уже началось` pill. No invented end time. Day-program (*День янтаря*) remains vivid. Initial scroll targets first future timed event. |
| 2 | **Orpheus multi-date recap** | **PASS** | Verified in HTML and [`v23-date24-orpheus-recap-scrolled-390x112-2x.png`](file:///home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-v23-research-20260721/v23-date24-orpheus-recap-scrolled-390x112-2x.png): 3-line time block `19:00 / 24 июля / 25 июля 17:00` present in track header. Deliberate recap block `Ещё даты / 25 июля · 17:00` renders cleanly after the digest body and before medallion slots. Reciprocal explicit family matching enforced. |
| 3 | **Sticky header (Layout A)** | **PASS** | Replaced annotated visual mess ([`photo_2026-07-21_12-28-15.jpg`](file:///home/dev/projects/events-bot-new/artifacts/codex/mobile-v23-telegram-feedback/photo_2026-07-21_12-28-15.jpg)) with clean 64px Layout A ([`v23-sticky-layout-a-320x667-2x.png`](file:///home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-v23-research-20260721/v23-sticky-layout-a-320x667-2x.png)). Row 1: `24 июля · 20 событий`, Row 2: `Вся область`. Zero wrapping at 320px/390px, left brand safe lane intact. |
| 4 | **Search core mechanics** | **PASS** | Preserves v58 donor architecture: authentic Yandex/Supabase PKCE auth flow, separate progress submit button whose background fills as progress bar (`search-mocked-authorized-progress-button-390x844-dpr2.png`), and canonical large `EventCard` result items. No fake email or fake saved state. |
| 5 | **Query learning UX** | **PASS** | Quiet `Готовые подборки` section with `Можно начать отсюда` eyebrow. Natural-language query phrases. Materialized queries are static `<a href="...">` links. Example buttons (`<button data-search-query-fill="...">`) populate input field without triggering auto-submit. Saved searches isolated to authenticated state. |
| 6 | **Public test suites** | **PASS** | Calendar focused 36/36 + inherited v22 106/106 PASS ([`validate-v23-final.log`](file:///home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-v23-research-20260721/validate-v23-final.log)). Search source 5/5 + occurrence 9/9 PASS. Smoke evidence verified in [`search-smoke-evidence.json`](file:///home/dev/.codex/worktrees/events-bot-new/mobile-v23-search/artifacts/codex/mobile-search-v23-20260721/search-smoke-evidence.json) (8/8 PASS). |

---

## Response to Explicit Critical Question

### Materialized Fixture Date (18 July) vs. Review Date (21 July)

**Verdict:** **RELEASE BLOCKER FOR HANDOFF** (Must be re-materialized before user release).

**Analysis:**
- The review date is **21 July 2026** (Tuesday). The nearest upcoming weekend is **25–26 July 2026**.
- The published page [`podborki/dzhaz-na-vyhodnyh/`](https://kenigevents.ru/preview-20260721-mobile-search-donor-v23/podborki/dzhaz-na-vyhodnyh/) is titled *"Джаз на ближайших выходных"* ("Jazz on nearest weekend").
- However, the page displays a single event: *Опера и джаз* on **18 июля** (last weekend's Saturday).
- Although the page includes a disclaimer (*"Данные афиши обновлены 2026-07-17"*) and carries `noindex` headers, **presenting past events (18 July) under the heading "nearest weekend" on 21 July violates semantic honesty**. A user entering "nearest weekend" expects the upcoming 25–26 July program. 

**Decision:** Relying on fixture convenience here undermines product integrity. The Astro static generator must re-materialize the query against the current dataset (21 July 2026) so that the page reflects 25–26 July events before user handoff.

---

## Visible Mobile Regressions in Preserved Search Donor

Preserving the Astro Search donor rather than refactoring it into the unified mobile layout introduces two noticeable visual/layout regressions:

```
+-----------------------------------------------------------------------+
| [REGRESSION 1] Viewport Left Edge Clipping                            |
| Search header brand tag (announcements-lockup--desktop) extends past  |
| the left screen margin on 390px viewports ("ы / ПОЛЮБИТЬ...").        |
+-----------------------------------------------------------------------+
| [REGRESSION 2] Missing Unified Bottom Navigation Bar                  |
| Search donor pages use an accordion (<details>) menu instead of the   |
| canonical v23 mobile bottom nav bar (Афиша | Даты | Поиск | Для меня).|
+-----------------------------------------------------------------------+
```

1. **Brand Lockup Viewport Overflow (Top Left):**
   - *Evidence:* In [`search-anonymous-top-collections-390x844-dpr2.png`](file:///home/dev/.codex/worktrees/events-bot-new/mobile-v23-search/artifacts/codex/mobile-search-v23-20260721/search-anonymous-top-collections-390x844-dpr2.png), the brand logo in `<header class="site-header">` uses `announcements-lockup--desktop`. The left wordmark text is clipped off the left screen edge.
   - *Fix:* Replace the header lockup in the Search donor with the mobile-safe `.brand-tag` component used in Calendar v23 (`segodnya/` & `date-2026-07-24/`).

2. **Inconsistent Navigation Shell (Bottom Nav Bar):**
   - *Evidence:* Calendar pages feature a fixed bottom navigation bar (`<nav class="bottom-nav">`). The Search donor pages omit this bar entirely, forcing mobile users to rely on header dropdown links.
   - *Fix:* Mount `<nav class="bottom-nav">` on `/poisk/` and `/podborki/*` with `aria-current="page"` set on the **Поиск** tab.

---

## Exact Required Changes Before User Handoff

1. **Re-materialize Static Collections:** Re-run static collection generation for `/podborki/dzhaz-na-vyhodnyh/` using the 21 July 2026 event dataset (targeting 25–26 July events).
2. **Align Search Header:** Update `AuthorizedEventSearch.astro` / `EventLayout.astro` header markup to use `.brand-tag` instead of unconstrained desktop lockups to fix mobile left-edge clipping.
3. **Attach Mobile Bottom Nav:** Include `<nav class="bottom-nav">` across all Search donor pages for consistent tab navigation across the entire web application.
