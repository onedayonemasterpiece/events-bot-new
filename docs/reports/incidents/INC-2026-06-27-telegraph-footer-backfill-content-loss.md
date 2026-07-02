# INC-2026-06-27 Telegraph footer backfill content loss

Status: mitigated (restoration complete; prevention follow-ups open)
Severity: sev1
Service: Telegraph event detail pages
Opened: 2026-06-27
Closed: —
Owners: Codex / events-bot release operator
Related incidents: —
Related docs: `docs/features/static-site-pages/README.md`, `docs/operations/release-governance.md`, `docs/operations/incident-management.md`

## Summary

During the Max social-link rollout for Telegraph event footers, a production one-off backfill script read `page.get("content_html", "")` after `Telegraph.get_page(..., return_html=True)`. The python-telegraph client actually returns HTML in the `content` key. The script therefore treated existing pages as empty and edited active/future Telegraph event pages to contain only the shared social footer.

## User / Business Impact

- Active/future Telegraph event pages could temporarily lose the event description, logistics, media and source footer, leaving only the title and social footer.
- Public Telegram/VK posts that link to those Telegraph pages still opened, but the detailed event content was missing.
- Max link itself was present, but the page contract was broken.

## Detection

- Detected by post-backfill verification: public pages had Max, but API verification showed `content_html_len=0`; inspecting `content` showed only the footer.
- A public HTML text check confirmed affected pages such as event `5878` were footer-only.
- Local package source confirmed python-telegraph stores HTML in `response['content']` when `return_html=True`.

## Timeline

- 2026-06-27 ~20:09 UTC: deployed Max footer hotfix SHA `7cc13e3d`.
- 2026-06-27 ~20:12-20:21 UTC: ran a backfill over 441 active/future Telegraph event pages using `content_html`; pages were edited footer-only.
- 2026-06-27 ~20:22 UTC: verification mismatch found (`content_html` empty, public Max present).
- 2026-06-27 ~20:24 UTC: restored event `6473` with `update_telegraph_event_page`; DB content-hash commit hit SQLite lock after the Telegraph edit, but public content was restored.
- 2026-06-27 ~20:25 UTC: started full restore with `update_telegraph_event_page`; restored early batch pages, then stopped when Telegraph flood control returned a long wait (~1953s).
- 2026-06-27 ~20:30 UTC: added code-level guard `telegraph_page_html()` and tests so future code reads the correct python-telegraph field.
- 2026-06-27 ~20:35 UTC: a fast restore attempt for event `5878` hit Telegraph flood control; existing fallback incorrectly created a replacement page. Reverted DB `5878` back to the original public Telegraph path and added a code fix so flood/transient edit errors no longer create replacement pages.


- 2026-06-28 07:03 UTC: read-only public audit checked 447 active/current/future Telegraph event pages; 375 were confirmed footer-only by public HTML criteria (`Max` footer present, no `Источников:`, very short article text, no event facts). No restore/backfill helper process was active on Fly before repair.
- 2026-06-28 07:06-07:11 UTC: restored priority/recent pages including `5878` and latest affected `@kenigevents` posts via `update_telegraph_event_page(event_id, db, None)` with `TELEGRAPH_VERIFY_EDITABLE_ON_NOCHANGE=1`; 31 pages were public-verified as repaired.
- 2026-06-28 07:14-07:25 UTC: restored the remaining 344 confirmed footer-only pages through the same event-page renderer/edit path. The repair run bounded Telegraph flood-control waits and encountered no Telegraph flood-control blocker. Optional LLM logistics cleanup was disabled for this incident repair run to avoid slow provider fallback while preserving full event body/source/footer rendering.
- 2026-06-28 07:26 UTC: final public audit of the same 447-page active/current/future set found `footer_only=0`, `suspicious=0`, `errors=0`. `/healthz` was OK.

## Root Cause

1. Incorrect python-telegraph response contract assumption: `get_page(..., return_html=True)` returns HTML in `content`, not `content_html`.
2. The backfill script lacked a mandatory pre-edit validation that existing body length/content was non-empty and that the post-edit page still contained event-specific text.
3. Bulk edit was run over all 441 pages after only checking footer idempotency, not a full content-preservation canary.

## Contributing Factors

- Existing code had several `content_html` reads, reinforcing the wrong assumption.
- Telegraph flood control makes immediate full rollback slow after a large batch.
- The one-off script was not committed/reviewed as a reusable operation with tests.

## Automation Contract

### Treat as regression guard when

- Any task reads, patches, backfills, rebuilds or bulk-edits Telegraph pages.
- Any code uses `Telegraph.get_page(..., return_html=True)`.
- Any social footer/navigation/month/festival maintenance script edits existing Telegraph content.

### Affected surfaces

- `main.py`: Telegraph `get_page` readers, footer/nav/image fixups, event page renderer.
- One-off production maintenance scripts run through Fly SSH.
- External system: Telegraph API and python-telegraph client contract.
- Smoke paths: public `telegra.ph/*` pages and `Telegraph.get_page` API output.

### Mandatory checks before closure or deploy

- Unit tests for `telegraph_page_html()` covering `content`, `content_html`, and node-list fallback.
- Targeted tests for footer idempotency with Telegraph-added `target="_blank"` anchors.
- Public smoke for representative Telegraph pages (at least event ids `5878`, `6473`, and one long-running exhibition):
  - Max link count is exactly `1`;
  - visible text length is above the event-content threshold, not footer-only;
  - event-specific description/logistics text is present.
- Restoration report covering all rows selected by the active/future Telegraph-page query, with errors/flood waits documented.
- Release-governance evidence: fix SHA reachable from `origin/main`, clean deploy, `/healthz` OK.

### Required evidence

- Deployed SHA for the `telegraph_page_html()` code fix.
- Unit test output.
- Public HTML verification output for sample pages.
- Restoration artifact with counts of restored/skipped/error pages and Telegraph flood-control blockers if any remain.

## Immediate Mitigation

- Stopped the unsafe backfill flow.
- Restored event `6473` and an early batch of pages using the canonical `update_telegraph_event_page()` renderer.
- Stopped the full restore when Telegraph flood control returned a long wait, to avoid a blocked remote process.

## Corrective Actions

- Added `telegraph_page_html(page)` to centralize python-telegraph response handling and prefer `content` for `return_html=True`.
- Replaced direct `content_html` readers in Telegraph maintenance paths with `telegraph_page_html()`.
- Added unit coverage for the python-telegraph `content` key and `content_html` fallback.
- Restricted event-page replacement fallback to `PAGE_ACCESS_DENIED`; Telegraph flood-control/transient edit failures now fail without creating new pages or orphaning already-published links.

## Follow-up Actions

- [x] Complete restoration of all active/future Telegraph event pages after Telegraph flood control clears.
- [x] Revert event `5878` DB path from the accidental replacement page back to the original public path (`codex_backup_20260627_telegraph_footer_5878_path`).
- [ ] Add a reusable checked maintenance script for social-footer backfills with canary mode, content-length guard, and post-edit public verification.
- [ ] Add an operational runbook note: never bulk-edit Telegraph pages from `content_html`; use `telegraph_page_html()` / canonical renderer.

## Release And Closure Evidence

- deployed SHA: `ab0f6aa9` (`origin/main`) for the flood-control replacement guard; `0f9d0f0a` for `telegraph_page_html()`.
- deploy path: manual `flyctl deploy` from clean `hotfix/social-footer-max-20260627` worktree.
- regression checks: `pytest -q tests/test_festival_nav.py tests/test_telegraph_side_effects.py` → `21 passed`; full restore/public smoke still pending.
- post-deploy verification: `/healthz` OK after deploy; full Telegraph restoration pending flood-control clearance.

## Prevention

- Centralized page HTML extraction prevents this exact API-contract mistake in code.
- Incident record must be raised for all future Telegraph bulk maintenance and requires content-preservation smoke, not only footer/link checks.

### 2026-06-28 restoration evidence

- Active/current/future public audit scope: 447 Telegraph event pages selected from production DB (`lifecycle_status=active`, `telegraph_url` present, `date >= 2026-06-27` or `end_date >= 2026-06-27`).
- Confirmed damaged before repair: 375 footer-only pages.
- Repaired: 375 pages total (`31` in the priority run, `344` in the remaining run).
- Remaining Telegraph blockers: none; no flood-control wait >60 seconds occurred during the final repair.
- Final public audit: `footer_only=0`, `suspicious=0`, `errors=0` across all 447 checked pages.
- Required special checks:
  - event `5878` stayed on original URL `https://telegra.ph/Koncertnaya-programma-Pesni-SSSR-06-10` and final public text has event facts, `Источников: 1`, Max count `1`;
  - event `6473` remained full-content with event facts, `Источников: 1`, Max count `1`;
  - latest 20 Telegram event posts (`tg_event_post_url` present, ordered by `tg_event_post_id`) had no footer-only Telegraph pages and each checked page had Max count `1` plus `Источников: 1`.
- Production `/healthz`: OK after repair.
- Restore/backfill process check after repair: no active Telegraph/backfill/restore helper process on Fly.
- Scope note: no bulk Max rollout over intact old pages was run; only pages confirmed footer-only by the public audit were repaired.
