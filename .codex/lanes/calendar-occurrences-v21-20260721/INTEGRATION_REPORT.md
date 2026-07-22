# Integration report — calendar mobile v21 and occurrence family contract

Date: 2026-07-21 (UTC; listing semantics use `Europe/Kaliningrad`)

## Integration base and scope

- Base: `origin/main@58440062`.
- Branch: `integration/calendar-occurrences-v21-final-20260721`.
- The current `origin/main` already contains the selectively integrated occurrence donor from `feature/related-events-compact-unified-20260721`; this branch does **not** merge any old lab wholesale.
- The unrelated dirty primary checkout and dirty `integration/static-crop-occurrences-20260721` worktree were not reset or overwritten.
- W02 owns only the published noindex research prototype/evidence. W03 adds the missing live-search projection/pagination hardening on top of current main.

## Requirement status

| ID | Result | Evidence |
| --- | --- | --- |
| P01 | Done | `/segodnya/` is 21 July 2026; strictly past rows show `Прошло`, neutral lighter surface and desaturated media; same-day rows without explicit end are not falsely completed. |
| P02 | Done | Normal/gallery cue boxes share right edge and 48 px geometry; the gallery variant changes shaft length only. |
| P03 | Done | Reviewed long Kant/Brachert rail keeps `Светлогорск · Дом-музей Германа Брахерта`. |
| P04 | Done | Hero is 11×6 with an extra bottom row, constrained pale left extension, date intersection cap, per-load crypto order/jitter, early nonlinear fade with unchanged endpoint and true DPR 1/2/3 assets. |
| P05 | Done | Parallax exists only at `/date-2026-07-24-parallax/`, factor 0.15 and reduced-motion off; the base date page is non-parallax. Product verdict: keep as isolated A/B research, not default. |
| P06 | Done | Popular sticky shelf header uses an 80 px safe-zone alternative; zero brand/title/icon intersection, rail height remains 112 px. |
| P07 | Done | Russian count forms covered for 1/2/4/5/21/24; Popular renders `24 события`. |
| R01–R06 | Done | Mutual explicit `other_date_ids` only; no semantic inference; date lists `per-date`; ranked/entity surfaces `per-family`; event detail always-visible selector; accepted 03/04/05/10 and rejected 01/02/11/12/13; exact compact and rail accessibility labels. |
| R07 | Done | Occurrence suite 9/9. |
| R08 | Done | Astro preview build: 383 pages. |
| R09 | Done | Full `npm run check:preview` passed: 303 events, occurrence and generated-output contracts included. |
| R10 | Done as regression; incident remains open | `tests/test_smart_update_merge_identity_gate.py`: 15/15. This frontend/search work does not repair production event rows 5754–5757 and does not close `INC-2026-07-18-dramteatr-same-day-event-glue`. |
| R11 | Done | Canonical linked-events/static-site docs and `CHANGELOG.md` updated. |
| Search E2E contract | Done | Snapshot emits reciprocal family ids/labels/ARIA. Edge collapses the complete ranked window before logical pagination, repeats after LLM rerank and shares the family seen-set with fallback; malformed/asymmetric payloads fail closed and the highest-ranked member is retained. |

## Validation

```text
npm run test:occurrences
  9 passed

/tmp/events-bot-pytest-20260721/bin/pytest -q tests/test_event_vector_sync.py
  12 passed

node --experimental-strip-types --test supabase/functions/event-search/occurrence-families.test.mjs
  5 passed

/tmp/events-bot-pytest-20260721/bin/pytest -q tests/test_smart_update_merge_identity_gate.py
  15 passed

npm run build:preview
  383 pages; dist/preview-20260721t101545-cd818728/

npm run check:preview
  PASS; 303 events; strict_related=false

git diff --check origin/main...HEAD
  PASS
```

The earlier Garage generated-output blocker belonged to the older base; after rebasing the integration on current `origin/main@58440062`, the full preview gate passes.

## Mobile prototype and publication

- Root: <https://kenigevents.ru/preview-20260721-mobile-calendar-city-popular-v21/>
- Today: <https://kenigevents.ru/preview-20260721-mobile-calendar-city-popular-v21/segodnya/>
- Popular: <https://kenigevents.ru/preview-20260721-mobile-calendar-city-popular-v21/populyarnoe/>
- Base date: <https://kenigevents.ru/preview-20260721-mobile-calendar-city-popular-v21/date-2026-07-24/>
- Parallax experiment: <https://kenigevents.ru/preview-20260721-mobile-calendar-city-popular-v21/date-2026-07-24-parallax/>
- Local Playwright validator: 106/106 PASS.
- Public-host Playwright validator: 106/106 PASS, including explicit lazy-image decode and prefixed hero asset resolution.
- Telegram existing forum topic: `Главная, Популярное, списки — wireframes`, topic id `122`; messages `459`–`465` contain the root and page links.

## External review

- Gemini 3.1 Pro High reviewed the final branch and public pages read-only after W03. All seven visual/occurrence/search criteria received **PASS**; full response: [`GEMINI_FINAL_ACCEPTANCE.md`](GEMINI_FINAL_ACCEPTANCE.md).
- The consultant agrees that parallax should remain an isolated experiment, not the default, and identifies the bounded 60-row Edge mapping cost as a non-blocking operational trade-off.
- Machine evidence remains authoritative for exact Playwright check counts: local/public **106/106**.
- Independent final checklist re-review marked Search `per-family`, integration hygiene and evidence **Done**; no code or acceptance blocker remains in this branch.

## Known limits

- This publishes a research prototype and an integration branch; it is not a production deployment.
- Parallax should not become the default without an isolated experiment because it competes with the scroll-linked disappearance cue.
- The production incident remains open until canonical rows 5754–5757 are repaired and public/rebuild evidence is attached under the incident workflow.
