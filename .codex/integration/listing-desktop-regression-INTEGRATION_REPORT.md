# Listing desktop regression — integration report

- Requirement lane: `R01–R04`
- Mode: serial integration (the affected layout, CSS ownership and acceptance
  gates were coupled and intentionally not split across concurrent writers)
- Branch: `hotfix/static-listing-desktop-preview-regression-20260720`
- Fix commit: `5d31d58aefebdcca7a0953ab5c4cd19213b3ee00`
- Outcome: committed, pushed and published as immutable V27 review preview

## Requirements

| ID | Status | Evidence |
| --- | --- | --- |
| R01 — restore Today/Tomorrow/Weekend desktop | Done | shared compiled design-system bundle; public 3-viewport Playwright pass |
| R02 — restore Popular desktop header without changing mobile | Done | sticky header/rail pass; mobile 360/390/430 preservation pass |
| R03 — prevent recurrence | Done | four-route compiled-CSS assertions plus executable geometry gate |
| R04 — publish and deliver review links | Done | public V27 HTTP `200`; Telegram topic `122`, message `411` |

Gemini 3.1 Pro final critical review returned `PASS` with no P0/P1 issue. The
separate stale assertions inside the older broad design-system checker remain
documented follow-up debt and were not concealed by this incident closure.
