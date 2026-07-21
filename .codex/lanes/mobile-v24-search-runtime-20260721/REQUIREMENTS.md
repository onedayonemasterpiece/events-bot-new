# Mobile v24 search runtime requirements

| ID | Requirement | Dependencies | Done when |
|---|---|---|---|
| R01 | Search results use the exact accepted static-page crop contract from the focus-6408 preview. | D01 | Runtime search cards show identical media geometry and smart-crop semantics; regression test passes. |
| R02 | Search button progress never visually moves backward and uses a coherent, accessible loading state. | D02,C01 | Monotonic state machine, no completion rollback, reduced-motion and error/retry states verified. |
| R03 | Mobile transient notifications are inventoried and unified as header-attached toasts with a remaining-time underline. | D02,C01 | Shared behavior, duration policy, pause/dismiss/a11y semantics and tests documented. |
| R04 | Free and children event discovery is designed across search, static pages and navigation; children medallion decision is explicit. | D03,C01 | Product decision, taxonomy/facet/page strategy, labels and analytics are in canonical docs/prototype. |
| R05 | Search uses the same mobile header contract as other mobile surfaces, coordinated with bottom navigation. | D04,C01 | Unified shell has stable safe areas, selected nav semantics and mobile visual acceptance. |
| R06 | Gemini Pro performs critical design consultation and final acceptance. | D01-D04 | Saved consultant artifacts with model ID and explicit verdict. |
| R07 | Full mobile QA and public prototype are delivered. | W01,W02 | Build/tests/Playwright at mobile + DPR, public URLs and Telegram handoff if release workflow supports it. |
