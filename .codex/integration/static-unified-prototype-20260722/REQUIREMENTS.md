# Requirement matrix

| ID | Requirement | Area | Dependencies | Conflict risk | Done when |
|---|---|---|---|---|---|
| R01 | Restore and apply Telegram 548/549 requirements | Product contract | Telegram read | low | Message 549 captured and reconciled |
| R02 | Identify every page type | Routing | branch/docs inventory | medium | Route/specimen matrix published |
| R03 | Combine accepted work from relevant branches without wholesale regression | Git/integration | donor map | high | Donor commits merged/reconciled with no lost fixes |
| R04 | One coherent responsive desktop/mobile prototype | UI/system | R03 | very high | Same URL adapts at mobile/desktop across page families |
| R05 | Real data, not production generation/root rollout | Data/release | exporter safety | high | Fresh read-only snapshot, noindex build prefix |
| R06 | Pages mutually linked | Navigation | R02/R04 | high | Prefix-scoped crawl and round trips pass |
| R07 | Owner can test desktop and mobile | QA | R04-R06 | high | Live links and responsive matrix pass |
| R08 | Send links here and to Telegram topic | Handoff | R07 | medium | Telegram receipt verified after publication |
