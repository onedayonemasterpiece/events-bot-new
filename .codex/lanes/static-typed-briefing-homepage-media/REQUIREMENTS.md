# Typed briefing: homepage/media iteration

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Primary lane | Parallelizable | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | Add an explicit named-person arrival narrative alongside the curiosity teaser. | copy/data | `briefingLab.ts`, lab page, tests | grounded demo event | medium | `product_code` | after audits | A named guest is visible and linked to the matching event. |
| R02 | Expand the visible scenario set beyond eight and remove hard-coded eight-item UI. | data/UI | `briefingLab.ts`, lab page, tests | none | medium | `product_code` | yes | Selector, progress and play-all reflect the expanded deck dynamically. |
| R03 | Present a clean homepage-like page; move laboratory controls into a compact bottom area. | UI | lab page, tests | none | medium | `product_code` | yes | Hero/categories/feed read as a page; LAB chrome is collapsed and secondary. |
| R04 | Show related narrative imagery on desktop for selected scenes. | UI/motion | lab page, existing assets, tests | media audit | high | `product_code` | after `visual_media_audit` | Selected scenes use reserved, non-shifting desktop media; mobile stays clean. |
| R05 | Add polite, friendly, locally voiced and lightly humorous copy. | copy/data | `briefingLab.ts`, docs | none | low | `product_code` | yes | Greeting, local phrase and gentle humor exist without pressure. |
| R06 | Specify weather-aware water/sea/event chains using the existing `kotopogoda` contract. | architecture | canonical briefing docs | source audit | high | `weather_chain_audit` | yes | Reusable source contract, freshness and safe fallback are documented. |
| R07 | Prototype a rare large right-side media easter egg with stripe-backed text and ease-in-out entry/exit; document future optimized video. | UI/motion | lab page, docs, tests | media audit | high | `product_code` | after `visual_media_audit` | One bounded scene demonstrates the effect without overflow or layout shift. |
| R08 | Add a public “Показать следующее” control when a narrative is complete/stopped. | interaction | lab page, tests | scenario ordering | medium | `product_code` | yes | 44px public control advances to the next eligible scene independently of LAB controls. |
| R09 | Use only the exact wide brand “О”, never a clipped full wordmark or stretched font glyph. | brand/visual | brand asset, lab page, tests | SVG audit | high | `visual_media_audit` | yes | Exact source geometry is isolated as its own asset and visually verified. |
| R10 | Define a narrative-and-chain engine with eligibility, state, transitions and promo-campaign control. | architecture | canonical briefing docs | weather audit | high | `weather_chain_audit` | yes | Production-oriented state graph and promo overlay contract are documented. |
| R11 | Define a large phrase bank plus bounded LLM semantic rewriting with fact locks and validation. | LLM/product | canonical briefing docs | engine contract | high | `weather_chain_audit` | yes | Phrase families, locked slots and generation guardrails are explicit. |
| R12 | Add a humorous guest template linked only when concrete facts support it. | copy/data | `briefingLab.ts`, docs, tests | grounded event facts | medium | `product_code` | after data inspection | A grounded implementation exists or the template is explicitly deferred with eligibility rules. |

Dependency graph: `visual_media_audit -> product_code -> serial_integrator`; `weather_chain_audit -> serial_integrator`; all lanes -> `merge_review`.
