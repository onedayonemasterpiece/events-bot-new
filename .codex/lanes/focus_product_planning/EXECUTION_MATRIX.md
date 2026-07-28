# Focus-group product prototype — execution matrix

Base: `origin/main` at `9ee8f56f6e822542d9af62d7bf7c532d5e10e032`.
Scope boundary: product/page mechanics only; no live Supabase mutation, production build, deployment, mailing, or prize draw execution.

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Done when |
|---|---|---|---|---|---|---|
| R01 | Благодарность: отдельное честное сообщение о паре пригласительных на любой спектакль от «Акт-Опус», с партнёрским логотипом | onboarding/product copy | legal disclaimer, partner asset | low | product-docs | copy, states, partner attribution and non-biased eligibility are specified |
| R02 | Письма о системе и обновлениях готовятся и отправляются вручную | communications | member lifecycle | low | product-docs | manual templates/checklist exist; no automated sender is introduced |
| R03 | Share/QR invite enters a special auth page and records bounded preview access in localStorage | access/onboarding | R08 | medium | focus-shell | fragment intake, storage marker, share UI and safe URL cleanup work in browser prototype |
| R04 | `/` becomes a focus-group testing stub; the current site lives under a secret prefix | routing | R03 | high | focus-shell | root no longer forwards to a test page and secret entry is navigable only after preview marker |
| R05 | Active participant gets overall NPS and page feedback | feedback UX | R03/R08 | medium | focus-shell | shared accessible controls distinguish NPS, usefulness and improvement feedback |
| R06 | Automatic personalized selections have a separate opt-in and become eligible only after consent plus interpretable preference actions | personalization | R07 | medium | for-me | eligibility/off states and explainable local preview are visible without pretending a sender exists |
| R07 | `Для меня`: explicit category choices plus editable interest-strength visualization | personalization UX | none | medium | for-me | categories support like/neutral/not-for-me and confidence bars with accessible controls |
| R08 | Invite → badge/congratulation → email/Yandex auth choice → secret home redirect | onboarding | R03/R04 | high | focus-shell | full prototype journey and recovery states are represented without pretending mock auth is production auth |
| R09 | Focus programme can end automatically or by command; public launch preserves personalization | lifecycle/data ownership | R03/R06 | medium | product-docs | state machine and local-state continuity contract are documented and surfaced |
| R10 | Focus-only functions share a lab icon/badge selected from SVG Repo | design system | icon/license | low | focus-shell | licensed local asset, source metadata and reusable badge component exist |
| R11 | Implement page/product mechanics only, not a production focus-group build | delivery | all | high | integration | buildable prototype, no deploy/live DB/email side effects |
| R12 | Preserve prompt safety: invite is not auth, no feedback-to-prize bias, honest prototype/fail-safe behavior | security/product | all | high | integration | copy/tests/docs do not overclaim access, auth, NPS or production readiness |
