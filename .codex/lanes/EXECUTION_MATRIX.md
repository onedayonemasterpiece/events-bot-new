# R3 execution matrix

Base: `feature/rzd-lastochka-medallion-20260723@68576d5b`

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Lane | Parallelizable? | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | Smart Update predicts missing duration into a separate forecast field only for transport-eligible events; transport may consume it | Smart Update, DB/export, transport | `smart_event_update.py`, `models.py`, `db.py`, export/transport modules, tests | none | high: schema/LLM contract | L01 | discovery parallel; write isolated | persisted forecast, extraction wins, eligibility gates LLM, transport fallback tested |
| R02 | Audit every organizer/festival manifest item, runtime/source/provenance and actual event resolution | medallions | manifests, assets, resolver, audit script/report | none | medium | L02 | yes | complete per-slug evidence table generated from current event set |
| R03 | Repair missing/false/ambiguous medallion matches while preserving fail-closed Unicode boundaries | medallions | resolver/manifests/tests | R02 findings | medium | L02 | serial after audit in same lane | mumod and all findings fixed with regression coverage |
| R04 | Playwright lab verification at 1440/390 with exact counts, no broken images/overflow | medallion lab | Playwright tests, lab page only if defect | R02/R03 | low | L02 then integration | yes after implementation | 27 organizer, 10 festival + 1 venue brand, required slugs, clean layout |
| R05 | Authorized Search acceptance using controlled test-only auth smoke, never a production bypass | search/auth tests | preview smoke scripts/tests | R10 donor integration | medium | L05 | yes | typing + authenticated result smoke passes without weakening public auth |
| R06 | Desktop leather tag preserves edging, readable fallback and refined shadow | desktop chrome/assets | layout/CSS, deterministic WebP, provenance/tests | R10 donor integration | medium | L03 | yes | visual QA shows full border at desktop and fallback remains readable |
| R07 | Remove every card path capable of visible fields; regress `Гоблинское сражение` | card media/layout | card component/media solver/tests | R10 donor integration | high | L03 | yes | reviewed page has cover-only visible pixels and automated no-field invariant |
| R08 | Restore keyboard shortcuts on clubs page | clubs/navigation | clubs page/shared keyboard module/tests | R10 donor integration | medium | L04 | yes | visible-order keyboard navigation and scoped hints work |
| R09 | Adapt club-card reference with real source images when available | clubs/data/UI | clubs data/page/assets/tests | R08 | medium | L04 | same lane | desktop cards use source-grounded covers and responsive design remains intact |
| R10 | Integrate current accepted R2 work into requested medallion base; build immutable linked preview | integration/release | selected donor commits, build/publish scripts | R01–R09 | high | INT | no | clean integrated build and immutable noindex preview with linked routes |
| R11 | Canonical docs, CHANGELOG, full coverage/final verification, agy Gemini Pro acceptance | docs/review | canonical feature docs, audit report, changelog | all | high | INT | no | all gates and consultant review recorded; all requirements reconciled |

Dependency order:

1. Map requested base and selected R2 donor commits.
2. L01–L05 work in isolated worktrees with disjoint ownership.
3. Integrator cherry-picks reviewed commits, resolves shared documentation centrally.
4. Full local/build/browser gates, immutable publish, then agy Gemini Pro acceptance.

