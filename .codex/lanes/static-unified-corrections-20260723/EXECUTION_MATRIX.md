# Static unified prototype corrections — execution matrix

Base SHA: `5c2db86811c34355a1894748b87af73fdb5b19e3`

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Lane | Parallelizable? | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | Integrate the accepted exhibitions-personal prototype into the review `/vystavki/` page using real current data. | Exhibitions discovery | `site/src/pages/vystavki/`, exhibition surface/projection library, focused tests | Current real-data export; exhibition duplicate incident guard | Medium | L01 | Yes | Public review route uses the donor interaction/presentation contract without fixed July fixtures and passes dynamic projection checks. |
| R02 | Make `/dlya-menya/` use the accepted large-card/feed layout and crop contract rather than its bespoke compact grid. | Personal feed/cards | `site/src/pages/dlya-menya/index.astro`, shared feed layout tests | Canonical `EventCard`; R07 metadata policy | Medium | L05 | Yes | Personal page and Search share the large-feed container behavior; cards retain the canonical media resolver. |
| R03 | Hide Search skeleton before a query and restore accepted responsive input/button styling. | Search | `AuthorizedEventSearch.astro`, search CSS, focused search tests | Search incident regression contract | Medium | L02 | Yes | Initial generated page has no visible skeleton; runtime alone reveals it during a request; desktop/mobile controls follow one accepted visual contract. |
| R04 | Populate Clubs from a fresh real-data projection rather than an empty build-gate artifact. | Interest clubs | clubs data/export contract, preview hub copy, focused tests | Fresh production snapshot/export | Medium | L03 | Yes | Review artifact contains policy-current clubs and every event link resolves inside the prefix. |
| R05 | Add ИЦАЭ Калининграда to Partners with a local official asset and provenance. | Partners | `info-partners.ts`, partner assets/provenance, focused tests | Logo-use remains prototype-only pending partner authorization | Low | L03 | Yes | Sixth tile renders with local official SVG, correct official URL, deterministic placement and no hotlink. |
| R06 | Decide and implement a product breadcrumb contract for desktop/mobile from primary research plus Gemini Pro review. | Navigation/SEO/a11y | shared Breadcrumb component, top-level pages, event detail, SEO/tests/docs | L01/L02/L03/L05 merged first; verified Gemini Pro evidence | High | L06 | Serial | Decorative one-hop crumbs are removed; true deep pages use deterministic semantic paths; mobile uses one parent link and desktop uses an accessible ordered chain. |
| R07 | Prevent event 6686’s unclassified text-heavy poster from being destructively cropped and fail closed on semantic classification errors. | Export/media/hero | exporter, media resolver, EventHero/DesktopEventPage, fixtures/tests | LLM-first semantic reclassification is separate; visual incident guard | High | L05 | Yes | Missing/error semantic evidence exports `unknown`; card/hero/gallery contain it; classified visual-only negative controls still cover. |
| R08 | Replace irrelevant next-morning rail output with plausible post-event choices using a labelled predictive duration when no exact duration exists. | Transport | event transport lib/component, prototype duration estimates, tests/docs | Gemini Pro duration estimate with provenance; explicit-duration regression | High | L04 | Yes | 6529 shows an explicitly labelled estimate and several usable same-day returns near the inferred finish; no next-morning option; exact-duration events remain unchanged. |

Dependency order:

1. L01–L05 implement in isolated worktrees from the same base.
2. Integrator cherry-picks accepted commits.
3. L06 is implemented serially after those merges because it removes/rewrites navigation in routes owned by the other lanes.
4. Integrator owns shared generated-output gates, canonical docs, `CHANGELOG.md`, real-data rebuild, immutable preview publication and final Gemini Pro acceptance.

