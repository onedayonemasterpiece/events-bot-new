# Related events consolidation integration report

## Baseline

- Base: `origin/main@3d0af26cbe2d456da31ff9b029e063be9b69894c`
- Branch: `docs/related-events-consolidation-20260721`
- Mode: parallel read-only discovery, serial documentation integration
- Code/runtime edits: none

## Lanes

| Lane | Requirement IDs | Status | Evidence integrated |
| --- | --- | --- | --- |
| mobile/backend | R01, R05 | completed | current main manifest/ranker/feed/cache/backend status and gaps |
| listings | R02, R05 | completed | main plus V13–V28/Exhibitions behavior, preview status and conflicts |
| static event pages | R03, R05 | completed | main related UI, preproduction status, occurrence candidate and gaps |
| legacy | R04, R05 | completed | linked graph, Telegraph/TG/VK flows, old parser, graph-quality evidence |
| visuals | R07 | completed | accepted/candidate/lab/rejected screenshot provenance and render routes |
| branch history | R04, R06, R08 | completed | ancestry/ahead-behind matrix and safe docs-only base recommendation |
| closure review | R01–R08 | completed | corrected current-vs-target claims, legacy/preprod gaps, Telegraph status handling and screenshot provenance |

## Requirement closure

| ID | Status | Output |
| --- | --- | --- |
| R01 mobile feed | Done | `inventory.md`, `requirements.md`, screenshots |
| R02 listing pages | Done | source/branch matrix, conflicts, acceptance and screenshots |
| R03 static event pages | Done | current/preprod/candidate separation and surface contract |
| R04 old/alternative implementations | Done | Telegraph, TG/VK, parser, sparse, labs and branch history |
| R05 unified behavior | Done | `related-events-surface-v1`, relation taxonomy and gates |
| R06 consolidation branch | Done | docs-only branch from fresh `origin/main` |
| R07 screenshots | Done | curated durable pack under `docs/features/linked-events/screenshots/` |
| R08 one-time review checklist | Done | numbered requirements plus release acceptance matrix |

## Material open implementation gaps (not hidden by this docs task)

- stable root/static listing routes are not promoted;
- final occurrence selector branch is not in main;
- strict/broad candidates can still mix in the current preloader;
- broad/personal continuation lacks proven canonical family exclusion;
- stable occurrence group/provenance model and full graph repair are not built;
- durable browser-feedback analytics and optional listing RPC are not deployed
  end-to-end.

These are explicitly marked requirements/gaps rather than reported as shipped.
