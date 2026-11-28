# INC-2026-11-28 — full static site candidate disabled

Status: OPEN; registry inc_6511a56fcd9438abf39b87b8. Related historical vector incident: inc_95018d0f3af198afabe57c82.

## Evidence and scope

Production ba37eb9cd619f2ae6b02c923676808700ed49876 runs related-only; candidate 32973 / Event 11599 CREATED; vector job 173379 done; Kaggle job 172504 done; history 2023 reported related_ready and published=false. Effectful updates 11440 and 10739 also did not create an HTML candidate. Root still served September prelaunch content. Full HTML candidate restoration is distinct from root promotion and from the vector incident.

## Corrective change under review

Use existing production-candidate profile (full catalog). Reuse checked related graph artifact and cache for full results, retain existing strict VK first-publication rule and ready-work priority; enforce configured maximum debounce from first effect. Do not trust scoped graph metadata as a full-catalog result. No root, DNS, infra or publisher changes.

## Acceptance

Required CI, actual full ordinary production candidate (snapshot, exporter, root + secret + browser artifacts, related graph, semantic and browser gates), DOM/links, real accepted CREATE and UPDATE followed by next natural cycle, VK readiness and queue progress. Root promotion is a separate authorized operation. No acceptance or deployment claimed here.
