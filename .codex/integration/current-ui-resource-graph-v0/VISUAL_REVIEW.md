# Current UI Resource Graph v0 — manual visual review

## Review boundary

Human review was performed on all 40 final local v13 raster specimens: every
selected page at `390x844` and `1728x900`. Four labelled contact sheets were
reviewed first; suspicious or structurally important pairs were then opened at
their original resolution. This is human evidence alongside, not a replacement
for, viewport assertions, layout stabilization, perceptual fingerprints and
computed-style observations.

The review is observational. It does not authorize UI fixes, component merges,
normalization or defragmentation.

## Results

| Page family / state | Mobile | Desktop | Human observation |
|---|---:|---:|---|
| Artifacts | reviewed | reviewed | Content and empty-state cards visible; no error surface. |
| Closed poster | reviewed | reviewed | Compact diagnostic state visible at both sizes. |
| Collections | reviewed | reviewed | Collection title/explanation visible; no blank main. |
| Day listing — representative | reviewed | reviewed | Date header and event row visible. |
| Day listing — structural outlier | reviewed | reviewed | Alternate date/event row visible; same family retained. |
| Event detail — representative | reviewed | reviewed | Hero, CTA and event facts visible in independent layouts. |
| Event detail — structural outlier | reviewed | reviewed | Alternate media/copy state visible in independent layouts. |
| Exhibitions | reviewed | reviewed | Dark editorial/listing composition visible. |
| Favorites | reviewed | reviewed | Signed-out/empty state visible. |
| Festivals | reviewed | reviewed | Festival calendar/cards visible. |
| Focus group | reviewed | reviewed | Deliberately sparse diagnostic panel is visible, not an error page. |
| For Me | reviewed | reviewed | Personalization explainer/auth state visible. |
| Home | reviewed | reviewed | Hero and primary CTA/card visible in distinct compositions. |
| Interest clubs | reviewed | reviewed | Club summary and future-meetings regions visible. |
| Labs/preview special | reviewed | reviewed | **Viewport conflict:** mobile shows only shell/logo/bottom nav while desktop shows a full event page. Keep as lab evidence; do not call it a responsive variant. |
| Partners/partnership | reviewed | reviewed | Partnership content and CTA visible. |
| Popular | reviewed | reviewed | Popular rows/cards visible. |
| Search | reviewed | reviewed | Query surface and suggested collections visible. |
| Unusual | reviewed | reviewed | Empty/preparation state visible. |
| Weekend listing | reviewed | reviewed | Weekend/two-day composition visible. |

No reviewed production representative or structural outlier showed a 404,
browser error, missing viewport, or wholly blank primary content. The one
shell-only mobile specimen is the lab/preview conflict above and must remain an
explicit unresolved observation (`NOT_MERGED`).
