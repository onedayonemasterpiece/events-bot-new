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

## Event-presentation follow-up — Actions 31278123911

The expanded accepted artifact contains 46 captures. All 23 desktop and all 23
mobile images were reviewed through labelled contact sheets. The five newly
reserved Event Detail pairs were also opened at original size:

| Exact specimen | Mobile | Desktop | Human observation |
|---|---:|---:|---|
| `akmal-svetlogorsk-7186` | reviewed | reviewed | Desktop split: large left poster/visual, right content, horizontal inline CTA. Mobile is a separate hero/card composition. |
| `kinopokaz-borba-za-bruklin-kaliningrad-7052` | reviewed | reviewed | Desktop editorial: wide landscape hero and an independent stacked CTA card at right. |
| `pechen-kaliningrad-7301` | reviewed | reviewed | Explicit portrait/square resolver state stays in the split layout with inline CTA. |
| `nauka-vsegda-kstati-progulka-s-uchenym-kaliningrad-6996` | reviewed | reviewed | Typed illustrated no-image fallback is visible; calendar CTA remains available. |
| `edit-piaf-na-balu-udachi-svetlogorsk-7048` | reviewed | reviewed | Desktop editorial hero/stacked CTA plus a companion board: one visibly larger poster at left and smaller remaining-photo previews at right. |

The structural outlier `muzykalnoe-loto-matershinnoe-kaliningrad-6258` was
also visible at both sizes and remained a split/document-or-unclassified-media
observation; its visual width did not override the exact runtime family marker.

No new blank/error/404 or viewport defect was found. The labs-preview-special
mobile shell-only observation was reopened at original size and remains the
same explicit lab conflict described above. Review is evidence only and does
not approve merging, normalization or defragmentation.
