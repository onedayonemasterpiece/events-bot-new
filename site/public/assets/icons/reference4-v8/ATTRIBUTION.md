# Reference 4 v8 icon sources

The implemented navigation uses one coherent **Phosphor Thin** family. The
family is MIT-licensed upstream; the linked SVG Repo item pages label the
individual downloads as CC0.

| Use | Asset | Source |
|---|---|---|
| Children | `baby-thin.svg` | [Phosphor Thin collection on SVG Repo](https://www.svgrepo.com/collection/phosphor-thin-icons/); exact SVG taken from upstream `@phosphor-icons/core` 2.1.1 (MIT), because the SVG Repo item page was checkpointed |
| Collections | `squares-four-thin.svg` | [Squares Four Thin #365765](https://www.svgrepo.com/svg/365765/squares-four-thin), CC0 |
| Unusual | `sparkle-thin.svg` | [Sparkle Thin #365749](https://www.svgrepo.com/svg/365749/sparkle-thin), CC0 |
| Clubs | `chats-thin.svg` | [Chats Thin #365240](https://www.svgrepo.com/svg/365240/chats-thin), CC0 |
| Exhibitions | `palette-thin.svg` | [Palette Thin #365613](https://www.svgrepo.com/svg/365613/palette-thin) |
| Festivals | `buildings-thin.svg` | [Buildings Thin #365181](https://www.svgrepo.com/svg/365181/buildings-thin) |
| Popular | `popular-trend-up-thin.svg` | [Trend Up Thin #365841](https://www.svgrepo.com/svg/365841/trend-up-thin) |
| Partners | `handshake-thin.svg` | [Handshake Thin #365460](https://www.svgrepo.com/svg/365460/handshake-thin) |
| Search | `search-thin.svg` | [Magnifying Glass Thin #365538](https://www.svgrepo.com/svg/365538/magnifying-glass-thin) |
| Personal | `user-focus-thin.svg` | [User Focus Thin #365857](https://www.svgrepo.com/svg/365857/user-focus-thin) |
| Favorites | `heart-thin.svg` | [Heart Thin #365473](https://www.svgrepo.com/svg/365473/heart-thin) |
| Share | `share-network-thin.svg` | [Share Network Thin #365704](https://www.svgrepo.com/svg/365704/share-network-thin) |

`Бесплатно` intentionally uses a project-native typographic `0 ₽` in a thin
circle. It is not an external pictogram: unlike a crossed ruble, it cannot be
misread as “rubles/cash prohibited”.

## Visually reviewed alternatives

- Popular: [Star Four Thin #365768](https://www.svgrepo.com/svg/365768/star-four-thin)
  is closer to the sparkle in the supplied reference but overlaps the existing
  “Для меня”/recommendation metaphor.
- Popular: [Solar Graph Up #525934](https://www.svgrepo.com/svg/525934/graph-up)
  communicates growth but is busier and more dashboard-like.
- Festivals: [Flag Banner Thin #365394](https://www.svgrepo.com/svg/365394/flag-banner-thin)
  is simpler, but the implemented architectural/gate allegory is closer to the
  supplied reference.
- Whole-family fallbacks: [Lucide Line](https://www.svgrepo.com/collection/lucide-line-icons/)
  (more robust but heavier) and [Solar Linear](https://www.svgrepo.com/collection/solar-linear-icons/)
  (more decorative but less semantically complete for this menu).

## R15 Collections curation

A local contact sheet compared Folder Thin, Squares Four Thin, Sparkle Thin and
Chats Thin at the same 96 px presentation before they were placed in the menu.
`Squares Four` was selected over `Folder` because four equal tiles read as a
curated set rather than file storage at small mobile size. `Sparkle` is distinct
from the existing Popular trend mark, and `Chats` expresses people meeting and
conversation without mixing icon families. All selected assets are unmodified
Phosphor Thin SVG Repo glyphs; CSS only scales and colors them.
