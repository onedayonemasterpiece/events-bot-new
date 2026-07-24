# Festival category icons

The festival calendar uses a coherent baseline from SVG Repo's
[Lucide Line Icons](https://www.svgrepo.com/collection/lucide-line-icons/),
plus two visually reviewed semantic exceptions for jazz and theatre.

| Use | Local file / shared-library id | SVG Repo source |
| --- | --- | --- |
| Literature | `389049-book-open.svg` / `svgrepo-389049-book-open` | [Book Open #389049](https://www.svgrepo.com/svg/389049/book-open) |
| Cinema | `389059-camera.svg` / `svgrepo-389059-camera` | [Camera #389059](https://www.svgrepo.com/svg/389059/camera) |
| Gastronomy | `389063-carrot.svg` / `svgrepo-389063-carrot` | [Carrot #389063](https://www.svgrepo.com/svg/389063/carrot) |
| History | `389241-history.svg` / `svgrepo-389241-history` | [History #389241](https://www.svgrepo.com/svg/389241/history) |
| Travel | `389291-map-pin.svg` / `svgrepo-389291-map-pin` | [Map Pin #389291](https://www.svgrepo.com/svg/389291/map-pin) |
| Maritime | `389003-anchor.svg` / `svgrepo-389003-anchor` | [Anchor #389003](https://www.svgrepo.com/svg/389003/anchor) |
| Author song / voice | `389302-mic.svg` / `svgrepo-389302-mic` | [Mic #389302](https://www.svgrepo.com/svg/389302/mic) |
| Music | `389324-music.svg` / `svgrepo-389324-music` | [Music #389324](https://www.svgrepo.com/svg/389324/music) |
| Art and culture | `389330-palette.svg` / `svgrepo-389330-palette` | [Palette #389330](https://www.svgrepo.com/svg/389330/palette) |
| General and family | `389439-star.svg` / `svgrepo-389439-star` | [Star #389439](https://www.svgrepo.com/svg/389439/star) |
| Theatre | `389461-ticket.svg` / `svgrepo-389461-ticket` | [Ticket #389461](https://www.svgrepo.com/svg/389461/ticket) |
| People and community | `389494-users.svg` / `svgrepo-389494-users` | [Users #389494](https://www.svgrepo.com/svg/389494/users) |
| Jazz | `120598-saxophone.svg` / `svgrepo-120598-saxophone` | [Saxophone #120598](https://www.svgrepo.com/svg/120598/saxophone) |
| Theatre | `103262-theatre-masks.svg` / `svgrepo-103262-theatre-masks` | [Theatre Masks #103262](https://www.svgrepo.com/svg/103262/theatre-masks) |

The anchor, mic and users assets extend the same Lucide Line family. Saxophone
and theatre masks are deliberate semantic exceptions selected after visual
comparison at 8, 11 and 14 pixels: they are substantially easier to recognise
than generic music and ticket symbols at the full card size. Compact layouts
fall back to one primary glyph.

All fourteen assets are listed by SVG Repo under the **CC0 License**. Their SVG
geometry is unmodified. Runtime CSS uses each file as an alpha mask so it can
inherit the category-chip colour without rewriting the source.
