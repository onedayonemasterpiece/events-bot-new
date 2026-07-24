# Festival social-video audit — 2026 calendar

> **Audited:** 2026-07-24
> **Scope:** 21 cards in `festivalTimeline.ts`
> **Method:** approved Telegram human session plus authenticated VK API,
> read-only. No S22 session, public VK HTML, downloads or social mutations.

This is a source-suitability inventory for a future poster-first,
click-to-load player. It is not a current embed list. Archive assets, heavy
crop, repost ownership and platform embed policy must be rechecked before
release. Autoplay is not allowed.

| Festival | Verdict | Edition | Exact source | Source media | Formation note |
| --- | --- | --- | --- | --- | --- |
| City Jazz | Conditional | archive 2025 | [VK post](https://vk.com/wall-7500115_9169) / [video](https://vk.com/video-7500115_456239268?list=6e080f5c07e279c357) | 1920×1080, 3:06 | Good horizontal recap, but ~41% crop in the 3:1 desktop card. A current [2026 Telegram clip](https://t.me/jazzfestivalru/1199) is portrait. |
| Соседи | No usable clip | — | Official/organiser VK feed inspected | 0 exact videos | Do not invent a player. |
| Гроздь | Conditional | **current 2026** | [Telegram](https://t.me/festivalgrozd/192) | 848×464, 2:25 | Exact invitation; use contained playback/overlay. |
| Море внутри | Conditional | **current 2026** | [VK post](https://vk.com/wall-195754292_11458) / [video](https://vk.com/video181167156_456239530?list=7534b983143886147d) | 720×1280, 0:15 | Short portrait invitation; repost-owned media requires availability recheck. |
| Большой Кауп | Conditional | archive 2025 | [Telegram](https://t.me/kaupfest/1050) | 720×1280, 0:20 | Exact portrait invitation; no exact named 2026 video confirmed. |
| Короче | Conditional | **current 2026** | [Telegram](https://t.me/korochefest/1309) | 720×1280, 0:06 | Exact date/open-call motion asset, not an atmosphere recap. |
| Мимикрия | Conditional | archive 2024 | [Telegram](https://t.me/kldzoo/4121) | 720×1280, 0:14 | Correct organiser; old portrait winners clip. |
| Территория мира — Территория музыки | **Strong** | archive 2025 | [Telegram](https://t.me/sobor39/5234) | 1280×720, 3:06 | Exact official report; ~18% desktop crop. |
| Street Food | Conditional | archive 2025 | [VK post](https://vk.com/wall-73659736_3970) / [video](https://vk.com/video-73659736_456239233?list=3d6792325547f53ee9) | 3840×2160, 1:34 | High-quality exact aftermovie; needs separate square poster and uncropped player. |
| Жили-были | Conditional | archive 2025 | [VK post](https://vk.com/wall-48855901_10563) / [video](https://vk.com/video-48855901_456239667?list=20573349b5e8efd086) | 3840×2160, 1:53 | Exact organiser recap; crop-heavy in the near-square card. |
| Народов много — Родина одна | No usable clip | — | Official VK/TG feeds inspected | 0 exact videos | An unrelated broad VK hit was rejected. |
| ВитаЛики | No usable clip | — | Official municipality VK/TG feeds inspected | 3 exact text posts, 0 video | Do not invent a player. |
| Водная ассамблея | **Strong** | archive 2025 | [Telegram](https://t.me/world_ocean_museum/10777) | 1280×720, 1:24 | Exact report and good wide-row/mobile fit. |
| Шедевры мировой классики | Conditional | archive 2024 | [VK post](https://vk.com/wall-104963527_10614) / [video](https://vk.com/video-104963527_456241538?list=e4bde2a54cd6bc1616) | 1280×720, 3:18 | Shorter than newer full streams, but old and crop-heavy. |
| Дни литературы | Conditional | archive 2024 | [Telegram](https://t.me/kaliningradlibrary/1322) | 848×464, 0:56 | Exact concise recap; needs separate poster/uncropped playback. |
| Тыквенный пир на весь мир | Conditional | archive 2025 | [Telegram](https://t.me/yantarnayatikva/331) | 480×848, 0:43 | Exact portrait recap; contained player only. |
| Острова | Conditional | archive 2025 | [Telegram](https://t.me/festostrova/903) | 1920×1080, 1:50 | Exact aftermovie; unsafe to crop while playing. |
| Клуб путешественников | **Strong** | archive 2024 | [Telegram](https://t.me/world_ocean_museum/8329) | 1280×720, 1:15 | Exact recap and good wide-row/mobile fit. |
| В единстве наша сила | No usable clip | archive 2021 rejected | [VK evidence](https://vk.com/video-177327805_456239188?list=a9d2e05b842cb41e01) | 800×450, 27:24 | Exact title but too old and too long; no newer exact clip confirmed. |
| Джаз в Филармонии | **Strong** | archive 2025 | [Telegram](https://t.me/filarmonia_39/1836) | 1280×694, 0:30 | Exact concise opener and close formation fit. |
| Декабристские вечера | Conditional | archive 2025 | [VK post](https://vk.com/wall-104963527_11808) / [video](https://vk.com/video-104963527_456242020?list=e6897c1130f4e405f9) | 1920×1080, 3:35 | Exact latest-cycle report; too narrow for the 3:1 desktop frame. |

## Release contract

1. Keep the existing local cover as the poster; do not download social video
   during static build.
2. Fetch/embed only after explicit click. Never autoplay or preload full video.
3. Show the full uncropped frame in a contained overlay/player; the card crop is
   a poster treatment, not a playback crop.
4. Label archive footage and never imply that it depicts the 2026 edition.
5. Recheck public availability, ownership/repost status and embed policy shortly
   before release.
6. The four `No usable clip` rows remain static images until a confirmed
   organiser-owned source appears.
