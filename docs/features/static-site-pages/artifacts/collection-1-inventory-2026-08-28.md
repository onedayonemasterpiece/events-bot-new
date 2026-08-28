# Первая коллекция артефактов — восстановленная owner-инвентаризация

Дата readback: 2026-08-28. Статус: focus-candidate SoT; `processed: NO` до повторной приёмки владельцем.

## Восстановленная иерархия

1. Owner decision `f5ea5e497a3c137e350645e0f6c35304853a8908`, `docs/features/static-site-pages/general-follow-up-code-agent-2026-08-04.md`.
2. Read-only reference-каталог `docs/features/static-site-pages/references/artefact-collection-1` из root checkout.
3. `docs/features/static-site-pages/references/artefact-locations.md` (все артефакты должны быть размещены; Янтарный космонавт может быть первой общей находкой).
4. Runtime и производные дизайн-артефакты.

Owner decision требует **ровно 7** готовых артефактов и прямо отвергает stale-модели на 5, 8 и 12 объектов. Поэтому прежняя Astro модель `1/5`, старый draft `1/8` и Focus eggs `12` не являются первой коллекцией.

## Exact source inventory

| # | artifact_id | public label from reference | source file | source dimensions | source SHA-256 | focus asset | focus asset SHA-256 |
|---:|---|---|---|---:|---|---|---|
| 1 | `amber_cosmonaut` | Янтарный космонавт | `amber-cosmonavt (3).png` | 1122×1402 | `17821f70d1e9dc3cdf5acacc0f44b9a1bd023b72ba868e71aa560cd5a438dede` | `amber-cosmonavt-3.webp` 576×720 | `42285e7b854e87b8ed146095ec56be92eda34919901a61fe318b50feb874f16f` |
| 2 | `baltic_light` | Балтийский маяк | `Baltic-light.png` | 2528×1684 | `45a03a16288bb8fc8e51a1631b1c36fda93ec3f71b35e7c78b1cbe10b46f2f6c` | `Baltic-light.webp` 720×480 | `989924293d3c92ffc101b581825d50c275837c36d378bb7f9c1dd1377a2f32f2` |
| 3 | `luise_queen_bridge` | Мост королевы Луизы | `Luise-queen-bridge.png` | 1792×2390 | `0618f5e721848ab67b772a18de460f93398462164772aa70e5a87323daa9d579` | `Luise-queen-bridge.webp` 540×720 | `8cbcfcc0b1cd73a386bcb083ff3f3dc2478da7da8d10ac187e4fcfe1fdeb71d3` |
| 4 | `marzipan_heart` | Марципановое сердце | `Marzypan-heart.png` | 1664×2566 | `7785ceaac1647d9704f3a6b7084b6bbacbcba8cc1017d46f51a72a1645d3d456` | `Marzypan-heart.webp` 467×720 | `d950c16842e1b1cc0b9278038c327244d83302af75cd9445ceee84c1baf408fd` |
| 5 | `sedov_bell` | Колокол «Седов» | `Sedov-bell.png` | 1844×2304 | `7f5c55fcda3182fc7d5985a87c785f5611a84fe47d6f21d6ea43303b022e789e` | `Sedov-bell.webp` 576×720 | `e9b1151c573f4513f13f676dfea0def1de25503bc3439416e74d4410c7d727bb` |
| 6 | `cosmonaut` | Космонавт | `cosmonavt.png` | 2048×2048 | `b26645b3b3a48fc391e024f84213cbb2a2a6f0e6cb5b4bd8e5b4c821de7e18fb` | `cosmonavt.webp` 720×720 | `8481df0cb55a75dbdda791f65e66699514860f1ce256a13c0c71757cf17bd219` |
| 7 | `old_brick` | Старый кирпич | `old-brick.png` | 1844×2304 | `0554d358b37e73f75fc35d898a6c288a2d09f24fac12d00646013a699ba04625` | `old-brick.webp` 576×720 | `bde5bd73e66299c87fe4ce016572ed97cc317dbe5288f0281c0300e042a78ea2` |

Производные WebP созданы из exact PNG через Pillow: RGBA, LANCZOS, bounding box 720×720, WebP quality 86, method 6, `exact=true`. Они предназначены только для noindex/focus candidate; права для public root не заявлены.

## Contract disposition

Canonical machine-readable registry: `site/src/data/artifact-collection-1.json`.

- `artifact_ids.length = 7`, identities уникальны;
- все семь source files имеют recorded SHA-256 и visual asset;
- placeholder `future_*` отсутствуют;
- оба визуально различных cosmonaut reference являются разными owner-counted identities; они не схлопываются в ошибочные «6 concepts»;
- исторические рассказы не выдумываются: текущий detail-copy описывает только owner reference и видимые признаки; полноценный fact-check остаётся отдельным редакционным gate;
- ordinary production остаётся fail-closed; public root не промотирован.

## Required state matrix

Для Astro и Penpot должны быть проверяемы:

- `none-found` / 0 of 7;
- `subset-found`;
- `all-found` / 7 of 7;
- pointer hover;
- keyboard focus;
- selected detail, desktop and mobile.

Статус owner comment OV-06 после материализации: `READY_FOR_OWNER_REREVIEW`, но `processed: NO` до фактической повторной проверки владельцем.
