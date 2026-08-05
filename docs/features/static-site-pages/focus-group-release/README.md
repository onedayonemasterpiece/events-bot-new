# Release-контракт фокус-группы

> **Статус:** current owner-corrected companion к [`../focus-group.md`](../focus-group.md).
> **Старый anonymous-first flow superseded.**

## Каноническая модель

```text
invite -> browse without login
feedback surface visible but disabled
explicit email/Yandex authentication
safe return
authenticated idempotent feedback
```

## Документы

- [`nps-ui.md`](nps-ui.md) — page score, service NPS и auth-gated UI;
- [`status.md`](status.md) — честная матрица current/target;
- [`testing.md`](testing.md) — acceptance matrix;
- [`prize-rules.md`](prize-rules.md) — blocked rebaseline после перехода с 12 на 7 артефактов;
- [`../focus-group.md`](../focus-group.md) — продуктовый source of truth.

## Инварианты

- site access не требует login;
- feedback send требует explicit authentication;
- silent anonymous Supabase session запрещена;
- pre-auth feedback requests = 0;
- profile/Favorites/hidden recovery остаются раздельными;
- current artifact collection contains exactly seven existing artifacts;
- docs/test scaffold не считается runtime PASS.
