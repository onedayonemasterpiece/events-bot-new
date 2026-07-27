# Участники событий и подписка на персон

> **Status:** UI, SQLite-реестр KGD80, Smart Update extraction/identity binding,
> статический export-контракт и сквозные person likes в personalization
> Supabase реализованы в `feature/static-event-participants-20260727`. Fuzzy
> BGE resolution для неизвестных персон ещё не включён.
>
> **Surfaces:** mobile и desktop event detail.

## Что реализовано

На странице конкретного события появился отдельный блок `Участники`. Он идёт
сразу после event-token medallions и не смешивает логотипы организаторов,
программ и способов оплаты с людьми.

Каждая карточка участника показывает:

- подтверждённый портрет либо fail-safe инициалы;
- имя и роль;
- отдельную метку `Хедлайнер` для `headliner` / `keynote`;
- кнопку-сердце и счётчик;
- ссылку на источник фотографии, когда экспортировалcя публичный портрет.

На desktop карточки образуют адаптивную сетку. На mobile это короткая
горизонтальная snap-лента под медальонами, до описания события. Кнопка лайка
имеет отдельный namespace и не меняет лайк самого события.

## Автоматический контур

Новый анонс не требует ручного переноса участника:

1. существующий semantic facts-pass Smart Update одновременно возвращает
   строгий `event_people` roster;
2. для каждой персоны он обязан указать точную цитату, роль, формат присутствия
   и billing (`headliner | featured | participant | unknown`);
3. валидатор принимает только дословно grounded quote и не «чинит» смысл
   детерминированно;
4. имя сопоставляется с alias-реестром KGD80;
5. подтверждённая связь записывается в SQLite
   `event_artist_appearance`, после чего обычная статическая сборка показывает
   карточку.

`headliner` не выводится из известности человека. Он появляется только при
явном главном billing в semantic decision. Автор произведения, герой лекции,
актёр в записи или организатор без личного участия не публикуются как
участники.

`roster_complete=true` позволяет идемпотентно снять ранее найденную связь, если
обновлённый источник больше не содержит человека. Неполный источник не удаляет
уже подтверждённый roster.

## Статический контракт

`PreviewEvent.participants` содержит:

```ts
type PreviewParticipant = {
  id: string;
  name: string;
  role: string;
  entity_kind: 'person' | 'group' | 'project';
  is_headliner: boolean;
  avatar_url: string | null;
  avatar_alt: string;
  likes_count: number;
  profile_url?: string | null;
  credit_text?: string | null;
  credit_url?: string | null;
  evidence_url?: string | null;
};
```

`site/scripts/export-production-preview-data.py` читает совместимый слой
`artist_registry_entity` + `event_artist_appearance`. Проекция fail-closed:

- старый snapshot без этих таблиц остаётся валидным и получает `participants=[]`;
- публикуются только `verified`-персоны с `confirmed` appearance,
  `physical_visit_status=confirmed|remote_confirmed`,
  `eligibility_status=eligible`,
  непустым participant evidence и без отмены;
- портрет допускается только при `media_identity_status=verified`, разрешённом
  rights status и конкретных `credit_text` + HTTPS source URL;
- непринятый портрет заменяется инициалами, а не изображением из открытого
  интернета.

Экспортер не извлекает имена regex-ами из описания. Смысловое извлечение
выполняется раньше, в Smart Update. Exact alias matching KGD80 — узкая
identity-операция, а не semantic classifier.

## Хранение и синхронизация

Для event-to-person relation канонический operational слой — Fly SQLite:
данные меняются вместе с событием и попадают в тот же консистентный snapshot
статической сборки. YDB не нужна в request path; она уместна для очереди
периодического CPU/BGE enrichment и run ledger. Публичные портреты должны жить
в Object Storage/CDN, а не раздувать каждую JSON-проекцию.

Для полного KGD80 exact aliases достаточно синхронного CPU lookup внутри Smart
Update: это дёшево и устраняет задержку для нового события. CPU+BGE остаётся
асинхронным batch-расширением для транслитераций, опечаток и неизвестных
зарубежных персон. BGE может предложить identity candidate, но не имеет права
сам подтвердить участие или headliner billing.

## Сквозные лайки и персонализация

Локального счётчика больше нет. Сердце работает с personalization Supabase и
привязано к стабильному `person_id`, а не к конкретному событию:

- один лайк Татьяне Удовенко виден на любом событии с ней;
- публичный `likes_count` общий для всех посетителей;
- собственное состояние синхронизируется между устройствами после входа через
  Яндекс;
- запись идемпотентная: браузер отправляет желаемое состояние `liked=true|false`,
  а не слепой toggle;
- после первого paint все персоны страницы читаются одним bounded RPC
  `get_person_like_snapshot_v1`; статическую страницу из-за каждого лайка
  перестраивать не нужно;
- UI обновляется оптимистично и сверяется с ответом сервера.

Хранение экологичное:

1. `personalization_person_like_state` — только текущая уникальная связь
   `(user_id, person_id)`, без истории кликов;
2. `personalization_person_like_counter` — один маленький агрегат на персону;
3. `personalization_person_like_subject` — allowlist публичных registry IDs;
4. raw append-only журнал не создаётся.

Прямой доступ браузера к таблицам закрыт RLS и grants. Публичный RPC возвращает
только агрегат и состояние текущего пользователя; write RPC доступен только
роли `authenticated`; anonymous Auth identities дополнительно отклоняются,
чтобы массовое создание временных аккаунтов не раздувало счётчик. При клике
без сессии сохраняется лишь короткоживущий pending intent в `sessionStorage`,
пользователь проходит Yandex login, после возврата выполняется тот же
идемпотентный RPC.

Новые registry IDs синхронизируются одной командой:

```bash
python scripts/sync_person_like_subjects_to_supabase.py
```

Следующий рекомендательный этап — использовать liked person IDs как positive
ranking signal без жёсткого исключения остальных событий.

## KGD80

В ветке находится полный текущий публичный каталог KGD80:

- **38 персон** — весь текущий registration roster плюс публичные profile
  identities — в `event_people/data/kgd80_people.json`;
- **40 портретов** в `site/public/assets/participants/`, включая альтернативные;
- полные имена, короткие варианты и обратный порядок имени/фамилии в aliases;
- регалии, source URL, credit и project provenance.

У 36 персон есть отдельные публичные KGD80-портреты. Для Владимира Чечко и
Шахнозы Усмановой исходный проект содержит тематические изображения событий,
но не отдельные портреты в speaker media manifest, поэтому их identities тоже
загружаются, а UI безопасно показывает инициалы вместо неверной фотографии.

`scripts/sync_kgd80_people_catalog.py` повторяемо пересобирает manifest и
копирует все WebP из checkout KGD80. На старте приложения
`ensure_kgd80_registry()` идемпотентно загружает manifest в SQLite; тот же seed
повторяется перед Smart Update identity binding. Поэтому обновление KGD80 —
один catalog sync, а не ручная операция на каждом новом событии.

```bash
python scripts/sync_kgd80_people_catalog.py --kgd80-root /path/to/kdg80
python scripts/seed_event_people.py --db /path/to/db.sqlite
```

Noindex specimen `/lab/event-participants/` проверяет оба viewport, headliner,
обычного спикера, initials fallback, публичный credit и состояние сердца.

## Не входит в эту ветку

- fuzzy/transliteration resolver и Kaggle CPU+BGE worker для персон вне exact
  KGD80 aliases;
- автоматическая публичная верификация ранее неизвестной персоны;
- отдельная подборка и мобильное вложенное меню `К нам едут`.
