# Участники событий и подписка на персон

> **Status:** UI и статический export-контракт реализованы в
> `feature/static-event-participants-20260727`; production-реестр и общий
> серверный счётчик лайков ещё не включены.
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
  `physical_visit_status=confirmed`, `eligibility_status=eligible`,
  непустым participant evidence и без отмены;
- портрет допускается только при `media_identity_status=verified`, разрешённом
  rights status и конкретных `credit_text` + HTTPS source URL;
- непринятый портрет заменяется инициалами, а не изображением из открытого
  интернета.

Экспортер не извлекает имена regex-ами из описания. Смысловое извлечение и
identity resolution остаются LLM-first/batch задачей; BGE подходит для поиска
кандидатов, дублей и транслитераций, но не является доказательством участия.

## Хранение и синхронизация

Для event-to-person relation канонический operational слой — Fly SQLite:
данные меняются вместе с событием и попадают в тот же консистентный snapshot
статической сборки. YDB не нужна в request path; она уместна для очереди
периодического CPU/BGE enrichment и run ledger. Публичные портреты должны жить
в Object Storage/CDN, а не раздувать каждую JSON-проекцию.

Batch matching может выполняться после Smart Update по расписанию. Smart Update
сохраняет source evidence, а отдельный Kaggle CPU + BGE pass сопоставляет имена
с реестром и пишет подтверждённую relation только после semantic/quality gate.

## Лайки и персонализация

В этой ветке сердце сохраняется на текущем устройстве в
`kenigevents:participant-likes:v1`. Отображаемое значение — build-time
`likes_count` плюс локальный выбор пользователя. Оно не выдаётся за глобальный
счётчик.

Следующий backend-этап:

1. анонимный/авторизованный `person_like` в Supabase;
2. идемпотентный toggle и агрегат публичного count;
3. выгрузка агрегата в статический participant DTO;
4. использование `liked_person_ids` как positive ranking signal в персональной
   ленте без жёсткого исключения остальных событий.

## KGD80

В репозитории сохранены два source-faithful portrait-canary из проекта
`/home/dev/projects/kdg80/site/public/generated/speakers/`:

- `/assets/participants/udovenko-tatyana.webp`;
- `/assets/participants/levchenkov-andrey.webp`.

Они нужны для интеграционных/визуальных проверок Татьяны Удовенко и Андрея
Левченкова. Полный KGD80 media catalog остаётся в исходном проекте и при
production-onboarding должен загрузиться в Object Storage с provenance, а не
копироваться целиком в bundle статического сайта.

Noindex specimen `/lab/event-participants/` проверяет оба viewport, headliner,
обычного спикера, initials fallback, публичный credit и состояние сердца.

## Не входит в эту ветку

- миграция и production seed всего реестра персон;
- автоматическое массовое извлечение из prose;
- серверный API и общий счётчик лайков;
- отдельная подборка и мобильное вложенное меню `К нам едут`.

Эта граница позволяет безопасно склеить законченный UI с отдельной data/backend
веткой, не протаскивая старую экспериментальную artist-arrivals реализацию в
актуальный `main`.
