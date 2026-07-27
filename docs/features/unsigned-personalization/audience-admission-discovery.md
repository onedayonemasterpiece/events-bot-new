# Audience and admission discovery

Канонический продуктовый контракт для запросов вида «бесплатно», «с детьми» и
«бесплатно с детьми». Документ разделяет стоимость входа, пригодность для
аудитории и тематические интересы: эти сигналы нельзя хранить или ранжировать как
один общий тег.

## Решение

1. **Бесплатность — условие допуска, а не небольшой ranking boost.** Если
   пользователь явно запросил бесплатное событие, exact-выдача не может содержать
   платное или событие с неизвестными условиями. Регистрация, предварительная
   запись и донат остаются отдельными admission-подтипами.
2. **«С детьми» — доказательное audience-решение.** `0+`/`6+`, тема
   `FAMILY`/`KIDS_SCHOOL` или слово в заголовке сами по себе не доказывают, что
   событие предназначено детям. Broad regex в UI запрещён.
3. **Составной запрос использует логическое И.** «Бесплатно с детьми» требует
   одновременно подтверждённую admission-категорию и подтверждённую аудиторию.
   Ослабленные совпадения показываются отдельно как «Возможно, подойдёт», с
   объяснением, какое условие не подтверждено.
4. **Один предикат — несколько поверхностей.** Search, материализованные
   подборки, календарные переходы и персональная лента используют одну
   версионированную серверную проекцию. Popular сохраняет порядок по популярности
   и не превращается в каталог фильтров.

## Целевой data contract

Admission нормализуется независимо от тематики:

```json
{
  "admission": {
    "value": "free_open|free_registration|free_booking|donation|paid|unknown",
    "confidence": 0.98,
    "evidence": ["вход свободный, регистрация обязательна"]
  }
}
```

`is_free` сохраняется как совместимое производное поле для уже проверенных
`free_*`, но `unknown` fail-closed и не проходит явный free-фильтр.

Audience извлекается LLM-first из первичного источника:

```json
{
  "audience": {
    "value": "kids|family|none|unknown",
    "age_band": ["preschool|school_age|teen|all_ages"],
    "confidence": 0.91,
    "evidence": ["семейный мастер-класс для детей 7–12 лет"],
    "conflicts": []
  }
}
```

Юридическое возрастное ограничение — только supporting context. Оно не может
самостоятельно создать `kids`/`family`. Exact допускается при наличии evidence,
отсутствии конфликтов и confidence не ниже выбранного production threshold;
порог и версия решения сохраняются рядом с результатом.

## UX поверхностей

### Search

- естественная фраза остаётся главным способом ввода;
- после нормализации намерения под строкой видны применённые условия
  `Бесплатно` и/или `С детьми`, каждое можно снять отдельно; рядом есть
  компактный `Сбросить`, когда условий больше одного;
- условия не должны появляться из UI-regex: чип отражает серверное
  нормализованное намерение;
- exact и possible/fallback — визуально и семантически разные секции;
- zero state объясняет, какое условие слишком узкое, и предлагает снять его, но
  не ослабляет запрос молча.

Пока audience contract не экспортируется, публичный Search не показывает
ложный строгий facet `С детьми`. Редакционная noindex-подборка может оставаться
примером формулировки, но не должна называться полной автоматической выборкой.

### Static landing pages

- noindex release-candidate route `/podborki/besplatnye-sobytiya/` уже
  материализуется из полного DB-export по проверенному совместимому
  `ticket.is_free=true`; он сохраняет актуальные ongoing события по
  `end_date`, схлопывает только explicit occurrence families и не имеет
  искусственного лимита в 24 карточки;
- пункт `Бесплатно` в mobile menu обязан вести на эту готовую подборку и никогда
  не подставляет текст в Search. Пустой/неизвестный admission fail-closed;
- эта реализация достаточна для noindex candidate и пользовательской приёмки,
  но production-indexable `/besplatno/` разрешается только после регулярной
  серверной `admission.value` проекции, freshness gate и проверки полного
  состава. Совместимый `ticket.is_free` не подменяет целевой evidence contract;
- `/detyam/` или `/semeynoe/` материализуется только после audience quality gate;
- составная страница публикуется/indexes только при достаточном числе exact
  событий и гарантированной свежести. Query-param варианты до этого canonical/noindex.

For the R15 candidate, the Free collection has one stable visual identity at
both scroll states. The expanded shelf shows the accepted large Free medallion
on the right; the compact sticky shelf retains a smaller instance of that same
identity rather than switching to a generic text chip. The medallion decorates
already-proven admission data: it cannot make an `unknown` event free, change
ordering, or replace the materialized collection predicate. `Бесплатно`
remains a top-level fast action even when the menu also lists it inside
`Подборки`.

### Calendar, Popular, Personal

- Calendar может дать переходы `Бесплатно` и `С детьми` в discovery sheet или
  блоке продолжения, но не перегружает date strip;
- Popular остаётся popularity-ordered. Допустимы отдельные ссылки/полки внутри
  уже отфильтрованного множества, но не скрытая перестановка общего рейтинга;
- Personal хранит price preference, audience preference и semantic interests
  раздельно. Один лайк не превращается автоматически в постоянное предпочтение
  «бесплатное» или «детское».

## Medallions

- Принятый `free-listing-medallion.svg` используется только на уже согласованных
  listing-surfaces. В generic Search card достаточно честной admission-подписи;
  декоративный круг не добавляется без отдельной визуальной приёмки поверхности.
- Детский декоративный медальон **не выпускается сейчас**: в данных нет
  evidence-backed audience decision, а принятого asset нет.
- После quality gate возможен сначала текстовый pill `Детям` / `Для всей семьи`
  с доступным label. Маскот или немаркированная декоративная картинка не нужна:
  она сильнее обещает пригодность, чем объясняет её.

## Quality and analytics

Нужно считать компактными event-событиями, а не отдельной строкой БД на каждый
progress tick:

- `discovery_filter_apply` / `discovery_filter_clear` с `surface`, admission и
  audience version;
- `discovery_result_open` с `exact|possible`, rank и collection id;
- `discovery_zero_result`, `filter_clear_after_zero`, целевые CTA и
  `not_interested`;
- sampled precision и contradiction rate (`free`+явная цена,
  `kids/family`+`16+|18+`/adult conflict), unknown/evidence-missing rate и
  materialization freshness.

Engagement не является доказательством корректности детской пригодности.

## Consultant decision trace, 2026-07-21

Gemini 3.1 Pro (High) подтвердил hard constraints, разделение price/audience и
отсрочку детского медальона. Предложение вычислять детскую пригодность через
пересечение старых topics и `age_restriction <= 12` отклонено после аудита
данных: оба входа содержат подтверждённые ложные совпадения. Аналогично не принят
совет автоматически нести free-медальон в generic Search card: это нарушает
принятую границу listing/detail/search surfaces.
