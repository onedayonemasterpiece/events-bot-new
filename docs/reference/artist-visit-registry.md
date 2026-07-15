# Реестр артистов для выявления гастрольных приездов

Статус: **seed / не готов для автоматической публикации**. Это каноническая
справка по исходному XLSX, его нормализованной копии и правилам безопасного
определения `местный / приезжий`. Продуктовый дайджест описан в
[artist-arrivals-and-unusual-events.md](../backlog/features/static-typed-briefing/artist-arrivals-and-unusual-events.md).

## Назначение и ключевое ограничение

Файл `artifacts/kaliningrad_artist_registry_batch_001.xlsx` можно использовать
как **словарь кандидатов на идентичность**: он помогает заметить имя известного
артиста в событии и поставить его в очередь на проверку. Это не список
калининградских артистов и не готовый справочник географии.

Следовательно:

- совпадение с реестром не доказывает, что артист приезжий;
- отсутствие в реестре тем более не доказывает, что артист приезжий или
  неизвестный;
- гражданство, язык имени, источник поста и место проведения события не
  доказывают постоянную базу артиста;
- формула «приезжает в регион» допустима только после двух независимых
  решений: подтверждён `participant` конкретного активного события и есть
  row-level evidence базы/географии за пределами Калининградской области.

## Провенанс supplied workbook

| Поле | Значение |
|---|---|
| Artifact | `artifacts/kaliningrad_artist_registry_batch_001.xlsx` (не коммитится) |
| SHA-256 | `c40b238910d677c935d4c19bafb2d2f3fc14294d83193d27a6047f15075843ac` |
| Workbook `Generated_at` | `2026-07-15` |
| Основной лист | `Registry_Batch_001` |
| Строк сущностей | 1 235 |
| Уникальных `Registry_ID` | 1 235 |
| Уникальных `Match_Key` | 1 231 |
| Duplicate key groups / rows | 4 / 8 |
| Строк с aliases | 47 |
| Row-level Wikidata QID | 0 |
| Подтверждённая активность | 0 |
| `Last_Verified_At` | 0 |

В workbook шесть листов: `README`, `Registry_Batch_001`, `Source_Buckets`,
`Matching_Guide`, `Data_Dictionary`, `Audit_Checks`. Все 1 235 строк имеют
только один из шести bucket-level URL, а не индивидуальный источник о человеке:

| Source bucket | Строк |
|---|---:|
| Wikipedia actors categories + public seed | 456 |
| Wikipedia pop singers + Russian stage catalogs | 243 |
| Wikipedia list of People's Artists of the Russian Federation | 181 |
| Wikipedia TV presenters + comedians categories | 170 |
| Wikipedia Russian rappers category | 101 |
| People's Artists + classical public seed | 84 |

`Verification_Status`: 1 054 строки — `seed_needs_row_verification`, 181 —
`source_bucket_verified_row_needs_enrichment`. В `Audit_Checks` cached result
формулы `COUNTA(Wikidata_QID)` равен 1 235, хотя ожидаемое значение указано 0 и
все фактические ячейки пусты. Канонический профиль пересчитывает фактические
значения и фиксирует **0**; cached audit cell нельзя использовать как метрику.

Четыре неоднозначных ключа уже правильно помечены
`duplicate_match_key_review`: `александр васильев`, `влади`, `хамиль`, `змей`.
Ни один из них нельзя auto-match без второго идентификационного сигнала.

## Канонический формат

Нормализованный, пригодный для машинного чтения snapshot:

`docs/reference/data/artist_registry_batch_001.canonical.json`

Корень:

```json
{
  "schema_version": "kenigevents.artist_visit_registry.v1",
  "source": {},
  "safety_contract": {},
  "profile": {},
  "entities": []
}
```

Обязательные части entity:

| Блок | Поля / смысл |
|---|---|
| Identity | `registry_id`, `entity_type`, `display_name`, `canonical_name`, `aliases` |
| Routing | `primary_domain`, `monitoring_priority` — только приоритет очереди, не известность или качество события |
| Matching | `match_key`, `token_sort_key`, `ambiguity_flags`, `duplicate_match_key` |
| Evidence | bucket source, lookup URL, QID, verification/activity timestamps; текущий `evidence_level=bucket_seed` |
| Locality | `status`, country/region/city, basis, evidence, freshness; у всех seed-строк сейчас `status=unknown` |

Допустимые будущие locality status:

- `local_verified` — актуальная основная профессиональная/резидентная база в
  Калининградской области подтверждена row-level источниками;
- `non_local_ru_verified` — подтверждённая база в другом регионе России;
- `non_local_international_verified` — подтверждённая база вне России;
- `mobile_or_mixed` — несколько актуальных баз либо гастрольная модель без
  честной единственной базы;
- `unknown` — данных нет, они устарели или противоречат друг другу.

`locality.evidence[]` после enrichment должно содержать как минимум:
`source_url`, `source_kind`, `retrieved_at`, `claim_text`/hash, `geography_type`
(`professional_base`, `residence`, `official_bio_base`), извлечённые
`country_code`/`region_code`/`city`, `valid_from` и `valid_until`. Национальность
можно хранить отдельно, но она не участвует в locality decision.

## Воспроизводимое преобразование

Конвертер не требует `openpyxl`:

```bash
python3 scripts/convert_artist_registry_xlsx.py \
  artifacts/kaliningrad_artist_registry_batch_001.xlsx \
  docs/reference/data/artist_registry_batch_001.canonical.json \
  --expected-sha256 c40b238910d677c935d4c19bafb2d2f3fc14294d83193d27a6047f15075843ac

# CI/ревью: проверить, что committed snapshot не устарел
python3 scripts/convert_artist_registry_xlsx.py \
  artifacts/kaliningrad_artist_registry_batch_001.xlsx \
  docs/reference/data/artist_registry_batch_001.canonical.json \
  --expected-sha256 c40b238910d677c935d4c19bafb2d2f3fc14294d83193d27a6047f15075843ac \
  --check
```

Конвертер валидирует колонки, обязательные поля, уникальность ID и fail-closed
маркировку duplicate match keys. Он намеренно выставляет всем строкам
`locality.status=unknown`: XLSX не содержит доказательств географии.

## Safe identity → locality pipeline

1. **Participant extraction (LLM-first).** Из source bundle события выделяются
   персоны/группы и их роль. `исполняет`, `выступает`, `спикер`, `гость` не
   смешиваются с `фильм с участием`, `трибьют`, `произведения`, упоминанием в
   описании или записью трансляции.
2. **Candidate recall.** Unique exact canonical/alias match может создать
   identity candidate. Fuzzy, token-sort и vector/BGE дают только top-k.
3. **Disambiguation.** Однословные имена, duplicate key, неполные ФИО и
   конфликт домена требуют второго сигнала (row URL/QID, официальная афиша,
   проект/состав, изображение как вспомогательное evidence) и LLM verifier.
4. **Row enrichment.** Индивидуальные official/agency/encyclopedic sources
   подтверждают identity, актуальность и профессиональную базу. Bucket URL и
   search URL недостаточны.
5. **Locality decision.** Версионированный LLM contract получает только
   row-level source claims и возвращает один из пяти status с evidence IDs и
   сроком годности. Неуверенность/конфликт → `unknown`.
6. **Visit decision.** Отдельный verifier подтверждает физическое участие в
   активном событии региона. Locality и visit нельзя склеивать одним lexical
   правилом.
7. **Audit.** Сохраняются `event_id`, source revision/hash, extracted span,
   candidates/scores, selected `registry_id`, locality evidence IDs, model и
   prompt version, решение/reviewer action.

### Auto / review / reject

- `auto identity candidate`: unique exact alias/canonical hit, не duplicate,
  есть source-grounded participant role;
- `manual/LLM review`: fuzzy hit, 0.75–0.92, однословный alias, duplicate key,
  domain conflict или несколько кандидатов;
- `reject/unknown`: только similarity, имя отсутствует в реестре, нет роли,
  источник говорит о произведении/записи/трибьюте, нет актуального row-level
  locality evidence.

Даже `auto identity candidate` **не означает auto visiting artist**.

## Refresh и контроль качества

- append-only batch ingestion; ID не переиспользуются;
- row-level verification прежде всего для активных будущих matches, а не
  массовое обогащение всех исторических seed-строк;
- locality evidence пересматривается по `valid_until`, при конфликте сразу
  деградирует в `unknown`;
- monthly candidate/source refresh, quarterly verified-row audit;
- измерять entity-match precision/recall отдельно от locality precision и
  visit-role precision;
- публичный дайджест запускается только после размеченного eval и precision
  gate, описанного в продуктовом контракте.

