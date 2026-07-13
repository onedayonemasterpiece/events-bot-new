# Event image duplicate audit and release gate

> Status: **P0 public-release gate / current baseline unknown and reported degraded on 2026-07-13**. This is the canonical runbook and copy-paste prompt for auditing duplicate images in active/future event galleries. It is an investigation contract, not a cleanup script.

## Release invariant

For every event eligible for the public static release:

- one gallery must not contain the same image more than once, including different URLs, hosts, formats, resolutions, compression, crops or minor overlays of the same underlying visual;
- exact/mirror/re-encode duplicates are always defects;
- materially different posters/slides/photos may remain when each adds distinct event information or visual content;
- the same asset reused by **different** events is reported separately and is not automatically an intra-event defect;
- generated non-photo product slides such as «Как добраться» are typed separately and are never compared or counted as source event media.

Release candidate acceptance requires:

- `events_with_confirmed_intra_event_duplicates = 0` across the full eligible active/future inventory;
- `unreviewed_candidate_clusters = 0` and no unresolved public-gallery download/reference gaps;
- every new or changed multi-image event in the 14-day quality window passes the automated detector, with ambiguous clusters adjudicated before GO;
- confirmed root-cause families are fixed at ingest/Smart Update/persistence/render selection, replayed and re-audited; deleting duplicate DB URLs alone is mitigation, not closure.

## Existing regression contracts

The audit must apply, not duplicate, these incidents:

- [INC-2026-06-09 event media duplicates](../reports/incidents/INC-2026-06-09-event-media-duplicates.md): managed-storage plus source-CDN mirrors and outbound-surface dedup gaps;
- [INC-2026-05-11 poster near-duplicate and tram photo dropped](../reports/incidents/INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped.md): re-encoded/cropped near-duplicates, missing `phash`, and the danger of dropping semantically useful distinct media;
- [INC-2026-05-05 event-source media aggregation gap](../reports/incidents/INC-2026-05-05-event-source-media-aggregation-gap.md): source aggregation must add missing unique media without re-adding duplicate repost media;
- [INC-2026-06-07 TG event publishing media/calendar dedup](../reports/incidents/INC-2026-06-07-tg-event-publishing-media-calendar-dedup.md): persisted and published mirror dedup.

`EventPoster.poster_hash` is an exact persisted digest. `EventPoster.phash` and the canonical `p/dh16/.../<hash>.webp` path use the repository’s 256-bit dHash implementation in `media_dedup.py`. Neither field alone proves the final gallery is clean: legacy `Event.photo_urls`, absent hashes, cross-host mirrors, crops and render-only selection must also be checked.

## Copy-paste prompt for the current production DB audit

```text
Проведи полный READ-ONLY аудит дублей изображений у событий в текущей production БД events-bot-new. Цель — установить фактическую ситуацию перед публичным релизом, а не сразу чистить данные. Не ограничивайся одинаковыми URL, DB hash или pHash: пройди путь от точного SHA-256 байтов до обязательного визуального просмотра всех multi-image событий.

Incident context / regression contracts:
- docs/operations/event-image-duplicate-audit.md
- docs/reports/incidents/INC-2026-06-09-event-media-duplicates.md
- docs/reports/incidents/INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped.md
- docs/reports/incidents/INC-2026-05-05-event-source-media-aggregation-gap.md
- docs/reports/incidents/INC-2026-06-07-tg-event-publishing-media-calendar-dedup.md
Сначала открой эти документы и выпиши обязательные проверки. Используй fly-prod-db-access для production SQLite. Ничего не изменяй в production DB, Storage, Telegraph, Telegram, VK или static bucket.

1. Зафиксируй воспроизводимую базу
- git fetch origin --prune; запиши origin/main SHA и рабочий SHA;
- сними read-only snapshot /data/db.sqlite по канонической инструкции, не работай на живой БД длительным сканированием;
- запиши SHA-256 snapshot-файла, размер, UTC-время, max(event.added_at/доступный update marker), counts event/eventposter/event_source;
- сохрани всё только в artifacts/codex/event-image-duplicate-audit-YYYYMMDD/ и не коммить артефакты;
- секреты/токены/полные приватные payload в отчёт и stdout не выводи.

2. Определи точный release inventory
- найди и процитируй реальный predicate публичного static exporter, не придумывай новый SQL-фильтр;
- возьми все canonical, active, non-silent current/future события, которые должны попасть в static release, включая продолжающиеся события по end_date;
- отдельными знаменателями покажи: все eligible events, events с 0/1/2+ gallery refs, total refs, distinct URLs, rows eventposter, refs только в Event.photo_urls и rows только в eventposter;
- generated transport/share/medallion/product assets не смешивай с source event media.

3. Собери единый media ledger
Для каждого event сохрани event id/title/date/end_date/lifecycle/identity/silent/public URLs и упорядоченное объединение:
- Event.photo_urls с исходной позицией;
- EventPoster.id, catbox_url, supabase_url, supabase_path, poster_hash, phash, OCR title/text, updated_at;
- provenance: event_source/source URL, если связь восстанавливается;
- текущий URL/позицию в generated static gallery/manifest, если preview/current artifact доступен.
Не схлопывай данные на этом шаге: ledger должен показать расхождения между DB слоями.

4. Структурный и hash-аудит
Для каждого события проверь и отчитай:
A. один и тот же exact URL несколько раз;
B. URL после безопасной нормализации/redirect resolve указывает на один объект;
C. один supabase_path/object key повторяется под разными URL;
D. одинаковый EventPoster.poster_hash внутри события;
E. одинаковый phash/dh16 внутри события;
F. отсутствующий phash, phash/path mismatch, noncanonical path;
G. photo_count != фактическому числу gallery refs;
H. managed p/dh16 WebP + raw VK/TG/qTickets/другой CDN mirror одного изображения;
I. Event.photo_urls содержит дубль, которого уже нет среди deduped EventPoster, или наоборот.
Запусти существующий scripts/inspect/audit_media_dedup.py на полном релевантном окне как один источник evidence, но прямо перечисли его blind spots: он не заменяет per-event union ledger и визуальный аудит.

5. Скачай и посчитай фактические байты
- каждый distinct referenced URL скачай один раз с bounded concurrency, timeout/retry и локальным cache;
- сохрани final URL, HTTP/status, content type, byte size, width/height, format и sha256(downloaded bytes);
- не считай URL разными изображениями, если SHA-256 байтов совпал;
- download failure повтори через допустимый mirror (Catbox/Supabase/source), но не подменяй недоступный public ref локальным файлом без отметки;
- все недоступные ссылки вынеси отдельно: active public gallery ref без визуальной проверки является release blocker/evidence gap.

6. Построй perceptual candidates, не выдавая similarity за приговор
- пересчитай repo-compatible dHash16 через media_dedup.compute_dhash_hex для каждого скачанного изображения и сравни с EventPoster.phash/path;
- high-priority candidates: exact dHash или Hamming <= 6;
- secondary review candidates: Hamming 7..12, одинаковый/близкий OCR, одинаковая композиция при другой resolution/aspect, managed/source mirrors;
- добавь второй независимый visual signal для crop/watermark/overlay случаев (например pHash + image embedding/SSIM/feature matching), но используй его только для candidate recall;
- exact SHA-256 = confirmed exact duplicate; все perceptual/crop/overlay cases требуют визуальной классификации;
- пороги и версии алгоритмов запиши в manifest. Не удаляй изображения автоматически по distance.

7. Обязательный просмотр глазами — 100%, не sample
- создай contact sheet для КАЖДОГО eligible события с 2+ изображениями, а не только для найденных hash-кандидатов;
- на sheet подпиши event_id, title/date, позицию, host, EventPoster.id, первые 10 символов SHA-256 и phash, Hamming до ближайшего соседа;
- листы делай читаемыми, без растягивания, с отдельной страницей при большом альбоме;
- открой через view_image и последовательно отсмотри все sheets. В evidence ledger у каждого multi-image event должен быть visual_review_status и reviewer note;
- для каждого подозрительного cluster поставь ровно одну классификацию:
  * exact_duplicate — одинаковые байты;
  * mirror_or_reencode_duplicate — тот же визуал, другой host/format/compression/resolution;
  * crop_or_minor_overlay_duplicate — та же основа, crop/watermark/мелкий CTA не добавляет самостоятельной ценности;
  * legitimate_distinct — разные фото/постеры/слайды, каждый добавляет содержание;
  * semantic_conflict — похожая база, но разные дата/время/программа; не удалять, передать на event/source investigation;
  * unrelated_media — не дубль, но изображение не относится к событию;
  * unreviewable — не удалось получить/прочитать; release blocker до разрешения.
- OCR-различие само по себе не делает картинки разными: учти регрессию event 4727, где одна афиша была переоцифрована с разной OCR-ошибкой.
- не схлопывай действительно разные программные слайды и не теряй семантически важные фото (трамвай для события в театральном трамвае).

8. Проверь фактические public surfaces
Для каждого visually confirmed intra-event duplicate проверь текущую static event page/preview и, если применимо, Telegraph, @kldevents и managed VK через канонические инструменты. Раздели:
- duplicate_visible_now;
- stale_duplicate_only_in_DB;
- duplicate_introduced_only_by_renderer/publisher;
- DB duplicate hidden by final guard.
Не делай вывод о public impact только из DB.

9. Посчитай фактическую ситуацию
Отчёт обязан содержать абсолютные числа и доли с явными denominator:
- eligible events; multi-image events; total gallery refs;
- events/refs with missing phash, failed download, DB-layer mismatch;
- events with exact URL/path duplicate;
- events with exact byte SHA duplicate;
- events with exact/near dHash candidates;
- visually confirmed events and excess duplicate refs by classification;
- events visually clean; unreviewed/unreviewable count (для законченного аудита должно быть 0);
- public-visible duplicate events по static/Telegraph/TG/VK;
- cross-event reuse отдельным разделом, не смешивая с intra-event defect rate.
Покажи top affected event IDs с public links и contact-sheet paths, но не ограничивай аудит этим top.

10. Найди root-cause families, без repair в этом проходе
Для каждого confirmed cluster проследи, где дубль возник:
- один source album;
- одинаковое media из нескольких EventSource/repost;
- source CDN + managed-storage mirror;
- missing/wrong phash или near-duplicate threshold gap;
- _apply_posters / eventposter persistence;
- event-source rehydration;
- Event.photo_urls normalization;
- static exporter/gallery composition;
- Telegraph/TG/VK publisher-specific append.
Сопоставь с существующими incident contracts. Если найден новый failure family — создай/обнови incident record, но не мутируй данные и не заявляй fix.

11. Артефакты и финальный ответ
Создай:
- inventory.jsonl или CSV;
- downloaded-media manifest с hashes/dimensions/status;
- candidate-clusters.json;
- visual-review.csv со строкой на каждый multi-image event/cluster;
- contact-sheets/;
- report.md с Executive summary, methodology, denominators, findings, public impact, root-cause families, false-positive/false-negative controls и proposed repair order;
- минимальные redacted SQL/query outputs и точные команды.

В финальном ответе дай:
1) честный итог: сколько событий реально имеют дубли и сколько лишних изображений;
2) breakdown exact/mirror/reencode/crop/public-only/DB-only;
3) ссылки на affected public pages и artifacts;
4) какие existing incidents reopened/updated;
5) какие root causes требуют отдельных задач;
6) что проверено глазами 100% всех eligible multi-image events;
7) явно: production mutation = none.

Стоп-условия:
- не заканчивай на SQL GROUP BY URL/poster_hash/phash;
- не называй near-hash пару дублем без просмотра;
- не говори «проверено глазами», если не существует visual-review row для каждого eligible multi-image event;
- не исправляй БД/Storage/public posts в рамках аудита;
- если snapshot, URL или public surface недоступны, зафиксируй blocker и продолжи по остальным слоям, не занижая denominator.
```

## Expected follow-up after the baseline

The audit produces facts and root-cause clusters. Repair must then be split into bounded tasks:

1. deterministic exact/mirror canonicalization and idempotency;
2. perceptual near-duplicate candidate detection with conservative fail-closed behavior;
3. LLM/vision-assisted adjudication only for semantically ambiguous crop/overlay/program variants;
4. affected DB/public-surface repair;
5. source-boundary replay plus incident regression checks;
6. repeat the full inventory audit and prove the release invariant.

Manual visual review is required for the **current baseline** because the actual false-negative shape is unknown. The steady-state target is automatic detection/containment plus monitored ambiguous cases, not a permanent routine manual publication process.
