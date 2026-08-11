# Mandatory T01–T76 matrix

Exact wording is copied from section 22 of the attached P0/SEV-1 request.

| ID | Original test requirement | Primary lane | Status | Evidence |
| --- | --- | --- | --- | --- |
| T01 | Пост без keyword dictionary проходит в durable queue и LLM. | RAW-DISCOVERY | planned | pending |
| T02 | Пост без text-даты проходит LLM. | RAW-DISCOVERY | planned | pending |
| T03 | event_ts_hint=NULL проходит LLM. | RAW-DISCOVERY | planned | pending |
| T04 | Ошибочный past event_ts_hint проходит LLM. | RAW-DISCOVERY | planned | pending |
| T05 | Ошибочный too_far hint проходит LLM. | RAW-DISCOVERY | planned | pending |
| T06 | Телефон, похожий на дату, не уничтожает carrier. | RAW-DISCOVERY | planned | pending |
| T07 | Историческая дата плюс будущий анонс проходит LLM. | RAW-DISCOVERY | planned | pending |
| T08 | Первая дата прошлая, следующая будущая — LLM видит обе. | RAW-DISCOVERY | planned | pending |
| T09 | Дата только на афише — OCR → LLM. | RAW-DISCOVERY | planned | pending |
| T10 | Blank/photo-only carrier — OCR → LLM. | RAW-DISCOVERY | planned | pending |
| T11 | Длинный исторический текст с будущей лекцией в конце — LLM. | RAW-DISCOVERY | planned | pending |
| T12 | Длинный административный текст с мероприятием — LLM. | RAW-DISCOVERY | planned | pending |
| T13 | Crawl safety cap создаёт continuation, не cursor gap. | RAW-DISCOVERY | planned | pending |
| T14 | Startup crawl не rejects NULL hint rows. | RAW-DISCOVERY | planned | pending |
| T15 | Все OCR blocks multi-card поста входят в evidence. | SEMANTIC-PARSE | planned | pending |
| T16 | Длинный основной текст не приводит к полному удалению OCR. | SEMANTIC-PARSE | planned | pending |
| T17 | Отсутствие regex-фразы «расписание на карточках» не уменьшает OCR. | SEMANTIC-PARSE | planned | pending |
| T18 | Недоступна одна афиша — CONFIRMED_NO_EVENT запрещён. | SEMANTIC-PARSE | planned | pending |
| T19 | Positive event можно сохранить при частичном evidence, но carrier остаётся доступным для enrichment. | SEMANTIC-PARSE | planned | pending |
| T20 | Prompt truncation явно фиксируется и ведёт к retry/complete coverage. | SEMANTIC-PARSE | planned | pending |
| T21 | Valid CONFIRMED_NO_EVENT → допустимый product outcome. | SEMANTIC-PARSE | planned | pending |
| T22 | Пустой provider response → retry. | SEMANTIC-PARSE | planned | pending |
| T23 | Malformed JSON → repair/retry. | SEMANTIC-PARSE | planned | pending |
| T24 | Truncated output → continuation/retry. | SEMANTIC-PARSE | planned | pending |
| T25 | Schema mismatch → retry. | SEMANTIC-PARSE | planned | pending |
| T26 | Timeout/429/RPD → durable retry. | SEMANTIC-PARSE | planned | pending |
| T27 | Multi-event source возвращает все children. | SEMANTIC-PARSE | planned | pending |
| T28 | Несколько сеансов одного события создают occurrence children. | SEMANTIC-PARSE | planned | pending |
| T29 | Recap + future event сохраняет future event. | SEMANTIC-PARSE | planned | pending |
| T30 | Giveaway + отдельно описанное событие сохраняет событие. | SEMANTIC-PARSE | planned | pending |
| T31 | Mixed cancellation + new event обрабатывает оба результата. | SEMANTIC-PARSE | planned | pending |
| T32 | LLM no-event + сильные event signals → verification, не reject. | SEMANTIC-PARSE | planned | pending |
| T33 | LLM date противоречит OCR → verification. | SEMANTIC-PARSE | planned | pending |
| T34 | Несколько source dates + один child → verification. | SEMANTIC-PARSE | planned | pending |
| T35 | Generic title → grounded fallback/verification. | SEMANTIC-PARSE | planned | pending |
| T36 | Verification technical error → retry. | SEMANTIC-PARSE | planned | pending |
| T37 | Verification uncertainty → retry, не reject. | SEMANTIC-PARSE | planned | pending |
| T38 | Нормальный carrier выполняет один parse, а не два. | SEMANTIC-PARSE | planned | pending |
| T39 | Weak title не удаляет draft. | SEMANTIC-PARSE | planned | pending |
| T40 | Suspicious venue не удаляет draft. | SEMANTIC-PARSE | planned | pending |
| T41 | Нет regex-visible date — draft не удаляется. | SEMANTIC-PARSE | planned | pending |
| T42 | Past regex conflict — verification, не reject. | SEMANTIC-PARSE | planned | pending |
| T43 | Один проблемный child не удаляет siblings. | SEMANTIC-PARSE | planned | pending |
| T44 | Free-form reject_reason не управляет terminal outcome. | SEMANTIC-PARSE | planned | pending |
| T45 | 429 освобождает lease и назначает retry. | TPM-BACKPRESSURE | planned | pending |
| T46 | RPD exhaustion назначает retry после reset. | TPM-BACKPRESSURE | planned | pending |
| T47 | Несколько keys одного project не считаются разными TPM pools. | TPM-BACKPRESSURE | planned | pending |
| T48 | Burst 1.5x p99 не теряет carriers. | TPM-BACKPRESSURE | planned | pending |
| T49 | Unknown-date carriers не starvation. | TPM-BACKPRESSURE | planned | pending |
| T50 | Age-based fairness работает. | TPM-BACKPRESSURE | planned | pending |
| T51 | Backlog сокращается после восстановления quota. | TPM-BACKPRESSURE | planned | pending |
| T52 | Prefetch/main не дублируют LLM call. | TPM-BACKPRESSURE | planned | pending |
| T53 | Exact payload replay не вызывает LLM. | TPM-BACKPRESSURE | planned | pending |
| T54 | Output budget сохраняет все multi-event children. | TPM-BACKPRESSURE | planned | pending |
| T55 | Reservation error измеряется against actual usage. | TPM-BACKPRESSURE | planned | pending |
| T56 | Same source + same occurrence + same fingerprint → NOOP. | SMART-UPDATE-GATES | planned | pending |
| T57 | Same occurrence + changed data → MERGED/update. | SMART-UPDATE-GATES | planned | pending |
| T58 | Different occurrence IDs → separate events. | SMART-UPDATE-GATES | planned | pending |
| T59 | RELATED_BUT_DISTINCT → create. | SMART-UPDATE-GATES | planned | pending |
| T60 | FESTIVAL_CONTEXT_SIBLING → create. | SMART-UPDATE-GATES | planned | pending |
| T61 | Unsafe/incoherent merge → create distinct. | SMART-UPDATE-GATES | planned | pending |
| T62 | Identity technical failure → retry. | SMART-UPDATE-GATES | planned | pending |
| T63 | Diagnostic event_id не запускает side effects. | SMART-UPDATE-GATES | planned | pending |
| T64 | Reordered siblings сохраняют event identity. | SMART-UPDATE-GATES | planned | pending |
| T65 | Вставка нового первого sibling не сдвигает старые bindings. | SMART-UPDATE-GATES | planned | pending |
| T66 | Все старые prefilter rejects попадают в dry-run. | CENSUS-RECOVERY | planned | pending |
| T67 | Discovery misses включены в census. | CENSUS-RECOVERY | planned | pending |
| T68 | Partial low-confidence child loss восстанавливается. | CENSUS-RECOVERY | planned | pending |
| T69 | Повторный dry-run идемпотентен. | CENSUS-RECOVERY | planned | pending |
| T70 | Production DB остаётся byte/data unchanged. | CENSUS-RECOVERY | planned | pending |
| T71 | AST gate запрещает semantic terminal before LLM. | SMART-UPDATE-GATES | planned | pending |
| T72 | AST gate запрещает prefilter_obvious_non_events=True. | SMART-UPDATE-GATES | planned | pending |
| T73 | AST gate запрещает event_ts_hint как terminal eligibility gate. | SMART-UPDATE-GATES | planned | pending |
| T74 | AST gate запрещает technical mark_failed в automatic ingestion. | SMART-UPDATE-GATES | planned | pending |
| T75 | AST gate запрещает free-form reject_reason → product rejection. | SMART-UPDATE-GATES | planned | pending |
| T76 | AST gate запрещает diagnostic event_id → downstream success. | SMART-UPDATE-GATES | planned | pending |
