# Mandatory T01–T76 matrix

Exact wording is copied from section 22 of the attached P0/SEV-1 request.

| ID | Original test requirement | Primary lane | Status | Evidence |
| --- | --- | --- | --- | --- |
| T01 | Пост без keyword dictionary проходит в durable queue и LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T02 | Пост без text-даты проходит LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T03 | event_ts_hint=NULL проходит LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T04 | Ошибочный past event_ts_hint проходит LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T05 | Ошибочный too_far hint проходит LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T06 | Телефон, похожий на дату, не уничтожает carrier. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T07 | Историческая дата плюс будущий анонс проходит LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T08 | Первая дата прошлая, следующая будущая — LLM видит обе. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T09 | Дата только на афише — OCR → LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T10 | Blank/photo-only carrier — OCR → LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T11 | Длинный исторический текст с будущей лекцией в конце — LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T12 | Длинный административный текст с мероприятием — LLM. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T13 | Crawl safety cap создаёт continuation, не cursor gap. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T14 | Startup crawl не rejects NULL hint rows. | RAW-DISCOVERY | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_review.py`, `test_vk_intake_future.py`; RAW-VK 82-pass receipt |
| T15 | Все OCR blocks multi-card поста входят в evidence. | SEMANTIC-PARSE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_intake_poster_budget.py`, `test_source_parse_contract.py` |
| T16 | Длинный основной текст не приводит к полному удалению OCR. | SEMANTIC-PARSE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_intake_poster_budget.py`, `test_source_parse_contract.py` |
| T17 | Отсутствие regex-фразы «расписание на карточках» не уменьшает OCR. | SEMANTIC-PARSE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_intake_poster_budget.py`, `test_source_parse_contract.py` |
| T18 | Недоступна одна афиша — CONFIRMED_NO_EVENT запрещён. | SEMANTIC-PARSE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_intake_poster_budget.py`, `test_source_parse_contract.py` |
| T19 | Positive event можно сохранить при частичном evidence, но carrier остаётся доступным для enrichment. | SEMANTIC-PARSE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_intake_poster_budget.py`, `test_source_parse_contract.py` |
| T20 | Prompt truncation явно фиксируется и ведёт к retry/complete coverage. | SEMANTIC-PARSE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_intake_poster_budget.py`, `test_source_parse_contract.py` |
| T21 | Valid CONFIRMED_NO_EVENT → допустимый product outcome. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T22 | Пустой provider response → retry. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T23 | Malformed JSON → repair/retry. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T24 | Truncated output → continuation/retry. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T25 | Schema mismatch → retry. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T26 | Timeout/429/RPD → durable retry. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T27 | Multi-event source возвращает все children. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T28 | Несколько сеансов одного события создают occurrence children. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T29 | Recap + future event сохраняет future event. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T30 | Giveaway + отдельно описанное событие сохраняет событие. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T31 | Mixed cancellation + new event обрабатывает оба результата. | SEMANTIC-PARSE | Done | `test_source_parse_contract.py`, `test_event_parse_llm_first_contract.py`, Telegram producer contract |
| T32 | LLM no-event + сильные event signals → verification, не reject. | SEMANTIC-PARSE | Done | `test_event_parse_llm_first_contract.py`; closed seven-reason verifier cases |
| T33 | LLM date противоречит OCR → verification. | SEMANTIC-PARSE | Done | `test_event_parse_llm_first_contract.py`; closed seven-reason verifier cases |
| T34 | Несколько source dates + один child → verification. | SEMANTIC-PARSE | Done | `test_event_parse_llm_first_contract.py`; closed seven-reason verifier cases |
| T35 | Generic title → grounded fallback/verification. | SEMANTIC-PARSE | Done | `test_event_parse_llm_first_contract.py`; closed seven-reason verifier cases |
| T36 | Verification technical error → retry. | SEMANTIC-PARSE | Done | `test_event_parse_llm_first_contract.py`; closed seven-reason verifier cases |
| T37 | Verification uncertainty → retry, не reject. | SEMANTIC-PARSE | Done | `test_event_parse_llm_first_contract.py`; closed seven-reason verifier cases |
| T38 | Нормальный carrier выполняет один parse, а не два. | SEMANTIC-PARSE | Done | `test_event_parse_llm_first_contract.py`; closed seven-reason verifier cases |
| T39 | Weak title не удаляет draft. | SEMANTIC-PARSE | Done | `test_vk_auto_queue_import.py`, `test_vk_raw_first_llm_contract.py`, Smart Update hint-only rails |
| T40 | Suspicious venue не удаляет draft. | SEMANTIC-PARSE | Done | `test_vk_auto_queue_import.py`, `test_vk_raw_first_llm_contract.py`, Smart Update hint-only rails |
| T41 | Нет regex-visible date — draft не удаляется. | SEMANTIC-PARSE | Done | `test_vk_auto_queue_import.py`, `test_vk_raw_first_llm_contract.py`, Smart Update hint-only rails |
| T42 | Past regex conflict — verification, не reject. | SEMANTIC-PARSE | Done | `test_vk_auto_queue_import.py`, `test_vk_raw_first_llm_contract.py`, Smart Update hint-only rails |
| T43 | Один проблемный child не удаляет siblings. | SEMANTIC-PARSE | Done | `test_vk_auto_queue_import.py`, `test_vk_raw_first_llm_contract.py`, Smart Update hint-only rails |
| T44 | Free-form reject_reason не управляет terminal outcome. | SEMANTIC-PARSE | Done | `test_vk_auto_queue_import.py`, `test_vk_raw_first_llm_contract.py`, Smart Update hint-only rails |
| T45 | 429 освобождает lease и назначает retry. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T46 | RPD exhaustion назначает retry после reset. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T47 | Несколько keys одного project не считаются разными TPM pools. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T48 | Burst 1.5x p99 не теряет carriers. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T49 | Unknown-date carriers не starvation. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T50 | Age-based fairness работает. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T51 | Backlog сокращается после восстановления quota. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T52 | Prefetch/main не дублируют LLM call. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T53 | Exact payload replay не вызывает LLM. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T54 | Output budget сохраняет все multi-event children. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T55 | Reservation error измеряется against actual usage. | TPM-BACKPRESSURE | Done | `test_vk_raw_first_llm_contract.py`, `test_vk_auto_queue_rate_limit.py`, `test_google_ai_tpm_calibration.py` |
| T56 | Same source + same occurrence + same fingerprint → NOOP. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T57 | Same occurrence + changed data → MERGED/update. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T58 | Different occurrence IDs → separate events. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T59 | RELATED_BUT_DISTINCT → create. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T60 | FESTIVAL_CONTEXT_SIBLING → create. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T61 | Unsafe/incoherent merge → create distinct. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T62 | Identity technical failure → retry. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T63 | Diagnostic event_id не запускает side effects. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T64 | Reordered siblings сохраняют event identity. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T65 | Вставка нового первого sibling не сдвигает старые bindings. | SMART-UPDATE-GATES | Done | `test_smart_update_automatic_identity_resolution.py`, `test_smart_update_occurrence_stability.py`, caller AST |
| T66 | Все старые prefilter rejects попадают в dry-run. | CENSUS-RECOVERY | Done | `test_recover_smart_update_identity_losses.py`, `test_smart_update_loss_census.py`, migration + production RO hashes |
| T67 | Discovery misses включены в census. | CENSUS-RECOVERY | Done | `test_recover_smart_update_identity_losses.py`, `test_smart_update_loss_census.py`, migration + production RO hashes |
| T68 | Partial low-confidence child loss восстанавливается. | CENSUS-RECOVERY | Partial | `test_recover_smart_update_identity_losses.py`, `test_smart_update_loss_census.py`, migration + production RO hashes; planner selects partial-child loss, but historical raw/model replay is unavailable |
| T69 | Повторный dry-run идемпотентен. | CENSUS-RECOVERY | Done | `test_recover_smart_update_identity_losses.py`, `test_smart_update_loss_census.py`, migration + production RO hashes |
| T70 | Production DB остаётся byte/data unchanged. | CENSUS-RECOVERY | Done | `test_recover_smart_update_identity_losses.py`, `test_smart_update_loss_census.py`, migration + production RO hashes |
| T71 | AST gate запрещает semantic terminal before LLM. | SMART-UPDATE-GATES | Done | `test_vk_raw_first_llm_contract.py::test_static_vk_ingestion_bans_semantic_shortcuts`, `test_smart_update_caller_typed_contract.py` |
| T72 | AST gate запрещает prefilter_obvious_non_events=True. | SMART-UPDATE-GATES | Done | `test_vk_raw_first_llm_contract.py::test_static_vk_ingestion_bans_semantic_shortcuts`, `test_smart_update_caller_typed_contract.py` |
| T73 | AST gate запрещает event_ts_hint как terminal eligibility gate. | SMART-UPDATE-GATES | Done | `test_vk_raw_first_llm_contract.py::test_static_vk_ingestion_bans_semantic_shortcuts`, `test_smart_update_caller_typed_contract.py` |
| T74 | AST gate запрещает technical mark_failed в automatic ingestion. | SMART-UPDATE-GATES | Done | `test_vk_raw_first_llm_contract.py::test_static_vk_ingestion_bans_semantic_shortcuts`, `test_smart_update_caller_typed_contract.py` |
| T75 | AST gate запрещает free-form reject_reason → product rejection. | SMART-UPDATE-GATES | Done | `test_vk_raw_first_llm_contract.py::test_static_vk_ingestion_bans_semantic_shortcuts`, `test_smart_update_caller_typed_contract.py` |
| T76 | AST gate запрещает diagnostic event_id → downstream success. | SMART-UPDATE-GATES | Done | `test_vk_raw_first_llm_contract.py::test_static_vk_ingestion_bans_semantic_shortcuts`, `test_smart_update_caller_typed_contract.py` |
