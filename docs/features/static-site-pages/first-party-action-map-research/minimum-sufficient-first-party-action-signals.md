# Минимально достаточные сигналы для собственной карты действий сайта
Краткий вывод
Для сайта событий я бы не строил «уменьшенную Метрику». Оптимальная архитектура — это first-party семантическая карта действий, где единицей анализа является не пиксель страницы и не «поведение сессии», а связка:

release/page version → archetype/layout → component contract/version → instance → semantic zone → action → expected effect → observed effect/performance.

Это принципиально меняет набор полезных сигналов. Самая сильная базовая комбинация — экспозиция зоны как denominator + дискретное действие + повторная попытка + ожидаемый результат компонента + задержка ответа + локальная нестабильность layout. Все эти данные можно связать с конкретным продуктовым решением, а не просто визуализировать как «горячее место».

Главные выводы исследования:

Клики и тапы нужны постоянно, но сами по себе недостаточны. Число кликов без denominator почти всегда двусмысленно: популярная зона может просто чаще попадать в viewport. Contentsquare поэтому отдельно считает exposure и, например, Attractiveness Rate относительно показов зоны; Hotjar также сопоставляет scroll/reach с interaction data. Это полезная операционная практика, хотя конкретные vendor-пороги нельзя переносить как универсальные нормы. 

Привязка к компоненту и semantic zone существенно полезнее абсолютных координат. Hotjar специально сохраняет координату клика относительно конкретного элемента, а не всей страницы; Clarity перешёл к element-based click maps именно для устойчивости к разным viewport, responsive design и состояниям страницы. Для вашей архитектуры это особенно сильный аргумент в пользу component_instance + semantic_zone + normalized_local_position, а не pageX/pageY как аналитической идентичности. 

Mouse movement нельзя считать направлением взгляда. Лабораторные исследования действительно находят корреляции между некоторыми mouse- и eye-показателями, особенно на уровне областей интереса и в определённых задачах, но зависимость зависит от пользователя, задачи и типа cursor behavior. В CHI-исследовании Huang, White и Buscher прямой тезис «cursor ≈ gaze» был поставлен под сомнение; последующая работа на 151 участнике и 18 динамических страницах также обнаружила систематические сходства, но не эквивалентность двух сигналов. 

Неподвижный указатель не означает ни «внимание», ни «затруднение» однозначно. Даже Contentsquare предупреждает, что длительный hover может означать как интерес, так и непонимание, а hesitation — как confusion, так и обычное потребление контента перед кликом. Это соответствует HCI-данным: динамика mouse dwell и eye fixation может иметь сходные тенденции, но это разные измеряемые процессы. 

Полные mouse trails не оправданы вашей задачей. Они дают существенно больше данных и приватностного риска, чем дополнительной решающей информации. Hotjar, например, для move heatmap дискретизирует положение мыши каждые 100 мс, то есть генерирует порядок величины больше событий, чем click/tap telemetry; независимая работа CHIIR показала, что mouse trajectories могут использоваться для поведенческого профилирования и предсказания демографических характеристик. 

rage click не следует трактовать как измерение гнева. Это эвристика. Hotjar, например, использует правило пяти кликов по одному элементу с интервалом до 500 мс и сам указывает на ложные срабатывания на каруселях; FullStory отдельно позволяет исключать элементы вроде календаря, где быстрое повторение является нормальным управлением. Clarity также прямо указывает, что dead interaction может происходить как из-за misleading UX, так и из-за высокой задержки. 

Поэтому итоговый принцип такой:

Постоянно собирать следует не «поведенческую насыщенность», а минимальный causal context вокруг значимых действий: возможность увидеть → попытка → результат → повтор → технические условия.

Vendor-документация ниже используется главным образом как подтверждение того, какие operational heuristics реально применяются в индустрии. Она не рассматривается как независимое доказательство психологических конструкций вроде «attention», «frustration» или «hesitation». В частности, утверждение Яндекс Метрики о том, что при затруднении движения мыши «как правило» хаотичны, следует считать продуктовой интерпретацией поставщика, а не достаточным научным основанием для такой классификации. 

Что действительно известно о спорных сигналах
Мышь и взгляд
Исследования mouse tracking дают более нюансированную картину, чем маркетинговое понятие «attention heatmap». В работе Huang, White и Buscher 36 участников выполняли 32 поисковые задачи; авторы изучали, в каких состояниях gaze и cursor действительно совпадают. Они обнаружили разные паттерны cursor behavior — чтение, hesitation, scrolling, clicking — и пришли к выводу, что прямую координату курсора нельзя универсально использовать как координату взгляда; модель, учитывающая контекст поведения, предсказывала gaze лучше, чем простая подстановка cursor position. 

Navalpakkam и соавторы показали, что некоторые агрегированные mouse measures действительно коррелируют с eye measures на нелинейных search layouts и позволяют оценивать распределение внимания между областями интереса. Но даже эта работа позиционирует mouse tracking как proxy, требующий моделирования контекста, а не как замену eye tracking. 

Более позднее исследование Milisavljevic и соавторов с 151 участником на 18 динамических веб-страницах обнаружило сходные temporal patterns некоторых eye- и mouse-показателей, но одновременно существенные различия между модальностями и зависимость от задачи. Это хороший аргумент за использование mouse data в специализированном эксперименте, но плохой аргумент за постоянную «карту внимания». 

Следовательно:

cursor location → user looked here — недопустимый вывод.

Допустим более слабый вывод:

cursor behavior over this component differed between variants/tasks, если заранее задана гипотеза и измерение ограничено desktop-кампанией.

Dwell, hover и hesitation
Указатель может оставаться над элементом потому, что пользователь читает его, собирается кликнуть, оставил мышь там после предыдущего действия, работает клавиатурой, смотрит в другую часть экрана или вообще временно не взаимодействует со страницей. Само отсутствие движения не различает эти состояния.

Это хорошо видно даже в vendor semantics. Contentsquare определяет Float Time как время hover над зоной, но прямо говорит, что большое значение может означать и интерес, и недостаточную ясность. Hesitation Time определяется как интервал между последним hover и первым кликом; документация также предупреждает, что большое значение может иметь позитивное или негативное объяснение и сильно искажается выбросами. 

Более того, Contentsquare считает hover даже без минимальной длительности, а затем интерпретирует его как признак «consumption». Это хороший пример того, почему vendor metric definition не должна автоматически становиться вашей ontology of attention. 

Поэтому для Product Atlas термин attention лучше вообще не выводить из pointer telemetry. Допустимые первичные факты:

zone_exposed, pointer_entered, pointer_dwell_ms, action_attempted.

Недопустимая автоматическая семантизация:

user_was_attentive, user_was_confused.

Rage, dead и повторные попытки
Здесь полезно разделить наблюдаемое поведение и интерпретацию.

Наблюдаемое:

attempt → attempt → attempt.

Интерпретация:

rage.

Первое является хорошей first-party telemetry. Второе — лишь гипотеза.

Hotjar считает rage-click определённой реализацией повторных кликов и документирует ложное срабатывание на каруселях; FullStory приводит аналогичный пример календарного виджета. Contentsquare Click Recurrence также прямо указывает, что recurrence > 1 может означать либо frustration, либо позитивное взаимодействие, например с каруселью. 

Именно поэтому компонентный contract должен знать, является ли действие:

single_shot, repeatable, stepper, carousel, drag, hold.

Тогда одна и та же последовательность из пяти действий может быть:

аномалией для buy_ticket;
нормой для calendar_next_month;
ожидаемой механикой для carousel_next.
Это намного надёжнее глобального rage threshold. Сам факт того, что FullStory предоставляет component-level exclusions для false positives, практически подтверждает необходимость такого контекста. 

Dead click также не должен определяться просто как «визуально ничего не случилось через N секунд». Clarity прямо перечисляет среди возможных причин dead interaction не только misleading UX или broken element, но и high latency. 

Лучше определять:

attempt_without_expected_effect

где ожидаемый эффект является частью component contract, например:

route_change, modal_open, filter_state_change, async_ack, download_started, external_navigation.

Как отделить UX-проблему от технической задержки
Это один из случаев, где browser performance APIs дают гораздо более сильный сигнал, чем дополнительное наблюдение за мышью.

W3C Event Timing API предназначен именно для измерения latency событий пользовательского ввода; он группирует связанные события в interaction и позволяет разделять время ожидания обработки, само выполнение обработчика и задержку до следующего визуального обновления. Это гораздо ближе к причинной диагностике «клик не сработал» по сравнению с произвольным rage score. 

Layout Instability API даёт LayoutShift entries и сведения об элементах, участвовавших в сдвиге; hadRecentInput позволяет отличать часть user-initiated shifts от неожиданных сдвигов. Это позволяет проверить сценарий «пользователь собирался нажать одну цель, но интерфейс сместился». При этом встроенное окно hadRecentInput — техническая семантика API, а не универсальный психологический порог для вашей аналитики. 

Практическая классификация может быть такой:

Наблюдение	Наиболее полезная интерпретация
Повторные попытки + высокий interaction latency + затем ожидаемый эффект	вероятнее всего performance-induced retry
Попытка + JS/network/application error	технический failure
Попытка + значимый local layout shift / target displacement	visual-stability / mis-targeting problem
Попытка + низкая latency + нет ожидаемого эффекта	сильный кандидат на affordance/interaction-contract проблему
Много попыток на repeatable control	может быть нормальным использованием
Много попыток на single_shot control без эффекта	высокий приоритет диагностики

FullStory также operationally объединяет frustration signals с network errors и page-performance evidence при поиске причин проблем, что поддерживает именно совместный, а не независимый анализ этих сигналов. 

Почему semantic zones лучше пикселей страницы
Здесь независимое HCI-доказательство менее прямое; это прежде всего сильный инженерный вывод из responsive UI и практики инструментов.

Pointer Events стандартизует единое понятие pointer для mouse, pen и touch и предоставляет target фактического события. 
 Hotjar сохраняет click position относительно target element, а Clarity прямо объясняет, что element-based mapping устойчивее абсолютной позиции при разных viewport, responsive layouts и page states. 

Для вашего случая значение абсолютной точки:

x=817, y=1324

почти исчезает после изменения header height, карточки, viewport или порядка блоков.

Зато:

event-card.v4 / zone=price / u=.82 / v=.44

остаётся сравнимым между экземплярами и часто между layout variants.

Поэтому координату стоит хранить только как локальное дополнительное свойство состоявшегося action, например:

[ u=\frac{x-x_{component}}{width_{component}},\qquad v=\frac{y-y_{component}}{height_{component}} ]

и, если высокая точность не нужна решению, сразу квантовать её в coarse bins. Это уменьшает стоимость и риск превращения системы в скрытый replay-layer.

Матрица сигналов и вердикты
ALWAYS здесь означает «часть минимального постоянного instrumentation слоя», а не «писать каждый DOM callback как отдельное событие». CAMPAIGN_ONLY — включать только для заранее сформулированной продуктовой гипотезы, ограниченных страниц/компонентов и периода. DO_NOT_COLLECT — дополнительная информация не оправдывает постоянную стоимость или риск для заявленной задачи.

Сигнал	Какое решение способен изменить и правильный denominator	Доказательность и типичные ложные выводы	Desktop / mobile	Нагрузка, privacy, обработка	Вердикт
Click / tap по semantic action	Проверяет affordance, выбор CTA, распределение взаимодействий по subzones. Основной показатель: views_with_action / eligible_exposed_views, а не число кликов. Contentsquare и другие zone analytics используют pageview/exposure denominators. 
Высокая как наблюдение, низкая как намерение. Клик доказывает действие, но не объясняет мотивацию или удовлетворённость.	Mouse click и touch tap объединять семантически, но хранить pointer_type; Pointer Events специально унифицирует эти input types. 
Низкая. Дискретные события, нет необходимости хранить DOM/text.	ALWAYS
Eligible exposure / impression semantic zone	Даёт denominator: видел ли пользователь вообще CTA, цену, дату, venue, filters. IntersectionObserver предназначен для наблюдения изменения пересечения target с viewport/root. 
Высокая для visibility, низкая для attention. Видимость ≠ чтение. Contentsquare использует собственный порог 150 мс — это vendor convention, а не универсальная норма. 
Одинаково необходимо. На mobile особенно важно из-за длинных страниц и меньшего viewport.	Низкая/средняя, если агрегировать state transitions, а не каждый callback.	ALWAYS
Visible time зоны	Помогает отличить «CTA не увидели» от «CTA был доступен, но его не выбрали» и сравнивать layout variants. Считать только пока Document visible; HTML Standard различает visible и hidden. 
Умеренная. Это время exposure, не attention. Vendor-интерпретации вида «11 секунд = content interested» слишком сильны для такого измерения. 
Одинаково применимо; mobile scroll чаще меняет экспозицию зон.	Низкая, если хранить суммарный интервал, а не heartbeat.	ALWAYS
Локальная позиция click/tap внутри component/zone	Проверяет, куда именно люди пытаются нажать внутри карточки: image/title/date/price/CTA; помогает расширить hit target или изменить clickable area.	Сильная инженерная обоснованность. Hotjar хранит координату относительно элемента, Clarity использует element-based mapping для responsive layouts. Абсолютная точка страницы намного менее переносима. 
На mobile особенно полезна для touch-target problems; на desktop — для ambiguous affordances.	Низкая при одном u,v на action; ещё ниже после coarse binning.	ALWAYS
Повторные попытки / inter-attempt interval	Определяет interaction cost и случаи, когда одного action недостаточно. Denominator: views_with_≥1_attempt, например views_with_retry / engaged_views.	Высокая как факт, умеренная как признак проблемы. Contentsquare прямо показывает двойную интерпретацию recurrence: frustration либо legitimate engagement. 
Полезно на обоих. Mobile retries особенно нельзя автоматически называть rage taps.	Низкая: нужны timestamps дискретных attempts в коротком view scope.	ALWAYS
Dead / no-effect interaction	Находит broken affordance, ошибочную clickable appearance, failure или отсутствие feedback. Denominator: только actions с объявленным expected_effect.	Умеренная, если определяется contract-based. Vendor dead-click heuristics двусмысленны: Clarity перечисляет broken UI, latency и misleading UX как разные причины одного сигнала. 
Одинаково важен click/tap. Touch feedback на mobile делает latency особенно заметной, но отдельного универсального threshold из этого не следует.	Низкая/средняя: требует effect observer.	ALWAYS, но только как производный expected_effect_missing
Rage-click / rage-tap label	Может быть triage-сигналом для конкретного компонента, если нужен быстрый поиск bursts.	Слабая как психологическая интерпретация. Hotjar использует собственный порог 5 кликов/500 мс и фиксирует false positives на carousel; FullStory документирует false positives для calendar controls. 
На touch и mouse одно и то же число repeats имеет разные motor/UI contexts.	Низкая при derivation из attempts; отдельный raw stream не нужен.	CAMPAIGN_ONLY; термин rapid_retry_cluster, не «гнев»
Interaction latency около action	Отделяет slow response от UX ambiguity; позволяет решить: менять backend/frontend performance, optimistic feedback или affordance.	Высокая техническая доказательность. Event Timing API создан для latency пользовательских interactions. 
Нужна на обоих input classes. Не смешивать device distributions без сегментации.	Низкая/средняя; хранить interaction summary, не continuous events.	ALWAYS
Layout shift около действия	Находит target displacement, поздний баннер/изображение, скачок карточки или CTA, способные сделать клик ошибочным.	Высокая техническая доказательность через Layout Instability API; sources и temporal relation дают гораздо более конкретную причину, чем mouse trajectory. 
Особенно критично на узких responsive layouts, но полезно на всех устройствах.	Низкая при сохранении агрегата/флага и manifest IDs вместо DOM snapshot.	ALWAYS, но только interaction-scoped
Semantic scroll reach	Решает, доходят ли пользователи до schedule, venue, FAQ, ticket CTA; denominator — eligible pageviews с такой зоной. Scroll maps у Hotjar/Yandex решают схожую задачу, но по физической глубине страницы. 
Высокая для reach, низкая для attention. Scroll depth говорит, что область была достигнута, а не прочитана.	На mobile абсолютные проценты страницы особенно плохо сравнимы с desktop; semantic-zone reach устойчивее.	Низкая, если выводить из exposure transitions.	ALWAYS
Fine-grained scroll stream: скорость, реверсы, oscillation	Может диагностировать поиск информации, особенно на длинной event detail page.	Умеренная/слабая. Vendor «excessive scroll = lost» является эвристикой; Clarity определяет его относительно expected average, а не как универсальный UX-факт. 
Механика wheel/trackpad и touch scrolling сильно различается. Pointer Events также различает типы устройств ввода. 
Средняя/высокая при raw stream.	CAMPAIGN_ONLY
Hover / pointer dwell	Может помочь проверить конкретную desktop-гипотезу: tooltip discovery, ambiguous card affordance, menu choice.	Слабая/умеренная. Hover нельзя однозначно переводить в attention; даже Contentsquare допускает interest и confusion как противоположные объяснения Float Time. 
Полноценного эквивалента hover для обычного direct-touch interaction нет; поэтому cross-device metric ломается концептуально. Pointer Events объединяет input API, но сами устройства остаются различными. 
Средняя, если писать enter/leave; высокая при pointermove.	CAMPAIGN_ONLY
Hesitation	Может проверять конкретную гипотезу: непонятна ли подпись CTA, ticket type или фильтр.	Слабая как общий construct. Contentsquare определяет hesitation через hover→click и сам предупреждает, что долгое значение может быть engagement или confusion и иметь большой variance. 
Desktop-определение через hover не переносится на touch. exposure→tap — уже другая метрика и тоже смешивает чтение, выбор и задержку.	Низкая из existing timestamps, но высокая аналитическая стоимость из-за неоднозначности.	CAMPAIGN_ONLY
Полный mouse trail / pointermove stream	Для заявленных решений почти нет уникальной задачи, которую нельзя решить click/exposure/retry/performance сигналами.	Недостаточно обоснован как gaze/attention proxy. HCI показывает контекстные корреляции, но не эквивалентность. Mouse trajectories также пригодны для behavioural profiling. 
Практически desktop-only construct; touch path представляет другую механику.	Высокая: Hotjar, например, sample-ит mouse position 10 раз/с. Повышенный privacy footprint. 
DO_NOT_COLLECT
Абсолютная page coordinate heatmap как источник истины	Может дать красивую визуализацию, но плохо отвечает на versioned component decisions.	Responsive/layout/page-state changes нарушают сопоставимость; element-relative mapping у Hotjar и Clarity специально решает эту проблему. 
Особенно ломается при сравнении desktop/mobile.	Низкая сама по себе, но стимулирует хранение лишнего raw positional data.	DO_NOT_COLLECT

Что не оправдывает сбор
Для постоянного слоя следует исключить полные mouse trails, raw pointermove streams, абсолютную историю координат страницы, «attention score» из мыши, универсальный rage score, универсальный hesitation score и high-frequency scroll history. Научные данные не позволяют надёжно превратить эти наблюдения в однозначные психологические состояния, а mouse trajectories дополнительно увеличивают privacy surface. 

Также не стоит сохранять отдельные производные вроде «user was confused» или «user was angry». Хранить следует наблюдаемые факты — retry, no_effect, latency, hover_duration в диагностической кампании — а интерпретацию формировать только на уровне конкретной продуктовой гипотезы. Vendor tools сами демонстрируют, насколько context-dependent эти эвристики: одинаковый recurrence может быть проблемой на CTA и нормальным использованием карусели. 

Минимальный MVP
Оптимальный MVP укладывается в девять сигналов и производных метрик. При этом raw-событий потребуется ещё меньше: многие показатели выводятся из одного action event, exposure state и outcome.

MVP-сигнал / показатель	Что сохранять	Основной показатель и denominator	Какое решение меняет
eligible_zone_exposure	zone_id, component/version, start/end visible interval, layout/release IDs	zone_reach_rate = exposed_views / eligible_views	переставить ли content/CTA, менять ли hierarchy
action_attempt	semantic action, click/tap, pointer type, component instance/zone, timestamp	action_view_rate = exposed_views_with_action / exposed_views	понятен ли affordance, какой subcomponent работает
local_action_position	normalized u,v или coarse cell внутри semantic zone	distribution только среди actions данной zone/version	расширить hit target, сделать card/row clickable, переставить CTA
retry_sequence	ordinal attempt и interval к предыдущей попытке в том же semantic action scope	retry_view_rate = engaged_views_with_retry / engaged_views; отдельно recurrence distribution	обнаружить лишние interaction steps или failure
expected_effect_outcome	success / no_effect / error / cancelled + contract effect ID	no_effect_rate = no_effect_attempts / effect-eligible_attempts	исправлять affordance, code path или feedback
interaction_latency	Event Timing summary для действия, по возможности input/processing/presentation decomposition	distribution/quantiles среди eligible attempts, а не глобальный «slow» threshold	performance fix vs UI redesign
near_action_layout_shift	aggregated shift around interaction, affected manifest zone/component	shift_affected_attempt_rate = affected_attempts / eligible_attempts	layout stability, placeholders, image/banner loading
semantic_scroll_reach	максимальная ключевая semantic zone, ставшая exposed; можно вывести из exposure events	reach(zone) / eligible_views	какая информация должна переместиться выше или стать sticky
visible_exposure_time	суммарное visible time ключевой зоны при document.visibilityState=visible	distribution conditional on exposure	отличить fleeting exposure от длительного availability, не называя это attention

IntersectionObserver предоставляет достаточно primitives для zone exposure, а HTML visibility state позволяет не считать background-tab time как видимость документа. 
 Event Timing и Layout Instability APIs обеспечивают performance/stability контекст без необходимости записывать replay. 

Каким должен быть component contract
Чтобы MVP действительно устранял statistics hell, contract должен задавать семантику до сбора, а не пытаться угадать её после.

Минимально достаточно иметь:

Поле contract	Назначение
semantic_action	например open_event, select_date, buy_ticket, open_venue_map
interaction_class	single_shot, repeatable, carousel, stepper, toggle, drag
expected_effect	navigation, state_change, modal_open, async_ack, external_navigation
effect_observer	как приложение доказывает, что effect произошёл
feedback_mode	immediate visual state / async loading / redirect
semantic_zones	стабильные именованные зоны контракта
local_geometry_policy	нужна ли координата и с какой дискретизацией
telemetry_policy	always, campaign, none для дополнительных сигналов

Тогда «dead» становится не универсальной попыткой определить, произошло ли хоть что-нибудь с DOM, а проверкой нарушения конкретного контракта. Это значительно сильнее vendor definitions, где «нет реакции в разумное время» неизбежно смешивает сломанный control, медленный server response и misleading affordance. 

Никаких универсальных порогов
Порог нельзя превращать в глобальное свойство аналитической платформы. Hotjar использует один конкретный rage algorithm — пять кликов с интервалом 500 мс — и одновременно документирует legitimate carousel false positives. 
 Contentsquare использует 150 мс для своей definition of exposure. 
 Эти значения полезны для понимания того, как vendors operationalize metrics, но не являются универсальными HCI-нормами.

Для вашей системы лучше хранить не флаг rage=true, а распределение inter-attempt intervals, а campaign analyzer уже задаёт порог для конкретного component contract и проверяет чувствительность результата к нему.

Аналогично вместо:

latency_bad = true

лучше сохранять достаточный summary и сравнивать распределения между version/layout/device cohorts. Event Timing как раз предоставляет измерение interaction latency без необходимости навязывать один продуктовый cutoff всем компонентам. 

Рекомендуемые диагностические кампании
Карточка события: где пользователь ожидает интерактивность
Гипотеза: разные части event card визуально воспринимаются как actionable не так, как определено interaction contract: люди пытаются нажать image, date/venue или пустую часть card, хотя clickable только title/CTA, либо наоборот.

На период одной-двух сопоставимых release cohorts к базовому MVP добавить на desktop pointer_enter/leave и coarse dwell только для отдельных subzones карточки. Постоянные mouse trails не нужны.

Основная карта анализа:

event-card contract/version × layout variant × semantic zone × exposed views × action attempts × no_effect × local u,v.

Зоны могут быть:

image, title, date_time, venue, price, badge, primary_cta, card_background.

Главные показатели:

[ ActionRate_z= \frac{\text{exposed views with ≥1 action on zone }z} {\text{eligible views exposed to zone }z} ]

и

[ NoEffectRate_z= \frac{\text{attempts without expected effect on }z} {\text{effect-eligible attempts on }z} ]

Hover/dwell здесь используется только как вспомогательная desktop evidence, а не как «куда смотрят». Исследования показывают, что cursor и gaze могут коррелировать в некоторых областях и задачах, но прямое отождествление невозможно; поэтому решение должно в первую очередь опираться на actual attempts и outcomes. 

Изменяемое решение: сделать всю карточку actionable или, наоборот, усилить границу CTA; изменить subzone hit areas; переставить title/date/venue; убрать ложные affordances.

После принятия решения hover instrumentation выключается.

Ticket / registration CTA: UX-проблема или медленный интерфейс
Это самая ценная кампания для сочетания behavioral и performance signals.

Для buy_ticket, register, select_ticket_type, select_date сохраняются:

attempt → expected feedback/effect → retry sequence → Event Timing → application/network outcome → local LayoutShift.

Цель — не получить «rage rate», а разделить по крайней мере четыре класса проблем:

performance-induced retry: пользователь повторяет действие, первоначальная попытка ещё обрабатывается, затем effect всё-таки происходит;

technical failure: attempt связан с JS/application/network error;

layout instability: target или связанный с ним content смещается около действия;

interaction/affordance failure: система технически responsive, но expected effect отсутствует или действие направлено на неинтерактивную semantic zone.

Clarity прямо признаёт high latency одной из причин dead click/tap; FullStory также объединяет frustration с errors/performance evidence. 
 Browser Event Timing и Layout Instability дают более непосредственную техническую проверку причин. 

Изменяемое решение: spinner/pressed state или optimistic feedback; блокировка double submit; frontend processing; backend latency; изменение ticket CTA; стабилизация layout; устранение misleading clickable surface.

После кампании special tracing network/application details можно сократить, сохранив постоянные summary outcome + interaction latency.

Достижимость ключевой информации на event detail
Для страницы события центральный вопрос обычно полезнее сформулировать не как «до какого процента страницы скроллят», а:

Какая доля eligible pageviews реально получила exposure ключевых semantic zones до того, как совершила или не совершила целевое действие?

Ключевые checkpoints:

hero/date, venue, ticket_summary, schedule, description, participants, faq, secondary_ticket_cta.

Постоянно достаточно semantic exposure/reach. На короткий diagnostic period можно дополнительно добавить coarse scroll direction/reversal transitions, например для проверки гипотезы, что люди многократно перескакивают между schedule и ticket information. Не нужен raw wheel/touch stream.

Физический scroll percentage плохо переносится между viewport и layout variants; даже Hotjar предупреждает, что scroll-map aggregation становится неточной при объединении страниц разной длины. 
 Именно поэтому здесь особенно выгоден ваш semantic manifest.

Главная метрика:

[ ReachRate(z)= \frac{\text{eligible views exposed to semantic zone }z} {\text{eligible views in archetype/layout variant}} ]

А для CTA:

[ ActionGivenExposure= \frac{\text{views exposed to CTA and acted}} {\text{views exposed to CTA}} ]

Это разделяет два принципиально разных случая:

низкий reach + хороший action-given-exposure → content/CTA стоит сделать доступнее;

высокий reach + низкий action-given-exposure → перемещение вверх само по себе, вероятно, проблему не решит.

Изменяемое решение: reorder event facts, sticky ticket CTA на mobile, перенос venue/date/price, сворачивание длинного description, изменение information hierarchy.

Передача результатов в Product Atlas и Resource Graph
Главное правило — ни Product Atlas, ни Resource Graph не должны становиться вторым хранилищем raw behavioral telemetry.

Product Atlas должен получать evidence, а не события
В Atlas следует передавать результат только тогда, когда он имеет:

decision/hypothesis → scoped evidence → denominator → comparison → decision implication.

Пример допустимого объекта:

text
Копировать
evidence:
  question: "Нужно ли сделать всю event card кликабельной?"
  scope:
    page_archetype: event_listing
    layout_variant: compact_grid_v3
    component_contract: event_card
    component_version: 4.2
    semantic_zone: card_background
    releases: [2026.08.1, 2026.08.2]
    device_class: mobile
  metric:
    name: no_effect_attempt_rate
    numerator: no_effect_attempts
    denominator: effect_eligible_attempts
  comparison:
    baseline_component_version: 4.1
  technical_context:
    interaction_latency_distribution: ...
    layout_shift_affected_rate: ...
  interpretation:
    hypothesis_supported: ...
  decision:
    candidate_change: expand_clickable_surface
Atlas не должен получать утверждение вроде:

«Карточка имеет высокий rage score».

Такое утверждение не содержит ни denominator, ни contract semantics, ни альтернативного объяснения. Vendor examples показывают, что repeated clicks сами по себе могут означать совершенно разные вещи в разных controls. 

Полезное правило качества Atlas:

Нет denominator или нет решения, которое может измениться, — нет Atlas evidence.

То есть:

523 clicks — не evidence.

18.4% exposed mobile views attempted venue text; 11.2% этих attempts не имели expected effect; latency была низкой — уже candidate evidence для изменения affordance.

Visible time также должен называться именно visible_exposure_time, а не attention_time: браузерные APIs могут подтвердить viewport/document visibility, но не направление взгляда. 

Resource Graph должен связывать evidence со стабильными объектами UI
В Resource Graph логично иметь узлы вроде:

text
Копировать
Release
PageVersion
PageArchetype
LayoutVariant
ComponentContract
ComponentVersion
ComponentInstance
SemanticZone
SemanticAction
ExpectedEffect
RenderManifest
Evidence
Decision
и связи:

text
Копировать
Release -> renders -> PageVersion
PageVersion -> realizes -> PageArchetype
PageVersion -> uses -> LayoutVariant
ComponentInstance -> implements -> ComponentVersion
ComponentVersion -> conforms_to -> ComponentContract
ComponentInstance -> contains -> SemanticZone
SemanticZone -> affords -> SemanticAction
SemanticAction -> expects -> ExpectedEffect
RenderManifest -> resolves -> ComponentInstance
Evidence -> concerns -> SemanticZone / ComponentVersion
Evidence -> observed_in -> Release
Evidence -> supports_or_challenges -> Decision
Это архитектурная рекомендация, а не стандарт индустрии. Она следует из вашей цели сделать данные сравнимыми между релизами и из практической устойчивости element/zone-based analytics к responsive layouts, которую используют Hotjar и Clarity. 

Особенно важно: raw coordinate не становится node identity. Координата является transient property конкретного action:

action → local_position(u,v),

а долговечная аналитическая сущность — semantic_zone.

Safe render manifest должен быть whitelist-only
В telemetry достаточно передавать только заранее разрешённую структуру:

text
Копировать
manifest_id
page_version
archetype
layout_variant
component_contract_id
component_version
instance_id
semantic_zone_id
semantic_action_id
geometry_class / local bounds if needed
Не следует автоматически копировать:

DOM HTML, текст формы, введённые значения, arbitrary attributes, CSS tree, пользовательский текст, полные URLs с query values или DOM snapshots.

Это также предотвращает постепенное превращение first-party action map в session-replay dataset. Приватностный аргумент особенно силён для непрерывных mouse data: исследования показывают, что trajectories могут быть использованы как поведенческий profiling signal, поэтому принцип data minimization здесь имеет не только архитектурное, но и эмпирическое основание. 

Version boundaries должны быть строгими по умолчанию
События разных component_version или layout_variant не следует автоматически объединять только потому, что DOM selector или URL совпал. Roll-up допустим лишь тогда, когда contract явно гарантирует сохранение той же semantic zone/action semantics.

Это устраняет одну из фундаментальных проблем обычной пиксельной heatmap: визуально агрегируется interaction с несколькими разными состояниями страницы. Clarity сам предупреждает, что click-map aggregation может включать pageviews, где показанный элемент не присутствовал, а element-based подход используется именно для лучшего соответствия состояниям UI. 

Если manifest не разрешился или version неизвестна, событие лучше пометить:

telemetry_scope=unresolved

и не смешивать с baseline.

Каждый показатель должен иметь обязательный denominator contract
Рекомендуемые определения:

text
Копировать
zone_reach_rate
= exposed_views(zone)
  / eligible_views(zone_present)

action_view_rate
= exposed_views_with_action(zone)
  / exposed_views(zone)

retry_view_rate
= views_with_second_or_later_attempt(action)
  / views_with_first_attempt(action)

attempt_recurrence
= total_attempts(action)
  / views_with_at_least_one_attempt(action)

no_effect_rate
= attempts_without_expected_effect
  / effect_eligible_attempts

technical_failure_rate
= attempts_with_confirmed_technical_failure
  / effect_eligible_attempts

layout_shift_affected_rate
= attempts_with_relevant_shift
  / layout_shift_observable_attempts
Это важнее самой визуализации. Например, Contentsquare Click Recurrence использует denominator pageviews with at least one click, а Exposure Rate — pageviews, благодаря чему два разных вопроса не смешиваются в одно число. 

click distribution = clicks(zone) / all clicks тоже может быть полезна для allocation questions, но она не отвечает на вопрос «какова вероятность взаимодействия после exposure»: Contentsquare отдельно определяет Click Distribution именно как долю всех кликов, иллюстрируя разницу между этими denominator semantics. 

В Atlas передаётся сравнение, а не абсолютный «плохой» score
Правильный объект evidence должен почти всегда сравнивать:

before / after, variant A / B, desktop / mobile, component v4.1 / v4.2, exposed / non-exposed, или latency cohort low / high.

Это особенно важно для hesitation, recurrence и scroll signals, где одно абсолютное значение не имеет стабильной семантики. Contentsquare прямо предупреждает о контекстной неоднозначности recurrence, float time и hesitation. 

Итоговая архитектурная позиция
Для этой задачи «карта действий» должна быть значительно ближе к versioned component observability for UX чем к традиционной behavioral heatmap.

Постоянный слой можно свести к четырём вопросам:

Было ли действие реально доступно?
Это eligible exposure, semantic reach и visible exposure time. IntersectionObserver и document visibility дают необходимые primitives без реконструкции сессии. 

Что именно пользователь попытался сделать?
Это semantic click/tap, component instance, zone и при необходимости локальная normalized position. Element-relative подход уже используется инструментами именно для устойчивости к responsive UI. 

Получил ли пользователь обещанный компонентом результат?
Это expected_effect_outcome, retries и recurrence — без эмоционального ярлыка rage. Документация Hotjar, FullStory и Contentsquare показывает, что одинаковые repeat patterns имеют как негативные, так и совершенно легитимные объяснения. 

Если результата не было или пришлось повторять — виноват interface contract или execution?
Здесь нужны Event Timing, application outcome и local LayoutShift, а не дополнительное наблюдение за курсором. 

Таким образом, рекомендуемая политика сбора выглядит так:

ALWAYS: click/tap, semantic exposure, visible exposure time, normalized local action position, retries, contract-based no-effect outcome, interaction latency, action-scoped layout shift, semantic scroll reach.

CAMPAIGN_ONLY: hover/dwell, hesitation, fine-grained scroll dynamics, neutral rapid_retry_cluster derivation.

DO_NOT_COLLECT: full mouse trails, persistent pointermove stream, absolute page-coordinate history как основа анализа, mouse-derived gaze/attention map, универсальные rage/hesitation/excess-scroll scores.

Это сохраняет именно ту часть heatmap-подхода, которая способна менять решение, и отбрасывает сигналы с худшим соотношением интерпретируемость / стоимость / privacy / риск ложного вывода. Независимые HCI-исследования особенно убедительно поддерживают осторожность с mouse-as-gaze; browser standards дают намного более сильные и непосредственно причинные инструменты для visibility и responsiveness; а документация самих аналитических vendors показывает, насколько их frustration/attention heuristics зависят от контекста и требуют исключений. 