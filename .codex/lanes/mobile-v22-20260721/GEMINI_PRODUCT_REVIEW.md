Я как Gemini 3.1 Pro High, выступая в роли критического UI/Product ревьюера, проанализировал код (`build-v21.py`, `AuthorizedEventSearch.astro`) и текущее состояние UI.

Вот точные (precise acceptance) решения по каждому из ваших запросов.

### 1) Проблема узких афиш (событие 6764, 180x320)
**Проблема:** Сейчас скрипт `build-v21.py` жестко требует `safe_crop=1` (наличие geometry), чтобы включить `cover`. Если `image_text_mode = visual_only`, но геометрии нет, он проваливается в fail-closed `natural` (object-fit: contain), получая ширину 63px.
**Решение:**
- **Для подтвержденного `visual_only`:** Отсутствие текста означает, что агрессивный crop *безопасен для смысла*. Нужно разрешить focal-aware 5:4. Если `focal_x/y` нет, форсируем `fx=0.5, fy=0.5`.
- **Для `unknown` (fail-closed):** Нельзя кропать, так как можно обрезать важный текст (время, место). Но чтобы UI не выглядел сломанным, оборачиваем узкий `natural` `<img>` в контейнер `.event-media--natural` с CSS-блюром самой картинки на фоне (или мягким градиентом), чтобы заполнить пространство 5:4.

### 2) Математически монотонный Parallax
**Проблема:** Конкуренция JS (scroll) и CSS (entry `@keyframes`) вызывает скачки.
**Решение:** Разнести зоны ответственности через CSS Variables.
Анимация входа (`hero-tile-in`) управляет *только* opacity внутреннего `.corner-hero__tile`. JS управляет *только* CSS переменными на обертке `.corner-hero`.
**Математическая гарантия (JS):**
```javascript
const maxScroll = 160;
const progress = Math.max(0, Math.min(1, window.scrollY / maxScroll));
// Строго монотонно убывает/растет от 0 до 1 без реверсов.
heroWrapper.style.setProperty('--scroll-fade', 1 - progress);
heroWrapper.style.setProperty('--scroll-offset', `${progress * 40}px`);
```
**CSS:**
```css
.corner-hero {
  transform: translateY(var(--scroll-offset, 0));
  opacity: var(--scroll-fade, 1);
}
```

### 3) Левый блок дат (Орфей 24 июля)
**Проблема:** Внедрение второй даты не должно ломать 112px высоту карточки и семантику per-date ленты.
**Решение:** Группировка через Flex-column, скрытие дублирующего текста от скринридеров.
**DOM & ARIA:**
```html
<div class="event-time-column" aria-label="24 июля в 19:00. Ещё сеанс: 25 июля 17:00">
  <time class="event-time event-time--primary" datetime="2026-07-24T19:00">
    <strong>19:00</strong>
    <span>24 июля</span>
  </time>
  <div class="event-time event-time--next" aria-hidden="true">
    <span>25 июля 17:00</span>
  </div>
</div>
```
**CSS:** `.event-time-column { display: flex; flex-direction: column; gap: 4px; line-height: 1; }` и уменьшенный кегль для `--next`.

### 4) Спроектированный мобильный Search UI
**Авторизация (Auth Gating):** Поле поиска всегда видимо. При `focus` неавторизованным пользователем под полем плавно выезжает блок: `Email magic-link` + `Войти через Яндекс`.
**Чистый URL (PKCE):** В коллбэке `exchangeCodeForSession` немедленно применять:
```javascript
window.history.replaceState({}, '', window.location.pathname);
```
Это стирает `?code=...` до первого рендера UI, предотвращая грязь в ссылках.
**Симуляция тегов (LLM Cloud):** Ни в коем случае не называем это «Ваши сохранения». Заголовок: **«Популярные запросы»** или **«Примеры для поиска»**.
*10 русских тегов (разные размеры через `.tag-s`, `.tag-m`, `.tag-l`):*
1. «джаз на выходных» (L)
2. «чтобы было интересно детям» (L)
3. «концерт классической музыки» (M)
4. «бесплатные выставки» (M)
5. «куда сходить вечером» (M)
6. «стендап» (S)
7. «электронная музыка» (S)
8. «мастер-классы для взрослых» (S)
9. «арт-вечеринка» (S)
10. «экскурсия по городу» (S)

### 5) Подписи для Telegram
1. `/preview-...-v21/` — **Главная лента v21**: исправлены узкие 180x320 картинки (центр-кроп для visual-only, подложка для unknown).
2. `/date-2026-07-25/` — **25 июля (Выходные)**: тест новой левой колонки с next occurrence (Орфей), проверка высоты 112px.
3. `/date-2026-07-24/` — **24 июля (Пятница)**: базовая сетка выдачи без искажений.
4. `/date-2026-07-24-parallax/` — **Параллакс**: контроль математически монотонного скролла и совместимости с entry animation.

---

### Итог: Таблица Acceptance

| Пункт | Статус | Рекомендации и Риски для Playwright (PW) |
|---|---|---|
| **1. Crop 6764** | **CHANGE** | **Риск:** обрезка лиц при `visual_only`. **PW-тест:** Assert ширины `.event-media` >= 90px. Проверка наличия CSS `backdrop-filter` у `.event-media--natural`. |
| **2. Parallax** | **CHANGE** | **Риск:** скачки Y-оси при скролле вверх. **PW-тест:** Dispatch `scroll` events 0 -> 100 -> 50. Assert, что CSS variable `--scroll-fade` строго пропорционален, а `transform` не сбрасывается в 0. |
| **3. Date Block** | **CHANGE** | **Риск:** Разрыв верстки при 3+ сеансах. **PW-тест:** Assert высоты `.event-row` === 112px. Assert: screen reader text включает слово "Ещё сеанс", а `div.event-time--next` имеет `aria-hidden="true"`. |
| **4. Search UI** | **CHANGE** | **Риск:** Утечка `code` в share-ссылки. **PW-тест:** Симуляция OAuth callback. Ожидание рендера. Assert: `window.location.search` не содержит `code` или `state`. |
| **5. TG Links** | **PASS** | Короткие подписи сформированы. Внедрений не требуется. |
