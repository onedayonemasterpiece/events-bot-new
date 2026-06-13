"""
Kaggle notebook для парсинга событий с сайта pyramida.info.

Входные данные: список URL из аргументов или переменной окружения PYRAMIDA_URLS.
Выходные данные: pyramida_events.json с распарсенными событиями.
"""

import asyncio
import os
import subprocess
import sys
import pandas as pd
import re
import json

def _load_status_loader():
    try:
        from kaggle_status_client import load_status_client as loader
        return loader
    except Exception as exc:
        print(f"[kaggle_status] import failed: {exc}", flush=True)
    import importlib.util
    from pathlib import Path
    for root in [Path(__file__).resolve().parent, Path.cwd(), Path("/kaggle/working"), Path("/kaggle/input")]:
        if not root.exists():
            continue
        candidates = [root / "kaggle_status_client.py"]
        try:
            candidates.extend(sorted(root.rglob("kaggle_status_client.py")))
        except Exception:
            pass
        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                spec = importlib.util.spec_from_file_location("events_bot_kaggle_status_client", candidate)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    print(f"[kaggle_status] loaded helper from {candidate}", flush=True)
                    return module.load_status_client
            except Exception as path_exc:
                print(f"[kaggle_status] helper load failed from {candidate}: {path_exc}", flush=True)
    return None


load_status_client = _load_status_loader()

STATUS_PROGRESS = {"phase": "bootstrap", "urls_total": 0, "url_index": 0, "events_parsed": 0}
STATUS_CLIENT = load_status_client(log=print) if load_status_client else None

# --- 1. УСТАНОВКА ---
def install_libs():
    try:
        import playwright
        import bs4
    except ImportError:
        print("⏳ Устанавливаем библиотеки...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "playwright", "beautifulsoup4", "pandas"])
        os.system("playwright install chromium")
        os.system("playwright install-deps")
        print("✅ Библиотеки готовы.")

install_libs()

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

BASE_DOMAIN = "https://pyramida.info"

# --- 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

# --- 3. ПАРСЕР ---
async def parse_pyramida_event(page, url):
    print(f"🔄 Загрузка: {url}")

    try:
        # Ждем стабилизации сети (networkidle) для SPA сайтов
        await page.goto(url, timeout=60000, wait_until='networkidle')

        # Пауза для рендеринга React
        await page.wait_for_timeout(3000)

        # Ждем H1 (гарантия, что контент появился)
        try:
            await page.wait_for_selector('h1', state='visible', timeout=10000)
        except:
            print("⚠️ H1 не появился сразу, пробуем парсить как есть...")

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # --- ЗАГОЛОВОК ---
        h1 = soup.find('h1')
        title = clean_text(h1.get_text()) if h1 else "Заголовок не найден"

        if title == "Заголовок не найден":
            return None

        # --- ИЗОБРАЖЕНИЕ ---
        image_url = ""
        # 1. По alt
        img = soup.find('img', alt=title)
        if img: image_url = img.get('src')
        # 2. По пути upload
        if not image_url:
            for i in soup.find_all('img'):
                src = i.get('src', '')
                if '/upload/' in src and 'resize_cache' in src:
                    image_url = src
                    break

        if image_url and image_url.startswith('/'):
            image_url = f"{BASE_DOMAIN}{image_url}"

        # --- ВОЗРАСТ ---
        age = ""
        for div in soup.find_all(['div', 'span']):
            txt = div.get_text(" ", strip=True)
            # Ищем короткую строку "12 +" или "0+"
            if len(txt) < 6 and re.match(r'^\d+\s*\+$', txt):
                age = txt.replace(" ", "")
                break

        # --- УНИВЕРСАЛЬНЫЙ ПОИСК ПОЛЕЙ (ДАТА, МЕСТО, ЦЕНА) ---
        def get_value_from_row(soup, label_pattern):
            label_tag = soup.find(string=re.compile(label_pattern))
            if not label_tag: return ""

            current = label_tag.parent
            for _ in range(3):
                if not current: break
                row_text = clean_text(current.get_text(" "))
                clean_label = label_tag.strip()
                # Если в строке текста больше, чем в лейбле - значит там значение
                if len(row_text) > len(clean_label) + 2:
                    val = re.sub(fr'{label_pattern}[:\s]*', '', row_text, count=1, flags=re.I).strip()
                    if "Previous" in val or "Next" in val: continue
                    return val
                current = current.parent
            return ""

        # МЕСТО
        location = get_value_from_row(soup, "Место")

        # ЦЕНА
        price = get_value_from_row(soup, "Цена")
        if not price and ("Вход бесплатный" in soup.get_text() or "вход свободный" in soup.get_text().lower()):
            price = "Бесплатно"

        # ДАТА
        date_raw = get_value_from_row(soup, "Дата")
        # Фолбек 1: input
        if not date_raw:
            label_date = soup.find(string=re.compile("Дата"))
            if label_date and label_date.parent:
                parent = label_date.parent
                inp = parent.find_next('input')
                if inp: date_raw = inp.get('value', '')

        # Фолбек 2: Расписание в описании (для событий без конкретной даты в шапке)
        if not date_raw:
            schedule_header = soup.find(string=re.compile(r"Расписание[:\.]?", re.I))
            if schedule_header and schedule_header.parent:
                parent = schedule_header.parent
                full_text = parent.get_text("\n", strip=True)
                # Если мало текста, берем соседей
                if len(full_text) < 20:
                    siblings = parent.find_next_siblings()
                    full_text += "\n" + "\n".join([s.get_text("\n", strip=True) for s in siblings[:3]])

                dates = []
                for line in full_text.split('\n'):
                    if re.search(r'\d{1,2}\s+[а-яА-ЯёЁ]+', line) and re.search(r'\d{2}:\d{2}', line):
                        dates.append(line.strip())
                if dates: date_raw = " | ".join(dates)

        # --- СТАТУС ---
        status = "unknown"
        page_text = soup.get_text(" ", strip=True).lower()
        if soup.find('button', string=re.compile(r"Купить", re.I)):
            status = "available"
        elif "регистрац" in page_text and ("пройти" in page_text or "необходима" in page_text):
             status = "registration_open"
        elif "билетов нет" in page_text:
            status = "sold_out"

        # --- ОПИСАНИЕ ---
        description = ""
        desc_label = soup.find(string=re.compile("Описание"))
        if desc_label:
            # Находим контейнер с текстом "Описание" (обычно p > strong)
            current = desc_label.parent
            # Поднимаемся до уровня блока (p или div), который содержит заголовок
            while current and current.name not in ['p', 'div', 'section']:
                current = current.parent

            if current:
                # Текст описания обычно находится в следующем блоке div
                next_div = current.find_next_sibling('div')
                if next_div:
                    description = next_div.get_text("\n", strip=True)

                # Если не нашли в соседе, проверяем родителя (иногда текст внутри того же блока)
                if not description:
                     parent_text = current.get_text("\n", strip=True)
                     if len(parent_text) > len("Описание:") + 10:
                         description = parent_text.replace("Описание:", "").strip()

            # Очистка от служебной информации
            stop_words = [
                "ИНФОРМАЦИЯ ОБ ОРГАНИЗАТОРЕ",
                "Продолжительность:",
                "Расписание:",
                "Важно знать",
                "Информация о возврате"
            ]
            for sw in stop_words:
                if sw in description:
                    description = description.split(sw, 1)[0].strip()

        return {
            "title": title,
            "date_raw": date_raw,
            "location": location,
            "price": price,
            "age_restriction": age,
            "ticket_status": status,
            "url": url,            # <--- ВАЖНО: Ссылка на покупку (исходная ссылка)
            "image_url": image_url,
            "description": description[:1000]  # Ограничим длину описания
        }

    except Exception as e:
        print(f"❌ Ошибка {url}: {e}")
        return None

# --- 4. ЗАПУСК ---
async def main():
    if STATUS_CLIENT and STATUS_CLIENT.enabled:
        STATUS_CLIENT.event("kernel_started", phase="preflight", status="running", progress=dict(STATUS_PROGRESS))
        STATUS_CLIENT.start_alive(interval_seconds=60, progress_provider=lambda: dict(STATUS_PROGRESS))
    # СПИСОК ССЫЛОК (ВХОДНЫЕ ДАННЫЕ)
    # Получаем URLs из переменной окружения или используем тестовые
    urls_env = os.environ.get("PYRAMIDA_URLS", "")
    if urls_env:
        urls = [u.strip() for u in urls_env.split(",") if u.strip()]
    else:
        # Тестовые URLs для запуска без аргументов
        urls = [
            "https://pyramida.info/tickets/novogodnee-nastroenie-ot-tantsy_54151730",
            "https://pyramida.info/tickets/minecraft-shou_49516085",
            "https://pyramida.info/tickets/puteshestvie-v-skazochnyy-son-vtoraya-partiya-biletov_53469631"
        ]

    print(f"📋 Парсинг {len(urls)} URL(s)...")
    STATUS_PROGRESS.update({"phase": "parse", "urls_total": len(urls)})
    results = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
            page = await context.new_page()

            for index, url in enumerate(urls, 1):
                STATUS_PROGRESS.update({"url_index": index, "current_url": url})
                data = await parse_pyramida_event(page, url)
                if data:
                    results.append(data)
                    STATUS_PROGRESS["events_parsed"] = len(results)

            await browser.close()

        STATUS_PROGRESS.update({"phase": "write_report", "events_parsed": len(results)})
        if results:
            df = pd.DataFrame(results)
            print("\n🎉 Готово! Проверка данных:")
            # Вывод ключевых полей для контроля
            print(df[['title', 'date_raw', 'price', 'url']].to_string())

            # Сохраняем JSON
            df.to_json('pyramida_events.json', orient='records', force_ascii=False, indent=4)
            print("\n💾 Сохранено в pyramida_events.json")
        else:
            # Сохраняем пустой JSON для обработки ошибок
            with open('pyramida_events.json', 'w', encoding='utf-8') as f:
                json.dump([], f)
            print("⚠️ Ничего не найдено, создан пустой файл.")
        if STATUS_CLIENT and STATUS_CLIENT.enabled:
            STATUS_CLIENT.event("report_written", phase="report", status="done", progress=dict(STATUS_PROGRESS))
    except Exception as exc:
        STATUS_PROGRESS.update({"phase": "failed"})
        if STATUS_CLIENT and STATUS_CLIENT.enabled:
            STATUS_CLIENT.event("report_written", phase="failed", status="failed", progress=dict(STATUS_PROGRESS), message=str(exc))
        raise
    finally:
        if STATUS_CLIENT and STATUS_CLIENT.enabled:
            STATUS_CLIENT.stop_alive()

# Запуск
asyncio.get_event_loop().run_until_complete(main())
