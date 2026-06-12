import logging
import json
import os
import sys
import shutil
import time
from datetime import datetime, date as date_cls
from typing import Optional, List

import requests
import urllib3
import openpyxl
from requests.adapters import HTTPAdapter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_EXCEL = os.path.join(SCRIPT_DIR, "codes_input.xlsx")
OUTPUT_JSON = os.path.join(SCRIPT_DIR, "keden_full_response.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "keden_parser.log")

DEFAULT_DATE = None
REQUEST_DELAY = 1.0
MAX_RETRIES = 3
SAVE_EVERY = 10
BASE_URL = "https://keden.kgd.gov.kz/api/v1/cnfea/cnfea/es/reference"


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("keden")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


log = setup_logger()


def ask_directions() -> List[str]:
    log.info("=" * 70)
    log.info("ВЫБОР НАПРАВЛЕНИЯ ЗАПРОСА")
    log.info("  1 — IN  (только импорт)")
    log.info("  2 — OUT (только экспорт)")
    log.info("  3 — IN + OUT (оба направления)")
    log.info("=" * 70)

    while True:
        try:
            choice = input("Введите номер (1/2/3) [по умолчанию 3]: ").strip()
        except (EOFError, KeyboardInterrupt):
            log.warning("Прервано пользователем")
            sys.exit(0)

        if choice == "" or choice == "3":
            log.info("Выбрано: IN + OUT")
            return ["IN", "OUT"]
        if choice == "1":
            log.info("Выбрано: только IN")
            return ["IN"]
        if choice == "2":
            log.info("Выбрано: только OUT")
            return ["OUT"]
        log.warning(f"Некорректный ввод '{choice}'. Введите 1, 2 или 3.")


def parse_date(value, row_num: int = 0) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date_cls):
        return value.strftime("%Y-%m-%d")

    s = str(value).strip()
    if not s:
        return None

    s = s.split()[0]
    s_normalized = s.replace("-", ".").replace("/", ".")

    formats_to_try = ["%d.%m.%Y", "%Y.%m.%d", "%d.%m.%y"]

    for fmt in formats_to_try:
        try:
            dt = datetime.strptime(s_normalized, fmt)
            if dt.year < 1970:
                dt = dt.replace(year=dt.year + 100)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    log.warning(f"Строка {row_num}: не распознана дата '{s}' — будет использована дата по умолчанию")
    return None


def validate_default_date(date_str) -> str:
    if date_str is None:
        today = datetime.now().strftime("%Y-%m-%d")
        log.info(f"DEFAULT_DATE не задан → используется сегодняшняя: {today}")
        return today

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except (ValueError, TypeError):
        pass

    parsed = parse_date(date_str)
    if parsed:
        log.info(f"DEFAULT_DATE сконвертирована: '{date_str}' → '{parsed}'")
        return parsed

    log.error(f"DEFAULT_DATE = '{date_str}' имеет неверный формат")
    sys.exit(1)


def read_codes_from_excel(filepath: str, default_date: str) -> list:
    if not os.path.exists(filepath):
        log.error(f"Входной файл не найден: {filepath}")
        sys.exit(1)

    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active

    items = []
    seen_pairs = set()
    duplicates_skipped = 0
    used_default = 0
    row_num = 0

    for row in ws.iter_rows(min_col=1, max_col=2, values_only=True):
        row_num += 1
        code_val = row[0] if len(row) >= 1 else None
        date_val = row[1] if len(row) >= 2 else None

        if row_num == 1:
            if code_val is None:
                continue
            code_str = str(code_val).strip().replace(".0", "")
            if not code_str.isdigit():
                continue

        if code_val is None:
            continue

        code = str(code_val).strip()
        if code.endswith(".0"):
            code = code[:-2]
        if not code:
            continue
        if code.isdigit() and len(code) < 10:
            code = code.zfill(10)

        parsed_date = parse_date(date_val, row_num)
        if parsed_date is None:
            parsed_date = default_date
            used_default += 1

        pair = (code, parsed_date)
        if pair in seen_pairs:
            duplicates_skipped += 1
            log.info(f"Строка {row_num}: дубликат {code} @ {parsed_date} — пропущено")
            continue

        seen_pairs.add(pair)
        items.append({"code": code, "date": parsed_date})

    wb.close()

    codes_set = {it["code"] for it in items}
    codes_with_multiple_dates = sum(
        1 for c in codes_set
        if sum(1 for it in items if it["code"] == c) > 1
    )

    log.info(f"Загружено {len(items)} пар код+дата из {filepath}")
    log.info(f"Уникальных кодов: {len(codes_set)}")
    log.info(f"Кодов с несколькими датами: {codes_with_multiple_dates}")
    if duplicates_skipped > 0:
        log.info(f"Дубликатов пропущено: {duplicates_skipped}")
    if used_default > 0:
        log.info(f"Применена дата по умолчанию: {used_default} раз ({default_date})")

    return items


def migrate_legacy_format(data: dict) -> dict:
    needs_migration = False
    for code, val in data.items():
        if isinstance(val, dict) and "date" in val and ("IN" in val or "OUT" in val):
            needs_migration = True
            break

    if not needs_migration:
        return data

    new_data = {}
    for code, val in data.items():
        if not isinstance(val, dict):
            new_data[code] = val
            continue

        if "date" in val and ("IN" in val or "OUT" in val):
            d = val.get("date") or "unknown"
            new_data[code] = {
                d: {
                    "IN": val.get("IN"),
                    "OUT": val.get("OUT"),
                }
            }
        else:
            new_data[code] = val

    return new_data


def load_existing_results(filepath: str) -> dict:
    if not os.path.exists(filepath):
        log.info("Файл прогресса отсутствует — старт с нуля")
        return {}

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            log.warning("Неверный формат JSON, начинаем с нуля")
            return {}
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"Ошибка чтения {filepath}: {e}")
        try:
            broken_path = filepath + ".broken"
            shutil.copy2(filepath, broken_path)
            log.info(f"Повреждённый файл скопирован в {broken_path}")
        except Exception:
            pass
        return {}

    migrated = migrate_legacy_format(data)
    if migrated is not data:
        log.info("Старый формат JSON конвертирован в новый")
        data = migrated

    total_pairs = sum(len(d) for d in data.values() if isinstance(d, dict))
    log.info(f"Загружено {len(data)} кодов, {total_pairs} пар код+дата")
    return data


def save_results(results: dict, filepath: str):
    if not results:
        return

    if os.path.exists(filepath):
        backup_path = filepath + ".backup"
        try:
            shutil.copy2(filepath, backup_path)
        except Exception as e:
            log.warning(f"Не удалось создать бэкап: {e}")

    tmp_path = filepath + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, filepath)
        total_pairs = sum(len(d) for d in results.values() if isinstance(d, dict))
        size = os.path.getsize(filepath)
        log.info(f"Сохранено: {len(results)} кодов, {total_pairs} пар, {size:,} байт")
    except Exception as e:
        log.error(f"Ошибка сохранения: {e}")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def is_pair_complete(results: dict, code: str, date: str, directions: List[str]) -> bool:
    code_data = results.get(code)
    if not isinstance(code_data, dict):
        return False
    date_data = code_data.get(date)
    if not isinstance(date_data, dict):
        return False
    for direction in directions:
        if date_data.get(direction) is None:
            return False
    return True


def safe_sleep(seconds: float):
    try:
        time.sleep(seconds)
    except (KeyboardInterrupt, SystemError, OSError):
        pass


class KedenParser:
    def __init__(self, delay: float = 1.0, max_retries: int = 3):
        self.delay = delay
        self.max_retries = max_retries
        self.xsrf_token: Optional[str] = None
        self._session: Optional[requests.Session] = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.verify = False
            adapter = HTTPAdapter(max_retries=0)
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)
        return self._session

    def _close_session(self):
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None

    def _get_xsrf_token(self) -> str:
        log.info("Получаем XSRF-токен")
        session = requests.Session()
        session.verify = False
        try:
            r = session.get(
                "https://keden.kgd.gov.kz/tnved",
                timeout=20,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/145.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            log.info(f"Запрос токена HTTP {r.status_code}")

            token = session.cookies.get("XSRF-TOKEN")
            if token:
                log.info(f"Токен получен из cookie: {token[:30]}...")
                return token

            set_cookie = r.headers.get("Set-Cookie", "")
            if "XSRF-TOKEN=" in set_cookie:
                token = set_cookie.split("XSRF-TOKEN=")[1].split(";")[0]
                if token:
                    log.info(f"Токен получен из Set-Cookie: {token[:30]}...")
                    return token

            text = r.text
            if 'name="csrf-token"' in text:
                start = text.find('name="csrf-token"')
                content_start = text.rfind('content="', max(0, start - 100), start)
                if content_start != -1:
                    content_start += len('content="')
                    content_end = text.find('"', content_start)
                    token = text[content_start:content_end]
                    if token:
                        log.info(f"Токен получен из meta: {token[:30]}...")
                        return token

            log.warning("Токен не найден")

        except Exception as e:
            log.error(f"Ошибка получения токена: {e}")
        finally:
            session.close()

        return ""

    def _get_headers(self) -> dict:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://keden.kgd.gov.kz/tnved",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
        if self.xsrf_token:
            headers["X-XSRF-TOKEN"] = self.xsrf_token
        return headers

    def _make_request(self, url: str) -> requests.Response:
        session = self._get_session()
        session.headers.update(self._get_headers())
        if self.xsrf_token:
            session.cookies.set("XSRF-TOKEN", self.xsrf_token)
        return session.get(url, timeout=(10, 30))

    def fetch_one(self, code: str, direction: str, date: str) -> Optional[dict]:
        url = f"{BASE_URL}/{code}/{direction}/{date}"
        label = "ИМПОРТ" if direction == "IN" else "ЭКСПОРТ"

        for attempt in range(1, self.max_retries + 1):
            try:
                log.info(f"{label} {code} @ {date} | попытка {attempt}/{self.max_retries}")
                response = self._make_request(url)

                if response.status_code == 200:
                    data = response.json()
                    ntr = data.get("nonTariffResolutions") or []
                    docs = sum(len(r.get("documents") or []) for r in ntr)
                    log.info(f"{label} OK: {len(ntr)} резолюций, {docs} документов")
                    return data

                if response.status_code == 403:
                    log.warning("HTTP 403 — обновляем токен")
                    self._close_session()
                    self.xsrf_token = self._get_xsrf_token()
                    continue

                if response.status_code == 429:
                    log.warning("HTTP 429 — rate limit, ждём 30 сек")
                    safe_sleep(30)
                    continue

                if response.status_code == 404:
                    log.info(f"{label} {code}: код не найден на сервере")
                    return {"nonTariffResolutions": [], "notFound": True}

                log.warning(f"HTTP {response.status_code}: {response.text[:200]}")

            except requests.exceptions.ReadTimeout:
                log.warning(f"TIMEOUT {code} {label}")
            except requests.exceptions.ConnectionError as e:
                log.warning(f"CONN_ERR {code} {label}: {str(e)[:100]}")
                self._close_session()
            except json.JSONDecodeError:
                log.warning("Невалидный JSON от сервера")
            except KeyboardInterrupt:
                raise
            except Exception as e:
                log.error(f"{type(e).__name__}: {e}")

            wait = self.delay * attempt
            safe_sleep(wait)

        return None


def main():
    log.info("=" * 70)
    log.info(f"KEDEN PARSER — старт {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    directions = ask_directions()
    default_date = validate_default_date(DEFAULT_DATE)

    log.info(f"Папка проекта:     {SCRIPT_DIR}")
    log.info(f"Входной файл:      {INPUT_EXCEL}")
    log.info(f"Выходной JSON:     {OUTPUT_JSON}")
    log.info(f"Лог-файл:          {LOG_FILE}")
    log.info(f"Дата по умолчанию: {default_date}")
    log.info(f"Направления:       {' + '.join(directions)}")
    log.info(f"Задержка:          {REQUEST_DELAY} сек")
    log.info(f"Попыток:           {MAX_RETRIES}")
    log.info(f"Автосейв:          каждые {SAVE_EVERY} запросов")

    items = read_codes_from_excel(INPUT_EXCEL, default_date)
    if not items:
        log.error("Список кодов пуст")
        sys.exit(1)

    results = load_existing_results(OUTPUT_JSON)

    remaining = [it for it in items if not is_pair_complete(results, it["code"], it["date"], directions)]
    already_done = len(items) - len(remaining)

    log.info("=" * 70)
    log.info(f"Всего пар код+дата:    {len(items)}")
    log.info(f"Уже обработано:        {already_done}")
    log.info(f"Осталось обработать:   {len(remaining)}")
    log.info(f"Запросов к API:        ~{len(remaining) * len(directions)}")
    log.info("=" * 70)

    if not remaining:
        log.info("Все пары код+дата уже обработаны для выбранных направлений")
        return

    parser = KedenParser(delay=REQUEST_DELAY, max_retries=MAX_RETRIES)
    parser.xsrf_token = parser._get_xsrf_token()

    new_count = 0
    error_count = 0
    start_time = time.time()

    try:
        for i, item in enumerate(remaining, 1):
            code = item["code"]
            date = item["date"]
            position = already_done + i
            left = len(remaining) - i

            log.info(f"--- [{position}/{len(items)}] {code} @ {date} | осталось: {left} ---")

            if code not in results or not isinstance(results[code], dict):
                results[code] = {}
            if date not in results[code] or not isinstance(results[code][date], dict):
                results[code][date] = {}

            try:
                for j, direction in enumerate(directions):
                    if results[code][date].get(direction) is not None:
                        log.info(f"{direction} уже собран ранее — пропуск")
                        continue

                    response = parser.fetch_one(code, direction, date)
                    results[code][date][direction] = response

                    if response is None:
                        error_count += 1

                    if j < len(directions) - 1:
                        safe_sleep(REQUEST_DELAY)

            except KeyboardInterrupt:
                log.warning(f"Прервано пользователем на {code} @ {date}")
                raise

            new_count += 1

            if new_count % SAVE_EVERY == 0:
                save_results(results, OUTPUT_JSON)
                elapsed = time.time() - start_time
                speed = new_count / elapsed if elapsed > 0 else 0
                eta_sec = left / speed if speed > 0 else 0
                log.info(f"Скорость: {speed:.2f} пар/сек | ETA: {eta_sec/60:.1f} мин")

            if i < len(remaining):
                safe_sleep(REQUEST_DELAY)

    except KeyboardInterrupt:
        log.warning("Сохраняем прогресс перед выходом")
    except Exception as e:
        log.exception(f"Неожиданная ошибка: {type(e).__name__}: {e}")
    finally:
        save_results(results, OUTPUT_JSON)
        parser._close_session()

    elapsed = time.time() - start_time
    successful = sum(
        1 for it in items
        if is_pair_complete(results, it["code"], it["date"], directions)
    )
    incomplete = len(items) - successful

    log.info("=" * 70)
    log.info("ИТОГО")
    log.info("=" * 70)
    log.info(f"Время работы:                 {elapsed/60:.1f} мин")
    log.info(f"Обработано за этот запуск:    {new_count}")
    log.info(f"Ошибок запросов:              {error_count}")
    log.info(f"Всего пар код+дата:           {len(items)}")
    log.info(f"Полностью собрано:            {successful}")
    log.info(f"С ошибками (требуют повтора): {incomplete}")
    log.info(f"Файл результата:              {OUTPUT_JSON}")
    if os.path.exists(OUTPUT_JSON):
        log.info(f"Размер файла:                 {os.path.getsize(OUTPUT_JSON):,} байт")
    log.info("=" * 70)

    if incomplete > 0:
        log.info(f"Запустите скрипт повторно — он добьёт оставшиеся {incomplete} пар")


if __name__ == "__main__":
    main()