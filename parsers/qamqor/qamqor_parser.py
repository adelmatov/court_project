"""
QAMQOR Parser - Основной парсер с обходом reCAPTCHA и тестовыми задержками.
Полная версия с stealth-режимом и визуальной отладкой.
"""

import asyncio
import argparse
import signal
import sys
import random
from types import FrameType
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager, suppress
from signal import Signals
from datetime import datetime

from playwright.async_api import async_playwright, Page, Response

from .core import (
    Config,
    DatabaseManager,
    DataProcessor,
    APIValidator,
    WebClient,
    StealthTabManager,
    LogManager,
    apply_stealth
)

class QamqorParser:
    """Основной парсер QAMQOR с обходом CAPTCHA и тестовыми задержками."""
    
    def __init__(self, mode: str = "full") -> None:
        self.config = Config()
        self.config.MODE = mode
        
        self.log_manager = LogManager(self.config, name="qamqor_parser")
        self.logger = self.log_manager.logger
        
        self.db_manager = DatabaseManager(self.config, self.logger)
        self.data_processor = DataProcessor(self.config, self.logger)
        self.api_validator = APIValidator(self.logger)
        self.web_client = WebClient(self.config, self.logger, self.api_validator)
        
        self.data_queue: asyncio.Queue = asyncio.Queue()
        self.shutdown_event = asyncio.Event()
        self.active_workers: List[asyncio.Task] = []
        
        self.region_stats: Dict = {}
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum: Signals, frame: Optional[FrameType]) -> None:
        """Обработчик сигналов завершения."""
        self.logger.warning(f"⚠️ Получен сигнал {signum}, завершение работы...")
        self.shutdown_event.set()
    
    async def _graceful_shutdown(self, timeout: float = 30.0):
        """Корректное завершение работы."""
        if not self.active_workers:
            return
        
        self.logger.warning(f"⚠️ Graceful shutdown: {len(self.active_workers)} воркеров...")
        self.shutdown_event.set()
        
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.active_workers, return_exceptions=True),
                timeout=timeout
            )
            self.logger.info("✅ Все воркеры завершены")
        except asyncio.TimeoutError:
            self.logger.warning(f"⚠️ Таймаут {timeout}s, отмена задач...")
            for task in self.active_workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self.active_workers, return_exceptions=True)
        finally:
            self.active_workers.clear()
    
    async def run(self) -> bool:
        """Главная функция запуска парсера."""
        try:
            self.logger.info("=" * 80)
            self.logger.info("🚀 ЗАПУСК ПАРСЕРА QAMQOR | Режим: %s", self.config.MODE.upper())
            self.logger.info("=" * 80)
            
            await self.db_manager.initialize_tables()
            self.region_stats = await self.db_manager.get_region_stats()
            
            if self.config.MODE == "missing_only":
                self.logger.info("🔍 Режим: ТОЛЬКО ПРОПУЩЕННЫЕ НОМЕРА")
                await self._run_missing_numbers_search()
            else:
                region_state = await self.db_manager.get_region_state()
                self.logger.info("📡 Режим: ПОЛНЫЙ ПАРСИНГ")
                await self._run_parsing(region_state)
                
                if not self.shutdown_event.is_set():
                    self.logger.info("")
                    self.logger.info("=" * 80)
                    self.logger.info("🔍 ФАЗА 2: ПОИСК ПРОПУЩЕННЫХ НОМЕРОВ")
                    self.logger.info("=" * 80)
                    await self._run_missing_numbers_search()
            
            await self._print_final_table()
            
            self.logger.info("=" * 80)
            self.logger.info("✅ ПАРСИНГ ЗАВЕРШЕН")
            self.logger.info("=" * 80)
            return True
            
        except KeyboardInterrupt:
            self.logger.warning("⚠️ Прервано пользователем")
            return False
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
            return False
    
    async def _run_parsing(self, region_state: Dict[int, int]):
        """Запуск основного процесса парсинга."""
        async with async_playwright() as playwright:
            self.logger.info("🔧 Запуск браузера...")
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-gpu",
                    "--disable-software-rasterizer",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-breakpad",
                    "--disable-component-extensions-with-background-pages",
                    "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                    "--disable-ipc-flooding-protection",
                    "--disable-renderer-backgrounding",
                    "--enable-features=NetworkService,NetworkServiceInProcess",
                    "--force-color-profile=srgb",
                    "--hide-scrollbars",
                    "--metrics-recording-only",
                    "--mute-audio",
                    "--no-first-run",
                    "--disable-infobars",
                    "--window-size=1920,1080"
                ]
            )
            
            self.logger.info("🔧 Создание контекста...")
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                locale="ru-RU,ru",
                timezone_id="Asia/Almaty",
                geolocation={"longitude": 76.8512, "latitude": 43.2220},
                permissions=["geolocation"],
                color_scheme="light",
                device_scale_factor=1,
                has_touch=False,
                is_mobile=False,
                extra_http_headers={
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1"
                }
            )
            
            # Health check
            self.logger.info("🏥 Запуск Health Check...")
            health_check_page = await context.new_page()
            
            # Применяем stealth
            await apply_stealth(health_check_page)
            
            # Блокировка ресурсов
            async def health_route_handler(route):
                if route.request.resource_type in ["image", "stylesheet", "font", "media"]:
                    await route.abort()
                else:
                    await route.continue_()
            
            await health_check_page.route("**/*", health_route_handler)
            
            try:
                self.logger.info(f"🌐 Загрузка страницы: {self.config.SEARCH_URL}")
                
                await health_check_page.goto(
                    self.config.SEARCH_URL,
                    timeout=self.config.PAGE_TIMEOUT,
                    wait_until="domcontentloaded"
                )
                
                # Ждем загрузку поля ввода
                await health_check_page.wait_for_selector(
                    'input[placeholder="Тексеру/ тіркеу нөмірі"]',
                    state="visible",
                    timeout=10000
                )
                
                self.logger.info("✅ Страница загружена")
                
                # Проверка API
                if not await self.web_client.check_api_health_with_page(health_check_page):
                    self.logger.error("❌ API недоступен")
                    return
                
                self.logger.info("✅ Health Check пройден успешно")
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка в health check: {e}", exc_info=True)
                return
                
            finally:
                await health_check_page.close()
            
            # Инициализация TabManager
            self.logger.info("🔧 Инициализация TabManager...")
            tab_manager = StealthTabManager(context, self.config, self.logger)
            await tab_manager.initialize()
            
            # Запуск обработчика данных
            data_handler_task = asyncio.create_task(
                self._data_handler(),
                name="data_handler"
            )
            
            # Заполнение очереди регионов
            region_queue: asyncio.Queue = asyncio.Queue()
            for region_code, start_pos in region_state.items():
                region_name = self.config.REGIONS[region_code]
                await region_queue.put((region_code, region_name, start_pos))
            
            self.logger.info("📋 В очереди %d регионов", len(self.config.REGIONS))
            
            # Запуск воркеров
            self.active_workers = [
                asyncio.create_task(
                    self._region_worker(worker_id, region_queue, tab_manager),
                    name=f"region_worker_{worker_id}"
                )
                for worker_id in range(self.config.MAX_CONCURRENT_TABS)
            ]
            
            self.logger.info("👷 Запущено %d воркеров", len(self.active_workers))
            
            try:
                await asyncio.gather(*self.active_workers, return_exceptions=True)
                
                if self.shutdown_event.is_set():
                    await self._graceful_shutdown(timeout=30.0)
            
            except KeyboardInterrupt:
                self.logger.warning("⚠️ KeyboardInterrupt - graceful shutdown...")
                await self._graceful_shutdown(timeout=30.0)
            
            finally:
                # Завершение обработчика данных
                try:
                    self.logger.info("🛑 Остановка обработчика данных...")
                    await self.data_queue.put(None)
                    await asyncio.wait_for(data_handler_task, timeout=15.0)
                    self.logger.info("✅ Data handler завершен")
                except asyncio.TimeoutError:
                    self.logger.warning("⚠️ Таймаут data_handler (15s)")
                    data_handler_task.cancel()
                    try:
                        await data_handler_task
                    except asyncio.CancelledError:
                        pass
                
                # Закрытие ресурсов
                await tab_manager.close_all()
                await browser.close()
                self.logger.info("🔒 Браузер закрыт")
    
    async def _run_missing_numbers_search(self):
        """Поиск и обработка пропущенных номеров."""
        missing_numbers = await self.db_manager.find_missing_numbers()
        
        if not missing_numbers:
            self.logger.info("✅ Пропущенных номеров не найдено")
            return
        
        total_missing = sum(len(nums) for nums in missing_numbers.values())
        self.logger.info("📋 Найдено пропущенных номеров: %d", total_missing)
        
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu"]
            )
            
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
            )
            
            tab_manager = StealthTabManager(context, self.config, self.logger)
            await tab_manager.initialize()
            
            data_handler_task = asyncio.create_task(
                self._data_handler(),
                name="missing_data_handler"
            )
            
            missing_queue: asyncio.Queue = asyncio.Queue()
            for region_code, numbers in missing_numbers.items():
                region_name = self.config.REGIONS[region_code]
                await missing_queue.put((region_code, region_name, numbers))
            
            self.active_workers = [
                asyncio.create_task(
                    self._missing_numbers_worker(worker_id, missing_queue, tab_manager),
                    name=f"missing_worker_{worker_id}"
                )
                for worker_id in range(self.config.MAX_CONCURRENT_TABS)
            ]
            
            try:
                await asyncio.gather(*self.active_workers, return_exceptions=True)
            finally:
                try:
                    await self.data_queue.put(None)
                    await asyncio.wait_for(data_handler_task, timeout=15.0)
                except asyncio.TimeoutError:
                    data_handler_task.cancel()
                
                await tab_manager.close_all()
                await browser.close()
    
    async def _region_worker(
        self,
        worker_id: int,
        region_queue: asyncio.Queue[Tuple[int, str, int]],
        tab_manager: StealthTabManager
    ) -> None:
        """Воркер для обработки региона."""
        self.logger.debug(f"✅ W{worker_id} запущен")
        
        while not self.shutdown_event.is_set():
            try:
                try:
                    region_data = await asyncio.wait_for(
                        region_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    if region_queue.empty():
                        self.logger.debug(f"✅ W{worker_id} завершен (очередь пуста)")
                        break
                    continue
                
                region_code, region_name, start_pos = region_data
                
            except asyncio.CancelledError:
                self.logger.debug(f"🛑 W{worker_id} отменен")
                break
            except Exception as e:
                self.logger.error(f"❌ W{worker_id} критическая ошибка: {e}")
                break
            
            if self.shutdown_event.is_set():
                self.logger.warning(f"⚠️ W{worker_id} | {region_name} пропущен (shutdown)")
                region_queue.task_done()
                continue
            
            success = False
            for attempt in range(1, self.config.REGION_RETRY_LIMIT + 1):
                try:
                    async with tab_manager.get_tab() as page:
                        await self._parse_region(
                            page, region_code, region_name, start_pos, worker_id
                        )
                    
                    success = True
                    break
                    
                except Exception as e:
                    if self.shutdown_event.is_set():
                        self.logger.error(f"❌ W{worker_id} | {region_name} остановлен")
                        break
                    
                    if attempt < self.config.REGION_RETRY_LIMIT:
                        delay = self.config.RETRY_DELAY * attempt
                        self.logger.warning(
                            f"⚠️ W{worker_id} | {region_name} "
                            f"попытка {attempt}/{self.config.REGION_RETRY_LIMIT}, "
                            f"повтор через {delay}s: {e}"
                        )
                        await asyncio.sleep(delay)
                    else:
                        self.logger.error(f"❌ W{worker_id} | {region_name} ОШИБКА")
            
            region_queue.task_done()
            
            if not success:
                self.logger.warning(f"⚠️ {region_name} не обработан полностью")
        
        self.logger.debug(f"✅ W{worker_id} завершен")
    
    async def _missing_numbers_worker(
        self,
        worker_id: int,
        missing_queue: asyncio.Queue[Tuple[int, str, List[int]]],
        tab_manager: StealthTabManager
    ) -> None:
        """Воркер для обработки пропущенных номеров."""
        self.logger.debug(f"✅ MW{worker_id} запущен (missing)")
        
        while not self.shutdown_event.is_set():
            try:
                try:
                    region_data = await asyncio.wait_for(
                        missing_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    if missing_queue.empty():
                        break
                    continue
                
                region_code, region_name, numbers = region_data
                    
            except asyncio.CancelledError:
                self.logger.debug(f"🛑 MW{worker_id} отменен")
                break
            
            if self.shutdown_event.is_set():
                self.logger.warning(f"⚠️ MW{worker_id} | {region_name} (missing) пропущен")
                missing_queue.task_done()
                continue
            
            for attempt in range(1, self.config.REGION_RETRY_LIMIT + 1):
                try:
                    async with tab_manager.get_tab() as page:
                        await self._process_missing_numbers(
                            page, region_code, region_name, numbers, worker_id
                        )
                    break
                    
                except Exception as e:
                    if self.shutdown_event.is_set():
                        break
                    
                    if attempt < self.config.REGION_RETRY_LIMIT:
                        delay = self.config.RETRY_DELAY * attempt
                        self.logger.warning(f"⚠️ MW{worker_id} | {region_name} retry {attempt}")
                        await asyncio.sleep(delay)
            
            missing_queue.task_done()
        
        self.logger.debug(f"✅ MW{worker_id} завершен (missing)")
    
    async def _parse_region(
        self,
        page: Page,
        region_code: int,
        region_name: str,
        start_position: int,
        worker_id: int
    ):
        """Парсинг региона с правильным ожиданием загрузки."""
        await page.goto(
            self.config.SEARCH_URL,
            timeout=self.config.PAGE_TIMEOUT,
            wait_until="domcontentloaded"
        )
        
        await page.wait_for_selector(
            'input[placeholder="Тексеру/ тіркеу нөмірі"]',
            state="visible",
            timeout=10000
        )
        
        # Случайная задержка для естественности
        await asyncio.sleep(
            random.uniform(self.config.NATURAL_DELAY_MIN, self.config.NATURAL_DELAY_MAX)
        )
        
        current_position = start_position
        empty_count = 0
        found_count = 0
        
        input_selector = 'input[placeholder="Тексеру/ тіркеу нөмірі"]'
        button_selector = "button.btn.btn-primary:has-text('Іздеу')"
        
        while current_position <= self.config.MAX_NUMBER and not self.shutdown_event.is_set():
            found_in_position = False
            
            for check_type in [1, 2]:
                if self.shutdown_event.is_set():
                    break
                
                reg_num = f"25{region_code}170101{check_type}/{current_position:05d}"
                
                try:
                    result = await self._try_number_safe(
                        page, reg_num, worker_id, input_selector, button_selector
                    )
                    
                    if result:
                        await self.data_queue.put({
                            'data': result,
                            'region_code': region_code
                        })
                        found_in_position = True
                        found_count += 1
                        empty_count = 0
                        break
                except Exception:
                    pass
            
            if not found_in_position:
                empty_count += 1
                if empty_count >= self.config.MAX_EMPTY_SEQUENCE:
                    break
            
            current_position += 1
        
        if region_code in self.region_stats:
            self.region_stats[region_code]['found_new'] += found_count
        
        self.log_manager.increment_metric('regions_completed')
        self.logger.info("✅ %s завершен (найдено: %d)", region_name, found_count)
    
    async def _process_missing_numbers(
        self,
        page: Page,
        region_code: int,
        region_name: str,
        numbers: List[int],
        worker_id: int
    ):
        """Обработка списка пропущенных номеров."""
        await page.goto(
            self.config.SEARCH_URL,
            timeout=self.config.PAGE_TIMEOUT,
            wait_until="domcontentloaded"
        )
        
        await page.wait_for_selector(
            'input[placeholder="Тексеру/ тіркеу нөмірі"]',
            state="visible",
            timeout=10000
        )
        
        found_count = 0
        
        for number in numbers:
            if self.shutdown_event.is_set():
                break
            
            for check_type in [1, 2]:
                reg_num = f"25{region_code}170101{check_type}/{number:05d}"
                
                try:
                    result = await self._try_number_safe(
                        page,
                        reg_num,
                        worker_id,
                        'input[placeholder="Тексеру/ тіркеу нөмірі"]',
                        "button.btn.btn-primary:has-text('Іздеу')"
                    )
                    
                    if result:
                        await self.data_queue.put({
                            'data': result,
                            'region_code': region_code
                        })
                        found_count += 1
                        break
                        
                except Exception:
                    pass
        
        if region_code in self.region_stats:
            self.region_stats[region_code]['found_new'] += found_count
        
        self.logger.info(
            "✅ %s: пропущенные (%d/%d)", 
            region_name, 
            found_count, 
            len(numbers)
        )
    
    async def _try_number_safe(
        self,
        page: Page,
        registration_number: str,
        worker_id: int,
        input_selector: str,
        button_selector: str
    ) -> Optional[Dict]:
        """Безопасная проверка номера БЕЗ race conditions."""
        self.log_manager.increment_metric('api_requests')
        
        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                if self.shutdown_event.is_set():
                    return None
                
                await page.wait_for_selector(input_selector, state="visible", timeout=5000)
                
                # Случайная задержка для имитации человека
                await asyncio.sleep(
                    random.uniform(self.config.NATURAL_DELAY_MIN, self.config.NATURAL_DELAY_MAX)
                )

                await page.fill(input_selector, '')
                await page.fill(input_selector, registration_number)

                await asyncio.sleep(
                    random.uniform(self.config.TYPING_DELAY_MIN, self.config.TYPING_DELAY_MAX)
                )
                
                async with self._response_listener(page) as wait_response:
                    await page.click(button_selector)
                    response_data = await wait_response()
                
                if not response_data:
                    if attempt < self.config.MAX_RETRIES:
                        await asyncio.sleep(self.config.RETRY_DELAY)
                        continue
                    return None
                
                is_valid, error_msg = self.api_validator.validate_response(
                    response_data,
                    context=f"W{worker_id}:{registration_number}"
                )
                
                if not is_valid:
                    if self.api_validator.is_critical_error(response_data):
                        self.logger.critical(f"🚨 КРИТИЧЕСКАЯ ОШИБКА API: {error_msg}")
                        self.shutdown_event.set()
                        return None
                    if attempt < self.config.MAX_RETRIES:
                        await asyncio.sleep(self.config.RETRY_DELAY)
                        continue
                    return None
                
                total_elements = response_data.get("data", {}).get("totalElements", 0)
                if total_elements == 0:
                    return None
                
                processed_data = self.data_processor.process_api_response(response_data)
                if processed_data:
                    self.log_manager.increment_metric('records_processed')
                
                return processed_data
                
            except asyncio.TimeoutError:
                if attempt < self.config.MAX_RETRIES:
                    await asyncio.sleep(self.config.RETRY_DELAY * attempt)
                else:
                    self.log_manager.increment_metric('api_errors')
                    return None
            except Exception as e:
                if self.shutdown_event.is_set():
                    return None
                if attempt < self.config.MAX_RETRIES:
                    await asyncio.sleep(self.config.RETRY_DELAY * attempt)
                else:
                    self.log_manager.increment_metric('api_errors')
                    return None
        
        return None
    
    @asynccontextmanager
    async def _response_listener(self, page: Page):
        """Контекстный менеджер для безопасной работы с обработчиками."""
        response_future: asyncio.Future[Dict] = asyncio.Future()
        
        async def handle_response(response: Response):
            try:
                if self.config.API_ENDPOINT in response.url and not response_future.done():
                    json_data = await response.json()
                    response_future.set_result(json_data)
            except Exception as e:
                if not response_future.done():
                    response_future.set_exception(e)
        
        page.on("response", handle_response)
        
        async def wait_response():
            try:
                return await asyncio.wait_for(
                    response_future,
                    timeout=self.config.RESPONSE_TIMEOUT
                )
            except:
                return None
        
        try:
            yield wait_response
        finally:
            with suppress(Exception):
                page.remove_listener("response", handle_response)
    
    async def _data_handler(self) -> None:
        """Обработчик очереди данных с периодическим сохранением."""
        batch = []
        last_save_time = asyncio.get_event_loop().time()
        save_interval = 5.0
        total_saved = 0
        
        self.logger.info(f"💾 Data handler запущен (батч={self.config.BATCH_SIZE})")
        
        while True:
            try:
                current_time = asyncio.get_event_loop().time()
                timeout = max(0.5, save_interval - (current_time - last_save_time))
                
                item = await asyncio.wait_for(self.data_queue.get(), timeout=timeout)
                
                if item is None:
                    self.logger.info("🛑 Сигнал завершения data handler")
                    break
                
                batch.append(item['data'])
                
                current_time = asyncio.get_event_loop().time()
                should_save = (
                    len(batch) >= self.config.BATCH_SIZE or
                    (current_time - last_save_time) >= save_interval
                )
                
                if should_save and batch:
                    try:
                        tax, customs = await self.db_manager.bulk_insert_data(batch, silent=True)
                        total_inserted = tax + customs
                        total_saved += total_inserted
                        
                        self.log_manager.increment_metric('db_inserts', total_inserted)
                        
                        if total_inserted > 0:
                            self.logger.info("💾 Сохранено: TAX=%d, CUSTOMS=%d, всего=%d", tax, customs, total_saved)
                    except Exception as e:
                        self.logger.error(f"❌ Ошибка сохранения: {e}")
                    
                    batch.clear()
                    last_save_time = asyncio.get_event_loop().time()
                
            except asyncio.TimeoutError:
                if batch:
                    try:
                        tax, customs = await self.db_manager.bulk_insert_data(batch, silent=True)
                        total_saved += (tax + customs)
                        self.log_manager.increment_metric('db_inserts', tax + customs)
                        if tax + customs > 0:
                            self.logger.info(f"💾 Периодическое сохранение: TAX={tax}, CUSTOMS={customs}")
                    except Exception as e:
                        self.logger.error(f"❌ Ошибка сохранения: {e}")
                    batch.clear()
                    last_save_time = asyncio.get_event_loop().time()
            
            except Exception as e:
                self.logger.error(f"❌ Ошибка data handler: {e}", exc_info=True)
                if batch:
                    try:
                        tax, customs = await self.db_manager.bulk_insert_data(batch, silent=False)
                        self.logger.info(f"💾 Аварийное сохранение: {len(batch)} записей")
                    except Exception:
                        self.logger.critical(f"🚨 ПОТЕРЯ ДАННЫХ: {len(batch)} записей")
                    batch.clear()
        
        # Финальное сохранение
        if batch:
            try:
                tax, customs = await self.db_manager.bulk_insert_data(batch, silent=False)
                total_saved += (tax + customs)
                self.logger.info(f"💾 Финальное сохранение: TAX={tax}, CUSTOMS={customs}")
            except Exception as e:
                self.logger.critical(f"🚨 ПОТЕРЯ ДАННЫХ: {len(batch)} записей: {e}")
        
        self.logger.info(f"✅ Data handler завершен. Всего: {total_saved} записей")
    
    async def _print_final_table(self) -> None:
        """Вывод красивой итоговой таблицы."""
        self.logger.info("")
        self.logger.info("=" * 120)
        self.logger.info("📊 СВОДНАЯ ТАБЛИЦА ПО РЕГИОНАМ")
        self.logger.info("=" * 120)
        
        header = f"{'Регион':<20} | {'Записей':>10} | {'След. номер':>12} | {'Пропущено':>11} | {'Найдено':>10}"
        self.logger.info(header)
        self.logger.info("-" * 120)
        
        total_records = total_missing = total_found = 0
        
        for region_code in sorted(self.config.REGIONS.keys()):
            region_name = self.config.REGIONS[region_code]
            stats = self.region_stats.get(region_code, {
                'total_records': 0,
                'next_number': 1,
                'missing_count': 0,
                'found_new': 0
            })
            
            row = (
                f"{region_name:<20} | {stats['total_records']:>10} | "
                f"{stats['next_number']:>12} | {stats['missing_count']:>11} | "
                f"{stats['found_new']:>10}"
            )
            self.logger.info(row)
            
            total_records += stats['total_records']
            total_missing += stats['missing_count']
            total_found += stats['found_new']
        
        self.logger.info("=" * 120)
        summary = f"{'ИТОГО':<20} | {total_records:>10} | {'-':>12} | {total_missing:>11} | {total_found:>10}"
        self.logger.info(summary)
        self.logger.info("=" * 120)
        
        metrics = self.log_manager.get_metrics_summary()
        self.logger.info("")
        self.logger.info("📈 МЕТРИКИ:")
        self.logger.info(f"   └─ Время: {metrics['elapsed_seconds']}s")
        self.logger.info(f"   └─ API запросов: {metrics['api_requests']}")
        self.logger.info(f"   └─ Обработано: {metrics['records_processed']}")
        self.logger.info(f"   └─ Скорость: {metrics['records_per_second']} зап/с")

def parse_arguments():
    """Парсинг аргументов командной строки."""
    parser = argparse.ArgumentParser(description='Парсер QAMQOR')
    parser.add_argument('--missing-only', action='store_true', help='Только пропущенные')
    return parser.parse_args()


async def main():
    """Точка входа."""
    args = parse_arguments()
    mode = "missing_only" if args.missing_only else "full"
    parser = QamqorParser(mode=mode)
    success = await parser.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
        sys.exit(1)