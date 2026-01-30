"""
QAMQOR Updater - Апдейтер существующих записей.
Синхронизирован с основным парсером.
"""

import asyncio
import argparse
import re
import signal
import sys
import random
from datetime import datetime, timedelta
from types import FrameType
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager, suppress
from signal import Signals

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


class QamqorUpdater:
    """Апдейтер с синхронизацией парсера."""
    
    def __init__(
        self, 
        force: bool = False, 
        statuses: Optional[List[str]] = None,
        single_number: Optional[str] = None
    ) -> None:
        self.config = Config()
        self.log_manager = LogManager(self.config, name="qamqor_updater")
        self.logger = self.log_manager.logger
        
        # ✅ Валидация номера с логированием
        if single_number:
            pattern = r'^' + self.config.YEAR_PREFIX + r'\d{7}170101[12]/\d{5}$'
            
            if not re.match(pattern, single_number):
                self.logger.error(f"Неверный формат номера: {single_number}")
                self.logger.error(
                    f"Ожидается: {self.config.YEAR_PREFIX}RRRRRRR170101T/SSSSS"
                )
                raise ValueError(f"Неверный формат номера: {single_number}")
        
        self.db_manager = DatabaseManager(self.config, self.logger)
        self.data_processor = DataProcessor(self.config, self.logger)
        self.api_validator = APIValidator(self.logger)
        self.web_client = WebClient(self.config, self.logger, self.api_validator)
        
        self.force = force
        self.statuses = statuses or self.config.UPDATE_STATUSES
        self.single_number = single_number
        
        self.update_queue: asyncio.Queue = asyncio.Queue()
        self.data_queue: asyncio.Queue = asyncio.Queue()
        self.shutdown_event = asyncio.Event()
        self.active_workers: List[asyncio.Task] = []
        
        self.stats = {
            'total_to_update': 0,
            'processed': 0,
            'updated': 0,
            'unchanged': 0,
            'not_found': 0,
            'errors': 0
        }
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum: Signals, frame: Optional[FrameType]) -> None:
        self.logger.warning(f"⚠️ Получен сигнал {signum}")
        self.shutdown_event.set()
    
    async def _graceful_shutdown(self, timeout: float = 30.0):
        """Корректное завершение."""
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
        """Главная функция."""
        try:
            self.logger.info("=" * 80)
            self.logger.info("🔄 ЗАПУСК АПДЕЙТЕРА QAMQOR")
            self.logger.info("=" * 80)
            
            await self.db_manager.initialize_tables()
            
            # Если указан конкретный номер
            if self.single_number:
                self.logger.info("🎯 Режим: ОБНОВЛЕНИЕ ОДНОГО НОМЕРА")
                self.logger.info(f"   Номер: {self.single_number}")
                
                try:
                    # ✅ Позиция типа проверки: 2 (год) + 7 (регион) + 6 (170101) = 15
                    check_type = self.single_number[15]
                    
                    if check_type == '1':
                        records = {'tax': [self.single_number], 'customs': []}
                        self.logger.info("   Тип: 📊 Налоговая проверка (TAX)")
                    elif check_type == '2':
                        records = {'tax': [], 'customs': [self.single_number]}
                        self.logger.info("   Тип: 🚢 Таможенная проверка (CUSTOMS)")
                    else:
                        self.logger.error(f"❌ Неверный тип проверки: '{check_type}'")
                        return False
                        
                except (IndexError, ValueError) as e:
                    self.logger.error(f"❌ Неверный формат номера: {e}")
                    return False
                
                total = 1
                self.stats['total_to_update'] = total
            else:
                # Массовое обновление
                self.logger.info("📊 Критерии отбора:")
                self.logger.info(f"   ├─ Статусы: {self.statuses}")
                self.logger.info(f"   ├─ Минимальный возраст: {self.config.UPDATE_MIN_AGE_DAYS} дней")
                self.logger.info(f"   ├─ Максимальный возраст: {self.config.UPDATE_MAX_AGE_DAYS} дней")
                self.logger.info(f"   ├─ Cooldown: {self.config.UPDATE_COOLDOWN_DAYS} дней")
                self.logger.info(f"   └─ Принудительно: {'Да' if self.force else 'Нет'}")
                
                records = await self.db_manager.get_records_to_update(
                    statuses=self.statuses,
                    force=self.force
                )
                
                total = len(records['tax']) + len(records['customs'])
                self.stats['total_to_update'] = total
                
                if total > 0:
                    self.logger.info("")
                    self.logger.info("📋 Найдено записей:")
                    self.logger.info(f"   ├─ TAX: {len(records['tax'])}")
                    self.logger.info(f"   ├─ CUSTOMS: {len(records['customs'])}")
                    self.logger.info(f"   └─ ВСЕГО: {total}")
            
            if total == 0:
                self.logger.info("✅ Нет записей для обновления")
                return True
            
            self.logger.info("")
            await self._run_update_process(records)
            await self._print_update_summary()
            
            self.logger.info("=" * 80)
            self.logger.info("✅ ОБНОВЛЕНИЕ ЗАВЕРШЕНО")
            self.logger.info("=" * 80)
            return True
            
        except KeyboardInterrupt:
            self.logger.warning("⚠️ Прервано пользователем")
            return False
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
            return False
    
    async def _run_update_process(self, records: Dict[str, List[str]]):
        """Процесс обновления."""
        all_numbers = records['tax'] + records['customs']
        
        if not all_numbers:
            self.logger.warning("⚠️ Нет номеров для обработки")
            return
        
        async with async_playwright() as playwright:
            self.logger.info("🔧 Запуск браузера...")
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-software-rasterizer"
                ]
            )
            
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                locale="ru-RU,ru",
                timezone_id="Asia/Almaty",
                extra_http_headers={
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                }
            )
            
            # Health check
            self.logger.info("🏥 Health Check...")
            health_page = await context.new_page()
            
            await apply_stealth(health_page)
            
            try:
                await health_page.goto(
                    self.config.SEARCH_URL,
                    wait_until="domcontentloaded",
                    timeout=self.config.PAGE_TIMEOUT
                )
                
                await health_page.wait_for_selector(
                    'input[placeholder="Тексеру/ тіркеу нөмірі"]',
                    state="visible",
                    timeout=10000
                )
                
                if not await self.web_client.check_api_health_with_page(health_page):
                    self.logger.error("❌ API недоступен")
                    return
                
                self.logger.info("✅ Health Check пройден")
                
            finally:
                await health_page.close()
            
            # Инициализация TabManager со stealth
            tab_manager = StealthTabManager(context, self.config, self.logger)
            await tab_manager.initialize()
            
            # Запуск data handler
            data_handler_task = asyncio.create_task(
                self._update_data_handler(),
                name="update_data_handler"
            )
            
            # Заполняем очередь номерами ДО запуска воркеров
            self.logger.info(f"📋 Добавление {len(all_numbers)} номеров в очередь...")
            for reg_num in all_numbers:
                await self.update_queue.put(reg_num)
            self.logger.info(f"✅ Очередь заполнена: {self.update_queue.qsize()} номеров")
            
            # Запуск воркеров
            self.active_workers = [
                asyncio.create_task(
                    self._update_worker(worker_id, tab_manager),
                    name=f"update_worker_{worker_id}"
                )
                for worker_id in range(self.config.MAX_CONCURRENT_TABS)
            ]
            
            self.logger.info("👷 Запущено %d воркеров", len(self.active_workers))
            
            try:
                await asyncio.gather(*self.active_workers, return_exceptions=True)
                
                if self.shutdown_event.is_set():
                    await self._graceful_shutdown(timeout=30.0)
                    
            except KeyboardInterrupt:
                await self._graceful_shutdown(timeout=30.0)
                
            finally:
                try:
                    self.logger.info("🛑 Остановка data handler...")
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
                
                await tab_manager.close_all()
                await browser.close()
                self.logger.info("🔒 Браузер закрыт")
    
    async def _update_worker(
        self, 
        worker_id: int, 
        tab_manager: StealthTabManager
    ) -> None:
        """Воркер обновления с постоянной вкладкой."""
        self.logger.debug(f"✅ UW{worker_id} запущен")
        
        async with tab_manager.get_tab() as page:
            try:
                await page.goto(
                    self.config.SEARCH_URL,
                    wait_until="domcontentloaded",
                    timeout=self.config.PAGE_TIMEOUT
                )
                
                await page.wait_for_selector(
                    'input[placeholder="Тексеру/ тіркеу нөмірі"]',
                    state="visible",
                    timeout=10000
                )
                
                self.logger.info(f"✅ UW{worker_id}: Страница загружена")
                
            except Exception as e:
                self.logger.error(f"❌ UW{worker_id}: Ошибка загрузки страницы: {e}")
                return
            
            processed_count = 0
            
            while not self.shutdown_event.is_set():
                try:
                    reg_num = await asyncio.wait_for(
                        self.update_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    if self.update_queue.empty():
                        self.logger.debug(f"✅ UW{worker_id}: Очередь пуста, завершение")
                        break
                    continue
                except asyncio.CancelledError:
                    break
                
                try:
                    result = await self._fetch_record_data(page, reg_num, worker_id)
                    if result:
                        await self.data_queue.put({'type': 'data', 'data': result})
                        self.stats['processed'] += 1
                        processed_count += 1
                    else:
                        self.stats['not_found'] += 1
                        
                except Exception as e:
                    self.stats['errors'] += 1
                    self.logger.error(f"❌ UW{worker_id} | {reg_num}: {e}")
                finally:
                    self.update_queue.task_done()
            
            self.logger.info(f" ✅ UW{worker_id} завершен (обработано: {processed_count})")
    
    async def _fetch_record_data(
        self,
        page: Page,
        registration_number: str,
        worker_id: int
    ) -> Optional[Dict]:
        """Получение данных записи."""
        self.log_manager.increment_metric('api_requests')
        
        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                if self.shutdown_event.is_set():
                    return None
                
                await asyncio.sleep(
                    random.uniform(
                        self.config.NATURAL_DELAY_MIN, 
                        self.config.NATURAL_DELAY_MAX
                    )
                )

                input_selector = 'input[placeholder="Тексеру/ тіркеу нөмірі"]'
                await page.fill(input_selector, '')
                await page.fill(input_selector, registration_number)

                await asyncio.sleep(
                    random.uniform(
                        self.config.TYPING_DELAY_MIN, 
                        self.config.TYPING_DELAY_MAX
                    )
                )
                
                async with self._response_listener(page) as wait_response:
                    await page.click("button.btn.btn-primary:has-text('Іздеу')")
                    response_data = await wait_response()
                
                if not response_data:
                    if attempt < self.config.MAX_RETRIES:
                        await asyncio.sleep(self.config.RETRY_DELAY)
                        continue
                    return None
                
                is_valid, error_msg = self.api_validator.validate_response(
                    response_data,
                    context=f"UW{worker_id}:{registration_number}"
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
                
                if response_data.get("data", {}).get("totalElements", 0) == 0:
                    return None
                
                processed = self.data_processor.process_api_response(response_data)
                if processed:
                    self.log_manager.increment_metric('records_processed')
                return processed
                
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
        """Контекстный менеджер для обработчика."""
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
    
    async def _update_data_handler(self) -> None:
        """Обработчик данных обновления."""
        batch = []
        last_save_time = asyncio.get_event_loop().time()
        save_interval = 5.0
        total_saved = 0
        
        self.logger.info(f"💾 Update data handler запущен (батч={self.config.UPDATE_BATCH_SIZE})")
        
        while True:
            try:
                current_time = asyncio.get_event_loop().time()
                timeout = max(0.5, save_interval - (current_time - last_save_time))
                
                item = await asyncio.wait_for(self.data_queue.get(), timeout=timeout)
                
                if item is None:
                    self.logger.info("🛑 Сигнал завершения update data handler")
                    break
                
                if isinstance(item, dict) and item.get('type') == 'data':
                    batch.append(item['data'])
                
                current_time = asyncio.get_event_loop().time()
                should_save = (
                    len(batch) >= self.config.UPDATE_BATCH_SIZE or
                    (current_time - last_save_time) >= save_interval
                )
                
                if should_save and batch:
                    try:
                        tax, customs, changes = await self.db_manager.bulk_update_data(batch, silent=True)
                        total = tax + customs
                        self.stats['updated'] += total
                        self.stats['unchanged'] += len(batch) - total
                        total_saved += total
                        
                        self.log_manager.increment_metric('db_updates', total)
                        
                        if total > 0:
                            self.logger.info("🔄 Обновлено: TAX=%d, CUSTOMS=%d, всего=%d", tax, customs, total_saved)
                    except Exception as e:
                        self.logger.error(f"❌ Ошибка обновления: {e}")
                    
                    batch.clear()
                    last_save_time = asyncio.get_event_loop().time()
                    
            except asyncio.TimeoutError:
                if batch:
                    try:
                        tax, customs, changes = await self.db_manager.bulk_update_data(batch, silent=True)
                        total = tax + customs
                        self.stats['updated'] += total
                        self.stats['unchanged'] += len(batch) - total
                        total_saved += total
                        
                        self.log_manager.increment_metric('db_updates', total)
                        
                        if total > 0:
                            self.logger.info(f"🔄 Периодическое обновление: TAX={tax}, CUSTOMS={customs}")
                    except Exception as e:
                        self.logger.error(f"❌ Ошибка обновления: {e}")
                    
                    batch.clear()
                    last_save_time = asyncio.get_event_loop().time()
            
            except Exception as e:
                self.logger.error(f"❌ Ошибка update data handler: {e}", exc_info=True)
                if batch:
                    try:
                        tax, customs, changes = await self.db_manager.bulk_update_data(batch, silent=False)
                        self.logger.info(f"💾 Аварийное обновление: {len(batch)} записей")
                    except Exception:
                        self.logger.critical(f"🚨 ПОТЕРЯ ДАННЫХ: {len(batch)} записей")
                    batch.clear()
        
        # Финальное сохранение
        if batch:
            try:
                tax, customs, changes = await self.db_manager.bulk_update_data(batch, silent=False)
                total = tax + customs
                total_saved += total
                self.stats['updated'] += total
                self.stats['unchanged'] += len(batch) - total
                self.logger.info(f"💾 Финальное обновление: TAX={tax}, CUSTOMS={customs}")
            except Exception as e:
                self.logger.critical(f"🚨 ПОТЕРЯ ДАННЫХ: {len(batch)} записей: {e}")
        
        self.logger.info(f"✅ Update data handler завершен. Всего обновлено: {total_saved}")
    
    async def _print_update_summary(self) -> None:
        """Итоговая статистика."""
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("📊 РЕЗУЛЬТАТЫ ОБНОВЛЕНИЯ")
        self.logger.info("=" * 80)
        self.logger.info(f"📋 Всего записей: {self.stats['total_to_update']}")
        self.logger.info(f"   ├─ Обработано: {self.stats['processed']}")
        self.logger.info(f"   ├─ Обновлено: {self.stats['updated']}")
        self.logger.info(f"   ├─ Без изменений: {self.stats['unchanged']}")
        self.logger.info(f"   ├─ Не найдено: {self.stats['not_found']}")
        self.logger.info(f"   └─ Ошибок: {self.stats['errors']}")
        self.logger.info("=" * 80)
        
        metrics = self.log_manager.get_metrics_summary()
        self.logger.info("")
        self.logger.info("📈 МЕТРИКИ:")
        self.logger.info(f"   └─ Время: {metrics['elapsed_seconds']}s")
        self.logger.info(f"   └─ API запросов: {metrics['api_requests']}")
        self.logger.info(f"   └─ Обработано: {metrics['records_processed']}")
        self.logger.info(f"   └─ Скорость: {metrics['records_per_second']} зап/с")


def parse_arguments():
    """Парсинг аргументов."""
    parser = argparse.ArgumentParser(
        description='Апдейтер QAMQOR',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Обновить конкретный номер (2026 год)
  python -m parsers.qamqor.qamqor_updater --number "2610000001701011/00123"
  
  # Массовое обновление по статусу "1"
  python -m parsers.qamqor.qamqor_updater --status "1"
  
  # Принудительное обновление всех
  python -m parsers.qamqor.qamqor_updater --force
        """
    )
    
    parser.add_argument(
        '--number',
        type=str,
        help='Обновить конкретный номер (пример: 2610000001701011/00123)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Принудительное обновление всех записей (игнорирует фильтры)'
    )
    parser.add_argument(
        '--status',
        type=str,
        help='Статусы для обновления через запятую (по умолчанию: "1")'
    )
    
    return parser.parse_args()


async def main():
    """Точка входа."""
    args = parse_arguments()
    
    # Валидация комбинации аргументов
    if args.number and (args.status or args.force):
        print("❌ --number нельзя использовать с --status или --force")
        sys.exit(1)
    
    statuses = [s.strip() for s in args.status.split(',')] if args.status else None
    
    try:
        updater = QamqorUpdater(
            force=args.force,
            statuses=statuses,
            single_number=args.number
        )
    except ValueError:
        # Ошибка валидации номера уже залогирована в __init__
        sys.exit(1)
    
    success = await updater.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
        sys.exit(1)