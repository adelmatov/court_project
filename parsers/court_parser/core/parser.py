# parsers/court_parser/core/parser.py
"""
Главный класс парсера с retry и восстановлением
"""

import asyncio
import aiohttp
from typing import Optional, Dict, List, Any

from config.settings import Settings
from core.session import SessionManager
from auth.authenticator import Authenticator
from search.form_handler import FormHandler
from search.search_engine import SearchEngine
from parsing.html_parser import ResultsParser
from database.db_manager import DatabaseManager
from database.models import CaseData, SearchResult
from utils.text_processor import TextProcessor
from utils.logger import get_logger
from utils.retry import RetryStrategy, RetryConfig, NonRetriableError


class CourtParser:
    """Главный класс парсера с retry и восстановлением"""
    
    def __init__(self, config_path: Optional[str] = None, update_mode: bool = False):
        # Загрузка конфигурации
        self.settings = Settings(config_path)
        
        # РЕЖИМ РАБОТЫ
        self.update_mode = update_mode
        
        # Retry конфигурация
        self.retry_config = self.settings.config.get('retry_settings', {})
        
        # Инициализация компонентов с retry
        self.session_manager = SessionManager(
            timeout=30,
            retry_config=self.retry_config
        )
        
        self.authenticator = Authenticator(
            self.settings.base_url,
            self.settings.auth,
            retry_config=self.retry_config
        )
        
        self.form_handler = FormHandler(self.settings.base_url)
        self.search_engine = SearchEngine(self.settings.base_url)
        self.results_parser = ResultsParser()
        self.db_manager = DatabaseManager(self.settings.database)
        self.text_processor = TextProcessor()
        
        # НОВОЕ: Lock для stateful операций с формой
        self.form_lock = asyncio.Lock()
        
        # Счетчик ошибок для переавторизации
        self.session_error_count = 0
        self.max_session_errors = 10
        
        # Счетчик переавторизаций
        self.reauth_count = 0
        self.max_reauth = self.retry_config.get('session_recovery', {}).get(
            'max_reauth_attempts', 2
        )
        
        self.logger = get_logger('court_parser')
        
        # Логирование режима
        mode_name = "Update Mode" if self.update_mode else "Full Scan Mode"
        self.logger.info(f"🚀 Парсер инициализирован в режиме: {mode_name}")
    
    async def initialize(self):
        """Инициализация (подключение к БД, авторизация)"""
        try:
            await self.db_manager.connect()
            await self.authenticator.authenticate(self.session_manager)
            self.logger.info("✅ Парсер готов к работе")
        except Exception as e:
            # При ошибке инициализации закрываем ресурсы
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self):
        """Очистка ресурсов"""
        try:
            await self.db_manager.disconnect()
        except Exception as e:
            self.logger.error(f"Ошибка закрытия БД: {e}")
        
        try:
            await self.session_manager.close()
        except Exception as e:
            self.logger.error(f"Ошибка закрытия сессии: {e}")
        
        self.logger.info("Ресурсы очищены")
    
    async def _handle_session_recovery(self, error: Exception) -> bool:
        """
        Обработка восстановления сессии
        
        Returns:
            True если удалось восстановить, False если нет
        """
        reauth_on_401 = self.retry_config.get('session_recovery', {}).get('reauth_on_401', True)
        
        # Проверяем что это HTTP 401
        if not (isinstance(error, (aiohttp.ClientError, NonRetriableError)) and '401' in str(error)):
            return False
        
        if not reauth_on_401:
            return False
        
        # Проверяем лимит переавторизаций
        if self.reauth_count >= self.max_reauth:
            self.logger.error(
                f"❌ Достигнут лимит переавторизаций ({self.max_reauth})"
            )
            return False
        
        self.reauth_count += 1
        
        self.logger.warning(
            f"⚠️ HTTP 401: Сессия истекла, переавторизация "
            f"({self.reauth_count}/{self.max_reauth})..."
        )
        
        try:
            await self.authenticator.authenticate(self.session_manager)
            self.session_error_count = 0  # Сброс счетчика ошибок
            self.logger.info("✅ Переавторизация успешна")
            return True
        
        except Exception as e:
            self.logger.error(f"❌ Переавторизация не удалась: {e}")
            return False
    
    async def _handle_rate_limit(self, response: aiohttp.ClientResponse):
        """Обработка HTTP 429 (Rate Limit)"""
        rate_limit_config = self.retry_config.get('rate_limit', {})
        
        # Читаем header Retry-After
        retry_after = response.headers.get('Retry-After')
        
        if retry_after and rate_limit_config.get('respect_retry_after_header', True):
            try:
                wait_time = int(retry_after)
            except ValueError:
                wait_time = rate_limit_config.get('default_wait', 60)
        else:
            wait_time = rate_limit_config.get('default_wait', 60)
        
        self.logger.warning(
            f"⚠️ HTTP 429 (Rate Limit), ждем {wait_time} сек..."
        )
        
        await asyncio.sleep(wait_time)
        
        # TODO: Уменьшить скорость запросов (реализуем позже в adaptive)
    
    async def search_and_save_with_retry(self, region_key: str, court_key: str,
                                    case_number: str, year: str = "2025") -> Dict[str, Any]:
        """
        Поиск и сохранение дела с retry и восстановлением
        
        Returns:
            {
                'success': True/False,
                'target_found': True/False,
                'total_saved': 5,
                'related_saved': 4,
                'target_case_number': '...',
                'error': None или строка с ошибкой
            }
        """
        search_retry_config = self.retry_config.get('search_case', {})
        
        if not search_retry_config:
            # Без retry
            try:
                return await self._do_search_and_save(region_key, court_key, case_number, year)
            except NonRetriableError as e:
                return {
                    'success': False,
                    'target_found': False,
                    'total_saved': 0,
                    'related_saved': 0,
                    'error': str(e)
                }
        
        # С retry
        retry_cfg = RetryConfig(search_retry_config)
        strategy = RetryStrategy(retry_cfg, self.session_manager.circuit_breaker)
        
        async def _search_with_recovery():
            try:
                return await self._do_search_and_save(region_key, court_key, case_number, year)
            
            except Exception as e:
                # Попытка восстановления сессии
                if await self._handle_session_recovery(e):
                    # Повторяем после успешной переавторизации
                    return await self._do_search_and_save(region_key, court_key, case_number, year)
                raise
        
        try:
            result = await strategy.execute_with_retry(
                _search_with_recovery,
                error_context=f"Поиск дела {case_number}"
            )
            
            # Успех - сбрасываем счетчик ошибок
            self.session_error_count = 0
            
            return result
        
        except NonRetriableError as e:
            # Постоянная ошибка (дело не существует)
            self.session_error_count = 0
            return {
                'success': False,
                'target_found': False,
                'total_saved': 0,
                'related_saved': 0,
                'error': str(e)
            }
        
        except Exception as e:
            # Временная ошибка, но retry исчерпан
            self.session_error_count += 1
            
            self.logger.error(f"❌ Не удалось найти дело после retry: {e}")
            
            # Проверка на множественные ошибки подряд
            if self.session_error_count >= self.max_session_errors:
                self.logger.warning(
                    f"⚠️ {self.max_session_errors} ошибок подряд, "
                    f"попытка переавторизации..."
                )
                
                if await self._handle_session_recovery(Exception("Multiple failures")):
                    self.session_error_count = 0
            
            return {
                'success': False,
                'target_found': False,
                'total_saved': 0,
                'related_saved': 0,
                'error': str(e)
            }

    # Алиас для обратной совместимости
    async def search_and_save(self, *args, **kwargs):
        return await self.search_and_save_with_retry(*args, **kwargs)
    
    async def _do_search_and_save(self, region_key: str, court_key: str,
                        case_number: str, year: str) -> Dict[str, Any]:
        """
        Один цикл поиска и сохранения с Lock на stateful операции
        
        Архитектура:
        1. [LOCK] Подготовка формы + выбор региона + поиск (stateful)
        2. [БЕЗ LOCK] Парсинг HTML + сохранение в БД (stateless)
        
        Returns:
            {
                'success': True/False,
                'target_found': True/False,
                'total_saved': 5,
                'related_saved': 4,
                'target_case_number': '6294-25-00-4/1',
                'error': None или строка с ошибкой
            }
        """
        # Получение конфигурации
        region_config = self.settings.get_region(region_key)
        court_config = self.settings.get_court(region_key, court_key)
        
        # Генерация полного номера
        full_case_number = self.text_processor.generate_case_number(
            region_config, court_config, year, int(case_number)
        )
        
        self.logger.info(f"🔍 Ищу дело: {full_case_number}")
        
        # ============================================================
        # КРИТИЧЕСКАЯ СЕКЦИЯ: Stateful операции (под Lock)
        # ============================================================
        async with self.form_lock:
            self.logger.debug(f"[{region_key}] Захватил form_lock")
            
            # Получение сессии
            session = await self.session_manager.get_session()
            
            # Подготовка формы
            viewstate, form_ids = await self.form_handler.prepare_search_form(session)
            
            # Выбор региона
            await self.form_handler.select_region(
                session, viewstate, region_config['id'], form_ids
            )
            
            await asyncio.sleep(1)
            
            # Поиск
            results_html = await self.search_engine.search_case(
                session, viewstate, region_config['id'], court_config['id'],
                year, full_case_number, form_ids,
                extract_sequence=self.update_mode
            )
            
            self.logger.debug(f"[{region_key}] Освободил form_lock")
        
        # ============================================================
        # Stateless операции (БЕЗ Lock — параллельно для всех)
        # ============================================================
        
        # Парсинг
        cases = self.results_parser.parse(results_html)
        
        if not cases:
            self.logger.info(f"❌ Ничего не найдено: {full_case_number}")
            return {
                'success': False,
                'target_found': False,
                'total_saved': 0,
                'related_saved': 0,
                'target_case_number': full_case_number,
                'error': 'no_results'
            }
        
        # Сохранение всех найденных дел
        saved_count = 0
        target_found = False
        related_count = 0
        
        for case in cases:
            # Проверка что дело из нашего региона/суда
            if not case.case_number.startswith(
                f"{region_config['kato_code']}{court_config['instance_code']}"
            ):
                continue
            
            # Проверка целевого дела
            is_target = (case.case_number == full_case_number)
            
            if is_target:
                target_found = True
            
            # Сохранение
            save_result = await self.db_manager.save_case(case)
            
            if save_result['status'] in ['saved', 'updated']:
                saved_count += 1
                
                if is_target:
                    judge = "✅ судья" if case.judge else "⚠️ без судьи"
                    parties = len(case.plaintiffs) + len(case.defendants)
                    events = len(case.events)
                    self.logger.info(
                        f"✅ ЦЕЛЕВОЕ: {case.case_number} "
                        f"({judge}, {parties} стороны, {events} события)"
                    )
                else:
                    related_count += 1
                    self.logger.debug(f"💾 Связанное: {case.case_number}")
        
        # Итоговый лог
        if saved_count > 0:
            if saved_count > 1:
                self.logger.info(
                    f"💾 Всего сохранено: {saved_count} дел "
                    f"(целевое: {1 if target_found else 0}, связанных: {related_count})"
                )
            
            if not target_found:
                self.logger.warning(
                    f"⚠️ Целевое дело {full_case_number} не найдено, "
                    f"но сохранено {saved_count} связанных"
                )
            
            return {
                'success': True,
                'target_found': target_found,
                'total_saved': saved_count,
                'related_saved': related_count,
                'target_case_number': full_case_number
            }
        else:
            self.logger.info(f"❌ Ничего не сохранено для дела {full_case_number}")
            
            return {
                'success': False,
                'target_found': False,
                'total_saved': 0,
                'related_saved': 0,
                'target_case_number': full_case_number,
                'error': 'nothing_saved'
            }
    
    # Алиас для обратной совместимости
    async def search_and_save(self, *args, **kwargs):
        return await self.search_and_save_with_retry(*args, **kwargs)
    
    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()
        # Не подавляем исключения
        return False