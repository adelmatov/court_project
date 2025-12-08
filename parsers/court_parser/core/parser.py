"""
Главный класс парсера с retry и восстановлением
"""
from typing import Dict, Any, Optional, List, Tuple
import asyncio
import aiohttp

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
from utils.constants import CaseStatus
import traceback

class CourtParser:
    """Главный класс парсера"""
    
    def __init__(self, config_path: Optional[str] = None):
        # Загрузка конфигурации
        self.settings = Settings(config_path)
        
        # Retry конфигурация
        self.retry_config = self.settings.config.get('retry_settings', {})
        
        # Инициализация компонентов
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
        
        # Lock для stateful операций
        self.form_lock = asyncio.Lock()
        
        # Счётчики ошибок
        self.session_error_count = 0
        self.max_session_errors = 10
        self.reauth_count = 0
        self.max_reauth = self.retry_config.get('session_recovery', {}).get(
            'max_reauth_attempts', 2
        )
        
        
        self.logger = get_logger('court_parser')
        self.logger.info("🚀 Парсер инициализирован")

    async def search_case_by_number(self, case_number: str) -> Tuple[Optional[str], List[CaseData]]:
        """
        Поиск дела по номеру
        
        Args:
            case_number: полный номер дела (например '7599-25-00-4а/215')
        
        Returns:
            (results_html, parsed_cases) — HTML для документов и список дел
            (None, []) — если не удалось определить регион
        """
        # Определяем регион и суд по номеру дела
        case_info = self.text_processor.find_region_and_court_by_case_number(
            case_number, self.settings.regions
        )
        
        if not case_info:
            self.logger.warning(f"⚠️ Не удалось определить регион: {case_number}")
            return None, []
        
        region_config = self.settings.get_region(case_info['region_key'])
        court_config = self.settings.get_court(case_info['region_key'], case_info['court_key'])
        
        # Поиск через форму
        async with self.form_lock:
            session = await self.session_manager.get_session()
            
            viewstate, form_ids = await self.form_handler.prepare_search_form(session)
            
            # Передаём весь region_config для поддержки search_region_id
            await self.form_handler.select_region(
                session, viewstate, region_config, form_ids
            )
            
            await asyncio.sleep(1)
            
            results_html = await self.search_engine.search_case(
                session, viewstate,
                region_config.get('search_region_id', region_config['id']),
                court_config['id'],
                case_info['year'],
                int(case_info['sequence']),
                form_ids
            )
        
        # Парсинг
        cases = self.results_parser.parse(results_html)
        
        return results_html, cases
    
    async def initialize(self):
        """Инициализация"""
        try:
            await self.db_manager.connect()
            await self.authenticator.authenticate(self.session_manager)
            self.logger.info("✅ Парсер готов к работе")
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            await self.cleanup()
            raise
    
    async def cleanup(self):
        """Очистка ресурсов"""
        try:
            await self.db_manager.disconnect()
        except:
            pass
        
        try:
            await self.session_manager.close()
        except:
            pass
        
        self.logger.info("Ресурсы очищены")
    
    async def search_and_save(
        self, 
        region_key: str, 
        court_key: str,
        sequence_number: int, 
        year: str = "2025"
    ) -> Dict[str, Any]:
        """
        Поиск и сохранение дела
        
        Args:
            region_key: ключ региона ('astana')
            court_key: ключ суда ('smas', 'appellate')
            sequence_number: порядковый номер (1, 2, 3, ...)
            year: год ('2025')
        
        Returns:
            {
                'success': True/False,
                'saved': True/False,
                'case_number': '6294-25-00-4/215',
                'error': None или строка
            }
        """
        search_retry_config = self.retry_config.get('search_case', {})
        
        if not search_retry_config:
            return await self._do_search_and_save(
                region_key, court_key, sequence_number, year
            )
        
        # С retry
        retry_cfg = RetryConfig(search_retry_config)
        strategy = RetryStrategy(retry_cfg, self.session_manager.circuit_breaker)
        
        async def _search_with_recovery():
            try:
                return await self._do_search_and_save(
                    region_key, court_key, sequence_number, year
                )
            except Exception as e:
                if await self._handle_session_recovery(e):
                    return await self._do_search_and_save(
                        region_key, court_key, sequence_number, year
                    )
                raise
        
        try:
            result = await strategy.execute_with_retry(
                _search_with_recovery,
                error_context=f"Поиск дела #{sequence_number}"
            )
            self.session_error_count = 0
            return result
        
        except NonRetriableError as e:
            return {
                'success': False,
                'saved': False,
                'case_number': None,
                'error': str(e)
            }
        
        except Exception as e:
            self.session_error_count += 1
            self.logger.error(f"❌ Ошибка поиска: {e}")
            return {
                'success': False,
                'saved': False,
                'case_number': None,
                'error': str(e)
            }
    
    async def _do_search_and_save(
        self, 
        region_key: str, 
        court_key: str,
        sequence_number: int, 
        year: str
    ) -> Dict[str, Any]:
        """
        Один цикл поиска и сохранения
        
        Сохраняет все дела, соответствующие целевому номеру,
        включая варианты с суффиксами (1), (2) и т.д.
        """
        region_config = self.settings.get_region(region_key)
        court_config = self.settings.get_court(region_key, court_key)
        
        target_case_number = self.text_processor.generate_case_number(
            region_config, court_config, year, sequence_number
        )
        
        self.logger.info(f"🔍 Ищу дело: {target_case_number}")
        
        # Используем общий метод поиска
        results_html, cases = await self.search_case_by_number(target_case_number)
        
        if results_html is None:
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.REGION_NOT_FOUND
            }
        
        if not cases:
            self.logger.info(f"❌ Ничего не найдено: {target_case_number}")
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.NO_RESULTS
            }
        
        # Находим ВСЕ дела, соответствующие целевому номеру (включая суффиксы)
        matching_cases = [
            case for case in cases 
            if self.text_processor.is_matching_case_number(case.case_number, target_case_number)
        ]
        
        if not matching_cases:
            self.logger.warning(f"⚠️ Целевое дело не найдено: {target_case_number}")
            self.logger.debug(f"Получено {len(cases)} дел: {[c.case_number for c in cases]}")
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.TARGET_NOT_FOUND
            }
        
        # Сохраняем все найденные дела
        saved_count = 0
        saved_numbers = []
        
        for case in matching_cases:
            save_result = await self.db_manager.save_case(case)
            
            if save_result['status'] in [CaseStatus.SAVED, CaseStatus.UPDATED]:
                saved_count += 1
                saved_numbers.append(case.case_number)
                
                judge_info = "✅ судья" if case.judge else "⚠️ без судьи"
                parties = len(case.plaintiffs) + len(case.defendants)
                events = len(case.events)
                
                self.logger.info(
                    f"✅ Сохранено: {case.case_number} "
                    f"({judge_info}, {parties} сторон, {events} событий)"
                )
        
        if saved_count > 0:
            return {
                'success': True,
                'saved': True,
                'saved_count': saved_count,
                'case_numbers': saved_numbers,
                'case_number': target_case_number,
                'results_html': results_html
            }
        
        return {
            'success': False,
            'saved': False,
            'saved_count': 0,
            'case_numbers': [],
            'case_number': target_case_number,
            'error': CaseStatus.SAVE_FAILED
        }
  
    async def _handle_session_recovery(self, error: Exception) -> bool:
        """Восстановление сессии"""
        if not (isinstance(error, (aiohttp.ClientError, NonRetriableError)) 
                and '401' in str(error)):
            return False
        
        if self.reauth_count >= self.max_reauth:
            return False
        
        self.reauth_count += 1
        self.logger.warning(f"⚠️ Переавторизация ({self.reauth_count}/{self.max_reauth})...")
        
        try:
            await self.authenticator.authenticate(self.session_manager)
            self.form_handler.reset_cache()
            self.session_error_count = 0
            self.logger.info("✅ Переавторизация успешна")
            return True
        except Exception as e:
            self.logger.error(f"❌ Переавторизация не удалась: {e}")
            return False 
    
    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()
        return False