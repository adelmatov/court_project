"""
Изолированный воркер для обработки одного региона
"""
import ssl
import asyncio
from typing import Dict, Any, Optional

import aiohttp

from config.settings import Settings
from auth.authenticator import Authenticator
from search.form_handler import FormHandler
from search.search_engine import SearchEngine
from parsing.html_parser import ResultsParser
from database.models import CaseData
from utils.text_processor import TextProcessor
from utils.logger import setup_worker_logger
from utils.constants import CaseStatus
import traceback


class RegionWorker:
    """
    Изолированный воркер для одного региона
    
    Каждый воркер имеет:
    - Свою HTTP-сессию
    - Свой FormHandler с отдельным кешем
    - Свой SearchEngine
    
    Это обеспечивает полную изоляцию состояния формы на сервере.
    """
    
    def __init__(self, settings: Settings, region_key: str):
        self.settings = settings
        self.region_key = region_key
        self.logger = setup_worker_logger(region_key)
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.form_handler: Optional[FormHandler] = None
        self.search_engine: Optional[SearchEngine] = None
        self.results_parser = ResultsParser()
        self.text_processor = TextProcessor()
        
        self.authenticated = False
        self.retry_config = settings.config.get('retry_settings', {})
    
    async def initialize(self) -> bool:
        """
        Инициализация воркера: создание сессии и авторизация
        
        Returns:
            True если успешно, False при ошибке
        """
        try:
            await self._create_session()
            
            self.form_handler = FormHandler(self.settings.base_url)
            self.search_engine = SearchEngine(self.settings.base_url)
            
            authenticator = Authenticator(
                self.settings.base_url,
                self.settings.auth,
                retry_config=self.retry_config
            )
            
            self.authenticated = await authenticator.authenticate(self)
            
            if self.authenticated:
                self.logger.info(f"Воркер {self.region_key} авторизован")
            
            return self.authenticated
            
        except Exception as e:
            self.logger.error(f"Ошибка инициализации воркера {self.region_key}: {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            return False
    
    async def _create_session(self):
        """Создание изолированной HTTP-сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=5)
        timeout = aiohttp.ClientTimeout(total=30)
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )
        
        self.logger.debug(f"Создана сессия для {self.region_key}")
    
    async def create_session(self):
        """Публичный метод для пересоздания сессии (для Authenticator)"""
        await self._create_session()
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Получить сессию (для Authenticator)"""
        if not self.session or self.session.closed:
            await self._create_session()
        return self.session
    
    async def search_and_save(
        self,
        db_manager,
        court_key: str,
        sequence_number: int,
        year: str
    ) -> Dict[str, Any]:
        """
        Поиск и сохранение дела
        
        Args:
            db_manager: менеджер БД (общий)
            court_key: ключ суда ('smas', 'appellate')
            sequence_number: порядковый номер дела
            year: год
        
        Returns:
            {
                'success': True/False,
                'saved': True/False,
                'case_number': str,
                'error': str or None
            }
        """
        if not self.authenticated:
            return {
                'success': False,
                'saved': False,
                'case_number': None,
                'error': 'not_authenticated'
            }
        
        try:
            return await self._do_search_and_save(
                db_manager, court_key, sequence_number, year
            )
        except Exception as e:
            self.logger.error(f"Ошибка поиска #{sequence_number}: {e}")
            return {
                'success': False,
                'saved': False,
                'case_number': None,
                'error': str(e)
            }
    
    async def _do_search_and_save(
        self,
        db_manager,
        court_key: str,
        sequence_number: int,
        year: str
    ) -> Dict[str, Any]:
        """Выполнение поиска и сохранения"""
        region_config = self.settings.get_region(self.region_key)
        court_config = self.settings.get_court(self.region_key, court_key)
        
        target_case_number = self.text_processor.generate_case_number(
            region_config, court_config, year, sequence_number
        )
        
        self.logger.debug(f"Поиск: {target_case_number}")
        
        results_html, cases = await self._search_case(
            region_config, court_config, year, sequence_number
        )
        
        if results_html is None:
            return {
                'success': False,
                'saved': False,
                'case_number': target_case_number,
                'error': CaseStatus.REGION_NOT_FOUND
            }
        
        if not cases:
            self.logger.debug(f"Не найдено: {target_case_number}")
            return {
                'success': False,
                'saved': False,
                'case_number': target_case_number,
                'error': CaseStatus.NO_RESULTS
            }
        
        case_to_save = next(
            (c for c in cases if c.case_number == target_case_number),
            None
        )
        
        if not case_to_save:
            self.logger.debug(f"Целевое дело не найдено среди {len(cases)} результатов")
            return {
                'success': False,
                'saved': False,
                'case_number': target_case_number,
                'error': CaseStatus.TARGET_NOT_FOUND
            }
        
        save_result = await db_manager.save_case(case_to_save)
        
        if save_result['status'] in [CaseStatus.SAVED, CaseStatus.UPDATED]:
            self.logger.info(
                f"Сохранено: {case_to_save.case_number} | "
                f"судья: {'да' if case_to_save.judge else 'нет'} | "
                f"сторон: {len(case_to_save.plaintiffs) + len(case_to_save.defendants)} | "
                f"событий: {len(case_to_save.events)}"
            )
            
            return {
                'success': True,
                'saved': True,
                'case_number': case_to_save.case_number,
                'results_html': results_html
            }
        
        return {
            'success': False,
            'saved': False,
            'case_number': target_case_number,
            'error': CaseStatus.SAVE_FAILED
        }
    
    async def _search_case(
        self,
        region_config: Dict,
        court_config: Dict,
        year: str,
        sequence_number: int
    ) -> tuple:
        """
        Выполнение поиска дела
        
        Returns:
            (results_html, [CaseData, ...])
        """
        viewstate, form_ids = await self.form_handler.prepare_search_form(
            self.session
        )
        
        await self.form_handler.select_region(
            self.session,
            viewstate,
            region_config['id'],
            form_ids
        )
        
        await asyncio.sleep(0.5)
        
        results_html = await self.search_engine.search_case(
            self.session,
            viewstate,
            region_config['id'],
            court_config['id'],
            year,
            sequence_number,
            form_ids
        )
        
        cases = self.results_parser.parse(results_html)
        
        return results_html, cases
    
    async def search_case_by_number(self, case_number: str) -> tuple:
        """
        Поиск дела по номеру
        
        Args:
            case_number: полный номер дела
        
        Returns:
            (results_html, [CaseData, ...])
        """
        case_info = self.text_processor.find_region_and_court_by_case_number(
            case_number, self.settings.regions
        )
        
        if not case_info:
            self.logger.warning(f"Не удалось определить регион: {case_number}")
            return None, []
        
        region_config = self.settings.get_region(case_info['region_key'])
        court_config = self.settings.get_court(
            case_info['region_key'], 
            case_info['court_key']
        )
        
        return await self._search_case(
            region_config,
            court_config,
            case_info['year'],
            int(case_info['sequence'])
        )
    
    async def cleanup(self):
        """Очистка ресурсов воркера"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.debug(f"Сессия {self.region_key} закрыта")
        
        self.authenticated = False
    
    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()
        return False