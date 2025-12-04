"""
Работа с поисковой формой
"""
from typing import Dict, Optional
import asyncio
import re
import aiohttp
from selectolax.parser import HTMLParser

from utils.logger import get_logger
from utils.retry import NonRetriableError
from utils.http_utils import HttpHeaders, ViewStateExtractor


class FormHandler:
    """Обработчик поисковой формы с кешированием ID"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('form_handler')
        
        # Кеш ID формы (извлекается один раз за сессию)
        self._cached_form_ids: Optional[Dict[str, str]] = None
        self._cache_initialized: bool = False
        self._cache_lock: asyncio.Lock = asyncio.Lock()
    
    def reset_cache(self):
        """
        Сброс кеша ID формы
        
        Вызывать при:
        - Переавторизации
        - Ошибках, связанных с невалидными ID
        """
        self._cached_form_ids = None
        self._cache_initialized = False
        self.logger.debug("Кеш ID формы сброшен")
    
    async def prepare_search_form(self, session: aiohttp.ClientSession) -> tuple:
        """
        Подготовка формы поиска
        
        - ViewState: извлекается КАЖДЫЙ раз (уникален для каждого запроса)
        - Form IDs: извлекаются ОДИН раз и кешируются
        
        Returns:
            (viewstate, form_ids)
        """
        url = f"{self.base_url}/form/lawsuit/"
        headers = self._get_headers()
        
        try:
            async with session.get(url, headers=headers) as response:
                # Обработка HTTP ошибок
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}: Постоянная ошибка")
                
                if response.status in [500, 502, 503, 504]:
                    raise aiohttp.ClientError(f"HTTP {response.status}: Сервер недоступен")
                
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}: Неожиданная ошибка")
                
                html = await response.text()
                
                # ViewState — всегда извлекаем заново
                viewstate = self._extract_viewstate(html)
                
                # Form IDs — извлекаем только один раз (с блокировкой)
                async with self._cache_lock:
                    if not self._cache_initialized:
                        self._cached_form_ids = self._extract_form_ids(html)
                        self._cache_initialized = True
                        
                        self.logger.info("📋 ID формы извлечены и закешированы:")
                        for key, value in self._cached_form_ids.items():
                            self.logger.info(f"   {key}: {value}")
                
                return viewstate, self._cached_form_ids
        
        except (aiohttp.ClientError, NonRetriableError):
            raise
        
        except Exception as e:
            self.logger.error(f"Ошибка подготовки формы: {e}")
            raise aiohttp.ClientError(f"Ошибка подготовки формы: {e}")

    def reset_cache(self):
        """
        Сброс кеша ID формы
        
        Вызывать при:
        - Переавторизации
        - Ошибках, связанных с невалидными ID
        """
        # Примечание: этот метод синхронный, но безопасен
        # т.к. просто сбрасывает флаги (атомарные операции в Python)
        self._cached_form_ids = None
        self._cache_initialized = False
        self.logger.debug("Кеш ID формы сброшен")
    
    async def select_region(self, session: aiohttp.ClientSession, 
                           viewstate: str, region_id: str, 
                           form_ids: Dict[str, str]):
        """Выбор региона в форме"""
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('form_base', 'j_idt45:j_idt46')
        
        data = {
            form_base: form_base,
            f'{form_base}:edit-district': region_id,
            f'{form_base}:edit-district-hide': '',
            f'{form_base}:edit-court': '',
            f'{form_base}:edit-year': '',
            f'{form_base}:edit-iin': '',
            f'{form_base}:edit-num': '',
            f'{form_base}:edit-fio': '',
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': f'{form_base}:edit-district',
            'javax.faces.partial.event': 'change',
            'javax.faces.partial.execute': f'{form_base}:edit-district @component',
            'javax.faces.partial.render': '@component',
            'javax.faces.behavior.event': 'change',
            'org.richfaces.ajax.component': f'{form_base}:edit-district',
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        headers = self._get_ajax_headers()
        
        try:
            async with session.post(url, data=data, headers=headers) as response:
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")
                
                if response.status in [500, 502, 503, 504]:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                self.logger.debug(f"Регион выбран: {region_id}")
        
        except (aiohttp.ClientError, NonRetriableError):
            raise
        
        except Exception as e:
            raise aiohttp.ClientError(f"Ошибка выбора региона: {e}")
    
    def _extract_viewstate(self, html: str) -> Optional[str]:
        """Извлечение ViewState"""
        return ViewStateExtractor.extract(html)
    
    def _extract_form_ids(self, html: str) -> Dict[str, str]:
        """Извлечение ID элементов формы"""
        parser = HTMLParser(html)
        ids = {}
        
        # Поиск базового ID формы
        form = parser.css_first('form')
        if form and form.attributes and form.attributes.get('id'):
            ids['form_id'] = form.attributes['id']
        
        # Поиск полей формы
        field_mappings = ['edit-district', 'edit-court', 'edit-year', 'edit-num']
        
        for field in field_mappings:
            elements = parser.css(f'[id*="{field}"]')
            for element in elements:
                if element.attributes and element.attributes.get('id'):
                    ids[field] = element.attributes['id']
                    name = element.attributes.get('name', '')
                    if ':' in name:
                        ids['form_base'] = ':'.join(name.split(':')[:-1])
                    break
        
        # Извлечение ID кнопки поиска
        search_button = self._extract_search_button_id(html, ids.get('form_base', ''))
        if search_button:
            ids['search_button'] = search_button
        else:
            self.logger.warning("ID кнопки поиска не найден, будет использован fallback")
        
        return ids
    
    def _extract_search_button_id(self, html: str, form_base: str) -> Optional[str]:
        """
        Извлечение ID кнопки поиска из RichFaces скрипта
        
        Ищет паттерн: goNext = function(...) { RichFaces.ajax("ID", ...)
        """
        import re
        
        pattern = r'goNext\s*=\s*function\s*\([^)]*\)\s*\{\s*RichFaces\.ajax\s*\(\s*["\']([^"\']+)["\']'
        match = re.search(pattern, html)
        
        if match:
            button_id = match.group(1)
            
            # Валидация: ID должен начинаться с form_base
            if form_base and not button_id.startswith(form_base):
                self.logger.warning(
                    f"ID '{button_id}' не соответствует form_base '{form_base}'"
                )
                return None
            
            return button_id
        
        return None
    
    def _get_headers(self) -> Dict[str, str]:
        """Базовые заголовки"""
        return HttpHeaders.get_base()

    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        return HttpHeaders.get_ajax()