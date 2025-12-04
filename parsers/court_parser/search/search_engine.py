"""
Поисковый движок
"""
from typing import Dict
import asyncio
import aiohttp

from utils.logger import get_logger
from utils.retry import NonRetriableError
from utils.http_utils import HttpHeaders


class SearchEngine:
    """Движок для поиска дел"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('search_engine')
    
    async def search_case(
        self, 
        session: aiohttp.ClientSession,
        viewstate: str, 
        region_id: str, 
        court_id: str,
        year: str, 
        sequence_number: int,
        form_ids: Dict[str, str]
    ) -> str:
        """
        Поиск дела по порядковому номеру
        
        Args:
            sequence_number: порядковый номер (1, 2, 3, ...)
        
        Returns:
            HTML с результатами
        """
        await self._send_search_request(
            session, viewstate, region_id, court_id,
            year, sequence_number, form_ids
        )
        
        await asyncio.sleep(0.5)
        
        results_html = await self._get_results(session)
        
        self.logger.debug(f"Поиск выполнен для номера: {sequence_number}")
        return results_html
    
    async def _send_search_request(
        self, 
        session: aiohttp.ClientSession,
        viewstate: str, 
        region_id: str, 
        court_id: str,
        year: str, 
        sequence_number: int,
        form_ids: Dict[str, str]
    ):
        """
        Отправка поискового запроса
        
        В edit-num всегда передаётся только порядковый номер
        """
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('form_base', 'j_idt45:j_idt46')
        
        search_button = form_ids.get('search_button')
        if not search_button:
            search_button = f'{form_base}:j_idt83'
            self.logger.warning(f"Fallback ID кнопки: {search_button}")
        
        # Всегда передаём только порядковый номер
        search_number = str(sequence_number)
        
        data = {
            form_base: form_base,
            f'{form_base}:edit-district': region_id,
            f'{form_base}:edit-district-hide': region_id,
            f'{form_base}:edit-court': court_id,
            f'{form_base}:edit-year': year,
            f'{form_base}:edit-iin': '',
            f'{form_base}:edit-num': search_number,
            f'{form_base}:edit-fio': '',
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': search_button,
            'javax.faces.partial.execute': f'{search_button} @component',
            'javax.faces.partial.render': '@component',
            'param1': f'{form_base}:edit-num',
            'org.richfaces.ajax.component': search_button,
            search_button: search_button,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        self.logger.debug(f"🔍 Поиск: регион={region_id}, суд={court_id}, год={year}, номер={search_number}")
        
        headers = self._get_ajax_headers()
        
        try:
            async with session.post(url, data=data, headers=headers) as response:
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")
                
                if response.status in [500, 502, 503, 504]:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                await response.text()
        
        except (aiohttp.ClientError, NonRetriableError):
            raise
        except Exception as e:
            raise aiohttp.ClientError(f"Ошибка поиска: {e}")
    
    async def _get_results(self, session: aiohttp.ClientSession) -> str:
        """Получение страницы с результатами"""
        url = f"{self.base_url}/lawsuit/lawsuitList.xhtml"
        headers = self._get_headers()
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")
                
                if response.status in [500, 502, 503, 504]:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                return await response.text()
        
        except (aiohttp.ClientError, NonRetriableError):
            raise
        except Exception as e:
            raise aiohttp.ClientError(f"Ошибка получения результатов: {e}")
    
    def _get_headers(self) -> Dict[str, str]:
        """Базовые заголовки"""
        return HttpHeaders.get_base()

    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        return HttpHeaders.get_ajax()