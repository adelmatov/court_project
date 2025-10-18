"""
Поисковый движок
"""
import asyncio
import aiohttp
from typing import Dict, Optional

from utils.logger import get_logger


class SearchEngine:
    """Движок для поиска дел"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('search_engine')
    
    async def search_case(self, session: aiohttp.ClientSession,
                 viewstate: str, region_id: str, court_id: str,
                 year: str, full_case_number: str,
                 form_ids: Dict[str, str],
                 extract_sequence: bool = False) -> str:  # ← ДОБАВЛЕН параметр
        """
        Поиск дела
        
        Args:
            full_case_number: полный номер дела (например, "6294-25-00-4/215")
            extract_sequence: 
                False (default) - передать полный номер в FormData (Full Scan Mode)
                True - передать только порядковый номер в FormData (Update Mode)
        
        Returns:
            HTML с результатами
        """
        # Отправка поискового запроса
        await self._send_search_request(
            session, viewstate, region_id, court_id,
            year, full_case_number, form_ids, extract_sequence  # ← ДОБАВЛЕН параметр
        )
        
        # Небольшая задержка для обработки на сервере
        await asyncio.sleep(0.5)
        
        # Получение результатов
        results_html = await self._get_results(session)
        
        self.logger.debug(f"Поиск выполнен для номера: {full_case_number}")
        return results_html
    
    async def _send_search_request(self, session: aiohttp.ClientSession,
                          viewstate: str, region_id: str, court_id: str,
                          year: str, full_case_number: str,
                          form_ids: Dict[str, str],
                          extract_sequence: bool = False):  # ← ДОБАВЛЕН параметр
        """
        Отправка поискового запроса
        
        Args:
            extract_sequence: 
                False - передать полный номер в FormData (Full Scan Mode)
                True - передать только порядковый номер в FormData (Update Mode)
        """
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('form_base', 'j_idt45:j_idt46')
        search_button = f'{form_base}:j_idt83'
        
        # КЛЮЧЕВАЯ ЛОГИКА: Выбор что передавать в FormData
        if extract_sequence:
            # Update Mode: только порядковый номер
            if '/' in full_case_number:
                search_number = full_case_number.split('/')[-1]  # "215"
            else:
                search_number = full_case_number
            self.logger.debug(f"Update Mode: извлечён порядковый {search_number} из {full_case_number}")
        else:
            # Full Scan Mode: полный номер
            search_number = full_case_number  # "6294-25-00-4/1"
            self.logger.debug(f"Full Scan Mode: используется полный номер {search_number}")
        
        data = {
            form_base: form_base,
            f'{form_base}:edit-district': region_id,
            f'{form_base}:edit-district-hide': region_id,
            f'{form_base}:edit-court': court_id,
            f'{form_base}:edit-year': year,
            f'{form_base}:edit-iin': '',
            f'{form_base}:edit-num': search_number,  # ← Зависит от режима!
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
        
        # Логирование для отладки
        self.logger.debug(f"🔍 Поиск дела:")
        self.logger.debug(f"   Регион ID: {region_id}")
        self.logger.debug(f"   Суд ID: {court_id}")
        self.logger.debug(f"   Год: {year}")
        self.logger.debug(f"   Полный номер: {full_case_number}")
        self.logger.debug(f"   В FormData: {search_number}")
        
        headers = self._get_ajax_headers()
        
        async with session.post(url, data=data, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} при отправке поиска")
            
            await response.text()
    
    async def _get_results(self, session: aiohttp.ClientSession) -> str:
        """Получение страницы с результатами"""
        url = f"{self.base_url}/lawsuit/lawsuitList.xhtml"
        headers = self._get_headers()
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} при получении результатов")
            
            return await response.text()
    
    def _get_headers(self) -> Dict[str, str]:
        """Базовые заголовки"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru,en;q=0.9'
        }
    
    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        headers = self._get_headers()
        headers.update({
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Faces-Request': 'partial/ajax'
        })
        return headers