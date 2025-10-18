"""
Работа с поисковой формой
"""
import asyncio
import aiohttp
from typing import Dict, Optional
from selectolax.parser import HTMLParser

from utils.logger import get_logger


class FormHandler:
    """Обработчик поисковой формы"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('form_handler')
    
    async def prepare_search_form(self, session: aiohttp.ClientSession) -> tuple:
        """
        Подготовка формы поиска
        
        Возвращает: (viewstate, form_ids)
        """
        url = f"{self.base_url}/form/lawsuit/"
        headers = self._get_headers()
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} при загрузке формы поиска")
            
            html = await response.text()
            viewstate = self._extract_viewstate(html)
            form_ids = self._extract_form_ids(html)
            
            self.logger.debug("Форма поиска подготовлена")
            return viewstate, form_ids
    
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
        
        async with session.post(url, data=data, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} при выборе региона")
            
            self.logger.debug(f"Регион выбран: {region_id}")
    
    def _extract_viewstate(self, html: str) -> Optional[str]:
        """Извлечение ViewState"""
        parser = HTMLParser(html)
        viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')
        
        if viewstate_input:
            return viewstate_input.attributes.get('value')
        return None
    
    def _extract_form_ids(self, html: str) -> Dict[str, str]:
        """Извлечение ID элементов формы"""
        parser = HTMLParser(html)
        ids = {}
        
        # Поиск базового ID формы
        form = parser.css_first('form')
        if form and form.attributes.get('id'):
            ids['form_id'] = form.attributes['id']
        
        # Поиск полей формы
        field_mappings = ['edit-district', 'edit-court', 'edit-year', 'edit-num']
        
        for field in field_mappings:
            elements = parser.css(f'[id*="{field}"]')
            for element in elements:
                if element.attributes.get('id'):
                    ids[field] = element.attributes['id']
                    name = element.attributes.get('name', '')
                    if ':' in name:
                        ids['form_base'] = ':'.join(name.split(':')[:-1])
                    break
        
        return ids
    
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