# parsers/court_parser/utils/http_utils.py
"""
Общие HTTP утилиты
"""

from typing import Dict, Optional
from selectolax.parser import HTMLParser


class HttpHeaders:
    """Фабрика HTTP заголовков"""
    
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    
    @staticmethod
    def get_base() -> Dict[str, str]:
        """Базовые HTTP заголовки"""
        return {
            'User-Agent': HttpHeaders.USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        }
    
    @staticmethod
    def get_ajax() -> Dict[str, str]:
        """AJAX заголовки для RichFaces"""
        headers = HttpHeaders.get_base()
        headers.update({
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Faces-Request': 'partial/ajax',
            'X-Requested-With': 'XMLHttpRequest',  # ✅ Исправлено!
        })
        return headers


class ViewStateExtractor:
    """Извлечение ViewState из HTML"""
    
    @staticmethod
    def extract(html: str) -> Optional[str]:
        """Извлечение ViewState из HTML"""
        parser = HTMLParser(html)
        viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')
        
        if viewstate_input and viewstate_input.attributes:
            return viewstate_input.attributes.get('value')
        return None


class AjaxRequestBuilder:
    """Построитель данных для AJAX запросов RichFaces"""
    
    @staticmethod
    def build(
        form_id: str,
        ajax_id: str,
        viewstate: str,
        extra_params: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """
        Построить данные для partial/ajax запроса
        """
        data = {
            form_id: form_id,
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': ajax_id,
            'javax.faces.partial.execute': f'{ajax_id} @component',
            'javax.faces.partial.render': '@component',
            'org.richfaces.ajax.component': ajax_id,
            ajax_id: ajax_id,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        if extra_params:
            data.update(extra_params)
        
        return data