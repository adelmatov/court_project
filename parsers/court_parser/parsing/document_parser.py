"""
Парсинг страниц с документами
"""
from typing import Dict, List, Optional, Tuple
import re
from selectolax.parser import HTMLParser

from database.models import DocumentInfo
from utils.text_processor import TextProcessor
from utils.logger import get_logger
from utils.http_utils import ViewStateExtractor


class DocumentParser:
    """Парсер страниц документов"""
    
    def __init__(self):
        self.text_processor = TextProcessor()
        self.logger = get_logger('document_parser')
    
    def extract_case_card_form(self, html: str) -> Optional[Dict[str, str]]:
        """
        Извлечение данных формы для открытия карточки дела из lawsuitList.xhtml
        """
        pattern = r'viewSelectedLawsuit\s*=\s*function\s*\([^)]*\)\s*\{\s*RichFaces\.ajax\s*\(\s*["\']([^"\']+)["\']'
        match = re.search(pattern, html)
        
        if not match:
            self.logger.warning("viewSelectedLawsuit не найден")
            return None
        
        ajax_id = match.group(1)
        form_id = ajax_id.rsplit(':', 1)[0]
        viewstate = self._extract_viewstate(html)
        
        if not viewstate:
            return None
        
        return {
            'form_id': form_id,
            'ajax_id': ajax_id,
            'viewstate': viewstate
        }
    
    def extract_document_list(self, html: str) -> Tuple[List[DocumentInfo], Optional[Dict[str, str]]]:
        """
        Извлечение списка документов из documentList.xhtml
        """
        parser = HTMLParser(html)
        documents = []
        
        rows = parser.css('table tbody tr.hover-effect')
        
        for row in rows:
            cells = row.css('td')
            if len(cells) < 3:
                continue
            
            # Дата
            date_text = self.text_processor.clean(cells[0].text())
            doc_date = self.text_processor.parse_date(date_text)
            if not doc_date:
                continue
            
            # Тип
            doc_type = self.text_processor.clean(cells[1].text())
            
            # Имя и индекс
            link = cells[2].css_first('a')
            if not link:
                continue
            
            doc_name = self.text_processor.clean(link.text())
            onclick = link.attributes.get('onclick', '')
            index_match = re.search(r'viewInlineDoc\s*\(\s*(\d+)\s*\)', onclick)
            
            if not index_match:
                continue
            
            documents.append(DocumentInfo(
                index=int(index_match.group(1)),
                doc_date=doc_date.date(),
                doc_name=doc_name,
                doc_type=doc_type
            ))
        
        form_data = self._extract_document_form(html)
        return documents, form_data
    
    def _extract_document_form(self, html: str) -> Optional[Dict[str, str]]:
        """Извлечение данных формы для открытия документа"""
        pattern = r'viewInlineDoc\s*=\s*function\s*\([^)]*\)\s*\{\s*RichFaces\.ajax\s*\(\s*["\']([^"\']+)["\']'
        match = re.search(pattern, html)
        
        if not match:
            return None
        
        ajax_id = match.group(1)
        form_id = ajax_id.rsplit(':', 1)[0]
        viewstate = self._extract_viewstate(html)
        
        return {
            'form_id': form_id,
            'ajax_id': ajax_id,
            'viewstate': viewstate
        }
    
    def extract_pdf_url(self, html: str) -> Optional[str]:
        """Извлечение URL документа из document.xhtml"""
        parser = HTMLParser(html)
        
        embed = parser.css_first('embed[type="application/pdf"]')
        if embed and embed.attributes:
            return embed.attributes.get('src')
        
        embed = parser.css_first('embed[src*=".pdf"]')
        if embed and embed.attributes:
            return embed.attributes.get('src')
        
        return None
    
    def _extract_viewstate(self, html: str) -> Optional[str]:
        return ViewStateExtractor.extract(html)