# parsers/court_parser/search/document_handler.py
"""
Обработка документов судебных дел
"""
from typing import Dict, List, Optional, Set
from pathlib import Path
import asyncio
import re
import aiohttp

from parsing.document_parser import DocumentParser
from database.models import DocumentInfo
from utils.text_processor import TextProcessor
from utils.logger import get_logger
from utils.http_utils import HttpHeaders, AjaxRequestBuilder


class DocumentHandler:
    """Обработчик загрузки документов"""
    
    def __init__(self, base_url: str, storage_dir: str = "./court_documents", regions_config: Dict = None):
        self.base_url = base_url
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.parser = DocumentParser()
        self.text_processor = TextProcessor()
        self.regions_config = regions_config or {}
        self.logger = get_logger('document_handler')
    
    def _get_case_folder(self, case_number: str) -> Path:
        """
        Определить папку для дела по номеру
        
        Вход: "7594-25-00-4/5229"
        Выход: documents/almaty/smas/2025/7594-25-00-4_5229/
        """
        # Парсим номер дела
        case_info = self.text_processor.find_region_and_court_by_case_number(
            case_number, self.regions_config
        )
        
        if case_info:
            region_key = case_info['region_key']
            court_key = case_info['court_key']
            year = case_info['year']
        else:
            # Fallback: извлекаем год из номера дела
            # "7594-25-00-4/5229" → год = "2025"
            match = re.match(r'\d+-(\d{2})-', case_number)
            if match:
                year_short = match.group(1)
                year = f"20{year_short}"
            else:
                year = "unknown"
            
            region_key = "unknown"
            court_key = "unknown"
        
        # Безопасное имя папки для дела
        safe_case = case_number.replace('/', '_')
        
        # Формируем путь: documents/region/court/year/case_number/
        folder = self.storage_dir / region_key / court_key / year / safe_case
        folder.mkdir(parents=True, exist_ok=True)
        
        return folder
    
    def _save_file(self, case_number: str, doc_info: DocumentInfo, content: bytes) -> str:
        """Сохранить файл на диск с новой структурой папок"""
        # Получаем папку для дела
        case_dir = self._get_case_folder(case_number)
        
        # Формируем имя файла
        date_prefix = doc_info.doc_date.strftime('%Y-%m-%d')
        safe_name = self._sanitize_filename(doc_info.doc_name)
        filename = f"{date_prefix}_{safe_name}"
        
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
        
        file_path = case_dir / filename
        
        with open(file_path, 'wb') as f:
            f.write(content)
        
        # Возвращаем относительный путь от storage_dir
        relative_path = file_path.relative_to(self.storage_dir)
        
        self.logger.info(f"Сохранён: {relative_path}")
        return str(relative_path)
    
    def _sanitize_filename(self, filename: str) -> str:
        """Очистка имени файла"""
        if filename.lower().endswith('.pdf'):
            filename = filename[:-4]
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'\s+', '_', filename)
        filename = re.sub(r'_+', '_', filename).strip('_')
        return filename[:100] if len(filename) > 100 else filename
    
    async def open_case_card(self, session: aiohttp.ClientSession,
                             results_html: str, case_index: int = 0) -> bool:
        """Открыть карточку дела из списка результатов"""
        form_data = self.parser.extract_case_card_form(results_html)
        if not form_data:
            return False
        
        url = f"{self.base_url}/lawsuit/lawsuitList.xhtml"
        
        data = AjaxRequestBuilder.build(
            form_id=form_data['form_id'],
            ajax_id=form_data['ajax_id'],
            viewstate=form_data['viewstate'],
            extra_params={'param1': str(case_index)}
        )
        
        headers = HttpHeaders.get_ajax()
        headers['Referer'] = url
        
        async with session.post(url, data=data, headers=headers) as response:
            return response.status == 200
    
    async def get_document_list(self, session: aiohttp.ClientSession):
        """Получить список документов"""
        url = f"{self.base_url}/lawsuit/documentList.xhtml"
        headers = HttpHeaders.get_base()
        headers['Referer'] = f"{self.base_url}/lawsuit/lawsuitList.xhtml"
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                return [], None
            html = await response.text()
            return self.parser.extract_document_list(html)
    
    async def open_document(self, session: aiohttp.ClientSession,
                            form_data: Dict[str, str], doc_index: int) -> bool:
        """Открыть документ по индексу"""
        url = f"{self.base_url}/lawsuit/documentList.xhtml"
        
        data = AjaxRequestBuilder.build(
            form_id=form_data['form_id'],
            ajax_id=form_data['ajax_id'],
            viewstate=form_data['viewstate'],
            extra_params={'param1': str(doc_index)}
        )
        
        headers = HttpHeaders.get_ajax()
        async with session.post(url, data=data, headers=headers) as response:
            return response.status == 200
    
    async def get_document_page(self, session: aiohttp.ClientSession) -> Optional[str]:
        """Получить страницу с PDF viewer"""
        url = f"{self.base_url}/lawsuit/document.xhtml"
        headers = HttpHeaders.get_base()
        headers['Referer'] = f"{self.base_url}/lawsuit/documentList.xhtml"
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                return None
            return await response.text()
    
    async def download_pdf(self, session: aiohttp.ClientSession, pdf_url: str,
                           case_number: str, doc_info: DocumentInfo) -> Optional[str]:
        """Скачать PDF файл"""
        full_url = f"{self.base_url}{pdf_url}" if pdf_url.startswith('/') else pdf_url
        headers = HttpHeaders.get_base()
        headers['Referer'] = f"{self.base_url}/lawsuit/document.xhtml"
        
        async with session.get(full_url, headers=headers) as response:
            if response.status != 200:
                return None
            content = await response.read()
            return self._save_file(case_number, doc_info, content)
    
    async def fetch_all_documents(
        self, 
        session: aiohttp.ClientSession,
        results_html: str, 
        case_number: str,
        case_index: int = 0,
        existing_keys: Optional[Set[str]] = None,
        delay: float = 1.0
    ) -> List[Dict]:
        """Скачать все новые документы для дела"""
        existing_keys = existing_keys or set()
        downloaded = []
        
        if not await self.open_case_card(session, results_html, case_index):
            return downloaded
        
        await asyncio.sleep(delay)
        
        documents, form_data = await self.get_document_list(session)
        if not documents or not form_data:
            return downloaded
        
        new_docs = [d for d in documents if d.unique_key not in existing_keys]
        if not new_docs:
            self.logger.info(f"Новых документов нет для {case_number}")
            return downloaded
        
        self.logger.info(f"Новых документов: {len(new_docs)}")
        
        for doc in new_docs:
            try:
                if not await self.open_document(session, form_data, doc.index):
                    continue
                await asyncio.sleep(delay)
                
                doc_html = await self.get_document_page(session)
                if not doc_html:
                    continue
                
                pdf_url = self.parser.extract_pdf_url(doc_html)
                if not pdf_url:
                    continue
                
                file_path = await self.download_pdf(session, pdf_url, case_number, doc)
                if file_path:
                    downloaded.append({
                        'doc_date': doc.doc_date,
                        'doc_name': doc.doc_name,
                        'file_path': file_path
                    })
                
                await asyncio.sleep(delay)
            except Exception as e:
                self.logger.error(f"Ошибка скачивания {doc.doc_name}: {e}")
        
        return downloaded
    
    def _get_headers(self) -> Dict[str, str]:
        return HttpHeaders.get_base()

    def _get_ajax_headers(self) -> Dict[str, str]:
        return HttpHeaders.get_ajax()