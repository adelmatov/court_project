"""
Извлечение данных из HTML элементов
"""
from typing import List, Tuple, Optional
from datetime import date

from database.models import EventData
from utils.text_processor import TextProcessor
from utils.logger import get_logger


class DataExtractor:
    """Извлечение данных из HTML элементов"""
    
    def __init__(self):
        self.text_processor = TextProcessor()
        self.logger = get_logger('data_extractor')
    
    def extract_case_info(self, cell) -> Tuple[str, Optional[date]]:
        """
        Извлечение номера дела и даты
        
        Возвращает: (case_number, case_date)
        """
        paragraphs = cell.css('p')
        case_number = ""
        case_date = None
        
        if paragraphs:
            # Первый параграф - номер дела
            case_number = self.text_processor.clean(paragraphs[0].text())
            
            # Второй параграф - дата (если есть)
            if len(paragraphs) > 1:
                date_str = self.text_processor.clean(paragraphs[1].text())
                parsed_date = self.text_processor.parse_date(date_str)
                if parsed_date:
                    case_date = parsed_date.date()
        
        return case_number, case_date
    
    def extract_parties(self, cell) -> Tuple[List[str], List[str]]:
        """
        Извлечение сторон дела
        
        Возвращает: (plaintiffs, defendants)
        """
        paragraphs = cell.css('p')
        plaintiffs = []
        defendants = []
        
        if len(paragraphs) >= 2:
            # Первый параграф - истцы
            plaintiffs_text = self.text_processor.clean(paragraphs[0].text())
            if plaintiffs_text:
                plaintiffs = self.text_processor.split_parties(plaintiffs_text)
            
            # Второй параграф - ответчики
            defendants_text = self.text_processor.clean(paragraphs[1].text())
            if defendants_text:
                defendants = self.text_processor.split_parties(defendants_text)
        
        return plaintiffs, defendants
    
    def extract_judge(self, cell) -> Optional[str]:
        """Извлечение имени судьи"""
        judge_text = self.text_processor.clean(cell.text())
        return judge_text if judge_text else None
    
    def extract_events(self, cell) -> List[EventData]:
        """Извлечение событий дела"""
        paragraphs = cell.css('p')
        events = []
        
        for paragraph in paragraphs:
            text = self.text_processor.clean(paragraph.text())
            
            # Формат: "15.01.2025 - Дело принято к производству"
            if ' - ' in text:
                try:
                    date_part, event_part = text.split(' - ', 1)
                    
                    parsed_date = self.text_processor.parse_date(date_part)
                    event_type = self.text_processor.clean(event_part)
                    
                    if parsed_date and event_type:
                        events.append(EventData(
                            event_type=event_type,
                            event_date=parsed_date.date()
                        ))
                except ValueError:
                    continue
        
        return events