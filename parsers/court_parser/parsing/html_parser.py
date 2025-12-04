"""
Парсинг HTML результатов
"""
import re
from typing import List, Optional
from selectolax.parser import HTMLParser

from database.models import CaseData
from parsing.data_extractor import DataExtractor
from utils.logger import get_logger


class ResultsParser:
    """Парсер результатов поиска"""
    
    NO_RESULTS_MESSAGES = [
        "По указанным данным ничего не найдено",
        "Көрсетілген деректер бойына ешнәрсе табылмады"
    ]
    
    def __init__(self):
        self.extractor = DataExtractor()
        self.logger = get_logger('results_parser')
    
    def parse(self, html: str) -> List[CaseData]:
        """
        Парсинг HTML с результатами
        
        Возвращает: список найденных дел с result_index
        """
        parser = HTMLParser(html)
        
        # Проверка на отсутствие результатов
        if self._is_no_results(parser):
            self.logger.debug("Результаты не найдены")
            return []
        
        # Поиск таблицы с результатами
        table = parser.css_first('table')
        if not table:
            self.logger.warning("Таблица результатов не найдена в HTML")
            return []
        
        # Парсинг строк таблицы
        results = self._parse_table(table)
        
        self.logger.debug(f"Распарсено дел: {len(results)}")
        return results
    
    def _is_no_results(self, parser: HTMLParser) -> bool:
        """Проверка сообщения об отсутствии результатов"""
        content = parser.css_first('.tab__inner-content')
        if not content:
            return True
        
        text = content.text()
        return any(msg in text for msg in self.NO_RESULTS_MESSAGES)
    
    def _parse_table(self, table) -> List[CaseData]:
        """Парсинг таблицы с делами"""
        rows = table.css('tbody tr')
        results = []
        
        for row in rows:
            try:
                case_data = self._parse_row(row)
                if case_data:
                    results.append(case_data)
            except Exception as e:
                self.logger.error(f"Ошибка парсинга строки: {e}")
                continue
        
        return results
    
    def _parse_row(self, row) -> Optional[CaseData]:
        """Парсинг одной строки таблицы с извлечением result_index"""
        cells = row.css('td')
        
        if len(cells) < 4:
            return None
        
        # Извлечение result_index из onclick атрибута строки
        result_index = self._extract_result_index(row)
        
        # Ячейка 1: Номер дела и дата
        case_number, case_date = self.extractor.extract_case_info(cells[0])
        if not case_number:
            return None
        
        # Ячейка 2: Стороны
        plaintiffs, defendants = self.extractor.extract_parties(cells[1])
        
        # Ячейка 3: Судья
        judge = self.extractor.extract_judge(cells[2])
        
        # Ячейка 4: История (события)
        events = self.extractor.extract_events(cells[3])
        
        return CaseData(
            case_number=case_number,
            case_date=case_date,
            judge=judge,
            plaintiffs=plaintiffs,
            defendants=defendants,
            events=events,
            result_index=result_index
        )
    
    def _extract_result_index(self, row) -> Optional[int]:
        """
        Извлечение индекса из onclick атрибута строки
        
        Пример: onclick="viewSelectedLawsuit(1);" → 1
        """
        if not row.attributes:
            return None
        
        onclick = row.attributes.get('onclick', '')
        
        # Паттерн: viewSelectedLawsuit(N)
        match = re.search(r'viewSelectedLawsuit\s*\(\s*(\d+)\s*\)', onclick)
        
        if match:
            return int(match.group(1))
        
        return None
    
    def find_case_index(self, cases: List[CaseData], target_case_number: str) -> Optional[int]:
        """
        Найти result_index для конкретного номера дела
        
        Args:
            cases: список распарсенных дел
            target_case_number: искомый номер дела
        
        Returns:
            result_index или None если не найдено
        """
        for case in cases:
            if case.case_number == target_case_number:
                return case.result_index
        
        return None