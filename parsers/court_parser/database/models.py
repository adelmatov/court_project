"""
Структуры данных для БД
"""
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Optional


@dataclass
class CaseData:
    """Данные дела"""
    case_number: str
    case_date: Optional[date] = None
    judge: Optional[str] = None
    plaintiffs: List[str] = field(default_factory=list)
    defendants: List[str] = field(default_factory=list)
    events: List['EventData'] = field(default_factory=list)
    result_index: Optional[int] = None  # Индекс в результатах поиска (не сохраняется в БД)
    
    def to_dict(self) -> dict:
        """Преобразование в словарь (без result_index — он не для БД)"""
        return {
            'case_number': self.case_number,
            'case_date': self.case_date,
            'judge': self.judge,
            'plaintiffs': self.plaintiffs,
            'defendants': self.defendants,
            'events': [e.to_dict() for e in self.events]
        }


@dataclass
class EventData:
    """Данные события"""
    event_type: str
    event_date: date
    
    def to_dict(self) -> dict:
        return {
            'event_type': self.event_type,
            'event_date': self.event_date
        }


@dataclass
class SearchResult:
    """Результат поиска"""
    found: bool
    case_data: Optional[CaseData] = None
    error: Optional[str] = None


@dataclass
class DocumentInfo:
    """Информация о документе"""
    index: int  # Индекс из onclick (уже правильно реализовано)
    doc_date: date
    doc_name: str
    doc_type: str = ""
    
    @property
    def unique_key(self) -> str:
        return f"{self.doc_date.isoformat()}|{self.doc_name}"
    
    def to_dict(self) -> dict:
        return {
            'index': self.index,
            'doc_date': self.doc_date,
            'doc_name': self.doc_name,
            'doc_type': self.doc_type
        }