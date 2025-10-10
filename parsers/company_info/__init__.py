"""
Company Info Parser Package
Парсер данных о компаниях из ba.prg.kz API
"""

__version__ = '1.0.0'

# Экспортируем основные классы для удобного импорта
from .main import CompanyParser
from .sources.registry import registry

__all__ = ['CompanyParser', 'registry']