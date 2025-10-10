"""
QAMQOR Parser - Общие компоненты.
"""

from .config import Config
from .database import DatabaseManager
from .data_processor import DataProcessor
from .api_validator import APIValidator
from .web_client import WebClient
from .tab_manager import TabManager
from .log_manager import LogManager

__all__ = [
    'Config',
    'DatabaseManager',
    'DataProcessor',
    'APIValidator',
    'WebClient',
    'TabManager',
    'LogManager'
]