"""
Централизованное логирование с цветным выводом
"""
import logging
import sys
import re
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler


class Colors:
    """ANSI цвета"""
    GREEN = '\033[32m'
    BRIGHT_GREEN = '\033[92m'
    RED = '\033[31m'
    YELLOW = '\033[33m'
    GRAY = '\033[90m'
    WHITE = '\033[97m'
    CYAN = '\033[36m'
    MAGENTA = '\033[35m'
    RESET = '\033[0m'
    
    @classmethod
    def strip(cls, text: str) -> str:
        """Удалить ANSI-коды из текста"""
        ansi_pattern = re.compile(r'\033\[[0-9;]*m')
        return ansi_pattern.sub('', text)


class ColoredFormatter(logging.Formatter):
    """Форматтер с цветами для консоли"""
    
    LEVEL_COLORS = {
        'DEBUG': Colors.GRAY,
        'INFO': Colors.GREEN,
        'WARNING': Colors.YELLOW,
        'ERROR': Colors.RED,
        'CRITICAL': Colors.MAGENTA,
    }
    
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        color = self.LEVEL_COLORS.get(record.levelname, Colors.WHITE)
        
        message = record.getMessage()
        if '\033[' in message:
            return message
        
        return f"{color}[{time_str}] {record.levelname:<7}{Colors.RESET} {message}"


class FileFormatter(logging.Formatter):
    """Форматтер для файла (без цветов)"""
    
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        message = record.getMessage()
        clean_message = Colors.strip(message)
        return f"{time_str} [{record.levelname:<7}] {record.name}: {clean_message}"


class ReportFormatter(logging.Formatter):
    """Форматтер для отчётов"""
    
    def format(self, record):
        message = record.getMessage()
        return Colors.strip(message)


class ProgressAwareHandler(logging.StreamHandler):
    """
    Handler который знает о прогресс-баре
    
    Когда прогресс активен — не выводит в консоль
    """
    
    _progress_active = False
    
    @classmethod
    def set_progress_active(cls, active: bool):
        cls._progress_active = active
    
    @classmethod
    def is_progress_active(cls) -> bool:
        return cls._progress_active
    
    def emit(self, record):
        if self._progress_active:
            return
        super().emit(record)


def setup_logger(
    name: str,
    log_dir: str = "logs",
    level: str = "INFO",
    console_output: bool = True
) -> logging.Logger:
    """
    Настройка логгера с ротацией
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    if logger.handlers:
        logger.handlers.clear()
    
    Path(log_dir).mkdir(exist_ok=True)
    file_handler = RotatingFileHandler(
        f"{log_dir}/parser.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(FileFormatter())
    logger.addHandler(file_handler)
    
    if console_output:
        console = ProgressAwareHandler(sys.stdout)
        console.setLevel(getattr(logging, level))
        console.setFormatter(ColoredFormatter())
        logger.addHandler(console)
    
    return logger


def setup_report_logger(log_dir: str = "logs") -> logging.Logger:
    """
    Настройка логгера для отчётов
    """
    logger = logging.getLogger('report')
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        logger.handlers.clear()
    
    Path(log_dir).mkdir(exist_ok=True)
    
    file_handler = RotatingFileHandler(
        f"{log_dir}/reports.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(ReportFormatter())
    logger.addHandler(file_handler)
    
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Получить логгер"""
    return logging.getLogger(name)


def setup_worker_logger(region_key: str, log_dir: str = "logs") -> logging.Logger:
    """
    Настройка логгера для воркера региона
    """
    return setup_logger(
        name=f'worker_{region_key}',
        log_dir=log_dir,
        level='DEBUG',
        console_output=False
    )


def set_progress_mode(active: bool):
    """
    Включить/выключить режим прогресса
    
    Когда активен — консольные логи подавляются
    """
    ProgressAwareHandler.set_progress_active(active)


def init_logging(log_dir: str = "logs", level: str = "INFO"):
    """Инициализация всех логгеров"""
    setup_logger('main', log_dir, level, console_output=True)
    setup_report_logger(log_dir)
    
    components = [
        'court_parser',
        'authenticator',
        'db_manager',
        'circuit_breaker',
        'retry_strategy',
        'session_manager',
        'results_parser',
        'data_extractor',
        'document_parser',
        'form_handler',
        'search_engine',
        'document_handler',
        'stats_collector',
        'stats_reporter'
    ]
    
    for component in components:
        setup_logger(component, log_dir, 'DEBUG', console_output=False)