"""
Простое логирование с цветным выводом
"""
import logging
import sys
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler


COLORS = {
    'DEBUG': '\033[36m',
    'INFO': '\033[32m',
    'WARNING': '\033[33m',
    'ERROR': '\033[31m',
    'CRITICAL': '\033[35m',
    'RESET': '\033[0m'
}


class ColoredFormatter(logging.Formatter):
    """Форматтер с цветами для консоли"""
    
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        color = COLORS.get(record.levelname, '')
        reset = COLORS['RESET']
        return f"{color}[{time_str}] {record.levelname}{reset}: {record.getMessage()}"


class FileFormatter(logging.Formatter):
    """Форматтер для файла"""
    
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        return f"{time_str} [{record.levelname}] {record.name}: {record.getMessage()}"

def setup_logger(
    name: str, 
    log_dir: str = "logs", 
    level: str = "INFO",
    console_output: bool = True
) -> logging.Logger:
    """
    Настройка логгера с ротацией
    
    Args:
        name: имя логгера
        log_dir: директория для логов
        level: уровень логирования
        console_output: выводить ли в консоль
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
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(getattr(logging, level))
        console.setFormatter(ColoredFormatter())
        logger.addHandler(console)
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """Получить логгер"""
    return logging.getLogger(name)

def setup_worker_logger(region_key: str, log_dir: str = "logs") -> logging.Logger:
    """
    Настройка логгера для воркера региона
    
    Только файловый вывод, без консоли
    """
    return setup_logger(
        name=f'worker_{region_key}',
        log_dir=log_dir,
        level='DEBUG',
        console_output=False
    )

def init_logging(log_dir: str = "logs", level: str = "INFO"):
    setup_logger('main', log_dir, level, console_output=False)
    
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