"""
Простое логирование с цветным выводом
"""
import logging
import sys
from datetime import datetime
from pathlib import Path


# Цвета для консоли
COLORS = {
    'DEBUG': '\033[36m',
    'INFO': '\033[32m',
    'WARNING': '\033[33m',
    'ERROR': '\033[31m',
    'CRITICAL': '\033[35m',
    'RESET': '\033[0m'
}


class ColoredFormatter(logging.Formatter):
    """Форматтер с цветами"""
    
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        color = COLORS.get(record.levelname, '')
        reset = COLORS['RESET']
        
        return f"{color}[{time_str}] {record.levelname}{reset}: {record.getMessage()}"


def setup_logger(name: str, log_dir: str = "logs", level: str = "INFO") -> logging.Logger:
    """Настройка логгера"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    # Консольный вывод
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(getattr(logging, level))
    console.setFormatter(ColoredFormatter())
    logger.addHandler(console)
    
    # Файловый вывод
    Path(log_dir).mkdir(exist_ok=True)
    file_handler = logging.FileHandler(
        f"{log_dir}/parser.log",
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    ))
    logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Получить логгер"""
    return logging.getLogger(name)