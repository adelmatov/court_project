"""
Логирование с ротацией по дате
"""
import logging
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional


def _get_ui():
    try:
        from utils.terminal_ui import get_ui
        return get_ui()
    except ImportError:
        return None


class Colors:
    """ANSI цвета"""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    GREEN = '\033[32m'
    BRIGHT_GREEN = '\033[92m'
    YELLOW = '\033[33m'
    RED = '\033[31m'
    CYAN = '\033[36m'
    GRAY = '\033[90m'
    WHITE = '\033[97m'
    
    @classmethod
    def strip(cls, text: str) -> str:
        import re
        return re.compile(r'\033\[[0-9;]*m').sub('', text)


class FileFormatter(logging.Formatter):
    """Форматтер для файла (без цветов)"""
    
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        message = record.getMessage()
        clean_message = Colors.strip(message)
        
        # Добавляем дополнительную информацию если есть
        extra_parts = []
        if hasattr(record, 'region') and record.region:
            extra_parts.append(f"region={record.region}")
        if hasattr(record, 'court') and record.court:
            extra_parts.append(f"court={record.court}")
        if hasattr(record, 'case_number') and record.case_number:
            extra_parts.append(f"case={record.case_number}")
        
        extra_str = f" [{', '.join(extra_parts)}]" if extra_parts else ""
        
        return f"{time_str} [{record.levelname:<7}] {record.name}{extra_str}: {clean_message}"


class ColoredConsoleFormatter(logging.Formatter):
    """Форматтер с цветами для консоли"""
    
    LEVEL_COLORS = {
        'DEBUG': Colors.GRAY,
        'INFO': Colors.GREEN,
        'WARNING': Colors.YELLOW,
        'ERROR': Colors.RED,
        'CRITICAL': Colors.RED + Colors.BOLD,
    }
    
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime('%H :%M:%S')
        color = self.LEVEL_COLORS.get(record.levelname, Colors.WHITE)
        return f"{Colors.DIM}[{time_str}]{Colors.RESET} {color}{record.levelname:<7}{Colors.RESET} {record.getMessage()}"


def cleanup_old_logs(log_dir: str, days: int = 3):
    """Удалить лог-файлы старше указанного количества дней"""
    log_path = Path(log_dir)
    if not log_path.exists():
        return
    
    cutoff_time = datetime.now() - timedelta(days=days)
    
    for log_file in log_path.glob("*.log"):
        try:
            file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_mtime < cutoff_time:
                log_file.unlink()
                print(f"Deleted old log: {log_file.name}")
        except Exception as e:
            print(f"Error deleting {log_file}: {e}")


def get_log_filename(prefix: str = "parser") -> str:
    """Получить имя лог-файла с датой и временем"""
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return f"{prefix}_{timestamp}.log"


_current_log_file: Optional[str] = None


def setup_logger(
    name: str,
    log_dir: str = "logs",
    level: str = "INFO",
    console_output: bool = True
) -> logging.Logger:
    """Настройка логгера"""
    global _current_log_file
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    # Создаём директорию логов
    Path(log_dir).mkdir(exist_ok=True)
    
    # Очищаем старые логи
    cleanup_old_logs(log_dir, days=3)
    
    # Создаём имя файла при первом вызове
    if _current_log_file is None:
        _current_log_file = get_log_filename("parser")
    
    log_file_path = Path(log_dir) / _current_log_file
    
    # Файловый handler
    file_handler = logging.FileHandler(
        log_file_path,
        encoding='utf-8',
        mode='a'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(FileFormatter())
    logger.addHandler(file_handler)
    
    # Консольный handler (для fallback когда UI неактивен)
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level))
        console_handler.setFormatter(ColoredConsoleFormatter())
        
        # Добавляем фильтр чтобы не дублировать вывод когда UI активен
        class UIFilter(logging.Filter):
            def filter(self, record):
                ui = _get_ui()
                # Пропускаем в консоль только если UI неактивен
                return ui is None or not ui._running
        
        console_handler.addFilter(UIFilter())
        logger.addHandler(console_handler)
    
    return logger


def setup_worker_logger(region_key: str, log_dir: str = "logs") -> logging.Logger:
    """Настройка логгера для воркера региона"""
    global _current_log_file
    
    logger_name = f'worker_{region_key}'
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    Path(log_dir).mkdir(exist_ok=True)
    
    if _current_log_file is None:
        _current_log_file = get_log_filename("parser")
    
    log_file_path = Path(log_dir) / _current_log_file
    
    file_handler = logging.FileHandler(
        log_file_path,
        encoding='utf-8',
        mode='a'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(FileFormatter())
    logger.addHandler(file_handler)
    
    return logger


def setup_report_logger(log_dir: str = "logs") -> logging.Logger:
    """Настройка логгера для отчётов"""
    logger = logging.getLogger('report')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    Path(log_dir).mkdir(exist_ok=True)
    
    report_file = Path(log_dir) / get_log_filename("reports")
    
    file_handler = logging.FileHandler(
        report_file,
        encoding='utf-8',
        mode='a'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(FileFormatter())
    logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Получить логгер"""
    return logging.getLogger(name)


def init_logging(log_dir: str = "logs", level: str = "INFO"):
    """Инициализация всех логгеров"""
    global _current_log_file
    _current_log_file = None  # Сброс для нового файла
    
    setup_logger('main', log_dir, level, console_output=True)
    setup_report_logger(log_dir)
    
    components = [
        'court_parser', 'authenticator', 'db_manager',
        'form_handler', 'search_engine', 'results_parser',
        'document_handler', 'region_worker', 'circuit_breaker',
        'retry_strategy', 'session_manager', 'data_extractor',
        'document_parser'
    ]
    
    for component in components:
        setup_logger(component, log_dir, 'DEBUG', console_output=False)
    
    # Логируем начало сессии
    main_logger = get_logger('main')
    main_logger.info("=" * 60)
    main_logger.info(f"New parsing session started at {datetime.now().isoformat()}")
    main_logger.info("=" * 60)


def set_progress_mode(active: bool):
    """Устаревшая функция — для совместимости"""
    pass