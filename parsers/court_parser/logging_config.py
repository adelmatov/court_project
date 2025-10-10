import logging
import logging.handlers
import queue
import threading
import sys
from datetime import datetime
from pathlib import Path

# Простые цвета для консоли
COLORS = {
    'DEBUG': '\033[36m',    # Голубой
    'INFO': '\033[32m',     # Зеленый  
    'WARNING': '\033[33m',  # Желтый
    'ERROR': '\033[31m',    # Красный
    'CRITICAL': '\033[35m', # Фиолетовый
    'SUCCESS': '\033[92m',  # Яркий зеленый для успешных операций
    'RESET': '\033[0m'      # Сброс
}

class SimpleFormatter(logging.Formatter):
    """Простой форматтер с flush для немедленного вывода"""
    
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        
        # Определяем цвет на основе сообщения
        message = record.getMessage()
        if any(word in message.upper() for word in ['НАЙДЕН', 'СОХРАНЕНО', 'УСПЕХ', 'СУДЬЯ НАЙДЕН']):
            color = COLORS['SUCCESS']
        else:
            color = COLORS.get(record.levelname, '')
            
        reset = COLORS['RESET']
        
        return f"{color}[{time_str}]{reset} {message}"

class FlushingStreamHandler(logging.StreamHandler):
    """StreamHandler с принудительным flush"""
    
    def emit(self, record):
        try:
            super().emit(record)
            self.flush()  # Принудительно сбрасываем буфер
        except Exception:
            self.handleError(record)

def setup_logging(log_dir="logs", console_level="INFO", file_level="DEBUG"):
    """Простая настройка логирования с принудительным flush"""
    
    # Создаем папку логов
    Path(log_dir).mkdir(exist_ok=True)
    
    # Настраиваем корневой логгер
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    # Консольный вывод с flush
    console = FlushingStreamHandler(sys.stdout)
    console.setLevel(getattr(logging, console_level))
    console.setFormatter(SimpleFormatter())
    logger.addHandler(console)
    
    # Файловый вывод с ротацией
    file_handler = logging.handlers.RotatingFileHandler(
        f"{log_dir}/parser.log", 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, file_level))
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    ))
    logger.addHandler(file_handler)

def get_logger(name):
    """Получить логгер"""
    return logging.getLogger(name)

# Обновленные функции логирования с зеленым цветом
def log_case_found(logger, case_number, judge_exists, parties_count, events_count):
    judge_text = "судья есть" if judge_exists else "судья отсутствует"
    # Используем зеленый цвет для найденных дел
    message = f"НАЙДЕН: {judge_text}, {parties_count} стороны, {events_count} события"
    logger.info(message)

def log_case_saved(logger, case_number, no_judge=False):
    suffix = " (без судьи)" if no_judge else ""
    # Используем зеленый цвет для сохраненных дел
    message = f"СОХРАНЕНО: дело {case_number}{suffix}"
    logger.info(message)

# Остальные функции без изменений...
def log_search_start(logger, case_number, region_name="", court_name=""):
    court_info = f" ({region_name}, {court_name})" if region_name else ""
    logger.info(f"Ищу дело: {case_number}{court_info}")

def log_case_not_found(logger, case_number):
    logger.info(f"Дело не найдено")

def log_missing_search_start(logger, region_name, court_name, year):
    logger.info(f"=== ПОИСК ПРОПУЩЕННЫХ НОМЕРОВ ===")
    logger.info(f"Переход к поиску пропущенных номеров ({region_name}, {court_name}, {year})")

def log_missing_found(logger, count, ranges_text=""):
    ranges_info = f" (диапазоны: {ranges_text})" if ranges_text else ""
    logger.info(f"Найдено пропусков: {count} номеров{ranges_info}")

def log_update_start(logger, update_type):
    if update_type == "judge":
        logger.info("=== ОБНОВЛЕНИЕ ДЕЛ ===")
        logger.info("Начинаю обновление дел без судей")
    elif update_type == "events":
        logger.info("Начинаю обновление истории дел")

def log_periodic_stats(logger, found, total, success_rate):
    logger.info(f"Статистика (каждые 100 попыток): найдено {found} из {total} ({success_rate}% успеха)")

def log_region_completed(logger, region_name, court_name, found_count, hours):
    logger.info(f"Завершен регион: {region_name}, {court_name} (найдено: {found_count} дела, время: {hours} часа)")

def log_error(logger, message, attempt=None, max_attempts=None):
    attempt_info = f" (попытка {attempt}/{max_attempts})" if attempt else ""
    logger.error(f"ОШИБКА: {message}{attempt_info}")

def log_critical(logger, message):
    logger.critical(f"КРИТИЧНО: {message}")

# Заглушки для совместимости со старым кодом
def set_trace_id(trace_id): pass
def log_stage_event(logger, stage, event, level="INFO", details=None):
    message = event.replace('_', ' ').title()
    if details:
        if 'case_number' in details:
            message = f"{message}: {details['case_number']}"
    getattr(logger, level.lower())(message)
def log_http_request(logger, url, status, duration, stage): pass