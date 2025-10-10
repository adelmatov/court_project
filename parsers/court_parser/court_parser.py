import asyncio
import aiohttp
import asyncpg
import json
import random
import re
import signal
import sys
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from selectolax.parser import HTMLParser
from logging_config import setup_logging, get_logger, log_search_start, log_case_found, log_case_saved, log_case_not_found, log_missing_search_start, log_missing_found, log_update_start, log_periodic_stats, log_region_completed, log_error, log_critical


# ============================================================================
# МОНИТОРИНГ ЗАПРОСОВ
# ============================================================================

class RequestMonitor:
    """Мониторинг успешности HTTP запросов"""
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.requests_history = []
        self.lock = asyncio.Lock()
        self.logger = get_logger('request_monitor')
    
    async def record_request(self, url: str, status_code: int, 
                           duration_ms: int, stage: str) -> None:
        """Записать результат запроса"""
        async with self.lock:
            timestamp = time.time()
            success = 200 <= status_code < 400
            
            self.requests_history.append({
                'timestamp': timestamp,
                'url': url,
                'status_code': status_code,
                'duration_ms': duration_ms,
                'stage': stage,
                'success': success
            })
            
            # Ограничиваем размер истории
            if len(self.requests_history) > self.window_size:
                self.requests_history.pop(0)
            
            # Логируем статистику каждые 20 запросов
            if len(self.requests_history) % 20 == 0:
                await self._log_statistics()
    
    async def _log_statistics(self) -> None:
        """Логировать статистику запросов только при критических проблемах"""
        if not self.requests_history:
            return
        
        total_requests = len(self.requests_history)
        successful_requests = sum(1 for r in self.requests_history if r['success'])
        success_rate = (successful_requests / total_requests) * 100
        
        # Подсчет 502 ошибок за текущий период
        error_502_count = sum(1 for r in self.requests_history if r['status_code'] == 502)
        
        # Логируем ТОЛЬКО критические проблемы
        if success_rate < 70:
            self.logger.warning(f"Низкий процент успеха: {round(success_rate, 1)}% за последние {total_requests} запросов")
        
        if error_502_count > 8:  # Повышен порог для меньшего количества предупреждений
            self.logger.warning(f"Много 502 ошибок: {error_502_count} из {total_requests}")
        
        # Критически низкий успех - детальная информация
        if success_rate < 50:
            # Статистика по кодам ответа только при критических проблемах
            status_codes = {}
            for request in self.requests_history:
                code = request['status_code']
                status_codes[code] = status_codes.get(code, 0) + 1
            
            # Показываем только проблемные коды
            problem_codes = {k: v for k, v in status_codes.items() if k >= 400}
            if problem_codes:
                self.logger.error(f"Коды ошибок: {problem_codes}")
    
    async def get_current_stats(self) -> Dict[str, Any]:
        """Получить текущую статистику"""
        async with self.lock:
            if not self.requests_history:
                return {'total_requests': 0, 'success_rate': 0}
            
            total = len(self.requests_history)
            successful = sum(1 for r in self.requests_history if r['success'])
            
            return {
                'total_requests': total,
                'success_rate': (successful / total) * 100,
                'recent_requests': self.requests_history[-5:] if self.requests_history else []
            }

# ============================================================================
# КОНФИГУРАЦИЯ И КОНСТАНТЫ
# ============================================================================

class ParsingConfig:
    """Константы для парсинга"""
    BATCH_SIZE = 10
    DELAY_NO_RESULTS = 1.5
    DELAY_WITH_RESULTS = 2.5
    DELAY_AFTER_502 = 8.0
    MAX_CONCURRENT_SESSIONS = 5
    SESSION_POOL_SIZE = 10
    MAX_CONSECUTIVE_NO_DATA = 5
    
    # Задержки для авторизации
    AUTH_STEP_DELAY = 1.0
    AUTH_CHECK_DELAY = 0.5
    
    # БД константы
    DB_MIN_POOL_SIZE = 1
    DB_MAX_POOL_SIZE = 20

class SQLQueries:
    """SQL запросы"""
    INSERT_JUDGES = """
        INSERT INTO judges (full_name) 
        VALUES (unnest($1::text[])) 
        ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name 
        RETURNING id, full_name
    """
    
    INSERT_PARTIES = """
        INSERT INTO parties (name) 
        VALUES (unnest($1::text[])) 
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name 
        RETURNING id, name
    """
    
    INSERT_EVENT_TYPES = """
        INSERT INTO event_types (name) 
        VALUES (unnest($1::text[])) 
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name 
        RETURNING id, name
    """
    
    INSERT_CASE_PARTIES = """
        INSERT INTO case_parties (case_id, party_id, party_role) 
        VALUES ($1, $2, $3) 
        ON CONFLICT (case_id, party_id, party_role) DO NOTHING
    """
    
    INSERT_CASE_EVENTS = """
        INSERT INTO case_events (case_id, event_type_id, event_date) 
        VALUES ($1, $2, $3)
        ON CONFLICT (case_id, event_type_id, event_date) DO NOTHING
    """
    
    SELECT_ALL_JUDGES = "SELECT id, full_name FROM judges"
    SELECT_ALL_PARTIES = "SELECT id, name FROM parties"
    SELECT_ALL_EVENT_TYPES = "SELECT id, name FROM event_types"
    
    GET_LAST_CASE_NUMBER = """
        SELECT COALESCE(MAX(
            CAST(SPLIT_PART(case_number, '/', 2) AS INTEGER)
        ), 0) as last_num
        FROM cases c
        WHERE case_number LIKE $1
        AND case_number ~ $2
    """

    GET_MISSING_NUMBERS = """
        WITH existing_numbers AS (
            SELECT CAST(SPLIT_PART(case_number, '/', 2) AS INTEGER) as num
            FROM cases 
            WHERE case_number LIKE $1
              AND case_number ~ $2
        ),
        target_range AS (
            SELECT generate_series($3::INTEGER, $4::INTEGER) as num
        )
        SELECT t.num as missing_number
        FROM target_range t
        LEFT JOIN existing_numbers e ON t.num = e.num
        WHERE e.num IS NULL
        ORDER BY t.num;
    """

# ============================================================================
# ИСКЛЮЧЕНИЯ
# ============================================================================

class CourtParserError(Exception):
    """Базовое исключение парсера"""
    pass

class AuthenticationError(CourtParserError):
    """Ошибка авторизации"""
    pass

class ParseError(CourtParserError):
    """Ошибка парсинга"""
    pass

class DatabaseError(CourtParserError):
    """Ошибка базы данных"""
    pass

class ConfigurationError(CourtParserError):
    """Ошибка конфигурации"""
    pass

# ============================================================================
# ВАЛИДАТОРЫ КОНФИГУРАЦИИ
# ============================================================================

class ConfigValidator:
    """Валидатор конфигурации"""
    
    @staticmethod
    def validate_database_config(db_config: Dict[str, Any]) -> None:
        """Валидация конфигурации базы данных"""
        required_fields = ['host', 'port', 'dbname', 'user', 'password']
        for field in required_fields:
            if field not in db_config:
                raise ConfigurationError(
                    f"Отсутствует обязательное поле БД: {field}"
                )
        
        if not isinstance(db_config['port'], int) or db_config['port'] <= 0:
            raise ConfigurationError(
                "Порт БД должен быть положительным числом"
            )
    
    @staticmethod
    def validate_parsing_config(config: Dict[str, Any]) -> None:
        """Валидация основной конфигурации парсинга"""
        if 'regions' not in config or not config['regions']:
            raise ConfigurationError("Не задана конфигурация регионов")
        
        for region_key, region_config in config['regions'].items():
            if 'courts' not in region_config:
                raise ConfigurationError(
                    f"Отсутствуют суды для региона {region_key}"
                )
            
            if 'kato_code' not in region_config:
                raise ConfigurationError(
                    f"Отсутствует kato_code для региона {region_key}"
                )

# ============================================================================
# ОБРАБОТЧИК ОШИБОК БД
# ============================================================================

class DatabaseErrorHandler:
    """Централизованная обработка ошибок БД"""
    
    @staticmethod
    async def handle_db_error(error: Exception, operation: str, 
                            case_number: str = "UNKNOWN") -> str:
        """Централизованная обработка ошибок БД"""
        logger = get_logger('db_error_handler')
        
        if isinstance(error, asyncpg.UniqueViolationError):
            logger.debug(f"Дело {case_number} уже существует")
            return 'skipped'
        
        logger.error(f"Ошибка БД при {operation} для дела {case_number}: {str(error)}")
        return 'error'

# ============================================================================
# МЕНЕДЖЕР ТРАНЗАКЦИОННЫХ БАТЧЕЙ
# ============================================================================

class TransactionalBatchManager:
    """Менеджер для безопасного управления батчами с гарантией целостности"""
    
    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size
        self.pending_cases = []
        self.logger = get_logger('batch_manager')
    
    async def add_case(self, case_data: Dict[str, Any], 
                      db_manager, force_save: bool = False) -> Dict[str, int]:
        """Добавить дело в батч с автоматическим сохранением"""
        self.pending_cases.append(case_data)
        
        # Сохраняем при достижении размера батча или принудительно
        if len(self.pending_cases) >= self.batch_size or force_save:
            return await self._save_current_batch(db_manager)
        
        return {'saved': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
    
    async def _save_current_batch(self, db_manager) -> Dict[str, int]:
        """Сохранение текущего батча с retry логикой"""
        if not self.pending_cases:
            return {'saved': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                # Создаем копию для безопасности
                batch_copy = self.pending_cases.copy()
                
                # Пытаемся сохранить
                stats = await db_manager.batch_save_cases(batch_copy)
                
                # Успешно - очищаем батч
                self.pending_cases.clear()
                
                self.logger.debug(f"Сохранено: батч из {len(batch_copy)} дел "
                                f"(успешно: {stats['saved']}, пропущено: {stats['skipped']}, ошибок: {stats['errors']})")
                
                return stats
                
            except Exception as e:
                last_error = e
                self.logger.warning(f"Ошибка сохранения батча (попытка {attempt + 1}): {str(e)}")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
        
        # Если все попытки неудачны - логируем для восстановления
        await self._handle_failed_batch(last_error)
        return {'saved': 0, 'updated': 0, 'skipped': 0, 'errors': len(self.pending_cases)}
    
    async def _handle_failed_batch(self, error: Exception) -> None:
        """Обработка критически важного батча, который не удалось сохранить"""
        # Сохраняем данные в файл для восстановления
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        recovery_file = f"recovery_batch_{timestamp}.json"
        
        try:
            with open(recovery_file, 'w', encoding='utf-8') as f:
                json.dump(self.pending_cases, f, ensure_ascii=False, indent=2, default=str)
            
            log_critical(self.logger, f"не удалось сохранить батч, данные записаны в файл восстановления {recovery_file}")
        except Exception as save_error:
            log_critical(self.logger, f"критическая ошибка: не удалось сохранить данные в файл восстановления: {str(save_error)}")
        
        # Очищаем батч после сохранения в файл
        self.pending_cases.clear()
    
    async def finalize(self, db_manager) -> Dict[str, int]:
        """Принудительное сохранение оставшихся данных"""
        return await self._save_current_batch(db_manager)

# ============================================================================
# КЛАСС ОБРАБОТКИ ТЕКСТА
# ============================================================================

class TextProcessor:
    """Класс для обработки и очистки извлекаемого текста"""
    _QUOTES_SET = frozenset('"»""„\'«')

    @staticmethod
    def clean_text(text: str) -> str:
        """Очистка текста от лишних пробелов и символов"""
        if not text:
            return text
        return ' '.join(text.split()).strip()
    
    @staticmethod
    def smart_split_parties(text: str) -> List[str]:
        """Умное разделение сторон с учетом кавычек"""
        if not text.strip():
            return []
        
        # Используем предопределенное множество кавычек
        has_quotes = any(char in TextProcessor._QUOTES_SET for char in text)
        
        if not has_quotes:
            # Нет кавычек - простое деление по запятой
            parts = text.split(',')
            cleaned_parts = []
            for part in parts:
                cleaned_part = part.strip(' .,;-')
                if cleaned_part:
                    cleaned_parts.append(cleaned_part)
            return cleaned_parts
        
        else:
            # Есть кавычки - простая но эффективная логика
            parts = []
            current_part = ""
            
            i = 0
            while i < len(text):
                char = text[i]
                current_part += char
                
                if char == ',':
                    prev_char = text[i - 1] if i > 0 else ''
                    part_without_comma = current_part[:-1]
                    
                    # Считаем кавычки в текущей части
                    quote_count = sum(
                        1 for c in part_without_comma if c in TextProcessor._QUOTES_SET
                    )
                    
                    # Простая логика: разделяем если:
                    # 1. Запятая сразу после кавычки ИЛИ
                    # 2. Четное количество кавычек И есть текст после 
                    #    последней кавычки ИЛИ  
                    # 3. Нет кавычек вообще
                    
                    is_after_quote = prev_char in TextProcessor._QUOTES_SET
                    has_no_quotes = quote_count == 0
                    
                    # Для четного количества - дополнительная проверка
                    is_balanced_with_text = False
                    if quote_count % 2 == 0 and quote_count > 0:
                        # Находим позицию последней кавычки
                        last_quote_pos = (
                            part_without_comma.rfind(prev_char) 
                            if prev_char in TextProcessor._QUOTES_SET else -1
                        )
                        if last_quote_pos == -1:
                            # Ищем любую последнюю кавычку
                            for j in range(len(part_without_comma) - 1, -1, -1):
                                if part_without_comma[j] in TextProcessor._QUOTES_SET:
                                    last_quote_pos = j
                                    break
                        
                        # Если после последней кавычки есть больше чем просто 
                        # пробелы
                        if last_quote_pos != -1:
                            after_last_quote = (
                                part_without_comma[last_quote_pos + 1:].strip()
                            )
                            # Разделяем только если после кавычки достаточно 
                            # текста (целые слова)
                            is_balanced_with_text = (
                                len(after_last_quote.split()) >= 2
                            )
                    
                    if (is_after_quote or has_no_quotes or 
                        is_balanced_with_text):
                        cleaned_part = part_without_comma.strip(' .,;-')
                        if cleaned_part:
                            parts.append(cleaned_part)
                        current_part = ""
                        
                        # Пропускаем пробел после запятой, если он есть
                        if i + 1 < len(text) and text[i + 1] == ' ':
                            i += 1
                
                i += 1
            
            # Последняя часть
            cleaned_part = current_part.strip(' .,;-')
            if cleaned_part:
                parts.append(cleaned_part)
            
            return parts
    
    @staticmethod
    def extract_sequence_number_from_case(case_number: str) -> Optional[int]:
        """Извлечь порядковый номер из номера дела"""
        if '/' in case_number:
            try:
                sequence_part = case_number.split('/')[-1]
                return int(sequence_part)
            except (ValueError, IndexError):
                pass
        return None
    
    @staticmethod
    def parse_date(date_str: str, 
                  format_str: str = '%d.%m.%Y') -> Optional[datetime]:
        """Парсинг даты из строки"""
        try:
            return datetime.strptime(date_str.strip(), format_str)
        except ValueError:
            return None
    
    @staticmethod
    def validate_case_number_format(case_number: str) -> bool:
        """Проверка формата номера дела"""
        return bool(case_number and '/' in case_number and '-' in case_number)
    
    @staticmethod
    def parse_full_case_number(full_case_number: str) -> Optional[Dict[str, str]]:
        """Парсинг полного номера дела на составные части"""
        try:
            if not TextProcessor.validate_case_number_format(full_case_number):
                return None
            
            prefix_part, sequence_part = full_case_number.split('/', 1)
            parts = prefix_part.split('-')
            
            if len(parts) != 4:
                return None
            
            return {
                'court_code': parts[0],
                'year_part': parts[1],
                'middle_part': parts[2],
                'case_type': parts[3],
                'sequence': sequence_part
            }
        except (ValueError, IndexError):
            return None
    
    @staticmethod
    def extract_case_info_from_cell(case_cell) -> Tuple[str, Optional[str]]:
        """Извлечь информацию о деле из ячейки таблицы"""
        case_paragraphs = case_cell.css('p')
        case_number = ""
        case_date = None
        
        if case_paragraphs:
            case_number = TextProcessor.clean_text(case_paragraphs[0].text())
            if len(case_paragraphs) > 1:
                case_date = TextProcessor.clean_text(case_paragraphs[1].text())
        
        return case_number, case_date
    
    @staticmethod
    def extract_parties_from_cell(parties_cell) -> Tuple[List[str], List[str]]:
        """Извлечь информацию о сторонах из ячейки таблицы"""
        parties_paragraphs = parties_cell.css('p')
        plaintiffs = []
        defendants = []
        
        if len(parties_paragraphs) == 2:
            first_party_text = TextProcessor.clean_text(
                parties_paragraphs[0].text()
            )
            if first_party_text:
                plaintiffs = TextProcessor.smart_split_parties(
                    first_party_text
                )
            
            second_party_text = TextProcessor.clean_text(
                parties_paragraphs[1].text()
            )
            if second_party_text:
                defendants = TextProcessor.smart_split_parties(
                    second_party_text
                )
        
        return plaintiffs, defendants
    
    @staticmethod
    def extract_judge_from_cell(judge_cell) -> Optional[str]:
        """Извлечь информацию о судье из ячейки таблицы"""
        judge_name = (
            TextProcessor.clean_text(judge_cell.text()) 
            if judge_cell else ""
        )
        return judge_name if judge_name else None
    
    @staticmethod
    def extract_events_from_cell(history_cell, 
                               case_number: str) -> List[Dict[str, Any]]:
        """Извлечь события дела из ячейки истории"""
        history_paragraphs = history_cell.css('p')
        events = []
        
        for paragraph in history_paragraphs:
            history_text = TextProcessor.clean_text(paragraph.text())
            if ' - ' in history_text:
                try:
                    date_part, event_part = history_text.split(' - ', 1)
                    event_date = TextProcessor.parse_date(date_part)
                    event_type = TextProcessor.clean_text(event_part)
                    
                    if event_date and event_type:
                        events.append({
                            'event_date': event_date.date(),
                            'event_type': event_type
                        })
                except ValueError:
                    pass
        
        return events

# ============================================================================
# СИСТЕМА УПРАВЛЕНИЯ НАЙДЕННЫМИ НОМЕРАМИ
# ============================================================================ 
class NumberRanges:
    """Эффективное хранение диапазонов найденных номеров"""
    
    def __init__(self):
        self.ranges = []
    
    def add_numbers(self, numbers: List[int]) -> None:
        """Добавить номера в диапазоны"""
        for num in sorted(numbers):
            self._add_single_number(num)
    
    def _add_single_number(self, num: int) -> None:
        """Добавить один номер"""
        for i, (start, end) in enumerate(self.ranges):
            if num < start - 1:
                self.ranges.insert(i, (num, num))
                self._merge_adjacent_ranges(i)
                return
            elif start <= num <= end:
                return
            elif num == end + 1:
                self.ranges[i] = (start, num)
                self._merge_adjacent_ranges(i)
                return
        
        self.ranges.append((num, num))
        if len(self.ranges) > 1:
            self._merge_adjacent_ranges(-1)
    
    def _merge_adjacent_ranges(self, index: int) -> None:
        """Объединить соседние диапазоны"""
        if index < 0:
            index = len(self.ranges) + index
        
        if index > 0:
            prev_start, prev_end = self.ranges[index - 1]
            curr_start, curr_end = self.ranges[index]
            if prev_end + 1 >= curr_start:
                self.ranges[index - 1] = (
                    prev_start, max(prev_end, curr_end)
                )
                self.ranges.pop(index)
                index -= 1
        
        if index < len(self.ranges) - 1:
            curr_start, curr_end = self.ranges[index]
            next_start, next_end = self.ranges[index + 1]
            if curr_end + 1 >= next_start:
                self.ranges[index] = (curr_start, max(curr_end, next_end))
                self.ranges.pop(index + 1)
    
    def contains(self, num: int) -> bool:
        """Проверить содержит ли номер"""
        for start, end in self.ranges:
            if start <= num <= end:
                return True
            elif num < start:
                return False
        return False

class SmartNumberTracker:
    """Умный трекер номеров с автоматическим выбором стратегии"""
    
    def __init__(self, max_number: int, memory_limit_mb: int = 50):
        self.max_number = max_number
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024
        self.found_numbers_set = set()
        self.found_numbers_ranges = None
        self.strategy = 'set'
        self.check_counter = 0
    
    def add_numbers(self, numbers: List[int]) -> None:
        """Добавить номера"""
        if not numbers:
            return
            
        self.check_counter += 1
        
        if self.check_counter % 1000 == 0:
            self._optimize_strategy()
        
        if self.strategy == 'set':
            self.found_numbers_set.update(numbers)
        elif self.strategy == 'ranges':
            if self.found_numbers_ranges is None:
                self._convert_to_ranges()
            self.found_numbers_ranges.add_numbers(numbers)
    
    def contains(self, number: int) -> bool:
        """Проверить содержит ли номер"""
        if self.strategy == 'set':
            return number in self.found_numbers_set
        elif self.strategy == 'ranges':
            return (
                self.found_numbers_ranges.contains(number) 
                if self.found_numbers_ranges else False
            )
        return False
    
    def _optimize_strategy(self) -> None:
        """Оптимизировать стратегию хранения"""
        estimated_memory = self._estimate_memory_usage()
        if (estimated_memory > self.memory_limit_bytes and 
            self.strategy == 'set'):
            self._convert_to_ranges()
    
    def _convert_to_ranges(self) -> None:
        """Конвертировать в диапазоны"""
        self.found_numbers_ranges = NumberRanges()
        if self.found_numbers_set:
            self.found_numbers_ranges.add_numbers(
                list(self.found_numbers_set)
            )
            self.found_numbers_set.clear()
        self.strategy = 'ranges'
    
    def _estimate_memory_usage(self) -> int:
        """Оценка использования памяти в байтах"""
        if self.strategy == 'set':
            # Python int object overhead + set overhead
            base_int_size = 28
            set_overhead = 144 + (len(self.found_numbers_set) * 8)
            return (len(self.found_numbers_set) * base_int_size + 
                   set_overhead)
        elif self.strategy == 'ranges' and self.found_numbers_ranges:
            # Tuple overhead (start, end) + list overhead
            tuple_size = 56  # (int, int) tuple
            list_overhead = 64
            return (len(self.found_numbers_ranges.ranges) * tuple_size + 
                   list_overhead)
        return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Статистика"""
        return {
            'strategy': self.strategy,
            'memory_mb': self._estimate_memory_usage() / 1024 / 1024
        }

class RegionCourtKey:
    """Ключ для идентификации уникальной комбинации регион-суд-год"""
    
    def __init__(self, region_key: str, court_key: str, year: str):
        self.region_key = region_key
        self.court_key = court_key
        self.year = year
        self.key = f"{region_key}_{court_key}_{year}"
    
    def __str__(self) -> str:
        return self.key
    
    def __hash__(self) -> int:
        return hash(self.key)
    
    def __eq__(self, other) -> bool:
        return isinstance(other, RegionCourtKey) and self.key == other.key

class GlobalParsingStateManager:
    """Глобальный менеджер состояний для всех асинхронных задач"""
    
    def __init__(self):
        self._states: Dict[RegionCourtKey, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self.logger = get_logger('state_manager')
    
    async def get_or_create_state(self, region_key: str, court_key: str, 
                                year: str, start_number: int, 
                                max_number: int) -> Dict[str, Any]:
        """Получить или создать состояние для региона-суда"""
        key = RegionCourtKey(region_key, court_key, year)
        
        async with self._lock:
            if key not in self._states:
                self._states[key] = self._create_initial_state(
                    start_number, max_number, key
                )
                self.logger.debug(f"Создано состояние для {region_key} - {court_key}, старт с номера {start_number}")
            
            return self._states[key]
    
    def _create_initial_state(self, start_number: int, max_number: int, 
                        key: RegionCourtKey) -> Dict[str, Any]:
        """Создать начальное состояние с правильным отслеживанием максимального номера"""
        return {
            'key': key,
            'consecutive_failures': 0,
            'consecutive_no_data': 0,
            'max_failures': 50,
            'current_number': start_number,
            'search_attempts': 0,
            'total_found': 0,
            'total_saved': 0,
            'cases_batch': [],
            'batch_manager': TransactionalBatchManager(ParsingConfig.BATCH_SIZE),
            'max_case_number': max_number,
            'highest_processed_number': start_number - 1,
            'found_numbers': SmartNumberTracker(max_number, memory_limit_mb=50),
            'start_number': start_number,
            'last_success_number': start_number - 1,
            'created_at': datetime.now(),
            'missing_found_count': 0,
            'missing_numbers_mode': False,
            'missing_numbers_queue': [],
            'missing_numbers_processed': 0,
        }
    
    async def get_global_stats(self) -> Dict[str, Any]:
        """Получить глобальную статистику"""
        async with self._lock:
            total_found = sum(
                state['total_found'] for state in self._states.values()
            )
            total_attempts = sum(
                state['search_attempts'] for state in self._states.values()
            )
            active_regions = len(self._states)
            
            return {
                'active_regions': active_regions,
                'total_found': total_found,
                'total_attempts': total_attempts,
                'success_rate': (
                    (total_found / total_attempts * 100) 
                    if total_attempts > 0 else 0
                ),
            }
    
    async def cleanup_completed(self) -> None:
        """Очистить завершенные состояния"""
        async with self._lock:
            to_remove = []
            for key, state in self._states.items():
                if (state['consecutive_failures'] >= state['max_failures'] or 
                    state['consecutive_no_data'] >= 
                    ParsingConfig.MAX_CONSECUTIVE_NO_DATA or
                    state['current_number'] > state['max_case_number']):
                    to_remove.append(key)
            
            for key in to_remove:
                self.logger.debug(f"Удалено завершенное состояние: {str(key)}")
                del self._states[key]

# ============================================================================
# УТИЛИТЫ
# ============================================================================

class HTMLUtils:
    """Утилиты для работы с HTML"""
    
    @staticmethod
    def extract_viewstate(html: str) -> Optional[str]:
        """Извлечь ViewState из HTML"""
        parser = HTMLParser(html)
        viewstate_input = parser.css_first(
            'input[name="javax.faces.ViewState"]'
        )
        if viewstate_input:
            return viewstate_input.attributes.get('value')
        return None

class FormExtractor:
    """Извлечение данных из форм"""
    
    @staticmethod
    def extract_language_ids(html: str) -> Dict[str, str]:
        """Извлечь ID для смены языка"""
        parser = HTMLParser(html)
        ids = {}
        
        script_elements = parser.css('script')
        for script in script_elements:
            if script.text() and 'selectLanguageRu' in script.text():
                script_text = script.text()
                match = re.search(r'RichFaces\.ajax\("([^"]+)"', script_text)
                if match:
                    ids['language_script_id'] = match.group(1)
                    break
        
        if not ids.get('language_script_id'):
            span_elements = parser.css('span')
            for span in span_elements:
                span_id = span.attributes.get('id', '')
                if 'f_l_temp' in span_id:
                    ids['language_script_id'] = span_id
                    break
                    
        return ids
    
    @staticmethod
    def extract_auth_ids(html: str) -> Dict[str, str]:
        """Извлечь ID формы авторизации"""
        parser = HTMLParser(html)
        ids = {}
        
        xin_input = (
            parser.css_first('input[placeholder*="ИИН"]') or 
            parser.css_first('input[type="email"]')
        )
        if xin_input and xin_input.attributes.get('name'):
            name = xin_input.attributes['name']
            ids['xin_field'] = name
            if ':' in name:
                ids['auth_form_base'] = ':'.join(name.split(':')[:-1])
        
        password_input = parser.css_first('input[type="password"]')
        if password_input and password_input.attributes.get('name'):
            ids['password_field'] = password_input.attributes['name']
        
        login_button = (
            parser.css_first('input[value*="Войти"]') or 
            parser.css_first('input[type="submit"]')
        )
        if login_button and login_button.attributes.get('name'):
            ids['login_button'] = login_button.attributes['name']
        
        return ids
    
    @staticmethod
    def extract_search_form_ids(parser: HTMLParser) -> Dict[str, str]:
        """Извлечь все ID формы поиска"""
        ids = {}
        
        form = parser.css_first('form')
        if form and form.attributes.get('id'):
            ids['form_id'] = form.attributes['id']
        
        field_mappings = [
            'edit-district', 'edit-court', 'edit-year', 
            'edit-num', 'edit-iin', 'edit-fio'
        ]
        
        for field in field_mappings:
            elements = parser.css(f'[id*="{field}"]')
            for element in elements:
                if element.attributes.get('id'):
                    ids[field] = element.attributes['id']
                    name = element.attributes.get('name', '')
                    if ':' in name:
                        ids[f'{field}_base'] = ':'.join(name.split(':')[:-1])
                    break
        
        script_elements = parser.css('script')
        for script in script_elements:
            if script.text() and 'goNext' in script.text():
                script_text = script.text()
                match = re.search(r'RichFaces\.ajax\("([^"]+)"', script_text)
                if match:
                    ids['search_script_id'] = match.group(1)
                    break
        
        return ids

class HTTPHeaders:
    """Генерация HTTP заголовков"""
    
    BASE_HEADERS = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/140.0.0.0 Safari/537.36'
        ),
        'Accept': (
            'text/html,application/xhtml+xml,application/xml;q=0.9,'
            'image/avif,image/webp,image/apng, */*;q=0.8'
        ),
        'Accept-Language': 'ru,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }
    
    @classmethod
    def get_base_headers(cls) -> Dict[str, str]:
        """Базовые заголовки"""
        return cls.BASE_HEADERS.copy()
    
    @classmethod
    def get_ajax_headers(cls) -> Dict[str, str]:
        """AJAX заголовки"""
        headers = cls.get_base_headers()
        headers.update({
            'Accept': '*/*',
            'Content-Type': (
                'application/x-www-form-urlencoded;charset=UTF-8'
            ),
            'Faces-Request': 'partial/ajax',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        })
        return headers

# ============================================================================
# МЕНЕДЖЕР АДАПТИВНЫХ ТАЙМ-АУТОВ
# ============================================================================

class AdaptiveTimeoutManager:
    """ИСПРАВЛЕНО: Более агрессивные тайм-ауты для предотвращения зависания"""
    
    def __init__(self):
        self.timeouts = {
            'auth': aiohttp.ClientTimeout(total=20, connect=8, sock_read=15),      # Уменьшено
            'search': aiohttp.ClientTimeout(total=25, connect=8, sock_read=20),    # Уменьшено
            'results': aiohttp.ClientTimeout(total=30, connect=8, sock_read=25),   # Уменьшено
            'form_load': aiohttp.ClientTimeout(total=15, connect=6, sock_read=12)  # Уменьшено
        }
        self.success_history = defaultdict(list)
        self.logger = get_logger('timeout_manager')
    
    def get_timeout(self, operation_type: str) -> aiohttp.ClientTimeout:
        """ИСПРАВЛЕНО: Более строгие тайм-ауты"""
        base_timeout = self.timeouts.get(operation_type, 
                                        aiohttp.ClientTimeout(total=20, connect=6))
        
        # Анализируем историю успешности
        recent_failures = self._get_recent_failure_rate(operation_type)
        
        if recent_failures > 0.4:  # Более 40% неудач
            # Увеличиваем тайм-аут умеренно (только на 30%)
            return aiohttp.ClientTimeout(
                total=int(base_timeout.total * 1.3) if base_timeout.total else 25,
                connect=int(base_timeout.connect * 1.3) if base_timeout.connect else 8,
                sock_read=int((base_timeout.sock_read or 15) * 1.3)
            )
        
        return base_timeout
    
    def record_result(self, operation_type: str, success: bool, duration: float) -> None:
        """Записать результат операции для адаптации"""
        self.success_history[operation_type].append({
            'success': success,
            'duration': duration,
            'timestamp': time.time()
        })
        
        # Ограничиваем историю последними 50 операциями
        if len(self.success_history[operation_type]) > 50:
            self.success_history[operation_type].pop(0)
    
    def _get_recent_failure_rate(self, operation_type: str) -> float:
        """Получить процент неудач за последние операции"""
        history = self.success_history[operation_type]
        if not history:
            return 0.0
        
        # Берем последние 10 операций
        recent = history[-10:]
        failures = sum(1 for r in recent if not r['success'])
        
        return failures / len(recent)

# ============================================================================
# МЕНЕДЖЕР СЕССИЙ
# ============================================================================

class SessionManager:
    """ИСПРАВЛЕНО: Полное управление жизненным циклом сессий"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = get_logger('session_manager')
        self.session_pool = asyncio.Queue(maxsize=ParsingConfig.SESSION_POOL_SIZE)
        self.base_url = config['base_url']
        self.session_stats = defaultdict(int)
        self.max_session_reuse = 100
        self.timeout_manager = AdaptiveTimeoutManager()
        
        # ДОБАВЛЕНО: Отслеживание всех созданных сессий
        self._all_sessions = set()
        self._creating_sessions = set()
        self._sessions_lock = asyncio.Lock()
        
    async def create_authorized_session(self) -> aiohttp.ClientSession:
        """ИСПРАВЛЕНО: Создание с полным отслеживанием сессий"""
        
        session_task_id = id(asyncio.current_task())
        
        async with self._sessions_lock:
            self._creating_sessions.add(session_task_id)
        
        session = None
        try:
            self.logger.debug("Создание новой авторизованной сессии")
            
            auth_timeout = self.timeout_manager.get_timeout('auth')
            # ИСПРАВЛЕНО: Создаем коннектор с явным закрытием
            connector = aiohttp.TCPConnector(ssl=False, limit=10, limit_per_host=5)
            session = aiohttp.ClientSession(
                timeout=auth_timeout, 
                connector=connector,
                connector_owner=True  # Сессия владеет коннектором
            )
            
            # Регистрируем сессию
            async with self._sessions_lock:
                self._all_sessions.add(session)
            
            start_time = time.time()
            try:
                await self._authorize_session_with_retry(session)
                
                duration = time.time() - start_time
                self.timeout_manager.record_result('auth', True, duration)
                
                self.logger.info("Авторизация успешна")
                return session
                
            except Exception as e:
                # При ошибке авторизации закрываем сессию
                await self._close_session_safely(session)
                
                duration = time.time() - start_time
                self.timeout_manager.record_result('auth', False, duration)
                
                log_error(self.logger, f"не удалось создать авторизованную сессию: {e}")
                raise AuthenticationError(f"Ошибка создания авторизованной сессии: {e}")
                
        except Exception as e:
            # При любой ошибке закрываем сессию
            if session:
                await self._close_session_safely(session)
            raise
        finally:
            async with self._sessions_lock:
                self._creating_sessions.discard(session_task_id)
    
    async def _authorize_session_with_retry(self, session: aiohttp.ClientSession) -> None:
        """Авторизация с retry логикой для каждого этапа"""
        max_auth_retries = 3
        auth_step_timeout = 30  # Секунд на каждый этап
        
        for attempt in range(max_auth_retries):
            try:
                self.logger.debug(f"Попытка авторизации {attempt + 1}/{max_auth_retries}")
                
                # Этап 1: Загрузка главной страницы с timeout
                viewstate = await asyncio.wait_for(
                    self._load_main_page(session), 
                    timeout=auth_step_timeout
                )
                
                # Этап 2: Переход на страницу авторизации
                await asyncio.wait_for(
                    self._navigate_to_auth_page(session, viewstate), 
                    timeout=auth_step_timeout
                )
                
                # Этап 3: Выполнение логина
                await asyncio.wait_for(
                    self._perform_login(session, viewstate), 
                    timeout=auth_step_timeout
                )
                
                # Этап 4: Проверка авторизации
                await asyncio.wait_for(
                    self._verify_authentication(session), 
                    timeout=auth_step_timeout
                )
                
                self.logger.debug("Авторизация завершена успешно")
                return  # Успешная авторизация
                
            except asyncio.TimeoutError:
                log_error(self.logger, f"таймаут авторизации на попытке {attempt + 1}")
                
            except Exception as e:
                log_error(self.logger, f"ошибка авторизации на попытке {attempt + 1}: {str(e)}")
            
            # Задержка перед следующей попыткой
            if attempt < max_auth_retries - 1:
                delay = 5 * (2 ** attempt)  # 5, 10, 20 секунд
                self.logger.debug(f"Задержка {delay}с перед следующей попыткой")
                await asyncio.sleep(delay)
        
        raise AuthenticationError("Не удалось авторизоваться после всех попыток")

    async def _load_main_page(self, session: aiohttp.ClientSession) -> Optional[str]:
        """Загрузка главной страницы"""
        url = f"{self.base_url}/"
        start_time = time.time()
        
        async with session.get(url, headers=HTTPHeaders.get_base_headers()) as response:
            duration_ms = int((time.time() - start_time) * 1000)
            
            if response.status != 200:
                raise AuthenticationError(
                    f"HTTP {response.status} при загрузке главной страницы"
                )
            
            html = await response.text()
            viewstate = HTMLUtils.extract_viewstate(html)
            
        await asyncio.sleep(ParsingConfig.AUTH_STEP_DELAY)
        return viewstate
    
    async def _navigate_to_auth_page(self, session: aiohttp.ClientSession, 
                                   viewstate: Optional[str]) -> None:
        """Переход на страницу авторизации"""
        url = f"{self.base_url}/index.xhtml"
        start_time = time.time()
        
        async with session.get(url, headers=HTTPHeaders.get_base_headers()) as response:
            current_html = await response.text() 
            lang_ids = FormExtractor.extract_language_ids(current_html)
            
        current_viewstate = HTMLUtils.extract_viewstate(current_html)
        
        script_id = lang_ids.get('language_script_id', 'f_l_temp:js_temp_1 ') 
        data = self._build_language_change_data(
            script_id, current_viewstate or viewstate
        )
        headers = HTTPHeaders.get_ajax_headers()
        headers['Referer'] = f'{self.base_url}/'
        
        start_time = time.time()
        async with session.post(url, data=data, headers=headers) as response:
            await response.text()
        
        await asyncio.sleep(ParsingConfig.AUTH_CHECK_DELAY)
    
    async def _perform_login(self, session: aiohttp.ClientSession, 
                           viewstate: Optional[str]) -> None:
        """Выполнение авторизации"""
        url = f"{self.base_url}/index.xhtml"
        start_time = time.time()
        
        async with session.get(url, headers=HTTPHeaders.get_base_headers()) as response:
            auth_html = await response.text()
            
        auth_ids = FormExtractor.extract_auth_ids(auth_html)
        current_viewstate = HTMLUtils.extract_viewstate(auth_html)
        
        auth_config = self.config['auth']
        data = self._build_login_data(
            auth_ids, auth_config, current_viewstate or viewstate
        )
        
        headers = HTTPHeaders.get_ajax_headers()
        headers['Referer'] = f'{self.base_url}/index.xhtml'
        
        start_time = time.time()
        async with session.post(url, data=data, headers=headers) as response:
            await response.text()
            
        await asyncio.sleep(ParsingConfig.AUTH_CHECK_DELAY)
    
    async def _verify_authentication(self, session: aiohttp.ClientSession) -> None:
        """Проверка авторизации"""
        services_url = f"{self.base_url}/form/proceedings/services.xhtml"
        start_time = time.time()
        
        async with session.get(services_url, headers=HTTPHeaders.get_base_headers()) as response:
            html = await response.text()
            
        auth_config = self.config['auth']
        if auth_config['user_name'] not in html:
            raise AuthenticationError("Авторизация неудачна")
        
        self.logger.debug("Проверка авторизации пройдена")
    
    def _build_language_change_data(self, script_id: str, 
                                  viewstate: Optional[str]) -> Dict[str, str]:
        """Построить данные для смены языка"""
        return {
            'f_l_temp': 'f_l_temp',
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': script_id,
            'javax.faces.partial.execute': f'{script_id} @component',
            'javax.faces.partial.render': '@component',
            'param1': f'{self.base_url}/',
            'org.richfaces.ajax.component': script_id,
            script_id: script_id,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
    
    def _build_login_data(self, auth_ids: Dict[str, str], 
                         auth_config: Dict[str, str], 
                         viewstate: Optional[str]) -> Dict[str, str]:
        """Построить данные для авторизации"""
        form_base = auth_ids.get('auth_form_base', 'j_idt90:auth')
        xin_field = auth_ids.get('xin_field', f'{form_base}:xin')
        password_field = auth_ids.get('password_field', f'{form_base}:password')
        login_button = auth_ids.get('login_button', f'{form_base}:j_idt98')
        
        return {
            form_base: form_base,
            xin_field: auth_config['login'],
            password_field: auth_config['password'],
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': login_button,
            'javax.faces.partial.event': 'click',
            'javax.faces.partial.execute': f'{login_button} @component',
            'javax.faces.partial.render': '@component',
            'org.richfaces.ajax.component': login_button,
            login_button: login_button,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Получить авторизованную сессию из пула"""
        try:
            session = self.session_pool.get_nowait()
            session_id = id(session)
            
            # Проверяем количество использований
            if self.session_stats[session_id] >= self.max_session_reuse:
                await session.close()
                del self.session_stats[session_id]
                return await self.create_authorized_session()
            
            self.session_stats[session_id] += 1
            return session
            
        except asyncio.QueueEmpty:
            return await self.create_authorized_session()
    
    async def _close_session_safely(self, session: aiohttp.ClientSession) -> None:
        """Безопасное закрытие одной сессии"""
        try:
            if not session.closed:
                await session.close()
            
            async with self._sessions_lock:
                self._all_sessions.discard(session)
                
        except Exception as e:
            self.logger.debug(f"Ошибка закрытия сессии: {str(e)}")    

    async def return_session(self, session: aiohttp.ClientSession) -> None:
        """ИСПРАВЛЕНО: Возврат сессии с контролем состояния"""
        if session.closed:
            self.logger.warning("Попытка вернуть уже закрытую сессию")
            async with self._sessions_lock:
                self._all_sessions.discard(session)
            return
            
        session_id = id(session)
        try:
            self.session_pool.put_nowait(session)
        except asyncio.QueueFull:
            # Пул переполнен - закрываем сессию
            await self._close_session_safely(session)
            if session_id in self.session_stats:
                del self.session_stats[session_id]
    
    async def close_all(self) -> None:
        """ИСПРАВЛЕНО: Полное закрытие всех сессий включая создающиеся"""
        
        # Ждем завершения создающихся сессий
        max_wait_time = 3.0
        wait_start = time.time()
        
        while self._creating_sessions and (time.time() - wait_start) < max_wait_time:
            self.logger.debug(f"Ожидание завершения создания {len(self._creating_sessions)} сессий...")
            await asyncio.sleep(0.1)
        
        if self._creating_sessions:
            self.logger.warning(f"Принудительное завершение с {len(self._creating_sessions)} создающимися сессиями")
        
        # Собираем все сессии из пула
        pool_sessions = []
        while True:
            try:
                session = self.session_pool.get_nowait()
                pool_sessions.append(session)
            except asyncio.QueueEmpty:
                break
        
        # ИСПРАВЛЕНО: Закрываем ВСЕ отслеживаемые сессии
        async with self._sessions_lock:
            all_sessions_to_close = list(self._all_sessions) + pool_sessions
            # Убираем дубликаты
            unique_sessions = list(set(all_sessions_to_close))
        
        closed_count = 0
        for session in unique_sessions:
            try:
                if not session.closed:
                    await session.close()
                    closed_count += 1
            except Exception as e:
                self.logger.debug(f"Ошибка закрытия сессии: {str(e)}")
        
        # Очищаем все данные
        self.session_stats.clear()
        async with self._sessions_lock:
            self._all_sessions.clear()
            self._creating_sessions.clear()
        
        # Дополнительная задержка для завершения закрытия коннекторов
        await asyncio.sleep(0.1)
        
        self.logger.debug(f"Закрыто {closed_count} сессий")

# ============================================================================
# МЕНЕДЖЕР БАЗЫ ДАННЫХ
# ============================================================================

class DatabaseCache:
    """Кеш для базы данных"""
    
    def __init__(self):
        self.judges_cache: Dict[str, int] = {}
        self.parties_cache: Dict[str, int] = {}
        self.event_types_cache: Dict[str, int] = {}
        self._cache_loaded = False
    
    def is_loaded(self) -> bool:
        return self._cache_loaded
    
    def mark_loaded(self) -> None:
        self._cache_loaded = True

class DatabaseManager:
    """Менеджер базы данных с улучшенным логированием"""
    
    def __init__(self, db_pool: asyncpg.Pool, text_processor: TextProcessor):
        self.db_pool = db_pool
        self.cache = DatabaseCache()
        self.text_processor = text_processor
        self.logger = get_logger('database_manager')
        
        # Блокировки для каждого типа сущности
        self._judges_lock = asyncio.Lock()
        self._parties_lock = asyncio.Lock()
        self._events_lock = asyncio.Lock()
        
    async def batch_save_cases(self, cases_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch сохранение дел с детальным логированием"""
        if not cases_data:
            return {'saved': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        self.logger.debug(f"Начинаю сохранение батча из {len(cases_data)} дел")
        
        # Создаем все связанные сущности заранее
        await self._batch_create_all_entities(cases_data)
        
        stats = {'saved': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        # Сохраняем дела по одному в отдельных транзакциях
        for i, case_data in enumerate(cases_data, 1):
            try:
                result = await self._save_single_case_safe(case_data)
                stats[result] += 1
                
                if i % 5 == 0:
                    self.logger.debug(f"Обработано дел: {i}/{len(cases_data)}")
                    
            except Exception as e:
                case_number = case_data.get('case_number', 'UNKNOWN')
                self.logger.error(f"Ошибка сохранения дела {case_number}: {str(e)}")
                stats['errors'] += 1
        
        self.logger.debug(f"Батч сохранен: успешно {stats['saved']}, "
                         f"обновлено {stats['updated']}, пропущено {stats['skipped']}, "
                         f"ошибок {stats['errors']}")
        
        return stats

    async def _batch_create_all_entities(self, cases_data: List[Dict[str, Any]]) -> None:
        """Массовое создание всех связанных сущностей заранее"""
        try:
            # Загружаем кеш
            async with self.db_pool.acquire() as conn:
                await self._ensure_cache_loaded(conn)
            
            # Собираем все уникальные сущности
            all_judges = set()
            all_parties = set()
            all_event_types = set()
            
            for case_data in cases_data:
                # Судьи
                judge_name = self.text_processor.clean_text(case_data.get('judge') or '')
                if judge_name and len(judge_name) <= 200:
                    all_judges.add(judge_name)
                
                # Стороны
                for party_list in [case_data.get('plaintiffs', []), case_data.get('defendants', [])]:
                    for party_name in party_list:
                        party_name = self.text_processor.clean_text(party_name or '')
                        if party_name and len(party_name) <= 500:
                            all_parties.add(party_name)
                
                # События
                for event_item in case_data.get('events', []):
                    event_type = self.text_processor.clean_text(event_item.get('event_type') or '')
                    if event_type and len(event_type) <= 300:
                        all_event_types.add(event_type)
            
            # Создаем все сущности параллельно
            await asyncio.gather(
                self._batch_create_judges(list(all_judges)),
                self._batch_create_parties(list(all_parties)),
                self._batch_create_event_types(list(all_event_types)),
                return_exceptions=True
            )
            
        except Exception as e:
            self.logger.warning(f"Ошибка создания связанных сущностей: {str(e)}")

    async def _batch_create_judges(self, judge_names: List[str]) -> None:
        """Массовое создание судей"""
        if not judge_names:
            return
            
        new_judges = [name for name in judge_names if name not in self.cache.judges_cache]
        if not new_judges:
            return
            
        async with self._judges_lock:
            try:
                async with self.db_pool.acquire() as conn:
                    results = await conn.fetch(
                        "INSERT INTO judges (full_name) SELECT unnest($1::text[]) "
                        "ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name "
                        "RETURNING id, full_name",
                        new_judges
                    )
                    
                    for row in results:
                        self.cache.judges_cache[row['full_name']] = row['id']
                        
            except Exception as e:
                self.logger.warning(f"Ошибка создания судей ({len(new_judges)} шт): {str(e)}")

    async def _batch_create_parties(self, party_names: List[str]) -> None:
        """Массовое создание сторон"""
        if not party_names:
            return
            
        new_parties = [name for name in party_names if name not in self.cache.parties_cache]
        if not new_parties:
            return
            
        async with self._parties_lock:
            try:
                async with self.db_pool.acquire() as conn:
                    results = await conn.fetch(
                        "INSERT INTO parties (name) SELECT unnest($1::text[]) "
                        "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name "
                        "RETURNING id, name",
                        new_parties
                    )
                    
                    for row in results:
                        self.cache.parties_cache[row['name']] = row['id']
                        
            except Exception as e:
                self.logger.warning(f"Ошибка создания сторон ({len(new_parties)} шт): {str(e)}")

    async def _batch_create_event_types(self, event_types: List[str]) -> None:
        """Массовое создание типов событий"""
        if not event_types:
            return
            
        new_types = [name for name in event_types if name not in self.cache.event_types_cache]
        if not new_types:
            return
            
        async with self._events_lock:
            try:
                async with self.db_pool.acquire() as conn:
                    results = await conn.fetch(
                        "INSERT INTO event_types (name) SELECT unnest($1::text[]) "
                        "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name "
                        "RETURNING id, name",
                        new_types
                    )
                    
                    for row in results:
                        self.cache.event_types_cache[row['name']] = row['id']
                        
            except Exception as e:
                self.logger.warning(f"Ошибка создания типов событий ({len(new_types)} шт): {str(e)}")

    async def _ensure_all_entities_exist_atomic(self, conn: asyncpg.Connection, 
                                              case_data: Dict[str, Any]) -> None:
        """Гарантированное создание всех связанных сущностей в одной транзакции"""
        entities_to_create = {
            'judges': set(),
            'parties': set(),
            'event_types': set()
        }
        
        # Собираем все сущности
        judge_name = self.text_processor.clean_text(case_data.get('judge') or '')
        if judge_name and len(judge_name) <= 200:
            entities_to_create['judges'].add(judge_name)
        
        for party_list in [case_data.get('plaintiffs', []), case_data.get('defendants', [])]:
            for party_name in party_list:
                party_name = self.text_processor.clean_text(party_name or '')
                if party_name and len(party_name) <= 500:
                    entities_to_create['parties'].add(party_name)
        
        for event_item in case_data.get('events', []):
            event_type = self.text_processor.clean_text(event_item.get('event_type') or '')
            if event_type and len(event_type) <= 300:
                entities_to_create['event_types'].add(event_type)
        
        # Создаем все сущности атомарно
        try:
            # Судьи
            if entities_to_create['judges']:
                for judge_name in entities_to_create['judges']:
                    if judge_name not in self.cache.judges_cache:
                        judge_id = await conn.fetchval(
                            "INSERT INTO judges (full_name) VALUES ($1) "
                            "ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name "
                            "RETURNING id",
                            judge_name
                        )
                        if judge_id:
                            self.cache.judges_cache[judge_name] = judge_id
            
            # Стороны
            if entities_to_create['parties']:
                for party_name in entities_to_create['parties']:
                    if party_name not in self.cache.parties_cache:
                        party_id = await conn.fetchval(
                            "INSERT INTO parties (name) VALUES ($1) "
                            "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name "
                            "RETURNING id",
                            party_name
                        )
                        if party_id:
                            self.cache.parties_cache[party_name] = party_id
            
            # События
            if entities_to_create['event_types']:
                for event_type in entities_to_create['event_types']:
                    if event_type not in self.cache.event_types_cache:
                        event_id = await conn.fetchval(
                            "INSERT INTO event_types (name) VALUES ($1) "
                            "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name "
                            "RETURNING id",
                            event_type
                        )
                        if event_id:
                            self.cache.event_types_cache[event_type] = event_id
                            
        except Exception as e:
            self.logger.error(f"Ошибка атомарного создания сущностей: {str(e)}")
            raise

    async def _verify_case_integrity(self, conn: asyncpg.Connection, 
                                   case_id: int, case_data: Dict[str, Any]) -> None:
        """Проверка целостности сохраненного дела"""
        case_number = case_data.get('case_number', 'UNKNOWN')
        
        try:
            # Проверяем основные данные дела
            case_info = await conn.fetchrow(
                "SELECT case_number, judge_id FROM cases WHERE id = $1", 
                case_id
            )
            
            if not case_info:
                raise DatabaseError(f"Дело {case_id} исчезло после сохранения")
            
            # Проверяем связь с судьей
            judge_name = self.text_processor.clean_text(case_data.get('judge') or '')
            if judge_name and not case_info['judge_id']:
                self.logger.warning(f"У дела {case_number} отсутствует связь с судьей")
                
                # Пытаемся восстановить связь
                judge_id = self.cache.judges_cache.get(judge_name)
                if judge_id:
                    await conn.execute(
                        "UPDATE cases SET judge_id = $1 WHERE id = $2", 
                        judge_id, case_id
                    )
                    self.logger.debug(f"Восстановлена связь с судьей для дела {case_number}")
            
            # Проверяем связи со сторонами
            parties_count = await conn.fetchval(
                "SELECT COUNT(*) FROM case_parties WHERE case_id = $1", 
                case_id
            )
            expected_parties = len(case_data.get('plaintiffs', [])) + len(case_data.get('defendants', []))
            
            if parties_count < expected_parties:
                self.logger.warning(f"У дела {case_number} не хватает связей со сторонами: "
                                  f"ожидалось {expected_parties}, найдено {parties_count}")
            
        except Exception as e:
            self.logger.error(f"Ошибка проверки целостности дела {case_number}: {str(e)}")

    async def _update_existing_case_safe(self, case_data: Dict[str, Any]) -> None:
        """Безопасное обновление существующего дела"""
        case_number = case_data.get('case_number', '')
        
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                # Проверяем есть ли судья у существующего дела
                existing_judge_id = await conn.fetchval(
                    "SELECT judge_id FROM cases WHERE case_number = $1",
                    case_number
                )
                
                judge_name = self.text_processor.clean_text(case_data.get('judge') or '')
                
                # Если у дела нет судьи, но есть информация о судье - добавляем
                if not existing_judge_id and judge_name:
                    judge_id = self.cache.judges_cache.get(judge_name)
                    if not judge_id:
                        judge_id = await conn.fetchval(
                            "INSERT INTO judges (full_name) VALUES ($1) "
                            "ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name "
                            "RETURNING id",
                            judge_name
                        )
                        if judge_id:
                            self.cache.judges_cache[judge_name] = judge_id
                    
                    if judge_id:
                        await conn.execute(
                            "UPDATE cases SET judge_id = $1, updated_at = CURRENT_TIMESTAMP "
                            "WHERE case_number = $2",
                            judge_id, case_number
                        )
                        self.logger.debug(f"Добавлен судья к существующему делу {case_number}")
                
                # Простое обновление метки времени
                await conn.execute(
                    "UPDATE cases SET updated_at = CURRENT_TIMESTAMP "
                    "WHERE case_number = $1",
                    case_number
                )

    async def _save_single_case_safe(self, case_data: Dict[str, Any]) -> str:
        """Максимально безопасное сохранение одного дела"""
        case_number = case_data.get('case_number', 'UNKNOWN')
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                async with self.db_pool.acquire() as conn:
                    # Одна большая транзакция для всего дела
                    async with conn.transaction():
                        await self._ensure_cache_loaded(conn)
                        
                        # Предварительно создаем ВСЕ связанные сущности
                        await self._ensure_all_entities_exist_atomic(conn, case_data)
                        
                        # Сохраняем дело
                        case_id = await self._save_case_with_validation(conn, case_data)
                        
                        if case_id:
                            # Сохраняем связи в той же транзакции
                            await self._save_case_relations_safe(conn, case_id, case_data)
                            
                            # Финальная проверка целостности
                            await self._verify_case_integrity(conn, case_id, case_data)
                            
                            return 'saved'
                        else:
                            return 'skipped'
                            
            except asyncpg.UniqueViolationError:
                try:
                    await self._update_existing_case_safe(case_data)
                    return 'updated'
                except Exception:
                    return 'skipped'
                    
            except (asyncpg.ConnectionDoesNotExistError, asyncpg.InterfaceError) as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Проблема с подключением к БД для дела {case_number}, попытка {attempt + 1}")
                    await asyncio.sleep(1)
                    continue
                else:
                    self.logger.error(f"Не удалось подключиться к БД для дела {case_number}: {str(e)}")
                    return 'skipped'
                    
            except Exception as e:
                self.logger.error(f"Ошибка сохранения дела {case_number}: {str(e)}")
                return 'skipped'
        
        return 'skipped'

    async def _save_case_with_validation(self, conn: asyncpg.Connection, 
                                       case_data: Dict[str, Any]) -> Optional[int]:
        """Атомарное сохранение дела с обязательной проверкой судьи"""
        case_number = case_data.get('case_number', '').strip()
        if not case_number or len(case_number) > 100:
            raise ValueError(f"Некорректный номер дела: {case_number}")
        
        fields = ['case_number']
        values = [case_number]
        
        # Обрабатываем дату
        if case_data.get('case_date'):
            try:
                case_date_str = str(case_data['case_date']).strip() 
                if case_date_str and case_date_str not in ['None', '']:
                    parsed_date = self.text_processor.parse_date(case_date_str)
                    if parsed_date and 1990 <= parsed_date.year <= 2030:
                        fields.append('case_date')
                        values.append(parsed_date.date())
            except Exception as e:
                self.logger.debug(f"Ошибка парсинга даты для дела {case_number}: {str(e)}")
        
        # Обязательная проверка и создание судьи ВНУТРИ транзакции
        judge_name = self.text_processor.clean_text(case_data.get('judge') or '')
        judge_id = None
        
        if judge_name and len(judge_name) <= 200:
            # Сначала пытаемся найти в кеше
            judge_id = self.cache.judges_cache.get(judge_name)
            
            # Если не в кеше, ищем/создаем в БД атомарно
            if not judge_id:
                judge_id = await conn.fetchval(
                    """
                    INSERT INTO judges (full_name) 
                    VALUES ($1) 
                    ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name 
                    RETURNING id
                    """,
                    judge_name
                )
                # Обновляем кеш
                if judge_id:
                    self.cache.judges_cache[judge_name] = judge_id
            
            # Дополнительная проверка существования
            if judge_id:
                exists = await conn.fetchval("SELECT 1 FROM judges WHERE id = $1", judge_id)
                if exists:
                    fields.append('judge_id')
                    values.append(judge_id)
                else:
                    self.logger.critical(f"Судья с ID {judge_id} исчез из БД для дела {case_number}")
                    judge_id = None
        
        # Логируем дела без судьи для контроля
        if not judge_id and judge_name:
            self.logger.warning(f"Дело {case_number} сохраняется без судьи: {judge_name}")
        
        try:
            fields_sql = ', '.join(fields)
            placeholders = ', '.join([f'${i+1}' for i in range(len(values))])
            
            if len(fields) > 1:
                update_fields = [f'{field} = EXCLUDED.{field}' for field in fields[1:]]
                update_fields.append('updated_at = CURRENT_TIMESTAMP')
                update_sql = ', '.join(update_fields)
                
                sql = f"""
                    INSERT INTO cases ({fields_sql}) 
                    VALUES ({placeholders}) 
                    ON CONFLICT (case_number) DO UPDATE SET {update_sql}
                    RETURNING id
                """
            else:
                sql = f"""
                    INSERT INTO cases ({fields_sql}) 
                    VALUES ({placeholders}) 
                    ON CONFLICT (case_number) DO NOTHING
                    RETURNING id
                """
            
            case_id = await conn.fetchval(sql, *values)
            
            # Дополнительное обновление judge_id если дело уже существовало
            if case_id and judge_id and 'judge_id' in fields:
                await conn.execute(
                    "UPDATE cases SET judge_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND judge_id IS NULL",
                    judge_id, case_id
                )
            
            return case_id
            
        except Exception as e:
            self.logger.error(f"Ошибка SQL при сохранении дела {case_number}: {str(e)}")
            raise

    async def _save_case_relations_safe(self, conn: asyncpg.Connection, 
                                  case_id: int, case_data: Dict[str, Any]) -> None:
        """Безопасное сохранение связей с атомарным сохранением событий"""
        case_number = case_data.get('case_number', 'UNKNOWN')
        
        # Сохраняем стороны (существующий код без изменений)
        try:
            for role, parties_list in [('plaintiff', case_data.get('plaintiffs', [])), 
                                    ('defendant', case_data.get('defendants', []))]:
                for party_name in parties_list:
                    party_name = self.text_processor.clean_text(party_name or '')
                    if party_name and len(party_name) <= 500:
                        party_id = self.cache.parties_cache.get(party_name)
                        if party_id:
                            # Дополнительная проверка существования
                            exists = await conn.fetchval("SELECT 1 FROM parties WHERE id = $1", party_id)
                            if exists:
                                await conn.execute(
                                    "INSERT INTO case_parties (case_id, party_id, party_role) "
                                    "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                                    case_id, party_id, role
                                )
                            else:
                                self.logger.warning(f"Сторона с ID {party_id} не найдена для дела {case_number}")
                        
        except Exception as e:
            self.logger.warning(f"Ошибка сохранения сторон дела {case_number}: {str(e)}")
        
        # Атомарное сохранение всех событий одним запросом
        try:
            events = case_data.get('events', [])
            if events:
                # Подготавливаем все события для batch insert
                events_batch = []
                for event_item in events:
                    event_type = self.text_processor.clean_text(event_item.get('event_type') or '')
                    event_date = event_item.get('event_date')
                    
                    if event_type and event_date and len(event_type) <= 300:
                        event_type_id = self.cache.event_types_cache.get(event_type)
                        if event_type_id:
                            # Дополнительная проверка существования
                            exists = await conn.fetchval("SELECT 1 FROM event_types WHERE id = $1", event_type_id)
                            if exists:
                                events_batch.append((case_id, event_type_id, event_date))
                            else:
                                self.logger.warning(f"Тип события с ID {event_type_id} не найден для дела {case_number}")
                
                # Сохраняем все события одним запросом (все или ничего)
                if events_batch:
                    await conn.executemany(
                        "INSERT INTO case_events (case_id, event_type_id, event_date) "
                        "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        events_batch
                    )
                    
                    self.logger.debug(f"Сохранено {len(events_batch)} событий для дела {case_number}")
                                
        except Exception as e:
            self.logger.error(f"Ошибка сохранения событий дела {case_number}: {str(e)}")
            # Важно: НЕ проглатываем ошибку - пробрасываем выше для отката транзакции
            raise

    async def _ensure_cache_loaded(self, conn: asyncpg.Connection) -> None:
        """Убедиться что кеш загружен"""
        if not self.cache.is_loaded():
            await self._load_all_caches(conn)
            self.cache.mark_loaded()
            self.logger.debug("Кеш БД загружен")
    
    async def _load_all_caches(self, conn: asyncpg.Connection) -> None:
        """Загрузить все кеши из БД"""
        judges = await conn.fetch(SQLQueries.SELECT_ALL_JUDGES)
        for judge in judges:
            self.cache.judges_cache[judge['full_name']] = judge['id']
        
        parties = await conn.fetch(SQLQueries.SELECT_ALL_PARTIES)
        for party in parties:
            self.cache.parties_cache[party['name']] = party['id']
        
        events = await conn.fetch(SQLQueries.SELECT_ALL_EVENT_TYPES)
        for event in events:
            self.cache.event_types_cache[event['name']] = event['id']
    
    async def get_prefix_statistics(self, pattern_like: str, 
                                  pattern_regex: str) -> Dict[str, Any]:
        """Получить статистику по префиксу"""
        async with self.db_pool.acquire() as conn:
            stats_query = """
                SELECT 
                    COUNT(*) as total_cases,
                    MIN(CAST(SPLIT_PART(case_number, '/', 2) AS INTEGER)) as min_number,
                    MAX(CAST(SPLIT_PART(case_number, '/', 2) AS INTEGER)) as max_number
                FROM cases 
                WHERE case_number LIKE $1 AND case_number ~ $2
            """
            result = await conn.fetchrow(stats_query, pattern_like, pattern_regex)
            
            return {
                'total_cases': result['total_cases'] if result else 0,
                'min_number': (
                    result['min_number'] 
                    if result and result['min_number'] else 0
                ),
                'max_number': (
                    result['max_number'] 
                    if result and result['max_number'] else 0
                )
            }
        
    async def get_missing_numbers(self, pattern_like: str, pattern_regex: str, 
                                 start_num: int, end_num: int, 
                                 limit: int = 100) -> List[int]:
        """Получить пропущенные номера для региона/суда"""
        
        # Убеждаемся что параметры - целые числа
        start_num = int(start_num)
        end_num = int(end_num)
        
        async with self.db_pool.acquire() as conn:
            result = await conn.fetch(
                SQLQueries.GET_MISSING_NUMBERS, 
                pattern_like, pattern_regex, start_num, end_num
            )
            return [row['missing_number'] for row in result]

    async def get_cases_without_judges(self, case_pattern: str = '-00-4/', 
                            limit: int = 1000) -> List[str]:
        """ОСТАВЛЯЕМ: Глобальный метод для получения дел СМАС без судей"""
        async with self.db_pool.acquire() as conn:
            if case_pattern == '-00-4/':
                # ТОЧНАЯ проверка: дело ДОЛЖНО быть СМАС (заканчиваться на -00-4/номер)
                query = """
                    SELECT case_number
                    FROM cases 
                    WHERE judge_id IS NULL 
                    AND case_number ~ '^[0-9]+-[0-9]+-00-4/[0-9]+$'
                    ORDER BY 
                        CAST(SPLIT_PART(case_number, '-', 2) AS INTEGER),
                        CAST(SPLIT_PART(case_number, '/', 2) AS INTEGER)
                    LIMIT $1
                """
                result = await conn.fetch(query, limit)
                
                # ДОПОЛНИТЕЛЬНАЯ проверка на клиенте для 100% гарантии
                verified_cases = []
                for row in result:
                    case_num = row['case_number']
                    # Строгая проверка: цифры-цифры-00-4/цифры
                    if re.match(r'^[0-9]+-[0-9]+-00-4/[0-9]+$', case_num):
                        verified_cases.append(case_num)
                
                self.logger.debug(f"✅ Найдено {len(verified_cases)} дел СМАС БЕЗ судей")
                return verified_cases
            else:
                # Для других паттернов - пустой список
                return []

# ============================================================================
# МЕНЕДЖЕР КОНФИГУРАЦИЙ
# ============================================================================

class ConfigManager:
    """Менеджер конфигурации с поддержкой режимов парсинга"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.parsing_modes = config.get('parsing_modes', {})
        self.logger = get_logger('config_manager')
    
    def get_mode_config(self, mode_name: str) -> Optional[Dict[str, Any]]:
        """Получить конфигурацию для режима"""
        if mode_name not in self.parsing_modes:
            self.logger.error(f"Режим {mode_name} не найден")
            return None
        
        mode_config = self.parsing_modes[mode_name].copy()
        return mode_config
    
    def list_available_modes(self) -> List[str]:
        """Получить список доступных режимов"""
        return list(self.parsing_modes.keys())
    
    def get_mode_description(self, mode_name: str) -> str:
        """Получить описание режима"""
        mode_config = self.parsing_modes.get(mode_name, {})
        return mode_config.get('description', 'Описание недоступно')
    
    def validate_mode_config(self, mode_name: str) -> bool:
        """Валидация конфигурации режима"""
        mode_config = self.get_mode_config(mode_name)
        if not mode_config:
            return False
        
        # Проверка в зависимости от типа режима
        if mode_name in ['mass_regions', 'mass_custom']:
            regions = mode_config.get('target_regions', [])
            for region in regions:
                if region not in self.config['regions']:
                    self.logger.error(f"Неверный регион {region} в режиме {mode_name}")
                    return False
        
        if mode_name in ['single_case']:
            if not mode_config.get('full_case_number'):
                self.logger.error(f"Отсутствует номер дела в режиме {mode_name}")
                return False
        
        if mode_name in ['multiple_cases', 'range_search']:
            region_key = mode_config.get('region_key')
            court_key = mode_config.get('court_key')
            
            if not region_key or region_key not in self.config['regions']:
                self.logger.error(f"Неверный ключ региона {region_key} в режиме {mode_name}")
                return False
            
            if (not court_key or 
                court_key not in self.config['regions'][region_key]['courts']):
                self.logger.error(f"Неверный ключ суда {court_key} в режиме {mode_name}")
                return False
        
        return True

# ============================================================================
# ПАРСЕР РЕЗУЛЬТАТОВ
# ============================================================================

class ResultsParser:
    """Парсер результатов поиска"""
    
    NO_RESULTS_MESSAGES = [
        "По указанным данным ничего не найдено", 
        "Көрсетілген деректер бойына ешнәрсе табылмады"
    ]
    
    def __init__(self, text_processor: TextProcessor):
        self.text_processor = text_processor
        self.logger = get_logger('results_parser')
        self._html_cache = {}
        self._max_cache_size = 100
    
    def parse_search_results(self, html: str, 
                           filter_by_case_number: Optional[str] = None) -> List[Dict[str, Any]]:
        """Парсинг результатов поиска с кешированием"""
        # Простое кеширование по хешу HTML
        cache_key = hash(html + str(filter_by_case_number))
        
        if cache_key in self._html_cache:
            return self._html_cache[cache_key].copy()
        
        results = self._parse_search_results_internal(html, filter_by_case_number)
        
        # Ограничиваем размер кеша
        if len(self._html_cache) >= self._max_cache_size:
            # Удаляем старейший элемент (простая FIFO стратегия)
            oldest_key = next(iter(self._html_cache))
            del self._html_cache[oldest_key]
        
        self._html_cache[cache_key] = results.copy()
        return results
    
    def _parse_search_results_internal(self, html: str, 
                                     filter_by_case_number: Optional[str] = None) -> List[Dict[str, Any]]:
        """Внутренний метод парсинга результатов поиска"""
        parser = HTMLParser(html)
        
        if self._is_no_results(parser):
            return []
        
        table = parser.css_first('table')
        if not table:
            self.logger.warning("Таблица результатов не найдена")
            return []
        
        results = self._parse_table_rows(table)
        
        # Фильтрация по точному совпадению номера дела (для пропущенных номеров)
        if filter_by_case_number:
            filtered_results = []
            original_count = len(results)
            
            for result in results:
                result_case_number = result.get('case_number', '')
                if result_case_number == filter_by_case_number:
                    filtered_results.append(result)
            
            # Логирование фильтрации для диагностики
            if original_count > len(filtered_results):
                filtered_out = original_count - len(filtered_results)
                self.logger.debug(f"Отфильтровано {filtered_out} из {original_count} результатов для {filter_by_case_number}")
            
            return filtered_results
        
        self.logger.debug(f"Распарсено результатов: {len(results)}")
        return results
    
    def extract_sequence_numbers_from_results(self, 
                                            results: List[Dict[str, Any]]) -> List[int]:
        """Извлечь порядковые номера из результатов поиска"""
        sequence_numbers = []
        
        for result in results:
            case_number = result.get('case_number', '')
            sequence_num = (
                self.text_processor.extract_sequence_number_from_case(
                    case_number
                )
            )
            if sequence_num is not None:
                sequence_numbers.append(sequence_num)
        
        return sequence_numbers
    
    def _is_no_results(self, parser: HTMLParser) -> bool:
        """Проверить наличие сообщения об отсутствии результатов"""
        content = parser.css_first('.tab__inner-content')
        if not content:
            return True
            
        content_text = content.text() 
        return any(text in content_text for text in self.NO_RESULTS_MESSAGES)
    
    def _parse_table_rows(self, table) -> List[Dict[str, Any]]:
        """Парсинг строк таблицы"""
        rows = table.css('tbody tr')
        results = []
        
        for row_idx, row in enumerate(rows):
            try:
                result = self._parse_single_row(row, row_idx)
                if result:
                    results.append(result)  
            except Exception as e:
                self.logger.error(f"Ошибка парсинга строки {row_idx + 1}: {str(e)}")
        
        return results
    
    def _parse_single_row(self, row, row_idx: int) -> Optional[Dict[str, Any]]:
        """Парсинг одной строки таблицы"""
        cells = row.css('td')
        if len(cells) < 4:
            return None
        
        case_number, case_date = (
            self.text_processor.extract_case_info_from_cell(cells[0])
        )
        if not case_number:
            return None
        
        plaintiffs, defendants = (
            self.text_processor.extract_parties_from_cell(cells[1])
        )
        judge_name = self.text_processor.extract_judge_from_cell(cells[2])
        events = self.text_processor.extract_events_from_cell(
            cells[3], case_number
        )
        
        return {
            'case_number': case_number,
            'case_date': case_date,
            'judge': judge_name,
            'plaintiffs': plaintiffs,
            'defendants': defendants,
            'events': events
        }

# ============================================================================
# ПОИСКОВЫЙ ДВИЖОК
# ============================================================================

class SearchEngine:
    """Движок для выполнения поисковых запросов"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('search_engine')
        self.retry_config = {
            'max_retries': 5,
            'backoff_factor': 2.0,
            'retry_on_status': [500, 502, 503, 504, 429]
        }
    
    async def select_region(self, session: aiohttp.ClientSession, 
                          viewstate: str, region_id: str, 
                          form_ids: Dict[str, str]) -> None:
        """Выбор региона"""
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('edit-district_base', 'j_idt45:j_idt46')
        
        data = self._build_region_select_data(form_base, region_id, viewstate)
        headers = HTTPHeaders.get_ajax_headers()
        headers['Referer'] = f'{self.base_url}/form/lawsuit/index.xhtml'
        
        start_time = time.time()
        async with session.post(url, data=data, headers=headers) as response:
            duration_ms = int((time.time() - start_time) * 1000)
            
            if response.status != 200:
                raise ParseError(f"Ошибка выбора региона: HTTP {response.status}")
            
            await response.text()
    
    async def search_case(self, session: aiohttp.ClientSession, 
                         viewstate: str, region_id: str, court_id: str, 
                         year: str, case_number: str,
                         form_ids: Dict[str, str]) -> str:
        """Поиск дела с retry логикой"""
        return await self.search_case_with_retry(
            session, viewstate, region_id, court_id, 
            year, case_number, form_ids
        )
    
    async def search_case_with_retry(self, session: aiohttp.ClientSession, 
                               viewstate: str, region_id: str, 
                               court_id: str, year: str, case_number: str,
                               form_ids: Dict[str, str]) -> str:
        """Поиск с повторными попытками при ошибках и специальной обработкой 502"""
        
        last_502_error = False
        
        for attempt in range(self.retry_config['max_retries']):
            try:
                await self._send_search_request(
                    session, viewstate, region_id, court_id, 
                    year, case_number, form_ids
                )
                
                # Если была 502 ошибка на предыдущих попытках, добавляем задержку
                if last_502_error and attempt > 0:
                    extra_delay = min(2.0 * (2 ** (attempt - 1)), 10.0)
                    self.logger.debug(f"Дополнительная задержка {extra_delay}с после 502")
                    await asyncio.sleep(extra_delay)
                
                return await self._get_search_results(session)
                
            except aiohttp.ClientError as e:
                error_str = str(e)
                is_502_error = '502' in error_str or 'Bad Gateway' in error_str
                
                if is_502_error:
                    last_502_error = True
                    log_error(self.logger, f"502 ошибка сервера для дела {case_number}", attempt + 1, self.retry_config['max_retries'])
                
                if attempt == self.retry_config['max_retries'] - 1:
                    # Последняя попытка - бросаем исключение
                    raise
                
                # Вычисляем задержку в зависимости от типа ошибки
                if is_502_error:
                    # Для 502: агрессивная экспоненциальная задержка
                    wait_time = min(
                        ParsingConfig.DELAY_AFTER_502 * (2 ** attempt),
                        30.0  # Максимум 30 секунд
                    )
                else:
                    # Для других ошибок: обычная задержка
                    wait_time = self.retry_config['backoff_factor'] * (2 ** attempt)
                
                self.logger.warning(f"Повтор через {wait_time}с (попытка {attempt + 1}/{self.retry_config['max_retries']})")
                
                await asyncio.sleep(wait_time)
        
        raise ParseError("Превышено максимальное количество попыток")
    
    async def _send_search_request(self, session: aiohttp.ClientSession, 
                                 viewstate: str, region_id: str, court_id: str, 
                                 year: str, case_number: str,
                                 form_ids: Dict[str, str]) -> None:
        """Отправить поисковый запрос"""
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('edit-district_base', 'j_idt45:j_idt46')
        
        data = self._build_search_data(
            form_base, region_id, court_id, year, 
            case_number, viewstate, form_ids
        )
        headers = HTTPHeaders.get_ajax_headers()
        headers['Referer'] = f'{self.base_url}/form/lawsuit/'
        
        start_time = time.time()
        async with session.post(url, data=data, headers=headers) as response:
            duration_ms = int((time.time() - start_time) * 1000)
            
            if hasattr(self, 'request_monitor'):
                await self.request_monitor.record_request(
                    url, response.status, duration_ms, 'SEARCH'
                )

            if response.status != 200:
                raise ParseError(f"Ошибка отправки поискового запроса: HTTP {response.status}")
            
            await response.text()
    
    async def _get_search_results(self, session: aiohttp.ClientSession) -> str:
        """Получить результаты поиска"""
        results_url = f"{self.base_url}/lawsuit/lawsuitList.xhtml"
        headers = HTTPHeaders.get_base_headers()
        headers['Referer'] = f'{self.base_url}/form/lawsuit/'
        
        start_time = time.time()
        async with session.get(results_url, headers=headers) as response:
            duration_ms = int((time.time() - start_time) * 1000)
            
            if response.status != 200:
                raise ParseError(f"Ошибка получения результатов: HTTP {response.status}")
            
            return await response.text()
    
    def _build_region_select_data(self, form_base: str, region_id: str, viewstate: str) -> Dict[str, str]:
        """Построить данные для выбора региона"""
        return {
            f'{form_base}': form_base,
            f'{form_base}:edit-district': region_id,
            f'{form_base}:edit-district-hide': '',
            f'{form_base}:edit-court': '',
            f'{form_base}:edit-year': '',
            f'{form_base}:edit-iin': '',
            f'{form_base}:edit-num': '',
            f'{form_base}:edit-fio': '',
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': f'{form_base}:edit-district',
            'javax.faces.partial.event': 'change',
            'javax.faces.partial.execute': (
                f'{form_base}:edit-district @component'
            ),
            'javax.faces.partial.render': '@component',
            'javax.faces.behavior.event': 'change',
            'org.richfaces.ajax.component': f'{form_base}:edit-district',
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
    
    def _build_search_data(self, form_base: str, region_id: str, 
                         court_id: str, year: str, case_number: str, 
                         viewstate: str, form_ids: Dict[str, str]) -> Dict[str, str]:
        """ Построить данные для поиска"""
        search_script_id = form_ids.get(
            'search_script_id', f'{form_base}:j_idt83'
        )
        
        return {
            f'{form_base}': form_base,
            f'{form_base}:edit-district': region_id,
            f'{form_base}:edit-district-hide': region_id,
            f'{form_base}:edit-court': court_id,
            f'{form_base}:edit-year': year,
            f'{form_base}:edit-iin': '',
            f'{form_base}:edit-num': case_number,
            f'{form_base}:edit-fio': '',
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': search_script_id,
            'javax.faces.partial.execute': f'{search_script_id} @component',
            'javax.faces.partial.render': '@component',
            'param1': f'{form_base}:edit-num',
            'org.richfaces.ajax.component': search_script_id,
            search_script_id: search_script_id,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }

# ============================================================================
# ОБНОВЛЯТОР ИСТОРИИ ДЕЛ
# ============================================================================
class CaseHistoryUpdater:
    """Обновлятор истории судебных дел с фильтрацией и сравнением событий"""
    
    def __init__(self, parser: 'CourtParser', config: Dict[str, Any]):
        self.parser = parser
        self.config = config
        self.filters = config.get('filters', {})
        self.batch_size = config.get('batch_size', 50) 
        self.delay_between_requests = config.get('delay_between_requests', 2.0)
        self.enable_statistics = config.get('enable_statistics', True)
        self.max_cases_to_process = self.filters.get('max_cases_to_process', 1000)
        
        self.logger = get_logger('case_history_updater')
        
        # ИСПРАВЛЕНО: Правильная инициализация stats
        self.stats = {
            'found_cases': 0,
            'processed_cases': 0,
            'updated_cases': 0,
            'no_updates_needed': 0,
            'errors': 0,
            'start_time': None,
            'batch_count': 0
        }
        
    async def run_history_update(self, year: Optional[str] = None) -> Dict[str, int]:
        """Основной метод обновления истории дел"""
        
        # ИСПРАВЛЕНО: Безопасное получение года
        target_year = year
        if not target_year:
            target_year = self.filters.get('year')
        if not target_year:
            target_year = self.parser.config['search_parameters']['default_year']
        
        # Убеждаемся что year - строка
        target_year = str(target_year)
        
        self.stats['start_time'] = datetime.now()
        
        self.logger.info(f"🔄 Запуск обновления истории дел за {target_year} год")
        self.logger.info(f"📋 Настройки: батч {self.batch_size}, максимум дел {self.max_cases_to_process}")
        
        try:
            # Получаем целевые дела из БД с фильтрацией
            target_cases = await self._get_filtered_cases(target_year)
            
            if not target_cases:
                self.logger.info("✅ Дела для обновления не найдены")
                return self.stats
            
            self.stats['found_cases'] = len(target_cases)
            self.logger.info(f"🎯 Найдено {len(target_cases)} дел для проверки обновлений")
            
            # Обрабатываем дела батчами
            await self._process_cases_in_batches(target_cases, target_year)
            
            # Финальная статистика
            await self._log_final_statistics()
            
            return self.stats
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка обновления истории: {str(e)}")
            # ИСПРАВЛЕНО: Добавляем подробности ошибки
            import traceback
            self.logger.error(f"Трассировка: {traceback.format_exc()}")
            
            self.stats['errors'] += 1
            raise
    
    async def _get_filtered_cases(self, year: str) -> List[Dict[str, Any]]:
        """Получение отфильтрованных дел из БД с поддержкой гибких фильтров"""
        
        defendant_keywords = self.filters.get('defendant_keywords', [])
        excluded_statuses = self.filters.get('excluded_statuses', [])
        
        if not defendant_keywords:
            self.logger.warning("⚠️ Не заданы ключевые слова для ответчиков")
            return []
        
        # ИСПРАВЛЕНО: Безопасная обработка года
        try:
            year_int = int(year)
            year_short = str(year_int)[-2:]  # Берем последние 2 цифры
        except (ValueError, TypeError):
            self.logger.error(f"❌ Неверный формат года: {year}")
            return []
        
        year_pattern = f"^[0-9]+-{year_short}-[0-9]+-[0-9]+/[0-9]+$"
        
        # Строим условия для ключевых слов ответчиков (поиск корней слов)
        defendant_conditions = []
        for keyword in defendant_keywords:
            # ИСПРАВЛЕНО: Проверяем что keyword - строка
            if not isinstance(keyword, str):
                continue
            # Ищем корень слова (убираем окончания)
            root = self._extract_word_root(keyword.lower())
            defendant_conditions.append(f"p.name ILIKE '%{root}%'")
        
        if not defendant_conditions:
            self.logger.error("❌ Нет валидных ключевых слов для фильтрации")
            return []
        
        defendant_clause = " OR ".join(defendant_conditions)
        
        # Строим условия исключения статусов с гибкой логикой
        exclusion_conditions = self._build_status_exclusion_conditions(excluded_statuses)
        exclusion_clause = " OR ".join(exclusion_conditions) if exclusion_conditions else "FALSE"
        
        query = f"""
            SELECT DISTINCT
                c.case_number,
                c.id as case_id,
                (SELECT COUNT(*) FROM case_events ce WHERE ce.case_id = c.id) as current_events_count,
                (SELECT j.full_name FROM judges j WHERE j.id = c.judge_id) as judge_name
            FROM cases c
            WHERE EXISTS (
                -- Есть ответчик с ключевыми словами
                SELECT 1 
                FROM case_parties cp 
                JOIN parties p ON cp.party_id = p.id
                WHERE cp.case_id = c.id 
                    AND cp.party_role = 'defendant'
                    AND ({defendant_clause})
            )
            AND c.case_number ~ $1
            AND NOT EXISTS (
                -- ИСКЛЮЧАЕМ дела с финальными статусами
                SELECT 1
                FROM case_events ce2
                JOIN event_types et ON ce2.event_type_id = et.id
                WHERE ce2.case_id = c.id
                    AND ({exclusion_clause})
            )
            ORDER BY c.case_number
            LIMIT $2
        """
        
        self.logger.debug(f"🔍 Фильтры: ключевые слова {defendant_keywords}, "
                         f"исключенные статусы {len(excluded_statuses)} шт.")
        
        async with self.parser.db_pool.acquire() as conn:
            try:
                results = await conn.fetch(query, year_pattern, self.max_cases_to_process)
                
                cases = []
                for row in results:
                    cases.append({
                        'case_number': row['case_number'],
                        'case_id': row['case_id'],
                        'current_events_count': row['current_events_count'],
                        'judge_name': row['judge_name']
                    })
                
                return cases
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка SQL запроса фильтрации: {str(e)}")
                
                # Fallback с упрощенной фильтрацией
                return await self._get_cases_with_simple_filter(conn, year_pattern, defendant_keywords[0])
    
    def _extract_word_root(self, word: str) -> str:
        """Извлечение корня слова (упрощенный алгоритм)"""
        # ИСПРАВЛЕНО: Проверяем входные данные
        if not isinstance(word, str) or not word.strip():
            return word
        
        word = word.strip().lower()
        
        # Удаляем типичные окончания
        endings = ['ный', 'ной', 'ому', 'ого', 'ами', 'ах', 'ов', 'ев', 'ы', 'и', 'а', 'о', 'е']
        
        for ending in sorted(endings, key=len, reverse=True):
            if len(word) > len(ending) + 2 and word.endswith(ending):
                return word[:-len(ending)]
        
        return word
    
    def _build_status_exclusion_conditions(self, excluded_statuses: List[str]) -> List[str]:
        """Построение гибких условий исключения статусов"""
        conditions = []
        
        # ИСПРАВЛЕНО: Проверяем что excluded_statuses - список
        if not isinstance(excluded_statuses, list):
            return conditions
        
        for status in excluded_statuses:
            # ИСПРАВЛЕНО: Проверяем что status - строка
            if not isinstance(status, str) or not status.strip():
                continue
                
            words = status.lower().strip().split()
            
            if len(words) == 1:
                # Одно слово - простой поиск
                conditions.append(f"et.name ILIKE '%{words[0]}%'")
            else:
                # Несколько слов - все слова должны присутствовать
                word_conditions = []
                for word in words:
                    if word.strip():  # Проверяем что слово не пустое
                        word_conditions.append(f"et.name ILIKE '%{word}%'")
                
                if word_conditions:  # Проверяем что есть валидные условия
                    # Объединяем через AND (все слова должны быть)
                    combined_condition = " AND ".join(word_conditions)
                    conditions.append(f"({combined_condition})")
        
        return conditions
    
    async def _get_cases_with_simple_filter(self, conn, year_pattern: str, 
                                          main_keyword: str) -> List[Dict[str, Any]]:
        """Упрощенная фильтрация при ошибке основного запроса"""
        
        self.logger.warning("⚠️ Используется упрощенная фильтрация")
        
        # ИСПРАВЛЕНО: Проверяем main_keyword
        if not isinstance(main_keyword, str) or not main_keyword.strip():
            self.logger.error("❌ Некорректное ключевое слово для упрощенной фильтрации")
            return []
        
        simple_query = """
            SELECT DISTINCT
                c.case_number,
                c.id as case_id,
                0 as current_events_count
            FROM cases c
            JOIN case_parties cp ON c.id = cp.case_id
            JOIN parties p ON cp.party_id = p.id
            WHERE cp.party_role = 'defendant'
                AND p.name ILIKE $1
                AND c.case_number ~ $2
            ORDER BY c.case_number
            LIMIT $3
        """
        
        try:
            results = await conn.fetch(
                simple_query, 
                f"%{main_keyword.strip()}%", 
                year_pattern, 
                self.max_cases_to_process
            )
            
            cases = []
            for row in results:
                cases.append({
                    'case_number': row['case_number'],
                    'case_id': row['case_id'],
                    'current_events_count': 0,  # Будем получать при обработке
                    'judge_name': None
                })
            
            return cases
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка упрощенной фильтрации: {str(e)}")
            return []
    
    async def _process_cases_in_batches(self, target_cases: List[Dict[str, Any]], 
                                      year: str) -> None:
        """Обработка дел батчами с переиспользованием существующих методов"""
        
        total_batches = (len(target_cases) + self.batch_size - 1) // self.batch_size
        
        for batch_num in range(0, len(target_cases), self.batch_size):
            # Проверяем shutdown
            if self.parser.shutdown_manager.shutdown_requested:
                self.logger.info("🛑 Обновление истории прервано по сигналу")
                break
            
            batch = target_cases[batch_num:batch_num + self.batch_size]
            current_batch_num = (batch_num // self.batch_size) + 1
            
            self.logger.info(f"📦 Обрабатываю батч {current_batch_num}/{total_batches} ({len(batch)} дел)")
            
            await self._process_single_batch(batch, year)
            
            self.stats['batch_count'] += 1
            
            # Задержка между батчами
            if current_batch_num < total_batches:  # Не делаем задержку после последнего батча
                await asyncio.sleep(self.delay_between_requests)
    
    async def _process_single_batch(self, batch: List[Dict[str, Any]], year: str) -> None:
        """Обработка одного батча дел"""
        
        # Получаем авторизованную сессию (переиспользуем SessionManager)
        session = await self.parser.session_manager.get_session()
        
        try:
            for case_info in batch:
                # Проверяем shutdown
                if self.parser.shutdown_manager.shutdown_requested:
                    break
                
                try:
                    await self._process_single_case_update(case_info, year, session)
                    self.stats['processed_cases'] += 1
                    
                except Exception as e:
                    case_number = case_info.get('case_number', 'UNKNOWN')
                    self.logger.error(f"❌ Ошибка обработки дела {case_number}: {str(e)}")
                    self.stats['errors'] += 1
                    continue
        
        finally:
            # Возвращаем сессию в пул
            await self.parser.session_manager.return_session(session)
    
    async def _process_single_case_update(self, case_info: Dict[str, Any], 
                                    year: str, session) -> None:
        """Обработка обновления одного дела с правильной логикой"""
        
        case_number = case_info['case_number']
        current_events_count = case_info.get('current_events_count', 0)
        
        # ИСПРАВЛЕНО: Получаем данные с сайта БЕЗ сохранения
        try:
            web_case_data = await self._get_case_data_from_web(case_number, year, session)
            
            if not web_case_data:
                self.logger.debug(f"🔍 Дело {case_number}: данные на сайте не найдены")
                self.stats['no_updates_needed'] += 1
                return
            
            # Сравниваем количество событий
            web_events_count = len(web_case_data.get('events', []))
            
            if web_events_count <= current_events_count:
                self.logger.debug(f"✅ Дело {case_number}: обновление не требуется "
                                f"(БД: {current_events_count}, сайт: {web_events_count})")
                self.stats['no_updates_needed'] += 1
                return
            
            # Есть новые события - сохраняем ТОЛЬКО новые события
            new_events_count = web_events_count - current_events_count
            self.logger.info(f"🔄 Дело {case_number}: найдены новые события "
                        f"(БД: {current_events_count} → сайт: {web_events_count}, "
                        f"добавляем {new_events_count})")
            
            # ИСПРАВЛЕНО: Сохраняем только новые события
            await self._save_updated_case_data(case_info, web_case_data)
            
            self.stats['updated_cases'] += 1
            
            # Показываем примеры первых обновлений
            # if self.stats['updated_cases'] <= 5:
            #     self._log_case_update_example(case_number, current_events_count, web_events_count)
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения данных для дела {case_number}: {str(e)}")
            self.stats['errors'] += 1
            # НЕ пробрасываем ошибку выше - продолжаем обработку других дел
    
    async def _get_case_data_from_web(self, case_number: str, year: str, session) -> Optional[Dict[str, Any]]:
        """Получение данных дела с сайта БЕЗ автоматического сохранения"""
        
        try:
            # Используем существующий TextProcessor для парсинга номера дела
            parsed_data = self.parser.text_processor.parse_full_case_number(case_number)
            if not parsed_data:
                self.logger.warning(f"⚠️ Неверный формат номера дела: {case_number}")
                return None
            
            # Находим регион и суд (переиспользуем existing метод)
            court_code = parsed_data['court_code']
            case_type = parsed_data['case_type']
            sequence_part = parsed_data['sequence']
            
            region_key, court_key = self._find_region_court_by_codes(court_code, case_type)
            
            if not region_key or not court_key:
                self.logger.warning(f"⚠️ Не найден регион/суд для дела {case_number}")
                return None
            
            # ИСПРАВЛЕНО: Используем НОВЫЙ метод который НЕ сохраняет данные
            result = await self.parser.get_case_data_without_saving(
                region_key=region_key,
                court_key=court_key,
                case_number=sequence_part,
                year=year,
                session=session
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения данных с сайта для {case_number}: {str(e)}")
            return None
    
    def _find_region_court_by_codes(self, court_code: str, case_type: str) -> Tuple[Optional[str], Optional[str]]:
        """Поиск региона и суда по кодам (переиспользуем логику из координатора)"""
        
        # Попытка с разными длинами КАТО кода
        for kato_len in [2, 3, 4]:
            if len(court_code) >= kato_len:
                kato_part = court_code[:kato_len]
                instance_part = court_code[kato_len:]
                
                for r_key, region_config in self.parser.config['regions'].items():
                    if region_config.get('kato_code') == kato_part:
                        for c_key, court_config in region_config.get('courts', {}).items():
                            if (court_config.get('instance_code') == instance_part and 
                                court_config.get('case_type_code') == case_type):
                                return r_key, c_key
        
        return None, None
    
    async def _save_updated_case_data(self, case_info: Dict[str, Any], 
                                web_case_data: Dict[str, Any]) -> None:
        """Сохранение ТОЛЬКО новых событий дела"""
        
        try:
            case_number = case_info['case_number']
            case_id = case_info['case_id']
            current_events_count = case_info.get('current_events_count', 0)
            
            # Получаем новые события с сайта
            web_events = web_case_data.get('events', [])
            web_events_count = len(web_events)
            
            if web_events_count <= current_events_count:
                self.logger.debug(f"ℹ️ Дело {case_number}: нет новых событий для сохранения")
                return
            
            # ИСПРАВЛЕНО: Сохраняем только НОВЫЕ события
            new_events_count = web_events_count - current_events_count
            
            # Берем только новые события (последние)
            new_events = web_events[-new_events_count:] if new_events_count > 0 else []
            
            if not new_events:
                self.logger.debug(f"ℹ️ Дело {case_number}: нет новых событий для добавления")
                return
            
            # Сохраняем только новые события напрямую в БД
            async with self.parser.db_pool.acquire() as conn:
                async with conn.transaction():
                    # Убеждаемся что кеш загружен
                    await self.parser.db_manager._ensure_cache_loaded(conn)
                    
                    # Добавляем только новые события
                    for event_item in new_events:
                        event_type = self.parser.text_processor.clean_text(
                            event_item.get('event_type') or ''
                        )
                        event_date = event_item.get('event_date')
                        
                        if event_type and event_date and len(event_type) <= 300:
                            # Получаем или создаем тип события
                            event_type_id = self.parser.db_manager.cache.event_types_cache.get(event_type)
                            
                            if not event_type_id:
                                # Создаем новый тип события
                                event_type_id = await conn.fetchval(
                                    "INSERT INTO event_types (name) VALUES ($1) "
                                    "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name "
                                    "RETURNING id",
                                    event_type
                                )
                                if event_type_id:
                                    self.parser.db_manager.cache.event_types_cache[event_type] = event_type_id
                            
                            if event_type_id:
                                # Добавляем новое событие
                                await conn.execute(
                                    "INSERT INTO case_events (case_id, event_type_id, event_date) "
                                    "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                                    case_id, event_type_id, event_date
                                )
                    
                    self.logger.info(f"✅ Дело {case_number}: добавлено {len(new_events)} новых событий")
                    
        except Exception as e:
            case_number = case_info.get('case_number', 'UNKNOWN')
            self.logger.error(f"❌ Ошибка сохранения новых событий дела {case_number}: {str(e)}")
            raise
    
    def _log_case_update_example(self, case_number: str, old_count: int, new_count: int) -> None:
        """Логирование примера обновления дела"""
        
        self.logger.info(f"📋 ПРИМЕР ОБНОВЛЕНИЯ: {case_number}")
        self.logger.info(f"   • Было событий: {old_count}")
        self.logger.info(f"   • Стало событий: {new_count}")
        self.logger.info(f"   • Добавлено: {new_count - old_count}")
    
    async def _log_final_statistics(self) -> None:
        """Финальная статистика обновления"""
        
        if not self.enable_statistics:
            return
        
        duration = datetime.now() - self.stats['start_time']
        hours = round(duration.total_seconds() / 3600, 1)
        
        success_rate = 0
        if self.stats['processed_cases'] > 0:
            success_rate = (self.stats['updated_cases'] / self.stats['processed_cases']) * 100
        
        self.logger.info(f"📊 ИТОГИ ОБНОВЛЕНИЯ ИСТОРИИ ДЕЛ:")
        self.logger.info(f"   • Найдено дел для проверки: {self.stats['found_cases']}")
        self.logger.info(f"   • Обработано дел: {self.stats['processed_cases']}")
        self.logger.info(f"   • Обновлено дел: {self.stats['updated_cases']}")
        self.logger.info(f"   • Без изменений: {self.stats['no_updates_needed']}")
        self.logger.info(f"   • Ошибок: {self.stats['errors']}")
        self.logger.info(f"   • Батчей обработано: {self.stats['batch_count']}")
        self.logger.info(f"   • Время выполнения: {hours} ч")
        self.logger.info(f"   • Процент обновлений: {round(success_rate, 1)}%")
        
        if self.stats['updated_cases'] > 0:
            avg_time_per_case = duration.total_seconds() / self.stats['processed_cases']
            self.logger.info(f"   • Среднее время на дело: {round(avg_time_per_case, 1)} сек")

# ============================================================================
# СИСТЕМА GRACEFUL SHUTDOWN
# ============================================================================

class GracefulShutdownManager:
    """ИСПРАВЛЕНО: Более надежная обработка сигналов"""
    
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self.shutdown_requested = False
        self.active_tasks = set()
        self.logger = get_logger('shutdown_manager')
        self._signal_handlers_set = False
        self._shutdown_in_progress = False
        
        # Устанавливаем обработчики сигналов
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """ИСПРАВЛЕНО: Более надежная настройка обработчиков сигналов"""
        try:
            # Проверяем что мы в главном потоке
            import threading
            if threading.current_thread() is threading.main_thread():
                def signal_handler(signum, frame):
                    if not self._shutdown_in_progress:
                        self._shutdown_in_progress = True
                        self.logger.info(f"🛑 Получен сигнал {signum} - начинаю корректное завершение")
                        self.shutdown_requested = True
                        
                        # Устанавливаем событие thread-safe способом
                        try:
                            loop = asyncio.get_running_loop()
                            loop.call_soon_threadsafe(self.shutdown_event.set)
                        except RuntimeError:
                            # Event loop не запущен - устанавливаем событие напрямую
                            try:
                                self.shutdown_event.set()
                            except:
                                pass
                
                # Устанавливаем обработчики для разных сигналов
                signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
                signal.signal(signal.SIGTERM, signal_handler)  # Завершение процесса
                
                # Дополнительно - игнорируем SIGPIPE если доступен
                try:
                    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
                except AttributeError:
                    pass  # Windows не поддерживает SIGPIPE
                
                self._signal_handlers_set = True
                self.logger.debug("Обработчики сигналов установлены")
            else:
                self.logger.debug("Не в главном потоке - обработчики сигналов не установлены")
                
        except Exception as e:
            self.logger.debug(f"Не удалось настроить обработчики сигналов: {str(e)}")
    
    def register_task(self, task: asyncio.Task) -> None:
        """Регистрировать активную задачу"""
        self.active_tasks.add(task)
        task.add_done_callback(lambda t: self.active_tasks.discard(t))
    
    async def wait_for_shutdown(self) -> None:
        """ИСПРАВЛЕНО: Корректное ожидание сигнала завершения"""
        if self._signal_handlers_set:
            try:
                await self.shutdown_event.wait()
            except Exception as e:
                self.logger.debug(f"Ошибка ожидания shutdown события: {str(e)}")
        else:
            # Fallback - периодическая проверка флага
            while not self.shutdown_requested:
                await asyncio.sleep(0.1)
                # Добавляем проверку на отмену задачи
                try:
                    # Проверяем не отменена ли текущая задача
                    current_task = asyncio.current_task()
                    if current_task and current_task.cancelled():
                        break
                except Exception:
                    pass
    
    async def shutdown_gracefully(self) -> None:
        """ИСПРАВЛЕНО: Graceful завершение всех активных задач без дублирования логов"""
        if not self.active_tasks:
            return  # Убираем лог, если нет задач
        
        self.logger.info(f"🔄 Завершаю {len(self.active_tasks)} активных задач...")
        
        # Отменяем все активные задачи
        for task in self.active_tasks.copy():
            if not task.done():
                task.cancel()
        
        # Ждем завершения всех задач
        if self.active_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.active_tasks, return_exceptions=True),
                    timeout=30.0
                )
                # ИСПРАВЛЕНО: Один лог вместо множественных
                if len(self.active_tasks) > 1:
                    self.logger.info("✅ Все задачи завершены корректно")
            except asyncio.TimeoutError:
                self.logger.warning("⏰ Тайм-аут ожидания завершения задач")
            except Exception as e:
                self.logger.error(f"Ошибка при завершении задач: {str(e)}")

# ============================================================================
# ОСНОВНОЙ ПАРСЕР С ИСПРАВЛЕННЫМ SHUTDOWN
# ============================================================================

class CourtParser:
    def __init__(self, config_path: Optional[str] = None, 
             global_state_manager: Optional[GlobalParsingStateManager] = None):
        self.config = self._load_config(config_path)
        self.config_manager = ConfigManager(self.config)
        self.base_url = self.config['base_url']
        self.global_state_manager = (
            global_state_manager or GlobalParsingStateManager()
        )
        self.text_processor = TextProcessor()
        self.request_monitor = RequestMonitor(window_size=200)
        self.logger = get_logger('court_parser')
        
        # ИСПРАВЛЕНО: Используем новый менеджер graceful shutdown
        self.shutdown_manager = GracefulShutdownManager()
        
        # ИСПРАВЛЕНО: Принудительно устанавливаем значение из конфига
        config_max_sessions = self.config.get('max_concurrent_sessions')
        if config_max_sessions is not None:
            if isinstance(config_max_sessions, int) and config_max_sessions > 0:
                old_value = ParsingConfig.MAX_CONCURRENT_SESSIONS
                ParsingConfig.MAX_CONCURRENT_SESSIONS = config_max_sessions
                self.logger.info(f"Лимит сессий установлен из конфига: {old_value} -> {config_max_sessions}")
            else:
                self.logger.warning(f"Некорректное значение max_concurrent_sessions в конфиге: {config_max_sessions}, используется по умолчанию: {ParsingConfig.MAX_CONCURRENT_SESSIONS}")
        else:
            self.logger.warning(f"max_concurrent_sessions не найден в конфиге, используется по умолчанию: {ParsingConfig.MAX_CONCURRENT_SESSIONS}")

    @property
    def _shutdown_requested(self) -> bool:
        """Проверка запроса на завершение"""
        return self.shutdown_manager.shutdown_requested
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Загрузка конфигурации"""
        if config_path is None:
            config_path = Path(__file__).parent / 'config.json'
        else:
            config_path = Path(config_path)
        
        if not config_path.exists():
            raise ConfigurationError(
                f"Конфигурационный файл не найден: {config_path}"
            )
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Валидация конфигурации
            ConfigValidator.validate_database_config(config['database'])
            ConfigValidator.validate_parsing_config(config)
            
            return config
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Ошибка в JSON файле {config_path}: {e}"
            )
        except Exception as e:
            raise ConfigurationError(
                f"Ошибка загрузки конфига {config_path}: {e}"
            )
    
    async def __aenter__(self):
        """Асинхронная инициализация"""
        self.logger.debug("Инициализация компонентов парсера")
        await self._initialize_components()
        self.logger.info("Парсер готов к работе")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронное завершение"""
        await self._cleanup_components()
    
    async def _initialize_components(self) -> None:
        """Инициализация с проверкой целостности данных"""
        self.session_manager = SessionManager(self.config)
        self.db_pool = await self._create_db_pool()
        self.db_manager = DatabaseManager(self.db_pool, self.text_processor)
        self.search_engine = SearchEngine(self.base_url)
        self.search_engine.request_monitor = self.request_monitor
        self.results_parser = ResultsParser(self.text_processor)
        
        # Проверка целостности при запуске
        await self._check_data_integrity()

    async def _check_data_integrity(self) -> None:  
        """Проверка и восстановление целостности данных"""
        try:
            async with self.db_pool.acquire() as conn:
                # Проверяем дела без судей
                orphaned_cases = await conn.fetch(
                    """
                    SELECT c.id, c.case_number 
                    FROM cases c 
                    WHERE c.judge_id IS NULL 
                    AND c.created_at > CURRENT_DATE - INTERVAL '7 days'
                    LIMIT 100
                    """
                )
                
                if orphaned_cases:
                    self.logger.warning(f"Найдено {len(orphaned_cases)} дел без судей за последние 7 дней")
                
                # Проверяем общую статистику
                total_cases = await conn.fetchval("SELECT COUNT(*) FROM cases")
                cases_with_judges = await conn.fetchval(
                    "SELECT COUNT(*) FROM cases WHERE judge_id IS NOT NULL"
                )
                
                integrity_percent = (cases_with_judges / total_cases * 100) if total_cases > 0 else 100
                
                self.logger.debug(f"Проверка целостности: {total_cases} всего дел, "
                                f"{cases_with_judges} с судьями ({round(integrity_percent, 1)}%)")
                
                if integrity_percent < 95:
                    self.logger.warning(f"Низкая целостность данных: {round(integrity_percent, 1)}%")
                    
        except Exception as e:
            self.logger.error(f"Ошибка проверки целостности: {str(e)}")
    
    async def _create_db_pool(self) -> asyncpg.Pool:
        """Создание пула БД"""
        db_config = self.config['database']
        
        pool = await asyncpg.create_pool(
            database=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            min_size=ParsingConfig.DB_MIN_POOL_SIZE,
            max_size=ParsingConfig.DB_MAX_POOL_SIZE
        )
        
        self.logger.debug(f"Пул БД создан: min={ParsingConfig.DB_MIN_POOL_SIZE}, max={ParsingConfig.DB_MAX_POOL_SIZE}")
        
        return pool
    
    async def _cleanup_components(self) -> None:
        """ИСПРАВЛЕНО: Правильная очередность очистки с задержками"""
        self.logger.debug("Начинаю очистку компонентов")
        
        try:
            # 1. Сначала graceful shutdown задач (с таймаутом)
            if hasattr(self, 'shutdown_manager') and self.shutdown_manager:
                try:
                    await asyncio.wait_for(
                        self.shutdown_manager.shutdown_gracefully(),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("Таймаут graceful shutdown")
            
            # 2. Небольшая задержка для завершения операций
            await asyncio.sleep(0.2)
            
            # 3. Закрываем сессии
            if hasattr(self, 'session_manager') and self.session_manager:
                try:
                    await asyncio.wait_for(
                        self.session_manager.close_all(),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("Таймаут закрытия сессий")
            
            # 4. Еще одна задержка для коннекторов
            await asyncio.sleep(0.2)
            
            # 5. Закрываем БД в последнюю очередь
            if hasattr(self, 'db_pool') and self.db_pool:
                try:
                    await asyncio.wait_for(
                        self.db_pool.close(),
                        timeout=3.0
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("Таймаут закрытия пула БД")
            
            self.logger.debug("Очистка компонентов завершена")
            
        except Exception as e:
            self.logger.error(f"Ошибка очистки компонентов: {str(e)}")
    
    async def parse_region_court(self, region_key: str, court_key: str, 
                           year: Optional[str] = None, 
                           start_number: Optional[int] = None,
                           session: Optional[aiohttp.ClientSession] = None) -> None:
        """Парсинг для конкретного региона и суда"""
        
        if year is None:
            year = self.config['search_parameters']['default_year']

        # Получаем названия для логов
        region_name = self.config['regions'][region_key].get('name', region_key)
        court_name = self.config['regions'][region_key]['courts'][court_key].get('name', court_key)

        self.logger.info(f"Начинаю парсинг: {region_name}, {court_name} ({year})")

        region_config = self.config['regions'][region_key]
        court_config = region_config['courts'][court_key]
        
        if start_number is None:
            start_number = await self._get_start_number(
                region_key, court_key, year
            )
        
        max_number = (
            self.config['search_parameters']['case_number_range']['end']
        )
        
        parsing_state = await self.global_state_manager.get_or_create_state(
            region_key, court_key, year, start_number, max_number
        )
        
        # Управляем сессией в зависимости от того, передана ли она извне
        session_provided = session is not None
        if not session_provided:
            session = await self.session_manager.get_session()
            self.logger.debug("Получена новая сессия")
        else:
            self.logger.debug("Используется предоставленная сессия")
        
        try:
            await self._perform_parsing(
                session, region_config, court_config, 
                year, parsing_state
            )
        finally:
            # Возвращаем сессию только если создали ее сами
            if not session_provided:
                await self.session_manager.return_session(session)
                self.logger.debug("Сессия возвращена в пул")
    
    async def _get_start_number(self, region_key: str, court_key: str, 
                              year: str) -> int:
        """Получить стартовый номер для парсинга"""
        region_config = self.config['regions'][region_key]
        court_config = region_config['courts'][court_key]
        
        kato_code = region_config['kato_code']
        instance_code = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        exact_prefix = f"{kato_code}{instance_code}-{year_short}-00-{case_type}"
        pattern_like = f"{exact_prefix}/%"
        pattern_regex = f"^{exact_prefix}/[0-9]+$"
        
        stats = await self.db_manager.get_prefix_statistics(
            pattern_like, pattern_regex
        )
        last_number = stats['max_number']
        
        start_number = last_number + 1 if last_number > 0 else 1
        
        self.logger.debug(f"Стартовый номер определен: {start_number} (последний в БД: {last_number})")
        
        return start_number
    
    async def _perform_parsing(self, session: aiohttp.ClientSession, 
                             region_config: Dict[str, Any], 
                             court_config: Dict[str, Any], 
                             year: str, parsing_state: Dict[str, Any]) -> None:
        """Выполнение основного парсинга"""
        key = parsing_state['key']
        
        region_name = region_config.get('name', key.region_key)
        court_name = court_config.get('name', key.court_key)
        
        self.logger.debug(f"Начинаю сессию парсинга: {region_name} - {court_name}")
        
        viewstate, form_ids = await self._prepare_search_form(session)
        await self.search_engine.select_region(
            session, viewstate, region_config['id'], form_ids
        )
        await self._main_parsing_loop(
            session, viewstate, region_config, court_config, 
            year, form_ids, parsing_state
        )
    
    async def _prepare_search_form(self, 
                                 session: aiohttp.ClientSession) -> Tuple[str, Dict[str, str]]:
        """Подготовка формы поиска"""
        url = f"{self.base_url}/form/lawsuit/"
        headers = HTTPHeaders.get_base_headers()
        headers['Referer'] = (
            f'{self.base_url}/form/proceedings/services.xhtml'
        )
        
        start_time = time.time()
        async with session.get(url, headers=headers) as response:
            duration_ms = int((time.time() - start_time) * 1000)
            
            if response.status != 200:
                raise ParseError(f"Ошибка загрузки страницы поиска: HTTP {response.status}")
            
            html = await response.text()
            viewstate = HTMLUtils.extract_viewstate(html)
            form_ids = FormExtractor.extract_search_form_ids(
                HTMLParser(html)
            )
            
            return viewstate, form_ids
    
    async def _main_parsing_loop(self, session: aiohttp.ClientSession, 
               viewstate: str, region_config: Dict[str, Any], 
               court_config: Dict[str, Any], year: str, 
               form_ids: Dict[str, str], 
               parsing_state: Dict[str, Any]) -> None:
        """ИСПРАВЛЕНО: Добавлены проверки shutdown во все критические точки"""
        key = parsing_state['key']
        region_name = region_config.get('name', key.region_key)
        court_name = court_config.get('name', key.court_key)
        
        try:
            while self._should_continue_parsing(parsing_state):
                # КРИТИЧНО: Проверка на КАЖДОЙ итерации
                if self.shutdown_manager.shutdown_requested:
                    self.logger.info("🛑 Прерывание парсинга по сигналу (начало цикла)")
                    break
                    
                parsing_state['current_number'] = self._get_next_search_number(parsing_state)
                
                case_number = self._generate_case_number(
                    region_config, court_config, year, 
                    parsing_state['current_number']
                )
                
                # КРИТИЧНО: Проверка перед каждым запросом
                if self.shutdown_manager.shutdown_requested:
                    self.logger.info("🛑 Прерывание парсинга по сигналу (перед запросом)")
                    break
                
                try:
                    await self._process_single_case(
                        session, viewstate, region_config, court_config, 
                        year, case_number, form_ids, parsing_state
                    )
                    
                    # КРИТИЧНО: Проверка после обработки
                    if self.shutdown_manager.shutdown_requested:
                        self.logger.info("🛑 Прерывание парсинга по сигналу (после обработки)")
                        break
                    
                    # Остальная логика...
                    if parsing_state['search_attempts'] % 100 == 0:
                        success_rate = (
                            parsing_state['total_found'] / 
                            parsing_state['search_attempts'] * 100
                        )
                        log_periodic_stats(self.logger, parsing_state['total_found'], 
                                        parsing_state['search_attempts'], round(success_rate, 1))
                    
                    if parsing_state['search_attempts'] % 500 == 0:
                        await self._log_global_stats()
                    
                    # КРИТИЧНО: Проверка перед задержкой
                    if self.shutdown_manager.shutdown_requested:
                        self.logger.info("🛑 Прерывание парсинга по сигналу (перед задержкой)")
                        break
                    
                    await self._apply_search_delay(
                        parsing_state.get('has_results', False),
                        had_502_error=parsing_state.get('last_had_502', False)
                    )
                    parsing_state['last_had_502'] = False
                    
                except Exception as e:
                    # Проверяем не является ли это результатом shutdown
                    if self.shutdown_manager.shutdown_requested:
                        self.logger.info("🛑 Исключение во время shutdown - завершаю")
                        break
                    
                    # Обычная обработка ошибок
                    is_502_error = '502' in str(e) or 'Bad Gateway' in str(e)
                    parsing_state['last_had_502'] = is_502_error
                    
                    if is_502_error:
                        log_error(self.logger, f"не удалось подключиться к сайту для дела {case_number}")
                    else:
                        self.logger.error(f"Ошибка поиска дела {case_number}: {str(e)}")
                    
                    parsing_state['consecutive_failures'] += 1 
                    if not parsing_state['missing_numbers_mode']:
                        parsing_state['current_number'] += 1
                    elif parsing_state['missing_numbers_queue']:
                        parsing_state['missing_numbers_queue'].pop(0)
                        parsing_state['missing_numbers_processed'] += 1
                    
                    if is_502_error:
                        await asyncio.sleep(random.uniform(1, 2))
        
        finally:
            await self._finalize_parsing(parsing_state, region_name, court_name)
    
    async def _finalize_parsing(self, state: Dict[str, Any], region_name: str, court_name: str) -> None:
        """ИСПРАВЛЕНО: Финализация с учетом graceful shutdown"""
        self.logger.info("Начинаю сохранение данных перед завершением...")
        
        try:
            # Принудительно сохраняем все накопленные данные
            if 'batch_manager' in state:
                final_stats = await state['batch_manager'].finalize(self.db_manager)
                state['total_saved'] += final_stats['saved']
                if final_stats['saved'] > 0:
                    self.logger.info(f"Сохранен финальный батч: {final_stats['saved']} дел")

            if state.get('cases_batch'):
                await self._save_batch(state)
                self.logger.info(f"Сохранен остаточный батч: {len(state['cases_batch'])} дел")
                
        except Exception as e:
            self.logger.error(f"Ошибка сохранения при завершении: {str(e)}")
        
        # Логируем итоги
        success_rate = (
            (state['total_found'] / state['search_attempts'] * 100) 
            if state['search_attempts'] > 0 else 0
        )
        
        duration = datetime.now() - state['created_at']
        hours = round(duration.total_seconds() / 3600, 1)
        
        # Финальный лог
        if self.shutdown_manager.shutdown_requested:
            self.logger.info(f"🛑 Парсинг прерван пользователем")
        
        self.logger.info(f"📊 Итоги для {region_name} - {court_name}: "
                        f"найдено {state['total_found']} дел за {hours}ч, "
                        f"успех {round(success_rate, 1)}%")
        
        self.logger.info("✅ Все процессы и семафоры корректно завершены")
    
    def _get_next_search_number(self, parsing_state: Dict[str, Any]) -> int:
        """Получить следующий номер для поиска"""
        current = parsing_state['current_number']
        found_numbers = parsing_state['found_numbers']
        
        while (current <= parsing_state['max_case_number'] and 
               found_numbers.contains(current)):
            current += 1
            
        return current
    
    def _should_continue_parsing(self, state: Dict[str, Any]) -> bool:
        """ИСПРАВЛЕНО: Проверка продолжения парсинга с более терпимыми условиями"""
        # Проверяем сигнал остановки
        if self.shutdown_manager.shutdown_requested:
            return False
        
        # В режиме поиска пропущенных номеров
        if state['missing_numbers_mode']:
            if not state['missing_numbers_queue']:
                self.logger.debug("Поиск пропущенных номеров завершен")
                return False
            return True
        
        # ИСПРАВЛЕНО: Более терпимые условия остановки
        max_consecutive_failures = state.get('max_failures', 100)  # Увеличено с 50 до 100
        max_consecutive_no_data = ParsingConfig.MAX_CONSECUTIVE_NO_DATA * 2  # Удваиваем лимит
        
        # Остановка только при достижении лимитов ИЛИ превышении максимального номера
        should_stop = (
            state['consecutive_failures'] >= max_consecutive_failures or 
            state['consecutive_no_data'] >= max_consecutive_no_data or
            state['current_number'] > state['max_case_number']
        )
        
        if should_stop:
            reason = ""
            if state['consecutive_failures'] >= max_consecutive_failures:
                reason = f"превышен лимит неудач ({state['consecutive_failures']}/{max_consecutive_failures})"
            elif state['consecutive_no_data'] >= max_consecutive_no_data:
                reason = f"слишком много попыток без данных ({state['consecutive_no_data']}/{max_consecutive_no_data})"
            elif state['current_number'] > state['max_case_number']:
                reason = f"достигнут максимальный номер ({state['current_number']})"
            
            self.logger.info(f"Завершение парсинга: {reason}")
        
        return not should_stop
    
    async def _process_single_case(self, session: aiohttp.ClientSession, 
                         viewstate: str, region_config: Dict[str, Any], 
                         court_config: Dict[str, Any], year: str, 
                         case_number: str, form_ids: Dict[str, str],
                         parsing_state: Dict[str, Any]) -> None:
        """Обработка одного дела с новой системой логирования"""
        parsing_state['search_attempts'] += 1
        key = parsing_state['key']
        
        # Получаем названия для логов
        region_name = region_config.get('name', key.region_key)
        court_name = court_config.get('name', key.court_key)
        
        # Правильное отслеживание максимального номера
        current_sequence = parsing_state['current_number']
        
        # Определяем номер для поиска
        if parsing_state['missing_numbers_mode']:
            if not parsing_state['missing_numbers_queue']:
                return
                
            missing_sequence = parsing_state['missing_numbers_queue'][0]
            search_number = str(missing_sequence)
            full_number = self._generate_case_number(
                region_config, court_config, year, missing_sequence
            )
            self.logger.info(f"Проверяю пропуск: {full_number}")
        else:
            search_number = case_number
            full_number = case_number
            log_search_start(self.logger, full_number, region_name, court_name)
        
        if not parsing_state['missing_numbers_mode']:
            # В обычном режиме всегда увеличиваем номер
            parsing_state['current_number'] += 1
        
        start_time = time.time()
        results_html = await self.search_engine.search_case(
            session, viewstate, region_config['id'], court_config['id'], 
            year, search_number, form_ids
        )
        search_duration_ms = int((time.time() - start_time) * 1000)
        
        if parsing_state['missing_numbers_mode']:
            results = self.results_parser.parse_search_results(
                results_html, filter_by_case_number=full_number
            )
        else:
            results = self.results_parser.parse_search_results(results_html)
        
        if results:
            parsing_state['consecutive_failures'] = 0
            parsing_state['consecutive_no_data'] = 0
            
            # Анализируем найденное дело для лога
            result = results[0]  # Берем первое дело для анализа
            judge_exists = bool(result.get('judge'))
            parties_count = len(result.get('plaintiffs', [])) + len(result.get('defendants', []))
            events_count = len(result.get('events', []))
            
            if parsing_state['missing_numbers_mode']:
                self.logger.info(f"НАЙДЕН: {'судья есть' if judge_exists else 'судья отсутствует'}, "
                               f"{parties_count} стороны, {events_count} события")
            else:
                log_case_found(self.logger, full_number, judge_exists, parties_count, events_count)
                
            for result in results:
                try:
                    stats = await parsing_state['batch_manager'].add_case(result, self.db_manager)
                    parsing_state['total_saved'] += stats['saved']
                except Exception as e:
                    self.logger.error(f"Ошибка сохранения дела: {str(e)}")
            
            parsing_state['total_found'] += len(results)
            parsing_state['has_results'] = True
            
            found_sequence_numbers = (
                self.results_parser.extract_sequence_numbers_from_results(results)
            )
            parsing_state['found_numbers'].add_numbers(found_sequence_numbers) # Обновляем максимальный номер из найденных результатов
            if found_sequence_numbers:
                max_found = max(found_sequence_numbers)
                if max_found > parsing_state.get('highest_processed_number', 0):
                    parsing_state['highest_processed_number'] = max_found
                    parsing_state['last_success_number'] = max_found
            
            # Логируем сохранение
            if parsing_state['missing_numbers_mode']:
                self.logger.info(f"Сохранено: новое дело {full_number}")
            else:
                log_case_saved(self.logger, full_number, not judge_exists)
            
            if parsing_state['missing_numbers_mode']:
                if parsing_state['missing_numbers_queue']: 
                    processed_num = parsing_state['missing_numbers_queue'].pop(0)
                    parsing_state['missing_numbers_processed'] += 1
                    parsing_state['missing_found_count'] = parsing_state.get('missing_found_count', 0) + len(results)
            else:
                parsing_state['last_success_number'] = parsing_state['current_number']
            
            if len(parsing_state['cases_batch']) >= ParsingConfig.BATCH_SIZE:
                await self._save_batch(parsing_state)
        else:
            parsing_state['consecutive_failures'] += 1
            parsing_state['consecutive_no_data'] += 1
            parsing_state['has_results'] = False
            
            if parsing_state['missing_numbers_mode']:
                self.logger.info("Пропуск подтвержден: дело не существует")
            else:
                log_case_not_found(self.logger, full_number)

            if parsing_state['missing_numbers_mode']:
                if parsing_state['missing_numbers_queue']:
                    failed_num = parsing_state['missing_numbers_queue'].pop(0)
                    parsing_state['missing_numbers_processed'] += 1
        
        # Логика переключения на поиск пропущенных номеров
        if (not parsing_state['missing_numbers_mode'] and 
            parsing_state['consecutive_no_data'] >= ParsingConfig.MAX_CONSECUTIVE_NO_DATA):
            await self._switch_to_missing_numbers_mode(
                parsing_state, region_config, court_config, year, region_name, court_name
            )
        
        # Обновляем текущий номер только в обычном режиме
        if not parsing_state['missing_numbers_mode'] and results:
            parsing_state['current_number'] += 1
    
    async def _save_batch(self, state: Dict[str, Any]) -> None:
        """Безопасное сохранение batch с использованием TransactionalBatchManager"""
        if not state['cases_batch']:
            return
        
        batch_manager = state['batch_manager']
        total_stats = {'saved': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        # Добавляем все дела из батча в менеджер
        for case_data in state['cases_batch']:
            try:
                stats = await batch_manager.add_case(case_data, self.db_manager, force_save=False)
                # Суммируем статистику
                for key in total_stats:
                    total_stats[key] += stats[key]
                
                # ИСПРАВЛЕНО: Проверяем сигнал остановки через shutdown_manager
                if self.shutdown_manager.shutdown_requested:
                    self.logger.info("🛑 Сохранение батча прервано по сигналу")
                    break
                    
            except Exception as e:
                self.logger.error(f"Ошибка менеджера батчей: {str(e)}")
                total_stats['errors'] += 1
        
        # Принудительно сохраняем оставшиеся данные
        final_stats = await batch_manager.finalize(self.db_manager)
        for key in total_stats:
            total_stats[key] += final_stats[key]
        
        state['total_saved'] += total_stats['saved']
        
        self.logger.debug(f"Сохранено: батч из {len(state['cases_batch'])} дел "
                         f"(успешно: {total_stats['saved']}, пропущено: {total_stats['skipped']}, ошибок: {total_stats['errors']})")
        
        # Очищаем старый батч
        state['cases_batch'] = []
    
    async def _apply_search_delay(self, has_results: bool, had_502_error: bool = False) -> None:
        """ИСПРАВЛЕНО: Прерываемая задержка с проверками shutdown"""
        if had_502_error:
            delay = ParsingConfig.DELAY_AFTER_502
            self.logger.debug(f"Увеличенная задержка {delay}с после 502 ошибки")
        else:
            delay = (
                ParsingConfig.DELAY_WITH_RESULTS 
                if has_results else ParsingConfig.DELAY_NO_RESULTS
            )
        
        # Добавляем случайную вариацию от 0 до 1 секунды
        actual_delay = random.uniform(delay, delay + 1.0)
        
        # ИСПРАВЛЕНО: Делим задержку на части для проверки shutdown
        check_interval = 0.5  # Проверяем каждые 0.5 секунд
        total_waited = 0.0
        
        while total_waited < actual_delay:
            # Проверяем shutdown перед каждой частью задержки
            if self.shutdown_manager.shutdown_requested:
                self.logger.debug("🛑 Задержка прервана по сигналу shutdown")
                break
                
            # Ждем либо оставшееся время, либо интервал проверки
            wait_time = min(check_interval, actual_delay - total_waited)
            await asyncio.sleep(wait_time)
            total_waited += wait_time
    
    async def _log_global_stats(self) -> None:
        """Логирование глобальной статистики"""
        stats = await self.global_state_manager.get_global_stats()
        
        self.logger.debug(f"Глобальная статистика: {stats['active_regions']} активных регионов, "
                         f"всего найдено {stats['total_found']} дел, "
                         f"успех {round(stats['success_rate'], 1)}%")
    
    def _generate_case_number(self, region_config: Dict[str, Any], 
                            court_config: Dict[str, Any], 
                            year: str, sequence: int) -> str:
        """Генерация номера дела"""
        kato_code = region_config['kato_code']
        instance_code = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        return f"{kato_code}{instance_code}-{year_short}-00-{case_type}/{sequence}"
    
    async def parse_specific_case_number(self, region_key: str, court_key: str, 
                           case_number: str, 
                           year: Optional[str] = None,
                           session: Optional[aiohttp.ClientSession] = None) -> Optional[Dict[str, Any]]:
        """Поиск одного конкретного номера дела"""
        
        if year is None:
            year = self.config['search_parameters']['default_year']

        region_config = self.config['regions'][region_key]
        court_config = region_config['courts'][court_key]
        
        # Получаем названия для логов
        region_name = region_config.get('name', region_key)
        court_name = court_config.get('name', court_key)
        
        full_case_number = self._generate_case_number(region_config, court_config, year, int(case_number))
        log_search_start(self.logger, full_case_number, region_name, court_name)
        
        session_provided = session is not None
        if not session_provided:
            session = await self.session_manager.get_session()
        
        try:
            viewstate, form_ids = await self._prepare_search_form(session)
            await self.search_engine.select_region(
                session, viewstate, region_config['id'], form_ids
            )
            
            try:
                sequence_number = int(case_number)
            except ValueError:
                self.logger.error(f"Неверный номер дела: {case_number}")
                return None
            
            # Генерируем ожидаемый полный номер для фильтрации
            expected_full_case_number = self._generate_case_number(
                region_config, court_config, year, sequence_number
            )
            
            start_time = time.time()
            results_html = await self.search_engine.search_case(
                session, viewstate, region_config['id'], court_config['id'], 
                year, case_number, form_ids  # Передаем только порядковый номер
            )
            search_duration_ms = int((time.time() - start_time) * 1000)
            
            # Применяем фильтрацию по точному совпадению полного номера
            results = self.results_parser.parse_search_results(
                results_html, 
                filter_by_case_number=expected_full_case_number
            )
            
            if results:
                result = results[0]
                judge_exists = bool(result.get('judge'))
                parties_count = len(result.get('plaintiffs', [])) + len(result.get('defendants', []))
                events_count = len(result.get('events', []))
                
                log_case_found(self.logger, expected_full_case_number, judge_exists, parties_count, events_count)
                
                stats = await self.db_manager.batch_save_cases(results)
                log_case_saved(self.logger, expected_full_case_number, not judge_exists)
                
                return results[0] if results else None
            else:
                log_case_not_found(self.logger, expected_full_case_number)
                return None
                
        finally:
            if not session_provided:
                await self.session_manager.return_session(session)

    async def parse_specific_case_numbers(self, region_key: str, court_key: str, 
                                    case_numbers: List[str], 
                                    year: Optional[str] = None,
                                    session: Optional[aiohttp.ClientSession] = None) -> List[Dict[str, Any]]:
        """Поиск списка конкретных номеров дел"""
        
        if year is None:
            year = self.config['search_parameters']['default_year']

        region_config = self.config['regions'][region_key]
        court_config = region_config['courts'][court_key]
        
        # Получаем названия для логов
        region_name = region_config.get('name', region_key)
        court_name = court_config.get('name', court_key)
        
        self.logger.debug(f"Начинаю поиск батча из {len(case_numbers)} дел в {region_name} - {court_name}")

        session_provided = session is not None
        if not session_provided:
            session = await self.session_manager.get_session()
            self.logger.debug("Получена сессия для батча")
        else:
            self.logger.debug("Используется предоставленная сессия для батча")
            
        all_results = []
        max_processed_number = 0
        
        try:
            viewstate, form_ids = await self._prepare_search_form(session)
            await self.search_engine.select_region(
                session, viewstate, region_config['id'], form_ids
            )
            
            for i, case_number in enumerate(case_numbers, 1):
                try:
                    sequence_number = int(case_number)
                    if sequence_number > max_processed_number:
                        max_processed_number = sequence_number
                except ValueError:
                    self.logger.warning(f"Неверный номер дела: {case_number}")
                    continue
                
                full_case_number = self._generate_case_number(region_config, court_config, year, sequence_number)
                self.logger.debug(f"Обрабатываю дело {full_case_number} ({i}/{len(case_numbers)})")
                
                try:
                    start_time = time.time()
                    results_html = await self.search_engine.search_case(
                        session, viewstate, region_config['id'], 
                        court_config['id'], year, case_number, form_ids  
                    )
                    search_duration_ms = int((time.time() - start_time) * 1000)
                    
                    expected_full_case_number = self._generate_case_number(
                        region_config, court_config, year, int(case_number)
                    )
                    
                    results = self.results_parser.parse_search_results(
                        results_html, 
                        filter_by_case_number=expected_full_case_number
                    )
                    
                    if results:
                        result = results[0]
                        judge_exists = bool(result.get('judge'))
                        parties_count = len(result.get('plaintiffs', [])) + len(result.get('defendants', []))
                        events_count = len(result.get('events', []))
                        
                        if i <= 10:  # Логируем детали только для первых 10 дел
                            self.logger.info(f"Обновляю дело: {expected_full_case_number}")
                            if judge_exists:
                                # Используем зеленый цвет для успеха
                                self.logger.info(f"УСПЕХ: судья найден и добавлен")
                            else:
                                self.logger.info(f"НЕТ ИЗМЕНЕНИЙ: судья по-прежнему отсутствует")
                        
                        all_results.extend(results)
                    else:
                        if i <= 10:  # Логируем детали только для первых 10 дел
                            self.logger.info(f"Обновляю дело: {expected_full_case_number}")
                            self.logger.info("НЕТ ИЗМЕНЕНИЙ: новых данных не найдено")
                    
                    await self._apply_search_delay(bool(results))
                    
                except Exception as e:
                    self.logger.error(f"Ошибка поиска дела {case_number}: {str(e)}")
                    continue
            
            if all_results:
                stats = await self.db_manager.batch_save_cases(all_results)
                self.logger.debug(f"Батч сохранен: {len(all_results)} дел, статистика: {stats}")
            
            self.logger.debug(f"Батч завершен: найдено {len(all_results)} из {len(case_numbers)} дел")
            
            return all_results
                
        finally:
            if not session_provided:
                await self.session_manager.return_session(session)
                self.logger.debug("Сессия батча возвращена в пул")

    async def _switch_to_missing_numbers_mode(self, parsing_state: Dict[str, Any], 
                                        region_config: Dict[str, Any], 
                                        court_config: Dict[str, Any], 
                                        year: str, region_name: str, court_name: str) -> None:
        """Переключение на режим поиска пропущенных номеров"""
        key = parsing_state['key']
        
        # Формируем паттерны для поиска пропущенных номеров
        kato_code = region_config['kato_code']
        instance_code = court_config['instance_code']  
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        exact_prefix = f"{kato_code}{instance_code}-{year_short}-00-{case_type}"
        pattern_like = f"{exact_prefix}/%"
        pattern_regex = f"^{exact_prefix}/[0-9]+$"
        
        # Получаем статистику для определения максимального номера
        stats = await self.db_manager.get_prefix_statistics(
            pattern_like, pattern_regex
        )
        max_number_in_db = stats['max_number']
        
        if max_number_in_db == 0:
            self.logger.debug("Нет данных для поиска пропущенных номеров")
            return
        
        try:
            # Ищем пропущенные номера от 1 до максимального в БД 
            missing_numbers = await self.db_manager.get_missing_numbers(
                pattern_like, pattern_regex, 
                1,  # ← Всегда с 1
                max_number_in_db  # ← До максимального в БД
            )
            
            if missing_numbers:
                parsing_state['missing_numbers_mode'] = True
                parsing_state['missing_numbers_queue'] = missing_numbers
                parsing_state['consecutive_failures'] = 0  # Сбрасываем счетчик
                parsing_state['consecutive_no_data'] = 0
                parsing_state['missing_found_count'] = 0

                # Формируем строку диапазонов для лога (упрощенно)
                ranges_text = f"{missing_numbers[0]}-{missing_numbers[-1]}" if len(missing_numbers) > 1 else str(missing_numbers[0])
                if len(missing_numbers) > 20:
                    ranges_text += "..."
                
                log_missing_search_start(self.logger, region_name, court_name, year)
                log_missing_found(self.logger, len(missing_numbers), ranges_text)
            else:
                self.logger.debug("Пропущенные номера не найдены")
                
        except Exception as e:
            self.logger.error(f"Ошибка запроса пропущенных номеров: {str(e)}")
            # Продолжаем обычный парсинг при ошибке

    async def run_history_update_mode(self, year: Optional[str] = None) -> Dict[str, int]:
        """Запуск режима обновления истории дел"""
        
        # Получаем конфигурацию режима
        mode_config = self.config_manager.get_mode_config('history_update')
        if not mode_config:
            raise ConfigurationError("Конфигурация режима history_update не найдена")
        
        self.logger.info("🔄 Запуск режима обновления истории дел")
        
        # Создаем обновлятор истории
        history_updater = CaseHistoryUpdater(self, mode_config)
        
        # Запускаем обновление
        return await history_updater.run_history_update(year)

    async def get_case_data_without_saving(self, region_key: str, court_key: str, 
                                     case_number: str, year: Optional[str] = None,
                                     session: Optional[aiohttp.ClientSession] = None) -> Optional[Dict[str, Any]]:
        """Получение данных дела с сайта БЕЗ сохранения в БД"""
        
        if year is None:
            year = self.config['search_parameters']['default_year']

        region_config = self.config['regions'][region_key]
        court_config = region_config['courts'][court_key]
        
        # Получаем названия для логов
        region_name = region_config.get('name', region_key)
        court_name = court_config.get('name', court_key)
        
        full_case_number = self._generate_case_number(region_config, court_config, year, int(case_number))
        
        session_provided = session is not None
        if not session_provided:
            session = await self.session_manager.get_session()
        
        try:
            viewstate, form_ids = await self._prepare_search_form(session)
            await self.search_engine.select_region(
                session, viewstate, region_config['id'], form_ids
            )
            
            try:
                sequence_number = int(case_number)
            except ValueError:
                self.logger.error(f"Неверный номер дела: {case_number}")
                return None
            
            # Генерируем ожидаемый полный номер для фильтрации
            expected_full_case_number = self._generate_case_number(
                region_config, court_config, year, sequence_number
            )
            
            start_time = time.time()
            results_html = await self.search_engine.search_case(
                session, viewstate, region_config['id'], court_config['id'], 
                year, case_number, form_ids
            )
            search_duration_ms = int((time.time() - start_time) * 1000)
            
            # Применяем фильтрацию по точному совпадению полного номера
            results = self.results_parser.parse_search_results(
                results_html, 
                filter_by_case_number=expected_full_case_number
            )
            
            if results:
                result = results[0]
                judge_exists = bool(result.get('judge'))
                parties_count = len(result.get('plaintiffs', [])) + len(result.get('defendants', []))
                events_count = len(result.get('events', []))
                
                # ТОЛЬКО логируем, НЕ сохраняем
                self.logger.debug(f"📄 Получены данные {expected_full_case_number}: "
                                f"{'судья есть' if judge_exists else 'судья отсутствует'}, "
                                f"{parties_count} стороны, {events_count} события")
                
                return results[0]
            else:
                self.logger.debug(f"📄 Данные не найдены: {expected_full_case_number}")
                return None
                
        finally:
            if not session_provided:
                await self.session_manager.return_session(session)

# ============================================================================
# КООРДИНАТОР ПАРСИНГА С ИСПРАВЛЕННЫМ SHUTDOWN
# ============================================================================

class ParsingCoordinator:
    def __init__(self, parser: CourtParser):
        self.parser = parser
        self.global_state_manager = parser.global_state_manager
        self.logger = get_logger('parsing_coordinator')
        
        # ИСПРАВЛЕНО: Берем значение ПОСЛЕ того как оно установлено в parser
        self.max_concurrent_sessions = ParsingConfig.MAX_CONCURRENT_SESSIONS
        
        # Дополнительная проверка что значение применилось
        config_value = parser.config.get('max_concurrent_sessions', 'НЕ НАЙДЕНО')
        actual_value = self.max_concurrent_sessions
        
        if str(config_value) != str(actual_value):
            self.logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: значение из конфига ({config_value}) не применилось! Используется: {actual_value}")
            # Принудительно устанавливаем
            if isinstance(config_value, int) and config_value > 0:
                self.max_concurrent_sessions = config_value
                self.logger.info(f"Принудительно установлено: {config_value}")
        
        self.logger.info(f"Координатор инициализирован с лимитом {self.max_concurrent_sessions} сессий")
    
    async def run_optimized_parsing(self, regions: Optional[List[str]] = None, 
                      courts: Optional[List[str]] = None, 
                      year: Optional[str] = None, 
                      start_numbers: Optional[Dict[str, int]] = None) -> None:
        """ОБНОВЛЕНО: 4-этапный парсинг с обновлением истории дел"""
        
        if regions is None or regions == []:
            regions = list(self.parser.config['regions'].keys())
            self.logger.debug(f"Выбраны все регионы: {len(regions)} шт.")
        
        if courts is None or courts == []:
            # Собираем все уникальные типы судов из конфигурации
            court_types = set()
            for region_config in self.parser.config['regions'].values():
                court_types.update(region_config['courts'].keys())
            courts = list(court_types)
            self.logger.debug(f"Выбраны все суды: {courts}")
        
        self.logger.info(f"🚀 ЭТАП 1/4: Основной парсинг - {len(regions)} регионов, {len(courts)} типов судов")
        
        # ========================================================================
        # ЭТАП 1: ОСНОВНОЙ ПАРСИНГ ВСЕХ РЕГИОНОВ
        # ========================================================================
        
        tasks = self._create_parsing_tasks(regions, courts, year, start_numbers)
        
        if tasks:
            monitor_task = asyncio.create_task(self._monitor_global_progress())
            
            try:
                await self._execute_parsing_tasks(tasks)
            finally:
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass
        
        # Проверяем не было ли прерывания
        if self.parser.shutdown_manager.shutdown_requested:
            self.logger.info("🛑 Парсинг прерван пользователем - пропускаю обновления")
            return
        
        self.logger.info("✅ ЭТАП 1/4 завершен: Основной парсинг выполнен")
        
        # ========================================================================
        # ЭТАП 2: ПОИСК ПРОПУЩЕННЫХ НОМЕРОВ
        # ========================================================================
        
        self.logger.info("🔍 ЭТАП 2/4: Поиск пропущенных номеров")
        
        # Логика поиска пропущенных номеров уже работает в основном парсере
        
        self.logger.info("✅ ЭТАП 2/4 завершен: Поиск пропущенных номеров выполнен")
        
        # ========================================================================
        # ЭТАП 3: ГЛОБАЛЬНОЕ ОБНОВЛЕНИЕ СУДЕЙ ДЛЯ ВСЕХ ДЕЛ СМАС
        # ========================================================================
        
        if self.parser.shutdown_manager.shutdown_requested:
            self.logger.info("🛑 Парсинг прерван - пропускаю глобальное обновление судей")
            return
        
        self.logger.info("👨‍⚖️ ЭТАП 3/4: Глобальное обновление судей для всех дел СМАС")
        
        try:
            await self._run_global_judge_updates(year or self.parser.config['search_parameters']['default_year'])
            self.logger.info("✅ ЭТАП 3/4 завершен: Глобальное обновление судей выполнено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка глобального обновления судей: {str(e)}")
        
        # ========================================================================
        # НОВЫЙ ЭТАП 4: ОБНОВЛЕНИЕ ИСТОРИИ ДЕЛ
        # ========================================================================
        
        if self.parser.shutdown_manager.shutdown_requested:
            self.logger.info("🛑 Парсинг прерван - пропускаю обновление истории дел")
            return
        
        self.logger.info("📚 ЭТАП 4/4: Обновление истории дел")
        
        try:
            # Проверяем есть ли конфигурация для обновления истории
            history_config = self.parser.config_manager.get_mode_config('history_update')
            
            if history_config:
                # Год наследуется от предыдущих этапов
                update_year = year or self.parser.config['search_parameters']['default_year']
                
                self.logger.info(f"🔄 Запуск обновления истории дел за {update_year} год")
                
                stats = await self.parser.run_history_update_mode(update_year)
                
                self.logger.info("✅ ЭТАП 4/4 завершен: Обновление истории дел выполнено")
                self.logger.info(f"📊 Результат: обновлено {stats['updated_cases']} из {stats['processed_cases']} дел")
            else:
                self.logger.info("⚠️ Конфигурация history_update не найдена - пропускаю обновление истории")
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления истории дел: {str(e)}")
        
        self.logger.info("🎉 ВСЕ ЭТАПЫ ЗАВЕРШЕНЫ: Полный цикл с обновлением истории выполнен")

    async def _run_global_judge_updates(self, year: str) -> None:
        """НОВЫЙ МЕТОД: Глобальное обновление судей для всех дел СМАС без судей"""
        
        try:
            # СТРОГИЙ паттерн ТОЛЬКО для СМАС: цифры-цифры-00-4/цифры
            case_pattern = '-00-4/'
            max_cases_to_update = 2000  # Ограничиваем количество для безопасности
            batch_size = 50  # Размер батча для обработки
            
            self.logger.info(f"🎯 Поиск всех дел СМАС без судей (строгий паттерн: номер-номер-00-4/номер)")
            
            # Получаем ВСЕ дела СМАС без судей из БД
            smas_cases_without_judges = await self.parser.db_manager.get_cases_without_judges(
                case_pattern, max_cases_to_update
            )
            
            if not smas_cases_without_judges:
                self.logger.info("✅ Дел СМАС без судей не найдено - обновление не требуется")
                return
            
            self.logger.info(f"📋 Найдено {len(smas_cases_without_judges)} дел СМАС без судей для обновления")
            
            # Группируем дела по регионам/судам для эффективной обработки
            grouped_cases = await self._group_smas_cases_by_court(smas_cases_without_judges)
            
            if not grouped_cases:
                self.logger.warning("❌ Не удалось сгруппировать дела по судам")
                return
            
            self.logger.info(f"🏛️ Дела сгруппированы по {len(grouped_cases)} судам")
            
            # Обрабатываем каждую группу судов
            total_processed = 0
            total_updated = 0
            
            for court_info, court_cases in grouped_cases.items():
                # Проверяем shutdown перед каждой группой
                if self.parser.shutdown_manager.shutdown_requested:
                    self.logger.info("🛑 Глобальное обновление судей прервано по сигналу")
                    break
                
                try:
                    self.logger.info(f"👨‍⚖️ Обновляю судей для {court_info}: {len(court_cases)} дел")
                    
                    processed, updated = await self._process_smas_court_group(
                        court_info, court_cases, batch_size
                    )
                    
                    total_processed += processed
                    total_updated += updated
                    
                    # Задержка между группами судов
                    await asyncio.sleep(2.0)
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обновления группы {court_info}: {str(e)}")
                    continue
            
            # Финальная статистика
            success_rate = (total_updated / total_processed * 100) if total_processed > 0 else 0
            
            self.logger.info(f"📊 Глобальное обновление судей завершено:")
            self.logger.info(f"   • Обработано дел: {total_processed}")
            self.logger.info(f"   • Обновлено судей: {total_updated}")  
            self.logger.info(f"   • Успешность: {round(success_rate, 1)}%")
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка глобального обновления судей: {str(e)}")
            raise

    async def _group_smas_cases_by_court(self, case_numbers: List[str]) -> Dict[str, List[str]]:
        """НОВЫЙ МЕТОД: Группировка СМАС дел по судам"""
        grouped = defaultdict(list)
        
        for case_number in case_numbers:
            try:
                # Парсим номер дела для определения суда
                parsed = self.parser.text_processor.parse_full_case_number(case_number)
                if not parsed:
                    self.logger.warning(f"Неверный формат номера дела: {case_number}")
                    continue
                
                # ДОПОЛНИТЕЛЬНАЯ проверка что это точно СМАС
                if parsed['case_type'] != '4':
                    self.logger.warning(f"Номер {case_number} не является СМАС (тип: {parsed['case_type']})")
                    continue
                
                # Ищем соответствующий регион и суд
                region_key, court_key = self._find_region_court_by_parsed_data(parsed)
                
                if region_key and court_key:
                    # Проверяем что найденный суд действительно СМАС
                    region_config = self.parser.config['regions'][region_key]
                    court_config = region_config['courts'][court_key]
                    
                    if court_config['case_type_code'] == '4':  # Подтверждаем СМАС
                        court_info = f"{region_key}_{court_key}_{parsed['year_part']}"
                        grouped[court_info].append(case_number)
                    else:
                        self.logger.warning(f"Суд {court_key} в регионе {region_key} не является СМАС")
                else:
                    self.logger.warning(f"Не найден суд для дела {case_number}")
                    
            except Exception as e:
                self.logger.error(f"Ошибка группировки дела {case_number}: {str(e)}")
                continue
        
        # Логируем результаты группировки
        total_grouped = sum(len(cases) for cases in grouped.values())
        filtered_out = len(case_numbers) - total_grouped
        
        if filtered_out > 0:
            self.logger.info(f"🔍 Отфильтровано {filtered_out} некорректных номеров из {len(case_numbers)}")
        
        self.logger.debug(f"Группировка: {len(grouped)} групп, всего {total_grouped} корректных дел")
        
        return dict(grouped)

    def _find_region_court_by_parsed_data(self, parsed_data: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
        """ИСПРАВЛЕНО: Найти регион и суд по разобранным данным дела"""
        
        court_code = parsed_data.get('court_code', '')
        case_type = parsed_data.get('case_type', '')
        
        if not court_code or not case_type:
            self.logger.debug(f"Недостаточно данных: court_code={court_code}, case_type={case_type}")
            return None, None
        
        # Для примера 6294-25-00-4/215:
        # court_code = "6294" 
        # case_type = "4"
        # Нужно найти: kato_code + instance_code = "6294"
        
        for region_key, region_config in self.parser.config['regions'].items():
            kato_code = region_config.get('kato_code', '')
            if not kato_code:
                continue
                
            # Проверяем все суды в регионе
            for court_key, court_config in region_config.get('courts', {}).items():
                instance_code = court_config.get('instance_code', '')
                court_case_type = court_config.get('case_type_code', '')
                
                if not instance_code or not court_case_type:
                    continue
                
                # Формируем ожидаемый код суда: КАТО + instance_code
                expected_court_code = kato_code + instance_code
                
                # Сравниваем с фактическим кодом из номера дела
                if expected_court_code == court_code and court_case_type == case_type:
                    self.logger.debug(f"Найдено соответствие: {court_code} -> {region_key}_{court_key} "
                                    f"(КАТО:{kato_code} + instance:{instance_code}, тип:{case_type})")
                    return region_key, court_key
        
        # Дополнительная попытка для случаев с ведущими нулями или особых форматов
        for region_key, region_config in self.parser.config['regions'].items():
            kato_code = region_config.get('kato_code', '')
            if not kato_code:
                continue
                
            for court_key, court_config in region_config.get('courts', {}).items():
                instance_code = court_config.get('instance_code', '')
                court_case_type = court_config.get('case_type_code', '')
                
                if not instance_code or not court_case_type:
                    continue
                
                # Проверяем различные комбинации с ведущими нулями
                possible_codes = [
                    kato_code + instance_code,
                    kato_code.zfill(2) + instance_code,
                    kato_code.zfill(3) + instance_code,
                    kato_code + instance_code.zfill(2),
                    kato_code + instance_code.zfill(3)
                ]
                
                for possible_code in possible_codes:
                    if possible_code == court_code and court_case_type == case_type:
                        self.logger.debug(f"Найдено соответствие (с дополнением): {court_code} -> {region_key}_{court_key}")
                        return region_key, court_key
        
        # Логируем для диагностики неудачного поиска
        self.logger.warning(f"Не найден регион/суд для кода {court_code}, тип дела {case_type}")
        
        # Показываем доступные варианты для первых нескольких регионов (для отладки)
        debug_info = []
        for region_key, region_config in list(self.parser.config['regions'].items())[:3]:
            kato = region_config.get('kato_code', 'N/A')
            for court_key, court_config in region_config.get('courts', {}).items():
                instance = court_config.get('instance_code', 'N/A')
                c_type = court_config.get('case_type_code', 'N/A')
                expected = kato + instance
                debug_info.append(f"{expected}(тип:{c_type})")
        
        if debug_info:
            self.logger.debug(f"Примеры доступных кодов: {', '.join(debug_info[:5])}")
        
        return None, None

    async def _process_smas_court_group(self, court_info: str, case_numbers: List[str], 
                                batch_size: int) -> Tuple[int, int]:
        """НОВЫЙ МЕТОД: Обработка группы СМАС дел для одного суда"""
        
        try:
            region_key, court_key, year_part = court_info.split('_')
            year = f"20{year_part}"
            
            # Получаем названия для логов
            region_name = self.parser.config['regions'][region_key].get('name', region_key)
            court_name = self.parser.config['regions'][region_key]['courts'][court_key].get('name', court_key)
            
            self.logger.debug(f"👨‍⚖️ Обновление судей: {len(case_numbers)} дел в {region_name} - {court_name}")
            
            session = await self.parser.session_manager.get_session()
            total_processed = 0
            total_updated = 0
            
            try:
                # Обрабатываем дела батчами
                for i in range(0, len(case_numbers), batch_size):
                    # Проверяем shutdown перед каждым батчем
                    if self.parser.shutdown_manager.shutdown_requested:
                        self.logger.info("🛑 Обновление судей прервано по сигналу")
                        break
                    
                    batch = case_numbers[i:i + batch_size]
                    
                    try:
                        # Извлекаем только порядковые номера из полных номеров
                        sequence_numbers = []
                        for case_number in batch:
                            if '/' in case_number:
                                try:
                                    sequence = case_number.split('/')[-1]
                                    sequence_numbers.append(sequence)
                                except:
                                    continue
                        
                        if sequence_numbers:
                            self.logger.debug(f"👨‍⚖️ Обрабатываю батч {i//batch_size + 1}: {len(sequence_numbers)} дел")
                            
                            # Используем существующий метод для поиска
                            results = await self.parser.parse_specific_case_numbers(
                                region_key, court_key, sequence_numbers, year, session
                            )
                            
                            # Подсчитываем обновления
                            batch_processed = len(sequence_numbers)
                            batch_updated = len([r for r in results if r.get('judge')])
                            
                            total_processed += batch_processed  
                            total_updated += batch_updated
                            
                            if batch_updated > 0:
                                self.logger.debug(f"✅ Батч завершен: найдено {batch_updated} судей из {batch_processed} дел")
                            
                            # Задержка между батчами
                            await asyncio.sleep(1.0)
                            
                    except Exception as batch_error:
                        self.logger.error(f"❌ Ошибка обработки батча: {str(batch_error)}")
                        continue
                
                return total_processed, total_updated
                
            finally:
                await self.parser.session_manager.return_session(session)
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки группы {court_info}: {str(e)}")
            return 0, 0
    
    async def _monitor_global_progress(self) -> None:
        """Мониторинг глобального прогресса"""
        while True:
            try:
                await asyncio.sleep(300)  # Каждые 5 минут
                stats = await self.global_state_manager.get_global_stats()
                
                self.logger.debug(f"Периодический отчет: {stats['active_regions']} активных регионов, "
                                f"всего найдено {stats['total_found']} дел, "
                                f"успех {round(stats['success_rate'], 1)}%")
                
                await self.global_state_manager.cleanup_completed()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Ошибка мониторинга: {str(e)}")
    
    def _create_parsing_tasks(self, regions: List[str], 
                        courts: Optional[List[str]], year: Optional[str], 
                        start_numbers: Optional[Dict[str, int]]) -> List:
        """Создание задач парсинга"""
        tasks = []
        semaphore = asyncio.Semaphore(self.max_concurrent_sessions)
        
        for region_key in regions:
            if region_key not in self.parser.config['regions']:
                self.logger.warning(f"Регион {region_key} не найден в конфигурации")
                continue
                
            region_config = self.parser.config['regions'][region_key]
            region_courts = (
                courts if courts else list(region_config['courts'].keys())
            )
            
            for court_key in region_courts:
                if court_key in region_config['courts']:
                    task = self._create_single_parsing_task(
                        semaphore, region_key, court_key, year, start_numbers
                    )
                    tasks.append(task)
                    
                    # Регистрируем задачу в shutdown_manager
                    self.parser.shutdown_manager.register_task(task)
                else:
                    self.logger.warning(f"Суд {court_key} не найден в регионе {region_key}")
        
        self.logger.info(f"Создано задач парсинга: {len(tasks)}")
        
        return tasks
    
    def _create_single_parsing_task(self, semaphore: asyncio.Semaphore, 
                  region_key: str, court_key: str, 
                  year: Optional[str], 
                  start_numbers: Optional[Dict[str, int]]):
        """ИСПРАВЛЕНО: Убираем автообновления судей из задач регионов"""
        
        async def parse_with_semaphore():
            session = None
            semaphore_acquired = False
            
            try:
                # Проверяем shutdown ДО получения семафора
                if self.parser.shutdown_manager.shutdown_requested:
                    self.logger.debug(f"🛑 Задача {region_key} - {court_key} пропущена - shutdown запрошен")
                    return
                
                # Получаем семафор
                await semaphore.acquire()
                semaphore_acquired = True
                
                # Повторная проверка после получения семафора
                if self.parser.shutdown_manager.shutdown_requested:
                    self.logger.debug(f"🛑 Задача {region_key} - {court_key} пропущена - shutdown после семафора")
                    return
                
                # Получаем сессию
                try:
                    session = await self.parser.session_manager.get_session()
                    self.logger.debug(f"Получена сессия для {region_key} - {court_key}")
                except Exception as session_error:
                    self.logger.error(f"Ошибка получения сессии для {region_key} - {court_key}: {str(session_error)}")
                    return
                
                start_num = (
                    start_numbers.get(f"{region_key}_{court_key}") 
                    if start_numbers else None
                )
                
                # ТОЛЬКО основной парсинг - БЕЗ обновлений судей
                try:
                    await self.parser.parse_region_court(
                        region_key, court_key, year, start_num, session=session
                    )
                except Exception as parsing_error:
                    if not isinstance(parsing_error, asyncio.CancelledError):
                        self.logger.error(f"Ошибка парсинга {region_key} - {court_key}: {str(parsing_error)}")
                    raise

            except asyncio.CancelledError:
                self.logger.debug(f"🛑 Задача {region_key} - {court_key} отменена")
                raise
            except Exception as e:
                self.logger.error(f"❌ Неожиданная ошибка задачи {region_key} - {court_key}: {str(e)}")
            finally:
                # КРИТИЧНО: Всегда освобождаем ресурсы
                try:
                    if session:
                        await self.parser.session_manager.return_session(session)
                        self.logger.debug(f"Сессия возвращена для {region_key} - {court_key}")
                except Exception as session_error:
                    self.logger.error(f"Ошибка возврата сессии для {region_key} - {court_key}: {str(session_error)}")
                finally:
                    if semaphore_acquired:
                        semaphore.release()
        
        # Создаем задачу с именем
        task = asyncio.create_task(
            parse_with_semaphore(), 
            name=f"parse_{region_key}_{court_key}"
        )
        return task
    
    async def _execute_parsing_tasks(self, tasks: List) -> None:
        """ИСПРАВЛЕНО: Правильное выполнение с использованием asyncio.wait вместо gather"""
        
        try:
            # ИСПРАВЛЕНО: Используем wait() вместо gather() для лучшего контроля
            shutdown_task = asyncio.create_task(
                self.parser.shutdown_manager.wait_for_shutdown()
            )
            
            # ИСПРАВЛЕНО: Не создаем дополнительную задачу gather - работаем напрямую с tasks
            all_tasks = set(tasks + [shutdown_task])
            
            # Ждем завершения хотя бы одной задачи (либо shutdown, либо все задачи парсинга)
            done, pending = await asyncio.wait(
                all_tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Проверяем что завершилось
            if shutdown_task in done:
                self.logger.info("🛑 Получен сигнал остановки - завершаю все задачи...")
                
                # Отменяем все незавершенные задачи парсинга
                parsing_tasks = [t for t in pending if t != shutdown_task]
                for task in parsing_tasks:
                    if not task.done():
                        task.cancel()
                
                # Ждем завершения отмененных задач с таймаутом
                if parsing_tasks:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*parsing_tasks, return_exceptions=True),
                            timeout=30.0
                        )
                    except asyncio.TimeoutError:
                        self.logger.warning("⏰ Таймаут ожидания завершения задач парсинга")
            else:
                # Некоторые задачи парсинга завершились, отменяем shutdown_task
                if shutdown_task in pending:
                    shutdown_task.cancel()
                    try:
                        await shutdown_task
                    except asyncio.CancelledError:
                        pass
                
                # Ждем завершения всех оставшихся задач парсинга
                parsing_tasks = [t for t in pending if t != shutdown_task]
                if parsing_tasks:
                    try:
                        await asyncio.gather(*parsing_tasks, return_exceptions=True)
                    except Exception as e:
                        self.logger.error(f"Ошибка ожидания задач парсинга: {str(e)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка выполнения задач: {str(e)}")
            # Дополнительная диагностика
            self.logger.error(f"Общее количество задач: {len(tasks)}")
            self.logger.error(f"Типы первых 3 задач: {[type(task).__name__ for task in tasks[:3]]}")
        
        finally:
            # Graceful shutdown только при необходимости
            if self.parser.shutdown_manager.shutdown_requested:
                await self.parser.shutdown_manager.shutdown_gracefully()
    
    async def run_by_court_type(self, court_type: Optional[str] = None, 
                          regions: Optional[List[str]] = None, 
                          year: Optional[str] = None, 
                          start_numbers: Optional[Dict[str, int]] = None) -> None:
        """Запуск парсера для конкретного типа суда"""
        if regions is None or regions == []:
            regions = list(self.parser.config['regions'].keys())
            self.logger.debug(f"Выбраны все регионы для типа суда: {len(regions)} шт.")
        
        if court_type is None:
            # Если court_type не указан, используем ВСЕ типы судов
            self.logger.info("Выбраны все типы судов")
            await self.run_optimized_parsing(
                regions=regions,
                courts=None,  # None означает все суды
                year=year,
                start_numbers=start_numbers
            )
        else:
            # Обычная логика для конкретного типа суда
            filtered_regions = self._filter_regions_by_court_type(
                regions, court_type
            )
            if not filtered_regions:
                self.logger.warning(f"Нет регионов с судами типа {court_type}")
                return
            
            self.logger.info(f"Запуск парсинга для типа суда {court_type}: {len(filtered_regions)} регионов")
            
            await self.run_optimized_parsing(
                regions=filtered_regions,
                courts=[court_type],
                year=year,
                start_numbers=start_numbers
            )
    
    def _filter_regions_by_court_type(self, regions: List[str], 
                                    court_type: str) -> List[str]:
        """Фильтрация регионов по типу суда"""
        filtered_regions = []
        
        for region_key in regions:
            if region_key in self.parser.config['regions']:
                if court_type in self.parser.config['regions'][region_key]['courts']:
                    filtered_regions.append(region_key)
        
        return filtered_regions
    
    async def search_single_case(self, region_key: str, court_key: str, 
                       case_number: str, 
                       year: Optional[str] = None,
                       session: Optional[aiohttp.ClientSession] = None) -> Optional[Dict[str, Any]]:
        """Поиск одного конкретного дела"""
        return await self.parser.parse_specific_case_number(
            region_key=region_key,
            court_key=court_key,
            case_number=case_number,
            year=year,
            session=session
        )

    async def search_multiple_cases(self, region_key: str, court_key: str, 
                            case_numbers: List[str], 
                            year: Optional[str] = None,
                            session: Optional[aiohttp.ClientSession] = None) -> List[Dict[str, Any]]:
        """Поиск списка конкретных дел"""
        return await self.parser.parse_specific_case_numbers(
            region_key=region_key,
            court_key=court_key,
            case_numbers=case_numbers,
            year=year,
            session=session
        )

    async def search_by_full_case_number(self, 
                                       full_case_number: str) -> Optional[Dict[str, Any]]:
        """Поиск по полному номеру дела"""
        
        try:
            parsed_data = (
                self.parser.text_processor.parse_full_case_number(
                    full_case_number
                )
            )
            if not parsed_data:
                self.logger.error(f"Неверный формат номера дела: {full_case_number}")
                return None
            
            court_code = parsed_data['court_code']
            year_part = parsed_data['year_part']
            case_type = parsed_data['case_type']
            sequence_part = parsed_data['sequence']
            
            # Поиск подходящего региона и суда
            region_key, court_key = self._find_region_court_by_codes(
                court_code, case_type
            )
            
            if not region_key or not court_key:
                self.logger.error(f"Не найден регион/суд для кодов {court_code}/{case_type}")
                return None
            
            year = f"20{year_part}"
            
            self.logger.debug(f"Полный номер {full_case_number} -> {region_key} - {court_key}, {year}, {sequence_part}")
            
            return await self.search_single_case(
                region_key=region_key,
                court_key=court_key,
                case_number=sequence_part,
                year=year
            )
                
        except Exception as e:
            self.logger.error(f"Ошибка поиска по полному номеру {full_case_number}: {str(e)}")
            return None
    
    def _find_region_court_by_codes(self, court_code: str, 
                                  case_type: str) -> Tuple[Optional[str], Optional[str]]:
        """Найти регион и суд по кодам"""
        # Попытка с 2-значным кодом КАТО
        if len(court_code) >= 4:
            kato_2 = court_code[:2]
            instance_2 = court_code[2:]
            
            for r_key, region_config in self.parser.config['regions'].items():
                if region_config['kato_code'] == kato_2:
                    for c_key, court_config in region_config['courts'].items():
                        if (court_config['instance_code'] == instance_2 and 
                            court_config['case_type_code'] == case_type):
                            return r_key, c_key
        
        # Попытка с 3-значным кодом КАТО
        if len(court_code) >= 4:
            kato_3 = court_code[:3]
            instance_rest = court_code[3:]
            
            for r_key, region_config in self.parser.config['regions'].items():
                if region_config['kato_code'] == kato_3:
                    for c_key, court_config in region_config['courts'].items():
                        if (court_config['instance_code'] == instance_rest and 
                            court_config['case_type_code'] == case_type):
                            return r_key, c_key
        
        return None, None
    
    async def search_missing_numbers(self, region_key: str, court_key: str, 
                                   year: str) -> None:
        """Поиск только пропущенных номеров для конкретного региона/суда"""
        
        # Получаем названия для логов
        region_name = self.parser.config['regions'][region_key].get('name', region_key)  
        court_name = self.parser.config['regions'][region_key]['courts'][court_key].get('name', court_key)
        
        log_missing_search_start(self.logger, region_name, court_name, year)
        
        # Проверяем существование региона и суда
        if region_key not in self.parser.config['regions']:
            self.logger.error(f"Регион {region_key} не найден")
            return
            
        region_config = self.parser.config['regions'][region_key]
        if court_key not in region_config['courts']:
            self.logger.error(f"Суд {court_key} не найден в регионе {region_key}")
            return
            
        court_config = region_config['courts'][court_key]
        
        # Формируем паттерны для БД запроса
        kato_code = region_config['kato_code']
        instance_code = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        exact_prefix = f"{kato_code}{instance_code}-{year_short}-00-{case_type}"
        pattern_like = f"{exact_prefix}/%"
        pattern_regex = f"^{exact_prefix}/[0-9]+$"
        
        # Получаем статистику для определения диапазона
        stats = await self.parser.db_manager.get_prefix_statistics(
            pattern_like, pattern_regex
        )
        
        if stats['max_number'] == 0:
            self.logger.info("Нет данных для поиска пропущенных номеров")
            return
        
        # Получаем пропущенные номера от 1 до максимального найденного
        missing_numbers = await self.parser.db_manager.get_missing_numbers(
            pattern_like, pattern_regex, 1, stats['max_number']
        )
        
        if not missing_numbers:
            self.logger.info("Пропущенные номера не найдены")
            return
        
        # Формируем строку диапазонов для лога
        ranges_text = f"{missing_numbers[0]}-{missing_numbers[-1]}" if len(missing_numbers) > 1 else str(missing_numbers[0])
        if len(missing_numbers) > 20:
            ranges_text += "..."
        
        log_missing_found(self.logger, len(missing_numbers), ranges_text)
        
        # Получаем авторизованную сессию
        session = await self.parser.session_manager.get_session()
        self.logger.debug("Получена сессия для поиска пропущенных номеров")
        
        all_results = []
        total_processed = 0
        total_found = 0
        max_processed_number = 0

        try:
            # Подготавливаем форму поиска
            viewstate, form_ids = await self.parser._prepare_search_form(session)
            await self.parser.search_engine.select_region(
                session, viewstate, region_config['id'], form_ids
            )
            
            # Обрабатываем каждый пропущенный номер
            try:
                for i, missing_num in enumerate(missing_numbers, 1):
                    # ИСПРАВЛЕНО: Проверяем сигнал остановки
                    if self.parser.shutdown_manager.shutdown_requested:
                        self.logger.info("🛑 Поиск пропущенных номеров прерван по сигналу")
                        break
                    
                    if missing_num > max_processed_number:
                        max_processed_number = missing_num
                        
                    expected_full_number = f"{exact_prefix}/{missing_num}"
                    
                    self.logger.info(f"Проверяю пропуск: {expected_full_number}")
                    
                    total_processed += 1
                    
                    try:
                        # Передаем только порядковый номер
                        start_time = time.time()
                        results_html = (
                            await self.parser.search_engine.search_case(
                                session, viewstate, region_config['id'], 
                                court_config['id'], year, str(missing_num), 
                                form_ids
                            )
                        )
                        search_duration_ms = int((time.time() - start_time) * 1000)
                        
                        # Используем фильтрацию по точному совпадению
                        results = (
                            self.parser.results_parser.parse_search_results(
                                results_html, 
                                filter_by_case_number=expected_full_number
                            )
                        )
                        
                        if results:
                            total_found += len(results)
                            
                            result = results[0]
                            judge_exists = bool(result.get('judge'))
                            parties_count = len(result.get('plaintiffs', [])) + len(result.get('defendants', []))
                            events_count = len(result.get('events', []))
                            
                            self.logger.info(f"НАЙДЕН: {'судья есть' if judge_exists else 'судья отсутствует'}, "
                                           f"{parties_count} стороны, {events_count} события")
                            self.logger.info(f"Сохранено: новое дело {expected_full_number}")
                            
                            all_results.extend(results)
                            
                            # Промежуточное сохранение каждые 10 дел
                            if len(all_results) >= 10:
                                stats = await self.parser.db_manager.batch_save_cases(
                                    all_results
                                )
                                self.logger.debug(f"Промежуточное сохранение: {len(all_results)} дел, статистика: {stats}")
                                all_results.clear()
                                
                        else:
                            self.logger.info("Пропуск подтвержден: дело не существует")
                        
                        # Применяем задержку между запросами
                        await self.parser._apply_search_delay(bool(results))
                        
                    except Exception as e:
                        self.logger.error(f"Ошибка поиска пропущенного номера {missing_num}: {str(e)}")
                        continue
                        
            except (KeyboardInterrupt, asyncio.CancelledError) as e:
                self.logger.warning(f"🛑 Поиск прерван ({type(e).__name__}), обработано {total_processed} номеров")
                
            except Exception as e:
                log_critical(self.logger, f"критическая ошибка поиска: {str(e)}")
                raise
                
            finally:
                # КРИТИЧНО: Сохраняем все накопленные результаты даже при прерывании
                if all_results:
                    try:
                        stats = await self.parser.db_manager.batch_save_cases(
                            all_results
                        )
                        self.logger.debug(f"Финальное сохранение: {len(all_results)} дел, статистика: {stats}")
                    except Exception as save_error:
                        log_critical(self.logger, f"не удалось сохранить финальный батч: {str(save_error)}")
                
                # Финальная статистика
                if total_processed > 0:
                    found_percentage = (
                        (total_found / total_processed * 100) 
                        if total_processed > 0 else 0
                    )
                    self.logger.info(f"✅ Поиск пропусков завершен: найдено {total_found} новых дела "
                                   f"из {total_processed} проверенных ({round(found_percentage, 1)}%)")
                    
        except Exception as outer_error:
            log_critical(self.logger, f"не удалось инициализировать поиск: {str(outer_error)}")
            raise
            
        finally:
            # Возвращаем сессию в пул в любом случае
            try:
                await self.parser.session_manager.return_session(session)
                self.logger.debug("Сессия поиска пропущенных номеров возвращена в пул")
            except Exception as session_error:
                self.logger.error(f"Ошибка возврата сессии: {str(session_error)}")

# ============================================================================
# ОСНОВНЫЕ ФУНКЦИИ
# ============================================================================

@asynccontextmanager
async def create_court_parser(config_path: Optional[str] = None):
    """Асинхронный контекстный менеджер для создания парсера"""
    parser = CourtParser(config_path)
    async with parser:
        yield parser

async def main() -> None:
    
    # ========================================================================
    # КОНФИГУРАЦИЯ РЕЖИМА ПАРСИНГА
    # ========================================================================
    
    selected_mode = "mass_regions_all"  # Измените режим здесь
    
    # ========================================================================
    # ИНИЦИАЛИЗАЦИЯ КОМПОНЕНТОВ
    # ========================================================================
    
    logger = None
    
    try:
        # Загружаем конфигурацию
        config_path = Path(__file__).parent / 'config.json'
        if not config_path.exists():
            raise ConfigurationError(
                f"Файл конфигурации не найден: {config_path}"
            )
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Проверяем и валидируем ограничение сессий ДО создания парсера
        max_sessions = config.get('max_concurrent_sessions')
        
        if max_sessions is None:
            max_sessions = 5  # значение по умолчанию
        elif not isinstance(max_sessions, int) or max_sessions < 1:
            max_sessions = 5
        elif max_sessions > 20:
            pass  # Оставляем как есть, но будем предупреждать в логах
        
        # Настройка логирования
        setup_logging(
            log_dir="logs",
            console_level="INFO",
            file_level="DEBUG"
        )
        logger = get_logger('main')

        # Детальное логирование конфигурации
        logger.info(f"🔧 Применяю настройки из конфига: максимум {max_sessions} одновременных сессий")

        if max_sessions > 10:
            logger.warning(f"Высокое значение параллельности ({max_sessions}), возможна нагрузка на сервер")
        elif max_sessions == 1:
            logger.info("Включен консервативный режим (1 сессия)")
        elif max_sessions <= 3:
            logger.info(f"Консервативный режим: {max_sessions} сессии")

        logger.info("Инициализация компонентов завершена")
        
    except Exception as e:
        if logger:
            logger.critical(f"Не удалось инициализировать компоненты: {str(e)}")
        raise ConfigurationError(f"Не удалось инициализировать компоненты: {e}")
    
    # ========================================================================
    # ОСНОВНОЙ ПАРСИНГ
    # ========================================================================
    
    try:
        logger.info(f"Запуск парсера в режиме: {selected_mode}")
        
        # 🚀 ОБНОВЛЕНО: информация о 4-этапном парсинге для массовых режимов
        if selected_mode in [
            "mass_regions", "mass_regions_all", 
            "mass_regions_all_courts", "mass_regions_everything",
            "mass_custom"
        ]:
            logger.info("🚀 ЗАПУСК 4-ЭТАПНОГО ПАРСИНГА:")
            logger.info("   ЭТАП 1: Основной парсинг всех регионов")
            logger.info("   ЭТАП 2: Автоматический поиск пропущенных номеров")  
            logger.info("   ЭТАП 3: Глобальное обновление судей для всех дел СМАС")
            logger.info("   ЭТАП 4: Обновление истории дел (новые события)")
        
        async with create_court_parser() as parser:
            coordinator = ParsingCoordinator(parser)
            
            # Критическая проверка что лимит сессий применился
            actual_limit = coordinator.max_concurrent_sessions
            expected_limit = max_sessions
            
            if actual_limit != expected_limit:
                logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: лимит сессий не применился!")
                logger.error(f"   Ожидалось: {expected_limit}, получилось: {actual_limit}")
                raise ConfigurationError(f"Не удалось применить лимит сессий из конфига")
            else:
                logger.info(f"✅ Лимит сессий корректно применен: {actual_limit}")
            
            # Получение конфигурации выбранного режима
            mode_config = parser.config_manager.get_mode_config(selected_mode)
            if not mode_config:
                available_modes = parser.config_manager.list_available_modes()
                logger.critical(f"Режим {selected_mode} не найден. Доступные режимы: {available_modes}")
                raise ConfigurationError(f"Неизвестный режим: {selected_mode}")
            
            logger.debug(f"Конфигурация режима {selected_mode} загружена")
            
            # Валидация конфигурации
            if not parser.config_manager.validate_mode_config(selected_mode):
                logger.critical(f"Неверная конфигурация режима {selected_mode}")
                raise ConfigurationError(f"Невалидная конфигурация режима: {selected_mode}")
            
            logger.info(f"Начинаю парсинг в режиме {selected_mode}")
            
            # ================================================================
            # ВЫПОЛНЕНИЕ ПАРСИНГА В ЗАВИСИМОСТИ ОТ РЕЖИМА
            # ================================================================
            
            if selected_mode == "single_case":
                case_number = mode_config['full_case_number']
                logger.info(f"Режим поиска одного дела: {case_number}")
                
                result = await coordinator.search_by_full_case_number(
                    full_case_number=case_number
                )
                
                if result:
                    judge = result.get('judge', 'Судья не указан')
                    logger.info(f"Дело найдено и сохранено: {result.get('case_number', 'N/A')}, судья: {judge}")
                else:
                    logger.info(f"Дело не найдено: {case_number}")
                    
            elif selected_mode == "multiple_cases":
                case_count = len(mode_config['case_numbers'])
                logger.info(f"Режим поиска нескольких дел: {case_count} дел в {mode_config['region_key']} - {mode_config['court_key']}")
                
                results = await coordinator.search_multiple_cases(
                    region_key=mode_config['region_key'],
                    court_key=mode_config['court_key'],
                    case_numbers=mode_config['case_numbers'],
                    year=mode_config['year']
                )
                
                found_count = len(results)
                success_rate = (
                    (found_count / case_count * 100) if case_count > 0 else 0
                )
                
                logger.info(f"Поиск нескольких дел завершен: найдено {found_count} из {case_count} ({round(success_rate, 1)}% успеха)")
                
                if found_count > 0:
                    examples = []
                    for i, result in enumerate(results[:3], 1):  # Показываем первые 3
                        case_num = result.get('case_number', 'N/A')
                        judge = result.get('judge', 'Судья не указан')
                        examples.append(f"{case_num} (судья: {judge})")
                    
                    logger.info(f"Примеры найденных дел: {'; '.join(examples)}")

            elif selected_mode == "range_search":
                start_num = mode_config['start_number']
                end_num = mode_config['end_number']
                case_numbers = [str(i) for i in range(start_num, end_num + 1)]
                
                logger.info(f"Режим поиска диапазона: {start_num}-{end_num} ({len(case_numbers)} дел) "
                           f"в {mode_config['region_key']} - {mode_config['court_key']}")
                
                results = await coordinator.search_multiple_cases(
                    region_key=mode_config['region_key'],
                    court_key=mode_config['court_key'],
                    case_numbers=case_numbers,
                    year=mode_config['year']
                )
                
                found_count = len(results)
                found_percentage = (found_count / len(case_numbers)) * 100
                
                logger.info(f"Поиск диапазона завершен: найдено {found_count} из {len(case_numbers)} дел ({round(found_percentage, 1)}%)")
                
                if found_count > 0:
                    found_numbers = [] 
                    for r in results:
                        case_num = r.get('case_number', '')
                        if '/' in case_num:
                            try:
                                num = int(case_num.split('/')[-1])
                                found_numbers.append(num)
                            except ValueError:
                                continue
                    
                    if found_numbers:
                        logger.info(f"Диапазон найденных номеров: {min(found_numbers)}-{max(found_numbers)}")

            elif selected_mode in [
                "mass_regions", "mass_regions_all", 
                "mass_regions_all_courts", "mass_regions_everything"
            ]:
                # 🚀 МАССОВЫЕ РЕЖИМЫ - ТЕПЕРЬ ВЫПОЛНЯЮТ ВСЕ 4 ЭТАПА
                target_regions = mode_config.get('target_regions')
                court_type = mode_config.get('court_type')
                
                # Определяем параметры для каждого режима
                if selected_mode == "mass_regions":
                    regions_desc = (
                        f"{len(target_regions)} выбранных регионов" 
                        if target_regions else "все регионы"
                    )
                    courts_desc = (
                        f"суд типа {court_type}" if court_type else "все суды"
                    )
                elif selected_mode == "mass_regions_all":
                    target_regions = None  # ВСЕ регионы
                    regions_desc = "ВСЕ регионы"
                    courts_desc = f"суд типа {court_type}"
                elif selected_mode in [
                    "mass_regions_all_courts", "mass_regions_everything"
                ]:
                    target_regions = None  # ВСЕ регионы
                    court_type = None      # ВСЕ суды
                    regions_desc = "ВСЕ регионы"
                    courts_desc = "ВСЕ типы судов"
                
                logger.info(f"Массовый парсинг: {regions_desc}, {courts_desc}, год {mode_config['year']}")
                
                # Подсчитываем ожидаемое количество задач
                all_regions = target_regions or list(config['regions'].keys())
                if court_type:
                    # Фильтруем регионы, где есть данный тип суда
                    valid_regions = []
                    for r in all_regions:
                        region_config_item = config['regions'].get(r, {})
                        if court_type in region_config_item.get('courts', {}):
                            valid_regions.append(r)
                    expected_tasks = len(valid_regions)
                else:
                    # Подсчитываем все комбинации регион-суд
                    expected_tasks = 0
                    for r in all_regions:
                        if r in config['regions']:
                            courts_count = len(
                                config['regions'][r].get('courts', {})
                            )
                            expected_tasks += courts_count
                
                # Расчет времени с учетом ограничения
                estimated_hours = expected_tasks / max_sessions * 0.5  # Примерно 30 мин на задачу
                
                logger.info(f"Ожидается задач: {expected_tasks}, примерное время: {round(estimated_hours, 1)} часов "
                           f"(при {max_sessions} параллельных сессиях)")
                
                # 🚀 ЗАПУСК ВСЕХ 4 ЭТАПОВ (включая новый этап обновления истории)
                await coordinator.run_by_court_type(
                    court_type=court_type,
                    regions=target_regions,
                    year=mode_config['year']
                )
                    
            elif selected_mode == "mass_custom":
                # 🚀 КАСТОМНЫЙ РЕЖИМ - ТОЖЕ ВЫПОЛНЯЕТ ВСЕ 4 ЭТАПА
                regions_count = len(mode_config['target_regions'])
                courts_count = len(mode_config['courts'])
                
                logger.info(f"Кастомный парсинг: {regions_count} регионов, {courts_count} типов судов, год {mode_config['year']}")
                logger.debug(f"Регионы: {mode_config['target_regions'][:3]}{'...' if regions_count > 3 else ''}")
                logger.debug(f"Суды: {mode_config['courts']}")
                
                start_numbers = mode_config.get('start_numbers', {})
                if start_numbers:
                    logger.debug(f"Настроены стартовые номера для {len(start_numbers)} комбинаций")
                
                # 🚀 ЗАПУСК ВСЕХ 4 ЭТАПОВ (включая новый этап обновления истории)
                await coordinator.run_optimized_parsing(
                    regions=mode_config['target_regions'],
                    courts=mode_config['courts'],
                    year=mode_config['year'],
                    start_numbers=start_numbers
                )

            elif selected_mode == "missing_numbers":
                region_name = config['regions'][mode_config['region_key']].get('name', mode_config['region_key'])
                court_name = config['regions'][mode_config['region_key']]['courts'][mode_config['court_key']].get('name', mode_config['court_key'])
                
                logger.info(f"Режим поиска пропущенных номеров: {region_name} - {court_name}, {mode_config['year']}")
                
                await coordinator.search_missing_numbers(
                    region_key=mode_config['region_key'], court_key=mode_config['court_key'],
                    year=mode_config['year']
                )
                
                logger.info("Поиск пропущенных номеров завершен")

            elif selected_mode in ["judges_update_only"]:
                logger.info("🎯 РЕЖИМ: Только обновление судей для дел СМАС")
                logger.info("Поиск и обновление всех дел СМАС без судей...")
                
                year_for_update = mode_config.get('year', config['search_parameters']['default_year'])
                max_cases = mode_config.get('max_cases_to_process', 2000)
                
                logger.info(f"Параметры: год {year_for_update}, макс. дел {max_cases}")
                
                try:
                    await coordinator._run_global_judge_updates(year_for_update)
                    logger.info("✅ Обновление судей успешно завершено")
                except Exception as update_error:
                    logger.error(f"❌ Ошибка обновления судей: {str(update_error)}")

            # ================================================================
            # НОВЫЙ НЕЗАВИСИМЫЙ РЕЖИМ: ОБНОВЛЕНИЕ ИСТОРИИ ДЕЛ
            # ================================================================
            
            elif selected_mode == "history_update":
                # Новый независимый режим обновления истории дел
                year_for_update = mode_config.get('filters', {}).get('year')
                
                if not year_for_update:
                    year_for_update = config['search_parameters']['default_year']
                    logger.info(f"Год не указан в режиме, используется по умолчанию: {year_for_update}")
                
                logger.info(f"🔄 НЕЗАВИСИМЫЙ РЕЖИМ: Обновление истории дел за {year_for_update} год")
                
                # Показываем настройки фильтров
                filters = mode_config.get('filters', {})
                defendant_keywords = filters.get('defendant_keywords', [])
                excluded_statuses = filters.get('excluded_statuses', [])
                max_cases = filters.get('max_cases_to_process', 1000)
                batch_size = mode_config.get('batch_size', 50)
                
                logger.info(f"📋 Настройки фильтрации:")
                logger.info(f"   • Ключевые слова ответчиков: {defendant_keywords}")
                logger.info(f"   • Исключенные статусы: {len(excluded_statuses)} шт.")
                logger.info(f"   • Максимум дел для обработки: {max_cases}")
                logger.info(f"   • Размер батча: {batch_size}")
                
                try:
                    stats = await parser.run_history_update_mode(year_for_update)
                    
                    logger.info(f"✅ Обновление истории завершено успешно")
                    
                    # Детальная статистика
                    if stats['processed_cases'] > 0:
                        update_rate = (stats['updated_cases'] / stats['processed_cases']) * 100
                        logger.info(f"📊 Итоговая статистика:")
                        logger.info(f"   • Найдено дел для проверки: {stats['found_cases']}")
                        logger.info(f"   • Обработано дел: {stats['processed_cases']}")
                        logger.info(f"   • Обновлено дел: {stats['updated_cases']}")
                        logger.info(f"   • Без изменений: {stats['no_updates_needed']}")
                        logger.info(f"   • Процент обновлений: {round(update_rate, 1)}%")
                    else:
                        logger.info("📊 Дела для обработки не найдены")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка режима обновления истории: {str(e)}")
                    raise
        
        # ====================================================================
        # ЗАВЕРШЕНИЕ УСПЕШНОГО ПАРСИНГА
        # ====================================================================
        
        logger.info("✅ Парсинг успешно завершен")
        
    except KeyboardInterrupt:
        if logger:
            logger.warning("🛑 Программа прервана пользователем")
        raise 
    except asyncio.CancelledError:
        if logger:
            logger.warning("🛑 Парсинг отменен")
        raise
        
    except Exception as e:
        if logger:
            logger.critical(f"💥 Критическая ошибка ({type(e).__name__}): {str(e)}")
        raise
    
    finally:
        # ====================================================================
        # ФИНАЛЬНАЯ ОЧИСТКА
        # ====================================================================
        
        try:
            if logger:
                logger.debug("Ресурсы очищены")
            
        except Exception as cleanup_error:
            if logger:
                logger.error(f"Ошибка очистки: {str(cleanup_error)}")

    return 0  # Успешное завершение

# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code or 0)
    except KeyboardInterrupt:
        # Финальное сообщение в stderr когда логгер может быть недоступен
        sys.stderr.write("\n🛑 Программа прервана пользователем\n")
        sys.stderr.flush()
        sys.exit(1)
    except Exception as e:
        # Финальное сообщение об ошибке когда логгер может быть недоступен
        import sys
        sys.stderr.write(f"\n💥 Фатальная ошибка: {e}\n")
        sys.stderr.flush()
        sys.exit(1)