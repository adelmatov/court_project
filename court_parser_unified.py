#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Объединенный файл парсера суда
Дата сборки: 2025-10-18 15:38:23
Автор: Court Parser Team

Этот файл содержит все модули проекта, объединенные в один файл.
Для запуска: python court_parser_unified.py

Структура:
- Утилиты (логирование, валидация, обработка текста)
- Конфигурация
- Аутентификация
- База данных
- Парсинг HTML
- Поисковые функции
- Основная логика
"""

# ============================================================================
# СТАНДАРТНЫЕ БИБЛИОТЕКИ
# ============================================================================
import os
import sys
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any


# Внешние библиотеки
from dataclasses import dataclass, field
from datetime import date
from datetime import datetime
from datetime import datetime, date
from pathlib import Path
from selectolax.parser import HTMLParser
from typing import Dict, Any, Optional
from typing import Dict, Any, Optional, List
from typing import Dict, List, Optional, Any
from typing import Dict, Optional
from typing import List, Optional
from typing import List, Optional, Dict
from typing import List, Tuple, Optional
from typing import Optional, Dict, Any
from typing import Optional, Dict, List, Any
from typing import Optional, List
import aiohttp
import asyncio
import asyncpg
import json
import logging
import re
import ssl
import sys
import traceback


# ============================================================================
# МОДУЛЬ: parsers/court_parser/utils/logger.py
# ============================================================================

"""
Простое логирование с цветным выводом
"""


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


# ============================================================================
# МОДУЛЬ: parsers/court_parser/utils/text_processor.py
# ============================================================================

"""
Обработка и очистка текста
"""


class TextProcessor:
    """Обработчик текста"""
    
    @staticmethod
    def clean(text: str) -> str:
        """Очистка текста от лишних пробелов"""
        if not text:
            return ""
        return ' '.join(text.split()).strip()
    
    @staticmethod
    def parse_date(date_str: str, format_str: str = '%d.%m.%Y') -> Optional[datetime]:
        """
        Парсинг даты с автоматическим исправлением года
        
        Примеры:
        '15.01.2025' → 2025-01-15
        '15.01.25' → 2025-01-15 (автоисправление)
        '15.01.1925' → None (некорректный год)
        """
        try:
            # Попытка парсинга
            parsed = datetime.strptime(date_str.strip(), format_str)
            
            # ИСПРАВЛЕНИЕ ГОДА
            year = parsed.year
            
            # Если год двузначный (0-99), добавляем 2000
            if year < 100:
                year = 2000 + year
                parsed = parsed.replace(year=year)
            
            # Если год в диапазоне 1900-1999, пытаемся исправить
            elif 1900 <= year < 2000:
                # Берем последние 2 цифры и добавляем 2000
                year_last_two = year % 100
                year = 2000 + year_last_two
                parsed = parsed.replace(year=year)
            
            # ВАЛИДАЦИЯ: год должен быть в разумном диапазоне
            if not (2000 <= parsed.year <= 2030):
                return None
            
            return parsed
            
        except (ValueError, AttributeError):
            return None
    
    @staticmethod
    def split_parties(text: str) -> List[str]:
        """
        Улучшенное разделение сторон
        
        Правила:
        1. Разделение по запятой (основное)
        2. Разделение после закрывающей кавычки + пробел + заглавная буква
        
        Примеры:
        'ТОО "Компания", Иванов' → ['ТОО "Компания"', 'Иванов']
        'ТОО "Компания" ИВАНОВ' → ['ТОО "Компания"', 'ИВАНОВ']
        'Петров, Сидоров' → ['Петров', 'Сидоров']
        """
        
        if not text.strip():
            return []
        
        # ШАГ 1: Добавляем запятые после кавычек перед заглавными буквами
        # Паттерн: кавычка + пробелы + заглавная буква (начало ФИО или организации)
        # Примеры: '" ИВАНОВ', '» ТОО', '" Государственное'
        
        text = re.sub(
            r'(["\»"„])\s+([А-ЯЁ][А-ЯЁа-яё\s]+)',  # После кавычки + пробел + заглавная
            r'\1, \2',  # Вставляем запятую
            text
        )
        
        # ШАГ 2: Разделяем по запятым с учетом кавычек
        parts = []
        current = ""
        in_quotes = False
        quote_chars = {'"', '»', '"', '„', '«'}
        
        for i, char in enumerate(text):
            if char in quote_chars:
                in_quotes = not in_quotes
            
            if char == ',' and not in_quotes:
                # Запятая вне кавычек - разделяем
                part = current.strip(' .,;-')
                if part and len(part) >= 5:  # Минимум 5 символов
                    parts.append(part)
                current = ""
            else:
                current += char
        
        # Последняя часть
        part = current.strip(' .,;-')
        if part and len(part) >= 5:
            parts.append(part)
        
        return parts
    
    @staticmethod
    def parse_case_number(case_number: str) -> Optional[Dict[str, str]]:
        """
        Парсинг номера дела
        
        Пример: "6294-25-00-4/215" →
        {
            'court_code': '6294',
            'year': '25',
            'middle': '00',
            'case_type': '4',
            'sequence': '215'
        }
        """
        pattern = r'^(\d+)-(\d+)-(\d+)-([0-9а-яА-Я]+)/(\d+)$'
        match = re.match(pattern, case_number)
        
        if not match:
            return None
        
        return {
            'court_code': match.group(1),
            'year': match.group(2),
            'middle': match.group(3),
            'case_type': match.group(4),
            'sequence': match.group(5)
        }
    
    @staticmethod
    def generate_case_number(region_config: Dict, court_config: Dict, 
                           year: str, sequence: int) -> str:
        """
        Генерация номера дела
        
        Формат: КАТО+instance-год-00-тип/порядковый
        Пример: 6294-25-00-4/215
        """
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        return f"{kato}{instance}-{year_short}-00-{case_type}/{sequence}"
    
    @staticmethod
    def parse_full_case_number(case_number: str) -> Optional[Dict]:
        """
        Распарсить полный номер дела
        
        Вход: "6294-25-00-4/215"
        Выход: {
            'court_code': '6294',
            'year_short': '25',
            'case_type': '4',
            'sequence': '215'
        }
        """
        pattern = r'^(\d+)-(\d+)-(\d+)-([0-9а-яА-Я]+)/(\d+)$'
        match = re.match(pattern, case_number)
        
        if not match:
            return None
        
        return {
            'court_code': match.group(1),
            'year_short': match.group(2),
            'middle': match.group(3),
            'case_type': match.group(4),
            'sequence': match.group(5)
        }

@staticmethod
def find_region_and_court_by_case_number(case_number: str, regions_config: Dict) -> Optional[Dict]:
    """
    Определить region_key и court_key по номеру дела
    
    Args:
        case_number: полный номер дела "6294-25-00-4/215"
        regions_config: конфигурация регионов из settings
    
    Returns:
        {
            'region_key': 'astana',
            'court_key': 'smas',
            'year': '2025',
            'sequence': '215'
        }
    """
    parsed = TextProcessor.parse_full_case_number(case_number)
    if not parsed:
        return None
    
    # Извлекаем код суда (КАТО + инстанция)
    court_code = parsed['court_code']
    case_type = parsed['case_type']
    
    # Ищем регион и суд по коду
    for region_key, region_config in regions_config.items():
        kato = region_config['kato_code']
        
        for court_key, court_config in region_config['courts'].items():
            instance = court_config['instance_code']
            full_code = f"{kato}{instance}"
            
            if court_code == full_code and court_config['case_type_code'] == case_type:
                # Восстанавливаем полный год из короткого
                year_short = int(parsed['year_short'])
                year = f"20{year_short:02d}"
                
                return {
                    'region_key': region_key,
                    'court_key': court_key,
                    'year': year,
                    'sequence': parsed['sequence']
                }
    
    return None


# ============================================================================
# МОДУЛЬ: parsers/court_parser/utils/validators.py
# ============================================================================

"""
Валидация данных
"""


class ValidationError(Exception):
    """Ошибка валидации"""
    pass


class DataValidator:
    """Валидатор данных"""
    
    @staticmethod
    def validate_case_data(data: Dict[str, Any]) -> bool:
        """Валидация данных дела"""
        # Обязательные поля
        if not data.get('case_number'):
            raise ValidationError("Отсутствует номер дела")
        
        # Проверка длины
        if len(data['case_number']) > 100:
            raise ValidationError("Номер дела слишком длинный")
        
        # Проверка даты
        if data.get('case_date'):
            date = data['case_date']
            if isinstance(date, datetime):
                if not (1990 <= date.year <= 2030):
                    raise ValidationError(f"Некорректный год: {date.year}")
        
        # Проверка судьи
        if data.get('judge') and len(data['judge']) > 200:
            raise ValidationError("Имя судьи слишком длинное")
        
        return True
    
    @staticmethod
    def validate_party_name(name: str) -> bool:
        """
        Валидация имени стороны
        
        Правила:
        - Минимум 5 символов
        - Максимум 500 символов
        - Не только цифры
        - Не только аббревиатура из 2-3 букв
        """
        if not name or not name.strip():
            return False
        
        name = name.strip()
        
        # Минимальная длина
        if len(name) < 5:
            return False
        
        # Максимальная длина
        if len(name) > 500:
            return False
        
        # Не только цифры
        if name.isdigit():
            return False
        
        # Не короткие аббревиатуры (АО, ТОО без названия)
        if len(name) < 10 and name.replace(' ', '').replace('"', '').isupper():
            return False
        
        return True
    
    @staticmethod
    def validate_event(event: Dict[str, Any]) -> bool:
        """Валидация события"""
        if not event.get('event_type'):
            return False
        
        if not event.get('event_date'):
            return False
        
        if len(event['event_type']) > 300:
            raise ValidationError("Тип события слишком длинный")
        
        return True


# ============================================================================
# МОДУЛЬ: parsers/court_parser/config/settings.py
# ============================================================================

"""
Загрузка и валидация конфигурации
"""


class ConfigurationError(Exception):
    """Ошибка конфигурации"""
    pass


class Settings:
    """Настройки парсера"""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config.json'
        
        self.config = self._load_config(config_path)
        self._validate()
    
    def _load_config(self, path: Path) -> Dict[str, Any]:
        """Загрузка конфигурации из JSON"""
        if not path.exists():
            raise ConfigurationError(f"Файл конфигурации не найден: {path}")
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Ошибка парсинга JSON: {e}")
    
    def _validate(self):
        """Валидация обязательных полей"""
        required = ['auth', 'base_url', 'database', 'regions', 'parsing_settings']
        for field in required:
            if field not in self.config:
                raise ConfigurationError(f"Отсутствует поле: {field}")
    
    @property
    def base_url(self) -> str:
        return self.config['base_url']
    
    @property
    def auth(self) -> Dict[str, str]:
        return self.config['auth']
    
    @property
    def database(self) -> Dict[str, Any]:
        return self.config['database']
    
    @property
    def regions(self) -> Dict[str, Any]:
        return self.config['regions']
    
    @property
    def parsing_settings(self) -> Dict[str, Any]:
        """Настройки парсинга"""
        return self.config['parsing_settings']
    
    @property
    def retry_settings(self) -> Dict[str, Any]:
        """Настройки retry"""
        return self.config.get('retry_settings', {})
    
    def get_region(self, region_key: str) -> Dict[str, Any]:
        """Получить конфигурацию региона"""
        if region_key not in self.regions:
            raise ConfigurationError(f"Регион не найден: {region_key}")
        return self.regions[region_key]
    
    def get_court(self, region_key: str, court_key: str) -> Dict[str, Any]:
        """Получить конфигурацию суда"""
        region = self.get_region(region_key)
        if court_key not in region['courts']:
            raise ConfigurationError(f"Суд не найден: {court_key}")
        return region['courts'][court_key]
    
    def get_target_regions(self) -> List[str]:
        """Получить список целевых регионов"""
        target = self.parsing_settings.get('target_regions')
        
        if target is None:
            # Все регионы
            return list(self.regions.keys())
        elif isinstance(target, list):
            # Конкретные регионы
            return target
        else:
            raise ConfigurationError("target_regions должен быть null или списком")
    
    def get_limit_regions(self) -> Optional[int]:
        """Лимит регионов для обработки"""
        return self.parsing_settings.get('limit_regions')
    
    def get_limit_cases_per_region(self) -> Optional[int]:
        """Лимит дел на регион"""
        return self.parsing_settings.get('limit_cases_per_region')
    
    @property
    def update_settings(self) -> Dict[str, Any]:
        """Настройки обновления"""
        return self.config.get('update_settings', {})


# ============================================================================
# МОДУЛЬ: parsers/court_parser/auth/authenticator.py
# ============================================================================

"""
Авторизация на сайте office.sud.kz
"""



# REMOVED IMPORT: from utils.logger import get_logger
# REMOVED IMPORT: from utils.retry import RetryStrategy, RetryConfig, NonRetriableError


class AuthenticationError(Exception):
    """Ошибка авторизации"""
    pass


class Authenticator:
    """Класс авторизации с retry"""
    
    def __init__(self, base_url: str, auth_config: Dict[str, str], 
                 retry_config: Optional[Dict] = None):
        self.base_url = base_url
        self.login = auth_config['login']
        self.password = auth_config['password']
        self.user_name = auth_config['user_name']
        self.logger = get_logger('authenticator')
        
        self.retry_config = retry_config or {}
    
    async def authenticate(self, session_manager) -> bool:
        """
        Полный процесс авторизации с retry
        
        Args:
            session_manager: SessionManager для создания новых сессий
        """
        auth_retry_config = self.retry_config.get('authentication', {})
        
        if not auth_retry_config:
            # Без retry
            return await self._do_authenticate(session_manager)
        
        # С retry
        retry_cfg = RetryConfig(auth_retry_config)
        strategy = RetryStrategy(retry_cfg)
        
        create_new_session = auth_retry_config.get('create_new_session', True)
        
        async def _auth_with_session_reset():
            try:
                return await self._do_authenticate(session_manager)
            except Exception as e:
                # При ошибке создаем новую сессию (если настроено)
                if create_new_session:
                    self.logger.debug("Создаю новую сессию перед retry...")
                    await session_manager.create_session()
                raise
        
        try:
            result = await strategy.execute_with_retry(
                _auth_with_session_reset,
                error_context="Авторизация"
            )
            return result
        
        except Exception as e:
            self.logger.error(f"❌ Авторизация не удалась: {e}")
            raise AuthenticationError(f"Не удалось авторизоваться: {e}") from e
    
    async def _do_authenticate(self, session_manager) -> bool:
        """Один цикл авторизации"""
        session = await session_manager.get_session()
        
        self.logger.info("Начинаю авторизацию...")
        
        # Этап 1: Главная страница
        viewstate = await self._load_main_page(session)
        await asyncio.sleep(1)
        
        # Этап 2: Смена языка
        await self._switch_to_russian(session, viewstate)
        await asyncio.sleep(0.5)
        
        # Этап 3: Логин
        await self._perform_login(session, viewstate)
        await asyncio.sleep(0.5)
        
        # Этап 4: Проверка
        is_authenticated = await self._verify_authentication(session)
        
        if is_authenticated:
            self.logger.info("✅ Авторизация успешна")
            return True
        else:
            # Проверка не прошла - это может быть как временная, так и постоянная ошибка
            retriable_on_fail = self.retry_config.get('authentication', {}).get(
                'retriable_on_auth_check_fail', True
            )
            
            if retriable_on_fail:
                raise AuthenticationError("Проверка авторизации не пройдена")
            else:
                raise NonRetriableError("Проверка авторизации не пройдена (неверные учетные данные?)")
    
    async def _load_main_page(self, session: aiohttp.ClientSession) -> Optional[str]:
        """Загрузка главной страницы и извлечение ViewState"""
        url = f"{self.base_url}/"
        headers = self._get_base_headers()
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise AuthenticationError(f"HTTP {response.status} при загрузке главной")
            
            html = await response.text()
            viewstate = self._extract_viewstate(html)
            
            self.logger.debug("Главная страница загружена")
            return viewstate
    
    async def _switch_to_russian(self, session: aiohttp.ClientSession, 
                                 viewstate: Optional[str]):
        """Смена языка на русский"""
        url = f"{self.base_url}/index.xhtml"
        
        # Получаем текущую страницу для ID элементов
        async with session.get(url, headers=self._get_base_headers()) as response:
            html = await response.text()
            current_viewstate = self._extract_viewstate(html)
        
        # Формируем данные для смены языка
        data = {
            'f_l_temp': 'f_l_temp',
            'javax.faces.ViewState': current_viewstate or viewstate,
            'javax.faces.source': 'f_l_temp:js_temp_1',
            'javax.faces.partial.execute': 'f_l_temp:js_temp_1 @component',
            'javax.faces.partial.render': '@component',
            'param1': f'{self.base_url}/',
            'org.richfaces.ajax.component': 'f_l_temp:js_temp_1',
            'f_l_temp:js_temp_1': 'f_l_temp:js_temp_1',
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        headers = self._get_ajax_headers()
        headers['Referer'] = f'{self.base_url}/'
        
        async with session.post(url, data=data, headers=headers) as response:
            if response.status != 200:
                raise AuthenticationError(f"HTTP {response.status} при смене языка")
            
            self.logger.debug("Язык переключен на русский")
    
    async def _perform_login(self, session: aiohttp.ClientSession, 
                            viewstate: Optional[str]):
        """Отправка логина и пароля"""
        url = f"{self.base_url}/index.xhtml"
        
        # Получаем форму авторизации
        async with session.get(url, headers=self._get_base_headers()) as response:
            html = await response.text()
            auth_ids = self._extract_auth_form_ids(html)
            current_viewstate = self._extract_viewstate(html)
        
        # Извлекаем ID элементов
        form_base = auth_ids.get('form_base', 'j_idt82:auth')
        submit_button = auth_ids.get('submit_button')
        
        # ВАЖНО: Если кнопка не найдена - используем дефолт
        if not submit_button:
            submit_button = f'{form_base}:j_idt89'
            self.logger.warning(f"ID кнопки не найден, используется дефолт: {submit_button}")
        else:
            self.logger.debug(f"Используется ID кнопки: {submit_button}")
        
        # Формируем данные для логина
        data = {
            form_base: form_base,
            f'{form_base}:xin': self.login,
            f'{form_base}:password': self.password,
            'javax.faces.ViewState': current_viewstate or viewstate,
            'javax.faces.source': submit_button,
            'javax.faces.partial.event': 'click',
            'javax.faces.partial.execute': f'{submit_button} @component',
            'javax.faces.partial.render': '@component',
            'org.richfaces.ajax.component': submit_button,
            submit_button: submit_button,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        headers = self._get_ajax_headers()
        headers['Referer'] = f'{self.base_url}/index.xhtml'
        
        async with session.post(url, data=data, headers=headers) as response:
            if response.status != 200:
                raise AuthenticationError(f"HTTP {response.status} при отправке логина")
            
            self.logger.debug("Логин и пароль отправлены")
    
    async def _verify_authentication(self, session: aiohttp.ClientSession) -> bool:
        """Проверка успешности авторизации"""
        url = f"{self.base_url}/form/proceedings/services.xhtml"
        
        async with session.get(url, headers=self._get_base_headers()) as response:
            if response.status != 200:
                self.logger.error(f"HTTP {response.status} при проверке авторизации")
                return False
            
            html = await response.text()
            
            # Проверяем наличие элементов авторизованной страницы
            checks = {
                'profile-context-menu': 'profile-context-menu' in html,
                'Выйти': 'Выйти' in html,
                'logout()': 'logout()' in html,
                'userInfo.xhtml': 'userInfo.xhtml' in html
            }
            
            passed = sum(checks.values())
            
            if passed >= 3:  # Минимум 3 признака из 4
                self.logger.info(f"✅ Авторизация подтверждена ({passed}/4 проверок)")
                return True
            
            self.logger.error(f"❌ Авторизация не подтверждена ({passed}/4 проверок)")
            
            # Сохраняем HTML для отладки
            try:
                with open('failed_auth_debug.html', 'w', encoding='utf-8') as f:
                    f.write(html)
                self.logger.info("HTML сохранён в failed_auth_debug.html")
            except:
                pass
            
            return False
    
    def _extract_viewstate(self, html: str) -> Optional[str]:
        """Извлечение ViewState из HTML"""
        parser = HTMLParser(html)
        viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')
        
        if viewstate_input and viewstate_input.attributes:
            return viewstate_input.attributes.get('value')
        return None
    
    def _extract_auth_form_ids(self, html: str) -> Dict[str, str]:
        """
        Извлечение ID элементов формы авторизации
        
        Возвращает:
            {
                'form_base': 'j_idt82:auth',
                'xin_field': 'j_idt82:auth:xin',
                'password_field': 'j_idt82:auth:password',
                'submit_button': 'j_idt82:auth:j_idt89'
            }
        """
        parser = HTMLParser(html)
        ids = {}
        
        # 1. Поиск поля ИИН (input[type="email"])
        xin_input = parser.css_first('input[type="email"]')
        if xin_input and xin_input.attributes:
            xin_name = xin_input.attributes.get('name', '') or ''
            xin_id = xin_input.attributes.get('id', '') or ''
            
            ids['xin_field'] = xin_name or xin_id
            
            # Извлекаем базовый ID формы из имени поля
            # Например: 'j_idt82:auth:xin' → 'j_idt82:auth'
            if ':' in ids['xin_field']:
                parts = ids['xin_field'].split(':')
                ids['form_base'] = ':'.join(parts[:-1])
        
        # 2. Поиск поля пароля (input[type="password"])
        password_input = parser.css_first('input[type="password"]')
        if password_input and password_input.attributes:
            password_name = password_input.attributes.get('name', '') or ''
            password_id = password_input.attributes.get('id', '') or ''
            ids['password_field'] = password_name or password_id
        
        # 3. Поиск кнопки "Войти"
        # Метод 1: По value="Войти" и type="submit"
        submit_buttons = parser.css('input[type="submit"]')
        
        for button in submit_buttons:
            if not button.attributes:
                continue
                
            button_value = button.attributes.get('value', '')
            if button_value is None:
                button_value = ''
            button_value = button_value.strip()
            
            button_id = button.attributes.get('id', '') or ''
            button_name = button.attributes.get('name', '') or ''
            
            # Проверяем текст кнопки
            if button_value.lower() in ['войти', 'login', 'кіру']:
                ids['submit_button'] = button_name or button_id
                self.logger.debug(f"Найдена кнопка входа: {ids['submit_button']}")
                break
        
        # Метод 2: По классу button-primary (если первый не сработал)
        if 'submit_button' not in ids:
            primary_buttons = parser.css('.button-primary[type="submit"]')
            if primary_buttons:
                button = primary_buttons[0]
                if button.attributes:
                    button_id = button.attributes.get('id', '') or ''
                    button_name = button.attributes.get('name', '') or ''
                    ids['submit_button'] = button_name or button_id
                    self.logger.debug(f"Найдена кнопка (по классу): {ids['submit_button']}")
        
        # Метод 3: По onclick с RichFaces.ajax (запасной)
        if 'submit_button' not in ids:
            ajax_elements = parser.css('[onclick*="RichFaces.ajax"]')
            for elem in ajax_elements:
                if not elem.attributes:
                    continue
                    
                elem_id = elem.attributes.get('id', '') or ''
                elem_name = elem.attributes.get('name', '') or ''
                elem_type = elem.attributes.get('type', '') or ''
                
                # Проверяем что это submit кнопка формы авторизации
                if 'auth' in (elem_id or elem_name) and elem_type == 'submit':
                    ids['submit_button'] = elem_name or elem_id
                    self.logger.debug(f"Найдена кнопка (через onclick): {ids['submit_button']}")
                    break
        
        return ids
    
    def _get_base_headers(self) -> Dict[str, str]:
        """Базовые HTTP заголовки"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru,en;q=0.9',
            'Cache-Control': 'no-cache'
        }
    
    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        headers = self._get_base_headers()
        headers.update({
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Faces-Request': 'partial/ajax',
            'X-Requested-With': 'XMLHttpRequest'
        })
        return headers


# ============================================================================
# МОДУЛЬ: parsers/court_parser/database/models.py
# ============================================================================

"""
Структуры данных для БД
"""


@dataclass
class CaseData:
    """Данные дела"""
    case_number: str
    case_date: Optional[date] = None
    judge: Optional[str] = None
    plaintiffs: List[str] = field(default_factory=list)
    defendants: List[str] = field(default_factory=list)
    events: List['EventData'] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Преобразование в словарь"""
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


# ============================================================================
# МОДУЛЬ: parsers/court_parser/database/db_manager.py
# ============================================================================

"""
Менеджер базы данных
"""

# REMOVED IMPORT: from database.models import CaseData, EventData
# REMOVED IMPORT: from utils.text_processor import TextProcessor
# REMOVED IMPORT: from utils.validators import DataValidator
# REMOVED IMPORT: from utils.logger import get_logger


class DatabaseManager:
    """Менеджер базы данных"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.pool: Optional[asyncpg.Pool] = None
        self.text_processor = TextProcessor()
        self.validator = DataValidator()
        self.logger = get_logger('db_manager')
        
        # Кеши для ID сущностей
        self.judges_cache: Dict[str, int] = {}
        self.parties_cache: Dict[str, int] = {}
        self.event_types_cache: Dict[str, int] = {}
    
    async def connect(self):
        """Подключение к БД"""
        self.pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['dbname'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            min_size=1,
            max_size=10
        )
        
        # Загрузка кешей
        await self._load_caches()
        
        self.logger.info("✅ Подключение к БД установлено")
    
    async def disconnect(self):
        """Отключение от БД"""
        if self.pool:
            await self.pool.close()
            self.logger.info("Подключение к БД закрыто")
    
    async def save_case(self, case_data: CaseData) -> Dict[str, Any]:
        """
        Сохранение дела в БД
        
        Возвращает: {'status': 'saved'|'updated'|'error', 'case_id': int}
        """
        try:
            # Валидация данных
            self.validator.validate_case_data(case_data.to_dict())
            
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Сохранение дела
                    case_id = await self._save_case_record(conn, case_data)
                    
                    if not case_id:
                        return {'status': 'error', 'case_id': None}
                    
                    # 2. Сохранение сторон
                    await self._save_parties(conn, case_id, case_data)
                    
                    # 3. Сохранение событий
                    await self._save_events(conn, case_id, case_data.events)
                    
                    self.logger.info(f"✅ Дело сохранено: {case_data.case_number}")
                    return {'status': 'saved', 'case_id': case_id}
        
        except asyncpg.UniqueViolationError:
            self.logger.debug(f"Дело уже существует: {case_data.case_number}")
            return {'status': 'updated', 'case_id': None}
        
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения дела {case_data.case_number}: {e}")
            return {'status': 'error', 'case_id': None}
    
    async def _save_case_record(self, conn: asyncpg.Connection, 
                               case_data: CaseData) -> Optional[int]:
        """Сохранение записи дела"""
        # Получение/создание судьи
        judge_id = None
        if case_data.judge:
            judge_id = await self._get_or_create_judge(conn, case_data.judge)
        
        # Вставка дела
        query = """
            INSERT INTO cases (case_number, case_date, judge_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (case_number) DO UPDATE 
            SET case_date = EXCLUDED.case_date,
                judge_id = COALESCE(EXCLUDED.judge_id, cases.judge_id),
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """
        
        case_id = await conn.fetchval(
            query,
            case_data.case_number,
            case_data.case_date,
            judge_id
        )
        
        return case_id
    
    async def _save_parties(self, conn: asyncpg.Connection, 
                          case_id: int, case_data: CaseData):
        """Сохранение сторон дела"""
        # Истцы
        for plaintiff in case_data.plaintiffs:
            if self.validator.validate_party_name(plaintiff):
                party_id = await self._get_or_create_party(conn, plaintiff)
                await self._link_party_to_case(conn, case_id, party_id, 'plaintiff')
        
        # Ответчики
        for defendant in case_data.defendants:
            if self.validator.validate_party_name(defendant):
                party_id = await self._get_or_create_party(conn, defendant)
                await self._link_party_to_case(conn, case_id, party_id, 'defendant')
    
    async def _save_events(self, conn: asyncpg.Connection, 
                         case_id: int, events: List[EventData]):
        """Сохранение событий дела"""
        for event in events:
            if self.validator.validate_event(event.to_dict()):
                event_type_id = await self._get_or_create_event_type(
                    conn, event.event_type
                )
                
                await conn.execute(
                    """
                    INSERT INTO case_events (case_id, event_type_id, event_date)
                    VALUES ($1, $2, $3)
                    ON CONFLICT DO NOTHING
                    """,
                    case_id, event_type_id, event.event_date
                )
    
    async def _get_or_create_judge(self, conn: asyncpg.Connection, 
                                  judge_name: str) -> int:
        """Получение или создание судьи"""
        judge_name = self.text_processor.clean(judge_name)
        
        # Проверка кеша
        if judge_name in self.judges_cache:
            return self.judges_cache[judge_name]
        
        # Создание/получение из БД
        judge_id = await conn.fetchval(
            """
            INSERT INTO judges (full_name)
            VALUES ($1)
            ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name
            RETURNING id
            """,
            judge_name
        )
        
        self.judges_cache[judge_name] = judge_id
        return judge_id
    
    async def _get_or_create_party(self, conn: asyncpg.Connection, 
                                  party_name: str) -> int:
        """Получение или создание стороны"""
        party_name = self.text_processor.clean(party_name)
        
        if party_name in self.parties_cache:
            return self.parties_cache[party_name]
        
        party_id = await conn.fetchval(
            """
            INSERT INTO parties (name)
            VALUES ($1)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            party_name
        )
        
        self.parties_cache[party_name] = party_id
        return party_id
    
    async def _get_or_create_event_type(self, conn: asyncpg.Connection, 
                                       event_type: str) -> int:
        """Получение или создание типа события"""
        event_type = self.text_processor.clean(event_type)
        
        if event_type in self.event_types_cache:
            return self.event_types_cache[event_type]
        
        event_type_id = await conn.fetchval(
            """
            INSERT INTO event_types (name)
            VALUES ($1)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            event_type
        )
        
        self.event_types_cache[event_type] = event_type_id
        return event_type_id
    
    async def _link_party_to_case(self, conn: asyncpg.Connection,
                                 case_id: int, party_id: int, role: str):
        """Связывание стороны с делом"""
        await conn.execute(
            """
            INSERT INTO case_parties (case_id, party_id, party_role)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            case_id, party_id, role
        )
    
    async def _load_caches(self):
        """Загрузка кешей из БД"""
        async with self.pool.acquire() as conn:
            # Судьи
            judges = await conn.fetch("SELECT id, full_name FROM judges")
            for row in judges:
                self.judges_cache[row['full_name']] = row['id']
            
            # Стороны
            parties = await conn.fetch("SELECT id, name FROM parties")
            for row in parties:
                self.parties_cache[row['name']] = row['id']
            
            # Типы событий
            events = await conn.fetch("SELECT id, name FROM event_types")
            for row in events:
                self.event_types_cache[row['name']] = row['id']
        
        self.logger.debug(f"Кеши загружены: {len(self.judges_cache)} судей, "
                         f"{len(self.parties_cache)} сторон, "
                         f"{len(self.event_types_cache)} типов событий")
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
    
    async def get_cases_for_update(self, filters: Dict) -> List[str]:
        """
        Получить номера дел для обновления
        
        Args:
            filters: {
                'defendant_keywords': ['доход'],
                'exclude_event_types': ['Завершение дела', ...],
                'update_interval_days': 2
            }
        
        Returns:
            ['6294-25-00-4/215', '6294-25-00-4/450', ...]
        """
        defendant_keywords = filters.get('defendant_keywords', [])
        exclude_events = filters.get('exclude_event_types', [])
        interval_days = filters.get('update_interval_days', 2)
        
        # Построение SQL запроса
        query = """
            SELECT DISTINCT c.case_number
            FROM cases c
        """
        
        conditions = []
        params = []
        param_counter = 1
        
        # ФИЛЬТР 1: По ответчику (если указан)
        if defendant_keywords:
            query += """
                JOIN case_parties cp ON c.id = cp.case_id AND cp.party_role = 'defendant'
                JOIN parties p ON cp.party_id = p.id
            """
            
            # OR логика для нескольких ключевых слов
            keyword_conditions = []
            for keyword in defendant_keywords:
                keyword_conditions.append(f"p.name ILIKE ${param_counter}")
                params.append(f'%{keyword}%')
                param_counter += 1
            
            conditions.append(f"({' OR '.join(keyword_conditions)})")
        
        # ФИЛЬТР 2: Исключить дела с определёнными событиями
        if exclude_events:
            placeholders = ', '.join([f'${i}' for i in range(param_counter, param_counter + len(exclude_events))])
            
            conditions.append(f"""
                NOT EXISTS (
                    SELECT 1 
                    FROM case_events ce
                    JOIN event_types et ON ce.event_type_id = et.id
                    WHERE ce.case_id = c.id
                    AND et.name IN ({placeholders})
                )
            """)
            
            params.extend(exclude_events)
            param_counter += len(exclude_events)
        
        # ФИЛЬТР 3: Не проверялись последние N дней
        conditions.append(f"""
            (
                c.last_updated_at IS NULL 
                OR c.last_updated_at < NOW() - INTERVAL '{interval_days} days'
            )
        """)
        
        # Собираем WHERE
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Сортировка: старые дела первыми
        query += " ORDER BY c.case_date ASC"
        
        # Выполнение запроса
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        case_numbers = [row['case_number'] for row in rows]
        
        self.logger.info(f"Найдено дел для обновления: {len(case_numbers)}")
        return case_numbers

    async def mark_case_as_updated(self, case_number: str):
        """
        Пометить дело как успешно обновлённое
        
        Вызывается ТОЛЬКО если весь цикл обновления прошёл успешно:
        - Дело найдено на сайте
        - События распарсены
        - Данные сохранены в БД
        - Без ошибок
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE cases 
                SET last_updated_at = CURRENT_TIMESTAMP 
                WHERE case_number = $1
            """, case_number)
        
        self.logger.debug(f"Дело помечено как обновлённое: {case_number}")


# ============================================================================
# МОДУЛЬ: parsers/court_parser/parsing/html_parser.py
# ============================================================================

"""
Парсинг HTML результатов
"""

# REMOVED IMPORT: from database.models import CaseData
# REMOVED IMPORT: from parsing.data_extractor import DataExtractor
# REMOVED IMPORT: from utils.logger import get_logger


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
        
        Возвращает: список найденных дел
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
        """Парсинг одной строки таблицы"""
        cells = row.css('td')
        
        if len(cells) < 4:
            return None
        
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
            events=events
        )


# ============================================================================
# МОДУЛЬ: parsers/court_parser/parsing/data_extractor.py
# ============================================================================

"""
Извлечение данных из HTML элементов
"""

# REMOVED IMPORT: from database.models import EventData
# REMOVED IMPORT: from utils.text_processor import TextProcessor
# REMOVED IMPORT: from utils.logger import get_logger


class DataExtractor:
    """Извлечение данных из HTML элементов"""
    
    def __init__(self):
        self.text_processor = TextProcessor()
        self.logger = get_logger('data_extractor')
    
    def extract_case_info(self, cell) -> Tuple[str, Optional[date]]:
        """
        Извлечение номера дела и даты
        
        Возвращает: (case_number, case_date)
        """
        paragraphs = cell.css('p')
        case_number = ""
        case_date = None
        
        if paragraphs:
            # Первый параграф - номер дела
            case_number = self.text_processor.clean(paragraphs[0].text())
            
            # Второй параграф - дата (если есть)
            if len(paragraphs) > 1:
                date_str = self.text_processor.clean(paragraphs[1].text())
                parsed_date = self.text_processor.parse_date(date_str)
                if parsed_date:
                    case_date = parsed_date.date()
        
        return case_number, case_date
    
    def extract_parties(self, cell) -> Tuple[List[str], List[str]]:
        """
        Извлечение сторон дела
        
        Возвращает: (plaintiffs, defendants)
        """
        paragraphs = cell.css('p')
        plaintiffs = []
        defendants = []
        
        if len(paragraphs) >= 2:
            # Первый параграф - истцы
            plaintiffs_text = self.text_processor.clean(paragraphs[0].text())
            if plaintiffs_text:
                plaintiffs = self.text_processor.split_parties(plaintiffs_text)
            
            # Второй параграф - ответчики
            defendants_text = self.text_processor.clean(paragraphs[1].text())
            if defendants_text:
                defendants = self.text_processor.split_parties(defendants_text)
        
        return plaintiffs, defendants
    
    def extract_judge(self, cell) -> Optional[str]:
        """Извлечение имени судьи"""
        judge_text = self.text_processor.clean(cell.text())
        return judge_text if judge_text else None
    
    def extract_events(self, cell) -> List[EventData]:
        """Извлечение событий дела"""
        paragraphs = cell.css('p')
        events = []
        
        for paragraph in paragraphs:
            text = self.text_processor.clean(paragraph.text())
            
            # Формат: "15.01.2025 - Дело принято к производству"
            if ' - ' in text:
                try:
                    date_part, event_part = text.split(' - ', 1)
                    
                    parsed_date = self.text_processor.parse_date(date_part)
                    event_type = self.text_processor.clean(event_part)
                    
                    if parsed_date and event_type:
                        events.append(EventData(
                            event_type=event_type,
                            event_date=parsed_date.date()
                        ))
                except ValueError:
                    continue
        
        return events


# ============================================================================
# МОДУЛЬ: parsers/court_parser/search/form_handler.py
# ============================================================================

"""
Работа с поисковой формой
"""

# REMOVED IMPORT: from utils.logger import get_logger


class FormHandler:
    """Обработчик поисковой формы"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('form_handler')
    
    async def prepare_search_form(self, session: aiohttp.ClientSession) -> tuple:
        """
        Подготовка формы поиска
        
        Возвращает: (viewstate, form_ids)
        """
        url = f"{self.base_url}/form/lawsuit/"
        headers = self._get_headers()
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} при загрузке формы поиска")
            
            html = await response.text()
            viewstate = self._extract_viewstate(html)
            form_ids = self._extract_form_ids(html)
            
            self.logger.debug("Форма поиска подготовлена")
            return viewstate, form_ids
    
    async def select_region(self, session: aiohttp.ClientSession, 
                          viewstate: str, region_id: str, 
                          form_ids: Dict[str, str]):
        """Выбор региона в форме"""
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('form_base', 'j_idt45:j_idt46')
        
        data = {
            form_base: form_base,
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
            'javax.faces.partial.execute': f'{form_base}:edit-district @component',
            'javax.faces.partial.render': '@component',
            'javax.faces.behavior.event': 'change',
            'org.richfaces.ajax.component': f'{form_base}:edit-district',
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        headers = self._get_ajax_headers()
        
        async with session.post(url, data=data, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} при выборе региона")
            
            self.logger.debug(f"Регион выбран: {region_id}")
    
    def _extract_viewstate(self, html: str) -> Optional[str]:
        """Извлечение ViewState"""
        parser = HTMLParser(html)
        viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')
        
        if viewstate_input:
            return viewstate_input.attributes.get('value')
        return None
    
    def _extract_form_ids(self, html: str) -> Dict[str, str]:
        """Извлечение ID элементов формы"""
        parser = HTMLParser(html)
        ids = {}
        
        # Поиск базового ID формы
        form = parser.css_first('form')
        if form and form.attributes.get('id'):
            ids['form_id'] = form.attributes['id']
        
        # Поиск полей формы
        field_mappings = ['edit-district', 'edit-court', 'edit-year', 'edit-num']
        
        for field in field_mappings:
            elements = parser.css(f'[id*="{field}"]')
            for element in elements:
                if element.attributes.get('id'):
                    ids[field] = element.attributes['id']
                    name = element.attributes.get('name', '')
                    if ':' in name:
                        ids['form_base'] = ':'.join(name.split(':')[:-1])
                    break
        
        return ids
    
    def _get_headers(self) -> Dict[str, str]:
        """Базовые заголовки"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru,en;q=0.9'
        }
    
    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        headers = self._get_headers()
        headers.update({
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Faces-Request': 'partial/ajax'
        })
        return headers


# ============================================================================
# МОДУЛЬ: parsers/court_parser/search/search_engine.py
# ============================================================================

"""
Поисковый движок
"""

# REMOVED IMPORT: from utils.logger import get_logger


class SearchEngine:
    """Движок для поиска дел"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('search_engine')
    
    async def search_case(self, session: aiohttp.ClientSession,
                 viewstate: str, region_id: str, court_id: str,
                 year: str, full_case_number: str,
                 form_ids: Dict[str, str],
                 extract_sequence: bool = False) -> str:  # ← ДОБАВЛЕН параметр
        """
        Поиск дела
        
        Args:
            full_case_number: полный номер дела (например, "6294-25-00-4/215")
            extract_sequence: 
                False (default) - передать полный номер в FormData (Full Scan Mode)
                True - передать только порядковый номер в FormData (Update Mode)
        
        Returns:
            HTML с результатами
        """
        # Отправка поискового запроса
        await self._send_search_request(
            session, viewstate, region_id, court_id,
            year, full_case_number, form_ids, extract_sequence  # ← ДОБАВЛЕН параметр
        )
        
        # Небольшая задержка для обработки на сервере
        await asyncio.sleep(0.5)
        
        # Получение результатов
        results_html = await self._get_results(session)
        
        self.logger.debug(f"Поиск выполнен для номера: {full_case_number}")
        return results_html
    
    async def _send_search_request(self, session: aiohttp.ClientSession,
                          viewstate: str, region_id: str, court_id: str,
                          year: str, full_case_number: str,
                          form_ids: Dict[str, str],
                          extract_sequence: bool = False):  # ← ДОБАВЛЕН параметр
        """
        Отправка поискового запроса
        
        Args:
            extract_sequence: 
                False - передать полный номер в FormData (Full Scan Mode)
                True - передать только порядковый номер в FormData (Update Mode)
        """
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('form_base', 'j_idt45:j_idt46')
        search_button = f'{form_base}:j_idt83'
        
        # КЛЮЧЕВАЯ ЛОГИКА: Выбор что передавать в FormData
        if extract_sequence:
            # Update Mode: только порядковый номер
            if '/' in full_case_number:
                search_number = full_case_number.split('/')[-1]  # "215"
            else:
                search_number = full_case_number
            self.logger.debug(f"Update Mode: извлечён порядковый {search_number} из {full_case_number}")
        else:
            # Full Scan Mode: полный номер
            search_number = full_case_number  # "6294-25-00-4/1"
            self.logger.debug(f"Full Scan Mode: используется полный номер {search_number}")
        
        data = {
            form_base: form_base,
            f'{form_base}:edit-district': region_id,
            f'{form_base}:edit-district-hide': region_id,
            f'{form_base}:edit-court': court_id,
            f'{form_base}:edit-year': year,
            f'{form_base}:edit-iin': '',
            f'{form_base}:edit-num': search_number,  # ← Зависит от режима!
            f'{form_base}:edit-fio': '',
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': search_button,
            'javax.faces.partial.execute': f'{search_button} @component',
            'javax.faces.partial.render': '@component',
            'param1': f'{form_base}:edit-num',
            'org.richfaces.ajax.component': search_button,
            search_button: search_button,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }
        
        # Логирование для отладки
        self.logger.debug(f"🔍 Поиск дела:")
        self.logger.debug(f"   Регион ID: {region_id}")
        self.logger.debug(f"   Суд ID: {court_id}")
        self.logger.debug(f"   Год: {year}")
        self.logger.debug(f"   Полный номер: {full_case_number}")
        self.logger.debug(f"   В FormData: {search_number}")
        
        headers = self._get_ajax_headers()
        
        async with session.post(url, data=data, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} при отправке поиска")
            
            await response.text()
    
    async def _get_results(self, session: aiohttp.ClientSession) -> str:
        """Получение страницы с результатами"""
        url = f"{self.base_url}/lawsuit/lawsuitList.xhtml"
        headers = self._get_headers()
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} при получении результатов")
            
            return await response.text()
    
    def _get_headers(self) -> Dict[str, str]:
        """Базовые заголовки"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru,en;q=0.9'
        }
    
    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        headers = self._get_headers()
        headers.update({
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Faces-Request': 'partial/ajax'
        })
        return headers


# ============================================================================
# МОДУЛЬ: parsers/court_parser/core/session.py
# ============================================================================

"""
Управление HTTP сессиями с retry
"""


# REMOVED IMPORT: from utils.logger import get_logger
# REMOVED IMPORT: from utils.retry import RetryStrategy, RetryConfig, CircuitBreaker, NonRetriableError


class SessionManager:
    """Менеджер HTTP сессий с автоматическим retry"""
    
    def __init__(self, timeout: int = 30, retry_config: Optional[Dict] = None):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = get_logger('session_manager')
        
        # Retry конфигурация
        self.retry_config = retry_config or {}
        self.circuit_breaker = None
        
        # Инициализация Circuit Breaker
        if 'circuit_breaker' in self.retry_config:
            self.circuit_breaker = CircuitBreaker(self.retry_config['circuit_breaker'])
    
    async def create_session(self) -> aiohttp.ClientSession:
        """Создание новой сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
        
        # SSL контекст без проверки сертификата
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=10)
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            connector=connector
        )
        
        self.logger.debug("Создана новая HTTP сессия")
        return self.session
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Получить текущую сессию"""
        if not self.session or self.session.closed:
            return await self.create_session()
        return self.session
    
    async def request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        """
        HTTP запрос с автоматическим retry
        
        Args:
            method: HTTP метод (GET, POST, etc)
            url: URL
            **kwargs: параметры для aiohttp
        
        Returns:
            aiohttp.ClientResponse
        
        Raises:
            NonRetriableError: если ошибка не подлежит retry (400, 401, 404, etc)
        """
        session = await self.get_session()
        
        # Получаем retry config
        http_retry_config = self.retry_config.get('http_request', {})
        
        if not http_retry_config:
            # Нет конфига - выполняем без retry
            return await session.request(method, url, **kwargs)
        
        # Retry стратегия
        retry_cfg = RetryConfig(http_retry_config)
        strategy = RetryStrategy(retry_cfg, self.circuit_breaker)
        
        async def _do_request():
            async with session.request(method, url, **kwargs) as response:
                # Проверка на non-retriable статусы
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")
                
                # Проверка на retriable статусы
                if strategy.is_retriable_status(response.status):
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                # Успех
                return response
        
        error_context = f"{method} {url}"
        return await strategy.execute_with_retry(_do_request, error_context=error_context)
    
    async def get(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        """GET запрос с retry"""
        return await self.request('GET', url, **kwargs)
    
    async def post(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        """POST запрос с retry"""
        return await self.request('POST', url, **kwargs)
    
    async def close(self):
        """Закрытие сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.debug("Сессия закрыта")
    
    async def __aenter__(self):
        await self.create_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# ============================================================================
# МОДУЛЬ: parsers/court_parser/core/parser.py
# ============================================================================

"""
Главный класс парсера с retry и восстановлением
"""


# REMOVED IMPORT: from config.settings import Settings
# REMOVED IMPORT: from core.session import SessionManager
# REMOVED IMPORT: from auth.authenticator import Authenticator
# REMOVED IMPORT: from search.form_handler import FormHandler
# REMOVED IMPORT: from search.search_engine import SearchEngine
# REMOVED IMPORT: from parsing.html_parser import ResultsParser
# REMOVED IMPORT: from database.db_manager import DatabaseManager
# REMOVED IMPORT: from database.models import CaseData, SearchResult
# REMOVED IMPORT: from utils.text_processor import TextProcessor
# REMOVED IMPORT: from utils.logger import get_logger
# REMOVED IMPORT: from utils.retry import RetryStrategy, RetryConfig, NonRetriableError


class CourtParser:
    """Главный класс парсера с retry и восстановлением"""
    
    def __init__(self, config_path: Optional[str] = None, update_mode: bool = False):  # ← ДОБАВЛЕН параметр
        # Загрузка конфигурации
        self.settings = Settings(config_path)
        
        # РЕЖИМ РАБОТЫ (НОВОЕ)
        self.update_mode = update_mode  # ← ДОБАВЛЕНО
        
        # Retry конфигурация
        self.retry_config = self.settings.config.get('retry_settings', {})
        
        # Инициализация компонентов с retry
        self.session_manager = SessionManager(
            timeout=30,
            retry_config=self.retry_config
        )
        
        self.authenticator = Authenticator(
            self.settings.base_url,
            self.settings.auth,
            retry_config=self.retry_config
        )
        
        self.form_handler = FormHandler(self.settings.base_url)
        self.search_engine = SearchEngine(self.settings.base_url)
        self.results_parser = ResultsParser()
        self.db_manager = DatabaseManager(self.settings.database)
        self.text_processor = TextProcessor()
        
        # Счетчик ошибок для переавторизации
        self.session_error_count = 0
        self.max_session_errors = 10
        
        # Счетчик переавторизаций
        self.reauth_count = 0
        self.max_reauth = self.retry_config.get('session_recovery', {}).get(
            'max_reauth_attempts', 2
        )
        
        self.logger = get_logger('court_parser')
        
        # ДОБАВЛЕНО: логирование режима
        mode_name = "Update Mode" if self.update_mode else "Full Scan Mode"
        self.logger.info(f"🚀 Парсер инициализирован в режиме: {mode_name}")
    
    async def initialize(self):
        """Инициализация (подключение к БД, авторизация)"""
        await self.db_manager.connect()
        await self.authenticator.authenticate(self.session_manager)
        self.logger.info("✅ Парсер готов к работе")
    
    async def cleanup(self):
        """Очистка ресурсов"""
        await self.db_manager.disconnect()
        await self.session_manager.close()
        self.logger.info("Ресурсы очищены")
    
    async def _handle_session_recovery(self, error: Exception) -> bool:
        """
        Обработка восстановления сессии
        
        Returns:
            True если удалось восстановить, False если нет
        """
        reauth_on_401 = self.retry_config.get('session_recovery', {}).get('reauth_on_401', True)
        
        # Проверяем что это HTTP 401
        if not (isinstance(error, (aiohttp.ClientError, NonRetriableError)) and '401' in str(error)):
            return False
        
        if not reauth_on_401:
            return False
        
        # Проверяем лимит переавторизаций
        if self.reauth_count >= self.max_reauth:
            self.logger.error(
                f"❌ Достигнут лимит переавторизаций ({self.max_reauth})"
            )
            return False
        
        self.reauth_count += 1
        
        self.logger.warning(
            f"⚠️ HTTP 401: Сессия истекла, переавторизация "
            f"({self.reauth_count}/{self.max_reauth})..."
        )
        
        try:
            await self.authenticator.authenticate(self.session_manager)
            self.session_error_count = 0  # Сброс счетчика ошибок
            self.logger.info("✅ Переавторизация успешна")
            return True
        
        except Exception as e:
            self.logger.error(f"❌ Переавторизация не удалась: {e}")
            return False
    
    async def _handle_rate_limit(self, response: aiohttp.ClientResponse):
        """Обработка HTTP 429 (Rate Limit)"""
        rate_limit_config = self.retry_config.get('rate_limit', {})
        
        # Читаем header Retry-After
        retry_after = response.headers.get('Retry-After')
        
        if retry_after and rate_limit_config.get('respect_retry_after_header', True):
            try:
                wait_time = int(retry_after)
            except ValueError:
                wait_time = rate_limit_config.get('default_wait', 60)
        else:
            wait_time = rate_limit_config.get('default_wait', 60)
        
        self.logger.warning(
            f"⚠️ HTTP 429 (Rate Limit), ждем {wait_time} сек..."
        )
        
        await asyncio.sleep(wait_time)
        
        # TODO: Уменьшить скорость запросов (реализуем позже в adaptive)
    
    async def search_and_save_with_retry(self, region_key: str, court_key: str,
                                    case_number: str, year: str = "2025") -> Dict[str, Any]:
        """
        Поиск и сохранение дела с retry и восстановлением
        
        Returns:
            {
                'success': True/False,
                'target_found': True/False,
                'total_saved': 5,
                'related_saved': 4,
                'target_case_number': '...',
                'error': None или строка с ошибкой
            }
        """
        search_retry_config = self.retry_config.get('search_case', {})
        
        if not search_retry_config:
            # Без retry
            try:
                return await self._do_search_and_save(region_key, court_key, case_number, year)
            except NonRetriableError as e:
                return {
                    'success': False,
                    'target_found': False,
                    'total_saved': 0,
                    'related_saved': 0,
                    'error': str(e)
                }
        
        # С retry
        retry_cfg = RetryConfig(search_retry_config)
        strategy = RetryStrategy(retry_cfg, self.session_manager.circuit_breaker)
        
        async def _search_with_recovery():
            try:
                return await self._do_search_and_save(region_key, court_key, case_number, year)
            
            except Exception as e:
                # Попытка восстановления сессии
                if await self._handle_session_recovery(e):
                    # Повторяем после успешной переавторизации
                    return await self._do_search_and_save(region_key, court_key, case_number, year)
                raise
        
        try:
            result = await strategy.execute_with_retry(
                _search_with_recovery,
                error_context=f"Поиск дела {case_number}"
            )
            
            # Успех - сбрасываем счетчик ошибок
            self.session_error_count = 0
            
            return result
        
        except NonRetriableError as e:
            # Постоянная ошибка (дело не существует)
            self.session_error_count = 0
            return {
                'success': False,
                'target_found': False,
                'total_saved': 0,
                'related_saved': 0,
                'error': str(e)
            }
        
        except Exception as e:
            # Временная ошибка, но retry исчерпан
            self.session_error_count += 1
            
            self.logger.error(f"❌ Не удалось найти дело после retry: {e}")
            
            # Проверка на множественные ошибки подряд
            if self.session_error_count >= self.max_session_errors:
                self.logger.warning(
                    f"⚠️ {self.max_session_errors} ошибок подряд, "
                    f"попытка переавторизации..."
                )
                
                if await self._handle_session_recovery(Exception("Multiple failures")):
                    self.session_error_count = 0
            
            return {
                'success': False,
                'target_found': False,
                'total_saved': 0,
                'related_saved': 0,
                'error': str(e)
            }

    # Алиас для обратной совместимости
    async def search_and_save(self, *args, **kwargs):
        return await self.search_and_save_with_retry(*args, **kwargs)
    
    async def _do_search_and_save(self, region_key: str, court_key: str,
                            case_number: str, year: str) -> Dict[str, Any]:
        """
        Один цикл поиска и сохранения
        
        Returns:
            {
                'success': True/False,
                'target_found': True/False,
                'total_saved': 5,
                'related_saved': 4,
                'target_case_number': '6294-25-00-4/1'
            }
        """
        # Получение конфигурации
        region_config = self.settings.get_region(region_key)
        court_config = self.settings.get_court(region_key, court_key)
        
        # Генерация полного номера
        full_case_number = self.text_processor.generate_case_number(
            region_config, court_config, year, int(case_number)
        )
        
        self.logger.info(f"🔍 Ищу дело: {full_case_number}")
        
        # Получение сессии
        session = await self.session_manager.get_session()
        
        # Подготовка формы
        viewstate, form_ids = await self.form_handler.prepare_search_form(session)
        
        # Выбор региона
        await self.form_handler.select_region(
            session, viewstate, region_config['id'], form_ids
        )
        
        await asyncio.sleep(1)
        
        # Поиск (ИЗМЕНЕНО: добавлен параметр extract_sequence)
        results_html = await self.search_engine.search_case(
            session, viewstate, region_config['id'], court_config['id'],
            year, full_case_number, form_ids,
            extract_sequence=self.update_mode  # ← ИЗМЕНЕНО: используем флаг парсера
        )
        
        # Парсинг
        cases = self.results_parser.parse(results_html)
        
        if not cases:
            self.logger.info(f"❌ Ничего не найдено: {full_case_number}")
            raise NonRetriableError("Дело не найдено")
        
        # Сохранение всех найденных дел
        saved_count = 0
        target_found = False
        related_count = 0
        
        for case in cases:
            # Проверка что дело из нашего региона/суда
            if not case.case_number.startswith(
                f"{region_config['kato_code']}{court_config['instance_code']}"
            ):
                continue
            
            # Проверка целевого дела
            is_target = (case.case_number == full_case_number)
            
            if is_target:
                target_found = True
            
            # Сохранение
            save_result = await self.db_manager.save_case(case)
            
            if save_result['status'] in ['saved', 'updated']:
                saved_count += 1
                
                if is_target:
                    judge = "✅ судья" if case.judge else "⚠️ без судьи"
                    parties = len(case.plaintiffs) + len(case.defendants)
                    events = len(case.events)
                    self.logger.info(
                        f"✅ ЦЕЛЕВОЕ: {case.case_number} "
                        f"({judge}, {parties} стороны, {events} события)"
                    )
                else:
                    related_count += 1
                    self.logger.debug(f"💾 Связанное: {case.case_number}")
        
        # Итоговый лог
        if saved_count > 0:
            if saved_count > 1:
                self.logger.info(
                    f"💾 Всего сохранено: {saved_count} дел "
                    f"(целевое: {1 if target_found else 0}, связанных: {related_count})"
                )
            
            if not target_found:
                self.logger.warning(
                    f"⚠️ Целевое дело {full_case_number} не найдено, "
                    f"но сохранено {saved_count} связанных"
                )
            
            return {
                'success': True,
                'target_found': target_found,
                'total_saved': saved_count,
                'related_saved': related_count,
                'target_case_number': full_case_number
            }
        else:
            raise NonRetriableError("Не удалось сохранить дела")
    
    # Алиас для обратной совместимости
    async def search_and_save(self, *args, **kwargs):
        return await self.search_and_save_with_retry(*args, **kwargs)
    
    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()


# ============================================================================
# МОДУЛЬ: parsers/court_parser/main.py
# ============================================================================

"""
Точка входа парсера
"""

# REMOVED IMPORT: from core.parser import CourtParser
# REMOVED IMPORT: from config.settings import Settings
# REMOVED IMPORT: from utils.logger import setup_logger
# REMOVED IMPORT: from utils import TextProcessor



async def parse_all_regions_from_config() -> dict:
    """Парсинг всех регионов согласно настройкам из config.json"""
    logger = setup_logger('main', level='INFO')
    
    # Загрузка настроек
    settings = Settings()
    ps = settings.parsing_settings
    
    year = ps.get('year', '2025')
    court_type = ps.get('court_type', 'smas')
    start_from = ps.get('start_from', 1)
    max_number = ps.get('max_number', 9999)
    max_consecutive_failures = ps.get('max_consecutive_failures', 50)
    delay_between_requests = ps.get('delay_between_requests', 2)
    delay_between_regions = ps.get('delay_between_regions', 5)
    
    # ЛИМИТЫ ДЛЯ ТЕСТИРОВАНИЯ
    limit_regions = settings.get_limit_regions()
    limit_cases_per_region = settings.get_limit_cases_per_region()
    
    logger.info("=" * 70)
    logger.info(f"МАССОВЫЙ ПАРСИНГ: {court_type} ({year})")
    logger.info("=" * 70)
    logger.info(f"Настройки из config.json:")
    logger.info(f"  Год: {year}")
    logger.info(f"  Тип суда: {court_type}")
    logger.info(f"  Диапазон номеров: {start_from}-{max_number}")
    logger.info(f"  Макс. неудач подряд: {max_consecutive_failures}")
    logger.info(f"  Задержка между запросами: {delay_between_requests} сек")
    logger.info(f"  Задержка между регионами: {delay_between_regions} сек")
    
    if limit_regions:
        logger.info(f"  🔒 ЛИМИТ РЕГИОНОВ: {limit_regions}")
    if limit_cases_per_region:
        logger.info(f"  🔒 ЛИМИТ ЗАПРОСОВ НА РЕГИОН: {limit_cases_per_region}")
    
    logger.info("=" * 70)
    
    # Получение списка регионов
    all_regions = settings.get_target_regions()
    
    # Применение лимита регионов
    if limit_regions:
        regions_to_process = all_regions[:limit_regions]
        logger.info(f"Обрабатываю {len(regions_to_process)} из {len(all_regions)} регионов")
    else:
        regions_to_process = all_regions
        logger.info(f"Обрабатываю все {len(regions_to_process)} регионов")
    
    total_stats = {
        'regions_processed': 0,
        'total_queries': 0,
        'total_target_cases': 0,
        'total_related_cases': 0,
        'total_cases_saved': 0
    }
    
    # ИЗМЕНЕНО: создаём парсер БЕЗ флага (Full Scan Mode)
    async with CourtParser() as parser:  # ← update_mode=False (default)
        for region_key in regions_to_process:
            logger.info(f"\n{'='*70}")
            logger.info(f"Регион: {settings.get_region(region_key)['name']}")
            logger.info(f"{'='*70}")
            
            try:
                # Парсинг региона
                stats = await parse_region_with_limits(
                    parser=parser,
                    region_key=region_key,
                    court_key=court_type,
                    year=year,
                    start_from=start_from,
                    max_number=max_number,
                    max_consecutive_failures=max_consecutive_failures,
                    delay_between_requests=delay_between_requests,
                    limit_cases=limit_cases_per_region
                )
                
                total_stats['regions_processed'] += 1
                total_stats['total_queries'] += stats['queries_made']
                total_stats['total_target_cases'] += stats['target_cases_found']
                total_stats['total_related_cases'] += stats['related_cases_found']
                total_stats['total_cases_saved'] += stats['total_cases_saved']
                
            except Exception as e:
                logger.error(f"Ошибка парсинга региона {region_key}: {e}")
                continue
            
            # Задержка между регионами
            if total_stats['regions_processed'] < len(regions_to_process):
                await asyncio.sleep(delay_between_regions)
    
    # Общая статистика
    logger.info("\n" + "=" * 70)
    logger.info("ОБЩАЯ СТАТИСТИКА:")
    logger.info(f"  Обработано регионов: {total_stats['regions_processed']}")
    logger.info(f"  Всего запросов к серверу: {total_stats['total_queries']}")
    logger.info(f"  Найдено целевых дел: {total_stats['total_target_cases']}")
    logger.info(f"  Найдено связанных дел: {total_stats['total_related_cases']}")
    logger.info(f"  Всего сохранено дел: {total_stats['total_cases_saved']}")
    
    if total_stats['total_queries'] > 0:
        avg_per_query = total_stats['total_cases_saved'] / total_stats['total_queries']
        logger.info(f"  Среднее дел на запрос: {avg_per_query:.1f}")
    
    logger.info("=" * 70)
    
    return total_stats


async def parse_region_with_limits(parser, region_key: str, court_key: str,
                                   year: str, start_from: int, max_number: int,
                                   max_consecutive_failures: int,
                                   delay_between_requests: float,
                                   limit_cases: Optional[int] = None) -> dict:
    """
    Парсинг региона с учетом лимита дел
    
    Args:
        limit_cases: максимальное количество дел для проверки (для тестирования)
    """
    logger = setup_logger('main', level='INFO')
    
    stats = {
        'queries_made': 0,              # Количество запросов к серверу
        'target_cases_found': 0,        # Найдено целевых дел
        'related_cases_found': 0,       # Найдено связанных дел
        'total_cases_saved': 0,         # Всего дел сохранено
        'consecutive_failures': 0
    }
    
    current_number = start_from
    
    while current_number <= max_number:
        # Проверка лимита дел
        if limit_cases and stats['queries_made'] >= limit_cases:
            logger.info(f"🔒 Достигнут лимит дел ({limit_cases}), завершаю регион")
            break
        
        # Проверка лимита неудач
        if stats['consecutive_failures'] >= max_consecutive_failures:
            logger.info(f"Достигнут лимит неудач ({max_consecutive_failures}), завершаю регион")
            break
        
        # Поиск дела
        result = await parser.search_and_save(
            region_key=region_key,
            court_key=court_key,
            case_number=str(current_number),
            year=year
        )
        
        stats['queries_made'] += 1
        
        if result['success']:
            # Успех
            stats['total_cases_saved'] += result['total_saved']
            
            if result['target_found']:
                stats['target_cases_found'] += 1
            
            stats['related_cases_found'] += result['related_saved']
            stats['consecutive_failures'] = 0
        else:
            # Неудача
            stats['consecutive_failures'] += 1
        
        # Периодическая статистика
        if stats['queries_made'] % 10 == 0:
            logger.info(
                f"📊 Прогресс: запросов {stats['queries_made']}, "
                f"найдено целевых {stats['target_cases_found']}, "
                f"всего сохранено {stats['total_cases_saved']}"
            )
        
        current_number += 1
        
        # Задержка между запросами
        await asyncio.sleep(delay_between_requests)
    
    # Итоговая статистика региона
    logger.info("-" * 70)
    logger.info(f"ИТОГИ РЕГИОНА:")
    logger.info(f"  Запросов к серверу: {stats['queries_made']}")
    logger.info(f"  Найдено целевых дел: {stats['target_cases_found']}")
    logger.info(f"  Найдено связанных дел: {stats['related_cases_found']}")
    logger.info(f"  Всего сохранено дел: {stats['total_cases_saved']}")
    
    if stats['queries_made'] > 0:
        target_rate = (stats['target_cases_found'] / stats['queries_made'] * 100)
        total_rate = (stats['total_cases_saved'] / stats['queries_made'] * 100)
        logger.info(f"  Процент целевых дел: {target_rate:.1f}%")
        logger.info(f"  Среднее дел на запрос: {stats['total_cases_saved'] / stats['queries_made']:.1f}")
    
    logger.info("-" * 70)
    
    return stats


async def update_cases_history():
    """
    Режим обновления истории дел
    """
    logger = setup_logger('main', level='INFO')
    
    # Загрузка настроек
    settings = Settings()
    update_config = settings.update_settings
    
    if not update_config.get('enabled'):
        logger.warning("⚠️ Update Mode отключен в config.json")
        return
    
    logger.info("\n" + "=" * 70)
    logger.info("РЕЖИМ ОБНОВЛЕНИЯ ИСТОРИИ ДЕЛ")
    logger.info("=" * 70)
    logger.info(f"Интервал обновления: {update_config['update_interval_days']} дней")
    logger.info(f"Фильтр по ответчику: {update_config['filters']['defendant_keywords']}")
    logger.info(f"Исключить события: {update_config['filters']['exclude_event_types']}")
    logger.info("=" * 70)
    
    stats = {
        'checked': 0,
        'updated': 0,
        'no_changes': 0,
        'errors': 0
    }
    
    # ИЗМЕНЕНО: создаём парсер С ФЛАГОМ Update Mode
    async with CourtParser(update_mode=True) as parser:  # ← ФЛАГ!
        # Получение списка дел для обновления
        cases_to_update = await parser.db_manager.get_cases_for_update({
            'defendant_keywords': update_config['filters']['defendant_keywords'],
            'exclude_event_types': update_config['filters']['exclude_event_types'],
            'update_interval_days': update_config['update_interval_days']
        })
        
        if not cases_to_update:
            logger.info("✅ Нет дел для обновления")
            return
        
        logger.info(f"\n📋 Найдено дел для обновления: {len(cases_to_update)}")
        logger.info(f"Начинаю проверку...\n")
        
        text_processor = TextProcessor()
        
        for i, case_number in enumerate(cases_to_update, 1):
            try:
                # Распарсить полный номер дела
                case_info = text_processor.find_region_and_court_by_case_number(
                    case_number, 
                    settings.regions
                )
                
                if not case_info:
                    logger.error(f"❌ Не удалось распарсить номер: {case_number}")
                    stats['errors'] += 1
                    continue
                
                # ИЗМЕНЕНО: вызов БЕЗ параметра update_mode (автоматически используется self.update_mode)
                result = await parser.search_and_save(
                    region_key=case_info['region_key'],
                    court_key=case_info['court_key'],
                    case_number=case_info['sequence'],
                    year=case_info['year']
                    # ← НЕТ параметра update_mode!
                )
                
                stats['checked'] += 1
                
                # КРИТИЧЕСКАЯ ЛОГИКА
                if result['success']:
                    # УСПЕХ: пометить как обновлённое
                    await parser.db_manager.mark_case_as_updated(case_number)
                    
                    if result['total_saved'] > 0:
                        stats['updated'] += 1
                        logger.info(f"✅ [{i}/{len(cases_to_update)}] {case_number}: +{result['total_saved']} событий")
                    else:
                        stats['no_changes'] += 1
                        logger.debug(f"⚪ [{i}/{len(cases_to_update)}] {case_number}: без изменений")
                else:
                    # НЕУДАЧА: НЕ помечать (last_updated_at остаётся старым)
                    stats['errors'] += 1
                    logger.warning(f"⚠️ [{i}/{len(cases_to_update)}] {case_number}: ошибка")
                
                # Периодическая статистика
                if stats['checked'] % 10 == 0:
                    logger.info(
                        f"\n📊 Прогресс: {stats['checked']}/{len(cases_to_update)} "
                        f"(обновлено: {stats['updated']}, без изменений: {stats['no_changes']}, ошибок: {stats['errors']})\n"
                    )
                
                # Задержка между запросами
                await asyncio.sleep(2)
                
            except Exception as e:
                # ИСКЛЮЧЕНИЕ: НЕ помечать
                stats['errors'] += 1
                logger.error(f"❌ [{i}/{len(cases_to_update)}] Ошибка обновления {case_number}: {e}")
                continue
    
    # Итоговая статистика
    logger.info("\n" + "=" * 70)
    logger.info("ИТОГИ ОБНОВЛЕНИЯ:")
    logger.info(f"  Проверено дел: {stats['checked']}")
    logger.info(f"  Обновлено (новые события): {stats['updated']}")
    logger.info(f"  Без изменений: {stats['no_changes']}")
    logger.info(f"  Ошибок: {stats['errors']}")
    
    if stats['errors'] > 0:
        logger.warning(
            f"\n⚠️ {stats['errors']} дел не обновились и будут проверены при следующем запуске"
        )
    
    logger.info("=" * 70)


async def main():
    """
    Главная функция - запуск парсинга согласно config.json
    """
    logger = setup_logger('main', level='INFO')
    
    logger.info("\n" + "=" * 70)
    logger.info("ПАРСЕР СУДЕБНЫХ ДЕЛ КАЗАХСТАНА")
    logger.info("=" * 70)
    logger.info("Версия: 2.0.0")
    logger.info("Режим: Боевой (настройки из config.json)")
    logger.info("=" * 70)
    
    try:
        # Проверка режима запуска
        if '--mode' in sys.argv:
            mode_index = sys.argv.index('--mode')
            if mode_index + 1 < len(sys.argv):
                mode = sys.argv[mode_index + 1]
                
                if mode == 'update':
                    # РЕЖИМ ОБНОВЛЕНИЯ
                    await update_cases_history()
                    logger.info("\n✅ Обновление завершено")
                    return 0
                else:
                    logger.error(f"❌ Неизвестный режим: {mode}")
                    logger.info("Доступные режимы: update")
                    return 1
        
        # По умолчанию: Full Scan Mode
        stats = await parse_all_regions_from_config()
        
        logger.info("\n✅ Парсер завершил работу успешно")
        return 0
    
    except KeyboardInterrupt:
        logger.warning("\n🛑 Прервано пользователем")
        return 1
    
    except Exception as e:
        logger.critical(f"\n💥 Критическая ошибка: {e}")
        logger.critical(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
