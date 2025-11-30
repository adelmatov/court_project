#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Объединенный файл парсера суда
Дата сборки: 2025-11-30 12:12:43

Этот файл содержит все модули проекта, объединенные в один файл.
Для запуска: python court_parser_unified.py
"""

# ============================================================================
# СТАНДАРТНЫЕ БИБЛИОТЕКИ
# ============================================================================
import os
import sys
import json
import time
import logging
import base64
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any


# Внешние библиотеки
from dataclasses import dataclass, field
from datetime import date
from datetime import datetime
from datetime import datetime, date
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from selectolax.parser import HTMLParser
from typing import Callable, Any, Optional, List, Type
from typing import Dict, Any, Optional
from typing import Dict, Any, Optional, List
from typing import Dict, List, Optional, Any, Set
from typing import Dict, Optional
from typing import List, Optional
from typing import List, Optional, Dict
from typing import List, Tuple, Optional
from typing import Optional, Dict, Any
from typing import Optional, Dict, List, Any, Set
from typing import Optional, List
import aiohttp
import asyncio
import asyncpg
import json
import logging
import random
import re
import ssl
import sys
import traceback

# ============================================================================
# ВСТРОЕННЫЕ РЕСУРСЫ
# ============================================================================

_EMBEDDED_RESOURCES = {
  "config.json": "{\n    \"auth\": {\n        \"login\": \"REMOVED\",\n        \"password\": \"REMOVED\",\n        \"user_name\": \"REMOVED\"\n    },\n    \"base_url\": \"https://office.sud.kz\",\n    \"database\": {\n        \"dbname\": \"court\",\n        \"user\": \"postgres\",\n        \"password\": \"admin\",\n        \"host\": \"localhost\",\n        \"port\": 5432\n    },\n    \"parsing_settings\": {\n        \"year\": \"2025\",\n        \"court_types\": [\"smas\"],\n        \"start_from\": 1,\n        \"max_number\": 9999,\n        \"max_consecutive_empty\": 5,\n        \"max_consecutive_failures\": 5,\n        \"max_parallel_regions\": 3,\n        \"delay_between_requests\": 1.7,\n        \n        \"region_retry_max_attempts\": 3,\n        \"region_retry_delay_seconds\": 5,\n        \n        \"limit_regions\": 1,\n        \"limit_cases_per_region\": null,\n        \"target_regions\": [\"almaty\"]\n    },\n    \"regions\": {\n        \"astana\": {\n            \"id\": \"2\",\n            \"name\": \"город Астана\",\n            \"kato_code\": \"71\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"413\",\n                    \"name\": \"Специализированный межрайонный административный суд города Астаны\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"2\",\n                    \"name\": \"Суд города Астаны\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"almaty\": {\n            \"id\": \"3\",\n            \"name\": \"город Алматы\",\n            \"kato_code\": \"75\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"414\",\n                    \"name\": \"Специализированный межрайонный административный суд города Алматы\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"13\",\n                    \"name\": \"Алматинский городской суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"shymkent\": {\n            \"id\": \"19\",\n            \"name\": \"город Шымкент\",\n            \"kato_code\": \"52\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"415\",\n                    \"name\": \"Специализированный межрайонный административный суд города Шымкента\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"383\",\n                    \"name\": \"Суд города Шымкента\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"akmola\": {\n            \"id\": \"4\",\n            \"name\": \"Акмолинская область\",\n            \"kato_code\": \"11\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"416\",\n                    \"name\": \"Специализированный межрайонный административный суд Акмолинской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"29\",\n                    \"name\": \"Акмолинский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"aktobe\": {\n            \"id\": \"5\",\n            \"name\": \"Актюбинская область\",\n            \"kato_code\": \"15\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"417\",\n                    \"name\": \"Специализированный межрайонный административный суд Актюбинской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"55\",\n                    \"name\": \"Актюбинский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"almaty_region\": {\n            \"id\": \"6\",\n            \"name\": \"Алматинская область\",\n            \"kato_code\": \"19\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"430\",\n                    \"name\": \"Специализированный межрайонный административный суд Алматинской области\",\n                    \"instance_code\": \"93\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"75\",\n                    \"name\": \"Алматинский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"atyrau\": {\n            \"id\": \"7\",\n            \"name\": \"Атырауская область\",\n            \"kato_code\": \"23\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"419\",\n                    \"name\": \"Специализированный межрайонный административный суд Атырауской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"105\",\n                    \"name\": \"Атырауский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"vko\": {\n            \"id\": \"8\",\n            \"name\": \"Восточно-Казахстанская область\",\n            \"kato_code\": \"63\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"420\",\n                    \"name\": \"Специализированный межрайонный административный суд Восточно-Казахстанской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"119\",\n                    \"name\": \"Восточно-Казахстанский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"zhambyl\": {\n            \"id\": \"9\",\n            \"name\": \"Жамбылская область\",\n            \"kato_code\": \"31\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"421\",\n                    \"name\": \"Специализированный межрайонный административный суд Жамбылской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"158\",\n                    \"name\": \"Жамбылский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"zko\": {\n            \"id\": \"10\",\n            \"name\": \"Западно-Казахстанская область\",\n            \"kato_code\": \"27\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"422\",\n                    \"name\": \"Специализированный межрайонный административный суд Западно-Казахстанской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"175\",\n                    \"name\": \"Западно-Казахстанский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"karaganda\": {\n            \"id\": \"11\",\n            \"name\": \"Карагандинская область\",\n            \"kato_code\": \"35\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"423\",\n                    \"name\": \"Специализированный межрайонный административный суд Карагандинской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"199\",\n                    \"name\": \"Карагандинский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"kostanay\": {\n            \"id\": \"12\",\n            \"name\": \"Костанайская область\",\n            \"kato_code\": \"39\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"424\",\n                    \"name\": \"Специализированный межрайонный административный суд Костанайской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"237\",\n                    \"name\": \"Костанайский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"kyzylorda\": {\n            \"id\": \"13\",\n            \"name\": \"Кызылординская область\",\n            \"kato_code\": \"43\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"425\",\n                    \"name\": \"Специализированный межрайонный административный суд Кызылординской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"266\",\n                    \"name\": \"Кызылординский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"mangystau\": {\n            \"id\": \"14\",\n            \"name\": \"Мангистауская область\",\n            \"kato_code\": \"47\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"426\",\n                    \"name\": \"Специализированный межрайонный административный суд Мангистауской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"281\",\n                    \"name\": \"Мангистауский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"pavlodar\": {\n            \"id\": \"15\",\n            \"name\": \"Павлодарская область\",\n            \"kato_code\": \"55\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"427\",\n                    \"name\": \"Специализированный межрайонный административный суд Павлодарской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"295\",\n                    \"name\": \"Павлодарский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"sko\": {\n            \"id\": \"16\",\n            \"name\": \"Северо-Казахстанская область\",\n            \"kato_code\": \"59\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"428\",\n                    \"name\": \"Специализированный межрайонный административный суд Северо-Казахстанской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"316\",\n                    \"name\": \"Северо-Казахстанский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"turkestan\": {\n            \"id\": \"17\",\n            \"name\": \"Туркестанская область\",\n            \"kato_code\": \"51\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"429\",\n                    \"name\": \"Специализированный межрайонный административный суд Туркестанской области\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"340\",\n                    \"name\": \"Туркестанский областной суд\",\n                    \"instance_code\": \"99\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"ulytau\": {\n            \"id\": \"20\",\n            \"name\": \"Область Ұлытау\",\n            \"kato_code\": \"62\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"482\",\n                    \"name\": \"Специализированный межрайонный административный суд области Ұлытау\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"476\",\n                    \"name\": \"Суд области Ұлытау\",\n                    \"instance_code\": \"00\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"abay\": {\n            \"id\": \"21\",\n            \"name\": \"Область Абай\",\n            \"kato_code\": \"10\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"467\",\n                    \"name\": \"Специализированный межрайонный административный суд области Абай\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"456\",\n                    \"name\": \"Суд области Абай\",\n                    \"instance_code\": \"00\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        },\n        \"zhetysu\": {\n            \"id\": \"22\",\n            \"name\": \"Область Жетісу\",\n            \"kato_code\": \"33\",\n            \"courts\": {\n                \"smas\": {\n                    \"id\": \"450\",\n                    \"name\": \"Специализированный межрайонный административный суд области Жетісу\",\n                    \"instance_code\": \"94\",\n                    \"case_type_code\": \"4\"\n                },\n                \"appellate\": {\n                    \"id\": \"437\",\n                    \"name\": \"Суд области Жетісу\",\n                    \"instance_code\": \"00\",\n                    \"case_type_code\": \"4а\"\n                }\n            }\n        }\n    },\n    \"retry_settings\": {\n        \"http_request\": {\n            \"max_attempts\": 3,\n            \"initial_delay\": 1.0,\n            \"backoff_multiplier\": 2.0,\n            \"max_delay\": 30.0,\n            \"jitter\": true,\n            \"retriable_status_codes\": [500, 502, 503, 504],\n            \"retriable_exceptions\": [\"TimeoutError\", \"ClientError\", \"ServerDisconnectedError\"]\n        },\n        \"authentication\": {\n            \"max_attempts\": 5,\n            \"initial_delay\": 2.0,\n            \"backoff_multiplier\": 2.0,\n            \"max_delay\": 60.0,\n            \"create_new_session\": true,\n            \"retriable_on_auth_check_fail\": true\n        },\n        \"search_case\": {\n            \"max_attempts\": 3,\n            \"delay\": 3.0,\n            \"backoff\": \"linear\",\n            \"save_failed_html\": true\n        },\n        \"rate_limit\": {\n            \"default_wait\": 60,\n            \"respect_retry_after_header\": true,\n            \"slow_down_multiplier\": 2.0,\n            \"slow_down_duration\": 600\n        },\n        \"circuit_breaker\": {\n            \"enabled\": true,\n            \"failure_threshold\": 20,\n            \"recovery_timeout\": 300,\n            \"half_open_max_attempts\": 3\n        },\n        \"session_recovery\": {\n            \"reauth_on_401\": true,\n            \"max_reauth_attempts\": 2\n        }\n    },\n    \"update_settings\": {\n        \"enabled\": true,\n        \"update_interval_days\": 2,\n        \"filters\": {\n            \"defendant_keywords\": [\"доход\"],\n            \"exclude_event_types\": [\n                \"Отправлено в архив\",\n                \"Завершение дела\",\n                \"Решение вступило в силу\"\n            ]\n        }\n    }\n}"
}

def get_embedded_resource(name: str) -> str:
    """Получить встроенный ресурс по имени"""
    return _EMBEDDED_RESOURCES.get(name, "")

def get_embedded_json(name: str) -> dict:
    """Получить встроенный JSON как словарь"""
    content = _EMBEDDED_RESOURCES.get(name, "{}")
    return json.loads(content)

def get_embedded_binary(name: str) -> bytes:
    """Получить бинарный ресурс (декодирует из base64)"""
    content = _EMBEDDED_RESOURCES.get(name, "")
    return base64.b64decode(content) if content else b""



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
# МОДУЛЬ: parsers/court_parser/utils/retry.py
# ============================================================================

"""
Гибкая система retry с поддержкой различных стратегий
"""



# REMOVED IMPORT: from utils.logger import get_logger


class RetryableError(Exception):
    """Ошибка, которую можно повторить"""
    pass


class NonRetriableError(Exception):
    """Ошибка, которую нельзя повторить"""
    pass


class CircuitBreakerOpenError(Exception):
    """Circuit Breaker в состоянии OPEN"""
    pass


class RetryConfig:
    """Конфигурация retry"""
    
    def __init__(self, config: dict):
        self.max_attempts = config.get('max_attempts', 3)
        self.initial_delay = config.get('initial_delay', 1.0)
        self.backoff_multiplier = config.get('backoff_multiplier', 2.0)
        self.max_delay = config.get('max_delay', 30.0)
        self.jitter = config.get('jitter', True)
        self.backoff = config.get('backoff', 'exponential')  # exponential или linear
        
        # Для HTTP retry
        self.retriable_status_codes = config.get('retriable_status_codes', [500, 502, 503, 504])
        self.retriable_exceptions = config.get('retriable_exceptions', [])


class CircuitBreaker:
    """
    Circuit Breaker паттерн
    
    Состояния:
    - CLOSED: нормальная работа
    - OPEN: сервер недоступен, не пытаемся
    - HALF_OPEN: проверка восстановления
    """
    
    def __init__(self, config: dict):
        self.enabled = config.get('enabled', True)
        self.failure_threshold = config.get('failure_threshold', 20)
        self.recovery_timeout = config.get('recovery_timeout', 300)  # секунд
        self.half_open_max_attempts = config.get('half_open_max_attempts', 3)
        
        self.state = 'CLOSED'
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_success_count = 0
        
        self.logger = get_logger('circuit_breaker')
    
    def record_success(self):
        """Запись успешного запроса"""
        if not self.enabled:
            return
        
        if self.state == 'HALF_OPEN':
            self.half_open_success_count += 1
            
            if self.half_open_success_count >= self.half_open_max_attempts:
                self.logger.info("🎉 Circuit Breaker: HALF_OPEN → CLOSED (сервер восстановлен)")
                self.state = 'CLOSED'
                self.failure_count = 0
                self.half_open_success_count = 0
        
        elif self.state == 'CLOSED':
            # Сбрасываем счетчик при успехе
            self.failure_count = max(0, self.failure_count - 1)
    
    def record_failure(self):
        """Запись неудачного запроса"""
        if not self.enabled:
            return
        
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.state == 'CLOSED' and self.failure_count >= self.failure_threshold:
            self.logger.critical(
                f"🚨 Circuit Breaker: CLOSED → OPEN "
                f"({self.failure_count} ошибок подряд)"
            )
            self.state = 'OPEN'
        
        elif self.state == 'HALF_OPEN':
            self.logger.warning("Circuit Breaker: HALF_OPEN → OPEN (проверка не пройдена)")
            self.state = 'OPEN'
            self.half_open_success_count = 0
    
    def can_execute(self) -> bool:
        """Можно ли выполнить запрос"""
        if not self.enabled:
            return True
        
        if self.state == 'CLOSED':
            return True
        
        if self.state == 'HALF_OPEN':
            return True
        
        # state == 'OPEN'
        if self.last_failure_time:
            elapsed = (datetime.now() - self.last_failure_time).total_seconds()
            
            if elapsed >= self.recovery_timeout:
                self.logger.info(
                    f"Circuit Breaker: OPEN → HALF_OPEN "
                    f"(пауза {self.recovery_timeout} сек прошла)"
                )
                self.state = 'HALF_OPEN'
                self.half_open_success_count = 0
                return True
        
        return False
    
    def get_wait_time(self) -> Optional[float]:
        """Сколько ждать до следующей попытки (если OPEN)"""
        if self.state != 'OPEN' or not self.last_failure_time:
            return None
        
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        remaining = self.recovery_timeout - elapsed
        
        return max(0, remaining)


class RetryStrategy:
    """Стратегия retry с гибкими настройками"""
    
    def __init__(self, config: RetryConfig, circuit_breaker: Optional[CircuitBreaker] = None):
        self.config = config
        self.circuit_breaker = circuit_breaker
        self.logger = get_logger('retry_strategy')
    
    def calculate_delay(self, attempt: int) -> float:
        """Расчет задержки перед следующей попыткой"""
        if self.config.backoff == 'linear':
            delay = self.config.initial_delay
        else:  # exponential
            delay = self.config.initial_delay * (self.config.backoff_multiplier ** (attempt - 1))
        
        # Ограничение максимальной задержки
        delay = min(delay, self.config.max_delay)
        
        # Jitter (случайность ±20%)
        if self.config.jitter:
            jitter_range = delay * 0.2
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)
    
    def is_retriable_exception(self, exc: Exception) -> bool:
        """Проверка что исключение можно повторить"""
        exc_name = type(exc).__name__
        
        # Проверка по списку из конфига
        if exc_name in self.config.retriable_exceptions:
            return True
        
        # Стандартные retriable исключения
        retriable_types = (
            asyncio.TimeoutError,
            aiohttp.ClientError,
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientConnectionError,
            ConnectionError,
            RetryableError
        )
        
        return isinstance(exc, retriable_types)
    
    def is_retriable_status(self, status: int) -> bool:
        """Проверка что HTTP статус можно повторить"""
        return status in self.config.retriable_status_codes
    
    async def execute_with_retry(self, 
                                func: Callable,
                                *args,
                                error_context: str = "",
                                **kwargs) -> Any:
        """
        Выполнение функции с retry
        
        Args:
            func: асинхронная функция для выполнения
            error_context: контекст для логирования (например, "HTTP GET /api")
            *args, **kwargs: параметры для func
        
        Raises:
            NonRetriableError: если ошибка не подлежит retry
            CircuitBreakerOpenError: если Circuit Breaker в состоянии OPEN
        """
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            # Проверка Circuit Breaker
            if self.circuit_breaker and not self.circuit_breaker.can_execute():
                wait_time = self.circuit_breaker.get_wait_time()
                
                if wait_time and wait_time > 0:
                    self.logger.warning(
                        f"⏸️ Circuit Breaker OPEN, ждем {wait_time:.0f} сек..."
                    )
                    await asyncio.sleep(wait_time)
                    
                    # Повторная проверка после ожидания
                    if not self.circuit_breaker.can_execute():
                        raise CircuitBreakerOpenError(
                            f"Circuit Breaker в состоянии OPEN, сервер недоступен"
                        )
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit Breaker в состоянии OPEN"
                    )
            
            try:
                # Попытка выполнения
                result = await func(*args, **kwargs)
                
                # Успех
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()
                
                if attempt > 1:
                    self.logger.info(f"✅ Успех на попытке {attempt}")
                
                return result
            
            except NonRetriableError:
                # Не повторяемая ошибка - прокидываем наверх
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()  # Не считаем как failure
                raise
            
            except Exception as exc:
                last_exception = exc
                
                # Проверяем можно ли повторить
                if not self.is_retriable_exception(exc):
                    self.logger.error(f"❌ Non-retriable error: {type(exc).__name__}: {exc}")
                    raise NonRetriableError(f"{type(exc).__name__}: {exc}") from exc
                
                # Записываем failure
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()
                
                # Если это последняя попытка - выбрасываем ошибку
                if attempt >= self.config.max_attempts:
                    self.logger.error(
                        f"❌ Все {self.config.max_attempts} попытки исчерпаны"
                    )
                    raise RetryableError(
                        f"Не удалось выполнить после {self.config.max_attempts} попыток: {exc}"
                    ) from exc
                
                # Логирование и задержка перед следующей попыткой
                delay = self.calculate_delay(attempt)
                
                self.logger.warning(
                    f"⚠️ [{error_context}] {type(exc).__name__} "
                    f"(попытка {attempt}/{self.config.max_attempts}), "
                    f"повтор через {delay:.1f} сек"
                )
                
                await asyncio.sleep(delay)
        
        # Не должно сюда дойти, но на всякий случай
        raise RetryableError(f"Unexpected retry exhaustion") from last_exception


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
        """
        Проверка успешности авторизации
        
        Raises:
            aiohttp.ClientError: при HTTP 502, 503, 504 (retriable ошибки)
            NonRetriableError: при HTTP 401, 403 (постоянные ошибки)
        
        Returns:
            True если авторизация успешна
            False если не удалось определить (будет обработано в _do_authenticate)
        """
        url = f"{self.base_url}/form/proceedings/services.xhtml"
        
        try:
            async with session.get(url, headers=self._get_base_headers()) as response:
                # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: обработка HTTP ошибок
                
                # Постоянные ошибки (не авторизован)
                if response.status in [401, 403]:
                    self.logger.error(f"HTTP {response.status}: Неверные учетные данные")
                    raise NonRetriableError(f"HTTP {response.status}: Авторизация отклонена сервером")
                
                # Временные ошибки сервера (retry)
                if response.status in [500, 502, 503, 504]:
                    self.logger.warning(f"HTTP {response.status}: Временная ошибка сервера")
                    raise aiohttp.ClientError(f"HTTP {response.status}: Сервер недоступен")
                
                # Успешный ответ
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
        
        except (aiohttp.ClientError, NonRetriableError):
            # Пробрасываем исключения дальше (для retry логики)
            raise
        
        except Exception as e:
            # Неожиданная ошибка
            self.logger.error(f"Неожиданная ошибка при проверке авторизации: {e}")
            raise aiohttp.ClientError(f"Ошибка проверки авторизации: {e}")
    
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
        """
        defendant_keywords = filters.get('defendant_keywords', [])
        exclude_events = filters.get('exclude_event_types', [])
        interval_days = filters.get('update_interval_days', 2)
        
        # Построение SQL запроса
        query = """
            SELECT DISTINCT c.case_number, c.case_date
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
        
        # Извлекаем только номера дел
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

    async def get_existing_case_numbers(
        self, 
        region_key: str, 
        court_key: str, 
        year: str,
        settings
    ) -> Set[int]:
        """
        Получить множество существующих порядковых номеров дел для региона/суда/года
        
        Args:
            region_key: ключ региона ('astana', 'almaty', ...)
            court_key: ключ суда ('smas', 'appellate')
            year: год ('2025')
            settings: экземпляр Settings для получения конфигурации
        
        Returns:
            {1, 2, 5, 10, 15, 23, 45, 67, 89, 100, ...}
        
        Example:
            >>> existing = await db.get_existing_case_numbers('astana', 'smas', '2025', settings)
            >>> 1075 in existing
            True
        """
        # Получаем конфигурацию для формирования префикса номера
        region_config = settings.get_region(region_key)
        court_config = settings.get_court(region_key, court_key)
        
        # Формируем префикс номера дела
        # Например: "6294-25-00-4/" для Астаны, SMAS, 2025
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]  # "2025" → "25"
        case_type = court_config['case_type_code']
        
        prefix = f"{kato}{instance}-{year_short}-00-{case_type}/"
        
        # SQL запрос
        query = """
            SELECT case_number
            FROM cases
            WHERE case_number LIKE $1
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, f"{prefix}%")
        
        # Извлекаем порядковые номера
        sequence_numbers = set()
        
        for row in rows:
            case_number = row['case_number']
            # Извлекаем порядковый номер из "6294-25-00-4/1075"
            if '/' in case_number:
                try:
                    seq_str = case_number.split('/')[-1]
                    seq_num = int(seq_str)
                    sequence_numbers.add(seq_num)
                except (ValueError, IndexError):
                    # Некорректный формат - пропускаем
                    self.logger.warning(f"Некорректный формат номера: {case_number}")
                    continue
        
        self.logger.info(
            f"Загружено существующих номеров для {region_key}/{court_key}/{year}: {len(sequence_numbers)}"
        )
        
        return sequence_numbers
    
    async def get_last_sequence_number(
        self, 
        region_key: str, 
        court_key: str, 
        year: str,
        settings
    ) -> int:
        """
        Получить последний (максимальный) порядковый номер дела для региона/суда/года
        
        Args:
            region_key: ключ региона ('astana')
            court_key: ключ суда ('smas', 'appellate')
            year: год ('2025')
            settings: экземпляр Settings
        
        Returns:
            Максимальный порядковый номер или 0 если дел нет
        
        Example:
            >>> last = await db.get_last_sequence_number('astana', 'smas', '2025', settings)
            >>> last
            1075
        """
        region_config = settings.get_region(region_key)
        court_config = settings.get_court(region_key, court_key)
        
        # Формируем префикс номера дела
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        
        prefix = f"{kato}{instance}-{year_short}-00-{case_type}/"
        
        query = """
            SELECT case_number
            FROM cases
            WHERE case_number LIKE $1
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, f"{prefix}%")
        
        if not rows:
            self.logger.info(f"Дел для {region_key}/{court_key}/{year} не найдено, начинаем с 1")
            return 0
        
        # Извлекаем максимальный порядковый номер
        max_sequence = 0
        
        for row in rows:
            case_number = row['case_number']
            if '/' in case_number:
                try:
                    seq_str = case_number.split('/')[-1]
                    seq_num = int(seq_str)
                    if seq_num > max_sequence:
                        max_sequence = seq_num
                except (ValueError, IndexError):
                    continue
        
        self.logger.info(
            f"Последний номер для {region_key}/{court_key}/{year}: {max_sequence}"
        )
        
        return max_sequence


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
# МОДУЛЬ: parsers/court_parser/search/form_handler.py
# ============================================================================

"""
Работа с поисковой формой
"""
# REMOVED IMPORT: from utils.retry import NonRetriableError

# REMOVED IMPORT: from utils.logger import get_logger


class FormHandler:
    """Обработчик поисковой формы с кешированием ID"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('form_handler')
        
        # Кеш ID формы (извлекается один раз за сессию)
        self._cached_form_ids: Optional[Dict[str, str]] = None
        self._cache_initialized: bool = False
    
    def reset_cache(self):
        """
        Сброс кеша ID формы
        
        Вызывать при:
        - Переавторизации
        - Ошибках, связанных с невалидными ID
        """
        self._cached_form_ids = None
        self._cache_initialized = False
        self.logger.debug("Кеш ID формы сброшен")
    
    async def prepare_search_form(self, session: aiohttp.ClientSession) -> tuple:
        """
        Подготовка формы поиска
        
        - ViewState: извлекается КАЖДЫЙ раз (уникален для каждого запроса)
        - Form IDs: извлекаются ОДИН раз и кешируются
        
        Returns:
            (viewstate, form_ids)
        """
        url = f"{self.base_url}/form/lawsuit/"
        headers = self._get_headers()
        
        try:
            async with session.get(url, headers=headers) as response:
                # Обработка HTTP ошибок
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}: Постоянная ошибка")
                
                if response.status in [500, 502, 503, 504]:
                    raise aiohttp.ClientError(f"HTTP {response.status}: Сервер недоступен")
                
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}: Неожиданная ошибка")
                
                html = await response.text()
                
                # ViewState — всегда извлекаем заново
                viewstate = self._extract_viewstate(html)
                
                # Form IDs — извлекаем только один раз
                if not self._cache_initialized:
                    self._cached_form_ids = self._extract_form_ids(html)
                    self._cache_initialized = True
                    
                    self.logger.info("📋 ID формы извлечены и закешированы:")
                    for key, value in self._cached_form_ids.items():
                        self.logger.info(f"   {key}: {value}")
                
                return viewstate, self._cached_form_ids
        
        except (aiohttp.ClientError, NonRetriableError):
            raise
        
        except Exception as e:
            self.logger.error(f"Ошибка подготовки формы: {e}")
            raise aiohttp.ClientError(f"Ошибка подготовки формы: {e}")
    
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
        
        try:
            async with session.post(url, data=data, headers=headers) as response:
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")
                
                if response.status in [500, 502, 503, 504]:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                self.logger.debug(f"Регион выбран: {region_id}")
        
        except (aiohttp.ClientError, NonRetriableError):
            raise
        
        except Exception as e:
            raise aiohttp.ClientError(f"Ошибка выбора региона: {e}")
    
    def _extract_viewstate(self, html: str) -> Optional[str]:
        """Извлечение ViewState"""
        parser = HTMLParser(html)
        viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')
        
        if viewstate_input and viewstate_input.attributes:
            return viewstate_input.attributes.get('value')
        return None
    
    def _extract_form_ids(self, html: str) -> Dict[str, str]:
        """Извлечение ID элементов формы"""
        parser = HTMLParser(html)
        ids = {}
        
        # Поиск базового ID формы
        form = parser.css_first('form')
        if form and form.attributes and form.attributes.get('id'):
            ids['form_id'] = form.attributes['id']
        
        # Поиск полей формы
        field_mappings = ['edit-district', 'edit-court', 'edit-year', 'edit-num']
        
        for field in field_mappings:
            elements = parser.css(f'[id*="{field}"]')
            for element in elements:
                if element.attributes and element.attributes.get('id'):
                    ids[field] = element.attributes['id']
                    name = element.attributes.get('name', '')
                    if ':' in name:
                        ids['form_base'] = ':'.join(name.split(':')[:-1])
                    break
        
        # Извлечение ID кнопки поиска
        search_button = self._extract_search_button_id(html, ids.get('form_base', ''))
        if search_button:
            ids['search_button'] = search_button
        else:
            self.logger.warning("ID кнопки поиска не найден, будет использован fallback")
        
        return ids
    
    def _extract_search_button_id(self, html: str, form_base: str) -> Optional[str]:
        """
        Извлечение ID кнопки поиска из RichFaces скрипта
        
        Ищет паттерн: goNext = function(...) { RichFaces.ajax("ID", ...)
        """
        
        pattern = r'goNext\s*=\s*function\s*\([^)]*\)\s*\{\s*RichFaces\.ajax\s*\(\s*["\']([^"\']+)["\']'
        match = re.search(pattern, html)
        
        if match:
            button_id = match.group(1)
            
            # Валидация: ID должен начинаться с form_base
            if form_base and not button_id.startswith(form_base):
                self.logger.warning(
                    f"ID '{button_id}' не соответствует form_base '{form_base}'"
                )
                return None
            
            return button_id
        
        return None
    
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
# REMOVED IMPORT: from utils.retry import NonRetriableError


class SearchEngine:
    """Движок для поиска дел"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('search_engine')
    
    async def search_case(
        self, 
        session: aiohttp.ClientSession,
        viewstate: str, 
        region_id: str, 
        court_id: str,
        year: str, 
        sequence_number: int,
        form_ids: Dict[str, str]
    ) -> str:
        """
        Поиск дела по порядковому номеру
        
        Args:
            sequence_number: порядковый номер (1, 2, 3, ...)
        
        Returns:
            HTML с результатами
        """
        await self._send_search_request(
            session, viewstate, region_id, court_id,
            year, sequence_number, form_ids
        )
        
        await asyncio.sleep(0.5)
        
        results_html = await self._get_results(session)
        
        self.logger.debug(f"Поиск выполнен для номера: {sequence_number}")
        return results_html
    
    async def _send_search_request(
        self, 
        session: aiohttp.ClientSession,
        viewstate: str, 
        region_id: str, 
        court_id: str,
        year: str, 
        sequence_number: int,
        form_ids: Dict[str, str]
    ):
        """
        Отправка поискового запроса
        
        В edit-num всегда передаётся только порядковый номер
        """
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('form_base', 'j_idt45:j_idt46')
        
        search_button = form_ids.get('search_button')
        if not search_button:
            search_button = f'{form_base}:j_idt83'
            self.logger.warning(f"Fallback ID кнопки: {search_button}")
        
        # Всегда передаём только порядковый номер
        search_number = str(sequence_number)
        
        data = {
            form_base: form_base,
            f'{form_base}:edit-district': region_id,
            f'{form_base}:edit-district-hide': region_id,
            f'{form_base}:edit-court': court_id,
            f'{form_base}:edit-year': year,
            f'{form_base}:edit-iin': '',
            f'{form_base}:edit-num': search_number,
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
        
        self.logger.debug(f"🔍 Поиск: регион={region_id}, суд={court_id}, год={year}, номер={search_number}")
        
        headers = self._get_ajax_headers()
        
        try:
            async with session.post(url, data=data, headers=headers) as response:
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")
                
                if response.status in [500, 502, 503, 504]:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                await response.text()
        
        except (aiohttp.ClientError, NonRetriableError):
            raise
        except Exception as e:
            raise aiohttp.ClientError(f"Ошибка поиска: {e}")
    
    async def _get_results(self, session: aiohttp.ClientSession) -> str:
        """Получение страницы с результатами"""
        url = f"{self.base_url}/lawsuit/lawsuitList.xhtml"
        headers = self._get_headers()
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")
                
                if response.status in [500, 502, 503, 504]:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                return await response.text()
        
        except (aiohttp.ClientError, NonRetriableError):
            raise
        except Exception as e:
            raise aiohttp.ClientError(f"Ошибка получения результатов: {e}")
    
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
    """Главный класс парсера"""
    
    def __init__(self, config_path: Optional[str] = None):
        # Загрузка конфигурации
        self.settings = Settings(config_path)
        
        # Retry конфигурация
        self.retry_config = self.settings.config.get('retry_settings', {})
        
        # Инициализация компонентов
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
        
        # Lock для stateful операций
        self.form_lock = asyncio.Lock()
        
        # Счётчики ошибок
        self.session_error_count = 0
        self.max_session_errors = 10
        self.reauth_count = 0
        self.max_reauth = self.retry_config.get('session_recovery', {}).get(
            'max_reauth_attempts', 2
        )
        
        
        self.logger = get_logger('court_parser')
        self.logger.info("🚀 Парсер инициализирован")
    
    async def initialize(self):
        """Инициализация"""
        try:
            await self.db_manager.connect()
            await self.authenticator.authenticate(self.session_manager)
            self.logger.info("✅ Парсер готов к работе")
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self):
        """Очистка ресурсов"""
        try:
            await self.db_manager.disconnect()
        except:
            pass
        
        try:
            await self.session_manager.close()
        except:
            pass
        
        self.logger.info("Ресурсы очищены")
    
    async def search_and_save(
        self, 
        region_key: str, 
        court_key: str,
        sequence_number: int, 
        year: str = "2025"
    ) -> Dict[str, Any]:
        """
        Поиск и сохранение дела
        
        Args:
            region_key: ключ региона ('astana')
            court_key: ключ суда ('smas', 'appellate')
            sequence_number: порядковый номер (1, 2, 3, ...)
            year: год ('2025')
        
        Returns:
            {
                'success': True/False,
                'saved': True/False,
                'case_number': '6294-25-00-4/215',
                'error': None или строка
            }
        """
        search_retry_config = self.retry_config.get('search_case', {})
        
        if not search_retry_config:
            return await self._do_search_and_save(
                region_key, court_key, sequence_number, year
            )
        
        # С retry
        retry_cfg = RetryConfig(search_retry_config)
        strategy = RetryStrategy(retry_cfg, self.session_manager.circuit_breaker)
        
        async def _search_with_recovery():
            try:
                return await self._do_search_and_save(
                    region_key, court_key, sequence_number, year
                )
            except Exception as e:
                if await self._handle_session_recovery(e):
                    return await self._do_search_and_save(
                        region_key, court_key, sequence_number, year
                    )
                raise
        
        try:
            result = await strategy.execute_with_retry(
                _search_with_recovery,
                error_context=f"Поиск дела #{sequence_number}"
            )
            self.session_error_count = 0
            return result
        
        except NonRetriableError as e:
            return {
                'success': False,
                'saved': False,
                'case_number': None,
                'error': str(e)
            }
        
        except Exception as e:
            self.session_error_count += 1
            self.logger.error(f"❌ Ошибка поиска: {e}")
            return {
                'success': False,
                'saved': False,
                'case_number': None,
                'error': str(e)
            }
    
    async def _do_search_and_save(
        self, 
        region_key: str, 
        court_key: str,
        sequence_number: int, 
        year: str
    ) -> Dict[str, Any]:
        """
        Один цикл поиска и сохранения
        """
        region_config = self.settings.get_region(region_key)
        court_config = self.settings.get_court(region_key, court_key)
        
        target_case_number = self.text_processor.generate_case_number(
            region_config, court_config, year, sequence_number
        )
        
        self.logger.info(f"🔍 Ищу дело: {target_case_number}")
        
        # Работа с формой
        async with self.form_lock:
            session = await self.session_manager.get_session()
            
            viewstate, form_ids = await self.form_handler.prepare_search_form(session)
            
            await self.form_handler.select_region(
                session, viewstate, region_config['id'], form_ids
            )
            
            await asyncio.sleep(1)
            
            results_html = await self.search_engine.search_case(
                session, viewstate, 
                region_config['id'], 
                court_config['id'],
                year, 
                sequence_number,
                form_ids
            )
        
        # Парсинг результатов
        cases = self.results_parser.parse(results_html)
        
        if not cases:
            self.logger.info(f"❌ Ничего не найдено: {target_case_number}")
            return {
                'success': False,
                'saved': False,
                'case_number': target_case_number,
                'error': 'no_results'
            }
        
        # Выбор дела для сохранения
        case_to_save = self._select_case_to_save(
            cases, court_key, target_case_number
        )
        
        if not case_to_save:
            self.logger.warning(f"⚠️ Целевое дело не найдено: {target_case_number}")
            return {
                'success': False,
                'saved': False,
                'case_number': target_case_number,
                'error': 'target_not_found'
            }
        
        # Сохранение
        save_result = await self.db_manager.save_case(case_to_save)
        
        if save_result['status'] in ['saved', 'updated']:
            judge_info = "✅ судья" if case_to_save.judge else "⚠️ без судьи"
            parties = len(case_to_save.plaintiffs) + len(case_to_save.defendants)
            events = len(case_to_save.events)
            
            self.logger.info(
                f"✅ Сохранено: {case_to_save.case_number} "
                f"({judge_info}, {parties} сторон, {events} событий)"
            )
            
            return {
                'success': True,
                'saved': True,
                'case_number': case_to_save.case_number
            }
        
        return {
            'success': False,
            'saved': False,
            'case_number': target_case_number,
            'error': 'save_failed'
        }
    
    def _select_case_to_save( self, 
        cases: List[CaseData], 
        court_key: str, 
        target_case_number: str
    ) -> Optional[CaseData]:
        """
        Выбор дела для сохранения в зависимости от типа суда
        
        Args:
            cases: список найденных дел
            court_key: тип суда ('smas', 'appellate')
            target_case_number: целевой номер дела
        
        Returns:
            CaseData или None
        """
        if court_key == 'smas':
            # СМЭС: сервер возвращает одно дело — берём первое
            if cases:
                return cases[0]
            return None
        
        elif court_key == 'appellate':
            # Апелляция: ищем целевое дело по точному номеру
            for case in cases:
                if case.case_number == target_case_number:
                    return case
            
            # Если не нашли точное совпадение — логируем что вернулось
            self.logger.debug(
                f"Апелляция: получено {len(cases)} дел, "
                f"целевое {target_case_number} не найдено"
            )
            for case in cases:
                self.logger.debug(f"  - {case.case_number}")
            
            return None
        
        else:
            # Другие типы судов: по умолчанию ищем целевое
            for case in cases:
                if case.case_number == target_case_number:
                    return case
            return cases[0] if cases else None
    
    async def _handle_session_recovery(self, error: Exception) -> bool:
        """Восстановление сессии"""
        if not (isinstance(error, (aiohttp.ClientError, NonRetriableError)) 
                and '401' in str(error)):
            return False
        
        if self.reauth_count >= self.max_reauth:
            return False
        
        self.reauth_count += 1
        self.logger.warning(f"⚠️ Переавторизация ({self.reauth_count}/{self.max_reauth})...")
        
        try:
            await self.authenticator.authenticate(self.session_manager)
            self.form_handler.reset_cache()
            self.session_error_count = 0
            self.logger.info("✅ Переавторизация успешна")
            return True
        except Exception as e:
            self.logger.error(f"❌ Переавторизация не удалась: {e}")
            return False
    
    
    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()
        return False


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
# МОДУЛЬ: parsers/court_parser/__init__.py
# ============================================================================

"""
Court Parser - Парсер судебных дел Казахстана
"""
__version__ = "2.0.0"
__author__ = "Your Name"
# REMOVED IMPORT: from core.parser import CourtParser
__all__ = ['CourtParser']


# ============================================================================
# МОДУЛЬ: parsers/court_parser/main.py
# ============================================================================

"""
Точка входа парсера
"""

# REMOVED IMPORT: from core.parser import CourtParser
# REMOVED IMPORT: from config.settings import Settings
# REMOVED IMPORT: from utils.logger import setup_logger
# REMOVED IMPORT: from utils.text_processor import TextProcessor


async def parse_all_regions_from_config() -> dict:
    """Парсинг всех регионов согласно настройкам из config.json"""
    logger = setup_logger('main', level='INFO')
    
    # Загрузка настроек
    settings = Settings()
    ps = settings.parsing_settings
    
    year = ps.get('year', '2025')
    court_types = ps.get('court_types', ['smas'])
    start_from = ps.get('start_from', 1)
    max_number = ps.get('max_number', 9999)
    max_consecutive_empty = ps.get('max_consecutive_empty', 200)
    delay_between_requests = ps.get('delay_between_requests', 2)
    max_parallel_regions = ps.get('max_parallel_regions', 1)
    
    # Настройки retry на уровне региона
    region_retry_max_attempts = ps.get('region_retry_max_attempts', 3)
    region_retry_delay = ps.get('region_retry_delay_seconds', 5)
    
    # ЛИМИТЫ ДЛЯ ТЕСТИРОВАНИЯ
    limit_regions = settings.get_limit_regions()
    limit_cases_per_region = settings.get_limit_cases_per_region()
    
    logger.info("=" * 70)
    logger.info(f"МАССОВЫЙ ПАРСИНГ: {', '.join(court_types)} ({year})")
    logger.info("=" * 70)
    logger.info(f"Настройки из config.json:")
    logger.info(f"  Год: {year}")
    logger.info(f"  Типы судов: {', '.join(court_types)}")
    logger.info(f"  Диапазон номеров: {start_from}-{max_number}")
    logger.info(f"  Лимит пустых подряд: {max_consecutive_empty}")
    logger.info(f"  Задержка между запросами: {delay_between_requests} сек")
    logger.info(f"  Параллельных регионов: {max_parallel_regions}")
    logger.info(f"  Retry на регион: {region_retry_max_attempts} попыток")
    
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
    
    # Общая статистика
    total_stats = {
        'regions_processed': 0,
        'regions_failed': 0,
        'total_queries': 0,
        'total_cases_saved': 0
    }
    stats_lock = asyncio.Lock()
    
    # Семафор для контроля параллельности
    semaphore = asyncio.Semaphore(max_parallel_regions)
    
    # Создаём парсер один раз
    async with CourtParser() as parser:
        
        async def process_region_with_retry(region_key: str):
            """Обработка региона с retry"""
            async with semaphore:
                region_config = settings.get_region(region_key)
                
                for attempt in range(1, region_retry_max_attempts + 1):
                    try:
                        logger.info(f"\n{'='*70}")
                        if attempt > 1:
                            logger.info(f"🔄 Регион: {region_config['name']} (попытка {attempt}/{region_retry_max_attempts})")
                        else:
                            logger.info(f"Регион: {region_config['name']}")
                        logger.info(f"{'='*70}")
                        
                        # Парсинг всех судов региона
                        region_stats = await process_region_all_courts(
                            parser=parser,
                            settings=settings,
                            region_key=region_key,
                            court_types=court_types,
                            year=year,
                            start_from=start_from,
                            max_number=max_number,
                            max_consecutive_empty=max_consecutive_empty,
                            delay_between_requests=delay_between_requests,
                            limit_cases=limit_cases_per_region
                        )
                        
                        # Успех → обновляем статистику
                        async with stats_lock:
                            total_stats['regions_processed'] += 1
                            total_stats['total_queries'] += region_stats['total_queries']
                            total_stats['total_cases_saved'] += region_stats['total_cases_saved']
                        
                        return region_stats
                    
                    except Exception as e:
                        if attempt < region_retry_max_attempts:
                            logger.warning(f"⚠️ Регион {region_config['name']}: ошибка (попытка {attempt})")
                            logger.warning(f"   {e}")
                            await parser.session_manager.create_session()
                            await asyncio.sleep(region_retry_delay)
                        else:
                            logger.error(f"❌ Регион {region_config['name']} failed")
                            logger.error(traceback.format_exc())
                            async with stats_lock:
                                total_stats['regions_failed'] += 1
                            return None
        
        # Запускаем все регионы
        tasks = [process_region_with_retry(r) for r in regions_to_process]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # Итоговая статистика
    logger.info("\n" + "=" * 70)
    logger.info("ОБЩАЯ СТАТИСТИКА:")
    logger.info(f"  Обработано регионов: {total_stats['regions_processed']}")
    if total_stats['regions_failed'] > 0:
        logger.info(f"  Регионов с ошибками: {total_stats['regions_failed']}")
    logger.info(f"  Всего запросов: {total_stats['total_queries']}")
    logger.info(f"  Всего сохранено: {total_stats['total_cases_saved']}")
    logger.info("=" * 70)
    
    return total_stats


async def process_region_all_courts(
    parser,
    settings,
    region_key: str,
    court_types: List[str],
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay_between_requests: float,
    limit_cases: Optional[int] = None
) -> dict:
    """Обработка всех судов региона"""
    logger = setup_logger('main', level='INFO')
    region_config = settings.get_region(region_key)
    
    region_stats = {
        'region_key': region_key,
        'courts_processed': 0,
        'total_queries': 0,
        'total_cases_saved': 0,
        'courts_stats': {}
    }
    
    for court_key in court_types:
        court_config = region_config['courts'].get(court_key)
        if not court_config:
            logger.warning(f"⚠️ Суд {court_key} не найден в регионе {region_key}")
            continue
            
        logger.info(f"\n📍 Суд: {court_config['name']}")
        
        try:
            court_stats = await parse_court(
                parser=parser,
                settings=settings,
                region_key=region_key,
                court_key=court_key,
                year=year,
                start_from=start_from,
                max_number=max_number,
                max_consecutive_empty=max_consecutive_empty,
                delay_between_requests=delay_between_requests,
                limit_cases=limit_cases
            )
            
            region_stats['courts_processed'] += 1
            region_stats['total_queries'] += court_stats['queries_made']
            region_stats['total_cases_saved'] += court_stats['cases_saved']
            region_stats['courts_stats'][court_key] = court_stats
            
        except Exception as e:
            logger.error(f"❌ Ошибка суда {court_key}: {e}")
            logger.error(traceback.format_exc())
            continue
    
    # Итоги региона
    logger.info(f"\n{'-'*70}")
    logger.info(f"ИТОГИ РЕГИОНА {region_config['name']}:")
    logger.info(f"  Судов: {region_stats['courts_processed']}/{len(court_types)}")
    logger.info(f"  Запросов: {region_stats['total_queries']}")
    logger.info(f"  Сохранено: {region_stats['total_cases_saved']}")
    logger.info(f"{'-'*70}")
    
    return region_stats


async def parse_court(
    parser,
    settings,
    region_key: str,
    court_key: str,
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay_between_requests: float,
    limit_cases: Optional[int] = None
) -> dict:
    """Парсинг одного суда"""
    logger = setup_logger('main', level='INFO')
    court_config = settings.get_court(region_key, court_key)
    
    stats = {
        'queries_made': 0,
        'cases_saved': 0,
        'consecutive_empty': 0
    }
    
    # Определяем стартовый номер
    last_in_db = await parser.db_manager.get_last_sequence_number(
        region_key, court_key, year, settings
    )
    
    if last_in_db > 0:
        actual_start = last_in_db + 1
        logger.info(f"📥 Последний в БД: {last_in_db}")
        logger.info(f"▶️  Продолжаю с: {actual_start}")
    else:
        actual_start = start_from
        logger.info(f"📥 БД пуста, старт с: {actual_start}")
    
    if actual_start > max_number:
        logger.info(f"✅ Все номера до {max_number} уже спарсены")
        return stats
    
    current_number = actual_start
    
    while current_number <= max_number:
        # Проверка лимитов
        if limit_cases and stats['queries_made'] >= limit_cases:
            logger.info(f"🔒 Лимит запросов ({limit_cases})")
            break
        
        if stats['consecutive_empty'] >= max_consecutive_empty:
            logger.info(f"🛑 Лимит пустых ({max_consecutive_empty}), стоп")
            break
        
        # Поиск
        result = await parser.search_and_save(
            region_key=region_key,
            court_key=court_key,
            sequence_number=current_number,
            year=year
        )
        
        stats['queries_made'] += 1
        
        # Результат
        if result['success'] and result.get('saved'):
            stats['cases_saved'] += 1
            stats['consecutive_empty'] = 0
        elif result.get('error') == 'no_results':
            stats['consecutive_empty'] += 1
        
        # Прогресс
        if stats['queries_made'] % 10 == 0:
            logger.info(
                f"📊 #{current_number} | "
                f"Запросов: {stats['queries_made']} | "
                f"Сохранено: {stats['cases_saved']} | "
                f"Пустых: {stats['consecutive_empty']}"
            )
        
        current_number += 1
        await asyncio.sleep(delay_between_requests)
    
    # Итоги
    logger.info(f"\n{'-'*70}")
    logger.info(f"ИТОГИ {court_config['name']}:")
    logger.info(f"  Диапазон: {actual_start}-{current_number - 1}")
    logger.info(f"  Запросов: {stats['queries_made']}")
    logger.info(f"  Сохранено: {stats['cases_saved']}")
    logger.info(f"{'-'*70}")
    
    return stats


async def update_cases_history():
    """Режим обновления истории дел"""
    logger = setup_logger('main', level='INFO')
    
    settings = Settings()
    update_config = settings.update_settings
    
    if not update_config.get('enabled'):
        logger.warning("⚠️ Update Mode отключен")
        return
    
    logger.info("\n" + "=" * 70)
    logger.info("РЕЖИМ ОБНОВЛЕНИЯ")
    logger.info("=" * 70)
    
    stats = {'checked': 0, 'updated': 0, 'errors': 0}
    
    async with CourtParser() as parser:
        cases_to_update = await parser.db_manager.get_cases_for_update({
            'defendant_keywords': update_config['filters']['defendant_keywords'],
            'exclude_event_types': update_config['filters']['exclude_event_types'],
            'update_interval_days': update_config['update_interval_days']
        })
        
        if not cases_to_update:
            logger.info("✅ Нет дел для обновления")
            return
        
        logger.info(f"📋 Дел для обновления: {len(cases_to_update)}")
        
        text_processor = TextProcessor()
        
        for i, case_number in enumerate(cases_to_update, 1):
            try:
                case_info = text_processor.find_region_and_court_by_case_number(
                    case_number, settings.regions
                )
                
                if not case_info:
                    stats['errors'] += 1
                    continue
                
                result = await parser.search_and_save(
                    region_key=case_info['region_key'],
                    court_key=case_info['court_key'],
                    sequence_number=int(case_info['sequence']),
                    year=case_info['year']
                )
                
                stats['checked'] += 1
                
                if result['success']:
                    await parser.db_manager.mark_case_as_updated(case_number)
                    if result.get('saved'):
                        stats['updated'] += 1
                else:
                    stats['errors'] += 1
                
                await asyncio.sleep(2)
                
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"❌ {case_number}: {e}")
    
    logger.info(f"\nИТОГИ: проверено {stats['checked']}, обновлено {stats['updated']}, ошибок {stats['errors']}")


async def main():
    """Главная функция"""
    logger = setup_logger('main', level='INFO')
    
    logger.info("\n" + "=" * 70)
    logger.info("ПАРСЕР СУДЕБНЫХ ДЕЛ КАЗАХСТАНА v2.0")
    logger.info("=" * 70)
    
    try:
        if '--mode' in sys.argv:
            idx = sys.argv.index('--mode')
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1] == 'update':
                await update_cases_history()
                return 0
        
        await parse_all_regions_from_config()
        logger.info("\n✅ Завершено")
        return 0
    
    except KeyboardInterrupt:
        logger.warning("\n🛑 Прервано")
        return 1
    
    except Exception as e:
        logger.critical(f"\n💥 Ошибка: {e}")
        logger.critical(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

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
