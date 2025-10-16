"""Централизованная конфигурация парсера."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set


@dataclass
class Config:
    """
    Централизованная конфигурация парсера QAMQOR.
    
    Содержит все настройки для работы парсера: API endpoints,
    параметры производительности, таймауты, настройки БД,
    маппинг полей и список регионов.
    """
    
    # API настройки
    BASE_URL: str = "https://qamqor.gov.kz"
    SEARCH_URL: str = "https://qamqor.gov.kz/check"
    API_ENDPOINT: str = "/api/public/check_status"
    
    # Производительность
    MAX_CONCURRENT_TABS: int = 3
    MAX_EMPTY_SEQUENCE: int = 5
    BATCH_SIZE: int = 50
    
    # Таймауты (миллисекунды для PAGE_TIMEOUT, секунды для остальных)
    PAGE_TIMEOUT: int = 90_000
    RESPONSE_TIMEOUT: int = 25
    
    # Retry логика
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 3.0
    REGION_RETRY_LIMIT: int = 3
    
    # Задержки (секунды) для имитации человека
    NATURAL_DELAY_MIN: float = 0.3
    NATURAL_DELAY_MAX: float = 0.8
    TYPING_DELAY_MIN: float = 0.2
    TYPING_DELAY_MAX: float = 0.5
    
    # Парсинг
    START_NUMBER: int = 1
    MAX_NUMBER: int = 99_999
    NUMBER_PADDING: int = 5
    
    # Пороги для батчей
    BULK_COPY_THRESHOLD: int = 1_000
    EXECUTE_VALUES_PAGE_SIZE: int = 500
    
    # Exponential backoff
    RETRY_BACKOFF_BASE: int = 2
    
    # Логирование
    LOG_DIR: Path = field(default_factory=lambda: Path("logs"))
    LOG_LEVEL: str = "INFO"
    SCREENSHOT_DIR: Path = field(default_factory=lambda: Path("screenshots"))
    
    # База данных
    DB_HOST: str = field(
        default_factory=lambda: os.getenv("QAMQOR_DB_HOST", "localhost")
    )
    DB_PORT: str = field(
        default_factory=lambda: os.getenv("QAMQOR_DB_PORT", "5432")
    )
    DB_NAME: str = field(
        default_factory=lambda: os.getenv("QAMQOR_DB_NAME", "qamqor")
    )
    DB_USER: str = field(
        default_factory=lambda: os.getenv("QAMQOR_DB_USER", "postgres")
    )
    DB_PASSWORD: str = field(
        default_factory=lambda: os.getenv("QAMQOR_DB_PASSWORD", "admin")
    )
    
    # Режимы работы
    MODE: str = "full"  # full | missing_only | update
    
    # Настройки апдейтера
    UPDATE_BATCH_SIZE: int = 100
    UPDATE_MIN_AGE_DAYS: int = 60
    UPDATE_COOLDOWN_DAYS: int = 7
    UPDATE_STATUSES: List[str] = field(default_factory=lambda: ["1"])
    UPDATE_TRACK_CHANGES: bool = True
    
    # Регионы
    REGIONS: Dict[int, str] = field(default_factory=lambda: {
        1000000: "Abay",
        1100000: "Akmolinsk",
        1500000: "Aktobe",
        1900000: "Almaty_region",
        2300000: "Atyrau",
        2700000: "ZKO",
        3100000: "Zhambyl",
        3300000: "Zhetisu",
        3500000: "Karaganda",
        3900000: "Kostanay",
        4300000: "Kyzylorda",
        4700000: "Mangistau",
        5500000: "Pavlodar",
        5900000: "SKO",
        6100000: "Turkestan",
        6200000: "Ulytau",
        6300000: "VKO",
        7100000: "Astana",
        7500000: "Almaty",
        7900000: "Shymkent"
    })
    
    # Маппинг полей API → БД
    FIELD_MAPPING: Dict[str, str] = field(default_factory=lambda: {
        "registrationNum": "registration_number",
        "regDate": "reg_date",
        "checkDate": "act_date",
        "beginDate": "start_date",
        "endDate": "end_date",
        "tlnSuspendDate": "suspend_date",
        "tlnResumeDate": "resume_date",
        "tlnProlongBegin": "prolong_start",
        "tlnProlongEnd": "prolong_end"
    })
    
    # Поля с датами (требуют нормализации)
    DATE_FIELDS: Set[str] = field(default_factory=lambda: {
        "reg_date", "act_date", "start_date", "end_date",
        "suspend_date", "resume_date", "prolong_start", "prolong_end"
    })
    
    # Исключенные пропущенные номера (известные отсутствующие данные)
    EXCLUDED_MISSING_NUMBERS: Dict[int, List[int]] = field(
        default_factory=lambda: {
            5900000: [1],
        }
    )
    
    # Тестовый номер для health check
    TEST_NUMBER: str = "2559000001701011/00002"
    
    def __post_init__(self) -> None:
        """
        Постинициализация: создание директорий и валидация параметров.
        
        Raises:
            ValueError: При некорректных значениях параметров
        """
        # Создание директорий
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Валидация параметров
        self._validate_parameters()
    
    def _validate_parameters(self) -> None:
        """
        Валидация конфигурационных параметров.
        
        Raises:
            ValueError: При некорректных значениях
        """
        if self.MAX_CONCURRENT_TABS < 1:
            raise ValueError("MAX_CONCURRENT_TABS должно быть >= 1")
        
        if self.BATCH_SIZE < 1:
            raise ValueError("BATCH_SIZE должно быть >= 1")
        
        if self.MAX_RETRIES < 1:
            raise ValueError("MAX_RETRIES должно быть >= 1")
        
        if self.PAGE_TIMEOUT < 1000:
            raise ValueError("PAGE_TIMEOUT должен быть >= 1000ms")
        
        if self.RESPONSE_TIMEOUT < 1:
            raise ValueError("RESPONSE_TIMEOUT должен быть >= 1s")
        
        if self.NATURAL_DELAY_MIN < 0 or self.NATURAL_DELAY_MAX < 0:
            raise ValueError("Задержки должны быть >= 0")
        
        if self.NATURAL_DELAY_MIN > self.NATURAL_DELAY_MAX:
            raise ValueError("MIN delay не может быть больше MAX")