"""Централизованная конфигурация."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set


@dataclass
class Config:
    """Централизованная конфигурация парсера."""
    def __post_init__(self):
        """Валидация конфигурации."""
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        
        # ✅ Валидация параметров
        if self.MAX_CONCURRENT_TABS < 1:
            raise ValueError("MAX_CONCURRENT_TABS должно быть >= 1")
        
        if self.BATCH_SIZE < 1:
            raise ValueError("BATCH_SIZE должно быть >= 1")
        
        if self.MAX_RETRIES < 1:
            raise ValueError("MAX_RETRIES должно быть >= 1")
    
    # API настройки
    BASE_URL: str = "https://qamqor.gov.kz"
    SEARCH_URL: str = "https://qamqor.gov.kz/check"
    API_ENDPOINT: str = "/api/public/check_status"
    
    # Производительность
    MAX_CONCURRENT_TABS: int = 3
    MAX_EMPTY_SEQUENCE: int = 5
    BATCH_SIZE: int = 50
    
    # Таймауты
    PAGE_TIMEOUT: int = 90000
    RESPONSE_TIMEOUT: int = 25
    
    # Retry логика
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 3.0
    REGION_RETRY_LIMIT: int = 3
    
    # Парсинг
    START_NUMBER: int = 1
    MAX_NUMBER: int = 99999
    NUMBER_PADDING: int = 5
    
    # Логирование
    LOG_DIR: Path = Path("logs")
    LOG_LEVEL: str = "INFO"
    SCREENSHOT_DIR: Path = Path("screenshots")
    
    # База данных
    DB_HOST: str = os.getenv("QAMQOR_DB_HOST", "localhost")
    DB_PORT: str = os.getenv("QAMQOR_DB_PORT", "5432")
    DB_NAME: str = os.getenv("QAMQOR_DB_NAME", "qamqor")
    DB_USER: str = os.getenv("QAMQOR_DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("QAMQOR_DB_PASSWORD", "admin")
    
    # Режимы работы
    MODE: str = "full"  # full | missing_only | update
    
    # Настройки апдейтера
    UPDATE_BATCH_SIZE: int = 100
    UPDATE_MIN_AGE_DAYS: int = 60  # Обновлять только записи старше 2 месяцев
    UPDATE_COOLDOWN_DAYS: int = 7   # Не обновлять чаще чем раз в 7 дней
    UPDATE_STATUSES: List[str] = field(default_factory=lambda: ["1"])
    UPDATE_TRACK_CHANGES: bool = True  # Записывать изменения в audit_log
    
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
    
    # Маппинг полей
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

    DATE_FIELDS: Set[str] = field(default_factory=lambda: {
        "reg_date", "act_date", "start_date", "end_date",
        "suspend_date", "resume_date", "prolong_start", "prolong_end"
    })

    EXCLUDED_MISSING_NUMBERS: Dict[int, List[int]] = field(default_factory=lambda: {
        5900000: [1],
    })

    TEST_NUMBER: str = "2559000001701011/00002"
    
    def __post_init__(self):
        """Создание директорий при инициализации."""
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)