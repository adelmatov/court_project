"""
Configuration settings for company parser
"""

import os
from pathlib import Path
from typing import Dict, Any
import sys

# Установить кодировку UTF-8 для вывода
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    os.environ["PYTHONIOENCODING"] = "utf-8"

# ════════════════════════════════════════════════════════════════
# LOAD .env FROM PARSER DIRECTORY
# ════════════════════════════════════════════════════════════════

from dotenv import load_dotenv

parser_dir = Path(__file__).resolve().parent.parent
env_path = parser_dir / '.env'

load_dotenv(dotenv_path=env_path)

if env_path.exists():
    print(f"[OK] Loaded .env from: {env_path}")
else:
    print(f"[!]  .env not found at: {env_path}")
    print(f"   Please create .env file or set environment variables")

# ════════════════════════════════════════════════════════════════
# DATABASE SETTINGS - COMPANIES (основная БД для парсера)
# ════════════════════════════════════════════════════════════════

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'companies'),  # БД для компаний
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Валидация обязательных параметров
_required_db_params = ['user', 'password']
_missing_params = [k for k in _required_db_params if not DB_CONFIG.get(k)]

if _missing_params:
    raise ValueError(
        f"Missing required database parameters: {', '.join(_missing_params)}\n"
        f"Please set them in .env file: {env_path}\n"
        f"Or copy .env.example and fill it:\n"
        f"  cp {parser_dir}/.env.example {parser_dir}/.env"
    )

# ════════════════════════════════════════════════════════════════
# DATABASE SETTINGS - QAMQOR (для чтения БИН)
# ════════════════════════════════════════════════════════════════

QAMQOR_DB_CONFIG = {
    'host': os.getenv('QAMQOR_DB_HOST', os.getenv('DB_HOST', 'localhost')),
    'port': int(os.getenv('QAMQOR_DB_PORT', os.getenv('DB_PORT', '5432'))),
    'database': 'qamqor',  # Всегда qamqor
    'user': os.getenv('QAMQOR_DB_USER', os.getenv('DB_USER')),
    'password': os.getenv('QAMQOR_DB_PASSWORD', os.getenv('DB_PASSWORD'))
}

# ════════════════════════════════════════════════════════════════
# DATABASE SETTINGS - TARGET (для проверки company таблицы)
# ════════════════════════════════════════════════════════════════

TARGET_DB_CONFIG = {
    'host': os.getenv('TARGET_DB_HOST', os.getenv('DB_HOST', 'localhost')),
    'port': int(os.getenv('TARGET_DB_PORT', os.getenv('DB_PORT', '5432'))),
    'database': os.getenv('TARGET_DB_NAME', os.getenv('DB_NAME', 'companies')),
    'user': os.getenv('TARGET_DB_USER', os.getenv('DB_USER')),
    'password': os.getenv('TARGET_DB_PASSWORD', os.getenv('DB_PASSWORD'))
}

# ════════════════════════════════════════════════════════════════
# API SETTINGS
# ════════════════════════════════════════════════════════════════

API_BASE_URL = os.getenv('COMPANY_API_URL', 'https://apiba.prgapp.kz')
API_TIMEOUT = int(os.getenv('API_TIMEOUT', '30'))

API_ENDPOINTS = {
    'search': '/GetCompanyListAsync',
    'info': '/CompanyFullInfo'
}

API_HEADERS = {
    'accept': 'application/json',
    'content-type': 'application/json',
    'origin': 'https://ba.prg.kz',
    'referer': 'https://ba.prg.kz/',
    'user-agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
    )
}

# ════════════════════════════════════════════════════════════════
# RATE LIMITING
# ════════════════════════════════════════════════════════════════

RATE_LIMIT: Dict[str, Any] = {
    'requests_per_minute': int(os.getenv('RATE_LIMIT_RPM', '10')),
    'min_delay_seconds': float(os.getenv('MIN_DELAY', '6.0')),
    'max_delay_seconds': float(os.getenv('MAX_DELAY', '10.0')),
    'retry_429_delay': int(os.getenv('RETRY_429_DELAY', '300'))
}

# ════════════════════════════════════════════════════════════════
# RETRY POLICY
# ════════════════════════════════════════════════════════════════

RETRY_POLICY: Dict[str, Any] = {
    'max_attempts': int(os.getenv('MAX_RETRIES', '3')),
    'backoff_delays': [60, 180, 600],
    'retry_on_status': [429, 500, 502, 503, 504]
}

# ════════════════════════════════════════════════════════════════
# PARSING
# ════════════════════════════════════════════════════════════════

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
UPDATE_DAYS_THRESHOLD = int(os.getenv('UPDATE_THRESHOLD_DAYS', '90'))

# ════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════

LOG_CONFIG = {
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
    'file': os.getenv('LOG_FILE', 'logs/company_parser.log'),
    'level': os.getenv('LOG_LEVEL', 'INFO')
}