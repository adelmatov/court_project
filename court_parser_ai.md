# 🤖 COURT PARSER - AI-Optimized Code Snapshot

**⏰ Generated:** 2026-06-22 23:54:25

## 📊 Quick Stats

| Metric | Value |
|--------|-------|
| **Files** | 26 |
| **Resources** | 1 |
| **Total Lines** | 7,284 |
| **Original Size** | 255.1 KB |
| **Optimized Size** | 245.5 KB |
| **Compression** | -95.3% ↓ |

---

## 🎯 Purpose

This snapshot contains the complete source code of the Court Parser project,
optimized for AI analysis and understanding. Every file is included with:
- ✅ Dependencies highlighted
- ✅ Code optimized for token efficiency
- ✅ Complete module structure
- ✅ Configuration files included
- ✅ Easy navigation

---

## 📑 Project Structure
parsers/court_parser/
├── utils/       - Logging, retry mechanisms, validators
├── config/      - Configuration, environment settings
├── auth/        - Authentication & login handling
├── database/    - PostgreSQL operations & queries
├── parsing/     - HTML parsing & data extraction
├── search/      - Search queries & filters
├── core/        - Main parser orchestrator
├── config.json  - Configuration file
└── main.py      - Application entry point

⊘ EXCLUDED:
├── ui_app/      - (UI application - not included)
├── tests/       - (Test files - not included)
└── logs/        - (Log files - not included)
---

## 🚀 Quick Navigation

### Files by Module

#### `ROOT`
- **1.** `authenticator.py` (393 lines)
- **2.** `settings.py` (179 lines)
- **3.** `parser.py` (344 lines)
- **4.** `region_worker.py` (333 lines)
- **5.** `session.py` (122 lines)
- **6.** `base_updater.py` (214 lines)
- **7.** `docs_updater.py` (141 lines)
- **8.** `events_updater.py` (74 lines)
- **9.** `gaps_updater.py` (359 lines)
- **10.** `judge_updater.py` (71 lines)
- **11.** `db_manager.py` (1264 lines)
- **12.** `models.py` (67 lines)
- **13.** `main.py` (1002 lines)
- **14.** `data_extractor.py` (93 lines)
- **15.** `document_parser.py` (122 lines)
- **16.** `html_parser.py` (142 lines)
- **17.** `document_handler.py` (290 lines)
- **18.** `form_handler.py` (214 lines)
- **19.** `search_engine.py` (148 lines)
- **20.** `constants.py` (23 lines)
- **21.** `http_utils.py` (81 lines)
- **22.** `logger.py` (257 lines)
- **23.** `retry.py` (280 lines)
- **24.** `terminal_ui.py` (699 lines)
- **25.** `text_processor.py` (287 lines)
- **26.** `validators.py` (85 lines)
## 📚 Module Overview

### ROOT
**Files:** 26 | **Lines:** 7284

```
parsers\court_parser\auth\authenticator.py
parsers\court_parser\config\settings.py
parsers\court_parser\core\parser.py
parsers\court_parser\core\region_worker.py
parsers\court_parser\core\session.py
parsers\court_parser\core\updaters\base_updater.py
parsers\court_parser\core\updaters\docs_updater.py
parsers\court_parser\core\updaters\events_updater.py
parsers\court_parser\core\updaters\gaps_updater.py
parsers\court_parser\core\updaters\judge_updater.py
parsers\court_parser\database\db_manager.py
parsers\court_parser\database\models.py
parsers\court_parser\main.py
parsers\court_parser\parsing\data_extractor.py
parsers\court_parser\parsing\document_parser.py
parsers\court_parser\parsing\html_parser.py
parsers\court_parser\search\document_handler.py
parsers\court_parser\search\form_handler.py
parsers\court_parser\search\search_engine.py
parsers\court_parser\utils\constants.py
parsers\court_parser\utils\http_utils.py
parsers\court_parser\utils\logger.py
parsers\court_parser\utils\retry.py
parsers\court_parser\utils\terminal_ui.py
parsers\court_parser\utils\text_processor.py
parsers\court_parser\utils\validators.py
```

---

## 📋 Configuration Files

### Resource 1: `parsers\court_parser\config.json`

```json
{
  "auth": {
    "login": "REMOVED",
    "password": "REMOVED",
    "user_name": "REMOVED"
  },
  "base_url": "https://office.sud.kz",
  "database": {
    "dbname": "court",
    "host": "localhost",
    "password": "REMOVED",
    "port": 5432,
    "user": "postgres"
  },
  "parsing_settings": {
    "court_types": [
      "supreme",
      "cassation",
      "smas",
      "appellate"
    ],
    "delay_between_requests": 0,
    "gaps_check_interval_days": 30,
    "gaps_tail_probe": 5,
    "limit_cases_per_region": null,
    "limit_regions": null,
    "max_consecutive_empty": 5,
    "max_consecutive_failures": 5,
    "max_gaps_per_session": 200,
    "max_number": 9999,
    "max_parallel_regions": 3,
    "region_retry_delay_seconds": 5,
    "region_retry_max_attempts": 3,
    "start_from": 1,
    "target_regions": null,
    "year": "auto",
    "year_transition_months": 6,
    "years": [
      2024
    ]
  },
  "regions": {
    "abay": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "456",
          "instance_code": "00",
          "name": "Суд области Абай"
        },
        "smas": {
          "case_type_code": "4",
          "id": "467",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд области Абай"
        }
      },
      "id": "21",
      "kato_code": "10",
      "name": "Область Абай"
    },
    "akmola": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "29",
          "instance_code": "99",
          "name": "Акмолинский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "416",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Акмолинской области"
        }
      },
      "id": "4",
      "kato_code": "11",
      "name": "Акмолинская область"
    },
    "aktobe": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "55",
          "instance_code": "99",
          "name": "Актюбинский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "417",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Актюбинской области"
        }
      },
      "id": "5",
      "kato_code": "15",
      "name": "Актюбинская область"
    },
    "almaty": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "13",
          "instance_code": "99",
          "name": "Алматинский городской суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "414",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд города Алматы"
        }
      },
      "id": "3",
      "kato_code": "75",
      "name": "город Алматы"
    },
    "almaty_region": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "75",
          "instance_code": "99",
          "name": "Алматинский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "430",
          "instance_code": "93",
          "name": "Специализированный межрайонный административный суд Алматинской области"
        }
      },
      "id": "6",
      "kato_code": "19",
      "name": "Алматинская область"
    },
    "astana": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "2",
          "instance_code": "99",
          "name": "Суд города Астаны"
        },
        "smas": {
          "case_type_code": "4",
          "id": "413",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд города Астаны"
        }
      },
      "id": "2",
      "kato_code": "71",
      "name": "город Астана"
    },
    "atyrau": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "105",
          "instance_code": "99",
          "name": "Атырауский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "419",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Атырауской области"
        }
      },
      "id": "7",
      "kato_code": "23",
      "name": "Атырауская область"
    },
    "karaganda": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "199",
          "instance_code": "99",
          "name": "Карагандинский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "423",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Карагандинской области"
        }
      },
      "id": "11",
      "kato_code": "35",
      "name": "Карагандинская область"
    },
    "kostanay": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "237",
          "instance_code": "99",
          "name": "Костанайский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "424",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Костанайской области"
        }
      },
      "id": "12",
      "kato_code": "39",
      "name": "Костанайская область"
    },
    "kyzylorda": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "266",
          "instance_code": "99",
          "name": "Кызылординский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "425",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Кызылординской области"
        }
      },
      "id": "13",
      "kato_code": "43",
      "name": "Кызылординская область"
    },
    "mangystau": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "281",
          "instance_code": "99",
          "name": "Мангистауский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "426",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Мангистауской области"
        }
      },
      "id": "14",
      "kato_code": "47",
      "name": "Мангистауская область"
    },
    "pavlodar": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "295",
          "instance_code": "99",
          "name": "Павлодарский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "427",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Павлодарской области"
        }
      },
      "id": "15",
      "kato_code": "55",
      "name": "Павлодарская область"
    },
    "republic": {
      "courts": {
        "cassation": {
          "case_type_code": "4к",
          "id": "495",
          "instance_code": "03",
          "name": "Кассационный суд Республики Казахстан"
        },
        "supreme": {
          "case_type_code": "6ап",
          "id": "1",
          "instance_code": "01",
          "name": "Верховный Суд Республики Казахстан"
        }
      },
      "id": "2",
      "kato_code": "60",
      "name": "Республиканские суды",
      "search_region_id": "2"
    },
    "shymkent": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "383",
          "instance_code": "99",
          "name": "Суд города Шымкента"
        },
        "smas": {
          "case_type_code": "4",
          "id": "415",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд города Шымкента"
        }
      },
      "id": "19",
      "kato_code": "52",
      "name": "город Шымкент"
    },
    "sko": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "316",
          "instance_code": "99",
          "name": "Северо-Казахстанский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "428",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Северо-Казахстанской области"
        }
      },
      "id": "16",
      "kato_code": "59",
      "name": "Северо-Казахстанская область"
    },
    "turkestan": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "340",
          "instance_code": "99",
          "name": "Туркестанский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "429",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Туркестанской области"
        }
      },
      "id": "17",
      "kato_code": "51",
      "name": "Туркестанская область"
    },
    "ulytau": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "476",
          "instance_code": "00",
          "name": "Суд области Ұлытау"
        },
        "smas": {
          "case_type_code": "4",
          "id": "482",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд области Ұлытау"
        }
      },
      "id": "20",
      "kato_code": "62",
      "name": "Область Ұлытау"
    },
    "vko": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "119",
          "instance_code": "99",
          "name": "Восточно-Казахстанский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "420",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Восточно-Казахстанской области"
        }
      },
      "id": "8",
      "kato_code": "63",
      "name": "Восточно-Казахстанская область"
    },
    "zhambyl": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "158",
          "instance_code": "99",
          "name": "Жамбылский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "421",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Жамбылской области"
        }
      },
      "id": "9",
      "kato_code": "31",
      "name": "Жамбылская область"
    },
    "zhetysu": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "437",
          "instance_code": "00",
          "name": "Суд области Жетісу"
        },
        "smas": {
          "case_type_code": "4",
          "id": "450",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд области Жетісу"
        }
      },
      "id": "22",
      "kato_code": "33",
      "name": "Область Жетісу"
    },
    "zko": {
      "courts": {
        "appellate": {
          "case_type_code": "4а",
          "id": "175",
          "instance_code": "99",
          "name": "Западно-Казахстанский областной суд"
        },
        "smas": {
          "case_type_code": "4",
          "id": "422",
          "instance_code": "94",
          "name": "Специализированный межрайонный административный суд Западно-Казахстанской области"
        }
      },
      "id": "10",
      "kato_code": "27",
      "name": "Западно-Казахстанская область"
    }
  },
  "retry_settings": {
    "authentication": {
      "backoff_multiplier": 2.0,
      "create_new_session": true,
      "initial_delay": 2.0,
      "max_attempts": 5,
      "max_delay": 60.0,
      "retriable_on_auth_check_fail": true
    },
    "circuit_breaker": {
      "enabled": true,
      "failure_threshold": 20,
      "half_open_max_attempts": 3,
      "recovery_timeout": 300
    },
    "http_request": {
      "backoff_multiplier": 2.0,
      "initial_delay": 1.0,
      "jitter": true,
      "max_attempts": 3,
      "max_delay": 30.0,
      "retriable_exceptions": [
        "TimeoutError",
        "ClientError",
        "ServerDisconnectedError"
      ],
      "retriable_status_codes": [
        500,
        502,
        503,
        504
      ]
    },
    "rate_limit": {
      "default_wait": 60,
      "respect_retry_after_header": true,
      "slow_down_duration": 600,
      "slow_down_multiplier": 2.0
    },
    "search_case": {
      "backoff": "linear",
      "delay": 3.0,
      "max_attempts": 3,
      "save_failed_html": true
    },
    "session_recovery": {
      "max_reauth_attempts": 2,
      "reauth_on_401": true
    }
  },
  "update_settings": {
    "case_events": {
      "check_interval_days": 2,
      "enabled": true,
      "filters": {
        "exclude_event_types": [],
        "party_keywords": [
          "доход"
        ],
        "party_role": "defendant"
      },
      "final_check_period_days": 30,
      "final_event_types": [
        "Возврат",
        "Завершение дела",
        "Решение вступило в силу",
        "Отправлено в архив",
        "Передано по неподсудности",
        "Оставлено без движения",
        "Прекращено"
      ],
      "max_stale_days": 730
    },
    "common": {
      "delay_between_requests": 0,
      "max_parallel_workers": 3
    },
    "docs": {
      "check_interval_days": 5,
      "final_post_check_delay_days": 10,
      "documents_max_attempts": 5,
      "download_delay": 0,
      "enabled": true,
      "filters": {
        "court_types": null,
        "order": "oldest",
        "party_keywords": [
          "доход"
        ],
        "party_role": "defendant",
        "regions": null,
        "year": null
      },
      "final_event_types": [
        "Возврат",
        "Завершение дела",
        "Решение вступило в силу",
        "Отправлено в архив",
        "Передано по неподсудности",
        "Оставлено без движения",
        "Прекращено"
      ],
      "max_per_court": null,
      "max_per_session": null,
      "storage_dir": "./docs"
    },
    "judge": {
      "check_interval_days": 0,
      "enabled": true,
      "final_event_types": [
        "Возврат",
        "Завершение дела",
        "Решение вступило в силу",
        "Отправлено в архив",
        "Передано по неподсудности",
        "Оставлено без движения",
        "Прекращено"
      ],
      "max_stale_days": 730,
      "target_courts": [
        "smas"
      ]
    }
  }
}
```

---

## 💻 Source Code

### File 1/26: `parsers\court_parser\auth\authenticator.py`
**Module:** `root` | **Lines:** 393

```python
# parsers/court_parser/auth/authenticator.py
"""
Авторизация на сайте office.sud.kz
"""

from typing import Dict, Optional
import asyncio
import aiohttp
from selectolax.parser import HTMLParser

from utils.logger import get_logger
from utils.retry import RetryStrategy, RetryConfig, NonRetriableError, RetryableError
from utils.http_utils import HttpHeaders, ViewStateExtractor
import traceback

class AuthenticationError(Exception):
    """Ошибка авторизации"""
    pass

class LanguageError(Exception):
    """Ошибка установки языка"""
    pass

class Authenticator:
    """Класс авторизации с retry и установкой языка"""

    # Константы для формы языка
    LANGUAGE_FORM_ID = 'f_l_temp'
    RUSSIAN_LANGUAGE_TRIGGER = 'f_l_temp:js_temp_1'

    # Признаки русского интерфейса
    RUSSIAN_INDICATORS = ['Войти', 'Вход', 'Пароль', 'Электронная почта']
    KAZAKH_INDICATORS = ['Кіру', 'Құпия сөз']

    def __init__(self, base_url: str, auth_config: Dict[str, str], 
                 retry_config: Optional[Dict] = None):
        self.base_url = base_url
        self.login = auth_config['login']
        self.password = auth_config['password']
        self.user_name = auth_config['user_name']
        self.logger = get_logger('authenticator')
        self.retry_config = retry_config or {}

    async def authenticate(self, session_manager) -> bool:
        """Полный процесс авторизации с retry"""
        auth_retry_config = self.retry_config.get('authentication', {})

        if not auth_retry_config:
            return await self._do_authenticate(session_manager)

        retry_cfg = RetryConfig(auth_retry_config)
        strategy = RetryStrategy(retry_cfg)
        create_new_session = auth_retry_config.get('create_new_session', True)

        async def _auth_with_session_reset():
            try:
                return await self._do_authenticate(session_manager)
            except RetryableError:
                if create_new_session:
                    self.logger.debug("Создаю новую сессию перед retry...")
                    await session_manager.create_session()
                raise

        try:
            return await strategy.execute_with_retry(
                _auth_with_session_reset,
                error_context="Авторизация"
            )
        except Exception as e:
            self.logger.error(f"❌ Авторизация не удалась: {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            raise AuthenticationError(f"Не удалось авторизоваться: {e}") from e

    async def _do_authenticate(self, session_manager) -> bool:
        """Один цикл авторизации"""
        session = await session_manager.get_session()

        self.logger.info("Начинаю авторизацию...")

        # Этап 1: Загрузка страницы
        html, viewstate = await self._load_page(session)
        await asyncio.sleep(0.5)

        # Этап 2: Установка русского языка
        html, viewstate = await self._ensure_russian_language(session, html, viewstate)
        await asyncio.sleep(0.5)

        # Этап 3: Извлечение данных формы авторизации
        form_ids = self._extract_auth_form_ids(html)

        if not form_ids.get('form_base') or not form_ids.get('submit_button'):
            with open("auth_form_not_found.html", "w", encoding="utf-8") as f:
                f.write(html)
            raise RetryableError(
                f"Форма авторизации не найдена. Извлечено: {form_ids}"
            )

        self.logger.info(f"📋 Форма: {form_ids['form_base']}, кнопка: {form_ids['submit_button']}")

        # Этап 4: Отправка логина
        await self._perform_login(session, viewstate, form_ids)
        await asyncio.sleep(0.5)

        # Этап 5: Проверка авторизации
        if await self._verify_authentication(session):
            self.logger.info("✅ Авторизация успешна")
            return True

        raise RetryableError("Проверка авторизации не пройдена")

    async def _load_page(self, session: aiohttp.ClientSession) -> tuple:
        """
        Загрузка страницы логина

        Returns:
            (html, viewstate)
        """
        url = f"{self.base_url}/index.xhtml"
        headers = self._get_base_headers()

        try:
            async with session.get(url, headers=headers) as response:
                if response.status in [500, 502, 503, 504]:
                    raise RetryableError(f"HTTP {response.status}")
                if response.status != 200:
                    raise RetryableError(f"HTTP {response.status}")

                html = await response.text()

                viewstate = ViewStateExtractor.extract(html)
                if not viewstate:
                    raise RetryableError("ViewState не найден")

                self.logger.debug("Страница загружена, ViewState извлечён")
                return html, viewstate

        except aiohttp.ClientError as e:
            raise RetryableError(f"Сетевая ошибка: {e}")

    async def _ensure_russian_language(
        self, 
        session: aiohttp.ClientSession, 
        html: str, 
        viewstate: str
    ) -> tuple:
        """
        Проверка и установка русского языка

        Returns:
            (html, viewstate) — обновлённые после смены языка
        """
        # Проверяем текущий язык
        if self._is_russian_interface(html):
            self.logger.info("🌐 Интерфейс уже на русском языке")
            return html, viewstate

        self.logger.info("🌐 Интерфейс на казахском, переключаю на русский...")

        # Отправляем POST для смены языка
        await self._send_language_change_request(session, viewstate)
        await asyncio.sleep(0.5)

        # Загружаем страницу заново для получения нового ViewState и проверки
        html, new_viewstate = await self._load_page(session)

        # Проверяем что язык сменился
        if not self._is_russian_interface(html):
            # Сохраняем для отладки
            with open("language_not_changed.html", "w", encoding="utf-8") as f:
                f.write(html)
            raise RetryableError("Не удалось переключить язык на русский")

        self.logger.info("✅ Язык успешно переключён на русский")
        return html, new_viewstate

    def _is_russian_interface(self, html: str) -> bool:
        """
        Проверка что интерфейс на русском языке

        Проверяет наличие кнопки "Войти" и отсутствие казахских элементов
        """
        # Проверяем наличие русских индикаторов
        russian_found = any(indicator in html for indicator in self.RUSSIAN_INDICATORS)

        # Проверяем отсутствие казахских индикаторов (в кнопках)
        # Ищем кнопку входа
        parser = HTMLParser(html)
        submit_buttons = parser.css('input[type="submit"]')

        for btn in submit_buttons:
            if not btn.attributes:
                continue
            value = btn.attributes.get('value', '')

            # Если кнопка "Войти" — русский
            if value == 'Войти':
                self.logger.debug("Найдена кнопка 'Войти' — интерфейс русский")
                return True

            # Если кнопка "Кіру" — казахский
            if value == 'Кіру':
                self.logger.debug("Найдена кнопка 'Кіру' — интерфейс казахский")
                return False

        # Fallback: проверка по общим индикаторам
        if russian_found:
            kazakh_found = any(indicator in html for indicator in self.KAZAKH_INDICATORS)
            return russian_found and not kazakh_found

        return False

    async def _send_language_change_request(
        self, 
        session: aiohttp.ClientSession, 
        viewstate: str
    ):
        """
        Отправка POST запроса для смены языка на русский

        Ответ не обрабатывается — изменения применяются при следующем GET
        """
        url = f"{self.base_url}/index.xhtml"

        data = {
            self.LANGUAGE_FORM_ID: self.LANGUAGE_FORM_ID,
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': self.RUSSIAN_LANGUAGE_TRIGGER,
            'javax.faces.partial.execute': f'{self.RUSSIAN_LANGUAGE_TRIGGER} @component',
            'javax.faces.partial.render': '@component',
            'param1': f'{self.base_url}/#',
            'org.richfaces.ajax.component': self.RUSSIAN_LANGUAGE_TRIGGER,
            self.RUSSIAN_LANGUAGE_TRIGGER: self.RUSSIAN_LANGUAGE_TRIGGER,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }

        headers = self._get_ajax_headers()
        headers['Referer'] = f'{self.base_url}/'
        headers['Origin'] = self.base_url

        try:
            async with session.post(url, data=data, headers=headers) as response:
                if response.status in [500, 502, 503, 504]:
                    raise RetryableError(f"HTTP {response.status} при смене языка")

                # Ответ не важен, просто читаем чтобы завершить запрос
                await response.text()

                self.logger.debug("POST запрос смены языка отправлен")

        except aiohttp.ClientError as e:
            raise RetryableError(f"Сетевая ошибка при смене языка: {e}")

    async def _perform_login(self, session: aiohttp.ClientSession, 
                            viewstate: str, form_ids: Dict[str, str]):
        """Отправка логина и пароля"""
        url = f"{self.base_url}/index.xhtml"

        form_base = form_ids['form_base']
        submit_button = form_ids['submit_button']

        data = {
            form_base: form_base,
            f'{form_base}:xin': self.login,
            f'{form_base}:password': self.password,
            'javax.faces.ViewState': viewstate,
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
        headers['Referer'] = url

        try:
            async with session.post(url, data=data, headers=headers) as response:
                if response.status in [400, 500, 502, 503, 504]:
                    text = await response.text()
                    self.logger.warning(f"HTTP {response.status} при логине: {text[:200]}")
                    raise RetryableError(f"HTTP {response.status} при логине")

                if response.status != 200:
                    raise RetryableError(f"HTTP {response.status}")

                self.logger.debug("Логин отправлен")

        except aiohttp.ClientError as e:
            raise RetryableError(f"Сетевая ошибка: {e}")

    async def _verify_authentication(self, session: aiohttp.ClientSession) -> bool:
        """Проверка успешности авторизации"""
        url = f"{self.base_url}/form/proceedings/services.xhtml"

        try:
            async with session.get(url, headers=self._get_base_headers(), 
                                   allow_redirects=False) as response:

                if response.status in [301, 302, 303, 307, 308]:
                    raise RetryableError("Редирект - не авторизован")

                if response.status in [401, 403]:
                    raise NonRetriableError(f"HTTP {response.status}: Доступ запрещён")

                if response.status in [500, 502, 503, 504]:
                    raise RetryableError(f"HTTP {response.status}")

                if response.status != 200:
                    raise RetryableError(f"HTTP {response.status}")

                html = await response.text()

                # Признаки успешной авторизации
                checks = [
                    'profile-context-menu' in html,
                    'Выйти' in html,
                    'logout()' in html,
                    'userInfo.xhtml' in html,
                ]

                passed = sum(checks)

                if passed >= 2:
                    return True

                # Если форма логина - точно не авторизован
                if 'password' in html.lower() and 'xin' in html.lower():
                    raise RetryableError("Обнаружена форма логина")

                # Хотя бы 1 признак - принимаем
                if passed >= 1:
                    self.logger.warning(f"⚠️ Только {passed}/4 признаков, но принимаем")
                    return True

                raise RetryableError(f"Нет признаков авторизации ({passed}/4)")

        except (RetryableError, NonRetriableError):
            raise
        except aiohttp.ClientError as e:
            raise RetryableError(f"Сетевая ошибка: {e}")

    def _extract_auth_form_ids(self, html: str) -> Dict[str, str]:
        """Динамическое извлечение ID элементов формы авторизации"""
        parser = HTMLParser(html)
        ids = {}

        # 1. Ищем поле email (ИИН)
        email_input = parser.css_first('input[type="email"]')
        if email_input and email_input.attributes:
            xin_name = email_input.attributes.get('name', '')
            if ':' in xin_name:
                # "j_idt72:auth:xin" → "j_idt72:auth"
                ids['form_base'] = ':'.join(xin_name.split(':')[:-1])
                ids['xin_field'] = xin_name

        # 2. Ищем кнопку "Войти" в форме авторизации
        if ids.get('form_base'):
            for btn in parser.css('input[type="submit"]'):
                if not btn.attributes:
                    continue

                btn_name = btn.attributes.get('name', '')
                btn_value = btn.attributes.get('value', '')
                btn_style = btn.attributes.get('style', '')

                # Пропускаем скрытые
                if 'display: none' in btn_style or 'display:none' in btn_style:
                    continue

                # Кнопка должна принадлежать форме авторизации
                if ids['form_base'] in btn_name:
                    # Предпочитаем кнопку "Войти"
                    if btn_value == 'Войти':
                        ids['submit_button'] = btn_name
                        break
                    # Или любую видимую кнопку формы
                    elif 'submit_button' not in ids:
                        ids['submit_button'] = btn_name

        self.logger.debug(f"Извлечённые ID: {ids}")
        return ids

    def _get_base_headers(self) -> Dict[str, str]:
        return HttpHeaders.get_base()

    def _get_ajax_headers(self) -> Dict[str, str]:
        return HttpHeaders.get_ajax()
```

### File 2/26: `parsers\court_parser\config\settings.py`
**Module:** `root` | **Lines:** 179

```python
"""
Загрузка и валидация конфигурации
"""
from datetime import datetime
import json
from pathlib import Path
from typing import Dict, Any, Optional, List

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

    def get_parsing_year(self) -> str:
        """
        Получить год для парсинга

        Returns:
            Текущий год если 'auto', иначе значение из конфига
        """
from datetime import datetime

        year = self.parsing_settings.get('year', 'auto')

        if year == 'auto' or year is None:
            return str(datetime.now().year)

        return str(year)

    def get_parsing_years(self) -> List[str]:
        """
        Список годов для парсинга и проверки пропусков.

        Логика:
        - текущий год: всегда
        - прошлый год: в переходный период (первые N месяцев года)
                       для добора декабрьских хвостов
        - можно переопределить явным списком в конфиге через 'years'

        Returns:
            ['2027'] или ['2027', '2026'] в переходный период
        """

        ps = self.parsing_settings

        # Явное переопределение списком (для ручного управления)
        explicit = ps.get('years')
        if explicit:
            return [str(y) for y in explicit]

        now = datetime.now()
        current = now.year
        years = [str(current)]

        # Переходное окно — добираем прошлый год в начале года
        transition_months = ps.get('year_transition_months', 3)
        if now.month <= transition_months:
            years.append(str(current - 1))

        return years

    @property
    def update_settings(self) -> Dict[str, Any]:
        """Настройки обновления"""
        return self.config.get('update_settings', {})

    def validate_docs_filters(self) -> bool:
        """Валидация фильтров документов"""
        docs_config = self.update_settings.get('docs', {})
        filters = docs_config.get('filters', {})

        # Проверка mode
        mode = filters.get('mode', 'any')
        if mode not in ('any', 'all'):
            raise ConfigurationError(f"docs.filters.mode должен быть 'any' или 'all', получено: {mode}")

        # Проверка party_role
        party_role = filters.get('party_role', 'any')
        if party_role not in ('any', 'plaintiff', 'defendant'):
            raise ConfigurationError(f"docs.filters.party_role некорректен: {party_role}")

        # Проверка что хотя бы один фильтр активен
        has_filter = (
            filters.get('missing_parties') or
            filters.get('missing_judge') or
            filters.get('party_keywords')
        )

        if not has_filter:
            raise ConfigurationError("docs.filters: должен быть активен хотя бы один фильтр")

        return True
```

### File 3/26: `parsers\court_parser\core\parser.py`
**Module:** `root` | **Lines:** 344

```python
"""
Главный класс парсера с retry и восстановлением
"""
from typing import Dict, Any, Optional, List, Tuple
import asyncio
import aiohttp

from config.settings import Settings
from core.session import SessionManager
from auth.authenticator import Authenticator
from search.form_handler import FormHandler
from search.search_engine import SearchEngine
from parsing.html_parser import ResultsParser
from database.db_manager import DatabaseManager
from database.models import CaseData, SearchResult
from utils.text_processor import TextProcessor
from utils.logger import get_logger
from utils.retry import RetryStrategy, RetryConfig, NonRetriableError
from utils.constants import CaseStatus
import traceback

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

    async def search_case_by_number(self, case_number: str) -> Tuple[Optional[str], List[CaseData]]:
        """
        Поиск дела по номеру

        Args:
            case_number: полный номер дела (например '7599-25-00-4а/215')

        Returns:
            (results_html, parsed_cases) — HTML для документов и список дел
            (None, []) — если не удалось определить регион
        """
        # Определяем регион и суд по номеру дела
        case_info = self.text_processor.find_region_and_court_by_case_number(
            case_number, self.settings.regions
        )

        if not case_info:
            self.logger.warning(f"⚠️ Не удалось определить регион: {case_number}")
            return None, []

        region_config = self.settings.get_region(case_info['region_key'])
        court_config = self.settings.get_court(case_info['region_key'], case_info['court_key'])

        # Поиск через форму
        async with self.form_lock:
            session = await self.session_manager.get_session()

            viewstate, form_ids = await self.form_handler.prepare_search_form(session)

            # Передаём весь region_config для поддержки search_region_id
            await self.form_handler.select_region(
                session, viewstate, region_config, form_ids
            )

            await asyncio.sleep(1)

            results_html = await self.search_engine.search_case(
                session, viewstate,
                region_config.get('search_region_id', region_config['id']),
                court_config['id'],
                case_info['year'],
                int(case_info['sequence']),
                form_ids
            )

        # Парсинг
        cases = self.results_parser.parse(results_html)

        return results_html, cases

    async def initialize(self):
        """Инициализация"""
        try:
            await self.db_manager.connect()
            await self.authenticator.authenticate(self.session_manager)
            self.logger.info("✅ Парсер готов к работе")
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
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

        Сохраняет все дела, соответствующие целевому номеру,
        включая варианты с суффиксами (1), (2) и т.д.
        """
        region_config = self.settings.get_region(region_key)
        court_config = self.settings.get_court(region_key, court_key)

        target_case_number = self.text_processor.generate_case_number(
            region_config, court_config, year, sequence_number
        )

        self.logger.info(f"🔍 Ищу дело: {target_case_number}")

        # Используем общий метод поиска
        results_html, cases = await self.search_case_by_number(target_case_number)

        if results_html is None:
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.REGION_NOT_FOUND
            }

        if not cases:
            self.logger.info(f"❌ Ничего не найдено: {target_case_number}")
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.NO_RESULTS
            }

        # Находим ВСЕ дела, соответствующие целевому номеру (включая суффиксы)
        matching_cases = [
            case for case in cases 
            if self.text_processor.is_matching_case_number(case.case_number, target_case_number)
        ]

        if not matching_cases:
            self.logger.warning(f"⚠️ Целевое дело не найдено: {target_case_number}")
            self.logger.debug(f"Получено {len(cases)} дел: {[c.case_number for c in cases]}")
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.TARGET_NOT_FOUND
            }

        # Сохраняем все найденные дела
        saved_count = 0
        saved_numbers = []

        for case in matching_cases:
            save_result = await self.db_manager.save_case(case)

            if save_result['status'] in [CaseStatus.SAVED, CaseStatus.UPDATED]:
                saved_count += 1
                saved_numbers.append(case.case_number)

                judge_info = "✅ судья" if case.judge else "⚠️ без судьи"
                parties = len(case.plaintiffs) + len(case.defendants)
                events = len(case.events)

                self.logger.info(
                    f"✅ Сохранено: {case.case_number} "
                    f"({judge_info}, {parties} сторон, {events} событий)"
                )

        if saved_count > 0:
            return {
                'success': True,
                'saved': True,
                'saved_count': saved_count,
                'case_numbers': saved_numbers,
                'case_number': target_case_number,
                'results_html': results_html
            }

        return {
            'success': False,
            'saved': False,
            'saved_count': 0,
            'case_numbers': [],
            'case_number': target_case_number,
            'error': CaseStatus.SAVE_FAILED
        }

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
```

### File 4/26: `parsers\court_parser\core\region_worker.py`
**Module:** `root` | **Lines:** 333

```python
"""
Изолированный воркер для обработки одного региона
"""
import ssl
import asyncio
from typing import Dict, Any, Optional, Union

import aiohttp

from config.settings import Settings
from auth.authenticator import Authenticator
from search.form_handler import FormHandler
from search.search_engine import SearchEngine
from parsing.html_parser import ResultsParser
from database.models import CaseData
from utils.text_processor import TextProcessor
from utils.logger import setup_worker_logger
from utils.constants import CaseStatus
import traceback

class RegionWorker:
    """
    Изолированный воркер для одного региона

    Каждый воркер имеет:
    - Свою HTTP-сессию
    - Свой FormHandler с отдельным кешем
    - Свой SearchEngine

    Это обеспечивает полную изоляцию состояния формы на сервере.
    """

    def __init__(self, settings: Settings, region_key: str):
        self.settings = settings
        self.region_key = region_key
        self.logger = setup_worker_logger(region_key)

        self.session: Optional[aiohttp.ClientSession] = None
        self.form_handler: Optional[FormHandler] = None
        self.search_engine: Optional[SearchEngine] = None
        self.results_parser = ResultsParser()
        self.text_processor = TextProcessor()

        self.authenticated = False
        self.retry_config = settings.config.get('retry_settings', {})

    async def initialize(self) -> bool:
        """
        Инициализация воркера: создание сессии и авторизация

        Returns:
            True если успешно, False при ошибке
        """
        try:
            await self._create_session()

            self.form_handler = FormHandler(self.settings.base_url)
            self.search_engine = SearchEngine(self.settings.base_url)

            authenticator = Authenticator(
                self.settings.base_url,
                self.settings.auth,
                retry_config=self.retry_config
            )

            self.authenticated = await authenticator.authenticate(self)

            if self.authenticated:
                self.logger.info(f"Воркер {self.region_key} авторизован")

            return self.authenticated

        except Exception as e:
            self.logger.error(f"Ошибка инициализации воркера {self.region_key}: {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            return False

    async def _create_session(self):
        """Создание изолированной HTTP-сессии"""
        if self.session and not self.session.closed:
            await self.session.close()

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=5)
        timeout = aiohttp.ClientTimeout(total=30)

        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )

        self.logger.debug(f"Создана сессия для {self.region_key}")

    async def create_session(self):
        """Публичный метод для пересоздания сессии (для Authenticator)"""
        await self._create_session()

    async def get_session(self) -> aiohttp.ClientSession:
        """Получить сессию (для Authenticator)"""
        if not self.session or self.session.closed:
            await self._create_session()
        return self.session

    async def search_and_save(
        self,
        db_manager,
        court_key: str,
        sequence_number: int,
        year: str
    ) -> Dict[str, Any]:
        """
        Поиск и сохранение дела

        Args:
            db_manager: менеджер БД (общий)
            court_key: ключ суда ('smas', 'appellate')
            sequence_number: порядковый номер дела
            year: год

        Returns:
            {
                'success': True/False,
                'saved': True/False,
                'case_number': str,
                'error': str or None
            }
        """
        if not self.authenticated:
            return {
                'success': False,
                'saved': False,
                'case_number': None,
                'error': 'not_authenticated'
            }

        try:
            return await self._do_search_and_save(
                db_manager, court_key, sequence_number, year
            )
        except Exception as e:
            self.logger.error(f"Ошибка поиска #{sequence_number}: {e}")
            return {
                'success': False,
                'saved': False,
                'case_number': None,
                'error': str(e)
            }

    async def _do_search_and_save(
        self,
        db_manager,
        court_key: str,
        sequence_number: int,
        year: str
    ) -> Dict[str, Any]:
        """
        Выполнение поиска и сохранения

        Сохраняет все дела с суффиксами
        """
        region_config = self.settings.get_region(self.region_key)
        court_config = self.settings.get_court(self.region_key, court_key)

        target_case_number = self.text_processor.generate_case_number(
            region_config, court_config, year, sequence_number
        )

        self.logger.debug(f"Поиск: {target_case_number}")

        results_html, cases = await self._search_case(
            region_config, court_config, year, sequence_number
        )

        if results_html is None:
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.REGION_NOT_FOUND
            }

        if not cases:
            self.logger.debug(f"Не найдено: {target_case_number}")
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.NO_RESULTS
            }

        # Находим ВСЕ дела, соответствующие целевому номеру
        matching_cases = [
            case for case in cases 
            if self.text_processor.is_matching_case_number(case.case_number, target_case_number)
        ]

        if not matching_cases:
            self.logger.debug(f"Целевое дело не найдено среди {len(cases)} результатов")
            return {
                'success': False,
                'saved': False,
                'saved_count': 0,
                'case_numbers': [],
                'case_number': target_case_number,
                'error': CaseStatus.TARGET_NOT_FOUND
            }

        # Сохраняем все найденные дела
        saved_count = 0
        saved_numbers = []

        for case in matching_cases:
            save_result = await db_manager.save_case(case)

            if save_result['status'] in [CaseStatus.SAVED, CaseStatus.UPDATED]:
                saved_count += 1
                saved_numbers.append(case.case_number)

                self.logger.info(
                    f"Сохранено: {case.case_number} | "
                    f"судья: {'да' if case.judge else 'нет'} | "
                    f"сторон: {len(case.plaintiffs) + len(case.defendants)} | "
                    f"событий: {len(case.events)}"
                )

        if saved_count > 0:
            return {
                'success': True,
                'saved': True,
                'saved_count': saved_count,
                'case_numbers': saved_numbers,
                'case_number': target_case_number,
                'results_html': results_html
            }

        return {
            'success': False,
            'saved': False,
            'saved_count': 0,
            'case_numbers': [],
            'case_number': target_case_number,
            'error': CaseStatus.SAVE_FAILED
        }

    async def _search_case(
        self,
        region_config: Dict,
        court_config: Dict,
        year: str,
        sequence_number: Union[int, str]
    ) -> tuple:
        """
        Выполнение поиска дела

        Для республиканских судов (Верховный, Кассационный) 
        используется search_region_id вместо id
        """
        viewstate, form_ids = await self.form_handler.prepare_search_form(
            self.session
        )

        # Передаём весь region_config вместо только id
        # FormHandler сам определит какой ID использовать
        await self.form_handler.select_region(
            self.session,
            viewstate,
            region_config,  # Изменено: передаём весь конфиг
            form_ids
        )

        await asyncio.sleep(0.5)

        results_html = await self.search_engine.search_case(
            self.session,
            viewstate,
            region_config.get('search_region_id', region_config['id']),  # Изменено
            court_config['id'],
            year,
            sequence_number,
            form_ids
        )

        cases = self.results_parser.parse(results_html)

        return results_html, cases

    async def search_case_by_number(self, case_number: str) -> tuple:
        """
        Поиск дела по номеру
        """
        case_info = self.text_processor.find_region_and_court_by_case_number(
            case_number, self.settings.regions
        )

        if not case_info:
            self.logger.warning(f"Не удалось определить регион: {case_number}")
            return None, []

        region_config = self.settings.get_region(case_info['region_key'])
        court_config = self.settings.get_court(
            case_info['region_key'], 
            case_info['court_key']
        )

        return await self._search_case(
            region_config,
            court_config,
            case_info['year'],
            case_info['sequence']  # ← передаём как строку
        )

    async def cleanup(self):
        """Очистка ресурсов воркера"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.debug(f"Сессия {self.region_key} закрыта")

        self.authenticated = False

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()
        return False
```

### File 5/26: `parsers\court_parser\core\session.py`
**Module:** `root` | **Lines:** 122

```python
"""
Управление HTTP сессиями с retry
"""
from typing import Dict, Any, Optional
import ssl
import asyncio
import aiohttp

from utils.logger import get_logger
from utils.retry import RetryStrategy, RetryConfig, CircuitBreaker, NonRetriableError

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

    async def request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """
        HTTP запрос с автоматическим retry

        Args:
            method: HTTP метод (GET, POST, etc)
            url: URL
            **kwargs: параметры для aiohttp

        Returns:
            {'status': int, 'text': str, 'headers': dict}

        Raises:
            NonRetriableError: если ошибка не подлежит retry (400, 401, 404, etc)
        """
        session = await self.get_session()

        # Получаем retry config
        http_retry_config = self.retry_config.get('http_request', {})

        async def _do_request() -> Dict[str, Any]:
            async with session.request(method, url, **kwargs) as response:
                # Читаем данные ДО выхода из контекстного менеджера
                text = await response.text()

                # Проверка на non-retriable статусы
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")

                # Проверка на retriable статусы
                if http_retry_config and response.status in http_retry_config.get('retriable_status_codes', [500, 502, 503, 504]):
                    raise aiohttp.ClientError(f"HTTP {response.status}")

                return {
                    'status': response.status,
                    'text': text,
                    'headers': dict(response.headers)
                }

        if not http_retry_config:
            return await _do_request()

        # Retry стратегия
        retry_cfg = RetryConfig(http_retry_config)
        strategy = RetryStrategy(retry_cfg, self.circuit_breaker)

        error_context = f"{method} {url}"
        return await strategy.execute_with_retry(_do_request, error_context=error_context)

    async def get(self, url: str, **kwargs) -> Dict[str, Any]:
        """GET запрос с retry"""
        return await self.request('GET', url, **kwargs)

    async def post(self, url: str, **kwargs) -> Dict[str, Any]:
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
```

### File 6/26: `parsers\court_parser\core\updaters\base_updater.py`
**Module:** `root` | **Lines:** 214

```python
"""
Базовый класс для updater'ов
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from collections import defaultdict
from datetime import datetime

from config.settings import Settings
from database.db_manager import DatabaseManager
from core.region_worker import RegionWorker
from utils.text_processor import TextProcessor
from utils.logger import get_logger
from utils.terminal_ui import init_ui, get_ui, Mode, RegionStatus

class BaseUpdater(ABC):
    """Базовый класс для всех updater'ов"""

    MODE: Mode = Mode.PARSE  # Переопределяется в наследниках

    def __init__(self, settings: Settings, db_manager: DatabaseManager):
        self.settings = settings
        self.db_manager = db_manager
        self.text_processor = TextProcessor()
        self.logger = get_logger(self.__class__.__name__.lower())

        self.common_config = settings.config.get('update_settings', {}).get('common', {})
        self.max_parallel = self.common_config.get('max_parallel_workers', 3)
        self.delay = self.common_config.get('delay_between_requests', 2.0)

        # Статистика
        self.stats = {
            'processed': 0,
            'errors': 0,
            'judges_found': 0,
            'events_added': 0,
            'docs_downloaded': 0,
        }

    @abstractmethod
    async def get_cases_to_process(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        raise NotImplementedError

    def _group_cases_by_region(self, case_numbers: List[str]) -> Dict[str, List[str]]:
        """Группировка дел по регионам"""
        grouped = defaultdict(list)

        for case_number in case_numbers:
            info = self.text_processor.find_region_and_court_by_case_number(
                case_number, self.settings.regions
            )
            if info:
                grouped[info['region_key']].append(case_number)
            else:
                self.logger.warning(f"Не удалось определить регион для {case_number}")

        return dict(grouped)

    async def run(self) -> Dict[str, Any]:
        """Запуск обработки"""
        # Получаем дела
        case_numbers = await self.get_cases_to_process()

        if not case_numbers:
            self.logger.info("Нет дел для обработки")
            return {'total': 0, 'processed': 0, 'skipped': True}

        # Группируем по регионам
        grouped = self._group_cases_by_region(case_numbers)

        if not grouped:
            self.logger.info("Нет дел после группировки по регионам")
            return {'total': len(case_numbers), 'processed': 0, 'skipped': True}

        self.logger.info(f"Дел к обработке: {len(case_numbers)}, регионов: {len(grouped)}")

        # Инициализация UI
        regions_display = {
            key: self.settings.get_region(key)['name']
            for key in grouped.keys()
        }

        ui = init_ui(self.MODE, regions_display, court_types=[])

        # Устанавливаем total для каждого региона
        for region_key, region_cases in grouped.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(region_cases)

        await ui.start()

        # Обработка
        semaphore = asyncio.Semaphore(self.max_parallel)

        try:
            tasks = [
                self._process_region_group(region_key, cases, semaphore, ui)
                for region_key, cases in grouped.items()
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

        finally:
            await ui.finish()

        # Финальный отчёт
        self._print_summary()

        return {
            'total': len(case_numbers),
            'processed': self.stats['processed'],
            'errors': self.stats['errors'],
        }

    async def _process_region_group(
        self, 
        region_key: str, 
        case_numbers: List[str],
        semaphore: asyncio.Semaphore,
        ui
    ) -> Dict[str, Any]:
        """Обработка группы дел одного региона"""
        async with semaphore:
            # Локальные счётчики для текущего региона
            region_stats = {
                'judges_found': 0,
                'events_added': 0,
                'docs_downloaded': 0,
            }
            worker = RegionWorker(self.settings, region_key)

            try:
                if not await worker.initialize():
                    self.logger.error(f"Не удалось инициализировать воркер {region_key}")
                    ui.region_error(region_key, "Init failed")
                    return {}

                ui.region_start(region_key)

                processed = 0

                for case_number in case_numbers:
                    try:
                        result = await self.process_case(worker, case_number)
                        processed += 1
                        self.stats['processed'] += 1

                        if result.get('error'):
                            self.stats['errors'] += 1

                        # Mode-specific stats
                        if result.get('judge_found'):
                            self.stats['judges_found'] += 1
                            region_stats['judges_found'] += 1  # ← ДОБАВИТЬ

                        if result.get('events_added'):
                            events_count = result['events_added']
                            self.stats['events_added'] += events_count
                            region_stats['events_added'] += events_count  # ← ДОБАВИТЬ

                        if result.get('documents_downloaded'):
                            docs_count = result['documents_downloaded']
                            self.stats['docs_downloaded'] += docs_count
                            region_stats['docs_downloaded'] += docs_count

                        # Обновляем UI
                        ui.update_progress(
                            region_key,
                            processed=processed,
                            found=region_stats['judges_found'],
                            events=region_stats['events_added'],
                            docs=region_stats['docs_downloaded']
                        )

                        await asyncio.sleep(self.delay)

                    except Exception as e:
                        self.logger.error(f"Ошибка обработки {case_number}: {e}")
                        self.stats['errors'] += 1
                        self.stats['processed'] += 1

                ui.region_done(region_key)

            except Exception as e:
                self.logger.error(f"Ошибка в регионе {region_key}: {e}")
                ui.region_error(region_key, str(e))

            finally:
                await worker.cleanup()

        return {}

    def _print_summary(self):
        """Вывод краткой статистики"""
        self.logger.info("-" * 40)
        self.logger.info(f"Обработано: {self.stats['processed']}")
        self.logger.info(f"Ошибок: {self.stats['errors']}")

        if self.stats['judges_found'] > 0:
            self.logger.info(f"Судей найдено: {self.stats['judges_found']}")
        if self.stats['events_added'] > 0:
            self.logger.info(f"Событий добавлено: {self.stats['events_added']}")
        if self.stats['docs_downloaded'] > 0:
            self.logger.info(f"Документов скачано: {self.stats['docs_downloaded']}")
```

### File 7/26: `parsers\court_parser\core\updaters\docs_updater.py`
**Module:** `root` | **Lines:** 141

```python
"""
Скачивание документов дел
"""
from typing import Dict, List, Any

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker
from search.document_handler import DocumentHandler
from utils.terminal_ui import Mode

class DocsUpdater(BaseUpdater):
    """Updater для скачивания документов"""

    MODE = Mode.DOCS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        docs_config = self.get_config()
        self.doc_handler = DocumentHandler(
            base_url=self.settings.base_url,
            storage_dir=docs_config.get('storage_dir', './documents'),
            regions_config=self.settings.regions
        )
        self.download_delay = docs_config.get('download_delay', 2.0)

    def get_config(self) -> Dict[str, Any]:
        return self.settings.config.get('update_settings', {}).get('docs', {})

    async def get_cases_to_process(self) -> List[str]:
        config = self.get_config()

        if not config.get('enabled', True):
            self.logger.info("DocsUpdater отключен в конфиге")
            return []

        filters = config.get('filters', {})

        cases = await self.db_manager.get_cases_for_documents(
            filters={
                'party_keywords': filters.get('party_keywords', []),
                'party_role': filters.get('party_role'),
                'court_types': filters.get('court_types'),
                'regions': filters.get('regions'),
                'year': filters.get('year'),
                'check_interval_days': config.get('check_interval_days', 5),
                'final_post_check_delay_days': config.get('final_post_check_delay_days', 10),
                'order': filters.get('order', 'oldest'),
            },
            limit=config.get('max_per_session'),
            final_event_types=config.get('final_event_types', []),
            max_attempts=config.get('documents_max_attempts', 5)
        )

        self.logger.info(f"Дел для скачивания документов: {len(cases)}")
        return [c['case_number'] for c in cases]

    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        result = {
            'case_number': case_number,
            'success': False,
            'documents_downloaded': 0
        }

        config = self.get_config()
        max_attempts = config.get('documents_max_attempts', 5)
        final_post_check_delay_days = config.get('final_post_check_delay_days', 10)
        final_event_types = config.get('final_event_types', [])

        try:
            results_html, cases = await worker.search_case_by_number(case_number)

            case_id = await self.db_manager.get_case_id(case_number)
            if not case_id:
                return result

            # Дело не найдено на сайте сейчас — фиксируем как проверку без прогресса
            if not results_html or not cases:
                await self.db_manager.finalize_document_check(
                    case_id=case_id,
                    final_event_types=final_event_types,
                    final_post_check_delay_days=final_post_check_delay_days,
                    made_progress=False,
                    max_attempts=max_attempts
                )
                return result

            target = next(
                (c for c in cases
                 if self.text_processor.is_matching_case_number(c.case_number, case_number)),
                None
            )

            if not target or target.result_index is None:
                await self.db_manager.finalize_document_check(
                    case_id=case_id,
                    final_event_types=final_event_types,
                    final_post_check_delay_days=final_post_check_delay_days,
                    made_progress=False,
                    max_attempts=max_attempts
                )
                return result

            # Обновляем события дела (если найдены новые статусы/события)
            await self.db_manager.update_case(target)

            # Получаем ключи уже скачанных ранее документов
            existing_keys = await self.db_manager.get_document_keys(case_id)

            # Скачиваем новые документы
            fetch = await self.doc_handler.fetch_all_documents(
                session=worker.session,
                results_html=results_html,
                case_number=case_number,
                case_index=target.result_index,
                existing_keys=existing_keys,
                delay=self.download_delay
            )

            downloaded = fetch['downloaded']
            if downloaded:
                await self.db_manager.save_documents(case_id, downloaded)
                result['documents_downloaded'] = len(downloaded)
                self.logger.info(f"Скачано: {len(downloaded)} документов для {case_number}")

            # Фиксируем статус жизненного цикла дела по результатам проверки
            await self.db_manager.finalize_document_check(
                case_id=case_id,
                final_event_types=final_event_types,
                final_post_check_delay_days=final_post_check_delay_days,
                made_progress=fetch['made_progress'],
                max_attempts=max_attempts
            )

            result['success'] = True

        except Exception as e:
            self.logger.error(f"Ошибка: {case_number}: {e}")
            result['error'] = str(e)

        return result
```

### File 8/26: `parsers\court_parser\core\updaters\events_updater.py`
**Module:** `root` | **Lines:** 74

```python
"""
Обновление событий (истории) дел
"""
from typing import Dict, List, Any

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker
from utils.terminal_ui import Mode

class EventsUpdater(BaseUpdater):
    """Updater для обновления событий дел"""

    MODE = Mode.EVENTS

    def get_config(self) -> Dict[str, Any]:
        return self.settings.config.get('update_settings', {}).get('case_events', {})

    async def get_cases_to_process(self) -> List[str]:
        config = self.get_config()

        if not config.get('enabled', True):
            self.logger.info("EventsUpdater отключен в конфиге")
            return []

        filters = config.get('filters', {})

        cases = await self.db_manager.get_cases_for_update({
            'defendant_keywords': filters.get('party_keywords', []),
            'exclude_event_types': filters.get('exclude_event_types', []),
            'update_interval_days': config.get('check_interval_days', 2),
            'final_event_types': config.get('final_event_types', []),
            'final_check_period_days': config.get('final_check_period_days', 30),
            'max_stale_days': config.get('max_stale_days'),
        })

        self.logger.info(f"Дел для обновления событий: {len(cases)}")
        return cases

    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        result = {
            'case_number': case_number,
            'success': False,
            'events_added': 0
        }

        try:
            _, cases = await worker.search_case_by_number(case_number)

            if not cases:
                return result

            target = next(
                (c for c in cases 
                 if self.text_processor.is_matching_case_number(c.case_number, case_number)),
                None
            )

            if not target:
                return result

            update_result = await self.db_manager.update_case(target)
            await self.db_manager.mark_case_as_updated(case_number)

            result['success'] = True 
            result['events_added'] = update_result.get('events_added', 0)

            if result['events_added'] > 0:
                self.logger.info(f"Добавлено событий: {result['events_added']} для {case_number}")

        except Exception as e:
            self.logger.error(f"Ошибка: {case_number}: {e}")
            result['error'] = str(e)

        return result
```

### File 9/26: `parsers\court_parser\core\updaters\gaps_updater.py`
**Module:** `root` | **Lines:** 359

```python
"""
Проверка и закрытие пропусков в нумерации дел
"""
from typing import Dict, List, Any, Set, Tuple
from datetime import datetime
from collections import defaultdict

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker
from utils.constants import CaseStatus

class GapsUpdater(BaseUpdater):
    """
    Updater для проверки пропущенных номеров дел

    Команда: --mode gaps

    Логика:
    1. Для каждого региона/суда/года получает существующие номера из БД
    2. Находит пропуски в последовательности
    3. Пытается спарсить пропущенные номера
    4. Обновляет метку времени проверки
    """

    MODE = 'gaps'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Конфигурация gaps
        self.gaps_config = self.settings.parsing_settings
        self.max_gaps_per_session = self.gaps_config.get('max_gaps_per_session', 200)
        self.gaps_check_interval_days = self.gaps_config.get('gaps_check_interval_days', 30)

        # Статистика
        self.total_gaps_found = 0
        self.total_gaps_closed = 0

    def get_config(self) -> Dict[str, Any]:
        """Получить конфигурацию"""
        return self.gaps_config

    async def get_cases_to_process(self) -> List[str]:
        """
        Этот метод не используется напрямую для gaps.
        Вместо этого используем get_gaps_to_process()
        """
        return []

    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        """Обработка одного пропущенного номера"""
        result = {
            'case_number': case_number,
            'success': False,
            'found': False,
            'saved': False
        }

        try:
            # Определяем регион и суд по номеру
            case_info = self.text_processor.find_region_and_court_by_case_number(
                case_number, self.settings.regions
            )

            if not case_info:
                result['error'] = 'region_not_found'
                return result

            # Поиск дела
            search_result = await worker.search_and_save(
                db_manager=self.db_manager,
                court_key=case_info['court_key'],
                sequence_number=int(case_info['sequence']),
                year=case_info['year']
            )

            result['success'] = True
            result['found'] = search_result.get('saved', False)
            result['saved'] = search_result.get('saved', False)

            if result['saved']:
                self.total_gaps_closed += 1
                self.logger.info(f"✅ Пропуск закрыт: {case_number}")
            else:
                self.logger.debug(f"Пропуск не найден на сайте: {case_number}")

        except Exception as e:
            self.logger.error(f"Ошибка обработки пропуска {case_number}: {e}")
            result['error'] = str(e)

        return result

    async def get_gaps_for_court(
        self,
        region_key: str,
        court_key: str,
        year: str
    ) -> List[int]:
        """
        Получить список пропущенных номеров для суда.

        Ищет дырки ДВУХ типов:
        1. Внутренние — между min и max существующих (классические пропуски)
        2. Хвостовые — несколько номеров ЗА max (на случай если сетевой сбой
        прервал парсинг в хвосте, и дела после max остались несобранными)
        """
        existing = await self.db_manager.get_existing_case_numbers(
            region_key, court_key, year, self.settings
        )

        if not existing:
            return []

        max_seq = max(existing)
        min_seq = min(existing)

        # 1. Внутренние дырки
        full_range = set(range(min_seq, max_seq + 1))
        gaps = full_range - existing

        # 2. Хвостовая проверка: пробуем несколько номеров за max.
        #    Если сбой оборвал хвост — дела найдутся. Если их реально нет —
        #    process_case вернёт no_results, и они просто не сохранятся.
        tail_probe = self.gaps_config.get('gaps_tail_probe', 5)
        for i in range(1, tail_probe + 1):
            gaps.add(max_seq + i)

        return sorted(gaps)

    async def run(self) -> Dict[str, Any]:
        """
        Запуск проверки пропусков

        Переопределяем базовый run() для специфичной логики gaps
        """
        years = self.settings.get_parsing_years()  # ← список годов
        court_types = self.gaps_config.get('court_types', ['smas', 'appellate'])
        target_regions = self.settings.get_target_regions()

        # Собираем все пропуски по регионам
        all_gaps: Dict[str, List[str]] = defaultdict(list)
        gaps_by_court: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        # Сохраняем для какого года какой суд проверялся (для обновления метаданных)
        # ключ: (region_key, court_key) → year
        checked_pairs: List[Tuple[str, str, str]] = []

        self.logger.info("=" * 60)
        self.logger.info(f"GAPS CHECK: Поиск пропусков по годам {years}")
        self.logger.info("=" * 60)

        # Фаза 1: Сбор пропусков по ВСЕМ годам
        for year in years:
            for region_key in target_regions:
                region_config = self.settings.get_region(region_key)
                available_courts = list(region_config.get('courts', {}).keys())

                courts_to_check = [c for c in court_types if c in available_courts]
                if not courts_to_check and available_courts:
                    courts_to_check = available_courts

                for court_key in courts_to_check:
                    should_check = await self.db_manager.should_check_gaps(
                        region_key, court_key, year, self.gaps_check_interval_days
                    )

                    if not should_check:
                        self.logger.debug(
                            f"Пропускаем {region_key}/{court_key}/{year} - "
                            f"проверялось менее {self.gaps_check_interval_days} дней назад"
                        )
                        continue

                    gaps = await self.get_gaps_for_court(region_key, court_key, year)

                    checked_pairs.append((region_key, court_key, year))

                    if gaps:
                        region_cfg = self.settings.get_region(region_key)
                        court_cfg = self.settings.get_court(region_key, court_key)

                        for seq in gaps:
                            case_number = self.text_processor.generate_case_number(
                                region_cfg, court_cfg, year, seq
                            )
                            all_gaps[region_key].append(case_number)

                        gaps_by_court[region_key][court_key] += len(gaps)
                        self.total_gaps_found += len(gaps)

                        self.logger.info(
                            f"📋 {region_key}/{court_key}/{year}: найдено {len(gaps)} пропусков"
                        )

        # Проверяем есть ли пропуски
        if not all_gaps:
            self.logger.info("✅ Пропусков не найдено!")
            return {'total_gaps': 0, 'closed': 0}

        self.logger.info("-" * 60)
        self.logger.info(f"Всего пропусков: {self.total_gaps_found}")

        # Ограничение на сессию
        total_to_process = sum(len(g) for g in all_gaps.values())
        if total_to_process > self.max_gaps_per_session:
            self.logger.warning(
                f"⚠️ Ограничение: обработаем только {self.max_gaps_per_session} "
                f"из {total_to_process} пропусков"
            )
            all_gaps = self._limit_gaps(all_gaps, self.max_gaps_per_session)

        self.logger.info("-" * 60)

        # Фаза 2: Инициализация UI
from utils.terminal_ui import init_ui, Mode

        regions_display = {
            key: self.settings.get_region(key)['name']
            for key in all_gaps.keys()
        }

        # Для gaps не нужны court_types в UI
        ui = init_ui(Mode.PARSE, regions_display, court_types=[])

        # Устанавливаем total для каждого региона
        for region_key, gaps_list in all_gaps.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(gaps_list)

        await ui.start()

        # Фаза 3: Обработка пропусков
import asyncio
        semaphore = asyncio.Semaphore(self.max_parallel)

        async def process_region_gaps(region_key: str, gap_numbers: List[str]):
            async with semaphore:
                worker = RegionWorker(self.settings, region_key)

                try:
                    if not await worker.initialize():
                        self.logger.error(f"Не удалось инициализировать воркер {region_key}")
                        ui.region_error(region_key, "Init failed")
                        return

                    ui.region_start(region_key)

                    processed = 0
                    closed = 0

                    for case_number in gap_numbers:
                        result = await self.process_case(worker, case_number)
                        processed += 1

                        if result.get('saved'):
                            closed += 1

                        # Обновляем UI
                        ui.update_progress(
                            region_key, 
                            processed=processed, 
                            found=closed
                        )

                        await asyncio.sleep(self.delay)

                    ui.region_done(region_key)

                    # Обновляем дату проверки пропусков по всем проверенным (суд, год)
                    region_checked = [
                        (ck, yr) for (rk, ck, yr) in checked_pairs
                        if rk == region_key
                    ]
                    for court_key, yr in region_checked:
                        existing = await self.db_manager.get_existing_case_numbers(
                            region_key, court_key, yr, self.settings
                        )
                        max_seq = max(existing) if existing else 0
                        await self.db_manager.update_gaps_check_date(
                            region_key, court_key, yr, max_seq
                        )

                except Exception as e:
                    self.logger.error(f"Ошибка в регионе {region_key}: {e}")
                    ui.region_error(region_key, str(e))

                finally:
                    await worker.cleanup()

        # Запускаем обработку
        tasks = [
            process_region_gaps(region_key, gaps_list)
            for region_key, gaps_list in all_gaps.items()
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

        await ui.finish()

        # Финальный отчёт
        self._print_report()

        return {
            'total_gaps': self.total_gaps_found,
            'closed': self.total_gaps_closed,
            'remaining': self.total_gaps_found - self.total_gaps_closed
        }

    def _limit_gaps(
        self, 
        all_gaps: Dict[str, List[str]], 
        limit: int
    ) -> Dict[str, List[str]]:
        """
        Ограничить количество пропусков с приоритетом по регионам

        Распределяем лимит пропорционально количеству пропусков
        """
        total = sum(len(g) for g in all_gaps.values())
        if total <= limit:
            return all_gaps

        result = {}
        remaining = limit

        # Сортируем регионы по количеству пропусков (больше = приоритетнее)
        sorted_regions = sorted(
            all_gaps.items(), 
            key=lambda x: len(x[1]), 
            reverse=True
        )

        for region_key, gaps in sorted_regions:
            # Пропорциональное распределение
            region_share = int(limit * len(gaps) / total)
            region_share = min(region_share, remaining, len(gaps))

            if region_share > 0:
                result[region_key] = gaps[:region_share]
                remaining -= region_share

        return result

    def _print_report(self):
        """Вывод финального отчёта"""
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("GAPS CHECK COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"Всего пропусков найдено: {self.total_gaps_found}")
        self.logger.info(f"Пропусков закрыто:       {self.total_gaps_closed}")
        self.logger.info(f"Осталось пропусков:      {self.total_gaps_found - self.total_gaps_closed}")

        if self.total_gaps_found > 0:
            pct = 100 * self.total_gaps_closed / self.total_gaps_found
            self.logger.info(f"Процент закрытия:        {pct:.1f}%")

        self.logger.info("=" * 60)
```

### File 10/26: `parsers\court_parser\core\updaters\judge_updater.py`
**Module:** `root` | **Lines:** 71

```python
"""
Обновление судей в делах СМАС
"""
from typing import Dict, List, Any

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker
from utils.terminal_ui import Mode

class JudgeUpdater(BaseUpdater):
    """Updater для обновления информации о судьях"""

    MODE = Mode.JUDGE

    def get_config(self) -> Dict[str, Any]:
        return self.settings.config.get('update_settings', {}).get('judge', {})

    async def get_cases_to_process(self) -> List[str]:
        config = self.get_config()

        if not config.get('enabled', True):
            self.logger.info("JudgeUpdater отключен в конфиге")
            return []

        interval_days = config.get('check_interval_days', 1)

        cases = await self.db_manager.get_smas_cases_without_judge(
            self.settings,
            interval_days=interval_days,
            final_event_types=config.get('final_event_types', []),
            max_stale_days=config.get('max_stale_days'),
        )

        self.logger.info(f"Дел без судьи: {len(cases)}")
        return cases

    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        result = {
            'case_number': case_number,
            'success': False,
            'judge_found': False,
        }

        try:
            _, cases = await worker.search_case_by_number(case_number)

            if not cases:
                return result

            target = next(
                (c for c in cases 
                 if self.text_processor.is_matching_case_number(c.case_number, case_number)),
                None
            )

            if not target:
                return result

            if target.judge:
                await self.db_manager.update_case(target)
                result['judge_found'] = True
                self.logger.info(f"Судья найден: {case_number} → {target.judge}")

            await self.db_manager.mark_case_as_updated(case_number)
            result['success'] = True

        except Exception as e:
            self.logger.error(f"Ошибка: {case_number}: {e}")
            result['error'] = str(e)

        return result
```

### File 11/26: `parsers\court_parser\database\db_manager.py`
**Module:** `root` | **Lines:** 1264

```python
"""
Менеджер базы данных
"""
import re
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timedelta
import asyncpg

from database.models import CaseData, EventData
from utils.text_processor import TextProcessor
from utils.validators import DataValidator
from utils.logger import get_logger
from utils.constants import PartyRole

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
import traceback

        try:
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

        except Exception as e:
            self.logger.critical(f"❌ Ошибка подключения к БД: {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            raise

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
                await self._link_party_to_case(conn, case_id, party_id, PartyRole.PLAINTIFF)

        # Ответчики
        for defendant in case_data.defendants:
            if self.validator.validate_party_name(defendant):
                party_id = await self._get_or_create_party(conn, defendant)
                await self._link_party_to_case(conn, case_id, party_id, PartyRole.DEFENDANT)

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

    def _extract_sequence_number(self, case_number: str) -> Optional[int]:
        """
        Извлечь порядковый номер из номера дела

        Обрабатывает суффиксы дубликатов (2), (3) и т.д.

        Примеры:
            7101-25-00-6/123     → 123
            7101-25-00-6/123(2)  → 123
            6001-25-00-6ап/1736  → 1736
            6001-25-00-6ап/1736(2) → 1736

        Returns:
            int или None если формат некорректный
        """

        if '/' not in case_number:
            return None

        # Берём часть после последнего /
        seq_part = case_number.split('/')[-1]

        # Убираем суффикс (N) если есть: "1736(2)" → "1736"
        seq_clean = re.sub(r'\(\d+\)$', '', seq_part)

        try:
            return int(seq_clean)
        except ValueError:
            self.logger.warning(f"Не удалось извлечь sequence из: {case_number}")
            return None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def get_cases_for_update(self, filters: Dict) -> List[str]:
        """
        Получить номера дел для обновления событий.

        Логика (по жизненному циклу, НЕ по возрасту):
        1. Ответчик содержит ключевые слова (если заданы)
        2. Нет исключённых событий (exclude_event_types)
        3. Не проверялось дольше update_interval_days
        4. Дело АКТИВНО (нет финального события)
           ИЛИ финал появился недавно (окно дозагрузки)
        5. Предохранитель: дело не заброшено (есть движение)
        """
        defendant_keywords = filters.get('defendant_keywords', [])
        exclude_events = filters.get('exclude_event_types', [])
        interval_days = filters.get('update_interval_days', 2)

        # НОВОЕ: критерий завершённости вместо возраста
        final_event_types = filters.get('final_event_types', [])
        final_check_period_days = filters.get('final_check_period_days', 30)
        max_stale_days = filters.get('max_stale_days')

        query = """
            SELECT DISTINCT c.case_number, c.case_date
            FROM cases c
        """

        conditions = []
        params = []
        param_counter = 1

        # ФИЛЬТР 1: По ответчику
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
            placeholders = ', '.join(
                [f'${i}' for i in range(param_counter, param_counter + len(exclude_events))]
            )
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

        # ★ ФИЛЬТР 4: Дело активно (нет финала) ИЛИ финал в окне дозагрузки
        if final_event_types:
            event_placeholders = ', '.join(
                [f"${param_counter + i}" for i in range(len(final_event_types))]
            )
            params.extend(final_event_types)
            param_counter += len(final_event_types)

            final_subq = f"""
                SELECT MAX(ce.event_date)
                FROM case_events ce
                JOIN event_types et ON ce.event_type_id = et.id
                WHERE ce.case_id = c.id
                AND et.name IN ({event_placeholders})
            """
            conditions.append(f"""
                (
                    ({final_subq}) IS NULL
                    OR ({final_subq}) > CURRENT_DATE - INTERVAL '{final_check_period_days} days'
                )
            """)

        # ★ ФИЛЬТР 5: Предохранитель от заброшенных дел
        # Смотрим давность ПОСЛЕДНЕГО события (или дату дела если событий нет)
        if max_stale_days:
            conditions.append(f"""
                COALESCE(
                    (SELECT MAX(ce.event_date) FROM case_events ce WHERE ce.case_id = c.id),
                    c.case_date
                ) > CURRENT_DATE - INTERVAL '{max_stale_days} days'
            """)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY c.case_date ASC"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        case_numbers = [row['case_number'] for row in rows]
        self.logger.info(f"Найдено дел для обновления событий: {len(case_numbers)}")
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
        """
        # Получаем конфигурацию для формирования префикса номера
        region_config = settings.get_region(region_key)
        court_config = settings.get_court(region_key, court_key)

        # Формируем префикс номера дела
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]
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
            seq_num = self._extract_sequence_number(row['case_number'])
            if seq_num is not None:
                sequence_numbers.add(seq_num)

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
            seq_num = self._extract_sequence_number(row['case_number'])
            if seq_num is not None and seq_num > max_sequence:
                max_sequence = seq_num

        self.logger.info(
            f"Последний номер для {region_key}/{court_key}/{year}: {max_sequence}"
        )

        return max_sequence

    def get_smas_instance_codes(self, settings) -> Set[str]:
        """
        Собрать все instance_code для судов СМАС из конфигурации

        Returns:
            {'94', '93', ...}
        """
        smas_codes = set()

        for region_key, region_config in settings.regions.items():
            courts = region_config.get('courts', {})
            smas_court = courts.get('smas')

            if smas_court and smas_court.get('instance_code'):
                smas_codes.add(smas_court['instance_code'])

        self.logger.debug(f"СМАС instance_codes: {smas_codes}")
        return smas_codes

    async def get_smas_cases_without_judge(
        self,
        settings,
        interval_days: int = 2,
        final_event_types: List[str] = None,
        max_stale_days: int = None
    ) -> List[str]:
        """
        Получить номера дел СМАС без назначенного судьи.

        Логика (по жизненному циклу):
        - судья не назначен
        - дело СМАС
        - не проверялось последние interval_days
        - дело НЕ завершено (нет финала) — у закрытого дела судья уже не появится
        - предохранитель: дело не заброшено
        """
        smas_codes = self.get_smas_instance_codes(settings)

        if not smas_codes:
            self.logger.warning("Не найдены instance_codes для СМАС в конфиге")
            return []

        code_conditions = []
        params = []
        param_counter = 1

        for code in smas_codes:
            code_conditions.append(f"SUBSTRING(case_number FROM 3 FOR 2) = ${param_counter}")
            params.append(code)
            param_counter += 1

        codes_where = f"({' OR '.join(code_conditions)})"

        conditions = [
            "judge_id IS NULL",
            codes_where,
            f"""(
                last_updated_at IS NULL
                OR last_updated_at < NOW() - INTERVAL '{interval_days} days'
            )"""
        ]

        # ★ Вместо возраста — дело не завершено
        if final_event_types:
            event_placeholders = ', '.join(
                [f"${param_counter + i}" for i in range(len(final_event_types))]
            )
            params.extend(final_event_types)
            param_counter += len(final_event_types)
            conditions.append(f"""
                NOT EXISTS (
                    SELECT 1 FROM case_events ce
                    JOIN event_types et ON ce.event_type_id = et.id
                    WHERE ce.case_id = cases.id
                    AND et.name IN ({event_placeholders})
                )
            """)

        # ★ Предохранитель от заброшенных дел
        if max_stale_days:
            conditions.append(f"""
                COALESCE(
                    (SELECT MAX(ce.event_date) FROM case_events ce WHERE ce.case_id = cases.id),
                    case_date
                ) > CURRENT_DATE - INTERVAL '{max_stale_days} days'
            """)

        query = f"""
            SELECT case_number
            FROM cases
            WHERE {' AND '.join(conditions)}
            ORDER BY case_date DESC
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        case_numbers = [row['case_number'] for row in rows]
        self.logger.info(f"Найдено дел СМАС без судьи: {len(case_numbers)}")
        return case_numbers

    # Docs processing:

    async def get_document_keys(self, case_id: int) -> set:
        """Получить ключи уже скачанных документов"""
        async with self.pool.acquire() as conn:
            # Выбираем doc_index вместе с датой и именем
            rows = await conn.fetch(
                "SELECT doc_index, doc_date, doc_name FROM case_documents WHERE case_id = $1",
                case_id
            )
        # Формируем ключ в новом формате с использованием doc_index
        return {f"{r['doc_date'].isoformat()}|{r['doc_index']}|{r['doc_name']}" for r in rows if r['doc_index'] is not None}

    async def save_documents(self, case_id: int, documents: List[Dict]) -> int:
        """Сохранить информацию о документах"""
        if not documents:
            return 0
        saved = 0
        async with self.pool.acquire() as conn:
            for doc in documents:
                try:
                    # Добавляем doc_index в INSERT-запрос и меняем условие ON CONFLICT на (case_id, doc_index)
                    await conn.execute("""
                        INSERT INTO case_documents (case_id, doc_index, doc_date, doc_name, file_path, file_size, downloaded_at)
                        VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
                        ON CONFLICT (case_id, doc_index) DO UPDATE
                        SET file_path = EXCLUDED.file_path, 
                            file_size = EXCLUDED.file_size,
                            doc_date = EXCLUDED.doc_date,
                            doc_name = EXCLUDED.doc_name,
                            downloaded_at = CURRENT_TIMESTAMP
                    """, case_id, doc['index'], doc['doc_date'], doc['doc_name'], doc['file_path'], doc.get('file_size'))
                    saved += 1
                except Exception as e:
                    self.logger.error(f"Ошибка сохранения документа: {e}")
        return saved

    async def get_cases_pending_documents(self, limit: int = None) -> List[Dict]:
        """Получить дела для скачивания документов"""
        query = """
            SELECT id, case_number FROM cases
            WHERE documents_pending = TRUE
            ORDER BY case_date DESC
        """
        if limit:
            query += f" LIMIT {limit}"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
        return [{'id': r['id'], 'case_number': r['case_number']} for r in rows]

    async def mark_documents_complete(self, case_id: int):
        """
        Документы скачаны ПОЛНОСТЬЮ.
        Сбрасываем попытки, помечаем complete, фиксируем дату проверки.
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE cases
                SET documents_pending = FALSE,
                    documents_complete = TRUE,
                    documents_attempts = 0,
                    documents_checked_at = CURRENT_TIMESTAMP
                WHERE id = $1
            """, case_id)
        self.logger.debug(f"Документы полные: case_id={case_id}")

    async def mark_documents_incomplete(self, case_id: int, made_progress: bool):
        """
        Документы скачаны НЕ полностью.

        made_progress=True  → скачали хоть один новый док → сброс счётчика (второй шанс)
        made_progress=False → прогресса нет → увеличиваем счётчик попыток

        documents_checked_at ставим, чтобы сработал интервал (не долбить каждую минуту),
        но documents_complete остаётся FALSE → дело снова попадёт в выборку после интервала.
        """
        async with self.pool.acquire() as conn:
            if made_progress:
                await conn.execute("""
                    UPDATE cases
                    SET documents_attempts = 0,
                        documents_checked_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, case_id)
            else:
                await conn.execute("""
                    UPDATE cases
                    SET documents_attempts = documents_attempts + 1,
                        documents_checked_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, case_id)
        self.logger.debug(
            f"Документы НЕ полные: case_id={case_id}, progress={made_progress}"
        )

    async def mark_documents_exhausted(self, case_id: int):
        """
        Попытки исчерпаны (attempts >= лимит), но документы не полные.
        Принудительно отпускаем дело (complete=TRUE), чтобы не зацикливаться.
        Логируем как WARNING для ручного разбора.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT case_number FROM cases WHERE id = $1", case_id)
            await conn.execute("""
                UPDATE cases
                SET documents_pending = FALSE,
                    documents_complete = TRUE,
                    documents_checked_at = CURRENT_TIMESTAMP
                WHERE id = $1
            """, case_id)
        case_number = row['case_number'] if row else case_id
        self.logger.warning(
            f"⚠️ Документы ЧАСТИЧНО скачаны (исчерпаны попытки): {case_number} "
            f"— требуется ручная проверка"
        )

    async def get_documents_attempts(self, case_id: int) -> int:
        """Текущее число попыток скачивания для дела."""
        async with self.pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT documents_attempts FROM cases WHERE id = $1", case_id
            )
        return val or 0

    async def mark_case_for_documents(self, case_id: int):
        """Пометить дело для скачивания документов"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE cases SET documents_pending = TRUE WHERE id = $1", case_id
            )

    async def get_cases_for_documents(
        self,
        filters: Dict,
        limit: int = None,
        final_event_types: List[str] = None,
        max_attempts: int = 5,
        **kwargs
    ) -> List[Dict]:
        """
        Получить дела для скачивания документов на основе конечного автомата (FSM).

        Критерии выбора:
        - c.documents_complete = FALSE (дело еще не заархивировано окончательно)
        - c.documents_attempts < max_attempts (количество неудачных попыток не превышено)
        - Соответствует фильтрам по сторонам, регионам, судам и годам.
        - Разделение на две группы планирования:
          1. Активные дела (нет финальных событий):
             - c.documents_checked_at IS NULL
             - ИЛИ c.documents_checked_at < NOW() - INTERVAL 'check_interval_days' (стандартно 5 дней)
          2. Завершенные дела в ожидании финального чека:
             - Есть финальное событие.
             - Прошло не менее final_post_check_delay_days (стандартно 10 дней) с момента финального события.
             - Финальный чек еще не проводился (c.documents_checked_at < final_event_date + final_post_check_delay_days).
        """
        interval_days = filters.get('check_interval_days', 5)
        final_post_check_delay_days = filters.get('final_post_check_delay_days', 10)

        events_to_use = final_event_types if final_event_types else [
            "Возврат", "Завершение дела", "Решение вступило в силу",
            "Отправлено в архив", "Передано по неподсудности",
            "Оставлено без движения", "Прекращено"
        ]

        params = []
        param_counter = 1
        base_conditions = []

        # Защита от зацикливания при постоянных сетевых сбоях
        base_conditions.append(f"c.documents_attempts < {int(max_attempts)}")

        # Формируем CTE для нахождения дат последних финальных событий
        event_placeholders = [f"${param_counter + i}" for i in range(len(events_to_use))]
        event_placeholders_str = ', '.join(event_placeholders)
        params.extend(events_to_use)
        param_counter += len(events_to_use)

        p_interval_days_idx = param_counter
        params.append(interval_days)
        param_counter += 1

        p_delay_days_idx = param_counter
        params.append(final_post_check_delay_days)
        param_counter += 1

        # CTE для получения максимальной даты закрытия дела
        cte = f"""
            WITH case_final_dates AS (
                SELECT ce.case_id, MAX(ce.event_date) as max_final_date
                FROM case_events ce
                JOIN event_types et ON ce.event_type_id = et.id
                WHERE et.name IN ({event_placeholders_str})
                GROUP BY ce.case_id
            )
        """

        # Добавляем FSM логику планирования проверок
        base_conditions.append(f"""
            (
                -- Группа 1: Активные дела (нет закрывающих событий) -> проверяем с интервалом (5 дней)
                (fd.max_final_date IS NULL AND (c.documents_checked_at IS NULL OR c.documents_checked_at < NOW() - (${p_interval_days_idx} * INTERVAL '1 day')))
                OR
                -- Группа 2: Завершенные дела -> ждем ровно delay_days (10 дней) с даты закрытия для финальной проверки
                (fd.max_final_date IS NOT NULL 
                 AND CURRENT_DATE >= fd.max_final_date + (${p_delay_days_idx} * INTERVAL '1 day')
                 AND (c.documents_checked_at IS NULL OR c.documents_checked_at < fd.max_final_date + (${p_delay_days_idx} * INTERVAL '1 day'))
                )
            )
        """)

        # === Фильтр по сторонам (keyword) ===
        party_keywords = filters.get('party_keywords', [])
        party_role = filters.get('party_role')

        if party_keywords:
            role_condition = ""
            if party_role:
                if isinstance(party_role, str):
                    party_role = [party_role]
                role_placeholders = [f"${param_counter + i}" for i in range(len(party_role))]
                role_condition = f"AND cp.party_role IN ({', '.join(role_placeholders)})"
                params.extend(party_role)
                param_counter += len(party_role)

            keyword_placeholders = []
            for keyword in party_keywords:
                keyword_placeholders.append(f"p.name ILIKE ${param_counter}")
                params.append(f'%{keyword}%')
                param_counter += 1
            keywords_sql = ' OR '.join(keyword_placeholders)

            base_conditions.append(f"""
                EXISTS (
                    SELECT 1 FROM case_parties cp
                    JOIN parties p ON cp.party_id = p.id
                    WHERE cp.case_id = c.id
                    {role_condition}
                    AND ({keywords_sql})
                )
            """)

        # === Фильтр по регионам ===
        regions = filters.get('regions')
        if regions:
            region_codes = self._get_region_kato_codes(regions)
            if region_codes:
                region_conditions = []
                for code in region_codes:
                    region_conditions.append(f"SUBSTRING(c.case_number FROM 1 FOR 2) = ${param_counter}")
                    params.append(code)
                    param_counter += 1
                base_conditions.append(f"({' OR '.join(region_conditions)})")

        # === Фильтр по типам судов ===
        court_types = filters.get('court_types')
        if court_types:
            court_codes = self._get_court_instance_codes(court_types)
            if court_codes:
                code_conditions = []
                for code in court_codes:
                    code_conditions.append(f"SUBSTRING(c.case_number FROM 3 FOR 2) = ${param_counter}")
                    params.append(code)
                    param_counter += 1
                base_conditions.append(f"({' OR '.join(code_conditions)})")

        # === Фильтр по году ===
        year = filters.get('year')
        if year:
            year_short = year[-2:]
            base_conditions.append(f"SUBSTRING(c.case_number FROM 6 FOR 2) = ${param_counter}")
            params.append(year_short)
            param_counter += 1

        query = f"""
            {cte}
            SELECT DISTINCT c.id, c.case_number, c.case_date, c.documents_checked_at
            FROM cases c
            LEFT JOIN case_final_dates fd ON c.id = fd.case_id
            WHERE c.documents_complete = FALSE AND 
        """
        query += ' AND '.join(base_conditions)

        order = filters.get('order', 'oldest')
        if order == 'oldest':
            query += " ORDER BY c.case_date ASC, c.documents_checked_at NULLS FIRST"
        else:
            query += " ORDER BY c.case_date DESC, c.documents_checked_at NULLS FIRST"

        if limit:
            query += f" LIMIT {limit}"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        self.logger.info(f"Найдено дел для документов: {len(rows)}")
        return [{'id': r['id'], 'case_number': r['case_number']} for r in rows]

    async def get_final_event_date(self, case_id: int, final_event_types: List[str]) -> Optional[datetime]:
        """
        Получить дату самого свежего финального события для дела

        Args:
            case_id: ID дела
            final_event_types: список типов финальных событий

        Returns:
            Дата самого свежего финального события или None
        """
        if not final_event_types:
            return None

        placeholders = ', '.join([f'${i+2}' for i in range(len(final_event_types))])

        query = f"""
            SELECT MAX(ce.event_date) as final_date
            FROM case_events ce
            JOIN event_types et ON ce.event_type_id = et.id
            WHERE ce.case_id = $1
            AND et.name IN ({placeholders})
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, case_id, *final_event_types)

        if row and row['final_date']:
            # event_date это date, конвертируем в datetime
            return datetime.combine(row['final_date'], datetime.min.time())

        return None

    def _get_court_instance_codes(self, court_types: List[str]) -> List[str]:
        """
        Получить instance_codes для указанных типов судов
        """
        court_code_map = {
            'smas': ['94', '93'],
            'appellate': ['99', '00'],
            'cassation': ['03'],
            'supreme': ['01'],
        }

        codes = set()
        for court_type in court_types:
            if court_type in court_code_map:
                codes.update(court_code_map[court_type])

        return list(codes)

    def _get_region_kato_codes(self, regions: List[str]) -> List[str]:
        """
        Получить КАТО-коды для указанных регионов
        """
        region_kato_map = {
            'republic': '60',
            'astana': '71',
            'almaty': '75',
            'shymkent': '52',
            'akmola': '11',
            'aktobe': '15',
            'almaty_region': '19',
            'atyrau': '23',
            'vko': '63',
            'zhambyl': '31',
            'zko': '27',
            'karaganda': '35',
            'kostanay': '39',
            'kyzylorda': '43',
            'mangystau': '47',
            'pavlodar': '55',
            'sko': '59',
            'turkestan': '51',
            'ulytau': '62',
            'abay': '10',
            'zhetysu': '33',
        }

        codes = []
        for region in regions:
            if region in region_kato_map:
                codes.append(region_kato_map[region])

        return codes

    async def get_case_id(self, case_number: str) -> Optional[int]:
        """Получить ID дела по номеру"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM cases WHERE case_number = $1",
                case_number
            )
            return row['id'] if row else None

    async def get_gaps_check_date(
        self, 
        region_key: str, 
        court_key: str, 
        year: str
    ) -> Optional[datetime]:
        """
        Получить дату последней проверки пропусков

        Returns:
            datetime или None если никогда не проверялось
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT gaps_checked_at 
                FROM parsing_metadata
                WHERE region_key = $1 AND court_key = $2 AND year = $3
            """, region_key, court_key, year)

            return row['gaps_checked_at'] if row else None

    async def update_gaps_check_date(
        self, 
        region_key: str, 
        court_key: str, 
        year: str,
        last_sequence: int = 0
    ):
        """
        Обновить дату проверки пропусков

        Вызывается после завершения проверки gaps для региона/суда/года
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parsing_metadata (region_key, court_key, year, gaps_checked_at, last_sequence_checked)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4)
                ON CONFLICT (region_key, court_key, year) 
                DO UPDATE SET 
                    gaps_checked_at = CURRENT_TIMESTAMP,
                    last_sequence_checked = $4,
                    updated_at = CURRENT_TIMESTAMP
            """, region_key, court_key, year, last_sequence)

            self.logger.debug(
                f"Обновлена дата проверки пропусков: {region_key}/{court_key}/{year}"
            )

    async def reset_gaps_check_date(
        self,
        region_key: str,
        court_key: str,
        year: str
    ):
        """
        Сбросить дату проверки пропусков → GAPS обязан перепроверить
        этот суд/год при следующем запуске, ИГНОРИРУЯ интервал.

        Вызывается когда после всех retry остались неподтверждённые
        (грязные) номера от технических ошибок.
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parsing_metadata (region_key, court_key, year, gaps_checked_at)
                VALUES ($1, $2, $3, NULL)
                ON CONFLICT (region_key, court_key, year)
                DO UPDATE SET gaps_checked_at = NULL, updated_at = CURRENT_TIMESTAMP
            """, region_key, court_key, year)

        self.logger.warning(
            f"Дата проверки пропусков СБРОШЕНА (требуется re-check): "
            f"{region_key}/{court_key}/{year}"
        )

    async def should_check_gaps(
        self, 
        region_key: str, 
        court_key: str, 
        year: str,
        interval_days: int = 30
    ) -> bool:
        """
        Проверить, нужно ли проверять пропуски

        Returns:
            True если прошло больше interval_days с последней проверки
        """
        last_check = await self.get_gaps_check_date(region_key, court_key, year)

        if last_check is None:
            self.logger.debug(f"Пропуски {region_key}/{court_key}/{year}: никогда не проверялись")
            return True

        days_since_check = (datetime.now() - last_check).days
        should_check = days_since_check >= interval_days

        self.logger.debug(
            f"Пропуски {region_key}/{court_key}/{year}: "
            f"проверялись {days_since_check} дней назад, "
            f"{'нужна проверка' if should_check else 'пропускаем'}"
        )

        return should_check

    async def update_case(self, case_data: CaseData) -> Dict[str, Any]:
        """
        Обновление дела (события, судья)

        Returns:
            {'case_id': int, 'events_added': int}
        """
        try:
            async with self.pool.acquire() as conn:
                # 1. Получаем case_id
                case_id = await conn.fetchval(
                    "SELECT id FROM cases WHERE case_number = $1",
                    case_data.case_number
                )

                if not case_id:
                    # Дело не найдено — создаём новое
                    result = await self.save_case(case_data)
                    return {
                        'case_id': result.get('case_id'),
                        'events_added': len(case_data.events)
                    }

                # 2. Обновляем судью (если появился)
                if case_data.judge:
                    judge_id = await self._get_or_create_judge(conn, case_data.judge)
                    await conn.execute(
                        "UPDATE cases SET judge_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
                        judge_id, case_id
                    )

                # 3. Получаем существующие события
                existing_events = await conn.fetch("""
                    SELECT et.name, ce.event_date 
                    FROM case_events ce
                    JOIN event_types et ON ce.event_type_id = et.id
                    WHERE ce.case_id = $1
                """, case_id)

                existing_keys = {
                    f"{row['name']}|{row['event_date'].isoformat()}" 
                    for row in existing_events
                }

                # 4. Добавляем новые события
                events_added = 0
                for event in case_data.events:
                    event_key = f"{event.event_type}|{event.event_date.isoformat()}"
                    if event_key not in existing_keys:
                        event_type_id = await self._get_or_create_event_type(conn, event.event_type)
                        await conn.execute("""
                            INSERT INTO case_events (case_id, event_type_id, event_date)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """, case_id, event_type_id, event.event_date)
                        events_added += 1

                # 5. Обновляем метку времени
                await conn.execute(
                    "UPDATE cases SET last_updated_at = CURRENT_TIMESTAMP WHERE id = $1",
                    case_id
                )

                return {'case_id': case_id, 'events_added': events_added}

        except Exception as e:
            self.logger.error(f"Ошибка обновления дела {case_data.case_number}: {e}")
            return {'case_id': None, 'events_added': 0}

    async def finalize_document_check(
        self,
        case_id: int,
        final_event_types: List[str],
        final_post_check_delay_days: int = 10,
        made_progress: bool = False,
        max_attempts: int = 5
    ) -> bool:
        """
        Обновляет статус планирования и полноты документов дела на основе его жизненного цикла.

        Логика:
        1. Если у дела обнаружено закрывающее событие:
           - Проверяем, наступил ли срок финального чека (прошло >= final_post_check_delay_days с даты закрытия).
           - Если срок наступил: переводим в documents_complete = TRUE (Архивировано). Больше не проверяем.
           - Если срок еще не наступил: обновляем дату последней проверки и ставим documents_complete = FALSE.
             Дело "замораживается" в БД и не будет выбираться до наступления даты финальной проверки.
        2. Если дело активное (нет закрывающих событий):
           - c.documents_complete остается FALSE (его нужно проверять каждые 5 дней).
           - Если есть прогресс (made_progress = True), сбрасываем попытки.
           - Если прогресса нет и превышен лимит попыток, переводим в complete = TRUE как предохранитель.
           - Обновляем documents_checked_at = NOW(), чтобы следующая проверка произошла через 5 дней.
        """
        events_to_use = final_event_types if final_event_types else [
            "Возврат", "Завершение дела", "Решение вступило в силу",
            "Отправлено в архив", "Передано по неподсудности",
            "Оставлено без движения", "Прекращено"
        ]

        final_date = await self.get_final_event_date(case_id, events_to_use)

        if final_date:
            now = datetime.now()
            days_passed = (now - final_date).days

            if days_passed >= final_post_check_delay_days:
                # Срок финального чека подошел -> переводим в завершенные окончательно!
                await self.mark_documents_complete(case_id)
                self.logger.info(
                    f"Дело [ID: {case_id}] закрыто окончательно (финальная проверка завершена через {days_passed} дн.)"
                )
                return True
            else:
                # Дело закрылось, но 10 дней еще не прошли -> отправляем в ожидание
                await self.mark_documents_incomplete(case_id, made_progress=made_progress)
                self.logger.info(
                    f"Дело [ID: {case_id}] ожидает срока финальной проверки (прошло {days_passed}/{final_post_check_delay_days} дн.)"
                )
                return False
        else:
            # Дело активное
            attempts = await self.get_documents_attempts(case_id)
            if not made_progress and attempts + 1 >= max_attempts:
                # Предохранитель для предотвращения зависания при постоянных сетевых сбоях
                await self.mark_documents_exhausted(case_id)
                return True
            else:
                await self.mark_documents_incomplete(case_id, made_progress=made_progress)
                self.logger.debug(
                    f"Дело [ID: {case_id}] проверено (активный статус, следующая проверка через 5 дн.)"
                )
                return False
```

### File 12/26: `parsers\court_parser\database\models.py`
**Module:** `root` | **Lines:** 67

```python
"""
Структуры данных для БД
"""
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Optional

@dataclass
class CaseData:
    """Данные дела"""
    case_number: str
    case_date: Optional[date] = None
    judge: Optional[str] = None
    plaintiffs: List[str] = field(default_factory=list)
    defendants: List[str] = field(default_factory=list)
    events: List['EventData'] = field(default_factory=list)
    result_index: Optional[int] = None  # Индекс в результатах поиска (не сохраняется в БД)

    def to_dict(self) -> dict:
        """Преобразование в словарь (без result_index — он не для БД)"""
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

@dataclass
class DocumentInfo:
    index: int  # Оригинальный ID документа с сайта
    doc_date: date
    doc_name: str
    doc_type: str = ""

    @property
    def unique_key(self) -> str:
        # Теперь ключ гарантированно уникален за счет self.index
        return f"{self.doc_date.isoformat()}|{self.index}|{self.doc_name}"

    def to_dict(self) -> dict:
        return {
            'index': self.index,
            'doc_date': self.doc_date,
            'doc_name': self.doc_name,
            'doc_type': self.doc_type
        }
```

### File 13/26: `parsers\court_parser\main.py`
**Module:** `root` | **Lines:** 1002

```python
"""
Точка входа парсера
"""
import sys
import os
import asyncio
from datetime import datetime

# ★ ФОРСИРУЕМ UTF-8 НА WINDOWS
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except:
        pass

from core.parser import CourtParser
from core.region_worker import RegionWorker
from config.settings import Settings
from database.db_manager import DatabaseManager
from utils.logger import init_logging, get_logger
from utils.terminal_ui import init_ui, get_ui, Mode, RegionStatus, CourtStatus

def _reset_ui():
    """Сброс глобального UI для корректного вывода логов"""
    try:
import utils.terminal_ui as terminal_ui
        if terminal_ui._ui_instance is not None:
            # Принудительно останавливаем UI
            terminal_ui._ui_instance._running = False
            terminal_ui._ui_instance = None
    except Exception:
        pass

async def parse_all_regions_from_config() -> dict:
    """Парсинг всех регионов"""

    settings = Settings()
    ps = settings.parsing_settings

    years = settings.get_parsing_years()   # ← список годов
    court_types = ps.get('court_types', ['smas', 'appellate'])
    start_from = ps.get('start_from', 1)
    max_number = ps.get('max_number', 9999)
    max_consecutive_empty = ps.get('max_consecutive_empty', 5)
    delay_between_requests = ps.get('delay_between_requests', 2)
    max_parallel_regions = ps.get('max_parallel_regions', 3)

    # Получаем регионы
    all_regions = settings.get_target_regions()
    limit_regions = settings.get_limit_regions()
    regions_to_process = all_regions[:limit_regions] if limit_regions else all_regions

    # Собираем информацию о регионах и их судах
    regions_display = {}
    region_courts = {}

    for key in regions_to_process:
        region_config = settings.get_region(key)
        regions_display[key] = region_config['name']

        # Получаем доступные суды для этого региона
        available_courts = list(region_config.get('courts', {}).keys())

        # Фильтруем только те, которые в court_types (или все если это особый регион)
        if available_courts:
            # Проверяем есть ли стандартные суды (smas, appellate)
            standard_courts = [c for c in court_types if c in available_courts]

            if standard_courts:
                # Обычный регион — используем стандартные суды
                region_courts[key] = standard_courts
            else:
                # Особый регион (Republic) — используем все его суды
                region_courts[key] = available_courts
        else:
            region_courts[key] = court_types

    # Инициализация UI с информацией о судах для каждого региона
    ui = init_ui(Mode.PARSE, regions_display, court_types, region_courts)

    # Подключение к БД
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()

    # Семафор для параллельности
    semaphore = asyncio.Semaphore(max_parallel_regions)

    # Статистика для отчёта
    report_data = {
        'no_judge': 0,
        'no_parties': 0,
    }

    logger = get_logger('main')

    try:
        await ui.start()

        async def process_region(region_key: str):
            async with semaphore:
                region_court_types = region_courts.get(region_key, court_types)

                # ★ Цикл по годам: текущий + хвост прошлого (в Q1)
                for year in years:
                    await process_region_with_ui(
                        region_key=region_key,
                        settings=settings,
                        db_manager=db_manager,
                        ui=ui,
                        court_types=region_court_types,
                        year=year,
                        start_from=start_from,
                        max_number=max_number,
                        max_consecutive_empty=max_consecutive_empty,
                        delay=delay_between_requests,
                        report_data=report_data,
                        logger=logger
                    )

        tasks = [process_region(r) for r in regions_to_process]
        await asyncio.gather(*tasks, return_exceptions=True)

        await ui.finish()
        ui.print_final_report(report_data)

    except KeyboardInterrupt:
        await ui.finish()
        print("\n⚠ Прервано пользователем")

    finally:
        await db_manager.disconnect()

    return {}

async def process_region_with_ui(
    region_key: str,
    settings: Settings,
    db_manager,
    ui,
    court_types: list,
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay: float,
    report_data: dict,
    logger
):
    """Обработка региона"""

    region_config = settings.get_region(region_key)
    worker = RegionWorker(settings, region_key)

    try:
        if not await worker.initialize():
            logger.error(f"Failed to initialize worker for {region_key}")
            ui.region_error(region_key, "Failed to initialize")
            return

        ui.region_start(region_key)
        logger.info(f"Started processing region: {region_key}")

        region_no_judge = 0

        for court_key in court_types:
            court_config = region_config['courts'].get(court_key)
            if not court_config:
                continue

            ui.court_start(region_key, court_key)
            logger.info(f"Started court: {region_key}/{court_key}")

            try:
                court_stats = await parse_court(
                    worker=worker,
                    db_manager=db_manager,
                    settings=settings,
                    region_key=region_key,
                    court_key=court_key,
                    year=year,
                    start_from=start_from,
                    max_number=max_number,
                    max_consecutive_empty=max_consecutive_empty,
                    delay=delay,
                    ui=ui,
                    logger=logger
                )

                ui.court_done(region_key, court_key, court_stats['saved'])
                logger.info(f"Completed court: {region_key}/{court_key}, saved: {court_stats['saved']}")

                region_no_judge += court_stats.get('no_judge', 0)

            except Exception as e:
                logger.error(f"Error in court {region_key}/{court_key}: {e}")
                ui.court_error(region_key, court_key, str(e))

        ui.region_done(region_key)
        logger.info(f"Completed region: {region_key}")

        report_data['no_judge'] += region_no_judge

    except Exception as e:
        logger.error(f"Error in region {region_key}: {e}", exc_info=True)
        ui.region_error(region_key, str(e))

    finally:
        await worker.cleanup()

async def parse_court(
    worker,
    db_manager,
    settings,
    region_key: str,
    court_key: str,
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay: float,
    ui,
    logger
) -> dict:
    """Парсинг суда"""

    stats = {
        'saved': 0,
        'queries': 0,
        'no_judge': 0,
        'consecutive_empty': 0,
    }

    # ★ Номера, по которым была ТЕХНИЧЕСКАЯ ошибка (не "дело отсутствует")
    failed_numbers: list = []

    def _is_technical_error(err) -> bool:
        """
        True  → запрос не подтвердил отсутствие дела (сеть/авторизация/circuit breaker/БД)
        False → сайт корректно ответил 'нет дела' (no_results / target_not_found)
                или номер некорректен (region_not_found)
        """
        if not err:
            return False
        if err in ('no_results', 'target_not_found', 'region_not_found'):
            return False  # подтверждённое отсутствие или баг номера — НЕ переспрашиваем
        return True       # сеть, авторизация, circuit breaker, save_failed

    # Получаем существующие номера
    existing = await db_manager.get_existing_case_numbers(
        region_key, court_key, year, settings
    )
    last_in_db = max(existing) if existing else 0

    current_number = last_in_db + 1 if last_in_db > 0 else start_from

    logger.debug(f"Starting from number {current_number} (last in DB: {last_in_db})")

    while current_number <= max_number:
        if stats['consecutive_empty'] >= max_consecutive_empty:
            logger.info(f"Reached {max_consecutive_empty} consecutive empty results, stopping")
            break

        result = await worker.search_and_save(
            db_manager=db_manager,
            court_key=court_key,
            sequence_number=current_number,
            year=year
        )

        stats['queries'] += 1
        ui.increment_queries(region_key)

        if result['success'] and result.get('saved'):
            saved_count = result.get('saved_count', 1)
            stats['saved'] += saved_count
            stats['consecutive_empty'] = 0
            ui.increment_saved(region_key, court_key, saved_count)

            case_numbers = result.get('case_numbers', [result.get('case_number')])
            for case_num in case_numbers:
                has_judge = result.get('has_judge', True)
                if not has_judge:
                    stats['no_judge'] += 1
                logger.info(
                    f"Saved: {case_num}",
                    extra={'region': region_key, 'court': court_key, 'case_number': case_num}
                )

        elif result.get('error') == 'no_results':
            # Сайт подтвердил: дела нет → реальная пустота
            stats['consecutive_empty'] += 1
            logger.debug(
                f"No results for #{current_number} (consecutive: {stats['consecutive_empty']})",
                extra={'region': region_key, 'court': court_key}
            )

        elif result.get('error') == 'target_not_found':
            # Сайт ответил, но целевого нет → тоже подтверждённая пустота
            stats['consecutive_empty'] += 1
            logger.debug(
                f"Target not found for #{current_number}",
                extra={'region': region_key, 'court': court_key}
            )

        elif _is_technical_error(result.get('error')):
            # ★ ТЕХНИЧЕСКАЯ ошибка: НЕ трогаем consecutive_empty, ЗАПОМИНАЕМ номер
            failed_numbers.append(current_number)
            logger.warning(
                f"Technical error for #{current_number}: {result.get('error')} "
                f"(will retry at end of court)",
                extra={'region': region_key, 'court': court_key}
            )

        current_number += 1
        await asyncio.sleep(delay)

    # =========================================================================
    # ★ ФИНАЛЬНЫЙ ПЕРЕСПРОС сбойных номеров (Уровень 2)
    # Сеть могла восстановиться за время прохода суда
    # =========================================================================
    if failed_numbers:
        logger.info(
            f"Retrying {len(failed_numbers)} failed numbers for {region_key}/{court_key}/{year}"
        )
        still_failed = []

        for num in failed_numbers:
            result = await worker.search_and_save(
                db_manager=db_manager,
                court_key=court_key,
                sequence_number=num,
                year=year
            )
            stats['queries'] += 1
            ui.increment_queries(region_key)

            if result['success'] and result.get('saved'):
                saved_count = result.get('saved_count', 1)
                stats['saved'] += saved_count
                ui.increment_saved(region_key, court_key, saved_count)
                logger.info(f"Retry success: #{num} saved")
            elif _is_technical_error(result.get('error')):
                still_failed.append(num)
            # no_results / target_not_found на ретрае → дело реально отсутствует, ОК

            await asyncio.sleep(delay)

        if still_failed:
            # Уровень 3: оставляем для GAPS следующей сессии.
            # Сбрасываем дату проверки этого суда → GAPS обязан перепроверить.
            logger.warning(
                f"{len(still_failed)} numbers still failed after retry "
                f"for {region_key}/{court_key}/{year}: {still_failed}. "
                f"Resetting gaps date for re-check next session."
            )
            await db_manager.reset_gaps_check_date(region_key, court_key, year)

    return stats

# === UPDATE РЕЖИМЫ ===

async def run_update_judge():
    """Режим обновления судей"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    logger = get_logger('main')

    try:
        judge_config = settings.update_settings.get('judge', {})
        cases = await db_manager.get_smas_cases_without_judge(
            settings,
            interval_days=judge_config.get('check_interval_days', 1),
            final_event_types=judge_config.get('final_event_types', []),
            max_stale_days=judge_config.get('max_stale_days'),
        )

        if not cases:
            logger.info("Нет дел для обновления")
            return

from utils.text_processor import TextProcessor
        tp = TextProcessor()

        grouped = {}
        for case_number in cases:
            info = tp.find_region_and_court_by_case_number(case_number, settings.regions)
            if info:
                grouped.setdefault(info['region_key'], []).append(case_number)

        regions_display = {k: settings.get_region(k)['name'] for k in grouped.keys()}
        ui = init_ui(Mode.JUDGE, regions_display, court_types=[])

        for region_key, region_cases in grouped.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(region_cases)

        await ui.start()

        semaphore = asyncio.Semaphore(3)

        async def process_region(region_key: str, region_cases: list):
            async with semaphore:
                worker = RegionWorker(settings, region_key)
                try:
                    if not await worker.initialize():
                        ui.region_error(region_key, "Init failed")
                        return

                    ui.region_start(region_key)

                    processed = 0
                    found = 0

                    for case_number in region_cases:
                        _, cases_found = await worker.search_case_by_number(case_number)

                        processed += 1

                        target = next(
                            (c for c in cases_found if tp.is_matching_case_number(c.case_number, case_number)),
                            None
                        )

                        if target and target.judge:
                            await db_manager.update_case(target)
                            found += 1
                            logger.info(f"Judge found for {case_number}: {target.judge}")

                        await db_manager.mark_case_as_updated(case_number)

                        ui.update_progress(region_key, processed=processed, found=found)

                        await asyncio.sleep(2)

                    ui.region_done(region_key)

                except Exception as e:
                    logger.error(f"Error in {region_key}: {e}")
                    ui.region_error(region_key, str(e))
                finally:
                    await worker.cleanup()

        tasks = [process_region(k, v) for k, v in grouped.items()]
        await asyncio.gather(*tasks, return_exceptions=True)

        await ui.finish()
        ui.print_final_report()

    finally:
        await db_manager.disconnect()

async def run_update_events():
    """Режим обновления событий"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    logger = get_logger('main')

    try:
        config = settings.update_settings.get('case_events', {})
        filters = config.get('filters', {})

        cases = await db_manager.get_cases_for_update({
            'defendant_keywords': filters.get('party_keywords', []),
            'exclude_event_types': filters.get('exclude_event_types', []),
            'update_interval_days': config.get('check_interval_days', 2),
            # НОВОЕ:
            'final_event_types': config.get('final_event_types', []),
            'final_check_period_days': config.get('final_check_period_days', 30),
            'max_stale_days': config.get('max_stale_days'),
        })

        if not cases:
            logger.info("Нет дел для обновления")
            return

from utils.text_processor import TextProcessor
        tp = TextProcessor()

        grouped = {}
        for case_number in cases:
            info = tp.find_region_and_court_by_case_number(case_number, settings.regions)
            if info:
                grouped.setdefault(info['region_key'], []).append(case_number)

        regions_display = {k: settings.get_region(k)['name'] for k in grouped.keys()}
        ui = init_ui(Mode.EVENTS, regions_display, court_types=[])

        for region_key, region_cases in grouped.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(region_cases)

        await ui.start()

        semaphore = asyncio.Semaphore(3)

        async def process_region(region_key: str, region_cases: list):
            async with semaphore:
                worker = RegionWorker(settings, region_key)
                try:
                    if not await worker.initialize():
                        ui.region_error(region_key, "Init failed")
                        return

                    ui.region_start(region_key)

                    processed = 0
                    events_total = 0

                    for case_number in region_cases:
                        _, cases_found = await worker.search_case_by_number(case_number)

                        processed += 1

                        target = next(
                            (c for c in cases_found if tp.is_matching_case_number(c.case_number, case_number)),
                            None
                        )

                        events_added = 0
                        if target:
                            result = await db_manager.update_case(target)
                            events_added = result.get('events_added', 0)
                            events_total += events_added
                            if events_added > 0:
                                logger.info(f"Added {events_added} events for {case_number}")

                        await db_manager.mark_case_as_updated(case_number)

                        ui.update_progress(region_key, processed=processed, events=events_total)

                        await asyncio.sleep(2)

                    ui.region_done(region_key)

                except Exception as e:
                    logger.error(f"Error in {region_key}: {e}")
                    ui.region_error(region_key, str(e))
                finally:
                    await worker.cleanup()

        tasks = [process_region(k, v) for k, v in grouped.items()]
        await asyncio.gather(*tasks, return_exceptions=True)

        await ui.finish()
        ui.print_final_report()

    finally:
        await db_manager.disconnect()

async def run_update_docs():
    """Режим скачивания документов"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    logger = get_logger('main')

    try:
        config = settings.update_settings.get('docs', {})
        filters = config.get('filters', {})
        final_post_check_delay_days = config.get('final_post_check_delay_days', 10)
        final_event_types = config.get('final_event_types', [])
        max_attempts = config.get('documents_max_attempts', 5)

        cases = await db_manager.get_cases_for_documents(
            filters={
                'party_keywords': filters.get('party_keywords', []),
                'party_role': filters.get('party_role'),
                'court_types': filters.get('court_types'),
                'regions': filters.get('regions'),
                'year': filters.get('year'),
                'check_interval_days': config.get('check_interval_days', 5),
                'final_post_check_delay_days': final_post_check_delay_days,
                'order': filters.get('order', 'oldest'),
            },
            limit=config.get('max_per_session'),
            final_event_types=final_event_types,
            max_attempts=max_attempts
        )

        if not cases:
            logger.info("Нет дел для обработки")
            return

from utils.text_processor import TextProcessor
from search.document_handler import DocumentHandler

        tp = TextProcessor()
        doc_handler = DocumentHandler(
            settings.base_url,
            config.get('storage_dir', './documents'),
            settings.regions
        )

        grouped = {}
        for case in cases:
            info = tp.find_region_and_court_by_case_number(case['case_number'], settings.regions)
            if info:
                grouped.setdefault(info['region_key'], []).append(case)

        regions_display = {k: settings.get_region(k)['name'] for k in grouped.keys()}
        ui = init_ui(Mode.DOCS, regions_display, court_types=[])

        for region_key, region_cases in grouped.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(region_cases)

        await ui.start()

        semaphore = asyncio.Semaphore(3)

        async def process_region(region_key: str, region_cases: list):
            async with semaphore:
                worker = RegionWorker(settings, region_key)
                try:
                    if not await worker.initialize():
                        ui.region_error(region_key, "Init failed")
                        return

                    ui.region_start(region_key)

                    processed = 0
                    docs_total = 0

                    for case in region_cases:
                        case_number = case['case_number']
                        case_id = case['id']

                        results_html, cases_found = await worker.search_case_by_number(case_number)
                        processed += 1

                        if not results_html or not cases_found:
                            await db_manager.finalize_document_check(
                                case_id=case_id,
                                final_event_types=final_event_types,
                                final_post_check_delay_days=final_post_check_delay_days,
                                made_progress=False,
                                max_attempts=max_attempts
                            )
                            continue

                        target = next(
                            (c for c in cases_found if tp.is_matching_case_number(c.case_number, case_number)),
                            None
                        )

                        if not target or target.result_index is None:
                            await db_manager.finalize_document_check(
                                case_id=case_id,
                                final_event_types=final_event_types,
                                final_post_check_delay_days=final_post_check_delay_days,
                                made_progress=False,
                                max_attempts=max_attempts
                            )
                            continue

                        # Обновляем события дела (если найдены новые статусы/события)
                        await db_manager.update_case(target)

                        existing_keys = await db_manager.get_document_keys(case_id)

                        fetch = await doc_handler.fetch_all_documents(
                            session=worker.session,
                            results_html=results_html,
                            case_number=case_number,
                            case_index=target.result_index,
                            existing_keys=existing_keys,
                            delay=config.get('download_delay', 0)
                        )

                        downloaded = fetch['downloaded']
                        if downloaded:
                            await db_manager.save_documents(case_id, downloaded)
                            docs_total += len(downloaded)
                            logger.info(f"Downloaded {len(downloaded)} docs for {case_number}")

                        # Вызов централизованного планировщика жизненного цикла документа
                        await db_manager.finalize_document_check(
                            case_id=case_id,
                            final_event_types=final_event_types,
                            final_post_check_delay_days=final_post_check_delay_days,
                            made_progress=fetch['made_progress'],
                            max_attempts=max_attempts
                        )

                        ui.update_progress(region_key, processed=processed, docs=docs_total)

                        await asyncio.sleep(2)

                    ui.region_done(region_key)

                except Exception as e:
                    logger.error(f"Error in {region_key}: {e}")
                    ui.region_error(region_key, str(e))
                finally:
                    await worker.cleanup()

        tasks = [process_region(k, v) for k, v in grouped.items()]
        await asyncio.gather(*tasks, return_exceptions=True)

        await ui.finish()
        ui.print_final_report()

    finally:
        await db_manager.disconnect()

async def run_gaps_check():
    """Режим проверки пропусков"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()

    try:
from core.updaters.gaps_updater import GapsUpdater

        gaps_updater = GapsUpdater(settings, db_manager)
        result = await gaps_updater.run()

        return result

    finally:
        await db_manager.disconnect()

async def run_pipeline(): 
    """
    Режим полного пайплайна: gaps → parse → events → docs
    """
    logger = get_logger('pipeline')

    logger.info("=" * 60)
    logger.info("PIPELINE START: gaps → parse → events → docs")
    logger.info("=" * 60)

    pipeline_start = datetime.now()

    # ★ Фиксируем годы один раз на весь pipeline (защита от запуска в полночь смены года)
    _settings_init = Settings()
    frozen_years = _settings_init.get_parsing_years()
    logger.info(f"Pipeline years: {frozen_years}")

    results = {
        'gaps': {'found': 0, 'closed': 0},
        'parse': {'saved': 0},
        'judge': {'processed': 0, 'found': 0},
        'events': {'processed': 0, 'events_added': 0},
        'docs': {'processed': 0, 'docs_downloaded': 0},
    }

    # =========================================================================
    # ЭТАП 0: GAPS
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 0: CHECKING GAPS")
    logger.info("-" * 60)

    try:
        settings = Settings()
        db_manager = DatabaseManager(settings.database)
        await db_manager.connect()

        try:
from core.updaters.gaps_updater import GapsUpdater
            gaps_updater = GapsUpdater(settings, db_manager)
            gaps_result = await gaps_updater.run()

            results['gaps']['found'] = gaps_result.get('total_gaps', 0)
            results['gaps']['closed'] = gaps_result.get('closed', 0)
        finally:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Gaps failed: {e}", exc_info=True)
    finally:
        _reset_ui()

    # =========================================================================
    # ЭТАП 1: PARSE
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 1: PARSING NEW CASES")
    logger.info("-" * 60)

    try:
        await parse_all_regions_from_config()

        ui = get_ui()
        if ui:
            results['parse']['saved'] = ui.stats.total_saved
    except Exception as e:
        logger.error(f"Parse failed: {e}", exc_info=True)
    finally:
        _reset_ui()

    # =========================================================================
    # ПОДСЧЁТ ДЕЛ ДЛЯ СЛЕДУЮЩИХ ЭТАПОВ
    # =========================================================================
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()

    try:
        # Подсчёт дел для events
        events_config = settings.update_settings.get('case_events', {})
        events_filters = events_config.get('filters', {})
        events_cases = await db_manager.get_cases_for_update({
            'defendant_keywords': events_filters.get('party_keywords', []),
            'exclude_event_types': events_filters.get('exclude_event_types', []),
            'update_interval_days': events_config.get('check_interval_days', 2),
            # НОВОЕ:
            'final_event_types': events_config.get('final_event_types', []),
            'final_check_period_days': events_config.get('final_check_period_days', 30),
            'max_stale_days': events_config.get('max_stale_days'),
        })

        # Подсчёт дел для docs
        docs_config = settings.update_settings.get('docs', {})
        docs_filters = docs_config.get('filters', {})
        docs_cases = await db_manager.get_cases_for_documents(
            filters={
                'party_keywords': docs_filters.get('party_keywords', []),
                'party_role': docs_filters.get('party_role'),
                'court_types': docs_filters.get('court_types'),
                'regions': docs_filters.get('regions'),
                'year': docs_filters.get('year'),
                'check_interval_days': docs_config.get('check_interval_days', 5),
                'final_post_check_delay_days': docs_config.get('final_post_check_delay_days', 10),
                'order': docs_filters.get('order', 'oldest'),
            },
            limit=docs_config.get('max_per_session'),
            final_event_types=docs_config.get('final_event_types', []),
            max_attempts=docs_config.get('documents_max_attempts', 5)
        )

        logger.info("")
        logger.info("=" * 60)
        logger.info(f"PENDING: Events={len(events_cases)} cases, Docs={len(docs_cases)} cases")
        logger.info("=" * 60)

    finally:
        await db_manager.disconnect()

    # =========================================================================
    # ЭТАП 2: JUDGE
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 2: UPDATING JUDGES")
    logger.info("-" * 60)

    try:
        settings = Settings()
        db_manager = DatabaseManager(settings.database)
        await db_manager.connect()

        try:
from core.updaters.judge_updater import JudgeUpdater
            judge_updater = JudgeUpdater(settings, db_manager)
            judge_result = await judge_updater.run()

            results['judge'] = {
                'processed': judge_result.get('processed', 0),
                'found': judge_updater.stats.get('judges_found', 0)
            }
        finally:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Judge failed: {e}", exc_info=True)
    finally:
        _reset_ui()

    # =========================================================================
    # ЭТАП 3: EVENTS
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 3: UPDATING EVENTS")
    logger.info("-" * 60)

    try:
        settings = Settings()
        db_manager = DatabaseManager(settings.database)
        await db_manager.connect()

        try:
from core.updaters.events_updater import EventsUpdater
            events_updater = EventsUpdater(settings, db_manager)
            events_result = await events_updater.run()

            results['events']['processed'] = events_result.get('processed', 0)
            results['events']['events_added'] = events_updater.stats.get('events_added', 0)
        finally:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Events failed: {e}", exc_info=True)
    finally:
        _reset_ui()

    # =========================================================================
    # ЭТАП 4: DOCS
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 4: DOWNLOADING DOCUMENTS")
    logger.info("-" * 60)

    try:
        settings = Settings()
        db_manager = DatabaseManager(settings.database)
        await db_manager.connect()

        try:
from core.updaters.docs_updater import DocsUpdater
            docs_updater = DocsUpdater(settings, db_manager)
            docs_result = await docs_updater.run()

            results['docs']['processed'] = docs_result.get('processed', 0)
            results['docs']['docs_downloaded'] = docs_updater.stats.get('docs_downloaded', 0)
        finally:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Docs failed: {e}", exc_info=True)
    finally:
        _reset_ui()

    # =========================================================================
    # ФИНАЛЬНЫЙ ОТЧЁТ
    # =========================================================================
    pipeline_elapsed = datetime.now() - pipeline_start
    total_minutes, total_seconds = divmod(int(pipeline_elapsed.total_seconds()), 60)

    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total time: {total_minutes}:{total_seconds:02d}")
    logger.info(f"Gaps:   {results['gaps']['closed']}/{results['gaps']['found']} closed")
    logger.info(f"Parse:  {results['parse']['saved']} cases")
    logger.info(f"Judge:  {results['judge']['found']}/{results['judge']['processed']} found")
    logger.info(f"Events: {results['events']['events_added']} added")
    logger.info(f"Docs:   {results['docs']['docs_downloaded']} downloaded")
    logger.info("=" * 60)

    return results

def main():
    """Главная функция"""
    init_logging(log_dir="logs", level="DEBUG")

    mode = 'parse'
    submode = None

    if '--mode' in sys.argv:
        idx = sys.argv.index('--mode')
        if idx + 1 < len(sys.argv):
            mode = sys.argv[idx + 1]
        if idx + 2 < len(sys.argv) and not sys.argv[idx + 2].startswith('-'):
            submode = sys.argv[idx + 2]

    logger = get_logger('main')

    try:
        if mode == 'parse':
            asyncio.run(parse_all_regions_from_config())

        elif mode == 'gaps':
            asyncio.run(run_gaps_check())

        elif mode == 'pipeline':
            asyncio.run(run_pipeline())

        elif mode == 'update':
            if submode == 'judge':
                asyncio.run(run_update_judge())
            elif submode == 'case_events':
                asyncio.run(run_update_events())
            elif submode == 'docs':
                asyncio.run(run_update_docs())
            else:
                logger.error("Usage: --mode update [judge|case_events|docs]")
                sys.exit(1)
        else:
            logger.error("Usage: --mode [parse|gaps|pipeline|update]")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(0)

if __name__ == '__main__':
    main()
```

### File 14/26: `parsers\court_parser\parsing\data_extractor.py`
**Module:** `root` | **Lines:** 93

```python
"""
Извлечение данных из HTML элементов
"""
from typing import List, Tuple, Optional
from datetime import date

from database.models import EventData
from utils.text_processor import TextProcessor
from utils.logger import get_logger

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
```

### File 15/26: `parsers\court_parser\parsing\document_parser.py`
**Module:** `root` | **Lines:** 122

```python
"""
Парсинг страниц с документами
"""
from typing import Dict, List, Optional, Tuple
import re
from selectolax.parser import HTMLParser

from database.models import DocumentInfo
from utils.text_processor import TextProcessor
from utils.logger import get_logger
from utils.http_utils import ViewStateExtractor

class DocumentParser:
    """Парсер страниц документов"""

    def __init__(self):
        self.text_processor = TextProcessor()
        self.logger = get_logger('document_parser')

    def extract_case_card_form(self, html: str) -> Optional[Dict[str, str]]:
        """
        Извлечение данных формы для открытия карточки дела из lawsuitList.xhtml
        """
        pattern = r'viewSelectedLawsuit\s*=\s*function\s*\([^)]*\)\s*\{\s*RichFaces\.ajax\s*\(\s*["\']([^"\']+)["\']'
        match = re.search(pattern, html)

        if not match:
            self.logger.warning("viewSelectedLawsuit не найден")
            return None

        ajax_id = match.group(1)
        form_id = ajax_id.rsplit(':', 1)[0]
        viewstate = self._extract_viewstate(html)

        if not viewstate:
            return None

        return {
            'form_id': form_id,
            'ajax_id': ajax_id,
            'viewstate': viewstate
        }

    def extract_document_list(self, html: str) -> Tuple[List[DocumentInfo], Optional[Dict[str, str]]]:
        """
        Извлечение списка документов из documentList.xhtml
        """
        parser = HTMLParser(html)
        documents = []

        rows = parser.css('table tbody tr.hover-effect')

        for row in rows:
            cells = row.css('td')
            if len(cells) < 3:
                continue

            # Дата
            date_text = self.text_processor.clean(cells[0].text())
            doc_date = self.text_processor.parse_date(date_text)
            if not doc_date:
                continue

            # Тип
            doc_type = self.text_processor.clean(cells[1].text())

            # Имя и индекс
            link = cells[2].css_first('a')
            if not link:
                continue

            doc_name = self.text_processor.clean(link.text())
            onclick = link.attributes.get('onclick', '')
            index_match = re.search(r'viewInlineDoc\s*\(\s*(\d+)\s*\)', onclick)

            if not index_match:
                continue

            documents.append(DocumentInfo(
                index=int(index_match.group(1)),
                doc_date=doc_date.date(),
                doc_name=doc_name,
                doc_type=doc_type
            ))

        form_data = self._extract_document_form(html)
        return documents, form_data

    def _extract_document_form(self, html: str) -> Optional[Dict[str, str]]:
        """Извлечение данных формы для открытия документа"""
        pattern = r'viewInlineDoc\s*=\s*function\s*\([^)]*\)\s*\{\s*RichFaces\.ajax\s*\(\s*["\']([^"\']+)["\']'
        match = re.search(pattern, html)

        if not match:
            return None

        ajax_id = match.group(1)
        form_id = ajax_id.rsplit(':', 1)[0]
        viewstate = self._extract_viewstate(html)

        return {
            'form_id': form_id,
            'ajax_id': ajax_id,
            'viewstate': viewstate
        }

    def extract_pdf_url(self, html: str) -> Optional[str]:
        """Извлечение URL документа из document.xhtml"""
        parser = HTMLParser(html)

        embed = parser.css_first('embed[type="application/pdf"]')
        if embed and embed.attributes:
            return embed.attributes.get('src')

        embed = parser.css_first('embed[src*=".pdf"]')
        if embed and embed.attributes:
            return embed.attributes.get('src')

        return None

    def _extract_viewstate(self, html: str) -> Optional[str]:
        return ViewStateExtractor.extract(html)
```

### File 16/26: `parsers\court_parser\parsing\html_parser.py`
**Module:** `root` | **Lines:** 142

```python
"""
Парсинг HTML результатов
"""
import re
from typing import List, Optional
from selectolax.parser import HTMLParser

from database.models import CaseData
from parsing.data_extractor import DataExtractor
from utils.logger import get_logger

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

        Возвращает: список найденных дел с result_index
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
        """Парсинг одной строки таблицы с извлечением result_index"""
        cells = row.css('td')

        if len(cells) < 4:
            return None

        # Извлечение result_index из onclick атрибута строки
        result_index = self._extract_result_index(row)

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
            events=events,
            result_index=result_index
        )

    def _extract_result_index(self, row) -> Optional[int]:
        """
        Извлечение индекса из onclick атрибута строки

        Пример: onclick="viewSelectedLawsuit(1);" → 1
        """
        if not row.attributes:
            return None

        onclick = row.attributes.get('onclick', '')

        # Паттерн: viewSelectedLawsuit(N)
        match = re.search(r'viewSelectedLawsuit\s*\(\s*(\d+)\s*\)', onclick)

        if match:
            return int(match.group(1))

        return None

    def find_case_index(self, cases: List[CaseData], target_case_number: str) -> Optional[int]:
        """
        Найти result_index для конкретного номера дела

        Args:
            cases: список распарсенных дел
            target_case_number: искомый номер дела

        Returns:
            result_index или None если не найдено
        """
        for case in cases:
            if case.case_number == target_case_number:
                return case.result_index

        return None
```

### File 17/26: `parsers\court_parser\search\document_handler.py`
**Module:** `root` | **Lines:** 290

```python
# parsers/court_parser/search/document_handler.py
"""
Обработка документов судебных дел
"""
from typing import Dict, List, Optional, Set
from pathlib import Path
import asyncio
import re
import aiohttp

from parsing.document_parser import DocumentParser
from database.models import DocumentInfo
from utils.text_processor import TextProcessor
from utils.logger import get_logger
from utils.http_utils import HttpHeaders, AjaxRequestBuilder
import tempfile
import shutil

class DocumentHandler:
    """Обработчик загрузки документов"""

    def __init__(self, base_url: str, storage_dir: str = "./court_documents", regions_config: Dict = None):
        self.base_url = base_url
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.parser = DocumentParser()
        self.text_processor = TextProcessor()
        self.regions_config = regions_config or {}
        self.logger = get_logger('document_handler')

    def _get_case_folder(self, case_number: str) -> Path:
        """
        Определить папку для дела по номеру

        Вход: "7594-25-00-4/5229"
        Выход: documents/almaty/smas/2025/7594-25-00-4_5229/
        """
        # Парсим номер дела
        case_info = self.text_processor.find_region_and_court_by_case_number(
            case_number, self.regions_config
        )

        if case_info:
            region_key = case_info['region_key']
            court_key = case_info['court_key']
            year = case_info['year']
        else:
            # Fallback: извлекаем год из номера дела
            # "7594-25-00-4/5229" → год = "2025"
            match = re.match(r'\d+-(\d{2})-', case_number)
            if match:
                year_short = match.group(1)
                year = f"20{year_short}"
            else:
                year = "unknown"

            region_key = "unknown"
            court_key = "unknown"

        # Безопасное имя папки для дела
        safe_case = case_number.replace('/', '_')

        # Формируем путь: documents/region/court/year/case_number/
        folder = self.storage_dir / region_key / court_key / year / safe_case
        folder.mkdir(parents=True, exist_ok=True)

        return folder

    def _save_file(self, case_number: str, doc_info: DocumentInfo, content: bytes) -> dict:
        """Сохранить файл на диск атомарно"""        
        case_dir = self._get_case_folder(case_number)

        date_prefix = doc_info.doc_date.strftime('%Y-%m-%d')
        safe_name = self._sanitize_filename(doc_info.doc_name)
        # Добавляем doc_info.index в имя файла для предотвращения коллизий на диске
        filename = f"{date_prefix}_{doc_info.index}_{safe_name}.pdf" 

        file_path = case_dir / filename

        # Атомарная запись: temp → rename
        with tempfile.NamedTemporaryFile(dir=case_dir, delete=False, suffix='.tmp') as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)

        shutil.move(str(tmp_path), str(file_path))

        relative_path = file_path.relative_to(self.storage_dir)

        self.logger.info(f"Сохранён: {relative_path} ({len(content)} bytes)")

        return {
            'file_path': str(relative_path),
            'file_size': len(content)
        }

    def _sanitize_filename(self, filename: str) -> str:
        """Очистка имени файла"""
        if filename.lower().endswith('.pdf'):
            filename = filename[:-4]
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'\s+', '_', filename)
        filename = re.sub(r'_+', '_', filename).strip('_')
        return filename[:100] if len(filename) > 100 else filename

    async def open_case_card(self, session: aiohttp.ClientSession,
                             results_html: str, case_index: int = 0) -> bool:
        """Открыть карточку дела из списка результатов"""
        form_data = self.parser.extract_case_card_form(results_html)
        if not form_data:
            return False

        url = f"{self.base_url}/lawsuit/lawsuitList.xhtml"

        data = AjaxRequestBuilder.build(
            form_id=form_data['form_id'],
            ajax_id=form_data['ajax_id'],
            viewstate=form_data['viewstate'],
            extra_params={'param1': str(case_index)}
        )

        headers = HttpHeaders.get_ajax()
        headers['Referer'] = url

        async with session.post(url, data=data, headers=headers) as response:
            return response.status == 200

    async def get_document_list(self, session: aiohttp.ClientSession):
        """Получить список документов"""
        url = f"{self.base_url}/lawsuit/documentList.xhtml"
        headers = HttpHeaders.get_base()
        headers['Referer'] = f"{self.base_url}/lawsuit/lawsuitList.xhtml"

        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                return [], None
            html = await response.text()
            return self.parser.extract_document_list(html)

    async def open_document(self, session: aiohttp.ClientSession,
                            form_data: Dict[str, str], doc_index: int) -> bool:
        """Открыть документ по индексу"""
        url = f"{self.base_url}/lawsuit/documentList.xhtml"

        data = AjaxRequestBuilder.build(
            form_id=form_data['form_id'],
            ajax_id=form_data['ajax_id'],
            viewstate=form_data['viewstate'],
            extra_params={'param1': str(doc_index)}
        )

        headers = HttpHeaders.get_ajax()
        async with session.post(url, data=data, headers=headers) as response:
            return response.status == 200

    async def get_document_page(self, session: aiohttp.ClientSession) -> Optional[str]:
        """Получить страницу с PDF viewer"""
        url = f"{self.base_url}/lawsuit/document.xhtml"
        headers = HttpHeaders.get_base()
        headers['Referer'] = f"{self.base_url}/lawsuit/documentList.xhtml"

        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                return None
            return await response.text()

    async def download_pdf(self, session: aiohttp.ClientSession, pdf_url: str,
                       case_number: str, doc_info: DocumentInfo) -> Optional[dict]:
        """Скачать PDF файл"""
        full_url = f"{self.base_url}{pdf_url}" if pdf_url.startswith('/') else pdf_url
        headers = HttpHeaders.get_base()
        headers['Referer'] = f"{self.base_url}/lawsuit/document.xhtml"

        async with session.get(full_url, headers=headers) as response:
            if response.status != 200:
                return None
            content = await response.read()
            return self._save_file(case_number, doc_info, content)

    async def fetch_all_documents(
        self,
        session: aiohttp.ClientSession,
        results_html: str,
        case_number: str,
        case_index: int = 0,
        existing_keys: Optional[Set[str]] = None,
        delay: float = 1.0
    ) -> Dict:
        """
        Скачать все новые документы для дела.

        Returns:
            {
                'downloaded': [...],        # список скачанных в этой сессии
                'total_on_site': int,       # всего документов на сайте
                'already_had': int,         # уже было в БД (existing_keys)
                'complete': bool,           # ВСЕ ли документы с сайта теперь на месте
                'made_progress': bool       # скачали ли хоть один новый
            }
        """
        existing_keys = existing_keys or set()
        downloaded = []

        result = {
            'downloaded': downloaded,
            'total_on_site': 0,
            'already_had': len(existing_keys),
            'complete': False,
            'made_progress': False,
        }

        # Не удалось открыть карточку — полнота неизвестна (считаем НЕ полным)
        if not await self.open_case_card(session, results_html, case_index):
            self.logger.warning(f"Не удалось открыть карточку дела {case_number}")
            return result

        await asyncio.sleep(delay)

        documents, form_data = await self.get_document_list(session)

        # Список документов получить не удалось — полнота неизвестна
        if documents is None:
            return result

        result['total_on_site'] = len(documents)

        # Если на сайте документов нет — дело "полное" (скачивать нечего)
        if not documents:
            result['complete'] = True
            return result

        if not form_data:
            self.logger.warning(f"Нет form_data для документов {case_number}")
            return result

        new_docs = [d for d in documents if d.unique_key not in existing_keys]

        # Все документы уже есть в БД → полное
        if not new_docs:
            result['complete'] = True
            self.logger.info(f"Все документы уже скачаны для {case_number}")
            return result

        self.logger.info(f"Новых документов: {len(new_docs)} для {case_number}")

        # Ключи, которые удалось закрыть (скачать) в этой сессии
        downloaded_keys = set()

        for doc in new_docs:
            try:
                if not await self.open_document(session, form_data, doc.index):
                    continue
                await asyncio.sleep(delay)

                doc_html = await self.get_document_page(session)
                if not doc_html:
                    continue

                pdf_url = self.parser.extract_pdf_url(doc_html)
                if not pdf_url:
                    continue

                file_info = await self.download_pdf(session, pdf_url, case_number, doc)
                if file_info:
                    downloaded.append({
                        'index': doc.index, # Передаем индекс для сохранения в базу данных
                        'doc_date': doc.doc_date,
                        'doc_name': doc.doc_name,
                        'file_path': file_info['file_path'],
                        'file_size': file_info['file_size']
                    })
                    downloaded_keys.add(doc.unique_key)

                await asyncio.sleep(delay)
            except Exception as e:
                self.logger.error(f"Ошибка скачивания {doc.doc_name} ({case_number}): {e}")

        result['made_progress'] = len(downloaded) > 0

        # Полнота: все документы с сайта теперь есть (старые existing + скачанные сейчас)
        covered = existing_keys | downloaded_keys
        all_site_keys = {d.unique_key for d in documents}
        result['complete'] = all_site_keys.issubset(covered)

        return result

    def _get_headers(self) -> Dict[str, str]:
        return HttpHeaders.get_base()

    def _get_ajax_headers(self) -> Dict[str, str]:
        return HttpHeaders.get_ajax()
```

### File 18/26: `parsers\court_parser\search\form_handler.py`
**Module:** `root` | **Lines:** 214

```python
"""
Работа с поисковой формой
"""
from typing import Dict, Optional, Any
import asyncio
import re
import aiohttp
from selectolax.parser import HTMLParser

from utils.logger import get_logger
from utils.retry import NonRetriableError
from utils.http_utils import HttpHeaders, ViewStateExtractor

class FormHandler:
    """Обработчик поисковой формы с кешированием ID"""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = get_logger('form_handler')

        # Кеш ID формы (извлекается один раз за сессию)
        self._cached_form_ids: Optional[Dict[str, str]] = None
        self._cache_initialized: bool = False
        self._cache_lock: asyncio.Lock = asyncio.Lock()

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

                # Form IDs — извлекаем только один раз (с блокировкой)
                async with self._cache_lock:
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

    def reset_cache(self):
        """
        Сброс кеша ID формы

        Вызывать при:
        - Переавторизации
        - Ошибках, связанных с невалидными ID
        """
        # Примечание: этот метод синхронный, но безопасен
        # т.к. просто сбрасывает флаги (атомарные операции в Python)
        self._cached_form_ids = None
        self._cache_initialized = False
        self.logger.debug("Кеш ID формы сброшен")

    async def select_region(self, session: aiohttp.ClientSession, 
                       viewstate: str, region_config: Dict[str, Any], 
                       form_ids: Dict[str, str]):
        """
        Выбор региона в форме

        Args:
            region_config: конфигурация региона (содержит id и search_region_id)
        """
        url = f"{self.base_url}/form/lawsuit/index.xhtml"
        form_base = form_ids.get('form_base', 'j_idt45:j_idt46')

        # Используем search_region_id если есть, иначе id
        # Это нужно для республиканских судов, которые ищутся через регион Астана
        region_id = region_config.get('search_region_id', region_config['id'])

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
        return ViewStateExtractor.extract(html)

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
import re

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
        return HttpHeaders.get_base()

    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        return HttpHeaders.get_ajax()
```

### File 19/26: `parsers\court_parser\search\search_engine.py`
**Module:** `root` | **Lines:** 148

```python
"""
Поисковый движок
"""
from typing import Dict, Union
import asyncio
import aiohttp

from utils.logger import get_logger
from utils.retry import NonRetriableError
from utils.http_utils import HttpHeaders

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
        sequence_number: Union[int, str],
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
        sequence_number: Union[int, str],
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
        return HttpHeaders.get_base()

    def _get_ajax_headers(self) -> Dict[str, str]:
        """AJAX заголовки"""
        return HttpHeaders.get_ajax()
```

### File 20/26: `parsers\court_parser\utils\constants.py`
**Module:** `root` | **Lines:** 23

```python
"""
Константы приложения
"""

class PartyRole:
    """Роли сторон в деле"""
    PLAINTIFF = 'plaintiff'
    DEFENDANT = 'defendant'

class CaseStatus:
    """Статусы результата операций с делом"""
    SAVED = 'saved'
    UPDATED = 'updated'
    ERROR = 'error'
    NO_RESULTS = 'no_results'
    TARGET_NOT_FOUND = 'target_not_found'
    REGION_NOT_FOUND = 'region_not_found'
    SAVE_FAILED = 'save_failed'

class HttpStatus:
    """HTTP статусы для обработки"""
    RETRIABLE = frozenset({500, 502, 503, 504})
    NON_RETRIABLE = frozenset({400, 401, 403, 404})
```

### File 21/26: `parsers\court_parser\utils\http_utils.py`
**Module:** `root` | **Lines:** 81

```python
# parsers/court_parser/utils/http_utils.py
"""
Общие HTTP утилиты
"""

from typing import Dict, Optional
from selectolax.parser import HTMLParser

class HttpHeaders:
    """Фабрика HTTP заголовков"""

    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

    @staticmethod
    def get_base() -> Dict[str, str]:
        """Базовые HTTP заголовки"""
        return {
            'User-Agent': HttpHeaders.USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        }

    @staticmethod
    def get_ajax() -> Dict[str, str]:
        """AJAX заголовки для RichFaces"""
        headers = HttpHeaders.get_base()
        headers.update({
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Faces-Request': 'partial/ajax',
            'X-Requested-With': 'XMLHttpRequest',  # ✅ Исправлено!
        })
        return headers

class ViewStateExtractor:
    """Извлечение ViewState из HTML"""

    @staticmethod
    def extract(html: str) -> Optional[str]:
        """Извлечение ViewState из HTML"""
        parser = HTMLParser(html)
        viewstate_input = parser.css_first('input[name="javax.faces.ViewState"]')

        if viewstate_input and viewstate_input.attributes:
            return viewstate_input.attributes.get('value')
        return None

class AjaxRequestBuilder:
    """Построитель данных для AJAX запросов RichFaces"""

    @staticmethod
    def build(
        form_id: str,
        ajax_id: str,
        viewstate: str,
        extra_params: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """
        Построить данные для partial/ajax запроса
        """
        data = {
            form_id: form_id,
            'javax.faces.ViewState': viewstate,
            'javax.faces.source': ajax_id,
            'javax.faces.partial.execute': f'{ajax_id} @component',
            'javax.faces.partial.render': '@component',
            'org.richfaces.ajax.component': ajax_id,
            ajax_id: ajax_id,
            'rfExt': 'null',
            'AJAX:EVENTS_COUNT': '1',
            'javax.faces.partial.ajax': 'true'
        }

        if extra_params:
            data.update(extra_params)

        return data
```

### File 22/26: `parsers\court_parser\utils\logger.py`
**Module:** `root` | **Lines:** 257

```python
# parsers/court_parser/utils/logger.py

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
        time_str = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
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
        except Exception:
            pass

def get_log_filename(prefix: str = "parser") -> str:
    """Получить имя лог-файла с датой и временем"""
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return f"{prefix}_{timestamp}.log"

# Глобальные переменные для отслеживания состояния
_current_log_file: Optional[str] = None
_logging_initialized: bool = False

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

    Path(log_dir).mkdir(exist_ok=True)
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

    # Консольный handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level))
        console_handler.setFormatter(ColoredConsoleFormatter())

        class UIFilter(logging.Filter):
            def filter(self, record):
                try:
                    ui = _get_ui()
                    # Пропускаем логи если:
                    # - UI не существует
                    # - UI не запущен
                    # - UI завершён (проверяем по наличию атрибута и значению)
                    if ui is None:
                        return True
                    if not hasattr(ui, '_running'):
                        return True
                    return not ui._running
                except Exception:
                    return True  # При любой ошибке — пропускаем лог

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
    global _current_log_file

    logger = logging.getLogger('report')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    Path(log_dir).mkdir(exist_ok=True)

    # Используем тот же файл что и основной логгер
    if _current_log_file is None:
        _current_log_file = get_log_filename("parser")

    log_file_path = Path(log_dir) / _current_log_file

    file_handler = logging.FileHandler(
        log_file_path,
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
    """Инициализация всех логгеров - вызывается ОДИН раз за сессию"""
    global _current_log_file, _logging_initialized

    # Предотвращаем повторную инициализацию
    if _logging_initialized:
        return

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

    _logging_initialized = True

def reset_logging():
    """Сброс состояния логирования (для тестов или перезапуска)"""
    global _current_log_file, _logging_initialized
    _current_log_file = None
    _logging_initialized = False

def set_progress_mode(active: bool):
    """Устаревшая функция — для совместимости"""
    pass
```

### File 23/26: `parsers\court_parser\utils\retry.py`
**Module:** `root` | **Lines:** 280

```python
"""
Гибкая система retry с поддержкой различных стратегий
"""

import asyncio
import random
from typing import Callable, Any, Optional, List, Type
from functools import wraps
from datetime import datetime, timedelta

import aiohttp

from utils.logger import get_logger

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
```

### File 24/26: `parsers\court_parser\utils\terminal_ui.py`
**Module:** `root` | **Lines:** 699

```python
"""
Единый модуль терминального UI

Формат: одна строка = один регион
"""
import sys
import threading
import asyncio
import shutil
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

class Mode(Enum):
    """Режимы работы"""
    PARSE = "PARSING"
    JUDGE = "JUDGE UPDATE"
    EVENTS = "EVENTS UPDATE"
    DOCS = "DOCS UPDATE"

class RegionStatus(Enum):
    """Статусы регионов"""
    WAIT = "wait"
    ACTIVE = "active"
    DONE = "done"
    ERROR = "error"

class CourtStatus(Enum):
    """Статусы судов"""
    WAIT = "wait"
    ACTIVE = "active"
    DONE = "done"
    ERROR = "error"

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

    # Символы
    CHECK = '✓'
    CROSS = '✗'
    WARN = '⚠'
    DOT = '●'

    _enabled = True

    @classmethod
    def disable(cls):
        for attr in ['RESET', 'BOLD', 'DIM', 'GREEN', 'BRIGHT_GREEN', 
                     'YELLOW', 'RED', 'CYAN', 'GRAY', 'WHITE']:
            setattr(cls, attr, '')
        cls.CHECK = '[v]'
        cls.CROSS = '[x]'
        cls.WARN = '[!]'
        cls.DOT = '[*]'
        cls._enabled = False

    @classmethod
    def strip(cls, text: str) -> str:
import re
        return re.compile(r'\033\[[0-9;]*m').sub('', text)

@dataclass
class CourtStats:
    """Статистика суда"""
    key: str
    status: CourtStatus = CourtStatus.WAIT
    saved: int = 0
    queries: int = 0

@dataclass
class RegionStats:
    """Статистика региона"""
    key: str
    name: str
    status: RegionStatus = RegionStatus.WAIT
    courts: Dict[str, CourtStats] = field(default_factory=dict)
    current_court: str = ""

    # Счётчики
    total_saved: int = 0
    total_queries: int = 0
    real_errors: int = 0  # Только реальные ошибки (сеть, авторизация)

    # Для update режимов
    processed: int = 0
    total_cases: int = 0
    judges_found: int = 0
    events_added: int = 0
    docs_downloaded: int = 0

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    line_number: int = -1  # Номер строки в консоли

    def elapsed_str(self) -> str:
        if not self.start_time:
            return ""
        end = self.end_time or datetime.now()
        seconds = int((end - self.start_time).total_seconds())
        minutes, secs = divmod(seconds, 60)
        return f"{minutes}:{secs:02d}"

@dataclass 
class GlobalStats:
    """Глобальная статистика"""
    mode: Mode = Mode.PARSE
    start_time: datetime = field(default_factory=datetime.now)

    total_saved: int = 0
    total_queries: int = 0
    total_real_errors: int = 0  # Только реальные ошибки

    # Для update
    total_processed: int = 0
    total_found: int = 0

    regions_active: int = 0
    regions_done: int = 0
    regions_total: int = 0

    def elapsed_str(self) -> str:
        seconds = int((datetime.now() - self.start_time).total_seconds())
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

class TerminalUI:
    """
    Терминальный UI с форматом: одна строка = один регион
    """

    # Ширины колонок
    COL_TIME = 10
    COL_REGION = 14
    COL_COURT = 22
    COL_DURATION = 6

    def __init__(self, mode: Mode = Mode.PARSE, court_types: List[str] = None):
        self.mode = mode
        self.court_types = court_types or ['smas', 'appellate']
        self.stats = GlobalStats(mode=mode)
        self.regions: Dict[str, RegionStats] = {}

        self._lock = threading.Lock()
        self._is_tty = sys.stdout.isatty()
        self._terminal_width = self._get_terminal_width()
        self._running = False
        self._total_lines = 0  # Количество строк регионов
        self._header_lines = 4  # Количество строк заголовка

        if not self._is_tty:
            Colors.disable()
        elif sys.platform == 'win32':
            self._enable_windows_ansi()

    def _print_line(self, text: str) -> None:
        """Выводит строку в stdout с автоматическим flush"""
import sys
        print(text)
        sys.stdout.flush()

    def _get_terminal_width(self) -> int:
        try:
            return shutil.get_terminal_size().columns
        except:
            return 100

    def _enable_windows_ansi(self):
        try:
import os
            os.system('')
        except:
            Colors.disable()

    def add_region(self, key: str, name: str, court_keys: List[str] = None):
        """
        Добавить регион

        Args:
            key: ключ региона
            name: название региона
            court_keys: список ключей судов для этого региона (если None — используются дефолтные)
        """
        with self._lock:
            # Используем переданные court_keys или дефолтные
            region_court_types = court_keys if court_keys else self.court_types
            courts = {ct: CourtStats(key=ct) for ct in region_court_types}

            self.regions[key] = RegionStats(
                key=key, 
                name=self._short_name(name),
                courts=courts
            )
            self.stats.regions_total += 1

    def _short_name(self, name: str) -> str:
        """Сокращение названий регионов"""
        replacements = {
            'Республиканские суды': 'Republic',
            'город ': '', 'область': '', 'Область ': '',
            'Акмолинская': 'Akmola', 'Актюбинская': 'Aktobe',
            'Алматинская': 'Almaty rgn', 'Атырауская': 'Atyrau',
            'Восточно-Казахстанская': 'VKO', 'Жамбылская': 'Zhambyl',
            'Западно-Казахстанская': 'ZKO', 'Карагандинская': 'Karaganda',
            'Костанайская': 'Kostanay', 'Кызылординская': 'Kyzylorda',
            'Мангистауская': 'Mangystau', 'Павлодарская': 'Pavlodar',
            'Северо-Казахстанская': 'SKO', 'Туркестанская': 'Turkestan',
            'Ұлытау': 'Ulytau', 'Абай': 'Abay', 'Жетісу': 'Zhetysu',
            'Астана': 'Astana', 'Алматы': 'Almaty', 'Шымкент': 'Shymkent',
        }
        for old, new in replacements.items():
            name = name.replace(old, new)
        return name.strip()[:12]

    # =========================================================================
    # РЕНДЕРИНГ СТРОКИ РЕГИОНА
    # =========================================================================

    def _render_region_line(self, region: RegionStats) -> str:
        """Рендеринг одной строки региона"""
        C = Colors

        # Время
        time_str = datetime.now().strftime('%H:%M:%S')
        time_col = f"{C.DIM}[{time_str}]{C.RESET}"

        # Название региона
        region_col = f"{C.CYAN}{region.name:<{self.COL_REGION}}{C.RESET}"

        # Суды — только те, которые есть у региона
        court_parts = []

        for court_key, court in region.courts.items():
            if court.status == CourtStatus.WAIT:
                court_str = f"{court_key} ..."
                color = C.GRAY
            elif court.status == CourtStatus.ACTIVE:
                court_str = f"{court_key} {C.DOT}"
                color = C.YELLOW
            elif court.status == CourtStatus.DONE:
                court_str = f"{court_key} {C.CHECK} {court.saved}"
                color = C.GREEN
            elif court.status == CourtStatus.ERROR:
                court_str = f"{court_key} {C.CROSS}"
                color = C.RED
            else:
                court_str = f"{court_key} ..."
                color = C.GRAY

            # Фиксированная ширина колонки
            plain_str = Colors.strip(court_str)
            padding = self.COL_COURT - len(plain_str)
            if padding < 0:
                padding = 0
            court_parts.append(f"{color}{court_str}{C.RESET}{' ' * padding}")

        courts_col = f"{C.DIM}│{C.RESET} ".join(court_parts)

        # Время выполнения региона
        duration_col = ""
        if region.status in [RegionStatus.DONE, RegionStatus.ERROR]:
            duration_col = f" {C.DIM}│{C.RESET} {region.elapsed_str()}"

        return f"{time_col} {region_col}{C.DIM}│{C.RESET} {courts_col}{duration_col}"

    def _render_update_region_line(self, region: RegionStats) -> str:
        """Рендеринг строки для update режимов"""
        C = Colors

        time_str = datetime.now().strftime('%H:%M:%S')
        time_col = f"{C.DIM}[{time_str}]{C.RESET}"

        region_col = f"{C.CYAN}{region.name:<{self.COL_REGION}}{C.RESET}"

        # Прогресс-бар
        if region.total_cases > 0:
            pct = region.processed / region.total_cases
            filled = int(pct * 10)
            bar = '█' * filled + '░' * (10 - filled)
            progress = f"{bar} {region.processed}/{region.total_cases}"
        elif region.status == RegionStatus.ACTIVE:
            progress = f"{C.DOT}"
        elif region.status == RegionStatus.WAIT:
            progress = "waiting..."
        else:
            progress = ""

        progress_col = f"{progress:<24}"

        # Результат по режиму
        if self.mode == Mode.JUDGE:
            result_col = f"found: {region.judges_found}"
        elif self.mode == Mode.EVENTS:
            result_col = f"events: {region.events_added}"
        elif self.mode == Mode.DOCS:
            result_col = f"docs: {region.docs_downloaded}"
        else:
            result_col = ""

        result_col = f"{result_col:<16}"

        # Время
        duration = region.elapsed_str() if region.status != RegionStatus.WAIT else ""

        return f"{time_col} {region_col}{C.DIM}│{C.RESET} {progress_col}{C.DIM}│{C.RESET} {result_col}{C.DIM}│{C.RESET} {duration}"

    # =========================================================================
    # УПРАВЛЕНИЕ ВЫВОДОМ
    # =========================================================================

    def _move_to_region_line(self, line_number: int):
        """Переместить курсор к строке региона"""
        if not self._is_tty:
            return

        # Вычисляем сколько строк вверх от текущей позиции
        current_line = self._header_lines + self._total_lines
        lines_up = current_line - line_number

        if lines_up > 0:
            sys.stdout.write(f'\033[{lines_up}A')  # Вверх
            sys.stdout.write('\033[2K')  # Очистить строку
            sys.stdout.write('\r')  # В начало

    def _restore_cursor(self):
        """Вернуть курсор в конец"""
        if not self._is_tty:
            return
        sys.stdout.write(f'\033[{self._header_lines + self._total_lines}H')

    def _update_region_display(self, region_key: str):
        """Обновить отображение региона"""
        if not self._running or not self._is_tty:
            return

        region = self.regions.get(region_key)
        if not region or region.line_number < 0:
            return

        with self._lock:
            # Сохраняем позицию курсора
            sys.stdout.write('\033[s')

            # Перемещаемся к строке региона
            self._move_to_region_line(region.line_number)

            # Рендерим строку
            if self.mode == Mode.PARSE:
                line = self._render_region_line(region)
            else:
                line = self._render_update_region_line(region)

            sys.stdout.write(line)

            # Восстанавливаем позицию курсора
            sys.stdout.write('\033[u')
            sys.stdout.flush()

    def _add_region_line(self, region_key: str):
        """Добавить новую строку региона"""
        region = self.regions.get(region_key)
        if not region:
            return

        with self._lock:
            region.line_number = self._header_lines + self._total_lines
            self._total_lines += 1

            if self.mode == Mode.PARSE:
                line = self._render_region_line(region)
            else:
                line = self._render_update_region_line(region)

            print(line)

    # =========================================================================
    # ПУБЛИЧНЫЙ API
    # =========================================================================

    def region_start(self, region_key: str):
        """Регион начал обработку"""
        region = self.regions.get(region_key)
        if not region:
            return

        region.status = RegionStatus.ACTIVE
        region.start_time = datetime.now()
        self.stats.regions_active += 1

        if region.line_number < 0:
            self._add_region_line(region_key)
        else:
            self._update_region_display(region_key)

    def court_start(self, region_key: str, court_key: str):
        """Суд начал обработку"""
        region = self.regions.get(region_key)
        if not region:
            return

        court = region.courts.get(court_key)
        if court:
            court.status = CourtStatus.ACTIVE

        region.current_court = court_key
        self._update_region_display(region_key)

    def court_done(self, region_key: str, court_key: str, saved: int = 0):
        """Суд завершил обработку"""
        region = self.regions.get(region_key)
        if not region:
            return

        court = region.courts.get(court_key)
        if court:
            court.status = CourtStatus.DONE
            court.saved = saved

        region.total_saved += saved
        self.stats.total_saved += saved

        self._update_region_display(region_key)

    def court_error(self, region_key: str, court_key: str, error: str = ""):
        """Ошибка в суде"""
        region = self.regions.get(region_key)
        if not region:
            return

        court = region.courts.get(court_key)
        if court:
            court.status = CourtStatus.ERROR

        region.real_errors += 1
        self.stats.total_real_errors += 1

        self._update_region_display(region_key)

    def region_done(self, region_key: str):
        """Регион завершил обработку"""
        region = self.regions.get(region_key)
        if not region:
            return

        region.status = RegionStatus.DONE
        region.end_time = datetime.now()
        self.stats.regions_active -= 1
        self.stats.regions_done += 1

        self._update_region_display(region_key)

    def region_error(self, region_key: str, error: str = ""):
        """Ошибка в регионе"""
        region = self.regions.get(region_key)
        if not region:
            return

        region.status = RegionStatus.ERROR
        region.end_time = datetime.now()
        region.real_errors += 1
        self.stats.regions_active -= 1
        self.stats.total_real_errors += 1

        self._update_region_display(region_key)

    def increment_saved(self, region_key: str, court_key: str, count: int = 1):
        """Увеличить счётчик сохранённых"""
        region = self.regions.get(region_key)
        if not region:
            return

        court = region.courts.get(court_key)
        if court:
            court.saved += count

        # ★ ОБНОВЛЯЕМ ДИСПЛЕЙ
        self._update_region_display(region_key)

    def increment_queries(self, region_key: str, count: int = 1):
        """Увеличить счётчик запросов"""
        region = self.regions.get(region_key)
        if region:
            region.total_queries += count
        self.stats.total_queries += count

    def update_progress(self, region_key: str, processed: int = None, 
                    found: int = None, events: int = None, docs: int = None):
        """Обновить прогресс update режима"""
        region = self.regions.get(region_key)
        if not region:
            return

        if processed is not None:
            region.processed = processed
            self.stats.total_processed = sum(r.processed for r in self.regions.values())

        if found is not None:
            region.judges_found = found
            self.stats.total_found = sum(r.judges_found for r in self.regions.values())

        if events is not None:
            region.events_added = events
            self.stats.total_found = sum(r.events_added for r in self.regions.values())

        if docs is not None:
            region.docs_downloaded = docs
            self.stats.total_found = sum(r.docs_downloaded for r in self.regions.values())

        # ★ ОБНОВЛЯЕМ ДИСПЛЕЙ
        self._update_region_display(region_key)

    # =========================================================================
    # ЖИЗНЕННЫЙ ЦИКЛ
    # =========================================================================

    async def start(self):
        """Запустить UI"""
        self._running = True

        C = Colors
        width = min(self._terminal_width, 100)

        print()
        print(f"{C.DIM}{'═' * width}{C.RESET}")
        print(f" {C.BRIGHT_GREEN}COURT PARSER v2.2{C.RESET} │ Mode: {self.mode.value}")
        print(f"{C.DIM}{'═' * width}{C.RESET}")
        print()

    async def finish(self):
        """Завершить UI"""
        self._running = False

        C = Colors
        width = min(self._terminal_width, 100)

        # Пересчитываем статистику
        self.stats.total_saved = sum(
            sum(c.saved for c in r.courts.values()) 
            for r in self.regions.values()
        )
        self.stats.total_real_errors = sum(r.real_errors for r in self.regions.values())

        errors = self.stats.total_real_errors

        print()
        print(f"{C.DIM}{'─' * width}{C.RESET}")

        if errors == 0:
            status = f"{C.GREEN}{Colors.CHECK} COMPLETE{C.RESET}"
        else:
            status = f"{C.YELLOW}{Colors.WARN} COMPLETE WITH ERRORS{C.RESET}"

        done = self.stats.regions_done
        total = self.stats.regions_total
        saved = self.stats.total_saved
        queries = self.stats.total_queries
        elapsed = self.stats.elapsed_str()

        print(f" {status} │ {done}/{total} regions │ {saved} saved │ {queries} queries │ {elapsed}")
        print(f"{C.DIM}{'─' * width}{C.RESET}")
        print()

    def print_final_report(self, extra_data: Dict = None):
        """Вывести финальный отчёт"""
        C = Colors
        width = min(self._terminal_width, 80)

        print(f"\n{C.DIM}╔{'═' * (width - 2)}╗{C.RESET}")

        title = f"{self.mode.value} COMPLETE"
        print(f"{C.DIM}║{C.RESET}{title:^{width - 2}}{C.DIM}║{C.RESET}")

        start = self.stats.start_time.strftime('%Y-%m-%d %H:%M')
        end = datetime.now().strftime('%H:%M')
        time_line = f"{start} → {end}"
        print(f"{C.DIM}║{C.RESET}{time_line:^{width - 2}}{C.DIM}║{C.RESET}")
        print(f"{C.DIM}╠{'═' * (width - 2)}╣{C.RESET}")

        # Статистика
        duration = self.stats.elapsed_str()
        total_saved = self.stats.total_saved
        total_queries = self.stats.total_queries
        regions_done = self.stats.regions_done
        regions_total = self.stats.regions_total
        real_errors = self.stats.total_real_errors

        elapsed_sec = (datetime.now() - self.stats.start_time).total_seconds()
        speed = (total_saved / elapsed_sec * 60) if elapsed_sec > 0 and total_saved > 0 else 0

        stats_lines = [
            ("Duration", duration),
            ("Total saved", f"{total_saved:,} cases"),
            ("Total queries", f"{total_queries:,}"),
            ("Regions", f"{regions_done}/{regions_total} completed"),
        ]

        if real_errors > 0:
            stats_lines.append(("Errors", f"{C.RED}{real_errors}{C.RESET}"))

        stats_lines.append(("Avg speed", f"{speed:.1f} cases/min"))

        print(f"{C.DIM}║{C.RESET}{' ' * (width - 2)}{C.DIM}║{C.RESET}")
        for label, value in stats_lines:
            line = f"   {label:<20} {value}"
            clean_len = len(Colors.strip(line))
            padding = width - 2 - clean_len
            print(f"{C.DIM}║{C.RESET}{line}{' ' * padding}{C.DIM}║{C.RESET}")
        print(f"{C.DIM}║{C.RESET}{' ' * (width - 2)}{C.DIM}║{C.RESET}")

        # Проблемы
        issues = []
        if extra_data:
            no_judge = extra_data.get('no_judge', 0)
            no_parties = extra_data.get('no_parties', 0)
            if no_judge > 0 and total_saved > 0:
                pct = no_judge / total_saved * 100
                issues.append(f"{C.YELLOW}•{C.RESET} {no_judge} cases without judge ({pct:.1f}%)")
            if no_parties > 0 and total_saved > 0:
                pct = no_parties / total_saved * 100
                issues.append(f"{C.YELLOW}•{C.RESET} {no_parties} cases without parties ({pct:.1f}%)")

        if issues:
            print(f"{C.DIM}╠{'═' * (width - 2)}╣{C.RESET}")
            print(f"{C.DIM}║{C.RESET}   {C.YELLOW}ATTENTION{C.RESET}{' ' * (width - 14)}{C.DIM}║{C.RESET}")
            for issue in issues:
                clean_len = len(Colors.strip(issue)) + 3
                padding = width - 2 - clean_len
                print(f"{C.DIM}║{C.RESET}   {issue}{' ' * padding}{C.DIM}║{C.RESET}")

        print(f"{C.DIM}╚{'═' * (width - 2)}╝{C.RESET}\n")

# =============================================================================
# ГЛОБАЛЬНЫЙ ИНСТАНС
# =============================================================================

_ui_instance: Optional[TerminalUI] = None

def get_ui() -> Optional[TerminalUI]:
    return _ui_instance

def init_ui(mode: Mode = Mode.PARSE, regions: Dict[str, str] = None,
            court_types: List[str] = None,
            region_courts: Dict[str, List[str]] = None) -> TerminalUI:
    """
    Инициализировать UI

    Args:
        mode: режим работы
        regions: {region_key: region_name}
        court_types: дефолтные типы судов ['smas', 'appellate']
        region_courts: {region_key: [court_keys]} — суды для каждого региона
    """
    global _ui_instance
    _ui_instance = TerminalUI(mode, court_types)

    if regions:
        for key, name in regions.items():
            # Получаем суды для конкретного региона или дефолтные
            courts = region_courts.get(key) if region_courts else None
            _ui_instance.add_region(key, name, courts)

    return _ui_instance

def _render_region_line_plain(self, region: RegionStats) -> str:
    """Рендеринг строки БЕЗ цветов (для логов)"""
    time_str = datetime.now().strftime('%H:%M:%S')

    # Суды
    court_parts = []
    for court_key, court in region.courts.items():
        if court.status == CourtStatus.WAIT:
            court_str = f"{court_key}: waiting"
        elif court.status == CourtStatus.ACTIVE:
            court_str = f"{court_key}: active"
        elif court.status == CourtStatus.DONE:
            court_str = f"{court_key}: done ({court.saved} saved)"
        elif court.status == CourtStatus.ERROR:
            court_str = f"{court_key}: error"
        else:
            court_str = f"{court_key}: unknown"
        court_parts.append(court_str)

    courts_str = " | ".join(court_parts)

    return f"[{time_str}] {region.name:<14} | {courts_str}"
```

### File 25/26: `parsers\court_parser\utils\text_processor.py`
**Module:** `root` | **Lines:** 287

```python
"""
Обработка и очистка текста
"""
import re
from datetime import datetime  # ← оставляем на уровне модуля
from typing import List, Optional, Dict

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
            current_year = datetime.now().year
            if not (2000 <= parsed.year <= current_year + 2):
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
import re

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

        Пример: "6294-25-00-4/215" или "6003-25-00-4к/1454(2)" →
        {
            'court_code': '6294',
            'year': '25',
            'middle': '00',
            'case_type': '4',
            'sequence': '215' или '1454(2)'
        }
        """
        pattern = r'^(\d+)-(\d+)-(\d+)-([0-9а-яА-Я]+)/(\d+(?:\(\d+\))?)$'
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

        Вход: "6294-25-00-4/215" или "6003-25-00-4к/1454(2)"
        Выход: {
            'court_code': '6294',
            'year_short': '25',
            'case_type': '4',
            'sequence': '215' или '1454(2)'
        }
        """
        pattern = r'^(\d+)-(\d+)-(\d+)-([0-9а-яА-Я]+)/(\d+(?:\(\d+\))?)$'
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

    @staticmethod
    def is_matching_case_number(case_number: str, target: str) -> bool:
        """
        Проверка соответствия номера дела целевому

        Правила:
        - Точное совпадение: "991" == "991" ✓
        - С суффиксом: "991(1)" matches "991" ✓
        - С суффиксом: "991(2)" matches "991" ✓
        - Другой номер: "992" != "991" ✗
        - Не суффикс: "9910" != "991" ✗

        Args:
            case_number: номер дела из ответа сервера
            target: целевой номер дела (без суффикса)

        Returns:
            True если номер соответствует целевому

        Examples:
            >>> is_matching_case_number("6003-25-00-4к/991", "6003-25-00-4к/991")
            True
            >>> is_matching_case_number("6003-25-00-4к/991(1)", "6003-25-00-4к/991")
            True
            >>> is_matching_case_number("6003-25-00-4к/992", "6003-25-00-4к/991")
            False
        """
        # Точное совпадение
        if case_number == target:
            return True

        # Проверка суффикса вида (1), (2), (10) и т.д.
        # Паттерн: целевой номер + скобки с числом
        pattern = f"^{re.escape(target)}\\(\\d+\\)$"
        return bool(re.match(pattern, case_number))

    @staticmethod
    def extract_base_case_number(case_number: str) -> str:
        """
        Извлечь базовый номер дела без суффикса

        Args:
            case_number: номер дела (возможно с суффиксом)

        Returns:
            Базовый номер без суффикса

        Examples:
            >>> extract_base_case_number("6003-25-00-4к/991(1)")
            "6003-25-00-4к/991"
            >>> extract_base_case_number("6003-25-00-4к/991")
            "6003-25-00-4к/991"
        """
        # Удаляем суффикс (N) в конце
        match = re.match(r'^(.+?)(\(\d+\))?$', case_number)
        if match:
            return match.group(1)
        return case_number
```

### File 26/26: `parsers\court_parser\utils\validators.py`
**Module:** `root` | **Lines:** 85

```python
"""
Валидация данных
"""
from typing import Dict, Any, Optional
from datetime import datetime

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
                current_year = datetime.now().year
                if not (1990 <= date.year <= current_year + 2):
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
```

---

## 📋 Summary

- **Total Files:** 26
- **Resource Files:** 1
- **Total Lines:** 7,284
- **Compression Efficiency:** -95.3% token reduction
- **Build Hash:** `a9309e259fc2`
- **Optimization:** Smart code compression without information loss

---

## 🤖 For AI Analysis

This document is optimized for:
- ✅ ChatGPT-4 / GPT-4o
- ✅ Claude 3 (Opus/Sonnet)
- ✅ Google Gemini
- ✅ Other LLMs

**Best practices:**
1. Upload entire file at once
2. Ask questions about specific modules or files
3. Request code reviews or architecture analysis
4. Request refactoring suggestions
5. Reference configuration in config.json for context

---

*Generated with AI-optimized code collector*
