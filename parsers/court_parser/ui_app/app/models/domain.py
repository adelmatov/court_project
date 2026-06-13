"""Доменные модели и справочники приложения."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ProcessState(str, Enum):
    """Состояние процесса парсера."""
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FINISHED = "finished"
    ERROR = "error"


class LogLevel(str, Enum):
    """Уровень строки лога (для цветовой подсветки)."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"
    EVENT = "EVENT"


# Режимы запуска парсера.
# Ключ   = строка для передачи в командную строку (после --mode).
# Значение = человекочитаемое название для отображения в UI.
#
# ВАЖНО: "update judge" / "update case_events" / "update docs" —
# это два аргумента, которые сервис разобьёт по пробелу:
#   python main.py --mode update judge
PARSER_MODES: dict[str, str] = {
    "parse": "Parse — парсинг новых дел",
    "gaps": "Gaps — поиск пропусков",
    "pipeline": "Pipeline — полный цикл",
    "update judge": "Update · Судьи",
    "update case_events": "Update · События",
    "update docs": "Update · Документы",
}


@dataclass(frozen=True)
class Case:
    """Одно судебное дело (для таблицы результатов)."""
    id: int
    case_number: str
    case_date: str | None
    judge: str | None
    documents_count: int
    documents_complete: bool
    updated_at: str | None


@dataclass(frozen=True)
class DashboardStats:
    """Статистика для дашборда."""
    total_cases: int = 0
    total_documents: int = 0
    cases_without_docs: int = 0
    cases_without_judge: int = 0