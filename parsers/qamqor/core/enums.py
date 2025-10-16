"""Перечисления для типизации и избежания magic strings."""

from enum import Enum


class CheckStatus(str, Enum):
    """
    Статусы проверок в системе QAMQOR.
    
    Используется для фильтрации записей при обновлении.
    """
    IN_PROGRESS = "1"
    COMPLETED = "2"
    CANCELLED = "3"
    
    def __str__(self) -> str:
        """Строковое представление."""
        return self.value


class APIResponseCode(str, Enum):
    """
    Коды ответов API QAMQOR.
    
    Определяет тип ответа и необходимость повторных попыток.
    """
    OK = "OK"
    RECAPTCHA = "CONTROLLER::RECAPTCHA"
    MAINTENANCE = "MAINTENANCE"
    SERVER_ERROR = "SERVER_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    
    def __str__(self) -> str:
        """Строковое представление."""
        return self.value
    
    @property
    def is_critical(self) -> bool:
        """Проверка критичности ошибки."""
        return self in {
            self.MAINTENANCE,
            self.SERVER_ERROR,
            self.INTERNAL_ERROR,
            self.SERVICE_UNAVAILABLE
        }


class APIResponseStatus(str, Enum):
    """Статусы HTTP ответов API."""
    SUCCESS = "success"
    ERROR = "error"
    
    def __str__(self) -> str:
        """Строковое представление."""
        return self.value


class ParserMode(str, Enum):
    """Режимы работы парсера."""
    FULL = "full"
    MISSING_ONLY = "missing_only"
    UPDATE = "update"
    
    def __str__(self) -> str:
        """Строковое представление."""
        return self.value


class TableName(str, Enum):
    """Названия таблиц в БД."""
    TAX = "qamqor_tax"
    CUSTOMS = "qamqor_customs"
    AUDIT_LOG = "audit_log"
    
    def __str__(self) -> str:
        """Строковое представление."""
        return self.value