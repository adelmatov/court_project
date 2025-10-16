"""Валидатор ответов от API."""

import logging
from typing import Dict, Optional, Tuple

from .enums import APIResponseCode, APIResponseStatus


class APIValidator:
    """
    Валидатор ответов от API QAMQOR.
    
    Проверяет корректность структуры ответа, коды состояния
    и определяет критичность ошибок.
    """
    
    def __init__(self, logger: logging.Logger):
        """
        Инициализация валидатора.
        
        Args:
            logger: Логгер для вывода сообщений
        """
        self.logger = logger
    
    def validate_response(
        self,
        response_data: Optional[Dict],
        context: str = ""
    ) -> Tuple[bool, Optional[str]]:
        """
        Проверка корректности ответа API.
        
        Args:
            response_data: Данные ответа от API
            context: Контекст для логирования (например, номер записи)
            
        Returns:
            Кортеж (is_valid, error_message):
                - is_valid: True если ответ корректен
                - error_message: Описание ошибки или None
        """
        if not response_data:
            return False, "Пустой ответ"
        
        # Проверка code
        code = response_data.get('code', '').upper()
        
        # ИГНОРИРУЕМ CONTROLLER::RECAPTCHA (ложная защита)
        if code == APIResponseCode.RECAPTCHA.value:
            self.logger.warning(
                "⚠️ %s | API вернул CAPTCHA, игнорируем",
                context
            )
            
            # Проверяем наличие данных
            data_obj = response_data.get('data')
            
            if data_obj is not None and isinstance(data_obj, dict):
                items = data_obj.get('items', [])
                if items and len(items) > 0:
                    # Данные есть - считаем успешным
                    self.logger.info(
                        "✅ %s | Данные получены несмотря на CAPTCHA",
                        context
                    )
                    return True, None
            
            # Данных нет - это пустая запись (не ошибка)
            self.logger.debug(
                "ℹ️ %s | CAPTCHA без данных - пустая запись",
                context
            )
            return True, None
        
        # Проверка на корректный code
        if code != APIResponseCode.OK.value:
            error_msg = f"Неверный code: {code}"
            if context:
                error_msg = f"{context} | {error_msg}"
            return False, error_msg
        
        # Проверка status
        status = response_data.get('status', '').lower()
        if status != APIResponseStatus.SUCCESS.value:
            error_msg = f"Неверный status: {status}"
            if context:
                error_msg = f"{context} | {error_msg}"
            return False, error_msg
        
        # Проверка структуры data
        if 'data' not in response_data:
            return False, f"{context} | Отсутствует поле 'data'"
        
        return True, None
    
    def is_critical_error(self, response_data: Optional[Dict]) -> bool:
        """
        Определяет, является ли ошибка критической.
        
        Критические ошибки требуют остановки парсера:
        - MAINTENANCE (техобслуживание)
        - SERVER_ERROR (серверная ошибка)
        - INTERNAL_ERROR (внутренняя ошибка)
        - SERVICE_UNAVAILABLE (сервис недоступен)
        
        CONTROLLER::RECAPTCHA НЕ является критической ошибкой.
        
        Args:
            response_data: Данные ответа от API
            
        Returns:
            True если ошибка критическая
        """
        if not response_data:
            return False
        
        code = response_data.get('code', '').upper()
        
        # CAPTCHA - НЕ критическая ошибка
        if code == APIResponseCode.RECAPTCHA.value:
            return False
        
        # Используем свойство enum
        try:
            api_code = APIResponseCode(code)
            return api_code.is_critical
        except ValueError:
            # Неизвестный код - не считаем критическим
            return False