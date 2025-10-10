"""Валидатор ответов от API."""

import logging
from typing import Dict, Optional, Tuple


class APIValidator:
    """Валидатор ответов от API."""
    
    def __init__(self, logger: logging.Logger):
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
            context: Контекст для логирования
            
        Returns:
            (is_valid, error_message)
        """
        if not response_data:
            return False, "Пустой ответ"
        
        # Проверка code
        code = response_data.get('code', '').upper()
        
        # ✅ ИГНОРИРУЕМ CONTROLLER::RECAPTCHA (это ложная защита)
        if code == 'CONTROLLER::RECAPTCHA':
            self.logger.warning(f"⚠️ {context} | API вернул CAPTCHA, игнорируем")
            
            # Проверяем наличие данных
            data_obj = response_data.get('data')
            
            # ✅ ПРОВЕРКА: data может быть None
            if data_obj is not None and isinstance(data_obj, dict):
                items = data_obj.get('items', [])
                if items and len(items) > 0:
                    # Данные есть - считаем успешным
                    self.logger.info(f"✅ {context} | Данные получены несмотря на CAPTCHA")
                    return True, None
            
            # Данных нет - это пустая запись (не ошибка)
            self.logger.debug(f"ℹ️ {context} | CAPTCHA без данных - пустая запись")
            return True, None
        
        if code != 'OK':
            error_msg = f"Неверный code: {code}"
            if context:
                error_msg = f"{context} | {error_msg}"
            return False, error_msg
        
        # Проверка status
        status = response_data.get('status', '').lower()
        if status != 'success':
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
        Определяет, является ли ошибка критической (требует остановки парсера).
        
        Критические ошибки:
        - MAINTENANCE (техобслуживание)
        - SERVER_ERROR (серверная ошибка)
        - INTERNAL_ERROR (внутренняя ошибка)
        - SERVICE_UNAVAILABLE (сервис недоступен)
        
        ✅ CONTROLLER::RECAPTCHA НЕ является критической ошибкой
        
        Args:
            response_data: Данные ответа от API
            
        Returns:
            True если ошибка критическая
        """
        if not response_data:
            return False
        
        code = response_data.get('code', '').upper()
        
        # ✅ CAPTCHA - НЕ критическая ошибка
        if code == 'CONTROLLER::RECAPTCHA':
            return False
        
        critical_codes = {
            'MAINTENANCE', 
            'SERVER_ERROR', 
            'INTERNAL_ERROR', 
            'SERVICE_UNAVAILABLE'
        }
        
        return code in critical_codes