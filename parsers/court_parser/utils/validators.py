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