"""Обработчик данных от API."""

import logging
from datetime import datetime
from typing import Dict, Optional

from .config import Config


class DataProcessor:
    """Обработчик ответов от API."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def process_api_response(self, response_data: Dict) -> Optional[Dict]:
        """
        Обработка ответа от API.
        
        Args:
            response_data: Данные ответа от API
            
        Returns:
            Обработанные данные или None при ошибке
        """
        try:
            data_obj = response_data.get('data')
            if not data_obj:
                return None
            
            items = data_obj.get('items', [])
            if not items:
                return None
            
            item = items[0]
            
            result = {}
            
            # Маппинг основных полей
            for api_field, db_field in self.config.FIELD_MAPPING.items():
                value = item.get(api_field)
                if db_field in self.config.DATE_FIELDS and value:
                    value = self._normalize_date(value)
                result[db_field] = value
            
            # Извлечение дополнительной информации
            self._extract_organization_info(item, result)
            self._extract_subject_info(item, result)
            self._extract_audit_info(item, result)
            
            # Проверка обязательного поля
            if not result.get('registration_number'):
                self.logger.warning("⚠️ Отсутствует registration_number")
                return None
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки данных: {e}", exc_info=True)
            return None
    
    def _extract_organization_info(self, item: Dict, result: Dict):
        """Извлечение информации об организации."""
        org = item.get('org', {})
        result['revenue_name'] = org.get('nameRu')
        
        org_kpssu = item.get('orgKpssu', {})
        result['kpssu_name'] = org_kpssu.get('nameRu')
        
        check_type = item.get('checkType', {})
        result['check_type'] = check_type.get('nameRu')
        
        status = item.get('status', {})
        result['status_id'] = str(status.get('id')) if status.get('id') is not None else None
        result['status'] = status.get('nameRu')
    
    def _extract_subject_info(self, item: Dict, result: Dict):
        """Извлечение информации о субъекте."""
        subjects = item.get('subjects', [])
        if subjects:
            subject = subjects[0]
            result['subject_bin'] = subject.get('bin')
            result['subject_name'] = subject.get('nameRu')
            result['subject_address'] = subject.get('address')
    
    def _extract_audit_info(self, item: Dict, result: Dict):
        """Извлечение информации об аудите."""
        queries = item.get('queries', [])
        for i, query in enumerate(queries[:2]):
            suffix = "" if i == 0 else "_1"
            query_check = query.get('queryCheck', {})
            theme_check = query.get('themeCheck', {})
            result[f'audit_theme{suffix}'] = query_check.get('nameRu')
            result[f'theme_check{suffix}'] = theme_check.get('nameRu')
    
    def _normalize_date(self, date_str: str) -> Optional[str]:
        """
        Нормализация даты в формат PostgreSQL (YYYY-MM-DD).
        
        Args:
            date_str: Строка с датой
            
        Returns:
            Нормализованная дата или None
        """
        if not date_str:
            return None
        
        # Проверяем, что дата уже в правильном формате
        if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError:
                pass
        
        # Пробуем распарсить другие форматы
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        self.logger.warning(f"⚠️ Не удалось распарсить дату: {date_str}")
        return None