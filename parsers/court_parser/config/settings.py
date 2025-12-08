"""
Загрузка и валидация конфигурации
"""
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
    