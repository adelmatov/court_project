"""
Data processor - Parse JSON from API
"""

from typing import Dict, List, Optional, Any
from datetime import datetime

from .logger import logger


class DataProcessor:
    """
    Process JSON data from API into database-ready format.
    """
    
    @staticmethod
    def parse_company(response: Dict[Any, Any]) -> Dict[str, Any]:
        """Parse API response with codes and descriptions."""
        
        basic = response.get('basicInfo', {})
        
        # Извлечь структуры {code, description}
        krp = DataProcessor._extract_code_and_desc(basic.get('krp'))
        kfc = DataProcessor._extract_code_and_desc(basic.get('kfc'))
        kse = DataProcessor._extract_code_and_desc(basic.get('kse'))
        status = DataProcessor._extract_code_and_desc(basic.get('status'))
        
        company = {
            'bin': basic.get('bin'),
            'name_ru': DataProcessor._extract_value(basic.get('titleRu')),
            'registration_date': DataProcessor._parse_date(
                DataProcessor._extract_value(basic.get('registrationDate'))
            ),
            'ceo_name': DataProcessor._extract_ceo_name(basic.get('ceo')),
            'is_nds': DataProcessor._extract_value(basic.get('isNds')),
            'degree_of_risk': DataProcessor._extract_value(basic.get('degreeOfRisk')),
            
            # Справочники (код + описание)
            'krp': krp,
            'kfc': kfc,
            'kse': kse,
            'status': status,
            
            'phone': response.get('egovContacts', {}).get('phone'),
        }
        
        # OKED
        primary_oked = DataProcessor._extract_value(basic.get('primaryOKED'))
        if primary_oked:
            parts = primary_oked.split(' ', 1)
            company['oked'] = {
                'code': parts[0],
                'name': parts[1] if len(parts) > 1 else None
            }
        else:
            company['oked'] = None
        
        # Остальное без изменений
        company['name_history'] = DataProcessor._parse_name_history(
            basic.get('titleRu'),
            company['registration_date']
        )
        company['taxes'] = DataProcessor._parse_taxes(response.get('taxes'))
        company['nds'] = DataProcessor._parse_nds(response.get('taxes'))
        company['relations'] = DataProcessor._parse_relations(
            response.get('relatedCompanies'),
            company['bin']
        )
        
        return company
    
    @staticmethod
    def _parse_name_history(
        title_ru: Optional[Dict],
        registration_date: str
    ) -> List[Dict[str, Any]]:
        """
        Parse name history from titleRu.actualListFront.
        
        Logic:
        1. Get current name from value
        2. Sort actualListFront by actualFrom (latest last)
        3. Take LAST entry from history
        4. If it differs from current → there was a rename
        
        Args:
            title_ru: titleRu field from API
            registration_date: Company registration date
        
        Returns:
            List of name history entries
        """
        
        if not title_ru:
            return []
        
        current_name = title_ru.get('value')
        history_list = title_ru.get('actualListFront', [])
        
        if not history_list:
            return [{
                'name_ru': current_name,
                'valid_from': registration_date,
                'valid_to': None,
                'is_current': True
            }]
        
        # Sort by actualFrom (latest last)
        sorted_history = sorted(
            history_list,
            key=lambda x: x['history']['actualFrom']
        )
        
        # Get last entry
        last_entry = sorted_history[-1]['history']
        last_name = last_entry['value']
        last_date = DataProcessor._parse_date(last_entry['actualFrom'])
        
        # If last differs from current
        if last_name != current_name:
            return [
                {
                    'name_ru': last_name,
                    'valid_from': registration_date,
                    'valid_to': last_date,
                    'is_current': False
                },
                {
                    'name_ru': current_name,
                    'valid_from': last_date,
                    'valid_to': None,
                    'is_current': True
                }
            ]
        
        # Check previous entry
        if len(sorted_history) > 1:
            prev_entry = sorted_history[-2]['history']
            prev_name = prev_entry['value']
            
            if prev_name != current_name:
                return [
                    {
                        'name_ru': prev_name,
                        'valid_from': registration_date,
                        'valid_to': last_date,
                        'is_current': False
                    },
                    {
                        'name_ru': current_name,
                        'valid_from': last_date,
                        'valid_to': None,
                        'is_current': True
                    }
                ]
        
        # No rename
        return [{
            'name_ru': current_name,
            'valid_from': registration_date,
            'valid_to': None,
            'is_current': True
        }]
    
    @staticmethod
    def _parse_taxes(taxes: Optional[Dict]) -> List[Dict[str, Any]]:
        """Parse taxes by year (only taxGraph, WITHOUT nds_paid)."""
        if not taxes:
            return []
        
        tax_graph = taxes.get('taxGraph', [])
        
        result = []
        for item in tax_graph:
            if not item:
                continue
            
            year = item.get('year')
            if not year:
                continue
            
            result.append({
                'year': year,
                'total_taxes': item.get('value', 0)
            })
        
        return result
    
    @staticmethod
    def _parse_nds(taxes: Optional[Dict]) -> List[Dict[str, Any]]:
        """
        Parse NDS data from ndsGraph.
        
        Args:
            taxes: taxes section from API response
        
        Returns:
            List of NDS entries by year
        """
        if not taxes:
            return []
        
        nds_graph = taxes.get('ndsGraph', [])
        
        result = []
        for item in nds_graph:
            if not item:
                continue
            
            year = item.get('year')
            if not year:
                continue
            
            result.append({
                'year': year,
                'nds_amount': item.get('value', 0)
            })
        
        return result

    @staticmethod
    def _parse_nds(taxes: Optional[Dict]) -> List[Dict[str, Any]]:
        """
        Parse NDS data from ndsGraph.
        
        Args:
            taxes: taxes section from API response
        
        Returns:
            List of NDS entries by year
        """
        if not taxes:
            return []
        
        nds_graph = taxes.get('ndsGraph', [])
        
        result = []
        for item in nds_graph:
            if not item:
                continue
            
            year = item.get('year')
            if not year:
                continue
            
            result.append({
                'year': year,
                'nds_amount': item.get('value', 0)
            })
        
        return result
    
    @staticmethod
    def _parse_relations(
        related: Optional[Dict],
        bin_from: str
    ) -> List[Dict[str, Any]]:
        """Parse related companies."""
        if not related:
            return []
        
        result = []
        
        # Same address
        same_address = related.get('sameAddress', {}).get('results', [])
        for item in same_address:
            if item and item.get('bin'):
                result.append({
                    'bin_from': bin_from,
                    'bin_to': item['bin'],
                    'relation_type': 'same_address'
                })
        
        # Same CEO
        same_fio = related.get('sameFio', {}).get('results', [])
        for item in same_fio:
            if item and item.get('bin'):
                result.append({
                    'bin_from': bin_from,
                    'bin_to': item['bin'],
                    'relation_type': 'same_ceo'
                })
        
        return result
    
    # ════════════════════════════════════════════════════════════════
    # HELPER METHODS
    # ════════════════════════════════════════════════════════════════
    
    @staticmethod
    def _extract_value(field: Optional[Dict]) -> Any:
        """
        Recursively extract value from nested dict structures.
        
        API returns data like:
        {'value': {'value': 'actual_data'}}
        or
        {'value': 'actual_data'}
        
        Args:
            field: Field from API response
        
        Returns:
            Extracted value (primitive type or None)
        """
        if field is None:
            return None
        
        # Если это не dict - вернуть как есть
        if not isinstance(field, dict):
            return field
        
        # РЕКУРСИВНО извлекаем value
        if 'value' in field:
            inner_value = field['value']
            # Если inner_value тоже dict с 'value' - рекурсия
            if isinstance(inner_value, dict) and 'value' in inner_value:
                return DataProcessor._extract_value(inner_value)
            # Иначе вернуть inner_value
            return inner_value
        
        # Если нет ключа 'value' - вернуть None
        return None
    
    @staticmethod
    def _extract_ceo_name(ceo_field: Optional[Dict]) -> Optional[str]:
        """Extract CEO name."""
        if not ceo_field:
            return None
        value = ceo_field.get('value', {})
        if isinstance(value, dict):
            return value.get('title')
        return None

    @staticmethod
    def _extract_code_and_desc(field: Optional[Dict]) -> Optional[Dict[str, Any]]:
        """
        Извлечь код И описание из справочника.
        
        Структура: {"value": {"value": 160, "description": "..."}}
        
        Returns:
            {'code': 160, 'name': '...'}
        """
        if not field:
            return None
        
        level1 = field.get('value')
        if not level1 or not isinstance(level1, dict):
            return None
        
        code = level1.get('value')
        description = level1.get('description')
        
        if code is not None and description:
            return {
                'code': int(code) if isinstance(code, (int, str)) and str(code).isdigit() else code,
                'name': str(description)
            }
        
        return None

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> Optional[str]:
        """Parse date to YYYY-MM-DD format."""
        if not date_str:
            return None
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None