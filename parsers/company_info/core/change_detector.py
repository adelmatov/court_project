"""
Change detector - Compare old and new company data
"""

from typing import Dict, Any

from .logger import logger


class ChangeDetector:
    """
    Detect changes between old and new company data.
    """
    
    FIELDS_TO_CHECK = [
        'name_ru',
        'ceo_name',
        'is_nds',
        'degree_of_risk',
        'krp',    # теперь dict {'code': ..., 'name': ...}
        'kfc',
        'kse',
        'status',
        'oked',
        'phone'
    ]
    
    @staticmethod
    def detect_changes(
        old_data: Dict[str, Any],
        new_data: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Detect changes between old and new data.
        
        Args:
            old_data: Existing company data from DB
            new_data: New company data from API
        
        Returns:
            Dict with changed fields:
            {
                'name_ru': {'old': 'OLD NAME', 'new': 'NEW NAME'},
                'ceo_name': {'old': 'OLD CEO', 'new': 'NEW CEO'}
            }
        """
        
        changes = {}
        
        for field in ChangeDetector.FIELDS_TO_CHECK:
            old_value = old_data.get(field)
            new_value = new_data.get(field)
            
            if old_value != new_value:
                changes[field] = {
                    'old': old_value,
                    'new': new_value
                }
        
        if changes:
            logger.debug(
                f"Detected {len(changes)} changes for BIN {new_data.get('bin')}"
            )
        
        return changes