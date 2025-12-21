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
        interval_days = config.get('check_interval_days', 2)
        max_active_age_days = config.get('max_active_age_days')
        
        cases = await self.db_manager.get_cases_for_update({
            'defendant_keywords': filters.get('party_keywords', []),
            'exclude_event_types': filters.get('exclude_event_types', []),
            'update_interval_days': interval_days,
            'max_active_age_days': max_active_age_days
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